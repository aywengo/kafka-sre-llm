#!/usr/bin/env python3
"""v3 Phase 1: question bank. Writes realistic on-call questions from documentation chunks with a
local OpenAI-compatible server (client and retries from generate_qa.py), then filters them.

  1. Seeds: SEED_RULES below. Every chunk of the "all" rows, plus a seeded random sample of the
     rest, sized so each category quota can be filled --over times. Chunks under 300 chars are skipped.
  2. Eval slice: 8% of seed files (hash of the path) are reserved. Their questions go to
     --eval-slice-out and never to --out.
  3. Generation: PER_SEED questions per seed chunk (2 by default, 3 for the short categories, set per
     rule in SEED_RULES), one record per chunk appended to --raw. Resumable: seeds already in --raw are
     skipped; a failed call, or a seed whose reply was twice not JSON, is retried by the next run.
     Second stream (--ooc N, default 250, 0 = off): one call per topic of OOC_TOPICS asks for questions on
     adjacent topics the corpus does not cover (no material shown). Records: origin "ooc", treatment "ooc",
     topic, no seed chunk. Same validation, leakage filter and dedupe; not counted in the category quota;
     trimmed to N (seeded). Index chunks mentioning each topic's key terms are reported before generation.
  4. Leakage filter against every question of every file in --eval-glob: normalised BM25
     similarity >= 0.6, or a shared span of 8 consecutive words.
  5. Dedupe (Jaccard >= 0.7 on token sets, first one wins; eval-slice questions come first, so
     the bank never holds a near-copy of one).
  6. Category quota: over-full categories are trimmed with the seeded RNG. A category that is
     short stays short (reported in --stats); questions are never relabelled. Up to --reserve (default 600)
     of the trimmed-off questions go to --reserve-out instead of the rejects (seeded order, spread over
     categories in proportion to the quota): valid bank questions that are simply not packed yet.
  7. Fields on every record: origin "seed"|"ooc"; treatment "normal"|"withheld"|"ooc"; split
     "train"|"valid"|"evalslice". Split is by seed FILE: 8% of files (hash of the path) are the eval slice,
     of the rest a seeded 3% are "valid", so no file contributes to two splits; ooc questions are split by
     topic (2 seeded topics go to valid). --withheld N (default 400) bank questions are marked
     treatment="withheld" (seeded, spread over categories in proportion to their size): build_packs_v3.py
     removes the seed chunk from their pack. Only troubleshooting, how-to and config-review questions of the
     TRAIN split are eligible (never eval slice, valid or reserve: valid and the slice must stay clean
     measurements of normal rows).
Steps 4-7 are recomputed from --raw on every run, so the outputs depend only on --raw, --chunks,
the eval files, --target, --reserve, --withheld, --ooc and --seed. Every drop goes to a rejects file next to --out
(work/v3/questions.rejects.jsonl) with the reason and, for leaks, the eval file it matched.

Usage (from the repository root, key in OMLX_API_KEY as for generate_qa.py):
  .venv/bin/python scripts/gen_questions.py                 # full run; rerun to resume
  .venv/bin/python scripts/gen_questions.py --limit 0       # no generation: redo filters and stats
  .venv/bin/python scripts/gen_questions.py --dry-run --limit 60 --target 60 --raw /tmp/q/raw.jsonl \
      --out /tmp/q/questions.jsonl --stats /tmp/q/stats.md --eval-slice-out /tmp/q/evalslice.jsonl
--dry-run uses a fake LLM (no network). Its records are marked, and a real run refuses a --raw
file that holds them (and the other way round).
"""
import argparse
import asyncio
import glob
import hashlib
import json
import math
import os
import random
import re
import sys
import time
from collections import Counter, defaultdict
from fnmatch import fnmatch
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_dataset import bucket, norm
from generate_qa import CONTEXT_MAX_CHARS, LLM, extract_json
from retrieve import B, K1, index, tokens

# Seed rules, first match wins: (source glob, path substrings or None = any path, take, category,
# questions per seed chunk). take: ALL = every matching chunk is a seed; SAMPLE = seeded random sample.
# The category is only the EXPECTED category of questions from these chunks. It sizes the sample
# (enough seeds per category for its quota); the category of a question is always the model's label.
# Chunks that match no row are sampled too, MIN_PER_SOURCE per source.
ALL, SAMPLE = "all", "sample"
PER_SEED = 2             # default questions asked per seed chunk (5th column of a rule)
SEED_RULES = [
    ("runbooks", ["/kafka/"], ALL, "kafka-ops", 3),
    ("runbooks", None, ALL, "strimzi", 3),
    ("apache-kafka", ["/operations/", "/security/", "/design/", "/implementation/"], ALL, "kafka-ops", 3),
    ("strimzi", ["managing", "operators", "cruise-control", "upgrading", "security", "deploying"],
     ALL, "strimzi", PER_SEED),
    ("*registry*", None, ALL, "schema-registry", 3),
    ("*karapace*", None, ALL, "schema-registry", 3),
    ("apache-kafka", ["/streams/", "/kafka-connect/"], SAMPLE, "streaming", PER_SEED),
    ("apache-kafka", ["/configuration/"], SAMPLE, "kafka-config", PER_SEED),
    ("apache-kafka-config", None, SAMPLE, "kafka-config", PER_SEED),
    ("apache-kafka", None, SAMPLE, "kafka-ops", PER_SEED),
    ("strimzi", None, SAMPLE, "strimzi", PER_SEED),
    ("kubernetes", None, SAMPLE, "kubernetes", PER_SEED),
    ("argo-cd", None, SAMPLE, "argo-cd", PER_SEED),
]
MIN_CHARS = 300          # shorter chunks are never seeds
MIN_PER_SOURCE = 20      # every source gets at least this many seeds (or all its chunks)
EVAL_SLICE_FRAC = 0.08   # share of seed files reserved for the eval slice
VALID_FRAC = 0.03        # share of the remaining seed files reserved for the "valid" split
WITHHOLD_TYPES = ("troubleshooting", "how-to", "config-review")  # types eligible for treatment "withheld"
WARN_RAW = 1.3           # warn when expected raw questions of a category < this x its quota
OOC_OVERGEN = 1.5        # ooc: ask for N x this many questions in total
OOC_VALID_TOPICS = 2     # ooc topics that go to the valid split
LEAK_SIM, SPAN_WORDS, JACCARD = 0.6, 8, 0.7

# Out-of-corpus topics (adjacent to the corpus, written from general knowledge; NOT taken from any eval
# file). (topic, key terms): the terms are only used to report how many index chunks mention the topic
# (case-insensitive substring), so a topic that turns out to be covered by the index can be spotted.
OOC_TOPICS = [
    ("ksqlDB", ["ksqldb"]),
    ("Debezium connectors", ["debezium"]),
    ("Amazon MSK", ["amazon msk"]),
    ("Confluent Cloud", ["confluent cloud"]),
    ("Apache Flink", ["flink"]),
    ("Kafka UI tools (AKHQ, Kafdrop, Conduktor)", ["akhq", "kafdrop", "conduktor"]),
    ("cert-manager", ["cert-manager"]),
    ("HashiCorp Vault", ["hashicorp vault"]),
    ("External Secrets Operator", ["external secrets"]),
    ("Helm chart internals", ["helm"]),
    ("Flux", ["fluxcd", "flux cd"]),
    ("Istio and Linkerd service mesh with Kafka", ["istio", "linkerd"]),
    ("Prometheus Operator and Alertmanager configuration", ["alertmanager"]),
    ("Grafana dashboards", ["grafana"]),
    ("Velero backups", ["velero"]),
    ("Terraform providers for Kafka", ["terraform"]),
    ("Redpanda", ["redpanda"]),
    ("Pulsar", ["pulsar"]),
    ("RabbitMQ migration", ["rabbitmq"]),
    ("client libraries (librdkafka, kafka-python, Spring Kafka, Sarama)",
     ["librdkafka", "kafka-python", "spring kafka", "sarama"]),
    ("JVM garbage-collector tuning flags", ["g1gc", "-xx:"]),
    ("Linux kernel and filesystem tuning (XFS, sysctl)", ["xfs", "sysctl"]),
    ("cloud load balancers and DNS", ["load balancer", "external-dns"]),
    ("OpenTelemetry tracing for Kafka clients", ["opentelemetry"]),
]

# Share of --target per category, with the description shown to the model.
CATEGORIES = {
    "kafka-ops": (0.25, "running Apache Kafka itself: brokers, KRaft controllers, topics and partitions, "
                        "replication, consumer groups and lag, clients, reassignment, monitoring, security, upgrades"),
    "strimzi": (0.25, "the Strimzi operators and their custom resources (Kafka, KafkaNodePool, KafkaTopic, "
                      "KafkaUser, KafkaConnect, KafkaMirrorMaker2, KafkaRebalance), Cruise Control and Drain "
                      "Cleaner under Strimzi"),
    "kafka-config": (0.15, "what a specific Kafka setting does, its default, scope or tuning (broker, topic, "
                           "producer, consumer, admin, Connect)"),
    "argo-cd": (0.125, "Argo CD and GitOps delivery"),
    "kubernetes": (0.125, "Kubernetes itself: pods, scheduling, storage, networking, RBAC, node maintenance"),
    "schema-registry": (0.05, "schema registries: subjects, versions, compatibility, serializers"),
    "streaming": (0.05, "Kafka Streams, Kafka Connect pipelines and stream processing applications"),
}
QUOTA = {c: share for c, (share, _) in CATEGORIES.items()}
TYPE_MIX = {"troubleshooting": 0.50, "how-to": 0.25, "config-review": 0.15, "concept": 0.10}
TYPE_ALIASES = {"howto": "how-to"}
MENTIONS = re.compile(r"\b(the|this) (material|excerpt|documentation)\b", re.I)
THINKING_OFF = {"chat_template_kwargs": {"thinking_mode": "chat"}}  # as scripts/gen_v2.sh, for DeepSeek
URL_FILE = Path.home() / ".config/kafka-sre-llm/omlx_url"

Q_SYSTEM = """You write the questions of a question bank for an assistant that is a senior Site \
Reliability Engineer specialised in Apache Kafka, Strimzi, Kubernetes, GitOps (Argo CD), schema \
registries and data streaming. You are given one piece of documentation material. You write \
questions only, no answers.

Rules:
- Write the way people ask during an incident or in Slack: describe the situation, the symptom, \
the alert or the exact error message, and name the component. No quiz questions.
- The question must make sense to someone who has never seen the material. Never mention "the \
material", "the excerpt" or "the documentation", and never write "according to".
- Do not put the answer in the question.
- Use only names (settings, metrics, commands, resource kinds, errors) that appear in the material.
- Types:
  troubleshooting: something is broken or looks wrong; the engineer reports a symptom, an alert \
or an error and asks what to check or how to fix it.
  how-to: how to carry out a procedure or an operation safely.
  config-review: a setting, manifest or design the engineer has or plans, and wants reviewed.
  concept: how something works, what happens if, or the difference between two things.
- Give each question exactly one category from this list, chosen by what the question is about:
{categories}
- If the material is navigation, a changelog, a list of links or has no technical substance, \
return [].

Style examples (tone and level of detail only; do not copy or rephrase them):
{examples}

Return ONLY a JSON array. Each item: {"question": "...", "type": \
"<troubleshooting|how-to|config-review|concept>", "category": "<one category from the list>"}"""

Q_USER = """Source: {source} — {title}

<material>
{text}
</material>

Write {n} questions an on-call engineer would actually ask that this material helps answer: \
symptoms, alerts, error messages, 'how do I safely…', 'what happens if…'. The question must make \
sense without the material.
Write {types}. The questions must be about different situations. If the material cannot support \
a type, write the closest type it does support and label it as what it is."""


# Out-of-corpus stream: same types and categories, no material.
OOC_SYSTEM = """You write the questions of a question bank for an assistant that is a senior Site \
Reliability Engineer specialised in Apache Kafka, Strimzi, Kubernetes, GitOps (Argo CD), schema \
registries and data streaming. You are given one adjacent technology topic. You write questions \
only, no answers.

Rules:
- Write the way an on-call engineer in a team that runs Apache Kafka on Kubernetes asks during an \
incident or in Slack: describe the situation, the symptom, the alert or the exact error message, and \
name the component. No quiz questions.
- The questions are about the adjacent topic as that team meets it next to its Kafka platform. Do \
not mention that you were given a topic; never write "according to".
- Do not put the answer in the question.
- Use only names (settings, metrics, commands, resource kinds, errors) that you are sure exist.
- Types:
  troubleshooting: something is broken or looks wrong; the engineer reports a symptom, an alert \
or an error and asks what to check or how to fix it.
  how-to: how to carry out a procedure or an operation safely.
  config-review: a setting, manifest or design the engineer has or plans, and wants reviewed.
  concept: how something works, what happens if, or the difference between two things.
- Give each question exactly one category from this list, the one it is nearest to:
{categories}

Style examples (tone and level of detail only; do not copy or rephrase them):
{examples}

Return ONLY a JSON array. Each item: {"question": "...", "type": \
"<troubleshooting|how-to|config-review|concept>", "category": "<one category from the list>"}"""

OOC_USER = """Topic: {topic}

Write {n} questions an on-call engineer in a team running Apache Kafka on Kubernetes would actually \
ask about this topic: symptoms, alerts, error messages, 'how do I safely…', 'what happens if…'. \
Write {types}. The questions must be about different situations."""


def rule_for(chunk):
    """-> (take, expected category, questions per seed) of the first matching rule."""
    for source, paths, take, category, per in SEED_RULES:
        if fnmatch(chunk["source"], source) and (paths is None or any(p in chunk["path"] for p in paths)):
            return take, category, per
    return SAMPLE, None, PER_SEED


def select_seeds(chunks, target, over, seed):
    """-> (seed chunks in a seeded random order, {chunk id: "rule" | "sample"})."""
    rng = random.Random(seed)
    seeds, how, pool, have, pool_per = [], {}, defaultdict(list), Counter(), {}
    for c in chunks:
        if len(c["text"]) < MIN_CHARS:
            continue
        take, category, per = rule_for(c)
        if take == ALL:
            seeds.append(c)
            how[c["id"]] = "rule"
            have[category] += per  # expected questions
        else:
            pool[category].append(c)
            pool_per.setdefault(category, per)  # the sample rules of one category share their per-seed number

    def add(cands, n):
        for c in rng.sample(cands, max(0, min(n, len(cands)))):
            seeds.append(c)
            how[c["id"]] = "sample"

    for cat, share in QUOTA.items():  # enough seeds per expected category to fill its quota `over` times
        add(pool[cat], math.ceil((share * target * over - have[cat]) / pool_per.get(cat, PER_SEED)))
    rest = [c for cs in pool.values() for c in cs]
    for source in sorted({c["source"] for c in rest}):  # every source is covered
        add([c for c in rest if c["source"] == source and c["id"] not in how],
            MIN_PER_SOURCE - sum(c["source"] == source for c in seeds))
    rng.shuffle(seeds)  # a partial run (--limit, crash) still covers every source
    return seeds, how


def seed_plan(plan, target, over):
    """Per expected category: (seeds planned, expected raw questions = sum of their per-seed numbers, raw
    questions wanted = `over` times the quota, quota). Expected raw under WARN_RAW x quota means the index
    has too few such chunks; the quota then depends on how the model labels other seeds."""
    seeds, raw = Counter(), Counter()
    for c in plan:
        _, cat, per = rule_for(c)
        seeds[cat] += 1
        raw[cat] += per
    return {cat: (seeds[cat], raw[cat], math.ceil(share * target * over), round(share * target))
            for cat, share in QUOTA.items()}


def plan_lines(plan, target, over):
    """The pre-run plan table, one line per category, with the capacity warning."""
    out = []
    for cat, (n, raw, want, quota) in seed_plan(plan, target, over).items():
        out.append(f"  {cat}: {n} seeds, expected raw {raw} vs quota {quota} ({raw / max(1, quota):.2f}x; "
                   f"wanted {want})" + (f"  WARNING: expected raw < {WARN_RAW} x quota" if raw < WARN_RAW * quota else ""))
    return out


def split_of(path, seed):
    """"evalslice" (fixed, never moves with --seed), "valid" (seeded) or "train", by seed FILE."""
    if in_eval_slice(path):
        return "evalslice"
    return "valid" if bucket(path, f"valid:{seed}") < VALID_FRAC else "train"


def apportion(total, weights, caps):
    """Whole numbers per key that sum to min(total, sum of caps), in proportion to `weights` (largest
    remainder), never above `caps`; what a capped key cannot take goes to the keys with room, by weight."""
    keys = list(weights)
    alloc, left = {k: 0 for k in keys}, min(total, sum(caps.values()))
    while left > 0:
        room = [k for k in keys if alloc[k] < caps[k]]
        wsum = sum(max(weights[k], 1e-9) for k in room)
        ideal = {k: left * max(weights[k], 1e-9) / wsum for k in room}
        give = {k: min(int(ideal[k]), caps[k] - alloc[k]) for k in room}
        if not any(give.values()):  # only fractions left: one each, largest fraction first
            for k in sorted(room, key=lambda k: (-(ideal[k] - int(ideal[k])), keys.index(k)))[:left]:
                give[k] = 1
        for k, g in give.items():
            alloc[k] += g
        left -= sum(give.values())
    return alloc


def topic_mentions(chunks):
    """{topic: number of index chunks whose text mentions any key term (case-insensitive substring)}."""
    low = [c["text"].lower() for c in chunks]
    return {t: sum(any(k in x for k in terms) for x in low) for t, terms in OOC_TOPICS}


def in_eval_slice(path):
    return bucket(path, "evalslice") < EVAL_SLICE_FRAC  # fixed salt: the reservation never moves with --seed


def system_prompt(style_file, seed, template=Q_SYSTEM):
    qs = [json.loads(l)["question"] for l in open(style_file) if l.strip()]
    examples = random.Random(seed).sample(qs, 5)
    return (template.replace("{categories}", "\n".join(f"  {c}: {d}" for c, (_, d) in CATEGORIES.items()))
            .replace("{examples}", "\n".join(f"- {q}" for q in examples)))


class FakeLLM:
    """--dry-run stand-in for generate_qa.LLM: no network, the same reply for the same prompt.
    Some replies are broken on purpose (no JSON at the first try, an empty list, an unknown
    category, a mention of the documentation, a repeated question, a copied style example, an
    unnormalised type) so that every later step has something to catch."""
    TEMPLATES = {
        "troubleshooting": "After a change to {title} we see errors mentioning {a} and {b}, and {c} "
                           "looks wrong. What should I check first?",
        "how-to": "How do I safely change {a} for {title} without disrupting {b} or {c}?",
        "config-review": "We plan to set {a} together with {b} for {title} in production. Is that "
                         "reasonable given {c}?",
        "concept": "What happens if {a} is used without {b} in {title}, and how does {c} affect it?",
    }

    def __init__(self):
        self.usage, self.seen = Counter(), set()

    async def chat(self, system, user, **_):
        h = int(hashlib.sha1(user.encode()).hexdigest(), 16)
        self.usage["calls"] += 1
        first, case = user not in self.seen, h % 25
        self.seen.add(user)
        if case == 0 and first:
            return "Sure! Here are the questions you asked for."
        if case == 1:
            return "[]"
        if user.startswith("Topic: "):
            return self.ooc_reply(system, user, h, case)
        source, title = re.match(r"Source: (.*?) — (.*)", user).groups()
        words = sorted(set(tokens(user.split("<material>")[1].split("</material>")[0])),
                       key=lambda w: hashlib.sha1((w + title).encode()).hexdigest()) + ["brokers"] * 36
        hint = next((r[3] for r in SEED_RULES if r[1] is None and fnmatch(source, r[0])), "kafka-ops")
        items = []
        for i, t in enumerate(re.findall(r'question \d+ of type "([a-z-]+)"', user)):
            a, b, c = words[3 * i: 3 * i + 3]
            # one label in five is not the expected category, as a real model would do
            cat = hint if (h >> (8 + 4 * i)) % 5 else list(QUOTA)[(h >> 32) % len(QUOTA)]
            items.append({"question": self.TEMPLATES[t].format(title=title, a=a, b=b, c=c),
                          "type": t, "category": cat})
        if case == 2:
            items[0]["category"] = "networking"
        if case == 3:
            items[0]["question"] += " The documentation is not clear about it."
        if case == 4:
            items[0]["question"] = "How do I safely restart one Kafka broker without losing in-flight messages?"
        if case == 5:
            items[-1]["question"] = system.split("Style examples")[1].split("\n- ")[1 + h % 5].split("\n")[0]
        if case == 6:
            items[0]["type"] = "howto"
            return "```json\n" + json.dumps(items) + "\n```"
        return json.dumps(items)

    @staticmethod
    def ooc_reply(system, user, h, case):
        """Questions on the topic; five hash-derived words each keep them apart under the dedupe."""
        topic = user.split("\n")[0][len("Topic: "):]
        items = []
        for i, t in enumerate(re.findall(r'question \d+ of type "([a-z-]+)"', user)):
            w = [hashlib.sha1(f"{user}:{i}:{j}".encode()).hexdigest()[:6] for j in range(5)]
            q = {"troubleshooting": f"Our brokers on Kubernetes started failing after we touched {topic}: logs show "
                                    f"{w[0]} {w[1]} and {w[2]}. What should I check first, {w[3]} or {w[4]}?",
                 "how-to": f"How do I safely wire {topic} to a Strimzi cluster without disturbing {w[0]} {w[1]}, "
                           f"given {w[2]} and {w[3]} on {w[4]}?",
                 "config-review": f"We plan {topic} with {w[0]} set to {w[1]} next to {w[2]}, {w[3]} and {w[4]} "
                                  f"in production. Does that look reasonable?",
                 "concept": f"What happens to {w[0]} when {topic} meets {w[1]}, and how do {w[2]} {w[3]} {w[4]} "
                            f"change it?"}[t]
            items.append({"question": q, "type": t, "category": list(QUOTA)[(h >> (8 + 3 * i)) % len(QUOTA)]})
        if case == 2:
            items[0]["category"] = "networking"
        if case == 3:
            items[0]["question"] += " The documentation is not clear about it."
        return json.dumps(items)


async def process(chunk, llm, system, args, sem, out_f, lock, stats):
    per = rule_for(chunk)[2]
    types = random.Random(f"{args.seed}:{chunk['id']}").choices(list(TYPE_MIX), weights=TYPE_MIX.values(), k=per)
    user = Q_USER.format(source=chunk["source"], title=chunk["title"] or chunk["path"].rsplit("/", 1)[-1],
                         text=chunk["text"][:CONTEXT_MAX_CHARS], n=per,
                         types=", ".join(f'question {i} of type "{t}"' for i, t in enumerate(types, 1)))
    async with sem:
        items = None
        for _ in range(2):  # one more try when the reply is not JSON; after that the next run tries again
            try:
                items = extract_json(await llm.chat(system, user))
            except Exception as e:  # not recorded, so the next run retries this seed
                print(f"  gen failed {chunk['id']}: {e!r}")
                stats["failed"] += 1
                return
            if items is not None:
                break
        rec = {"chunk_id": chunk["id"], "source": chunk["source"], "path": chunk["path"], "types": types,
               "items": [it for it in items or [] if isinstance(it, dict)]}
        if items is None:
            rec["parse_fail"] = True
        if args.dry_run:
            rec["dry_run"] = True
        async with lock:
            # Always one record per seed (even empty) so reruns skip it.
            out_f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            out_f.flush()
            stats["done"] += 1
            stats["items"] += len(rec["items"])
            if stats["done"] % 50 == 0:
                mins = (time.monotonic() - stats["_t0"]) / 60
                print(f"  done={stats['done']} questions={stats['items']} failed={stats['failed']} | "
                      f"{stats['done'] / max(mins, 1e-9):.1f} seeds/min | {llm.usage['calls']} calls, "
                      f"{llm.usage['completion_tokens'] / max(1, llm.usage['calls']):.0f} out tok/call", flush=True)


async def process_ooc(topic, n, llm, system, args, sem, out_f, lock, stats):
    """One call for one out-of-corpus topic, n questions, no material. Record: {ooc_topic, n, types, items}."""
    types = random.Random(f"{args.seed}:ooc:{topic}").choices(list(TYPE_MIX), weights=TYPE_MIX.values(), k=n)
    user = OOC_USER.format(topic=topic, n=n, types=", ".join(f'question {i} of type "{t}"'
                                                             for i, t in enumerate(types, 1)))
    async with sem:
        items = None
        for _ in range(2):
            try:  # about 90 tokens per question, whatever --max-tokens says
                items = extract_json(await llm.chat(system, user, max_tokens=max(args.max_tokens, 120 * n + 200)))
            except Exception as e:
                print(f"  ooc gen failed {topic!r}: {e!r}")
                stats["failed"] += 1
                return
            if items is not None:
                break
        rec = {"ooc_topic": topic, "n": n, "types": types, "items": [it for it in items or [] if isinstance(it, dict)]}
        if items is None:
            rec["parse_fail"] = True
        if args.dry_run:
            rec["dry_run"] = True
        async with lock:
            out_f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            out_f.flush()
            stats["ooc_done"] += 1
            stats["ooc_items"] += len(rec["items"])


def read_raw(path, dry_run):
    recs = [json.loads(l) for l in open(path) if l.strip()] if Path(path).exists() else []
    if any(bool(r.get("dry_run")) != dry_run for r in recs):
        sys.exit(f"{path} holds {'real' if dry_run else '--dry-run'} records; use another --raw or delete it")
    return recs


def load_eval(pattern, idf, idf_unseen):
    """Eval questions as BM25 documents: [(file, {term: weight}, score against itself)], plus their word spans."""
    files = sorted(glob.glob(pattern))
    if not files:
        sys.exit(f"no eval files match {pattern!r}: the leakage filter has nothing to check against")
    qs, counts = [], {}
    for f in files:
        rows = [(f, json.loads(l)["question"]) for l in open(f) if l.strip()]
        if not rows:
            sys.exit(f"{f}: no questions")
        counts[f] = len(rows)
        qs += rows
    avg = sum(len(tokens(q)) for _, q in qs) / len(qs)
    docs, spans = [], {}
    for f, q in qs:
        tf = Counter(tokens(q))
        n = sum(tf.values())
        w = {t: idf.get(t, idf_unseen) * c * (K1 + 1) / (c + K1 * (1 - B + B * n / avg)) for t, c in tf.items()}
        if w:
            docs.append((f, w, sum(w.values())))
        for g in word_spans(q):
            spans.setdefault(g, f)
    return {"files": counts, "docs": docs, "spans": spans}


def word_spans(question):
    w = norm(question).split()
    return [" ".join(w[i:i + SPAN_WORDS]) for i in range(len(w) - SPAN_WORDS + 1)]


def leak(question, ev):
    """-> (highest normalised BM25 similarity to an eval question, its file, file sharing a word span or None).
    Normalised = score of `question` as a query against the eval question / the eval question's own score."""
    qt = set(tokens(question))
    # sums run over w (insertion order), not over the set: the same floats in every process
    sim, f_sim = max((sum(x for t, x in w.items() if t in qt) / own, f) for f, w, own in ev["docs"])
    return sim, f_sim, next((ev["spans"][g] for g in word_spans(question) if g in ev["spans"]), None)


def candidate(rec, it):
    """One raw item -> (bank record, reason to drop it or None)."""
    q = it.get("question")
    q = q.strip() if isinstance(q, str) else ""
    typ = re.sub(r"[\s_]+", "-", str(it.get("type", "")).strip().lower())
    typ, cat = TYPE_ALIASES.get(typ, typ), str(it.get("category", "")).strip().lower()
    reason = ("invalid" if len(q) < 15 else "bad_type" if typ not in TYPE_MIX
              else "bad_category" if cat not in QUOTA else "mentions_material" if MENTIONS.search(q) else None)
    c = {"id": hashlib.sha1(q.encode()).hexdigest()[:16], "question": q, "type": typ, "category": cat}
    if "ooc_topic" in rec:
        return {**c, "origin": "ooc", "topic": rec["ooc_topic"]}, reason
    return {**c, "origin": "seed", "seed_chunk_id": rec["chunk_id"], "seed_source": rec["source"],
            "seed_path": rec["path"]}, reason


def finish(c, treatment, split):
    """Bank record with the fields every record carries (key order fixed)."""
    head = {k: c[k] for k in ("id", "question", "type", "category", "origin")}
    tail = {k: c[k] for k in (("topic",) if c["origin"] == "ooc" else ("seed_chunk_id", "seed_source", "seed_path"))}
    return {**head, "treatment": treatment, "split": split, **tail}


def dedupe(cands):
    """-> (kept, [(dropped, the kept one it repeats)]). Jaccard on token sets, first one wins."""
    kept, sets, inv, dups = [], [], defaultdict(list), []
    for c in cands:
        s = set(tokens(c["question"]))
        shared = Counter(j for t in s for j in inv[t])
        j = min((j for j, n in shared.items() if n / (len(s) + len(sets[j]) - n) >= JACCARD), default=None)
        if j is not None:
            dups.append((c, kept[j]))
            continue
        for t in s:
            inv[t].append(len(kept))
        kept.append(c)
        sets.append(s)
    return kept, dups


def ooc_n(args):
    """Questions asked per topic call: ceil(N x 1.5 / number of topics); 0 when the stream is off."""
    return math.ceil(args.ooc * OOC_OVERGEN / len(OOC_TOPICS)) if args.ooc > 0 else 0


def is_slice(c):
    return c["origin"] == "seed" and in_eval_slice(c["seed_path"])


def build(args, chunks, idf):
    """Filters, dedupe, splits, quota, reserve, withheld marks and the ooc stream from the raw file.
    Writes --out, --eval-slice-out, --reserve-out and the rejects."""
    pos = {c["id"]: i for i, c in enumerate(chunks)}
    topic_pos, n_ooc = {t: i for i, (t, _) in enumerate(OOC_TOPICS)}, ooc_n(args)
    raw = read_raw(args.raw, args.dry_run)
    seed_recs = {r["chunk_id"]: r for r in raw if r.get("chunk_id") in pos}  # the last record of a seed counts
    ooc_recs = {r["ooc_topic"]: r for r in raw if n_ooc and r.get("ooc_topic") in topic_pos and r.get("n") == n_ooc}
    # index order, not completion order: the result does not depend on how the calls interleaved
    recs = (sorted(seed_recs.values(), key=lambda r: pos[r["chunk_id"]])
            + sorted(ooc_recs.values(), key=lambda r: topic_pos[r["ooc_topic"]]))
    ev = load_eval(args.eval_glob, idf, math.log(1 + (len(chunks) + 0.5) / 0.5))

    drops, cands, near = [], [], Counter()
    for r in recs:
        for it in r["items"]:
            c, reason = candidate(r, it)
            extra = {}
            if not reason:
                sim, f_sim, f_span = leak(c["question"], ev)
                if sim >= LEAK_SIM:
                    reason, extra = "leak_bm25", {"similarity": round(sim, 3), "eval_file": f_sim}
                elif f_span:
                    reason, extra = "leak_span8", {"similarity": round(sim, 3), "eval_file": f_span}
                else:
                    near["0.5-0.6" if sim >= 0.5 else "0.4-0.5" if sim >= 0.4 else "under 0.4"] += 1
            if reason:
                drops.append({**c, "reason": reason, **extra})
            else:
                cands.append(c)

    # eval-slice questions first, ooc last (stable): a bank question never repeats a slice question
    cands.sort(key=lambda c: 2 if c["origin"] == "ooc" else 0 if is_slice(c) else 1)
    kept, dups = dedupe(cands)
    drops += [{**c, "reason": "duplicate", "duplicate_of": o["id"]} for c, o in dups]
    dup_of_slice = sum(is_slice(o) and not is_slice(c) for c, o in dups)
    slice_qs = [c for c in kept if is_slice(c)]
    bank = [c for c in kept if c["origin"] == "seed" and not is_slice(c)]
    ooc_kept = [c for c in kept if c["origin"] == "ooc"]

    # category quota, then the reserve out of what the quota trimmed off
    out, per_cat, trimmed = [], {}, {}
    for cat, share in QUOTA.items():
        have, quota = [c for c in bank if c["category"] == cat], round(share * args.target)
        keep = have if len(have) <= quota else random.Random(f"{args.seed}:{cat}").sample(have, quota)
        ids = {c["id"] for c in keep}
        trimmed[cat] = [c for c in have if c["id"] not in ids]
        out += keep
        per_cat[cat] = (quota, len(have), len(keep))
    res_n = apportion(args.reserve, dict(QUOTA), {cat: len(t) for cat, t in trimmed.items()})
    reserve = []
    for cat in QUOTA:
        picked = random.Random(f"{args.seed}:reserve:{cat}").sample(trimmed[cat], res_n[cat])  # seeded order
        reserve += picked
        used = {c["id"] for c in picked}
        drops += [{**c, "reason": "over_quota"} for c in trimmed[cat] if c["id"] not in used]
    out.sort(key=lambda c: pos[c["seed_chunk_id"]])
    reserve.sort(key=lambda c: pos[c["seed_chunk_id"]])
    assert not any(in_eval_slice(c["seed_path"]) for c in out + reserve)

    # withheld: train split only, eligible types, spread over categories in proportion to their size
    elig = {cat: [c for c in out if c["category"] == cat and c["type"] in WITHHOLD_TYPES
                  and split_of(c["seed_path"], args.seed) == "train"] for cat in QUOTA}
    wh_n = apportion(args.withheld, {cat: per_cat[cat][2] for cat in QUOTA}, {cat: len(e) for cat, e in elig.items()})
    withheld = {c["id"] for cat in QUOTA
                for c in random.Random(f"{args.seed}:withheld:{cat}").sample(elig[cat], wh_n[cat])}

    # ooc: trimmed to N (seeded, not in the quota); two seeded topics go to valid
    ooc_avail = len(ooc_kept)
    if len(ooc_kept) > args.ooc:
        keep = {c["id"] for c in random.Random(f"{args.seed}:ooc").sample(ooc_kept, args.ooc)}
        drops += [{**c, "reason": "over_ooc"} for c in ooc_kept if c["id"] not in keep]
        ooc_kept = [c for c in ooc_kept if c["id"] in keep]
    live = [t for t, _ in OOC_TOPICS if any(c["topic"] == t for c in ooc_kept)]
    valid_topics = set(random.Random(f"{args.seed}:oocvalid").sample(live, min(OOC_VALID_TOPICS, len(live))))

    final = ([finish(c, "withheld" if c["id"] in withheld else "normal", split_of(c["seed_path"], args.seed)) for c in out]
             + [finish(c, "ooc", "valid" if c["topic"] in valid_topics else "train") for c in ooc_kept])
    reserve_out = [finish(c, "normal", split_of(c["seed_path"], args.seed)) for c in reserve]
    slice_out = [finish(c, "normal", "evalslice") for c in slice_qs]
    rejects = Path(args.out).with_suffix(".rejects.jsonl")
    for path, rows in ((args.out, final), (args.eval_slice_out, slice_out), (args.reserve_out, reserve_out),
                       (rejects, drops)):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            for r in rows:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
    return {"recs": recs, "stale": len(raw) - len(recs), "ev": ev, "near": near, "drops": drops, "dups": len(dups),
            "dup_of_slice": dup_of_slice, "per_cat": per_cat, "out": final, "bank": [r for r in final if r["origin"] == "seed"],
            "ooc": [r for r in final if r["origin"] == "ooc"], "ooc_available": ooc_avail, "slice": slice_out, "reserve": reserve_out,
            "rejects": rejects, "trimmed": {cat: len(t) for cat, t in trimmed.items()}}


def table(head, rows):
    return (["| " + " | ".join(head) + " |", "|" + "---|" * len(head)]
            + ["| " + " | ".join(str(x) for x in r) + " |" for r in rows] + [""])


def write_stats(args, chunks, plan, seeds, how, s, mentions):
    recs, out = s["recs"], s["bank"]
    done = {r["chunk_id"] for r in recs if "chunk_id" in r}
    drop = Counter(d["reason"] for d in s["drops"])
    n_raw = sum(len(r["items"]) for r in recs)
    L = ["# Question bank (v3 Phase 1)", ""]
    if args.dry_run:
        L += ["**DRY RUN: the questions come from the fake LLM, not from a model.**", ""]
    L += [f"chunks `{args.chunks}`, model `{'fake' if args.dry_run else args.model}`, target {args.target}, "
          f"seed {args.seed}, over-generation {args.over}, questions per seed set per rule in SEED_RULES "
          f"(default {PER_SEED}).", "",
          f"**{len(out)} bank questions kept** in `{args.out}` (target {args.target}) plus {len(s['ooc'])} "
          f"out-of-corpus (--ooc {args.ooc}); {len(s['reserve'])} in the reserve `{args.reserve_out}` "
          f"(--reserve {args.reserve}), {len(s['slice'])} in the eval slice `{args.eval_slice_out}`, "
          f"{len(s['drops'])} drops in `{s['rejects']}`.", ""]

    L += ["## Seeds per source", ""]
    rows = []
    for src in sorted({c["source"] for c in chunks}):
        mine = [c for c in seeds if c["source"] == src]
        rows.append((src, sum(c["source"] == src and len(c["text"]) >= MIN_CHARS for c in chunks),
                     sum(how[c["id"]] == "rule" for c in mine), sum(how[c["id"]] == "sample" for c in mine),
                     sum(c["id"] in done for c in mine), sum(in_eval_slice(c["path"]) for c in mine)))
    rows.append(("**total**", *(sum(r[i] for r in rows) for i in range(1, 6))))
    L += table(["source", f"chunks >= {MIN_CHARS} chars", "seeds by rule", "seeds sampled", "generated",
                "seeds in eval slice"], rows)
    if len(seeds) < len(plan):
        L += [f"--limit {args.limit}: {len(seeds)} of the {len(plan)} planned seeds.", ""]
    L += [f"Plan per expected category (SEED_RULES; expected raw = sum of the per-seed question numbers of the "
          f"planned seeds; wanted = quota x {args.over}; warning below {WARN_RAW} x quota):", ""]
    L += table(["expected category", "seeds planned", "expected raw", "quota", "raw / quota", "raw wanted", ""],
               [(c, n, raw, quota, f"{raw / max(1, quota):.2f}", want,
                 f"WARNING: under {WARN_RAW} x quota" if raw < WARN_RAW * quota else "")
                for c, (n, raw, want, quota) in seed_plan(plan, args.target, args.over).items()])
    extra = len(done - {c["id"] for c in seeds})
    L += [f"{len(recs)} seed records in `{args.raw}`"
          + (f" ({extra} from earlier runs with other seeds, used as well)" if extra else "")
          + (f"; {s['stale']} records skipped (chunk id not in the index, or replaced by a retry)"
             if s["stale"] else "")
          + f". {sum(not r['items'] for r in recs)} seeds gave no question, "
          f"{sum(bool(r.get('parse_fail')) for r in recs)} of them because the reply was not JSON "
          f"(the next run tries those again).", ""]

    L += ["## Raw questions and drops", "", f"Raw questions: **{n_raw}**.", ""]
    names = [("invalid", "no question text or under 15 chars"), ("bad_type", "type not in the list"),
             ("bad_category", "category not in the list (never relabelled)"),
             ("mentions_material", "mentions the material / excerpt / documentation"),
             ("leak_bm25", f"normalised BM25 similarity >= {LEAK_SIM} to an eval question"),
             ("leak_span8", f"shares {SPAN_WORDS} consecutive words with an eval question"),
             ("duplicate", f"Jaccard >= {JACCARD} with an earlier question"),
             ("over_quota", "category over its quota, trimmed at random (seeded), not taken into the reserve"),
             ("over_ooc", f"out-of-corpus questions above --ooc {args.ooc}, trimmed at random (seeded)")]
    L += table(["reason", "dropped", "meaning"], [(k, drop[k], v) for k, v in names])
    L += [f"Duplicates: **{s['dups']}**, of which {s['dup_of_slice']} bank questions repeating an eval-slice question.",
          ""]

    L += ["## Leakage filter", "",
          f"Eval files used by the leakage filter (`{args.eval_glob}`): "
          + ", ".join(f"`{f}` ({n} questions)" for f, n in s["ev"]["files"].items()) + ".", ""]
    by_file = Counter((d["eval_file"], d["reason"]) for d in s["drops"] if "eval_file" in d)
    if by_file:
        L += table(["eval file", "reason", "dropped"], [(f, r, n) for (f, r), n in sorted(by_file.items())])
    L += ["Highest similarity of the questions that passed: "
          + ", ".join(f"{k}: {s['near'][k]}" for k in ("0.5-0.6", "0.4-0.5", "under 0.4")) + ".", ""]

    L += ["## Kept per category", ""]
    L += table(["category", "quota", "available after filters", "kept", ""],
               [(c, q, have, kept, "SHORT" if kept < q else "") for c, (q, have, kept) in s["per_cat"].items()])
    short = [f"{c} {kept} of {q}" for c, (q, _, kept) in s["per_cat"].items() if kept < q]
    L += ["Quota not reached (kept what there is, nothing relabelled or moved between categories): "
          + "; ".join(short) + "." if short else "Every category reached its quota.", ""]

    types = Counter(c["type"] for c in out)
    L += ["## Kept per type", ""]
    L += table(["type", "kept", "share", "aimed at"],
               [(t, types[t], f"{types[t] / max(1, len(out)):.0%}", f"{w:.0%}") for t, w in TYPE_MIX.items()])
    L += ["## Kept per seed source", ""]
    L += table(["seed source", "kept"], sorted(Counter(c["seed_source"] for c in out).items()))

    files = {c["path"]: c["source"] for c in seeds}
    sliced = [p for p in files if in_eval_slice(p)]
    L += ["## Eval slice", "",
          f"{len(sliced)} of {len(files)} seed files reserved ({len(sliced) / max(1, len(files)):.1%}; "
          f"bucket(path) < {EVAL_SLICE_FRAC}). **{len(s['slice'])} questions** in `{args.eval_slice_out}` after the "
          f"leakage filter and dedupe (no quota); none of these files contributes to `{args.out}`.", ""]
    L += table(["seed source", "files in slice", "eval-slice questions"],
               [(src, sum(files[p] == src for p in sliced), sum(c["seed_source"] == src for c in s["slice"]))
                for src in sorted(set(files.values()))])

    cats = list(QUOTA)
    res = Counter(c["category"] for c in s["reserve"])
    L += ["## Reserve", "",
          f"{len(s['reserve'])} of --reserve {args.reserve} questions in `{args.reserve_out}` (valid bank questions, "
          f"not packed yet; taken from the questions the quota trimmed off, spread over categories in proportion "
          f"to the quota).", ""]
    L += table(["category", "quota", "trimmed off by the quota", "reserve", "dropped as over_quota"],
               [(c, q, s["trimmed"][c], res[c], s["trimmed"][c] - res[c]) for c, (q, _, _) in s["per_cat"].items()])

    L += ["## Out-of-corpus stream", ""]
    if not args.ooc:
        L += ["Off (--ooc 0).", ""]
    else:
        n_ooc = ooc_n(args)
        have = Counter(c["topic"] for c in s["ooc"])
        by_split = {c["topic"]: c["split"] for c in s["ooc"]}
        raw_n = {r["ooc_topic"]: len(r["items"]) for r in recs if "ooc_topic" in r}
        L += [f"--ooc {args.ooc}: {n_ooc} questions asked per topic call ({len(OOC_TOPICS)} topics, "
              f"ceil(N x {OOC_OVERGEN} / topics)); {s['ooc_available']} survived validation, leakage filter and "
              f"dedupe, trimmed to {len(s['ooc'])}. Not counted in the category quota.", "",
              "Index chunks that mention a topic's key terms (case-insensitive substring) show how well the "
              "index already covers it; no topic is dropped automatically.", ""]
        L += table(["topic", "key terms", "index chunks mentioning", "raw", "kept", "split"],
                   [(t, ", ".join(terms), mentions[t], raw_n.get(t, 0), have[t], by_split.get(t, ""))
                    for t, terms in OOC_TOPICS])
        L += [f"Ooc questions by category (the model's nearest label): "
              + ", ".join(f"{c} {n}" for c, n in sorted(Counter(c["category"] for c in s["ooc"]).items())) + ".", ""]

    wh = [c for c in s["bank"] if c["treatment"] == "withheld"]
    wtypes = list(WITHHOLD_TYPES)
    L += ["## Withheld", "",
          f"{len(wh)} of --withheld {args.withheld} bank questions marked treatment=\"withheld\" (train split, types "
          f"{', '.join(wtypes)}; seeded, spread over categories in proportion to their size). The seed chunk is "
          f"removed from their pack by build_packs_v3.py.", ""]
    L += table(["category", "bank size", *wtypes, "withheld"],
               [(c, per_cat_kept, *(sum(w["category"] == c and w["type"] == t for w in wh) for t in wtypes),
                 sum(w["category"] == c for w in wh)) for c, per_cat_kept in
                ((c, s["per_cat"][c][2]) for c in cats)])

    L += ["## Split, origin and treatment", "",
          "Split is by seed file (eval slice 8% of files, then a seeded 3% of the rest for valid); ooc by topic "
          f"({OOC_VALID_TOPICS} topics to valid). Withheld rows are drawn from the train split only.", ""]
    allr = s["out"] + s["slice"] + s["reserve"]
    kinds = [("`--out` bank", s["bank"]), ("`--out` ooc", s["ooc"]), ("eval slice file", s["slice"]),
             ("reserve file", s["reserve"])]
    L += table(["file", "rows", "train", "valid", "evalslice"],
               [(n, len(r), *(sum(x["split"] == sp for x in r) for sp in ("train", "valid", "evalslice")))
                for n, r in kinds])
    L += table(["origin", "treatment", "rows (all files)"],
               [(o, t, n) for (o, t), n in sorted(Counter((x["origin"], x["treatment"]) for x in allr).items())])
    file_splits = defaultdict(set)
    for x in allr:
        if x["origin"] == "seed":
            file_splits[x["seed_path"]].add(x["split"])
    L += [f"Seed files whose questions fall into more than one split: {sum(len(v) > 1 for v in file_splits.values())} "
          f"(must be 0).", ""]
    Path(args.stats).parent.mkdir(parents=True, exist_ok=True)
    Path(args.stats).write_text("\n".join(L))


async def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--chunks", default="work/v3/chunks.jsonl")
    ap.add_argument("--url", default=URL_FILE.read_text().strip() if URL_FILE.exists() else None,
                    help=f"OpenAI-compatible server (default: the address in {URL_FILE})")
    ap.add_argument("--model", default="DeepSeek-V4.1-Flash-1017-oQ4e-mtp")
    ap.add_argument("--api-key", default=os.environ.get("OMLX_API_KEY", "local"))
    ap.add_argument("--temperature", type=float, default=0.7)
    ap.add_argument("--max-tokens", type=int, default=1024)
    ap.add_argument("--extra-body", type=json.loads, default=THINKING_OFF,
                    help="JSON merged into every request (default: thinking off for DeepSeek)")
    ap.add_argument("--concurrency", type=int, default=4)
    ap.add_argument("--target", type=int, default=3800, help="seed-origin bank questions kept after all filters "
                    "(the quota; the reserve and the ooc questions come on top)")
    ap.add_argument("--reserve", type=int, default=600,
                    help="up to N questions the quota trimmed off go to --reserve-out (spread over categories)")
    ap.add_argument("--ooc", type=int, default=250,
                    help="out-of-corpus questions (one call per topic of OOC_TOPICS), 0 = off")
    ap.add_argument("--withheld", type=int, default=400,
                    help="bank questions marked treatment=withheld (train split, troubleshooting/how-to/config-review)")
    ap.add_argument("--over", type=float, default=1.6,
                    help="over-generation: seeds are sampled for this many times each category quota")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--eval-glob", default="eval/questions*.jsonl", help="leakage filter: every matching file")
    ap.add_argument("--style-file", default="eval/questions.jsonl",
                    help="5 questions of this file are shown to the model as style examples")
    ap.add_argument("--raw", default="work/v3/questions_raw.jsonl", help="generation results, one line per seed")
    ap.add_argument("--out", default="work/v3/questions.jsonl")
    ap.add_argument("--stats", default="runs/v3/bank_stats.md")
    ap.add_argument("--eval-slice-out", default="work/v3/questions_evalslice.jsonl")
    ap.add_argument("--reserve-out", default="work/v3/questions_reserve.jsonl")
    ap.add_argument("--limit", type=int, help="use only the first N seeds (0 = no generation, redo the filters)")
    ap.add_argument("--dry-run", action="store_true", help="fake LLM, no network: tests the whole pipeline")
    args = ap.parse_args()

    chunks, _, idf, *_ = index(args.chunks)
    plan, how = select_seeds(chunks, args.target, args.over, args.seed)
    seeds = plan if args.limit is None else plan[:args.limit]
    Path(args.raw).parent.mkdir(parents=True, exist_ok=True)
    raw0 = read_raw(args.raw, args.dry_run)
    done = {r["chunk_id"] for r in raw0 if "chunk_id" in r and not r.get("parse_fail")}
    todo = [c for c in seeds if c["id"] not in done]
    n_ooc = ooc_n(args)
    ooc_done = {r["ooc_topic"] for r in raw0 if r.get("n") == n_ooc and not r.get("parse_fail")}
    ooc_todo = [t for t, _ in OOC_TOPICS if n_ooc and args.limit != 0 and t not in ooc_done]
    print(f"{len(plan)} seeds planned ({sum(v == 'rule' for v in how.values())} by rule, "
          f"{sum(v == 'sample' for v in how.values())} sampled), {len(seeds)} in this run: "
          f"{len(seeds) - len(todo)} already done, {len(todo)} to process")
    print("plan per expected category (seeds, expected raw questions against quota):")
    print("\n".join(plan_lines(plan, args.target, args.over)))
    mentions = topic_mentions(chunks)
    if args.ooc:
        print(f"out-of-corpus: {len(OOC_TOPICS)} topics, {n_ooc} questions per call, {len(ooc_todo)} calls to make; "
              f"index chunks mentioning the topic's key terms:")
        print("\n".join(f"  {mentions[t]:5d}  {t}" for t, _ in OOC_TOPICS))

    stats = {"done": 0, "items": 0, "failed": 0, "ooc_done": 0, "ooc_items": 0, "_t0": time.monotonic()}
    if todo or ooc_todo:
        if args.dry_run:
            llm = FakeLLM()
        elif not args.url:
            sys.exit(f"no --url and no {URL_FILE}")
        else:
            url = args.url.rstrip("/")
            usage = {"calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "reasoning_tokens": 0}
            llm = LLM(url if url.endswith("/v1") else url + "/v1", args.model, args.api_key, args.temperature,
                      args.max_tokens, args.extra_body, usage)
        system = system_prompt(args.style_file, args.seed)
        ooc_system = system_prompt(args.style_file, args.seed, OOC_SYSTEM)
        sem, lock = asyncio.Semaphore(args.concurrency), asyncio.Lock()
        with open(args.raw, "a") as f:
            await asyncio.gather(*(process(c, llm, system, args, sem, f, lock, stats) for c in todo),
                                 *(process_ooc(t, n_ooc, llm, ooc_system, args, sem, f, lock, stats)
                                   for t in ooc_todo))
        print(f"generated: {stats['done']} seeds, {stats['items']} questions, {stats['ooc_done']} ooc topics, "
              f"{stats['ooc_items']} ooc questions, {stats['failed']} failed (rerun to retry them), "
              f"{llm.usage['calls']} calls")

    s = build(args, chunks, idf)
    write_stats(args, chunks, plan, seeds, how, s, mentions)
    drop = Counter(d["reason"] for d in s["drops"])
    print(f"kept {len(s['bank'])} of target {args.target} + {len(s['ooc'])} ooc -> {args.out}; eval slice "
          f"{len(s['slice'])} -> {args.eval_slice_out}; reserve {len(s['reserve'])} -> {args.reserve_out}; "
          f"dropped {dict(drop)}")
    allr = s["out"] + s["slice"] + s["reserve"]
    print("split: " + ", ".join(f"{sp} {sum(x['split'] == sp for x in allr)}" for sp in ("train", "valid", "evalslice"))
          + " | treatment: " + ", ".join(f"{t} {sum(x['treatment'] == t for x in allr)}"
                                          for t in ("normal", "withheld", "ooc"))
          + " | origin: " + ", ".join(f"{o} {sum(x['origin'] == o for x in allr)}" for o in ("seed", "ooc")))
    for cat, (quota, have, kept) in s["per_cat"].items():
        print(f"  {cat}: {kept}/{quota}" + (f" SHORT (available {have})" if kept < quota else ""))
    print(f"stats: {args.stats}")
    if stats["failed"]:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
