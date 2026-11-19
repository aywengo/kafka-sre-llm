#!/usr/bin/env python3
"""Computed-sizing items of the a3 slice of tier A (WP1c T6): a value with a unit, checked by `number_within`.

Eight templates, three parameter sets each, from formulas that a pinned file states (file, line, quoted phrase; the
phrase is looked up in the cited line at build time, a formula whose passage is not there is a failed derivation).
Two classes, reported separately because the first measures arithmetic and not retrieval:
  arith_only      the question states the formula; the pinned passage anchors the quantity it rests on
  rule_from_docs  the question gives the quantities only; the rule is the pinned passage

Truth, no model (D6): the expected value is computed exactly (fractions); the tolerance is per item (0 for integer
conversions, 1 % otherwise, 3 % for the page-cache rule of thumb, below the 4.9 % of MB against MiB and the 2.4 % of KB against KiB). Every item carries
perturbed answers: unit slips (MB against MiB, ms against s, bytes against KiB, ...) and off-by-a-factor errors
(forgotten replication factor, forgotten division, ...). The reference must pass and each perturbed answer must
fail `number_within`; a template or parameter set where one of these does not hold is counted and skipped.

Candidates with no pinned passage for their formula (consumer catch-up time from lag and rates; partitions needed
for a target throughput) are counted and skipped, not filled in by hand.

Version 3 (fix round 2, N1): `fresh` parameter sets for six templates, ids from 201; the existing items are unchanged.
Deterministic: one random stream per template (seed|catalogue hash|template), only for the phrasing; no set iteration.
`generate()` returns the items and writes no file. Run: .venv/bin/python scripts/eval2_items_a3_sizing.py
"""
import math
import random
import sys
from collections import Counter
from fractions import Fraction
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import eval2_items_a3_order as order  # noqa: E402  (anchor check, near-duplicate scan)
import eval2_score as sc  # noqa: E402
from eval2_checks_a3 import check_number_within  # noqa: E402
from kafka_sre_rag.catalogue import Catalogues, catalogue_hash, default_dir  # noqa: E402

VERSION = "eval2_items_a3_sizing/3"
# Version 2 (review finding M2): a rule-of-thumb item whose reference is a rounded estimate takes a 3 % tolerance (a
# rounded "12 GiB" for 11.72 is 2.4 % off; the nearest perturbation is 6.9 % off). Its items get new numbers (101...).
V2_ID_BASE = {"size_page_cache": 100}
# Version 3 (round 2, N1): further parameter sets for the hidden side (`fresh`): never in an open file, numbers from 201.
FRESH_ID_BASE = 200
DEFAULT_SEED = "m4-a3-1"
KIND = "computed_sizing"
F = Fraction

CFG_STREAMS = "raw/apache-kafka/docs/streams/developer-guide/config-streams.md"
TOPIC_CFG = "raw/apache-kafka-config/content/en/43/generated/topic_config.html"
CONSUMER_CFG = "raw/apache-kafka-config/content/en/43/generated/consumer_config.html"
STREAMS_CFG = "raw/apache-kafka-config/content/en/43/generated/streams_config.html"
HW = "raw/apache-kafka/docs/operations/hardware-and-os.md"
DESIGN = "raw/apache-kafka/docs/design/design.md"
K8S_RES = "raw/kubernetes/content/en/docs/concepts/configuration/manage-resources-containers.md"


def fmt(v):
    """A number as an answer: integers as they are, else four significant digits."""
    v = F(v)
    if v.denominator == 1:
        return str(v.numerator)
    x = float(v)
    s = repr(round(x, max(0, 3 - int(math.floor(math.log10(abs(x)))))))
    return s[:-2] if s.endswith(".0") else s


def ans(v, unit):
    return "%s %s" % (fmt(v), unit)


# ------------------------------------------------------------------ templates
# Each template: id, klass, anchor (file, line, phrase), unit, tol, params, and a function giving, per parameter set,
# (question core, expected value, slips); a slip is (label, kind, value, unit) with kind "unit" or "factor".
def t_disk(p):
    r, d, rf, b = p
    v = F(r) * d * 86400 * rf / b / 1024
    q = ("A topic takes %s MiB/s of producer traffic and keeps its data for %d days. Its replication factor is %d and its "
         "partitions are spread evenly over %d brokers. How much disk does one broker use for this topic's retained data, "
         "in GiB? Use: disk per broker = write rate x retention x replication factor / number of brokers; ignore "
         "indexes and any overhead." % (r, d, rf, b))
    return q, v, [("GB against GiB", "unit", v, "GB"), ("MiB against GiB", "unit", v, "MiB"),
                  ("forgot the replication factor", "factor", v / rf, "GiB"),
                  ("forgot to divide by the brokers", "factor", v * b, "GiB")]


def t_pagecache(p):
    w, = p
    v = F(w) * 30 / 1024
    q = ("A Kafka cluster takes %d MiB/s of writes. Using the back-of-the-envelope rule of the Kafka documentation for "
         "the memory needed to buffer active readers and writers, how much memory should the cluster have for that "
         "buffer, in GiB?" % w)
    return q, v, [("GB against GiB", "unit", v, "GB"), ("MiB against GiB", "unit", v, "MiB"),
                  ("60 seconds instead of 30", "factor", v * 2, "GiB"), ("forgot the buffering time", "factor", v / 30, "GiB")]


def t_heartbeat(p):
    s, = p
    v = F(s, 3)
    q = ("A classic-protocol consumer group uses `session.timeout.ms` = %d. What is the highest `heartbeat.interval.ms` "
         "the Kafka documentation says you should typically set, in milliseconds?" % s)
    return q, v, [("ms against s", "unit", v, "s"), ("seconds written as ms", "unit", v / 1000, "ms"),
                  ("session timeout itself", "factor", v * 3, "ms"), ("two thirds instead of one third", "factor", v * 2, "ms")]


def t_retention(p):
    n, unit, ms_per, wrong = p
    v = F(n) * ms_per
    q = ("A topic must keep its records for %d %s. What value of `retention.ms` is that, in milliseconds? "
         "(1 day = 24 hours, 1 hour = 60 minutes, 1 minute = 60 seconds, 1 second = 1000 ms.)" % (n, unit))
    return q, v, [("ms against s", "unit", v, "s"), ("seconds written as ms", "unit", v / 1000, "ms"),
                  (wrong[0], "factor", v * F(wrong[1]), "ms")]


def t_quantity(p):
    q_, b = p
    v = F(b)
    suffix = q_.lstrip("0123456789")
    n = F(q_[:len(q_) - len(suffix)])
    twin = {"Mi": ("M", 10 ** 6), "Gi": ("G", 10 ** 9), "M": ("Mi", 2 ** 20), "G": ("Gi", 2 ** 30)}[suffix]
    q = ("A container's memory limit in a Kubernetes manifest is `%s`. How many bytes is that? Give an integer number "
         "of bytes." % q_)
    return q, v, [("%s read as %s" % (suffix, twin[0]), "unit", n * twin[1], "bytes"),
                  ("bytes against KiB", "unit", v, "KiB"), ("KiB count written as bytes", "unit", v / 1024, "bytes"),
                  ("a factor of 1000", "factor", v * 1000, "bytes")]


def t_follower(p):
    i, rf = p
    v = F(i) * (rf - 1)
    q = ("Producers write %d MiB/s to a topic with replication factor %d. Each follower fetches the whole stream from "
         "the partition leaders. How much replication traffic do the followers pull in total, in MiB/s? Use: follower "
         "traffic = ingress x (replication factor - 1)." % (i, rf))
    return q, v, [("MB/s against MiB/s", "unit", v, "MB/s"), ("MiB/min against MiB/s", "unit", v, "MiB/min"),
                  ("counted the leader copy too", "factor", F(i) * rf, "MiB/s"),
                  ("one follower only", "factor", F(i), "MiB/s")]


def t_topic_retention(p):
    g, parts = p
    v = F(g) * parts
    q = ("A topic has `retention.bytes` = %d GiB and %d partitions. What is the topic's retention size in GiB, that is, "
         "how much data can the topic keep before old segments are deleted? Count one copy of each partition." % (g, parts))
    return q, v, [("GB against GiB", "unit", v, "GB"), ("MiB against GiB", "unit", v, "MiB"),
                  ("forgot the partitions", "factor", F(g), "GiB"), ("added instead of multiplied", "factor", F(g + parts), "GiB")]


def t_cache(p):
    total, threads = p
    v = F(total, threads)
    q = ("A Kafka Streams application sets `statestore.cache.max.bytes` to %d MiB and runs with `num.stream.threads` = %d. "
         "On average, how much of that cache does each stream thread get, in MiB?" % (total, threads))
    return q, v, [("MB against MiB", "unit", v, "MB"), ("KiB against MiB", "unit", v, "KiB"),
                  ("forgot to divide by the threads", "factor", F(total), "MiB"),
                  ("multiplied by the threads", "factor", v * threads, "MiB")]


TEMPLATES = [
    {"id": "size_disk_rf", "klass": "arith_only", "domain": "kafka", "unit": "GiB", "tol": 0.01, "fn": t_disk,
     "anchor": (CFG_STREAMS, 1636, "more storage space as well (3x with the replication factor of 3)"),
     "params": [(5, 3, 3, 6), (12, 2, 3, 4), (2, 7, 2, 3)], "fresh": [(8, 4, 3, 5)]},
    {"id": "size_page_cache", "klass": "rule_from_docs", "domain": "kafka", "unit": "GiB", "tol": 0.03, "fn": t_pagecache,
     "anchor": (HW, 31, "assuming you want to be able to buffer for 30 seconds and compute your memory need as write_throughput*30"),
     "params": [(120,), (400,), (75,)]},
    {"id": "size_heartbeat", "klass": "rule_from_docs", "domain": "kafka", "unit": "ms", "tol": 0, "fn": t_heartbeat,
     "anchor": (CONSUMER_CFG, 64, "typically should be set no higher than 1/3 of that value"),
     "params": [(45000,), (18000,), (90000,)], "fresh": [(27000,), (120000,)]},
    {"id": "size_retention_ms", "klass": "arith_only", "domain": "kafka", "unit": "ms", "tol": 0, "fn": t_retention,
     "anchor": (TOPIC_CFG, 315, "604800000 (7 days)"),
     "params": [(3, "days", 86400000, ("hours as days", F(1, 24))), (36, "hours", 3600000, ("minutes as hours", F(1, 60))),
                (14, "days", 86400000, ("hours as days", F(1, 24)))],
     "fresh": [(21, "days", 86400000, ("hours as days", F(1, 24)))]},
    {"id": "size_k8s_quantity", "klass": "rule_from_docs", "domain": "kubernetes", "unit": "bytes", "tol": 0, "fn": t_quantity,
     "anchor": (K8S_RES, 184, "power-of-two equivalents: Ei, Pi, Ti, Gi"),
     "params": [("512Mi", 512 * 2 ** 20), ("2Gi", 2 * 2 ** 30), ("750M", 750 * 10 ** 6)],
     "fresh": [("4Gi", 4 * 2 ** 30)]},
    {"id": "size_follower_traffic", "klass": "arith_only", "domain": "kafka", "unit": "MiB/s", "tol": 0.01, "fn": t_follower,
     "anchor": (DESIGN, 287, "Followers consume messages from the leader just as a normal Kafka consumer would"),
     "params": [(20, 3), (45, 4), (8, 5)]},
    {"id": "size_topic_retention", "klass": "rule_from_docs", "domain": "kafka", "unit": "GiB", "tol": 0.01, "fn": t_topic_retention,
     "anchor": (TOPIC_CFG, 301, "multiply it by the number of partitions to compute the topic retention in bytes"),
     "params": [(10, 12), (50, 6), (4, 24)]},
    {"id": "size_streams_cache", "klass": "rule_from_docs", "domain": "kafka", "unit": "MiB", "tol": 0.01, "fn": t_cache,
     "anchor": (STREAMS_CFG, 294, "statestore cache across all threads"),
     "params": [(64, 8), (10, 2), (200, 4)], "fresh": [(96, 6)]},
]

# no pinned passage states these formulas: counted and skipped
UNANCHORED = [
    ("size_catch_up", "consumer catch-up time = lag / (consume rate - produce rate)",
     "no pinned passage states the formula (the lag column in basic-kafka-operations.md only names the columns)"),
    ("size_partitions", "partitions needed = target throughput / per-partition throughput",
     "no pinned passage of apache-kafka/docs states a partition-count formula"),
]

PHRASINGS = ["{q}", "Quick sizing question. {q}", "For capacity planning: {q}"]
FORMAT_NOTE = "Finish with a final line `ANSWER: <number> <unit>`."


def build_item(tpl, params, n, cats, chash, seed, rng, corpus, stats, fresh=False):
    ok, why = order.verify_anchor(*tpl["anchor"])
    if not ok:
        stats["derivation_failed:%s: anchor: %s" % (tpl["id"], why)] += 1
        return None
    q, v, slips = tpl["fn"](params)
    unit = tpl["unit"]
    core = PHRASINGS[rng.randrange(3)].format(q=q)
    hit, dd = order.near_duplicate_of(core, corpus)
    if hit:
        stats["dropped:near_duplicate:%s" % tpl["id"]] += 1
        return None
    chk = {"type": "number_within", "value": float(v) if v.denominator != 1 else int(v), "unit": unit, "tolerance": tpl["tol"]}
    perts = [{"label": lab, "kind": kind, "text": "Worked out with the stated rule.\nANSWER: %s" % ans(val, u),
              "violates": 1, "check": "number_within"} for lab, kind, val, u in slips]
    item = {
        "id": "a3-%s-%03d" % (tpl["id"], n + (FRESH_ID_BASE if fresh else V2_ID_BASE.get(tpl["id"], 0))), "tier": "A", "kind": KIND, "family": "%s:%s" % (tpl["id"], "-".join(str(x) for x in params)),
        "domain": tpl["domain"], "question": core + "\n\n" + FORMAT_NOTE,
        "checks": [{"type": "answer_line", "format": True}, chk],
        "reference": "Worked out with the stated rule.\nANSWER: %s" % ans(v, unit),
        "sources": [{"catalogue": "pinned_text", "entry": tpl["id"], "fact": tpl["anchor"][2], "file": tpl["anchor"][0],
                     "line": tpl["anchor"][1]}],
        "versions": {"kubernetes": "v1.37.2"} if tpl["domain"] == "kubernetes" else {"kafka": "4.3"},
        "template": tpl["id"], "anchored": True, "failure_class": tpl["klass"], "exposed": False,
        "catalogue_hash": chash, "seed": seed, "generator": VERSION, "dedup": dd, "chance": 0.0,
        "perturbed_reference": perts[0]["text"], "perturbed_violates": 1, "perturbed": perts,
    }
    # the reference passes; every perturbed answer fails number_within
    bad = [r["type"] for r in sc.run_checks(item, item["reference"], cats) if not r["pass"]]
    if bad:
        stats["derivation_failed:%s: the reference fails %s" % (tpl["id"], ",".join(bad))] += 1
        return None
    for p in perts:
        if check_number_within(chk, p["text"], cats)[0]:
            stats["derivation_failed:%s %s: the perturbed answer %r passes" % (tpl["id"], list(params), p["label"])] += 1
            return None
    kinds = {p["kind"] for p in perts}
    if kinds != {"unit", "factor"}:
        stats["derivation_failed:%s: lacks a unit or a factor slip" % tpl["id"]] += 1
        return None
    return item


def generate_with_stats(cats=None, seed=DEFAULT_SEED, corpus=None):
    cats = cats or Catalogues.load()
    chash = catalogue_hash(default_dir())
    corpus = order.load_corpus() if corpus is None else corpus
    stats = Counter()
    for tid, label, why in UNANCHORED:
        stats["derivation_failed:%s: %s" % (tid, why)] += 1
    items = []
    for tpl in TEMPLATES:
        rng = random.Random("%s|%s|%s" % (seed, chash, tpl["id"]))
        n = 0
        for params in tpl["params"]:
            n += 1
            item = build_item(tpl, params, n, cats, chash, seed, rng, corpus, stats)
            if item is None:
                n -= 1
                continue
            items.append(item)
        n = 0
        for params in tpl.get("fresh", []):
            n += 1
            item = build_item(tpl, params, n, cats, chash, seed, rng, corpus, stats, fresh=True)
            if item is None:
                n -= 1
                continue
            items.append(item)
    items.sort(key=lambda i: i["id"])
    stats["items"] = len(items)
    return items, stats


def generate(cats=None, seed=DEFAULT_SEED):
    return generate_with_stats(cats, seed)[0]


def main():
    items, stats = generate_with_stats()
    print("%s: %d items made (plan: 24)" % (VERSION, len(items)))
    for k in sorted(stats):
        if k != "items":
            print("  %s x%d" % (k, stats[k]))
    best = order.max_similarity(items, None)
    print("highest similarity to an eval, training or open question: jaccard %.3f, containment %.3f (%s, item %s)" % best)
    cls = Counter(i["failure_class"] for i in items)
    print("classes: %s; families: %d" % (dict(sorted(cls.items())), len({i["family"] for i in items})))


if __name__ == "__main__":
    main()
