#!/usr/bin/env python3
"""Generate the mechanical tier A items of the M4 evaluation (slices A1 and A2). No model is involved anywhere.

Truth comes from the catalogues (work/m4/catalogues, built by scripts/build_catalogues.py), never
from a model. Kinds: name_setting, is_real, connect_rest, argocd_command, error_name (A1) and
write_resource, fix_manifest, write_command, version_trap (A2, drafts built by scripts/eval2_kinds_a2.py,
checked by the new check types of scripts/eval2_score.py). The new kinds are at least NEW_KIND_SHARE of the set.

Exposed items (shown to people, 60 in all): 20 of the first version, 20 chosen by seed under the earlier
catalogues and 20 of the new kinds are embedded verbatim below (EXPOSED_V1/V2/V3_TEXT). At generation each is
re-checked against the current catalogues: its reference must still pass its checks, the perturbed reference
must still fail, and every fact it cites must still hold. A surviving item is written unchanged except for
catalogue_hash, which is set to the current hash (the catalogues it was last verified against). An item the
catalogues contradict is left out, a line is printed and it is listed in the table at the end; it is never
altered. `--dump-exposed FILE` is used once to choose the twenty rows of the new kinds before they are embedded.

Rebalanced mix (version 2 of the generator): plain lookups are the minority; the rest are near-miss
names (one controlled edit of a real key or error name, checked against every scope), wrong scope and
scope-dependent defaults, confusable neighbours (similar names, different meanings), value and type
traps (enumerated values with a plausible invalid one offered, special defaults, update modes),
Connect REST and Argo CD with method and multi-flag traps, and error names with more than two
outcomes. The 20 items shown to people in the first version are kept verbatim (EXPOSED_V1, below);
20 further items chosen by a fixed seed are marked exposed as well (40 in all). The table printed
at the end gives, for every kind and class, the score of a blind guess.

Output: eval/m4/build/tier_a1.jsonl, one JSON object per line, sorted by id:
  id, tier "A", kind, family, domain, question, checks, reference, sources, versions, template,
  anchored, plus: failure_class, exposed, overlap (word overlap of the question with the catalogue
  description it was made from), overlap_band, catalogue_hash, seed, generator, dedup,
  perturbed_reference and perturbed_violates (the reference with check number N deliberately
  violated; used by the tests, never sent to a model).

Construction rules (agents: evaluation_design_review.md, changes 7 to 10):
  - at most MAX_PER_TEMPLATE items per template; a family is the template plus the catalogue group
    (for config keys the scope, for broker keys the scope and the first word of the key);
  - a cap per failure class where the catalogues allow it (CLASS_CAPS) and hard classes are
    picked first;
  - every item has a reference that must pass all its checks, and a perturbed reference that must
    fail the violated check; a failing reference is repaired (next candidate form) or dropped;
    the counts per kind are printed;
  - near-duplicate matching against eval/*.jsonl, work/v3/questions*.jsonl and
    data/{train,valid,test}.jsonl (and so against the old real questions): normalised token sets,
    dropped when Jaccard >= JACCARD_T, or containment >= CONTAIN_T with both sets at least
    MIN_TOKENS long;
  - is_real is at most 10 % of the items (REAL_SHARE; 8 % now), three verdicts of about the same size; "not in my
    sources" comes from what is still incomplete (Kafka tools without an option parser, kubectl flags, Strimzi
    annotations and the labels of the operator pod), "does not exist" from complete lists;
  - deterministic for a seed and the catalogue hash; the hash is written into every item.

Usage: .venv/bin/python scripts/eval2_items_a1.py [--catalogues DIR] [--out FILE] [--seed S]
"""
import argparse
import difflib
import glob
import json
import random
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import eval2_kinds_a2 as k2  # noqa: E402
import eval2_score as sc  # noqa: E402
from kafka_sre_rag.catalogue import KNOWN, NOT_COVERED, UNKNOWN, Catalogues, catalogue_hash  # noqa: E402

VERSION = "eval2_items_a1/3"
DEFAULT_SEED = "m4-a1-1"
DEFAULT_OUT = ROOT / "eval" / "m4" / "build" / "tier_a1.jsonl"

MAX_PER_TEMPLATE = 10
FAMILY_CAP = 2  # items of one family inside one template
JACCARD_T = 0.5
CONTAIN_T = 0.8
MIN_TOKENS = 8
EXPOSED_N = 20  # the first version's exposed items, kept verbatim (EXPOSED_V1)
EXPOSED_NEW_N = 20  # the second twenty, chosen by seed under the earlier catalogues and kept verbatim (EXPOSED_V2)
EXPOSED_KINDS_N = 20  # twenty items of the new kinds, chosen by seed, kept verbatim (EXPOSED_V3)
NEW_KINDS = ("write_resource", "fix_manifest", "write_command", "version_trap")
NEW_KIND_SHARE = 0.30  # the new kinds together are at least this share of the set
EXTRA_KEYS = ("preserves_unknown", "fault", "resource_kind", "old_value", "validator_codes")  # draft keys kept in the item
MIX_MIN = {"near_miss_name": 0.15, "scope": 0.15, "confusable_neighbour": 0.10, "value_type_trap": 0.10}
MAX_PLAIN = 0.35
MAX_GUESS = 0.20
REAL_SHARE = 0.08  # is_real is at most this share of all items (the design allows 10 %)
OVERLAP_BANDS = ((0.5, "high"), (0.25, "mid"))  # q_in_desc >= 0.5 high, >= 0.25 mid, else low
CLASS_CAPS = {"near_miss_name": 90, "wrong_scope": 60, "scope_dependent": 40, "trap_method": 12,
              "not_covered": 14, "confusable_neighbour": 64, "value_type_trap": 72, "two_facts": 12,
              "multi_flag": 40, "negation": 100, "wrong_nesting": 30, "wrong_api_version": 12, "version": 24}
HARD_FIRST = ("scope_dependent", "wrong_scope", "confusable_neighbour", "near_miss_name", "value_type_trap",
              "multi_flag", "two_facts", "trap_method", "not_covered")
ANSWER_NOTE = "Finish with a final line `ANSWER: %s`."

SCOPE_H = {
    "broker": "broker", "topic": "topic-level", "producer": "producer", "consumer": "consumer",
    "admin": "admin client", "group": "group coordinator", "streams": "Kafka Streams",
    "connect": "Connect worker", "sink_connector": "Connect sink connector",
    "source_connector": "Connect source connector",
    "mirror_checkpoint": "MirrorMaker 2 checkpoint connector", "mirror_connector": "MirrorMaker 2 connector",
    "mirror_heartbeat": "MirrorMaker 2 heartbeat connector", "mirror_source": "MirrorMaker 2 source connector",
    "remote_log_manager": "remote log manager", "remote_log_metadata_manager": "remote log metadata manager",
}
SCOPE_DOMAIN = {"connect": "connect", "sink_connector": "connect", "source_connector": "connect",
                "streams": "streams", "mirror_checkpoint": "mirrormaker", "mirror_connector": "mirrormaker",
                "mirror_heartbeat": "mirrormaker", "mirror_source": "mirrormaker"}
SCOPE_OPTIONS = ["broker", "topic", "producer", "consumer", "admin", "group", "streams", "connect",
                 "sink_connector", "source_connector"]
SCOPE_NAMES = {"broker": ["broker"], "topic": ["topic"], "producer": ["producer"], "consumer": ["consumer"],
               "admin": ["admin", "admin client"], "group": ["group", "group coordinator"],
               "streams": ["streams", "kafka streams"], "connect": ["connect", "connect worker"],
               "sink_connector": ["sink connector", "sink_connector", "connect sink connector"],
               "source_connector": ["source connector", "source_connector", "connect source connector"]}
SCOPE_OPTION_TEXT = ("broker, topic, producer, consumer, admin, group, streams, connect, sink connector, "
                     "source connector")
SCOPE_WORDS = {"consumer", "producer", "connect", "streams", "topic", "broker", "group", "admin"}
TYPE_LIST = "boolean, class, double, int, list, long, password, short, string"
TYPE_SYNONYMS = {"int": ["int", "integer"], "boolean": ["boolean", "bool"]}
SWAPS = [("interval", "period"), ("timeout", "deadline"), ("records", "messages"), ("size", "length"),
         ("enable", "enabled"), ("max", "maximum"), ("min", "minimum"), ("retention", "retain"),
         ("threads", "workers"), ("partitions", "partition"), ("fetch", "pull"), ("poll", "fetch"),
         ("session", "connection"), ("buffer", "cache"), ("num", "count"), ("wait", "delay"),
         ("replicas", "copies"), ("backoff", "delay"), ("bytes", "size"), ("threshold", "limit")]
UNIT_SWAPS = [(".ms", ".seconds"), (".bytes", ".kb"), (".ms", ".sec"), (".seconds", ".ms"),
              (".hours", ".days"), (".minutes", ".ms"), (".bytes", ".mb")]
APPS = ["orders-api", "payments", "inventory-svc", "web-frontend", "billing", "search-indexer", "checkout",
        "notifications"]
STOP = set("""a an the of to in on for and or is are be it its this that which what with as by at from
you your i we do does did how can should would will not no if then than so there their they them has have
had was were been being into over under per via also any all each other such these those when where while
whose who whom about between""".split())


# ------------------------------------------------------------------ text helpers
def tokens(text):
    return {w for w in re.findall(r"[a-z0-9]+", (text or "").lower()) if w not in STOP and len(w) > 1}


def overlap(core, desc):
    q, d = tokens(core), tokens(desc)
    if not desc or not q or not d:
        return None
    both = len(q & d)
    return {"q_in_desc": round(both / len(q), 3), "desc_in_q": round(both / len(d), 3)}


def band(ov):
    if ov is None:
        return "none"
    for t, name in OVERLAP_BANDS:
        if ov["q_in_desc"] >= t:
            return name
    return "low"


def excerpt(desc, lo=60, hi=300):
    """The first sentences of a description, between lo and hi characters, or None."""
    sents = re.split(r"(?<=[.!?])\s+", desc.strip())
    out = ""
    for s in sents:
        if len(out) + len(s) + 1 > hi and out:
            break
        out = (out + " " + s).strip()
        if len(out) >= lo:
            break
    return out if lo <= len(out) <= hi else None


def find_line(rel_file, pattern):
    """Line number (1-based) of the first line of a file on disk that matches the regex, else None."""
    try:
        for i, ln in enumerate((ROOT / rel_file).read_text(errors="replace").split("\n"), 1):
            if re.search(pattern, ln):
                return i
    except OSError:
        return None
    return None


def ans(expl, value):
    return "%s\nANSWER: %s" % (expl, value)


# ------------------------------------------------------------------ near-duplicate matching
def corpus_texts():
    files = sorted(glob.glob(str(ROOT / "eval" / "*.jsonl")) + glob.glob(str(ROOT / "work" / "v3" / "questions*.jsonl"))
                   + [str(ROOT / "data" / n) for n in ("train.jsonl", "valid.jsonl", "test.jsonl")
                      if (ROOT / "data" / n).is_file()])
    seen, out = set(), []
    for f in files:
        rel = str(Path(f).resolve().relative_to(ROOT))
        for line in Path(f).read_text(errors="replace").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except ValueError:
                continue
            texts = []
            if isinstance(row, dict):
                if isinstance(row.get("question"), str):
                    texts.append(row["question"])
                for m in row.get("messages") or []:
                    if isinstance(m, dict) and m.get("role") == "user" and isinstance(m.get("content"), str):
                        texts.append(m["content"])
            for t in texts:
                if t not in seen:
                    seen.add(t)
                    out.append((rel, tokens(t)))
    return out, files


def near_duplicate(core, corpus):
    a = tokens(core)
    best = (0.0, 0.0, None)
    for rel, b in corpus:
        if not b:
            continue
        inter = len(a & b)
        if not inter:
            continue
        j = inter / len(a | b)
        c = inter / min(len(a), len(b)) if min(len(a), len(b)) >= MIN_TOKENS else 0.0
        if max(j, c) > max(best[0], best[1]):
            best = (j, c, rel)
    hit = best[0] >= JACCARD_T or best[1] >= CONTAIN_T
    return hit, {"max_jaccard": round(best[0], 3), "max_containment": round(best[1], 3), "nearest": best[2]}


# ------------------------------------------------------------------ builder
class Gen:
    def __init__(self, cats, seed, chash):
        self.cats = cats
        self.chash = chash
        self.seed = seed
        self.rng = random.Random("%s|%s" % (seed, chash))
        self.corpus, self.corpus_files = corpus_texts()
        self.items = []
        self.class_counts = Counter()
        self.template_counts = Counter()
        self.family_counts = Counter()
        self.stats = defaultdict(Counter)  # kind -> built, kept, dropped:<why>, repaired
        self.drop_reasons = defaultdict(Counter)
        self.cores = set()
        self.drop_examples = []
        self.id_no = Counter()
        self.reserved = set()
        self.chances = {}
        self.baseline_runs, self.baseline_pass = Counter(), Counter()
        self.exposed_dropped = []  # (id, reason) of exposed items the current catalogues contradict
        allent = cats.data["kafka_config"]["entries"]
        self.all_keys = {e["key"] for e in allent}
        self.vocab = key_vocab(cats)
        self.first_segs = sorted({e["key"].split(".")[0] for e in allent})
        self.by_scope = defaultdict(list)
        self.by_key = defaultdict(list)
        self.by_key_all = defaultdict(list)
        self.cfg_by = {(e["scope"], e["key"]): e for e in allent}
        for e in allent:
            self.by_scope[e["scope"]].append(e)
            self.by_key_all[e["key"]].append(e)
            if e["scope"] in SCOPE_H:
                self.by_key[e["key"]].append(e)
        self.enum_values = sorted({v for e in allent for v in (e.get("valid_values_list") or []) if VALUE_RE.match(v)})
        self.err_names = {e["name"] for e in cats.data["protocol_errors"]["entries"]}
        self.tpl_guess = {}  # template -> (mode, chance): how a blind guess is scored, see item_guess
        self.pool_modal = {}  # template -> share of the commonest answer among all candidates
        self.load_exposed()

    def load_exposed(self):
        """The exposed items of every earlier version, verbatim. An item is kept only if its reference still
        passes its checks against the current catalogues and the facts it cites still hold; its catalogue_hash
        is then set to the current hash (the catalogues it was last verified against). An item the catalogues
        contradict is not altered: it is left out, a line says why, and the reason is kept in exposed_dropped.
        Kept items count against the template and family caps."""
        for row in EXPOSED_ALL_ROWS():
            self.reserved.add(row["id"])
            why = exposed_problems(self, row)
            if why:
                self.exposed_dropped.append((row["id"], why))
                print("exposed item %s excluded: %s" % (row["id"], why))
                continue
            row = dict(row, catalogue_hash=self.chash)
            self.items.append(row)
            self.cores.add(row["question"].rsplit("\n\n", 1)[0])
            self.class_counts[row["failure_class"]] += 1
            self.template_counts[row["template"]] += 1
            self.family_counts[row["family"]] += 1
            self.chances[row["id"]] = row.get("chance", LEGACY_CHANCE.get(row["template"], 0.0))
            if row["kind"] in NEW_KINDS:
                self.chances[row["id"]] = 1 / 3.0 if row["template"] == "vtrap_added_a" else 0.0

    # draft = dict(kind, template, group, domain, failure_class, core, fmt, checks, refs, perturbed,
    #              violates, sources, versions, anchored, desc)
    def verify(self, d):
        item = {"id": "tmp", "checks": d["checks"]}
        why = None
        ref = None
        for i, r in enumerate(d["refs"]):
            res = sc.run_checks(item, r, self.cats)
            if all(x["pass"] for x in res):
                ref = r
                if i > 0:
                    self.stats[d["kind"]]["repaired"] += 1
                break
        if ref is None:
            return None, "reference_fails_its_checks"
        pres = sc.run_checks(item, d["perturbed"], self.cats)
        if pres[d["violates"]]["pass"] or all(x["pass"] for x in pres):
            return None, "perturbed_reference_passes"
        if not pres[0]["pass"]:
            return None, "perturbed_reference_lost_its_format"
        return ref, None

    def keep(self, d, class_caps=True):
        """Try to add draft d. Returns True when kept."""
        kind, tpl, fam = d["kind"], d["template"], d["template"] + ":" + d["group"]
        st = self.stats[kind]
        st["built"] += 1
        fc = d["failure_class"]
        if self.template_counts[tpl] >= MAX_PER_TEMPLATE or self.family_counts[fam] >= d.get("fam_cap", FAMILY_CAP):
            st["skipped_cap"] += 1
            return False
        if class_caps and fc in CLASS_CAPS and self.class_counts[fc] >= CLASS_CAPS[fc]:
            st["skipped_cap"] += 1
            return False
        if d["core"] in self.cores:
            self.drop_reasons[kind]["duplicate_question"] += 1
            return False
        ref, why = self.verify(d)
        if why:
            self.drop_reasons[kind][why] += 1
            self.drop_examples.append((kind, d["template"], why, d["core"][:120]))
            return False
        hit, dd = near_duplicate(d["core"], self.corpus)
        if hit:
            self.drop_reasons[kind]["near_duplicate"] += 1
            return False
        ov = overlap(d["core"], d.get("desc"))
        n = self.id_no[tpl] + 1
        while "a1-%s-%03d" % (tpl, n) in self.reserved:
            n += 1
        self.id_no[tpl] = n
        item = {
            "id": "a1-%s-%03d" % (tpl, n), "tier": "A", "kind": kind, "family": fam, "domain": d["domain"],
            "question": d["core"] + "\n\n" + d["fmt"], "checks": d["checks"], "reference": ref,
            "sources": d["sources"], "versions": d["versions"], "template": tpl, "anchored": d["anchored"],
            "failure_class": fc, "exposed": False, "overlap": ov, "overlap_band": band(ov),
            "catalogue_hash": self.chash, "seed": self.seed, "generator": VERSION, "dedup": dd,
            "perturbed_reference": d["perturbed"], "perturbed_violates": d["violates"],
        }
        for key in EXTRA_KEYS:
            if key in d:
                item[key] = d[key]
        if d.get("baseline") is not None:  # a blind answer (a skeleton, the faulty manifest, the bare tool) must fail
            b = sc.run_checks(item, d["baseline"], self.cats)
            self.baseline_runs[kind] += 1
            if all(x["pass"] for x in b):
                self.baseline_pass[kind] += 1
                d["chance"] = 1.0
        self.items.append(item)
        self.chances[item["id"]] = d.get("chance", 0.0)
        self.cores.add(d["core"])
        self.class_counts[fc] += 1
        self.template_counts[tpl] += 1
        self.family_counts[fam] += 1
        st["kept"] += 1
        return True

    def fill(self, drafts, n, fam_cap=FAMILY_CAP, guess=("pool", 0.0), balance=None, hard_first=True):
        """Keep up to n drafts: shuffled, hard failure classes first, family cap, class caps.

        guess = (mode, chance) says how a blind guess is scored for this template: "pool" the share of the
        commonest answer among all candidates (open answers), "kept" the same among the kept items (answers
        balanced on purpose), "chance" the chance alone; chance is 1 / number of options or 2 ** -options.
        balance = a function of a draft: the first picks then take the buckets in turn.
        """
        drafts = list(drafts)
        if drafts and drafts[0]["kind"] in NEW_KINDS:  # the embedded exposed items of a template are part of its n
            n = max(0, n - sum(1 for i in self.items if i["template"] == drafts[0]["template"]))
        if drafts:
            tpl = drafts[0]["template"]
            self.tpl_guess[tpl] = guess
            for d in drafts:
                d.setdefault("chance", guess[1])
            share = Counter(canon(d["checks"]) for d in drafts)
            self.pool_modal[tpl] = max(self.pool_modal.get(tpl, 0.0), max(share.values()) / len(drafts))
        self.rng.shuffle(drafts)
        if hard_first:
            order = {c: i for i, c in enumerate(HARD_FIRST)}
            drafts.sort(key=lambda d: order.get(d["failure_class"], len(order)))
        if balance:
            buckets = defaultdict(list)
            for d in drafts:
                buckets[balance(d)].append(d)
            keys = sorted(buckets, key=str)
            drafts = [buckets[k][i] for i in range(max(len(v) for v in buckets.values())) for k in keys
                      if i < len(buckets[k])]
        got = 0
        for d in drafts:
            if got >= n:
                break
            d["fam_cap"] = fam_cap
            if self.keep(d):
                got += 1
        return got


# ------------------------------------------------------------------ name the setting
def cfg_entries(cats):
    return [e for e in cats.data["kafka_config"]["entries"] if e["scope"] in SCOPE_H]


def cfg_group(e):
    return e["scope"] + (":" + e["key"].split(".")[0] if e["scope"] == "broker" else "")


def cfg_src(e, fact):
    return {"catalogue": "kafka_config", "entry": "%s/%s" % (e["scope"], e["key"]), "file": e["source_file"],
            "line": e["source_line"], "fact": fact}


def cfg_versions():
    return {"kafka": "4.3"}


def kf(sh):
    """`Kafka Streams` stays, `producer` becomes `Kafka producer`."""
    return sh if sh.startswith("Kafka") else "Kafka " + sh


def cfg_domain(scope):
    return SCOPE_DOMAIN.get(scope, "kafka")


def mk(kind, template, group, domain, fc, core, fmt, checks, refs, perturbed, violates, sources, versions,
       anchored, desc=None):
    return {"kind": kind, "template": template, "group": group, "domain": domain, "failure_class": fc,
            "core": core, "fmt": fmt, "checks": checks, "refs": refs, "perturbed": perturbed,
            "violates": violates, "sources": sources, "versions": versions, "anchored": anchored, "desc": desc}


def drafts_key_from_desc(g, variant):
    ents = cfg_entries(g.cats)
    by_scope = defaultdict(list)
    for e in ents:
        by_scope[e["scope"]].append(e)
    out = []
    for e in ents:
        ex = excerpt(e["description"])
        if not ex or e["key"] in ex or e["key"].replace(".", " ") in ex:
            continue
        if sum(1 for o in by_scope[e["scope"]] if o["description"].startswith(ex)) != 1:
            continue
        sib = [o["key"] for o in by_scope[e["scope"]] if o["key"] != e["key"]]
        near = [k for k in sib if k.rsplit(".", 1)[0] == e["key"].rsplit(".", 1)[0] and len(k) > 0]
        fc = "confusable_neighbour" if len(near) >= 2 else "plain_lookup"
        sh = SCOPE_H[e["scope"]]
        core = {
            "a": "In the Kafka 4.3 %s configuration reference, which key is described as follows?\n\n> %s\n\n"
                 "Give the exact key name." % (sh, ex),
            "b": "I'm looking for a %s setting and all I remember is its description: \"%s\" What is the "
                 "exact name of the setting?" % (kf(sh), ex),
            "c": "Which %s config property matches this documentation text (Kafka 4.3)? %s" % (kf(sh), ex),
        }[variant]
        k = e["key"]
        out.append(mk("name_setting", "ns_key_desc_" + variant, cfg_group(e), cfg_domain(e["scope"]), fc, core,
                      ANSWER_NOTE % "<key>",
                      [{"type": "answer_line", "format": True},
                       {"type": "answer_equals", "accepted": [k], "norm": "exact"}],
                      [ans("The description is the one of `%s` in the %s configuration reference." % (k, sh), k)],
                      ans("The description is the one of `%s`." % (k + ".x"), k + ".x"), 1,
                      [cfg_src(e, "key %s has this description" % k)], cfg_versions(), True, e["description"]))
    return out


def default_forms(e):
    dv, df = e.get("default_value"), e.get("default")
    forms = [dv]
    if df and df != dv:
        forms.append(df)
        gl = re.fullmatch(r".*?\s+\((\d[^()]*)\)", df)
        if gl:
            forms.append(gl.group(1))
    return forms


def drafts_default(g, variant):
    ents = cfg_entries(g.cats)
    keyscopes = defaultdict(list)
    for e in ents:
        keyscopes[e["key"]].append(e)
    out = []
    for e in ents:
        dv = e.get("default_value")
        if dv in (None, "", "null", '""') or len(dv) > 30 or "<" in dv or "\n" in dv:
            continue
        if e.get("type") in ("class", "password", None):
            continue
        multi = {x["default_value"] for x in keyscopes[e["key"]]}
        fc = "scope_dependent" if len(multi) > 1 else "plain_lookup"
        sh = SCOPE_H[e["scope"]]
        k = e["key"]
        core = {
            "a": "What is the default value of the %s configuration `%s` in Kafka 4.3?" % (sh, k),
            "b": "With no override, which value does Kafka 4.3 use for `%s` in the %s configuration?" % (k, sh),
            "c": "According to the Kafka 4.3 %s configuration reference, what does `%s` default to?" % (sh, k),
        }[variant]
        wrong = dv + "0" if dv.isdigit() else dv + "x"
        forms = default_forms(e)
        out.append(mk("name_setting", "ns_default_" + variant, cfg_group(e), cfg_domain(e["scope"]), fc, core,
                      ANSWER_NOTE % "<default value>" + " Give the bare value.",
                      [{"type": "answer_line", "format": True},
                       {"type": "answer_equals", "accepted": forms, "norm": "ci"}],
                      [ans("The default of `%s` is %s." % (k, f), f) for f in forms],
                      ans("The default of `%s` is %s." % (k, wrong), wrong), 1,
                      [cfg_src(e, "default %s" % e["default"])], cfg_versions(), True, e["description"]))
    return out


def drafts_values(g, variant):
    out = []
    for e in cfg_entries(g.cats):
        vl = e.get("valid_values_list")
        if not vl or not 2 <= len(vl) <= 6 or any(len(v) > 25 for v in vl):
            continue
        k, sh = e["key"], SCOPE_H[e["scope"]]
        core = {
            "a": "Which values does the %s setting `%s` accept (Kafka 4.3 reference)? Give all of them, "
                 "separated by commas." % (kf(sh), k),
            "b": "List every allowed value of `%s` in the %s configuration of Kafka 4.3, separated by commas." % (k, sh),
        }[variant]
        out.append(mk("name_setting", "ns_values_" + variant, cfg_group(e), cfg_domain(e["scope"]), "plain_lookup",
                      core, ANSWER_NOTE % "<value 1>, <value 2>, ...",
                      [{"type": "answer_line", "format": True}, {"type": "answer_set", "accepted": vl}],
                      [ans("`%s` accepts: %s." % (k, ", ".join(vl)), ", ".join(vl)),
                       ans("`%s` accepts: %s." % (k, ",".join(vl)), ",".join(vl))],
                      ans("`%s` accepts: %s." % (k, ", ".join(vl[:-1])), ", ".join(vl[:-1])), 1,
                      [cfg_src(e, "valid values %s" % e["valid_values"])], cfg_versions(), True, e["description"]))
    return out


def drafts_type(g, variant):
    out = []
    for e in cfg_entries(g.cats):
        t = e.get("type")
        if t not in ("boolean", "class", "double", "int", "list", "long", "password", "short", "string"):
            continue
        k, sh = e["key"], SCOPE_H[e["scope"]]
        core = {
            "a": "What is the configuration type of `%s` in the Kafka 4.3 %s configuration reference?" % (k, sh),
            "b": "In the %s configuration reference of Kafka 4.3, `%s` is listed with which Type?" % (sh, k),
        }[variant]
        acc = TYPE_SYNONYMS.get(t, [t])
        wrong = "string" if t != "string" else "int"
        out.append(mk("name_setting", "ns_type_" + variant, cfg_group(e), cfg_domain(e["scope"]), "plain_lookup",
                      core, "Choose one of: %s. " % TYPE_LIST + ANSWER_NOTE % "<type>",
                      [{"type": "answer_line", "format": True},
                       {"type": "answer_equals", "accepted": acc, "norm": "ci"}],
                      [ans("`%s` has the type %s." % (k, a), a) for a in acc],
                      ans("`%s` has the type %s." % (k, wrong), wrong), 1,
                      [cfg_src(e, "type %s" % t)], cfg_versions(), True, e["description"]))
    return out


def drafts_scope(g, variant):
    ents = cfg_entries(g.cats)
    count = Counter(e["key"] for e in ents)
    allscopes = defaultdict(set)
    for e in ents:
        allscopes[e["key"]].add(e["scope"])
    out = []
    for e in ents:
        if count[e["key"]] != 1 or e["scope"] not in SCOPE_OPTIONS:
            continue
        k = e["key"]
        words = set(re.split(r"[._-]", k))
        misleading = [w for w in words if w in SCOPE_WORDS and w != e["scope"].split("_")[0]]
        fc = "wrong_scope" if misleading else "plain_lookup"
        core = {
            "a": "Which Kafka component's configuration reference lists `%s` (Kafka 4.3)?" % k,
            "b": "`%s` is a Kafka configuration key. In which configuration reference of Kafka 4.3 is it defined?" % k,
        }[variant]
        acc = SCOPE_NAMES[e["scope"]]
        wrong = "producer" if e["scope"] != "producer" else "consumer"
        out.append(mk("name_setting", "ns_scope_" + variant, cfg_group(e), cfg_domain(e["scope"]), fc, core,
                      "Choose exactly one of: %s. " % SCOPE_OPTION_TEXT + ANSWER_NOTE % "<component>",
                      [{"type": "answer_line", "format": True},
                       {"type": "answer_equals", "accepted": acc, "norm": "ci"}],
                      [ans("`%s` is defined in the %s configuration." % (k, a), a) for a in acc],
                      ans("`%s` is defined in the %s configuration." % (k, wrong), wrong), 1,
                      [cfg_src(e, "only scope: %s" % e["scope"])], cfg_versions(), True, e["description"]))
    return out


# ------------------------------------------------------------------ is this real
REAL_FMT = ("Answer with exactly one of: `exists`, `does not exist`, `not in my sources`. "
            "Finish with a final line `ANSWER: <one of the three>`.")
VERDICT_TEXT = {"exists": "exists", "does not exist": "does not exist", "not in my sources": "not in my sources"}


def real_draft(template, group, domain, fc, core, expected, sources, anchored, desc=None, anchored_versions=None):
    wrong = "does not exist" if expected == "exists" else "exists"
    return mk("is_real", template, group, domain, fc, core, REAL_FMT,
              [{"type": "answer_line", "format": True}, {"type": "answer_verdict", "expected": expected}],
              [ans("My answer, based on the sources: %s." % expected, expected)],
              ans("My answer, based on the sources: %s." % wrong, wrong), 1, sources,
              anchored_versions or {"kafka": "4.3"}, anchored, desc)


def drafts_real_key(g, want_exists, want_fake):
    ents = [e for e in cfg_entries(g.cats) if e["scope"] in SCOPE_OPTIONS]
    out_e, out_f = [], []
    for e in ents:
        sh, k = SCOPE_H[e["scope"]], e["key"]
        core = "Is `%s` a real configuration key of the Kafka 4.3 %s configuration?" % (k, sh)
        out_e.append(real_draft("real_key_plain", cfg_group(e), cfg_domain(e["scope"]), "plain_lookup", core, "exists",
                                [cfg_src(e, "key exists in scope %s" % e["scope"])], True, e["description"]))
        fakes = valid_fakes(g, e)
        for fake, tag, how in ([fakes[g.rng.randrange(len(fakes))]] if fakes else []):
            core = "Is `%s` a real configuration key of the Kafka 4.3 %s configuration?" % (fake, sh)
            out_f.append(real_draft("real_key_plain", cfg_group(e), cfg_domain(e["scope"]), "near_miss_name", core,
                                    "does not exist",
                                    [{"catalogue": "kafka_config", "absent": fake, "edit": how + " of " + k,
                                      "file": e["source_file"], "line": None,
                                      "fact": "absent from every scope of the complete reference"}], True, e["description"]))
    return out_e, out_f


def drafts_real_scope(g):
    ents = cfg_entries(g.cats)
    count = Counter(e["key"] for e in ents)
    out_e, out_w = [], []
    for e in ents:
        if count[e["key"]] != 1 or e["scope"] not in ("broker", "producer", "consumer", "admin", "streams", "topic"):
            continue
        k = e["key"]
        out_e.append(real_draft("real_key_scope", cfg_group(e), cfg_domain(e["scope"]), "plain_lookup",
                                "Is `%s` a key of the Kafka 4.3 %s configuration?" % (k, SCOPE_H[e["scope"]]),
                                "exists", [cfg_src(e, "key in scope %s" % e["scope"])], True, e["description"]))
        for ws in ("producer", "consumer", "broker", "admin"):
            if ws != e["scope"] and g.cats.config_key(k, ws).status == "unknown":
                out_w.append(real_draft("real_key_scope", cfg_group(e), cfg_domain(ws), "wrong_scope",
                                        "Is `%s` a key of the Kafka 4.3 %s configuration?" % (k, SCOPE_H[ws]),
                                        "does not exist",
                                        [dict(cfg_src(e, "key exists only in scope %s" % e["scope"]),
                                              absent_from=ws)], True, e["description"]))
                break
    return out_e, out_w


def drafts_real_error(g):
    names = g.cats.data["protocol_errors"]["entries"]
    out_e, out_f = [], []
    all_names = {e["name"] for e in names}
    for e in names:
        if e["name"] in ("NONE", "UNKNOWN_SERVER_ERROR") or not e["description"]:
            continue
        core = "Is `%s` an error name in the Kafka protocol error list (Kafka 4.3)?" % e["name"]
        src = {"catalogue": "protocol_errors", "entry": e["name"], "file": e["source_file"], "line": e["source_line"],
               "fact": "error code %d" % e["code"]}
        out_e.append(real_draft("real_error", e["name"].split("_")[0], "kafka", "plain_lookup", core, "exists",
                                [src], True, e["description"]))
        words = e["name"].split("_")
        for a, b in (("NOT", "NO"), ("TOO", "OVER"), ("INVALID", "BAD"), ("UNKNOWN", "MISSING"), ("OUT_OF", "BEYOND"),
                     ("TIMEOUT", "TIMED_OUT"), ("NOT_AVAILABLE", "UNAVAILABLE"), ("LARGE", "BIG"), ("EXCEPTION", "FAILURE"),
                     ("REPLICAS", "REPLICA"), ("PARTITION", "PARTITIONS"), ("EPOCH", "VERSION")):
            fake = e["name"].replace(a, b, 1) if a in e["name"] else None
            if fake and fake != e["name"] and fake not in all_names and g.cats.error_name(fake).status == "unknown":
                core = "Is `%s` an error name in the Kafka protocol error list (Kafka 4.3)?" % fake
                out_f.append(real_draft("real_error", e["name"].split("_")[0], "kafka", "near_miss_name", core,
                                        "does not exist",
                                        [{"catalogue": "protocol_errors", "absent": fake,
                                          "edit": "swapped word %s -> %s of %s" % (a, b, e["name"]),
                                          "file": e["source_file"], "line": None,
                                          "fact": "absent from the complete error list"}], True, e["description"]))
                break
    return out_e, out_f


def drafts_real_rest(g):
    c = g.cats.data["connect_rest"]["entries"]
    out_e, out_w = [], []
    for e in c:
        if e["method"] != "POST" and e["method"] != "PUT":
            continue
        tpl = e["path"]
        ex = re.sub(r"\{[^}]*\}", "orders-sink", tpl)
        core = "Does the Kafka Connect REST interface (Kafka 4.3) serve `%s %s`?" % (e["method"], ex)
        src = {"catalogue": "connect_rest", "entry": "%s %s" % (e["method"], tpl), "file": e["source_file"],
               "line": e["source_line"], "fact": "operation %s" % e["operation_id"]}
        out_e.append(real_draft("real_rest", "connect", "connect", "plain_lookup", core, "exists", [src], True,
                                e["summary"], {"kafka": "4.3.2"}))
        other = "PUT" if e["method"] == "POST" else "POST"
        if g.cats.rest_operation(other, ex).status == "unknown":
            core = "Does the Kafka Connect REST interface (Kafka 4.3) serve `%s %s`?" % (other, ex)
            out_w.append(real_draft("real_rest", "connect", "connect", "trap_method", core, "does not exist",
                                    [dict(src, absent="%s %s" % (other, tpl),
                                          fact="the path exists but only with %s" % e["method"])], True, e["summary"],
                                    {"kafka": "4.3.2"}))
    return out_e, out_w


def flag_words(f):
    return f[2:].split("-")


def flag_edits(f):
    """Controlled single edits of a real flag: [(fake, how)]."""
    w = flag_words(f)
    out = []
    for i, x in enumerate(w):
        for a, b in SWAPS:
            for p_, q in ((a, b), (b, a)):
                if x == p_:
                    out.append(("--" + "-".join(w[:i] + [q] + w[i + 1:]), "swapped word %s -> %s" % (p_, q)))
        if len(x) >= 4 and x.isalpha():
            alt = x[:-1] if x.endswith("s") and not x.endswith("ss") else x + "s"
            out.append(("--" + "-".join(w[:i] + [alt] + w[i + 1:]), "singular/plural %s -> %s" % (x, alt)))
    if len(w) >= 3:
        for i in range(len(w)):
            out.append(("--" + "-".join(w[:i] + w[i + 1:]), "dropped word %s" % w[i]))
    if len(w) == 2:
        out.append(("--" + w[1] + "-" + w[0], "swapped word order"))
    return out


def cli_vocab(g):
    """option -> tools of the complete Kafka tools that have it."""
    voc = defaultdict(set)
    for e in g.cats.data["kafka_cli"]["entries"]:
        if e.get("complete") is True:
            for o in e["options"]:
                if o.startswith("--"):
                    voc[o].add(e["tool"])
    return voc


def drafts_real_flag(g):
    """Flags of tools whose option list is complete: real ones, and made-up ones (an edit of a real flag, or a flag
    of another tool). The expected answers are `exists` and `does not exist`."""
    voc = cli_vocab(g)
    out_e, out_n, out_w = [], [], []
    for e in g.cats.data["kafka_cli"]["entries"]:
        if e.get("complete") is not True or len(e["options"]) < 5:
            continue
        tool = e["tool"]
        real = sorted(o for o in e["options"] if o.startswith("--") and o not in ("--help", "--version"))
        for opt in real:
            o = e["options"][opt]
            core = "Is `%s` an option of `%s` in Kafka 4.3?" % (opt, tool)
            out_e.append(real_draft("real_flag", tool, "kafka", "plain_lookup", core, "exists",
                                    [{"catalogue": "kafka_cli", "entry": "%s %s" % (tool, opt), "file": o["files"][0],
                                      "line": o.get("line"), "fact": "option of the complete parser"}], True,
                                    o.get("description")))
        edits = []
        for opt in real:
            for fake, how in flag_edits(opt):
                if g.cats.cli_option(tool, fake).status == UNKNOWN and fake not in voc:
                    edits.append((fake, how + " of " + opt))
        other = [f for f in sorted(voc) if tool not in voc[f] and g.cats.cli_option(tool, f).status == UNKNOWN
                 and f not in ("--help", "--version")]
        if edits:
            fake, how = edits[g.rng.randrange(len(edits))]
            out_n.append(real_draft("real_flag", tool, "kafka", "near_miss_name",
                                    "Is `%s` an option of `%s` in Kafka 4.3?" % (fake, tool), "does not exist",
                                    [{"catalogue": "kafka_cli", "entry": tool, "absent": fake, "edit": how,
                                      "file": e["source_file"], "line": None,
                                      "fact": "absent from the complete option list of the tool"}], True, None))
        if other:
            fake = other[g.rng.randrange(len(other))]
            out_w.append(real_draft("real_flag", tool, "kafka", "wrong_scope",
                                    "Is `%s` an option of `%s` in Kafka 4.3?" % (fake, tool), "does not exist",
                                    [{"catalogue": "kafka_cli", "entry": tool, "absent": fake,
                                      "edit": "a flag of " + ", ".join(sorted(voc[fake]))[:80],
                                      "file": e["source_file"], "line": None,
                                      "fact": "absent from the complete option list of the tool"}], True, None))
    return out_e, out_n + out_w


def drafts_real_flag_nc(g):
    """Names from tools whose catalogue is not complete (Kafka tools without an option parser, kubectl):
    the expected answer is `not in my sources`; a flag the catalogue holds is `exists`."""
    voc = cli_vocab(g)
    out_n, out_e = [], []
    kube = [t for t in g.cats.data["kubernetes"]["cli"] if t["options"]]
    kvoc = sorted({o for t in kube for o in t["options"] if o.startswith("--")})
    groups = [(e, "kafka_cli", "real_flag_nc", sorted(voc), "kafka", {"kafka": "4.3"})
              for e in g.cats.data["kafka_cli"]["entries"] if e.get("complete") is False]
    groups += [(t, "kubernetes", "real_kubectl_nc", kvoc, "kubernetes", {"kubernetes": "v1.37.2"}) for t in kube]
    for e, cat, tpl, vocab, domain, vers in groups:
        tool = e["tool"]
        free = [f for f in vocab if g.cats.cli_option(tool, f).status == NOT_COVERED and f not in e["options"]]
        g.rng.shuffle(free)
        why = e.get("reason") or "kubectl has no generated reference in the sources: only the flags seen in examples (%d)" % len(e["options"])
        for fl in free[:2 if cat == "kubernetes" else 3]:
            core = "Is `%s` an option of `%s`%s?" % (fl, tool, " in Kafka 4.3" if cat == "kafka_cli" else " (Kubernetes 1.37)")
            out_n.append(real_draft(tpl, tool, domain, "not_covered", core, "not in my sources",
                                    [{"catalogue": cat, "entry": tool, "absent": fl, "file": e["source_file"], "line": None,
                                      "fact": "the sources show only the options seen in examples (%d); the tool's full option "
                                              "list is not in them: %s" % (len(e["options"]), why[:80])}],
                                    False, None, vers))
        for opt, o in sorted(e["options"].items()):
            if opt.startswith("--") and opt not in ("--bootstrap-server", "--version") and g.cats.cli_option(tool, opt).known:
                core = "Is `%s` an option of `%s`%s?" % (opt, tool, " in Kafka 4.3" if cat == "kafka_cli" else " (Kubernetes 1.37)")
                out_e.append(real_draft(tpl, tool, domain, "plain_lookup", core, "exists",
                                        [{"catalogue": cat, "entry": "%s %s" % (tool, opt), "file": o["files"][0],
                                          "line": o.get("line") or find_line(o["files"][0], re.escape(opt) + r"(?![\w-])"),
                                          "fact": "option seen in the sources (%s)" % o["evidence"]}], True, None, vers))
    return out_n, out_e


def drafts_real_annotation_nc(g):
    """Strimzi annotation and label names. The annotation catalogue lists only the names the documentation
    mentions, and the label catalogue holds the two labels of the operator pod: any other name is `not in my
    sources` (programmatic: the last word of a real name swapped for the last word of another real name)."""
    out = []
    ann = g.cats.data["strimzi_annotations"]
    names = [e["name"] for e in ann["entries"]]
    src_file = ann["entries"][0]["source_file"]
    lasts = sorted({n.rsplit("-", 1)[-1] for n in names if "-" in n})
    fakes = []
    for n in names:
        if "-" not in n:
            continue
        head = n.rsplit("-", 1)[0]
        for w in lasts:
            fake = head + "-" + w
            if fake not in names and g.cats.annotation(fake).status == NOT_COVERED:
                fakes.append((fake, n))
    g.rng.shuffle(fakes)
    seen = set()
    for fake, n in fakes:
        if fake in seen:
            continue
        seen.add(fake)
        core = "Is `%s` an annotation that Strimzi 1.3 reads or sets on its resources?" % fake
        out.append(real_draft("real_annotation_nc", "strimzi", "strimzi", "not_covered", core, "not in my sources",
                              [{"catalogue": "strimzi_annotations", "absent": fake, "file": src_file, "line": None,
                                "fact": "the annotation catalogue lists only names the docs mention; it is not complete"}],
                              False, None, {"strimzi": "1.3.1"}))
        if len(out) >= 12:
            break
    return out


def drafts_real_label_nc(g):
    out_n, out_e = [], []
    ann = g.cats.data["strimzi_annotations"]
    labs = g.cats.operator_pod_labels()
    dep_file = (g.cats.operator_deployment() or {}).get("source_file") or ann["entries"][0]["source_file"]
    for name in sorted(labs):
        core = "Does the Strimzi 1.3.1 Cluster Operator pod of the install files carry the label `%s`?" % name
        out_e.append(real_draft("real_label_nc", "strimzi", "strimzi", "plain_lookup", core, "exists",
                                [{"catalogue": "strimzi_resources", "entry": "operator pod label %s" % name, "file": dep_file,
                                  "line": None, "fact": "label %s=%s" % (name, labs[name])}], True, None,
                                {"strimzi": "1.3.1"}))
    for e in ann["entries"]:
        n = e["name"]
        if g.cats.operator_label(n).status != NOT_COVERED:
            continue
        core = "Does the Strimzi 1.3.1 Cluster Operator pod of the install files carry the label `%s`?" % n
        out_n.append(real_draft("real_label_nc", "strimzi", "strimzi", "not_covered", core, "not in my sources",
                                [{"catalogue": "strimzi_resources", "entry": "operator pod labels", "absent": n,
                                  "file": dep_file, "line": None,
                                  "fact": "only the labels of the operator pod of the install files are in the catalogue; "
                                          "other labels are not covered"}], False, None, {"strimzi": "1.3.1"}))
    g.rng.shuffle(out_n)
    return out_n[:6], out_e


# ------------------------------------------------------------------ Connect REST
REST_OPS = [
    # (operation id, phrase, example path, group, wrong method for the trap or None)
    ("restartConnector", "restart the connector `orders-sink` (the connector instance, not its tasks)", "/connectors/orders-sink/restart", "lifecycle", "PUT"),
    ("restartTask", "restart task 2 of the connector `orders-sink`", "/connectors/orders-sink/tasks/2/restart", "lifecycle", "PUT"),
    ("pauseConnector", "pause the connector `orders-sink`", "/connectors/orders-sink/pause", "lifecycle", "POST"),
    ("resumeConnector", "resume the paused connector `orders-sink`", "/connectors/orders-sink/resume", "lifecycle", "POST"),
    ("stopConnector", "stop the connector `orders-sink` (its tasks are shut down, the configuration stays)", "/connectors/orders-sink/stop", "lifecycle", "POST"),
    ("destroyConnector", "delete the connector `orders-sink`", "/connectors/orders-sink", "lifecycle", None),
    ("getConnectorStatus", "get the status of the connector `orders-sink`", "/connectors/orders-sink/status", "info", None),
    ("getTaskStatus", "get the status of task 0 of the connector `orders-sink`", "/connectors/orders-sink/tasks/0/status", "info", None),
    ("getTaskConfigs", "list the tasks of the connector `orders-sink` with their configurations", "/connectors/orders-sink/tasks", "info", None),
    ("getConnector", "get the details of the connector `orders-sink`", "/connectors/orders-sink", "info", None),
    ("listConnectors", "list the names of all active connectors", "/connectors", "info", None),
    ("getConnectorActiveTopics", "get the topics the connector `orders-sink` has been using", "/connectors/orders-sink/topics", "info", None),
    ("getOffsets", "read the current offsets of the connector `orders-sink`", "/connectors/orders-sink/offsets", "offsets", None),
    ("resetConnectorOffsets", "reset (delete) the offsets of the stopped connector `orders-sink`", "/connectors/orders-sink/offsets", "offsets", "POST"),
    ("alterConnectorOffsets", "alter the offsets of the stopped connector `orders-sink` with a JSON body", "/connectors/orders-sink/offsets", "offsets", "PUT"),
    ("getConnectorConfig", "read the current configuration of the connector `orders-sink`", "/connectors/orders-sink/config", "config", None),
    ("putConnectorConfig", "create or reconfigure the connector `orders-sink` from a JSON config body (idempotent)", "/connectors/orders-sink/config", "config", "POST"),
    ("patchConnectorConfig", "change only some keys of the configuration of the connector `orders-sink`", "/connectors/orders-sink/config", "config", "POST"),
    ("createConnector", "create a new connector from a JSON body with `name` and `config`", "/connectors", "config", "PUT"),
    ("listConnectorPlugins", "list the connector plugins installed on the worker", "/connector-plugins", "plugins", None),
    ("validateConfigs", "validate a configuration for the plugin `FileStreamSinkConnector` without creating a connector", "/connector-plugins/FileStreamSinkConnector/config/validate", "plugins", "POST"),
    ("getConnectorConfigDef", "get the configuration definition of the plugin `FileStreamSinkConnector`", "/connector-plugins/FileStreamSinkConnector/config", "plugins", None),
    ("resetConnectorActiveTopics", "reset the list of active topics of the connector `orders-sink`", "/connectors/orders-sink/topics/reset", "info", "POST"),
    ("listLoggers", "list the loggers of the worker whose level is explicitly set", "/admin/loggers", "worker", None),
    ("setLevel", "set the log level of the logger `org.apache.kafka.connect` at runtime", "/admin/loggers/org.apache.kafka.connect", "worker", "POST"),
    ("healthCheck", "check the readiness and liveness of the worker", "/health", "worker", None),
    ("serverInfo", "get the version of the worker and the id of the Kafka cluster", "/", "worker", None),
]


def rest_checks(e):
    return [{"type": "answer_line", "format": True}, {"type": "rest_method", "method": e["method"]},
            {"type": "rest_path", "template": e["path"]}]


def drafts_rest(g, template):
    ops = {e["operation_id"]: e for e in g.cats.data["connect_rest"]["entries"]}
    out = []
    for opid, phrase, ex, group, wrong in REST_OPS:
        e = ops[opid]
        assert g.cats.rest_operation(e["method"], ex).known, (opid, ex)
        src = {"catalogue": "connect_rest", "entry": "%s %s" % (e["method"], e["path"]), "file": e["source_file"],
               "line": e["source_line"], "fact": "operation %s: %s" % (opid, e["summary"])}
        good = "%s %s" % (e["method"], ex)
        if template == "rest_trap":
            if not wrong:
                continue
            assert g.cats.rest_operation(wrong, ex).status == "unknown", (opid, wrong)
            core = ("A teammate's runbook says to %s with `%s %s`. Is that the right method and path in the "
                    "Kafka Connect REST interface (Kafka 4.3)? Give the correct request line." % (phrase, wrong, ex))
            fc, bad = "trap_method", "%s %s" % (wrong, ex)
        else:
            if template == "rest_which_a":
                core = "Which HTTP method and path of the Kafka Connect REST interface (Kafka 4.3) would you use to %s?" % phrase
            else:
                core = ("I need to %s through the REST API of a Kafka 4.3 Connect worker. What is the exact "
                        "request line (method and path)?" % phrase)
            fc = "plain_lookup"
            bad = "%s %s" % ("DELETE" if e["method"] != "DELETE" else "GET", ex)
        out.append(mk("connect_rest", template, group, "connect", fc, core,
                      ANSWER_NOTE % "<METHOD> <path>" + " Use the path as the worker serves it, for example with the connector name filled in.",
                      rest_checks(e), [ans("The Connect REST interface serves this as %s." % good, good)],
                      ans("The Connect REST interface serves this as %s." % bad, bad), 1, [src],
                      {"kafka": "4.3.2"}, True, e["summary"]))
    return out


# ------------------------------------------------------------------ Argo CD commands
ARGO_OPS = [
    # (phrase, the reference command line, group, trap note or None); {app} is filled from APPS
    ("sync the application `{app}` and delete the resources that are no longer in Git", "argocd app sync {app} --prune", "app", "`--auto-prune` belongs to `app set` and `app create`, not to `app sync`"),
    ("preview what a sync of `{app}` would do, without changing the cluster", "argocd app sync {app} --dry-run", "app", None),
    ("sync only the resources of `{app}` that are out of sync", "argocd app sync {app} --apply-out-of-sync-only", "app", None),
    ("sync `{app}` to the Git revision `v1.4.2`", "argocd app sync {app} --revision v1.4.2", "app", None),
    ("sync only the Service `web` (core group) of the application `{app}`", "argocd app sync {app} --resource :Service:web", "app", None),
    ("start a sync of `{app}` and return at once, without waiting for it to finish", "argocd app sync {app} --async", "app", None),
    ("show the difference between the live state and the target state of `{app}`, after a hard refresh", "argocd app diff {app} --hard-refresh", "app", None),
    ("roll the application `{app}` back to history ID 5", "argocd app rollback {app} 5", "app", None),
    ("show the deployment history of `{app}`", "argocd app history {app}", "app", None),
    ("delete the application `{app}` without the confirmation prompt", "argocd app delete {app} --yes", "app", None),
    ("list the applications that belong to the project `team-a`", "argocd app list --project team-a", "app", None),
    ("list the applications whose label `env` is `prod`, using the long selector flag", "argocd app list --selector env=prod", "app", None),
    ("get the details of `{app}` after a hard refresh", "argocd app get {app} --hard-refresh", "app", None),
    ("terminate the running operation of the application `{app}`", "argocd app terminate-op {app}", "app", None),
    ("wait until `{app}` is healthy, for at most 300 seconds", "argocd app wait {app} --health --timeout 300", "app", None),
    ("print the manifests of `{app}` for the revision `main`", "argocd app manifests {app} --revision main", "app", None),
    ("follow the logs of the pods of `{app}`", "argocd app logs {app} --follow", "app", None),
    ("turn on automated sync with self-heal and pruning for `{app}`", "argocd app set {app} --sync-policy automated --self-heal --auto-prune", "app", "`--auto-prune` and `--self-heal` belong to `app set`; `app sync` has neither"),
    ("set the target revision of `{app}` to `main`", "argocd app set {app} --revision main", "app", None),
    ("create an application from the manifest file `app.yaml`", "argocd app create --file app.yaml", "app", None),
    ("list the resources of `{app}` that are orphaned", "argocd app resources {app} --orphaned", "app", None),
    ("create the project `team-a` with the description `Team A apps`", "argocd proj create team-a --description 'Team A apps'", "proj", None),
    ("delete the project `team-a`", "argocd proj delete team-a", "proj", None),
    ("register the Git repository `https://git.example.com/org/repo.git` using the SSH private key at `~/.ssh/id_ed25519`", "argocd repo add https://git.example.com/org/repo.git --ssh-private-key-path ~/.ssh/id_ed25519", "repo", None),
    ("log in to the Argo CD server `argocd.example.com` as user `admin`", "argocd login argocd.example.com --username admin", "login", None),
    ("add the cluster of the kubeconfig context `prod-eu` to Argo CD under the name `prod-eu`", "argocd cluster add prod-eu --name prod-eu", "cluster", None),
    ("list all ApplicationSets", "argocd appset list", "appset", None),
    ("get the ApplicationSet `team-apps`", "argocd appset get team-apps", "appset", None),
    # two or more flags (the flags are checked against the catalogue when the item is built)
    ("sync `{app}` with pruning, as a dry run", "argocd app sync {app} --prune --dry-run", "app", None),
    ("sync `{app}` to the Git revision `v1.4.2` and delete the resources that are no longer in Git", "argocd app sync {app} --revision v1.4.2 --prune", "app", None),
    ("force a sync of `{app}` and return at once, without waiting for it to finish", "argocd app sync {app} --force --async", "app", None),
    ("preview a sync of `{app}` at the revision `main`, changing nothing", "argocd app sync {app} --dry-run --revision main", "app", None),
    ("wait until `{app}` is both synced and healthy, for at most 120 seconds", "argocd app wait {app} --sync --health --timeout 120", "app", None),
    ("list the applications of the project `team-a` as JSON", "argocd app list --project team-a --output json", "app", None),
    ("get `{app}` as YAML after a hard refresh", "argocd app get {app} --hard-refresh --output yaml", "app", None),
    ("delete `{app}` without the confirmation prompt, using foreground propagation", "argocd app delete {app} --propagation-policy foreground --yes", "app", None),
    ("show the diff of `{app}` against the revision `main` after a hard refresh", "argocd app diff {app} --revision main --hard-refresh", "app", None),
    ("create the application `{app}` from the repository `https://git.example.com/org/repo.git`, path `k8s`, deploying to the cluster `https://kubernetes.default.svc`, namespace `prod`", "argocd app create {app} --repo https://git.example.com/org/repo.git --path k8s --dest-server https://kubernetes.default.svc --dest-namespace prod", "app", None),
    ("log in to the Argo CD server `argocd.example.com` as user `admin`, over gRPC-Web", "argocd login argocd.example.com --username admin --grpc-web", "login", None),
    ("roll the application `{app}` back to history ID 5 and prune the resources that were added since", "argocd app rollback {app} 5 --prune", "app", None),
    ("follow the last 100 log lines of the pods of `{app}`", "argocd app logs {app} --follow --tail 100", "app", None),
    ("sync only the out-of-sync resources of `{app}` and prune", "argocd app sync {app} --apply-out-of-sync-only --prune", "app", None),
    ("add the cluster of the kubeconfig context `prod-eu` under the name `prod-eu`, without the confirmation prompt", "argocd cluster add prod-eu --name prod-eu --yes", "cluster", None),
    ("create the project `team-a` with the description `Team A apps` and the source repository `https://git.example.com/org/repo.git`", "argocd proj create team-a --description 'Team A apps' --src https://git.example.com/org/repo.git", "proj", None),
]
ARGO_TEMPLATES = ["argo_cmd_a", "argo_cmd_b", "argo_cmd_c"]


def short_flag(rel_file, flag):
    """`-y` for `-y, --yes` in the option table of a command page, else None."""
    try:
        for ln in (ROOT / rel_file).read_text(errors="replace").split("\n"):
            m = re.match(r"^\s+(-\w), " + re.escape(flag) + r"\b", ln)
            if m:
                return m.group(1)
    except OSError:
        pass
    return None


def drafts_argo(g, template):
    c = g.cats
    out = []
    ops = [o for i, o in enumerate(ARGO_OPS) if ARGO_TEMPLATES[i % 3] == template]
    for i, (phrase, line, group, trap) in enumerate(ops):
        app = APPS[(i * 3 + ARGO_TEMPLATES.index(template)) % len(APPS)]
        phrase, line = phrase.replace("{app}", app), line.replace("{app}", app)
        r = sc.resolve_argocd(line, c)
        cmd, flags, toks = r["command"], r["flags"], r["positionals"]
        assert c.argocd_command(cmd).known and not r["bad_subcommand"], line
        entry = [t for t in c.data["argocd"]["cli"] if t["tool"] == cmd][0]
        alts = {}
        srcs = [{"catalogue": "argocd", "entry": cmd, "file": entry["source_file"],
                 "line": find_line(entry["source_file"], r"^## "), "fact": "command exists: %s" % entry["description"]}]
        for f in flags:
            assert c.cli_option(cmd, f).known, (cmd, f)
            fl = entry["options"][f]["files"][0]
            sf = short_flag(fl, f)
            if sf:
                alts[f] = [sf]
            srcs.append({"catalogue": "argocd", "entry": "%s %s" % (cmd, f), "file": fl,
                         "line": find_line(fl, r"^\s+(?:-\w, )?" + re.escape(f) + r"\b"), "fact": "the command has the flag"})
        core = {
            "argo_cmd_a": "Write the `argocd` command to %s." % phrase,
            "argo_cmd_b": "On a CI runner where `argocd` is already logged in, which single command would %s?" % phrase,
            "argo_cmd_c": "How do I %s with the Argo CD CLI? Give the command." % phrase,
        }[template]
        if flags:
            first = flags[0]
            pert = line.replace(" " + first, "", 1)
        else:
            pert = line.replace(cmd.split()[-1], "frobnicate", 1)
        checks = [{"type": "has_command", "format": True},
                  {"type": "cmdline_spec", "command": cmd, "required_flags": flags, "tokens": toks,
                   "flag_alternatives": alts},
                  {"type": "no_invented_cli", "tool": "argocd"}]
        fc = "wrong_scope" if trap else "multi_flag" if len(flags) >= 2 else "plain_lookup"
        d = mk("argocd_command", template, group, "argocd", fc, core,
               "Give the command in a code block; use only `argocd` sub-commands and flags that exist.",
               checks, ["```bash\n%s\n```" % line], "```bash\n%s\n```" % pert, 1, srcs, {"argo-cd": "v3.6.1"}, True,
               entry["description"])
        if trap:
            d["trap"] = trap
        out.append(d)
    return out


# ------------------------------------------------------------------ error names
def err_pool(g, maxlen=400):
    names = g.cats.data["protocol_errors"]["entries"]
    return [e for e in names if e["name"] not in ("NONE", "UNKNOWN_SERVER_ERROR") and 40 <= len(e["description"]) <= maxlen
            and e["name"] not in e["description"]]


def jacc(a, b):
    a, b = tokens(a), tokens(b)
    return len(a & b) / len(a | b) if a | b else 0.0


def err_src(e, fact):
    return {"catalogue": "protocol_errors", "entry": e["name"], "file": e["source_file"], "line": e["source_line"], "fact": fact}


def drafts_err_desc(g, variant):
    pool = err_pool(g)
    allerr = g.cats.data["protocol_errors"]["entries"]
    out = []
    for e in pool:
        sims = [jacc(e["description"], o["description"]) for o in allerr if o["name"] != e["name"]]
        if max(sims) >= 0.7:
            continue
        fc = "confusable_neighbour" if max(sims) >= 0.4 else "plain_lookup"
        core = {
            "a": "Which Kafka protocol error does the broker return in this situation: \"%s\" Give the error name "
                 "as the Kafka protocol reference lists it." % e["description"],
            "b": "In the Kafka 4.3 protocol error list, which error name goes with this description?\n\n> %s" % e["description"],
        }[variant]
        wrong = "SERVER_" + e["name"]
        out.append(mk("error_name", "err_desc_" + variant, e["name"].split("_")[0], "kafka", fc, core,
                      ANSWER_NOTE % "<ERROR_NAME>",
                      [{"type": "answer_line", "format": True},
                       {"type": "answer_equals", "accepted": [e["name"]], "norm": "upper_snake"}],
                      [ans("This is `%s`." % e["name"], e["name"])], ans("This is `%s`." % wrong, wrong), 1,
                      [err_src(e, "description of %s" % e["name"])], {"kafka": "4.3"}, True, e["description"]))
    return out


def drafts_err_retriable(g):
    out = []
    for e in err_pool(g, 600):
        core = "According to the Kafka 4.3 protocol error list, is the error `%s` retriable? Answer yes or no." % e["name"]
        v, w = ("yes", "no") if e["retriable"] else ("no", "yes")
        out.append(mk("error_name", "err_retriable", e["name"].split("_")[0], "kafka", "plain_lookup", core,
                      ANSWER_NOTE % "yes|no",
                      [{"type": "answer_line", "format": True},
                       {"type": "answer_equals", "accepted": [v] + (["true"] if v == "yes" else ["false"]), "norm": "ci"}],
                      [ans("The protocol list marks `%s` as retriable: %s." % (e["name"], v), v)],
                      ans("The protocol list marks `%s` as retriable: %s." % (e["name"], w), w), 1,
                      [err_src(e, "retriable %s" % e["retriable"])], {"kafka": "4.3"}, True, e["description"]))
    # balance: half retriable, half not
    yes = [d for d in out if d["checks"][1]["accepted"][0] == "yes"]
    no = [d for d in out if d["checks"][1]["accepted"][0] == "no"]
    g.rng.shuffle(yes)
    g.rng.shuffle(no)
    k = min(len(yes), len(no))
    return [d for pair in zip(yes[:k], no[:k]) for d in pair]


def drafts_err_mc(g):
    pool = err_pool(g, 170)
    out = []
    for e in pool:
        others = [o for o in pool if o["name"] != e["name"]]
        others.sort(key=lambda o: (-jacc(e["description"], o["description"]), o["name"]))
        close = others[:6]
        dis = g.rng.sample(close, 3)
        fc = "confusable_neighbour" if max(jacc(e["description"], d["description"]) for d in dis) >= 0.2 else "plain_lookup"
        opts = [e] + dis
        g.rng.shuffle(opts)
        letter = "ABCD"[opts.index(e)]
        wrong = "ABCD"[(opts.index(e) + 1) % 4]
        body = "\n".join("%s) %s" % ("ABCD"[i], o["description"]) for i, o in enumerate(opts))
        core = "Which description matches the Kafka protocol error `%s` (Kafka 4.3 error list)?\n\n%s" % (e["name"], body)
        out.append(mk("error_name", "err_mc", e["name"].split("_")[0], "kafka", fc, core,
                      ANSWER_NOTE % "<A|B|C|D>",
                      [{"type": "answer_line", "format": True}, {"type": "answer_letter", "expected": letter}],
                      [ans("`%s` means: %s" % (e["name"], e["description"]), letter)],
                      ans("`%s` means something else." % e["name"], wrong), 1,
                      [err_src(e, "description of %s" % e["name"])] +
                      [err_src(d, "distractor: description of %s" % d["name"]) for d in dis], {"kafka": "4.3"}, True,
                      e["description"]))
    return out


# ------------------------------------------------------------------ blind-guess bookkeeping
def canon(checks):
    """The canonical answer of an item, used for the share of the commonest answer."""
    out = []
    for c in checks:
        t = c["type"]
        if t == "answer_equals":
            out.append(norm_key(c["accepted"][0]))
        elif t == "answer_set":
            out.append("|".join(sorted(norm_key(a) for a in c["accepted"])))
        elif t == "answer_letter":
            out.append(c["expected"])
        elif t == "answer_verdict":
            out.append(c["expected"])
        elif t == "rest_method":
            out.append(c["method"])
        elif t == "rest_path":
            out.append(c["template"])
        elif t == "cmdline_spec":
            out.append(c["command"] + " " + " ".join(sorted(c.get("required_flags", []))))
    return " / ".join(out)


def norm_key(a):
    return str(a).strip().lower()


# Exposed items of templates that carry no per-item chance: the chance of a blind guess, by template.
LEGACY_CHANCE = {"err_retriable": 0.5, "ns_scope_a": 0.1, "ns_scope_b": 0.1, "ns_type_a": 1 / 9.0, "ns_type_b": 1 / 9.0,
                 "err_mc": 0.25}


def item_guess(g, item, kept_modal):
    """Score of a blind guess on one item: the larger of the chance over the listed options and the share of the
    commonest answer (among all candidates for open answers, among the kept items where the answers were
    balanced on purpose). is_real: the larger of 1/3 and the commonest verdict of the kind."""
    if item["kind"] == "is_real":
        return max(1 / 3.0, kept_modal["is_real"])
    mode = g.tpl_guess.get(item["template"], ("pool", 0.0))[0]
    base = g.pool_modal.get(item["template"], 0.0) if mode == "pool" else kept_modal.get(item["template"], 0.0) if mode == "kept" else 0.0
    return max(g.chances.get(item["id"], LEGACY_CHANCE.get(item["template"], 0.0)), base)


def kept_modal_shares(items):
    by = defaultdict(list)
    for i in items:
        by[i["template"]].append(canon(i["checks"]))
        by["is_real"] += [canon(i["checks"])] if i["kind"] == "is_real" else []
    return {k: (max(Counter(v).values()) / len(v) if v else 0.0) for k, v in by.items()} | \
        {k: 0.0 for k in ()}


# ------------------------------------------------------------------ near-miss edits of key names
NOUNS = {"partition", "replica", "record", "thread", "message", "connection", "request", "offset", "segment",
         "listener", "broker", "member", "task", "worker", "retry", "metric", "header", "converter", "transform"}
UNIT_EDITS = [(".ms", ".seconds"), (".seconds", ".ms"), (".ms", ".sec"), (".ms", ".minutes"), (".bytes", ".kb"),
              (".bytes", ".mb"), (".hours", ".days"), (".minutes", ".ms"), (".seconds", ".minutes"), (".ms", ".millis")]
EDIT_GROUPS = {"word": ("word", "order"), "unit": ("unit",), "plural": ("plural",), "drop": ("drop", "prefix")}
EDIT_PHRASE = {"word": "a swapped word", "order": "swapped word order", "unit": "a wrong unit suffix",
               "plural": "singular against plural", "drop": "a dropped word", "prefix": "a neighbouring prefix"}


def key_vocab(cats):
    segs = set()
    for e in cats.data["kafka_config"]["entries"]:
        segs.update(e["key"].split("."))
    return segs


def key_edits(g, key):
    """Controlled single edits of a real key: [(fake, tag, how)], not filtered against the catalogue yet."""
    segs = key.split(".")
    vocab = g.vocab
    out = []
    for i, s in enumerate(segs):
        for a, b in SWAPS:
            for x, y in ((a, b), (b, a)):
                if s == x:
                    out.append((".".join(segs[:i] + [y] + segs[i + 1:]), "word", "swapped word %s -> %s" % (x, y)))
        if len(s) >= 4 and s.isalpha():
            alt = s[:-1] if s.endswith("s") and not s.endswith("ss") else s + "s"
            if alt in vocab or s in NOUNS:
                out.append((".".join(segs[:i] + [alt] + segs[i + 1:]), "plural", "singular/plural %s -> %s" % (s, alt)))
    for i in range(len(segs) - 2):  # adjacent words swapped, the unit at the end stays
        out.append((".".join(segs[:i] + [segs[i + 1], segs[i]] + segs[i + 2:]), "order",
                    "swapped word order %s %s" % (segs[i], segs[i + 1])))
    for a, b in UNIT_EDITS:
        if key.endswith(a):
            out.append((key[:-len(a)] + b, "unit", "wrong unit suffix %s -> %s" % (a, b)))
    if len(segs) >= 4:
        for i in range(1, len(segs) - 1):
            out.append((".".join(segs[:i] + segs[i + 1:]), "drop", "dropped word %s" % segs[i]))
    first = segs[0]
    for o in g.first_segs:
        if o != first and len(o) >= 4 and difflib.SequenceMatcher(None, first, o).ratio() >= 0.78:
            out.append((".".join([o] + segs[1:]), "prefix", "neighbouring prefix %s -> %s" % (first, o)))
    return out


def valid_fakes(g, e, tags=None):
    """Edits of the real key e that are real keys in no scope (and not in the catalogue by any lookup)."""
    res, seen = [], set()
    for fake, tag, how in key_edits(g, e["key"]):
        if tags and tag not in tags:
            continue
        if fake in g.all_keys or fake in seen or fake == e["key"]:
            continue
        if g.cats.config_key(fake, e["scope"]).status != "unknown" or g.cats.config_key(fake).status != "unknown":
            continue
        seen.add(fake)
        res.append((fake, tag, how))
    return res


def uniq_excerpt(g, e, avoid=()):
    ex = excerpt(e["description"])
    if not ex or e["key"] in ex or e["key"].replace(".", " ") in ex or any(a in ex for a in avoid):
        return None
    if sum(1 for o in g.by_scope[e["scope"]] if o["description"].startswith(ex)) != 1:
        return None
    return ex


def absent_src(e, fake, how):
    return {"catalogue": "kafka_config", "absent": fake, "edit": how + " of " + e["key"], "file": e["source_file"],
            "line": None, "fact": "absent from every scope of the complete reference"}


def drafts_nm(g, group):
    """Name the real setting when a teammate's key has one controlled edit."""
    tags = EDIT_GROUPS[group]
    out = []
    for e in cfg_entries(g.cats):
        fakes = valid_fakes(g, e, tags)
        if not fakes:
            continue
        fake, tag, how = fakes[g.rng.randrange(len(fakes))]
        ex = uniq_excerpt(g, e, avoid=(fake,))
        if not ex:
            continue
        sh, k = SCOPE_H[e["scope"]], e["key"]
        core = {
            "word": "A teammate's %s configuration uses `%s` for the setting described as: \"%s\" What is the "
                    "exact name of the real setting in the Kafka 4.3 reference?" % (kf(sh), fake, ex),
            "unit": "I wrote `%s` in a %s config, meaning the setting described as \"%s\" What is the exact "
                    "key name in the Kafka 4.3 reference?" % (fake, kf(sh), ex),
            "plural": "In a review of a Kafka 4.3 %s configuration someone wrote `%s` for this setting: \"%s\" Which "
                      "key name does the reference really use?" % (sh, fake, ex),
            "drop": "Kafka 4.3 %s configuration: a runbook says to set `%s`, the setting described as \"%s\" Give "
                    "the exact name the reference uses for it." % (sh, fake, ex),
        }[group]
        out.append(mk("name_setting", "ns_nm_" + group, cfg_group(e), cfg_domain(e["scope"]), "near_miss_name", core,
                      ANSWER_NOTE % "<key>",
                      [{"type": "answer_line", "format": True},
                       {"type": "answer_equals", "accepted": [k], "norm": "exact"}],
                      [ans("The setting meant is `%s`." % k, k)],
                      ans("The setting is `%s`, as written." % fake, fake), 1,
                      [cfg_src(e, "key %s has this description" % k), absent_src(e, fake, how)], cfg_versions(),
                      True, e["description"]))
    return out


# ------------------------------------------------------------------ scope dependent and wrong scope
def plain_default(dv):
    return dv is not None and dv.strip() not in ("", '""') and len(dv) <= 30 and "<" not in dv and "\n" not in dv


def drafts_sd(g, variant):
    """The same key in several scopes with different defaults, asked for one named scope."""
    out = []
    for e in cfg_entries(g.cats):
        same = g.by_key[e["key"]]
        dv = e.get("default_value")
        if len({x["default_value"] for x in same}) < 2 or not plain_default(dv):
            continue
        if e.get("type") in ("class", "password", None):
            continue
        others = sorted({x["default_value"] for x in same if x["default_value"] != dv and plain_default(x["default_value"])})
        if not others:
            continue
        sh, k = SCOPE_H[e["scope"]], e["key"]
        names = ", ".join(SCOPE_H[x["scope"]] for x in same)
        core = {
            "a": "The key `%s` appears in several Kafka 4.3 configuration references (%s) and its default is not the "
                 "same in all of them. What is its default in the %s configuration?" % (k, names, sh),
            "b": "Someone says `%s` defaults to %s in Kafka. That is the default in another component. What does it "
                 "default to in the %s configuration of Kafka 4.3?" % (k, others[0], sh),
            "c": "Which default value does the Kafka 4.3 %s configuration give to `%s`? (Other components, such as %s, "
                 "use other defaults for the same key.)" % (sh, k, ", ".join(SCOPE_H[x["scope"]] for x in same if x is not e)[:80]),
        }[variant]
        forms = default_forms(e)
        srcs = [cfg_src(e, "default %s in scope %s" % (e["default"], e["scope"]))]
        srcs += [cfg_src(x, "default %s in scope %s" % (x["default"], x["scope"])) for x in same
                 if x is not e and x["default_value"] != dv][:2]
        out.append(mk("name_setting", "sd_default_" + variant, cfg_group(e), cfg_domain(e["scope"]), "scope_dependent",
                      core, ANSWER_NOTE % "<default value>" + " Give the bare value.",
                      [{"type": "answer_line", "format": True},
                       {"type": "answer_equals", "accepted": forms, "norm": "ci"}],
                      [ans("The default of `%s` in the %s configuration is %s." % (k, sh, f), f) for f in forms],
                      ans("The default of `%s` is %s." % (k, others[0]), others[0]), 1, srcs, cfg_versions(), True,
                      e["description"]))
    return out


def drafts_ws_set(g):
    """In which configurations does a key appear (every one of them must be named)."""
    out = []
    seen = set()
    for e in cfg_entries(g.cats):
        k = e["key"]
        if k in seen:
            continue
        same = g.by_key[k]
        scopes = sorted({x["scope"] for x in same})
        if not set(scopes) <= set(SCOPE_OPTIONS) or not 2 <= len(scopes) <= 4:
            continue
        seen.add(k)
        names = [SCOPE_OPT_NAME[s] for s in scopes]
        wrong = names[:-1]
        core = ("In which of the Kafka 4.3 configuration references does the key `%s` appear? Options: %s. Name every "
                "one that lists it, separated by commas." % (k, SCOPE_OPTION_TEXT))
        out.append(mk("name_setting", "ws_scopes_set", k.split(".")[0], cfg_domain(scopes[0]), "wrong_scope", core,
                      ANSWER_NOTE % "<component>, <component>, ...",
                      [{"type": "answer_line", "format": True}, {"type": "answer_set", "accepted": names}],
                      [ans("`%s` is listed in: %s." % (k, ", ".join(names)), ", ".join(names)),
                       ans("`%s` is listed in: %s." % (k, ",".join(names)), ",".join(names))],
                      ans("`%s` is listed in: %s." % (k, ", ".join(wrong)), ", ".join(wrong)), 1,
                      [cfg_src(x, "key listed in scope %s" % x["scope"]) for x in same], cfg_versions(), True,
                      e["description"]))
    return out


SCOPE_OPT_NAME = {s: (s.replace("_", " ") if s.endswith("_connector") else s) for s in SCOPE_OPTIONS}


# ------------------------------------------------------------------ confusable neighbours
def key_neighbours(g, e):
    """Other keys of the same scope with a similar name (two or more words shared, at most two differing)
    and a clearly different description."""
    sa = set(e["key"].split("."))
    out = []
    for o in g.by_scope[e["scope"]]:
        if o["key"] == e["key"] or not o["description"] or not e["description"]:
            continue
        sb = set(o["key"].split("."))
        if len(sa & sb) >= 2 and len(sa ^ sb) <= 2 and jacc(e["description"], o["description"]) < 0.4:
            out.append(o)
    return sorted(out, key=lambda o: o["key"])


def drafts_cn(g, variant):
    out = []
    for e in cfg_entries(g.cats):
        nb = key_neighbours(g, e)
        if not nb:
            continue
        if variant == "default":
            nb = [o for o in nb if plain_default(o.get("default_value")) and o["default_value"] != e.get("default_value")]
            if not nb or not plain_default(e.get("default_value")) or e.get("type") in ("class", "password", None):
                continue
        ex = uniq_excerpt(g, e, avoid=[o["key"] for o in nb])
        if variant != "default" and not ex:
            continue
        if variant != "open" and len(nb) < 2:
            continue  # a choice of three, not a coin flip
        nb = nb[:2]
        sh, k = SCOPE_H[e["scope"]], e["key"]
        opts = [e] + nb
        g.rng.shuffle(opts)
        names = ", ".join("`%s`" % o["key"] for o in opts)
        srcs = [cfg_src(e, "the key asked for")] + [cfg_src(o, "similarly named neighbour %s" % o["key"]) for o in nb]
        if variant == "open":
            core = ("The Kafka 4.3 %s configuration has several similarly named settings (for example %s). Which key is "
                    "described as follows?\n\n> %s\n\nGive the exact key name." % (sh, "`%s`" % nb[0]["key"], ex))
            nopt, accepted = None, [k]
            fmt = ANSWER_NOTE % "<key>"
            ref = ans("That is `%s`, not `%s`." % (k, nb[0]["key"]), k)
            bad = ans("That is `%s`." % nb[0]["key"], nb[0]["key"])
        elif variant == "choice":
            core = "Which of these Kafka 4.3 %s settings is described as follows: %s?\n\n> %s" % (sh, names, ex)
            fmt = ANSWER_NOTE % "<key>" + " Give one key from the list."
            ref = ans("That is `%s`." % k, k)
            bad = ans("That is `%s`." % nb[0]["key"], nb[0]["key"])
        else:
            dv = e["default_value"]
            alt = [o for o in nb if o["default_value"] != dv]
            core = "Which of %s (settings of the Kafka 4.3 %s configuration) has the default value `%s`?" % (names, sh, dv)
            fmt = ANSWER_NOTE % "<key>" + " Give one key from the list."
            ref = ans("That is `%s`." % k, k)
            bad = ans("That is `%s`." % alt[0]["key"], alt[0]["key"])
            # a neighbour with the same default would make the answer ambiguous
            if any(o["default_value"] == dv for o in nb):
                continue
        out.append(mk("name_setting", "cn_" + variant, cfg_group(e), cfg_domain(e["scope"]), "confusable_neighbour",
                      core, fmt, [{"type": "answer_line", "format": True},
                                  {"type": "answer_equals", "accepted": [k], "norm": "exact"}],
                      [ref], bad, 1, srcs, cfg_versions(), True, e["description"]))
        out[-1]["chance"] = 1.0 / len(opts) if variant != "open" else 0.0
    return out


# ------------------------------------------------------------------ value and type traps
VALUE_RE = re.compile(r"^[\w.\-]+$")


def invalid_value(g, vl, key):
    """A plausible but invalid value: the most similar value taken from another enumerated key."""
    low = {v.lower() for v in vl}
    best = (0.0, None)
    for v in g.enum_values:
        if v.lower() in low:
            continue
        r = max(difflib.SequenceMatcher(None, v.lower(), w.lower()).ratio() for w in vl)
        if r > best[0] or (r == best[0] and best[1] is not None and v < best[1]):
            best = (r, v)
    return best[1] if best[0] >= 0.3 else None


def drafts_vt_values(g, variant):
    out = []
    for e in cfg_entries(g.cats):
        vl = e.get("valid_values_list")
        if not vl or not 2 <= len(vl) <= 6 or not all(VALUE_RE.match(v) and len(v) <= 25 for v in vl):
            continue
        bad = invalid_value(g, vl, e["key"])
        if not bad:
            continue
        k, sh = e["key"], SCOPE_H[e["scope"]]
        if variant == "a":
            opts = sorted(vl + [bad])
            core = ("Which of these values are valid for the %s setting `%s` (Kafka 4.3 reference): %s? Name every "
                    "valid one, separated by commas." % (kf(sh), k, ", ".join(opts)))
        else:
            core = ("A colleague set `%s=%s` in a %s configuration. Which values does the Kafka 4.3 reference list "
                    "as valid for `%s`? Give all of them, separated by commas." % (k, bad, kf(sh), k))
        item = mk("name_setting", "vt_values_" + variant, cfg_group(e), cfg_domain(e["scope"]), "value_type_trap",
                  core, ANSWER_NOTE % "<value 1>, <value 2>, ...",
                  [{"type": "answer_line", "format": True}, {"type": "answer_set", "accepted": vl}],
                  [ans("`%s` accepts: %s; `%s` is not valid." % (k, ", ".join(vl), bad), ", ".join(vl)),
                   ans("`%s` accepts: %s." % (k, ",".join(vl)), ",".join(vl))],
                  ans("`%s` accepts: %s." % (k, ", ".join(vl + [bad])), ", ".join(vl + [bad])), 1,
                  [cfg_src(e, "valid values %s" % e["valid_values"])], cfg_versions(), True, e["description"])
        item["chance"] = 2.0 ** -(len(vl) + 1) if variant == "a" else 0.0
        out.append(item)
    return out


def default_bucket(dv):
    if dv == "null":
        return "null"
    if dv == "-1":
        return "-1"
    if dv == "0":
        return "0"
    if dv in ('""', ""):
        return "empty"
    if re.fullmatch(r"\d{10,}", dv or ""):
        return "huge"
    return None


UNITS = (".ms", ".seconds", ".bytes", ".minutes", ".hours", ".days")


def drafts_vt_default(g, variant):
    out = []
    ents = cfg_entries(g.cats)
    typical = {}
    for u in UNITS:
        vals = Counter(e["default_value"] for e in ents if e["key"].endswith(u) and (e.get("default_value") or "").isdigit()
                       and int(e["default_value"]) > 0)
        if vals:
            typical[u] = sorted(vals.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]
    for e in ents:
        dv = e.get("default_value")
        b = default_bucket(dv) if dv is not None else None
        if b is None or e.get("type") in ("class", "password", None) or "<" in (dv or ""):
            continue
        k, sh = e["key"], SCOPE_H[e["scope"]]
        notation = " Write the value as the reference does: `null`, `-1`, `0`, or \"\" for an empty string or list."
        forms = default_forms(e) + (["", "empty", "empty string", "empty list"] if b == "empty" else [])
        if variant == "a":
            if not e.get("description"):
                continue
            core = "What is the default value of the %s setting `%s` in Kafka 4.3?" % (sh, k)
            bad = "30000" if b != "empty" else "none"
            bad = bad if bad not in forms else "1"
            fmt = ANSWER_NOTE % "<default value>" + notation
        else:
            unit = next((u for u in UNITS if k.endswith(u)), None)
            if not unit or unit not in typical or b not in ("null", "-1", "0"):
                continue
            bad = typical[unit]
            core = ("`%s` ends in `%s`, so one would expect a default like %s. What is its actual default in the %s "
                    "configuration of Kafka 4.3?" % (k, unit, bad, sh))
            fmt = ANSWER_NOTE % "<default value>" + notation
        out.append(mk("name_setting", "vt_default_" + variant, cfg_group(e), cfg_domain(e["scope"]), "value_type_trap",
                      core, fmt, [{"type": "answer_line", "format": True},
                                  {"type": "answer_equals", "accepted": forms, "norm": "ci"}],
                      [ans("The default of `%s` is %s." % (k, f), f) for f in forms],
                      ans("The default of `%s` is %s." % (k, bad), bad), 1,
                      [cfg_src(e, "default %s" % e["default"])], cfg_versions(), True, e["description"]))
        out[-1]["bucket"] = b
    return out


def drafts_vt_update(g, variant):
    out = []
    ents = [e for e in g.cats.data["kafka_config"]["entries"] if e["scope"] == "broker" and e.get("update_mode")]
    modes = ["cluster-wide", "per-broker", "read-only"]
    for e in ents:
        m, k = e["update_mode"], e["key"]
        if variant == "a":
            core = ("What is the dynamic update mode of the Kafka 4.3 broker setting `%s`: `read-only`, `per-broker` or "
                    "`cluster-wide`?" % k)
            out.append(mk("name_setting", "vt_update_a", cfg_group(e), "kafka", "value_type_trap", core,
                          ANSWER_NOTE % "<read-only|per-broker|cluster-wide>",
                          [{"type": "answer_line", "format": True},
                           {"type": "answer_equals", "accepted": [m, m.replace("-", " ")], "norm": "ci"}],
                          [ans("The update mode of `%s` is %s." % (k, m), m)],
                          ans("The update mode of `%s` is %s." % (k, modes[(modes.index(m) + 1) % 3]),
                              modes[(modes.index(m) + 1) % 3]), 1,
                          [cfg_src(e, "update mode %s" % m)], cfg_versions(), True, e["description"]))
            out[-1]["bucket"] = m
        else:
            first = k.split(".")[0]
            others = []
            for m2 in modes:
                if m2 == m:
                    continue
                pool = [x for x in ents if x["update_mode"] == m2]
                pool.sort(key=lambda x: (x["key"].split(".")[0] != first, jacc(x["key"].replace(".", " "), k.replace(".", " ")) * -1, x["key"]))
                others.append(pool[0])
            opts = [e] + others
            g.rng.shuffle(opts)
            names = ", ".join("`%s`" % o["key"] for o in opts)
            core = "Which of these Kafka 4.3 broker settings has the dynamic update mode `%s`: %s?" % (m, names)
            out.append(mk("name_setting", "vt_update_b", cfg_group(e), "kafka", "value_type_trap", core,
                          ANSWER_NOTE % "<key>" + " Give one key from the list.",
                          [{"type": "answer_line", "format": True},
                           {"type": "answer_equals", "accepted": [k], "norm": "exact"}],
                          [ans("That is `%s`." % k, k)], ans("That is `%s`." % others[0]["key"], others[0]["key"]), 1,
                          [cfg_src(e, "update mode %s" % m)] + [cfg_src(o, "update mode %s (distractor)" % o["update_mode"]) for o in others],
                          cfg_versions(), True, e["description"]))
            out[-1]["bucket"] = m
            out[-1]["chance"] = 1 / 3.0
    return out


# ------------------------------------------------------------------ errors with more than two outcomes
ERR_SWAPS = [("NOT", "NO"), ("TOO", "OVER"), ("INVALID", "BAD"), ("UNKNOWN", "MISSING"), ("OUT_OF", "BEYOND"),
             ("TIMEOUT", "TIMED_OUT"), ("NOT_AVAILABLE", "UNAVAILABLE"), ("LARGE", "BIG"), ("EXCEPTION", "FAILURE"),
             ("REPLICAS", "REPLICA"), ("PARTITION", "PARTITIONS"), ("EPOCH", "VERSION"), ("OR", "AND"),
             ("FENCED", "STALE"), ("COORDINATOR", "LEADER"), ("LEADER", "COORDINATOR"), ("GROUP", "MEMBER"),
             ("DUPLICATE", "REPEATED"), ("REQUEST", "REQUESTS"), ("RECORD", "MESSAGE")]


def err_fakes(g, name):
    """Controlled edits of an error name that are not in the complete error list: [(fake, how)]."""
    allnames = g.err_names
    words = name.split("_")
    out = []
    padded = "_" + name + "_"
    for a, b in ERR_SWAPS:  # whole words only
        if "_%s_" % a in padded:
            out.append((padded.replace("_%s_" % a, "_%s_" % b, 1).strip("_"), "swapped word %s -> %s" % (a, b)))
    if len(words) >= 4:
        for i in range(1, len(words) - 1):
            out.append(("_".join(words[:i] + words[i + 1:]), "dropped word %s" % words[i]))
    for i in range(len(words) - 1):
        out.append(("_".join(words[:i] + [words[i + 1], words[i]] + words[i + 2:]), "swapped word order"))
    res, seen = [], set()
    for fake, how in out:
        if fake == name or fake in allnames or fake in seen or g.cats.error_name(fake).status != "unknown":
            continue
        seen.add(fake)
        res.append((fake, how))
    return res


def name_sim(a, b):
    sa, sb = set(a.split("_")), set(b.split("_"))
    return len(sa & sb) / len(sa | sb)


def drafts_err_cond(g):
    pool = err_pool(g, 200)
    out = []
    for e in pool:
        nbs = [o for o in pool if o["name"] != e["name"] and jacc(e["description"], o["description"]) < 0.4
               and name_sim(e["name"], o["name"]) >= 0.25]
        nbs.sort(key=lambda o: (-(name_sim(e["name"], o["name"]) + jacc(e["description"], o["description"])), o["name"]))
        if not nbs:
            continue
        o = nbs[0]
        fe, fo = err_fakes(g, e["name"]), err_fakes(g, o["name"])
        fakes = []
        if fe:
            fakes.append(fe[0])
        if fo:
            fakes.append(fo[0])
        for extra in fe[1:] + fo[1:]:
            if len(fakes) < 2 and extra not in fakes:
                fakes.append(extra)
        if len(fakes) < 2:
            continue
        opts = [e["name"], o["name"], fakes[0][0], fakes[1][0]]
        g.rng.shuffle(opts)
        letter = "ABCD"[opts.index(e["name"])]
        wrong = "ABCD"[opts.index(fakes[0][0])]
        body = "\n".join("%s) `%s`" % ("ABCD"[i], n) for i, n in enumerate(opts))
        core = ("A Kafka 4.3 broker answers with a protocol error in this situation: \"%s\" Which of these four names is "
                "the error the protocol reference lists for it?\n\n%s" % (e["description"], body))
        srcs = [err_src(e, "description of %s" % e["name"]), err_src(o, "real neighbour: %s" % o["name"])]
        srcs += [{"catalogue": "protocol_errors", "absent": f, "edit": how + " of the real name", "file": e["source_file"],
                  "line": None, "fact": "absent from the complete error list"} for f, how in fakes]
        out.append(mk("error_name", "err_cond_mc", e["name"].split("_")[0], "kafka", "near_miss_name", core,
                      ANSWER_NOTE % "<A|B|C|D>",
                      [{"type": "answer_line", "format": True}, {"type": "answer_letter", "expected": letter}],
                      [ans("The error is `%s`." % e["name"], letter)], ans("The error is `%s`." % fakes[0][0], wrong), 1,
                      srcs, {"kafka": "4.3"}, True, e["description"]))
        out[-1]["chance"] = 0.25
    return out


def drafts_err_which(g, variant):
    """Which of four similarly named errors is (not) retriable: exactly one differs from the other three."""
    errs = [e for e in g.cats.data["protocol_errors"]["entries"] if e["name"] not in ("NONE", "UNKNOWN_SERVER_ERROR")]
    out = []
    want = True if variant == "a" else False
    for e in errs:
        if e["retriable"] != want:
            continue
        others = [o for o in errs if o["retriable"] != want and name_sim(e["name"], o["name"]) > 0]
        others.sort(key=lambda o: (-name_sim(e["name"], o["name"]), o["name"]))
        if len(others) < 3:
            continue
        others = others[:3]
        opts = [e] + others
        g.rng.shuffle(opts)
        letter = "ABCD"[opts.index(e)]
        body = "\n".join("%s) `%s`" % ("ABCD"[i], o["name"]) for i, o in enumerate(opts))
        core = ("Exactly one of these Kafka protocol errors is %s according to the Kafka 4.3 error list. Which one?\n\n%s"
                % ("marked retriable" if want else "not retriable", body))
        out.append(mk("error_name", "err_which_" + variant, e["name"].split("_")[0], "kafka", "confusable_neighbour", core,
                      ANSWER_NOTE % "<A|B|C|D>",
                      [{"type": "answer_line", "format": True}, {"type": "answer_letter", "expected": letter}],
                      [ans("`%s` is the one." % e["name"], letter)],
                      ans("`%s` is the one." % others[0]["name"], "ABCD"[opts.index(others[0])]), 1,
                      [err_src(e, "retriable %s" % e["retriable"])] +
                      [err_src(o, "retriable %s (distractor)" % o["retriable"]) for o in others],
                      {"kafka": "4.3"}, True, e["description"]))
        out[-1]["chance"] = 0.25
    return out


def drafts_err_name_retr(g):
    """The error for a described condition, and whether it is retriable: two facts, balanced."""
    out = []
    for e in err_pool(g, 400):
        flag = "retriable" if e["retriable"] else "not retriable"
        wrong = "not retriable" if e["retriable"] else "retriable"
        core = ("Which Kafka protocol error does the broker return in this situation: \"%s\" Give the error name as the "
                "Kafka 4.3 protocol reference lists it, and say whether the list marks it retriable." % e["description"])
        out.append(mk("error_name", "err_name_retr", e["name"].split("_")[0], "kafka", "two_facts", core,
                      ANSWER_NOTE % "<ERROR_NAME>, <retriable|not retriable>",
                      [{"type": "answer_line", "format": True},
                       {"type": "answer_set", "accepted": [e["name"].lower(), flag]}],
                      [ans("`%s`, %s." % (e["name"], flag), "%s, %s" % (e["name"], flag))],
                      ans("`%s`, %s." % (e["name"], wrong), "%s, %s" % (e["name"], wrong)), 1,
                      [err_src(e, "description and retriable %s of %s" % (e["retriable"], e["name"]))], {"kafka": "4.3"},
                      True, e["description"]))
        out[-1]["bucket"] = flag
    return out


# ------------------------------------------------------------------ assembly
def generate(cats, seed, chash):
    g = Gen(cats, seed, chash)
    bucket = lambda d: d["bucket"]  # noqa: E731
    # plain lookups, kept few
    for v, n in (("a", 5), ("b", 5), ("c", 5)):
        g.fill(drafts_key_from_desc(g, v), n)
    for v, n in (("a", 5), ("b", 4)):
        g.fill(drafts_default(g, v), n)
    for v, n in (("a", 3), ("b", 3)):
        g.fill(drafts_type(g, v), n, guess=("chance", 1 / 9.0))
    for v, n in (("a", 6), ("b", 6)):
        g.fill(drafts_scope(g, v), n, guess=("chance", 0.1))
    # near-miss names
    for grp in ("word", "unit", "plural", "drop"):
        g.fill(drafts_nm(g, grp), 8)
    # wrong scope and scope dependent
    for v, n in (("a", 8), ("b", 8), ("c", 8)):
        g.fill(drafts_sd(g, v), n)
    g.fill(drafts_ws_set(g), 8, guess=("pool", 2.0 ** -10))
    # confusable neighbours
    for v in ("open", "choice", "default"):
        g.fill(drafts_cn(g, v), 8, guess=("chance", 0.0))
    # value and type traps
    g.fill(drafts_vt_values(g, "a"), 8, guess=("chance", 0.0))
    g.fill(drafts_vt_values(g, "b"), 6)
    g.fill(drafts_vt_default(g, "a"), 8, guess=("kept", 0.0), balance=bucket)
    g.fill(drafts_vt_default(g, "b"), 6, guess=("kept", 0.0), balance=bucket, fam_cap=4)
    g.fill(drafts_vt_update(g, "a"), 9, guess=("kept", 0.0), balance=bucket, fam_cap=4)
    g.fill(drafts_vt_update(g, "b"), 9, guess=("chance", 1 / 3.0), balance=bucket)
    # Connect REST and Argo CD
    g.fill(drafts_rest(g, "rest_which_a"), 8)
    g.fill(drafts_rest(g, "rest_which_b"), 8)
    g.fill(drafts_rest(g, "rest_trap"), 10)
    for t in ARGO_TEMPLATES:
        ds = drafts_argo(g, t)
        multi = [d for d in ds if len(d["checks"][1]["required_flags"]) >= 2]
        g.fill(multi, 4, fam_cap=6)
        g.fill([d for d in ds if d not in multi], 6, fam_cap=6)
    # error names
    g.fill(drafts_err_desc(g, "a"), 4)
    g.fill(drafts_err_desc(g, "b"), 4)
    g.fill(drafts_err_mc(g), 4, guess=("chance", 0.25))
    g.fill(drafts_err_cond(g), 10, guess=("chance", 0.25))
    g.fill(drafts_err_which(g, "a"), 4, guess=("chance", 0.25))
    g.fill(drafts_err_which(g, "b"), 4, guess=("chance", 0.25))
    g.fill(drafts_err_name_retr(g), 8, balance=bucket)
    # the new kinds (scripts/eval2_kinds_a2.py), on their own random stream
    cats = g.cats
    r2 = random.Random("%s|%s|kinds" % (seed, chash))
    for which, n_a, n_b in (("topic", 4, 1), ("user", 4, 1), ("connector", 4, 1), ("nodepool", 4, 1), ("rebalance", 3, 1),
                            ("argo", 4, 1), ("netpol", 3, 1), ("pdb", 3, 1), ("deploy", 4, 1), ("kafka", 4, 1)):
        g.fill(k2.drafts_write(g, r2, cats, which, "a"), n_a, fam_cap=4, guess=("chance", 0.0))
        g.fill(k2.drafts_write(g, r2, cats, which, "b"), n_b, fam_cap=4, guess=("chance", 0.0))
    pairs = k2.scope_pairs(cats)
    for fk, counts in (("near", (10, 10, 8)), ("enum", (5, 4, 3)), ("nest", (5, 4, 0)), ("api", (3, 3, 0)),
                       ("scope", (3, 3, 3))):
        for var, n in zip("abc", counts):
            if n:
                g.fill(k2.drafts_fix(g, r2, cats, fk, var, pairs, n_try=60), n, fam_cap=3, guess=("chance", 0.0))
    g.cmd_stats = Counter()
    ds = k2.drafts_cmd(g, r2, cats, g.cmd_stats)
    for t in "abcde":
        g.fill([d for d in ds if d["template"] == "wc_" + t], 7, fam_cap=6, guess=("chance", 0.0))
    ds = k2.drafts_vtrap(g, r2, cats)
    for tpl, n in (("vtrap_default_a", 2), ("vtrap_default_b", 2), ("vtrap_range_a", 2), ("vtrap_values_a", 1)):
        g.fill([d for d in ds if d["template"] == tpl], n, fam_cap=3, guess=("chance", 0.0))
    g.fill([d for d in ds if d["template"] == "vtrap_added_a"], 10, fam_cap=3, guess=("chance", 1 / 3.0), balance=bucket)
    # is_real: at most REAL_SHARE of the final items, three verdicts of about the same size
    others = sum(1 for i in g.items if i["kind"] != "is_real")
    total_real = int(others * REAL_SHARE / (1 - REAL_SHARE))
    quota = total_real // 3
    have = Counter(canon(i["checks"]) for i in g.items if i["kind"] == "is_real")
    ke, kf = drafts_real_key(g, 0, 0)
    se, sw = drafts_real_scope(g)
    ee, ef = drafts_real_error(g)
    re_, rw = drafts_real_rest(g)
    fl_e, fl_n = drafts_real_flag(g)
    nc_n, nc_e = drafts_real_flag_nc(g)
    an = drafts_real_annotation_nc(g)
    lb_n, lb_e = drafts_real_label_nc(g)

    def tpl_of(ds, t):
        return [d for d in ds if d["template"] == t]
    plan = {"exists": [(ke, 3), (se, 2), (ee, 2), (re_, 2), (fl_e, 4), (tpl_of(nc_e, "real_flag_nc"), 1),
                       (tpl_of(nc_e, "real_kubectl_nc"), 1), (lb_e, 2)],
            "does not exist": [(fl_n, 4), (kf, 3), (sw, 3), (ef, 2), (rw, 2)],
            "not in my sources": [(tpl_of(nc_n, "real_flag_nc"), 6), (tpl_of(nc_n, "real_kubectl_nc"), 4), (an, 4), (lb_n, 3)]}
    for verdict in ("exists", "does not exist", "not in my sources"):
        want = quota - have[verdict]
        for drafts, n in plan[verdict]:
            n = min(n, want)
            if n > 0 and drafts:
                want -= g.fill(drafts, n, fam_cap=3, guess=("pool", 0.0))
    g.items.sort(key=lambda i: i["id"])
    # twenty items of the new kinds are exposed too, chosen by seed; once they are embedded (EXPOSED_V3_TEXT) the
    # embedded rows are used instead and nothing is chosen here
    if not EXPOSED_V3_ROWS():
        rng = random.Random("%s|exposed3" % seed)
        pool = [i for i in g.items if i["kind"] in NEW_KINDS and not i["exposed"]]
        for it in rng.sample(pool, min(EXPOSED_KINDS_N, len(pool))):
            it["exposed"] = True
    return g


MIX_NAMES = {"plain_lookup": "plain lookup", "near_miss_name": "near-miss name",
             "scope": "wrong scope / scope dependent", "confusable_neighbour": "confusable neighbour",
             "value_type_trap": "value and type trap", "trap_method": "REST method trap",
             "multi_flag": "Argo CD two or more flags", "two_facts": "error name and retriable",
             "not_covered": "not in my sources"}


def mix_group(fc):
    return "scope" if fc in ("wrong_scope", "scope_dependent") else fc


def tables(g):
    items = g.items
    n = len(items)
    lines = []

    def tab(title, key):
        c = Counter(i[key] for i in items)
        lines.append("%s:\n%s" % (title, "\n".join("  %-26s %3d  %5.1f %%" % (k, v, 100.0 * v / n) for k, v in sorted(c.items()))))
    lines.append("items: %d  (anchored %d, exposed %d)" % (n, sum(i["anchored"] for i in items),
                                                         sum(i["exposed"] for i in items)))
    tab("per kind", "kind")
    lines.append("per kind and failure class:")
    for kind in sorted({i["kind"] for i in items}):
        c = Counter(i["failure_class"] for i in items if i["kind"] == kind)
        lines.append("  %-16s %s" % (kind, ", ".join("%s %d" % kv for kv in sorted(c.items()))))
    newk = sum(1 for i in items if i["kind"] in NEW_KINDS)
    lines.append("new kinds (%s): %d of %d = %.1f %% (at least %.0f %%)  %s" % (
        ", ".join(NEW_KINDS), newk, n, 100.0 * newk / n, 100 * NEW_KIND_SHARE, "met" if newk >= NEW_KIND_SHARE * n else "NOT MET"))
    emb = EXPOSED_ALL_ROWS()
    lines.append("exposed items: %d marked; %d embedded (first version %d, second %d, new kinds %d), %d excluded because the "
                 "current catalogues contradict them" % (sum(i["exposed"] for i in items), len(emb), len(EXPOSED_V1_ROWS()),
                                                         len(EXPOSED_V2_ROWS()), len(EXPOSED_V3_ROWS()), len(g.exposed_dropped)))
    for i, why in g.exposed_dropped:
        lines.append("  excluded %s: %s" % (i, why))
    for kind in sorted(g.baseline_runs):
        lines.append("blind baseline (skeleton / faulty manifest as given / bare tool) passes all checks: %d of %d %s items" % (
            g.baseline_pass[kind], g.baseline_runs[kind], kind))
    if getattr(g, "cmd_stats", None):
        lines.append("write_command tasks left out because the catalogue could not derive them: %s" % (
            dict(g.cmd_stats) or "none"))
    tab("per domain", "domain")
    tab("per failure class", "failure_class")
    tab("per template", "template")
    tab("per overlap band (question words found in the catalogue description)", "overlap_band")
    lines.append("families: %d" % len({i["family"] for i in items}))
    # the mix against the targets
    grp = Counter(mix_group(i["failure_class"]) for i in items)
    lines.append("mix against the targets:")
    plain = grp["plain_lookup"] / n
    lines.append("  %-34s %3d  %5.1f %%  (at most %.0f %%)  %s" % ("plain lookup", grp["plain_lookup"], 100 * plain,
                                                                  100 * MAX_PLAIN, "met" if plain <= MAX_PLAIN else "NOT MET"))
    for k, lo in MIX_MIN.items():
        share = grp[k] / n
        lines.append("  %-34s %3d  %5.1f %%  (at least %.0f %%)  %s" % (MIX_NAMES[k], grp[k], 100 * share, 100 * lo,
                                                                       "met" if share >= lo else "NOT MET"))
    for k in ("trap_method", "multi_flag", "two_facts", "not_covered"):
        lines.append("  %-34s %3d  %5.1f %%" % (MIX_NAMES[k], grp[k], 100.0 * grp[k] / n))
    argo = [i for i in items if i["kind"] == "argocd_command"]
    multi = [i for i in argo if len(i["checks"][1].get("required_flags", [])) >= 2]
    lines.append("  Argo CD items with two or more required flags: %d of %d = %.0f %% (at least 33 %%)  %s" % (
        len(multi), len(argo), 100.0 * len(multi) / max(1, len(argo)), "met" if 3 * len(multi) >= len(argo) else "NOT MET"))
    real = [i for i in items if i["kind"] == "is_real"]
    verd = Counter(canon(i["checks"]) for i in real)
    lines.append("  is_real: %d of %d = %.1f %% (at most %.0f %%); verdicts %s" % (
        len(real), n, 100.0 * len(real) / n, 100 * REAL_SHARE, dict(sorted(verd.items()))))
    # the score of a blind guess
    km = kept_modal_shares(items)
    gs = {i["id"]: item_guess(g, i, km) for i in items}
    lines.append("score of a blind guess (the better of: chance over the listed options, share of the commonest answer):")
    for title, key in (("kind", "kind"), ("failure class", "failure_class"), ("template", "template")):
        by = defaultdict(list)
        for i in items:
            by[i[key]].append(gs[i["id"]])
        lines.append("  per %s:" % title)
        for k in sorted(by):
            lines.append("    %-26s %3d items  guess %5.1f %%" % (k, len(by[k]), 100.0 * sum(by[k]) / len(by[k])))
    overall = sum(gs.values()) / n
    lines.append("  overall guess over %d items: %.1f %% (limit %.0f %%)  %s" % (
        n, 100 * overall, 100 * MAX_GUESS, "met" if overall < MAX_GUESS else "NOT MET"))
    lines.append("drop and repair counts per kind (candidates tried / kept / repaired; drops by reason):")
    for kind in sorted(g.stats):
        st = g.stats[kind]
        drops = ", ".join("%s %d" % kv for kv in sorted(g.drop_reasons[kind].items())) or "none"
        lines.append("  %-16s tried %4d  kept %3d  repaired %d  skipped_by_caps %d  dropped: %s" % (
            kind, st["built"], st["kept"], st["repaired"], st["skipped_cap"], drops))
    lines.append("near-duplicate corpus: %d texts from %d files; thresholds Jaccard >= %.2f or containment >= %.2f (both sets >= %d tokens)" % (
        len(g.corpus), len(g.corpus_files), JACCARD_T, CONTAIN_T, MIN_TOKENS))
    mx = max((i["dedup"]["max_jaccard"] for i in items), default=0)
    lines.append("highest Jaccard of a kept item against the corpus: %.3f" % mx)
    return "\n".join(lines)


# ------------------------------------------------------------------ the items shown to people in the first version
# Verbatim rows of the first version's output that were exposed (same ids, same text); they must not change.
# The generator of that version is gone, so they are kept here, one JSON object per line.
EXPOSED_V1_TEXT = r'''
{"anchored": true, "catalogue_hash": "b67627d2bf03ef784ac1d3294527086fe7bf6f7d6e2647d6e758936b4641e6d9", "checks": [{"format": true, "type": "has_command"}, {"command": "argocd app sync", "flag_alternatives": {}, "required_flags": ["--resource"], "tokens": ["billing", ":Service:web"], "type": "cmdline_spec"}, {"tool": "argocd", "type": "no_invented_cli"}], "dedup": {"max_containment": 0.333, "max_jaccard": 0.143, "nearest": "data/train.jsonl"}, "domain": "argocd", "exposed": true, "failure_class": "plain_lookup", "family": "argo_cmd_b:app", "generator": "eval2_items_a1/1", "id": "a1-argo_cmd_b-006", "kind": "argocd_command", "overlap": {"desc_in_q": 0.5, "q_in_desc": 0.133}, "overlap_band": "low", "perturbed_reference": "```bash\nargocd app sync billing :Service:web\n```", "perturbed_violates": 1, "question": "On a CI runner where `argocd` is already logged in, which single command would sync only the Service `web` (core group) of the application `billing`?\n\nGive the command in a code block; use only `argocd` sub-commands and flags that exist.", "reference": "```bash\nargocd app sync billing --resource :Service:web\n```", "seed": "m4-a1-1", "sources": [{"catalogue": "argocd", "entry": "argocd app sync", "fact": "command exists: Sync an application to its target state", "file": "raw/argo-cd/docs/user-guide/commands/argocd_app_sync.md", "line": 3}, {"catalogue": "argocd", "entry": "argocd app sync --resource", "fact": "the command has the flag", "file": "raw/argo-cd/docs/user-guide/commands/argocd_app_sync.md", "line": 62}], "template": "argo_cmd_b", "tier": "A", "versions": {"argo-cd": "v3.6.1"}}
{"anchored": true, "catalogue_hash": "b67627d2bf03ef784ac1d3294527086fe7bf6f7d6e2647d6e758936b4641e6d9", "checks": [{"format": true, "type": "answer_line"}, {"accepted": ["COORDINATOR_LOAD_IN_PROGRESS"], "norm": "upper_snake", "type": "answer_equals"}], "dedup": {"max_containment": 0.3, "max_jaccard": 0.136, "nearest": "data/train.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "plain_lookup", "family": "err_desc_a:COORDINATOR", "generator": "eval2_items_a1/1", "id": "a1-err_desc_a-002", "kind": "error_name", "overlap": {"desc_in_q": 1.0, "q_in_desc": 0.333}, "overlap_band": "mid", "perturbed_reference": "This is `SERVER_COORDINATOR_LOAD_IN_PROGRESS`.\nANSWER: SERVER_COORDINATOR_LOAD_IN_PROGRESS", "perturbed_violates": 1, "question": "Which Kafka protocol error does the broker return in this situation: \"The coordinator is loading and hence can't process requests.\" Give the error name as the Kafka protocol reference lists it.\n\nFinish with a final line `ANSWER: <ERROR_NAME>`.", "reference": "This is `COORDINATOR_LOAD_IN_PROGRESS`.\nANSWER: COORDINATOR_LOAD_IN_PROGRESS", "seed": "m4-a1-1", "sources": [{"catalogue": "protocol_errors", "entry": "COORDINATOR_LOAD_IN_PROGRESS", "fact": "description of COORDINATOR_LOAD_IN_PROGRESS", "file": "raw/apache-kafka-config/content/en/43/generated/protocol_errors.html", "line": 22}], "template": "err_desc_a", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "b67627d2bf03ef784ac1d3294527086fe7bf6f7d6e2647d6e758936b4641e6d9", "checks": [{"format": true, "type": "answer_line"}, {"accepted": ["TELEMETRY_TOO_LARGE"], "norm": "upper_snake", "type": "answer_equals"}], "dedup": {"max_containment": 0.353, "max_jaccard": 0.167, "nearest": "work/v3/questions.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "plain_lookup", "family": "err_desc_b:TELEMETRY", "generator": "eval2_items_a1/1", "id": "a1-err_desc_b-003", "kind": "error_name", "overlap": {"desc_in_q": 1.0, "q_in_desc": 0.588}, "overlap_band": "high", "perturbed_reference": "This is `SERVER_TELEMETRY_TOO_LARGE`.\nANSWER: SERVER_TELEMETRY_TOO_LARGE", "perturbed_violates": 1, "question": "In the Kafka 4.3 protocol error list, which error name goes with this description?\n\n> Client sent a push telemetry request larger than the maximum size the broker will accept.\n\nFinish with a final line `ANSWER: <ERROR_NAME>`.", "reference": "This is `TELEMETRY_TOO_LARGE`.\nANSWER: TELEMETRY_TOO_LARGE", "seed": "m4-a1-1", "sources": [{"catalogue": "protocol_errors", "entry": "TELEMETRY_TOO_LARGE", "fact": "description of TELEMETRY_TOO_LARGE", "file": "raw/apache-kafka-config/content/en/43/generated/protocol_errors.html", "line": 126}], "template": "err_desc_b", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "b67627d2bf03ef784ac1d3294527086fe7bf6f7d6e2647d6e758936b4641e6d9", "checks": [{"format": true, "type": "answer_line"}, {"expected": "D", "type": "answer_letter"}], "dedup": {"max_containment": 0.5, "max_jaccard": 0.162, "nearest": "data/train.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "plain_lookup", "family": "err_mc:INVALID", "generator": "eval2_items_a1/1", "id": "a1-err_mc-005", "kind": "error_name", "overlap": {"desc_in_q": 1.0, "q_in_desc": 0.194}, "overlap_band": "low", "perturbed_reference": "`INVALID_RECORD` means something else.\nANSWER: A", "perturbed_violates": 1, "question": "Which description matches the Kafka protocol error `INVALID_RECORD` (Kafka 4.3 error list)?\n\nA) The broker rejected this static consumer since another consumer with the same group.instance.id has registered with a different member.id.\nB) The broker received a duplicate sequence number.\nC) The controller has considered the broker registration to be invalid.\nD) This record has failed the validation on broker and hence will be rejected.\n\nFinish with a final line `ANSWER: <A|B|C|D>`.", "reference": "`INVALID_RECORD` means: This record has failed the validation on broker and hence will be rejected.\nANSWER: D", "seed": "m4-a1-1", "sources": [{"catalogue": "protocol_errors", "entry": "INVALID_RECORD", "fact": "description of INVALID_RECORD", "file": "raw/apache-kafka-config/content/en/43/generated/protocol_errors.html", "line": 95}, {"catalogue": "protocol_errors", "entry": "INVALID_REGISTRATION", "fact": "distractor: description of INVALID_REGISTRATION", "file": "raw/apache-kafka-config/content/en/43/generated/protocol_errors.html", "line": 127}, {"catalogue": "protocol_errors", "entry": "FENCED_INSTANCE_ID", "fact": "distractor: description of FENCED_INSTANCE_ID", "file": "raw/apache-kafka-config/content/en/43/generated/protocol_errors.html", "line": 90}, {"catalogue": "protocol_errors", "entry": "DUPLICATE_SEQUENCE_NUMBER", "fact": "distractor: description of DUPLICATE_SEQUENCE_NUMBER", "file": "raw/apache-kafka-config/content/en/43/generated/protocol_errors.html", "line": 54}], "template": "err_mc", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "b67627d2bf03ef784ac1d3294527086fe7bf6f7d6e2647d6e758936b4641e6d9", "checks": [{"format": true, "type": "answer_line"}, {"accepted": ["yes", "true"], "norm": "ci", "type": "answer_equals"}], "dedup": {"max_containment": 0.4, "max_jaccard": 0.091, "nearest": "work/v3/questions.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "plain_lookup", "family": "err_retriable:NETWORK", "generator": "eval2_items_a1/1", "id": "a1-err_retriable-005", "kind": "error_name", "overlap": {"desc_in_q": 0.0, "q_in_desc": 0.0}, "overlap_band": "low", "perturbed_reference": "The protocol list marks `NETWORK_EXCEPTION` as retriable: no.\nANSWER: no", "perturbed_violates": 1, "question": "According to the Kafka 4.3 protocol error list, is the error `NETWORK_EXCEPTION` retriable? Answer yes or no.\n\nFinish with a final line `ANSWER: yes|no`.", "reference": "The protocol list marks `NETWORK_EXCEPTION` as retriable: yes.\nANSWER: yes", "seed": "m4-a1-1", "sources": [{"catalogue": "protocol_errors", "entry": "NETWORK_EXCEPTION", "fact": "retriable True", "file": "raw/apache-kafka-config/content/en/43/generated/protocol_errors.html", "line": 21}], "template": "err_retriable", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "b67627d2bf03ef784ac1d3294527086fe7bf6f7d6e2647d6e758936b4641e6d9", "checks": [{"format": true, "type": "answer_line"}, {"accepted": ["52428800", "52428800 (50 mebibytes)", "50 mebibytes"], "norm": "ci", "type": "answer_equals"}], "dedup": {"max_containment": 0.625, "max_jaccard": 0.217, "nearest": "data/train.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "scope_dependent", "family": "ns_default_a:consumer", "generator": "eval2_items_a1/1", "id": "a1-ns_default_a-007", "kind": "name_setting", "overlap": {"desc_in_q": 0.106, "q_in_desc": 0.625}, "overlap_band": "high", "perturbed_reference": "The default of `fetch.max.bytes` is 524288000.\nANSWER: 524288000", "perturbed_violates": 1, "question": "What is the default value of the consumer configuration `fetch.max.bytes` in Kafka 4.3?\n\nFinish with a final line `ANSWER: <default value>`. Give the bare value.", "reference": "The default of `fetch.max.bytes` is 52428800.\nANSWER: 52428800", "seed": "m4-a1-1", "sources": [{"catalogue": "kafka_config", "entry": "consumer/fetch.max.bytes", "fact": "default 52428800 (50 mebibytes)", "file": "raw/apache-kafka-config/content/en/43/generated/consumer_config.html", "line": 263}], "template": "ns_default_a", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "b67627d2bf03ef784ac1d3294527086fe7bf6f7d6e2647d6e758936b4641e6d9", "checks": [{"format": true, "type": "answer_line"}, {"accepted": ["ssl.truststore.password"], "norm": "exact", "type": "answer_equals"}], "dedup": {"max_containment": 0.4, "max_jaccard": 0.138, "nearest": "data/train.jsonl"}, "domain": "connect", "exposed": true, "failure_class": "confusable_neighbour", "family": "ns_key_desc_a:connect", "generator": "eval2_items_a1/1", "id": "a1-ns_key_desc_a-002", "kind": "name_setting", "overlap": {"desc_in_q": 0.8, "q_in_desc": 0.522}, "overlap_band": "high", "perturbed_reference": "The description is the one of `ssl.truststore.password.x`.\nANSWER: ssl.truststore.password.x", "perturbed_violates": 1, "question": "In the Kafka 4.3 Connect worker configuration reference, which key is described as follows?\n\n> The password for the trust store file. If a password is not set, trust store file configured will still be used, but integrity checking is disabled.\n\nGive the exact key name.\n\nFinish with a final line `ANSWER: <key>`.", "reference": "The description is the one of `ssl.truststore.password` in the Connect worker configuration reference.\nANSWER: ssl.truststore.password", "seed": "m4-a1-1", "sources": [{"catalogue": "kafka_config", "entry": "connect/ssl.truststore.password", "fact": "key ssl.truststore.password has this description", "file": "raw/apache-kafka-config/content/en/43/generated/connect_config.html", "line": 203}], "template": "ns_key_desc_a", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "b67627d2bf03ef784ac1d3294527086fe7bf6f7d6e2647d6e758936b4641e6d9", "checks": [{"format": true, "type": "answer_line"}, {"accepted": ["ssl.truststore.type"], "norm": "exact", "type": "answer_equals"}], "dedup": {"max_containment": 0.556, "max_jaccard": 0.179, "nearest": "data/train.jsonl"}, "domain": "mirrormaker", "exposed": true, "failure_class": "confusable_neighbour", "family": "ns_key_desc_b:mirror_connector", "generator": "eval2_items_a1/1", "id": "a1-ns_key_desc_b-004", "kind": "name_setting", "overlap": {"desc_in_q": 1.0, "q_in_desc": 0.625}, "overlap_band": "high", "perturbed_reference": "The description is the one of `ssl.truststore.type.x`.\nANSWER: ssl.truststore.type.x", "perturbed_violates": 1, "question": "I'm looking for a Kafka MirrorMaker 2 connector setting and all I remember is its description: \"The file format of the trust store file. The values currently supported by the default ssl.engine.factory.class are [JKS, PKCS12, PEM].\" What is the exact name of the setting?\n\nFinish with a final line `ANSWER: <key>`.", "reference": "The description is the one of `ssl.truststore.type` in the MirrorMaker 2 connector configuration reference.\nANSWER: ssl.truststore.type", "seed": "m4-a1-1", "sources": [{"catalogue": "kafka_config", "entry": "mirror_connector/ssl.truststore.type", "fact": "key ssl.truststore.type has this description", "file": "raw/apache-kafka-config/content/en/43/generated/mirror_connector_config.html", "line": 393}], "template": "ns_key_desc_b", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "b67627d2bf03ef784ac1d3294527086fe7bf6f7d6e2647d6e758936b4641e6d9", "checks": [{"format": true, "type": "answer_line"}, {"accepted": ["remote.log.delete.on.disable"], "norm": "exact", "type": "answer_equals"}], "dedup": {"max_containment": 0.375, "max_jaccard": 0.136, "nearest": "data/train.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "plain_lookup", "family": "ns_key_desc_b:topic", "generator": "eval2_items_a1/1", "id": "a1-ns_key_desc_b-007", "kind": "name_setting", "overlap": {"desc_in_q": 0.529, "q_in_desc": 0.529}, "overlap_band": "high", "perturbed_reference": "The description is the one of `remote.log.delete.on.disable.x`.\nANSWER: remote.log.delete.on.disable.x", "perturbed_violates": 1, "question": "I'm looking for a Kafka topic-level setting and all I remember is its description: \"Determines whether tiered data for a topic should be deleted after tiered storage is disabled on a topic.\" What is the exact name of the setting?\n\nFinish with a final line `ANSWER: <key>`.", "reference": "The description is the one of `remote.log.delete.on.disable` in the topic-level configuration reference.\nANSWER: remote.log.delete.on.disable", "seed": "m4-a1-1", "sources": [{"catalogue": "kafka_config", "entry": "topic/remote.log.delete.on.disable", "fact": "key remote.log.delete.on.disable has this description", "file": "raw/apache-kafka-config/content/en/43/generated/topic_config.html", "line": 278}], "template": "ns_key_desc_b", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "b67627d2bf03ef784ac1d3294527086fe7bf6f7d6e2647d6e758936b4641e6d9", "checks": [{"format": true, "type": "answer_line"}, {"accepted": ["group", "group coordinator"], "norm": "ci", "type": "answer_equals"}], "dedup": {"max_containment": 0.5, "max_jaccard": 0.308, "nearest": "data/train.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "wrong_scope", "family": "ns_scope_a:group", "generator": "eval2_items_a1/1", "id": "a1-ns_scope_a-006", "kind": "name_setting", "overlap": {"desc_in_q": 0.6, "q_in_desc": 0.333}, "overlap_band": "mid", "perturbed_reference": "`streams.assignment.interval.ms` is defined in the producer configuration.\nANSWER: producer", "perturbed_violates": 1, "question": "Which Kafka component's configuration reference lists `streams.assignment.interval.ms` (Kafka 4.3)?\n\nChoose exactly one of: broker, topic, producer, consumer, admin, group, streams, connect, sink connector, source connector. Finish with a final line `ANSWER: <component>`.", "reference": "`streams.assignment.interval.ms` is defined in the group configuration.\nANSWER: group", "seed": "m4-a1-1", "sources": [{"catalogue": "kafka_config", "entry": "group/streams.assignment.interval.ms", "fact": "only scope: group", "file": "raw/apache-kafka-config/content/en/43/generated/group_config.html", "line": 123}], "template": "ns_scope_a", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "b67627d2bf03ef784ac1d3294527086fe7bf6f7d6e2647d6e758936b4641e6d9", "checks": [{"format": true, "type": "answer_line"}, {"accepted": ["broker"], "norm": "ci", "type": "answer_equals"}], "dedup": {"max_containment": 0.667, "max_jaccard": 0.429, "nearest": "data/train.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "wrong_scope", "family": "ns_scope_b:broker:group", "generator": "eval2_items_a1/1", "id": "a1-ns_scope_b-001", "kind": "name_setting", "overlap": {"desc_in_q": 0.333, "q_in_desc": 0.182}, "overlap_band": "low", "perturbed_reference": "`group.consumer.max.session.timeout.ms` is defined in the producer configuration.\nANSWER: producer", "perturbed_violates": 1, "question": "`group.consumer.max.session.timeout.ms` is a Kafka configuration key. In which configuration reference of Kafka 4.3 is it defined?\n\nChoose exactly one of: broker, topic, producer, consumer, admin, group, streams, connect, sink connector, source connector. Finish with a final line `ANSWER: <component>`.", "reference": "`group.consumer.max.session.timeout.ms` is defined in the broker configuration.\nANSWER: broker", "seed": "m4-a1-1", "sources": [{"catalogue": "kafka_config", "entry": "broker/group.consumer.max.session.timeout.ms", "fact": "only scope: broker", "file": "raw/apache-kafka-config/content/en/43/generated/kafka_config.html", "line": 1301}], "template": "ns_scope_b", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "b67627d2bf03ef784ac1d3294527086fe7bf6f7d6e2647d6e758936b4641e6d9", "checks": [{"format": true, "type": "answer_line"}, {"accepted": ["string"], "norm": "ci", "type": "answer_equals"}], "dedup": {"max_containment": 0.556, "max_jaccard": 0.217, "nearest": "data/train.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "plain_lookup", "family": "ns_type_a:broker:ssl", "generator": "eval2_items_a1/1", "id": "a1-ns_type_a-006", "kind": "name_setting", "overlap": {"desc_in_q": 0.114, "q_in_desc": 0.444}, "overlap_band": "mid", "perturbed_reference": "`ssl.principal.mapping.rules` has the type int.\nANSWER: int", "perturbed_violates": 1, "question": "What is the configuration type of `ssl.principal.mapping.rules` in the Kafka 4.3 broker configuration reference?\n\nChoose one of: boolean, class, double, int, list, long, password, short, string. Finish with a final line `ANSWER: <type>`.", "reference": "`ssl.principal.mapping.rules` has the type string.\nANSWER: string", "seed": "m4-a1-1", "sources": [{"catalogue": "kafka_config", "entry": "broker/ssl.principal.mapping.rules", "fact": "type string", "file": "raw/apache-kafka-config/content/en/43/generated/kafka_config.html", "line": 3710}], "template": "ns_type_a", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "b67627d2bf03ef784ac1d3294527086fe7bf6f7d6e2647d6e758936b4641e6d9", "checks": [{"format": true, "type": "answer_line"}, {"accepted": ["legacy", "new"], "type": "answer_set"}], "dedup": {"max_containment": 0.375, "max_jaccard": 0.158, "nearest": "data/train.jsonl"}, "domain": "mirrormaker", "exposed": true, "failure_class": "plain_lookup", "family": "ns_values_a:mirror_source", "generator": "eval2_items_a1/1", "id": "a1-ns_values_a-006", "kind": "name_setting", "overlap": {"desc_in_q": 0.167, "q_in_desc": 0.429}, "overlap_band": "mid", "perturbed_reference": "`metric.names.formats` accepts: legacy.\nANSWER: legacy", "perturbed_violates": 1, "question": "Which values does the Kafka MirrorMaker 2 source connector setting `metric.names.formats` accept (Kafka 4.3 reference)? Give all of them, separated by commas.\n\nFinish with a final line `ANSWER: <value 1>, <value 2>, ...`.", "reference": "`metric.names.formats` accepts: legacy, new.\nANSWER: legacy, new", "seed": "m4-a1-1", "sources": [{"catalogue": "kafka_config", "entry": "mirror_source/metric.names.formats", "fact": "valid values [legacy, new]", "file": "raw/apache-kafka-config/content/en/43/generated/mirror_source_config.html", "line": 73}], "template": "ns_values_a", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "b67627d2bf03ef784ac1d3294527086fe7bf6f7d6e2647d6e758936b4641e6d9", "checks": [{"format": true, "type": "answer_line"}, {"accepted": ["source", "target"], "type": "answer_set"}], "dedup": {"max_containment": 0.5, "max_jaccard": 0.211, "nearest": "data/train.jsonl"}, "domain": "mirrormaker", "exposed": true, "failure_class": "plain_lookup", "family": "ns_values_a:mirror_source", "generator": "eval2_items_a1/1", "id": "a1-ns_values_a-008", "kind": "name_setting", "overlap": {"desc_in_q": 0.833, "q_in_desc": 0.333}, "overlap_band": "mid", "perturbed_reference": "`offset-syncs.topic.location` accepts: source.\nANSWER: source", "perturbed_violates": 1, "question": "Which values does the Kafka MirrorMaker 2 source connector setting `offset-syncs.topic.location` accept (Kafka 4.3 reference)? Give all of them, separated by commas.\n\nFinish with a final line `ANSWER: <value 1>, <value 2>, ...`.", "reference": "`offset-syncs.topic.location` accepts: source, target.\nANSWER: source, target", "seed": "m4-a1-1", "sources": [{"catalogue": "kafka_config", "entry": "mirror_source/offset-syncs.topic.location", "fact": "valid values [source, target]", "file": "raw/apache-kafka-config/content/en/43/generated/mirror_source_config.html", "line": 83}], "template": "ns_values_a", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "b67627d2bf03ef784ac1d3294527086fe7bf6f7d6e2647d6e758936b4641e6d9", "checks": [{"format": true, "type": "answer_line"}, {"accepted": ["read_committed", "read_uncommitted"], "type": "answer_set"}], "dedup": {"max_containment": 0.385, "max_jaccard": 0.132, "nearest": "work/v3/questions.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "plain_lookup", "family": "ns_values_b:group", "generator": "eval2_items_a1/1", "id": "a1-ns_values_b-002", "kind": "name_setting", "overlap": {"desc_in_q": 0.091, "q_in_desc": 0.154}, "overlap_band": "low", "perturbed_reference": "`share.isolation.level` accepts: read_committed.\nANSWER: read_committed", "perturbed_violates": 1, "question": "List every allowed value of `share.isolation.level` in the group coordinator configuration of Kafka 4.3, separated by commas.\n\nFinish with a final line `ANSWER: <value 1>, <value 2>, ...`.", "reference": "`share.isolation.level` accepts: read_committed, read_uncommitted.\nANSWER: read_committed, read_uncommitted", "seed": "m4-a1-1", "sources": [{"catalogue": "kafka_config", "entry": "group/share.isolation.level", "fact": "valid values [read_committed, read_uncommitted]", "file": "raw/apache-kafka-config/content/en/43/generated/group_config.html", "line": 73}], "template": "ns_values_b", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "b67627d2bf03ef784ac1d3294527086fe7bf6f7d6e2647d6e758936b4641e6d9", "checks": [{"format": true, "type": "answer_line"}, {"accepted": ["none", "restart"], "type": "answer_set"}], "dedup": {"max_containment": 0.4, "max_jaccard": 0.211, "nearest": "data/train.jsonl"}, "domain": "mirrormaker", "exposed": true, "failure_class": "plain_lookup", "family": "ns_values_b:mirror_connector", "generator": "eval2_items_a1/1", "id": "a1-ns_values_b-003", "kind": "name_setting", "overlap": {"desc_in_q": 0.208, "q_in_desc": 0.385}, "overlap_band": "mid", "perturbed_reference": "`config.action.reload` accepts: none.\nANSWER: none", "perturbed_violates": 1, "question": "List every allowed value of `config.action.reload` in the MirrorMaker 2 connector configuration of Kafka 4.3, separated by commas.\n\nFinish with a final line `ANSWER: <value 1>, <value 2>, ...`.", "reference": "`config.action.reload` accepts: none, restart.\nANSWER: none, restart", "seed": "m4-a1-1", "sources": [{"catalogue": "kafka_config", "entry": "mirror_connector/config.action.reload", "fact": "valid values [none, restart]", "file": "raw/apache-kafka-config/content/en/43/generated/mirror_connector_config.html", "line": 863}], "template": "ns_values_b", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "b67627d2bf03ef784ac1d3294527086fe7bf6f7d6e2647d6e758936b4641e6d9", "checks": [{"format": true, "type": "answer_line"}, {"accepted": ["broker", "controller"], "type": "answer_set"}], "dedup": {"max_containment": 0.455, "max_jaccard": 0.167, "nearest": "work/v3/questions.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "plain_lookup", "family": "ns_values_b:broker:process", "generator": "eval2_items_a1/1", "id": "a1-ns_values_b-005", "kind": "name_setting", "overlap": {"desc_in_q": 0.5, "q_in_desc": 0.273}, "overlap_band": "mid", "perturbed_reference": "`process.roles` accepts: broker.\nANSWER: broker", "perturbed_violates": 1, "question": "List every allowed value of `process.roles` in the broker configuration of Kafka 4.3, separated by commas.\n\nFinish with a final line `ANSWER: <value 1>, <value 2>, ...`.", "reference": "`process.roles` accepts: broker, controller.\nANSWER: broker, controller", "seed": "m4-a1-1", "sources": [{"catalogue": "kafka_config", "entry": "broker/process.roles", "fact": "valid values [broker, controller]", "file": "raw/apache-kafka-config/content/en/43/generated/kafka_config.html", "line": 25}], "template": "ns_values_b", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "b67627d2bf03ef784ac1d3294527086fe7bf6f7d6e2647d6e758936b4641e6d9", "checks": [{"format": true, "type": "answer_line"}, {"expected": "does not exist", "type": "answer_verdict"}], "dedup": {"max_containment": 0.5, "max_jaccard": 0.129, "nearest": "work/v3/questions.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "near_miss_name", "family": "real_error:FENCED", "generator": "eval2_items_a1/1", "id": "a1-real_error-003", "kind": "is_real", "overlap": {"desc_in_q": 0.111, "q_in_desc": 0.125}, "overlap_band": "low", "perturbed_reference": "My answer, based on the sources: exists.\nANSWER: exists", "perturbed_violates": 1, "question": "Is `FENCED_STATE_VERSION` an error name in the Kafka protocol error list (Kafka 4.3)?\n\nAnswer with exactly one of: `exists`, `does not exist`, `not in my sources`. Finish with a final line `ANSWER: <one of the three>`.", "reference": "My answer, based on the sources: does not exist.\nANSWER: does not exist", "seed": "m4-a1-1", "sources": [{"absent": "FENCED_STATE_VERSION", "catalogue": "protocol_errors", "edit": "swapped word EPOCH -> VERSION of FENCED_STATE_EPOCH", "fact": "absent from the complete error list", "file": "raw/apache-kafka-config/content/en/43/generated/protocol_errors.html", "line": null}], "template": "real_error", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "b67627d2bf03ef784ac1d3294527086fe7bf6f7d6e2647d6e758936b4641e6d9", "checks": [{"format": true, "type": "answer_line"}, {"expected": "does not exist", "type": "answer_verdict"}], "dedup": {"max_containment": 0.5, "max_jaccard": 0.161, "nearest": "work/v3/questions.jsonl"}, "domain": "connect", "exposed": true, "failure_class": "near_miss_name", "family": "real_key_plain:connect", "generator": "eval2_items_a1/1", "id": "a1-real_key_plain-003", "kind": "is_real", "overlap": {"desc_in_q": 0.25, "q_in_desc": 0.2}, "overlap_band": "low", "perturbed_reference": "My answer, based on the sources: exists.\nANSWER: exists", "perturbed_violates": 1, "question": "Is `connections.maximum.idle.ms` a real configuration key of the Kafka 4.3 Connect worker configuration?\n\nAnswer with exactly one of: `exists`, `does not exist`, `not in my sources`. Finish with a final line `ANSWER: <one of the three>`.", "reference": "My answer, based on the sources: does not exist.\nANSWER: does not exist", "seed": "m4-a1-1", "sources": [{"absent": "connections.maximum.idle.ms", "catalogue": "kafka_config", "edit": "swapped word max -> maximum of connections.max.idle.ms", "fact": "absent from every scope of the complete reference", "file": "raw/apache-kafka-config/content/en/43/generated/connect_config.html", "line": null}], "template": "real_key_plain", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "b67627d2bf03ef784ac1d3294527086fe7bf6f7d6e2647d6e758936b4641e6d9", "checks": [{"format": true, "type": "answer_line"}, {"method": "PUT", "type": "rest_method"}, {"template": "/connectors/{connector}/pause", "type": "rest_path"}], "dedup": {"max_containment": 0.5, "max_jaccard": 0.25, "nearest": "data/train.jsonl"}, "domain": "connect", "exposed": true, "failure_class": "plain_lookup", "family": "rest_which_a:lifecycle", "generator": "eval2_items_a1/1", "id": "a1-rest_which_a-003", "kind": "connect_rest", "overlap": {"desc_in_q": 0.667, "q_in_desc": 0.167}, "overlap_band": "low", "perturbed_reference": "The Connect REST interface serves this as DELETE /connectors/orders-sink/pause.\nANSWER: DELETE /connectors/orders-sink/pause", "perturbed_violates": 1, "question": "Which HTTP method and path of the Kafka Connect REST interface (Kafka 4.3) would you use to pause the connector `orders-sink`?\n\nFinish with a final line `ANSWER: <METHOD> <path>`. Use the path as the worker serves it, for example with the connector name filled in.", "reference": "The Connect REST interface serves this as PUT /connectors/orders-sink/pause.\nANSWER: PUT /connectors/orders-sink/pause", "seed": "m4-a1-1", "sources": [{"catalogue": "connect_rest", "entry": "PUT /connectors/{connector}/pause", "fact": "operation pauseConnector: Pause the specified connector", "file": "raw/apache-kafka-config/content/en/43/generated/connect_rest.yaml", "line": 311}], "template": "rest_which_a", "tier": "A", "versions": {"kafka": "4.3.2"}}
'''


# The second twenty exposed items, chosen by seed (m4-a1-1|exposed2) from the 313 items built on the earlier
# catalogues (hash b67627d2bf03...) and printed in runs/m4/reports/tier_a1_rebalance.md. Verbatim rows of that output.
EXPOSED_V2_TEXT = r'''
{"anchored": true, "catalogue_hash": "b67627d2bf03ef784ac1d3294527086fe7bf6f7d6e2647d6e758936b4641e6d9", "checks": [{"format": true, "type": "has_command"}, {"command": "argocd app sync", "flag_alternatives": {}, "required_flags": ["--prune"], "tokens": ["orders-api"], "type": "cmdline_spec"}, {"tool": "argocd", "type": "no_invented_cli"}], "dedup": {"max_containment": 0.364, "max_jaccard": 0.129, "nearest": "data/train.jsonl"}, "domain": "argocd", "exposed": true, "failure_class": "wrong_scope", "family": "argo_cmd_a:app", "generator": "eval2_items_a1/2", "id": "a1-argo_cmd_a-005", "kind": "argocd_command", "overlap": {"desc_in_q": 0.5, "q_in_desc": 0.182}, "overlap_band": "low", "perturbed_reference": "```bash\nargocd app sync orders-api\n```", "perturbed_violates": 1, "question": "Write the `argocd` command to sync the application `orders-api` and delete the resources that are no longer in Git.\n\nGive the command in a code block; use only `argocd` sub-commands and flags that exist.", "reference": "```bash\nargocd app sync orders-api --prune\n```", "seed": "m4-a1-1", "sources": [{"catalogue": "argocd", "entry": "argocd app sync", "fact": "command exists: Sync an application to its target state", "file": "raw/argo-cd/docs/user-guide/commands/argocd_app_sync.md", "line": 3}, {"catalogue": "argocd", "entry": "argocd app sync --prune", "fact": "the command has the flag", "file": "raw/argo-cd/docs/user-guide/commands/argocd_app_sync.md", "line": 60}], "template": "argo_cmd_a", "tier": "A", "versions": {"argo-cd": "v3.6.1"}}
{"anchored": true, "catalogue_hash": "b67627d2bf03ef784ac1d3294527086fe7bf6f7d6e2647d6e758936b4641e6d9", "checks": [{"format": true, "type": "has_command"}, {"command": "argocd app rollback", "flag_alternatives": {}, "required_flags": ["--prune"], "tokens": ["notifications", "5"], "type": "cmdline_spec"}, {"tool": "argocd", "type": "no_invented_cli"}], "dedup": {"max_containment": 0.333, "max_jaccard": 0.158, "nearest": "data/valid.jsonl"}, "domain": "argocd", "exposed": true, "failure_class": "plain_lookup", "family": "argo_cmd_a:app", "generator": "eval2_items_a1/2", "id": "a1-argo_cmd_a-006", "kind": "argocd_command", "overlap": {"desc_in_q": 0.375, "q_in_desc": 0.231}, "overlap_band": "low", "perturbed_reference": "```bash\nargocd app rollback notifications 5\n```", "perturbed_violates": 1, "question": "Write the `argocd` command to roll the application `notifications` back to history ID 5 and prune the resources that were added since.\n\nGive the command in a code block; use only `argocd` sub-commands and flags that exist.", "reference": "```bash\nargocd app rollback notifications 5 --prune\n```", "seed": "m4-a1-1", "sources": [{"catalogue": "argocd", "entry": "argocd app rollback", "fact": "command exists: Rollback application to a previous deployed version by History ID, omitted will Rollback to the previous version", "file": "raw/argo-cd/docs/user-guide/commands/argocd_app_rollback.md", "line": 3}, {"catalogue": "argocd", "entry": "argocd app rollback --prune", "fact": "the command has the flag", "file": "raw/argo-cd/docs/user-guide/commands/argocd_app_rollback.md", "line": 17}], "template": "argo_cmd_a", "tier": "A", "versions": {"argo-cd": "v3.6.1"}}
{"anchored": true, "catalogue_hash": "b67627d2bf03ef784ac1d3294527086fe7bf6f7d6e2647d6e758936b4641e6d9", "checks": [{"format": true, "type": "has_command"}, {"command": "argocd appset get", "flag_alternatives": {}, "required_flags": [], "tokens": ["team-apps"], "type": "cmdline_spec"}, {"tool": "argocd", "type": "no_invented_cli"}], "dedup": {"max_containment": 0.0, "max_jaccard": 0.19, "nearest": "work/v3/questions.rejects.jsonl"}, "domain": "argocd", "exposed": true, "failure_class": "plain_lookup", "family": "argo_cmd_a:appset", "generator": "eval2_items_a1/2", "id": "a1-argo_cmd_a-009", "kind": "argocd_command", "overlap": {"desc_in_q": 0.667, "q_in_desc": 0.286}, "overlap_band": "mid", "perturbed_reference": "```bash\nargocd appset frobnicate team-apps\n```", "perturbed_violates": 1, "question": "Write the `argocd` command to get the ApplicationSet `team-apps`.\n\nGive the command in a code block; use only `argocd` sub-commands and flags that exist.", "reference": "```bash\nargocd appset get team-apps\n```", "seed": "m4-a1-1", "sources": [{"catalogue": "argocd", "entry": "argocd appset get", "fact": "command exists: Get ApplicationSet details", "file": "raw/argo-cd/docs/user-guide/commands/argocd_appset_get.md", "line": 3}], "template": "argo_cmd_a", "tier": "A", "versions": {"argo-cd": "v3.6.1"}}
{"anchored": true, "catalogue_hash": "b67627d2bf03ef784ac1d3294527086fe7bf6f7d6e2647d6e758936b4641e6d9", "checks": [{"format": true, "type": "has_command"}, {"command": "argocd app create", "flag_alternatives": {}, "required_flags": ["--repo", "--path", "--dest-server", "--dest-namespace"], "tokens": ["search-indexer", "https://git.example.com/org/repo.git", "k8s", "https://kubernetes.default.svc", "prod"], "type": "cmdline_spec"}, {"tool": "argocd", "type": "no_invented_cli"}], "dedup": {"max_containment": 0.519, "max_jaccard": 0.237, "nearest": "work/v3/questions.jsonl"}, "domain": "argocd", "exposed": true, "failure_class": "multi_flag", "family": "argo_cmd_b:app", "generator": "eval2_items_a1/2", "id": "a1-argo_cmd_b-004", "kind": "argocd_command", "overlap": {"desc_in_q": 1.0, "q_in_desc": 0.074}, "overlap_band": "low", "perturbed_reference": "```bash\nargocd app create search-indexer https://git.example.com/org/repo.git --path k8s --dest-server https://kubernetes.default.svc --dest-namespace prod\n```", "perturbed_violates": 1, "question": "On a CI runner where `argocd` is already logged in, which single command would create the application `search-indexer` from the repository `https://git.example.com/org/repo.git`, path `k8s`, deploying to the cluster `https://kubernetes.default.svc`, namespace `prod`?\n\nGive the command in a code block; use only `argocd` sub-commands and flags that exist.", "reference": "```bash\nargocd app create search-indexer --repo https://git.example.com/org/repo.git --path k8s --dest-server https://kubernetes.default.svc --dest-namespace prod\n```", "seed": "m4-a1-1", "sources": [{"catalogue": "argocd", "entry": "argocd app create", "fact": "command exists: Create an application", "file": "raw/argo-cd/docs/user-guide/commands/argocd_app_create.md", "line": 3}, {"catalogue": "argocd", "entry": "argocd app create --repo", "fact": "the command has the flag", "file": "raw/argo-cd/docs/user-guide/commands/argocd_app_create.md", "line": 102}, {"catalogue": "argocd", "entry": "argocd app create --path", "fact": "the command has the flag", "file": "raw/argo-cd/docs/user-guide/commands/argocd_app_create.md", "line": 97}, {"catalogue": "argocd", "entry": "argocd app create --dest-server", "fact": "the command has the flag", "file": "raw/argo-cd/docs/user-guide/commands/argocd_app_create.md", "line": 49}, {"catalogue": "argocd", "entry": "argocd app create --dest-namespace", "fact": "the command has the flag", "file": "raw/argo-cd/docs/user-guide/commands/argocd_app_create.md", "line": 48}], "template": "argo_cmd_b", "tier": "A", "versions": {"argo-cd": "v3.6.1"}}
{"anchored": true, "catalogue_hash": "b67627d2bf03ef784ac1d3294527086fe7bf6f7d6e2647d6e758936b4641e6d9", "checks": [{"format": true, "type": "answer_line"}, {"accepted": ["NOT_ENOUGH_REPLICAS"], "norm": "upper_snake", "type": "answer_equals"}], "dedup": {"max_containment": 0.375, "max_jaccard": 0.136, "nearest": "data/valid.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "confusable_neighbour", "family": "err_desc_a:NOT", "generator": "eval2_items_a1/2", "id": "a1-err_desc_a-001", "kind": "error_name", "overlap": {"desc_in_q": 1.0, "q_in_desc": 0.412}, "overlap_band": "mid", "perturbed_reference": "This is `SERVER_NOT_ENOUGH_REPLICAS`.\nANSWER: SERVER_NOT_ENOUGH_REPLICAS", "perturbed_violates": 1, "question": "Which Kafka protocol error does the broker return in this situation: \"Messages are rejected since there are fewer in-sync replicas than required.\" Give the error name as the Kafka protocol reference lists it.\n\nFinish with a final line `ANSWER: <ERROR_NAME>`.", "reference": "This is `NOT_ENOUGH_REPLICAS`.\nANSWER: NOT_ENOUGH_REPLICAS", "seed": "m4-a1-1", "sources": [{"catalogue": "protocol_errors", "entry": "NOT_ENOUGH_REPLICAS", "fact": "description of NOT_ENOUGH_REPLICAS", "file": "raw/apache-kafka-config/content/en/43/generated/protocol_errors.html", "line": 27}], "template": "err_desc_a", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "b67627d2bf03ef784ac1d3294527086fe7bf6f7d6e2647d6e758936b4641e6d9", "checks": [{"format": true, "type": "answer_line"}, {"expected": "B", "type": "answer_letter"}], "dedup": {"max_containment": 0.4, "max_jaccard": 0.174, "nearest": "data/train.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "confusable_neighbour", "family": "err_which_a:COORDINATOR", "generator": "eval2_items_a1/2", "id": "a1-err_which_a-002", "kind": "error_name", "overlap": {"desc_in_q": 1.0, "q_in_desc": 0.118}, "overlap_band": "low", "perturbed_reference": "`BROKER_NOT_AVAILABLE` is the one.\nANSWER: C", "perturbed_violates": 1, "question": "Exactly one of these Kafka protocol errors is marked retriable according to the Kafka 4.3 error list. Which one?\n\nA) `OPERATION_NOT_ATTEMPTED`\nB) `COORDINATOR_NOT_AVAILABLE`\nC) `BROKER_NOT_AVAILABLE`\nD) `RESOURCE_NOT_FOUND`\n\nFinish with a final line `ANSWER: <A|B|C|D>`.", "reference": "`COORDINATOR_NOT_AVAILABLE` is the one.\nANSWER: B", "seed": "m4-a1-1", "sources": [{"catalogue": "protocol_errors", "entry": "COORDINATOR_NOT_AVAILABLE", "fact": "retriable True", "file": "raw/apache-kafka-config/content/en/43/generated/protocol_errors.html", "line": 23}, {"catalogue": "protocol_errors", "entry": "BROKER_NOT_AVAILABLE", "fact": "retriable False (distractor)", "file": "raw/apache-kafka-config/content/en/43/generated/protocol_errors.html", "line": 16}, {"catalogue": "protocol_errors", "entry": "OPERATION_NOT_ATTEMPTED", "fact": "retriable False (distractor)", "file": "raw/apache-kafka-config/content/en/43/generated/protocol_errors.html", "line": 63}, {"catalogue": "protocol_errors", "entry": "RESOURCE_NOT_FOUND", "fact": "retriable False (distractor)", "file": "raw/apache-kafka-config/content/en/43/generated/protocol_errors.html", "line": 99}], "template": "err_which_a", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "b67627d2bf03ef784ac1d3294527086fe7bf6f7d6e2647d6e758936b4641e6d9", "checks": [{"format": true, "type": "answer_line"}, {"accepted": ["false"], "norm": "ci", "type": "answer_equals"}], "dedup": {"max_containment": 0.6, "max_jaccard": 0.214, "nearest": "work/v3/questions.rejects.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "scope_dependent", "family": "ns_default_b:admin", "generator": "eval2_items_a1/2", "id": "a1-ns_default_b-003", "kind": "name_setting", "overlap": {"desc_in_q": 0.375, "q_in_desc": 0.3}, "overlap_band": "mid", "perturbed_reference": "The default of `enable.metrics.push` is falsex.\nANSWER: falsex", "perturbed_violates": 1, "question": "With no override, which value does Kafka 4.3 use for `enable.metrics.push` in the admin client configuration?\n\nFinish with a final line `ANSWER: <default value>`. Give the bare value.", "reference": "The default of `enable.metrics.push` is false.\nANSWER: false", "seed": "m4-a1-1", "sources": [{"catalogue": "kafka_config", "entry": "admin/enable.metrics.push", "fact": "default false", "file": "raw/apache-kafka-config/content/en/43/generated/admin_client_config.html", "line": 503}], "template": "ns_default_b", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "b67627d2bf03ef784ac1d3294527086fe7bf6f7d6e2647d6e758936b4641e6d9", "checks": [{"format": true, "type": "answer_line"}, {"accepted": ["socket.connection.setup.timeout.ms"], "norm": "exact", "type": "answer_equals"}], "dedup": {"max_containment": 0.538, "max_jaccard": 0.25, "nearest": "work/v3/questions_evalslice.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "near_miss_name", "family": "ns_nm_drop:broker:socket", "generator": "eval2_items_a1/2", "id": "a1-ns_nm_drop-007", "kind": "name_setting", "overlap": {"desc_in_q": 0.36, "q_in_desc": 0.409}, "overlap_band": "mid", "perturbed_reference": "The setting is `socket.connection.setup.ms`, as written.\nANSWER: socket.connection.setup.ms", "perturbed_violates": 1, "question": "Kafka 4.3 broker configuration: a runbook says to set `socket.connection.setup.ms`, the setting described as \"The amount of time the client will wait for the socket connection to be established.\" Give the exact name the reference uses for it.\n\nFinish with a final line `ANSWER: <key>`.", "reference": "The setting meant is `socket.connection.setup.timeout.ms`.\nANSWER: socket.connection.setup.timeout.ms", "seed": "m4-a1-1", "sources": [{"catalogue": "kafka_config", "entry": "broker/socket.connection.setup.timeout.ms", "fact": "key socket.connection.setup.timeout.ms has this description", "file": "raw/apache-kafka-config/content/en/43/generated/kafka_config.html", "line": 2962}, {"absent": "socket.connection.setup.ms", "catalogue": "kafka_config", "edit": "dropped word timeout of socket.connection.setup.timeout.ms", "fact": "absent from every scope of the complete reference", "file": "raw/apache-kafka-config/content/en/43/generated/kafka_config.html", "line": null}], "template": "ns_nm_drop", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "b67627d2bf03ef784ac1d3294527086fe7bf6f7d6e2647d6e758936b4641e6d9", "checks": [{"format": true, "type": "answer_line"}, {"accepted": ["log.segment.delete.delay.ms"], "norm": "exact", "type": "answer_equals"}], "dedup": {"max_containment": 0.5, "max_jaccard": 0.143, "nearest": "data/train.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "near_miss_name", "family": "ns_nm_plural:broker:log", "generator": "eval2_items_a1/2", "id": "a1-ns_nm_plural-004", "kind": "name_setting", "overlap": {"desc_in_q": 0.533, "q_in_desc": 0.333}, "overlap_band": "mid", "perturbed_reference": "The setting is `log.segments.delete.delay.ms`, as written.\nANSWER: log.segments.delete.delay.ms", "perturbed_violates": 1, "question": "In a review of a Kafka 4.3 broker configuration someone wrote `log.segments.delete.delay.ms` for this setting: \"The amount of time to wait before deleting a file from the filesystem.\" Which key name does the reference really use?\n\nFinish with a final line `ANSWER: <key>`.", "reference": "The setting meant is `log.segment.delete.delay.ms`.\nANSWER: log.segment.delete.delay.ms", "seed": "m4-a1-1", "sources": [{"catalogue": "kafka_config", "entry": "broker/log.segment.delete.delay.ms", "fact": "key log.segment.delete.delay.ms has this description", "file": "raw/apache-kafka-config/content/en/43/generated/kafka_config.html", "line": 421}, {"absent": "log.segments.delete.delay.ms", "catalogue": "kafka_config", "edit": "singular/plural segment -> segments of log.segment.delete.delay.ms", "fact": "absent from every scope of the complete reference", "file": "raw/apache-kafka-config/content/en/43/generated/kafka_config.html", "line": null}], "template": "ns_nm_plural", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "b67627d2bf03ef784ac1d3294527086fe7bf6f7d6e2647d6e758936b4641e6d9", "checks": [{"format": true, "type": "answer_line"}, {"accepted": ["broker"], "norm": "ci", "type": "answer_equals"}], "dedup": {"max_containment": 0.667, "max_jaccard": 0.429, "nearest": "data/train.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "wrong_scope", "family": "ns_scope_a:broker:group", "generator": "eval2_items_a1/2", "id": "a1-ns_scope_a-003", "kind": "name_setting", "overlap": {"desc_in_q": 0.6, "q_in_desc": 0.545}, "overlap_band": "high", "perturbed_reference": "`group.streams.max.heartbeat.interval.ms` is defined in the producer configuration.\nANSWER: producer", "perturbed_violates": 1, "question": "Which Kafka component's configuration reference lists `group.streams.max.heartbeat.interval.ms` (Kafka 4.3)?\n\nChoose exactly one of: broker, topic, producer, consumer, admin, group, streams, connect, sink connector, source connector. Finish with a final line `ANSWER: <component>`.", "reference": "`group.streams.max.heartbeat.interval.ms` is defined in the broker configuration.\nANSWER: broker", "seed": "m4-a1-1", "sources": [{"catalogue": "kafka_config", "entry": "broker/group.streams.max.heartbeat.interval.ms", "fact": "only scope: broker", "file": "raw/apache-kafka-config/content/en/43/generated/kafka_config.html", "line": 1730}], "template": "ns_scope_a", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "b67627d2bf03ef784ac1d3294527086fe7bf6f7d6e2647d6e758936b4641e6d9", "checks": [{"format": true, "type": "answer_line"}, {"accepted": ["broker"], "norm": "ci", "type": "answer_equals"}], "dedup": {"max_containment": 0.625, "max_jaccard": 0.385, "nearest": "data/train.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "wrong_scope", "family": "ns_scope_b:broker:group", "generator": "eval2_items_a1/2", "id": "a1-ns_scope_b-003", "kind": "name_setting", "overlap": {"desc_in_q": 0.5, "q_in_desc": 0.2}, "overlap_band": "low", "perturbed_reference": "`group.streams.heartbeat.interval.ms` is defined in the producer configuration.\nANSWER: producer", "perturbed_violates": 1, "question": "`group.streams.heartbeat.interval.ms` is a Kafka configuration key. In which configuration reference of Kafka 4.3 is it defined?\n\nChoose exactly one of: broker, topic, producer, consumer, admin, group, streams, connect, sink connector, source connector. Finish with a final line `ANSWER: <component>`.", "reference": "`group.streams.heartbeat.interval.ms` is defined in the broker configuration.\nANSWER: broker", "seed": "m4-a1-1", "sources": [{"catalogue": "kafka_config", "entry": "broker/group.streams.heartbeat.interval.ms", "fact": "only scope: broker", "file": "raw/apache-kafka-config/content/en/43/generated/kafka_config.html", "line": 1697}], "template": "ns_scope_b", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": false, "catalogue_hash": "b67627d2bf03ef784ac1d3294527086fe7bf6f7d6e2647d6e758936b4641e6d9", "checks": [{"format": true, "type": "answer_line"}, {"expected": "not in my sources", "type": "answer_verdict"}], "dedup": {"max_containment": 0.0, "max_jaccard": 0.3, "nearest": "data/train.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "not_covered", "family": "real_flag_nc:kafka-topics.sh", "generator": "eval2_items_a1/2", "id": "a1-real_flag_nc-002", "kind": "is_real", "overlap": null, "overlap_band": "none", "perturbed_reference": "My answer, based on the sources: exists.\nANSWER: exists", "perturbed_violates": 1, "question": "Is `--replicas` an option of `kafka-topics.sh` in Kafka 4.3?\n\nAnswer with exactly one of: `exists`, `does not exist`, `not in my sources`. Finish with a final line `ANSWER: <one of the three>`.", "reference": "My answer, based on the sources: not in my sources.\nANSWER: not in my sources", "seed": "m4-a1-1", "sources": [{"absent": "--replicas", "catalogue": "kafka_cli", "entry": "kafka-topics.sh", "fact": "the sources show only the options seen in examples (10); the tool's full option list is not in them", "file": "raw/apache-kafka/docs/configuration/topic-configs.md", "line": null}], "template": "real_flag_nc", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "b67627d2bf03ef784ac1d3294527086fe7bf6f7d6e2647d6e758936b4641e6d9", "checks": [{"format": true, "type": "answer_line"}, {"expected": "does not exist", "type": "answer_verdict"}], "dedup": {"max_containment": 0.0, "max_jaccard": 0.429, "nearest": "data/train.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "wrong_scope", "family": "real_key_scope:consumer", "generator": "eval2_items_a1/2", "id": "a1-real_key_scope-005", "kind": "is_real", "overlap": null, "overlap_band": "none", "perturbed_reference": "My answer, based on the sources: exists.\nANSWER: exists", "perturbed_violates": 1, "question": "Is `auto.offset.reset` a key of the Kafka 4.3 producer configuration?\n\nAnswer with exactly one of: `exists`, `does not exist`, `not in my sources`. Finish with a final line `ANSWER: <one of the three>`.", "reference": "My answer, based on the sources: does not exist.\nANSWER: does not exist", "seed": "m4-a1-1", "sources": [{"absent_from": "producer", "catalogue": "kafka_config", "entry": "consumer/auto.offset.reset", "fact": "key exists only in scope consumer", "file": "raw/apache-kafka-config/content/en/43/generated/consumer_config.html", "line": 203}], "template": "real_key_scope", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "b67627d2bf03ef784ac1d3294527086fe7bf6f7d6e2647d6e758936b4641e6d9", "checks": [{"format": true, "type": "answer_line"}, {"method": "GET", "type": "rest_method"}, {"template": "/health", "type": "rest_path"}], "dedup": {"max_containment": 0.417, "max_jaccard": 0.125, "nearest": "work/v3/questions.jsonl"}, "domain": "connect", "exposed": true, "failure_class": "plain_lookup", "family": "rest_which_a:worker", "generator": "eval2_items_a1/2", "id": "a1-rest_which_a-002", "kind": "connect_rest", "overlap": {"desc_in_q": 0.571, "q_in_desc": 0.333}, "overlap_band": "mid", "perturbed_reference": "The Connect REST interface serves this as DELETE /health.\nANSWER: DELETE /health", "perturbed_violates": 1, "question": "Which HTTP method and path of the Kafka Connect REST interface (Kafka 4.3) would you use to check the readiness and liveness of the worker?\n\nFinish with a final line `ANSWER: <METHOD> <path>`. Use the path as the worker serves it, for example with the connector name filled in.", "reference": "The Connect REST interface serves this as GET /health.\nANSWER: GET /health", "seed": "m4-a1-1", "sources": [{"catalogue": "connect_rest", "entry": "GET /health", "fact": "operation healthCheck: Health check endpoint to verify worker readiness and liveness", "file": "raw/apache-kafka-config/content/en/43/generated/connect_rest.yaml", "line": 499}], "template": "rest_which_a", "tier": "A", "versions": {"kafka": "4.3.2"}}
{"anchored": true, "catalogue_hash": "b67627d2bf03ef784ac1d3294527086fe7bf6f7d6e2647d6e758936b4641e6d9", "checks": [{"format": true, "type": "answer_line"}, {"method": "PUT", "type": "rest_method"}, {"template": "/connectors/{connector}/stop", "type": "rest_path"}], "dedup": {"max_containment": 0.5, "max_jaccard": 0.19, "nearest": "data/train.jsonl"}, "domain": "connect", "exposed": true, "failure_class": "plain_lookup", "family": "rest_which_a:lifecycle", "generator": "eval2_items_a1/2", "id": "a1-rest_which_a-007", "kind": "connect_rest", "overlap": {"desc_in_q": 0.667, "q_in_desc": 0.118}, "overlap_band": "low", "perturbed_reference": "The Connect REST interface serves this as DELETE /connectors/orders-sink/stop.\nANSWER: DELETE /connectors/orders-sink/stop", "perturbed_violates": 1, "question": "Which HTTP method and path of the Kafka Connect REST interface (Kafka 4.3) would you use to stop the connector `orders-sink` (its tasks are shut down, the configuration stays)?\n\nFinish with a final line `ANSWER: <METHOD> <path>`. Use the path as the worker serves it, for example with the connector name filled in.", "reference": "The Connect REST interface serves this as PUT /connectors/orders-sink/stop.\nANSWER: PUT /connectors/orders-sink/stop", "seed": "m4-a1-1", "sources": [{"catalogue": "connect_rest", "entry": "PUT /connectors/{connector}/stop", "fact": "operation stopConnector: Stop the specified connector", "file": "raw/apache-kafka-config/content/en/43/generated/connect_rest.yaml", "line": 389}], "template": "rest_which_a", "tier": "A", "versions": {"kafka": "4.3.2"}}
{"anchored": true, "catalogue_hash": "b67627d2bf03ef784ac1d3294527086fe7bf6f7d6e2647d6e758936b4641e6d9", "checks": [{"format": true, "type": "answer_line"}, {"method": "PUT", "type": "rest_method"}, {"template": "/admin/loggers/{logger}", "type": "rest_path"}], "dedup": {"max_containment": 0.5, "max_jaccard": 0.208, "nearest": "data/train.jsonl"}, "domain": "connect", "exposed": true, "failure_class": "plain_lookup", "family": "rest_which_b:worker", "generator": "eval2_items_a1/2", "id": "a1-rest_which_b-005", "kind": "connect_rest", "overlap": {"desc_in_q": 0.8, "q_in_desc": 0.211}, "overlap_band": "low", "perturbed_reference": "The Connect REST interface serves this as DELETE /admin/loggers/org.apache.kafka.connect.\nANSWER: DELETE /admin/loggers/org.apache.kafka.connect", "perturbed_violates": 1, "question": "I need to set the log level of the logger `org.apache.kafka.connect` at runtime through the REST API of a Kafka 4.3 Connect worker. What is the exact request line (method and path)?\n\nFinish with a final line `ANSWER: <METHOD> <path>`. Use the path as the worker serves it, for example with the connector name filled in.", "reference": "The Connect REST interface serves this as PUT /admin/loggers/org.apache.kafka.connect.\nANSWER: PUT /admin/loggers/org.apache.kafka.connect", "seed": "m4-a1-1", "sources": [{"catalogue": "connect_rest", "entry": "PUT /admin/loggers/{logger}", "fact": "operation setLevel: Set the log level for the specified logger", "file": "raw/apache-kafka-config/content/en/43/generated/connect_rest.yaml", "line": 50}], "template": "rest_which_b", "tier": "A", "versions": {"kafka": "4.3.2"}}
{"anchored": true, "catalogue_hash": "b67627d2bf03ef784ac1d3294527086fe7bf6f7d6e2647d6e758936b4641e6d9", "checks": [{"format": true, "type": "answer_line"}, {"accepted": ["32768", "32768 (32 kibibytes)", "32 kibibytes"], "norm": "ci", "type": "answer_equals"}], "dedup": {"max_containment": 0.5, "max_jaccard": 0.167, "nearest": "data/train.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "scope_dependent", "family": "sd_default_c:producer", "generator": "eval2_items_a1/2", "id": "a1-sd_default_c-001", "kind": "name_setting", "overlap": {"desc_in_q": 0.417, "q_in_desc": 0.25}, "overlap_band": "mid", "perturbed_reference": "The default of `receive.buffer.bytes` is 65536.\nANSWER: 65536", "perturbed_violates": 1, "question": "Which default value does the Kafka 4.3 producer configuration give to `receive.buffer.bytes`? (Other components, such as admin client, Connect worker, consumer, Kafka Streams, use other defaults for the same key.)\n\nFinish with a final line `ANSWER: <default value>`. Give the bare value.", "reference": "The default of `receive.buffer.bytes` in the producer configuration is 32768.\nANSWER: 32768", "seed": "m4-a1-1", "sources": [{"catalogue": "kafka_config", "entry": "producer/receive.buffer.bytes", "fact": "default 32768 (32 kibibytes) in scope producer", "file": "raw/apache-kafka-config/content/en/43/generated/producer_config.html", "line": 293}, {"catalogue": "kafka_config", "entry": "admin/receive.buffer.bytes", "fact": "default 65536 (64 kibibytes) in scope admin", "file": "raw/apache-kafka-config/content/en/43/generated/admin_client_config.html", "line": 163}, {"catalogue": "kafka_config", "entry": "consumer/receive.buffer.bytes", "fact": "default 65536 (64 kibibytes) in scope consumer", "file": "raw/apache-kafka-config/content/en/43/generated/consumer_config.html", "line": 333}], "template": "sd_default_c", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "b67627d2bf03ef784ac1d3294527086fe7bf6f7d6e2647d6e758936b4641e6d9", "checks": [{"format": true, "type": "answer_line"}, {"accepted": ["65536", "65536 (64 kibibytes)", "64 kibibytes"], "norm": "ci", "type": "answer_equals"}], "dedup": {"max_containment": 0.5, "max_jaccard": 0.167, "nearest": "data/train.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "scope_dependent", "family": "sd_default_c:admin", "generator": "eval2_items_a1/2", "id": "a1-sd_default_c-008", "kind": "name_setting", "overlap": {"desc_in_q": 0.417, "q_in_desc": 0.25}, "overlap_band": "mid", "perturbed_reference": "The default of `receive.buffer.bytes` is 32768.\nANSWER: 32768", "perturbed_violates": 1, "question": "Which default value does the Kafka 4.3 admin client configuration give to `receive.buffer.bytes`? (Other components, such as Connect worker, consumer, producer, Kafka Streams, use other defaults for the same key.)\n\nFinish with a final line `ANSWER: <default value>`. Give the bare value.", "reference": "The default of `receive.buffer.bytes` in the admin client configuration is 65536.\nANSWER: 65536", "seed": "m4-a1-1", "sources": [{"catalogue": "kafka_config", "entry": "admin/receive.buffer.bytes", "fact": "default 65536 (64 kibibytes) in scope admin", "file": "raw/apache-kafka-config/content/en/43/generated/admin_client_config.html", "line": 163}, {"catalogue": "kafka_config", "entry": "connect/receive.buffer.bytes", "fact": "default 32768 (32 kibibytes) in scope connect", "file": "raw/apache-kafka-config/content/en/43/generated/connect_config.html", "line": 243}, {"catalogue": "kafka_config", "entry": "producer/receive.buffer.bytes", "fact": "default 32768 (32 kibibytes) in scope producer", "file": "raw/apache-kafka-config/content/en/43/generated/producer_config.html", "line": 293}], "template": "sd_default_c", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "b67627d2bf03ef784ac1d3294527086fe7bf6f7d6e2647d6e758936b4641e6d9", "checks": [{"format": true, "type": "answer_line"}, {"accepted": ["-1"], "norm": "ci", "type": "answer_equals"}], "dedup": {"max_containment": 0.0, "max_jaccard": 0.286, "nearest": "data/train.jsonl"}, "domain": "streams", "exposed": true, "failure_class": "value_type_trap", "family": "vt_default_a:streams", "generator": "eval2_items_a1/2", "id": "a1-vt_default_a-003", "kind": "name_setting", "overlap": {"desc_in_q": 0.176, "q_in_desc": 0.429}, "overlap_band": "mid", "perturbed_reference": "The default of `replication.factor` is 30000.\nANSWER: 30000", "perturbed_violates": 1, "question": "What is the default value of the Kafka Streams setting `replication.factor` in Kafka 4.3?\n\nFinish with a final line `ANSWER: <default value>`. Write the value as the reference does: `null`, `-1`, `0`, or \"\" for an empty string or list.", "reference": "The default of `replication.factor` is -1.\nANSWER: -1", "seed": "m4-a1-1", "sources": [{"catalogue": "kafka_config", "entry": "streams/replication.factor", "fact": "default -1", "file": "raw/apache-kafka-config/content/en/43/generated/streams_config.html", "line": 273}], "template": "vt_default_a", "tier": "A", "versions": {"kafka": "4.3"}}
{"anchored": true, "catalogue_hash": "b67627d2bf03ef784ac1d3294527086fe7bf6f7d6e2647d6e758936b4641e6d9", "checks": [{"format": true, "type": "answer_line"}, {"accepted": ["cluster-wide", "cluster wide"], "norm": "ci", "type": "answer_equals"}], "dedup": {"max_containment": 0.667, "max_jaccard": 0.316, "nearest": "data/train.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "value_type_trap", "family": "vt_update_a:broker:group", "generator": "eval2_items_a1/2", "id": "a1-vt_update_a-004", "kind": "name_setting", "overlap": {"desc_in_q": 0.1, "q_in_desc": 0.125}, "overlap_band": "low", "perturbed_reference": "The update mode of `group.coordinator.cached.buffer.max.bytes` is per-broker.\nANSWER: per-broker", "perturbed_violates": 1, "question": "What is the dynamic update mode of the Kafka 4.3 broker setting `group.coordinator.cached.buffer.max.bytes`: `read-only`, `per-broker` or `cluster-wide`?\n\nFinish with a final line `ANSWER: <read-only|per-broker|cluster-wide>`.", "reference": "The update mode of `group.coordinator.cached.buffer.max.bytes` is cluster-wide.\nANSWER: cluster-wide", "seed": "m4-a1-1", "sources": [{"catalogue": "kafka_config", "entry": "broker/group.coordinator.cached.buffer.max.bytes", "fact": "update mode cluster-wide", "file": "raw/apache-kafka-config/content/en/43/generated/kafka_config.html", "line": 1389}], "template": "vt_update_a", "tier": "A", "versions": {"kafka": "4.3"}}
'''


# Twenty exposed items of the new kinds (write a resource, fix the manifest, write the command, version trap),
# chosen by seed (m4-a1-1|exposed3) from the items of the first build of those kinds and printed in
# runs/m4/reports/tier_a2_kinds.md. Verbatim rows of that output.
EXPOSED_V3_TEXT = r'''
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "checks": [{"format": true, "type": "yaml_document"}, {"api_version": "argoproj.io/v1alpha1", "kind": "Application", "type": "yaml_one_doc"}, {"kind": "Application", "type": "yaml_strict"}, {"fields": [{"path": ["apiVersion"], "value": "argoproj.io/v1alpha1"}, {"path": ["kind"], "value": "Application"}, {"path": ["metadata", "name"], "value": "clickstream-app"}, {"path": ["metadata", "namespace"], "value": "argocd"}, {"path": ["spec", "project"], "value": "platform"}, {"path": ["spec", "source", "repoURL"], "value": "https://git.example.com/org/kafka-config.git"}, {"path": ["spec", "source", "targetRevision"], "value": "v2.1.0"}, {"path": ["spec", "source", "path"], "value": "deploy/prod"}, {"path": ["spec", "destination", "server"], "value": "https://kubernetes.default.svc"}, {"path": ["spec", "destination", "namespace"], "value": "staging"}, {"path": ["spec", "syncPolicy", "syncOptions"], "value": ["CreateNamespace=true"]}], "kind": "Application", "type": "yaml_fields_equal"}], "dedup": {"max_containment": 0.652, "max_jaccard": 0.224, "nearest": "data/train.jsonl"}, "domain": "argocd", "exposed": true, "failure_class": "wrong_api_version", "family": "fm_api_a:Application:destination+project+source+syncPolicy", "fault": {"class": "wrong_api_version", "detail": "apiVersion `argoproj.io/v1beta2` for Application", "faulty_path": ["apiVersion"], "original_path": ["apiVersion"]}, "generator": "eval2_items_a1/3", "id": "a1-fm_api_a-001", "kind": "fix_manifest", "overlap": null, "overlap_band": "none", "perturbed_reference": "```yaml\napiVersion: argoproj.io/v1beta2\nkind: Application\nmetadata:\n  name: clickstream-app\n  namespace: argocd\nspec:\n  project: platform\n  source:\n    repoURL: https://git.example.com/org/kafka-config.git\n    targetRevision: v2.1.0\n    path: deploy/prod\n  destination:\n    server: https://kubernetes.default.svc\n    namespace: staging\n  syncPolicy:\n    syncOptions:\n    - CreateNamespace=true\n```", "perturbed_violates": 2, "preserves_unknown": [{"path": "spec", "preserves_unknown_fields": false}, {"path": "spec.source", "preserves_unknown_fields": false}, {"path": "spec.destination", "preserves_unknown_fields": false}, {"path": "spec.syncPolicy", "preserves_unknown_fields": false}], "question": "`kubectl apply` (or the Argo CD / Strimzi validation) rejects the manifest below. It has exactly one mistake. Return the corrected manifest in full as one YAML document and change nothing that is not wrong.\n\n```yaml\napiVersion: argoproj.io/v1beta2\nkind: Application\nmetadata:\n  name: clickstream-app\n  namespace: argocd\nspec:\n  project: platform\n  source:\n    repoURL: https://git.example.com/org/kafka-config.git\n    targetRevision: v2.1.0\n    path: deploy/prod\n  destination:\n    server: https://kubernetes.default.svc\n    namespace: staging\n  syncPolicy:\n    syncOptions:\n    - CreateNamespace=true\n```\n\nGive the corrected manifest in one fenced code block.", "reference": "```yaml\napiVersion: argoproj.io/v1alpha1\nkind: Application\nmetadata:\n  name: clickstream-app\n  namespace: argocd\nspec:\n  project: platform\n  source:\n    repoURL: https://git.example.com/org/kafka-config.git\n    targetRevision: v2.1.0\n    path: deploy/prod\n  destination:\n    server: https://kubernetes.default.svc\n    namespace: staging\n  syncPolicy:\n    syncOptions:\n    - CreateNamespace=true\n```", "resource_kind": "Application", "seed": "m4-a1-1", "sources": [{"catalogue": "argocd", "entry": "Application spec.project", "fact": "type string; required", "file": "raw_catalogue/argocd-crds/manifests/crds/application-crd.yaml", "line": null}, {"catalogue": "argocd", "entry": "Application spec.source.repoURL", "fact": "type string; required", "file": "raw_catalogue/argocd-crds/manifests/crds/application-crd.yaml", "line": null}, {"catalogue": "argocd", "entry": "Application spec.source.targetRevision", "fact": "type string", "file": "raw_catalogue/argocd-crds/manifests/crds/application-crd.yaml", "line": null}, {"catalogue": "argocd", "entry": "Application spec.source.path", "fact": "type string", "file": "raw_catalogue/argocd-crds/manifests/crds/application-crd.yaml", "line": null}, {"catalogue": "argocd", "entry": "Application spec.destination.server", "fact": "type string", "file": "raw_catalogue/argocd-crds/manifests/crds/application-crd.yaml", "line": null}, {"catalogue": "argocd", "entry": "Application spec.destination.namespace", "fact": "type string", "file": "raw_catalogue/argocd-crds/manifests/crds/application-crd.yaml", "line": null}, {"catalogue": "argocd", "entry": "Application spec.syncPolicy.syncOptions", "fact": "type array", "file": "raw_catalogue/argocd-crds/manifests/crds/application-crd.yaml", "line": null}, {"catalogue": "argocd", "entry": "Application apiVersion", "fact": "the schema serves argoproj.io/v1alpha1 only", "file": "raw_catalogue/argocd-crds/manifests/crds/application-crd.yaml", "line": null}], "template": "fm_api_a", "tier": "A", "validator_codes": ["unknown_api_version"], "versions": {"argo-cd": "v3.6.1"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "checks": [{"format": true, "type": "yaml_document"}, {"api_version": "policy/v1", "kind": "PodDisruptionBudget", "type": "yaml_one_doc"}, {"kind": "PodDisruptionBudget", "type": "yaml_strict"}, {"fields": [{"path": ["apiVersion"], "value": "policy/v1"}, {"path": ["kind"], "value": "PodDisruptionBudget"}, {"path": ["metadata", "name"], "value": "connect-pdb"}, {"path": ["metadata", "namespace"], "value": "kafka"}, {"path": ["spec", "selector", "matchLabels", "app"], "value": "schema-registry"}, {"path": ["spec", "selector", "matchLabels", "component"], "value": "worker"}, {"path": ["spec", "minAvailable"], "value": 3}, {"path": ["spec", "unhealthyPodEvictionPolicy"], "value": "AlwaysAllow"}], "kind": "PodDisruptionBudget", "type": "yaml_fields_equal"}], "dedup": {"max_containment": 0.556, "max_jaccard": 0.109, "nearest": "data/train.jsonl"}, "domain": "kubernetes", "exposed": true, "failure_class": "wrong_api_version", "family": "fm_api_a:PodDisruptionBudget:minAvailable+selector+unhealthyPodEvictionPolicy", "fault": {"class": "wrong_api_version", "detail": "apiVersion `policy/v1beta1` for PodDisruptionBudget", "faulty_path": ["apiVersion"], "original_path": ["apiVersion"]}, "generator": "eval2_items_a1/3", "id": "a1-fm_api_a-003", "kind": "fix_manifest", "overlap": null, "overlap_band": "none", "perturbed_reference": "```yaml\napiVersion: policy/v1beta1\nkind: PodDisruptionBudget\nmetadata:\n  name: connect-pdb\n  namespace: kafka\nspec:\n  selector:\n    matchLabels:\n      app: schema-registry\n      component: worker\n  minAvailable: 3\n  unhealthyPodEvictionPolicy: AlwaysAllow\n```", "perturbed_violates": 2, "preserves_unknown": [{"path": "spec.selector.matchLabels", "preserves_unknown_fields": true}, {"path": "spec", "preserves_unknown_fields": false}], "question": "`kubectl apply` (or the Argo CD / Strimzi validation) rejects the manifest below. It has exactly one mistake. Return the corrected manifest in full as one YAML document and change nothing that is not wrong.\n\n```yaml\napiVersion: policy/v1beta1\nkind: PodDisruptionBudget\nmetadata:\n  name: connect-pdb\n  namespace: kafka\nspec:\n  selector:\n    matchLabels:\n      app: schema-registry\n      component: worker\n  minAvailable: 3\n  unhealthyPodEvictionPolicy: AlwaysAllow\n```\n\nGive the corrected manifest in one fenced code block.", "reference": "```yaml\napiVersion: policy/v1\nkind: PodDisruptionBudget\nmetadata:\n  name: connect-pdb\n  namespace: kafka\nspec:\n  selector:\n    matchLabels:\n      app: schema-registry\n      component: worker\n  minAvailable: 3\n  unhealthyPodEvictionPolicy: AlwaysAllow\n```", "resource_kind": "PodDisruptionBudget", "seed": "m4-a1-1", "sources": [{"catalogue": "kubernetes", "entry": "PodDisruptionBudget spec.selector.matchLabels", "fact": "type object", "file": "raw_catalogue/kubernetes-api/api/openapi-spec/swagger.json", "line": null}, {"catalogue": "kubernetes", "entry": "PodDisruptionBudget spec.minAvailable", "fact": "type integer/string", "file": "raw_catalogue/kubernetes-api/api/openapi-spec/swagger.json", "line": null}, {"catalogue": "kubernetes", "entry": "PodDisruptionBudget spec.unhealthyPodEvictionPolicy", "fact": "type string", "file": "raw_catalogue/kubernetes-api/api/openapi-spec/swagger.json", "line": null}, {"catalogue": "kubernetes", "entry": "PodDisruptionBudget apiVersion", "fact": "the schema serves policy/v1 only", "file": "raw_catalogue/kubernetes-api/api/openapi-spec/swagger.json", "line": null}], "template": "fm_api_a", "tier": "A", "validator_codes": ["unknown_api_version"], "versions": {"kubernetes": "v1.37.2"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "checks": [{"format": true, "type": "yaml_document"}, {"api_version": "kafka.strimzi.io/v1", "kind": "Kafka", "type": "yaml_one_doc"}, {"kind": "Kafka", "type": "yaml_strict"}, {"fields": [{"path": ["apiVersion"], "value": "kafka.strimzi.io/v1"}, {"path": ["kind"], "value": "Kafka"}, {"path": ["metadata", "name"], "value": "data-platform"}, {"path": ["metadata", "namespace"], "value": "kafka"}, {"path": ["spec", "kafka", "listeners", 0, "name"], "value": "plain"}, {"path": ["spec", "kafka", "listeners", 0, "port"], "value": 9094}, {"path": ["spec", "kafka", "listeners", 0, "type"], "value": "loadbalancer"}, {"path": ["spec", "kafka", "listeners", 0, "tls"], "value": true}, {"loose": true, "path": ["spec", "kafka", "config", "group.share.record.lock.duration.ms"], "value": 600000}, {"loose": true, "path": ["spec", "kafka", "config", "group.share.max.heartbeat.interval.ms"], "value": 10000}], "kind": "Kafka", "type": "yaml_fields_equal"}, {"kind": "Kafka", "paths": [["spec", "kafka", "listeners", 0, "nmae"]], "type": "yaml_fields_absent"}, {"kind": "Kafka", "path": ["spec", "kafka", "config"], "scope": "broker", "type": "yaml_config_keys"}], "dedup": {"max_containment": 0.778, "max_jaccard": 0.13, "nearest": "data/train.jsonl"}, "domain": "strimzi", "exposed": true, "failure_class": "near_miss_name", "family": "fm_near_a:Kafka:kafka", "fault": {"class": "near_miss_name", "detail": "`nmae` for `name`", "faulty_path": ["spec", "kafka", "listeners", 0, "nmae"], "original_path": ["spec", "kafka", "listeners", 0, "name"]}, "generator": "eval2_items_a1/3", "id": "a1-fm_near_a-002", "kind": "fix_manifest", "overlap": null, "overlap_band": "none", "perturbed_reference": "```yaml\napiVersion: kafka.strimzi.io/v1\nkind: Kafka\nmetadata:\n  name: data-platform\n  namespace: kafka\nspec:\n  kafka:\n    listeners:\n    - nmae: plain\n      port: 9094\n      type: loadbalancer\n      tls: true\n    config:\n      group.share.record.lock.duration.ms: 600000\n      group.share.max.heartbeat.interval.ms: 10000\n```", "perturbed_violates": 2, "preserves_unknown": [{"path": "spec.kafka.listeners[]", "preserves_unknown_fields": false}, {"path": "spec.kafka.config", "preserves_unknown_fields": true}], "question": "`kubectl apply` (or the Argo CD / Strimzi validation) rejects the manifest below. It has exactly one mistake. Return the corrected manifest in full as one YAML document and change nothing that is not wrong.\n\n```yaml\napiVersion: kafka.strimzi.io/v1\nkind: Kafka\nmetadata:\n  name: data-platform\n  namespace: kafka\nspec:\n  kafka:\n    listeners:\n    - nmae: plain\n      port: 9094\n      type: loadbalancer\n      tls: true\n    config:\n      group.share.record.lock.duration.ms: 600000\n      group.share.max.heartbeat.interval.ms: 10000\n```\n\nGive the corrected manifest in one fenced code block.", "reference": "```yaml\napiVersion: kafka.strimzi.io/v1\nkind: Kafka\nmetadata:\n  name: data-platform\n  namespace: kafka\nspec:\n  kafka:\n    listeners:\n    - name: plain\n      port: 9094\n      type: loadbalancer\n      tls: true\n    config:\n      group.share.record.lock.duration.ms: 600000\n      group.share.max.heartbeat.interval.ms: 10000\n```", "resource_kind": "Kafka", "seed": "m4-a1-1", "sources": [{"catalogue": "strimzi_resources", "entry": "Kafka spec.kafka.listeners[].name", "fact": "type string; required", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/040-Crd-kafka.yaml", "line": null}, {"catalogue": "strimzi_resources", "entry": "Kafka spec.kafka.listeners[].port", "fact": "type integer; required", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/040-Crd-kafka.yaml", "line": null}, {"catalogue": "strimzi_resources", "entry": "Kafka spec.kafka.listeners[].type", "fact": "type string; one of cluster-ip, ingress, internal, loadbalancer, nodeport, route, tlsroute; required", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/040-Crd-kafka.yaml", "line": null}, {"catalogue": "strimzi_resources", "entry": "Kafka spec.kafka.listeners[].tls", "fact": "type boolean; required", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/040-Crd-kafka.yaml", "line": null}, {"catalogue": "kafka_config", "entry": "broker/group.share.record.lock.duration.ms", "fact": "key exists in scope broker; type int; valid values [1000,...,3600000]", "file": "raw/apache-kafka-config/content/en/43/generated/kafka_config.html", "line": 1664}, {"catalogue": "kafka_config", "entry": "broker/group.share.max.heartbeat.interval.ms", "fact": "key exists in scope broker; type int; valid values [1,...]", "file": "raw/apache-kafka-config/content/en/43/generated/kafka_config.html", "line": 1521}, {"catalogue": "strimzi_resources", "entry": "Kafka spec.kafka.config", "fact": "free-form map (the schema keeps unknown keys); keys are checked against the broker configuration catalogue", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/040-Crd-kafka.yaml", "line": null}], "template": "fm_near_a", "tier": "A", "validator_codes": ["missing_required", "unknown_field"], "versions": {"kafka": "4.3", "strimzi": "1.3.1"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "checks": [{"format": true, "type": "yaml_document"}, {"api_version": "kafka.strimzi.io/v1", "kind": "KafkaConnector", "type": "yaml_one_doc"}, {"kind": "KafkaConnector", "type": "yaml_strict"}, {"fields": [{"path": ["apiVersion"], "value": "kafka.strimzi.io/v1"}, {"path": ["kind"], "value": "KafkaConnector"}, {"path": ["metadata", "name"], "value": "fraud-alerts-connector"}, {"path": ["metadata", "namespace"], "value": "data"}, {"path": ["metadata", "labels", "strimzi.io/cluster"], "value": "data-connect"}, {"path": ["spec", "class"], "value": "io.example.warehouse.WarehouseSinkConnector"}, {"path": ["spec", "tasksMax"], "value": 8}, {"path": ["spec", "state"], "value": "stopped"}, {"loose": true, "path": ["spec", "config", "errors.deadletterqueue.context.headers.enable"], "value": true}], "kind": "KafkaConnector", "type": "yaml_fields_equal"}, {"kind": "KafkaConnector", "paths": [["spec", "tasksMaxs"]], "type": "yaml_fields_absent"}, {"kind": "KafkaConnector", "path": ["spec", "config"], "scope": "sink_connector", "type": "yaml_config_keys"}], "dedup": {"max_containment": 0.647, "max_jaccard": 0.193, "nearest": "data/train.jsonl"}, "domain": "strimzi", "exposed": true, "failure_class": "near_miss_name", "family": "fm_near_a:KafkaConnector:class+config+state+tasksMax", "fault": {"class": "near_miss_name", "detail": "`tasksMaxs` for `tasksMax`", "faulty_path": ["spec", "tasksMaxs"], "original_path": ["spec", "tasksMax"]}, "generator": "eval2_items_a1/3", "id": "a1-fm_near_a-004", "kind": "fix_manifest", "overlap": null, "overlap_band": "none", "perturbed_reference": "```yaml\napiVersion: kafka.strimzi.io/v1\nkind: KafkaConnector\nmetadata:\n  name: fraud-alerts-connector\n  namespace: data\n  labels:\n    strimzi.io/cluster: data-connect\nspec:\n  class: io.example.warehouse.WarehouseSinkConnector\n  tasksMaxs: 8\n  state: stopped\n  config:\n    errors.deadletterqueue.context.headers.enable: true\n```", "perturbed_violates": 2, "preserves_unknown": [{"path": "spec", "preserves_unknown_fields": false}, {"path": "spec.config", "preserves_unknown_fields": true}], "question": "`kubectl apply` (or the Argo CD / Strimzi validation) rejects the manifest below. It has exactly one mistake. Return the corrected manifest in full as one YAML document and change nothing that is not wrong.\n\n```yaml\napiVersion: kafka.strimzi.io/v1\nkind: KafkaConnector\nmetadata:\n  name: fraud-alerts-connector\n  namespace: data\n  labels:\n    strimzi.io/cluster: data-connect\nspec:\n  class: io.example.warehouse.WarehouseSinkConnector\n  tasksMaxs: 8\n  state: stopped\n  config:\n    errors.deadletterqueue.context.headers.enable: true\n```\n\nGive the corrected manifest in one fenced code block.", "reference": "```yaml\napiVersion: kafka.strimzi.io/v1\nkind: KafkaConnector\nmetadata:\n  name: fraud-alerts-connector\n  namespace: data\n  labels:\n    strimzi.io/cluster: data-connect\nspec:\n  class: io.example.warehouse.WarehouseSinkConnector\n  tasksMax: 8\n  state: stopped\n  config:\n    errors.deadletterqueue.context.headers.enable: true\n```", "resource_kind": "KafkaConnector", "seed": "m4-a1-1", "sources": [{"catalogue": "strimzi_annotations", "entry": "strimzi.io/cluster", "fact": "the label name is in the Strimzi documentation", "file": "raw/strimzi/documentation/api/io.strimzi.api.kafka.model.common.EnvironmentVariableRack.adoc", "line": null}, {"catalogue": "strimzi_resources", "entry": "KafkaConnector spec.class", "fact": "type string", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/047-Crd-kafkaconnector.yaml", "line": null}, {"catalogue": "strimzi_resources", "entry": "KafkaConnector spec.tasksMax", "fact": "type integer", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/047-Crd-kafkaconnector.yaml", "line": null}, {"catalogue": "strimzi_resources", "entry": "KafkaConnector spec.state", "fact": "type string; one of paused, running, stopped", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/047-Crd-kafkaconnector.yaml", "line": null}, {"catalogue": "kafka_config", "entry": "sink_connector/errors.deadletterqueue.context.headers.enable", "fact": "key exists in scope sink_connector; type boolean", "file": "raw/apache-kafka-config/content/en/43/generated/sink_connector_config.html", "line": 233}, {"catalogue": "strimzi_resources", "entry": "KafkaConnector spec.config", "fact": "free-form map (the schema keeps unknown keys); keys are checked against the sink_connector configuration catalogue", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/047-Crd-kafkaconnector.yaml", "line": null}], "template": "fm_near_a", "tier": "A", "validator_codes": ["unknown_field"], "versions": {"kafka": "4.3", "strimzi": "1.3.1"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "checks": [{"format": true, "type": "yaml_document"}, {"api_version": "kafka.strimzi.io/v1", "kind": "KafkaNodePool", "type": "yaml_one_doc"}, {"kind": "KafkaNodePool", "type": "yaml_strict"}, {"fields": [{"path": ["apiVersion"], "value": "kafka.strimzi.io/v1"}, {"path": ["kind"], "value": "KafkaNodePool"}, {"path": ["metadata", "name"], "value": "controllers"}, {"path": ["metadata", "namespace"], "value": "prod"}, {"path": ["metadata", "labels", "strimzi.io/cluster"], "value": "data-platform"}, {"path": ["spec", "roles"], "value": ["broker", "controller"]}, {"path": ["spec", "replicas"], "value": 5}, {"path": ["spec", "storage", "type"], "value": "persistent-claim"}, {"path": ["spec", "storage", "size"], "value": "500Gi"}, {"path": ["spec", "storage", "deleteClaim"], "value": false}, {"path": ["spec", "jvmOptions", "-Xmx"], "value": "4g"}], "kind": "KafkaNodePool", "type": "yaml_fields_equal"}, {"kind": "KafkaNodePool", "paths": [["spec", "Replicas"]], "type": "yaml_fields_absent"}], "dedup": {"max_containment": 0.629, "max_jaccard": 0.344, "nearest": "data/train.jsonl"}, "domain": "strimzi", "exposed": true, "failure_class": "near_miss_name", "family": "fm_near_a:KafkaNodePool:jvmOptions+replicas+roles+storage", "fault": {"class": "near_miss_name", "detail": "`Replicas` for `replicas`", "faulty_path": ["spec", "Replicas"], "original_path": ["spec", "replicas"]}, "generator": "eval2_items_a1/3", "id": "a1-fm_near_a-007", "kind": "fix_manifest", "overlap": null, "overlap_band": "none", "perturbed_reference": "```yaml\napiVersion: kafka.strimzi.io/v1\nkind: KafkaNodePool\nmetadata:\n  name: controllers\n  namespace: prod\n  labels:\n    strimzi.io/cluster: data-platform\nspec:\n  roles:\n  - broker\n  - controller\n  Replicas: 5\n  storage:\n    type: persistent-claim\n    size: 500Gi\n    deleteClaim: false\n  jvmOptions:\n    -Xmx: 4g\n```", "perturbed_violates": 2, "preserves_unknown": [{"path": "spec", "preserves_unknown_fields": false}, {"path": "spec.storage", "preserves_unknown_fields": false}, {"path": "spec.jvmOptions", "preserves_unknown_fields": false}], "question": "`kubectl apply` (or the Argo CD / Strimzi validation) rejects the manifest below. It has exactly one mistake. Return the corrected manifest in full as one YAML document and change nothing that is not wrong.\n\n```yaml\napiVersion: kafka.strimzi.io/v1\nkind: KafkaNodePool\nmetadata:\n  name: controllers\n  namespace: prod\n  labels:\n    strimzi.io/cluster: data-platform\nspec:\n  roles:\n  - broker\n  - controller\n  Replicas: 5\n  storage:\n    type: persistent-claim\n    size: 500Gi\n    deleteClaim: false\n  jvmOptions:\n    -Xmx: 4g\n```\n\nGive the corrected manifest in one fenced code block.", "reference": "```yaml\napiVersion: kafka.strimzi.io/v1\nkind: KafkaNodePool\nmetadata:\n  name: controllers\n  namespace: prod\n  labels:\n    strimzi.io/cluster: data-platform\nspec:\n  roles:\n  - broker\n  - controller\n  replicas: 5\n  storage:\n    type: persistent-claim\n    size: 500Gi\n    deleteClaim: false\n  jvmOptions:\n    -Xmx: 4g\n```", "resource_kind": "KafkaNodePool", "seed": "m4-a1-1", "sources": [{"catalogue": "strimzi_annotations", "entry": "strimzi.io/cluster", "fact": "the label name is in the Strimzi documentation", "file": "raw/strimzi/documentation/api/io.strimzi.api.kafka.model.common.EnvironmentVariableRack.adoc", "line": null}, {"catalogue": "strimzi_resources", "entry": "KafkaNodePool spec.roles", "fact": "type array; required", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/045-Crd-kafkanodepool.yaml", "line": null}, {"catalogue": "strimzi_resources", "entry": "KafkaNodePool spec.replicas", "fact": "type integer; required", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/045-Crd-kafkanodepool.yaml", "line": null}, {"catalogue": "strimzi_resources", "entry": "KafkaNodePool spec.storage.type", "fact": "type string; one of ephemeral, jbod, persistent-claim; required", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/045-Crd-kafkanodepool.yaml", "line": null}, {"catalogue": "strimzi_resources", "entry": "KafkaNodePool spec.storage.size", "fact": "type string", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/045-Crd-kafkanodepool.yaml", "line": null}, {"catalogue": "strimzi_resources", "entry": "KafkaNodePool spec.storage.deleteClaim", "fact": "type boolean", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/045-Crd-kafkanodepool.yaml", "line": null}, {"catalogue": "strimzi_resources", "entry": "KafkaNodePool spec.jvmOptions.-Xmx", "fact": "type string", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/045-Crd-kafkanodepool.yaml", "line": null}], "template": "fm_near_a", "tier": "A", "validator_codes": ["missing_required", "unknown_field"], "versions": {"kafka": "4.3", "strimzi": "1.3.1"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "checks": [{"format": true, "type": "yaml_document"}, {"api_version": "networking.k8s.io/v1", "kind": "NetworkPolicy", "type": "yaml_one_doc"}, {"kind": "NetworkPolicy", "type": "yaml_strict"}, {"fields": [{"path": ["apiVersion"], "value": "networking.k8s.io/v1"}, {"path": ["kind"], "value": "NetworkPolicy"}, {"path": ["metadata", "name"], "value": "restrict-brokers"}, {"path": ["metadata", "namespace"], "value": "streaming"}, {"path": ["spec", "podSelector", "matchLabels", "app"], "value": "kafka"}, {"path": ["spec", "policyTypes"], "value": ["Ingress"]}, {"path": ["spec", "ingress", 0, "from", 0, "podSelector", "matchLabels", "app"], "value": "billing"}, {"path": ["spec", "ingress", 0, "ports", 0, "port"], "value": 8083}, {"path": ["spec", "ingress", 0, "ports", 0, "protocol"], "value": "TCP"}], "kind": "NetworkPolicy", "type": "yaml_fields_equal"}, {"kind": "NetworkPolicy", "paths": [["spec", "policyType"]], "type": "yaml_fields_absent"}], "dedup": {"max_containment": 0.471, "max_jaccard": 0.157, "nearest": "data/train.jsonl"}, "domain": "kubernetes", "exposed": true, "failure_class": "near_miss_name", "family": "fm_near_b:NetworkPolicy:ingress+podSelector+policyTypes", "fault": {"class": "near_miss_name", "detail": "`policyType` for `policyTypes`", "faulty_path": ["spec", "policyType"], "original_path": ["spec", "policyTypes"]}, "generator": "eval2_items_a1/3", "id": "a1-fm_near_b-001", "kind": "fix_manifest", "overlap": null, "overlap_band": "none", "perturbed_reference": "```yaml\napiVersion: networking.k8s.io/v1\nkind: NetworkPolicy\nmetadata:\n  name: restrict-brokers\n  namespace: streaming\nspec:\n  podSelector:\n    matchLabels:\n      app: kafka\n  policyType:\n  - Ingress\n  ingress:\n  - from:\n    - podSelector:\n        matchLabels:\n          app: billing\n    ports:\n    - port: 8083\n      protocol: TCP\n```", "perturbed_violates": 2, "preserves_unknown": [{"path": "spec.podSelector.matchLabels", "preserves_unknown_fields": true}, {"path": "spec", "preserves_unknown_fields": false}, {"path": "spec.ingress[].from[].podSelector.matchLabels", "preserves_unknown_fields": true}, {"path": "spec.ingress[].ports[]", "preserves_unknown_fields": false}], "question": "A reviewer says this manifest has exactly one error. Fix it and give the complete corrected manifest (one YAML document); every other value must stay as it is.\n\n```yaml\napiVersion: networking.k8s.io/v1\nkind: NetworkPolicy\nmetadata:\n  name: restrict-brokers\n  namespace: streaming\nspec:\n  podSelector:\n    matchLabels:\n      app: kafka\n  policyType:\n  - Ingress\n  ingress:\n  - from:\n    - podSelector:\n        matchLabels:\n          app: billing\n    ports:\n    - port: 8083\n      protocol: TCP\n```\n\nGive the corrected manifest in one fenced code block.", "reference": "```yaml\napiVersion: networking.k8s.io/v1\nkind: NetworkPolicy\nmetadata:\n  name: restrict-brokers\n  namespace: streaming\nspec:\n  podSelector:\n    matchLabels:\n      app: kafka\n  policyTypes:\n  - Ingress\n  ingress:\n  - from:\n    - podSelector:\n        matchLabels:\n          app: billing\n    ports:\n    - port: 8083\n      protocol: TCP\n```", "resource_kind": "NetworkPolicy", "seed": "m4-a1-1", "sources": [{"catalogue": "kubernetes", "entry": "NetworkPolicy spec.podSelector.matchLabels", "fact": "type object", "file": "raw_catalogue/kubernetes-api/api/openapi-spec/swagger.json", "line": null}, {"catalogue": "kubernetes", "entry": "NetworkPolicy spec.policyTypes", "fact": "type array", "file": "raw_catalogue/kubernetes-api/api/openapi-spec/swagger.json", "line": null}, {"catalogue": "kubernetes", "entry": "NetworkPolicy spec.ingress[].from[].podSelector.matchLabels", "fact": "type object", "file": "raw_catalogue/kubernetes-api/api/openapi-spec/swagger.json", "line": null}, {"catalogue": "kubernetes", "entry": "NetworkPolicy spec.ingress[].ports[].port", "fact": "type integer/string", "file": "raw_catalogue/kubernetes-api/api/openapi-spec/swagger.json", "line": null}, {"catalogue": "kubernetes", "entry": "NetworkPolicy spec.ingress[].ports[].protocol", "fact": "type string", "file": "raw_catalogue/kubernetes-api/api/openapi-spec/swagger.json", "line": null}], "template": "fm_near_b", "tier": "A", "validator_codes": ["unknown_field"], "versions": {"kubernetes": "v1.37.2"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "checks": [{"format": true, "type": "yaml_document"}, {"api_version": "policy/v1", "kind": "PodDisruptionBudget", "type": "yaml_one_doc"}, {"kind": "PodDisruptionBudget", "type": "yaml_strict"}, {"fields": [{"path": ["apiVersion"], "value": "policy/v1"}, {"path": ["kind"], "value": "PodDisruptionBudget"}, {"path": ["metadata", "name"], "value": "kafka-pdb"}, {"path": ["metadata", "namespace"], "value": "prod"}, {"path": ["spec", "selector", "matchLabels", "app"], "value": "kafka"}, {"path": ["spec", "selector", "matchLabels", "component"], "value": "broker"}, {"path": ["spec", "maxUnavailable"], "value": 2}, {"path": ["spec", "unhealthyPodEvictionPolicy"], "value": "IfHealthyBudget"}], "kind": "PodDisruptionBudget", "type": "yaml_fields_equal"}, {"kind": "PodDisruptionBudget", "paths": [["spec", "maxunavailable"]], "type": "yaml_fields_absent"}], "dedup": {"max_containment": 0.412, "max_jaccard": 0.152, "nearest": "data/train.jsonl"}, "domain": "kubernetes", "exposed": true, "failure_class": "near_miss_name", "family": "fm_near_b:PodDisruptionBudget:maxUnavailable+selector+unhealthyPodEvictionPolicy", "fault": {"class": "near_miss_name", "detail": "`maxunavailable` for `maxUnavailable`", "faulty_path": ["spec", "maxunavailable"], "original_path": ["spec", "maxUnavailable"]}, "generator": "eval2_items_a1/3", "id": "a1-fm_near_b-006", "kind": "fix_manifest", "overlap": null, "overlap_band": "none", "perturbed_reference": "```yaml\napiVersion: policy/v1\nkind: PodDisruptionBudget\nmetadata:\n  name: kafka-pdb\n  namespace: prod\nspec:\n  selector:\n    matchLabels:\n      app: kafka\n      component: broker\n  maxunavailable: 2\n  unhealthyPodEvictionPolicy: IfHealthyBudget\n```", "perturbed_violates": 2, "preserves_unknown": [{"path": "spec.selector.matchLabels", "preserves_unknown_fields": true}, {"path": "spec", "preserves_unknown_fields": false}], "question": "A reviewer says this manifest has exactly one error. Fix it and give the complete corrected manifest (one YAML document); every other value must stay as it is.\n\n```yaml\napiVersion: policy/v1\nkind: PodDisruptionBudget\nmetadata:\n  name: kafka-pdb\n  namespace: prod\nspec:\n  selector:\n    matchLabels:\n      app: kafka\n      component: broker\n  maxunavailable: 2\n  unhealthyPodEvictionPolicy: IfHealthyBudget\n```\n\nGive the corrected manifest in one fenced code block.", "reference": "```yaml\napiVersion: policy/v1\nkind: PodDisruptionBudget\nmetadata:\n  name: kafka-pdb\n  namespace: prod\nspec:\n  selector:\n    matchLabels:\n      app: kafka\n      component: broker\n  maxUnavailable: 2\n  unhealthyPodEvictionPolicy: IfHealthyBudget\n```", "resource_kind": "PodDisruptionBudget", "seed": "m4-a1-1", "sources": [{"catalogue": "kubernetes", "entry": "PodDisruptionBudget spec.selector.matchLabels", "fact": "type object", "file": "raw_catalogue/kubernetes-api/api/openapi-spec/swagger.json", "line": null}, {"catalogue": "kubernetes", "entry": "PodDisruptionBudget spec.maxUnavailable", "fact": "type integer/string", "file": "raw_catalogue/kubernetes-api/api/openapi-spec/swagger.json", "line": null}, {"catalogue": "kubernetes", "entry": "PodDisruptionBudget spec.unhealthyPodEvictionPolicy", "fact": "type string", "file": "raw_catalogue/kubernetes-api/api/openapi-spec/swagger.json", "line": null}], "template": "fm_near_b", "tier": "A", "validator_codes": ["unknown_field"], "versions": {"kubernetes": "v1.37.2"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "checks": [{"format": true, "type": "yaml_document"}, {"api_version": "networking.k8s.io/v1", "kind": "NetworkPolicy", "type": "yaml_one_doc"}, {"kind": "NetworkPolicy", "type": "yaml_strict"}, {"fields": [{"path": ["apiVersion"], "value": "networking.k8s.io/v1"}, {"path": ["kind"], "value": "NetworkPolicy"}, {"path": ["metadata", "name"], "value": "restrict-brokers"}, {"path": ["metadata", "namespace"], "value": "prod"}, {"path": ["spec", "podSelector", "matchLabels", "app"], "value": "schema-registry"}, {"path": ["spec", "policyTypes"], "value": ["Ingress"]}, {"path": ["spec", "ingress", 0, "from", 0, "podSelector", "matchLabels", "app"], "value": "billing"}, {"path": ["spec", "ingress", 0, "ports", 0, "port"], "value": 9093}, {"path": ["spec", "ingress", 0, "ports", 0, "protocol"], "value": "TCP"}], "kind": "NetworkPolicy", "type": "yaml_fields_equal"}, {"kind": "NetworkPolicy", "paths": [["spec", "PolicyTypes"]], "type": "yaml_fields_absent"}], "dedup": {"max_containment": 0.412, "max_jaccard": 0.132, "nearest": "data/train.jsonl"}, "domain": "kubernetes", "exposed": true, "failure_class": "near_miss_name", "family": "fm_near_c:NetworkPolicy:ingress+podSelector+policyTypes", "fault": {"class": "near_miss_name", "detail": "`PolicyTypes` for `policyTypes`", "faulty_path": ["spec", "PolicyTypes"], "original_path": ["spec", "policyTypes"]}, "generator": "eval2_items_a1/3", "id": "a1-fm_near_c-003", "kind": "fix_manifest", "overlap": null, "overlap_band": "none", "perturbed_reference": "```yaml\napiVersion: networking.k8s.io/v1\nkind: NetworkPolicy\nmetadata:\n  name: restrict-brokers\n  namespace: prod\nspec:\n  podSelector:\n    matchLabels:\n      app: schema-registry\n  PolicyTypes:\n  - Ingress\n  ingress:\n  - from:\n    - podSelector:\n        matchLabels:\n          app: billing\n    ports:\n    - port: 9093\n      protocol: TCP\n```", "perturbed_violates": 2, "preserves_unknown": [{"path": "spec.podSelector.matchLabels", "preserves_unknown_fields": true}, {"path": "spec", "preserves_unknown_fields": false}, {"path": "spec.ingress[].from[].podSelector.matchLabels", "preserves_unknown_fields": true}, {"path": "spec.ingress[].ports[]", "preserves_unknown_fields": false}], "question": "This manifest fails validation against the schema of its kind, and the cause is a single mistake. Return the whole manifest with that mistake corrected and everything else unchanged (one YAML document).\n\n```yaml\napiVersion: networking.k8s.io/v1\nkind: NetworkPolicy\nmetadata:\n  name: restrict-brokers\n  namespace: prod\nspec:\n  podSelector:\n    matchLabels:\n      app: schema-registry\n  PolicyTypes:\n  - Ingress\n  ingress:\n  - from:\n    - podSelector:\n        matchLabels:\n          app: billing\n    ports:\n    - port: 9093\n      protocol: TCP\n```\n\nGive the corrected manifest in one fenced code block.", "reference": "```yaml\napiVersion: networking.k8s.io/v1\nkind: NetworkPolicy\nmetadata:\n  name: restrict-brokers\n  namespace: prod\nspec:\n  podSelector:\n    matchLabels:\n      app: schema-registry\n  policyTypes:\n  - Ingress\n  ingress:\n  - from:\n    - podSelector:\n        matchLabels:\n          app: billing\n    ports:\n    - port: 9093\n      protocol: TCP\n```", "resource_kind": "NetworkPolicy", "seed": "m4-a1-1", "sources": [{"catalogue": "kubernetes", "entry": "NetworkPolicy spec.podSelector.matchLabels", "fact": "type object", "file": "raw_catalogue/kubernetes-api/api/openapi-spec/swagger.json", "line": null}, {"catalogue": "kubernetes", "entry": "NetworkPolicy spec.policyTypes", "fact": "type array", "file": "raw_catalogue/kubernetes-api/api/openapi-spec/swagger.json", "line": null}, {"catalogue": "kubernetes", "entry": "NetworkPolicy spec.ingress[].from[].podSelector.matchLabels", "fact": "type object", "file": "raw_catalogue/kubernetes-api/api/openapi-spec/swagger.json", "line": null}, {"catalogue": "kubernetes", "entry": "NetworkPolicy spec.ingress[].ports[].port", "fact": "type integer/string", "file": "raw_catalogue/kubernetes-api/api/openapi-spec/swagger.json", "line": null}, {"catalogue": "kubernetes", "entry": "NetworkPolicy spec.ingress[].ports[].protocol", "fact": "type string", "file": "raw_catalogue/kubernetes-api/api/openapi-spec/swagger.json", "line": null}], "template": "fm_near_c", "tier": "A", "validator_codes": ["unknown_field"], "versions": {"kubernetes": "v1.37.2"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "checks": [{"format": true, "type": "yaml_document"}, {"api_version": "policy/v1", "kind": "PodDisruptionBudget", "type": "yaml_one_doc"}, {"kind": "PodDisruptionBudget", "type": "yaml_strict"}, {"fields": [{"path": ["apiVersion"], "value": "policy/v1"}, {"path": ["kind"], "value": "PodDisruptionBudget"}, {"path": ["metadata", "name"], "value": "connect-pdb"}, {"path": ["metadata", "namespace"], "value": "kafka"}, {"path": ["spec", "selector", "matchLabels", "app"], "value": "kafka-connect"}, {"path": ["spec", "selector", "matchLabels", "component"], "value": "controller"}, {"path": ["spec", "maxUnavailable"], "value": 1}, {"path": ["spec", "unhealthyPodEvictionPolicy"], "value": "AlwaysAllow"}], "kind": "PodDisruptionBudget", "type": "yaml_fields_equal"}, {"kind": "PodDisruptionBudget", "paths": [["spec", "maxunavailable"]], "type": "yaml_fields_absent"}], "dedup": {"max_containment": 0.412, "max_jaccard": 0.149, "nearest": "data/train.jsonl"}, "domain": "kubernetes", "exposed": true, "failure_class": "near_miss_name", "family": "fm_near_c:PodDisruptionBudget:maxUnavailable+selector+unhealthyPodEvictionPolicy", "fault": {"class": "near_miss_name", "detail": "`maxunavailable` for `maxUnavailable`", "faulty_path": ["spec", "maxunavailable"], "original_path": ["spec", "maxUnavailable"]}, "generator": "eval2_items_a1/3", "id": "a1-fm_near_c-005", "kind": "fix_manifest", "overlap": null, "overlap_band": "none", "perturbed_reference": "```yaml\napiVersion: policy/v1\nkind: PodDisruptionBudget\nmetadata:\n  name: connect-pdb\n  namespace: kafka\nspec:\n  selector:\n    matchLabels:\n      app: kafka-connect\n      component: controller\n  maxunavailable: 1\n  unhealthyPodEvictionPolicy: AlwaysAllow\n```", "perturbed_violates": 2, "preserves_unknown": [{"path": "spec.selector.matchLabels", "preserves_unknown_fields": true}, {"path": "spec", "preserves_unknown_fields": false}], "question": "This manifest fails validation against the schema of its kind, and the cause is a single mistake. Return the whole manifest with that mistake corrected and everything else unchanged (one YAML document).\n\n```yaml\napiVersion: policy/v1\nkind: PodDisruptionBudget\nmetadata:\n  name: connect-pdb\n  namespace: kafka\nspec:\n  selector:\n    matchLabels:\n      app: kafka-connect\n      component: controller\n  maxunavailable: 1\n  unhealthyPodEvictionPolicy: AlwaysAllow\n```\n\nGive the corrected manifest in one fenced code block.", "reference": "```yaml\napiVersion: policy/v1\nkind: PodDisruptionBudget\nmetadata:\n  name: connect-pdb\n  namespace: kafka\nspec:\n  selector:\n    matchLabels:\n      app: kafka-connect\n      component: controller\n  maxUnavailable: 1\n  unhealthyPodEvictionPolicy: AlwaysAllow\n```", "resource_kind": "PodDisruptionBudget", "seed": "m4-a1-1", "sources": [{"catalogue": "kubernetes", "entry": "PodDisruptionBudget spec.selector.matchLabels", "fact": "type object", "file": "raw_catalogue/kubernetes-api/api/openapi-spec/swagger.json", "line": null}, {"catalogue": "kubernetes", "entry": "PodDisruptionBudget spec.maxUnavailable", "fact": "type integer/string", "file": "raw_catalogue/kubernetes-api/api/openapi-spec/swagger.json", "line": null}, {"catalogue": "kubernetes", "entry": "PodDisruptionBudget spec.unhealthyPodEvictionPolicy", "fact": "type string", "file": "raw_catalogue/kubernetes-api/api/openapi-spec/swagger.json", "line": null}], "template": "fm_near_c", "tier": "A", "validator_codes": ["unknown_field"], "versions": {"kubernetes": "v1.37.2"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "checks": [{"format": true, "type": "yaml_document"}, {"api_version": "policy/v1", "kind": "PodDisruptionBudget", "type": "yaml_one_doc"}, {"kind": "PodDisruptionBudget", "type": "yaml_strict"}, {"fields": [{"path": ["apiVersion"], "value": "policy/v1"}, {"path": ["kind"], "value": "PodDisruptionBudget"}, {"path": ["metadata", "name"], "value": "broker-budget"}, {"path": ["metadata", "namespace"], "value": "prod"}, {"path": ["spec", "selector", "matchLabels", "app"], "value": "kafka"}, {"path": ["spec", "selector", "matchLabels", "component"], "value": "worker"}, {"path": ["spec", "minAvailable"], "value": 2}, {"path": ["spec", "unhealthyPodEvictionPolicy"], "value": "AlwaysAllow"}], "kind": "PodDisruptionBudget", "type": "yaml_fields_equal"}, {"kind": "PodDisruptionBudget", "paths": [["spec", "selector", "minAvailable"]], "type": "yaml_fields_absent"}], "dedup": {"max_containment": 0.412, "max_jaccard": 0.149, "nearest": "data/train.jsonl"}, "domain": "kubernetes", "exposed": true, "failure_class": "wrong_nesting", "family": "fm_nest_b:PodDisruptionBudget:minAvailable+selector+unhealthyPodEvictionPolicy", "fault": {"class": "wrong_nesting", "detail": "`spec.minAvailable` moved down, to `spec.selector.minAvailable`", "faulty_path": ["spec", "selector", "minAvailable"], "original_path": ["spec", "minAvailable"]}, "generator": "eval2_items_a1/3", "id": "a1-fm_nest_b-001", "kind": "fix_manifest", "overlap": null, "overlap_band": "none", "perturbed_reference": "```yaml\napiVersion: policy/v1\nkind: PodDisruptionBudget\nmetadata:\n  name: broker-budget\n  namespace: prod\nspec:\n  selector:\n    matchLabels:\n      app: kafka\n      component: worker\n    minAvailable: 2\n  unhealthyPodEvictionPolicy: AlwaysAllow\n```", "perturbed_violates": 2, "preserves_unknown": [{"path": "spec.selector.matchLabels", "preserves_unknown_fields": true}, {"path": "spec", "preserves_unknown_fields": false}], "question": "A reviewer says this manifest has exactly one error. Fix it and give the complete corrected manifest (one YAML document); every other value must stay as it is.\n\n```yaml\napiVersion: policy/v1\nkind: PodDisruptionBudget\nmetadata:\n  name: broker-budget\n  namespace: prod\nspec:\n  selector:\n    matchLabels:\n      app: kafka\n      component: worker\n    minAvailable: 2\n  unhealthyPodEvictionPolicy: AlwaysAllow\n```\n\nGive the corrected manifest in one fenced code block.", "reference": "```yaml\napiVersion: policy/v1\nkind: PodDisruptionBudget\nmetadata:\n  name: broker-budget\n  namespace: prod\nspec:\n  selector:\n    matchLabels:\n      app: kafka\n      component: worker\n  minAvailable: 2\n  unhealthyPodEvictionPolicy: AlwaysAllow\n```", "resource_kind": "PodDisruptionBudget", "seed": "m4-a1-1", "sources": [{"catalogue": "kubernetes", "entry": "PodDisruptionBudget spec.selector.matchLabels", "fact": "type object", "file": "raw_catalogue/kubernetes-api/api/openapi-spec/swagger.json", "line": null}, {"catalogue": "kubernetes", "entry": "PodDisruptionBudget spec.minAvailable", "fact": "type integer/string", "file": "raw_catalogue/kubernetes-api/api/openapi-spec/swagger.json", "line": null}, {"catalogue": "kubernetes", "entry": "PodDisruptionBudget spec.unhealthyPodEvictionPolicy", "fact": "type string", "file": "raw_catalogue/kubernetes-api/api/openapi-spec/swagger.json", "line": null}], "template": "fm_nest_b", "tier": "A", "validator_codes": ["unknown_field"], "versions": {"kubernetes": "v1.37.2"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "checks": [{"format": true, "type": "yaml_document"}, {"api_version": "kafka.strimzi.io/v1", "kind": "KafkaUser", "type": "yaml_one_doc"}, {"kind": "KafkaUser", "type": "yaml_strict"}, {"fields": [{"path": ["apiVersion"], "value": "kafka.strimzi.io/v1"}, {"path": ["kind"], "value": "KafkaUser"}, {"path": ["metadata", "name"], "value": "clickstream-app"}, {"path": ["metadata", "namespace"], "value": "kafka"}, {"path": ["metadata", "labels", "strimzi.io/cluster"], "value": "prod-cluster"}, {"path": ["spec", "authentication", "type"], "value": "scram-sha-512"}, {"path": ["spec", "quotas", "requestPercentage"], "value": 25}, {"path": ["spec", "authorization", "type"], "value": "simple"}, {"path": ["spec", "authorization", "acls", 0, "resource", "type"], "value": "group"}, {"path": ["spec", "authorization", "acls", 0, "resource", "name"], "value": "fraud-alerts"}, {"path": ["spec", "authorization", "acls", 0, "resource", "patternType"], "value": "literal"}, {"path": ["spec", "authorization", "acls", 0, "operations"], "value": ["AlterConfigs", "Read"]}], "kind": "KafkaUser", "type": "yaml_fields_equal"}, {"kind": "KafkaUser", "paths": [["spec", "authorization", "acls", 0, "name"]], "type": "yaml_fields_absent"}], "dedup": {"max_containment": 0.727, "max_jaccard": 0.145, "nearest": "data/train.jsonl"}, "domain": "strimzi", "exposed": true, "failure_class": "wrong_nesting", "family": "fm_nest_b:KafkaUser:authentication+authorization+quotas", "fault": {"class": "wrong_nesting", "detail": "`spec.authorization.acls[0].resource.name` moved up, to `spec.authorization.acls[0].name`", "faulty_path": ["spec", "authorization", "acls", 0, "name"], "original_path": ["spec", "authorization", "acls", 0, "resource", "name"]}, "generator": "eval2_items_a1/3", "id": "a1-fm_nest_b-003", "kind": "fix_manifest", "overlap": null, "overlap_band": "none", "perturbed_reference": "```yaml\napiVersion: kafka.strimzi.io/v1\nkind: KafkaUser\nmetadata:\n  name: clickstream-app\n  namespace: kafka\n  labels:\n    strimzi.io/cluster: prod-cluster\nspec:\n  authentication:\n    type: scram-sha-512\n  quotas:\n    requestPercentage: 25\n  authorization:\n    type: simple\n    acls:\n    - resource:\n        type: group\n        patternType: literal\n      operations:\n      - AlterConfigs\n      - Read\n      name: fraud-alerts\n```", "perturbed_violates": 2, "preserves_unknown": [{"path": "spec.authentication", "preserves_unknown_fields": false}, {"path": "spec.quotas", "preserves_unknown_fields": false}, {"path": "spec.authorization", "preserves_unknown_fields": false}, {"path": "spec.authorization.acls[].resource", "preserves_unknown_fields": false}, {"path": "spec.authorization.acls[]", "preserves_unknown_fields": false}], "question": "A reviewer says this manifest has exactly one error. Fix it and give the complete corrected manifest (one YAML document); every other value must stay as it is.\n\n```yaml\napiVersion: kafka.strimzi.io/v1\nkind: KafkaUser\nmetadata:\n  name: clickstream-app\n  namespace: kafka\n  labels:\n    strimzi.io/cluster: prod-cluster\nspec:\n  authentication:\n    type: scram-sha-512\n  quotas:\n    requestPercentage: 25\n  authorization:\n    type: simple\n    acls:\n    - resource:\n        type: group\n        patternType: literal\n      operations:\n      - AlterConfigs\n      - Read\n      name: fraud-alerts\n```\n\nGive the corrected manifest in one fenced code block.", "reference": "```yaml\napiVersion: kafka.strimzi.io/v1\nkind: KafkaUser\nmetadata:\n  name: clickstream-app\n  namespace: kafka\n  labels:\n    strimzi.io/cluster: prod-cluster\nspec:\n  authentication:\n    type: scram-sha-512\n  quotas:\n    requestPercentage: 25\n  authorization:\n    type: simple\n    acls:\n    - resource:\n        type: group\n        name: fraud-alerts\n        patternType: literal\n      operations:\n      - AlterConfigs\n      - Read\n```", "resource_kind": "KafkaUser", "seed": "m4-a1-1", "sources": [{"catalogue": "strimzi_annotations", "entry": "strimzi.io/cluster", "fact": "the label name is in the Strimzi documentation", "file": "raw/strimzi/documentation/api/io.strimzi.api.kafka.model.common.EnvironmentVariableRack.adoc", "line": null}, {"catalogue": "strimzi_resources", "entry": "KafkaUser spec.authentication.type", "fact": "type string; one of scram-sha-512, tls, tls-external; required", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/044-Crd-kafkauser.yaml", "line": null}, {"catalogue": "strimzi_resources", "entry": "KafkaUser spec.quotas.requestPercentage", "fact": "type integer", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/044-Crd-kafkauser.yaml", "line": null}, {"catalogue": "strimzi_resources", "entry": "KafkaUser spec.authorization.type", "fact": "type string; one of simple; required", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/044-Crd-kafkauser.yaml", "line": null}, {"catalogue": "strimzi_resources", "entry": "KafkaUser spec.authorization.acls[].resource.type", "fact": "type string; one of cluster, group, topic, transactionalId; required", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/044-Crd-kafkauser.yaml", "line": null}, {"catalogue": "strimzi_resources", "entry": "KafkaUser spec.authorization.acls[].resource.name", "fact": "type string", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/044-Crd-kafkauser.yaml", "line": null}, {"catalogue": "strimzi_resources", "entry": "KafkaUser spec.authorization.acls[].resource.patternType", "fact": "type string; one of literal, prefix", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/044-Crd-kafkauser.yaml", "line": null}, {"catalogue": "strimzi_resources", "entry": "KafkaUser spec.authorization.acls[].operations", "fact": "type array; required", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/044-Crd-kafkauser.yaml", "line": null}], "template": "fm_nest_b", "tier": "A", "validator_codes": ["unknown_field"], "versions": {"kafka": "4.3", "strimzi": "1.3.1"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "checks": [{"format": true, "type": "yaml_document"}, {"api_version": "kafka.strimzi.io/v1", "kind": "Kafka", "type": "yaml_one_doc"}, {"kind": "Kafka", "type": "yaml_strict"}, {"fields": [{"path": ["apiVersion"], "value": "kafka.strimzi.io/v1"}, {"path": ["kind"], "value": "Kafka"}, {"path": ["metadata", "name"], "value": "events-kafka"}, {"path": ["metadata", "namespace"], "value": "streaming"}, {"path": ["spec", "kafka", "listeners", 0, "name"], "value": "plain"}, {"path": ["spec", "kafka", "listeners", 0, "port"], "value": 9092}, {"path": ["spec", "kafka", "listeners", 0, "type"], "value": "loadbalancer"}, {"path": ["spec", "kafka", "listeners", 0, "tls"], "value": true}, {"loose": true, "path": ["spec", "kafka", "config", "log.index.interval.bytes"], "value": 10485760}, {"loose": true, "path": ["spec", "kafka", "config", "group.streams.max.size"], "value": 10}, {"loose": true, "path": ["spec", "kafka", "config", "num.replica.alter.log.dirs.threads"], "value": 100}], "kind": "Kafka", "type": "yaml_fields_equal"}, {"kind": "Kafka", "paths": [["spec", "kafka", "config", "index.interval.bytes"]], "type": "yaml_fields_absent"}, {"kind": "Kafka", "path": ["spec", "kafka", "config"], "scope": "broker", "type": "yaml_config_keys"}], "dedup": {"max_containment": 0.75, "max_jaccard": 0.103, "nearest": "data/train.jsonl"}, "domain": "strimzi", "exposed": true, "failure_class": "wrong_scope", "family": "fm_scope_a:Kafka:kafka", "fault": {"class": "wrong_scope", "detail": "the topic-level key `index.interval.bytes` where the broker key `log.index.interval.bytes` belongs", "faulty_path": ["spec", "kafka", "config", "index.interval.bytes"], "original_path": ["spec", "kafka", "config", "log.index.interval.bytes"]}, "generator": "eval2_items_a1/3", "id": "a1-fm_scope_a-002", "kind": "fix_manifest", "overlap": null, "overlap_band": "none", "perturbed_reference": "```yaml\napiVersion: kafka.strimzi.io/v1\nkind: Kafka\nmetadata:\n  name: events-kafka\n  namespace: streaming\nspec:\n  kafka:\n    listeners:\n    - name: plain\n      port: 9092\n      type: loadbalancer\n      tls: true\n    config:\n      index.interval.bytes: 10485760\n      group.streams.max.size: 10\n      num.replica.alter.log.dirs.threads: 100\n```", "perturbed_violates": 5, "preserves_unknown": [{"path": "spec.kafka.listeners[]", "preserves_unknown_fields": false}, {"path": "spec.kafka.config", "preserves_unknown_fields": true}], "question": "`kubectl apply` (or the Argo CD / Strimzi validation) rejects the manifest below. It has exactly one mistake. Return the corrected manifest in full as one YAML document and change nothing that is not wrong.\n\n```yaml\napiVersion: kafka.strimzi.io/v1\nkind: Kafka\nmetadata:\n  name: events-kafka\n  namespace: streaming\nspec:\n  kafka:\n    listeners:\n    - name: plain\n      port: 9092\n      type: loadbalancer\n      tls: true\n    config:\n      index.interval.bytes: 10485760\n      group.streams.max.size: 10\n      num.replica.alter.log.dirs.threads: 100\n```\n\nGive the corrected manifest in one fenced code block.", "reference": "```yaml\napiVersion: kafka.strimzi.io/v1\nkind: Kafka\nmetadata:\n  name: events-kafka\n  namespace: streaming\nspec:\n  kafka:\n    listeners:\n    - name: plain\n      port: 9092\n      type: loadbalancer\n      tls: true\n    config:\n      log.index.interval.bytes: 10485760\n      group.streams.max.size: 10\n      num.replica.alter.log.dirs.threads: 100\n```", "resource_kind": "Kafka", "seed": "m4-a1-1", "sources": [{"catalogue": "strimzi_resources", "entry": "Kafka spec.kafka.listeners[].name", "fact": "type string; required", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/040-Crd-kafka.yaml", "line": null}, {"catalogue": "strimzi_resources", "entry": "Kafka spec.kafka.listeners[].port", "fact": "type integer; required", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/040-Crd-kafka.yaml", "line": null}, {"catalogue": "strimzi_resources", "entry": "Kafka spec.kafka.listeners[].type", "fact": "type string; one of cluster-ip, ingress, internal, loadbalancer, nodeport, route, tlsroute; required", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/040-Crd-kafka.yaml", "line": null}, {"catalogue": "strimzi_resources", "entry": "Kafka spec.kafka.listeners[].tls", "fact": "type boolean; required", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/040-Crd-kafka.yaml", "line": null}, {"catalogue": "kafka_config", "entry": "broker/log.index.interval.bytes", "fact": "key exists in scope broker; type int; valid values [0,...]", "file": "raw/apache-kafka-config/content/en/43/generated/kafka_config.html", "line": 1994}, {"catalogue": "kafka_config", "entry": "broker/group.streams.max.size", "fact": "key exists in scope broker; type int; valid values [1,...]", "file": "raw/apache-kafka-config/content/en/43/generated/kafka_config.html", "line": 1752}, {"catalogue": "kafka_config", "entry": "broker/num.replica.alter.log.dirs.threads", "fact": "key exists in scope broker; type int", "file": "raw/apache-kafka-config/content/en/43/generated/kafka_config.html", "line": 564}, {"catalogue": "strimzi_resources", "entry": "Kafka spec.kafka.config", "fact": "free-form map (the schema keeps unknown keys); keys are checked against the broker configuration catalogue", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/040-Crd-kafka.yaml", "line": null}, {"catalogue": "strimzi_resources", "entry": "Kafka spec.kafka.config", "fact": "type object", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/040-Crd-kafka.yaml", "line": null}], "template": "fm_scope_a", "tier": "A", "validator_codes": [], "versions": {"kafka": "4.3", "strimzi": "1.3.1"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "checks": [{"format": true, "type": "yaml_document"}, {"api_version": "kafka.strimzi.io/v1", "kind": "KafkaTopic", "type": "yaml_one_doc"}, {"kind": "KafkaTopic", "type": "yaml_strict"}, {"fields": [{"path": ["apiVersion"], "value": "kafka.strimzi.io/v1"}, {"path": ["kind"], "value": "KafkaTopic"}, {"path": ["metadata", "name"], "value": "orders-events"}, {"path": ["metadata", "namespace"], "value": "prod"}, {"path": ["metadata", "labels", "strimzi.io/cluster"], "value": "data-platform"}, {"path": ["spec", "partitions"], "value": 3}, {"path": ["spec", "replicas"], "value": 2}, {"loose": true, "path": ["spec", "config", "max.message.bytes"], "value": 104857600}, {"loose": true, "path": ["spec", "config", "local.retention.ms"], "value": 1000}], "kind": "KafkaTopic", "type": "yaml_fields_equal"}, {"kind": "KafkaTopic", "paths": [["spec", "config", "log.local.retention.ms"]], "type": "yaml_fields_absent"}, {"kind": "KafkaTopic", "path": ["spec", "config"], "scope": "topic", "type": "yaml_config_keys"}], "dedup": {"max_containment": 0.647, "max_jaccard": 0.212, "nearest": "data/train.jsonl"}, "domain": "strimzi", "exposed": true, "failure_class": "wrong_scope", "family": "fm_scope_b:KafkaTopic:config+partitions+replicas", "fault": {"class": "wrong_scope", "detail": "the broker key `log.local.retention.ms` where the topic key `local.retention.ms` belongs", "faulty_path": ["spec", "config", "log.local.retention.ms"], "original_path": ["spec", "config", "local.retention.ms"]}, "generator": "eval2_items_a1/3", "id": "a1-fm_scope_b-001", "kind": "fix_manifest", "overlap": null, "overlap_band": "none", "perturbed_reference": "```yaml\napiVersion: kafka.strimzi.io/v1\nkind: KafkaTopic\nmetadata:\n  name: orders-events\n  namespace: prod\n  labels:\n    strimzi.io/cluster: data-platform\nspec:\n  partitions: 3\n  replicas: 2\n  config:\n    max.message.bytes: 104857600\n    log.local.retention.ms: 1000\n```", "perturbed_violates": 5, "preserves_unknown": [{"path": "spec", "preserves_unknown_fields": false}, {"path": "spec.config", "preserves_unknown_fields": true}], "question": "A reviewer says this manifest has exactly one error. Fix it and give the complete corrected manifest (one YAML document); every other value must stay as it is.\n\n```yaml\napiVersion: kafka.strimzi.io/v1\nkind: KafkaTopic\nmetadata:\n  name: orders-events\n  namespace: prod\n  labels:\n    strimzi.io/cluster: data-platform\nspec:\n  partitions: 3\n  replicas: 2\n  config:\n    max.message.bytes: 104857600\n    log.local.retention.ms: 1000\n```\n\nGive the corrected manifest in one fenced code block.", "reference": "```yaml\napiVersion: kafka.strimzi.io/v1\nkind: KafkaTopic\nmetadata:\n  name: orders-events\n  namespace: prod\n  labels:\n    strimzi.io/cluster: data-platform\nspec:\n  partitions: 3\n  replicas: 2\n  config:\n    max.message.bytes: 104857600\n    local.retention.ms: 1000\n```", "resource_kind": "KafkaTopic", "seed": "m4-a1-1", "sources": [{"catalogue": "strimzi_annotations", "entry": "strimzi.io/cluster", "fact": "the label name is in the Strimzi documentation", "file": "raw/strimzi/documentation/api/io.strimzi.api.kafka.model.common.EnvironmentVariableRack.adoc", "line": null}, {"catalogue": "strimzi_resources", "entry": "KafkaTopic spec.partitions", "fact": "type integer", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/043-Crd-kafkatopic.yaml", "line": null}, {"catalogue": "strimzi_resources", "entry": "KafkaTopic spec.replicas", "fact": "type integer", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/043-Crd-kafkatopic.yaml", "line": null}, {"catalogue": "kafka_config", "entry": "topic/max.message.bytes", "fact": "key exists in scope topic; type int; valid values [0,...]", "file": "raw/apache-kafka-config/content/en/43/generated/topic_config.html", "line": 179}, {"catalogue": "kafka_config", "entry": "topic/local.retention.ms", "fact": "key exists in scope topic; type long; valid values [-2,...]", "file": "raw/apache-kafka-config/content/en/43/generated/topic_config.html", "line": 146}, {"catalogue": "strimzi_resources", "entry": "KafkaTopic spec.config", "fact": "free-form map (the schema keeps unknown keys); keys are checked against the topic configuration catalogue", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/043-Crd-kafkatopic.yaml", "line": null}, {"catalogue": "strimzi_resources", "entry": "KafkaTopic spec.config", "fact": "type object", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/043-Crd-kafkatopic.yaml", "line": null}], "template": "fm_scope_b", "tier": "A", "validator_codes": [], "versions": {"kafka": "4.3", "strimzi": "1.3.1"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "checks": [{"format": true, "type": "has_kafka_command"}, {"forbidden_flags": [], "required_flags": {"--bootstrap-server": "kafka:9092", "--num-records": "100000", "--record-size": "1024", "--throughput": "-1", "--topic": "perf-test"}, "tool": "kafka-producer-perf-test.sh", "type": "kcmd_spec"}, {"tool": "kafka-producer-perf-test.sh", "type": "kcmd_known_flags"}, {"tool": "kafka-producer-perf-test.sh", "type": "kcmd_constraints"}], "dedup": {"max_containment": 0.381, "max_jaccard": 0.211, "nearest": "work/v3/questions.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "multi_flag", "family": "wc_b:kafka-producer-perf-test:--topic", "generator": "eval2_items_a1/3", "id": "a1-wc_b-006", "kind": "write_command", "overlap": null, "overlap_band": "none", "perturbed_reference": "```bash\nbin/kafka-producer-perf-test.sh --bootstrap-server kafka:9092 --topic perf-test --num-records 100000 --record-size 1024\n```", "perturbed_violates": 1, "question": "On a broker host with Kafka 4.3 installed, which single `kafka-producer-perf-test.sh` command would measure the producer throughput of the topic `perf-test` with 100000 records of 1024 bytes and no throttling? Bootstrap server: `kafka:9092`.\n\nGive the command in a code block; use only options that exist for the tool.", "reference": "```bash\nbin/kafka-producer-perf-test.sh --bootstrap-server kafka:9092 --topic perf-test --num-records 100000 --record-size 1024 --throughput -1\n```", "seed": "m4-a1-1", "sources": [{"catalogue": "kafka_cli", "entry": "kafka-producer-perf-test.sh --throughput", "fact": "option of the complete parser", "file": "raw_catalogue/apache-kafka-tools/tools/src/main/java/org/apache/kafka/tools/ProducerPerformance.java", "line": null}, {"catalogue": "kafka_cli", "entry": "kafka-producer-perf-test.sh --record-size", "fact": "option of the complete parser", "file": "raw_catalogue/apache-kafka-tools/tools/src/main/java/org/apache/kafka/tools/ProducerPerformance.java", "line": null}, {"catalogue": "kafka_cli", "entry": "kafka-producer-perf-test.sh --num-records", "fact": "option of the complete parser", "file": "raw_catalogue/apache-kafka-tools/tools/src/main/java/org/apache/kafka/tools/ProducerPerformance.java", "line": null}, {"catalogue": "kafka_cli", "entry": "kafka-producer-perf-test.sh", "fact": "the tool's option parser is complete; every flag on the line is checked against it", "file": "raw_catalogue/apache-kafka-tools/bin/kafka-producer-perf-test.sh", "line": null}], "template": "wc_b", "tier": "A", "versions": {"kafka": "4.3.2"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "checks": [{"format": true, "type": "has_kafka_command"}, {"forbidden_flags": ["--delete"], "required_flags": {"--bootstrap-server": "kafka:9092", "--describe": null, "--group": "orders-streams", "--members": null}, "tool": "kafka-streams-groups.sh", "type": "kcmd_spec"}, {"tool": "kafka-streams-groups.sh", "type": "kcmd_known_flags"}, {"tool": "kafka-streams-groups.sh", "type": "kcmd_constraints"}], "dedup": {"max_containment": 0.538, "max_jaccard": 0.171, "nearest": "work/v3/questions.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "multi_flag", "family": "wc_d:kafka-streams-groups:--describe", "generator": "eval2_items_a1/3", "id": "a1-wc_d-002", "kind": "write_command", "overlap": null, "overlap_band": "none", "perturbed_reference": "```bash\nbin/kafka-streams-groups.sh --bootstrap-server kafka:9092 --describe --group orders-streams --members --delete\n```", "perturbed_violates": 1, "question": "I need to describe the streams group `orders-streams` with its members. What exact command do I run? Kafka 4.3, bootstrap server `kafka:9092`.\n\nGive the command in a code block; use only options that exist for the tool.", "reference": "```bash\nbin/kafka-streams-groups.sh --bootstrap-server kafka:9092 --describe --group orders-streams --members\n```", "seed": "m4-a1-1", "sources": [{"catalogue": "kafka_cli", "entry": "kafka-streams-groups.sh --describe", "fact": "option of the complete parser", "file": "raw_catalogue/apache-kafka-tools/tools/src/main/java/org/apache/kafka/tools/streams/StreamsGroupCommandOptions.java", "line": null}, {"catalogue": "kafka_cli", "entry": "kafka-streams-groups.sh --members", "fact": "option of the complete parser", "file": "raw_catalogue/apache-kafka-tools/tools/src/main/java/org/apache/kafka/tools/streams/StreamsGroupCommandOptions.java", "line": null}, {"catalogue": "kafka_cli", "entry": "kafka-streams-groups.sh", "fact": "the tool's option parser is complete; every flag on the line is checked against it", "file": "raw_catalogue/apache-kafka-tools/bin/kafka-streams-groups.sh", "line": null}], "template": "wc_d", "tier": "A", "versions": {"kafka": "4.3.2"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "checks": [{"format": true, "type": "has_kafka_command"}, {"forbidden_flags": ["--alter"], "required_flags": {"--all": null, "--bootstrap-server": "kafka:9092", "--describe": null, "--entity-name": "orders-events", "--entity-type": "topics"}, "tool": "kafka-configs.sh", "type": "kcmd_spec"}, {"tool": "kafka-configs.sh", "type": "kcmd_known_flags"}, {"tool": "kafka-configs.sh", "type": "kcmd_constraints"}], "dedup": {"max_containment": 0.4, "max_jaccard": 0.214, "nearest": "data/train.jsonl"}, "domain": "kafka", "exposed": true, "failure_class": "multi_flag", "family": "wc_d:kafka-configs:--describe", "generator": "eval2_items_a1/3", "id": "a1-wc_d-006", "kind": "write_command", "overlap": {"desc_in_q": 0.143, "q_in_desc": 0.067}, "overlap_band": "low", "perturbed_reference": "```bash\nbin/kafka-configs.sh --bootstrap-server kafka:9092 --describe --entity-type topics --entity-name orders-events --all --alter\n```", "perturbed_violates": 1, "question": "I need to show all configuration of the topic `orders-events`, including the defaults. What exact command do I run? Kafka 4.3, bootstrap server `kafka:9092`.\n\nGive the command in a code block; use only options that exist for the tool.", "reference": "```bash\nbin/kafka-configs.sh --bootstrap-server kafka:9092 --describe --entity-type topics --entity-name orders-events --all\n```", "seed": "m4-a1-1", "sources": [{"catalogue": "kafka_cli", "entry": "kafka-configs.sh --all", "fact": "description: List all configs for the given entity, including static configs if available.", "file": "raw_catalogue/apache-kafka-tools/core/src/main/scala/kafka/admin/ConfigCommand.scala", "line": null}, {"catalogue": "kafka_cli", "entry": "kafka-configs.sh", "fact": "the tool's option parser is complete; every flag on the line is checked against it", "file": "raw_catalogue/apache-kafka-tools/bin/kafka-configs.sh", "line": null}], "template": "wc_d", "tier": "A", "versions": {"kafka": "4.3.2"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "checks": [{"format": true, "type": "yaml_document"}, {"api_version": "apps/v1", "kind": "Deployment", "type": "yaml_one_doc"}, {"kind": "Deployment", "type": "yaml_strict"}, {"fields": [{"path": ["metadata", "name"], "value": "search"}, {"path": ["metadata", "namespace"], "value": "kafka"}, {"path": ["spec", "replicas"], "value": 2}, {"path": ["spec", "selector", "matchLabels", "app"], "value": "search"}, {"path": ["spec", "template", "metadata", "labels", "app"], "value": "search"}, {"path": ["spec", "template", "spec", "containers", 0, "name"], "value": "app"}, {"path": ["spec", "template", "spec", "containers", 0, "image"], "value": "registry.example.com/search:1.4.2"}, {"path": ["spec", "template", "spec", "containers", 0, "resources", "requests", "cpu"], "value": "500m"}, {"path": ["spec", "template", "spec", "containers", 0, "resources", "requests", "memory"], "value": "1Gi"}, {"path": ["spec", "template", "spec", "containers", 0, "resources", "limits", "memory"], "value": "1Gi"}], "kind": "Deployment", "type": "yaml_fields_equal"}, {"kind": "Deployment", "paths": [["spec", "template", "spec", "containers", 0, "resources", "limits", "cpu"]], "type": "yaml_fields_absent"}], "dedup": {"max_containment": 0.625, "max_jaccard": 0.114, "nearest": "eval/answers_1024_copy.jsonl"}, "domain": "kubernetes", "exposed": true, "failure_class": "negation", "family": "wr_deploy_a:Deployment:replicas+selector+template", "generator": "eval2_items_a1/3", "id": "a1-wr_deploy_a-004", "kind": "write_resource", "overlap": null, "overlap_band": "none", "perturbed_reference": "```yaml\napiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: search\n  namespace: kafka\nspec:\n  replicas: 3\n  selector:\n    matchLabels:\n      app: search\n  template:\n    metadata:\n      labels:\n        app: search\n    spec:\n      containers:\n      - name: app\n        image: registry.example.com/search:1.4.2\n        resources:\n          requests:\n            cpu: 500m\n            memory: 1Gi\n          limits:\n            memory: 1Gi\n```", "perturbed_violates": 3, "preserves_unknown": [{"path": "spec", "preserves_unknown_fields": false}, {"path": "spec.selector.matchLabels", "preserves_unknown_fields": true}, {"path": "spec.template.metadata.labels", "preserves_unknown_fields": true}, {"path": "spec.template.spec.containers[]", "preserves_unknown_fields": false}, {"path": "spec.template.spec.containers[].resources.requests", "preserves_unknown_fields": true}, {"path": "spec.template.spec.containers[].resources.limits", "preserves_unknown_fields": true}], "question": "Write a Kubernetes `Deployment` resource (apiVersion `apps/v1`) named `search` in the namespace `kafka`, with 2 replicas, the selector label `app: search`, the same label on the pods, one container `app`, image `registry.example.com/search:1.4.2`, requests of `500m` CPU, requests of `1Gi` memory and a memory limit of `1Gi`. Do not set a CPU limit (`resources.limits.cpu`). The schema is strict: a field it does not define is an error. Give exactly one YAML document.\n\nPut the manifest in one fenced code block.", "reference": "```yaml\napiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: search\n  namespace: kafka\nspec:\n  replicas: 2\n  selector:\n    matchLabels:\n      app: search\n  template:\n    metadata:\n      labels:\n        app: search\n    spec:\n      containers:\n      - name: app\n        image: registry.example.com/search:1.4.2\n        resources:\n          requests:\n            cpu: 500m\n            memory: 1Gi\n          limits:\n            memory: 1Gi\n```", "resource_kind": "Deployment", "seed": "m4-a1-1", "sources": [{"catalogue": "kubernetes", "entry": "Deployment spec.replicas", "fact": "type integer", "file": "raw_catalogue/kubernetes-api/api/openapi-spec/swagger.json", "line": null}, {"catalogue": "kubernetes", "entry": "Deployment spec.selector.matchLabels", "fact": "type object", "file": "raw_catalogue/kubernetes-api/api/openapi-spec/swagger.json", "line": null}, {"catalogue": "kubernetes", "entry": "Deployment spec.template.metadata.labels", "fact": "type object", "file": "raw_catalogue/kubernetes-api/api/openapi-spec/swagger.json", "line": null}, {"catalogue": "kubernetes", "entry": "Deployment spec.template.spec.containers[].name", "fact": "type string; required", "file": "raw_catalogue/kubernetes-api/api/openapi-spec/swagger.json", "line": null}, {"catalogue": "kubernetes", "entry": "Deployment spec.template.spec.containers[].image", "fact": "type string", "file": "raw_catalogue/kubernetes-api/api/openapi-spec/swagger.json", "line": null}, {"catalogue": "kubernetes", "entry": "Deployment spec.template.spec.containers[].resources.requests", "fact": "type object", "file": "raw_catalogue/kubernetes-api/api/openapi-spec/swagger.json", "line": null}, {"catalogue": "kubernetes", "entry": "Deployment spec.template.spec.containers[].resources.limits", "fact": "type object", "file": "raw_catalogue/kubernetes-api/api/openapi-spec/swagger.json", "line": null}, {"absent": "spec.template.spec.containers[].resources.limits.cpu", "catalogue": "kubernetes", "entry": "Deployment spec.template.spec.containers[].resources.limits.cpu", "fact": "not a field of the schema", "file": "raw_catalogue/kubernetes-api/api/openapi-spec/swagger.json", "line": null}], "template": "wr_deploy_a", "tier": "A", "versions": {"kubernetes": "v1.37.2"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "checks": [{"format": true, "type": "yaml_document"}, {"api_version": "kafka.strimzi.io/v1", "kind": "Kafka", "type": "yaml_one_doc"}, {"kind": "Kafka", "type": "yaml_strict"}, {"fields": [{"path": ["metadata", "name"], "value": "prod-cluster"}, {"path": ["metadata", "namespace"], "value": "prod"}, {"path": ["spec", "kafka", "listeners", 0, "name"], "value": "tls"}, {"path": ["spec", "kafka", "listeners", 0, "port"], "value": 9092}, {"path": ["spec", "kafka", "listeners", 0, "type"], "value": "cluster-ip"}, {"path": ["spec", "kafka", "listeners", 0, "tls"], "value": false}, {"loose": true, "path": ["spec", "kafka", "config", "delegation.token.expiry.time.ms"], "value": 30000}, {"loose": true, "path": ["spec", "kafka", "config", "auto.create.topics.enable"], "value": false}, {"loose": true, "path": ["spec", "kafka", "config", "share.coordinator.write.timeout.ms"], "value": 604800000}], "kind": "Kafka", "type": "yaml_fields_equal"}, {"kind": "Kafka", "paths": [["spec", "kafka", "config", "log.message.timestamp.after.max.ms"], ["spec", "zookeeper"]], "type": "yaml_fields_absent"}, {"kind": "Kafka", "path": ["spec", "kafka", "config"], "scope": "broker", "type": "yaml_config_keys"}], "dedup": {"max_containment": 0.75, "max_jaccard": 0.086, "nearest": "data/train.jsonl"}, "domain": "strimzi", "exposed": true, "failure_class": "negation", "family": "wr_kafka_a:Kafka:kafka", "generator": "eval2_items_a1/3", "id": "a1-wr_kafka_a-003", "kind": "write_resource", "overlap": null, "overlap_band": "none", "perturbed_reference": "```yaml\napiVersion: kafka.strimzi.io/v1\nkind: Kafka\nmetadata:\n  name: prod-cluster\n  namespace: prod\nspec:\n  kafka:\n    listeners:\n    - name: tlsx\n      port: 9092\n      type: cluster-ip\n      tls: false\n    config:\n      delegation.token.expiry.time.ms: 30000\n      auto.create.topics.enable: false\n      share.coordinator.write.timeout.ms: 604800000\n```", "perturbed_violates": 3, "preserves_unknown": [{"path": "spec.kafka.listeners[]", "preserves_unknown_fields": false}, {"path": "spec.kafka.config", "preserves_unknown_fields": true}], "question": "Write a Strimzi `Kafka` resource (apiVersion `kafka.strimzi.io/v1`) named `prod-cluster` in the namespace `prod`, with one listener named `tls`, on port 9092, of type `cluster-ip`, `tls: false`, `delegation.token.expiry.time.ms` set to 30000, `auto.create.topics.enable` set to false and `share.coordinator.write.timeout.ms` set to 604800000. Do not set `log.message.timestamp.after.max.ms` (leave it at its default) and `spec.zookeeper` (the cluster uses KRaft with node pools). The schema is strict: a field it does not define is an error. `spec.kafka.config` is a free-form map (the schema keeps unknown keys there), but its keys must be real Kafka broker settings. Give exactly one YAML document.\n\nPut the manifest in one fenced code block.", "reference": "```yaml\napiVersion: kafka.strimzi.io/v1\nkind: Kafka\nmetadata:\n  name: prod-cluster\n  namespace: prod\nspec:\n  kafka:\n    listeners:\n    - name: tls\n      port: 9092\n      type: cluster-ip\n      tls: false\n    config:\n      delegation.token.expiry.time.ms: 30000\n      auto.create.topics.enable: false\n      share.coordinator.write.timeout.ms: 604800000\n```", "resource_kind": "Kafka", "seed": "m4-a1-1", "sources": [{"catalogue": "strimzi_resources", "entry": "Kafka spec.kafka.listeners[].name", "fact": "type string; required", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/040-Crd-kafka.yaml", "line": null}, {"catalogue": "strimzi_resources", "entry": "Kafka spec.kafka.listeners[].port", "fact": "type integer; required", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/040-Crd-kafka.yaml", "line": null}, {"catalogue": "strimzi_resources", "entry": "Kafka spec.kafka.listeners[].type", "fact": "type string; one of cluster-ip, ingress, internal, loadbalancer, nodeport, route, tlsroute; required", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/040-Crd-kafka.yaml", "line": null}, {"catalogue": "strimzi_resources", "entry": "Kafka spec.kafka.listeners[].tls", "fact": "type boolean; required", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/040-Crd-kafka.yaml", "line": null}, {"catalogue": "kafka_config", "entry": "broker/delegation.token.expiry.time.ms", "fact": "key exists in scope broker; type long; valid values [1,...]", "file": "raw/apache-kafka-config/content/en/43/generated/kafka_config.html", "line": 1158}, {"catalogue": "kafka_config", "entry": "broker/auto.create.topics.enable", "fact": "key exists in scope broker; type boolean", "file": "raw/apache-kafka-config/content/en/43/generated/kafka_config.html", "line": 69}, {"catalogue": "kafka_config", "entry": "broker/share.coordinator.write.timeout.ms", "fact": "key exists in scope broker; type int; valid values [1,...]", "file": "raw/apache-kafka-config/content/en/43/generated/kafka_config.html", "line": 872}, {"catalogue": "strimzi_resources", "entry": "Kafka spec.kafka.config", "fact": "free-form map (the schema keeps unknown keys); keys are checked against the broker configuration catalogue", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/040-Crd-kafka.yaml", "line": null}, {"catalogue": "kafka_config", "entry": "broker/log.message.timestamp.after.max.ms", "fact": "key exists in scope broker; the item excludes it", "file": "raw/apache-kafka-config/content/en/43/generated/kafka_config.html", "line": 2038}, {"absent": "spec.zookeeper", "catalogue": "strimzi_resources", "entry": "Kafka spec.zookeeper", "fact": "not a field of the schema", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/040-Crd-kafka.yaml", "line": null}], "template": "wr_kafka_a", "tier": "A", "versions": {"kafka": "4.3", "strimzi": "1.3.1"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "checks": [{"format": true, "type": "yaml_document"}, {"api_version": "kafka.strimzi.io/v1", "kind": "Kafka", "type": "yaml_one_doc"}, {"kind": "Kafka", "type": "yaml_strict"}, {"fields": [{"path": ["metadata", "name"], "value": "events-kafka"}, {"path": ["metadata", "namespace"], "value": "data"}, {"path": ["spec", "kafka", "listeners", 0, "name"], "value": "tls"}, {"path": ["spec", "kafka", "listeners", 0, "port"], "value": 9092}, {"path": ["spec", "kafka", "listeners", 0, "type"], "value": "ingress"}, {"path": ["spec", "kafka", "listeners", 0, "tls"], "value": true}, {"loose": true, "path": ["spec", "kafka", "config", "log.index.interval.bytes"], "value": 5242880}, {"loose": true, "path": ["spec", "kafka", "config", "group.streams.initial.rebalance.delay.ms"], "value": 604800000}], "kind": "Kafka", "type": "yaml_fields_equal"}, {"kind": "Kafka", "paths": [["spec", "kafka", "config", "replication.quota.window.size.seconds"], ["spec", "zookeeper"]], "type": "yaml_fields_absent"}, {"kind": "Kafka", "path": ["spec", "kafka", "config"], "scope": "broker", "type": "yaml_config_keys"}], "dedup": {"max_containment": 0.778, "max_jaccard": 0.1, "nearest": "data/train.jsonl"}, "domain": "strimzi", "exposed": true, "failure_class": "negation", "family": "wr_kafka_b:Kafka:kafka", "generator": "eval2_items_a1/3", "id": "a1-wr_kafka_b-001", "kind": "write_resource", "overlap": null, "overlap_band": "none", "perturbed_reference": "```yaml\napiVersion: kafka.strimzi.io/v1\nkind: Kafka\nmetadata:\n  name: events-kafka\n  namespace: data\nspec:\n  kafka:\n    listeners:\n    - name: tlsx\n      port: 9092\n      type: ingress\n      tls: true\n    config:\n      log.index.interval.bytes: 5242880\n      group.streams.initial.rebalance.delay.ms: 604800000\n```", "perturbed_violates": 3, "preserves_unknown": [{"path": "spec.kafka.listeners[]", "preserves_unknown_fields": false}, {"path": "spec.kafka.config", "preserves_unknown_fields": true}], "question": "I need the manifest of a Strimzi `Kafka` named `events-kafka` in the namespace `data`, apiVersion `kafka.strimzi.io/v1`: one listener named `tls`, on port 9092, of type `ingress`, `tls: true`, `log.index.interval.bytes` set to 5242880 and `group.streams.initial.rebalance.delay.ms` set to 604800000. Leave out `replication.quota.window.size.seconds` (leave it at its default) and `spec.zookeeper` (the cluster uses KRaft with node pools). The schema is strict: a field it does not define is an error. `spec.kafka.config` is a free-form map (the schema keeps unknown keys there), but its keys must be real Kafka broker settings. Reply with exactly one YAML document.\n\nPut the manifest in one fenced code block.", "reference": "```yaml\napiVersion: kafka.strimzi.io/v1\nkind: Kafka\nmetadata:\n  name: events-kafka\n  namespace: data\nspec:\n  kafka:\n    listeners:\n    - name: tls\n      port: 9092\n      type: ingress\n      tls: true\n    config:\n      log.index.interval.bytes: 5242880\n      group.streams.initial.rebalance.delay.ms: 604800000\n```", "resource_kind": "Kafka", "seed": "m4-a1-1", "sources": [{"catalogue": "strimzi_resources", "entry": "Kafka spec.kafka.listeners[].name", "fact": "type string; required", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/040-Crd-kafka.yaml", "line": null}, {"catalogue": "strimzi_resources", "entry": "Kafka spec.kafka.listeners[].port", "fact": "type integer; required", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/040-Crd-kafka.yaml", "line": null}, {"catalogue": "strimzi_resources", "entry": "Kafka spec.kafka.listeners[].type", "fact": "type string; one of cluster-ip, ingress, internal, loadbalancer, nodeport, route, tlsroute; required", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/040-Crd-kafka.yaml", "line": null}, {"catalogue": "strimzi_resources", "entry": "Kafka spec.kafka.listeners[].tls", "fact": "type boolean; required", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/040-Crd-kafka.yaml", "line": null}, {"catalogue": "kafka_config", "entry": "broker/log.index.interval.bytes", "fact": "key exists in scope broker; type int; valid values [0,...]", "file": "raw/apache-kafka-config/content/en/43/generated/kafka_config.html", "line": 1994}, {"catalogue": "kafka_config", "entry": "broker/group.streams.initial.rebalance.delay.ms", "fact": "key exists in scope broker; type int; valid values [0,...]", "file": "raw/apache-kafka-config/content/en/43/generated/kafka_config.html", "line": 1708}, {"catalogue": "strimzi_resources", "entry": "Kafka spec.kafka.config", "fact": "free-form map (the schema keeps unknown keys); keys are checked against the broker configuration catalogue", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/040-Crd-kafka.yaml", "line": null}, {"catalogue": "kafka_config", "entry": "broker/replication.quota.window.size.seconds", "fact": "key exists in scope broker; the item excludes it", "file": "raw/apache-kafka-config/content/en/43/generated/kafka_config.html", "line": 3512}, {"absent": "spec.zookeeper", "catalogue": "strimzi_resources", "entry": "Kafka spec.zookeeper", "fact": "not a field of the schema", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/040-Crd-kafka.yaml", "line": null}], "template": "wr_kafka_b", "tier": "A", "versions": {"kafka": "4.3", "strimzi": "1.3.1"}}
{"anchored": true, "catalogue_hash": "e319653a543211a5366cd1a5734c83ee4d2683c2d41077058369116804a8447e", "checks": [{"format": true, "type": "yaml_document"}, {"api_version": "kafka.strimzi.io/v1", "kind": "KafkaTopic", "type": "yaml_one_doc"}, {"kind": "KafkaTopic", "type": "yaml_strict"}, {"fields": [{"path": ["metadata", "name"], "value": "metrics-raw"}, {"path": ["metadata", "namespace"], "value": "streaming"}, {"path": ["metadata", "labels", "strimzi.io/cluster"], "value": "data-platform"}, {"path": ["spec", "partitions"], "value": 48}, {"path": ["spec", "replicas"], "value": 3}, {"loose": true, "path": ["spec", "config", "remote.log.copy.disable"], "value": true}, {"loose": true, "path": ["spec", "config", "retention.ms"], "value": 86400000}], "kind": "KafkaTopic", "type": "yaml_fields_equal"}, {"kind": "KafkaTopic", "paths": [["spec", "config", "remote.storage.enable"], ["spec", "topicName"]], "type": "yaml_fields_absent"}, {"kind": "KafkaTopic", "path": ["spec", "config"], "scope": "topic", "type": "yaml_config_keys"}], "dedup": {"max_containment": 0.625, "max_jaccard": 0.078, "nearest": "data/train.jsonl"}, "domain": "strimzi", "exposed": true, "failure_class": "negation", "family": "wr_topic_b:KafkaTopic:config+partitions+replicas", "generator": "eval2_items_a1/3", "id": "a1-wr_topic_b-001", "kind": "write_resource", "overlap": null, "overlap_band": "none", "perturbed_reference": "```yaml\napiVersion: kafka.strimzi.io/v1\nkind: KafkaTopic\nmetadata:\n  name: metrics-raw\n  namespace: streaming\n  labels:\n    strimzi.io/cluster: data-platform\nspec:\n  partitions: 49\n  replicas: 3\n  config:\n    remote.log.copy.disable: true\n    retention.ms: 86400000\n```", "perturbed_violates": 3, "preserves_unknown": [{"path": "spec", "preserves_unknown_fields": false}, {"path": "spec.config", "preserves_unknown_fields": true}], "question": "I need the manifest of a Strimzi `KafkaTopic` named `metrics-raw` in the namespace `streaming` labelled `strimzi.io/cluster: data-platform`, apiVersion `kafka.strimzi.io/v1`: 48 partitions, 3 replicas, `remote.log.copy.disable` set to true and `retention.ms` set to 86400000. Leave out `remote.storage.enable` (leave it at its default) and `spec.topicName` (the resource name is the topic name). The schema is strict: a field it does not define is an error. `spec.config` is a free-form map (the schema keeps unknown keys there), but its keys must be real Kafka topic-level settings. Reply with exactly one YAML document.\n\nPut the manifest in one fenced code block.", "reference": "```yaml\napiVersion: kafka.strimzi.io/v1\nkind: KafkaTopic\nmetadata:\n  name: metrics-raw\n  namespace: streaming\n  labels:\n    strimzi.io/cluster: data-platform\nspec:\n  partitions: 48\n  replicas: 3\n  config:\n    remote.log.copy.disable: true\n    retention.ms: 86400000\n```", "resource_kind": "KafkaTopic", "seed": "m4-a1-1", "sources": [{"catalogue": "strimzi_annotations", "entry": "strimzi.io/cluster", "fact": "the label name is in the Strimzi documentation", "file": "raw/strimzi/documentation/api/io.strimzi.api.kafka.model.common.EnvironmentVariableRack.adoc", "line": null}, {"catalogue": "strimzi_resources", "entry": "KafkaTopic spec.partitions", "fact": "type integer", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/043-Crd-kafkatopic.yaml", "line": null}, {"catalogue": "strimzi_resources", "entry": "KafkaTopic spec.replicas", "fact": "type integer", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/043-Crd-kafkatopic.yaml", "line": null}, {"catalogue": "kafka_config", "entry": "topic/remote.log.copy.disable", "fact": "key exists in scope topic; type boolean", "file": "raw/apache-kafka-config/content/en/43/generated/topic_config.html", "line": 267}, {"catalogue": "kafka_config", "entry": "topic/retention.ms", "fact": "key exists in scope topic; type long; valid values [-1,...]", "file": "raw/apache-kafka-config/content/en/43/generated/topic_config.html", "line": 311}, {"catalogue": "strimzi_resources", "entry": "KafkaTopic spec.config", "fact": "free-form map (the schema keeps unknown keys); keys are checked against the topic configuration catalogue", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/043-Crd-kafkatopic.yaml", "line": null}, {"catalogue": "kafka_config", "entry": "topic/remote.storage.enable", "fact": "key exists in scope topic; the item excludes it", "file": "raw/apache-kafka-config/content/en/43/generated/topic_config.html", "line": 289}, {"catalogue": "strimzi_resources", "entry": "KafkaTopic spec.topicName", "fact": "type string", "file": "raw_catalogue/strimzi-install/packaging/install/cluster-operator/043-Crd-kafkatopic.yaml", "line": null}], "template": "wr_topic_b", "tier": "A", "versions": {"kafka": "4.3", "strimzi": "1.3.1"}}
'''


def _rows(text):
    return [json.loads(ln) for ln in text.splitlines() if ln.strip()]


def EXPOSED_V1_ROWS():
    return _rows(EXPOSED_V1_TEXT)


def EXPOSED_V2_ROWS():
    return _rows(EXPOSED_V2_TEXT)


def EXPOSED_V3_ROWS():
    return _rows(EXPOSED_V3_TEXT)


def EXPOSED_ALL_ROWS():
    return EXPOSED_V1_ROWS() + EXPOSED_V2_ROWS() + EXPOSED_V3_ROWS()


# ------------------------------------------------------------------ do the exposed items still hold?
def facts_view(cats):
    """What anchor_problems and exposed_problems read of a generator, without building one (used by the tests)."""
    import types
    allent = cats.data["kafka_config"]["entries"]
    v = types.SimpleNamespace(cats=cats, all_keys={e["key"] for e in allent},
                              cfg_by={(e["scope"], e["key"]): e for e in allent}, by_key_all=defaultdict(list))
    for e in allent:
        v.by_key_all[e["key"]].append(e)
    return v


def anchor_problems(g, s):
    """Problems with one cited fact of an item against the current catalogues (a list of strings). Only facts of
    a known shape are re-derived; an entry that is gone, or a stated value that differs, is a problem."""
    cat, fact, cats = s.get("catalogue"), s.get("fact", ""), g.cats
    if cat == "kafka_config":
        if "absent_from" in s:
            scope, _, key = s["entry"].partition("/")
            if (scope, key) not in g.cfg_by:
                return ["entry %s is gone" % s["entry"]]
            return ["`%s` is now a key of scope %s" % (key, s["absent_from"])] if cats.config_key(key, s["absent_from"]).status != UNKNOWN else []
        if "absent" in s:
            return ["absent key `%s` is now in the reference" % s["absent"]] if s["absent"] in g.all_keys else []
        scope, _, key = s["entry"].partition("/")
        e = g.cfg_by.get((scope, key))
        if e is None:
            return ["entry %s is gone" % s["entry"]]
        bad = []
        m = re.match(r"default (.+?)(?: in scope \w+)?$", fact)
        if m and m.group(1) != e.get("default"):
            bad.append("default is now %r, the item says %r" % (e.get("default"), m.group(1)))
        m = re.match(r"type (\w+)$", fact)
        if m and m.group(1) != e.get("type"):
            bad.append("type is now %r" % e.get("type"))
        m = re.match(r"valid values (.+)$", fact)
        if m and m.group(1) != e.get("valid_values"):
            bad.append("valid values are now %r" % e.get("valid_values"))
        m = re.match(r"(?:only scope|key exists only in scope): (\w+)$|key exists only in scope (\w+)", fact)
        if m:
            want = m.group(1) or m.group(2)
            scopes = {x["scope"] for x in g.by_key_all[key] if x["scope"] in SCOPE_H}
            if scopes != {want}:
                bad.append("the key is now in scopes %s" % sorted(scopes))
        m = re.match(r"update mode (.+?)(?: \(distractor\))?$", fact)
        if m and m.group(1) != e.get("update_mode"):
            bad.append("update mode is now %r" % e.get("update_mode"))
        return bad
    if cat == "protocol_errors":
        names = {e["name"]: e for e in cats.data["protocol_errors"]["entries"]}
        if "absent" in s:
            return ["absent error name `%s` is now in the list" % s["absent"]] if s["absent"] in names else []
        e = names.get(s["entry"])
        if e is None:
            return ["error %s is gone" % s["entry"]]
        m = re.match(r"retriable (True|False)", fact)
        if m and (m.group(1) == "True") != bool(e["retriable"]):
            return ["retriable is now %s" % e["retriable"]]
        return []
    if cat == "connect_rest":
        ops = {"%s %s" % (e["method"], e["path"]) for e in cats.data["connect_rest"]["entries"]}
        if "absent" in s:
            return ["`%s` is now an operation" % s["absent"]] if s["absent"] in ops else []
        return [] if s["entry"] in ops else ["operation %s is gone" % s["entry"]]
    if cat in ("strimzi_resources", "kubernetes", "argocd") and not s.get("entry", "").startswith(("argocd ", "kubectl ", "operator pod")):
        kind, _, path = s["entry"].partition(" ")
        if path == "apiVersion":
            e = cats.schema_entry(kind)
            m = re.search(r"serves (\S+) only", fact)
            return [] if e and (not m or e["api_version"] == m.group(1)) else ["%s is no longer served as the item says" % kind]
        look = cats.field_path(kind, path if path != "(root)" else "spec")
        if "absent" in s:  # below a free-form map the schema cannot say more: not covered is not a contradiction
            ok = look.status == UNKNOWN or (look.status == NOT_COVERED and "free-form" in (look.reason or ""))
            return [] if ok else ["`%s` of %s is now %s" % (path, kind, look.status)]
        return [] if look.known else ["field %s of %s is now %s" % (path, kind, look.status)]
    if cat == "kafka_config_history":
        scope, _, key = s["entry"].partition("/")
        hits = [e for e in cats.data["kafka_config_history"]["entries"] if e["scope"] == scope and e["key"] == key]
        if not hits:
            return ["history entry %s is gone" % s["entry"]]
        v = hits[0]["versions"]
        m = re.match(r"default (.+?) in 4\.1, (.+?) in 4\.3$", fact)
        if m and (k2.norm_default(v["4.1"]["default_value"]), k2.norm_default(v["4.3"]["default_value"])) != (
                k2.norm_default(m.group(1)), k2.norm_default(m.group(2))):
            return ["the defaults of %s changed" % key]
        m = re.match(r"absent from the 4\.1 reference, present from (\S+)$", fact)
        if m and (v["4.1"] is not None or v[m.group(1)] is None):
            return ["%s is no longer first listed in %s" % (key, m.group(1))]
        return []
    if cat == "argocd":
        parts = s["entry"].split(" --")
        if not cats.argocd_command(parts[0]).known:
            return ["command %s is gone" % parts[0]]
        if len(parts) > 1 and not cats.cli_option(parts[0], "--" + parts[1]).known:
            return ["flag --%s of %s is gone" % (parts[1], parts[0])]
        return []
    if cat in ("kafka_cli", "kubernetes") and ("absent" in s or " --" in s.get("entry", "")):
        if "absent" in s:
            look = cats.cli_option(s["entry"], s["absent"])
            want = UNKNOWN if "complete option list" in fact else NOT_COVERED
            return [] if look.status == want else ["`%s` of %s is now %s (the item needs %s)" % (s["absent"], s["entry"], look.status, want)]
        tool, _, flag = s["entry"].partition(" --")
        return [] if cats.cli_option(tool, "--" + flag).known else ["flag --%s of %s is not in the catalogue" % (flag, tool)]
    if cat == "strimzi_annotations" and "absent" in s:
        st = cats.annotation(s["absent"]).status
        return [] if st == NOT_COVERED else ["annotation `%s` is now %s" % (s["absent"], st)]
    return []


def exposed_problems(g, row):
    """Why an exposed item no longer holds against the current catalogues, or None. The row is not changed."""
    item = {"id": row["id"], "checks": row["checks"]}
    why = []
    try:
        res = sc.run_checks(item, row["reference"], g.cats)
    except Exception as ex:  # a check type the scorer no longer knows
        return "the checks cannot be run: %s" % ex
    for r in res:
        if not r["pass"]:
            why.append("reference fails check %d (%s): %s" % (r["check"], r["type"], r["detail"][:100]))
    pres = sc.run_checks(item, row["perturbed_reference"], g.cats)
    if pres[row["perturbed_violates"]]["pass"] or all(r["pass"] for r in pres):
        why.append("the perturbed reference now passes")
    for s in row["sources"]:
        why += anchor_problems(g, s)
    return "; ".join(why) if why else None


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--catalogues", help="catalogue directory (default work/m4/catalogues)")
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    ap.add_argument("--seed", default=DEFAULT_SEED)
    ap.add_argument("--dump-exposed", help="write the rows chosen as the exposed items of the new kinds to this file "
                                           "(used once to embed them as EXPOSED_V3_TEXT; nothing is chosen when they are embedded)")
    args = ap.parse_args(argv)
    cats = Catalogues.load(args.catalogues)
    missing = [n for n in ("kafka_config", "protocol_errors", "connect_rest", "argocd", "kafka_cli", "strimzi_annotations")
               if not cats.data[n]]
    if missing:
        print("FAIL: catalogues not built: %s (run scripts/build_catalogues.py)" % ", ".join(missing), file=sys.stderr)
        return 2
    chash = catalogue_hash(args.catalogues)
    g = generate(cats, args.seed, chash)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("".join(json.dumps(i, sort_keys=True, ensure_ascii=False) + "\n" for i in g.items))
    if args.dump_exposed:
        emb = {r["id"] for r in EXPOSED_ALL_ROWS()}
        rows = [i for i in g.items if i["exposed"] and i["kind"] in NEW_KINDS and i["id"] not in emb]
        Path(args.dump_exposed).write_text("".join(json.dumps(i, sort_keys=True, ensure_ascii=False) + "\n" for i in rows))
        print("wrote %d exposed rows of the new kinds to %s" % (len(rows), args.dump_exposed))
    print("catalogue hash %s  seed %s" % (chash, args.seed))
    print(tables(g))
    print("wrote %s" % out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
