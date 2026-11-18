#!/usr/bin/env python3
"""Generate the first tier A items of the M4 evaluation (slice A1). No model is involved anywhere.

Truth comes from the catalogues (work/m4/catalogues, built by scripts/build_catalogues.py), never
from a model. Kinds: name_setting, is_real, connect_rest, argocd_command, error_name.

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
  - is_real is at most 10 % of the items;
  - deterministic for a seed and the catalogue hash; the hash is written into every item.

Usage: .venv/bin/python scripts/eval2_items_a1.py [--catalogues DIR] [--out FILE] [--seed S]
"""
import argparse
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
import eval2_score as sc  # noqa: E402
from kafka_sre_rag.catalogue import Catalogues, catalogue_hash  # noqa: E402

VERSION = "eval2_items_a1/1"
DEFAULT_SEED = "m4-a1-1"
DEFAULT_OUT = ROOT / "eval" / "m4" / "build" / "tier_a1.jsonl"

MAX_PER_TEMPLATE = 10
FAMILY_CAP = 2  # items of one family inside one template
JACCARD_T = 0.5
CONTAIN_T = 0.8
MIN_TOKENS = 8
EXPOSED_N = 20
REAL_SHARE = 0.10  # is_real is at most this share of all items
OVERLAP_BANDS = ((0.5, "high"), (0.25, "mid"))  # q_in_desc >= 0.5 high, >= 0.25 mid, else low
CLASS_CAPS = {"near_miss_name": 24, "wrong_scope": 22, "scope_dependent": 12, "trap_method": 12,
              "not_covered": 8, "confusable_neighbour": 14}
HARD_FIRST = ("scope_dependent", "wrong_scope", "confusable_neighbour", "near_miss_name", "trap_method",
              "not_covered")
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
        n = self.template_counts[tpl] + 1
        item = {
            "id": "a1-%s-%03d" % (tpl, n), "tier": "A", "kind": kind, "family": fam, "domain": d["domain"],
            "question": d["core"] + "\n\n" + d["fmt"], "checks": d["checks"], "reference": ref,
            "sources": d["sources"], "versions": d["versions"], "template": tpl, "anchored": d["anchored"],
            "failure_class": fc, "exposed": False, "overlap": ov, "overlap_band": band(ov),
            "catalogue_hash": self.chash, "seed": self.seed, "generator": VERSION, "dedup": dd,
            "perturbed_reference": d["perturbed"], "perturbed_violates": d["violates"],
        }
        self.items.append(item)
        self.cores.add(d["core"])
        self.class_counts[fc] += 1
        self.template_counts[tpl] += 1
        self.family_counts[fam] += 1
        st["kept"] += 1
        return True

    def fill(self, drafts, n, fam_cap=FAMILY_CAP):
        """Keep up to n drafts: shuffled, hard failure classes first, family cap, class caps."""
        drafts = list(drafts)
        self.rng.shuffle(drafts)
        order = {c: i for i, c in enumerate(HARD_FIRST)}
        drafts.sort(key=lambda d: order.get(d["failure_class"], len(order)))
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
            "b": "I'm looking for a Kafka %s setting and all I remember is its description: \"%s\" What is the "
                 "exact name of the setting?" % (sh, ex),
            "c": "Which Kafka %s config property matches this documentation text (Kafka 4.3)? %s" % (sh, ex),
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
            "a": "Which values does the Kafka %s setting `%s` accept (Kafka 4.3 reference)? Give all of them, "
                 "separated by commas." % (sh, k),
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


def near_misses(g, e, all_keys):
    """Controlled edits of a real key that are not in the catalogue in any scope: (fake, how)."""
    out = []
    segs = re.split(r"(\.)", e["key"])
    for a, b in SWAPS:
        for x, y in ((a, b), (b, a)):
            for i, s in enumerate(segs):
                if s == x:
                    fake = "".join(segs[:i] + [y] + segs[i + 1:])
                    out.append((fake, "swapped word %s -> %s" % (x, y)))
    for a, b in UNIT_SWAPS:
        if e["key"].endswith(a):
            out.append((e["key"][:-len(a)] + b, "wrong unit suffix %s -> %s" % (a, b)))
    res, seen = [], set()
    for fake, how in out:
        if fake in all_keys or fake in seen:
            continue
        if g.cats.config_key(fake, e["scope"]).status != "unknown" or g.cats.config_key(fake).status != "unknown":
            continue
        seen.add(fake)
        res.append((fake, how))
    return res


def drafts_real_key(g, want_exists, want_fake):
    ents = [e for e in cfg_entries(g.cats) if e["scope"] in SCOPE_OPTIONS]
    all_keys = {e["key"] for e in g.cats.data["kafka_config"]["entries"]}
    out_e, out_f = [], []
    for e in ents:
        sh, k = SCOPE_H[e["scope"]], e["key"]
        core = "Is `%s` a real configuration key of the Kafka 4.3 %s configuration?" % (k, sh)
        out_e.append(real_draft("real_key_plain", cfg_group(e), cfg_domain(e["scope"]), "plain_lookup", core, "exists",
                                [cfg_src(e, "key exists in scope %s" % e["scope"])], True, e["description"]))
        for fake, how in near_misses(g, e, all_keys)[:1]:
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


FAKE_FLAGS = {"kafka-topics.sh": ["--unparsed", "--partition-count", "--replicas", "--topic-name"],
              "kafka-consumer-groups.sh": ["--reset-lag", "--group-name", "--rewind"],
              "kafka-configs.sh": ["--set-config", "--entity", "--remove-key"],
              "kafka-console-consumer.sh": ["--from-start", "--group-name", "--topic-name"]}


def drafts_real_flag_nc(g):
    """Names from tools whose catalogue is not complete: the expected answer is `not in my sources`."""
    out_n, out_e = [], []
    for e in g.cats.data["kafka_cli"]["entries"]:
        if e.get("complete") is not False:
            continue
        tool = e["tool"]
        for fl in FAKE_FLAGS.get(tool, []):
            if g.cats.cli_option(tool, fl).status != "not_covered":
                continue
            core = "Is `%s` an option of `%s` in Kafka 4.3?" % (fl, tool)
            out_n.append(real_draft("real_flag_nc", tool, "kafka", "not_covered", core, "not in my sources",
                                    [{"catalogue": "kafka_cli", "entry": tool, "absent": fl, "file": e["source_file"],
                                      "line": None, "fact": "the sources show only the options seen in examples "
                                                            "(%d); the tool's full option list is not in them" % len(e["options"])}],
                                    False, None))
        for opt, o in sorted(e["options"].items())[:3]:
            if o["evidence"] == "seen_in_example" and opt not in ("--bootstrap-server",):
                core = "Is `%s` an option of `%s` in Kafka 4.3?" % (opt, tool)
                out_e.append(real_draft("real_flag_nc", tool, "kafka", "plain_lookup", core, "exists",
                                        [{"catalogue": "kafka_cli", "entry": "%s %s" % (tool, opt),
                                          "file": o["files"][0], "line": find_line(o["files"][0], re.escape(opt) + r"(?![\w-])"),
                                          "fact": "option used in a documented example"}], True, None))
    return out_n, out_e


def drafts_real_annotation_nc(g):
    out = []
    for name in ("strimzi.io/rebalance-timeout", "strimzi.io/pause-reconciliation-now", "strimzi.io/restart-all",
                 "strimzi.io/rolling-update"):
        if g.cats.annotation(name).status != "not_covered":
            continue
        core = "Is `%s` an annotation that Strimzi 1.3 reads or sets on its resources?" % name
        out.append(real_draft("real_annotation_nc", "strimzi", "strimzi", "not_covered", core, "not in my sources",
                              [{"catalogue": "strimzi_annotations", "absent": name,
                                "file": g.cats.data["strimzi_annotations"]["entries"][0]["source_file"], "line": None,
                                "fact": "the annotation catalogue lists only names the docs mention; it is not complete"}],
                              False, None, {"strimzi": "1.3.1"}))
    return out


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
        d = mk("argocd_command", template, group, "argocd", "wrong_scope" if trap else "plain_lookup", core,
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


# ------------------------------------------------------------------ assembly
def generate(cats, seed, chash):
    g = Gen(cats, seed, chash)
    for v, n in (("a", 10), ("b", 10), ("c", 10)):
        g.fill(drafts_key_from_desc(g, v), n)
    for v, n in (("a", 10), ("b", 10), ("c", 10)):
        g.fill(drafts_default(g, v), n)
    for v, n in (("a", 8), ("b", 8)):
        g.fill(drafts_values(g, v), n)
    for v, n in (("a", 6), ("b", 6)):
        g.fill(drafts_type(g, v), n)
    for v, n in (("a", 6), ("b", 6)):
        g.fill(drafts_scope(g, v), n)
    g.fill(drafts_rest(g, "rest_which_a"), 10)
    g.fill(drafts_rest(g, "rest_which_b"), 10)
    g.fill(drafts_rest(g, "rest_trap"), 10)
    for t in ARGO_TEMPLATES:
        g.fill(drafts_argo(g, t), 10, fam_cap=6)
    g.fill(drafts_err_desc(g, "a"), 10)
    g.fill(drafts_err_desc(g, "b"), 10)
    g.fill(drafts_err_retriable(g), 10)
    g.fill(drafts_err_mc(g), 10)
    # is_real: at most REAL_SHARE of the final items
    others = len(g.items)
    budget = int(others * REAL_SHARE / (1 - REAL_SHARE))
    plan = []
    ke, kf = drafts_real_key(g, 0, 0)
    se, sw = drafts_real_scope(g)
    ee, ef = drafts_real_error(g)
    re_, rw = drafts_real_rest(g)
    fn, fe = drafts_real_flag_nc(g)
    an = drafts_real_annotation_nc(g)
    plan = [(ke, 2), (kf, 4), (se, 1), (sw, 3), (ee, 1), (ef, 2), (re_, 1), (rw, 2), (fn, 3), (fe, 1), (an, 1)]
    total = 0
    for drafts, n in plan:
        n = min(n, budget - total)
        if n > 0 and drafts:
            total += g.fill(drafts, n, fam_cap=3)
    g.items.sort(key=lambda i: i["id"])
    rng = random.Random("%s|exposed" % seed)
    for i in rng.sample(range(len(g.items)), min(EXPOSED_N, len(g.items))):
        g.items[i]["exposed"] = True
    return g


def tables(g):
    items = g.items
    lines = []

    def tab(title, key):
        c = Counter(i[key] for i in items)
        lines.append("%s:\n%s" % (title, "\n".join("  %-26s %3d" % kv for kv in sorted(c.items()))))
    lines.append("items: %d  (anchored %d, exposed %d)" % (len(items), sum(i["anchored"] for i in items),
                                                         sum(i["exposed"] for i in items)))
    tab("per kind", "kind")
    tab("per domain", "domain")
    tab("per failure class", "failure_class")
    tab("per template", "template")
    tab("per overlap band (question words found in the catalogue description)", "overlap_band")
    lines.append("families: %d" % len({i["family"] for i in items}))
    lines.append("drop and repair counts per kind (candidates tried / kept / repaired; drops by reason):")
    for kind in sorted(g.stats):
        st = g.stats[kind]
        drops = ", ".join("%s %d" % kv for kv in sorted(g.drop_reasons[kind].items())) or "none"
        lines.append("  %-16s tried %4d  kept %3d  repaired %d  skipped_by_caps %d  dropped: %s" % (
            kind, st["built"], st["kept"], st["repaired"], st["skipped_cap"], drops))
    real = sum(1 for i in items if i["kind"] == "is_real")
    lines.append("is_real share: %d of %d = %.1f %% (limit %.0f %%)" % (real, len(items), 100.0 * real / len(items), 100 * REAL_SHARE))
    lines.append("near-duplicate corpus: %d texts from %d files; thresholds Jaccard >= %.2f or containment >= %.2f (both sets >= %d tokens)" % (
        len(g.corpus), len(g.corpus_files), JACCARD_T, CONTAIN_T, MIN_TOKENS))
    mx = max((i["dedup"]["max_jaccard"] for i in items), default=0)
    lines.append("highest Jaccard of a kept item against the corpus: %.3f" % mx)
    return "\n".join(lines)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--catalogues", help="catalogue directory (default work/m4/catalogues)")
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    ap.add_argument("--seed", default=DEFAULT_SEED)
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
    print("catalogue hash %s  seed %s" % (chash, args.seed))
    print(tables(g))
    print("wrote %s" % out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
