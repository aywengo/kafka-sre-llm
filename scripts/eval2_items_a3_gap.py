#!/usr/bin/env python3
"""Not-covered items of the a3 slice of tier A (WP1c T7): gaps and their controls (12 and 12 planned).

A gap asks for something the frozen index cannot answer; the right answer says the documentation does not cover it
and gives no value (checks `declines` and `no_invented_value` of scripts/eval2_checks_a3.py). Its control is the near
twin whose fact is in the index and in the catalogue; the right answer gives it (`answer_equals`). A gap and its
control share `pair`, so the split puts them on one side (scripts/eval2_items_a3.py `group_of`).

Three templates, four pairs each (the gap and the control of a template share its id, so a template has 8 items):
  nc_old_version   G1  a setting of Kafka 3.x that the 4.x pages no longer have, asked for the 3.x release; the control
                       is a current key asked for 4.3. A gap key must also be UNKNOWN in the 4.3 catalogue of its scope.
  nc_deployment    G2  a value of the owner's own deployment (a named topic or cluster); the control is the documented
                       default of the same setting.
  nc_other_product G3  a setting of a product the sources do not cover (Confluent Schema Registry, Redpanda); the control
                       is a Kafka 4.3 broker key.

Version 2 (review findings S5, B5, B4, M3): G1 keys are ones whose existence in the asked release (3.9) a pinned source
shows, a removal note of the 4.0 upgrade notes (`premise`: file, line, phrase, anchor-checked); the notes are in the index,
so for those keys the only chunks allowed to hit are chunks of that file, and the lines naming the key must not state a
default. A perturbation that gives a value remembered from the old release is labelled `unsupported_value` (it may be true;
the sources do not support it). G3 gaps also search the product name together with the concept (`cooc`); G2 gaps the
deployment name together with the key. The Schema Registry compatibility level pair is replaced (the Apicurio pages in the
index state its default). Controls accept the documented value with the unit the documentation itself uses.

Absence record (D3, the Owner's go K5): the generator reads the frozen index, read-only, after checking its sha256
against runs/v3/INDEX_FROZEN.json (scorer's ChunkIndex.load; a mismatch stops with IndexMismatch). Each gap lists its
search terms and the number of chunks that hit each. A chunk hits a term when the term occurs in its text, title or
path (a) exactly, (b) ignoring case, or (c) after dropping every character that is not a letter or digit from both
sides, so `quota.consumer.default`, `quota_consumer_default`, `quota-consumer-default` and `quota consumer default` all
count. All 6444 chunks of every source (documentation, runbooks, Karapace, Apicurio) are searched. A pair whose gap
term has a hit is dropped and counted, never edited. Terms are the setting name (G1, G3), the product name when it
has no hit (Redpanda; `kafkastore` as a bare prefix would also hit "Kafka stores" under rule (c), so it is not used) and
the deployment name (G2).

Control facts: the catalogue entry (kafka_config: scope, key, default) must exist with a plain default, and a chunk of
the index must hold the key's heading and its `Default:` line with that same default. Else the pair is dropped.

No model (D6). One random stream is not needed: nothing is drawn; the order is the order of SPECS. Deterministic under
any PYTHONHASHSEED. `generate()` returns the items and writes no file. Run: .venv/bin/python scripts/eval2_items_a3_gap.py
"""
import json
import re
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import eval2_items_a1 as a1  # noqa: E402
import eval2_items_a3_order as order  # noqa: E402
import eval2_score as sc  # noqa: E402
from eval2_checks_a3 import check_declines, check_no_invented_value  # noqa: E402
from kafka_sre_rag import catalogue_a3  # noqa: E402
from kafka_sre_rag.catalogue import Catalogues, catalogue_hash, default_dir  # noqa: E402

VERSION = "eval2_items_a3_gap/2"
V2_ID_BASE = 100  # version 2 changed every item (keys, checks, controls): numbers start at 101, no version-1 id is reused
UPGRADE = "raw/apache-kafka/docs/getting-started/upgrade.md"
PINNED_VERSIONS = ["4.1", "4.2", "4.3", "4.3.2"]
DEFAULT_SEED = "m4-a3-1"
KIND = "not_covered"
INDEX_PATH = ROOT / sc.DEFAULT_INDEX
FROZEN_PATH = ROOT / sc.DEFAULT_FROZEN
HIT_RULE = ("a chunk hits a term when the term is in its text, title or path exactly, ignoring case, or after dropping "
            "every non-alphanumeric character from both (all chunks of every source)")
DECLINE = "not in my sources"
FORMAT_NOTE = ("Finish with a final line `ANSWER: <value>`. Give the bare value. If the documentation you have does not "
               "cover it, finish with `ANSWER: %s`." % DECLINE)
SCOPE_WORD = {"broker": "broker", "streams": "Kafka Streams", "connect": "Connect worker", "topic": "topic-level"}

# gap spec: key, scope (where the real key lives; for G1/G2 the catalogue scope), terms, ask(gap question), invented
# (a plausible invented value), version/where words; control spec: scope, key, wrong (a wrong value).
SPECS = [
    # ---- G1: older version; every key is shown to exist in 3.9 by a removal note of the pinned 4.x upgrade notes
    {"template": "nc_old_version", "domain": "kafka", "kind": "old",
     "gap": {"key": "offsets.commit.required.acks", "scope": "broker", "version": "3.9", "terms": ["offsets.commit.required.acks"],
             "invented": "-1", "unsupported": True,
             "premise": {"file": UPGRADE, "line": 266, "phrase": "The `offsets.commit.required.acks` configuration was removed"}},
     "ctl": {"scope": "broker", "key": "quota.window.num", "wrong": "10"}},
    {"template": "nc_old_version", "domain": "kafka", "kind": "old",
     "gap": {"key": "log.message.timestamp.difference.max.ms", "scope": "broker", "version": "3.9",
             "terms": ["log.message.timestamp.difference.max.ms"], "invented": "9223372036854775807", "unsupported": True,
             "premise": {"file": UPGRADE, "line": 267, "phrase": "The `log.message.timestamp.difference.max.ms` configuration was removed"}},
     "ctl": {"scope": "broker", "key": "metrics.num.samples", "wrong": "5"}},
    {"template": "nc_old_version", "domain": "kafka", "kind": "old",
     "gap": {"key": "auto.include.jmx.reporter", "scope": "broker", "scope_word": "metrics", "version": "3.9",
             "terms": ["auto.include.jmx.reporter"], "invented": "true", "unsupported": True,
             "premise": {"file": UPGRADE, "line": 251, "phrase": "The `auto.include.jmx.reporter` configuration was removed"}},
     "ctl": {"scope": "streams", "key": "poll.ms", "wrong": "1000"}},
    {"template": "nc_old_version", "domain": "connect", "kind": "old",
     "gap": {"key": "add.source.alias.to.metrics", "scope": "connect", "scope_word": "MirrorMaker 2 source connector",
             "version": "3.9", "terms": ["add.source.alias.to.metrics"], "invented": "true", "unsupported": True,
             "premise": {"file": UPGRADE, "line": 275, "phrase": "The `add.source.alias.to.metrics` configuration was removed"}},
     "ctl": {"scope": "connect", "key": "offset.flush.timeout.ms", "wrong": "10000"}},
    # ---- G2: a value of the owner's deployment
    {"template": "nc_deployment", "domain": "kafka", "kind": "deploy",
     "gap": {"key": "retention.ms", "scope": "topic", "name": "payments-ledger", "noun": "production topic",
             "terms": ["payments-ledger"], "cooc": ["payments-ledger", "retention.ms"], "invented": "2592000000"},
     "ctl": {"scope": "topic", "key": "retention.ms", "wrong": "86400000"}},
    {"template": "nc_deployment", "domain": "kafka", "kind": "deploy",
     "gap": {"key": "min.insync.replicas", "scope": "topic", "name": "fraud-scoring", "noun": "production topic",
             "terms": ["fraud-scoring"], "cooc": ["fraud-scoring", "min.insync.replicas"], "invented": "2"},
     "ctl": {"scope": "topic", "key": "min.insync.replicas", "wrong": "2"}},
    {"template": "nc_deployment", "domain": "kafka", "kind": "deploy",
     "gap": {"key": "log.retention.hours", "scope": "broker", "name": "prod-eu1", "noun": "cluster",
             "terms": ["prod-eu1"], "cooc": ["prod-eu1", "log.retention.hours"], "invented": "72"},
     "ctl": {"scope": "broker", "key": "log.retention.hours", "wrong": "24"}},
    {"template": "nc_deployment", "domain": "connect", "kind": "deploy",
     "gap": {"key": "offset.flush.interval.ms", "scope": "connect", "name": "billing-cluster", "noun": "cluster",
             "terms": ["billing-cluster"], "cooc": ["billing-cluster", "offset.flush.interval.ms"], "invented": "10000"},
     "ctl": {"scope": "connect", "key": "offset.flush.interval.ms", "wrong": "10000"}},
    # ---- G3: a product the sources do not cover (`cooc`: the product name together with the concept)
    {"template": "nc_other_product", "domain": "kafka", "kind": "other",
     "gap": {"key": "kafkastore.topic.replication.factor", "product": "Confluent Schema Registry",
             "terms": ["kafkastore.topic.replication.factor"], "cooc": ["Confluent Schema Registry", "replication factor"],
             "invented": "1"},
     "ctl": {"scope": "broker", "key": "offsets.topic.replication.factor", "wrong": "1"}},
    {"template": "nc_other_product", "domain": "kafka", "kind": "other",
     "gap": {"key": "kafkastore.timeout.ms", "product": "Confluent Schema Registry",
             "terms": ["kafkastore.timeout.ms"], "cooc": ["Confluent Schema Registry", "timeout"], "invented": "60000"},
     "ctl": {"scope": "broker", "key": "replica.socket.timeout.ms", "wrong": "60000"}},
    {"template": "nc_other_product", "domain": "kafka", "kind": "other",
     "gap": {"key": "kafkastore.init.timeout.ms", "product": "Confluent Schema Registry",
             "terms": ["kafkastore.init.timeout.ms"], "cooc": ["Confluent Schema Registry", "init timeout"], "invented": "30000"},
     "ctl": {"scope": "broker", "key": "controller.socket.timeout.ms", "wrong": "60000"}},
    {"template": "nc_other_product", "domain": "kafka", "kind": "other",
     "gap": {"key": "kafka_batch_max_bytes", "product": "Redpanda", "terms": ["kafka_batch_max_bytes", "redpanda"],
             "cooc": ["Redpanda", "batch max bytes"], "invented": "1048576"},
     "ctl": {"scope": "broker", "key": "message.max.bytes", "wrong": "1048576"}},
]
UNANCHORED = []  # every gap is anchored by its absence record, every control by catalogue and index


# ------------------------------------------------------------------ the index
def load_index():
    """The frozen index, read-only, hash checked against runs/v3/INDEX_FROZEN.json (IndexMismatch on a difference)."""
    idx = sc.ChunkIndex.load(INDEX_PATH, FROZEN_PATH)
    return idx, json.loads(FROZEN_PATH.read_text())["sha256"]


def alnum(s):
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


class Searcher:
    """Term search over every chunk (text, title, path): exact, case-insensitive, separator-insensitive."""

    def __init__(self, rows):
        self.rows = rows
        self.fields = [(r.get("text") or "", r.get("title") or "", r.get("path") or "") for r in rows]
        self.low = [tuple(f.lower() for f in fs) for fs in self.fields]
        self.flat = [tuple(alnum(f) for f in fs) for fs in self.fields]

    def hits(self, term):
        """{"exact": n, "case_insensitive": n, "separator_insensitive": n}, each a number of chunks."""
        tl, tn = term.lower(), alnum(term)
        assert tn, "empty term"
        out = {"exact": 0, "case_insensitive": 0, "separator_insensitive": 0}
        for ex, lo, fl in zip(self.fields, self.low, self.flat):
            if any(term in f for f in ex):
                out["exact"] += 1
            if any(tl in f for f in lo):
                out["case_insensitive"] += 1
            if any(tn in f for f in fl):
                out["separator_insensitive"] += 1
        return out


    def cooccur(self, a, b, skip_path=None):
        """Number of chunks (not of the file `skip_path`) holding both `a` and `b` (separator-insensitive, any of
        text, title, path)."""
        na, nb = alnum(a), alnum(b)
        assert na and nb, "empty term"
        n = 0
        for r, fl in zip(self.rows, self.flat):
            if skip_path and r.get("path") == skip_path:
                continue
            joined = " ".join(fl)
            if na in joined and nb in joined:
                n += 1
        return n

    def hit_chunks(self, term):
        """The rows of the chunks that hit `term` under any of the three rules."""
        tn = alnum(term)
        return [r for r, fl in zip(self.rows, self.flat) if any(tn in f for f in fl)]


def premise_check(searcher, spec_gap, root=ROOT):
    """For a gap whose existence is shown by a pinned note (`premise`): (ok, why, chunk ids). The anchor must hold; the
    chunks that hit the key must all be chunks of the premise file; no sentence that names the key may state a default."""
    pr = spec_gap["premise"]
    ok, why = order.verify_anchor(pr["file"], pr["line"], pr["phrase"], root)
    if not ok:
        return False, "premise anchor: " + why, []
    ids = []
    for r in searcher.hit_chunks(spec_gap["key"]):
        if r.get("path") != pr["file"]:
            return False, "a chunk of %s mentions the key" % r.get("path"), []
        for sent in re.split(r"(?<=[.!?])\s+|\n", r["text"]):
            if alnum(spec_gap["key"]) in alnum(sent) and re.search(r"default", sent, re.I):
                return False, "a sentence naming the key states a default", []
        ids.append(r["id"])
    return True, "", ids


def absence_record(searcher, terms, index_sha, cooc=None, premise_path=None):
    """Hits per term (chunks of `premise_path` are not counted: for a removed key they are the premise, checked by
    `premise_check`); `cooc` = [product, concept]: chunks holding both."""
    per = {t: searcher.hits(t) for t in terms}
    total = sum(sum(h.values()) for h in per.values())
    rec = {"terms": list(terms), "hits": total, "hits_per_term": per, "hit_rule": HIT_RULE,
           "chunks_searched": len(searcher.rows), "index_sha256": index_sha}
    if cooc:
        n = searcher.cooccur(cooc[0], cooc[1], premise_path)
        rec["cooccurrence"] = {"a": cooc[0], "b": cooc[1], "chunks_with_both": n}
        rec["hits"] += n
    return rec


def index_fact(idx, entry):
    """The id of a chunk of the entry's own file that holds the key's section and its `Default:` line with the
    entry's default, or None. A key's section is either a `## key` heading inside a chunk, or the start of a chunk
    whose title is the key (the chunker puts a section's own heading into the title)."""
    want = entry.get("default") or entry.get("default_value")
    for c in idx.by_path.get(entry["source_file"], []):
        t = c["text"]
        m = re.search(r"(?m)^## " + re.escape(entry["key"]) + r"\n", t)
        if m:
            rest = t[m.end():]
        elif c.get("title") == entry["key"]:
            rest = t
        else:
            continue
        nxt = re.search(r"(?m)^## ", rest)
        rest = rest[:nxt.start()] if nxt else rest
        d = re.search(r"(?m)^Default:\n(.*)$", rest)
        if d and d.group(1).strip() == want:
            return c["id"]
    return None


# ------------------------------------------------------------------ texts
def ticks(s):
    return "`%s`" % s


def questions(spec):
    g, c, k = spec["gap"], spec["ctl"], spec["kind"]
    if k == "old":
        return (("Kafka %s: what was the default of the %s setting `%s` in that release?" % (g["version"], g.get("scope_word") or SCOPE_WORD[g["scope"]], g["key"])),
                ("Kafka 4.3: what is the default of the %s setting `%s` in that release?" % (SCOPE_WORD[c["scope"]], c["key"])))
    if k == "deploy":
        if g["scope"] == "topic":
            gq = "Which value does our %s `%s` have for the topic-level setting `%s`?" % (g["noun"], g["name"], g["key"])
            cq = "Which value does Kafka 4.3 give the topic-level setting `%s` when a topic does not set it?" % c["key"]
        elif g["scope"] == "broker":
            gq = "Which value of the broker setting `%s` do the brokers of our %s `%s` run with?" % (g["key"], g["noun"], g["name"])
            cq = "Which value of the broker setting `%s` does Kafka 4.3 use when the broker file does not set it?" % c["key"]
        else:
            gq = "Which value of the Connect worker setting `%s` do the workers of our %s `%s` run with?" % (g["key"], g["noun"], g["name"])
            cq = "Which value of the Connect worker setting `%s` does Kafka 4.3 use when the worker file does not set it?" % c["key"]
        return gq, cq
    return (("In %s, what does the setting `%s` default to?" % (g["product"], g["key"])),
            ("In Apache Kafka 4.3, what does the %s setting `%s` default to?" % (SCOPE_WORD[c["scope"]], c["key"])))


def gap_reference(spec):
    g, k = spec["gap"], spec["kind"]
    if k == "old":
        s = ("The documentation I have does not cover `%s` for Kafka %s, so I cannot give the default it had then."
             % (g["key"], g["version"]))
    elif k == "deploy":
        s = ("The value of `%s` for `%s` belongs to your own deployment; the documentation I have does not cover it."
             % (g["key"], g["name"]))
    else:
        s = ("The documentation I have does not cover %s settings such as `%s`, so I cannot give its default."
             % (g["product"], g["key"]))
    return "%s\nANSWER: %s" % (s, DECLINE)


def numbers_of(text):
    return sorted({n.strip(",") for n in re.findall(r"(?<![\w.-])\d[\d,]*(?:\.\d+)*(?![\w-])", text)})


def backticked(text):
    return sorted(set(re.findall(r"`([^`\n]+)`", text)))


# ------------------------------------------------------------------ items
def cfg_entry(cats, scope, key):
    for e in cats.data["kafka_config"]["entries"]:
        if e["scope"] == scope and e["key"] == key:
            return e
    return None


UNIT_SUFFIX = {".ms": ("ms", "milliseconds"), ".hours": ("hours",), ".minutes": ("minutes",), ".seconds": ("seconds",),
               ".bytes": ("bytes",)}


def control_forms(e):
    """The accepted forms of a control: the bare default, the documentation's own forms (`10000 (10 seconds)` and its
    gloss), and, when the key name carries the unit the documentation uses (.ms, .hours, .minutes, .seconds, .bytes),
    the bare number with that unit ("100 ms", "168 hours"): the same rule for every entry."""
    forms = a1.default_forms(e)
    dv = e.get("default_value") or ""
    for suf, units in UNIT_SUFFIX.items():
        if e["key"].endswith(suf) and re.fullmatch(r"-?\d+", dv):
            forms += ["%s %s" % (dv, u) for u in units]
    return forms


def common(spec, role, n, q, chash, chash_a3, seed, dd, pair):
    t = spec["template"]
    key = spec["gap"]["key"] if role == "gap" else spec["ctl"]["key"]
    return {"id": "a3-%s-%03d" % (t, n + V2_ID_BASE), "tier": "A", "kind": KIND, "family": "%s:%s:%s" % (t, role, key),
            "domain": spec["domain"], "question": q + "\n\n" + FORMAT_NOTE,
            "template": t, "pair": pair, "anchored": True,
            "failure_class": "not_covered" if role == "gap" else "plain_lookup", "exposed": False, "overlap": None,
            "overlap_band": "none", "catalogue_hash": chash, "catalogue_hash_a3": chash_a3, "seed": seed,
            "generator": VERSION, "dedup": dd, "chance": 1.0 if role == "gap" else 0.0}


def build_pair(spec, n_gap, cats, idx, searcher, index_sha, chash, chash_a3, seed, corpus, stats):
    """(gap item, control item), or None with the reason counted."""
    t, g, c = spec["template"], spec["gap"], spec["ctl"]
    tag = "%s:%s" % (t, g["key"])
    # -- the gap: absence record, and for G1 the key must be unknown to the 4.3 catalogue of its scope
    rec = absence_record(searcher, g["terms"], index_sha, g.get("cooc"))
    if g.get("premise"):
        ok, why, ids = premise_check(searcher, g)
        if not ok:
            stats["dropped:%s: premise: %s" % (tag, why)] += 1
            return None
        pr = g["premise"]
        rec["premise"] = {"file": pr["file"], "line": pr["line"], "phrase": pr["phrase"], "chunk_ids": ids,
                          "hits_in_premise_file": rec["hits"],
                          "rule": "the key is in the removal note of the pinned upgrade notes, which shows it existed in "
                                  "%s; no other chunk mentions it and the sentences naming it state no default" % g["version"]}
        rec["hits"] = 0
    if rec["hits"]:
        stats["dropped:%s: the terms have %d chunk hits in the index" % (tag, rec["hits"])] += 1
        return None
    if spec["kind"] == "old" and cats.config_key(g["key"], g["scope"]).status != "unknown":
        stats["dropped:%s: the key is not unknown to the 4.3 catalogue of scope %s" % (tag, g["scope"])] += 1
        return None
    # -- the control: catalogue entry with a plain default, and the same fact in an index chunk
    e = cfg_entry(cats, c["scope"], c["key"])
    if e is None or not a1.plain_default(e.get("default_value")):
        stats["dropped:%s: control %s/%s has no plain default in the catalogue" % (tag, c["scope"], c["key"])] += 1
        return None
    cid = index_fact(idx, e)
    if cid is None:
        stats["dropped:%s: control %s/%s: no index chunk holds its Default line" % (tag, c["scope"], c["key"])] += 1
        return None
    gq, cq = questions(spec)
    pair = "nc:%s:%s" % (t[3:], g["key"])
    # near-duplicates of an eval, training or open question
    ghit, gdd = order.near_duplicate_of(gq, corpus)
    chit, cdd = order.near_duplicate_of(cq, corpus)
    if ghit or chit:
        stats["dropped:%s: near_duplicate (%s)" % (tag, "gap" if ghit else "control")] += 1
        return None
    # ---- gap item
    gi = common(spec, "gap", n_gap, gq, chash, chash_a3, seed, gdd, pair)
    allowed_n = numbers_of(gq)
    allowed_i = backticked(gq)
    ge = cfg_entry(cats, g["scope"], g["key"]) if g.get("scope") else None
    asked = [g["key"]] + [g[k] for k in ("name", "product") if g.get(k)]
    dchk = {"type": "declines"}
    nchk = {"type": "no_invented_value", "allowed_numbers": allowed_n, "allowed_identifiers": allowed_i, "asked": asked,
            "default_numbers": numbers_of((ge or {}).get("default_value") or "") if ge and spec["kind"] == "deploy" else [],
            "pinned_versions": PINNED_VERSIONS}
    gi["checks"] = [{"format": True, "type": "answer_line"}, dchk, nchk]
    ref = gap_reference(spec)
    invented = "The default is %s.\nANSWER: %s" % (g["invented"], g["invented"]) if spec["kind"] != "deploy" else \
        "It is set to %s there.\nANSWER: %s" % (g["invented"], g["invented"])
    confident = "That setting has a sensible built-in value that applies unless it is overridden.\nANSWER: the built-in value"
    gi["reference"] = ref
    gi["perturbed"] = [
        {"label": "unsupported_value" if g.get("unsupported") else "invented_value", "text": invented, "violates": 2, "check": "no_invented_value"},
        {"label": "confident_no_decline", "text": confident, "violates": 1, "check": "declines"},
    ]
    gi["perturbed_reference"], gi["perturbed_violates"] = invented, 2
    gi["versions"] = {"kafka": g["version"]} if spec["kind"] == "old" else (
        {"kafka": "4.3"} if spec["kind"] == "deploy" else {"kafka": "4.3"})
    gi["sources"] = [dict({"catalogue": "frozen_index", "fact": "no chunk of the frozen index mentions the terms",
                           "file": "work/v3/chunks_s1.jsonl", "line": None, "absent": ", ".join(g["terms"])}, **rec)]
    if spec["kind"] == "old":
        gi["sources"].append({"catalogue": "kafka_config", "fact": "the key is not in the Kafka 4.3 reference of scope %s" % g["scope"],
                              "entry": None, "file": None, "line": None, "absent": g["key"], "scope": g["scope"]})
    # ---- control item
    ci = common(spec, "ctl", n_gap + 1, cq, chash, chash_a3, seed, cdd, pair)
    forms = control_forms(e)
    ci["checks"] = [{"format": True, "type": "answer_line"}, {"type": "answer_equals", "accepted": forms, "norm": "ci"}]
    ci["reference"] = "The default of `%s` is %s.\nANSWER: %s" % (c["key"], forms[0], forms[0])  # the bare value first
    wrong = "The default of `%s` is %s.\nANSWER: %s" % (c["key"], c["wrong"], c["wrong"])
    declined = "The documentation I have does not cover this.\nANSWER: %s" % DECLINE
    ci["perturbed"] = [
        {"label": "decline", "text": declined, "violates": 1, "check": "answer_equals"},
        {"label": "wrong_value", "text": wrong, "violates": 1, "check": "answer_equals"},
    ]
    ci["perturbed_reference"], ci["perturbed_violates"] = declined, 1
    ci["versions"] = {"kafka": "4.3"}
    ci["sources"] = [a1.cfg_src(e, "default %s" % e["default"]),
                     {"catalogue": "frozen_index", "fact": "chunk holds the key heading and its Default line",
                      "file": e["source_file"], "line": None, "chunk_id": cid, "index_sha256": index_sha}]
    # ---- every reference passes its checks; every perturbed answer fails the named check
    for it in (gi, ci):
        bad = [r["type"] for r in sc.run_checks(it, it["reference"], cats) if not r["pass"]]
        if bad:
            stats["derivation_failed:%s: the reference of %s fails %s" % (tag, it["id"], ",".join(bad))] += 1
            return None
        for p in it["perturbed"]:
            res = sc.run_checks(it, p["text"], cats)
            if res[p["violates"]]["pass"] or res[p["violates"]]["type"] != p["check"]:
                stats["derivation_failed:%s: the %s answer of %s passes %s" % (tag, p["label"], it["id"], p["check"])] += 1
                return None
    return gi, ci


def generate_with_stats(cats=None, seed=DEFAULT_SEED, corpus=None, idx=None):
    cats = cats or Catalogues.load()
    chash = catalogue_hash(default_dir())
    chash_a3 = catalogue_a3.manifest()["catalogue_hash"]
    corpus = order.load_corpus() if corpus is None else corpus
    if idx is None:
        idx, index_sha = load_index()
    else:
        index_sha = json.loads(FROZEN_PATH.read_text())["sha256"]
    searcher = Searcher(idx.rows)
    stats = Counter()
    items, counters = [], Counter()
    for spec in SPECS:
        t = spec["template"]
        pr = build_pair(spec, counters[t] + 1, cats, idx, searcher, index_sha, chash, chash_a3, seed, corpus, stats)
        if pr is None:
            continue
        counters[t] += 2
        items += list(pr)
    items.sort(key=lambda i: i["id"])
    stats["items"] = len(items)
    return items, stats


def generate(cats=None, seed=DEFAULT_SEED):
    return generate_with_stats(cats, seed)[0]


def main():
    items, stats = generate_with_stats()
    gaps = [i for i in items if i["failure_class"] == "not_covered"]
    ctls = [i for i in items if i["failure_class"] != "not_covered"]
    print("%s: %d gaps (plan 12) and %d controls (plan 12)" % (VERSION, len(gaps), len(ctls)))
    for k in sorted(stats):
        if k != "items":
            print("  %s x%d" % (k, stats[k]))
    best = order.max_similarity(items, None)
    print("highest similarity to an eval, training or open question: jaccard %.3f, containment %.3f (%s, item %s)" % best)
    print("families: %d; templates: %s; pairs: %d" % (len({i["family"] for i in items}),
                                                      sorted({i["template"] for i in items}), len({i["pair"] for i in items})))
    for g in gaps:
        print("  %s terms %s hits %d" % (g["id"], g["sources"][0]["terms"], g["sources"][0]["hits"]))


if __name__ == "__main__":
    main()
