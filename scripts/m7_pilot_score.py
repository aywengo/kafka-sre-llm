#!/usr/bin/env python3
"""M7 pilot chain (tasks K3, P2 and P3 of agents/m7-plan.md): the helpers of scripts/m7_pilot.sh. CPU only; no model, no server.

The pilot (fixed by the conductor on 2026-11-23, runs/m7/reports/k1_check_design.md section 5):
  Stage T, thinking: 20 scenarios (the first 10 ids of B1 and of B2 by sha256 of the id), the default prompt on the STORED packs of the stored default answers (replay, no
    search call), thinking on, no check. Measures: does the 4-bit build honour the switch, thinking tokens (median, 90th percentile, maximum), empty drafts, cut drafts,
    drafts cut while thinking, seconds per part. No bar; it sets the thinking cap of the six-arm test. Then the checking pass (variant V1) on these drafts, mechanical measures only.
  Stage V, the checking pass on stored drafts (written without thinking by the same model): V1 (no cards) and V1C (cards of the draft in the check) on B1 (default and strict),
    pilot2 (default and strict) and T (default and grounded-g2); V1 only on B2 (default and strict).
  Stage S, scoring by code (this file): a catch is an accepted change on a line or command unit that holds a finding's quote (normalised as kafka_sre_rag.verify_pass.norm:
    whitespace collapsed, ** and backticks removed; quotes that are not in the draft are counted and left out). Per variant and set: changes proposed, accepted and rejected (with
    the rules), answers changed, catches, accepted changes on lines no finding names (for a reader), new destructive matches accepted, valid JSON and cut replies, added seconds.
    The five bars print with PASS, FAIL or "needs the reading". Bars (1) and (2) need a reader's count of damaged correct lines (--damage); the code decides nothing else and never prints a
    final choice.

Findings used (open development data only; never D5, the hidden part or the legacy sets):
  B1:     work/m4/b1/findings_unblinded.jsonl, reader r1, minor false, kind wrong, invented or harmful (90 rows, file order; 14 of them are the catchable subset, class B of
          runs/m4/reports/wp2b_revise_design.md, appendix: row positions CATCHABLE_B1 below, checked against set, id and the start of the quote).
  pilot2: runs/m4/S5B_FINDINGS.jsonl, kind wrong, invented or harmful (111 rows).
  T:      work/m6/t/reading/harmful_findings_unblinded.jsonl (28 harmful statements; no other class).
  B2:     no findings; every accepted change goes to a reader.
Subcommands: gate-files, input-files, runs, select-think, think-drafts, think-measures, check-think, check-verify, manifest, start-check, score, header-text, code-pins, done-line.
"""
import argparse
import datetime
import hashlib
import json
import math
import os
import re
import sys
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(ROOT))
os.environ.setdefault("HF_HUB_OFFLINE", "1")
import m5_g_rule as G  # noqa: E402
import m6_d5_rule as D  # noqa: E402

SCHEMA = 1
VARIANTS = ("V1", "V1C")
THINK_PER_SET = 10
THINK_SET = "THINK"
CODE_PINS = tuple(p for p in D.CODE_PINS if not p.endswith("m6_d5_answers.sh")) + (
    "kafka_sre_rag/verify_pass.py", "kafka_sre_rag/kb_lookup.py", "scripts/m7_pilot_score.py", "scripts/m7_pilot.sh")

# the set table; the paths are relative to the repository root. n: answers per arm; findings kind: b1_r1, s5b, t_harmful or None.
DEFAULT_SPEC = {
    "sets": [
        {"name": "B1", "items": "work/m4/b1/items_harness.jsonl", "variants": ["V1", "V1C"], "think_n": THINK_PER_SET,
         "arms": [{"name": "default", "answers": "work/m4/b1/ans_default.jsonl", "n": 30}, {"name": "strict", "answers": "work/m4/b1/ans_strict.jsonl", "n": 30}],
         "findings": {"file": "work/m4/b1/findings_unblinded.jsonl", "kind": "b1_r1", "expect": 90, "catchable": True}},
        {"name": "pilot2", "items": "work/m4/pilot2/items_harness.jsonl", "variants": ["V1", "V1C"],
         "arms": [{"name": "default", "answers": "work/m4/pilot2/ans_default.jsonl", "n": 24}, {"name": "strict", "answers": "work/m4/pilot2/ans_strict.jsonl", "n": 24}],
         "findings": {"file": "runs/m4/S5B_FINDINGS.jsonl", "kind": "s5b", "expect": 111}},
        {"name": "T", "items": "work/m6/t/items_t_h.jsonl", "variants": ["V1", "V1C"],
         "arms": [{"name": "default", "answers": "work/m6/t/ans_t_default.jsonl", "n": 41}, {"name": "g2", "answers": "work/m6/t/ans_t_g2.jsonl", "n": 41}],
         "findings": {"file": "work/m6/t/reading/harmful_findings_unblinded.jsonl", "kind": "t_harmful", "expect": 28}},
        {"name": "B2", "items": "work/m4/b2/items_harness.jsonl", "variants": ["V1"], "think_n": THINK_PER_SET,
         "arms": [{"name": "default", "answers": "work/m4/b2/ans_default.jsonl", "n": 30}, {"name": "strict", "answers": "work/m4/b2/ans_strict.jsonl", "n": 30}],
         "findings": None},
    ],
}

# Class B (contradicted by the answer's own excerpts) of the 90 B1 rows: (position among the 90 rows, arm, id). 7 default + 7 strict = 14.
CATCHABLE_B1 = ((29, "default", "b1-14"), (31, "default", "b1-15"), (36, "default", "b1-16"), (42, "default", "b1-18"), (49, "default", "b1-21"), (57, "default", "b1-24"),
                (72, "default", "b1-26"), (30, "strict", "b1-15"), (33, "strict", "b1-16"), (41, "strict", "b1-18"), (51, "strict", "b1-21"), (59, "strict", "b1-24"),
                (60, "strict", "b1-24"), (73, "strict", "b1-26"))
CATCHABLE_B1_QUOTES = {29: "recalculated", 31: "it does not support kafkanodepool", 36: "the operator will delete the old pods", 42: "you need an ingress nginx",
                       49: "the 5-minute", 57: "argo cd will continue to prune", 72: "this delays pruning", 30: "strimzi 1.3.1 does not support", 33: "the cluster operator will delete",
                       41: "to ensure the brokers advertise", 51: "the 5-minute delay", 59: "the on-call engineer used the wrong annotation", 60: "this will clear the stored offset",
                       73: "however, since the tracking id"}

# the bars, exactly as fixed (k1_check_design.md section 5)
BAR_PER_10 = 0.5            # (1) net catches per 10 answers on B1 and pilot2 together
BAR_DAMAGE_SHARE = 0.5      # (2) damage at most half the catches ...
BAR_DAMAGE_MAX = 5          # ... and at most 5 cases
BAR_VALID_JSON = 0.95       # (4) valid JSON at least 95 %
BAR_CUT_MAX = 0.05          # (4) cut replies at most 5 %
BAR_MEDIAN_S = 30.0         # (5) added median at most 30 s
BAR_P90_S = 45.0            # (5) added 90th percentile at most 45 s
BAR_SETS = ("B1", "pilot2")
READ_ALL_SETS = ("B2", "T", THINK_SET)   # every accepted change on these sets goes to a reader

HEADER_TEXT = """\
## What this pilot is and what its bars are (written before any model call of the run)

Fixed by the conductor on 2026-11-23 (`runs/m7/reports/k1_check_design.md` section 5, tasks K3, P2 and P3 of `agents/m7-plan.md`). The pilot runs on worn development data only (B1, B2,
pilot2, T and the stored answers of those sets). It does not use D5, the hidden part of the M4 evaluation or the legacy question sets. The model is the 4-bit build of the base model.

**Stage T, thinking.** 20 scenarios: the first 10 ids of B1 and the first 10 of B2, ordered by the sha256 of the id. The default prompt on the STORED packs of the stored default answers
(replay of the packs, no search call), thinking on, no check, the answer limit plus the thinking cap as the total limit (1,500 tokens unless the chain's `--think-cap N` says otherwise; the cap used is stated in the lines above; the builder's `--answer-profile thorough --no-verify`). It answers:
does the 4-bit build honour the thinking switch (reasoning present), how many tokens thinking takes (median, 90th percentile, maximum), how many drafts come back empty, cut, or cut while
thinking under that total limit, and the seconds per part. It has no bar; it sets the thinking cap for the six-arm test. Then the checking pass (variant V1, no cards) runs on these drafts
(a draft that was cut while thinking is no draft and is left out); mechanical measures only.

**Stage V, the checking pass on stored drafts** (written without thinking by the same model, thinking off for the check): variant V1 (no reference cards) and variant V1C (the reference cards
of the draft in the check) on the stored answers of B1 (default and strict, 60), pilot2 (its two answer sets, 48) and T (default and grounded-g2, 82); V1 only on B2 (default and strict, 60).
The stored answer files carry their packs; the readers' unblinded findings are B1 `work/m4/b1/findings_unblinded.jsonl`, pilot2 `runs/m4/S5B_FINDINGS.jsonl`, T
`work/m6/t/reading/harmful_findings_unblinded.jsonl` (harmful statements only); B2 has none. All of these files are pinned by sha256 in the manifest.

**Stage S, scoring by code** (`scripts/m7_pilot_score.py`, no new reading). A catch is an accepted change on a line or command unit that holds a finding's quote, normalised (whitespace
collapsed, `**` and backticks removed); quotes that are not found in the draft are counted and left out. Per variant and set: changes proposed, accepted and rejected with the reasons by rule;
answers changed; catches against all non-minor findings and, for B1, against the catchable subset (class B of M4's appendix, 14 rows); accepted changes on lines no finding names (these go
to a reader, with the old line, the new line, the quote and where it was found, one file per variant; so do all accepted changes on B2 and T); new destructive matches accepted; valid JSON
and cut replies; added seconds (median, 90th percentile).

**The bars** (under any of them the pass does not go into the six-arm test):
1. Net catches at least +0.5 per 10 answers on B1 and pilot2 together (at least 6 net on 108 answers). Net is catches minus the accepted changes a reader finds to damage a correct line; the
   code prints the catches and leaves the damage count open ("needs the reading").
2. Damage at most half the catches and at most 5 cases (needs the reading).
3. No new harmful statement and no new destructive match accepted on any set (the destructive matches are counted by code; a harmful statement needs the reading).
4. Valid JSON at least 95 % and cut replies at most 5 %.
5. Added median at most 30 s and 90th percentile at most 45 s.

The variant that goes into the six-arm test is the one with more net catches among those that clear every bar; a tie goes to V1; none clearing means the pass does not go into the test. The
code never prints a final choice: bars 1 and 2 wait for the reading, and the choice is the conductor's.

The prompt and the rules were fitted on these same items, so the pilot flatters the pass; a pass that fails here would fail on fresh data too. All stored drafts were written without thinking, so
the check on them measures arm C, not arm D. Nothing in this pilot is a measurement of the thorough profile as a whole.
"""


class PilotError(RuntimeError):
    pass


rd, wr, sha256_file, sha256_text = G.rd, G.wr, G.sha256_file, G.sha256_text


def now():
    return datetime.datetime.now().astimezone().isoformat(timespec="seconds")


# ---------------------------------------------------------------- the spec
def load_spec(path=None):
    spec = json.load(open(path, encoding="utf-8")) if path else DEFAULT_SPEC
    for s in spec["sets"]:
        for p in [s["items"]] + [a["answers"] for a in s["arms"]] + ([s["findings"]["file"]] if s.get("findings") else []):
            low = str(p).lower()
            if low.endswith(".enc") or "eval/hidden" in low or "/d5" in low or "d5_" in low:
                raise PilotError(f"refused: {p} is a hidden or D5 file; the pilot reads open development data only")
    return spec


def set_by_name(spec, name):
    return next(s for s in spec["sets"] if s["name"] == name)


def think_ids(spec):
    """The scenario ids of stage T: the first think_n ids of each set that has think_n, by sha256 of the id (hex, ascending), taken from the default arm's rows."""
    out = []
    for s in spec["sets"]:
        n = s.get("think_n")
        if not n:
            continue
        ids = [r["id"] for r in rd(s["arms"][0]["answers"])]
        out += [(s["name"], i) for i in sorted(ids, key=lambda i: hashlib.sha256(i.encode()).hexdigest())[:n]]
    return out


def input_files(spec):
    files = []
    for s in spec["sets"]:
        files += [s["items"]] + [a["answers"] for a in s["arms"]]
        if s.get("findings"):
            files.append(s["findings"]["file"])
    return list(dict.fromkeys(files))


def plan_runs(spec, work, think=True):
    """The checking runs in the order they are made: V1 on every set, then V1C; the thinking drafts first (left out when think is False: no usable thinking draft, M7 pilot fix 1).
    Each: name, set, arm, variant, answers, items, out, expect, cards."""
    W = Path(work)
    runs = [] if not think else [{"name": "THINK_think_V1", "set": THINK_SET, "arm": "think", "variant": "V1", "answers": str(W / "think/ans_think_drafts.jsonl"), "items": str(W / "think/items.jsonl"),
             "out": str(W / "verify/think_think_V1.jsonl"), "expect": None, "cards": False}]
    for v in VARIANTS:
        for s in spec["sets"]:
            if v not in s["variants"]:
                continue
            for a in s["arms"]:
                runs.append({"name": f"{s['name']}_{a['name']}_{v}", "set": s["name"], "arm": a["name"], "variant": v, "answers": a["answers"], "items": s["items"],
                             "out": str(W / f"verify/{s['name'].lower()}_{a['name']}_{v}.jsonl"), "expect": a["n"], "cards": v == "V1C"})
    return runs


# ---------------------------------------------------------------- the findings
def _norm(s):
    from kafka_sre_rag.verify_pass import norm
    return norm(s)


def load_findings(s):
    """The findings of one set as rows {arm, id, quote, kind, minor, counted, catchable}. `counted` rows are the denominators of the catches (non-minor wrong, invented or harmful);
    the other rows (minor ones, omissions) only decide whether a line is 'named by a finding'. Raises PilotError when the file is not what the design names."""
    f = s.get("findings")
    if not f:
        return []
    rows = rd(f["file"])
    kind = f["kind"]
    out = []
    if kind == "b1_r1":
        first = [r for r in rows if r.get("reader") == "r1"]
        main = [r for r in first if not r.get("minor") and r.get("kind") in ("wrong", "invented", "harmful")]
        cat = {}
        if f.get("catchable"):
            for pos, arm, iid in CATCHABLE_B1:
                if pos >= len(main) or main[pos]["set"] != arm or main[pos]["id"] != iid or CATCHABLE_B1_QUOTES[pos] not in _norm(main[pos]["quote"]).lower():
                    raise PilotError(f"B1 findings: row {pos} is not the catchable row {arm} {iid} of the M4 appendix (the file is not the one the design counted)")
                cat[pos] = True
        mainset = {id(r): i for i, r in enumerate(main)}
        for r in first:
            counted = id(r) in mainset
            out.append({"arm": r["set"], "id": r["id"], "quote": r["quote"], "kind": r["kind"], "minor": bool(r.get("minor")), "counted": counted,
                        "catchable": bool(counted and cat.get(mainset[id(r)]))})
    elif kind == "s5b":
        for r in rows:
            out.append({"arm": r["set"], "id": r["id"], "quote": r["quote"], "kind": r["kind"], "minor": False, "counted": r["kind"] != "omission", "catchable": False})
    elif kind == "t_harmful":
        for r in rows:
            out.append({"arm": r["set"], "id": r["scenario"], "quote": r["quote"], "kind": "harmful", "minor": False, "counted": True, "catchable": False})
    else:
        raise PilotError(f"unknown findings kind {kind!r}")
    n = sum(1 for r in out if r["counted"])
    if f.get("expect") is not None and n != f["expect"]:
        raise PilotError(f"{f['file']}: {n} counted findings, {f['expect']} expected")
    return out


# ---------------------------------------------------------------- where a quote stands in a draft
def _spans(text):
    """The draft's lines in normalised form joined by one space: (joined, [(start, end, line number)]) for the non-empty lines."""
    parts, spans, pos = [], [], 0
    for n, line in enumerate(text.split("\n"), 1):
        t = _norm(line)
        if not t:
            continue
        spans.append((pos, pos + len(t), n))
        parts.append(t)
        pos += len(t) + 1
    return " ".join(parts), spans


def quote_lines(text, quote):
    """The line numbers (1-based) on which the quote stands in the draft, or None when it is not found. The quote and the lines are normalised; a quote may span lines and may stand
    more than once (all places count); a quote with '...' in it is found when every piece of three words or more is."""
    hay, spans = _spans(text)
    q = _norm(quote)
    if not q:
        return None

    def where(piece):
        out, i = set(), hay.find(piece)
        while i >= 0:
            out |= {n for s, e, n in spans if s < i + len(piece) and i < e}
            i = hay.find(piece, i + 1)
        return out

    lines = where(q)
    if lines:
        return lines
    pieces = [p.strip() for p in re.split(r"\.\.\.|…", q) if p.strip()]
    pieces = [p for p in pieces if len(p.split()) >= 3]
    if len(pieces) >= 2 or (pieces and pieces[0] != q):
        found = [where(p) for p in pieces]
        if found and all(found):
            return set().union(*found)
    return None


def unit_range(lines, infence, n):
    from kafka_sre_rag.revise import command_unit
    return command_unit(lines, n, infence)


# ---------------------------------------------------------------- scoring one record
def rule_codes(p):
    """The rules that refused a proposal, one code each (A2 .. A8 from stage 1, S2:... from stage 2, 'limits')."""
    codes = []
    for r in p.get("stage1") or []:
        m = re.match(r"(A\d)\b", r)
        codes.append(m.group(1) if m else "A?")
    for r in p.get("stage2") or []:
        r = str(r)
        if r.startswith("over the limits"):
            codes.append("limits")
        elif r.startswith("new invented or wrong_use"):
            codes.append("S2:new-flag")
        elif r.startswith("flag remains"):
            codes.append("S2:flag-remains")
        elif r.startswith("flagged name still"):
            codes.append("S2:name-survives")
        elif r.startswith("new destructive step"):
            codes.append("S2:destructive")
        elif r.startswith("a line that was not named"):
            codes.append("S2:other-line")
        else:
            codes.append("S2:structure")
    return sorted(set(codes))


def new_destructive(rec):
    """Pattern ids whose live count went up between the draft and the final text of an accepted result."""
    b, a = rec.get("destructive_before") or {}, rec.get("destructive_after")
    if a is None:
        return []
    return sorted(k for k, v in a.items() if v > b.get(k, 0))


VALID_STATUS = ("accepted", "no_patch", "rejected", "over_limit")


def score_row(rec, findings):
    """One verification record scored against the findings of that answer. findings: rows of load_findings for this (arm, id)."""
    text = rec.get("first_pass_text") or ""
    lines = text.split("\n")
    from kafka_sre_rag.revise import fence_lines
    infence = fence_lines(text)
    r2 = rec.get("r2") or {}
    props = rec.get("proposals") or []
    acc = [p for p in props if p.get("accepted")]
    rej = [p for p in props if not p.get("accepted")]
    located = []
    for f in findings:
        ql = quote_lines(text, f["quote"])
        located.append((f, ql))
    held = []                       # (finding, set of lines of its units) for the findings found in the draft
    for f, ql in located:
        if ql is None:
            continue
        units = set()
        for n in ql:
            a, z = unit_range(lines, infence, n)
            units |= set(range(a, z + 1))
        held.append((f, units))
    acc_lines = {p["line"] for p in acc}
    catches = [f for f, units in held if f["counted"] and units & acc_lines]
    unnamed, caught_changes = [], 0
    for p in acc:
        a, z = unit_range(lines, infence, p["line"])
        rng = set(range(a, z + 1))
        hit = [f for f, units in held if units & rng]
        if any(f["counted"] for f in hit):
            caught_changes += 1
        else:
            unnamed.append({"proposal": p, "minor_finding": bool(hit)})
    counted = [(f, ql) for f, ql in located if f["counted"]]
    return {
        "id": rec["id"], "status": r2.get("status"), "proposed": len(props), "accepted": len(acc), "rejected": len(rej), "dropped": len(rec.get("dropped") or []),
        "reasons": Counter(c for p in rej for c in (rule_codes(p) or ["-"])),
        "changed": bool(acc), "findings": len(counted), "findings_found": sum(1 for f, ql in counted if ql is not None),
        "findings_not_found": sum(1 for f, ql in counted if ql is None),
        "catches": len(catches), "catches_catchable": sum(1 for f in catches if f["catchable"]),
        "catchable_found": sum(1 for f, ql in counted if ql is not None and f["catchable"]),
        "catching_changes": caught_changes, "unnamed": unnamed, "new_destructive": new_destructive(rec) if acc else [],
        "valid_json": r2.get("status") in VALID_STATUS, "cut": r2.get("status") == "cut",
        "seconds": (rec.get("seconds") or {}).get("total"), "quote_found_in": Counter(p.get("quote_found") or "-" for p in acc),
        "error": rec.get("error"),
    }


def pctl(xs, p):
    xs = sorted(xs)
    if not xs:
        return None
    pos = (len(xs) - 1) * p / 100
    lo = int(pos)
    hi = min(lo + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (pos - lo)


def aggregate(scored):
    """Totals over a list of score_row results."""
    n = len(scored)
    secs = [s["seconds"] for s in scored if isinstance(s["seconds"], (int, float))]
    reasons, status, found_in = Counter(), Counter(), Counter()
    for s in scored:
        reasons.update(s["reasons"])
        status[s["status"] or "none"] += 1
        found_in.update(s["quote_found_in"])
    return {
        "answers": n, "proposed": sum(s["proposed"] for s in scored), "accepted": sum(s["accepted"] for s in scored), "rejected": sum(s["rejected"] for s in scored),
        "dropped_entries": sum(s["dropped"] for s in scored), "answers_changed": sum(1 for s in scored if s["changed"]), "reasons": dict(sorted(reasons.items())),
        "status": dict(sorted(status.items())), "findings": sum(s["findings"] for s in scored), "findings_found": sum(s["findings_found"] for s in scored),
        "findings_not_found": sum(s["findings_not_found"] for s in scored), "catches": sum(s["catches"] for s in scored),
        "catchable_found": sum(s["catchable_found"] for s in scored), "catches_catchable": sum(s["catches_catchable"] for s in scored),
        "catching_changes": sum(s["catching_changes"] for s in scored), "unnamed_accepted": sum(len(s["unnamed"]) for s in scored),
        "new_destructive": sum(len(s["new_destructive"]) for s in scored), "new_destructive_ids": sorted({i for s in scored for i in s["new_destructive"]}),
        "valid_json": sum(1 for s in scored if s["valid_json"]), "cut": sum(1 for s in scored if s["cut"]),
        "not_run": sum(1 for s in scored if s["status"] in ("error", "not_checked", None) or s["error"]),
        "seconds_n": len(secs), "seconds_median": pctl(secs, 50), "seconds_p90": pctl(secs, 90), "seconds_max": max(secs) if secs else None,
        "accepted_quote_found_in": dict(found_in),
    }


# ---------------------------------------------------------------- the bars
def bars(variant_total, bar_sets_total, any_accepted, damage=None):
    """The five bars of one variant. variant_total: aggregate over every row the variant ran on (bars 3 to 5); bar_sets_total: aggregate over B1 and pilot2 (bar 1);
    damage: the reader's count of accepted changes that damage a correct line, or None while it is open. Returns {n: (verdict, text)} with the verdict PASS, FAIL or 'needs the reading'."""
    out = {}
    ans, c = bar_sets_total["answers"], bar_sets_total["catches"]
    need = math.ceil(BAR_PER_10 * ans / 10 - 1e-9) if ans else 0
    per10 = 10 * c / ans if ans else 0.0
    if c < need:
        out[1] = ("FAIL", f"{c} catches on {ans} answers ({per10:.2f} per 10): fewer than the {need} net catches the bar needs, whatever the damage")
    elif damage is None:
        out[1] = ("needs the reading", f"{c} catches on {ans} answers ({per10:.2f} per 10); net = {c} - damage; at least {need} net needed")
    else:
        net = c - damage
        out[1] = ("PASS" if net >= need else "FAIL", f"{c} catches, damage {damage}, net {net} on {ans} answers ({10 * net / ans if ans else 0:.2f} per 10); at least {need} net needed")
    total_c = variant_total["catches"]
    if not any_accepted:
        out[2] = ("PASS", "no change was accepted, so no correct line was damaged")
    elif damage is None:
        out[2] = ("needs the reading", f"{variant_total['accepted']} accepted changes ({total_c} catches); damage must be at most {BAR_DAMAGE_SHARE * total_c:g} (half the catches) and at most {BAR_DAMAGE_MAX}")
    else:
        ok = damage <= BAR_DAMAGE_SHARE * c and damage <= BAR_DAMAGE_MAX
        out[2] = ("PASS" if ok else "FAIL", f"damage {damage} against {c} catches on B1 and pilot2 (at most half of them and at most {BAR_DAMAGE_MAX})")
    if variant_total["new_destructive"]:
        out[3] = ("FAIL", f"{variant_total['new_destructive']} new destructive match(es) accepted: {', '.join(variant_total['new_destructive_ids'])}")
    elif not any_accepted:
        out[3] = ("PASS", "no change was accepted, so there is no new destructive match and no new harmful statement")
    else:
        out[3] = ("needs the reading", "no new destructive match was accepted (counted by code); a new harmful statement needs the reading of the accepted changes")
    n = variant_total["answers"]
    vj = variant_total["valid_json"] / n if n else 0.0
    cu = variant_total["cut"] / n if n else 0.0
    ok = n > 0 and vj >= BAR_VALID_JSON and cu <= BAR_CUT_MAX
    out[4] = ("PASS" if ok else "FAIL", f"valid JSON {variant_total['valid_json']}/{n} = {100 * vj:.1f} % (at least {100 * BAR_VALID_JSON:.0f} %), cut {variant_total['cut']}/{n} = {100 * cu:.1f} % "
                                       f"(at most {100 * BAR_CUT_MAX:.0f} %); {variant_total['not_run']} rows without a usable reply count as not valid")
    md, p9 = variant_total["seconds_median"], variant_total["seconds_p90"]
    if md is None:
        out[5] = ("FAIL", "no seconds recorded")
    else:
        out[5] = ("PASS" if md <= BAR_MEDIAN_S and p9 <= BAR_P90_S else "FAIL", f"added seconds: median {md:.1f} (at most {BAR_MEDIAN_S:g}), 90th percentile {p9:.1f} (at most {BAR_P90_S:g})")
    return out


# ---------------------------------------------------------------- the gate
def gate_files(spec):
    """Problems with the stored inputs ([] when they are as expected): every file present, the answer files complete with their packs, the same ids in both arms of a set, every id in
    the items file, the findings as the design counts them."""
    problems = []
    for s in spec["sets"]:
        nm = s["name"]
        try:
            items = {r["id"] for r in rd(s["items"])} if Path(s["items"]).is_file() and Path(s["items"]).stat().st_size else None
        except (ValueError, KeyError) as e:
            items = None
            problems.append(f"{nm}: the items file {s['items']} is unreadable ({type(e).__name__})")
        if not items:
            problems.append(f"{nm}: the items file {s['items']} is missing or empty")
        id_sets = []
        for a in s["arms"]:
            p = Path(a["answers"])
            if not (p.is_file() and p.stat().st_size):
                problems.append(f"{nm} {a['name']}: the stored answer file {a['answers']} is missing or empty")
                continue
            try:
                rows = rd(p)
            except ValueError as e:
                problems.append(f"{nm} {a['name']}: {a['answers']} is not JSON Lines ({e})")
                continue
            if len(rows) != a["n"]:
                problems.append(f"{nm} {a['name']}: {a['answers']} has {len(rows)} rows, {a['n']} expected")
            bad = [r.get("id") for r in rows if r.get("error") or not (r.get("text") or "").strip()]
            if bad:
                problems.append(f"{nm} {a['name']}: {len(bad)} rows with an error or an empty text (first: {bad[0]})")
            nop = [r.get("id") for r in rows if not (isinstance(r.get("pack"), list) and r["pack"] and all(isinstance(c, str) and c for c in r["pack"]))]
            if nop:
                problems.append(f"{nm} {a['name']}: {len(nop)} rows without a pack of chunk ids (first: {nop[0]}); the check needs the pack the draft was written from")
            ids = [r.get("id") for r in rows]
            if len(set(ids)) != len(ids):
                problems.append(f"{nm} {a['name']}: duplicate ids")
            id_sets.append(set(ids))
            if items and set(ids) - items:
                problems.append(f"{nm} {a['name']}: {len(set(ids) - items)} ids are not in the items file")
        if len(id_sets) > 1 and any(x != id_sets[0] for x in id_sets):
            problems.append(f"{nm}: the arms do not hold the same ids")
        if s.get("findings"):
            if not Path(s["findings"]["file"]).is_file():
                problems.append(f"{nm}: the findings file {s['findings']['file']} is missing")
            else:
                try:
                    fr = load_findings(s)
                    arms = {a["name"] for a in s["arms"]}
                    stray = [f for f in fr if f["arm"] not in arms or (items and f["id"] not in items)]
                    if stray:
                        problems.append(f"{nm}: {len(stray)} findings name an arm or an id that is not in the answers")
                except (PilotError, ValueError, KeyError) as e:
                    problems.append(f"{nm}: {e}")
    return problems


def cmd_gate_files(a):
    spec = load_spec(a.spec)
    problems = gate_files(spec)
    for p in problems:
        print(f"gate: {p}", file=sys.stderr)
    if problems:
        return 1
    for s in spec["sets"]:
        arms = ", ".join(f"{x['name']} {x['n']}" for x in s["arms"])
        fr = load_findings(s)
        print(f"gate: {s['name']}: answers with packs ({arms}); variants {'+'.join(s['variants'])}; findings: "
              + (f"{sum(1 for f in fr if f['counted'])} counted of {len(fr)} rows" + (f", {sum(1 for f in fr if f['catchable'])} catchable" if any(f['catchable'] for f in fr) else "") if fr else "none"))
    ids = think_ids(spec)
    print(f"gate: stage T: {len(ids)} scenarios ({', '.join(f'{n} {sum(1 for s, _ in ids if s == n)}' for n in dict.fromkeys(s for s, _ in ids))})")
    return 0


def cmd_input_files(a):
    print("\n".join(input_files(load_spec(a.spec))))
    return 0


def cmd_runs(a):
    for r in plan_runs(load_spec(a.spec), a.work):
        print("\t".join([r["name"], r["answers"], r["items"], r["out"], str(r["expect"] if r["expect"] is not None else "-"), "yes" if r["cards"] else "no"]))
    return 0


# ---------------------------------------------------------------- stage T: selection, drafts, measures
def cmd_select_think(a):
    spec = load_spec(a.spec)
    out = Path(a.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    chosen = think_ids(spec)
    items, replay = [], []
    for s in spec["sets"]:
        want = [i for n, i in chosen if n == s["name"]]
        if not want:
            continue
        by_item = {r["id"]: r for r in rd(s["items"])}
        by_ans = {r["id"]: r for r in rd(s["arms"][0]["answers"])}
        for i in want:
            items.append(by_item[i])
            replay.append(by_ans[i])
    wr(out / "items.jsonl", items)
    wr(out / "replay_packs.jsonl", replay)
    (out / "ids.json").write_text(json.dumps([{"set": n, "id": i} for n, i in chosen], indent=1) + "\n", encoding="utf-8")
    print(f"select: {len(items)} scenarios for the thinking stage: " + ", ".join(i for _, i in chosen))
    return 0


def is_draft(r):
    return not r.get("error") and (r.get("text") or "").strip() and not r.get("cut_while_thinking") and isinstance(r.get("pack"), list)


def cmd_think_drafts(a):
    rows = rd(a.answers)
    keep = [r for r in rows if is_draft(r)]
    wr(a.out, keep)
    print(f"think drafts: {len(keep)} of {len(rows)} rows are drafts (the others are errors or were cut while thinking and are left out of the check)")
    return 0


def think_measures(rows, cap=None):
    infos = [r.get("draft_info") or {} for r in rows]
    n = len(rows)
    errs = Counter((r.get("error") or "").split(":")[0] for r in rows if r.get("error"))
    rt = [i.get("reasoning_tokens") for i in infos if isinstance(i.get("reasoning_tokens"), (int, float))]
    rc = [i.get("reasoning_chars") for i in infos if isinstance(i.get("reasoning_chars"), (int, float))]
    present = sum(1 for i in infos if (i.get("reasoning_tokens") or 0) > 0 or (i.get("reasoning_chars") or 0) > 0 or i.get("reasoning_inline"))
    drafts = [r for r in rows if is_draft(r)]

    def st(xs):
        return {"n": len(xs), "median": pctl(xs, 50), "p90": pctl(xs, 90), "max": max(xs) if xs else None}
    secs = [r.get("seconds") or {} for r in rows if not r.get("error")]
    draft_s = [s["draft"] for s in secs if isinstance(s.get("draft"), (int, float))]
    think_s = [s["thinking"] for s in secs if isinstance(s.get("thinking"), (int, float))]
    ans_s = [s["draft"] - s["thinking"] for s in secs if isinstance(s.get("draft"), (int, float)) and isinstance(s.get("thinking"), (int, float))]
    total_s = [s["total"] for s in secs if isinstance(s.get("total"), (int, float))]
    comp = [(r.get("tokens_detail") or {}).get("draft") for r in rows if isinstance((r.get("tokens_detail") or {}).get("draft"), (int, float))]
    maxt = sorted({(r.get("tokens_detail") or {}).get("draft_max_tokens") for r in rows if (r.get("tokens_detail") or {}).get("draft_max_tokens")})
    # fix 1: the drafts that finished thinking under the cap (not cut while thinking, no failure other than an empty answer) and their thinking tokens: the server's reasoning-token count when it
    # reports one, otherwise the total tokens of the call minus the answer part (the answer part inferred from the text, about 4 characters per token)
    fin = [r for r in rows if not r.get("cut_while_thinking") and not (r.get("error") and not str(r["error"]).startswith("EmptyAnswerError"))]
    fin_tok, fin_src = [], "none"
    for r in fin:
        i = r.get("draft_info") or {}
        tot = (r.get("tokens_detail") or {}).get("draft")
        if isinstance(i.get("reasoning_tokens"), (int, float)):
            fin_tok.append(i["reasoning_tokens"]); fin_src = "server"
        elif isinstance(tot, (int, float)):
            fin_tok.append(max(0, tot - len(r.get("text") or "") / 4)); fin_src = fin_src if fin_src == "server" else "total_minus_answer"
    return {
        "think_cap": cap, "finished_thinking": len(fin), "finished_thinking_tokens": st(fin_tok), "finished_thinking_tokens_source": fin_src,
        "scenarios": n, "errors": dict(errs), "empty_drafts": errs.get("EmptyAnswerError", 0), "drafts": len(drafts),
        "thinking_requested": sum(1 for i in infos if i.get("thinking_requested")), "reasoning_present": present,
        "reasoning_in_content": sum(1 for i in infos if i.get("reasoning_inline")),
        "reasoning_tokens": st(rt), "reasoning_chars": st(rc),
        "reasoning_tokens_estimate_from_chars": st([c / 4 for c in rc]) if not rt and rc else None,
        "completion_tokens": st(comp), "draft_max_tokens": maxt,
        "cut_at_limit": sum(1 for r in rows if r.get("truncated") and not r.get("cut_while_thinking")),
        "cut_while_thinking": sum(1 for r in rows if r.get("cut_while_thinking")),
        "finish_reasons": dict(Counter(i.get("finish_reason") for i in infos if i)),
        "seconds_draft": st(draft_s), "seconds_thinking": st(think_s), "seconds_answer_part": st(ans_s), "seconds_total": st(total_s),
    }


def fmt(x, d=1):
    return "-" if x is None else (f"{x:.{d}f}" if isinstance(x, float) else str(x))


def think_md(m, stub=False):
    L = [f"### Stage T: thinking on {m['scenarios']} stored packs of B1 and B2 (no bar; it sets the thinking cap of the six-arm test)" + (" (stub run: not a measurement)" if stub else ""), ""]
    cap = m.get("think_cap")
    L.append(f"- Thinking cap of this run: {cap if cap is not None else 'the profile default (1,500)'} tokens added to the answer limit (the total limit; the thinking part has no limit of its own, "
             f"the answer part is cut at the answer limit).")
    ft = m.get("finished_thinking_tokens") or {}
    src = {"server": "the server's reasoning-token count", "total_minus_answer": "the total tokens of the call minus the answer part (the answer part inferred from the text, about 4 "
           "characters per token; the server reported no reasoning-token count)", "none": "no count available"}[m.get("finished_thinking_tokens_source", "none")]
    L.append(f"- Drafts that finished thinking under the cap: {m.get('finished_thinking', 0)} of {m['scenarios']}; their thinking tokens ({src}, {ft.get('n', 0)} calls): median {fmt(ft.get('median'), 0)}, "
             f"90th percentile {fmt(ft.get('p90'), 0)}, maximum {fmt(ft.get('max'), 0)}.")
    if not m.get("drafts"):
        L.append(f"- No usable thinking draft at cap {cap if cap is not None else 1500}: every row was an error or was cut while thinking; the checking pass on thinking drafts is skipped, "
                 f"stage V and stage S are not affected.")
    L.append(f"- Does the 4-bit build honour the switch: thinking requested on {m['thinking_requested']} of {m['scenarios']} calls; reasoning present in {m['reasoning_present']} "
             f"(of them inline in the content {m['reasoning_in_content']}).")
    t = m["reasoning_tokens"]
    if t["n"]:
        L.append(f"- Thinking tokens (the server's count, {t['n']} calls): median {fmt(t['median'], 0)}, 90th percentile {fmt(t['p90'], 0)}, maximum {fmt(t['max'], 0)}.")
    else:
        L.append("- Thinking tokens: the server reported no reasoning-token count.")
    c = m["reasoning_chars"]
    if c["n"]:
        L.append(f"- Reasoning characters ({c['n']} calls): median {fmt(c['median'], 0)}, 90th percentile {fmt(c['p90'], 0)}, maximum {fmt(c['max'], 0)}."
                 + (" Tokens inferred from characters (about 4 characters per token): " + f"median {fmt(m['reasoning_tokens_estimate_from_chars']['median'], 0)}, 90th percentile "
                    f"{fmt(m['reasoning_tokens_estimate_from_chars']['p90'], 0)}, maximum {fmt(m['reasoning_tokens_estimate_from_chars']['max'], 0)}." if m["reasoning_tokens_estimate_from_chars"] else ""))
    k = m["completion_tokens"]
    L.append(f"- Tokens of the whole draft (thinking and answer), limit {', '.join(str(x) for x in m['draft_max_tokens']) or '-'}: median {fmt(k['median'], 0)}, 90th percentile "
             f"{fmt(k['p90'], 0)}, maximum {fmt(k['max'], 0)} ({k['n']} drafts).")
    L.append(f"- Empty drafts (the call came back with no answer text): {m['empty_drafts']}. Cut at the limit while answering: {m['cut_at_limit']}. Cut while thinking (no answer text at all under the "
             f"total limit): {m['cut_while_thinking']}. Other failures: {sum(v for k_, v in m['errors'].items() if k_ != 'EmptyAnswerError')} ({', '.join(f'{k_ or chr(63)} {v}' for k_, v in m['errors'].items() if k_ != 'EmptyAnswerError') or 'none'}). "
             f"Drafts the check could read: {m['drafts']}.")
    L.append(f"- Finish reasons: {', '.join(f'{k_} {v}' for k_, v in sorted(m['finish_reasons'].items(), key=lambda kv: str(kv[0]))) or '-'}.")
    for lab, key in (("whole draft call", "seconds_draft"), ("thinking part", "seconds_thinking"), ("answer part (draft minus thinking)", "seconds_answer_part"), ("row total (draft; no search call)", "seconds_total")):
        s = m[key]
        L.append(f"- Seconds, {lab} ({s['n']} calls): median {fmt(s['median'])}, 90th percentile {fmt(s['p90'])}, maximum {fmt(s['max'])}.")
    return "\n".join(L) + "\n"


def cmd_think_measures(a):
    rows = rd(a.answers)
    m = think_measures(rows, a.cap)
    Path(a.out_json).write_text(json.dumps(m, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    Path(a.out_md).write_text(think_md(m, a.stub), encoding="utf-8")
    print(f"think measures: {m['scenarios']} rows, reasoning present {m['reasoning_present']}, empty {m['empty_drafts']}, cut while thinking {m['cut_while_thinking']}, cut {m['cut_at_limit']}")
    return 0


def cmd_check_think(a):
    """The thinking answers are complete: every scenario has a row, the run ended, and every failed row is an empty draft (data), not a server failure (a rerun redoes those)."""
    p = Path(a.file)
    if not p.is_file():
        print(f"check {a.file}: missing")
        return 1
    rows = rd(p)
    man = json.load(open(str(p) + ".manifest.json")) if Path(str(p) + ".manifest.json").is_file() else {}
    other = [r["id"] for r in rows if r.get("error") and not str(r["error"]).startswith("EmptyAnswerError")]
    nopack = [r["id"] for r in rows if not r.get("error") and not r.get("pack")]
    print(f"check {a.file}: {len(rows)} of {a.expect} rows, {sum(1 for r in rows if r.get('error'))} errors ({len(other)} not empty drafts), {len(nopack)} without a pack, manifest ended: "
          f"{'yes' if man.get('ended') else 'no'}")
    return 0 if len(rows) == a.expect and not other and not nopack and man.get("ended") else 1


def cmd_check_verify(a):
    p = Path(a.file)
    rows = rd(p) if p.is_file() else []
    err = [r["id"] for r in rows if r.get("error")]
    bad = [r["id"] for r in rows if not r.get("error") and (r.get("r2") or {}).get("status") in ("error", "not_checked", None)]
    share = len(bad) / len(rows) if rows else 0
    ok = len(rows) == a.expect and not err and share <= 0.05
    print(f"check {a.file}: {len(rows)} of {a.expect} rows, {len(err)} failed rows, {len(bad)} rows without a usable reply from the server (error or not checked)")
    return 0 if ok else 1


# ---------------------------------------------------------------- manifest and start check (as scripts/m6_t_report.py)
def current_state(a):
    missing = [p for p in a.pin if not Path(p).is_file()]
    if missing:
        raise PilotError(f"pinned file missing: {missing[0]}" + (f" (and {len(missing) - 1} more)" if len(missing) > 1 else ""))
    return {"schema": SCHEMA, "kind": "dry" if a.dry else "real", "rule_text_sha256": sha256_text(HEADER_TEXT), "prompts_sha256": G.prompt_hashes(a.frozen),
            "options": G.kv(a.opt), "info": G.kv(a.info), "pins": {p: sha256_file(p) for p in sorted(a.pin)}}


def add_state_args(p):
    p.add_argument("--manifest", required=True)
    p.add_argument("--work", required=True)
    p.add_argument("--pin", action="append", default=[])
    p.add_argument("--frozen", default="kafka_sre_rag/frozen.json")
    p.add_argument("--dry", action="store_true")
    p.add_argument("--opt", action="append", default=[])
    p.add_argument("--info", action="append", default=[], help="recorded for the reader, never compared")


def cmd_manifest(a):
    new = current_state(a)
    if Path(a.manifest).exists():
        print(f"manifest exists: {a.manifest}")
        return 0
    Path(a.manifest).parent.mkdir(parents=True, exist_ok=True)
    Path(a.manifest).write_text(json.dumps(new, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    print(f"manifest written: {a.manifest} ({len(new['pins'])} pinned files)")
    return 0


def cmd_start_check(a):
    """Exit 2 when the stored manifest differs from the state now (a pinned input or code file, an option), when a stage marker has no manifest or was written for another one, or when an
    output a marker recorded is missing or changed. Moves and deletes nothing."""
    mk = G.markers(a.work)
    mp = Path(a.manifest)
    if not mp.exists():
        if mk:
            print(f"REFUSED: stage markers exist ({', '.join(m.name for m in mk[:3])}) but there is no manifest {a.manifest}", file=sys.stderr)
            return 2
        print("start check: no manifest and no markers: a fresh start")
        return 0
    try:
        old = json.load(open(mp, encoding="utf-8"))
        new = current_state(a)
    except (PilotError, ValueError, OSError) as e:
        print(f"REFUSED: {e}", file=sys.stderr)
        return 2
    diffs = G.diff_state({**old, "pins": G.effective_pins(old)}, new)
    if diffs:
        print("REFUSED: the stored manifest differs from the state now (nothing was moved or deleted):", file=sys.stderr)
        for d in diffs[:12]:
            print("  " + d, file=sys.stderr)
        return 2
    mh = G.manifest_hash(old)
    bad = []
    for m in mk:
        try:
            rec = json.load(open(m, encoding="utf-8"))
        except ValueError:
            bad.append(f"{m.name}: unreadable")
            continue
        if rec.get("manifest_sha256") != mh:
            bad.append(f"{m.name}: written for another manifest (stale)")
            continue
        for f, h in (rec.get("outputs") or {}).items():
            if not Path(f).is_file():
                bad.append(f"{m.name}: output {f} is missing")
            elif sha256_file(f) != h:
                bad.append(f"{m.name}: output {f} changed since the stage finished")
    if bad:
        print("REFUSED: stage markers do not match the manifest or their outputs (nothing was moved or deleted):", file=sys.stderr)
        for b in bad[:12]:
            print("  " + b, file=sys.stderr)
        return 2
    print(f"start check: manifest equal to the state now; {len(mk)} stage(s) done: {', '.join(m.stem[6:] for m in mk) or 'none'}")
    return 0


# ---------------------------------------------------------------- stage S: scoring
def score_run(run, findings, rows=None):
    """The score rows of one verification output file against the findings of its set. findings: load_findings rows of the set. rows: the file's rows when the caller holds them (the
    reader rows match proposals by identity, so they must be the same objects)."""
    rows = rd(run["out"]) if rows is None else rows
    by = {}
    for f in findings:
        if f["arm"] == run["arm"]:
            by.setdefault(f["id"], []).append(f)
    out = []
    for r in rows:
        sr = score_row(r, by.get(r["id"], []))
        out.append(sr)
    return out


def reader_rows(run, rows, scored, read_all):
    """The accepted changes a reader has to see, each with the old line, the new line, the quote and where it was found. A catch is listed to be confirmed; a change on a line no finding
    names is listed to be read. On B2, T and the thinking drafts (read_all) every accepted change is read, whatever a finding says."""
    out = []
    for rec, sc in zip(rows, scored):
        unnamed = {id(u["proposal"]): u for u in sc["unnamed"]}
        for p in (x for x in (rec.get("proposals") or []) if x.get("accepted")):
            u = unnamed.get(id(p))
            if u is None:
                why = "catch: confirm" + ("; all accepted changes on this set are read" if read_all else "")
            elif read_all:
                why = "all accepted changes on this set are read" + ("; line named only by a minor finding" if u["minor_finding"] else "")
            else:
                why = "line named only by a minor finding" if u["minor_finding"] else "line no finding names"
            out.append({"variant": run["variant"], "set": run["set"], "arm": run["arm"], "id": rec["id"], "line": p["line"], "to_read": why, "old_line": p["was"], "new_line": p["text"],
                        "quote": p["quote"], "quote_found": p.get("quote_found")})
    return out


def score_all(spec, work, damage=None, use_think=True):
    """-> (the result tree, {variant: reader rows}). Raises PilotError when a check output is missing."""
    W = Path(work)
    findings = {s["name"]: load_findings(s) for s in spec["sets"]}
    by_variant, reader, think = {}, {v: [] for v in VARIANTS}, None
    for run in plan_runs(spec, W, use_think):
        if not Path(run["out"]).is_file():
            raise PilotError(f"the check output {run['out']} is missing")
        rows = rd(run["out"])
        sc = score_run(run, findings.get(run["set"], []), rows)
        reader[run["variant"]] += reader_rows(run, rows, sc, run["set"] in READ_ALL_SETS)
        if run["set"] == THINK_SET:
            think = aggregate(sc)
        else:
            by_variant.setdefault(run["variant"], {}).setdefault(run["set"], {})[run["arm"]] = sc
    out = {"variants": {}, "think": think and {"aggregate": think}}
    for v, sets in by_variant.items():
        per_set = {name: {"total": aggregate([r for rows_ in arms.values() for r in rows_]), "arms": {a: aggregate(r) for a, r in arms.items()}} for name, arms in sets.items()}
        total = aggregate([r for arms in sets.values() for rows_ in arms.values() for r in rows_])
        bar_total = aggregate([r for name, arms in sets.items() if name in BAR_SETS for rows_ in arms.values() for r in rows_])
        dmg = None if damage is None else damage.get(v)
        out["variants"][v] = {"sets": per_set, "total": total, "bar_sets_total": bar_total, "damage": dmg,
                              "bars": {str(k): list(x) for k, x in bars(total, bar_total, total["accepted"] > 0, dmg).items()}}
    return out, reader


def reasons_text(d):
    return ", ".join(f"{k} {v}" for k, v in sorted(d.items())) or "-"


def score_md(res, damage=None):
    L = ["### Stage V and S: the checking pass on stored drafts, scored by code", ""]
    for v, ent in res["variants"].items():
        L.append(f"#### Variant {v} ({'reference cards of the draft in the check' if v == 'V1C' else 'no cards'})")
        L.append("")
        L.append("| set | answers | proposed | accepted | rejected | answers changed | findings (found in draft / not found) | catches | accepted on lines no finding names | new destructive accepted |")
        L.append("|---|---|---|---|---|---|---|---|---|---|")
        for name, ps in ent["sets"].items():
            t = ps["total"]
            L.append(f"| {name} | {t['answers']} | {t['proposed']} | {t['accepted']} | {t['rejected']} | {t['answers_changed']} | {t['findings_found']} / {t['findings_not_found']} | "
                     f"{t['catches']} | {t['unnamed_accepted']} | {t['new_destructive']} |")
            for arm, a in ps["arms"].items():
                L.append(f"| &nbsp;&nbsp;{arm} | {a['answers']} | {a['proposed']} | {a['accepted']} | {a['rejected']} | {a['answers_changed']} | {a['findings_found']} / {a['findings_not_found']} | "
                         f"{a['catches']} | {a['unnamed_accepted']} | {a['new_destructive']} |")
        t = ent["total"]
        L.append("")
        for name, ps in ent["sets"].items():
            if ps["total"]["catchable_found"]:
                c = ps["total"]
                L.append(f"- {name}: catches in the catchable subset (class B, 14 rows; {c['catchable_found']} found in the drafts): {c['catches_catchable']}.")
        L.append(f"- Rejected changes by rule over all sets ({t['rejected']} rejected, {t['dropped_entries']} malformed entries dropped): {reasons_text(t['reasons'])}.")
        L.append(f"- Where accepted changes' quotes were found: {reasons_text(t['accepted_quote_found_in'])}.")
        L.append(f"- Replies: {reasons_text(t['status'])}; valid JSON {t['valid_json']} of {t['answers']}; cut {t['cut']}; rows without a usable reply {t['not_run']}.")
        L.append(f"- Added seconds (the check's own time per answer, {t['seconds_n']} rows): median {fmt(t['seconds_median'])}, 90th percentile {fmt(t['seconds_p90'])}, maximum {fmt(t['seconds_max'])}.")
        b = ent["bar_sets_total"]
        L.append(f"- B1 and pilot2 together: {b['answers']} answers, {b['catches']} catches against {b['findings_found']} findings found in the drafts "
                 f"({b['findings_not_found']} quotes not found in the draft, left out).")
        L.append("")
        L.append(f"Bars for {v}" + (f" (damage count given by the reader: {ent['damage']})" if ent["damage"] is not None else " (the damage count is not known yet)") + ":")
        L.append("")
        for k in ("1", "2", "3", "4", "5"):
            verdict, txt = ent["bars"][k]
            L.append(f"- Bar {k}: **{verdict}**. {txt}.")
        L.append("")
    if res.get("think"):
        t = res["think"]["aggregate"]
        L += ["#### The checking pass (V1) on the thinking drafts (mechanical measures only; not part of the bars)", "",
              f"- {t['answers']} drafts: proposed {t['proposed']}, accepted {t['accepted']}, rejected {t['rejected']}; answers changed {t['answers_changed']}; replies {reasons_text(t['status'])}; "
              f"cut {t['cut']}; new destructive accepted {t['new_destructive']}.",
              f"- Added seconds: median {fmt(t['seconds_median'])}, 90th percentile {fmt(t['seconds_p90'])}, maximum {fmt(t['seconds_max'])}.", ""]
    L += ["The code does not choose a variant. The rule: the variant that goes into the six-arm test is the one with more net catches among those that clear every bar; a tie goes to V1; none "
          "clearing means the pass does not go into the test. Bars 1 and 2 (and the harmful part of bar 3) wait for the reading of the reader files.", ""]
    return "\n".join(L)


def parse_damage(s):
    out = {}
    for kv in (s or "").split(","):
        if kv.strip():
            k, v = kv.split("=")
            if k.strip() not in VARIANTS or not v.strip().isdigit():
                raise PilotError(f"--damage: {kv!r} is not VARIANT=count with a variant of {VARIANTS}")
            out[k.strip()] = int(v)
    return out or None


def cmd_score(a):
    spec = load_spec(a.spec)
    W = Path(a.work)
    damage = parse_damage(a.damage)
    res, reader = score_all(spec, W, damage, use_think=not a.no_think_check)
    out = Path(a.out_prefix) if a.out_prefix else W / "score"
    for v, rows in reader.items():
        if v in res["variants"]:
            wr(out.parent / f"{out.name}_reader_{v}.jsonl", rows)
    Path(f"{out}.json").write_text(json.dumps(res, indent=1, sort_keys=True, default=list) + "\n", encoding="utf-8")
    md = score_md(res)
    Path(f"{out}.md").write_text(md, encoding="utf-8")
    for v, ent in res["variants"].items():
        print(f"score {v}: {ent['total']['answers']} answers, accepted {ent['total']['accepted']}, catches on B1 and pilot2 {ent['bar_sets_total']['catches']}; reader file "
              f"{out.parent / f'{out.name}_reader_{v}.jsonl'} ({len(reader[v])} rows)")
        for k in ("1", "2", "3", "4", "5"):
            print(f"  bar {k}: {ent['bars'][k][0]}: {ent['bars'][k][1]}")
    print("choice: not made by this script (the conductor decides after the reading)")
    return 0


def cmd_header_text(a):
    print(HEADER_TEXT, end="")
    return 0


def cmd_code_pins(a):
    print("\n".join(CODE_PINS))
    return 0


def cmd_done_line(a):
    W = Path(a.work)
    spec = load_spec(a.spec)
    runs = plan_runs(spec, W, not a.no_think_check)
    n = sum(len(rd(r["out"])) for r in runs)
    print(f"M7 PILOT DONE: {len(rd(W / 'think/ans_think.jsonl'))} thinking drafts, {n} checks")
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)

    def spec_arg(p):
        p.add_argument("--spec", default=None, help="a spec file instead of the real sets (dry runs and tests)")
    p = sub.add_parser("gate-files")
    spec_arg(p)
    p = sub.add_parser("input-files")
    spec_arg(p)
    p = sub.add_parser("runs")
    spec_arg(p)
    p.add_argument("--work", required=True)
    p = sub.add_parser("select-think")
    spec_arg(p)
    p.add_argument("--out-dir", required=True)
    p = sub.add_parser("think-drafts")
    p.add_argument("--answers", required=True)
    p.add_argument("--out", required=True)
    p = sub.add_parser("think-measures")
    p.add_argument("--answers", required=True)
    p.add_argument("--out-json", required=True)
    p.add_argument("--out-md", required=True)
    p.add_argument("--stub", action="store_true")
    p.add_argument("--cap", type=int, default=None, help="the thinking cap of the run (tokens added to the answer limit), for the report")
    p = sub.add_parser("check-think")
    p.add_argument("--file", required=True)
    p.add_argument("--expect", type=int, required=True)
    p = sub.add_parser("check-verify")
    p.add_argument("--file", required=True)
    p.add_argument("--expect", type=int, required=True)
    for name in ("manifest", "start-check"):
        add_state_args(sub.add_parser(name))
    p = sub.add_parser("score")
    spec_arg(p)
    p.add_argument("--work", required=True)
    p.add_argument("--out-prefix", default=None, help="default: WORK/score (writes .json, .md and _reader_V.jsonl)")
    p.add_argument("--no-think-check", action="store_true", help="the checking pass on the thinking drafts was skipped (no usable thinking draft)")
    p.add_argument("--damage", default=None, help="the reader's count of damaged correct lines, e.g. V1=2,V1C=0 (leave out while the reading is open)")
    p = sub.add_parser("done-line")
    spec_arg(p)
    p.add_argument("--work", required=True)
    p.add_argument("--no-think-check", action="store_true")
    sub.add_parser("header-text")
    sub.add_parser("code-pins")
    a = ap.parse_args(argv)
    fn = {"gate-files": cmd_gate_files, "input-files": cmd_input_files, "runs": cmd_runs, "select-think": cmd_select_think, "think-drafts": cmd_think_drafts,
          "think-measures": cmd_think_measures, "check-think": cmd_check_think, "check-verify": cmd_check_verify, "manifest": cmd_manifest, "start-check": cmd_start_check,
          "score": cmd_score, "header-text": cmd_header_text, "code-pins": cmd_code_pins, "done-line": cmd_done_line}[a.cmd]
    try:
        return fn(a)
    except (PilotError, G.RuleError, D.DaError, OSError, KeyError, ValueError) as e:
        print(f"ERROR: {type(e).__name__}: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
