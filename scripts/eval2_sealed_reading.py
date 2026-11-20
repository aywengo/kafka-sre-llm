#!/usr/bin/env python3
"""Sealed blind reading of hidden judged answers (agents/m4-evaluation.md, "Readings on the hidden half", J2).

  eval2_sealed_reading.py bundle --items I.enc --rubrics R.enc --answers default=A1.enc strict=A2.enc --chunks C \\
      --out-dir DIR --seed 20261119 --first-sessions 6 --second-sessions 2 [--second-reader 0.25]
  eval2_sealed_reading.py totals --dir DIR [--default default] [--resamples 10000] [--seed 1] [--json-out FILE]

Every input and output that holds hidden rows is read and written through eval2.py's encryption (.enc, key from
EVAL_HIDDEN_KEY or ~/.config/kafka-sre-llm/eval_hidden.key). --chunks may be plain or .enc. Plain text lives in memory only; no
temporary plain file is written. Printed lines hold names, counts, hashes and totals, never an item id of a finding, a quote or a text.

bundle: writes DIR/first_<k>.jsonl.enc (k = 1..N) and DIR/second_<k>.jsonl.enc (k = 1..M) and the blind key DIR/blind_key.json.enc.
A bundle row is one answer: {"id": "<item>-<letter>", "item", "question", "evidence", "points" (the rubric's points as the open reading
shows them), "reference", "answer", "excerpts" (the passages of the answer's own pack)}. The letters are drawn per item from the seed, rows
are shuffled, both answers of an item lie in the same bundle. The second readers get a seeded share (default 25 %) of the items (whole
items, stratified by kind, like eval2_blind.py), with the same answer names. Nothing of the set, mode or configuration is in a bundle.

Reader sessions read a row with `sealed_io.py show BUNDLE --ids ROW` and write findings with `sealed_io.py seal DIR/findings_<bundle>.jsonl.enc
--schema finding [--append]` (row format of eval2_blind.py). An answer read without a finding gets a row of kind "clean"
(quote and point empty). At totals every answer of a bundle must have a finding row or a clean row, else it is an error (a count and the
bundle name; an unread answer cannot pass as a clean one).

totals: unblinds in memory and prints totals only (see the output). Rows that fail validation (answer not in the reader's bundle, quote
not found in the answer after whitespace is normalised, omission naming no must point of the item, duplicate) are not counted; only their
number is printed.
"""
import argparse
import hashlib
import json
import random
import re
import sys
from collections import Counter, OrderedDict, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import eval2  # noqa: E402
import eval2_blind as eb  # noqa: E402
import eval2_judge as ej  # noqa: E402
from eval2 import EvalError  # noqa: E402
from eval2_pilot_report import split_point_ids, load_tag_file, TagError  # noqa: E402

KEY_NAME = "blind_key.json.enc"
WIH = ("wrong", "invented", "harmful")
READER_DIFF = 0.30
E3_RATE, E3_MIN_ANSWERS = 2.0, 60


class ReadingError(Exception):
    pass


def require_enc(path, what):
    if not eval2.is_hidden(path):
        raise ReadingError(f"{what} must be an .enc path")


def read_rows(path):
    try:
        return eval2.read_jsonl(path)
    except ValueError:
        raise ReadingError(f"{Path(path).name}: not valid JSONL") from None


def write_enc(path, data):
    path = Path(path)
    if path.exists():
        raise ReadingError(f"{path.name} exists; not overwriting")
    eval2.write_atomic(path, eval2.encrypt_bytes(data))
    return hashlib.sha256(path.read_bytes()).hexdigest()


def rows_bytes(rows):
    return "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows).encode("utf-8")


# ---------------------------------------------------------------- bundle

def split_groups(ids, n):
    return [ids[i::n] for i in range(n)]


def cmd_bundle(a):
    sets = eb.parse_named(a.answers, "--answers")
    for what, p in [("--items", a.items), ("--rubrics", a.rubrics), *[(f"--answers {s}", x) for s, x in sets.items()]]:
        require_enc(p, what)
    if not sets:
        raise ReadingError("--answers: at least one answer set is needed")
    items = read_rows(a.items)
    rub = {r["id"]: r for r in read_rows(a.rubrics)}
    answers = {s: {r["id"]: r for r in read_rows(p)} for s, p in sets.items()}
    chunks = {c["id"]: c for c in read_rows(a.chunks)}
    if not items:
        raise ReadingError("no items")
    if a.first_sessions < 1 or a.second_sessions < 0:
        raise ReadingError("session counts: first at least 1, second at least 0")
    if len(items) < a.first_sessions:
        raise ReadingError("fewer items than first-reader sessions")
    for it in items:
        if it["id"] not in rub:
            raise ReadingError(f"item {it['id']} has no rubric")
        for s in sets:
            r = answers[s].get(it["id"])
            if r is None or r.get("error") or not (r.get("text") or "").strip():
                raise ReadingError(f"set {s}: no usable answer for item {it['id']}")
    out = Path(a.out_dir)
    names_new = [f"first_{k}.jsonl.enc" for k in range(1, a.first_sessions + 1)] + \
                [f"second_{k}.jsonl.enc" for k in range(1, a.second_sessions + 1)] + [KEY_NAME]
    for n in names_new:
        if (out / n).exists():
            raise ReadingError(f"{n} exists in the output folder; not overwriting")

    aid, rows_of, must_of = OrderedDict(), {}, {}
    for it in items:
        iid = it["id"]
        names = eb.neutral_names(iid, sets, a.seed)
        aid.update(names)
        rb = rub[iid]
        must_of[iid] = {"must": [p["id"] for p in rb["must"]],
                        "groups": [sorted(g) for g in ej.groups_of(rb)]}
        points = "\n".join(eb.render_points(rb))
        for name, s in names.items():
            parts = []
            for cid in answers[s][iid].get("pack") or []:
                c = chunks.get(cid)
                if c is None:
                    raise ReadingError(f"answer {name}: chunk {cid} is not in the chunks file")
                head = f"----- chunk {cid} | {c.get('path') or c.get('source') or ''}" + \
                       (f" | {c['title']}" if c.get("title") else "") + " -----"
                parts.append(head + "\n" + c["text"].rstrip("\n"))
            rows_of[name] = {"id": name, "item": iid, "question": it["question"], "evidence": it.get("evidence") or "",
                             "points": points, "reference": rb.get("reference") or "",
                             "answer": answers[s][iid]["text"].rstrip("\n"),
                             "excerpts": "\n\n".join(parts) if parts else "(none)"}

    order = sorted((it["id"] for it in items), key=lambda i: eval2._order_key(a.seed, i))
    kind_of = {it["id"]: it.get("kind") or "" for it in items}
    bundles = OrderedDict()
    for k, grp in enumerate(split_groups(order, a.first_sessions), 1):
        bundles[f"first_{k}"] = grp
    second_ids = []
    if a.second_sessions:
        if not 0 < a.second_reader <= 1:
            raise ReadingError("--second-reader: a fraction between 0 and 1")
        second_ids = eb.pick_second(items, a.second_reader, a.seed)
        if len(second_ids) < a.second_sessions:
            raise ReadingError("fewer second-reader items than second-reader sessions")
        ordered = sorted(second_ids, key=lambda i: eval2._order_key(a.seed + "s", i))
        for k, grp in enumerate(split_groups(ordered, a.second_sessions), 1):
            bundles[f"second_{k}"] = grp

    out.mkdir(parents=True, exist_ok=True)
    report = []
    key_bundles = OrderedDict()
    for name, grp in bundles.items():
        members = set(grp)
        rows = [rows_of[n] for n in aid if n.rsplit("-", 1)[0] in members]
        random.Random(f"{a.seed}:rows:{name}").shuffle(rows)
        h = write_enc(out / f"{name}.jsonl.enc", rows_bytes(rows))
        key_bundles[name] = [r["id"] for r in rows]
        report.append((f"{name}.jsonl.enc", len(rows), h))
    key = {"seed": a.seed, "sets": list(sets), "items": order, "second_ids": sorted(second_ids),
           "aid": aid, "bundles": key_bundles, "points": must_of, "kind": kind_of}
    kh = write_enc(out / KEY_NAME, json.dumps(key, ensure_ascii=False).encode("utf-8"))
    for n, c, h in report:
        print(f"bundle {n}: {c} rows, sha256 {h}")
    print(f"key {KEY_NAME}: sha256 {kh}")
    if len(sets) == 1:
        print("one answer set only: the readers know the set (a default-only reading); there is no blinding between modes")
    print(f"first readers {a.first_sessions}, second readers {a.second_sessions}; items {len(items)}, answers {len(aid)}, "
          f"second-reader items {len(second_ids)} ({100 * len(second_ids) / len(items):.0f} %)")
    return 0


# ---------------------------------------------------------------- totals

def norm(s):
    return re.sub(r"\s+", " ", s or "").strip()


def validate(row, bundle_names, key, text_of):
    """-> None if the row counts, else a reason code (never printed with ids)."""
    name = row.get("answer")
    if name not in bundle_names:
        return "not in the reader's bundle"
    if row.get("id") != name.rsplit("-", 1)[0]:
        return "id does not match answer"
    kind = row.get("kind")
    if kind not in eb.KINDS:
        return "bad kind"
    for f in ("minor", "supported"):
        if f in row and not isinstance(row[f], bool):
            return "bad flag"
    pts = key["points"][row["id"]]
    if kind == "omission":
        if "supported" not in row:
            return "omission without supported"
        ids = split_point_ids(row.get("point"))
        if not ids or any(i not in pts["must"] for i in ids):
            return "omission names no must point"
        if len(set(ids)) > 1 and sorted(set(ids)) not in pts["groups"]:
            return "omission names no any_of group"
        return None
    q = norm(row.get("quote"))
    if not q:
        return "no quote"
    if q not in norm(text_of[name]):
        return "quote not found"
    return None


def load_tags(path, known_ids):
    try:
        return load_tag_file(path, known_ids)
    except TagError as e:
        raise ReadingError(str(e)) from None


def percentile(sorted_vals, q):
    if not sorted_vals:
        return float("nan")
    pos = q * (len(sorted_vals) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(sorted_vals) - 1)
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (pos - lo)


def interval(vals):
    v = sorted(vals)
    return percentile(v, 0.05), percentile(v, 0.95)


def per10(num, den):
    return 10.0 * num / den if den else float("nan")


def cmd_totals(a):
    d = Path(a.dir)
    key = json.loads(eval2.decrypt_bytes((d / KEY_NAME).read_bytes()).decode("utf-8"))
    sets = key["sets"]
    if a.default not in sets:
        raise ReadingError("--default is not a set of the key")
    bundles = key["bundles"]
    text_of = {}
    for b in bundles:
        for r in read_rows(d / f"{b}.jsonl.enc"):
            text_of[r["id"]] = r["answer"]
    rows_by_bundle, why_count = {}, Counter()
    missing = [b for b in bundles if not (d / f"findings_{b}.jsonl.enc").exists()]
    if missing:
        raise ReadingError("no findings file for bundle(s): " + ", ".join(missing))
    for b, names in bundles.items():
        rows, seen = [], set()
        covered, cleaned, found = set(), set(), set()
        for r in read_rows(d / f"findings_{b}.jsonl.enc"):
            if r.get("answer") in names:
                covered.add(r["answer"])
            if r.get("kind") == "clean":
                if r.get("answer") in names:
                    cleaned.add(r["answer"])
                continue
            why = validate(r, set(names), key, text_of)
            sig = (r.get("answer"), r.get("kind"), norm(r.get("quote")), r.get("point") or "")
            if why is None and sig in seen:
                why = "duplicate"
            seen.add(sig)
            if r.get("answer") in names:
                found.add(r["answer"])
            if why is not None:
                why_count[why] += 1
                continue
            rows.append(r)
        unread = len(set(names) - covered)
        if unread:
            raise ReadingError(f"bundle {b}: {unread} of {len(names)} answers have neither a finding nor a clean row")
        if cleaned & found:
            raise ReadingError(f"bundle {b}: {len(cleaned & found)} answers have both a clean row and a finding")
        rows_by_bundle[b] = rows

    first_b = [b for b in bundles if b.startswith("first_")]
    second_b = [b for b in bundles if b.startswith("second_")]
    set_of = key["aid"]

    # per item, per set: [answers, nonminor wrong, invented, harmful, minor wrong, invented, harmful, omissions, supported omissions]
    cells = defaultdict(lambda: [0] * 9)
    kcol = {"wrong": 0, "invented": 1, "harmful": 2}
    for b in first_b:
        for n in bundles[b]:
            cells[(n.rsplit("-", 1)[0], set_of[n])][0] += 1
        for r in rows_by_bundle[b]:
            c = cells[(r["id"], set_of[r["answer"]])]
            if r["kind"] == "omission":
                c[7] += 1
                c[8] += bool(r.get("supported"))
            else:
                c[1 + kcol[r["kind"]] + (3 if r.get("minor") else 0)] += 1
    items = sorted({i for i, _ in cells})
    all_items = items
    tagged = load_tags(a.tag_file, set(all_items))

    def analyse(excl, label):
        items = [i for i in all_items if i not in excl]
        if not items:
            raise ReadingError('no items remain after the exclusion')
        outl = []
        out = outl.append

        def col(sample, s, *cols):
            return sum(cells[(i, s)][c] for i in sample for c in cols)

        NM, MI = (1, 2, 3), (4, 5, 6)
        rng = random.Random(a.seed)
        samples = [[rng.choice(items) for _ in items] for _ in range(a.resamples)]

        def rate_of(x, s):
            return per10(col(x, s, *NM), col(x, s, 0))

        out(f"first readers: {len(first_b)} bundles, second readers: {len(second_b)} bundles; bootstrap over {len(items)} items, "
            f"{a.resamples} resamples, seed {a.seed}, 90 % interval")
        if not excl:
            out(f"rows not counted: {sum(why_count.values())} (duplicates {why_count['duplicate']}); "
                f"quotes not found in their answer: {why_count['quote not found']}")
        res = {}
        for s in sets:
            n = col(items, s, 0)
            nm, mi, har = col(items, s, *NM), col(items, s, *MI), col(items, s, 3)
            om, sup = col(items, s, 7), col(items, s, 8)
            ci_nm = interval([rate_of(x, s) for x in samples])
            ci_mi = interval([per10(col(x, s, *MI), col(x, s, 0)) for x in samples])
            res[s] = {"answers_read": n, "rate_per_10": per10(nm, n), "rate_ci90": [ci_nm[0], ci_nm[1]],
                      "harmful": har, "minor_rate_per_10": per10(mi, n), "minor_by_kind": {k: col(items, s, 4 + c) for k, c in kcol.items()},
                      "nm": nm}
            out(f"set {s}: answers read {n}")
            out(f"  wrong+invented+harmful non-minor {nm} = {per10(nm, n):.2f} per 10 [{ci_nm[0]:.2f}, {ci_nm[1]:.2f}]")
            out(f"  wrong+invented+harmful minor {mi} = {per10(mi, n):.2f} per 10 [{ci_mi[0]:.2f}, {ci_mi[1]:.2f}]")
            out("  minor by kind: " + ", ".join(f"{k} {res[s]['minor_by_kind'][k]}" for k in kcol) + " (not in the rate, not in harmful)")
            out(f"  harmful (non-minor) {har}")
            out(f"  omissions {om} = {per10(om, n):.2f} per 10; supported {sup}")

        # the second reader against the first on the shared answers (protocol rule 7: wrong+invented+harmful, minor rows included)
        usable, disagreement = None, None
        if second_b:
            shared_names = {n for b in second_b for n in bundles[b] if n.rsplit("-", 1)[0] not in excl}

            def tally(bs, only_shared):
                t = defaultdict(lambda: [0, 0])    # set -> [all rows, non-minor rows]
                for b in bs:
                    for r in rows_by_bundle[b]:
                        if r["kind"] in WIH and r["id"] not in excl and (not only_shared or r["answer"] in shared_names):
                            t[set_of[r["answer"]]][0] += 1
                            t[set_of[r["answer"]]][1] += not r.get("minor")
                return t
            f_t, s_t = tally(first_b, True), tally(second_b, False)
            shared_n = Counter(set_of[n] for n in shared_names)
            shared = len(shared_names)

            def pct(x, y):
                big = max(x, y)
                return abs(x - y) / big if big else 0.0
            t1, t2 = sum(v[0] for v in f_t.values()), sum(v[0] for v in s_t.values())
            n1, n2 = sum(v[1] for v in f_t.values()), sum(v[1] for v in s_t.values())
            diff = pct(t1, t2)
            usable = diff <= READER_DIFF
            out(f"readers on {shared} shared answers: first {t1}, second {t2} "
                f"wrong+invented+harmful (both sets together, minor rows included); difference {100 * diff:.0f} % of the larger (at most {int(100 * READER_DIFF)} %): "
                f"{'PASS' if usable else 'readings not used'}")
            out(f"  non-minor rows only: first {n1}, second {n2}, difference {100 * pct(n1, n2):.0f} % (reported, not the criterion)")
            per_set = {}
            for s in sets:
                a1, a2 = f_t[s], s_t[s]
                per_set[s] = {"shared_answers": shared_n[s], "total_first": a1[0], "total_second": a2[0],
                              "difference_pct": 100 * pct(a1[0], a2[0]), "nonminor_first": a1[1], "nonminor_second": a2[1],
                              "nonminor_difference_pct": 100 * pct(a1[1], a2[1]),
                              "disagreement_per_10": per10(abs(a1[1] - a2[1]), shared_n[s])}
                out(f"  set {s}: {shared_n[s]} shared answers, first {a1[0]}, second {a2[0]} ({100 * pct(a1[0], a2[0]):.0f} %); "
                    f"non-minor first {a1[1]}, second {a2[1]} ({100 * pct(a1[1], a2[1]):.0f} %) (reported, not gating)")
            pooled = per10(abs(n1 - n2), shared)
            disagreement = {"per_set_per_10": {s: per_set[s]["disagreement_per_10"] for s in sets}, "pooled_per_10": pooled,
                            "shared_answers": shared, "totals_first": t1, "totals_second": t2, "difference_pct": 100 * diff,
                            "nonminor_first": n1, "nonminor_second": n2, "nonminor_difference_pct": 100 * pct(n1, n2),
                            "per_set": per_set}
            out("readers' disagreement, |first - second| non-minor wrong+invented+harmful per 10 answers on the shared answers: "
                + ", ".join(f"set {s} {per_set[s]['disagreement_per_10']:.2f}" for s in sets) + f"; pooled {pooled:.2f}")
        else:
            out("no second reader: the 30 % rule cannot be applied")

        for s in sets:
            if s == a.default:
                continue
            diffs = [rate_of(x, s) - rate_of(x, a.default) for x in samples]
            lo, hi = interval(diffs)
            pt = res[s]["rate_per_10"] - res[a.default]["rate_per_10"]
            out(f"paired {s} minus {a.default}, non-minor wrong+invented+harmful per 10: {pt:+.2f} [{lo:+.2f}, {hi:+.2f}]; "
                f"harmful {res[s]['harmful']} vs {res[a.default]['harmful']}")

        # the E3 rule of MILESTONE.md on the default set: non-minor rows only, for the rate and for harmful
        r = res[a.default]
        reasons = []
        if r["answers_read"] < E3_MIN_ANSWERS:
            reasons.append(f"{E3_MIN_ANSWERS - r['answers_read']} answers short of {E3_MIN_ANSWERS}")
        if r["rate_per_10"] > E3_RATE:
            reasons.append(f"rate {r['rate_per_10'] - E3_RATE:+.2f} per 10 above {E3_RATE}")
        if r["harmful"] > 0:
            reasons.append(f"harmful {r['harmful']} (must be 0)")
        if usable is not True:
            why = ("readings not used: the readers' totals differ by more than 30 %" if usable is False
                   else "no second reader, the 30 % rule cannot be applied")
            out(f"E3 {label}: NOT MEASURED ({why})" if label else f"E3: NOT MEASURED ({why})")
            e3 = "NOT MEASURED"
        else:
            e3 = "MET" if not reasons else "NOT MET"
            ci = r["rate_ci90"]
            out(f"E3 rule {label}on set {a.default} (non-minor rows only): rate {r['rate_per_10']:.2f} per 10 [{ci[0]:.2f}, {ci[1]:.2f}] "
                f"(at most {E3_RATE}), harmful {r['harmful']} (must be 0), answers {r['answers_read']} (at least {E3_MIN_ANSWERS}), "
                f"readers PASS: {e3}" + ("" if not reasons else " (" + "; ".join(reasons) + ")"))
        return outl, res, disagreement, usable, e3, reasons, r

    outl, res, disagreement, usable, e3, reasons, r = analyse(set(), "")
    print("\n".join(outl))
    without = {}
    for name, ids in tagged.items():
        excl = set(ids)
        o2, res2, dis2, usable2, e32, reasons2, r2 = analyse(excl, "for information ")
        print(f"\nwithout the items tagged {name} ({len(excl)} items; {len(all_items) - len(excl)} remain)")
        print("\n".join(o2))
        without[name] = (res2, dis2, usable2, e32, reasons2, r2, len(excl))

    if a.json_out:
        others = [s for s in sets if s != a.default]
        strict = "strict" if "strict" in sets and a.default != "strict" else (others[0] if len(others) == 1 else None)
        js = {"unit": "wrong+invented+harmful, non-minor, per 10 answers", "default_set": a.default, "strict_set": strict,
              "first_readers": len(first_b), "second_readers": len(second_b), "resamples": a.resamples, "seed": a.seed,
              "sets": {s: {k: v for k, v in res[s].items() if k != "nm"} for s in sets},
              "default_rate": r["rate_per_10"], "default_harmful": r["harmful"],
              "readers_usable": usable, "reader_disagreement": None if disagreement is None else disagreement["pooled_per_10"],
              "disagreement": disagreement, "readers_difference_pct": None if disagreement is None else disagreement["difference_pct"],
              "e3": {"verdict": e3, "reasons": reasons, "rate_limit": E3_RATE, "min_answers": E3_MIN_ANSWERS}}
        if without:
            js["without_tagged"] = {}
            for name, (res2, dis2, usable2, e32, reasons2, r2, n_ex) in without.items():
                js["without_tagged"][name] = {
                    "excluded_items": n_ex, "remaining_items": len(all_items) - n_ex,
                    "sets": {s: {k: v for k, v in res2[s].items() if k != "nm"} for s in sets},
                    "default_rate": r2["rate_per_10"], "default_harmful": r2["harmful"],
                    "readers_usable": usable2,
                    "reader_disagreement": None if dis2 is None else dis2["pooled_per_10"],
                    "disagreement": dis2,
                    "readers_difference_pct": None if dis2 is None else dis2["difference_pct"],
                    "strict_rate": res2[strict]["rate_per_10"] if strict else None,
                    "strict_harmful": res2[strict]["harmful"] if strict else None,
                    "e3": {"verdict": e32, "reasons": reasons2, "rate_limit": E3_RATE, "min_answers": E3_MIN_ANSWERS,
                           "for_information": True}}
        if strict:
            js["strict_rate"] = res[strict]["rate_per_10"]
            js["strict_harmful"] = res[strict]["harmful"]
        data = json.dumps(js, indent=1).encode("utf-8")
        target = Path(a.json_out)
        if target.exists():
            raise ReadingError(f"{target.name} exists; not overwriting")
        eval2.write_atomic(target, eval2.encrypt_bytes(data) if eval2.is_hidden(target) else data)
        print(f"totals written to {target.name} (totals only)")
    return 0


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("bundle")
    b.add_argument("--items", required=True)
    b.add_argument("--rubrics", required=True)
    b.add_argument("--answers", nargs="+", required=True, metavar="NAME=PATH")
    b.add_argument("--chunks", required=True)
    b.add_argument("--out-dir", required=True)
    b.add_argument("--seed", default="20261119")
    b.add_argument("--first-sessions", type=int, required=True)
    b.add_argument("--second-sessions", type=int, required=True)
    b.add_argument("--second-reader", type=float, default=0.25)
    b.set_defaults(fn=cmd_bundle)
    t = sub.add_parser("totals")
    t.add_argument("--dir", required=True)
    t.add_argument("--default", default="default")
    t.add_argument("--resamples", type=int, default=10000)
    t.add_argument("--seed", type=int, default=1)
    t.add_argument("--tag-file", help="eval/m4/HIDDEN_TAGS.json: print the totals again without each tag's items (for information)")
    t.add_argument("--json-out", help="write the totals (no ids, no text) as JSON in the shape of eval2_compare.py e2 --reading-json")
    t.set_defaults(fn=cmd_totals)
    a = p.parse_args(argv)
    try:
        return a.fn(a)
    except (ReadingError, EvalError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    except (OSError, KeyError, ValueError) as e:
        print(f"error: {type(e).__name__}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
