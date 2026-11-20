#!/usr/bin/env python3
"""Paired comparison of two score files of the M4 mechanical tier (agents/m4-evaluation.md, "Statistics"), and the report of step S6.

  eval2_compare.py pair --a ref.scores.jsonl --b cand.scores.jsonl --items items.jsonl [--margin 5]
        [--answers-a a.jsonl --answers-b b.jsonl] [--resamples 10000] [--seed 1] [--sizes 245] [--json out.json]
  eval2_compare.py pick-model --kind q4|bf16 < models.json       (picks the server's own model id; exits 1 if not exactly one)
  eval2_compare.py s6 --run A1=a1.jsonl --run A2=a2.jsonl --run S=... --run F=... --run FR=... --items items.jsonl --out report.md

Direction: the difference is b minus a (a = reference, b = candidate); "non-inferior" means b is not worse than a by the margin.
Per pair: per-item agreement and discordance p_d; all-pass rate of each with a 95 % Wilson interval; the paired difference with
a bootstrap interval clustered by family (families resampled with replacement, seeded, 10,000 resamples unless told); the
effective item count (variance of the per-item difference over the bootstrap variance, at most n); the smallest difference
visible at 80 % power, MDE = 2.80 * sqrt(p_d / n) (binary score, two-sided alpha 0.05); a non-inferiority verdict against a margin
in percentage points (pass when the lower end of the 90 % interval lies above minus the margin, else "not shown"); the same
rows per kind, marked descriptive; the items that differ (open side only). A hidden file (name ends in .enc, or hidden items)
gives totals only: no ids, no text. Given two answers files it also counts answer texts that are byte-identical.

  eval2_compare.py e2 --config Q4D=a.scores.jsonl [--config Q4D=a3.scores.jsonl] --config F=... [--config FR|Q4S|P0=...] --items I [--items I2]
        [--judged-results R --rubrics RB --judged-items JI] [--reading-json T.json | --reading-default-rate X ...] [--json out]
        the E2 decision rules of runs/m4/reports/e2_plan.md (sections 2 to 4) applied by code: (a) the cost of 4-bit, (b) the mode,
        (c) starting figures. Open files, or hidden .enc files (never mixed; hidden prints totals only).
Pure Python; no model, no server.
"""
import argparse
import json
import math
import random
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import eval2  # noqa: E402

Z_POWER = 2.80          # 1.96 + 0.84
DEFAULT_RESAMPLES = 10000


class CompareError(RuntimeError):
    pass


# ---------------------------------------------------------------- statistics

def wilson(k, n, z=1.96):
    if n == 0:
        return (None, None)
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (max(0.0, c - h), min(1.0, c + h))


def percentile(sorted_xs, p):
    if not sorted_xs:
        return None
    pos = (len(sorted_xs) - 1) * p / 100
    lo = int(pos)
    hi = min(lo + 1, len(sorted_xs) - 1)
    return sorted_xs[lo] + (sorted_xs[hi] - sorted_xs[lo]) * (pos - lo)


def mde(p_d, n):
    """Smallest paired difference of a binary score visible at 80 % power, two-sided 0.05, as a fraction."""
    if not n:
        return None
    return Z_POWER * math.sqrt(p_d / n)


def cluster_bootstrap(clusters, resamples, seed):
    """clusters: list of (sum of per-item differences, item count) per family. Returns the sorted resampled mean
    differences (sum over sampled families / items in them) and their variance."""
    rng = random.Random(seed)
    f = len(clusters)
    out = []
    for _ in range(resamples):
        s = m = 0
        for j in rng.choices(range(f), k=f):
            s += clusters[j][0]
            m += clusters[j][1]
        out.append(s / m)
    out.sort()
    mean = sum(out) / len(out)
    var = sum((x - mean) ** 2 for x in out) / (len(out) - 1) if len(out) > 1 else 0.0
    return out, var


def paired_stats(ids, a, b, family, resamples, seed, margin_pts=None):
    """ids: items in both files; a, b: id -> bool; family: id -> str."""
    n = len(ids)
    if n == 0:
        raise CompareError("no items in common")
    ka, kb = sum(a[i] for i in ids), sum(b[i] for i in ids)
    b01 = sum(1 for i in ids if not a[i] and b[i])   # fails in a, passes in b
    b10 = sum(1 for i in ids if a[i] and not b[i])   # passes in a, fails in b
    p_d = (b01 + b10) / n
    diff = (kb - ka) / n
    fam = defaultdict(lambda: [0, 0])
    for i in ids:
        fam[family[i]][0] += int(b[i]) - int(a[i])
        fam[family[i]][1] += 1
    clusters = sorted((tuple(v) for v in fam.values()))  # sorted: independent of dict order
    boot, var_boot = cluster_bootstrap(clusters, resamples, seed)
    d_vals = [int(b[i]) - int(a[i]) for i in ids]
    var_item = (sum(x * x for x in d_vals) - n * diff * diff) / (n - 1) if n > 1 else 0.0
    n_eff = min(n, var_item / var_boot) if var_boot > 0 and var_item > 0 else None
    ci90 = (percentile(boot, 5), percentile(boot, 95))
    ci95 = (percentile(boot, 2.5), percentile(boot, 97.5))
    res = {
        "n": n, "families": len(clusters), "agree": n - b01 - b10, "agreement": (n - b01 - b10) / n,
        "discordant": b01 + b10, "p_d": p_d, "a_to_b_gain": b01, "a_to_b_loss": b10,
        "a_pass": ka, "a_rate": ka / n, "a_wilson95": wilson(ka, n),
        "b_pass": kb, "b_rate": kb / n, "b_wilson95": wilson(kb, n),
        "diff": diff, "ci90": ci90, "ci95": ci95, "resamples": resamples, "seed": seed,
        "n_eff": n_eff, "mde": mde(p_d, n), "mde_at_n_eff": mde(p_d, n_eff) if n_eff else None,
    }
    if margin_pts is not None:
        m = margin_pts / 100.0
        res["margin_pts"] = margin_pts
        res["noninferior"] = bool(ci90[0] > -m)
        res["verdict"] = ("pass" if res["noninferior"] else "not shown")
    return res


# ---------------------------------------------------------------- loading

def load_scores(path):
    out = {}
    for r in eval2.read_jsonl(path):
        if "all_pass" in r and "id" in r:
            out[r["id"]] = bool(r["all_pass"])
    if not out:
        raise CompareError(f"{path}: no tier A score rows (all_pass)")
    return out


OUTCOMES = ("pass", "correct_decline", "wrong_decline", "wrong", "abstained")


def load_outcomes(path):
    """{id: outcome} from a score file whose rows carry the decline outcome (decision J5); {} when they do not."""
    return {r["id"]: r["outcome"] for r in eval2.read_jsonl(path) if "all_pass" in r and "id" in r and r.get("outcome")}


def outcome_table(ids, oa, ob, kind=None, only_kind=None):
    """Paired table of outcomes: matrix[a_outcome][b_outcome] counts, and the marginals of each set. Counts only."""
    if only_kind is not None:
        ids = [i for i in ids if kind.get(i) == only_kind]
    ids = [i for i in ids if i in oa and i in ob]
    m = {a: {b: 0 for b in OUTCOMES} for a in OUTCOMES}
    for i in ids:
        m[oa[i]][ob[i]] += 1
    return {"n": len(ids), "matrix": m, "a": {o: sum(m[o].values()) for o in OUTCOMES},
            "b": {o: sum(m[a][o] for a in OUTCOMES) for o in OUTCOMES}}


def load_items(path):
    return {it["id"]: it for it in eval2.read_jsonl(path)}


def answer_identity(path_a, path_b):
    """Counts over the items answered without error in both files."""
    ra = {r["id"]: r for r in eval2.read_jsonl(path_a) if not r.get("error") and r.get("text") is not None}
    rb = {r["id"]: r for r in eval2.read_jsonl(path_b) if not r.get("error") and r.get("text") is not None}
    common = sorted(set(ra) & set(rb))
    same = [i for i in common if ra[i]["text"].encode("utf-8") == rb[i]["text"].encode("utf-8")]
    same_pack = [i for i in common if ra[i].get("pack") == rb[i].get("pack")]
    return {"common": len(common), "identical_text": len(same), "identical_pack": len(same_pack),
            "only_in_a": len(set(ra) - set(rb)), "only_in_b": len(set(rb) - set(ra)),
            "identical_ids": same}


def compare_pair(a_path, b_path, items_path, resamples=DEFAULT_RESAMPLES, seed=1, margin=None, sizes=(),
                 answers_a=None, answers_b=None):
    hidden = any(eval2.is_hidden(p) for p in (a_path, b_path, items_path) if p)
    sa, sb = load_scores(a_path), load_scores(b_path)
    items = load_items(items_path) if items_path else {}
    ids = sorted(set(sa) & set(sb))
    family = {i: (items.get(i) or {}).get("family") or i for i in ids}
    kind = {i: (items.get(i) or {}).get("kind") or "unknown" for i in ids}
    res = {"hidden": hidden, "only_in_a": len(set(sa) - set(sb)), "only_in_b": len(set(sb) - set(sa))}
    res["overall"] = paired_stats(ids, sa, sb, family, resamples, seed, margin)
    res["mde_at_sizes"] = {str(n): mde(res["overall"]["p_d"], n) for n in sizes}
    by_kind = {}
    for k in sorted(set(kind.values())):
        kids = [i for i in ids if kind[i] == k]
        by_kind[k] = paired_stats(kids, sa, sb, family, resamples, seed, margin)
        by_kind[k]["descriptive"] = True
    res["by_kind"] = by_kind
    differ = [(i, "gain" if sb[i] else "loss", kind[i]) for i in ids if sa[i] != sb[i]]
    res["differ_counts"] = {"gain": sum(1 for d in differ if d[1] == "gain"), "loss": sum(1 for d in differ if d[1] == "loss")}
    oa, ob = (load_outcomes(a_path), load_outcomes(b_path)) 
    if oa and ob:
        res["outcomes"] = outcome_table(ids, oa, ob)
        res["outcomes_by_kind"] = {k: outcome_table(ids, oa, ob, kind, k) for k in sorted(set(kind.values()))}
    if not hidden:
        res["differ"] = [{"id": i, "direction": d, "kind": k} for i, d, k in differ]
    if answers_a and answers_b:
        ai = answer_identity(answers_a, answers_b)
        if hidden:
            ai.pop("identical_ids")
        res["answers"] = ai
    return res


def pct(x, d=1):
    return "n/a" if x is None else f"{100 * x:.{d}f}"


def render_pair(res, a_name, b_name, out):
    P = lambda s="": print(s, file=out)
    o = res["overall"]
    P(f"== {b_name} against {a_name} (difference = {b_name} minus {a_name})" + (" [hidden: totals only]" if res["hidden"] else ""))
    P(f"items in both {o['n']}  families {o['families']}  only in {a_name} {res['only_in_a']}  only in {b_name} {res['only_in_b']}")
    P(f"per-item agreement {o['agree']}/{o['n']} = {pct(o['agreement'])} %   discordant {o['discordant']} "
      f"(p_d {o['p_d']:.4f}; {o['a_to_b_gain']} fail->pass, {o['a_to_b_loss']} pass->fail)")
    wa, wb = o["a_wilson95"], o["b_wilson95"]
    P(f"all-pass {a_name}: {o['a_pass']}/{o['n']} = {pct(o['a_rate'])} %  Wilson 95 % [{pct(wa[0])}, {pct(wa[1])}]")
    P(f"all-pass {b_name}: {o['b_pass']}/{o['n']} = {pct(o['b_rate'])} %  Wilson 95 % [{pct(wb[0])}, {pct(wb[1])}]")
    P(f"paired difference {100 * o['diff']:+.2f} points; bootstrap clustered by family ({o['resamples']} resamples, seed {o['seed']}): "
      f"90 % [{100 * o['ci90'][0]:+.2f}, {100 * o['ci90'][1]:+.2f}]  95 % [{100 * o['ci95'][0]:+.2f}, {100 * o['ci95'][1]:+.2f}]")
    ne = o["n_eff"]
    P("effective item count " + (f"{ne:.1f} of {o['n']}" if ne else f"not defined (no differences or no variance); n {o['n']}"))
    P(f"smallest visible difference at 80 % power, MDE = 2.80 * sqrt(p_d / n): {pct(o['mde'], 2)} points at n {o['n']}"
      + (f", {pct(o['mde_at_n_eff'], 2)} points at the effective count" if o["mde_at_n_eff"] else ""))
    for n, v in res["mde_at_sizes"].items():
        P(f"  same p_d at n {n}: {pct(v, 2)} points")
    if "margin_pts" in o:
        P(f"non-inferiority of {b_name} to {a_name}, margin {o['margin_pts']:g} points: lower end of the 90 % interval "
          f"{100 * o['ci90'][0]:+.2f} against {-o['margin_pts']:g}: {o['verdict'].upper()}")
    P()
    P("per kind (descriptive: small n, no own power):")
    P(f"{'kind':<18}{'n':>5}{a_name:>9}{b_name:>9}{'diff':>8}{'90 % interval':>20}{'gain':>6}{'loss':>6}")
    for k, s in res["by_kind"].items():
        P(f"{k:<18}{s['n']:>5}{pct(s['a_rate']):>9}{pct(s['b_rate']):>9}{100 * s['diff']:>+8.1f}"
          f"{'[%+.1f, %+.1f]' % (100 * s['ci90'][0], 100 * s['ci90'][1]):>20}{s['a_to_b_gain']:>6}{s['a_to_b_loss']:>6}")
    if "outcomes" in res:
        ot = res["outcomes"]
        P()
        P(f"decline outcome, paired (rows {a_name}, columns {b_name}; counts over {ot['n']} items; all-pass above is unchanged)")
        P(f"{'':<18}" + "".join(f"{o:>17}" for o in OUTCOMES) + f"{'total':>8}")
        for a in OUTCOMES:
            P(f"{a:<18}" + "".join(f"{ot['matrix'][a][b]:>17}" for b in OUTCOMES) + f"{ot['a'][a]:>8}")
        P(f"{'total':<18}" + "".join(f"{ot['b'][b]:>17}" for b in OUTCOMES) + f"{ot['n']:>8}")
        P("per kind (counts: " + a_name + " / " + b_name + ")")
        P(f"{'kind':<18}{'n':>5}" + "".join(f"{o:>17}" for o in OUTCOMES))
        for k, t in res["outcomes_by_kind"].items():
            P(f"{k:<18}{t['n']:>5}" + "".join(f"{'%d / %d' % (t['a'][o], t['b'][o]):>17}" for o in OUTCOMES))
    if "answers" in res:
        ai = res["answers"]
        P()
        P(f"answer texts byte-identical: {ai['identical_text']} of {ai['common']} answered in both; identical packs {ai['identical_pack']} of {ai['common']}"
          f" (only in {a_name} {ai['only_in_a']}, only in {b_name} {ai['only_in_b']})")
    P()
    if res["hidden"]:
        P(f"items that differ: {res['differ_counts']['gain']} fail->pass, {res['differ_counts']['loss']} pass->fail (hidden: no ids shown)")
    else:
        P(f"items that differ ({len(res['differ'])}):")
        for d in res["differ"]:
            P(f"  {d['id']}  {d['kind']}  {'fails in %s, passes in %s' % (a_name, b_name) if d['direction'] == 'gain' else 'passes in %s, fails in %s' % (a_name, b_name)}")


# ---------------------------------------------------------------- model pick

def pick_model(ids, kind):
    """The server's own id for the 4-bit or the bf16 build of Qwen3.8-27B; exactly one must match."""
    want = {"q4": ("4bit", "4-bit"), "bf16": ("bf16",)}[kind]
    low = [(i, i.lower()) for i in ids]
    exact = [i for i, l in low if kind == "q4" and i == "Qwen3.8-27B-4bit"]
    if exact:
        return exact[0]
    hits = [i for i, l in low if "qwen3.8-27b" in l and any(w in l for w in want) and "-mtp" not in l]
    if len(hits) != 1:
        raise CompareError(f"{kind}: {len(hits)} models match on the server ({', '.join(hits) or 'none'})")
    return hits[0]


# ---------------------------------------------------------------- S6 report

STAGES = ("search_terms", "retrieval", "answer", "total")
MANIFEST_KEYS = ("run", "config", "backend", "model_requested", "model_echoed", "mode", "k", "rerank", "extra_body",
                 "max_tokens", "unmeasured_reasons", "index_sha256", "reranker_revision", "catalogue_sha256",
                 "runtime_git_commit", "runtime_tree_dirty", "concurrency", "host_class", "items_count", "subset",
                 "replay", "started", "ended")


def timing(rows):
    ok = [r for r in rows if not r.get("error")]
    out = {"answers": len(rows), "errors": len(rows) - len(ok), "truncated": sum(1 for r in ok if r.get("truncated"))}
    for st in STAGES:
        xs = sorted(r["seconds"][st] for r in ok)
        out[st] = {"median": percentile(xs, 50), "p90": percentile(xs, 90), "max": max(xs) if xs else None}
    toks = sorted(r["tokens"] for r in ok if r.get("tokens") is not None)
    out["tokens"] = {"median": percentile(toks, 50), "p90": percentile(toks, 90), "max": max(toks) if toks else None}
    out["over_60s"] = sum(1 for r in ok if r["seconds"]["total"] > 60)
    return out


def identifier_totals(score_rows):
    tally = Counter()
    with_unknown = 0
    for r in score_rows:
        ident = r.get("identifiers") or {}
        for st in ("known", "unknown", "not_covered"):
            tally[st] += len(ident.get(st, []))
        if ident.get("unknown"):
            with_unknown += 1
    classified = tally["known"] + tally["unknown"]
    return {"known": tally["known"], "unknown": tally["unknown"], "not_covered": tally["not_covered"],
            "precision": tally["known"] / classified if classified else None, "answers_with_unknown": with_unknown}


def ceiling_floor(score_sets):
    """score_sets: list of id->bool over the same ids. Share passed by every run and by none."""
    ids = sorted(set.intersection(*[set(s) for s in score_sets]))
    allp = sum(1 for i in ids if all(s[i] for s in score_sets))
    nonep = sum(1 for i in ids if not any(s[i] for s in score_sets))
    return {"n": len(ids), "all_runs_pass": allp, "no_run_passes": nonep}


def fnum(x, d=1):
    return "n/a" if x is None else f"{x:.{d}f}"


def build_s6_report(runs, items_path, margin, resamples, seed, hidden_n, servers, extra_notes=(), dry=False):
    """runs: ordered dict name -> answers path (score file next to it). Returns markdown text."""
    items = load_items(items_path)
    n_items = len(items)
    names = list(runs)
    scores = {n: eval2.read_jsonl(eval2.score_path(p)) for n, p in runs.items()}
    sc = {n: load_scores(eval2.score_path(runs[n])) for n in names}
    answers = {n: eval2.read_jsonl(p) for n, p in runs.items()}
    manifests = {n: eval2.read_manifest(p) for n, p in runs.items()}
    pairs = [("A1", "A2", "noise: the same configuration twice", None),
             ("F", "A1", "cost of 4-bit, end to end (a = full precision, b = 4-bit)", margin),
             ("FR", "A1", "cost of 4-bit on identical excerpts (a = full precision with replayed packs, b = 4-bit)", margin),
             ("A1", "S", "default against strict mode, 4-bit", None)]
    res = {}
    for a, b, label, m in pairs:
        if a in sc and b in sc:
            res[(a, b)] = (label, compare_pair(eval2.score_path(runs[a]), eval2.score_path(runs[b]), items_path, resamples,
                                               seed, m, (hidden_n,) if hidden_n else (),
                                               runs[a] if a == "A1" and b == "A2" else None,
                                               runs[b] if a == "A1" and b == "A2" else None))
    L = []
    P = lambda s="": L.append(s)
    # result line
    parts = []
    if ("A1", "A2") in res:
        o = res[("A1", "A2")][1]["overall"]
        ai = res[("A1", "A2")][1].get("answers") or {}
        parts.append(f"noise A1 vs A2: p_d {o['p_d']:.3f} ({o['discordant']} of {o['n']} items differ), all-pass {pct(o['a_rate'])} / {pct(o['b_rate'])} %, "
                     f"{ai.get('identical_text', '?')} of {ai.get('common', '?')} answer texts byte-identical, MDE {pct(o['mde'], 1)} points at n {o['n']}")
    for key, nm in ((("F", "A1"), "end to end"), (("FR", "A1"), "identical excerpts")):
        if key in res:
            o = res[key][1]["overall"]
            parts.append(f"4-bit minus full precision {nm}: {100 * o['diff']:+.1f} points, 90 % interval [{100 * o['ci90'][0]:+.1f}, {100 * o['ci90'][1]:+.1f}], "
                         f"non-inferiority at {margin:g} points {o['verdict']} (first look on the open half)")
    P("# S6: noise, and 4-bit against full precision (open mechanical tier)" + (" [DRY RUN: stub data, no model]" if dry else ""))
    P()
    P("result: " + "; ".join(parts) + ".")
    P()
    P("This is a first look on the **open** half (not the decision). The decision on the cost of 4-bit is taken later on the hidden half, "
      "against the margin fixed in advance, and costs one of the six hidden runs.")
    P()
    P(f"Items: {n_items} open mechanical items (`eval/m4/open/tier_a.jsonl`), one request at a time, thinking off, frozen index. "
      f"Bootstrap: {resamples} resamples clustered by family, seed {seed}.")
    # manifests
    P()
    P("## Runs and manifests")
    P()
    P("| run | config | model (requested / echoed) | mode | replay | items | answers | errors | truncated |")
    P("|---|---|---|---|---|---|---|---|---|")
    for n in names:
        m = manifests[n] or {}
        t = timing(answers[n])
        rp = (m.get("replay") or {}).get("run") if m.get("replay") else "-"
        P(f"| {n} | {m.get('config')} | {m.get('model_requested')} / {m.get('model_echoed')} | {m.get('mode')} | {rp} | {m.get('items_count')} | "
          f"{t['answers']} | {t['errors']} | {t['truncated']} |")
    P()
    P("Key manifest fields per run:")
    P()
    for n in names:
        m = manifests[n] or {}
        P(f"- **{n}**: " + "; ".join(f"{k} {json.dumps(m.get(k), ensure_ascii=False) if not isinstance(m.get(k), str) else m.get(k)}"
                                      for k in MANIFEST_KEYS if k in m))
    if servers:
        P()
        P("Server state recorded before each run (loaded models and version fields only):")
        P()
        for n in names:
            if n in servers:
                P(f"- **{n}**: {json.dumps(servers[n], ensure_ascii=False)}")
    # pass rates
    P()
    P("## All-pass rate per run")
    P()
    P("| run | all-pass | rate | Wilson 95 % | checks share | format failures |")
    P("|---|---|---|---|---|---|")
    for n in names:
        rows = scores[n]
        k, nn = sum(1 for r in rows if r["all_pass"]), len(rows)
        lo, hi = wilson(k, nn)
        cp, ct = sum(r["checks_passed"] for r in rows), sum(r["checks_total"] for r in rows)
        P(f"| {n} | {k}/{nn} | {pct(k / nn if nn else None)} % | [{pct(lo)}, {pct(hi)}] | {pct(cp / ct if ct else None)} % | {sum(1 for r in rows if not r.get('format_ok', True))} |")
    # pairs
    for (a, b), (label, r) in res.items():
        P()
        P(f"## {b} against {a}: {label}")
        P()
        P("```")
        buf = _Buf()
        render_pair(r, a, b, buf)
        P(buf.text().rstrip())
        P("```")
    # MDE
    P()
    P("## Smallest visible difference (80 % power, two-sided 0.05)")
    P()
    P(f"MDE = 2.80 * sqrt(p_d / n), in points of the all-pass rate. Open size n = {n_items}"
      + (f"; hidden size n = {hidden_n}" if hidden_n else "") + ". The discordance measured here is on the open half; "
      "the hidden half has other items, so its p_d is an estimate from this one.")
    P()
    P("| comparison | p_d | MDE at open n | " + (f"MDE at hidden n {hidden_n} |" if hidden_n else "") )
    P("|---|---|---|" + ("---|" if hidden_n else ""))
    for (a, b), (label, r) in res.items():
        o = r["overall"]
        P(f"| {b} vs {a} | {o['p_d']:.3f} | {pct(mde(o['p_d'], n_items), 1)} |" + (f" {pct(mde(o['p_d'], hidden_n), 1)} |" if hidden_n else ""))
    # ceiling and floor
    P()
    P("## Ceiling and floor")
    P()
    cf = ceiling_floor([sc[n] for n in names])
    P(f"Over all {len(names)} runs ({', '.join(names)}) on {cf['n']} items: every run passes {cf['all_runs_pass']} ({pct(cf['all_runs_pass'] / cf['n'])} %), "
      f"no run passes {cf['no_run_passes']} ({pct(cf['no_run_passes'] / cf['n'])} %).")
    dn = [n for n in names if n != "S"]
    if len(dn) != len(names):
        cd = ceiling_floor([sc[n] for n in dn])
        P(f"Over the default-mode runs only ({', '.join(dn)}): every run passes {cd['all_runs_pass']} ({pct(cd['all_runs_pass'] / cd['n'])} %), "
          f"no run passes {cd['no_run_passes']} ({pct(cd['no_run_passes'] / cd['n'])} %).")
    P("The design's early sign: a pass rate above 85 % means the tier is too easy to separate configurations.")
    # per kind
    P()
    P("## Pass rate per kind (descriptive: small n per kind, no own power)")
    P()
    kinds = sorted({it["kind"] for it in items.values()})
    P("| kind | n | " + " | ".join(names) + " |")
    P("|---|---|" + "---|" * len(names))
    for k in kinds:
        ids = [i for i, it in items.items() if it["kind"] == k]
        cells = []
        for n in names:
            have = [i for i in ids if i in sc[n]]
            cells.append(f"{sum(sc[n][i] for i in have)}/{len(have)}")
        P(f"| {k} | {len(ids)} | " + " | ".join(cells) + " |")
    # identifiers
    P()
    P("## Identifier precision (scorer's classification; not_covered is never counted as invented)")
    P()
    P("| run | known | unknown | not covered | precision known/(known+unknown) | answers with an unknown identifier |")
    P("|---|---|---|---|---|---|")
    for n in names:
        t = identifier_totals(scores[n])
        P(f"| {n} | {t['known']} | {t['unknown']} | {t['not_covered']} | {pct(t['precision'])} % | {t['answers_with_unknown']} |")
    # timing
    P()
    P("## Timing (seconds per answer; one request at a time)")
    P()
    P("Speed figures hold only if the server had no other client during the run; the chain checks idleness before each run, not during.")
    P()
    P("| run | stage | median | p90 | max |")
    P("|---|---|---|---|---|")
    for n in names:
        t = timing(answers[n])
        for st in STAGES:
            P(f"| {n} | {st} | {fnum(t[st]['median'])} | {fnum(t[st]['p90'])} | {fnum(t[st]['max'])} |")
        P(f"| {n} | answer tokens | {fnum(t['tokens']['median'], 0)} | {fnum(t['tokens']['p90'], 0)} | {fnum(t['tokens']['max'], 0)} |")
    P()
    P("| run | answers over 60 s (total) |")
    P("|---|---|")
    for n in names:
        P(f"| {n} | {timing(answers[n])['over_60s']} of {timing(answers[n])['answers']} |")
    # not measured
    P()
    P("## Not measured")
    P()
    for s in ("The hidden half: nothing here is a decision; the decision costs a hidden run, with its rule written in `runs/m4/HIDDEN_LEDGER.md` first.",
              "Tier B (judged) and the five other task types of `GOAL.md`: this is the mechanical tier only, whose items are short and mostly single-fact.",
              "Thinking on, other concurrency, other hosts; the in-process (mlx) path.",
              "Quality on real questions: these items are not the Owner's real questions.",
              "Whether the server was idle during a run: checked before each run only.",
              "Strict mode was run once, so its own noise is not known; per-kind rows are descriptive.",
              "The server's version string: recorded when the server reports one in its status, otherwise absent."):
        P(f"- {s}")
    for s in extra_notes:
        P(f"- {s}")
    return "\n".join(L) + "\n"


class _Buf:
    def __init__(self):
        self.parts = []

    def write(self, s):
        self.parts.append(s)

    def text(self):
        return "".join(self.parts)

    def flush(self):
        pass


# ---------------------------------------------------------------- E2 decision rules (runs/m4/reports/e2_plan.md, sections 2 to 4)

E2_MARGIN_PTS = 5.0           # rule 1(a) and 1(b), mechanical tier, in points
E2_P_MARGIN = 0.10            # rule 1(b), judged coverage P
E2_MIN_KIND = 5               # a kind is reported only with at least this many items
A3_TAG = "shares_variant_with_published"


def a3_item(it):
    """Items of slice a3: id prefix "a3-" or a generator of eval2_items_a3 (fields that exist in eval/m4/open/tier_a3.jsonl)."""
    return str(it.get("id", "")).startswith("a3-") or "items_a3" in str(it.get("generator", ""))


def kind_shown(k, counts, hidden):
    """Whether kind k of the item counts is printed. A kind needs at least E2_MIN_KIND items. On hidden input the items of the
    kinds not printed (the remainder) must also be none or at least E2_MIN_KIND, else total minus the printed kinds would give
    a group of 1 to 4 items; then the smallest printed kind joins the remainder (it has at least E2_MIN_KIND, so one step is enough)."""
    shown = {x for x, c in counts.items() if c >= E2_MIN_KIND}
    if hidden:
        rem = sum(c for x, c in counts.items() if x not in shown)
        if shown and 0 < rem < E2_MIN_KIND:
            shown.discard(min(shown, key=lambda x: (counts[x], x)))
    return k in shown


def load_items_many(paths):
    items = {}
    for p in paths:
        for i, it in load_items(p).items():
            if i in items:
                raise CompareError(f"{p}: an item id appears in two items files (count of such ids not shown)")
            items[i] = it
    return items


def load_scores_many(paths):
    """Merge several score files of one configuration (for example tier A and slice a3); an id twice is an error without the id."""
    out = {}
    for p in paths:
        s = load_scores(p)
        if set(s) & set(out):
            raise CompareError(f"{p}: {len(set(s) & set(out))} item ids already given by another file of the same configuration")
        out.update(s)
    return out


def load_judged_p(paths, rubrics_path):
    """{set: {id: mean coverage P over the valid passes}} from judge result rows, through eval2_judge.coverage_row.
    Relies on the row fields set, id, pass, valid_reply, must, must_not (and pack / essential_points, used by coverage_row)."""
    from eval2_judge import coverage_row
    rub = {r["id"]: r for r in eval2.read_jsonl(rubrics_path)}
    acc = defaultdict(lambda: defaultdict(list))
    for p in paths:
        for r in eval2.read_jsonl(p):
            if not r.get("valid_reply") or "must" not in r or "must_not" not in r or r.get("id") not in rub or "set" not in r:
                continue
            acc[r["set"]][r["id"]].append(coverage_row(r, rub[r["id"]])["P"])
    return {st: {i: sum(v) / len(v) for i, v in d.items()} for st, d in acc.items()}


def mean_boot(ids, vals, resamples, seed):
    """Mean of vals over ids with a 90 % bootstrap interval over the items (each item is its own cluster)."""
    n = len(ids)
    if not n:
        return {"n": 0, "mean": None, "ci90": (None, None)}
    boot, _ = cluster_bootstrap(sorted((vals[i], 1) for i in ids), resamples, seed)
    return {"n": n, "mean": sum(vals[i] for i in ids) / n, "ci90": (percentile(boot, 5), percentile(boot, 95))}


def judged_pair(a, b, resamples, seed):
    """b minus a of per-scenario P over the scenarios in both, bootstrap over scenarios."""
    ids = sorted(set(a) & set(b))
    if not ids:
        return None
    d = {i: b[i] - a[i] for i in ids}
    r = mean_boot(ids, d, resamples, seed)
    r["only_in_a"], r["only_in_b"] = len(set(a) - set(b)), len(set(b) - set(a))
    r["a_mean"] = sum(a[i] for i in ids) / len(ids)
    r["b_mean"] = sum(b[i] for i in ids) / len(ids)
    return r


def rate_figure(ids, sc, family, resamples, seed):
    n = len(ids)
    if not n:
        return {"n": 0, "pass": 0, "rate": None, "wilson95": (None, None), "ci90": (None, None)}
    fam = defaultdict(lambda: [0, 0])
    for i in ids:
        fam[family[i]][0] += int(sc[i])
        fam[family[i]][1] += 1
    boot, _ = cluster_bootstrap(sorted(tuple(v) for v in fam.values()), resamples, seed)
    k = sum(sc[i] for i in ids)
    return {"n": n, "pass": k, "rate": k / n, "wilson95": wilson(k, n), "ci90": (percentile(boot, 5), percentile(boot, 95)),
            "families": len(fam)}


def reading_from_args(a):
    """The sealed reading's totals, from a small JSON file and/or numbers on the command line (the command line wins).
    Fields: default_rate, strict_rate (wrong+invented+harmful non-minor per 10), default_harmful, strict_harmful,
    reader_disagreement (the readers' disagreement in the same unit, per 10 answers), readers_usable (false when the readers'
    totals differ by more than 30 %)."""
    d = {}
    if a.reading_json:
        d.update(json.loads(eval2.read_text(a.reading_json)))
    for k in ("default_rate", "strict_rate", "default_harmful", "strict_harmful", "reader_disagreement"):
        v = getattr(a, "reading_" + k)
        if v is not None:
            d[k] = v
    if a.readers_usable is not None:
        d["readers_usable"] = a.readers_usable == "yes"
    return d


def mode_reading_verdict(reading):
    """Third criterion of rule 1(b): strict's rate lower than default's by more than the readers' disagreement, harmful not higher.
    -> (state, text) with state in met | not met | waiting."""
    need = ("default_rate", "strict_rate", "default_harmful", "strict_harmful", "reader_disagreement")
    missing = [k for k in need if reading.get(k) is None]
    if missing:
        return "waiting", "waiting for the reading" + ("" if len(missing) == len(need) else " (missing: " + ", ".join(missing) + ")")
    if reading.get("readers_usable") is False:
        return "waiting", "waiting for the reading: the readers' totals differ by more than 30 %, the reading is not used"
    gain = reading["default_rate"] - reading["strict_rate"]
    ok_rate = gain > reading["reader_disagreement"]
    ok_harm = reading["strict_harmful"] <= reading["default_harmful"]
    txt = (f"strict lower than default by {gain:+.2f} per 10 (must exceed the readers' disagreement {reading['reader_disagreement']:.2f}): "
           f"{'yes' if ok_rate else 'no'}; harmful strict {reading['strict_harmful']} against default {reading['default_harmful']} "
           f"(not higher): {'yes' if ok_harm else 'no'}")
    return ("met" if ok_rate and ok_harm else "not met"), txt


def compare_e2(configs, items_paths, judged=None, resamples=DEFAULT_RESAMPLES, seed=1, reading=None):
    """configs: {name: [score files]}; judged: {"results": [...], "rubrics": path, "items": path or None}; reading: dict or None."""
    allp = [p for ps in configs.values() for p in ps] + list(items_paths)
    if judged:
        allp += list(judged["results"]) + [judged["rubrics"]] + ([judged["items"]] if judged.get("items") else [])
    flags = {eval2.is_hidden(p) for p in allp}
    if len(flags) > 1:
        raise CompareError("open and hidden files are never mixed in one comparison")
    hidden = flags == {True}
    items = load_items_many(items_paths)
    sc = {n: load_scores_many(ps) for n, ps in configs.items()}
    res = {"hidden": hidden, "resamples": resamples, "seed": seed, "configs": sorted(sc)}
    family = {i: it.get("family") or i for i, it in items.items()}
    kind = {i: it.get("kind") or "unknown" for i, it in items.items()}
    # bootstrap cluster: the item's pair where it has one (the a3 split groups by pair), else its family
    cluster = {i: (("pair", str(it["pair"])) if it.get("pair") else ("family", family[i])) for i, it in items.items()}

    def pair(a, b, margin=None):
        if a not in sc or b not in sc:
            return None
        ids = sorted(set(sc[a]) & set(sc[b]))
        if not ids:
            return None
        fam = {i: cluster.get(i, ("family", i)) for i in ids}
        r = paired_stats(ids, sc[a], sc[b], fam, resamples, seed, margin)
        r["clusters"] = r["families"]
        r["families"] = len({family.get(i, i) for i in ids})
        r["only_in_a"], r["only_in_b"] = len(set(sc[a]) - set(sc[b])), len(set(sc[b]) - set(sc[a]))
        return r

    # (a) the cost of 4-bit
    a = {}
    r = pair("F", "Q4D", E2_MARGIN_PTS)
    if r:
        lo, hi = r["ci90"]
        m = E2_MARGIN_PTS / 100
        r["one_sided"] = "PASS" if lo > -m else "FAIL"
        r["two_sided"] = "PASS" if (lo > -m and hi < m) else "FAIL"
        r["upper_above_margin"] = bool(hi > m)
        a["Q4D_minus_F"] = r
    a["Q4D_minus_FR"] = pair("FR", "Q4D")
    res["a_cost_of_4bit"] = a

    # judged coverage
    jp = load_judged_p(judged["results"], judged["rubrics"]) if judged else {}
    jitems = load_items(judged["items"]) if judged and judged.get("items") else {}
    res["judged_sets"] = {s: len(d) for s, d in sorted(jp.items())}

    def jpair(x, y):
        return judged_pair(jp[x], jp[y], resamples, seed) if x in jp and y in jp else None

    # (b) the mode
    b = {}
    r = pair("Q4D", "Q4S", E2_MARGIN_PTS)
    b["mechanical"] = r
    jr = jpair("Q4D", "Q4S")
    if jr:
        jr["noninferior"] = bool(jr["ci90"][0] > -E2_P_MARGIN)
    b["judged_P"] = jr
    b["mde_mechanical"] = r["mde"] if r else None
    rd = reading or {}
    b["reading"] = rd
    state, text = mode_reading_verdict(rd)
    b["reading_state"], b["reading_text"] = state, text
    mech_state = "waiting" if not r else ("met" if r["noninferior"] else "not met")
    p_state = "waiting" if not jr else ("met" if jr["noninferior"] else "not met")
    b["criteria"] = {"mechanical": mech_state, "judged_P": p_state, "reading": state}
    if state == "waiting":
        b["verdict"] = "waiting for the reading"
    elif "waiting" in (mech_state, p_state):
        b["verdict"] = "waiting for the " + ("mechanical runs" if mech_state == "waiting" else "judged coverage")
    elif mech_state == p_state == state == "met":
        b["verdict"] = "STRICT meets all three criteria"
    else:
        b["verdict"] = "DEFAULT stays the reference mode (not met: " + ", ".join(k for k, v in b["criteria"].items() if v != "met") + ")"
    res["b_mode"] = b

    # (c) starting figures
    figs = {}
    for n in sorted(sc):
        ids = sorted(i for i in sc[n] if i in items)
        a3 = [i for i in ids if a3_item(items[i])]
        seg = {"mechanical": ids, "tier_A_without_a3": [i for i in ids if i not in set(a3)], "a3": a3,
               "a3_without_tagged": [i for i in a3 if not items[i].get(A3_TAG)]}
        f = {k: rate_figure(v, sc[n], cluster, resamples, seed) for k, v in seg.items()}
        f["a3_tagged"] = sum(1 for i in a3 if items[i].get(A3_TAG))
        kinds = {}
        counts = Counter(kind[i] for i in ids)
        for k in sorted(counts):
            kids = [i for i in ids if kind[i] == k]
            if kind_shown(k, counts, hidden):
                kk = len(kids)
                kp = sum(sc[n][i] for i in kids)
                kinds[k] = {"n": kk, "pass": kp, "rate": kp / kk, "wilson95": wilson(kp, kk)}
        f["kinds"] = kinds
        figs[n] = f
    res["c_figures"] = {"mechanical": figs}
    jfigs = {}
    for s, d in sorted(jp.items()):
        ids = sorted(d)
        f = {"all": mean_boot(ids, d, resamples, seed), "kinds": {}}
        jkind = {i: (jitems.get(i) or {}).get("kind") or "unknown" for i in ids}
        jcounts = Counter(jkind.values())
        for k in sorted(jcounts):
            kids = [i for i in ids if jkind[i] == k]
            if kind_shown(k, jcounts, hidden):
                f["kinds"][k] = mean_boot(kids, d, resamples, seed)
        jfigs[s] = f
    res["c_figures"]["judged_P"] = jfigs
    res["c_closed_book_vs_retrieval"] = {"mechanical": pair("P0", "Q4D"), "judged_P": jpair("P0", "Q4D"), "descriptive": True}
    return res


def render_e2(res, out):
    P = lambda s="": print(s, file=out)
    pc = lambda x: "n/a" if x is None else f"{100 * x:+.2f}"
    ci = lambda t: "[%s, %s]" % (pc(t[0]), pc(t[1]))
    P("== E2 decision rules" + (" [hidden: totals only]" if res["hidden"] else "") +
      f"  (bootstrap clustered by family, {res['resamples']} resamples, seed {res['seed']}, 90 % interval; differences in points)")
    P("configurations: " + ", ".join(res["configs"]))
    P()
    P("(a) the cost of 4-bit: all-pass of Q4D minus F, same items")
    a = res["a_cost_of_4bit"]
    r = a.get("Q4D_minus_F")
    if not r:
        P("  not computed: scores of Q4D and F are both needed")
    else:
        P(f"  items {r['n']}  families {r['families']}  all-pass Q4D {r['b_pass']}/{r['n']} = {pc(r['b_rate'])[1:]} %  F {r['a_pass']}/{r['n']} = {pc(r['a_rate'])[1:]} %")
        P(f"  bootstrap clusters used: {r['clusters']} (an item's pair where it has one, else its family)")
        P(f"  difference {pc(r['diff'])}  90 % interval {ci(r['ci90'])}  discordant {r['discordant']} (p_d {r['p_d']:.4f})")
        P(f"  one-sided reading (PASS if the lower end is above -{E2_MARGIN_PTS:g}): {r['one_sided']}  (lower end {pc(r['ci90'][0])})")
        P(f"  two-sided reading (PASS if the whole interval is inside +-{E2_MARGIN_PTS:g}): {r['two_sided']}  (interval {ci(r['ci90'])})")
        P("  the Owner has not chosen between the two readings; both are shown"
          + ("; the upper end is above +%g (noted, not a fail)" % E2_MARGIN_PTS if r["upper_above_margin"] else ""))
        P(f"  smallest difference visible at 80 % power from this discordance: {pct(r['mde'], 2)} points at n {r['n']}")
    r = a.get("Q4D_minus_FR")
    if r:
        P(f"  beside it, Q4D minus FR (decides nothing alone): items {r['n']}  difference {pc(r['diff'])}  90 % interval {ci(r['ci90'])}  p_d {r['p_d']:.4f}")
    else:
        P("  Q4D minus FR: not computed (FR scores not given)")
    P()
    P("(b) the mode: strict (Q4S) minus default (Q4D)")
    b = res["b_mode"]
    r = b["mechanical"]
    if r:
        P(f"  mechanical items {r['n']}: difference {pc(r['diff'])}  90 % interval {ci(r['ci90'])}  need lower end above -{E2_MARGIN_PTS:g}: "
          f"{'met' if r['noninferior'] else 'not met'}  (MDE {pct(r['mde'], 2)} points)")
    else:
        P("  mechanical: not computed (scores of Q4D and Q4S are both needed)")
    j = b["judged_P"]
    if j:
        P(f"  judged coverage P, scenarios {j['n']}: difference {j['mean']:+.4f}  90 % interval [{j['ci90'][0]:+.4f}, {j['ci90'][1]:+.4f}]  "
          f"need lower end above -{E2_P_MARGIN:.2f}: {'met' if j['noninferior'] else 'not met'}")
    else:
        P("  judged coverage P: not computed (judged results of Q4D and Q4S are both needed)")
    P(f"  sealed reading: {b['reading_text']}")
    P(f"  criteria: mechanical {b['criteria']['mechanical']}, judged P {b['criteria']['judged_P']}, reading {b['criteria']['reading']}")
    P(f"  verdict of rule 1(b): {b['verdict']}")
    P()
    P("(c) starting figures (decide nothing; all-pass rate, Wilson 95 % for kinds, bootstrap clustered by family 90 % for tiers)")
    P(f"{'config':<8}{'tier':<22}{'n':>5}{'pass':>6}{'rate %':>9}{'interval %':>22}")
    for n, f in res["c_figures"]["mechanical"].items():
        for k in ("mechanical", "tier_A_without_a3", "a3", "a3_without_tagged"):
            x = f[k]
            iv = "n/a" if x["rate"] is None else "[%.1f, %.1f]" % (100 * x["ci90"][0], 100 * x["ci90"][1])
            P(f"{n:<8}{k:<22}{x['n']:>5}{x['pass']:>6}{pct(x['rate']):>9}{iv:>22}")
        P(f"{'':<8}a3 items tagged (shared variant): {f['a3_tagged']}")
        for k, x in f["kinds"].items():
            P(f"{n:<8}{'kind ' + k:<22}{x['n']:>5}{x['pass']:>6}{pct(x['rate']):>9}{'[%.1f, %.1f] (Wilson 95)' % (100 * x['wilson95'][0], 100 * x['wilson95'][1]):>34}")
    if res["c_figures"]["judged_P"]:
        P(f"judged coverage P per configuration (mean over scenarios, bootstrap over scenarios 90 %; kinds with at least {E2_MIN_KIND} scenarios):")
        for s, f in res["c_figures"]["judged_P"].items():
            x = f["all"]
            P(f"  {s}: scenarios {x['n']}  mean P {x['mean']:.4f}  [{x['ci90'][0]:.4f}, {x['ci90'][1]:.4f}]")
            for k, y in f["kinds"].items():
                P(f"    {k}: scenarios {y['n']}  mean P {y['mean']:.4f}  [{y['ci90'][0]:.4f}, {y['ci90'][1]:.4f}]")
    cb = res["c_closed_book_vs_retrieval"]
    P("closed book against retrieval, Q4D minus P0 (paired, descriptive):")
    r = cb["mechanical"]
    P("  mechanical: " + (f"items {r['n']}  difference {pc(r['diff'])}  90 % interval {ci(r['ci90'])}" if r else "not computed (P0 scores not given)"))
    j = cb["judged_P"]
    P("  judged P: " + (f"scenarios {j['n']}  difference {j['mean']:+.4f}  90 % interval [{j['ci90'][0]:+.4f}, {j['ci90'][1]:+.4f}]"
                        if j else "not computed (P0 judged results not given)"))



def e2_verdicts(res):
    """The verdicts and figures of render_e2 as one dict: totals only, no item id (res holds none)."""
    out = {"hidden": res["hidden"], "resamples": res["resamples"], "seed": res["seed"], "configs": res["configs"],
           "margin_pts": E2_MARGIN_PTS, "p_margin": E2_P_MARGIN}
    r = res["a_cost_of_4bit"].get("Q4D_minus_F")
    if r:
        out["rule_1a"] = {"state": "computed", "n": r["n"], "families": r["families"], "clusters": r["clusters"],
                          "q4d_pass": r["b_pass"], "f_pass": r["a_pass"], "diff": r["diff"], "ci90": list(r["ci90"]),
                          "discordant": r["discordant"], "p_d": r["p_d"], "mde": r["mde"],
                          "one_sided": r["one_sided"], "two_sided": r["two_sided"], "upper_above_margin": r["upper_above_margin"]}
    else:
        out["rule_1a"] = {"state": "not computed: scores of Q4D and F are both needed"}
    fr = res["a_cost_of_4bit"].get("Q4D_minus_FR")
    out["q4d_minus_fr"] = {"n": fr["n"], "diff": fr["diff"], "ci90": list(fr["ci90"]), "p_d": fr["p_d"]} if fr else None
    b = res["b_mode"]
    m, j = b["mechanical"], b["judged_P"]
    out["rule_1b"] = {
        "verdict": b["verdict"], "criteria": b["criteria"], "reading_text": b["reading_text"],
        "mechanical": {"n": m["n"], "diff": m["diff"], "ci90": list(m["ci90"]), "mde": m["mde"], "noninferior": m["noninferior"]} if m else None,
        "judged_P": {"n": j["n"], "diff": j["mean"], "ci90": list(j["ci90"]), "noninferior": j["noninferior"]} if j else None}
    out["starting_figures"] = {"mechanical": res["c_figures"]["mechanical"], "judged_P": res["c_figures"]["judged_P"],
                               "closed_book_vs_retrieval": {k: (None if v is None else
                                                                {x: v[x] for x in ("n", "diff", "mean", "ci90") if x in v})
                                                            for k, v in res["c_closed_book_vs_retrieval"].items() if k != "descriptive"}}
    return out


# ---------------------------------------------------------------- cli

def kv(s):
    if "=" not in s:
        raise argparse.ArgumentTypeError("expected NAME=PATH")
    k, v = s.split("=", 1)
    return k, v


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("pair")
    p.add_argument("--a", required=True, help="score file of the reference (or of the first run)")
    p.add_argument("--b", required=True, help="score file of the candidate (or of the repeated run); may be the same file")
    p.add_argument("--items", help="items file (family and kind); without it every item is its own family")
    p.add_argument("--a-name", default="a")
    p.add_argument("--b-name", default="b")
    p.add_argument("--margin", type=float, default=None, help="non-inferiority margin in percentage points")
    p.add_argument("--resamples", type=int, default=DEFAULT_RESAMPLES)
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--sizes", type=int, nargs="*", default=[], help="further sizes n for the MDE at the measured p_d")
    p.add_argument("--answers-a")
    p.add_argument("--answers-b")
    p.add_argument("--json", help="write the result here (an .enc name encrypts)")
    m = sub.add_parser("pick-model")
    m.add_argument("--kind", choices=["q4", "bf16"], required=True)
    s = sub.add_parser("s6")
    s.add_argument("--run", action="append", type=kv, required=True, help="NAME=answers file; names A1 A2 S F FR; score file next to it")
    s.add_argument("--items", required=True)
    s.add_argument("--out", required=True)
    s.add_argument("--margin", type=float, default=5.0)
    s.add_argument("--resamples", type=int, default=DEFAULT_RESAMPLES)
    s.add_argument("--seed", type=int, default=1)
    s.add_argument("--hidden-n", type=int, default=0)
    s.add_argument("--server", action="append", type=kv, default=[], help="NAME=JSON file of the server state before that run")
    s.add_argument("--note", action="append", default=[])
    s.add_argument("--dry", action="store_true")
    e = sub.add_parser("e2", help="the E2 decision rules applied by code")
    e.add_argument("--config", action="append", type=kv, required=True,
                   help="NAME=score file (names Q4D, Q4S, F, FR, P0); the same name may be given again to merge files (tier A and a3)")
    e.add_argument("--items", action="append", required=True, help="items file (family, kind, a3 tag); repeat for several")
    e.add_argument("--judged-results", action="append", default=[], help="judge results file(s) with coverage rows; the row's set names the configuration")
    e.add_argument("--rubrics", help="rubrics file of the judged scenarios")
    e.add_argument("--judged-items", help="items file of the judged scenarios (kind)")
    e.add_argument("--reading-json", help="small JSON of the sealed reading's totals (default_rate, strict_rate, default_harmful, "
                                            "strict_harmful, reader_disagreement, readers_usable)")
    for k in ("default_rate", "strict_rate", "reader_disagreement"):
        e.add_argument("--reading-" + k.replace("_", "-"), dest="reading_" + k, type=float, default=None)
    for k in ("default_harmful", "strict_harmful"):
        e.add_argument("--reading-" + k.replace("_", "-"), dest="reading_" + k, type=int, default=None)
    e.add_argument("--readers-usable", choices=["yes", "no"], default=None)
    e.add_argument("--resamples", type=int, default=DEFAULT_RESAMPLES)
    e.add_argument("--seed", type=int, default=1)
    e.add_argument("--json", help="write the result here (an .enc name encrypts; required for hidden input)")
    e.add_argument("--json-out", help="write the verdicts and figures (totals only, no ids) here; an .enc name encrypts and is required for hidden input")
    a = ap.parse_args(argv)
    try:
        if a.cmd == "pair":
            if a.json and eval2.is_hidden(a.a) != eval2.is_hidden(a.json):
                raise CompareError("a hidden comparison writes a .enc json, a clear one does not")
            r = compare_pair(a.a, a.b, a.items, a.resamples, a.seed, a.margin, a.sizes, a.answers_a, a.answers_b)
            render_pair(r, a.a_name, a.b_name, sys.stdout)
            if a.json:
                data = (json.dumps(r, indent=1, ensure_ascii=False) + "\n").encode()
                eval2.write_atomic(a.json, eval2.encrypt_bytes(data) if eval2.is_hidden(a.json) else data)
        elif a.cmd == "e2":
            cfg = defaultdict(list)
            for n, pth in a.config:
                cfg[n].append(pth)
            judged = None
            if a.judged_results:
                if not a.rubrics:
                    raise CompareError("--judged-results needs --rubrics")
                judged = {"results": a.judged_results, "rubrics": a.rubrics, "items": a.judged_items}
            r = compare_e2(dict(cfg), a.items, judged, a.resamples, a.seed, reading_from_args(a))
            for jp_ in (a.json, a.json_out):
                if jp_ and eval2.is_hidden(jp_) != r["hidden"]:
                    raise CompareError("a hidden comparison writes a .enc json, a clear one does not")
            render_e2(r, sys.stdout)
            if a.json_out:
                data = (json.dumps(e2_verdicts(r), indent=1, ensure_ascii=False) + "\n").encode()
                eval2.write_atomic(a.json_out, eval2.encrypt_bytes(data) if r["hidden"] else data)
            if a.json:
                data = (json.dumps(r, indent=1, ensure_ascii=False) + "\n").encode()
                eval2.write_atomic(a.json, eval2.encrypt_bytes(data) if r["hidden"] else data)
        elif a.cmd == "pick-model":
            d = json.load(sys.stdin)
            ids = [x["id"] for x in (d.get("data") or []) if isinstance(x, dict) and x.get("id")]
            print(pick_model(ids, a.kind))
        elif a.cmd == "s6":
            runs = dict(a.run)
            servers = {}
            for n, pth in a.server:
                servers[n] = json.loads(Path(pth).read_text())
            text = build_s6_report(runs, a.items, a.margin, a.resamples, a.seed, a.hidden_n, servers, a.note, a.dry)
            eval2.write_atomic(a.out, text.encode())
            print(text.splitlines()[2] if len(text.splitlines()) > 2 else "")
            print(f"report written to {a.out}")
    except (CompareError, eval2.EvalError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
