#!/usr/bin/env python3
"""Helper of scripts/m4_e5_bakeoff.sh (E5, the open screen of the base-model bake-off; plan runs/m4/reports/e5_plan.md, sections 1, 3, 4).

Open files only. No model, no server, no network. Nothing here reads a hidden file: every path argument is refused when it ends in .enc or
has a path part named hidden. Prints names, counts and totals.

  manifest          write runs/m4/E5_SCREEN_MANIFEST.json (refuses to overwrite): the stored reference answers by sha256, the open inputs, the
                    frozen files, the scripts that decide, the settings, the model ids as the server lists them
  pin-check         the pinned files against the manifest (the reference files, the open inputs, the frozen files, the scripts the answers and
                    the judge come from); the compare script, the chain and this helper are recorded for information only
  merge-rows        join jsonl files (ids must be unique)        merge-items   the same, harness-normalised items (tier, kind, family, domain)
  answers-state     one answers file: rows, errors, empty, ended; exit 0 clean and complete, 2 complete with errors, 1 incomplete
  judge-expect      the number of judge rows the answer sets give (rows with text, times the passes)       judge-done   that many valid rows
  runline           one line of numbers for an answers file     median   the median of seconds.total     running-median   a line while a run goes
  mem-parse         the server's memory in GB from its status call (stdin), or "none"       mem-summary   the peak of the samples
  fake-status       the fake server status of a dry run          dry-inject   a planted defect of a dry run (empty answers, a changed repeat)
  report            the screen's report (markdown)
"""
import argparse
import hashlib
import json
import re
import statistics
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

REFERENCE = {
    "A1_answers": "work/m4/s6/ans_A1.jsonl",          # 251 open tier A items
    "a3_answers": "work/m4/e2/ans_O_Q4D_a3.jsonl",    # 88 open a3 items
    "B1_answers": "work/m4/b1/ans_default.jsonl",     # 30 open judged scenarios
    "B2_answers": "work/m4/b2/ans_default.jsonl",     # 30 open judged scenarios
}
INPUTS = ["eval/m4/open/tier_a.jsonl", "eval/m4/open/tier_a3.jsonl", "eval/m4/b1/items.jsonl", "eval/m4/b1/rubrics.jsonl",
          "eval/m4/b2/items.jsonl", "eval/m4/b2/rubrics.jsonl", "eval/m4/smoke_ids.json", "eval/m4/SPLIT.json", "eval/m4/SPLIT_A3.json",
          "work/v3/chunks_s1.jsonl", "work/m4/catalogues/CATALOGUES.json", "runs/v3/INDEX_FROZEN.json"]
SCRIPTS_ENFORCED = ["scripts/eval2.py", "scripts/eval2_score.py", "scripts/eval2_judge.py", "scripts/eval2_idcheck.py", "scripts/run_guarded.py"]
SCRIPTS_INFO = ["scripts/eval2_compare.py", "scripts/m4_e5_bakeoff.sh", "scripts/m4_e5_screen_helper.py"]
THINK_RE = re.compile(r"</?\s*(think|thinking|reasoning)\s*>", re.I)
E2_MANIFEST = "runs/m4/E2_MANIFEST.json"


class HelperError(RuntimeError):
    pass


def refuse_hidden(p):
    s = str(p)
    if s.endswith(".enc") or "hidden" in Path(s).parts:
        raise HelperError(f"refused: {Path(s).name} is a hidden-side path (the screen uses open files only)")
    return p


def sha(path):
    h = hashlib.sha256()
    with open(refuse_hidden(path), "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def rows_of(path):
    p = Path(refuse_hidden(path))
    if not p.exists():
        return []
    return [json.loads(l) for l in p.read_text().splitlines() if l.strip()]


def manifest_of(path):
    p = Path(str(path) + ".manifest.json")
    return json.loads(p.read_text()) if p.exists() else {}


def reload_count(path):
    """How many times the server reloaded the model while a run was resumed (the `reloads` list of the run's manifest, written by eval2.py)."""
    return len(manifest_of(path).get("reloads") or [])


def tree_files(rel_dir, patterns=("*.py", "*.json")):
    d = ROOT / rel_dir
    out = []
    for pat in patterns:
        out += [p for p in d.glob(pat) if p.is_file()]
    return sorted({p.relative_to(ROOT).as_posix() for p in out})


def golden_files():
    d = ROOT / "tests/golden"
    return sorted(p.relative_to(ROOT).as_posix() for p in d.iterdir() if p.is_file())


def pinned_now():
    """{name: sha256} of the enforced group, and the information group."""
    enforced, info = {}, {}
    for k, rel in REFERENCE.items():
        enforced[f"reference/{k}"] = sha(ROOT / rel)
        if (ROOT / (rel + ".manifest.json")).exists():
            enforced[f"reference/{k}.manifest"] = sha(ROOT / (rel + ".manifest.json"))
    for rel in INPUTS + SCRIPTS_ENFORCED + tree_files("kafka_sre_rag") + golden_files():
        enforced[rel] = sha(ROOT / rel)
    for rel in SCRIPTS_INFO:
        if (ROOT / rel).exists():
            info[rel] = sha(ROOT / rel)
    return enforced, info


def num(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool)


# ---------------------------------------------------------------- manifest and pin-check

def cmd_manifest(a):
    out = Path(refuse_hidden(a.out))
    if out.exists():
        raise HelperError(f"{out} exists: the manifest is written once")
    enforced, info = pinned_now()
    ref = {}
    for k, rel in REFERENCE.items():
        rows = rows_of(ROOT / rel)
        ref[k] = {"file": rel, "sha256": enforced[f"reference/{k}"], "rows": len(rows), "errors": sum(1 for r in rows if r.get("error"))}
    models = json.loads(Path(a.models_json).read_text()) if a.models_json else {}
    listed = sorted(m.get("id") for m in (models.get("data") or []) if isinstance(m, dict) and m.get("id"))
    server = json.loads(Path(a.server_json).read_text()) if a.server_json else {}
    cands = {}
    for spec in a.candidate:
        key, _, rest = spec.partition("=")
        configured, _, resolved = rest.partition("=")
        cands[key] = {"configured_id": configured, "server_id": resolved or None, "listed": bool(resolved) and resolved in listed}
    e2 = {}
    if (ROOT / E2_MANIFEST).exists():
        j = (json.loads((ROOT / E2_MANIFEST).read_text()).get("judge") or {})
        e2 = {"judge_script_sha256": j.get("script_sha256"), "judge_settings_name": j.get("settings_name"), "judge_instruction": j.get("instruction")}
    judge_now = enforced["scripts/eval2_judge.py"]
    doc = {
        "manifest_version": "e5_screen_manifest/1",
        "what": "E5 open screen of the base-model bake-off (no hidden look); rule S1 to S4 of runs/m4/reports/e5_plan.md section 1",
        "reference": {"name": a.reference_model, "files": ref,
                      "note": "stored answers, never regenerated; re-scored on the CPU with the current scorer; re-judged with the current judge code"},
        "pinned": {"count": len(enforced), "sha256": hashlib.sha256(json.dumps(enforced, sort_keys=True).encode()).hexdigest(), "files": enforced},
        "information_only": info,
        "judge": {"script_sha256": judge_now, "e2_manifest": e2,
                  "equals_e2_manifest": (e2.get("judge_script_sha256") == judge_now) if e2.get("judge_script_sha256") else None},
        "settings": json.loads(a.settings),
        "candidates": cands,
        "candidates_this_launch": a.selected.split(",") if a.selected else [],
        "server": {"version_fields": {k: v for k, v in server.items() if "version" in k.lower()}, "models_listed": listed},
        "dry_run": bool(a.dry_run),
    }
    out.write_text(json.dumps(doc, indent=1, ensure_ascii=False) + "\n")
    print(f"manifest written: {out.name}, {len(enforced)} pinned files, reference rows "
          + ", ".join(f"{k} {v['rows']}" for k, v in ref.items()))
    return 0


def cmd_pin_check(a):
    m = json.loads(Path(refuse_hidden(a.manifest)).read_text())
    enforced, _ = pinned_now()
    want = m["pinned"]["files"]
    bad = sorted(k for k in set(want) | set(enforced) if want.get(k) != enforced.get(k))
    if bad:
        print(f"PIN CHECK FAILED: {len(bad)} pinned file(s) differ from the manifest: " + ", ".join(bad[:12]) + (" ..." if len(bad) > 12 else ""))
        return 1
    print(f"pin check: {len(want)} pinned files match the manifest (the reference answers, the open inputs, the frozen files, the answer, score, judge and identifier scripts)")
    return 0


# ---------------------------------------------------------------- files

def write_if_changed(path, text):
    """A joined file that holds these bytes already is not touched (a resumed launch leaves every file and its time as it was)."""
    p = Path(refuse_hidden(path))
    p.parent.mkdir(parents=True, exist_ok=True)
    if not p.exists() or p.read_text() != text:
        p.write_text(text)


def cmd_merge_rows(a):
    rows, seen = [], set()
    for p in a.inp:
        for r in rows_of(p):
            if r["id"] in seen:
                raise HelperError(f"id {r['id']} occurs twice across {', '.join(Path(x).name for x in a.inp)}")
            seen.add(r["id"])
            rows.append(r)
    write_if_changed(a.out, "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows))
    print(f"merged {len(rows)} rows into {Path(a.out).name}")
    return 0


def normalise_item(r, domain):
    row = {"tier": "B", "kind": "open", "family": r.get("id"), "domain": domain, **r}
    if isinstance(row["domain"], list):
        row["domains"], row["domain"] = row["domain"], ",".join(map(str, row["domain"])) or domain
    for k in ("tier", "kind", "family", "domain"):
        if not isinstance(row[k], str) or not row[k]:
            raise HelperError(f"item {r.get('id')}: {k} is not a non-empty string")
    return row


def cmd_merge_items(a):
    rows, seen = [], set()
    for p in a.inp:
        for r in rows_of(p):
            if not r.get("id") or not r.get("question"):
                raise HelperError("an item without id or question")
            if r["id"] in seen:
                raise HelperError(f"id {r['id']} occurs twice")
            seen.add(r["id"])
            rows.append(normalise_item(r, a.domain))
    write_if_changed(a.out, "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows))
    print(f"merged {len(rows)} items into {Path(a.out).name}")
    return 0


def counts(path):
    rows = rows_of(path)
    ok = [r for r in rows if not r.get("error")]
    return {"rows": len(rows), "errors": len(rows) - len(ok), "empty": sum(1 for r in ok if not (r.get("text") or "").strip()),
            "reasoning": sum(1 for r in ok if THINK_RE.search(r.get("text") or "") or r.get("reasoning") or r.get("reasoning_content")),
            "truncated": sum(1 for r in ok if r.get("truncated")),
            "ended": bool(manifest_of(path).get("ended")), "ok": ok}


def cmd_answers_state(a):
    c = counts(a.file)
    print(f"{Path(a.file).name}: rows {c['rows']} of {a.expect}, errors {c['errors']}, empty {c['empty']}, ended {'yes' if c['ended'] else 'no'}")
    if c["rows"] != a.expect or not c["ended"]:
        return 1
    return 0 if not c["errors"] else 2


def judge_rows(sets):
    n = 0
    for spec in sets:
        name, _, path = spec.partition("=")
        n += sum(1 for r in rows_of(path) if not r.get("error") and (r.get("text") or "").strip())
    return n


def cmd_judge_expect(a):
    print(judge_rows(a.answers) * a.passes)
    return 0


def cmd_judge_done(a):
    p = Path(refuse_hidden(a.file))
    if not p.exists():
        return 1
    rows = rows_of(p)
    return 0 if len(rows) == a.expect and all(r.get("valid_reply") for r in rows) else 1


def secs(rows):
    return [r["seconds"]["total"] for r in rows if not r.get("error") and (r.get("seconds") or {}).get("total") is not None]


def cmd_runline(a):
    c = counts(a.file)
    rows = rows_of(a.file)
    t = secs(rows)
    tk = [r["tokens"] for r in rows if num(r.get("tokens"))]
    print(f"run {a.name}: {c['rows']} answers, {c['errors']} errors, {c['empty']} empty, {c['reasoning']} with reasoning text or think tags, "
          f"{c['truncated']} cut at the limit, median {statistics.median(t) if t else 0:.1f} s per answer (max {max(t) if t else 0:.1f} s), "
          f"median {statistics.median(tk) if tk else 0:.0f} tokens ({len(tk)} with a token count), total {sum(t) / 3600:.2f} h, "
          f"{reload_count(a.file)} model reloads")
    return 0


def cmd_median(a):
    t = secs(rows_of(a.file))
    print(f"{statistics.median(t):.3f}" if t else "")
    return 0 if t else 1


def cmd_running_median(a):
    t = secs(rows_of(a.file))
    state = Path(a.state)
    last = int(state.read_text()) if state.exists() else -1
    if len(t) == last or not t:
        return 0
    state.write_text(str(len(t)))
    med = statistics.median(t)
    ref = float(a.ref_median) if a.ref_median not in (None, "") else None
    lim = f"; the limit is 1.15 x the reference's median {ref:.1f} s = {1.15 * ref:.1f} s: {'ABOVE the limit' if med > 1.15 * ref else 'within the limit'}" if ref else ""
    print(f"[watch] {a.label}: {len(t)} answers so far, running median {med:.1f} s{lim}", flush=True)
    return 0


def parse_gb(v):
    if num(v):
        return v / 1e9 if v > 1e6 else float(v)
    m = re.match(r"\s*([0-9]+(?:\.[0-9]+)?)\s*([KMGT]?i?B)?", str(v or ""), re.I)
    if not m:
        return None
    x, u = float(m.group(1)), (m.group(2) or "GB").upper().replace("I", "")
    return x * {"B": 1e-9, "KB": 1e-6, "MB": 1e-3, "GB": 1.0, "TB": 1e3}.get(u, 1.0)


def find_memory(d):
    """The server's model memory in GB from a status dict: the figure the load test used (model_memory_used_formatted), else a
    numeric model_memory_used; None when the call gives no memory figure."""
    if not isinstance(d, dict):
        return None
    for k in ("model_memory_used_formatted", "model_memory_used"):
        if k in d:
            g = parse_gb(d[k])
            if g is not None:
                return g
    for v in d.values():
        if isinstance(v, dict):
            g = find_memory(v)
            if g is not None:
                return g
    return None


def cmd_mem_parse(a):
    try:
        g = find_memory(json.loads(sys.stdin.read()))
    except Exception:
        g = None
    print("none" if g is None else f"{g:.2f}")
    return 0


def mem_samples(path):
    p = Path(path)
    out = []
    if p.exists():
        for l in p.read_text().splitlines():
            parts = l.split()
            if len(parts) == 2:
                try:
                    out.append(float(parts[1]))
                except ValueError:
                    pass
    return out


def cmd_mem_summary(a):
    s = mem_samples(a.file)
    print(f"peak {max(s):.2f} GB over {len(s)} samples" if s else "no memory figure: the status call gave none")
    return 0


def cmd_fake_status(a):
    p = Path(a.file)
    d = json.loads(p.read_text()) if p.exists() else {"loaded_models": [], "active_requests": 0, "waiting_requests": 0, "version": "dry-run"}
    lm = list(d.get("loaded_models") or [])
    if a.action == "load" and a.model not in lm:
        lm.append(a.model)
    if a.action == "unload":
        lm = [m for m in lm if m != a.model]
    d["loaded_models"] = lm
    if lm:
        d["model_memory_used_formatted"] = "15.30 GB"   # stub figure of a dry run
    else:
        d.pop("model_memory_used_formatted", None)
    p.write_text(json.dumps(d))
    return 0


def cmd_dry_inject(a):
    rows = rows_of(a.file)
    if a.kind == "empty":
        for r in rows[:2]:
            r["text"] = ""
    elif a.kind == "nondet":
        for r in rows[:1]:
            if not (r.get("text") or "").endswith(" drift"):
                r["text"] = (r.get("text") or "") + " drift"
    new = "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows)
    if new != Path(a.file).read_text():   # idempotent: a file that holds the defect already is not touched
        Path(a.file).write_text(new)
    print(f"dry run: planted '{a.kind}' in {Path(a.file).name}")
    return 0


# ---------------------------------------------------------------- report

def fmt_hours(sec):
    return f"{sec / 3600:.2f} h"


def read_times(path):
    out = {}
    p = Path(path)
    if p.exists():
        for l in p.read_text().splitlines():
            f = l.split()
            if len(f) == 4:
                out.setdefault(f[0], {})[f[1]] = out.get(f[0], {}).get(f[1], 0) + max(0, int(f[3]) - int(f[2]))
    return out


def summary_line(path):
    p = Path(path)
    if not p.exists():
        return "not scored"
    d = json.loads(p.read_text())
    return f"{d.get('all_pass')} of {d.get('items_scored')} all-pass ({d.get('missing_answers')} missing)"


def mean_p(results, rubrics, name):
    try:
        sys.path.insert(0, str(ROOT / "scripts"))
        import eval2_compare as ec
        jp = ec.load_judged_p([results], rubrics)
        v = jp.get(name) or {}
        return f"{sum(v.values()) / len(v):.3f} over {len(v)} scenarios" if v else "no judged rows"
    except Exception as e:   # the compare script is edited by others; a broken import must not stop the report
        return f"not computed ({type(e).__name__})"


def run_row(path):
    c = counts(path)
    rows = rows_of(path)
    t = secs(rows)
    tk = [r["tokens"] for r in rows if num(r.get("tokens"))]
    return (f"{c['rows']} answers, {c['errors']} errors, {c['empty']} empty, {c['reasoning']} with reasoning or think tags, "
            f"{c['truncated']} cut at the limit ({100 * c['truncated'] / max(1, c['rows']):.1f} %), median {statistics.median(t) if t else 0:.1f} s, "
            f"median tokens {statistics.median(tk) if tk else 0:.0f}, {reload_count(path)} model reloads")


def cmd_report(a):
    W = Path(refuse_hidden(a.work))
    keys = a.candidates.split(",") if a.candidates else []
    names = dict(s.split("=", 1) for s in a.names)
    cmp_txt = Path(a.compare_out).read_text() if a.compare_out and Path(a.compare_out).exists() else "(the comparison was not run)"
    verdicts = dict(re.findall(r"^-- ([^:\s]+): (\w+)", cmp_txt, re.M))
    times = read_times(W / "times.tsv")
    ref_med = Path(W / "ref/median.txt").read_text().strip() if (W / "ref/median.txt").exists() else "?"
    L = []
    P = L.append
    P(f"# E5 screen: open items, no hidden look{' [DRY RUN: stub generator, stub judge, fake server status; every number is stub data or a small sample]' if a.dry_run else ''}")
    P("")
    P("Written by `scripts/m4_e5_bakeoff.sh` (stage SCREEN). Plan: `runs/m4/reports/e5_plan.md` section 1. The reference is the 4-bit Qwen3.8-27B; its answers are "
      "the stored open answers (never regenerated), re-scored on the CPU with the current scorer and re-judged with the current judge code and settings "
      "(two passes), so that the reference and the candidates are judged by the same code. Same settings for every model: golden prompts, frozen index and "
      "retrieval, greedy decoding, thinking off through the same extra body, the 2,048-token limit, one request at a time, default mode, end to end. "
      f"Manifest: `{a.manifest}`. Nothing under the hidden folder was read.")
    P("")
    P("## Verdict per candidate (rule S1 to S4, by `eval2_compare.py e5-screen`)")
    P("")
    P("| Candidate | Model id (as the server lists it) | Result | S1 | S2 | S3 | S4 |")
    P("|---|---|---|---|---|---|---|")
    for k in keys:
        blk = re.search(rf"^-- {re.escape(k)}: .*?(?=^-- |\Z|^go on)", cmp_txt, re.M | re.S)
        b = blk.group(0) if blk else ""
        st = lambda c: (re.search(rf"^\s+{c} (\w[\w ]*?):", b, re.M) or [None, "?"])[1]
        P(f"| {k} | {names.get(k, '?')} | {verdicts.get(k, '?')} | {st('S1')} | {st('S2')} | {st('S3')} | {st('S4')} |")
    P("")
    P("Full output of the rule:")
    P("")
    P("```")
    P(cmp_txt.rstrip())
    P("```")
    P("")
    P("## Reference (stored answers; re-scored and re-judged)")
    P("")
    P(f"- Mechanical, re-scored: tier A open {summary_line(W / 'ref/A1.summary.json')}; a3 open {summary_line(W / 'ref/a3.summary.json')}.")
    P(f"- Judged scenarios (B1 30 + B2 30): median answer {ref_med} s (`seconds.total` of the stored runs); coverage P (mean of two passes) "
      f"{mean_p(W / 'judge_results.jsonl', W / 'rubrics_judged.jsonl', 'R') if (W / 'judge_results.jsonl').exists() else 'not judged'}.")
    for f_, lab in (("ref/idcheck_A1.txt", "identifier check, tier A"), ("ref/idcheck_a3.txt", "identifier check, a3"), ("ref/idcheck_judged.txt", "identifier check, judged")):
        if (W / f_).exists():
            P(f"- {lab}:")
            P("")
            P("```")
            P((W / f_).read_text().rstrip())
            P("```")
    for k in keys:
        d = W / k
        P("")
        P(f"## Candidate {k} ({names.get(k, '?')})")
        P("")
        det = "not run"
        if (d / "ans_smokeA.jsonl").exists() and (d / "ans_smokeB.jsonl").exists():
            ra, rb = rows_of(d / "ans_smokeA.jsonl"), {r["id"]: r for r in rows_of(d / "ans_smokeB.jsonl")}
            diff = sum(1 for r in ra if r["id"] not in rb or (r.get("text") or "").encode() != (rb[r["id"]].get("text") or "").encode())
            det = f"{len(ra) - diff} of {len(ra)} answers byte-identical across the two runs ({'identical' if not diff else str(diff) + ' differ: S1 failure'})"
        P(f"- Determinism check (the first 20 open smoke items, two separate runs): {det}.")
        P(f"- Mechanical, default mode, end to end: {summary_line(d / 'mech.summary.json')} (reference on its own 339 items: see above; the paired difference is in the rule's S2 line).")
        P(f"- Judged scenarios: coverage P {mean_p(W / 'judge_results.jsonl', W / 'rubrics_judged.jsonl', k) if (W / 'judge_results.jsonl').exists() else 'not judged'}.")
        P("- Runs (seconds per answer include search, retrieval and generation):")
        P("")
        P("```")
        for nm in ("smokeA", "smokeB", "mech", "judged"):
            if (d / f"ans_{nm}.jsonl").exists():
                P(f"{nm:<7} " + run_row(d / f"ans_{nm}.jsonl"))
        P("```")
        ld = d / "loaded.json"
        if ld.exists():
            j = json.loads(ld.read_text())
            P(f"- Server memory with the candidate loaded alone (status call before the first answer): "
              + (f"{j['memory_gb']:.2f} GB" if num(j.get("memory_gb")) else "the status call gave no memory figure") + f"; loaded models then: {len(j.get('loaded') or [])} ({'alone' if j.get('alone') else 'NOT alone'}).")
        P(f"- Server memory during the judged run (sampled in the background, every {a.mem_every} s): "
          + (f"{(lambda s: f'peak {max(s):.2f} GB over {len(s)} samples')(mem_samples(d / 'mem_samples.txt'))}" if mem_samples(d / "mem_samples.txt") else "no memory figure: the status call gave none") + ".")
        for f_, lab in (("idcheck_mech.txt", "identifier check, mechanical answers"), ("idcheck_judged.txt", "identifier check, judged answers")):
            if (d / f_).exists():
                P(f"- {lab}:")
                P("")
                P("```")
                P((d / f_).read_text().rstrip())
                P("```")
        tt = times.get(k, {})
        P(f"- Time on the server: {fmt_hours(sum(tt.values()))} ("
          + (", ".join(f"{s} {fmt_hours(v)}" for s, v in tt.items()) or "no step timed") + ").")
    P("")
    P("## Times")
    P("")
    tj = times.get("judge", {})
    P(f"- Judge (DeepSeek, instruction v2, coverage role, two passes, reference and candidates in one run): {fmt_hours(sum(tj.values()))}.")
    P(f"- All steps timed in this work folder: {fmt_hours(sum(sum(v.values()) for v in times.values()))} of wall time (a resumed run adds only the steps it ran).")
    P("")
    P("## What was not checked")
    P("")
    P("- Quality beyond the rule: the screen decides only whom to drop; nothing here is a measured ranking.")
    P("- The reference's stored answers come from several commits (see `runs/m4/reports/e5_t4_templates.md`); the prompts were not regenerated and compared.")
    P("- The open half leans slightly toward the reference, because the prompts were built on Qwen.")
    P("- Memory: read from the status call's `model_memory_used_formatted`; where a sample says none, the call gave no figure.")
    Path(a.out).write_text("\n".join(L) + "\n")
    print(f"report written: {a.out}")
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("manifest")
    s.add_argument("--out", required=True)
    s.add_argument("--settings", required=True, help="JSON of the settings")
    s.add_argument("--candidate", action="append", default=[], metavar="KEY=CONFIGURED_ID=SERVER_ID")
    s.add_argument("--selected", default="")
    s.add_argument("--reference-model", default="Qwen3.8-27B-4bit")
    s.add_argument("--models-json")
    s.add_argument("--server-json")
    s.add_argument("--dry-run", action="store_true")
    s = sub.add_parser("pin-check")
    s.add_argument("--manifest", required=True)
    for n in ("merge-rows", "merge-items"):
        s = sub.add_parser(n)
        s.add_argument("--out", required=True)
        s.add_argument("--in", dest="inp", nargs="+", required=True)
        if n == "merge-items":
            s.add_argument("--domain", default="b12")
    s = sub.add_parser("answers-state")
    s.add_argument("--file", required=True)
    s.add_argument("--expect", type=int, required=True)
    s = sub.add_parser("judge-expect")
    s.add_argument("--answers", nargs="+", required=True, metavar="NAME=PATH")
    s.add_argument("--passes", type=int, default=2)
    s = sub.add_parser("judge-done")
    s.add_argument("--file", required=True)
    s.add_argument("--expect", type=int, required=True)
    s = sub.add_parser("runline")
    s.add_argument("--name", required=True)
    s.add_argument("--file", required=True)
    s = sub.add_parser("median")
    s.add_argument("--file", required=True)
    s = sub.add_parser("running-median")
    s.add_argument("--file", required=True)
    s.add_argument("--label", default="judged")
    s.add_argument("--ref-median", default="")
    s.add_argument("--state", required=True)
    sub.add_parser("mem-parse")
    s = sub.add_parser("mem-summary")
    s.add_argument("--file", required=True)
    s = sub.add_parser("fake-status")
    s.add_argument("--file", required=True)
    s.add_argument("action", choices=["init", "load", "unload"])
    s.add_argument("model", nargs="?")
    s = sub.add_parser("dry-inject")
    s.add_argument("--file", required=True)
    s.add_argument("--kind", choices=["empty", "nondet"], required=True)
    s = sub.add_parser("report")
    s.add_argument("--work", required=True)
    s.add_argument("--out", required=True)
    s.add_argument("--candidates", default="")
    s.add_argument("--names", nargs="*", default=[], metavar="KEY=ID")
    s.add_argument("--compare-out")
    s.add_argument("--manifest", default="runs/m4/E5_SCREEN_MANIFEST.json")
    s.add_argument("--mem-every", default="60")
    s.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(argv)
    try:
        for attr in ("out", "file", "manifest", "work", "models_json", "server_json", "compare_out", "state"):
            if getattr(a, attr, None):
                refuse_hidden(getattr(a, attr))
        for attr in ("inp",):
            for p in getattr(a, attr, None) or []:
                refuse_hidden(p)
        for spec in getattr(a, "answers", None) or []:
            refuse_hidden(spec.partition("=")[2])
        return {"manifest": cmd_manifest, "pin-check": cmd_pin_check, "merge-rows": cmd_merge_rows, "merge-items": cmd_merge_items,
                "answers-state": cmd_answers_state, "judge-expect": cmd_judge_expect, "judge-done": cmd_judge_done, "runline": cmd_runline,
                "median": cmd_median, "running-median": cmd_running_median, "mem-parse": cmd_mem_parse, "mem-summary": cmd_mem_summary,
                "fake-status": cmd_fake_status, "dry-inject": cmd_dry_inject, "report": cmd_report}[a.cmd](a)
    except HelperError as e:
        print(str(e), file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
