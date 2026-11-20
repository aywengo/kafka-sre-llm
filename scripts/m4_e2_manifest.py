#!/usr/bin/env python3
"""Helper of scripts/m4_e2_starting_point.sh (E2, the starting-point measurement on the hidden half).

Every subcommand that touches hidden rows reads them through scripts/eval2.py (key from EVAL_HIDDEN_KEY or
~/.config/kafka-sre-llm/eval_hidden.key), keeps plain text in memory only and prints names, counts, hashes and totals: never an item
text, an answer, a quote, and never an id of a hidden item on stdout or stderr (the id scan names files, not ids). Errors name the
condition, not values. Outputs that hold hidden rows end in .enc.

  prepare          write the manifest (refuses to overwrite)            verify-manifest   compare the manifest with the current state
  gate             the hidden gate (every condition printed, exit 1 if any fails)
  ledger           ledger rows 1 and 2 present, each with a date and a decision rule (the file is read, never written)
  counts           row counts of the hidden files (and rubric ids = item ids)
  shape            key names and counts of the hidden items and rubrics against what the answer step, the judge, the bundles and the scorer
                   read; the stub judge, the stub answers, the scorer and a throwaway reading bundle run on them (nothing plain is kept)
  scan             no plain-text file under a folder contains an id of a hidden item; with --text also none of its distinctive text
  gate-g           the byte-identity gate: answers against the stored ones
  check-answers    complete answers file (rows, errors, empty, packs, manifest ended)        runline   one line of totals
  judge-done       the judge results file holds the expected rows, all valid
  merge            join items or rubrics files into one (.enc stays .enc), optional harness normalisation of items
  subset           seeded subset of rows (by id)         items-for   the items of an items file whose ids occur in an answers file
  agreement        two judges on the same answers (units, totals only)                         empty-enc   an empty encrypted file
  synth            synthetic hidden files (for dry runs and tests) with marker strings in the text fields
  verdict          the rule-1(a) verdict of the chosen margin reading from the output of eval2_compare.py e2
  fake-status      edit the fake server status of a dry run
"""
import argparse
import hashlib
import json
import os
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))
import eval2  # noqa: E402

MANIFEST_VERSION = "e2_manifest/1"
EXPECTED = {"tier_a": 245, "tier_a3": 60, "judged_1": 24, "judged_2": 36}
JUDGE = {"model": "DeepSeek-V4.1-Flash-1017-oQ4e-mtp", "settings_name": "deepseek-nothink-12k", "instruction": "v2", "coverage": True,
         "extra_body": {"chat_template_kwargs": {"thinking_mode": "chat"}, "max_tokens": 4096}, "source_chars": 12000, "passes": [1, 2],
         "concurrency": 3}
CROSS = {"model": "GLM-5.3-Flash-oQ8e-mtp", "settings_name": "glm-think-12k", "extra_body": {"max_tokens": 32768}, "source_chars": 12000,
         "passes": [1], "subset": 24, "seed": 1, "concurrency": 3}


class GateError(Exception):
    pass


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def rjson(path):
    return json.loads(Path(path).read_text())


def rows_of(path):
    return eval2.read_jsonl(path)


def wrows(path, rows):
    eval2.write_jsonl(path, rows)


def grep_const(path, name):
    m = re.search(r'^%s\s*=\s*"([^"]+)"' % re.escape(name), Path(path).read_text(), re.M)
    return m.group(1) if m else None


def git(*args, cwd=ROOT):
    r = subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True)
    return r.returncode, r.stdout.strip()


def tree_dirty(repo):
    """Tracked or untracked changes, except untracked files under eval/hidden/ (the encrypted hidden files wait there)."""
    rc, out = git("status", "--porcelain", "--untracked-files=all", cwd=repo)
    if rc != 0:
        return None
    return [l for l in out.splitlines() if l.strip() and not l.startswith("?? eval/hidden/")]


def golden_hash(repo):
    h = hashlib.sha256()
    g = Path(repo) / "tests/golden"
    for p in sorted(x for x in g.rglob("*") if x.is_file() and "__pycache__" not in x.parts):
        h.update(str(p.relative_to(g)).encode() + b"\0" + p.read_bytes() + b"\0")
    return h.hexdigest()


def pick(models_json, kind):
    r = subprocess.run([sys.executable, str(ROOT / "scripts/eval2_compare.py"), "pick-model", "--kind", kind], input=Path(models_json).read_text(),
                       capture_output=True, text=True)
    return r.stdout.strip() if r.returncode == 0 else None


def listed_models(models_json):
    d = rjson(models_json)
    return sorted(m.get("id") for m in d.get("data", []) if isinstance(m, dict))


def revise_prompt_hash(repo):
    try:
        from kafka_sre_rag import revise
        return revise.prompt_sha256()
    except Exception as e:  # noqa: BLE001
        return f"unavailable:{type(e).__name__}"


def compute(a):
    """The state the manifest records. Pure reads: hashes, versions, names."""
    repo = Path(a.repo)
    frozen = rjson(repo / "runs/v3/INDEX_FROZEN.json")
    idx = repo / frozen["index_path"]
    chunks = Path(a.chunks) if getattr(a, "chunks", None) else repo / "work/v3/chunks_s1.jsonl"
    s6ref = Path(a.s6_ref) if getattr(a, "s6_ref", None) else repo / "work/m4/s6/ans_A1.jsonl"
    cat = repo / "work/m4/catalogues/CATALOGUES.json"
    cat3 = repo / "work/m4/catalogues_a3/CATALOGUES.json"
    rc, head = git("rev-parse", "HEAD", cwd=repo)
    srv = rjson(a.server_json) if a.server_json else {}
    q4, bf = pick(a.models_json, "q4"), pick(a.models_json, "bf16")
    lm = listed_models(a.models_json)
    hidden = {}
    for spec in a.hidden or []:
        name, _, path = spec.partition("=")
        hidden[name] = {"file": Path(path).name, "sha256": sha(path)}
    m = {
        "manifest_version": MANIFEST_VERSION,
        "commit": head if rc == 0 else None,
        "index": {"path": frozen["index_path"], "sha256": sha(idx), "recorded_sha256": frozen["sha256"],
                  "reranker_revision": (frozen.get("reranker") or {}).get("revision")},
        "chunks": {"name": chunks.name, "sha256": sha(chunks), "recorded_sha256": frozen["sha256"]},
        "s6_reference": {"name": s6ref.name, "sha256": sha(s6ref) if s6ref.exists() else None},
        "catalogues": {"open_sha256": sha(cat), "a3_sha256": sha(cat3) if cat3.exists() else None},
        "golden_prompts": {"sha256": golden_hash(repo)},
        "frozen": {"frozen_json_sha256": sha(repo / "kafka_sre_rag/frozen.json"), "prompts_py_sha256": sha(repo / "kafka_sre_rag/prompts.py")},
        "scorer": {"version": grep_const(repo / "scripts/eval2_score.py", "VERSION"), "sha256": sha(repo / "scripts/eval2_score.py")},
        "idcheck": {"version": grep_const(repo / "scripts/eval2_idcheck.py", "VERSION"), "sha256": sha(repo / "scripts/eval2_idcheck.py")},
        "judge": {"script_sha256": sha(repo / "scripts/eval2_judge.py"), **JUDGE, "cross": CROSS},
        "revise": {"version": grep_const(repo / "kafka_sre_rag/revise.py", "VERSION"), "layout": grep_const(repo / "kafka_sre_rag/revise.py", "LAYOUT"),
                   "prompt_sha256": revise_prompt_hash(repo)},
        "checker": {"version": grep_const(repo / "kafka_sre_rag/checker.py", "VERSION")},
        "harness": {n: sha(repo / "scripts" / n) for n in ("eval2.py", "eval2_compare.py", "eval2_sealed_reading.py", "sealed_io.py",
                                                            "eval2_pilot_report.py", "eval2_first_words.py", "run_guarded.py")},
        "chain": {"script_sha256": sha(repo / "scripts/m4_e2_starting_point.sh"), "helper_sha256": sha(Path(__file__))},
        "hidden_files": hidden,
        "split": {n: sha(repo / "eval/m4" / n) for n in ("SPLIT.json", "SPLIT_A3.json") if (repo / "eval/m4" / n).exists()},
        "server": {"version": srv.get("version"), "models_listed": lm, "q4": q4, "bf16": bf,
                   "judge_listed": JUDGE["model"] in lm, "cross_listed": CROSS["model"] in lm},
    }
    return m


def flat(d, prefix=""):
    out = {}
    for k, v in d.items():
        key = prefix + k
        if isinstance(v, dict):
            out.update(flat(v, key + "."))
        else:
            out[key] = v
    return out


def diff_names(want, have):
    fw, fh = flat(want), flat(have)
    return sorted(k for k in set(fw) | set(fh) if fw.get(k) != fh.get(k))


def cmd_prepare(a):
    out = Path(a.out)
    if out.exists():
        print(f"prepare: {out.name} exists; not overwritten (the gate compares against it)")
        return 0
    m = compute(a)
    problems = []
    if not m["commit"]:
        problems.append("no git commit")
    if not m["server"]["q4"]:
        problems.append("the 4-bit model is not listed (or ambiguous)")
    if not m["server"]["bf16"]:
        problems.append("the full-precision model is not listed (or ambiguous)")
    if not m["server"]["judge_listed"]:
        problems.append("the judge model is not listed")
    if not m["server"]["cross_listed"] and not a.no_cross_judge:
        problems.append("the cross-check judge model is not listed")
    if not all(v for v in (m["index"]["sha256"] == m["index"]["recorded_sha256"],)):
        problems.append("the index hash differs from runs/v3/INDEX_FROZEN.json")
    if m["chunks"]["sha256"] != m["chunks"]["recorded_sha256"]:
        problems.append("the chunks file hash differs from runs/v3/INDEX_FROZEN.json")
    if not m["s6_reference"]["sha256"]:
        problems.append("the S6 reference answers (the stored A1 run of gate G) are missing")
    if m["revise"]["prompt_sha256"].startswith("unavailable"):
        problems.append("the revise prompt hash is unavailable")
    if problems:
        for p in problems:
            print(f"prepare: {p}", file=sys.stderr)
        return 1
    m["options"] = {"r2": bool(a.r2), "reading": a.reading, "f_judged": bool(a.f_judged), "cross_judge": not a.no_cross_judge}
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(out.name + ".tmp")
    tmp.write_text(json.dumps(m, indent=1, sort_keys=True) + "\n")
    os.replace(tmp, out)
    print(f"prepare: manifest written ({len(flat(m))} fields), commit {str(m['commit'])[:10]}, sha256 {sha(out)[:12]}")
    return 0


def check_manifest(a):
    """-> list of field names that differ (the manifest against the current state); live server fields included."""
    want = rjson(a.manifest)
    have = compute(a)
    want.pop("options", None)
    for d in (want, have):   # the list of every model the server lists is recorded for information; what is compared are the models the run uses
        (d.get("server") or {}).pop("models_listed", None)
    return diff_names(want, have)


def cmd_verify_manifest(a):
    bad = check_manifest(a)
    if bad:
        print("manifest differs in: " + ", ".join(bad))
        return 1
    print("manifest: every hash matches")
    return 0


# ---------------------------------------------------------------- ledger

def ledger_rows(path):
    rows = {}
    for line in Path(path).read_text().splitlines():
        if not line.lstrip().startswith("|"):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if cells and cells[0] in ("1", "2") and len(cells) >= 5:
            rows[cells[0]] = cells
    return rows


def ledger_problems(path):
    out = []
    if not Path(path).exists():
        return ["the ledger file is missing"]
    rows = ledger_rows(path)
    for n in ("1", "2"):
        c = rows.get(n)
        if not c:
            out.append(f"ledger row {n} is missing")
            continue
        if not c[1] or "<" in c[1]:
            out.append(f"ledger row {n} has no date")
        if len(c[4]) < 40 or "<" in c[4]:
            out.append(f"ledger row {n} has no decision rule")
    return out


def cmd_ledger(a):
    bad = ledger_problems(a.file)
    for b in bad:
        print(b)
    if not bad:
        print("ledger: rows 1 and 2 present, each with a date and a decision rule")
    return 1 if bad else 0


# ---------------------------------------------------------------- counts, scan

def parse_hidden(specs):
    out = {}
    for s in specs:
        n, _, p = s.partition("=")
        out[n] = p
    return out


def count_problems(h):
    """h: name -> path for tier_a, tier_a3, judged_1, judged_2, rubrics_1, rubrics_2. -> list of condition texts."""
    out = []
    ids = {}
    for name, want in EXPECTED.items():
        p = h.get(name)
        if not p:
            out.append(f"{name}: no file given")
            continue
        if not Path(p).exists():
            out.append(f"{name}: file missing")
            continue
        try:
            rows = rows_of(p)
        except Exception as e:  # noqa: BLE001
            out.append(f"{name}: unreadable ({type(e).__name__})")
            continue
        ids[name] = [r.get("id") for r in rows]
        if len(rows) != want:
            out.append(f"{name}: {len(rows)} rows, expected {want}")
    for n, k in (("rubrics_1", "judged_1"), ("rubrics_2", "judged_2")):
        p = h.get(n)
        if not p or not Path(p).exists():
            out.append(f"{n}: file missing or not given")
            continue
        try:
            rb = [r.get("id") for r in rows_of(p)]
        except Exception as e:  # noqa: BLE001
            out.append(f"{n}: unreadable ({type(e).__name__})")
            continue
        if k in ids and rb != ids[k]:
            out.append(f"{n}: ids differ from {k} (or other order)")
    allids = [i for v in ids.values() for i in v]
    if len(allids) != len(set(allids)):
        out.append("ids repeat across the hidden items files")
    return out


def cmd_counts(a):
    h = parse_hidden(a.hidden)
    bad = count_problems(h)
    for b in bad:
        print(b)
    if not bad:
        print("counts: " + ", ".join(f"{n} {EXPECTED[n]}" for n in EXPECTED) + "; rubric ids equal item ids")
    return 1 if bad else 0


# ---------------------------------------------------------------- shape: key names and counts of the hidden files, and the tools on them

# key -> (who reads it, required). "answer" = scripts/eval2.py answer (validate_items, full_question), "judge" = scripts/eval2_judge.py,
# "bundle" = scripts/eval2_sealed_reading.py bundle, "scorer" = scripts/eval2_score.py, "compare" = scripts/eval2_compare.py.
SHAPE = {
    "judged_items": {
        "keys": {"id": ("answer, judge, bundle", True), "tier": ("answer", True), "kind": ("answer, bundle", True), "family": ("answer", True),
                 "domain": ("answer", True), "question": ("answer, judge, bundle", True), "evidence": ("answer, judge, bundle", False)},
        "ignored_ok": {"failure_classes", "versions", "seed", "generator", "template", "domains"}},
    "judged_rubrics": {
        "keys": {"id": ("judge", True), "must": ("judge, bundle", True), "must_not": ("judge, bundle", False), "any_of": ("judge, bundle", False),
                 "covered_chunks": ("judge", False), "source_text": ("judge", False), "question": ("judge", False), "evidence": ("judge", False),
                 "reference": ("bundle", False)},
        "ignored_ok": {"correct_alternative", "generic", "note", "weights_status", "covered_by_index", "probe_of"}},
    "mech_items": {
        "keys": {"id": ("answer, scorer", True), "tier": ("answer", True), "kind": ("answer, scorer", True), "family": ("answer, compare", True),
                 "domain": ("answer", True), "question": ("answer, scorer", True), "checks": ("scorer", True), "evidence": ("answer", False),
                 "sources": ("scorer", False), "pair": ("compare", False), "exposed": ("split", False),
                 "shares_variant_with_published": ("scorer, compare", False)},
        "ignored_ok": {"anchored", "catalogue_hash", "catalogue_hash_a3", "dedup", "failure_class", "generator", "overlap", "overlap_band",
                       "perturbed", "perturbed_reference", "perturbed_violates", "reference", "seed", "template", "versions", "language",
                       "not_in_index", "chance"}},
}
SHAPE_FILES = (("judged_1", "judged_items"), ("judged_2", "judged_items"), ("rubrics_1", "judged_rubrics"), ("rubrics_2", "judged_rubrics"),
               ("tier_a", "mech_items"), ("tier_a3", "mech_items"))
KIND_FLOOR = 5
PRECEDENT = {"judged_items": ("eval/m4/b1/items.jsonl", "eval/m4/b2/items.jsonl", "eval/m4/pilot2/items.jsonl"),
             "judged_rubrics": ("eval/m4/b1/rubrics.jsonl", "eval/m4/b2/rubrics.jsonl", "eval/m4/pilot2/rubrics.jsonl"),
             "mech_items": ("eval/m4/open/tier_a.jsonl", "eval/m4/open/tier_a3.jsonl")}


def precedent_keys(kind):
    """Keys that the open files of the same kind carry (generator metadata such as fault or vbw): known, and not read by a tool."""
    keys = set()
    for p in PRECEDENT[kind]:
        try:
            for l in (ROOT / p).read_text().splitlines():
                if l.strip():
                    keys |= set(json.loads(l))
        except (OSError, ValueError):
            continue
    return keys


def has_text(v):
    if isinstance(v, str):
        return bool(v.strip())
    if isinstance(v, (list, tuple)):
        return any(has_text(x) for x in v)
    if isinstance(v, dict):
        return any(has_text(x) for x in v.values())
    return False


def filled(v):
    """A value that is there: not None, not an empty or blank string, list or dict."""
    if v is None:
        return False
    if isinstance(v, str):
        return bool(v.strip())
    if isinstance(v, (list, tuple, dict)):
        return len(v) > 0
    return True


def merged_kind_groups(counter, floor=KIND_FLOOR):
    """kind -> count, merged so that no group is under `floor`: [(label, count)]."""
    big = sorted((k, c) for k, c in counter.items() if c >= floor)
    small = [(k, c) for k, c in counter.items() if c < floor]
    rest = sum(c for _, c in small)
    if not small:
        return big
    if rest >= floor or not big:
        return big + [(f"merged {len(small)} kind(s)" if big else f"all {len(small)} kind(s)", rest)]
    low = min(big, key=lambda kc: kc[1])   # fewer than the floor left over: they join the smallest shown group
    big.remove(low)
    return big + [(f"{low[0]} + merged {len(small)} kind(s)", low[1] + rest)]


def shape_file(name, kind, rows):
    """-> (lines, problems) for one hidden file: key names and counts only."""
    spec = SHAPE[kind]
    lines, problems = [], []
    if any(not isinstance(r, dict) for r in rows):
        return [f"shape {name}: {sum(1 for r in rows if not isinstance(r, dict))} row(s) are not objects"], [f"{name}: rows that are not objects"]
    pres, txt = Counter(), Counter()
    for r in rows:
        for k, v in r.items():
            pres[k] += 1
            if has_text(v):
                txt[k] += 1
    lines.append(f"shape {name} ({kind}, {len(rows)} rows): keys present, rows with the key: " + ", ".join(f"{k} {pres[k]}" for k in sorted(pres)))
    for k, (who, req) in spec["keys"].items():
        lack = sum(1 for r in rows if not filled(r.get(k)))
        absent = len(rows) - pres[k]
        lines.append(f"shape {name}: read by {who}: {k} ({'required' if req else 'optional'}): {len(rows) - lack} filled, {absent} lacking, {lack - absent} empty")
        if req and lack:
            problems.append(f"{name}: required key {k}: {absent} lacking, {lack - absent} empty")
    known = spec["ignored_ok"] | precedent_keys(kind)
    unread = {k: c for k, c in txt.items() if k not in spec["keys"] and k not in known}
    allowed = {k: c for k, c in txt.items() if k in known and k not in spec["keys"]}
    if allowed:
        lines.append(f"shape {name}: text keys that nothing reads, known metadata (the same keys are in the open files): " + ", ".join(f"{k} {c}" for k, c in sorted(allowed.items())))
    lines.append(f"shape {name}: text keys that neither the answer step, the judge, the bundles nor the scorer read: " +
                 (", ".join(f"{k} in {c} row(s)" for k, c in sorted(unread.items())) if unread else "none"))
    for k, c in sorted(unread.items()):
        problems.append(f"{name}: key {k} holds text in {c} row(s) and nothing reads it")
    if kind == "judged_items":
        bad_dom = sum(1 for r in rows if not (isinstance(r.get("domain"), str) and r["domain"].strip()
                                              or isinstance(r.get("domain"), list) and r["domain"] and all(isinstance(x, str) and x.strip() for x in r["domain"])))
        bad_tier = sum(1 for r in rows if r.get("tier") not in eval2.TIERS)
        bad_str = {k: sum(1 for r in rows if filled(r.get(k)) and not isinstance(r.get(k), str)) for k in ("id", "tier", "kind", "family", "question")}
        if bad_dom:
            problems.append(f"{name}: domain is neither a non-empty string nor a list of them in {bad_dom} row(s)")
        if bad_tier:
            problems.append(f"{name}: tier is not one of {list(eval2.TIERS)} in {bad_tier} row(s)")
        problems += [f"{name}: {k} is not a string in {c} row(s)" for k, c in bad_str.items() if c]
    if kind == "mech_items":
        bad_str = {k: sum(1 for r in rows if filled(r.get(k)) and not isinstance(r.get(k), str)) for k in ("id", "tier", "kind", "family", "domain", "question")}
        bad_tier = sum(1 for r in rows if r.get("tier") not in eval2.TIERS)
        bad_chk = sum(1 for r in rows if not (isinstance(r.get("checks"), list) and r["checks"] and all(isinstance(c, dict) and c.get("type") for c in r["checks"])))
        lines.append(f"shape {name}: rows whose checks are not a non-empty list of objects with a type: {bad_chk}")
        if bad_chk:
            problems.append(f"{name}: checks are not a non-empty list of typed objects in {bad_chk} row(s)")
        if bad_tier:
            problems.append(f"{name}: tier is not one of {list(eval2.TIERS)} in {bad_tier} row(s)")
        problems += [f"{name}: {k} is not a string in {c} row(s)" for k, c in bad_str.items() if c]
    if kind == "judged_rubrics":
        def pts_bad(r, key, need):
            return not isinstance(r.get(key) or [], list) or any(not isinstance(p, dict) or any(not filled(p.get(n)) for n in need) for p in r.get(key) or [])
        bm = sum(1 for r in rows if pts_bad(r, "must", ("id", "text", "weight")))
        bn = sum(1 for r in rows if pts_bad(r, "must_not", ("id", "text", "severity")))
        lines.append(f"shape {name}: rows with a must point lacking id, text or weight: {bm}; with a must_not point lacking id, text or severity: {bn}")
        if bm:
            problems.append(f"{name}: a must point lacks id, text or weight in {bm} row(s)")
        if bn:
            problems.append(f"{name}: a must_not point lacks id, text or severity in {bn} row(s)")
        nosrc = sum(1 for r in rows if not filled(r.get("covered_chunks")) and not filled(r.get("source_text")))
        lines.append(f"shape {name}: rows with neither covered_chunks nor source_text: {nosrc}")
    if "kind" in spec["keys"]:
        groups = merged_kind_groups(Counter(str(r.get("kind")) for r in rows))
        lines.append(f"shape {name}: kinds (merged so that no group is under {KIND_FLOOR}): " + ", ".join(f"{k} {c}" for k, c in groups))
    return lines, problems


def run_step(label, argv, notes, problems, timeout=1800):
    """A subprocess whose output is dropped (it may carry ids). -> returncode; a failure is named by its label and code only."""
    try:
        r = subprocess.run(argv, cwd=ROOT, capture_output=True, text=True, timeout=timeout)
        rc = r.returncode
    except subprocess.TimeoutExpired:
        rc = -9
    notes.append(f"shape tools: {label}: exit {rc}")
    if rc != 0:
        problems.append(f"tools: {label} exited {rc}")
    return rc


def shape_tools(a, h, rows, notes, problems):
    """The real hidden files through the answer step (stub, closed book), the stub judge, a throwaway reading bundle and the scorer, in a
    private temporary folder of .enc files that is removed at the end."""
    py, S = sys.executable, str(ROOT / "scripts")
    chunks = a.chunks or str(ROOT / "work/v3/chunks_s1.jsonl")
    cats = a.catalogues or str(ROOT / "work/m4/catalogues")
    tmp = Path(tempfile.mkdtemp(prefix="e2_shape_"))
    eb = json.dumps({"chat_template_kwargs": {"enable_thinking": False}})
    jeb = json.dumps(JUDGE["extra_body"])
    try:
        # the judged items as the chain merges and normalises them
        ji, jr = tmp / "ji.jsonl.enc", tmp / "jr.jsonl.enc"
        try:
            items = [normalise_item(r, "e2") for k in ("judged_1", "judged_2") for r in rows[k]]
        except (SystemExit, Exception) as e:  # noqa: BLE001
            problems.append(f"tools: the judged items could not be normalised ({type(e).__name__})")
            return
        eval2.write_jsonl(ji, items)
        eval2.write_jsonl(jr, rows["rubrics_1"] + rows["rubrics_2"])
        n_j = len(items)
        ans = tmp / "ans_judged.jsonl.enc"
        if run_step("answer step, stub, closed book, judged items", [py, f"{S}/eval2.py", "answer", "--items", str(ji), "--out", str(ans), "--config", "e2-shape",
                    "--backend", "stub", "--extra-body", eb, "--mode", "closed", "--catalogue", cats, "--host-class", "shape check"], notes, problems) == 0:
            got = eval2.read_jsonl(ans)
            good = sum(1 for r in got if not r.get("error") and (r.get("text") or "").strip())
            notes.append(f"shape tools: stub answers on the judged items: {good} of {n_j} usable")
            if good != n_j:
                problems.append(f"tools: {n_j - good} of {n_j} stub answers on the judged items are unusable")
            res = tmp / "res.jsonl.enc"
            if run_step("stub judge, 2 passes, coverage role", [py, f"{S}/eval2_judge.py", "run", "--rubrics", str(jr), "--items", str(ji), "--chunks", chunks,
                        "--source-chars", str(JUDGE["source_chars"]), "--instruction", "v2", "--coverage", "--answers", f"P0={ans}", "--judge-model", "stub-model",
                        "--judge-extra-body", jeb, "--concurrency", "1", "--passes", "1", "2", "--cache", str(tmp / "cache.jsonl.enc"), "--out", str(res),
                        "--stub-calls", "--settings-name", "stub-shape"], notes, problems) == 0:
                rr = eval2.read_jsonl(res)
                ok = sum(1 for r in rr if r.get("valid_reply"))
                notes.append(f"shape tools: stub judge rows: {ok} valid of {len(rr)} (expected {2 * n_j})")
                if ok != 2 * n_j or len(rr) != 2 * n_j:
                    problems.append(f"tools: the stub judge gave {ok} valid of {len(rr)} rows, expected {2 * n_j}")
            run_step("throwaway reading bundle (two answer sets, 6 first and 2 second sessions)", [py, f"{S}/eval2_sealed_reading.py", "bundle", "--items", str(ji),
                     "--rubrics", str(jr), "--answers", f"default={ans}", f"strict={ans}", "--chunks", chunks, "--out-dir", str(tmp / "bundles"), "--seed", "20261119",
                     "--first-sessions", "6", "--second-sessions", "2"], notes, problems)
        # the mechanical files through the answer step and the scorer
        mi = [a.hidden_map["tier_a"], a.hidden_map["tier_a3"]]
        n_m = len(rows["tier_a"]) + len(rows["tier_a3"])
        ansm = tmp / "ans_mech.jsonl.enc"
        if run_step("answer step, stub, closed book, mechanical items", [py, f"{S}/eval2.py", "answer", "--items", *mi, "--out", str(ansm), "--config", "e2-shape",
                    "--backend", "stub", "--extra-body", eb, "--mode", "closed", "--catalogue", cats, "--host-class", "shape check"], notes, problems) == 0:
            summ = tmp / "summary.json"
            if run_step("hidden scorer on the stub answers", [py, f"{S}/eval2_score.py", "--items", *mi, "--answers", str(ansm), "--scores", str(tmp / "scores.jsonl.enc"),
                        "--summary", str(summ), "--catalogues", cats], notes, problems) == 0:
                d = json.loads(summ.read_text())
                notes.append(f"shape tools: scorer: {d.get('items_scored')} scored, {d.get('missing_answers')} missing (expected {n_m} and 0)")
                if d.get("items_scored") != n_m or d.get("missing_answers"):
                    problems.append(f"tools: the scorer scored {d.get('items_scored')} of {n_m} with {d.get('missing_answers')} missing")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        notes.append(f"shape tools: the temporary folder is removed ({'yes' if not tmp.exists() else 'NO'})")


def cmd_shape(a):
    h = parse_hidden(a.hidden)
    a.hidden_map = h
    problems, rows, lines = [], {}, []
    for name, kind in SHAPE_FILES:
        p = h.get(name)
        if not p or not Path(p).exists():
            problems.append(f"{name}: file missing or not given")
            continue
        try:
            rows[name] = rows_of(p)
        except Exception as e:  # noqa: BLE001
            problems.append(f"{name}: unreadable ({type(e).__name__})")
            continue
        ls, ps = shape_file(name, kind, rows[name])
        lines += ls
        problems += ps
    for l in lines:
        print(l)
    if len(rows) == len(SHAPE_FILES) and not problems and not a.no_tools:
        notes, tp = [], []
        shape_tools(a, h, rows, notes, tp)
        for n in notes:
            print(n)
        problems += tp
    elif len(rows) == len(SHAPE_FILES) and problems and not a.no_tools:
        print("shape tools: not run, because the files above have problems")
    for p in problems:
        print(f"shape PROBLEM: {p}")
    print(f"shape: {'OK' if not problems else str(len(problems)) + ' problem(s)'}")
    return 1 if problems else 0


def hidden_ids(h):
    s = set()
    for name in EXPECTED:
        if h.get(name) and Path(h[name]).exists():
            s |= {r.get("id") for r in rows_of(h[name]) if r.get("id")}
    return s


def scan_plain(folder, ids, exclude=()):
    """Files under folder (not .enc, not under an excluded folder) that contain an id of a hidden item. -> list of relative names."""
    if not ids:
        return []
    rx = re.compile(r"(?<![\w-])(?:%s)(?![\w-])" % "|".join(re.escape(i) for i in sorted(ids, key=len, reverse=True)))
    hits, root = [], Path(folder)
    excl = [Path(e).resolve() for e in exclude]
    for p in root.rglob("*"):
        if not p.is_file() or p.name.endswith(".enc") or p.name.endswith(".enc.tmp"):
            continue
        rp = p.resolve()
        if any(e in rp.parents or e == rp for e in excl):
            continue
        try:
            data = p.read_bytes().decode("utf-8", "ignore")
        except OSError:
            continue
        if rx.search(data):
            hits.append(str(p.relative_to(root)))
    return hits


def cmd_scan(a):
    h = parse_hidden(a.hidden)
    ids = hidden_ids(h)
    hits = scan_plain(a.dir, ids, a.exclude or [])
    if hits:
        print(f"scan: {len(hits)} plain-text file(s) under {a.dir} hold a hidden id: " + ", ".join(hits[:10]))
        return 1
    print(f"scan: no plain-text file under {a.dir} holds an id of a hidden item ({len(ids)} ids checked)")
    if a.text:
        marks, nrows, dropped = text_markers(h)
        th = scan_text(a.dir, marks, a.also or [])
        if th or not marks:
            print(f"scan: {len(th)} plain file(s) hold hidden text: " + ", ".join(th[:10]) if th else "scan: no hidden text could be read to check against")
            return 1
        print(f"scan: no plain file under {a.dir} (logs and the heartbeat included) holds distinctive text of a hidden row "
              f"({len(marks)} strings from {nrows} rows; {dropped} left out as the open files hold them too)")
    return 0


# ---------------------------------------------------------------- plain-file scan for hidden text (distinctive lines of the decrypted rows)

MARKER_MIN_CHARS, MARKER_MIN_WORDS, MARKER_WIDTH = 20, 3, 60


def norm_line(s):
    return " ".join(s.lower().split())


def string_leaves(v):
    if isinstance(v, str):
        yield v
    elif isinstance(v, (list, tuple)):
        for x in v:
            yield from string_leaves(x)
    elif isinstance(v, dict):
        for x in v.values():
            yield from string_leaves(x)


def prose_lines(s):
    """The normalised lines of a string that are long enough to be distinctive (at least 20 characters and 3 words)."""
    out = []
    for ln in s.splitlines():
        n = norm_line(ln)
        if len(n) >= MARKER_MIN_CHARS and len(n.split()) >= MARKER_MIN_WORDS:
            out.append(n)
    return out


def row_markers(row):
    """Distinctive strings of one hidden row: the longest line of every text field (any depth) and the row's longest three lines,
    each cut to 60 characters. Kept in memory only."""
    allines, marks = [], set()
    for leaf in string_leaves(row):
        ls = prose_lines(leaf)
        if ls:
            marks.add(max(ls, key=len)[:MARKER_WIDTH])
            allines += ls
    for n in sorted(set(allines), key=len, reverse=True)[:3]:
        marks.add(n[:MARKER_WIDTH])
    return marks


def open_corpus():
    """The normalised text lines of the open evaluation files (plain jsonl under eval/m4): a line that also occurs there is not
    distinctive for the hidden half (the hidden items are written from the same templates)."""
    lines = []
    for p in sorted((ROOT / "eval/m4").rglob("*.jsonl")):
        try:
            for l in p.read_text().splitlines():
                if l.strip():
                    for leaf in string_leaves(json.loads(l)):
                        lines += prose_lines(leaf)
        except (OSError, ValueError):
            continue
    return "\n".join(lines)


def hidden_rows(h):
    """Every row of the hidden files (items and rubrics) that read, in memory."""
    rows = []
    for name in ("tier_a", "tier_a3", "judged_1", "judged_2", "rubrics_1", "rubrics_2"):
        if h.get(name) and Path(h[name]).exists():
            rows += rows_of(h[name])
    return rows


def text_markers(h, corpus=None):
    """-> (markers, rows read, markers left out because the open files hold the same line)."""
    rows = hidden_rows(h)
    marks = set()
    for r in rows:
        marks |= row_markers(r)
    corpus = open_corpus() if corpus is None else corpus
    keep = sorted(m for m in marks if m not in corpus)
    return keep, len(rows), len(marks) - len(keep)


def scan_text(folder, markers, also=()):
    """Plain files under `folder` (every one, logs included, not .enc) and the extra files or folders in `also` that hold one of the
    markers. -> sorted list of names (never the text)."""
    if not markers:
        return []
    roots = [Path(folder)] + [Path(x) for x in also]
    seen, hits = set(), []
    for root in roots:
        files = [root] if root.is_file() else (list(root.rglob("*")) if root.is_dir() else [])
        for p in files:
            if not p.is_file() or p.name.endswith(".enc") or p.name.endswith(".enc.tmp"):
                continue
            rp = p.resolve()
            if rp in seen:
                continue
            seen.add(rp)
            try:
                data = p.read_bytes().decode("utf-8", "ignore")
            except OSError:
                continue
            a = norm_line(data)
            b = norm_line(data.replace('\\"', '"').replace("\\n", " "))
            if any(m in a or m in b for m in markers):
                hits.append(p.name if root.is_file() and root not in (Path(folder),) else str(p.relative_to(root)))
    return sorted(set(hits))


# ---------------------------------------------------------------- the hidden gate

def cmd_gate(a):
    repo = Path(a.repo)
    results = []

    def cond(name, ok, note=""):
        results.append((name, bool(ok), note))

    # 1. tests and verify
    if a.tests == "skip":
        cond("tests", True, "skipped (dry run)")
    else:
        rc, head = git("rev-parse", "HEAD", cwd=repo)
        okf = Path(a.tests_ok_file) if a.tests_ok_file else None
        if okf and okf.exists() and okf.read_text().strip() == head:
            cond("tests", True, f"passed earlier on this commit ({head[:10]})")
        else:
            r = subprocess.run(a.test_cmd, shell=True, cwd=repo, capture_output=True, text=True)
            tail = (r.stdout + r.stderr).strip().splitlines()[-1:] or [""]
            cond("tests", r.returncode == 0, tail[0][:120])
            if r.returncode == 0 and okf:
                okf.write_text(head + "\n")
    v = subprocess.run([sys.executable, "-m", "kafka_sre_rag", "verify"], cwd=repo, capture_output=True, text=True)
    cond("verify", v.returncode == 0, "kafka_sre_rag verify")
    # 2. clean tree and commit recorded
    man = None
    try:
        man = rjson(a.manifest)
    except Exception:  # noqa: BLE001
        pass
    cond("manifest file present", man is not None)
    rc, head = git("rev-parse", "HEAD", cwd=repo)
    if a.allow_dirty:
        cond("clean tree", True, "not enforced (dry run)")
    else:
        dirty = tree_dirty(repo)
        cond("clean tree", dirty == [], f"{len(dirty or [])} changed file(s)" if dirty else "")
    cond("commit recorded", bool(man) and rc == 0 and man.get("commit") == head, "manifest commit equals HEAD" if man else "")
    # 3. every hash of the manifest
    if man is not None:
        bad = check_manifest(a)
        cond("manifest hashes", not bad, "differs in: " + ", ".join(bad) if bad else "every field matches")
    # 4. ledger
    lp = ledger_problems(a.ledger)
    cond("ledger rows 1 and 2", not lp, "; ".join(lp))
    # 5. counts
    h = parse_hidden(a.hidden)
    cp = count_problems(h)
    cond("hidden row counts 245, 60, 24, 36", not cp, "; ".join(cp))
    # 6. G and O
    for stage in ("G", "O"):
        mk = Path(a.markers) / f"{stage}.passed"
        ok = False
        if mk.exists():
            try:
                d = rjson(mk)
                ok = d.get("passed") is True and (man is None or d.get("commit") == man.get("commit"))
            except Exception:  # noqa: BLE001
                ok = False
        cond(f"stage {stage} passed", ok, "marker present and for this commit" if ok else "no passed marker for this commit")
    # 7. no plain-text file with a hidden id
    try:
        ids = hidden_ids(h)   # the ids of the files that read; a file that does not read is reported by the count condition
    except Exception:  # noqa: BLE001
        ids = set()
    hits = scan_plain(a.work_dir, ids, a.exclude or [])
    cond("no plain-text file with a hidden id", not hits and bool(ids), f"{len(hits)} file(s): " + ", ".join(hits[:5]) if hits else
         ("" if ids else "no hidden id could be read to check against"))
    try:
        marks, nrows, _ = text_markers(h)
    except Exception:  # noqa: BLE001
        marks, nrows = [], 0
    th = scan_text(a.work_dir, marks, a.also or [])
    cond("no plain file with hidden text", not th and bool(marks),
         f"{len(th)} file(s): " + ", ".join(th[:5]) if th else ("" if marks else "no hidden text could be read to check against"))
    for name, ok, note in results:
        print(f"gate {'PASS' if ok else 'FAIL'}: {name}" + (f" ({note})" if note else ""))
    bad = [n for n, ok, _ in results if not ok]
    print(f"hidden gate: {'OPEN' if not bad else 'REFUSED (' + str(len(bad)) + ' condition(s) failed)'}")
    return 0 if not bad else 1


# ---------------------------------------------------------------- stage helpers

def cmd_gate_g(a):
    got = {r["id"]: r for r in rows_of(a.answers)}
    ref = {r["id"]: r for r in rows_of(a.stored)}
    ids = sorted(got)
    miss = [i for i in ids if i not in ref]
    same_text = sum(1 for i in ids if i in ref and got[i].get("text") == ref[i].get("text"))
    same_pack = sum(1 for i in ids if i in ref and got[i].get("pack") == ref[i].get("pack"))
    errs = sum(1 for i in ids if got[i].get("error"))
    ok = len(ids) == a.expect and not miss and same_text == len(ids) and same_pack == len(ids) and not errs
    print(f"gate G: {len(ids)} answers (expected {a.expect}), {same_text} of {len(ids)} texts byte-identical to the stored run, "
          f"{same_pack} of {len(ids)} packs identical, {len(miss)} without a stored answer, {errs} errors: {'PASS' if ok else 'FAIL'}")
    return 0 if ok else 1


def cmd_check_answers(a):
    rows = rows_of(a.file)
    err = [r for r in rows if r.get("error")]
    empty = [] if a.no_text else [r for r in rows if not r.get("error") and not (r.get("text") or "").strip()]
    nopack = [r for r in rows if not r.get("error") and a.packs and not r.get("pack")]
    m = eval2.read_manifest(a.file)
    ended = bool(m and m.get("ended"))
    ok = len(rows) == a.expect and not err and not empty and not nopack and ended
    print(f"check {Path(a.file).name}: {len(rows)} of {a.expect} rows, {len(err)} errors, {len(empty)} empty, "
          f"{len(nopack)} without a pack, manifest ended: {'yes' if ended else 'no'}")
    return 0 if ok else 1


def cmd_runline(a):
    rows = rows_of(a.file)
    t = [r["seconds"]["total"] for r in rows if not r.get("error") and r.get("seconds")]
    tk = [r["tokens"] for r in rows if r.get("tokens") is not None]
    cut = sum(1 for r in rows if r.get("truncated"))
    print(f"run {a.name}: {len(rows)} answers, {sum(1 for r in rows if r.get('error'))} errors, "
          f"median {statistics.median(t) if t else 0:.1f} s per answer, max {max(t) if t else 0:.1f} s, "
          f"median {statistics.median(tk) if tk else 0:.0f} tokens, {cut} cut at the limit")
    return 0


def cmd_judge_done(a):
    p = Path(a.file)
    if not p.exists():
        return 1
    rows = rows_of(p)
    return 0 if len(rows) == a.expect and all(r.get("valid_reply") for r in rows) else 1


def normalise_item(r, domain):
    row = {"tier": "B", "kind": "open", "family": r.get("id"), "domain": domain, **r}
    if isinstance(row["domain"], list):
        row["domains"], row["domain"] = row["domain"], ",".join(map(str, row["domain"])) or domain
    for k in ("tier", "kind", "family", "domain"):
        if not isinstance(row[k], str) or not row[k]:
            raise SystemExit(f"item {r.get('id')}: {k} is not a non-empty string")
    return row


def cmd_merge(a):
    rows = []
    for p in a.inputs:
        rows += rows_of(p)
    ids = [r.get("id") for r in rows]
    if len(ids) != len(set(ids)):
        print("merge: an id repeats across the inputs", file=sys.stderr)
        return 1
    if a.normalize_items:
        rows = [normalise_item(r, a.normalize_items) for r in rows]
    if eval2.is_hidden(a.out) != any(eval2.is_hidden(p) for p in a.inputs) and not a.allow_plain_out:
        print("merge: the output must be .enc exactly when an input is", file=sys.stderr)
        return 1
    wrows(a.out, rows)
    print(f"merge: {len(rows)} rows from {len(a.inputs)} file(s) into {Path(a.out).name}")
    return 0


def order_key(seed, iid):
    return hashlib.sha256(f"{seed}:{iid}".encode()).hexdigest()


def cmd_subset(a):
    rows = rows_of(a.input)
    if a.set:
        rows = [r for r in rows if r.get("set") == a.set]
    chosen = {r["id"] for r in sorted(rows, key=lambda r: order_key(a.seed, r["id"]))[: a.n]}
    keep = [r for r in rows if r["id"] in chosen]
    wrows(a.out, keep)
    print(f"subset: {len(keep)} of {len(rows)} rows (seed {a.seed}) into {Path(a.out).name}")
    return 0


def cmd_items_for(a):
    ids = {r["id"] for r in rows_of(a.answers)}
    rows = []
    for p in a.items:
        rows += [r for r in rows_of(p) if r["id"] in ids]
    wrows(a.out, rows)
    print(f"items-for: {len(rows)} items of {len(ids)} answers into {Path(a.out).name}")
    return 0 if len(rows) == len(ids) else 1


def cmd_empty_enc(a):
    wrows(a.out, [])
    print(f"empty-enc: {Path(a.out).name}")
    return 0


def cmd_agreement(a):
    import eval2_judge as ej
    rub = {r["id"]: r for r in rows_of(a.rubrics)}
    pick_rows = lambda p: {r["id"]: r for r in rows_of(p) if r.get("valid_reply") and r.get("set") == a.set and r.get("pass") == a.passno}
    A, B = pick_rows(a.a), pick_rows(a.b)
    common = sorted(set(A) & set(B))
    units = same = 0
    dP = []
    for i in common:
        ca, cb = ej.coverage_row(A[i], rub[i]), ej.coverage_row(B[i], rub[i])
        ua = {tuple(u["ids"]): u["status"] for u in ca["units"]}
        ub = {tuple(u["ids"]): u["status"] for u in cb["units"]}
        for k in set(ua) & set(ub):
            units += 1
            same += ua[k] == ub[k]
        dP.append(abs(ca["P"] - cb["P"]))
    if units:
        print(f"cross-check agreement (set {a.set}, pass {a.passno}): {len(common)} answers judged by both (of {len(A)} and {len(B)}), "
              f"{units} essential units compared, {same} with the same status = {100 * same / units:.1f} %, "
              f"mean absolute difference in P {sum(dP) / len(dP):.3f}")
    else:
        print(f"cross-check agreement: {len(common)} answers judged by both; no unit to compare")
    return 0


def cmd_packs_equal(a):
    ra = {r["id"]: r.get("pack") for r in rows_of(a.a)}
    rb = {r["id"]: r.get("pack") for r in rows_of(a.b)}
    bad = [i for i in rb if ra.get(i) != rb[i]]
    print(f"replay check: {len(rb) - len(bad)} of {len(rb)} packs identical to the source run")
    return 1 if bad else 0


def cmd_patched(a):
    """Answers whose R2 patch was accepted, with the patched text, for an extra reading (only with --r2)."""
    ans = {r["id"]: r for r in rows_of(a.answers)}
    out = []
    for r in rows_of(a.revise):
        if (r.get("r2") or {}).get("status") == "accepted" and r.get("id") in ans:
            row = dict(ans[r["id"]])
            row["text"] = r["final_text"]
            out.append(row)
    print(f"patched: {len(out)} answers with an accepted R2 patch")
    if not out:
        return 3
    wrows(a.out, out)
    return 0


def cmd_fake_status(a):
    p = Path(a.file)
    d = json.loads(p.read_text()) if p.exists() else {"loaded_models": [], "active_requests": 0, "waiting_requests": 0, "version": "dry-run"}
    lm = [m for m in d.get("loaded_models") or []]
    if a.action == "load" and a.model not in lm:
        lm.append(a.model)
    if a.action == "unload":
        lm = [m for m in lm if m != a.model]
    if a.action == "init" and a.model:
        lm = json.loads(a.model)
    d["loaded_models"] = lm
    p.write_text(json.dumps(d))
    return 0


def cmd_verdict(a):
    t = Path(a.compare_out).read_text()
    lines = {k: next((l.strip() for l in t.splitlines() if l.strip().startswith(f"{k}-sided reading")), None) for k in ("one", "two")}
    if not a.reading:
        print("MARGIN READING NOT CHOSEN: the one-sided and the two-sided reading of rule 1(a) are both shown above; no verdict is printed "
              "(the Owner chooses, then rerun the report with --margin-reading)")
        for k in ("one", "two"):
            print(f"  {k}-sided: {lines[k] or 'not found'}  [not a verdict]")
        return 0
    k = "one" if a.reading == "one-sided" else "two"
    m = re.search(r":\s*(PASS|FAIL)\b", lines[k] or "")
    if not m:
        print(f"VERDICT of rule 1(a) in the {a.reading} reading: not found in the comparison output", file=sys.stderr)
        return 1
    print(f"VERDICT of rule 1(a), {a.reading} reading: {m.group(1)}   ({lines[k]})")
    return 0


# ---------------------------------------------------------------- synthetic hidden files (dry runs and tests)

def cmd_synth(a):
    """Synthetic hidden files of the right row counts, built from open items (so the scorer and the judge run on them), with new ids
    and a marker string in the question of every row. Encrypted with the key of the environment (a temporary key in dry runs)."""
    out = Path(a.dir)
    out.mkdir(parents=True, exist_ok=True)
    rd = lambda p: [json.loads(l) for l in Path(ROOT / p).read_text().splitlines() if l.strip()]

    def plant(s, label):
        """A marker at the start of every line of a text field, so that a leak of any line of it can be found."""
        return "\n".join(f"{label} {ln}" for ln in str(s).split("\n"))

    def plant_rubric(rb, label):
        for k in ("question", "reference", "generic", "correct_alternative", "note", "source_text", "evidence"):
            if isinstance(rb.get(k), str) and rb[k].strip():
                rb[k] = plant(rb[k], f"{label}-{k}")
        for pk, fields in (("must", ("text", "source")), ("must_not", ("text", "condition", "near_miss"))):
            rb[pk] = [dict(p) for p in rb.get(pk) or []]
            for i, p in enumerate(rb[pk]):
                for k in fields:
                    if isinstance(p.get(k), str) and p[k].strip():
                        p[k] = plant(p[k], f"{label}-{pk}{i}-{k}")
        return rb

    def mark(r, tag, n):
        r = dict(r)
        old = r["id"]
        r["id"] = f"{tag}-{n:03d}"
        if "family" in r and isinstance(r["family"], str):
            r["family"] = f"{tag}f-{r['family']}"
        label = f"{a.marker}-{tag}-{n:03d}"
        r["question"] = f"{label} " + str(r.get("question", ""))
        for k in ("evidence", "reference", "perturbed_reference"):   # every other text field of the row carries a marker too
            if isinstance(r.get(k), str) and r[k].strip():
                r[k] = plant(r[k], f"{label}-{k}")
        return r, old

    a1, a3 = rd("eval/m4/open/tier_a.jsonl")[:245 - a.drop_a], rd("eval/m4/open/tier_a3.jsonl")[:60]
    b1, b2 = rd("eval/m4/b1/items.jsonl"), rd("eval/m4/b2/items.jsonl")
    r1, r2 = rd("eval/m4/b1/rubrics.jsonl"), rd("eval/m4/b2/rubrics.jsonl")
    pools = {"tier_a": a1, "tier_a3": a3}
    for name, rows in pools.items():
        new = [mark(r, name, i + 1)[0] for i, r in enumerate(rows)]
        wrows(out / f"{name}.jsonl.enc", new)
    j_items = b1 + b2   # the synthetic judged rows: 24 (first file) + 36 (the rest of B1 and all of B2)
    j_rub = r1 + r2
    by_rub = {r["id"]: r for r in j_rub}
    for name, lo, hi in (("judged_1", 0, 24), ("judged_2", 24, 60)):
        its, rbs = [], []
        for i in range(lo, hi):
            it, old = mark(j_items[i], "hj", i + 1)
            if i % 5 == 0:   # some rows carry an evidence field, so that full_question and the judge's evidence path are exercised
                it["evidence"] = plant("evidence line one of a synthetic scenario\nevidence line two of a synthetic scenario", f"{a.marker}-hj-{i + 1:03d}-evidence")
            rb = dict(by_rub[old])
            rb["id"] = it["id"]
            plant_rubric(rb, f"{a.marker}-hj-{i + 1:03d}-rubric")
            if "question" in rb:
                rb["question"] = it["question"]
            its.append(it)
            rbs.append(rb)
        wrows(out / f"{name}_items.jsonl.enc", its)
        wrows(out / f"{name}_rubrics.jsonl.enc", rbs)
    print(f"synth: 4 items files ({245}, {60}, {24}, {36} rows) and 2 rubrics files in {out.name}")
    return 0


def build_parser():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    def common(p, hidden=True):
        p.add_argument("--repo", default=str(ROOT))
        p.add_argument("--server-json", help="the server's status JSON (version fields)")
        p.add_argument("--models-json", help="the server's model listing")
        p.add_argument("--chunks", help="the chunks file the manifest hashes (default work/v3/chunks_s1.jsonl; for tests)")
        p.add_argument("--s6-ref", help="the S6 A1 reference answers gate G compares with (default work/m4/s6/ans_A1.jsonl; for tests)")
        if hidden:
            p.add_argument("--hidden", nargs="*", default=[], metavar="NAME=PATH.enc")

    for n in ("prepare", "verify-manifest", "gate"):
        p = sub.add_parser(n)
        common(p)
        p.add_argument("--manifest" if n != "prepare" else "--out", required=True, dest="manifest" if n != "prepare" else "out")
        if n == "prepare":
            p.add_argument("--r2", action="store_true")
            p.add_argument("--reading", default="both")
            p.add_argument("--f-judged", action="store_true")
            p.add_argument("--no-cross-judge", action="store_true")
    g = sub.choices["gate"]
    g.add_argument("--ledger", required=True)
    g.add_argument("--work-dir", required=True)
    g.add_argument("--markers", required=True)
    g.add_argument("--exclude", nargs="*", default=[])
    g.add_argument("--also", nargs="*", default=[], help="more files or folders to scan for hidden text (this run's logs, the heartbeat)")
    g.add_argument("--allow-dirty", action="store_true")
    g.add_argument("--tests", choices=["run", "skip"], default="run")
    g.add_argument("--test-cmd", default=os.environ.get("M4_E2_TEST_CMD", ".venv/bin/python -m unittest discover -s tests"))
    g.add_argument("--tests-ok-file")
    s = sub.add_parser("ledger")
    s.add_argument("--file", required=True)
    s = sub.add_parser("counts")
    s.add_argument("--hidden", nargs="+", required=True)
    s = sub.add_parser("shape")
    s.add_argument("--hidden", nargs="+", required=True, metavar="NAME=PATH.enc")
    s.add_argument("--chunks", help="the chunks file (default work/v3/chunks_s1.jsonl)")
    s.add_argument("--catalogues", help="the catalogue folder (default work/m4/catalogues)")
    s.add_argument("--no-tools", action="store_true", help="key names and counts only; do not run the stub tools")
    s = sub.add_parser("scan")
    s.add_argument("--dir", required=True)
    s.add_argument("--hidden", nargs="+", required=True)
    s.add_argument("--exclude", nargs="*", default=[])
    s.add_argument("--text", action="store_true", help="also look for distinctive text of the hidden rows (logs and --also files included)")
    s.add_argument("--also", nargs="*", default=[], help="more files or folders to scan for text (this run's logs, the heartbeat)")
    s = sub.add_parser("gate-g")
    s.add_argument("--answers", required=True)
    s.add_argument("--stored", required=True)
    s.add_argument("--expect", type=int, required=True)
    s = sub.add_parser("check-answers")
    s.add_argument("--file", required=True)
    s.add_argument("--expect", type=int, required=True)
    s.add_argument("--packs", action="store_true")
    s.add_argument("--no-text", action="store_true", help="revise records (no answer text)")
    s = sub.add_parser("runline")
    s.add_argument("--name", required=True)
    s.add_argument("--file", required=True)
    s = sub.add_parser("judge-done")
    s.add_argument("--file", required=True)
    s.add_argument("--expect", type=int, required=True)
    s = sub.add_parser("merge")
    s.add_argument("--out", required=True)
    s.add_argument("--in", dest="inputs", nargs="+", required=True)
    s.add_argument("--normalize-items", metavar="DOMAIN")
    s.add_argument("--allow-plain-out", action="store_true")
    s = sub.add_parser("subset")
    s.add_argument("--in", dest="input", required=True)
    s.add_argument("--out", required=True)
    s.add_argument("--n", type=int, required=True)
    s.add_argument("--seed", default="1")
    s.add_argument("--set")
    s = sub.add_parser("items-for")
    s.add_argument("--items", nargs="+", required=True)
    s.add_argument("--answers", required=True)
    s.add_argument("--out", required=True)
    s = sub.add_parser("empty-enc")
    s.add_argument("--out", required=True)
    s = sub.add_parser("agreement")
    s.add_argument("--a", required=True)
    s.add_argument("--b", required=True)
    s.add_argument("--rubrics", required=True)
    s.add_argument("--set", required=True)
    s.add_argument("--passno", type=int, default=1)
    s = sub.add_parser("packs-equal")
    s.add_argument("--a", required=True, help="the source run")
    s.add_argument("--b", required=True, help="the replayed run")
    s = sub.add_parser("patched")
    s.add_argument("--answers", required=True)
    s.add_argument("--revise", required=True)
    s.add_argument("--out", required=True)
    s = sub.add_parser("fake-status")
    s.add_argument("--file", required=True)
    s.add_argument("action", choices=["init", "load", "unload"])
    s.add_argument("model", nargs="?")
    s = sub.add_parser("verdict")
    s.add_argument("--compare-out", required=True)
    s.add_argument("--reading", choices=["one-sided", "two-sided"])
    s = sub.add_parser("synth")
    s.add_argument("--dir", required=True)
    s.add_argument("--marker", default="SYNTHMARK")
    s.add_argument("--drop-a", type=int, default=0, help="rows left out of the synthetic tier A file (to test the row-count condition)")
    return ap


CMDS = {"prepare": cmd_prepare, "verify-manifest": cmd_verify_manifest, "gate": cmd_gate, "ledger": cmd_ledger, "counts": cmd_counts, "shape": cmd_shape,
        "scan": cmd_scan, "gate-g": cmd_gate_g, "check-answers": cmd_check_answers, "runline": cmd_runline, "judge-done": cmd_judge_done,
        "merge": cmd_merge, "subset": cmd_subset, "items-for": cmd_items_for, "empty-enc": cmd_empty_enc, "agreement": cmd_agreement,
        "fake-status": cmd_fake_status, "packs-equal": cmd_packs_equal, "patched": cmd_patched, "verdict": cmd_verdict, "synth": cmd_synth}


def main(argv=None):
    a = build_parser().parse_args(argv)
    try:
        return CMDS[a.cmd](a)
    except eval2.EvalError as e:
        print(f"{a.cmd}: {e}", file=sys.stderr)
        return 2
    except Exception as e:  # noqa: BLE001  the message of an exception could carry a hidden value: the class only
        print(f"{a.cmd}: failed ({type(e).__name__})", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
