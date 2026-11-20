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
  scan             no plain-text file under a folder contains an id of a hidden item
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
import statistics
import subprocess
import sys
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
    ids = hidden_ids(parse_hidden(a.hidden))
    hits = scan_plain(a.dir, ids, a.exclude or [])
    if hits:
        print(f"scan: {len(hits)} plain-text file(s) under {a.dir} hold a hidden id: " + ", ".join(hits[:10]))
        return 1
    print(f"scan: no plain-text file under {a.dir} holds an id of a hidden item ({len(ids)} ids checked)")
    return 0


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

    def mark(r, tag, n):
        r = dict(r)
        old = r["id"]
        r["id"] = f"{tag}-{n:03d}"
        if "family" in r and isinstance(r["family"], str):
            r["family"] = f"{tag}f-{r['family']}"
        r["question"] = f"{a.marker}-{tag}-{n:03d} " + str(r.get("question", ""))
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
            rb = dict(by_rub[old])
            rb["id"] = it["id"]
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
    g.add_argument("--allow-dirty", action="store_true")
    g.add_argument("--tests", choices=["run", "skip"], default="run")
    g.add_argument("--test-cmd", default=os.environ.get("M4_E2_TEST_CMD", ".venv/bin/python -m unittest discover -s tests"))
    g.add_argument("--tests-ok-file")
    s = sub.add_parser("ledger")
    s.add_argument("--file", required=True)
    s = sub.add_parser("counts")
    s.add_argument("--hidden", nargs="+", required=True)
    s = sub.add_parser("scan")
    s.add_argument("--dir", required=True)
    s.add_argument("--hidden", nargs="+", required=True)
    s.add_argument("--exclude", nargs="*", default=[])
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


CMDS = {"prepare": cmd_prepare, "verify-manifest": cmd_verify_manifest, "gate": cmd_gate, "ledger": cmd_ledger, "counts": cmd_counts,
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
