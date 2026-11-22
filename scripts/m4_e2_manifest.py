#!/usr/bin/env python3
"""Helper of scripts/m4_e2_starting_point.sh (E2, the starting-point measurement on the hidden half).

Every subcommand that touches hidden rows reads them through scripts/eval2.py (key from EVAL_HIDDEN_KEY or
~/.config/kafka-sre-llm/eval_hidden.key), keeps plain text in memory only and prints names, counts, hashes and totals: never an item
text, an answer, a quote, and never an id of a hidden item on stdout or stderr (the id scan names files, not ids). Errors name the
condition, not values. Outputs that hold hidden rows end in .enc.

  prepare          write the manifest (refuses to overwrite)            verify-manifest   compare the manifest with the current state
  gate             the hidden gate (every condition printed, exit 1 if any fails)
  pin-check        the pinned files only (tracked files under scripts/, kafka_sre_rag/, tests/golden/, eval/m4/, requirements.txt, the hidden .enc
                   files): the hash against the manifest, and no uncommitted change in a pinned path; the head commit is information only
  ledger           ledger rows present (--rows, default 1 2), each with a date and a decision rule (the file is read, never written)
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
  rehearsal-prepare  encrypted copies of open items (10 tier A, 10 a3, 6 B2) with the rehearsal's temporary key, plain open input files, a ledger
  rehearsal-findings fake findings for the bundles of a rehearsal (sealed with the temporary key)
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
EXPECTED = {"tier_a": 245, "tier_a3": 60, "judged_1": 25, "judged_2": 36}
# What the run depends on: tracked files under these paths (the hash is over their working-tree content, taken at prepare). A commit that
# touches only other files (MILESTONE.md, agents/, runs/, README.md, tests/ outside tests/golden) does not change it; the head commit is
# recorded for information only.
PIN_PATHS = ("scripts", "kafka_sre_rag", "tests/golden", "eval/m4", "requirements.txt")
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
    """Uncommitted changes (tracked or untracked) in a pinned path (PIN_PATHS); a change elsewhere does not count. -> list of status lines
    (paths only), None when git fails."""
    rc, out = git("status", "--porcelain", "--untracked-files=all", "--", *PIN_PATHS, cwd=repo)
    if rc != 0:
        return None
    return [l for l in out.splitlines() if l.strip() and not l.startswith("?? eval/hidden/")]


def status_path(line):
    """The path of a `git status --porcelain` line (the output is stripped, so the leading blank of the first line may be gone)."""
    return line.split(None, 1)[-1]


def pinned_files(repo):
    """Tracked files under PIN_PATHS, sorted. -> list of paths relative to the repository."""
    r = subprocess.run(["git", "ls-files", "-z", "--", *PIN_PATHS], cwd=repo, capture_output=True)
    if r.returncode != 0:
        raise eval2.EvalError("git ls-files failed in the repository")
    return sorted(p for p in r.stdout.decode().split("\0") if p)


def compute_pins(repo):
    """The pinned state: sha256 of every tracked pinned file as it is in the working tree (a deleted file counts as 'missing'), and one hash
    over the lot (path, content hash). Names and hashes only."""
    repo = Path(repo)
    files = {}
    for p in pinned_files(repo):
        f = repo / p
        files[p] = sha(f) if f.is_file() else "missing"
    h = hashlib.sha256()
    for p, d in files.items():
        h.update(f"{p}\0{d}\0".encode())
    return {"paths": list(PIN_PATHS), "count": len(files), "sha256": h.hexdigest(), "files": files}


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
    have_models = bool(a.models_json) and Path(a.models_json).is_file()   # the start check of a stage that needs no server may have no listing
    q4, bf = (pick(a.models_json, "q4"), pick(a.models_json, "bf16")) if have_models else (None, None)
    lm = listed_models(a.models_json) if have_models else []
    hidden = {}
    for spec in a.hidden or []:
        name, _, path = spec.partition("=")
        hidden[name] = {"file": Path(path).name, "sha256": sha(path)}
    m = {
        "manifest_version": MANIFEST_VERSION,
        "commit": head if rc == 0 else None,   # information only: the run is pinned by "pinned" (files), not by the commit
        "pinned": compute_pins(repo),
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


def current_options(a):
    """The options of a launch that decide what the run is: R2, the reading, the full-precision judged run, the cross-check, the mode (real or
    rehearsal) and the tag file (name and hash; the file names ids, so only its hash is stored)."""
    o = {"r2": bool(a.r2), "reading": a.reading, "f_judged": bool(a.f_judged), "cross_judge": not a.no_cross_judge}
    if getattr(a, "mode", None):
        o["mode"] = a.mode
    tf = getattr(a, "tag_file", None)
    if tf:
        o["tag_file"] = {"name": Path(tf).name, "sha256": sha(tf) if Path(tf).is_file() else "missing"}
    return o


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
    m["options"] = current_options(a)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(out.name + ".tmp")
    tmp.write_text(json.dumps(m, indent=1, sort_keys=True) + "\n")
    os.replace(tmp, out)
    print(f"prepare: manifest written ({len(flat(m))} fields), {m['pinned']['count']} pinned files (sha256 {m['pinned']['sha256'][:12]}), "
          f"commit {str(m['commit'])[:10]} (information only), sha256 {sha(out)[:12]}")
    return 0


def check_manifest(a):
    """-> list of field names that differ (the manifest against the current state); live server fields included."""
    want = rjson(a.manifest)
    have = compute(a)
    want.pop("options", None)
    for d in (want, have):   # the list of every model the server lists is recorded for information; what is compared are the models the run uses
        (d.get("server") or {}).pop("models_listed", None)
        d.pop("commit", None)   # the head commit is for information only: the pinned files decide (a commit elsewhere does not stop the run)
    return diff_names(want, have)


def pin_problems(repo, man, allow_dirty=False):
    """-> list of conditions that fail: a pinned file that differs from the manifest (named by path), or an uncommitted change in a pinned path."""
    out = []
    want = (man or {}).get("pinned")
    if not want:
        return ["the manifest records no pinned files (made by an older chain)"]
    have = compute_pins(repo)
    if have["sha256"] != want["sha256"]:
        wf, hf = want.get("files") or {}, have["files"]
        names = sorted(k for k in set(wf) | set(hf) if wf.get(k) != hf.get(k))
        out.append("a pinned file changed since prepare: " + ", ".join(names[:12]) + (f" (and {len(names) - 12} more)" if len(names) > 12 else ""))
    if not allow_dirty:
        dirty = tree_dirty(repo)
        if dirty is None:
            out.append("git status failed")
        elif dirty:
            out.append(f"{len(dirty)} uncommitted change(s) in a pinned path: " + ", ".join(status_path(l) for l in dirty[:8]))
    return out


def cmd_pin_check(a):
    repo = Path(a.repo)
    man = rjson(a.manifest)
    bad = pin_problems(repo, man, a.allow_dirty)
    for spec in a.hidden or []:   # the hidden files the run reads: the hash of the .enc file against the manifest
        name, _, path = spec.partition("=")
        want = (man.get("hidden_files") or {}).get(name) or {}
        if not Path(path).is_file() or sha(path) != want.get("sha256"):
            bad.append(f"hidden file {name} differs from the manifest (or is missing)")
    for b in bad:
        print(f"pin REFUSED: {b}")
    if not bad:
        print(f"pin: {man['pinned']['count']} pinned files and {len(a.hidden or [])} hidden files match the manifest; "
              f"head {str(git('rev-parse', 'HEAD', cwd=repo)[1])[:10]} (manifest {str(man.get('commit'))[:10]}), information only")
    return 1 if bad else 0


def cmd_verify_manifest(a):
    bad = check_manifest(a)
    if bad:
        print("manifest differs in: " + ", ".join(bad))
        return 1
    print("manifest: every hash matches")
    return 0


# ---------------------------------------------------------------- the check at the start of every launch

def short(v):
    """A value for a message: a hash shortened to 8 characters, a short scalar as it is, anything else only as 'differs'. Never content."""
    if v is None or isinstance(v, (bool, int, float)):
        return str(v)
    if isinstance(v, str):
        if re.fullmatch(r"[0-9a-f]{64}", v):
            return v[:8]
        return v if len(v) <= 48 and "\n" not in v else "(text)"
    return "(structure)"


def start_markers(work):
    work = Path(work)
    found = sorted(work.glob("*.passed"))
    if (work / "GATE.opened").exists():
        found.append(work / "GATE.opened")
    return found


def start_problems(a, want, markers):
    """-> (problems, notes). Problems name fields and files with short hashes only."""
    problems, notes = [], []
    if want is None:
        if markers:
            problems.append("stage marker(s) without a manifest: " + ", ".join(m.name for m in markers))
        return problems, notes
    have = compute(a)
    for d in (want, have):   # the head commit is information only; the server's version and the list of everything it lists are checked by the gate
        d.pop("commit", None)
        (d.get("server") or {}).pop("models_listed", None)
        (d.get("server") or {}).pop("version", None)
    wopt = want.pop("options", None) or {}
    hopt = current_options(a)
    have.pop("options", None)
    if not (a.models_json and Path(a.models_json).is_file()):   # no listing in hand (a stage that needs no server): the model ids are not compared
        want.pop("server", None)
        have.pop("server", None)
    fw, fh = flat(want), flat(have)
    for k in sorted(set(fw) | set(fh)):
        if fw.get(k) != fh.get(k):
            problems.append(f"{k}: {short(fw.get(k, 'absent'))} -> {short(fh.get(k, 'absent'))}")
    fw, fh = flat(wopt, "options."), flat(hopt, "options.")
    legacy = []
    for k in sorted(set(fw) | set(fh)):
        if k in fh and k not in fw:
            legacy.append(k)   # an older manifest did not record this option: nothing to compare
        elif fw.get(k) != fh.get(k):
            problems.append(f"{k}: {short(fw.get(k, 'absent'))} -> {short(fh.get(k, 'absent'))}")
    if legacy:
        notes.append("not recorded by the manifest: " + ", ".join(legacy))
    pin = (want.get("pinned") or {}).get("sha256")
    work = Path(a.work_dir)
    for mk in markers:
        try:
            d = json.loads(mk.read_text())
        except Exception:   # noqa: BLE001
            problems.append(f"marker {mk.name}: not readable")
            continue
        if d.get("pin") and d.get("pin") != pin:
            problems.append(f"marker {mk.name}: written under another manifest (pin {short(d['pin'])} against {short(pin)})")
        outs = d.get("outputs")
        if outs is None and mk.suffix == ".passed":
            notes.append(f"marker {mk.name} records no output hashes")
        for rel, digest in sorted((outs or {}).items()):
            f = work / rel
            if not f.is_file():
                problems.append(f"marker {mk.name}: output {rel} is missing")
            elif sha(f) != digest:
                problems.append(f"marker {mk.name}: output {rel} changed ({short(digest)} -> {short(sha(f))})")
    return problems, notes


def cmd_start_check(a):
    """Exit 0: a fresh work folder, or the stored manifest and the markers agree with this launch (resume). Exit 2: refused, with the fields and the
    mv lines. Reads only; moves and deletes nothing."""
    work, man = Path(a.work_dir), Path(a.manifest)
    markers = start_markers(work) if work.is_dir() else []
    if not man.exists() and not markers:
        print("start check: no manifest and no stage marker: a fresh run")
        return 0
    problems, notes = start_problems(a, rjson(man) if man.exists() else None, markers)
    if not problems:
        done = [m.stem for m in markers if m.suffix == ".passed"]
        print(f"start check: the stored manifest agrees with this launch; resuming; {len(done)} stage(s) already done"
              + (f" ({', '.join(done)})" if done else "") + (f"; note: {'; '.join(notes)}" if notes else ""))
        return 0
    stamp = __import__("time").strftime("%Y%m%d-%H%M%S")
    print("start check REFUSED: the work folder holds a manifest or stage markers that do not agree with this launch; nothing was started, moved or deleted")
    for p in problems:
        print(f"  differs: {p}")
    print("  To start again under changed inputs, move the old run aside (this chain never moves or deletes), then start it again:")
    print(f"    mv {work} {work}.aside-{stamp}")
    try:
        inside = man.resolve().is_relative_to(work.resolve())
    except OSError:
        inside = False
    if man.exists() and not inside:
        print(f"    mv {man} {man}.aside-{stamp}")
    return 2


# ---------------------------------------------------------------- ledger

def ledger_rows(path, wanted=("1", "2")):
    rows = {}
    for line in Path(path).read_text().splitlines():
        if not line.lstrip().startswith("|"):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if cells and cells[0] in wanted and len(cells) >= 5:
            rows[cells[0]] = cells
    return rows


def ledger_label(nums):
    """"rows 1 and 2" (the E2 wording); "row 3"; "rows 1, 2 and 3"."""
    nums = [str(n) for n in nums]
    return ("row " if len(nums) == 1 else "rows ") + (nums[0] if len(nums) == 1 else ", ".join(nums[:-1]) + " and " + nums[-1])


def ledger_problems(path, nums=("1", "2")):
    """Problems of the ledger rows `nums` (default 1 and 2, as for E2; a later chain, such as E5, requires row 3)."""
    out = []
    nums = tuple(str(n) for n in nums)
    if not Path(path).exists():
        return ["the ledger file is missing"]
    rows = ledger_rows(path, nums)
    for n in nums:
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
    nums = tuple(getattr(a, "rows", None) or ("1", "2"))
    bad = ledger_problems(a.file, nums)
    for b in bad:
        print(b)
    if not bad:
        print(f"ledger: {ledger_label(nums)} present, each with a date and a decision rule")
    return 1 if bad else 0


# ---------------------------------------------------------------- counts, scan

def parse_hidden(specs):
    out = {}
    for s in specs:
        n, _, p = s.partition("=")
        out[n] = p
    return out


def parse_expected(specs):
    """['tier_a=10', ...] -> {name: n}; none given: the real counts."""
    if not specs:
        return dict(EXPECTED)
    out = {}
    for s in specs:
        n, _, v = s.partition("=")
        if n not in EXPECTED or not v.isdigit():
            raise eval2.EvalError(f"--expected {s!r}: expected NAME=COUNT with NAME one of {sorted(EXPECTED)}")
        out[n] = int(v)
    if set(out) != set(EXPECTED):
        raise eval2.EvalError("--expected: all four items files need a count")
    return out


def count_problems(h, expected=None):
    """h: name -> path for tier_a, tier_a3, judged_1, judged_2, rubrics_1, rubrics_2. -> list of condition texts."""
    out = []
    ids = {}
    for name, want in (expected or EXPECTED).items():
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
    ex = parse_expected(a.expected)
    bad = count_problems(h, ex)
    for b in bad:
        print(b)
    if not bad:
        print("counts: " + ", ".join(f"{n} {ex[n]}" for n in ex) + "; rubric ids equal item ids")
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
# Keys of judged items that describe the item for people and that no tool reads. A sealed session labelled `side` and `adaptation`
# as metadata on 2026-11-20 (an answerer needs nothing from them; no question refers to content outside `question` and `evidence`).
# A new key must be checked the same way before it is added here.
KNOWN_JUDGED_METADATA = ["side", "adaptation"]
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
    meta = set(KNOWN_JUDGED_METADATA) if kind == "judged_items" else set()
    known |= meta
    unread = {k: c for k, c in txt.items() if k not in spec["keys"] and k not in known}
    allowed = {k: c for k, c in txt.items() if k in known and k not in spec["keys"]}
    allowed_meta = {k: c for k, c in allowed.items() if k in meta}
    allowed = {k: c for k, c in allowed.items() if k not in meta}
    if allowed:
        lines.append(f"shape {name}: text keys that nothing reads, known metadata (the same keys are in the open files): " + ", ".join(f"{k} {c}" for k, c in sorted(allowed.items())))
    if allowed_meta:
        lines.append(f"shape {name}: text keys that nothing reads, known metadata (labelled metadata by a sealed session, 2026-11-20): " + ", ".join(f"{k} {c}" for k, c in sorted(allowed_meta.items())))
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


def marker_summary(nbuilt, nkept, nrows, unmarked):
    return (f"{nbuilt} marker(s) built, {nbuilt - nkept} dropped as public (the open evaluation files or the plain outputs of the open stages G and O hold the same line), "
            f"{nkept} kept; {len(unmarked)} of {nrows} hidden row(s) left with no marker (only the id scan covers them)")


def cmd_scan(a):
    h = parse_hidden(a.hidden)
    ids = hidden_ids(h)
    hits = scan_plain(a.dir, ids, a.exclude or [])
    if hits:
        print(f"scan: {len(hits)} plain-text file(s) under {a.dir} hold a hidden id: " + ", ".join(hits[:10]))
        return 1
    print(f"scan: no plain-text file under {a.dir} holds an id of a hidden item ({len(ids)} ids checked)")
    if a.text:
        rec = Path(a.freeze) if a.freeze else Path(a.dir) / FREEZE_NAME
        if a.require_freeze and not rec.is_file():
            print(f"scan: no record of the open-stage files ({rec.name}): the gate has not frozen them")
            return 1
        if a.explain:
            return explain_scan(a, h, rec)
        detail = {}
        marks, nrows, dropped = text_markers(h, corpus_files=a.corpus_files, work=a.dir, record=rec, detail=detail)
        unchanged, changed = freeze_state(a.dir, rec)
        th = scan_text(a.dir, marks, a.also or [], skip=unchanged)
        print("scan: " + marker_summary(len(marks) + dropped, len(marks), nrows, detail["unmarked"]))
        if a.list_unmarked_ids and detail["unmarked"]:
            print("scan: rows with no marker: " + ", ".join(sorted(detail["unmarked"])))
        if changed:
            print(f"scan: {len(changed)} plain file(s) of stages G and O changed or appeared after the gate and were scanned in full: " + ", ".join(changed[:10]))
        if th or not marks:
            print(f"scan: {len(th)} plain file(s) hold hidden text: " + ", ".join(th[:10]) if th else "scan: no hidden text could be read to check against")
            return 1
        print(f"scan: no plain file under {a.dir} (logs and the heartbeat included) holds distinctive text of a hidden row "
              f"({len(marks)} strings from {nrows} rows; {dropped} left out as public text)")
    return 0


def explain_scan(a, h, rec):
    """Counts only: per plain file that matches a marker, how many of the matching markers occur in the pinned open evaluation files,
    in the open stages' input files, or nowhere public. A file produced by an open stage with a marker that is nowhere public: a failure."""
    rows = hidden_rows(h)
    allm = set()
    for r in rows:
        allm |= row_markers(r)
    ev = open_corpus(a.corpus_files)
    root = Path(a.dir)
    ins = "\n".join(text_forms(root / n) for n in OPEN_STAGE_INPUTS if (root / n).is_file())
    produced = {p.resolve() for p in open_stage_files(a.dir)}
    seen, bad, matched = set(), 0, 0
    print(f"explain: {len(allm)} marker(s) built from {len(rows)} hidden row(s); counts per matching plain file (each marker counted once: open evaluation files first, "
          "then the open stages' input files, then nowhere public)")
    for r0 in [root] + [Path(x) for x in a.also or []]:
        files = [r0] if r0.is_file() else (sorted(r0.rglob("*")) if r0.is_dir() else [])
        for p in files:
            if not p.is_file() or p.name.endswith(".enc") or p.name.endswith(".enc.tmp") or p.resolve() in seen:
                continue
            seen.add(p.resolve())
            text = text_forms(p)
            hit = [m for m in allm if m in text]
            if not hit:
                continue
            matched += 1
            n_ev = sum(1 for m in hit if m in ev)
            n_in = sum(1 for m in hit if m not in ev and m in ins)
            n_no = len(hit) - n_ev - n_in
            name = str(p.relative_to(root)) if root in p.parents else p.name
            tag = "open-stage file" if p.resolve() in produced else "other file"
            print(f"explain: {name} ({tag}): {len(hit)} matching marker(s); {n_ev} in the open evaluation files, {n_in} in the open stages' inputs, {n_no} nowhere public")
            if n_no:
                bad += 1
                print(f"explain FAIL: {name}: {n_no} matching marker(s) occur nowhere public" +
                      (" although an open stage produced this file: it is not a coincidence with public text" if p.resolve() in produced else ""))
    print(f"explain: {matched} plain file(s) match; {bad} with a marker that is nowhere public")
    return 1 if bad else 0


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


def open_corpus(files=None):
    """The normalised text lines of the open evaluation files (plain jsonl under eval/m4, or the given files): a line that also occurs
    there is not distinctive for the hidden half (the hidden items are written from the same templates). `files` is for a rehearsal,
    whose hidden rows are open items: only the plain input files of the rehearsal count as a legitimate plain copy."""
    lines = []
    for p in sorted(Path(f) for f in files) if files is not None else sorted((ROOT / "eval/m4").rglob("*.jsonl")):
        try:
            for l in p.read_text().splitlines():
                if l.strip():
                    for leaf in string_leaves(json.loads(l)):
                        lines += prose_lines(leaf)
                        lines.append(norm_line(leaf))   # the whole field with its lines joined, as the scan reads a file
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


# plain files that stages G and O write into the work folder: open items only, so their text is public (frozen at the gate)
OPEN_STAGE_PATTERNS = ("ans_G*", "ans_O_*", "check_G*", "check_O_*", "idcheck_O_*", "gate_g.txt", "O_judge_*", "pilot2_items_h.jsonl",
                       "smoke_items.jsonl", "server_G*.json", "server_O_*.json", "server_judge_O_*.json")
OPEN_STAGE_INPUTS = ("pilot2_items_h.jsonl", "smoke_items.jsonl")   # what the open stages read besides the files under eval/m4
FREEZE_NAME = "OPEN_STAGES.frozen.json"


def text_forms(p):
    """A file's text the way the scan reads it: normalised, and again with JSON escapes undone."""
    try:
        data = Path(p).read_bytes().decode("utf-8", "ignore")
    except OSError:
        return ""
    return norm_line(data) + "\n" + norm_line(data.replace('\\"', '"').replace("\\n", " "))


def open_stage_files(work):
    root, out = Path(work), set()
    for pat in OPEN_STAGE_PATTERNS:
        out |= {p for p in root.glob(pat) if p.is_file() and not p.name.endswith(".enc")}
    return sorted(out)


def freeze_record(work):
    return {"files": {p.name: sha(p) for p in open_stage_files(work)}}


def freeze_state(work, record):
    """-> (unchanged files as resolved paths, names of changed or new or missing ones). Without a record every current file counts as unchanged."""
    root = Path(work)
    if not Path(record).is_file():
        return {p.resolve() for p in open_stage_files(work)}, []
    want = rjson(record).get("files", {})
    unchanged, changed = set(), []
    for n, hh in sorted(want.items()):
        p = root / n
        if p.is_file() and sha(p) == hh:
            unchanged.add(p.resolve())
        else:
            changed.append(n)
    changed += [p.name for p in open_stage_files(work) if p.name not in want]
    return unchanged, sorted(set(changed))


def text_markers(h, corpus=None, corpus_files=None, work=None, record=None, detail=None):
    """-> (markers kept, rows read, markers left out because public text holds the same line). Public text: the open files under
    eval/m4 and, when `work` is given, the plain files of stages G and O in it that are unchanged since the record (or all of them
    when there is no record yet). `detail` (a dict) gets "unmarked": ids of the rows that are left with no marker."""
    rows = hidden_rows(h)
    per, marks = [], set()
    for r in rows:
        m = row_markers(r)
        per.append((r.get("id"), m))
        marks |= m
    corpus = open_corpus(corpus_files) if corpus is None else corpus
    if work is not None:
        unchanged, _ = freeze_state(work, record or Path(work) / FREEZE_NAME)
        corpus += "\n" + "\n".join(text_forms(p) for p in sorted(unchanged))
    keep = sorted(m for m in marks if m not in corpus)
    ks = set(keep)
    if detail is not None:
        detail["unmarked"] = [str(i) for i, m in per if not (m & ks)]
    return keep, len(rows), len(marks) - len(keep)


def scan_text(folder, markers, also=(), skip=()):
    """Plain files under `folder` (every one, logs included, not .enc) and the extra files or folders in `also` that hold one of the
    markers; the files in `skip` (resolved paths: the open-stage files unchanged since the gate) are not read. -> sorted list of names (never the text)."""
    if not markers:
        return []
    roots = [Path(folder)] + [Path(x) for x in also]
    seen, hits = set(skip), []
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
        pin_now = compute_pins(repo)["sha256"]   # keyed on the pinned files, not the commit: a commit elsewhere does not rerun the suite
        okf = Path(a.tests_ok_file) if a.tests_ok_file else None
        if okf and okf.exists() and okf.read_text().strip() == pin_now:
            cond("tests", True, f"passed earlier on these pinned files ({pin_now[:10]})")
        else:
            r = subprocess.run(a.test_cmd, shell=True, cwd=repo, capture_output=True, text=True)
            tail = (r.stdout + r.stderr).strip().splitlines()[-1:] or [""]
            cond("tests", r.returncode == 0, tail[0][:120])
            if r.returncode == 0 and okf:
                okf.write_text(pin_now + "\n")
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
        cond("no uncommitted change in a pinned path", True, "not enforced (dry run)")
    else:
        dirty = tree_dirty(repo)
        cond("no uncommitted change in a pinned path", dirty == [], f"{len(dirty or [])} changed file(s): " + ", ".join(status_path(l) for l in (dirty or [])[:8]) if dirty else "")
    cond("commit (information only)", True, ("manifest commit equals HEAD" if man.get("commit") == head else
         f"HEAD {str(head)[:10]} differs from the manifest's {str(man.get('commit'))[:10]}; the pinned files decide") if man else "")
    if man is not None:
        pp = pin_problems(repo, man, True)   # the dirty part is the condition above
        cond("pinned files", not pp, "; ".join(pp) if pp else f"{man['pinned']['count']} files match the manifest")
    # 3. every hash of the manifest
    if man is not None:
        bad = check_manifest(a)
        cond("manifest hashes", not bad, "differs in: " + ", ".join(bad) if bad else "every field matches")
    # 4. ledger
    lnums = tuple(getattr(a, "ledger_rows", None) or ("1", "2"))
    lp = ledger_problems(a.ledger, lnums)
    cond("ledger " + ledger_label(lnums), not lp, "; ".join(lp))
    # 5. counts
    h = parse_hidden(a.hidden)
    ex = parse_expected(a.expected)
    cp = count_problems(h, ex)
    cond("hidden row counts " + ", ".join(str(ex[n]) for n in ex), not cp, "; ".join(cp))
    # 6. G and O
    for stage in ("G", "O"):
        mk = Path(a.markers) / f"{stage}.passed"
        ok = False
        if mk.exists():
            try:
                d = rjson(mk)
                ok = d.get("passed") is True and (man is None or d.get("pin") == (man.get("pinned") or {}).get("sha256"))
            except Exception:  # noqa: BLE001
                ok = False
        cond(f"stage {stage} passed", ok, "marker present and for these pinned files" if ok else "no passed marker for these pinned files")
    # 7. no plain-text file with a hidden id
    try:
        ids = hidden_ids(h)   # the ids of the files that read; a file that does not read is reported by the count condition
    except Exception:  # noqa: BLE001
        ids = set()
    hits = scan_plain(a.work_dir, ids, a.exclude or [])
    cond("no plain-text file with a hidden id", not hits and bool(ids), f"{len(hits)} file(s): " + ", ".join(hits[:5]) if hits else
         ("" if ids else "no hidden id could be read to check against"))
    rec = Path(a.markers) / FREEZE_NAME
    unchanged, changed = freeze_state(a.work_dir, rec) if rec.is_file() else (None, [])
    cond("plain files of stages G and O unchanged since they were frozen", not changed,
         f"{len(changed)} changed or new: " + ", ".join(changed[:5]) if changed else
         (f"{len(unchanged)} file(s) match the record" if rec.is_file() else "no record yet; made when the gate opens"))
    detail = {}
    try:
        marks, nrows, dropped = text_markers(h, corpus_files=a.corpus_files, work=a.work_dir, record=rec, detail=detail)
    except Exception:  # noqa: BLE001
        marks, nrows, dropped, detail = [], 0, 0, {"unmarked": []}
    th = scan_text(a.work_dir, marks, a.also or [], skip=freeze_state(a.work_dir, rec)[0])
    summ = marker_summary(len(marks) + dropped, len(marks), nrows, detail["unmarked"])
    cond("no plain file with hidden text", not th and bool(marks),
         (f"{len(th)} file(s): " + ", ".join(th[:5]) + "; " + summ) if th else (summ if marks else "no hidden text could be read to check against"))
    for name, ok, note in results:
        print(f"gate {'PASS' if ok else 'FAIL'}: {name}" + (f" ({note})" if note else ""))
    bad = [n for n, ok, _ in results if not ok]
    if not bad and not rec.is_file():   # the open stages' plain files are frozen here: later scans check that they did not change
        tmp = rec.with_name(rec.name + ".tmp")
        tmp.write_text(json.dumps(freeze_record(a.work_dir), indent=1, sort_keys=True) + "\n")
        os.replace(tmp, rec)
        print(f"gate: froze {len(freeze_record(a.work_dir)['files'])} plain file(s) of stages G and O ({rec.name}, sha256 per file)")
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
          f"median {statistics.median(tk) if tk else 0:.0f} tokens, {cut} cut at the limit, "
          f"{len((eval2.read_manifest(a.file) or {}).get('reloads') or [])} model reloads")
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

def cmd_tags_synth(a):
    """A stand-in tag file for a dry run or a rehearsal: the first judged id of each of the two judged files under the tag conductor_exposed.
    Written in plain, to a folder outside the work folder, and removed by the chain at its end."""
    ids = []
    for spec in a.hidden:
        _, _, path = spec.partition("=")
        rows = rows_of(path)
        if rows:
            ids.append(rows[0]["id"])
    Path(a.out).write_text(json.dumps({"schema": 1, "tags": {"conductor_exposed": {"ids": sorted(ids)}}}) + "\n")
    print(f"tags-synth: {len(ids)} ids")
    return 0 if ids else 1


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
    p2, p2r = rd("eval/m4/pilot2/items.jsonl"), rd("eval/m4/pilot2/rubrics.jsonl")
    j_items = b1 + b2 + p2[:1]   # the synthetic judged rows: 25 (first file) + 36 (the rest), as the real judged files
    j_rub = r1 + r2 + [r for r in p2r if r["id"] == p2[0]["id"]]
    by_rub = {r["id"]: r for r in j_rub}
    for name, lo, hi in (("judged_1", 0, 25), ("judged_2", 25, 61)):
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
    print(f"synth: 4 items files ({245}, {60}, {25}, {36} rows) and 2 rubrics files in {out.name}")
    return 0


# ---------------------------------------------------------------- rehearsal: open items encrypted as if they were hidden

REH_COUNTS = {"tier_a": 10, "tier_a3": 10, "judged_1": 2, "judged_2": 4}
REH_SEED = "20261120"


def spread(groups, n, seed, size=lambda g: 1):
    """Pick groups round-robin over their kind (kinds sorted, within a kind in the seeded order) until exactly n rows are chosen; a group
    that does not fit the rows still needed is skipped. groups: list of (kind, key, rows). -> list of the chosen (kind, key, rows)."""
    by_kind = {}
    for g in sorted(groups, key=lambda g: order_key(seed, g[1])):
        by_kind.setdefault(g[0], []).append(g)
    chosen, left = [], n
    while left > 0 and any(by_kind.values()):
        progressed = False
        for k in sorted(by_kind):
            while by_kind[k] and left > 0:
                g = by_kind[k].pop(0)
                if size(g) <= left:
                    chosen.append(g)
                    left -= size(g)
                    progressed = True
                    break
        if not progressed:
            break
    if left:
        raise eval2.EvalError(f"rehearsal sample: {left} of {n} rows cannot be filled")
    return chosen


def cmd_rehearsal_prepare(a):
    """Encrypted copies of open items (temporary key of the environment), the plain open input files of the rehearsal, a rehearsal ledger."""
    out = Path(a.dir)
    hid, plain = out / "rehearsal_hidden", out / "rehearsal_plain"
    if (hid / "tier_a.jsonl.enc").exists():
        print("rehearsal-prepare: the files exist; not overwritten")
        return 0
    key = os.environ.get("EVAL_HIDDEN_KEY")
    if not key or Path(key).resolve().parent != out.resolve():
        print("rehearsal-prepare: EVAL_HIDDEN_KEY must name a key file inside the rehearsal folder (never the real key)", file=sys.stderr)
        return 2
    rd = lambda p: [json.loads(l) for l in (ROOT / p).read_text().splitlines() if l.strip()]
    seed = a.seed
    smoke = rd_json(ROOT / "eval/m4/smoke_ids.json")["ids"]
    a1 = {r["id"] for r in rd("work/m4/s6/ans_A1.jsonl")}
    tier_a, a3 = rd("eval/m4/open/tier_a.jsonl"), rd("eval/m4/open/tier_a3.jsonl")
    smoke_ok = [i for i in sorted(smoke, key=lambda i: order_key(seed, i)) if i in a1]
    if len(smoke_ok) < REH_COUNTS["tier_a"]:
        raise eval2.EvalError("rehearsal sample: fewer than 10 smoke items have a stored S6 answer")
    smoke10, smoke6 = smoke_ok[:10], smoke_ok[:6]
    by_id = {r["id"]: r for r in tier_a}
    # hidden tier A: 10 items spread over the kinds, none of them a smoke item (the smoke items are in plain files of the run)
    pool = [(r["kind"], r["id"], [r]) for r in tier_a if r["id"] not in set(smoke)]
    a_rows = [g[2][0] for g in spread(pool, REH_COUNTS["tier_a"], seed)]
    # a3: whole pairs together (an item without a pair is its own group); 10 hidden, 10 for stage O, none in both
    groups = {}
    for r in a3:
        groups.setdefault(r.get("pair") or r["id"], []).append(r)
    ga = [(rows[0]["kind"], k, rows) for k, rows in groups.items()]
    hid_g = spread(ga, REH_COUNTS["tier_a3"], seed, size=lambda g: len(g[2]))
    taken = {g[1] for g in hid_g}
    o_g = spread([g for g in ga if g[1] not in taken], 10, seed, size=lambda g: len(g[2]))
    a3_rows = [r for g in hid_g for r in g[2]]
    a3_o = [r for g in o_g for r in g[2]]
    # judged: 6 B2 scenarios spread over the kinds (items with their rubrics), split 2 and 4; stage O: 6 pilot2 scenarios
    b2, b2r = rd("eval/m4/b2/items.jsonl"), {r["id"]: r for r in rd("eval/m4/b2/rubrics.jsonl")}
    p2, p2r = rd("eval/m4/pilot2/items.jsonl"), {r["id"]: r for r in rd("eval/m4/pilot2/rubrics.jsonl")}
    j = [g[2][0] for g in spread([(r["kind"], r["id"], [r]) for r in b2 if r["id"] in b2r], 6, seed)]
    p6 = [g[2][0] for g in spread([(r.get("kind") or "", r["id"], [r]) for r in p2 if r["id"] in p2r], 6, seed)]
    if {r["id"] for r in j} & {r["id"] for r in p6}:
        raise eval2.EvalError("rehearsal sample: a B2 scenario is also a pilot2 scenario")
    for d in (hid, plain):
        d.mkdir(parents=True, exist_ok=True)
    wrows(hid / "tier_a.jsonl.enc", a_rows)
    wrows(hid / "tier_a3.jsonl.enc", a3_rows)
    for name, part in (("judged_1", j[:2]), ("judged_2", j[2:])):
        wrows(hid / f"{name}_items.jsonl.enc", part)
        wrows(hid / f"{name}_rubrics.jsonl.enc", [b2r[r["id"]] for r in part])
    pw = lambda name, rows: (plain / name).write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows))
    pw("smoke10_items.jsonl", [by_id[i] for i in smoke10])
    pw("smoke6_items.jsonl", [by_id[i] for i in smoke6])
    pw("a3_10_items.jsonl", a3_o)
    pw("pilot6_items.jsonl", p6)
    pw("pilot6_rubrics.jsonl", [p2r[r["id"]] for r in p6])
    rule = "REHEARSAL: no decision; the figures of 20 items decide nothing and are not a look at the hidden half"
    (out / "REHEARSAL_LEDGER.md").write_text(
        "| # | Date | Run | Configuration | Decision rule written beforehand | Result totals | Who |\n|---|---|---|---|---|---|---|\n"
        f"| 1 | {a.date} | REHEARSAL of E2 (open items, encrypted) | 10 tier A, 10 a3, 6 B2 | {rule} | | job-operator |\n"
        f"| 2 | {a.date} | REHEARSAL of E3 (open items, encrypted) | 6 B2 | {rule} | | job-operator |\n")
    print(f"rehearsal-prepare: hidden copies {len(a_rows)} tier A, {len(a3_rows)} a3, {len(j[:2])} + {len(j[2:])} judged with rubrics; plain input files: "
          f"{len(smoke10)} + {len(smoke6)} smoke, {len(a3_o)} a3, {len(p6)} pilot2 scenarios; ledger written in {out.name}")
    return 0


def rd_json(p):
    return json.loads(Path(p).read_text())


def cmd_rehearsal_findings(a):
    """Fake findings for every bundle of a rehearsal (a clean row for most answers, a 'wrong' finding with a quote taken from the answer for
    about a third of them), sealed with the temporary key, so that `eval2_sealed_reading.py totals` runs on them."""
    d = Path(a.dir)
    key = json.loads(eval2.decrypt_bytes((d / "blind_key.json.enc").read_bytes()).decode("utf-8"))
    n_files = n_rows = n_find = 0
    for b in key["bundles"]:
        target = d / f"findings_{b}.jsonl.enc"
        if target.exists():
            continue
        rows = []
        for r in rows_of(d / f"{b}.jsonl.enc"):
            name, text = r["id"], r["answer"]
            if int(hashlib.sha256(f"{a.seed}:{name}".encode()).hexdigest(), 16) % 3 == 0 and len(text.strip()) >= 20:
                q = " ".join(text.split())[:40]
                rows.append({"answer": name, "id": r["item"], "kind": "wrong", "quote": q, "point": "", "supported": False, "minor": False,
                             "source": "", "note": "REHEARSAL fake finding"})
                n_find += 1
            else:
                rows.append({"answer": name, "id": r["item"], "kind": "clean", "quote": "", "point": "", "supported": False, "minor": False,
                             "source": "", "note": "REHEARSAL fake clean row"})
        wrows(target, rows)
        n_files += 1
        n_rows += len(rows)
    print(f"rehearsal-findings: {n_files} findings file(s) written, {n_rows} rows, {n_find} fake findings (sealed with the temporary key)")
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
    for n in ("prepare", "start-check"):
        if n == "start-check":
            p = sub.add_parser(n)
            common(p)
            p.add_argument("--manifest", required=True)
            p.add_argument("--work-dir", required=True)
            p.add_argument("--r2", action="store_true")
            p.add_argument("--reading", default="both")
            p.add_argument("--f-judged", action="store_true")
            p.add_argument("--no-cross-judge", action="store_true")
        else:
            p = sub.choices[n]
        p.add_argument("--mode", choices=["real", "rehearsal"], default=None)
        p.add_argument("--tag-file", default=None)
    pc = sub.add_parser("pin-check")
    pc.add_argument("--repo", default=str(ROOT))
    pc.add_argument("--manifest", required=True)
    pc.add_argument("--hidden", nargs="*", default=[], metavar="NAME=PATH.enc")
    pc.add_argument("--allow-dirty", action="store_true", help="dry runs: the uncommitted-change condition is not enforced")
    g = sub.choices["gate"]
    g.add_argument("--ledger", required=True)
    g.add_argument("--ledger-rows", nargs="+", default=["1", "2"], metavar="N", help="the ledger rows to require (default 1 2; E5: 3)")
    g.add_argument("--work-dir", required=True)
    g.add_argument("--markers", required=True)
    g.add_argument("--exclude", nargs="*", default=[])
    g.add_argument("--also", nargs="*", default=[], help="more files or folders to scan for hidden text (this run's logs, the heartbeat)")
    g.add_argument("--corpus-files", nargs="*", default=None, help="rehearsal: see scan")
    g.add_argument("--expected", nargs="*", default=[], metavar="NAME=COUNT", help="rehearsal: the row counts of the four items files")
    g.add_argument("--allow-dirty", action="store_true")
    g.add_argument("--tests", choices=["run", "skip"], default="run")
    g.add_argument("--test-cmd", default=os.environ.get("M4_E2_TEST_CMD", ".venv/bin/python -m unittest discover -s tests"))
    g.add_argument("--tests-ok-file")
    s = sub.add_parser("ledger")
    s.add_argument("--file", required=True)
    s.add_argument("--rows", nargs="+", default=["1", "2"], metavar="N", help="the ledger rows to require (default 1 2)")
    s = sub.add_parser("counts")
    s.add_argument("--hidden", nargs="+", required=True)
    s.add_argument("--expected", nargs="*", default=[], metavar="NAME=COUNT")
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
    s.add_argument("--corpus-files", nargs="*", default=None, help="rehearsal: the plain open input files; a line in them is not a marker (default: all open files under eval/m4)")
    s.add_argument("--freeze", help=f"the record of the open-stage files (default DIR/{FREEZE_NAME})")
    s.add_argument("--require-freeze", action="store_true", help="fail when there is no record of the open-stage files (the scans after the hidden stages)")
    s.add_argument("--explain", action="store_true", help="with --text: per matching plain file, counts of markers by where else they occur (counts only; no text, no line)")
    s.add_argument("--list-unmarked-ids", action="store_true", help="print the ids of the hidden rows left with no marker (not for files in the work folder)")
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
    s = sub.add_parser("rehearsal-prepare")
    s.add_argument("--dir", required=True, help="the rehearsal work folder (the key file of EVAL_HIDDEN_KEY must lie in it)")
    s.add_argument("--seed", default=REH_SEED)
    s.add_argument("--date", default=__import__("datetime").date.today().isoformat())
    s = sub.add_parser("rehearsal-findings")
    s.add_argument("--dir", required=True, help="the bundles folder")
    s.add_argument("--seed", default=REH_SEED)
    s = sub.add_parser("tags-synth")
    s.add_argument("--out", required=True)
    s.add_argument("--hidden", nargs="+", required=True, metavar="NAME=PATH.enc")
    s = sub.add_parser("synth")
    s.add_argument("--dir", required=True)
    s.add_argument("--marker", default="SYNTHMARK")
    s.add_argument("--drop-a", type=int, default=0, help="rows left out of the synthetic tier A file (to test the row-count condition)")
    return ap


CMDS = {"prepare": cmd_prepare, "start-check": cmd_start_check, "verify-manifest": cmd_verify_manifest, "gate": cmd_gate, "pin-check": cmd_pin_check, "ledger": cmd_ledger, "counts": cmd_counts, "rehearsal-prepare": cmd_rehearsal_prepare, "rehearsal-findings": cmd_rehearsal_findings, "shape": cmd_shape,
        "scan": cmd_scan, "gate-g": cmd_gate_g, "check-answers": cmd_check_answers, "runline": cmd_runline, "judge-done": cmd_judge_done,
        "merge": cmd_merge, "subset": cmd_subset, "items-for": cmd_items_for, "empty-enc": cmd_empty_enc, "agreement": cmd_agreement,
        "fake-status": cmd_fake_status, "packs-equal": cmd_packs_equal, "patched": cmd_patched, "verdict": cmd_verdict, "synth": cmd_synth, "tags-synth": cmd_tags_synth}


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
