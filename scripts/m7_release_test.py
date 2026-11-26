#!/usr/bin/env python3
"""M7 release test: the helpers of scripts/m7_release_test.sh (MILESTONE.md, "The release test"). CPU only; the chain itself is the shell script.

  arms          the arms of configs.json (an entry with an `ask_command`) and their ask argv, checked
  manifest      write the run's manifest (configs sha256, arms, sets, seeds, rule text hash, pinned files) or refuse when it differs (an amended config = a new manifest)
  ledger-check  refuse unless the hidden ledger holds a line with the token release-look-h6 and the configs sha256
  answer        one arm on one set: the config's `ask ... --json` command per item, one at a time, resumable by rows (h6: encrypted rows, a counter only)
  packs         recover each answer's pack (chunk ids) from the question and the returned expansion; the sources must match position by position
  judge-run     eval2_judge.py with its output filtered (a hidden run prints no item id)
  server        gate | unload | status (W2: never unload another client's model; W16: only the DeepSeek build, logged by the caller)
  bundle        blind reading bundles: d7 (open: first reader, second-family export) and h6 (sealed, first reader only)
  unblind       d7 (open) or h6 (sealed, encrypted output) readers' findings
  pack-input    the adjudication pack's input files (scripts/m7_adjudication_pack.py build)
  measures      per-answer records of a set: judge, flags and adjudication in one file (h6: encrypted)
  stub-ask      the stand-in for `python -m kafka_sre_rag ask ... --json` in a dry run

Hidden handling (h6): items, rubrics, answers, judge outputs, findings and measures are .enc files (eval2.py's format and key); plain text lives in memory only; every
printed line holds counts, never an id or a text; the ask call's output is captured and never printed. Exit codes: 0 done, 1 failed, 2 refused.
"""
import argparse
import hashlib
import json
import os
import re
import shlex
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))
import eval2 as E  # noqa: E402
import m7_release_rule as RR  # noqa: E402

SEEDS = {"bundle": 20261126, "pack_shuffle": 20261127, "pack_sample": 20261128, "bootstrap": RR.BOOT_SEED}
LEDGER_TOKEN = "release-look-h6"
H6_TAG = "conductor_exposed_h6"
DS_MODEL = "DeepSeek-V4.1-Flash-1017-oQ4e-mtp"
D7_ITEMS, D7_RUBRICS = "eval/m7/d7/items.jsonl", "eval/m7/d7/rubrics.jsonl"
H6_ITEMS, H6_RUBRICS = "eval/hidden/tier_b_h6_items_final.jsonl.enc", "eval/hidden/tier_b_h6_rubrics_final.jsonl.enc"
SET_COUNT = {"d7": 60, "h6": 60}
ASK_TIMEOUT = 1800


class Refused(Exception):
    pass


def sha256_file(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def sha256_text(s):
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def is_hidden_set(s):
    return s == "h6"


# ---------------------------------------------------------------- configs and arms

def load_configs(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def arm_names(cfgs):
    """Every top-level entry with its own ask_command is an arm (R, C, and later an R variant)."""
    return [k for k, v in cfgs.items() if isinstance(v, dict) and isinstance(v.get("ask_command"), str)]


def ask_flags(cmd):
    """The flags between `ask` and the question placeholder of an ask_command. Refuses anything that is not a real ask with --json."""
    toks = shlex.split(cmd)
    while toks and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*=.*", toks[0]):
        toks = toks[1:]
    if "kafka_sre_rag" not in toks or "ask" not in toks:
        raise Refused("an ask_command is not a `python -m kafka_sre_rag ask` command")
    rest = toks[toks.index("ask") + 1:]
    if not rest or rest[-1] != "<question>":
        raise Refused("an ask_command does not end with the <question> placeholder")
    flags = rest[:-1]
    for bad in ("--dry-run", "--stream", "--service-url"):
        if bad in flags:
            raise Refused(f"an ask_command carries {bad}")
    if "--json" not in flags:
        raise Refused("an ask_command lacks --json")
    return flags


def arm_info(cfgs, arm):
    if arm not in arm_names(cfgs):
        raise Refused(f"arm {arm} is not an entry with an ask_command in configs.json (arms: {', '.join(arm_names(cfgs))})")
    c = cfgs[arm]
    flags = ask_flags(c["ask_command"])
    mid = (c.get("model") or {}).get("oMLX_model_id")
    if not mid and "--model" in flags:
        mid = flags[flags.index("--model") + 1]
    if not mid:
        raise Refused(f"arm {arm}: no model id")
    return {"arm": arm, "flags": flags, "model": mid, "ask_sha256": sha256_text(json.dumps(flags))}


def ask_argv(info, question, python=None, stub=False):
    if stub:
        return [python or sys.executable, str(ROOT / "scripts/m7_release_test.py"), "stub-ask", *info["flags"], question]
    return [python or sys.executable, "-m", "kafka_sre_rag", "ask", *info["flags"], question]


def cmd_arms(a):
    cfgs = load_configs(a.configs)
    want = [x for x in a.arms.split(",") if x] or arm_names(cfgs)
    for arm in want:
        i = arm_info(cfgs, arm)
        print(f"{arm}: model {i['model']}, {len(i['flags'])} ask flags, ask_sha256 {i['ask_sha256'][:16]}")
    print("MODELS " + " ".join(arm_info(cfgs, x)["model"] for x in want))
    return 0


# ---------------------------------------------------------------- manifest and ledger

def state_of(a):
    cfgs_sha = sha256_file(a.configs)
    cfgs = load_configs(a.configs)
    arms = [x for x in a.arms.split(",") if x]
    rule = RR.rule_text(arms, a.sets.split(","), cfgs_sha, a.candidate, a.control)
    return {
        "schema": 1, "configs_sha256": cfgs_sha, "arms": arms, "sets": a.sets.split(","), "candidate": a.candidate, "control": a.control,
        "ask": {x: arm_info(cfgs, x) for x in arms}, "seeds": SEEDS, "rule_text_sha256": sha256_text(rule),
        "pins": {p: sha256_file(p) for p in a.pin if Path(p).exists()}, "missing_pins": sorted(p for p in a.pin if not Path(p).exists()),
        "options": dict(o.partition("=")[::2] for o in a.opt),
    }


def diff_state(old, new):
    out = []
    for k in sorted(set(old) | set(new)):
        if old.get(k) != new.get(k):
            out.append(k)
    return out


def cmd_manifest(a):
    m = Path(a.work) / "MANIFEST.json"
    new = state_of(a)
    if m.exists():
        old = json.loads(m.read_text(encoding="utf-8"))
        if old.get("configs_sha256") != new["configs_sha256"]:
            raise Refused("configs.json differs from the sha256 recorded in the manifest: an amended configuration means a new manifest (a new work folder); nothing was changed")
        d = diff_state(old, new)
        if d:
            raise Refused("the stored manifest differs from the state now in: " + ", ".join(d) + "; nothing was moved or deleted")
        print("manifest equal to the state now")
        return 0
    if any(Path(a.work).glob("ans_*")):
        raise Refused("answer files exist but there is no manifest; the manifest and the header come first")
    Path(a.work).mkdir(parents=True, exist_ok=True)
    m.write_text(json.dumps(new, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    print(f"manifest written; configs sha256 {new['configs_sha256']}; rule text sha256 {new['rule_text_sha256'][:16]}")
    return 0


def manifest_hash(path):
    return sha256_text(Path(path).read_text(encoding="utf-8"))[:16]


def ledger_ok(ledger, sha):
    p = Path(ledger)
    if not p.is_file():
        return False
    return any(LEDGER_TOKEN in line and sha in line for line in p.read_text(encoding="utf-8", errors="replace").splitlines())


def check_ledger(ledger, sha):
    if not ledger_ok(ledger, sha):
        raise Refused(f"the hidden ledger holds no line with the token {LEDGER_TOKEN} and the configs sha256 {sha}; the conductor writes that row before the look; no h6 answer was made")


def cmd_ledger_check(a):
    check_ledger(a.ledger, a.configs_sha256 or sha256_file(a.configs))
    print("ledger row found")
    return 0


def check_set_inputs(set_name, items, rubrics, stub):
    """Open sets use open files; the hidden set uses .enc files only; a stub run never touches eval/hidden/."""
    hid = is_hidden_set(set_name)
    for what, p in (("items", items), ("rubrics", rubrics)):
        if E.is_hidden(p) != hid:
            raise Refused(f"{set_name}: {what} must {'end in .enc' if hid else 'be an open file'}")
        if stub and (ROOT / "eval/hidden") in (Path(p).resolve(), *Path(p).resolve().parents):
            raise Refused("a stub run never uses the real hidden files (eval/hidden/)")


def cmd_check_inputs(a):
    check_set_inputs(a.set, a.items, a.rubrics, a.stub)
    items = E.read_items(a.items)
    rub = {r["id"] for r in E.read_jsonl(a.rubrics)}
    if a.limit:
        if not a.stub:
            raise Refused("--limit is for stub runs only")
        items = items[:a.limit]
    elif not a.stub and len(items) != SET_COUNT[a.set]:
        raise Refused(f"{a.set}: {len(items)} items, {SET_COUNT[a.set]} expected")
    if any(it["id"] not in rub for it in items) or rub - {it["id"] for it in items}:
        raise Refused(f"{a.set}: the items and the rubrics do not cover the same scenarios")
    if a.set == "h6" and not a.stub and not all(it["id"].startswith("h6-") for it in items):
        raise Refused("h6: the items are not the h6 scenarios")
    print(f"{a.set}: {len(items)} items with rubrics ({'hidden: counts only' if is_hidden_set(a.set) else 'open'})")
    return 0


# ---------------------------------------------------------------- rows

def load_rows(path):
    return E.read_jsonl(path) if Path(path).exists() else []


def save_rows(path, rows):
    E.write_jsonl(path, rows)


def words_of(text):
    return len((text or "").split())


def secs_of(r):
    s = r.get("seconds") or {}
    return s.get("total") if isinstance(s, dict) else s


def run_ask(info, question, python, stub, env):
    """-> (row fields, None) or (None, short error code). Output is captured; neither the question nor stderr is ever printed or stored for a hidden run."""
    argv = ask_argv(info, question, python, stub)
    t0 = time.monotonic()
    try:
        p = subprocess.run(argv, capture_output=True, text=True, timeout=ASK_TIMEOUT, cwd=ROOT, env=env)
    except subprocess.TimeoutExpired:
        return None, "timeout"
    except OSError:
        return None, "start failed"
    wall = time.monotonic() - t0
    if p.returncode != 0:
        return None, f"exit {p.returncode}"
    try:
        d = json.loads(p.stdout.strip().splitlines()[-1])
    except (ValueError, IndexError):
        return None, "no JSON"
    if not isinstance(d, dict) or not (d.get("text") or "").strip():
        return None, "empty answer"
    t = d.get("timings") or {}
    sec = {"search_terms": t.get("expand_s", 0.0), "retrieval": t.get("retrieve_s", 0.0), "answer": t.get("answer_s", 0.0)}
    sec["total"] = sum(sec.values())
    row = {"text": d["text"], "tokens": d.get("tokens"), "truncated": bool(d.get("truncated")), "seconds": sec, "wall_s": wall, "search_terms": d.get("expansion"),
           "sources": d.get("sources"), "measured": d.get("measured"), "unmeasured_reasons": d.get("unmeasured_reasons")}
    if d.get("lookup_cards") is not None:
        lc = d["lookup_cards"]
        row["lookup"] = {k: lc.get(k) for k in ("cards_found", "chars", "seconds", "catalogue_sha256") if isinstance(lc, dict)} if isinstance(lc, dict) else None
    return row, None


def cmd_answer(a):
    cfgs = load_configs(a.configs)
    sha = sha256_file(a.configs)
    info = arm_info(cfgs, a.arm)
    hid = is_hidden_set(a.set)
    if E.is_hidden(a.items) != hid or E.is_hidden(a.out) != hid:
        raise Refused(f"{a.set}: items and answers files must {'end in .enc' if hid else 'be open files'}")
    if a.stub and ROOT / "eval/hidden" in (Path(a.items).resolve(), *Path(a.items).resolve().parents):
        raise Refused("a stub run never uses the real hidden files (eval/hidden/)")
    if hid:
        if not a.ledger:
            raise Refused("an h6 answer needs --ledger (runs/m4/HIDDEN_LEDGER.md)")
        check_ledger(a.ledger, sha)
    items = E.read_items(a.items)
    if a.limit:
        if not a.stub:
            raise Refused("--limit is for stub runs only")
        items = items[:a.limit]
    elif not a.stub and len(items) != SET_COUNT[a.set]:
        raise Refused(f"{a.set}: {len(items)} items, {SET_COUNT[a.set]} expected")
    rows = [r for r in load_rows(a.out) if not r.get("error")]
    done = {r["id"] for r in rows}
    todo = [it for it in items if it["id"] not in done]
    tag = f"[answer {a.arm} {a.set}]"
    print(f"{tag} {len(items)} items, {len(done)} already done, {len(todo)} to do; model {info['model']}; ask_sha256 {info['ask_sha256'][:16]}", flush=True)
    env = dict(os.environ)
    failed = 0
    for i, it in enumerate(todo, 1):
        row = {"id": it["id"], "arm": a.arm, "set": a.set, "text": None, "tokens": None, "truncated": False, "seconds": None, "search_terms": None,
               "pack": None, "error": None, "ask_sha256": info["ask_sha256"]}
        res, err = None, None
        for attempt in range(2):
            res, err = run_ask(info, E.full_question(it), a.python, a.stub, env)
            if res:
                break
        if res:
            row.update(res)
            row["words"] = words_of(res["text"])
        else:
            row["error"] = err
            failed += 1
        rows.append(row)
        save_rows(a.out, rows)
        n = len(done) + i
        if hid:     # a counter only: no id, no text, no timing of an item
            print(f"{tag} {n}/{len(items)} {'ok' if res else 'failed'}", flush=True)
        else:
            s = secs_of(row)
            print(f"{tag} {n}/{len(items)} {it['id']} {'ok' if res else 'failed: ' + str(err)}" + (f" total={s:.1f}s tokens={row['tokens']} cut={'yes' if row['truncated'] else 'no'}" if res else ""), flush=True)
    if failed:
        print(f"{tag} {failed} item(s) failed after two tries; a rerun retries them", flush=True)
        return 1
    print(f"{tag} complete: {len(items)} answers", flush=True)
    return 0


# ---------------------------------------------------------------- packs

def chunk_key(c):
    return (c.get("source"), c.get("title"), c.get("path"))


def keys_resolver(chunks_path):
    """Dry runs only: a source maps to a chunk by (source, title, path); an ambiguous key is refused."""
    by = {}
    for c in (json.loads(l) for l in Path(chunks_path).read_text(encoding="utf-8").splitlines() if l.strip()):
        by.setdefault(chunk_key(c), []).append(c["id"])

    def resolve(question, expansion, sources):
        ids = []
        for s in sources:
            c = by.get(chunk_key(s)) or []
            if len(c) != 1:
                raise Refused("a source maps to no chunk or to several (keys resolver)")
            ids.append(c[0])
        return ids
    return resolve


def retrieval_resolver(chunks_path):
    """The real pack: the frozen retrieval on the question and the returned expansion, as `ask` does it (CPU, the reranker). Must equal the sources position by position."""
    from kafka_sre_rag import retrieval

    def resolve(question, expansion, sources):
        got = retrieval.select(question, expansion, k="k6", index_path=chunks_path) if chunks_path else retrieval.select(question, expansion, k="k6")
        if len(got) != len(sources):
            raise Refused("the recomputed pack has another size than the answer's sources")
        for g, s in zip(got, sources):
            if chunk_key(g) != chunk_key(s):
                raise Refused("the recomputed pack differs from the answer's sources")
            if g.get("ce_score") is not None and s.get("ce_score") is not None and abs(float(g["ce_score"]) - float(s["ce_score"])) > 1e-3:
                raise Refused("the recomputed pack's scores differ from the answer's")
        return [g["id"] for g in got]
    return resolve


def cmd_packs(a):
    hid = a.set == "h6"
    items = {it["id"]: it for it in E.read_items(a.items)}
    resolve = keys_resolver(a.chunks) if a.resolver == "keys" else retrieval_resolver(a.chunks if a.chunks_override else None)
    if a.resolver == "keys" and not a.stub:
        raise Refused("the keys resolver is for stub runs only")
    tag = f"[packs {a.arm} {a.set}]"
    rows = load_rows(a.answers)
    todo = [r for r in rows if not r.get("error") and not r.get("pack")]
    print(f"{tag} {len(rows)} rows, {len(todo)} to resolve", flush=True)
    bad = 0
    for i, r in enumerate(todo, 1):
        try:
            r["pack"] = resolve(E.full_question(items[r["id"]]), r.get("search_terms") or "", r.get("sources") or [])
            r["pack_check"] = "exact"
        except Refused as e:
            r["error"] = "pack not recovered"
            r["error_detail"] = None if hid else str(e)
            bad += 1
        save_rows(a.answers, rows)
        print(f"{tag} {i}/{len(todo)} {'ok' if not r.get('error') else 'failed'}" + ("" if hid else f" {r['id']}"), flush=True)
    if bad:
        print(f"{tag} {bad} row(s) failed: their packs were not recovered exactly; the rows are marked as errors", flush=True)
        return 1
    return 0


# ---------------------------------------------------------------- judge wrapper

SAFE_JUDGE = re.compile(r"^(matched |instruction |items |settings |\d+ judgments|judge totals|wrote |judge time|  server busy|  retry)")
JUDGE_LINE = re.compile(r"^\[judge\] (\d+) cached\s+\S+ \S+ p(\d) valid=(\w+)")


def cmd_judge_run(a):
    hid = a.hidden
    cmd = [a.python, str(ROOT / "scripts/eval2_judge.py"), "run", *a.rest]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, cwd=ROOT)
    withheld = 0
    for line in p.stdout:
        line = line.rstrip("\n")
        if not hid:
            print(line, flush=True)
            continue
        m = JUDGE_LINE.match(line)
        if m:
            print(f"[judge] {m.group(1)} calls done, pass {m.group(2)}, valid={m.group(3)}", flush=True)
        elif SAFE_JUDGE.match(line):
            print(line, flush=True)
        else:
            withheld += 1
    rc = p.wait()
    if hid and withheld:
        print(f"[judge] {withheld} output line(s) withheld (hidden run)", flush=True)
    return rc


def cmd_judge_check(a):
    rows = E.read_jsonl(a.out) if Path(a.out).exists() else []
    valid = sum(1 for r in rows if r.get("valid_reply"))
    print(f"judge rows: {len(rows)} (expected {a.expect}), valid {valid}, unscored {len(rows) - valid}")
    return 0 if len(rows) == a.expect and valid == len(rows) else 1


# ---------------------------------------------------------------- server

def server_base():
    u = (os.environ.get("OMLX_URL") or "").rstrip("/")
    if not u:
        raise Refused("no server address (OMLX_URL)")
    return u[:-3] if u.endswith("/v1") else u


def http(method, path, timeout=30):
    import urllib.request
    req = urllib.request.Request(server_base() + path, method=method, headers={"Authorization": "Bearer " + os.environ.get("OMLX_API_KEY", "")})
    with urllib.request.urlopen(req, timeout=timeout) as r:   # noqa: S310 (the Owner's own server)
        return r.status, r.read().decode("utf-8", "replace")


def kb2(*args):
    p = subprocess.run([sys.executable, str(ROOT / "scripts/m7_kb2_rule.py"), *args], capture_output=True, text=True)
    return p.returncode, p.stdout, p.stderr


def get_status(a):
    if a.stub_file:
        rc, out, _ = kb2("stub-status", "--file", a.stub_file)
        return json.loads(out)
    try:
        st, body = http("GET", "/api/status")
        return json.loads(body)
    except Exception as e:  # noqa: BLE001
        raise Refused(f"the server status is unreadable ({type(e).__name__})") from None


def cmd_server(a):
    if a.action == "status":
        st = get_status(a)
        print(json.dumps({k: st.get(k) for k in ("active_requests", "waiting_requests", "loaded_models")}))
        return 0
    if a.action == "gate":
        st = get_status(a)
        tmp = Path(a.work) / "server_status.json"
        tmp.write_text(json.dumps(st), encoding="utf-8")
        args = ["server-gate", "--status", str(tmp), "--need", a.need, "--w16", a.w16 or DS_MODEL]
        for m in a.marker:
            args += ["--marker", m]
        rc, out, err = kb2(*args)
        sys.stdout.write(out)
        sys.stderr.write(err)
        return rc
    if a.action == "unload":
        n = 0
        while True:       # no active or waiting request, then the unload (never for another client's model: the gate decided which ids come here)
            st = get_status(a)
            if int(st.get("active_requests", 1)) + int(st.get("waiting_requests", 0)) == 0:
                break
            n += 1
            if n > 120:
                raise Refused("the server is still busy after 60 minutes; nothing was unloaded")
            time.sleep(0 if a.stub_file else 30)
        if a.stub_file:
            kb2("stub-unload", "--file", a.stub_file, "--model", a.model)
            print(f"unloaded {a.model} (stub server)")
            return 0
        code, body = http("POST", f"/admin/api/models/{a.model}/unload", timeout=300)
        if not 200 <= code < 300:
            raise Refused(f"the server answered {code} to the unload of {a.model}")
        w = 0
        while a.model in (get_status(a).get("loaded_models") or []):
            time.sleep(5)
            w += 5
            if w > 600:
                raise Refused(f"{a.model} is still loaded 600 s after the unload was accepted")
        print(f"unloaded {a.model}; the server answered {code}; gone after {w} s")
        return 0
    if a.action == "load":     # the stub server loads a model on the first request
        if a.stub_file:
            kb2("stub-load", "--file", a.stub_file, "--model", a.model)
        return 0
    return 1


# ---------------------------------------------------------------- blind reading bundles

def leak_terms(cfgs, arms):
    t = set()
    for x in arms:
        c = cfgs[x]
        m = c.get("model") or {}
        for k in ("oMLX_model_id", "hf_repo", "base_model"):
            if m.get(k):
                t.add(m[k])
    return sorted(t)


def leak_check(dirs, terms):
    hits = 0
    for d in dirs:
        for f in Path(d).rglob("*"):
            if f.is_file():
                txt = f.read_text(encoding="utf-8", errors="replace").lower()
                hits += sum(1 for t in terms if t.lower() in txt)
    return hits


def run_py(args, hidden=False, quiet=False):
    p = subprocess.run([sys.executable, *args], capture_output=True, text=True, cwd=ROOT)
    if not (hidden or quiet):
        sys.stdout.write(p.stdout)
        sys.stderr.write(p.stderr)
    elif p.returncode != 0:      # hidden: the helper scripts' own messages name counts and classes only, but nothing is forwarded but the exit code
        print(f"(a sealed helper step failed with exit {p.returncode}; its output is withheld)", file=sys.stderr)
    return p


def cmd_bundle(a):
    cfgs = load_configs(a.configs)
    arms = [x for x in a.arms.split(",") if x]
    sets = {x: p for x, p in (s.partition("=")[::2] for s in a.answers)}
    if a.set == "d7":
        out = Path(a.out)
        first, second, export = out / "first", out / "second", out / "export_second_reader"
        for d in (first, second, export):
            if d.exists() and any(d.iterdir()):
                raise Refused(f"{d.name} exists and is not empty; not overwriting")
        key = out / "KEY.json"
        p = run_py([str(ROOT / "scripts/eval2_blind.py"), "bundle", "--items", a.items, "--rubrics", a.rubrics, "--answers", *a.answers, "--chunks", a.chunks,
                    "--out", str(first), "--key", str(key), "--seed", str(SEEDS["bundle"]), "--second-reader", str(a.second_fraction), "--second-out", str(second),
                    "--harm-consequences"], quiet=True)
        sys.stdout.write("\n".join(l for l in p.stdout.splitlines() if l.startswith(("bundle", "second bundle"))) + "\n")
        if p.returncode:
            sys.stderr.write(p.stderr)
            return 1
        shutil_copytree(second, export)
        terms = leak_terms(cfgs, arms)
        n = leak_check([first, second, export], terms)
        if n:
            print(f"REFUSED: {n} bundle file(s) name a model; remove the bundles before reading", file=sys.stderr)
            return 2
        print(f"d7 bundles: first reader {first}, second family's export (D7 only; no key, no answers file) {export}; seed {SEEDS['bundle']}; key {key} (no reader opens it)")
        return 0
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    p = run_py([str(ROOT / "scripts/eval2_sealed_reading.py"), "bundle", "--items", a.items, "--rubrics", a.rubrics, "--answers", *a.answers, "--chunks", a.chunks,
                "--out-dir", str(out), "--seed", str(SEEDS["bundle"]), "--first-sessions", str(a.sessions), "--second-sessions", "0"], hidden=True)
    if p.returncode:
        return 1
    n_b = len(list(out.glob("first_*.jsonl.enc")))
    print(f"h6 sealed bundles: {n_b} first-reader bundle(s), no export, no second family; seed {SEEDS['bundle']}")
    return 0


def shutil_copytree(src, dst):
    import shutil
    shutil.copytree(src, dst, dirs_exist_ok=True)


# ---------------------------------------------------------------- unblinding

def cmd_unblind(a):
    if a.set == "d7":
        readers = [f"{n}={p}" for n, p in (s.partition("=")[::2] for s in a.findings)]
        args = [str(ROOT / "scripts/eval2_blind.py"), "unblind", "--key", a.key, "--rubrics", a.rubrics, "--answers", *a.answers, "--findings", *readers,
                "--out", a.out, "--reads-out", a.reads_out, "--require-consequence"]
        if a.scope:
            args += ["--scope", *a.scope]
        p = run_py(args)
        return p.returncode
    # sealed: the findings of the first reader, validated as the sealed totals do; one encrypted row per finding with the arm and the scenario
    import eval2_sealed_reading as SR
    d = Path(a.dir)
    key = json.loads(E.decrypt_bytes((d / SR.KEY_NAME).read_bytes()).decode("utf-8"))
    text_of = {}
    for b in key["bundles"]:
        for r in SR.read_rows(d / f"{b}.jsonl.enc"):
            text_of[r["id"]] = r["answer"]
    out, rejected, seen = [], 0, set()
    for b, names in key["bundles"].items():
        f = d / f"findings_{b}.jsonl.enc"
        if not f.exists():
            print(f"no findings file for {len(key['bundles'])} bundle(s) expected", file=sys.stderr)
            return 1
        covered = set()
        for r in SR.read_rows(f):
            if r.get("answer") in names:
                covered.add(r["answer"])
            if r.get("kind") == "clean":
                continue
            why = SR.validate(r, set(names), key, text_of)
            sig = (r.get("answer"), r.get("kind"), SR.norm(r.get("quote")), r.get("point") or "")
            if why is None and sig in seen:
                why = "duplicate"
            seen.add(sig)
            if why:
                rejected += 1
                continue
            out.append({"set": key["aid"][r["answer"]], "id": r["id"], "reader": a.reader, "kind": r["kind"], "quote": r.get("quote") or "", "minor": bool(r.get("minor")),
                        "note": r.get("note") or "", "consequence": r.get("consequence") or "", "source": r.get("source") or ""})
        if len(set(names) - covered):
            print(f"{len(set(names) - covered)} answer(s) of a bundle have neither a finding nor a clean row", file=sys.stderr)
            return 1
    E.write_jsonl(a.out, out)
    print(f"h6: {len(out)} findings valid, {rejected} rejected (counts only); written encrypted")
    return 0


# ---------------------------------------------------------------- the adjudication pack's input

def cmd_pack_input(a):
    cfgs_arms = [x for x in a.arms.split(",") if x]
    for x in cfgs_arms:
        if x not in ("R", "C"):
            raise Refused(f"arm {x}: scripts/m7_adjudication_pack.py takes the configurations R and C only (its CONFIGS); an R variant needs that tuple widened (a decision, not made here)")
    hid = a.set == "h6"
    items = {it["id"]: it for it in E.read_items(a.items)}
    answers, flags = [], []
    flag_ids = None
    for arm, path in (s.partition("=")[::2] for s in a.answers):
        for r in load_rows(path):
            if r.get("error"):
                continue
            answers.append({"answer_id": f"{a.set}-{arm}-{r['id']}", "set": a.set, "scenario_id": r["id"], "config": arm, "text": r["text"], "pack_chunk_ids": r.get("pack") or []})
    ids = {x["answer_id"] for x in answers}
    for f in E.read_jsonl(a.findings):
        aid = f"{a.set}-{f['set']}-{f['id']}"
        if aid not in ids or f["kind"] not in ("harmful", "wrong", "invented"):
            continue
        if f["kind"] == "harmful":
            kind = "harm"
        elif f.get("minor"):
            continue
        else:
            kind = "material_error"
        flags.append({"answer_id": aid, "reader": f["reader"], "kind": kind, "quote": f["quote"], "rationale": f.get("note") or "",
                      "plausible_consequence": f.get("consequence") or "", "source_chunk_ids": []})
    scen = [{"scenario_id": i, "question": it["question"], "evidence": it.get("evidence") or ""} for i, it in items.items()]
    outd = Path(a.out_dir)
    outd.mkdir(parents=True, exist_ok=True)
    ext = ".enc" if hid else ""
    for name, rows in (("answers", answers), ("scenarios", scen), ("flags", flags)):
        E.write_jsonl(outd / f"{name}.jsonl{ext}", rows)
    n_h = sum(1 for f in flags if f["kind"] == "harm")
    print(f"pack input for {a.set}: {len(answers)} answers, {len(scen)} scenarios, {n_h} harm flags, {len(flags) - n_h} material-error flags; seeds shuffle {SEEDS['pack_shuffle']} sample {SEEDS['pack_sample']}")
    if hid:
        print("h6: the files are encrypted; scripts/m7_adjudication_pack.py reads plain files only, so building the sealed pack needs its .enc-aware read (a decision, see the build report)")
    else:
        print(f"build: scripts/m7_adjudication_pack.py build --answers {outd}/answers.jsonl --scenarios {outd}/scenarios.jsonl --flags {outd}/flags.jsonl --out DIR "
              f"--shuffle-seed {SEEDS['pack_shuffle']} --sample-seed {SEEDS['pack_sample']}")
    return 0


# ---------------------------------------------------------------- measures

def adjudication_of(verdicts_path, key_path, set_name):
    """-> (totals {config: {...}}, upheld answer ids {config: set}) for one set, from the Owner's verdicts and the pack's key; None when absent."""
    if not (verdicts_path and key_path and Path(verdicts_path).exists() and Path(key_path).exists()):
        return None, {}
    v = json.loads(Path(verdicts_path).read_text(encoding="utf-8"))
    key = json.loads(Path(key_path).read_text(encoding="utf-8"))
    verdicts = v.get("verdicts", {})
    tot, upheld = {}, {}
    for k, n in key["denominators"].items():
        s, _, c = k.partition("|")
        if s == set_name:
            tot[c] = {"answers": n, "H": 0, "pending": 0, "samples_drawn": 0, "samples_read": 0, "sample_harm": 0}
            upheld[c] = set()
    for pid, it in key["items"].items():
        if it["set"] != set_name:
            continue
        t, vd = tot[it["config"]], verdicts.get(pid, {})
        if it["kind"] == "flag":
            s2 = vd.get("stage2")
            if not s2:
                t["pending"] += 1
            elif s2["harmful"]:
                upheld[it["config"]].add(it["answer_id"])
        else:
            t["samples_drawn"] += 1
            s1 = vd.get("stage1")
            if not s1:
                t["pending"] += 1
            else:
                t["samples_read"] += 1
                t["sample_harm"] += 1 if s1["harmful"] else 0
    for c in tot:
        tot[c]["H"] = len(upheld[c])
        tot[c]["upheld_ids"] = sorted(upheld[c])
    return tot, upheld


def cmd_measures(a):
    import eval2_judge as EJ
    arms = [x for x in a.arms.split(",") if x]
    items = E.read_items(a.items)
    ids = [it["id"] for it in items]
    rub = {r["id"]: r for r in E.read_jsonl(a.rubrics)}
    judge = [r for r in E.read_jsonl(a.judge) if r.get("valid_reply")] if a.judge and Path(a.judge).exists() else []
    findings = E.read_jsonl(a.findings) if a.findings and Path(a.findings).exists() else None
    adj, _ = adjudication_of(a.verdicts, a.pack_key, a.set)
    out = {"set": a.set, "arms": {}, "adjudication": None, "reading_complete": findings is not None, "first_reader": a.first_reader}
    for arm, path in (s.partition("=")[::2] for s in a.answers):
        if arm not in arms:
            continue
        ans = {r["id"]: r for r in load_rows(path)}
        recs = []
        for iid in ids:
            r = ans.get(iid)
            rec = {"id": iid, "error": None, "words": 0, "seconds": None, "cut": False, "P": None, "essential_missed": None, "points_absent": None,
                   "wrong": None, "material": None, "harm_flag": None, "harm_upheld": None}
            if r is None or r.get("error") or not (r.get("text") or "").strip():
                rec["error"] = (r or {}).get("error") or "missing"
            else:
                rec.update(words=words_of(r["text"]), seconds=secs_of(r), cut=bool(r.get("truncated")))
                js = [x for x in judge if x["set"] == arm and x["id"] == iid]
                if js:
                    cs = [EJ.coverage_row(x, rub[iid]) for x in js]
                    rec["P"] = sum(c["P"] for c in cs) / len(cs)
                    rec["essential_missed"] = sum(sum(1 for u in c["units"] if u["status"] == "missed") for c in cs) / len(cs)
                    rec["points_absent"] = sum(sum(1 for e in x["must"].values() if e["verdict"] == "absent") for x in js) / len(js)
                    rec["passes_valid"] = len(js)
                if findings is not None:
                    fs = [f for f in findings if f["set"] == arm and f["id"] == iid]
                    rec["wrong"] = sum(1 for f in fs if f["reader"] == a.first_reader and f["kind"] in ("wrong", "invented") and not f.get("minor"))
                    rec["material"] = any(f["kind"] in ("wrong", "invented") and not f.get("minor") for f in fs)
                    rec["harm_flag"] = any(f["kind"] == "harmful" for f in fs)
                if adj is not None and arm in adj:
                    rec["harm_upheld"] = (f"{a.set}-{arm}-{iid}" in set(adj[arm]["upheld_ids"])) if (rec["harm_flag"] or findings is None) else False
            recs.append(rec)
        out["arms"][arm] = {"records": recs}
    if adj is not None:
        out["adjudication"] = {c: {k: v for k, v in t.items() if k != "upheld_ids"} for c, t in adj.items()}
    if a.tag_file and Path(a.tag_file).exists():
        tags = json.loads(Path(a.tag_file).read_text(encoding="utf-8")).get("tags", {})
        if a.set == "h6" and H6_TAG in tags:
            out["tag_exclude"] = {H6_TAG: [i for i in tags[H6_TAG]["ids"] if i in set(ids)]}
    blob = (json.dumps(out, indent=1) + "\n").encode()
    E.write_atomic(a.out, E.encrypt_bytes(blob) if E.is_hidden(a.out) else blob)
    n = {k: len(v["records"]) for k, v in out["arms"].items()}
    print(f"measures {a.set}: {n} records; judged {sum(1 for v in out['arms'].values() for r in v['records'] if r['P'] is not None)}; reading {'in' if findings is not None else 'pending'}; "
          f"adjudication {'in' if adj is not None else 'pending'}")
    return 0


# ---------------------------------------------------------------- stub ask (dry runs)

def cmd_stub_ask(a, rest):
    """Stands in for `python -m kafka_sre_rag ask <flags> --json QUESTION`: the same arguments, a deterministic JSON line. M7_STUB_CHUNKS names the chunk file;
    M7_STUB_FAIL names a substring of a question whose call fails (exit 1, and the question is echoed to stderr, as a bad runtime might)."""
    flags, q = rest[:-1], rest[-1]
    fail = os.environ.get("M7_STUB_FAIL")
    if fail and fail in q:
        print("stub failure for: " + q, file=sys.stderr)
        return 1
    model = flags[flags.index("--model") + 1] if "--model" in flags else "stub"
    chunks_path = os.environ.get("M7_STUB_CHUNKS") or str(ROOT / "work/v3/chunks_s1.jsonl")
    chunks = [json.loads(l) for l in Path(chunks_path).read_text(encoding="utf-8").splitlines() if l.strip()]
    seen, srcs = set(), []
    h = int(hashlib.sha256((model + q).encode()).hexdigest(), 16)
    start = int(hashlib.sha256(q.encode()).hexdigest(), 16) % len(chunks)
    for j in range(min(6, len(chunks))):
        c = chunks[(start + j) % len(chunks)]
        if chunk_key(c) in seen:
            continue
        seen.add(chunk_key(c))
        srcs.append({"source": c["source"], "title": c["title"], "path": c["path"], "bm25_rank": j + 1, "ce_score": round(1.0 - j * 0.1, 3)})
    text = f"Stub answer {h % 1000} to: {q[:40]}. " + " ".join(f"word{k}" for k in range(20 + h % 50))
    out = {"question": q, "text": text, "sources": srcs, "expansion": "stub terms", "timings": {"expand_s": 0.1, "retrieve_s": 0.2, "answer_s": 1.0 + (h % 7)},
           "measured": False, "tokens": 40 + h % 30, "truncated": False, "unmeasured_reasons": ["stub"]}
    if "--lookup-cards" in flags:
        out["lookup_cards"] = {"cards_found": 1, "chars": 100, "seconds": 0.1, "catalogue_sha256": "stub"}
    print(json.dumps(out, ensure_ascii=False))
    return 0


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] == "stub-ask":
        return cmd_stub_ask(None, argv[1:])
    if argv and argv[0] == "judge-run":       # judge-run [--hidden] [--python PY] -- ARGS OF eval2_judge.py run
        if "--" not in argv:
            print("usage: judge-run [--hidden] [--python PY] -- ARGS", file=sys.stderr)
            return 2
        i = argv.index("--")
        ns = argparse.Namespace(hidden="--hidden" in argv[1:i], python=(argv[argv.index("--python") + 1] if "--python" in argv[1:i] else sys.executable), rest=argv[i + 1:])
        return cmd_judge_run(ns)
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)

    def configs(p):
        p.add_argument("--configs", default=str(ROOT / "eval/m7/release_test/configs.json"))
        p.add_argument("--arms", default="R,C")

    p = sub.add_parser("arms"); configs(p)
    p = sub.add_parser("manifest"); configs(p)
    p.add_argument("--work", required=True)
    p.add_argument("--sets", default="d7,h6")
    p.add_argument("--candidate", default="R")
    p.add_argument("--control", default="C")
    p.add_argument("--pin", action="append", default=[])
    p.add_argument("--opt", action="append", default=[])
    p = sub.add_parser("ledger-check"); configs(p)
    p.add_argument("--ledger", required=True)
    p.add_argument("--configs-sha256")
    p = sub.add_parser("answer"); configs(p)
    p.add_argument("--arm", required=True)
    p.add_argument("--set", required=True, choices=sorted(SET_COUNT))
    p.add_argument("--items", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--ledger")
    p.add_argument("--python")
    p.add_argument("--stub", action="store_true")
    p.add_argument("--limit", type=int, default=0)
    p = sub.add_parser("check-inputs")
    p.add_argument("--set", required=True, choices=sorted(SET_COUNT))
    p.add_argument("--items", required=True)
    p.add_argument("--rubrics", required=True)
    p.add_argument("--stub", action="store_true")
    p.add_argument("--limit", type=int, default=0)
    p = sub.add_parser("packs")
    p.add_argument("--set", required=True)
    p.add_argument("--arm", required=True)
    p.add_argument("--items", required=True)
    p.add_argument("--answers", required=True)
    p.add_argument("--chunks", default="work/v3/chunks_s1.jsonl")
    p.add_argument("--chunks-override", action="store_true")
    p.add_argument("--resolver", choices=["retrieval", "keys"], default="retrieval")
    p.add_argument("--stub", action="store_true")
    p = sub.add_parser("judge-check")
    p.add_argument("--out", required=True)
    p.add_argument("--expect", type=int, required=True)
    p = sub.add_parser("server")
    p.add_argument("action", choices=["gate", "unload", "status", "load"])
    p.add_argument("--work", default=".")
    p.add_argument("--need", default="")
    p.add_argument("--model", default="")
    p.add_argument("--marker", action="append", default=[])
    p.add_argument("--w16", default=None)
    p.add_argument("--stub-file", default=None)
    p = sub.add_parser("bundle"); configs(p)
    p.add_argument("--set", required=True, choices=sorted(SET_COUNT))
    p.add_argument("--items", required=True)
    p.add_argument("--rubrics", required=True)
    p.add_argument("--answers", nargs="+", required=True, metavar="ARM=PATH")
    p.add_argument("--chunks", default="work/v3/chunks_s1.jsonl")
    p.add_argument("--out", required=True)
    p.add_argument("--second-fraction", type=float, default=1.0)
    p.add_argument("--sessions", type=int, default=6)
    p = sub.add_parser("unblind")
    p.add_argument("--set", required=True, choices=sorted(SET_COUNT))
    p.add_argument("--key")
    p.add_argument("--rubrics")
    p.add_argument("--answers", nargs="*", default=[])
    p.add_argument("--findings", nargs="*", default=[], metavar="READER=PATH")
    p.add_argument("--scope", nargs="*")
    p.add_argument("--out", required=True)
    p.add_argument("--reads-out")
    p.add_argument("--dir")
    p.add_argument("--reader", default="claude")
    p = sub.add_parser("pack-input"); configs(p)
    p.add_argument("--set", required=True, choices=sorted(SET_COUNT))
    p.add_argument("--items", required=True)
    p.add_argument("--answers", nargs="+", required=True, metavar="ARM=PATH")
    p.add_argument("--findings", required=True)
    p.add_argument("--out-dir", required=True)
    p = sub.add_parser("measures")
    p.add_argument("--set", required=True, choices=sorted(SET_COUNT))
    p.add_argument("--arms", default="R,C")
    p.add_argument("--items", required=True)
    p.add_argument("--rubrics", required=True)
    p.add_argument("--answers", nargs="+", required=True, metavar="ARM=PATH")
    p.add_argument("--judge")
    p.add_argument("--findings")
    p.add_argument("--first-reader", default="claude")
    p.add_argument("--verdicts")
    p.add_argument("--pack-key")
    p.add_argument("--tag-file", default=str(ROOT / "eval/m4/HIDDEN_TAGS.json"))
    p.add_argument("--out", required=True)
    a = ap.parse_args(argv)
    try:
        return {"arms": cmd_arms, "manifest": cmd_manifest, "ledger-check": cmd_ledger_check, "answer": cmd_answer, "packs": cmd_packs, "judge-run": cmd_judge_run,
                "server": cmd_server, "bundle": cmd_bundle, "unblind": cmd_unblind, "pack-input": cmd_pack_input, "measures": cmd_measures, "check-inputs": cmd_check_inputs, "judge-check": cmd_judge_check}[a.cmd](a)
    except Refused as e:
        print(f"REFUSED: {e}", file=sys.stderr)
        return 2
    except E.EvalError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
