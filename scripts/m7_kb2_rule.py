#!/usr/bin/env python3
"""M7 KB phase 2, I3: the helpers of scripts/m7_kb2.sh (the checks and files the chain makes, as commands). CPU only.

  manifest | start-check | mark      the run's state (pinned files, the KB prompt texts' hash, options), its markers
  server-gate                        the server before a model stage (W2, narrowed by W16 for the writer's DeepSeek build)
  stub-status | stub-load | stub-unload   the stub server's status in a dry run (a JSON file the chain reads instead of the server)
  sanity-rule                        the stop rule of the sanity run (kb2_design.md section 7), its mechanical parts
  reading-sample                     the sanity reading's sample: 40 accepted and 20 rejected entries, blind bundle and key
  full-gate                          the full run's preconditions: the sanity run passed its rule (same prompts and models) and its reading
  index                              the accepted entries (checker "ok"), duplicates dropped, sorted by id, rendered
  audit-sample                       the audit's sample (section 4): 200 accepted, the cross entries up to 40 more, 50 rejected
  rule-text                          the sanity rule and the audit bar, as written into the report header

Exit codes: 0 done; 1 a check failed; 2 refused or an input is missing.
"""
import argparse
import hashlib
import json
import os
import statistics
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
from kafka_sre_rag import kb_entry as K, prompts as P  # noqa: E402

SCHEMA = 1
W16_MODEL = "DeepSeek-V4.1-Flash-1017-oQ4e-mtp"      # the one foreign model the chain may unload (W16)
KB_PROMPT_NAMES = ("KB_ENTRY_PROMPT_VERSION", "KB_CHECK_PROMPT_VERSION", "KB_WRITER_SYSTEM", "KB_WRITER_USER", "KB_CHECK_SYSTEM",
                   "KB_CHECK_USER", "KB_ENTRY_RENDER")
# the sanity rule (section 7): stop and rework the prompt when any holds
SAN_VALID_MIN = 0.95          # share of windows with a valid reply
SAN_GATE_FAIL_MAX = 0.15      # share of written entries that fail the gate
SAN_MEDIAN = (1, 8)           # median entries written per window
SAN_ACCEPT = (0.50, 0.98)     # share of gated entries the checker accepts
SAN_READ_NOTOK_MAX = 4        # of the 40 accepted entries read
SAN_PROJ_MAX_H = 16.0         # projected full writer time
SAN_READ_ACC, SAN_READ_REJ, SAN_READ_SALT = 40, 20, "kb2-sanity-read:"
AUDIT_N, AUDIT_CROSS_MAX, AUDIT_REJ, AUDIT_SALT, AUDIT_BAR = 200, 40, 50, "kb2-audit:", 190
FULL_WINDOWS = 1487


def sha256_file(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def sha256_text(s):
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def kb_prompts_sha256():
    """The hash of the KB prompt constants and of the two functions that fill them (prompts.py itself is also pinned by the chain)."""
    import inspect
    texts = {n: getattr(P, n) for n in KB_PROMPT_NAMES}
    texts.update({f.__name__: inspect.getsource(f) for f in (P.kb_writer_messages, P.kb_check_messages)})
    return sha256_text(json.dumps(texts, sort_keys=True, ensure_ascii=False))


def rows(p):
    p = Path(p)
    if not p.exists():
        return []
    with open(p, encoding="utf-8") as f:
        return [json.loads(l) for l in f if l.strip()]


def load_json(p):
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def kv(items):
    out = {}
    for it in items or []:
        k, _, v = it.partition("=")
        out[k] = v
    return out


# ---------------------------------------------------------------- manifest, start check, markers
def state(a):
    missing = [p for p in a.pin if not Path(p).is_file()]
    if missing:
        raise ValueError(f"pinned file missing: {missing[0]}")
    return {"schema": SCHEMA, "kind": "dry" if a.dry else "real", "kb_prompts_sha256": kb_prompts_sha256(),
            "entry_prompt_version": P.KB_ENTRY_PROMPT_VERSION, "check_prompt_version": P.KB_CHECK_PROMPT_VERSION,
            "options": kv(a.opt), "info": kv(a.info), "pins": {p: sha256_file(p) for p in sorted(a.pin)}}


def manifest_hash(m):
    return sha256_text(json.dumps({k: v for k, v in m.items() if k != "info"}, sort_keys=True))


def diff(old, new):
    out = []
    for k in sorted(set(old) | set(new)):
        if k == "info" or old.get(k) == new.get(k):
            continue
        if isinstance(old.get(k), dict) and isinstance(new.get(k), dict):
            out += [f"{k}.{kk}: stored {str(old[k].get(kk))[:16]}, now {str(new[k].get(kk))[:16]}"
                    for kk in sorted(set(old[k]) | set(new[k])) if old[k].get(kk) != new[k].get(kk)]
        else:
            out.append(f"{k}: stored {old.get(k)!r}, now {new.get(k)!r}")
    return out


def cmd_manifest(a):
    m = Path(a.work) / "MANIFEST.json"
    if m.exists():
        print(f"manifest exists: {m}")
        return 0
    m.write_text(json.dumps(state(a), indent=1, sort_keys=True) + "\n", encoding="utf-8")
    print(f"manifest written: {m}")
    return 0


def cmd_start_check(a):
    m = Path(a.work) / "MANIFEST.json"
    mk = sorted(Path(a.work).glob("stage_*.done"))
    if not m.exists():
        if mk:
            print(f"REFUSED: stage markers exist but there is no manifest {m}", file=sys.stderr)
            return 2
        print("start check: no manifest and no markers: a fresh start")
        return 0
    try:
        old, new = load_json(m), state(a)
    except (ValueError, OSError) as e:
        print(f"REFUSED: {e}", file=sys.stderr)
        return 2
    d = diff(old, new)
    if d:
        print("REFUSED: the stored manifest differs from the state now (nothing was moved or deleted):", file=sys.stderr)
        for x in d[:12]:
            print("  " + x, file=sys.stderr)
        return 2
    mh, bad = manifest_hash(old), []
    for f in mk:
        rec = load_json(f)
        if rec.get("manifest_sha256") != mh:
            bad.append(f"{f.name}: written for another manifest")
        for o, h in (rec.get("outputs") or {}).items():
            if not Path(o).is_file():
                bad.append(f"{f.name}: output {o} is missing")
            elif sha256_file(o) != h:
                bad.append(f"{f.name}: output {o} changed since the stage finished")
    if bad:
        print("REFUSED: stage markers do not match the manifest or their outputs (nothing was moved or deleted):", file=sys.stderr)
        for b in bad[:12]:
            print("  " + b, file=sys.stderr)
        return 2
    print(f"start check: manifest equal to the state now; {len(mk)} stage(s) done: {', '.join(f.stem[6:] for f in mk) or 'none'}")
    return 0


def cmd_mark(a):
    mh = manifest_hash(load_json(Path(a.work) / "MANIFEST.json"))
    rec = {"stage": a.stage, "manifest_sha256": mh, "outputs": {f: sha256_file(f) for f in a.out}}
    (Path(a.work) / f"stage_{a.stage}.done").write_text(json.dumps(rec, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    print(f"stage {a.stage} done ({len(a.out)} output files hashed)")
    return 0


# ---------------------------------------------------------------- the server gate (W2, W16)
def _epoch(v):
    """A load time as seconds since the epoch: a number, or an ISO 8601 string; None when it cannot be read (as m7_six_arm_rule.py)."""
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return float(v)
    try:
        import datetime
        return datetime.datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp()
    except (ValueError, TypeError):
        return None


def cmd_server_gate(a):
    """Before a model stage (and at the chain's gate). Ours (W2, K4b): a loaded model with a marker this chain wrote when it
    loaded it, AND a server-reported load time (`model_load_times`: {model: epoch seconds or ISO time}) not later than the
    marker's time. A marker whose load time the server does not report cannot be checked: refused (a leftover marker must never
    make the chain unload another client's model; ported from scripts/m7_six_arm_rule.py, made strict by the review of f45dd13).
    Printed decisions:
    UNLOAD <id>    a model of ours that this stage does not use
    ADOPT <id>     the writer's DeepSeek build loaded by another client while the writer needs it (--need is that model): used
                   as it is, without our marker; logged; left loaded after the writer (W16 as the conductor read it)
    W16 <id>       the writer's DeepSeek build loaded by another client while a stage needs another model: W16 allows its unload
                   (after no request is active or waiting; the chain waits for that and logs the unload as W16's)
    Refused (exit 2, the model named, nothing unloaded): any other model without a valid marker; a request active or waiting,
    except while a W16 unload is due (that unload waits until the server is idle)."""
    try:
        st = load_json(a.status)
        busy = int(st.get("active_requests", 1)) + int(st.get("waiting_requests", 0))
        loaded = list(st.get("loaded_models") or [])
    except (OSError, ValueError, TypeError):
        print("REFUSED: the server status is unreadable", file=sys.stderr)
        return 2
    times = st.get("model_load_times") or {}
    if not isinstance(times, dict):
        times = {}
    ours = set()
    for spec in a.marker or []:
        mid, _, path = spec.partition("=")
        if not mid or not path:
            print(f"REFUSED: --marker {spec!r}: expected ID=PATH", file=sys.stderr)
            return 2
        if not os.path.exists(path) or mid not in loaded:
            continue
        lt = _epoch(times.get(mid)) if mid in times else None
        if lt is None:
            print(f"REFUSED: the model {mid} is loaded and this chain has a marker for it ({path}), but the server reports no load time "
                  f"for it, so the marker's age cannot be checked; it may be another client's copy; nothing was unloaded (the Owner decides)",
                  file=sys.stderr)
            return 2
        if lt > os.path.getmtime(path) + 1:
            print(f"REFUSED: the model {mid} was loaded at {lt:.0f}, after this chain's marker {path} was written: another client loaded "
                  f"it; this chain never unloads a model it did not load; nothing was unloaded", file=sys.stderr)
            return 2
        ours.add(mid)
    foreign = [m for m in loaded if m not in ours]
    w16 = [m for m in foreign if m == a.w16]
    other = [m for m in foreign if m != a.w16]
    if other:
        print(f"REFUSED: the model {', '.join(other)} is loaded by another client (no valid marker of this chain); only {a.w16} may be "
              f"unloaded under W16; any other model waits for the Owner (W2); nothing was unloaded", file=sys.stderr)
        return 2
    adopt = [m for m in w16 if m == a.need]
    w16 = [m for m in w16 if m != a.need]
    if busy and not w16:
        print(f"REFUSED: the server has {busy} active or waiting request(s) (another client); nothing was started", file=sys.stderr)
        return 2
    for m in loaded:
        if m in ours and m != a.need:
            print(f"UNLOAD {m}")
    for m in w16:
        print(f"W16 {m}")
    for m in adopt:
        print(f"ADOPT {m}")
    if busy:
        print(f"note: {busy} active or waiting request(s); the W16 unload waits until the server is idle")
    return 0


def _stub(a):
    p = Path(a.file)
    return p, (load_json(p) if p.exists() else {"active_requests": 0, "waiting_requests": 0, "loaded_models": []})


def _stub_save(p, st):
    p.write_text(json.dumps(st, sort_keys=True) + "\n", encoding="utf-8")


def cmd_stub_status(a):
    """Print the stub server's status as /api/status would; a planted busy count falls by one per read."""
    p, st = _stub(a)
    if a.init:
        if p.exists():
            return 0
        import time
        lm = [m for m in (a.loaded or "").split(",") if m]
        st = {"active_requests": a.busy, "waiting_requests": 0, "loaded_models": lm, "model_load_times": {m: time.time() for m in lm}}
        _stub_save(p, st)
        return 0
    print(json.dumps(st))
    if st.get("active_requests"):
        st["active_requests"] -= 1
        _stub_save(p, st)
    return 0


def cmd_stub_load(a):
    """The stub server loads a model on the chain's first request: it reports the load time, as model_load_times."""
    import time
    p, st = _stub(a)
    if a.model not in st["loaded_models"]:
        st["loaded_models"].append(a.model)
        st.setdefault("model_load_times", {})[a.model] = time.time()
    _stub_save(p, st)
    return 0


def cmd_stub_unload(a):
    p, st = _stub(a)
    st["loaded_models"] = [m for m in st["loaded_models"] if m != a.model]
    (st.get("model_load_times") or {}).pop(a.model, None)
    _stub_save(p, st)
    print("202")
    return 0


# ---------------------------------------------------------------- the sanity rule and its reading
def sanity(write_dir, check_dir, concurrency, total=FULL_WINDOWS):
    w = rows(Path(write_dir) / "windows.jsonl")
    c = rows(Path(check_dir) / "checks.jsonl")
    ok = [r for r in w if r["status"] == "ok"]
    written = sum(r["written"] for r in ok)
    rej = sum(r["gate_rejects"] for r in ok)
    gated = written - rej
    acc = sum(1 for r in c if r["verdict"] == "ok")
    med = statistics.median([r["written"] for r in ok]) if ok else 0
    own = [r for r in w if not r.get("reused_from")]
    proj = (sum(r["seconds"] for r in own) / max(1, concurrency)) / max(1, len(own)) * total / 3600 if own else None
    items = [
        ("valid replies", f"{len(ok)} of {len(w)} windows", (len(ok) / len(w) if w else 0) >= SAN_VALID_MIN, f"at least {SAN_VALID_MIN:.0%}"),
        ("gate failures", f"{rej} of {written} written entries", (rej / written if written else 1) <= SAN_GATE_FAIL_MAX, f"at most {SAN_GATE_FAIL_MAX:.0%}"),
        ("median entries written per window", f"{med}", SAN_MEDIAN[0] <= med <= SAN_MEDIAN[1], f"{SAN_MEDIAN[0]} to {SAN_MEDIAN[1]}"),
        ("checker accepts", f"{acc} of {gated} gated entries", SAN_ACCEPT[0] <= (acc / gated if gated else 0) <= SAN_ACCEPT[1],
         f"{SAN_ACCEPT[0]:.0%} to {SAN_ACCEPT[1]:.0%}"),
        ("projected full writer time", "not computable" if proj is None else f"{proj:.1f} h (inferred: the windows' seconds / concurrency {concurrency}, x {total})",
         proj is not None and proj <= SAN_PROJ_MAX_H, f"at most {SAN_PROJ_MAX_H:.0f} h"),
    ]
    return {"items": [{"name": n, "value": v, "pass": bool(p), "bar": b} for n, v, p, b in items],
            "mechanical_pass": all(p for _, _, p, _ in items), "windows": len(w), "accepted": acc, "gated": gated,
            "reading": f"pending: at most {SAN_READ_NOTOK_MAX} of the {SAN_READ_ACC} accepted entries read may be rated not ok"}


def run_identity(writer, checker):
    """What a sanity result is valid for: the prompt versions, the KB prompt texts' hash, the writer and the checker."""
    return {"entry_prompt_version": P.KB_ENTRY_PROMPT_VERSION, "check_prompt_version": P.KB_CHECK_PROMPT_VERSION,
            "kb_prompts_sha256": kb_prompts_sha256(), "writer": writer, "checker": checker}


def cmd_sanity_rule(a):
    r = sanity(a.write_dir, a.check_dir, a.concurrency, a.total)
    r["identity"] = run_identity(a.writer, a.checker)
    for it in r["items"]:
        print(f"  {it['name']}: {it['value']} (bar {it['bar']}): {'pass' if it['pass'] else 'FAIL'}")
    print(f"SANITY (mechanical parts): {'PASS' if r['mechanical_pass'] else 'STOP: rework the prompt (new version, new sanity run)'}; "
          f"the reading: {r['reading']}")
    if a.out:
        Path(a.out).write_text(json.dumps(r, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    return 0 if r["mechanical_pass"] else 1


def _draw(ids, salt, n):
    return sorted(ids, key=lambda i: sha256_text(salt + i))[:n]


def _chunks(slice_path, record):
    import kb_write_entries as W
    sc, _, _ = W.load_slice_chunks(slice_path, record)
    return {c["id"]: c for c in sc}


def _bundle(entries, chunks, title, note):
    """A blind reading bundle: each entry with its title, statement, applies_when, quotes and its source chunks as the model
    receives them; no verdict and no checker reason."""
    out = [f"# {title}", "", note, ""]
    for n, e in enumerate(entries, 1):
        out += [f"## Item {n} (`{e['id']}`, kind {e['kind']})", "", f"**Title.** {e['title']}", f"**Statement.** {e['statement']}"]
        if e.get("applies_when"):
            out.append(f"**Applies when.** {e['applies_when']}")
        out.append("**Quotes.**")
        out += [f"- `{q['chunk']}`: \"{K.norm_ws(q['text'])}\"" for q in e["quotes"]]
        out += ["", "**Source chunks, as the model receives them.**", ""]
        for s in e["sources"]:
            out += [f"`{s['id']}` ({s['path']}):", "", "```", K.chunk_view(chunks[s["id"]]), "```", ""]
        out += ["Rating: ok / partial / wrong; one short reason.", ""]
    return "\n".join(out) + "\n"


def _write_sample(out_dir, name, entries, chunks, title, note, key):
    d = Path(out_dir)
    d.mkdir(parents=True, exist_ok=True)
    (d / f"{name}.md").write_text(_bundle(entries, chunks, title, note), encoding="utf-8")
    (d / f"{name}_key.json").write_text(json.dumps(key, indent=1, sort_keys=True) + "\n", encoding="utf-8")


def pick_reading(chk, salt, n_acc, n_rej):
    """The reading sample's ids (before the blind order): up to n_acc accepted entries, and up to n_rej not accepted ones, the checker's
    own rejections (partial, wrong, unsupported) first and unchecked entries only to fill the quota; each part drawn by sha256(salt + id)."""
    acc = [r["id"] for r in chk if r["verdict"] == "ok"]
    judged = [r["id"] for r in chk if r["verdict"] in ("partial", "wrong", "unsupported")]
    rest = [r["id"] for r in chk if r["verdict"] != "ok" and r["id"] not in set(judged)]
    rej = _draw(judged, salt, n_rej)
    rej += _draw(rest, salt, n_rej - len(rej))
    return _draw(acc, salt, n_acc) + rej


def cmd_reading_sample(a):
    ents = {e["id"]: e for e in rows(Path(a.write_dir) / "entries_raw.jsonl")}
    chk = rows(Path(a.check_dir) / "checks.jsonl")
    acc = [r["id"] for r in chk if r["verdict"] == "ok"]
    rej = [r["id"] for r in chk if r["verdict"] != "ok"]
    pick = pick_reading(chk, a.salt, a.accepted, a.rejected)
    order = sorted(pick, key=lambda i: sha256_text(a.salt + "order:" + i))
    chunks = _chunks(a.slice, a.record)
    verdict = {r["id"]: r["verdict"] for r in chk}
    _write_sample(a.out_dir, "sanity_reading", [ents[i] for i in order], chunks, "KB2 sanity reading (blind)",
                  f"{len(order)} entries: up to {a.accepted} the checker accepted and up to {a.rejected} it did not, in a drawn order; the "
                  "reader does not see which. Rate each under the checker's three conditions (every claim stated by the quotes; nothing "
                  "added or dropped; the quote means in its chunk what the statement says).",
                  {"order": order, "checker_verdict": {i: verdict[i] for i in order}, "salt": a.salt})
    print(f"reading sample: {sum(1 for i in pick if verdict[i] == 'ok')} accepted, {sum(1 for i in pick if verdict[i] != 'ok')} not accepted "
          f"({len(acc)} and {len(rej)} available) -> {a.out_dir}/sanity_reading.md")
    return 0


RATINGS = ("ok", "partial", "wrong")


def reading_count(result_path, key_path):
    """The sanity reading's result file: one JSON object per line {"id", "rating": ok|partial|wrong, "reason"}; its ids must equal
    the reading sample's (sanity_reading_key.json), each once. Returns (not-ok count among the entries the checker accepted, the
    number of those entries, the file's sha256). Raises ValueError with the reason."""
    key = load_json(key_path)
    want = list(key["order"])
    got = rows(result_path)
    ids = [r.get("id") for r in got]
    if len(ids) != len(set(ids)):
        raise ValueError("an id is rated twice")
    if set(ids) != set(want):
        miss, extra = sorted(set(want) - set(ids)), sorted(set(ids) - set(want))
        raise ValueError(f"the ids differ from the reading sample's ({len(miss)} missing, {len(extra)} not in the sample)")
    bad = [r["id"] for r in got if r.get("rating") not in RATINGS]
    if bad:
        raise ValueError(f"{len(bad)} row(s) without a rating of ok, partial or wrong (first {bad[0]})")
    acc = [i for i in want if key["checker_verdict"][i] == "ok"]
    rating = {r["id"]: r["rating"] for r in got}
    return sum(1 for i in acc if rating[i] != "ok"), len(acc), sha256_file(result_path)


def cmd_full_gate(a):
    """The full run's preconditions: the sanity result passed its rule, for the same prompt versions, KB prompt texts, writer and
    checker as this run and as the sanity folder's MANIFEST.json; the reading's result file covers exactly the reading sample and
    rates at most 4 of the accepted entries not ok."""
    try:
        r = load_json(a.sanity_result)
        sm = load_json(Path(a.sanity_dir) / "MANIFEST.json")
    except (OSError, ValueError):
        print(f"REFUSED: the sanity run's result or manifest is missing or unreadable in {a.sanity_dir} (run --sanity first)", file=sys.stderr)
        return 2
    if not r.get("mechanical_pass"):
        print("REFUSED: the sanity run failed its rule (mechanical parts); rework the prompt first", file=sys.stderr)
        return 2
    now = run_identity(a.writer, a.checker)
    stored = r.get("identity") or {}
    man = {"entry_prompt_version": sm.get("entry_prompt_version"), "check_prompt_version": sm.get("check_prompt_version"),
           "kb_prompts_sha256": sm.get("kb_prompts_sha256"), "writer": (sm.get("options") or {}).get("writer"),
           "checker": (sm.get("options") or {}).get("checker")}
    for name, other in (("the sanity result", stored), ("the sanity folder's manifest", man)):
        diff = [k for k in now if other.get(k) != now[k]]
        if diff:
            print(f"REFUSED: {name} was made for another {', '.join(diff)} than this run (sanity: "
                  + "; ".join(f"{k}={str(other.get(k))[:16]}" for k in diff) + "; now: " + "; ".join(f"{k}={str(now[k])[:16]}" for k in diff)
                  + "): a new sanity run first", file=sys.stderr)
            return 2
    if not a.reading:
        print("REFUSED: --sanity-reading FILE (the sanity reading's result) is required", file=sys.stderr)
        return 2
    try:
        n, of, sha = reading_count(a.reading, Path(a.sanity_dir) / "reading" / "sanity_reading_key.json")
    except (OSError, ValueError, KeyError) as e:
        print(f"REFUSED: the sanity reading's result {a.reading} is not usable: {e}", file=sys.stderr)
        return 2
    if n > SAN_READ_NOTOK_MAX:
        print(f"REFUSED: the sanity reading rated {n} of {of} accepted entries not ok (at most {SAN_READ_NOTOK_MAX}): rework the prompt", file=sys.stderr)
        return 2
    print(f"full gate: the sanity run passed its rule; the reading rated {n} of {of} accepted entries not ok; reading file sha256 {sha}")
    return 0


# ---------------------------------------------------------------- the accepted entries and the audit's sample
def cmd_index(a):
    """Accepted entries (checker ok) in slice order of their windows; an exact duplicate of the normalised statement of an
    earlier entry is dropped (the first kept); then sorted by id, each with its rendered text."""
    order = {r["id"]: n for n, r in enumerate(rows(a.slice))}
    ok = {r["id"] for r in rows(Path(a.check_dir) / "checks.jsonl") if r["verdict"] == "ok"}
    ents = [e for e in rows(Path(a.write_dir) / "entries_raw.jsonl") if e["id"] in ok]
    ents.sort(key=lambda e: (order[e["window"]["target"]], e["id"]))
    seen, keep, dup = set(), [], 0
    for e in ents:
        k = K.norm_statement(e["statement"])
        if k in seen:
            dup += 1
            continue
        seen.add(k)
        keep.append({**e, "rendered": K.render_text(e)})
    keep.sort(key=lambda e: e["id"])
    out = Path(a.out)
    tmp = Path(str(out) + ".tmp")
    tmp.write_text("".join(K.dumps(e) + "\n" for e in keep), encoding="utf-8")
    tmp.replace(out)
    kinds = {k: sum(1 for e in keep if e["kind"] == k) for k in K.KINDS}
    print(f"index: {len(keep)} accepted entries ({dup} exact duplicates dropped); kinds " + ", ".join(f"{k} {v}" for k, v in kinds.items())
          + f"; sha256 {sha256_file(out)}")
    return 0


def cmd_audit_sample(a):
    idx = rows(a.index)
    ids = [e["id"] for e in idx]
    main = _draw(ids, AUDIT_SALT, AUDIT_N)
    cross = [i for i in _draw([e["id"] for e in idx if e["kind"] == "cross"], AUDIT_SALT, len(idx)) if i not in set(main)][:AUDIT_CROSS_MAX]
    chk = rows(Path(a.check_dir) / "checks.jsonl")
    rej = _draw([r["id"] for r in chk if r["verdict"] != "ok"], AUDIT_SALT, AUDIT_REJ)
    ents = {e["id"]: e for e in rows(Path(a.write_dir) / "entries_raw.jsonl")}
    chunks = _chunks(a.slice, a.record)
    second = _draw(main, AUDIT_SALT + "second:", AUDIT_N // 4)
    note = (f"Audit of the accepted entries (kb2_design.md section 4). Bar: at least {AUDIT_BAR} of the {AUDIT_N} main items rated ok. "
            "Rate each under the checker's three conditions. No entry is repaired on the basis of this sample.")
    _write_sample(a.out_dir, "audit_main", [ents[i] for i in main], chunks, "KB2 audit: main sample", note, {"ids": main, "salt": AUDIT_SALT})
    _write_sample(a.out_dir, "audit_cross", [ents[i] for i in cross], chunks, "KB2 audit: cross entries (reported separately)", note, {"ids": cross})
    _write_sample(a.out_dir, "audit_rejected", [ents[i] for i in rej], chunks, "KB2 audit: rejected entries (cost, not bar)", note, {"ids": rej})
    _write_sample(a.out_dir, "audit_second_rater", [ents[i] for i in second], chunks, "KB2 audit: second rater (a quarter of the main sample)", note, {"ids": second})
    print(f"audit sample: {len(main)} main, {len(cross)} cross, {len(rej)} rejected, {len(second)} for the second rater -> {a.out_dir}")
    return 0


def cmd_rule_text(a):
    print("**The sanity rule (kb2_design.md section 7, fixed before any entry exists).** The 50 slice chunks with the lowest "
          "sha256(`kb2-sanity2:` + chunk id) (selection `sanity2`, drawn from the slice chunks that are not among the 50 of the earlier kbe/1 and kbe/2 runs, "
          "which used `kb2-sanity:`), their windows, writer then checker, then a reading of 40 accepted and 20 rejected entries. "
          "Stop and rework the prompt (new version, new sanity run) if any holds: fewer than 95 % of windows give a valid reply; more than "
          "15 % of written entries fail the mechanical gate; median entries per window outside 1 to 8; the checker accepts fewer than 50 % "
          "or more than 98 % of gated entries; more than 4 of the 40 accepted entries are rated not ok; a projected full writer time over "
          "16 h. A second failed sanity run stops the pilot until the conductor has seen it. The chain computes the mechanical parts; the "
          "reading's result file is given to the full run (`--sanity-reading FILE`, its ids those of the reading sample), which refuses more than 4 of the accepted entries rated not ok.")
    print()
    print(f"**The audit bar (section 4).** {AUDIT_N} accepted entries drawn by sha256(`{AUDIT_SALT}` + entry id), lowest first, plus every "
          f"accepted cross entry up to {AUDIT_CROSS_MAX} more (reported separately); at least {AUDIT_BAR} of {AUDIT_N} rated ok, or the entries "
          "do not enter and the pilot stops. The chain draws the sample; the rating is a reading session.")
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("manifest", "start-check"):
        p = sub.add_parser(name)
        p.add_argument("--work", required=True)
        p.add_argument("--pin", action="append", default=[])
        p.add_argument("--opt", action="append", default=[])
        p.add_argument("--info", action="append", default=[])
        p.add_argument("--dry", action="store_true")
    p = sub.add_parser("mark")
    p.add_argument("--work", required=True)
    p.add_argument("--stage", required=True)
    p.add_argument("--out", nargs="*", default=[])
    p = sub.add_parser("server-gate")
    p.add_argument("--status", required=True)
    p.add_argument("--need", required=True)
    p.add_argument("--marker", action="append", default=[])
    p.add_argument("--w16", default=W16_MODEL)
    p = sub.add_parser("stub-status")
    p.add_argument("--file", required=True)
    p.add_argument("--init", action="store_true")
    p.add_argument("--loaded", default="")
    p.add_argument("--busy", type=int, default=0)
    for name in ("stub-load", "stub-unload"):
        p = sub.add_parser(name)
        p.add_argument("--file", required=True)
        p.add_argument("--model", required=True)
    p = sub.add_parser("sanity-rule")
    p.add_argument("--write-dir", required=True)
    p.add_argument("--check-dir", required=True)
    p.add_argument("--concurrency", type=int, default=1)  # W21: one request at a time
    p.add_argument("--total", type=int, default=FULL_WINDOWS)
    p.add_argument("--writer", required=True)
    p.add_argument("--checker", required=True)
    p.add_argument("--out", default=None)
    p = sub.add_parser("reading-sample")
    for x in ("--write-dir", "--check-dir", "--slice", "--out-dir"):
        p.add_argument(x, required=True)
    p.add_argument("--record", default="runs/v3/INDEX_FROZEN.json")
    p.add_argument("--accepted", type=int, default=SAN_READ_ACC)
    p.add_argument("--rejected", type=int, default=SAN_READ_REJ)
    p.add_argument("--salt", default=SAN_READ_SALT)
    p = sub.add_parser("full-gate")
    p.add_argument("--sanity-result", required=True)
    p.add_argument("--sanity-dir", required=True)
    p.add_argument("--reading", default=None)
    p.add_argument("--writer", required=True)
    p.add_argument("--checker", required=True)
    p = sub.add_parser("index")
    for x in ("--write-dir", "--check-dir", "--slice", "--out"):
        p.add_argument(x, required=True)
    p = sub.add_parser("audit-sample")
    for x in ("--index", "--write-dir", "--check-dir", "--slice", "--out-dir"):
        p.add_argument(x, required=True)
    p.add_argument("--record", default="runs/v3/INDEX_FROZEN.json")
    sub.add_parser("rule-text")
    a = ap.parse_args(argv)
    return {"manifest": cmd_manifest, "start-check": cmd_start_check, "mark": cmd_mark, "server-gate": cmd_server_gate,
            "stub-status": cmd_stub_status, "stub-load": cmd_stub_load, "stub-unload": cmd_stub_unload, "sanity-rule": cmd_sanity_rule,
            "reading-sample": cmd_reading_sample, "full-gate": cmd_full_gate, "index": cmd_index, "audit-sample": cmd_audit_sample,
            "rule-text": cmd_rule_text}[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())
