#!/usr/bin/env python3
"""M7 KB phase 2, I2: the entry writer (runs/m7/reports/kb2_design.md section 3). One server call per window.

For each selected slice chunk (index order) the window is it plus the previous and the next chunk of the same file; the
writer model (DeepSeek on the local server, thinking off, temperature 0) replies {"entries": [...]}. A reply that does not
parse or breaks the reply schema is retried once at temperature 0.3; a second failure is a failed window row (counted).
Each entry then passes the mechanical gate (kafka_sre_rag/kb_entry.py) or is kept with its reasons in the rejects file.

  .venv/bin/python scripts/run_guarded.py --stall-min 20 --log L -- .venv/bin/python scripts/kb_write_entries.py \\
      --slice work/m7/kb/slice.jsonl --select sanity --out-dir work/m7/kb/sanity --model DeepSeek-V4.1-Flash-1017-oQ4e-mtp \\
      --done runs/m7/logs/m7_kb2.sanity.write.done

Outputs in --out-dir: windows.jsonl (one row per window: status, counts, seconds, tokens; the resume key),
entries_raw.jsonl (one row per gated entry, with the code's fields), entries_gate_rejects.jsonl (one row per rejected entry
with the reasons). A window's lines are appended together after its call; a window without its row is redone on resume
(entries of it written before a crash are removed first). A line per window on stdout:
  [write] 123/1487 <target id> entries=4 gate_fail=0 12.3s tokens=812
Every row carries prompts.KB_ENTRY_PROMPT_VERSION and the writer model; a folder holding rows of another version or model is
refused (a changed prompt means a new folder and a new sanity run). --reuse DIR copies the rows of selected windows from an
earlier folder (the sanity run) when their version and model are the same; other windows are written.
The done marker (--done) records the counts and the sha256 of every output; a second run with everything done writes nothing.

The server address and key come from OMLX_URL / OMLX_API_KEY or ~/.config/kafka-sre-llm/ (omlx_url, judge_key); they are
never written or printed. --backend stub answers from an in-process stub server (tests and the chain's dry run); its plants
come from the environment: KB_STUB_WRITE_BAD (target ids whose replies never parse), KB_STUB_WRITE_RETRY (bad at temperature
0, good at the retry), KB_STUB_WRITE_GATE (one entry with an invented identifier added), KB_STUB_STOP_AFTER (HTTP 500
after N calls), KB_STUB_WRONG_MODEL_AFTER (another model answers after N calls).
Exit: 0 done; 1 a server or input error (resumable); 2 refused (also: the server answered with another model; never retried).
"""
import argparse
import concurrent.futures as cf
import datetime as dt
import hashlib
import json
import os
import re
import statistics
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
from kafka_sre_rag import kb_entry as K, prompts as P  # noqa: E402
from kafka_sre_rag.generators import strip_think, is_loopback_host  # noqa: E402

WRITER_MODEL = "DeepSeek-V4.1-Flash-1017-oQ4e-mtp"
EXTRA_BODY = {"chat_template_kwargs": {"thinking_mode": "chat"}}     # DeepSeek's switch for thinking off (gen_v2.sh, the M7 judge)
MAX_TOKENS, CONCURRENCY, RETRY_TEMPERATURE = 3000, 3, 0.3
SANITY_N, SANITY_SALT = 50, "kb2-sanity:"
FILES = ("windows.jsonl", "entries_raw.jsonl", "entries_gate_rejects.jsonl")


class ServerError(Exception):
    pass


class WrongModel(ServerError):
    """The server answered with another model than the one asked for: another client changed the server. A refusal (exit 2),
    never retried: the chain's server gate must see the server before anything goes on (review of f45dd13)."""


def now():
    return dt.datetime.now().astimezone().isoformat(timespec="seconds")


def sha256_file(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def server_config(base_url=None):
    """(url, key) from the argument, OMLX_URL / OMLX_API_KEY or ~/.config/kafka-sre-llm/. Never printed."""
    cfg = Path.home() / ".config" / "kafka-sre-llm"
    url = base_url or os.environ.get("OMLX_URL")
    if not url and (cfg / "omlx_url").is_file():
        url = (cfg / "omlx_url").read_text().strip()
    key = os.environ.get("OMLX_API_KEY")
    if not key and (cfg / "judge_key").is_file():
        key = (cfg / "judge_key").read_text().strip()
    return url, key


class Client:
    """One OpenAI-compatible chat call per request; thinking off through EXTRA_BODY. transport: an httpx transport (the
    stub server in tests and dry runs)."""

    def __init__(self, url, key, model, transport=None, timeout=900):
        import httpx
        self.httpx = httpx
        if not url:
            raise ServerError("no server address (OMLX_URL, --base-url or ~/.config/kafka-sre-llm/omlx_url)")
        base = url.rstrip("/")
        self.url = base + ("/chat/completions" if base.endswith("/v1") else "/v1/chat/completions")
        self.model = model
        self.headers = {"Authorization": f"Bearer {key}"} if key else {}
        self.client = httpx.Client(transport=transport, timeout=timeout,
                                   trust_env=not (transport is None and is_loopback_host(base)))

    def chat(self, messages, max_tokens, temperature=0.0):
        body = {"model": self.model, "messages": messages, "max_tokens": max_tokens, "temperature": temperature, **EXTRA_BODY}
        try:
            r = self.client.post(self.url, json=body, headers=self.headers)
        except self.httpx.HTTPError as e:
            raise ServerError(f"the server could not be reached ({type(e).__name__})") from None
        if r.status_code >= 400:
            raise ServerError(f"the server returned HTTP {r.status_code}")
        d = r.json()
        if d.get("model") and d["model"] != self.model:
            raise WrongModel(f"the server answered with model {d['model']!r}, not {self.model!r}")
        ch = d["choices"][0]
        u = d.get("usage") or {}
        return {"text": strip_think(ch["message"].get("content") or ""), "finish": ch.get("finish_reason"),
                "tokens": u.get("completion_tokens", 0), "prompt_tokens": u.get("prompt_tokens", 0)}


def parse_json(text):
    """The one JSON object of a reply (a code fence or text around it tolerated). None when there is none."""
    t = (text or "").strip()
    t = re.sub(r"^```[a-zA-Z]*\s*|\s*```$", "", t)
    i, j = t.find("{"), t.rfind("}")
    if i < 0 or j < i:
        return None
    try:
        return json.loads(t[i:j + 1])
    except ValueError:
        return None


def stub_transport(handler):
    """An in-process stub server: handler(body) -> reply text (or a dict {"text", "finish"}). KB_STUB_STOP_AFTER=N: every
    call after the N-th answers HTTP 500 (a planted server stop, to rehearse a resume)."""
    import httpx
    stop = int(os.environ.get("KB_STUB_STOP_AFTER") or 0)
    wrong = int(os.environ.get("KB_STUB_WRONG_MODEL_AFTER") or 0)     # every call after the N-th is answered by another model
    calls = [0]
    lock = threading.Lock()

    def h(request):
        with lock:
            calls[0] += 1
            n = calls[0]
        if stop and n > stop:
            return httpx.Response(500, json={"error": "planted stop"})
        body = json.loads(request.content)
        out = handler(body)
        if isinstance(out, str):
            out = {"text": out}
        return httpx.Response(200, json={"model": "Another-Clients-Model" if wrong and n > wrong else body["model"], "choices": [{"message": {"content": out["text"]},
                                         "finish_reason": out.get("finish", "stop")}],
                                         "usage": {"completion_tokens": len(out["text"]) // 4,
                                                   "prompt_tokens": sum(len(m["content"]) for m in body["messages"]) // 4}})
    return httpx.MockTransport(h)


def _env_ids(name):
    return {x for x in os.environ.get(name, "").split(",") if x}


def stub_writer(chunks_by_id):
    """A deterministic writer for tests and dry runs: one entry per target that quotes the target's first words."""
    bad, retry, gate = _env_ids("KB_STUB_WRITE_BAD"), _env_ids("KB_STUB_WRITE_RETRY"), _env_ids("KB_STUB_WRITE_GATE")

    def handler(body):
        user = body["messages"][-1]["content"]
        m = re.search(r"entries for the target chunk (\S+?)\.", user)
        target = m.group(1)
        if target in bad or (target in retry and body["temperature"] == 0):
            return "not JSON at all"
        c = chunks_by_id[target]
        words = K.norm_ws(c["text"]).split(" ")
        quote = ""
        for w in words:
            if len(quote) + len(w) + 1 > 120:
                break
            quote = (quote + " " + w).strip()
        if not quote:
            return json.dumps({"entries": []})
        entries = [{"kind": "fact", "title": " ".join(quote.split(" ")[:5])[:100], "statement": quote,
                    "sources": [target], "quotes": [{"chunk": target, "text": quote}]}]
        if target in gate:
            entries.append({"kind": "value", "title": "Invented", "statement": "The setting `stub.invented.key` defaults to 42.",
                            "sources": [target], "quotes": [{"chunk": target, "text": quote}]})
        return json.dumps({"entries": entries})
    return handler


def load_rows(p):
    p = Path(p)
    return [json.loads(l) for l in open(p, encoding="utf-8") if l.strip()] if p.exists() else []


def write_rows(p, rows):
    tmp = Path(str(p) + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(K.dumps(r) + "\n")
    tmp.replace(p)


def sanity_ids(ids, n=SANITY_N):
    """The sanity rule (section 7): the n slice chunks with the lowest sha256("kb2-sanity:" + id), in index order."""
    pick = set(sorted(ids, key=lambda i: hashlib.sha256((SANITY_SALT + i).encode()).hexdigest())[:n])
    return [i for i in ids if i in pick]


def load_slice_chunks(slice_path, record):
    import kb_slice
    chunks, rec = kb_slice.load_index(record)
    ids = kb_slice.load_slice(slice_path)
    if ids != kb_slice.slice_ids(chunks):
        raise SystemExit(f"REFUSED: {slice_path} does not reproduce from the slice rule on the frozen index")
    byid = {c["id"]: c for c in chunks}
    return [byid[i] for i in ids], rec, sha256_file(slice_path)


def select(slice_chunks, how):
    ids = [c["id"] for c in slice_chunks]
    if how == "all":
        return ids
    if how == "sanity":
        return sanity_ids(ids)
    want = [json.loads(l)["id"] if l.strip().startswith("{") else l.strip() for l in open(how, encoding="utf-8") if l.strip()]
    unknown = [i for i in want if i not in set(ids)]
    if unknown:
        raise SystemExit(f"REFUSED: {len(unknown)} selected ids are not in the slice (first {unknown[0]})")
    return [i for i in ids if i in set(want)]


def check_versions(rows, field, version, model_field, model, where):
    for r in rows:
        if r.get(field) != version or r.get(model_field) != model:
            raise SystemExit(f"REFUSED: {where} holds rows of {field} {r.get(field)!r} / {model_field} {r.get(model_field)!r}; "
                             f"this run is {version!r} / {model!r}: outputs of two versions or models are never mixed (use a new folder)")


def window_key(r):
    """The window (target id) of an output row: a string, or the entry's {"target": ...}."""
    w = r.get("window")
    return w.get("target") if isinstance(w, dict) else w


def restore(out, done_ids, files=FILES):
    """Remove the lines of windows without a row (a crash between the lines of a window); returns how many."""
    removed = 0
    for name in files[1:]:
        p = out / name
        rows = load_rows(p)
        keep = [r for r in rows if window_key(r) in done_ids]
        if len(keep) != len(rows):
            removed += len(rows) - len(keep)
            write_rows(p, keep)
    return removed


def write_marker(path, out, files, info):
    rec = {**info, "outputs": {str(out / f): sha256_file(out / f) for f in files}}
    p = Path(path)
    if p.exists():
        try:
            old = json.load(open(p, encoding="utf-8"))
        except ValueError:
            old = {}
        if old.get("outputs") == rec["outputs"]:
            return False
    p.parent.mkdir(parents=True, exist_ok=True)
    rec["created"] = now()
    tmp = Path(str(p) + ".tmp")
    tmp.write_text(json.dumps(rec, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(p)
    return True


def write_window(client, slice_chunks, idx, target, commits, max_tokens):
    t0 = time.monotonic()
    _, win = K.window_of(slice_chunks, idx)
    msgs = P.kb_writer_messages(K.window_text(win, target), target)
    attempts, why, tokens, ptokens = 0, None, 0, 0
    for temp in (0.0, RETRY_TEMPERATURE):
        attempts += 1
        r = client.chat(msgs, max_tokens, temperature=temp)
        tokens += r["tokens"]
        ptokens += r["prompt_tokens"]
        obj = parse_json(r["text"])
        if obj is None:
            why = "the reply is not one JSON object" + (" (cut by the token limit)" if r["finish"] == "length" else "")
            continue
        why, ok, rej = K.check_reply(obj, win, target)
        if why is None:
            break
    created = now()
    row = {"window": target, "chunks": [c["id"] for c in win], "prompt_version": P.KB_ENTRY_PROMPT_VERSION,
           "writer": client.model, "attempts": attempts, "seconds": round(time.monotonic() - t0, 2), "tokens": tokens,
           "prompt_tokens": ptokens, "created": created}
    if why is not None:
        row.update({"status": "failed", "failed": why, "entries": 0, "gate_rejects": 0, "written": 0})
        return row, [], []
    entries, rejects, seen = [], [], set()
    for e in ok:
        fe = K.finish_entry(e, win, target, prompt_version=P.KB_ENTRY_PROMPT_VERSION, writer=client.model,
                            created=created, commits=commits)
        reasons = K.render_check(fe)
        if fe["id"] in seen:
            reasons = reasons + ["an exact duplicate of an earlier entry of this window"]
        seen.add(fe["id"])
        if reasons:
            rej.append((e, reasons))
        else:
            entries.append(fe)
    rejects = [{"window": target, "entry": e, "reasons": rs, "prompt_version": P.KB_ENTRY_PROMPT_VERSION,
                "writer": client.model} for e, rs in rej]
    row.update({"status": "ok", "entries": len(entries), "gate_rejects": len(rejects), "written": len(entries) + len(rejects)})
    return row, entries, rejects


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--slice", required=True)
    ap.add_argument("--record", default="runs/v3/INDEX_FROZEN.json")
    ap.add_argument("--select", default="sanity", help="sanity (the 50 of section 7), all, or a file of ids")
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--model", default=WRITER_MODEL)
    ap.add_argument("--backend", choices=["openai", "stub"], default="openai")
    ap.add_argument("--base-url", default=None, help="the server (default: OMLX_URL or ~/.config/kafka-sre-llm/omlx_url)")
    ap.add_argument("--concurrency", type=int, default=CONCURRENCY)
    ap.add_argument("--max-tokens", type=int, default=MAX_TOKENS)
    ap.add_argument("--reuse", default=None, help="an earlier output folder whose rows of selected windows are copied (same version and model)")
    ap.add_argument("--done", default=None, help="the done marker (counts and the sha256 of every output)")
    a = ap.parse_args(argv)
    try:
        slice_chunks, rec, slice_sha = load_slice_chunks(a.slice, a.record)
        sel = select(slice_chunks, a.select)
    except SystemExit as e:
        print(e, file=sys.stderr)
        return 2
    pos = {c["id"]: i for i, c in enumerate(slice_chunks)}
    out = Path(a.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    for f in FILES:
        (out / f).touch()
    wrows = load_rows(out / "windows.jsonl")
    try:
        check_versions(wrows, "prompt_version", P.KB_ENTRY_PROMPT_VERSION, "writer", a.model, out / "windows.jsonl")
        check_versions(load_rows(out / "entries_raw.jsonl"), "prompt_version", P.KB_ENTRY_PROMPT_VERSION, "writer", a.model,
                       out / "entries_raw.jsonl")
    except SystemExit as e:
        print(e, file=sys.stderr)
        return 2
    extra = [r["window"] for r in wrows if r["window"] not in set(sel)]
    if extra:
        print(f"REFUSED: {out} holds rows of {len(extra)} windows outside this selection (first {extra[0]}); use another folder", file=sys.stderr)
        return 2
    done = {r["window"] for r in wrows}
    removed = restore(out, done)
    if removed:
        print(f"[write] resume: removed {removed} line(s) of windows that had no window row (a stop between the lines of a window)")
    lock = threading.Lock()

    def append(row, entries, rejects):
        with lock:
            for name, rows in (("entries_raw.jsonl", entries), ("entries_gate_rejects.jsonl", rejects), ("windows.jsonl", [row])):
                with open(out / name, "a", encoding="utf-8") as f:
                    for r in rows:
                        f.write(K.dumps(r) + "\n")
                    f.flush()
                    os.fsync(f.fileno())

    n_total, counter = len(sel), [len(done)]
    if a.reuse:
        rdir = Path(a.reuse)
        rw = [r for r in load_rows(rdir / "windows.jsonl") if r["window"] in set(sel) and r["window"] not in done]
        same = [r for r in rw if r.get("prompt_version") == P.KB_ENTRY_PROMPT_VERSION and r.get("writer") == a.model]
        if len(same) != len(rw):
            print(f"[write] reuse: {len(rw) - len(same)} window(s) of {rdir} have another prompt version or model and are written again")
        ents = load_rows(rdir / "entries_raw.jsonl")
        rejs = load_rows(rdir / "entries_gate_rejects.jsonl")
        for r in sorted(same, key=lambda r: pos[r["window"]]):
            w = r["window"]
            append({**r, "reused_from": str(rdir)}, [e for e in ents if window_key(e) == w],
                   [x for x in rejs if window_key(x) == w])
            done.add(w)
            counter[0] += 1
            print(f"[write] {counter[0]}/{n_total} {w} entries={r['entries']} gate_fail={r['gate_rejects']} reused", flush=True)
    todo = [w for w in sel if w not in done]
    if todo:
        if a.backend == "stub":
            transport = stub_transport(stub_writer({c["id"]: c for c in slice_chunks}))
            url, key = "http://stub.invalid/v1", None
        else:
            transport = None
            url, key = server_config(a.base_url)
        try:
            client = Client(url, key, a.model, transport=transport)
        except ServerError as e:
            print(f"ERROR: {e}", file=sys.stderr)
            return 1
        print(f"[write] {len(todo)} window(s) to write of {n_total} ({len(done)} done); model {a.model}, prompt {P.KB_ENTRY_PROMPT_VERSION}, "
              f"concurrency {a.concurrency}, max_tokens {a.max_tokens}", flush=True)
        commits = rec.get("source_commits") or {}
        err = None
        with cf.ThreadPoolExecutor(max_workers=max(1, a.concurrency)) as ex:
            stop = threading.Event()     # set on a server error: the windows not yet started return at once (no new call). Never
            # cancel the futures (shutdown(cancel_futures=True)): as_completed then waits forever for them (the hang of 2026-11-24)

            def job(w):
                return None if stop.is_set() else write_window(client, slice_chunks, pos[w], w, commits, a.max_tokens)
            futs = {ex.submit(job, w): w for w in todo}
            for fu in cf.as_completed(futs):
                w = futs[fu]
                try:
                    res = fu.result()
                    if res is None:
                        continue                 # not started after a server error; written on the resume
                    row, entries, rejects = res
                except ServerError as e:
                    err = e if err is None or (isinstance(e, WrongModel) and not isinstance(err, WrongModel)) else err
                    stop.set()                                        # no new call after a server error
                    print(f"[write] -/{n_total} {w} SERVER ERROR ({e}); not written", flush=True)
                    continue
                append(row, entries, rejects)
                with lock:
                    counter[0] += 1
                    k = counter[0]
                tail = f" FAILED ({row['failed']})" if row["status"] == "failed" else (" retried" if row["attempts"] > 1 else "")
                print(f"[write] {k}/{n_total} {w} entries={row['entries']} gate_fail={row['gate_rejects']} {row['seconds']}s "
                      f"tokens={row['tokens']}{tail}", flush=True)
        if isinstance(err, WrongModel):
            print(f"REFUSED: {err}; another client changed the server; the windows written so far are kept (the chain's server gate decides)", file=sys.stderr)
            return 2
        if err:
            print(f"ERROR: {err}; the windows written so far are kept (resume by running again)", file=sys.stderr)
            return 1
    rows = load_rows(out / "windows.jsonl")
    ok = [r for r in rows if r["status"] == "ok"]
    per = [r["entries"] + r["gate_rejects"] for r in ok]
    written, rejected = sum(per), sum(r["gate_rejects"] for r in ok)
    info = {"stage": "write", "select": a.select, "windows": len(rows), "windows_ok": len(ok), "windows_failed": len(rows) - len(ok),
            "windows_retried": sum(1 for r in rows if r["attempts"] > 1), "entries_written": written, "entries_gated": written - rejected,
            "gate_rejects": rejected, "median_written_per_window": statistics.median(per) if per else 0,
            "prompt_version": P.KB_ENTRY_PROMPT_VERSION, "writer": a.model, "slice_sha256": slice_sha}
    if len(rows) != n_total:
        print(f"ERROR: {len(rows)} window rows for {n_total} selected windows", file=sys.stderr)
        return 1
    if a.done:
        new = write_marker(a.done, out, FILES, info)
        print(f"[write] done marker {'written' if new else 'unchanged (nothing new)'}: {a.done}")
    print(f"[write] summary: {len(rows)} windows ({len(ok)} ok, {len(rows) - len(ok)} failed, {info['windows_retried']} retried); "
          f"{written} entries written, {rejected} gate rejects; median written per window {info['median_written_per_window']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
