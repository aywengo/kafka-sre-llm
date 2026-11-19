#!/usr/bin/env python3
"""Harness of the M4 evaluation (skeleton): answers, manifest, pack replay, hidden part, run sizes, report.

  eval2.py convert-legacy --in eval/questions_real.jsonl --out items.jsonl --prefix real
  eval2.py answer --items items.jsonl --out answers.jsonl --config NAME --backend openai --model M [...]
  eval2.py answer ... --replay-packs other_answers.jsonl        (same excerpts as another run)
  eval2.py answer ... --subset smoke                            (fixed stratified subset of the open file)
  eval2.py keygen                                               (key for the hidden part; never overwrites)
  eval2.py split --items all.jsonl --seed 7 --open-out open.jsonl --hidden-out hidden.jsonl.enc
  eval2.py report answers.jsonl [answers2.jsonl.enc ...]

Items: JSON Lines; id (unique), tier (A, B or legacy), kind, family, domain, question; tier B may carry
`evidence`, appended under a line "Evidence:". Unknown keys are carried through. One request at a time.
Hidden files end in .enc (AES-256-CBC through the openssl binary, HMAC-SHA256 checked in Python); progress
lines for them carry ids and timings only. Key: EVAL_HIDDEN_KEY or ~/.config/kafka-sre-llm/eval_hidden.key.
The server URL comes from --base-url or OMLX_URL and is never written to the manifest.
"""
import argparse
import hashlib
import hmac
import json
import os
import shutil
import subprocess
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DEFAULT_KEY = Path.home() / ".config/kafka-sre-llm/eval_hidden.key"
SMOKE_FILE = ROOT / "eval/m4/smoke_ids.json"
MAGIC = b"EVAL2ENC1\n"
TIERS = ("A", "B", "legacy")
REQUIRED = ("id", "tier", "kind", "family", "domain", "question")
MANIFEST_VOLATILE = ("started", "ended")  # not compared on resume


class EvalError(RuntimeError):
    pass


# ---------------------------------------------------------------- encryption

def key_path():
    return Path(os.environ.get("EVAL_HIDDEN_KEY") or DEFAULT_KEY).expanduser()


def keygen(path=None):
    path = Path(path) if path else key_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        raise EvalError("key file exists; refusing to overwrite it") from None
    with os.fdopen(fd, "w") as f:
        f.write(os.urandom(32).hex() + "\n")
    return path


def _load_key(path=None):
    path = Path(path) if path else key_path()
    if not path.exists():
        raise EvalError("hidden key file not found (EVAL_HIDDEN_KEY, or run 'eval2.py keygen')")
    if path.stat().st_mode & 0o077:
        raise EvalError("hidden key file is readable by group or others; chmod 600 it")
    raw = path.read_text().strip()
    try:
        key = bytes.fromhex(raw)
    except ValueError:
        raise EvalError("hidden key file is not hex") from None
    if len(key) < 32:
        raise EvalError("hidden key is shorter than 32 bytes")
    return key


def _openssl(args, data, passphrase):
    exe = shutil.which("openssl")
    if not exe:
        raise EvalError("openssl not found on PATH")
    r, w = os.pipe()
    try:
        os.write(w, passphrase)
        os.close(w)
        p = subprocess.run([exe, "enc", "-aes-256-cbc", "-pbkdf2", "-iter", "100000", "-md", "sha256",
                            "-pass", f"fd:{r}", *args], input=data, capture_output=True, pass_fds=(r,))
    finally:
        os.close(r)
    if p.returncode != 0:  # openssl's own message is not shown: it could echo nothing useful and costs nothing to omit
        raise EvalError("openssl failed")
    return p.stdout


def _subkeys(key):
    return (hmac.new(key, b"eval2-enc", hashlib.sha256).hexdigest().encode(),
            hmac.new(key, b"eval2-mac", hashlib.sha256).digest())


def encrypt_bytes(data, key_file=None):
    enc, mac = _subkeys(_load_key(key_file))
    ct = _openssl([], data, enc)
    return MAGIC + hmac.new(mac, ct, hashlib.sha256).digest() + ct


def decrypt_bytes(blob, key_file=None):
    enc, mac = _subkeys(_load_key(key_file))
    if not blob.startswith(MAGIC) or len(blob) < len(MAGIC) + 32:
        raise EvalError("not an eval2 encrypted file")
    tag, ct = blob[len(MAGIC):len(MAGIC) + 32], blob[len(MAGIC) + 32:]
    if not hmac.compare_digest(tag, hmac.new(mac, ct, hashlib.sha256).digest()):
        raise EvalError("encrypted file failed its integrity check (wrong key or damaged file)")
    return _openssl(["-d"], ct, enc)


def is_hidden(path):
    return str(path).endswith(".enc")


def write_atomic(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def read_text(path):
    b = Path(path).read_bytes()
    return (decrypt_bytes(b) if is_hidden(path) else b).decode("utf-8")


def read_jsonl(path):
    if not Path(path).exists():
        return []
    return [json.loads(l) for l in read_text(path).splitlines() if l.strip()]


def write_jsonl(path, rows):
    data = "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows).encode("utf-8")
    write_atomic(path, encrypt_bytes(data) if is_hidden(path) else data)


def sha256_bytes(b):
    return hashlib.sha256(b).hexdigest()


def sha256_file(path):
    return sha256_bytes(Path(path).read_bytes())


# ---------------------------------------------------------------- items

def validate_items(items):
    seen = set()
    for n, it in enumerate(items, 1):
        miss = [k for k in REQUIRED if not it.get(k) or not isinstance(it.get(k), str)]
        if miss:
            raise EvalError(f"item {n}: missing or non-string keys {miss}")
        if it["tier"] not in TIERS:
            raise EvalError(f"item {n}: tier must be one of {TIERS}")
        if it["id"] in seen:
            raise EvalError(f"duplicate item id {it['id']}")
        seen.add(it["id"])
    return items


def read_items(path):
    return validate_items(read_jsonl(path))


def full_question(item):
    q = item["question"]
    if item.get("evidence"):
        q += "\n\nEvidence:\n" + item["evidence"]
    return q


def convert_legacy(src, prefix):
    out = []
    for n, line in enumerate((l for l in Path(src).read_text().splitlines() if l.strip()), 1):
        row = json.loads(line)
        iid = f"{prefix}-{n:03d}"
        cat = row.get("category") or "legacy"
        out.append({**row, "id": iid, "tier": "legacy", "kind": cat, "family": iid, "domain": cat,
                    "question": row["question"]})
    return validate_items(out)


# ---------------------------------------------------------------- smoke subset

def _order_key(seed, iid):
    return hashlib.sha256(f"{seed}:{iid}".encode()).hexdigest()


def _allocate(sizes, total):
    """Largest-remainder allocation of `total` over the strata sizes (dict kind -> count)."""
    n = sum(sizes.values())
    if total >= n:
        return dict(sizes)
    quota = {k: total * v / n for k, v in sizes.items()}
    alloc = {k: int(q) for k, q in quota.items()}
    for k in sorted(sizes, key=lambda k: (-(quota[k] - alloc[k]), k))[: total - sum(alloc.values())]:
        alloc[k] += 1
    return alloc


def build_smoke(items, smoke_a, smoke_b, seed):
    ids = []
    for tier, want in (("A", smoke_a), ("B", smoke_b)):
        by_kind = defaultdict(list)
        for it in items:
            if it["tier"] == tier:
                by_kind[it["kind"]].append(it["id"])
        alloc = _allocate({k: len(v) for k, v in by_kind.items()}, want)
        for kind, ids_k in by_kind.items():
            ids += sorted(ids_k, key=lambda i: _order_key(seed, i))[: alloc[kind]]
    return {"seed": seed, "smoke_a": smoke_a, "smoke_b": smoke_b, "ids": sorted(ids)}


def smoke_subset(items, path, smoke_a, smoke_b, seed, rebuild=False):
    path = Path(path)
    if path.exists() and not rebuild:
        sm = json.loads(path.read_text())
        if (sm["smoke_a"], sm["smoke_b"], sm["seed"]) != (smoke_a, smoke_b, seed):
            raise EvalError(f"{path} was built with other sizes or seed; use --rebuild-smoke to replace it")
        have = {it["id"] for it in items}
        gone = [i for i in sm["ids"] if i not in have]
        if gone:
            raise EvalError(f"{len(gone)} smoke ids are not in the items file (first: {gone[0]})")
    else:
        sm = build_smoke(items, smoke_a, smoke_b, seed)
        path.parent.mkdir(parents=True, exist_ok=True)
        write_atomic(path, (json.dumps(sm, indent=1) + "\n").encode())
    want = set(sm["ids"])
    return [it for it in items if it["id"] in want]


# ---------------------------------------------------------------- split

def split_families(items, seed, share=0.5):
    """Whole families to the hidden side until the hidden share per (tier, kind) is reached as nearly as
    families allow. Deterministic in (items, seed, share). Returns (open_items, hidden_items).
    A family with any item marked `"exposed": true` (shown to people) is kept on the open side as a whole;
    the share is then reached with the remaining families."""
    fam = defaultdict(list)
    for it in items:
        fam[it["family"]].append(it)
    forced_open = {f for f, its in fam.items() if any(it.get("exposed") is True for it in its)}
    cells_total = Counter((it["tier"], it["kind"]) for it in items)
    target = {c: share * n for c, n in cells_total.items()}
    got = Counter()
    hidden = set()
    for f in sorted((f for f in fam if f not in forced_open), key=lambda f: _order_key(seed, f)):
        add = Counter((it["tier"], it["kind"]) for it in fam[f])
        before = sum(abs(got[c] - target[c]) for c in add)
        after = sum(abs(got[c] + n - target[c]) for c, n in add.items())
        if after < before:
            hidden.add(f)
            got.update(add)
    return ([it for it in items if it["family"] not in hidden],
            [it for it in items if it["family"] in hidden])


def counts_by_cell(items):
    return dict(sorted(Counter(f"{it['tier']}/{it['kind']}" for it in items).items()))


# ---------------------------------------------------------------- manifest

def git_info():
    def git(*a):
        return subprocess.run(["git", "-C", str(ROOT), *a], capture_output=True, text=True).stdout
    commit = git("rev-parse", "HEAD").strip() or None
    dirty = bool(git("status", "--porcelain", "--untracked-files=no", "--", "kafka_sre_rag",
                     "scripts/retrieve.py", "scripts/build_packs_v3.py").strip())
    return commit, dirty


def catalogue_hash(directory):
    d = Path(directory)
    files = sorted(p for p in d.rglob("*") if p.is_file())
    if not files:
        raise EvalError(f"catalogue directory {d} holds no files")
    h = hashlib.sha256()
    for p in files:
        h.update(f"{p.relative_to(d).as_posix()}\0{sha256_file(p)}\n".encode())
    return h.hexdigest()


def comparable(manifest):
    return {k: v for k, v in manifest.items() if k not in MANIFEST_VOLATILE and k != "run"}


def manifest_path(answers):
    p = str(answers)
    return Path(p + (".manifest.json.enc" if is_hidden(p) else ".manifest.json"))


def read_manifest(answers):
    p = manifest_path(answers)
    if not p.exists():
        return None
    return json.loads(read_text(p))


def write_manifest(answers, m):
    data = (json.dumps(m, indent=1, ensure_ascii=False) + "\n").encode()
    write_atomic(manifest_path(answers), encrypt_bytes(data) if is_hidden(answers) else data)


def listed_model(gen):
    """The server's own entry for the requested model (build or path, when it lists one), else None."""
    client, url = getattr(gen, "client", None), getattr(gen, "models_url", None)
    if client is None or not url:
        return None
    try:
        r = client.get(url, headers=getattr(gen, "headers", {}))
        for m in r.json().get("data") or []:
            if isinstance(m, dict) and m.get("id") == gen.model:
                return m
    except Exception:
        pass
    return None


def build_manifest(a, gen, cfg, items_file, n_items, subset, replay_info):
    from kafka_sre_rag import pipeline, profiles
    ident = getattr(gen, "identity", {}) or {}
    listing = listed_model(gen)
    commit, dirty = git_info()
    reasons = []
    if a.mode != "default":
        reasons.append(f"mode {a.mode}")
    if a.k != "k6":
        reasons.append(f"k {a.k}")
    if not pipeline.is_reference(gen, cfg):
        reasons.append("generator is not the reference model")
    if a.no_rerank:
        reasons.append("no reranker")
    if a.max_tokens is not None:
        reasons.append(f"max_tokens override {a.max_tokens}")
    profile = getattr(a, "profile", "frozen")
    if profile != "frozen":
        reasons.append(profiles.unmeasured_reason(profile))
    m = {
        "harness": "eval2", "config": a.config, "backend": ident.get("backend"),
        "model_requested": ident.get("model"),
        "model_echoed": listing.get("id") if listing else None,
        "model_build": listing if listing else (ident.get("model") if ident.get("backend") == "mlx" else None),
        "adapter": ident.get("adapter") or getattr(a, "adapter", None),
        "mode": a.mode, "k": a.k, "rerank": not a.no_rerank,
        "extra_body": a.extra_body, "max_tokens": a.max_tokens,
        "unmeasured_reasons": reasons,
        "index_sha256": cfg["index"]["sha256"], "reranker_revision": cfg["retrieval"]["reranker"]["revision"],
        "catalogue_sha256": catalogue_hash(a.catalogue) if a.catalogue else None,
        "runtime_git_commit": commit, "runtime_tree_dirty": dirty,
        "concurrency": 1, "host_class": a.host_class,
        "items_sha256": sha256_file(items_file), "items_count": n_items, "subset": subset,
        "replay": replay_info, "hidden": is_hidden(a.out),
        "started": datetime.now(timezone.utc).isoformat(timespec="seconds"), "ended": None,
    }
    m.update(profiles.manifest_fields(profile))  # nothing is added for the frozen profile
    return m


def run_id(m):
    blob = json.dumps(comparable(m), sort_keys=True, ensure_ascii=False).encode()
    return f"{m['config']}-{sha256_bytes(blob)[:10]}"


# ---------------------------------------------------------------- generator

class StubGen:
    """--backend stub: no model; a fixed expansion and a fixed answer."""
    backend = "stub"
    identity = {"backend": "stub", "model": "stub", "adapter": None}

    def chat(self, messages, max_tokens, on_token=None):
        from kafka_sre_rag import prompts
        if messages[0]["content"] == prompts.EXPAND_SYSTEM:
            return "consumer group, kafka-consumer-groups.sh, --describe", 0, False
        return "[stub answer: no model was called]", 0, False


def make_generator(a):
    if a.backend == "stub":
        return StubGen()
    from kafka_sre_rag.__main__ import make_generator as mk
    ns = argparse.Namespace(dry_run=False, backend=a.backend, model=a.model, adapter=a.adapter,
                            base_url=a.base_url, extra_body=a.extra_body)
    return mk(ns)


# ---------------------------------------------------------------- answer

def one_line(e, limit=300):
    return (" ".join(f"{type(e).__name__}: {e}".split()))[:limit]


def fmt_progress(i, n, iid, row, hidden):
    s = row["seconds"]
    tail = "FAILED" if row["error"] else "ok"
    if row["error"] and not hidden:
        tail += f" {row['error']}"
    toks = row["tokens"] if row["tokens"] is not None else "-"
    return (f"[answer] {i}/{n} id={iid} search={s['search_terms']:.1f}s retrieval={s['retrieval']:.1f}s "
            f"answer={s['answer']:.1f}s total={s['total']:.1f}s tokens={toks} {tail}")


class AnswerStore:
    """Rows of one answers file; each add is durable before the next item (append for clear files,
    atomic rewrite of the whole encrypted file for hidden ones)."""

    def __init__(self, path):
        self.path, self.hidden = Path(path), is_hidden(path)
        self.rows = read_jsonl(path)

    def drop_failed(self):
        keep = [r for r in self.rows if not r.get("error")]
        if len(keep) != len(self.rows):
            self.rows = keep
            write_jsonl(self.path, keep)

    def add(self, row):
        self.rows.append(row)
        if self.hidden:
            write_jsonl(self.path, self.rows)
        else:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
                f.flush()
                os.fsync(f.fileno())


def load_replay(path, items):
    src = {r["id"]: r for r in read_jsonl(path)}
    missing = [it["id"] for it in items if not isinstance(src.get(it["id"], {}).get("pack"), list)
               or src[it["id"]].get("error")]
    if missing:
        raise EvalError(f"--replay-packs: {len(missing)} items have no stored pack in the other run "
                        f"(first: {missing[0]})")
    return {i: src[i]["pack"] for i in src}


def cmd_answer(a, out=sys.stdout):
    hidden = is_hidden(a.items)
    if hidden != is_hidden(a.out):
        raise EvalError("hidden items need an answers file ending in .enc, and the other way round")
    items = read_items(a.items)
    subset = None
    if a.subset == "smoke":
        if hidden:
            raise EvalError("--subset smoke applies to the open file only")
        items = smoke_subset(items, a.smoke_file, a.smoke_a, a.smoke_b, a.seed, a.rebuild_smoke)
        subset = f"smoke:{a.smoke_a}/{a.smoke_b}:seed{a.seed}"
    if a.limit:
        items = items[: a.limit]
        subset = (subset + "+" if subset else "") + f"first:{a.limit}"
    from kafka_sre_rag import config, evalhooks, profiles
    cfg = config.load()
    profile = getattr(a, "profile", "frozen")
    if profile != "frozen" and a.replay_packs:
        raise EvalError("--profile applies to retrieval; --replay-packs uses stored packs and no retrieval")
    if a.backend != "stub":
        config.verify(cfg=cfg)  # a real run must be on the frozen index and prompts
        profiles.verify_index(profile)
    replay, replay_info = None, None
    if a.replay_packs:
        if is_hidden(a.replay_packs) != hidden:
            raise EvalError("--replay-packs must be hidden exactly when the run is hidden")
        replay = load_replay(a.replay_packs, items)
        src_m = read_manifest(a.replay_packs)
        replay_info = {"answers_sha256": sha256_file(a.replay_packs), "run": (src_m or {}).get("run")}
    gen = make_generator(a)
    manifest = build_manifest(a, gen, cfg, a.items, len(items), subset, replay_info)
    manifest["run"] = run_id(manifest)
    store = AnswerStore(a.out)
    old = read_manifest(a.out)
    if old is not None:
        if comparable(old) != comparable(manifest):
            diff = sorted(k for k in set(comparable(old)) | set(comparable(manifest))
                          if comparable(old).get(k) != comparable(manifest).get(k))
            raise EvalError(f"manifest differs from the existing run in {diff}; not resuming")
        manifest["started"] = old["started"]
    elif store.rows:
        raise EvalError("answers file has rows but no manifest; not resuming")
    write_manifest(a.out, manifest)
    store.drop_failed()
    done = {r["id"] for r in store.rows}
    todo = [it for it in items if it["id"] not in done]
    print(f"[answer] run {manifest['run']}: {len(items)} items, {len(done)} already done, {len(todo)} to do",
          file=out, flush=True)
    consecutive = 0
    for i, it in enumerate(todo, 1):
        row = {"id": it["id"], "text": None, "tokens": None, "truncated": False,
               "seconds": {"search_terms": 0.0, "retrieval": 0.0, "answer": 0.0, "total": 0.0},
               "search_terms": None, "pack": None, "error": None, "run": manifest["run"]}
        if replay is not None:
            row["replayed_from"] = replay_info["run"] or replay_info["answers_sha256"][:12]
        t0 = time.monotonic()
        try:
            r = evalhooks.run_item(full_question(it), gen, cfg, k=a.k, mode=a.mode, rerank=not a.no_rerank,
                                   max_tokens=a.max_tokens, pack_ids=None if replay is None else replay[it["id"]],
                                   **({} if profile == "frozen" else {"profile": profile}))
            row.update(text=r["text"], tokens=r["tokens"] or None, truncated=r["truncated"],
                       seconds=r["seconds"], search_terms=r["search_terms"], pack=r["pack"])
            consecutive = 0
        except Exception as e:  # recorded; the run continues
            row["error"] = one_line(e)
            row["seconds"]["total"] = time.monotonic() - t0
            consecutive += 1
        store.add(row)
        print(fmt_progress(i, len(todo), it["id"], row, hidden), file=out, flush=True)
        if a.max_consecutive_errors and consecutive >= a.max_consecutive_errors:
            print(f"[answer] stopping: {consecutive} failed items in a row", file=out, flush=True)
            return 1
    manifest["ended"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    write_manifest(a.out, manifest)
    print(f"[answer] done {len(store.rows)} rows, {sum(1 for r in store.rows if r['error'])} errors",
          file=out, flush=True)
    return 0


# ---------------------------------------------------------------- report

def pct(xs, p):
    xs = sorted(xs)
    if not xs:
        return None
    pos = (len(xs) - 1) * p / 100
    lo = int(pos)
    hi = min(lo + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (pos - lo)


def summ(xs):
    if not xs:
        return "n=0"
    return (f"n={len(xs)} median={pct(xs, 50):.2f} min={min(xs):.2f} max={max(xs):.2f} p90={pct(xs, 90):.2f}")


def score_path(answers):
    p = Path(answers)
    if is_hidden(p):
        return p.with_name(p.name[:-4] + ".scores.jsonl.enc")
    return p.with_name(p.stem + ".scores.jsonl")


OUTCOMES = ("pass", "correct_decline", "wrong_decline", "wrong", "abstained")


def outcome_line(rows):
    """Totals of the decline outcome (decision J5), beside the all-pass rate: counts only, never ids."""
    rs = [s for s in rows if "outcome" in s]
    n = len(rs)
    cnt = {o: sum(1 for s in rs if s["outcome"] == o) for o in OUTCOMES}
    nii = sum(1 for s in rs if (s.get("coverage") or {}).get("not_in_index"))
    return ("outcomes " + "  ".join(f"{o} {cnt[o]}/{n} = {100 * cnt[o] / n:.1f} %" for o in OUTCOMES)
            + f"  (items not in the index {nii})")


def P_idcheck_block(rows, score_file, hidden, P):
    """Identifier totals (scripts/eval2_idcheck.py report_block) for the free-text answers of a file; totals only for a hidden file.
    Rows that have a tier A score row (a mechanical answer) are left out, so reports of mechanical runs do not change."""
    a_ids = set()
    if Path(score_file).exists():
        a_ids = {s.get("id") for s in read_jsonl(score_file) if "all_pass" in s}
    free = [r for r in rows if r.get("id") not in a_ids]
    if not free:
        return
    try:
        import eval2_idcheck                       # scripts/ is on sys.path when this file is run or imported from the tests
        P(eval2_idcheck.report_block(free, hidden))
    except Exception as e:                         # no catalogues on this machine: say so, do not fail the report
        P(f"identifier check not run: {type(e).__name__}")


def report_one(path, scores=None, out=sys.stdout):
    hidden = is_hidden(path)
    rows = read_jsonl(path)
    m = read_manifest(path)
    name = (m or {}).get("config") or Path(path).name
    P = lambda s="": print(s, file=out)
    P(f"== {name}{' (hidden: totals only)' if hidden else ''}")
    if m:
        P(f"run {m.get('run')}  backend {m.get('backend')}  model {m.get('model_requested')}  mode {m.get('mode')}  "
          f"concurrency {m.get('concurrency')}  host {m.get('host_class')}")
    errs = [r for r in rows if r.get("error")]
    ok = [r for r in rows if not r.get("error")]
    P(f"answers {len(rows)}  errors {len(errs)}  truncated {sum(1 for r in ok if r.get('truncated'))}")
    if errs and not hidden:
        P("failed ids: " + ", ".join(r["id"] for r in errs))
    sp = scores or score_path(path)
    if Path(sp).exists():
        sc = read_jsonl(sp)
        a_rows = [s for s in sc if "all_pass" in s]
        b_rows = [s for s in sc if "score" in s]
        if a_rows:
            n = len(a_rows)
            P(f"tier A scored {n}  all-pass {sum(1 for s in a_rows if s['all_pass'])}/{n}  "
              f"checks passed {sum(s['checks_passed'] for s in a_rows)}/{sum(s['checks_total'] for s in a_rows)}")
        if any("outcome" in s for s in a_rows):
            P(outcome_line(a_rows))
        if b_rows:
            P(f"tier B scored {len(b_rows)}  mean score {sum(s['score'] for s in b_rows) / len(b_rows):.3f}")
        if not a_rows and not b_rows:
            P("score file holds no scorable rows")
    else:
        P("no score file")
    P_idcheck_block(rows, sp, hidden, P)
    P("total seconds   " + summ([r["seconds"]["total"] for r in ok]))
    P("answer tokens   " + summ([r["tokens"] for r in ok if r.get("tokens") is not None]))
    for st in ("search_terms", "retrieval", "answer"):
        P(f"{st:<15}  seconds " + summ([r["seconds"][st] for r in ok]))


def cmd_report(a, out=sys.stdout):
    for n, p in enumerate(a.answers):
        if n:
            print(file=out)
        sc = a.scores[n] if a.scores and n < len(a.scores) else None
        report_one(p, sc, out)
    return 0


# ---------------------------------------------------------------- cli

def build_parser():
    ap = argparse.ArgumentParser(prog="eval2.py", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("convert-legacy")
    c.add_argument("--in", dest="src", required=True)
    c.add_argument("--out", required=True)
    c.add_argument("--prefix", required=True, help="id prefix, e.g. real or holdout")
    k = sub.add_parser("keygen")
    k.add_argument("--path")
    s = sub.add_parser("split")
    s.add_argument("--items", required=True)
    s.add_argument("--seed", type=int, required=True)
    s.add_argument("--share", type=float, default=0.5)
    s.add_argument("--open-out", required=True)
    s.add_argument("--hidden-out", required=True)
    an = sub.add_parser("answer")
    an.add_argument("--items", required=True)
    an.add_argument("--out", required=True)
    an.add_argument("--config", required=True, help="configuration name")
    an.add_argument("--backend", choices=["mlx", "openai", "stub"], default="openai")
    an.add_argument("--model")
    an.add_argument("--adapter")
    an.add_argument("--base-url", help="openai: server URL (default: OMLX_URL)")
    an.add_argument("--extra-body", type=json.loads, default=None, metavar="JSON")
    an.add_argument("--max-tokens", type=int, default=None)
    an.add_argument("--mode", choices=["default", "strict"], default="default")
    an.add_argument("--k", choices=["k6", "k3"], default="k6")
    an.add_argument("--profile", choices=["frozen", "r1", "r2", "r3"], default="frozen",
                    help="retrieval profile (default frozen); r1, r2 and r3 are unmeasured candidates "
                         "(kafka_sre_rag/candidate.json) and are recorded in the manifest")
    an.add_argument("--no-rerank", action="store_true", help="BM25 order only (unmeasured)")
    an.add_argument("--catalogue", help="catalogue directory to hash into the manifest")
    an.add_argument("--host-class", default=None, help='free text, for example "Mac Studio"')
    an.add_argument("--replay-packs", help="answers file of another run: use its stored chunk ids")
    an.add_argument("--subset", choices=["smoke"])
    an.add_argument("--smoke-a", type=int, default=60)
    an.add_argument("--smoke-b", type=int, default=24)
    an.add_argument("--seed", type=int, default=1, help="seed of the smoke subset")
    an.add_argument("--smoke-file", default=str(SMOKE_FILE))
    an.add_argument("--rebuild-smoke", action="store_true")
    an.add_argument("--limit", type=int, default=None, help="first N items only (recorded in the manifest)")
    an.add_argument("--max-consecutive-errors", type=int, default=5, help="stop after N failures in a row (0: never)")
    r = sub.add_parser("report")
    r.add_argument("answers", nargs="+")
    r.add_argument("--scores", nargs="*", help="score files, in the order of the answers files "
                   "(default: <answers stem>.scores.jsonl next to each)")
    return ap


def main(argv=None):
    a = build_parser().parse_args(argv)
    try:
        if a.cmd == "convert-legacy":
            items = convert_legacy(a.src, a.prefix)
            write_jsonl(a.out, items)
            print(f"converted {len(items)} items to {a.out}")
        elif a.cmd == "keygen":
            keygen(a.path)
            print("key written (mode 600)")
        elif a.cmd == "split":
            items = read_items(a.items)
            op, hd = split_families(items, a.seed, a.share)
            write_jsonl(a.open_out, op)
            write_jsonl(a.hidden_out, hd)
            print(f"open {len(op)} items, hidden {len(hd)} items, families hidden "
                  f"{len({i['family'] for i in hd})} of {len({i['family'] for i in items})}")
            print("open  by tier/kind: " + json.dumps(counts_by_cell(op)))
            print("hidden by tier/kind: " + json.dumps(counts_by_cell(hd)))
        elif a.cmd == "answer":
            return cmd_answer(a)
        elif a.cmd == "report":
            return cmd_report(a)
    except EvalError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
