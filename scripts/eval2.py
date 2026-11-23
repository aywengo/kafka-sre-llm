#!/usr/bin/env python3
"""Harness of the M4 evaluation (skeleton): answers, manifest, pack replay, hidden part, run sizes, report.

  eval2.py convert-legacy --in eval/questions_real.jsonl --out items.jsonl --prefix real
  eval2.py answer --items items.jsonl --out answers.jsonl --config NAME --backend openai --model M [...]
  eval2.py answer ... --replay-packs other_answers.jsonl        (same excerpts as another run; also hidden .enc files)
  eval2.py answer ... --mode closed                             (closed book, configuration P0: no search terms, no
                                                                retrieval, empty pack; refused with --replay-packs)
  eval2.py answer ... --subset smoke                            (fixed stratified subset of the open file)
  eval2.py answer --items items.jsonl --revise-from answers.jsonl [--r2 --backend openai --model M] [--catalogue DIR]
                                                                (revise replay, below)
  eval2.py keygen                                               (key for the hidden part; never overwrites)
  eval2.py split --items all.jsonl --seed 7 --open-out open.jsonl --hidden-out hidden.jsonl.enc
  eval2.py report answers.jsonl [answers2.jsonl.enc ...]

Items: JSON Lines; id (unique), tier (A, B or legacy), kind, family, domain, question; tier B may carry
`evidence`, appended under a line "Evidence:". Unknown keys are carried through. One request at a time.
Hidden files end in .enc (AES-256-CBC through the openssl binary, HMAC-SHA256 checked in Python); progress
lines for them carry ids and timings only. Key: EVAL_HIDDEN_KEY or ~/.config/kafka-sre-llm/eval_hidden.key.
The server URL comes from --base-url or OMLX_URL and is never written to the manifest.

Revise replay (--revise-from): the stored first-pass answers and packs of another run go through the revise step of
kafka_sre_rag/revise.py (R1 marks; with --r2 also the patch call to the model) with the catalogue check. Nothing is
regenerated and the stored answers file is only read. One record per item is written to --out (default: the answers
file with .revise.jsonl in place of .jsonl, next to it), with the first-pass text, findings, patches, final text, both
check results, timings and the revise prompt hash. Resumable by id; hidden items need .enc files throughout.
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
MANIFEST_VOLATILE = ("started", "ended", "runtime_git_commits", "reloads",
                     "prompt_hash_form")  # not compared on resume (the lists exist only after --allow-commit-change or a model reload; the hash form is a description of the rows, not of the run, so older answer files stay resumable)
# The form hashed into the rows' prompt_sha256 / search_terms_prompt_sha256 (M6 F-a): recorded in the manifest.
PROMPT_HASH_FORM = ("sha256 of the UTF-8 bytes of json.dumps([{'role': ..., 'content': ...}, ...], sort_keys=True, "
                    "ensure_ascii=False, separators=(',', ':')) over the exact messages sent to the model; "
                    "prompt_chars is the sum of the characters of the contents")
# What the server lists for a model (model_build): id, object, owned_by, model_type and max_model_len describe the model itself and are compared on
# resume; "created" is the server's load stamp (it changes whenever the server reloads the same model) and is not compared: a change is recorded
# in the manifest's "reloads" list and logged.
LOAD_STAMP_FIELDS = ("created",)
# The M4 reference configuration (MILESTONE.md "Decisions", 2026-11-18): the 4-bit build on the server path. A run is
# labelled as on the reference when the server lists exactly this model (the manifest records both, see build_manifest).
M4_REFERENCE_BACKEND = "openai"
M4_REFERENCE_MODEL = "Qwen3.8-27B-4bit"


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


def items_paths(items):
    """The items argument as a list of paths: one path (str or Path) or several."""
    return [items] if isinstance(items, (str, Path)) else list(items)


def items_hidden(items):
    """True when the items file(s) are hidden (.enc). Open and hidden files are never mixed in one run."""
    flags = {is_hidden(p) for p in items_paths(items)}
    if len(flags) > 1:
        raise EvalError("open and hidden items files are never mixed in one run")
    return flags.pop()


def read_items_files(items):
    """Rows of one or several items files, in the order given; ids are unique across all of them."""
    items_hidden(items)
    rows = []
    for p in items_paths(items):
        rows += read_jsonl(p)
    return validate_items(rows)


def items_digest(items):
    """sha256 of one items file; for several files the sha256 of their sha256 lines in the order given."""
    paths = items_paths(items)
    if len(paths) == 1:
        return sha256_file(paths[0])
    return sha256_bytes("".join(sha256_file(p) + "\n" for p in paths).encode())


def items_files_record(items, rows=None):
    """Manifest field for several items files (None for one file: its manifest is unchanged): name, sha256, count."""
    paths = items_paths(items)
    if len(paths) == 1:
        return None
    return [{"name": Path(p).name, "sha256": sha256_file(p), "count": len(read_jsonl(p))} for p in paths]


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


MIN_GROUP = 5


def hidden_kind_groups(counts):
    """Kinds that a hidden report may show: -> (shown, other). shown: {kind: n} for kinds with at least MIN_GROUP items;
    other: the kinds merged into 'other' (a list). The remainder is empty or has at least MIN_GROUP items, so the total
    minus the shown kinds never isolates fewer than MIN_GROUP items; the smallest shown kinds are merged until that holds."""
    shown = {k: n for k, n in counts.items() if n >= MIN_GROUP}
    other = [k for k in counts if k not in shown]
    while shown and 0 < sum(counts[k] for k in other) < MIN_GROUP:
        k = min(shown, key=lambda x: (shown[x], x))
        del shown[k]
        other.append(k)
    return shown, sorted(other)


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
    out = {k: v for k, v in manifest.items() if k not in MANIFEST_VOLATILE and k != "run"}
    if isinstance(out.get("model_build"), dict):   # the load stamp is not part of the model's identity
        out["model_build"] = {k: v for k, v in out["model_build"].items() if k not in LOAD_STAMP_FIELDS}
    return out


def load_stamp(manifest):
    """The server's load stamp of the model a run used (None when the run recorded no server listing)."""
    b = manifest.get("model_build")
    if not isinstance(b, dict):
        return None
    st = {k: b[k] for k in LOAD_STAMP_FIELDS if k in b}
    return json.dumps(st, sort_keys=True) if st else None


def note_reload(old, manifest, out):
    """On a resume: when only the load stamp of the same model differs, say so in the log and keep a `reloads` list in the manifest."""
    reloads = list(old.get("reloads") or [])
    o, n = load_stamp(old), load_stamp(manifest)
    if o != n:
        print(f"model reloaded: {o or 'none'} -> {n or 'none'}", file=out, flush=True)
        reloads.append({"old": o, "new": n, "at": datetime.now(timezone.utc).isoformat(timespec="seconds")})
    if reloads:
        manifest["reloads"] = reloads


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
    """items_file: one path or a list of paths (several files add the field items_files; one file adds nothing)."""
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
    thorough = thorough_profile(a)
    if thorough is not None:
        reasons.append(thorough.unmeasured_reason())
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
        "items_sha256": items_digest(items_file), "items_count": n_items, "subset": subset,
        "replay": replay_info, "hidden": is_hidden(a.out), "prompt_hash_form": PROMPT_HASH_FORM,
        "started": datetime.now(timezone.utc).isoformat(timespec="seconds"), "ended": None,
    }
    # The M4 label: pipeline.is_reference knows only the v3 in-process build and stays as it is (reasons above).
    listed = listing.get("id") if listing else None
    m["m4_config"] = a.config
    m["model_listed"] = listed
    m["m4_reference_model"] = M4_REFERENCE_MODEL
    m["is_m4_reference"] = bool(ident.get("backend") == M4_REFERENCE_BACKEND and not m["adapter"]
                                and listed == M4_REFERENCE_MODEL)
    if a.mode == "closed":
        from kafka_sre_rag import prompts
        m["closed_book"] = {"prompt": "prompts.SYSTEM_PROMPT and the bare question (as the v3 plain base)",
                            "system_prompt_sha256": sha256_bytes(prompts.SYSTEM_PROMPT.encode("utf-8")),
                            "search_terms_call": False, "retrieval": False, "pack": "empty"}
    m.update(profiles.manifest_fields(profile))  # nothing is added for the frozen profile
    if thorough is not None:  # nothing is added without the answer profile
        m["answer_profile"] = thorough.manifest()
    rec = items_files_record(items_file)
    if rec is not None:
        m["items_files"] = rec
    return m


def messages_fingerprint(messages):
    """(sha256 hex, total characters of the contents) of a message list in the canonical form PROMPT_HASH_FORM."""
    canon = [{"role": m["role"], "content": m["content"]} for m in messages]
    blob = json.dumps(canon, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return sha256_bytes(blob), sum(len(m["content"]) for m in canon)


class RecordingGen:
    """Wraps a generator and keeps the messages of the search-terms call (the one that starts with EXPAND_SYSTEM)."""

    def __init__(self, gen):
        self._gen = gen
        self.expand_messages = None

    def __getattr__(self, name):
        return getattr(self._gen, name)

    def chat(self, messages, *args, **kw):
        from kafka_sre_rag import prompts
        if messages and messages[0]["content"] == prompts.EXPAND_SYSTEM:
            self.expand_messages = [dict(m) for m in messages]
        return self._gen.chat(messages, *args, **kw)


def prompt_hash_rows(a_rows):
    """Per id: (answer prompt sha256 or None, search-terms prompt sha256 or None); None = not recorded."""
    return {r["id"]: (r.get("prompt_sha256"), r.get("search_terms_prompt_sha256")) for r in a_rows}


def compare_prompt_hashes(rows_a, rows_b):
    """Compare two answer files by id. Returns dict: common, equal, differ (ids), not_recorded (ids lacking the field in
    either file), only_a, only_b. The search-terms hash is compared too where both files have one."""
    A, B = prompt_hash_rows(rows_a), prompt_hash_rows(rows_b)
    res = {"common": 0, "equal": [], "differ": [], "not_recorded": [],
           "only_a": sorted(set(A) - set(B)), "only_b": sorted(set(B) - set(A))}
    for iid in sorted(set(A) & set(B)):
        res["common"] += 1
        (pa, sa), (pb, sb) = A[iid], B[iid]
        if pa is None or pb is None:
            res["not_recorded"].append(iid)
        elif pa != pb or (sa is not None and sb is not None and sa != sb):
            res["differ"].append(iid)
        else:
            res["equal"].append(iid)
    return res


def cmd_compare_prompts(a, out=None):
    out = out or sys.stdout
    if is_hidden(a.a) or is_hidden(a.b):
        raise EvalError("compare-prompts reads open answer files only")
    r = compare_prompt_hashes(read_jsonl(a.a), read_jsonl(a.b))
    print(f"[compare-prompts] {r['common']} ids in both: {len(r['equal'])} equal, {len(r['differ'])} differ, "
          f"{len(r['not_recorded'])} not recorded; only in A {len(r['only_a'])}, only in B {len(r['only_b'])}", file=out)
    for key in ("differ", "not_recorded"):
        for iid in r[key]:
            print(f"  {key}: {iid}", file=out)
    return 0


def run_id(m):
    blob = json.dumps(comparable(m), sort_keys=True, ensure_ascii=False).encode()
    return f"{m['config']}-{sha256_bytes(blob)[:10]}"


# ---------------------------------------------------------------- generator

class StubGen:
    """--backend stub: no model; a fixed expansion and a fixed answer."""
    backend = "stub"
    identity = {"backend": "stub", "model": "stub", "adapter": None}

    def chat(self, messages, max_tokens, on_token=None, **kw):
        from kafka_sre_rag import prompts
        if messages[0]["content"] == prompts.EXPAND_SYSTEM:
            return "consumer group, kafka-consumer-groups.sh, --describe", 0, False
        if messages[0]["content"] == prompts.VERIFY_SYSTEM:  # the checking pass of the thorough profile
            return '{"replacements": []}', 0, False
        return "[stub answer: no model was called]", 0, False


def thorough_profile(a):
    """The EXPERIMENTAL answer profile of the options (kafka_sre_rag.verify_pass.Profile), or None when it is off."""
    if not getattr(a, "answer_profile", None):
        return None
    from kafka_sre_rag.verify_pass import Profile
    kw = {} if getattr(a, "think_cap", None) is None else {"thinking_tokens": a.think_cap}   # M7 K4: the thinking cap (tokens added to the answer limit)
    return Profile(thinking=not a.no_thinking, verify=not a.no_verify, cards=not a.no_cards, **kw)


def thorough_row(r):
    """The fields an answer row gets under the answer profile (none without it)."""
    from kafka_sre_rag import verify_pass
    th = r["thorough"]
    info = th["draft_info"] or {}
    rec = th["record"]
    return {"first_pass_text": th["first_pass_text"],
            "tokens_detail": {"draft": th["tokens"], "draft_reasoning": info.get("reasoning_tokens"),
                              "draft_reasoning_chars": info.get("reasoning_chars"),
                              "draft_max_tokens": th["max_tokens"],
                              "check": (rec or {}).get("r2", {}).get("tokens")},
            "thorough": verify_pass.compact(rec), "thorough_note": verify_pass.summary_line(rec),
            "draft_info": info, "cut_while_thinking": th["cut_while_thinking"], "draft_streamed": th["draft_streamed"]}


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
    if hidden:   # id, timings and whether the row is valid; the token count shows which answers were cut, so it is a total only
        return (f"[answer] {i}/{n} id={iid} search={s['search_terms']:.1f}s retrieval={s['retrieval']:.1f}s "
                f"answer={s['answer']:.1f}s total={s['total']:.1f}s {tail}")
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
    src_m = read_manifest(path)
    if (src_m or {}).get("mode") == "closed":
        raise EvalError("--replay-packs: the other run is a closed-book run and has no packs")
    src = {r["id"]: r for r in read_jsonl(path)}
    missing = [it["id"] for it in items if not src.get(it["id"], {}).get("pack")
               or not isinstance(src[it["id"]]["pack"], list) or src[it["id"]].get("error")]
    if missing:
        raise EvalError(f"--replay-packs: {len(missing)} items have no stored pack in the other run "
                        f"(first: {missing[0]})")
    return {i: src[i]["pack"] for i in src}


def revise_out_path(answers):
    p = str(answers)
    return p[:-len(".jsonl.enc")] + ".revise.jsonl.enc" if p.endswith(".jsonl.enc") else (
        p[:-len(".jsonl")] + ".revise.jsonl" if p.endswith(".jsonl") else p + ".revise.jsonl")


def cmd_revise(a, out=sys.stdout):
    """--revise-from: the revise step on stored answers and packs. No retrieval, no first-pass generation."""
    from kafka_sre_rag import config, evalhooks, revise as rv, service
    src_path = Path(a.revise_from)
    a.out = a.out or revise_out_path(src_path)
    hidden = items_hidden(a.items)
    if not (hidden == is_hidden(a.out) == is_hidden(src_path)):
        raise EvalError("hidden items, the stored answers and the records file must all end in .enc, or none")
    if Path(a.out).resolve() == src_path.resolve():
        raise EvalError("--out is the stored answers file; the stored answers are never changed")
    if not a.catalogue:
        raise EvalError("--revise-from needs --catalogue DIR (the catalogues of the check)")
    items = read_items_files(a.items)
    if a.limit:
        items = items[: a.limit]
    stored = {r["id"]: r for r in read_jsonl(src_path)}
    missing = [it["id"] for it in items if not stored.get(it["id"], {}).get("text")
               or not isinstance(stored[it["id"]].get("pack"), list) or stored[it["id"]].get("error")]
    if missing:
        raise EvalError(f"--revise-from: {len(missing)} items have no stored answer and pack (first: {missing[0]})")
    cfg = config.load()
    gen = None
    if a.r2:
        if a.backend != "stub":
            config.verify(cfg=cfg)
        gen = make_generator(a)
    try:
        checker = service.AnswerChecker(a.catalogue)
    except service.CatalogueError as e:
        raise EvalError(str(e))
    reviser = rv.Reviser(checker.check, gen, a.max_tokens)
    ident = getattr(gen, "identity", None) or {}
    commit, dirty = git_info()
    manifest = {"harness": "eval2", "kind": "revise", "config": a.config, "answers_sha256": sha256_file(src_path),
                "answers_run": (read_manifest(src_path) or {}).get("run"), "items_sha256": items_digest(a.items),
                "items_count": len(items), "r2": bool(a.r2), "backend": ident.get("backend"),
                "model_requested": ident.get("model"), "max_tokens": a.max_tokens or rv.REVISE_MAX_TOKENS,
                "checker_catalogue_hash": checker.catalogue_hash, "revise_version": rv.VERSION,
                "revise_layout": rv.LAYOUT, "revise_prompt_sha256": rv.prompt_sha256(),
                "runtime_git_commit": commit, "runtime_tree_dirty": dirty, "hidden": hidden,
                "started": datetime.now(timezone.utc).isoformat(timespec="seconds"), "ended": None}
    if items_files_record(a.items) is not None:
        manifest["items_files"] = items_files_record(a.items)
    manifest["run"] = run_id(manifest)
    store = AnswerStore(a.out)
    old = read_manifest(a.out)
    if old is not None:
        if comparable(old) != comparable(manifest):
            diff = sorted(k for k in set(comparable(old)) | set(comparable(manifest)) if comparable(old).get(k) != comparable(manifest).get(k))
            if getattr(a, "allow_commit_change", False) and diff == ["runtime_git_commit"]:   # as in cmd_answer: the caller vouches for the code
                first = old.get("runtime_git_commits") or [old.get("runtime_git_commit")]
                manifest["runtime_git_commits"] = first + [manifest["runtime_git_commit"]]
            else:
                raise EvalError("manifest differs from the existing records file; not resuming")
        elif old.get("runtime_git_commits"):
            manifest["runtime_git_commits"] = old["runtime_git_commits"]
        manifest["run"] = old["run"]
        manifest["started"] = old["started"]
    elif store.rows:
        raise EvalError("records file has rows but no manifest; not resuming")
    write_manifest(a.out, manifest)
    store.drop_failed()
    done = {r["id"] for r in store.rows}
    todo = [it for it in items if it["id"] not in done]
    print(f"[revise] run {manifest['run']}: {len(items)} items, {len(done)} already done, {len(todo)} to do",
          file=out, flush=True)
    consecutive = 0
    for i, it in enumerate(todo, 1):
        t0 = time.monotonic()
        row = {"id": it["id"], "error": None, "run": manifest["run"]}
        try:
            chunks = evalhooks.chunks_by_ids(stored[it["id"]]["pack"])
            rec = reviser.revise(stored[it["id"]]["text"], chunks, r2=bool(a.r2))
            row.update(rec)
            consecutive = 0
        except Exception as e:  # recorded; the run continues
            row["error"] = one_line(e)
            consecutive += 1
        row["seconds"] = {"total": time.monotonic() - t0}
        store.add(row)
        if hidden:
            print(f"[revise] {i}/{len(todo)} id={it['id']} total={row['seconds']['total']:.1f}s "
                  f"{'FAILED' if row['error'] else 'ok'}", file=out, flush=True)
        else:
            st = (row.get("r2") or {}).get("status", "-")
            print(f"[revise] {i}/{len(todo)} id={it['id']} flagged={row.get('flagged', '-')} r2={st} "
                  f"total={row['seconds']['total']:.1f}s {'FAILED' if row['error'] else 'ok'}"
                  + ("" if not row["error"] else " " + row["error"]), file=out, flush=True)
        if a.max_consecutive_errors and consecutive >= a.max_consecutive_errors:
            print(f"[revise] stopping: {consecutive} failed items in a row", file=out, flush=True)
            return 1
    manifest["ended"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    write_manifest(a.out, manifest)
    print(f"[revise] done {len(store.rows)} rows, {sum(1 for r in store.rows if r['error'])} errors",
          file=out, flush=True)
    if hidden:   # per-item values are not printed during the run; their totals are, once
        good = [r for r in store.rows if not r["error"]]
        r2 = Counter((r.get("r2") or {}).get("status", "-") for r in good)
        print(f"[revise] totals over {len(good)} valid rows: flagged {sum(r.get('flagged') or 0 for r in good)}, "
              "r2 " + ", ".join(f"{k} {v}" for k, v in sorted(r2.items(), key=lambda kv: str(kv[0]))), file=out, flush=True)
    return 0


def cmd_answer(a, out=sys.stdout):
    if getattr(a, "revise_from", None):
        if getattr(a, "answer_profile", None):
            raise EvalError("--answer-profile cannot be used with --revise-from (use python -m kafka_sre_rag.verify_pass run)")
        return cmd_revise(a, out)
    if not a.out:
        raise EvalError("--out is required")
    hidden = items_hidden(a.items)
    if hidden != is_hidden(a.out):
        raise EvalError("hidden items need an answers file ending in .enc, and the other way round")
    items = read_items_files(a.items)
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
    closed = a.mode == "closed"
    if closed and a.replay_packs:
        raise EvalError("--replay-packs is refused for a closed-book run (no packs are used)")
    if closed and (profile != "frozen" or a.k != "k6" or a.no_rerank):
        raise EvalError("--mode closed uses no retrieval: --profile, --k and --no-rerank do not apply")
    thorough = thorough_profile(a)
    th_kw = {}
    if thorough is not None:  # EXPERIMENTAL answer profile; without it nothing below changes
        from kafka_sre_rag import service
        if closed:
            raise EvalError("--answer-profile needs excerpts; it cannot be used with --mode closed")
        # M7 K4: --no-thinking with --no-verify is allowed here (arm A of the six-arm test: today's call on the profile's transport, so that every arm
        # shares it); ask and chat still refuse it
        if not a.catalogue:
            raise EvalError("--answer-profile needs --catalogue DIR (the catalogues of the checking pass)")
        ctk = (a.extra_body or {}).get("chat_template_kwargs") if isinstance(a.extra_body, dict) else None
        if not (isinstance(ctk, dict) and ctk.get("enable_thinking") is False):
            # M7 P1 review S7: the profile sets the key per call; arm A, run with the same --extra-body, then sends
            # the very bytes the profile's calls with thinking off send
            raise EvalError("--answer-profile needs --extra-body to set chat_template_kwargs.enable_thinking to false "
                            "(as arm A runs), so that the arms differ only in their switch")
        try:
            th_kw = {"thorough": thorough, "thorough_checker": service.AnswerChecker(a.catalogue)}
        except service.CatalogueError as e:
            raise EvalError(str(e))
        if thorough.verify and thorough.cards:  # the lookup is loaded once for the run (hash checked)
            from kafka_sre_rag.kb_lookup import KbLookup
            ck = th_kw["thorough_checker"]
            th_kw["kb"] = KbLookup(ck.cats, ck.directory, verify=True)
    elif (getattr(a, "no_thinking", False) or getattr(a, "no_verify", False) or getattr(a, "no_cards", False)
          or getattr(a, "think_cap", None) is not None):
        raise EvalError("--no-thinking, --no-verify, --no-cards and --think-cap need --answer-profile thorough")
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
            if getattr(a, "allow_commit_change", False) and diff == ["runtime_git_commit"]:
                # only the head commit differs and the caller vouches for the code (the E2 chain pins the files): resume, record every commit
                first = old.get("runtime_git_commits") or [old.get("runtime_git_commit")]
                manifest["runtime_git_commits"] = first + [manifest["runtime_git_commit"]]
                manifest["run"] = old["run"]   # the run keeps its identity (replays and reports refer to it)
            else:
                raise EvalError(f"manifest differs from the existing run in {diff}; not resuming")
        elif old.get("runtime_git_commits"):
            manifest["runtime_git_commits"] = old["runtime_git_commits"]
        manifest["run"] = old["run"]   # the run keeps its identity across a resume (a model reload does not change it)
        note_reload(old, manifest, out)
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
        if closed:
            row["closed_book"] = True  # pack stays [] below: no retrieval was made
        t0 = time.monotonic()
        rec = RecordingGen(gen)
        try:
            r = evalhooks.run_item(full_question(it), rec, cfg, k=a.k, mode=a.mode, rerank=not a.no_rerank,
                                   max_tokens=a.max_tokens, pack_ids=None if replay is None else replay[it["id"]],
                                   **({} if profile == "frozen" else {"profile": profile}), **th_kw)
            row.update(text=r["text"], tokens=r["tokens"] or None, truncated=r["truncated"],
                       seconds=r["seconds"], search_terms=r["search_terms"], pack=r["pack"])
            if "thorough" in r:
                row.update(thorough_row(r))
            row["prompt_sha256"], row["prompt_chars"] = messages_fingerprint(r["messages"])
            if rec.expand_messages is not None:
                row["search_terms_prompt_sha256"], row["search_terms_prompt_chars"] = messages_fingerprint(rec.expand_messages)
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
    if hidden:
        good = [r for r in store.rows if not r["error"]]
        print(f"[answer] totals over {len(good)} valid rows: tokens {sum(r.get('tokens') or 0 for r in good)}, "
              f"truncated {sum(1 for r in good if r.get('truncated'))}", file=out, flush=True)
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


SHARES_VARIANT = "shares_variant_with_published"


def items_breakdown(items, a_rows, hidden, P):
    """Totals per items file and per kind from the tier A score rows; counts only, never ids or text. A file whose
    items carry `shares_variant_with_published` also gets its total without those items."""
    if items_hidden(items) != hidden:
        raise EvalError("--items must be hidden exactly when the answers file is hidden")
    by_id = {s["id"]: s for s in a_rows}
    for p in items_paths(items):
        rows = read_jsonl(p)
        pairs = [(it, by_id[it["id"]]) for it in rows if it["id"] in by_id]
        k = sum(1 for _, s in pairs if s["all_pass"])
        P(f"items file {Path(p).name}: {len(rows)} items, scored {len(pairs)}, all-pass {k}/{len(pairs)}")
        kinds = sorted({it["kind"] for it, _ in pairs})
        if hidden:   # a kind is shown only when it and the remainder each hold at least MIN_GROUP items
            shown, other = hidden_kind_groups(Counter(it["kind"] for it, _ in pairs))
            groups = [(k, [k]) for k in sorted(shown)] + ([("other", other)] if other else [])
        else:
            groups = [(k, [k]) for k in kinds]
        for label, members in groups:
            ks = [s for it, s in pairs if it["kind"] in members]
            P(f"  kind {label}: scored {len(ks)}, all-pass {sum(1 for s in ks if s['all_pass'])}/{len(ks)}")
        tagged = [s for it, s in pairs if it.get(SHARES_VARIANT)]
        if tagged:
            rest = [s for it, s in pairs if not it.get(SHARES_VARIANT)]
            P(f"  total with the {len(tagged)} items tagged {SHARES_VARIANT}: all-pass {k}/{len(pairs)}; "
              f"without them: all-pass {sum(1 for s in rest if s['all_pass'])}/{len(rest)}")


def report_one(path, scores=None, out=sys.stdout, items=None):
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
        if items:
            items_breakdown(items, a_rows, hidden, P)
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
        report_one(p, sc, out, a.items)
    return 0


# ---------------------------------------------------------------- cli

def build_parser():
    from kafka_sre_rag import profiles   # the profile choices come from the profiles module (kafka_sre_rag/candidate.json)
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
    an.add_argument("--items", required=True, nargs="+",
                    help="items file(s), all open or all hidden (.enc); several files are joined in the order given")
    an.add_argument("--out", help="answers file (required, except with --revise-from)")
    an.add_argument("--config", required=True, help="configuration name")
    an.add_argument("--backend", choices=["mlx", "openai", "stub"], default="openai")
    an.add_argument("--model")
    an.add_argument("--adapter")
    an.add_argument("--base-url", help="openai: server URL (default: OMLX_URL)")
    an.add_argument("--extra-body", type=json.loads, default=None, metavar="JSON")
    an.add_argument("--max-tokens", type=int, default=None)
    an.add_argument("--mode", choices=["default", "strict", "closed", "grounded-g1", "grounded-g2", "grounded-g3"], default="default",
                    help="closed: closed book (no retrieval, empty pack; the plain-base prompt); grounded-g1 and "
                         "grounded-g2 / grounded-g3: EXPERIMENTAL grounded prompts (unmeasured, recorded in the manifest)")
    an.add_argument("--k", choices=["k6", "k3"], default="k6")
    an.add_argument("--profile", choices=list(profiles.PROFILES), default="frozen",
                    help="retrieval profile (default frozen); the others (r1 to r4 and the r4 settings) are EXPERIMENTAL, unmeasured candidates "
                         "(kafka_sre_rag/candidate.json) and are recorded in the manifest")
    an.add_argument("--no-rerank", action="store_true", help="BM25 order only (unmeasured)")
    an.add_argument("--catalogue", help="catalogue directory to hash into the manifest")
    an.add_argument("--host-class", default=None, help='free text, for example "Mac Studio"')
    an.add_argument("--replay-packs", help="answers file of another run: use its stored chunk ids")
    an.add_argument("--allow-commit-change", action="store_true", help="resume an unfinished answers file when only runtime_git_commit differs; the manifest lists every commit (runtime_git_commits)")
    an.add_argument("--revise-from", metavar="ANSWERS",
                    help="run the revise step (R1 marks) on the stored answers and packs of this answers file; "
                         "no generation; records go to --out (default: next to the answers). Needs --catalogue")
    an.add_argument("--answer-profile", choices=["thorough"], default=None,
                    help="EXPERIMENTAL, unmeasured: the draft with thinking on, then the checking pass "
                         "(kafka_sre_rag/verify_pass.py); needs --catalogue. Rows then also carry first_pass_text, "
                         "tokens_detail, the pass's record (thorough) and seconds draft, thinking and check")
    an.add_argument("--no-thinking", action="store_true", help="with --answer-profile: check only (draft without thinking)")
    an.add_argument("--no-verify", action="store_true", help="with --answer-profile: thinking only (no checking pass)")
    an.add_argument("--no-cards", action="store_true", help="with --answer-profile: no reference cards in the check")
    an.add_argument("--think-cap", type=int, default=None, metavar="N",
                    help="with --answer-profile: tokens of thinking added to the answer limit (default: the profile's 1,500)")
    an.add_argument("--r2", action="store_true",
                    help="with --revise-from: also the R2 patch call (one model call per flagged answer; "
                         "--backend and --model as for answers)")
    an.add_argument("--subset", choices=["smoke"])
    an.add_argument("--smoke-a", type=int, default=60)
    an.add_argument("--smoke-b", type=int, default=24)
    an.add_argument("--seed", type=int, default=1, help="seed of the smoke subset")
    an.add_argument("--smoke-file", default=str(SMOKE_FILE))
    an.add_argument("--rebuild-smoke", action="store_true")
    an.add_argument("--limit", type=int, default=None, help="first N items only (recorded in the manifest)")
    an.add_argument("--max-consecutive-errors", type=int, default=5, help="stop after N failures in a row (0: never)")
    cp = sub.add_parser("compare-prompts", help="compare the prompt hashes of two open answer files by id")
    cp.add_argument("a")
    cp.add_argument("b")
    r = sub.add_parser("report")
    r.add_argument("answers", nargs="+")
    r.add_argument("--items", nargs="*", default=None,
                   help="items file(s) of the run (all open or all hidden): adds totals per items file and per kind")
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
            shown, other = hidden_kind_groups(counts_by_cell(hd))   # a cell is shown only if it and the remainder hold >= 5 items
            hcells = {k: n for k, n in counts_by_cell(hd).items() if k in shown}
            if other:
                hcells["other"] = sum(n for k, n in counts_by_cell(hd).items() if k in other)
            print("hidden by tier/kind: " + json.dumps(hcells))
        elif a.cmd == "answer":
            return cmd_answer(a)
        elif a.cmd == "compare-prompts":
            return cmd_compare_prompts(a)
        elif a.cmd == "report":
            return cmd_report(a)
    except EvalError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
