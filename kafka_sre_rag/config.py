"""The frozen configuration (frozen.json) and its verification. Stdlib only."""
import hashlib
import json
from pathlib import Path

PKG = Path(__file__).resolve().parent
ROOT = PKG.parent
FROZEN = PKG / "frozen.json"
PROMPT_NAMES = ("SYSTEM_PROMPT", "EXPAND_SYSTEM", "INSTRUCTION", "STRICT")


class ConfigError(RuntimeError):
    pass


def load():
    with open(FROZEN, encoding="utf-8") as f:
        return json.load(f)


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def index_file(cfg=None):
    return ROOT / (cfg or load())["index"]["index_path"]


def prompt_hashes():
    from . import prompts
    return {n: hashlib.sha256(getattr(prompts, n).encode("utf-8")).hexdigest() for n in PROMPT_NAMES}


def reranker_revision_cached(cfg):
    """Revision the local Hugging Face cache holds for the reranker, or None if not cached. Returns the pinned
    revision when its snapshot directory exists (the state after the runtime's own first download, which writes
    no refs/main); otherwise what refs/main resolves to."""
    import os
    home = Path(os.environ.get("HF_HOME", Path.home() / ".cache/huggingface"))
    rr = cfg["retrieval"]["reranker"]
    repo = Path(os.environ.get("HF_HUB_CACHE", home / "hub")) / ("models--" + rr["model"].replace("/", "--"))
    if (repo / "snapshots" / rr["revision"]).is_dir():
        return rr["revision"]
    ref = repo / "refs/main"
    return ref.read_text().strip() if ref.exists() else None


def verify(index_path=None, cfg=None):
    """Recompute the index sha256 and the prompt hashes; raise ConfigError on any mismatch.
    Returns a report dict. index_path None = the frozen index. The reranker revision is checked against
    the local cache when it is there (a missing cache is reported, not an error)."""
    cfg = cfg or load()
    problems, report = [], {}
    path = Path(index_path) if index_path else index_file(cfg)
    want = cfg["index"]["sha256"]
    if not path.exists():
        problems.append(f"index file not found: {path}")
    else:
        got = sha256_file(path)
        report["index"] = {"path": str(path), "sha256": got, "expected": want, "match": got == want}
        if got != want:
            problems.append(f"index sha256 mismatch for {path}: got {got}, frozen {want}")
    got_p = prompt_hashes()
    report["prompts"] = {}
    for n in PROMPT_NAMES:
        ok = got_p[n] == cfg["prompts_sha256"][n]
        report["prompts"][n] = {"sha256": got_p[n], "match": ok}
        if not ok:
            problems.append(f"prompt {n} changed: got {got_p[n]}, frozen {cfg['prompts_sha256'][n]}")
    rev, cached = cfg["retrieval"]["reranker"]["revision"], reranker_revision_cached(cfg)
    report["reranker"] = {"revision": rev, "cached": cached, "match": None if cached is None else cached == rev}
    if cached is not None and cached != rev:
        problems.append(f"reranker revision in the local cache is {cached}, frozen {rev}")
    if problems:
        raise ConfigError("frozen configuration check FAILED:\n  " + "\n  ".join(problems))
    return report


def unmeasured_banner(reasons):
    """'UNMEASURED CONFIGURATION' text for a list of override reasons, or '' when there are none."""
    reasons = [r for r in reasons if r]
    return "UNMEASURED CONFIGURATION: " + "; ".join(reasons) if reasons else ""


DEFAULT_CATALOGUES = ROOT / "work" / "m4" / "catalogues"
BUILD_CATALOGUES_HINT = "python scripts/build_catalogues.py"


def catalogues_line(directory=None):
    """The `verify` line about the catalogues of the answer checker. Never raises: a missing directory is
    reported as 'not built' (the checker is optional)."""
    d = Path(directory) if directory else DEFAULT_CATALOGUES
    manifest = d / "CATALOGUES.json"
    if not d.is_dir() or not manifest.is_file():
        return "catalogues: not built (python scripts/build_catalogues.py)"
    try:
        from .catalogue import manifest_for
        got = manifest_for(d)["catalogue_hash"]
        want = json.loads(manifest.read_text(encoding="utf-8")).get("catalogue_hash")
    except (OSError, ValueError, ImportError):
        return "catalogues: unreadable (rebuild with python scripts/build_catalogues.py)"
    return f"catalogues: {'match' if got == want else 'MISMATCH with CATALOGUES.json'}  {got}"
