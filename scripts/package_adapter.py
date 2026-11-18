#!/usr/bin/env python3
"""Assemble the upload directory for the v2 retrieval-only adapter (CPU only, no network, no upload).

Writes <out-dir> (default out/kafka-sre-27b-v2-retrieval-only) with exactly:

    adapters.safetensors   copied from the adapter folder (never a numbered checkpoint)
    adapter_config.json    cleaned copy of the adapter's config (local paths removed, keys listed)
    README.md              model_cards/kafka-sre-27b-v2-retrieval-only.md
    NOTICE                 model_cards/NOTICE.kafka-sre-27b-v2-retrieval-only
    LICENSE                copy of the root LICENSE
    SHA256SUMS             sha256 of the five files above

Refuses to run if the output directory holds any other file, or if the config, card or notice contains
a home path, an IPv4 literal, or the content of a file in ~/.config/kafka-sre-llm/ (never printed).

Usage: .venv/bin/python scripts/package_adapter.py [--adapter-dir adapters/kafka-sre-v2-resume]
       [--out-dir out/kafka-sre-27b-v2-retrieval-only]
"""
import argparse
import hashlib
import json
import re
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WEIGHTS = "adapters.safetensors"
CONFIG = "adapter_config.json"
ALLOWED = {WEIGHTS, CONFIG, "README.md", "NOTICE", "LICENSE", "SHA256SUMS"}
# Keys of the training config that only name local paths of the machine that trained the adapter.
# mlx-lm reads fine_tune_type, num_layers and lora_parameters when it loads an adapter.
LOCAL_KEYS = ("adapter_path", "config", "data", "resume_adapter_file")
BASE_MODEL = "mlx-community/Qwen3.8-27B-bf16"
HOME_RE = re.compile(r"/Users/[^/\s\"']+|/home/[^/\s\"']+|[A-Za-z]:\\Users\\")
IPV4_RE = re.compile(r"(?<![\d.])(?:(?:25[0-5]|2[0-4]\d|1?\d?\d)\.){3}(?:25[0-5]|2[0-4]\d|1?\d?\d)(?![\d.])")


class PackageError(RuntimeError):
    pass


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def secret_values(config_dir):
    """(file name, content) of each non-empty file in the secrets directory; contents are never printed."""
    d = Path(config_dir).expanduser()
    out = []
    if d.is_dir():
        for p in sorted(d.iterdir()):
            if p.is_file():
                try:
                    v = p.read_text(errors="ignore").strip()
                except OSError:
                    continue
                if v:
                    out.append((p.name, v))
    return out


def scan_text(label, text, secrets, home=None):
    """Raise PackageError if the text holds a home path, an IPv4 literal or a secret. Never echoes a secret."""
    problems = []
    if HOME_RE.search(text) or (home and str(home) in text):
        problems.append("a home path")
    if IPV4_RE.search(text):
        problems.append("an IPv4 literal")
    for name, v in secrets:
        if v in text:
            problems.append(f"the content of ~/.config/kafka-sre-llm/{name}")
    if problems:
        raise PackageError(f"{label} contains " + ", ".join(problems))


def clean_config(raw):
    """Return (cleaned dict, list of changed keys). The adapter's own config is never edited."""
    cfg = json.loads(raw)
    changed = []
    for k in LOCAL_KEYS:
        if k in cfg:
            del cfg[k]
            changed.append(f"{k} (removed)")
    if "model" in cfg and cfg["model"] != BASE_MODEL:
        cfg["model"] = BASE_MODEL
        changed.append("model (set to the public conversion name)")
    return cfg, changed


def check_out_dir(out):
    if out.exists():
        extra = sorted(p.name for p in out.iterdir() if p.name not in ALLOWED)
        if extra:
            raise PackageError(f"{out} holds files that do not belong in the package: {extra}")


def write_sums(out):
    names = sorted(ALLOWED - {"SHA256SUMS"})
    lines = [f"{sha256_file(out / n)}  {n}" for n in names]
    (out / "SHA256SUMS").write_text("\n".join(lines) + "\n")
    return lines


def build(adapter_dir, out, card, notice, license_file, config_dir, home=None):
    adapter_dir, out = Path(adapter_dir), Path(out)
    for p in (adapter_dir / WEIGHTS, adapter_dir / CONFIG, card, notice, license_file):
        if not Path(p).is_file():
            raise PackageError(f"missing input: {p.name if hasattr(p, 'name') else p}")
    check_out_dir(out)
    secrets = secret_values(config_dir)
    cfg, changed = clean_config((adapter_dir / CONFIG).read_text())
    cleaned = json.dumps(cfg, indent=4) + "\n"
    scan_text(CONFIG + " (after cleaning)", cleaned, secrets, home)
    scan_text("the model card", Path(card).read_text(), secrets, home)
    scan_text("the notice", Path(notice).read_text(), secrets, home)
    out.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(adapter_dir / WEIGHTS, out / WEIGHTS)
    (out / CONFIG).write_text(cleaned)
    shutil.copyfile(card, out / "README.md")
    shutil.copyfile(notice, out / "NOTICE")
    shutil.copyfile(license_file, out / "LICENSE")
    lines = write_sums(out)
    found = sorted(p.name for p in out.iterdir())
    if set(found) != ALLOWED:
        raise PackageError(f"output holds {found}, expected {sorted(ALLOWED)}")
    return lines, changed


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--adapter-dir", default="adapters/kafka-sre-v2-resume")
    ap.add_argument("--out-dir", default="out/kafka-sre-27b-v2-retrieval-only")
    ap.add_argument("--root", default=str(ROOT), help="repository root (default: this repository)")
    ap.add_argument("--secrets-dir", default="~/.config/kafka-sre-llm")
    a = ap.parse_args(argv)
    root = Path(a.root)

    def rel(p):
        p = Path(p)
        return p if p.is_absolute() else root / p

    out = rel(a.out_dir)
    lines, changed = build(
        rel(a.adapter_dir), out,
        root / "model_cards/kafka-sre-27b-v2-retrieval-only.md",
        root / "model_cards/NOTICE.kafka-sre-27b-v2-retrieval-only",
        root / "LICENSE", a.secrets_dir, home=Path.home())
    print(f"wrote {out}")
    for n in sorted(ALLOWED):
        print(f"  {n}  {(out / n).stat().st_size} bytes  {sha256_file(out / n)}")
    print("adapter_config.json keys changed: " + (", ".join(changed) if changed else "none"))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except PackageError as e:
        print(f"package_adapter: {e}", file=sys.stderr)
        sys.exit(1)
