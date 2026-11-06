#!/usr/bin/env python3
"""Shallow, sparse clone of every git source listed in sources.yaml."""
import argparse
import subprocess
from pathlib import Path

import yaml


def run(cmd, cwd=None):
    print("+", " ".join(cmd))
    subprocess.run(cmd, cwd=cwd, check=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="sources.yaml")
    ap.add_argument("--dest", default="raw")
    ap.add_argument("--only", nargs="*", help="limit to these source names")
    args = ap.parse_args()

    cfg = yaml.safe_load(Path(args.config).read_text())
    dest = Path(args.dest)
    dest.mkdir(exist_ok=True)

    for src in cfg["sources"]:
        if "repo" not in src:
            continue
        if args.only and src["name"] not in args.only:
            continue
        target = dest / src["name"]
        ref = src.get("ref")
        if not target.exists():
            run(["git", "clone", "--depth", "1", "--filter=blob:none", "--sparse",
                 src["repo"], str(target)])
            run(["git", "sparse-checkout", "set", *src["paths"]], cwd=target)
        if ref:
            run(["git", "fetch", "--depth", "1", "origin", "tag", ref], cwd=target)
            run(["git", "checkout", "--quiet", ref], cwd=target)
        else:
            run(["git", "pull", "--depth", "1"], cwd=target)
        commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=target, text=True).strip()
        (target / ".source-commit").write_text(commit + "\n")
        (target / ".source-ref").write_text((ref or "default branch") + "\n")


if __name__ == "__main__":
    main()
