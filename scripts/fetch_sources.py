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
        if target.exists():
            run(["git", "pull", "--depth", "1"], cwd=target)
            continue
        run(["git", "clone", "--depth", "1", "--filter=blob:none", "--sparse",
             src["repo"], str(target)])
        run(["git", "sparse-checkout", "set", *src["paths"]], cwd=target)
        commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=target, text=True).strip()
        (target / ".source-commit").write_text(commit + "\n")


if __name__ == "__main__":
    main()
