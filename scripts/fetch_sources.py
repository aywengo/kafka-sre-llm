#!/usr/bin/env python3
"""Shallow, sparse clone of every git source listed in sources.yaml."""
import argparse
import re
import subprocess
import sys
from pathlib import Path

import yaml

COMMIT_HASH = re.compile(r"[0-9a-f]{40}")   # a `ref` that is a full commit hash, not a tag


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
        # set on every run, not only on clone: an existing clone must pick up
        # changed paths (e.g. a version switch from content/en/44 to 43)
        # `files` and `attribute_files` are single files. Cone mode always checks
        # out files at the repository root; a file elsewhere needs its directory
        # in the cone unless one of `paths` already covers it.
        files = [*src.get("files", []), *src.get("attribute_files", [])]
        file_dirs = [d for d in dict.fromkeys(Path(f).parent.as_posix() for f in files)
                     if d != "." and not any(d == p or d.startswith(p.rstrip("/") + "/") for p in src["paths"])]
        run(["git", "sparse-checkout", "set", *src["paths"], *file_dirs], cwd=target)
        if ref and COMMIT_HASH.fullmatch(ref):
            run(["git", "fetch", "--depth", "1", "origin", ref], cwd=target)
            run(["git", "checkout", "--quiet", ref], cwd=target)
        elif ref:
            run(["git", "fetch", "--depth", "1", "origin", "tag", ref], cwd=target)
            run(["git", "checkout", "--quiet", ref], cwd=target)
        else:
            run(["git", "fetch", "--depth", "1", "origin"], cwd=target)
            run(["git", "reset", "--hard", "--quiet", "FETCH_HEAD"], cwd=target)
        missing = [p for p in [*src["paths"], *files] if not (target / p).exists()]
        if missing:
            sys.exit(f"{src['name']}: configured paths missing after checkout: {', '.join(missing)}")
        commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=target, text=True).strip()
        (target / ".source-commit").write_text(commit + "\n")
        (target / ".source-ref").write_text((ref or "default branch") + "\n")


if __name__ == "__main__":
    main()
