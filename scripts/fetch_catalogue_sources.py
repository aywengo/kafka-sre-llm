#!/usr/bin/env python3
"""Fetch the sources of the catalogues listed in catalogue_sources.yaml into raw_catalogue/<name>/.

For each source: a shallow (depth 1), blobless, sparse git clone of the pinned commit into a
temporary directory (public repositories, no token, nothing about the owner is sent), the check
that the tag resolves to the commit written in the YAML, the check of the licence from the
LICENSE file at the tag, and a copy of only the files that match `globs` (plus LICENSE).
raw_catalogue/FETCHED.json records, per source: repository, ref, resolved commit, licence, the
sha256 of every file and of LICENSE, and the sizes. It holds no timestamps, so a second run on
unchanged sources gives the same file.

Stops before writing anything when the selected files of all sources together exceed 50 MB.
A source that cannot be fetched is reported and the others still run (exit code 1 at the end).

Usage: .venv/bin/python scripts/fetch_catalogue_sources.py [--only NAME ...] [--out DIR]
"""
import argparse
import fnmatch
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
LIMIT_BYTES = 50 * 1024 * 1024
VERSION = "fetch_catalogue_sources/1"


class FetchError(Exception):
    pass


def git(args, cwd):
    env = dict(os.environ, GIT_TERMINAL_PROMPT="0", GIT_CONFIG_GLOBAL="/dev/null", GIT_CONFIG_SYSTEM="/dev/null")
    r = subprocess.run(["git"] + args, cwd=str(cwd), env=env, capture_output=True, text=True)
    if r.returncode != 0:
        raise FetchError("git %s failed: %s" % (" ".join(args[:2]), r.stderr.strip()[-400:]))
    return r.stdout


def resolve_tag(repo, ref, commit):
    """The commit that tag `ref` points to (peeled), or None when `ref` is not a tag name."""
    out = git(["ls-remote", repo, "refs/tags/" + ref, "refs/tags/" + ref + "^{}"], cwd=tempfile.gettempdir())
    peeled = plain = None
    for line in out.splitlines():
        sha, name = line.split()
        if name.endswith("^{}"):
            peeled = sha
        else:
            plain = sha
    return peeled or plain


def licence_of(text):
    t = " ".join(text.split())
    if "Apache License" in t and "Version 2.0, January 2004" in t:
        return "Apache-2.0"
    return "unknown"


def sha256_file(p):
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def fetch_one(src, work):
    name, repo, ref, commit = src["name"], src["repo"], str(src["ref"]), src["commit"]
    if ref != commit:
        got = resolve_tag(repo, ref, commit)
        if got != commit:
            raise FetchError("tag %s of %s resolves to %s, the list says %s" % (ref, repo, got, commit))
    d = Path(work) / name
    d.mkdir(parents=True)
    git(["init", "-q"], d)
    git(["remote", "add", "origin", repo], d)
    git(["fetch", "-q", "--depth", "1", "--filter=blob:none", "origin", commit], d)
    head = git(["rev-parse", "FETCH_HEAD^{commit}"], d).strip()
    if head != commit:
        raise FetchError("fetched %s, expected %s" % (head, commit))
    git(["sparse-checkout", "set", "--no-cone", "/LICENSE"] + ["/" + p.strip("/") for p in src["paths"]], d)
    git(["checkout", "-q", "FETCH_HEAD"], d)
    files = []
    for p in sorted(d.rglob("*")):
        if ".git" in p.relative_to(d).parts or not p.is_file():
            continue
        r = p.relative_to(d).as_posix()
        if r == "LICENSE" or any(fnmatch.fnmatch(r, g) for g in src["globs"]):
            files.append(r)
    lic_file = d / "LICENSE"
    if not lic_file.is_file():
        raise FetchError("%s: no LICENSE file at the tag" % name)
    lic = licence_of(lic_file.read_text(errors="replace"))
    if lic != src["license"]:
        raise FetchError("%s: LICENSE says %s, the list says %s" % (name, lic, src["license"]))
    unmatched = [g for g in src["globs"] if not any(fnmatch.fnmatch(f, g) for f in files)]
    if unmatched:
        raise FetchError("%s: no file matches %s" % (name, unmatched))
    return d, files, lic


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--only", nargs="*", help="fetch only these sources")
    ap.add_argument("--out", default=str(ROOT / "raw_catalogue"))
    ap.add_argument("--list", default=str(ROOT / "catalogue_sources.yaml"))
    args = ap.parse_args(argv)
    out = Path(args.out)
    sources = yaml.safe_load(Path(args.list).read_text())["sources"]
    if args.only:
        sources = [s for s in sources if s["name"] in args.only]
    failed = []
    fetched = {}
    with tempfile.TemporaryDirectory(prefix="catalogue_fetch_") as work:
        for s in sources:
            try:
                d, files, lic = fetch_one(s, work)
                fetched[s["name"]] = (s, d, files, lic)
                size = sum((d / f).stat().st_size for f in files)
                print("%-20s %s@%s %d files %d bytes licence %s" % (s["name"], s["repo"].split("/")[-1][:-4],
                                                                     s["ref"][:12], len(files), size, lic))
            except FetchError as e:
                failed.append(s["name"])
                print("FAIL %s: %s" % (s["name"], e), file=sys.stderr)
        total = sum((d / f).stat().st_size for _, d, files, _ in fetched.values() for f in files)
        if total > LIMIT_BYTES:
            print("STOP: %d bytes selected, over the 50 MB limit; nothing written" % total, file=sys.stderr)
            return 3
        out.mkdir(parents=True, exist_ok=True)
        manifest = {"fetcher": VERSION, "sources": {}, "total_bytes": total}
        old = {}
        if (out / "FETCHED.json").is_file():
            old = json.loads((out / "FETCHED.json").read_text()).get("sources", {})
        for name, (s, d, files, lic) in sorted(fetched.items()):
            dest = out / name
            if dest.exists():
                shutil.rmtree(dest)  # our own previous copy of this source
            for f in files:
                (dest / f).parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(d / f, dest / f)
            sums = {f: sha256_file(dest / f) for f in files if f != "LICENSE"}
            manifest["sources"][name] = {
                "repo": s["repo"], "ref": str(s["ref"]), "commit": s["commit"], "license": lic,
                "license_sha256": sha256_file(dest / "LICENSE"),
                "feeds": s.get("feeds", []),
                "bytes": sum((dest / f).stat().st_size for f in files),
                "files": dict(sorted(sums.items())),
            }
        for name, rec in old.items():  # sources not fetched in this run keep their record
            manifest["sources"].setdefault(name, rec)
        manifest["total_bytes"] = sum(r["bytes"] for r in manifest["sources"].values())
        (out / "FETCHED.json").write_text(json.dumps(manifest, indent=1, sort_keys=True) + "\n")
    print("total %d bytes in %d sources; %s" % (manifest["total_bytes"], len(manifest["sources"]),
                                                out / "FETCHED.json"))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
