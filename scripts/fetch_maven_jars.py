#!/usr/bin/env python3
"""Fetch the Kafka client jars and a sparse clone of the Kafka sources for the a3 Java API catalogue (WP1c T1b).

Downloads exactly (nothing else; no rocksdbjni, no slf4j, no Python wheel):
  org.apache.kafka:kafka-clients:4.3.2   10,322,356 B  (Maven Central)
  org.apache.kafka:kafka-streams:4.3.2    2,290,469 B  (Maven Central)
Each jar is kept only when its size and sha256 equal the values T0 verified against the checksum files that
Central serves next to the jar; a mismatch deletes the download and exits 2. Idempotent: a jar already on disk
that matches is not fetched again.

  --clone   also make a shallow, blobless, sparse clone of apache/kafka at the pinned commit (tag 4.3.2) into
            raw_catalogue/apache-kafka-api/, only the paths the a3 catalogue and its anchors need, and check
            that HEAD is the pinned commit.

Output (git-ignored): raw_catalogue/maven/*.jar, raw_catalogue/apache-kafka-api/, and the record
raw_catalogue/FETCHED_MAVEN.json (URL, size, hash, licence, date). raw_catalogue/FETCHED.json is not touched.
No server address or key is used. Usage: .venv/bin/python scripts/fetch_maven_jars.py [--clone] [--dest DIR]
"""
import argparse
import datetime
import hashlib
import json
import os
import subprocess
import sys
import urllib.request
from pathlib import Path

VERSION = "fetch_maven_jars/1"
ROOT = Path(__file__).resolve().parent.parent
BASE = "https://repo1.maven.org/maven2/org/apache/kafka"
JARS = {
    "kafka-clients-4.3.2.jar": {
        "url": BASE + "/kafka-clients/4.3.2/kafka-clients-4.3.2.jar", "bytes": 10322356,
        "sha256": "edaf0818414a61a98d1d2e58d6966be8c9f780cef6ded2ffbab10dd841b2bfb8"},
    "kafka-streams-4.3.2.jar": {
        "url": BASE + "/kafka-streams/4.3.2/kafka-streams-4.3.2.jar", "bytes": 2290469,
        "sha256": "01a15a6671e210c6e60ca60ce1e09cfab6b40315e57ccd1807fa0fc6d99e4828"},
}
LICENCE = "Apache-2.0"
REPO = "https://github.com/apache/kafka.git"
REF = "4.3.2"
COMMIT = "0a6c96d3b61167f1141df2032f25f10fd6e118fe"
CLONE_NAME = "apache-kafka-api"
CLONE_PATHS = [
    "clients/src/main/java/org/apache/kafka/clients/consumer",
    "clients/src/main/java/org/apache/kafka/clients/producer",
    "clients/src/main/java/org/apache/kafka/clients/admin",
    "clients/src/main/java/org/apache/kafka/common/utils/Utils.java",
    "clients/src/main/java/org/apache/kafka/common/config/ConfigDef.java",
    "clients/src/main/java/org/apache/kafka/common/config/TopicConfig.java",
    "streams/src/main/java/org/apache/kafka/streams",
    # topic-config name validation on create and alter
    "storage/src/main/java/org/apache/kafka/storage/internals/log/LogConfig.java",
    "metadata/src/main/java/org/apache/kafka/controller/ConfigurationControlManager.java",
    "metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java",
    "core/src/main/scala/kafka/server/ControllerConfigurationValidator.scala",
]


class FetchError(Exception):
    pass


def sha256_file(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def check(path, want):
    """None when the file matches size and sha256, else a one-line reason."""
    if path.stat().st_size != want["bytes"]:
        return "size %d, expected %d" % (path.stat().st_size, want["bytes"])
    got = sha256_file(path)
    if got != want["sha256"]:
        return "sha256 %s, expected %s" % (got, want["sha256"])
    return None


def fetch_jar(name, want, dest):
    path = dest / name
    if path.is_file() and check(path, want) is None:
        print("%-28s present, size and sha256 match" % name)
        return
    tmp = dest / (name + ".part")
    with urllib.request.urlopen(want["url"], timeout=120) as r, open(tmp, "wb") as f:
        for b in iter(lambda: r.read(1 << 20), b""):
            f.write(b)
    why = check(tmp, want)
    if why:
        tmp.unlink()
        raise FetchError("%s: %s; download removed" % (name, why))
    os.replace(tmp, path)
    print("%-28s fetched %d B, sha256 match" % (name, want["bytes"]))


def git(args, cwd):
    env = dict(os.environ, GIT_TERMINAL_PROMPT="0")
    r = subprocess.run(["git"] + args, cwd=str(cwd), env=env, capture_output=True, text=True)
    if r.returncode:
        raise FetchError("git %s failed: %s" % (" ".join(args[:2]), r.stderr.strip()[-400:]))
    return r.stdout.strip()


def clone_files(d):
    files = {}
    for p in sorted(d.rglob("*")):
        if ".git" in p.relative_to(d).parts or not p.is_file():
            continue
        files[str(p.relative_to(d))] = sha256_file(p)
    return files


def fetch_clone(dest):
    d = dest / CLONE_NAME
    if (d / ".git").is_dir():
        head = git(["rev-parse", "HEAD"], d)
        if head == COMMIT:
            git(["sparse-checkout", "set", "--no-cone", "/LICENSE"] + ["/" + p for p in CLONE_PATHS], d)
            git(["checkout", "-q", COMMIT], d)
            print("%-28s present, HEAD %s" % (CLONE_NAME, head))
            return {"commit": head, "path": CLONE_NAME, "files": clone_files(d)}
        raise FetchError("%s exists with HEAD %s, not the pinned commit; remove it by hand" % (d, head))
    d.mkdir(parents=True)
    git(["init", "-q"], d)
    git(["remote", "add", "origin", REPO], d)
    git(["fetch", "-q", "--depth", "1", "--filter=blob:none", "origin", COMMIT], d)
    if git(["rev-parse", "FETCH_HEAD^{commit}"], d) != COMMIT:
        raise FetchError("fetched commit is not the pinned one")
    git(["sparse-checkout", "set", "--no-cone", "/LICENSE"] + ["/" + p for p in CLONE_PATHS], d)
    git(["checkout", "-q", "FETCH_HEAD"], d)
    head = git(["rev-parse", "HEAD"], d)
    if head != COMMIT:
        raise FetchError("HEAD %s is not the pinned commit %s" % (head, COMMIT))
    files = clone_files(d)
    print("%-28s cloned, HEAD %s, %d files" % (CLONE_NAME, head, len(files)))
    return {"commit": head, "path": CLONE_NAME, "files": files}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--clone", action="store_true")
    ap.add_argument("--dest", default=str(ROOT / "raw_catalogue"))
    a = ap.parse_args(argv)
    dest = Path(a.dest)
    mv = dest / "maven"
    mv.mkdir(parents=True, exist_ok=True)
    rec_path = dest / "FETCHED_MAVEN.json"
    rec = json.loads(rec_path.read_text()) if rec_path.is_file() else {}
    try:
        for name, want in JARS.items():
            fetch_jar(name, want, mv)
        rec["fetcher"] = VERSION
        rec["fetched"] = rec.get("fetched") or datetime.date.today().isoformat()
        rec["jars"] = {n: {"url": w["url"], "bytes": w["bytes"], "sha256": w["sha256"], "licence": LICENCE,
                           "checked_against": "T0: checksum files served by Maven Central next to the jar"}
                       for n, w in JARS.items()}
        if a.clone:
            c = fetch_clone(dest)
            rec["clone"] = dict(c, repo=REPO, ref=REF, licence=LICENCE, paths=CLONE_PATHS)
    except (FetchError, OSError) as e:
        print("fetch_maven_jars: FAILED: %s" % e, file=sys.stderr)
        return 2
    rec_path.write_text(json.dumps(rec, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    print("record", rec_path.name)
    return 0


if __name__ == "__main__":
    sys.exit(main())
