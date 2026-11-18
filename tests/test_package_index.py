import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import package_index as P  # noqa: E402

SOURCES = [
    {"name": "alpha", "repo": "x", "ref": "1.0", "license": "Apache-2.0"},
    {"name": "beta", "repo": "y", "ref": "c" * 40, "license": "CC-BY-4.0"},
    {"name": "unused", "repo": "z", "ref": "2.0", "license": "Apache-2.0"},
    {"name": "runbooks", "local": "./data/runbooks/runbooks", "license": "Apache-2.0"},
]
COMMITS = {"alpha": "a" * 40, "beta": "c" * 40}


def chunk(source, path, lic="Apache-2.0", i="1"):
    return {"id": i, "source": source, "license": lic, "path": path, "title": "t", "text": "x"}


class Table(unittest.TestCase):
    def test_counts_licences_and_pins(self):
        chunks = [chunk("alpha", "raw/alpha/a.md"), chunk("alpha", "raw/alpha/b.md"),
                  chunk("beta", "raw/beta/a.md", "CC-BY-4.0"),
                  chunk("runbooks", LOCAL + "kafka/ok.md")]
        t = P.source_table(chunks, SOURCES, COMMITS).splitlines()
        self.assertIn("| alpha | Apache-2.0 | 2 | `1.0` (`" + "a" * 40 + "`) |", t)
        self.assertIn("| beta | CC-BY-4.0 | 1 | `" + "c" * 40 + "` |", t)
        self.assertTrue(any(l.startswith("| runbooks | Apache-2.0 | 1 |") for l in t))
        self.assertFalse(any("unused" in l for l in t))
        self.assertIn("| **total** | | **4** | |", t)

    def test_unknown_source_is_an_error(self):
        with self.assertRaises(P.PackageError):
            P.source_table([chunk("ghost", "raw/ghost/a.md")], SOURCES, COMMITS)


LOCAL = P.LOCAL_PREFIX


class Refusal(unittest.TestCase):
    def test_hash_mismatch_refuses(self):
        with tempfile.TemporaryDirectory() as d:
            f = Path(d) / "c.jsonl"
            f.write_text(json.dumps(chunk("alpha", "raw/alpha/a.md")) + "\n")
            good = P.sha256_file(f)
            self.assertEqual(P.check_hash(f, good), good)
            with self.assertRaises(P.PackageError):
                P.check_hash(f, "0" * 64)


class Forbidden(unittest.TestCase):
    HOLD = ["kafka/kraft-quorum-debugging.md\n", "strimzi/topic-not-ready.md\n", "\n"]
    EXCL = ["*/kafka/schema-registry-wire-format-and-magic-byte.md", "*/kubernetes/*.md"]

    def test_clean_chunks_pass(self):
        chunks = [chunk("runbooks", LOCAL + "strimzi/consumer-lag.md"), chunk("alpha", "raw/alpha/x.md")]
        self.assertEqual(P.forbidden_chunks(chunks, self.HOLD, self.EXCL), [])
        P.check_forbidden(chunks, self.HOLD, self.EXCL)

    def test_held_out_runbook_is_found(self):
        chunks = [chunk("runbooks", LOCAL + "kafka/kraft-quorum-debugging.md", i="h")]
        self.assertEqual(P.forbidden_chunks(chunks, self.HOLD, self.EXCL), ["h"])
        with self.assertRaises(P.PackageError):
            P.check_forbidden(chunks, self.HOLD, self.EXCL)

    def test_excluded_new_runbooks_are_found(self):
        chunks = [chunk("runbooks", LOCAL + "kafka/schema-registry-wire-format-and-magic-byte.md", i="n1"),
                  chunk("runbooks", LOCAL + "kubernetes/container-cpu-throttling-metrics.md", i="n2")]
        self.assertEqual(P.forbidden_chunks(chunks, self.HOLD, self.EXCL), ["n1", "n2"])


class Readme(unittest.TestCase):
    def test_placeholder_and_final_numbers(self):
        cfg = {"index": {"sha256": "ab" * 32, "index_path": "work/v3/chunks_s1.jsonl"},
               "retrieval": {"reranker": {"model": "m/r"}}}
        chunks = [chunk("alpha", "raw/alpha/a.md")]
        r = P.render_readme(chunks, "TABLE", cfg)
        self.assertIn(P.PLACEHOLDER, r)
        self.assertIn("apache-2.0", r)
        self.assertIn("cc-by-4.0", r)
        r2 = P.render_readme(chunks, "TABLE", cfg, measured="M", headline="H", size="S")
        self.assertNotIn(P.PLACEHOLDER, r2)


class Packaging(unittest.TestCase):
    def _root(self, tmp, with_license=True):
        import hashlib
        root = Path(tmp)
        (root / "runs/v3").mkdir(parents=True)
        (root / "kafka_sre_rag").mkdir()
        (root / "work").mkdir()
        (root / "data/runbooks").mkdir(parents=True)
        idx = (json.dumps(chunk("alpha", "raw/alpha/a.md")) + "\n").encode()
        (root / "work/c.jsonl").write_bytes(idx)
        sha = hashlib.sha256(idx).hexdigest()
        frozen = {"sha256": sha, "index_path": "work/c.jsonl", "chunks": 1,
                  "chunks_per_source": {"alpha": 1}, "source_commits": {"alpha": "a" * 40}}
        (root / "runs/v3/INDEX_FROZEN.json").write_text(json.dumps(frozen))
        cfg = {"index": {"sha256": sha, "index_path": "work/c.jsonl"},
               "retrieval": {"reranker": {"model": "m/r"}}}
        (root / "kafka_sre_rag/frozen.json").write_text(json.dumps(cfg))
        (root / "sources.yaml").write_text("sources:\n  - {name: alpha, repo: x, ref: '1.0', license: Apache-2.0}\n")
        (root / "data/runbooks/holdout.txt").write_text("")
        (root / "NOTICE").write_text("notice text\n")
        if with_license:
            (root / "LICENSE").write_text("Apache License text\n")
        return root

    def test_license_is_copied_and_listed_in_sums(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp)
            self.assertEqual(P.main(["--root", str(root), "--out-dir", "out/pkg"]), 0)
            out = root / "out/pkg"
            self.assertEqual((out / "LICENSE").read_text(), "Apache License text\n")
            sums = (out / "SHA256SUMS").read_text()
            self.assertIn("  LICENSE\n", sums)
            self.assertIn("  NOTICE\n", sums)

    def test_missing_license_is_an_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp, with_license=False)
            with self.assertRaises(P.PackageError):
                P.main(["--root", str(root), "--out-dir", "out/pkg"])

    def test_dataset_card_has_the_disclaimer_sentence(self):
        cfg = {"index": {"sha256": "ab" * 32, "index_path": "work/v3/chunks_s1.jsonl"},
               "retrieval": {"reranker": {"model": "m/r"}}}
        r = P.render_readme([chunk("alpha", "raw/alpha/a.md")], "TABLE", cfg)
        self.assertIn("Section 5 of that licence", r)


if __name__ == "__main__":
    unittest.main()
