import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import package_entries as P  # noqa: E402

SOURCES = ("sources:\n"
           "  - {name: alpha, repo: x, ref: '1.0', license: Apache-2.0}\n"
           "  - {name: beta, repo: y, ref: '2.0', license: CC-BY-4.0}\n"
           "  - {name: gamma, repo: z, ref: '3.0', license: Apache-2.0}\n")


def entry(i, source="alpha", pv="kbe/5", writer="W"):
    return {"id": f"kbe-{i}", "kind": "fact", "prompt_version": pv, "writer": writer, "title": "t",
            "statement": "s", "quotes": [], "sources": [{"source": source, "id": "c1"}]}


class Fixture(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.t = Path(self._tmp.name)
        self.root, self.data, self.out = self.t / "root", self.t / "data", self.t / "out"
        (self.root / "model_cards").mkdir(parents=True)
        (self.data / "raw" / "alpha").mkdir(parents=True)
        (self.root / "sources.yaml").write_text(SOURCES)
        (self.root / "LICENSE").write_text("Apache License\n")
        (self.data / "raw" / "alpha" / "NOTICE").write_text("Apache alpha\n")
        (self.data / "raw" / "alpha" / "NOTICE-binary").write_text("not copied\n")
        self.entries = [entry(1), entry(2), entry(3, "gamma")]
        self.index = self.data / "entries_index.jsonl"
        self.write_index(self.entries)
        self.record = self.root / "kb_entries.json"
        self.write_record()
        self.write_texts()

    def tearDown(self):
        self._tmp.cleanup()

    def write_index(self, entries):
        self.index.write_text("".join(json.dumps(e) + "\n" for e in entries))

    def write_record(self, **over):
        rec = {"sha256": P.sha256_file(self.index), "entries": len(self.entries), "file": "entries_index.jsonl",
               "writer": {"model": "W", "prompt_version": "kbe/5"}, "checker": {"model": "C", "prompt_version": "kbc/4"}}
        rec.update(over)
        self.record.write_text(json.dumps(rec))

    def write_texts(self, card_extra="", notice_extra=""):
        sha, rsha = P.sha256_file(self.index), P.sha256_file(self.record)
        (self.root / P.CARD).write_text(f"card {sha} {rsha} 3 entries {card_extra}\n")
        (self.root / P.NOTICE).write_text(f"notice 3 entries {notice_extra}\n")

    def build(self, **kw):
        return P.build(self.root, self.out, self.index, self.record, self.data, **kw)


class Build(Fixture):
    def test_folder_manifest_and_names(self):
        listing, markers = self.build()
        paths = [f["path"] for f in listing]
        self.assertEqual(paths, ["LICENSE", "NOTICE", "README.md", "entries_index.jsonl", "kb_entries.json",
                                 "notices/alpha/NOTICE"])
        self.assertEqual(markers, [])
        man = json.loads((self.out / "MANIFEST.json").read_text())
        self.assertEqual(man["files"], listing)
        self.assertEqual(man["repo_id"], "aywengo/kafka-sre-fact-entries-v4")
        self.assertEqual(man["entries"], 3)
        self.assertEqual(man["entries_per_source"], {"alpha": 2, "gamma": 1})
        for f in listing:
            p = self.out / f["path"]
            self.assertEqual(P.sha256_file(p), f["sha256"])
            self.assertEqual(p.stat().st_size, f["size"])
        self.assertEqual((self.out / "entries_index.jsonl").read_bytes(), self.index.read_bytes())
        self.assertFalse((self.out / "notices" / "alpha" / "NOTICE-binary").exists())
        self.assertFalse((self.out / "notices" / "gamma").exists())

    def test_rebuild_is_identical(self):
        a, _ = self.build()
        b, _ = self.build()
        self.assertEqual(a, b)

    def test_open_markers_are_reported_and_final_refuses(self):
        self.write_texts(card_extra="PLACEHOLDER for h6")
        _, markers = self.build()
        self.assertEqual(markers, ["PLACEHOLDER"])
        with self.assertRaises(P.PackageError):
            self.build(final=True)

    def test_final_passes_without_markers(self):
        self.build(final=True)


class Refusals(Fixture):
    def test_index_hash_differs_from_record(self):
        self.write_record(sha256="0" * 64)
        with self.assertRaisesRegex(P.PackageError, "differs from the record"):
            self.build()
        self.assertFalse(self.out.exists())

    def test_count_differs(self):
        self.write_record(entries=4)
        with self.assertRaisesRegex(P.PackageError, "record says 4"):
            self.build()

    def test_prompt_version_differs(self):
        self.entries[1] = entry(2, pv="kbe/6")
        self.write_index(self.entries)
        self.write_record()
        with self.assertRaisesRegex(P.PackageError, "differ from the record's writer"):
            self.build()

    def test_duplicate_ids(self):
        self.entries[1] = entry(1)
        self.write_index(self.entries)
        self.write_record()
        with self.assertRaisesRegex(P.PackageError, "duplicate"):
            self.build()

    def test_non_apache_source(self):
        self.entries[2] = entry(3, "beta")
        self.write_index(self.entries)
        self.write_record()
        with self.assertRaisesRegex(P.PackageError, "not Apache-2.0"):
            self.build()

    def test_unknown_source(self):
        self.entries[2] = entry(3, "ghost")
        self.write_index(self.entries)
        self.write_record()
        with self.assertRaisesRegex(P.PackageError, "not in sources.yaml"):
            self.build()

    def test_card_without_hash(self):
        (self.root / P.CARD).write_text("card 3 entries\n")
        with self.assertRaisesRegex(P.PackageError, "does not state the index sha256"):
            self.build()

    def test_card_without_count(self):
        sha, rsha = P.sha256_file(self.index), P.sha256_file(self.record)
        (self.root / P.CARD).write_text(f"card {sha} {rsha}\n")
        with self.assertRaisesRegex(P.PackageError, "count"):
            self.build()

    def test_leaks_in_card_or_notice(self):
        for extra in ("/Users/someone/x", "/home/someone", "OMLX_API_KEY", "sk-" + "a" * 20, "server 203.0.113.9"):
            with self.subTest(extra=extra):
                self.write_texts(card_extra=extra)
                with self.assertRaises(P.PackageError):
                    self.build()
        self.write_texts(notice_extra="see ~/.config/x")
        with self.assertRaises(P.PackageError):
            self.build()

    def test_version_numbers_are_not_addresses(self):
        self.write_texts(card_extra="Kafka 4.3.2 and Strimzi 1.3.1")
        self.build()


class Main(Fixture):
    def test_cli_writes_folder(self):
        rc = P.main(["--root", str(self.root), "--data-root", str(self.data), "--out-dir", str(self.out),
                     "--entries-index", str(self.index), "--record", str(self.record)])
        self.assertEqual(rc, 0)
        self.assertTrue((self.out / "MANIFEST.json").exists())


if __name__ == "__main__":
    unittest.main()
