import contextlib
import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
import eval2  # noqa: E402
import ingest_owner_questions as ing  # noqa: E402


def entry(kind="debugging", **over):
    d = {"kind": kind, "versions": "{}", "question": "|\n      SECRETQ text", "evidence": "''",
         "what_was_wrong": "w", "what_fixed_it": "f", "essential": "\n      - SECRETE point",
         "dangerous": "[]", "scrub: yes": None, "side": "hidden", "source": "own", "note": "SECRETN"}
    lines = ["  - kind: " + kind]
    for k, v in d.items():
        if k == "kind" or k in over and over[k] is None:
            continue
        if k == "scrub: yes":
            lines.append("    scrub: yes")
            continue
        lines.append(f"    {k}: {over.get(k, v)}")
    return "\n".join(lines)


def yaml_of(nums, **over):
    body = "\n".join(f"  # {n}\n{entry(**over)}" for n in nums)
    return "# header comment\n# not numbered\nquestions:\n" + body + "\n"


class T(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.TemporaryDirectory()
        self.d = Path(self.td.name)
        self.key = self.d / "key"
        self.key.write_text(os.urandom(32).hex())
        self.key.chmod(0o600)
        self.addCleanup(self.td.cleanup)

    def run_main(self, text, *extra):
        inbox = self.d / "in.yaml"
        inbox.write_text(text)
        args = ["--inbox", str(inbox), "--out", str(self.d / "o.enc"),
                "--manifest", str(self.d / "m.json"), "--key-file", str(self.key), *extra]
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            rc = ing.main(args)
        return rc, out.getvalue(), err.getvalue()

    def test_ids_from_comments(self):
        p = ing.parse_inbox(yaml_of([2, 5, 51]))
        self.assertEqual([c for c, _ in p], [2, 5, 51])
        self.assertEqual([r["id"] for r in ing.build_rows(p, "x")], ["oq-002", "oq-005", "oq-051"])

    def test_count_mismatch(self):
        with self.assertRaises(ing.IntakeError):
            ing.parse_inbox(yaml_of([2, 3]).replace("  # 3\n", ""))

    def test_duplicate_case(self):
        with self.assertRaisesRegex(ing.IntakeError, "duplicate"):
            ing.parse_inbox(yaml_of([4, 4]))

    def test_missing_field_no_text(self):
        rc, out, err = self.run_main(yaml_of([7], note=None))
        self.assertEqual(rc, 2)
        self.assertIn("case 7", err)
        self.assertIn("note", err)
        self.assertNotIn("SECRET", out + err)

    def test_bad_kind_and_empty_essential(self):
        for over in ({"kind": "nonsense"}, {"side": "either"}, {"essential": "[]"}):
            rc, out, err = self.run_main(yaml_of([3], **over))
            self.assertEqual(rc, 2, over)
            self.assertNotIn("SECRET", out + err)

    def test_provenance_and_round_trip(self):
        rc, out, err = self.run_main(yaml_of([2, 9]))
        self.assertEqual(rc, 0, err)
        self.assertNotIn("SECRET", out)
        rows = [json.loads(l) for l in eval2.decrypt_bytes((self.d / "o.enc").read_bytes(), str(self.key))
                .decode().splitlines()]
        self.assertEqual([r["id"] for r in rows], ["oq-002", "oq-009"])
        r = rows[0]
        self.assertNotIn("source", r)
        self.assertNotIn("note", r)
        self.assertEqual(r["provenance"], {"source": "own", "note": "SECRETN"})
        self.assertEqual(len(r["intake_sha256"]), 64)
        m = json.loads((self.d / "m.json").read_text())
        self.assertEqual(m["total"], 2)
        self.assertNotIn("SECRET", json.dumps(m))
        self.assertEqual(m["items"]["oq-002"]["versions_empty"], True)

    def test_refuse_overwrite(self):
        self.assertEqual(self.run_main(yaml_of([2]))[0], 0)
        rc, _, err = self.run_main(yaml_of([2]))
        self.assertEqual(rc, 2)
        self.assertIn("--force", err)
        self.assertEqual(self.run_main(yaml_of([2]), "--force")[0], 0)

    def test_dry_run_writes_nothing(self):
        rc, out, _ = self.run_main(yaml_of([2]), "--dry-run")
        self.assertEqual(rc, 0)
        self.assertFalse((self.d / "o.enc").exists())
        self.assertFalse((self.d / "m.json").exists())
        self.assertIn("oq-002", out)


if __name__ == "__main__":
    unittest.main()
