import contextlib
import io
import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import eval2  # noqa: E402
import sealed_io  # noqa: E402

NEEDS_OPENSSL = unittest.skipUnless(shutil.which("openssl"), "openssl not on PATH")
SECRET = "zebra-sentinel-text"


def rubric(i, n_must=2):
    return {"id": i, "must": [{"id": f"m{k}", "text": SECRET, "weight": 2 if k == 1 else 1,
                               "not_indexed": k == 2} for k in range(1, n_must + 1)],
            "must_not": [{"id": "n1", "text": SECRET}], "any_of": [["m1", "m2"]]}


def jl(rows):
    return "".join(json.dumps(r) + "\n" for r in rows)


@NEEDS_OPENSSL
class SealedIO(unittest.TestCase):
    def setUp(self):
        self.d = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.d, True)
        key = self.d / "k"
        eval2.keygen(key)
        p = mock.patch.dict(os.environ, {"EVAL_HIDDEN_KEY": str(key)})
        p.start()
        self.addCleanup(p.stop)
        self.out = self.d / "x.enc"

    def run_cmd(self, argv, stdin=""):
        o, e = io.StringIO(), io.StringIO()
        with mock.patch.object(sys, "stdin", io.StringIO(stdin)), \
                contextlib.redirect_stdout(o), contextlib.redirect_stderr(e):
            rc = sealed_io.main(argv)
        return rc, o.getvalue(), e.getvalue()

    def seal(self, rows, *extra):
        return self.run_cmd(["seal", str(self.out), *extra], jl(rows))

    def test_round_trip_and_no_plain_text(self):
        rows = [rubric("a"), rubric("b")]
        rc, out, _ = self.seal(rows, "--schema", "rubric")
        self.assertEqual(rc, 0)
        self.assertIn("a,b", out)
        self.assertNotIn(SECRET, out)
        self.assertNotIn(SECRET.encode(), self.out.read_bytes())
        rc, out, _ = self.run_cmd(["show", str(self.out)])
        self.assertEqual(out.splitlines()[0], sealed_io.BANNER)
        self.assertEqual([json.loads(x) for x in out.splitlines()[1:]], rows)
        rc, out, _ = self.run_cmd(["show", str(self.out), "--ids", "b"])
        self.assertEqual(len(out.splitlines()), 2)

    def test_refuse_overwrite(self):
        self.seal([rubric("a")])
        rc, _, err = self.seal([rubric("c")])
        self.assertEqual(rc, 1)
        self.assertEqual(self.run_cmd(["ids", str(self.out)])[1].split(), ["count", "1", "a"])

    def test_append_and_duplicate(self):
        self.seal([rubric("a")])
        rc, out, _ = self.seal([rubric("b")], "--append")
        self.assertEqual(rc, 0)
        rc, _, err = self.seal([rubric("a")], "--append")
        self.assertEqual(rc, 1)
        self.assertIn("a", err)
        self.assertNotIn(SECRET, err)
        self.assertEqual(self.run_cmd(["ids", str(self.out)])[1].split(), ["count", "2", "a", "b"])

    def test_schema_error_names_id_and_field_only(self):
        bad = rubric("r9")
        del bad["must_not"]
        rc, out, err = self.seal([bad], "--schema", "rubric")
        self.assertEqual(rc, 1)
        self.assertIn("r9", err)
        self.assertIn("must_not", err)
        self.assertNotIn(SECRET, err + out)
        self.assertFalse(self.out.exists())
        rc, _, err = self.seal([{"id": "v1", "verdict": "maybe", "changes": SECRET}], "--schema", "review")
        self.assertEqual(rc, 1)
        self.assertNotIn(SECRET, err)
        rc, _, err = self.run_cmd(["seal", str(self.out)], '{"id": "x", ' + SECRET + "\n")
        self.assertEqual(rc, 1)
        self.assertNotIn(SECRET, err)
        rc, _, err = self.seal([{"id": "v1", "verdict": "accept", "changes": []}], "--schema", "review")
        self.assertEqual(rc, 0)

    def test_ids_and_stats_print_counts_only(self):
        self.seal([rubric("a", 3), rubric("b")], "--schema", "rubric")
        rc, out, _ = self.run_cmd(["ids", str(self.out)])
        self.assertNotIn(SECRET, out)
        rc, out, _ = self.run_cmd(["stats", str(self.out), "--schema", "rubric"])
        self.assertNotIn(SECRET, out)
        lines = out.splitlines()
        self.assertEqual(lines[0], "a must=3 weight2=1 any_of=1 must_not=1 not_indexed=1")
        self.assertEqual(lines[-1], "total must=5 weight2=2 any_of=2 must_not=2 not_indexed=2 rows=2")

    def test_no_temp_file_in_temp_folder(self):
        # The shared temp folder is busy with other test runs, so point the temp folder at an
        # empty private one (tempfile, TMPDIR for child processes) and require it to stay empty.
        tmp = self.d / "tmp"
        tmp.mkdir()
        with mock.patch.object(tempfile, "tempdir", str(tmp)), \
                mock.patch.dict(os.environ, {"TMPDIR": str(tmp)}):
            self.assertEqual(Path(tempfile.gettempdir()), tmp)
            self.seal([rubric("a")], "--schema", "rubric")
            self.seal([rubric("b")], "--append")
            self.run_cmd(["show", str(self.out)])
            self.run_cmd(["stats", str(self.out), "--schema", "rubric"])
        self.assertEqual(os.listdir(tmp), [])
        self.assertEqual(sorted(os.listdir(self.d)), ["k", "tmp", "x.enc"])


if __name__ == "__main__":
    unittest.main()
