"""Offline tests of scripts/eval2_first_words.py: a stub run end to end, resume, refusal of hidden files. No model."""
import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import eval2_first_words as fw  # noqa: E402


def run(argv):
    buf, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(err):
        rc = fw.main(argv)
    return rc, buf.getvalue(), err.getvalue()


class FirstWordsTest(unittest.TestCase):
    def test_stub_run_end_to_end(self):
        with tempfile.TemporaryDirectory() as d:
            rc, out, _ = run(["run", "--stub", "--out", d])
            self.assertEqual(rc, 0)
            rows = [json.loads(l) for l in (Path(d) / "rows.jsonl").read_text().splitlines()]
            self.assertEqual(len(rows), 120)
            self.assertEqual({r["tier"] for r in rows}, {"judged", "mechanical"})
            self.assertEqual(sum(r["tier"] == "judged" for r in rows), 60)
            for k in ("first_words_s", "wall_s", "tokens", "at_limit", "search_s", "retrieve_s", "answer_s"):
                self.assertIn(k, rows[0])
            self.assertEqual(len([l for l in out.splitlines() if " ok" in l]), 120)
            s = json.loads((Path(d) / "summary.json").read_text())
            for t in ("judged", "mechanical"):
                self.assertEqual(s[t]["ok"], 60)
                self.assertTrue(s[t]["bar_median_under_60s"]["met"])
                self.assertEqual(s[t]["bar_first_words_within_10s"]["n"], 60)
                self.assertIsNotNone(s[t]["s_per_1000_tokens"])
            self.assertIn("first words within 10 s", (Path(d) / "summary.md").read_text())

    def test_resume_skips_finished(self):
        with tempfile.TemporaryDirectory() as d:
            run(["run", "--stub", "--out", d, "--n", "3"])
            rows_file = Path(d) / "rows.jsonl"
            lines = rows_file.read_text().splitlines()
            self.assertEqual(len(lines), 6)
            rc, out, _ = run(["run", "--stub", "--out", d, "--n", "3"])
            self.assertEqual(rc, 0)
            self.assertIn("0 to ask", out)
            self.assertEqual(rows_file.read_text().splitlines(), lines)
            rc, out, _ = run(["run", "--stub", "--out", d, "--n", "4"])
            self.assertIn("2 to ask", out)
            self.assertEqual(len(rows_file.read_text().splitlines()), 8)

    def test_failed_item_is_asked_again(self):
        with tempfile.TemporaryDirectory() as d:
            run(["run", "--stub", "--out", d, "--n", "1", "--tier", "judged"])
            f = Path(d) / "rows.jsonl"
            r = json.loads(f.read_text().splitlines()[0])
            r["error"] = "HTTP 500"
            f.write_text(json.dumps(r) + "\n")
            rc, out, _ = run(["run", "--stub", "--out", d, "--n", "1", "--tier", "judged"])
            self.assertIn("1 to ask", out)
            s = json.loads((Path(d) / "summary.json").read_text())
            self.assertEqual(s["judged"]["ok"], 1)

    def test_refuses_enc_and_hidden_folder(self):
        with tempfile.TemporaryDirectory() as d:
            for bad in ("x.jsonl.enc", "eval/hidden/tier_b.jsonl", "other.enc"):
                rc, _, err = run(["run", "--stub", "--out", d, "--judged-items", bad])
                self.assertEqual(rc, 2, bad)
                self.assertIn("refused", err)
            rc, _, err = run(["run", "--stub", "--out", d, "--ids-file", "ids.json.enc"])
            self.assertEqual(rc, 2)
            self.assertFalse((Path(d) / "rows.jsonl").exists())

    def test_refuses_changed_config_on_resume(self):
        with tempfile.TemporaryDirectory() as d:
            run(["run", "--stub", "--out", d, "--n", "1"])
            rc, out, _ = run(["run", "--stub", "--out", d, "--n", "1", "--k", "k3"])
            self.assertEqual(rc, 2)
            self.assertIn("refused", out)

    def test_percentile_and_limit_share(self):
        self.assertEqual(fw.percentile([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 90), 9)
        rows = [{"tier": "t", "id": str(i), "wall_s": 70.0, "first_words_s": 12.0, "tokens": 2048, "at_limit": i < 3,
                 "error": None} for i in range(4)]
        s = fw.summarise(rows)["t"]
        self.assertEqual(s["at_limit"], 3)
        self.assertFalse(s["bar_median_under_60s"]["met"])
        self.assertFalse(s["bar_first_words_within_10s"]["met"])
        self.assertAlmostEqual(s["s_per_1000_tokens"], 34.18, places=2)


if __name__ == "__main__":
    unittest.main()
