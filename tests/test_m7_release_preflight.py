"""The CPU preflight of the release test's pack recovery (scripts/m7_release_preflight.py): needs the frozen index and the cached reranker, so it skips without them."""
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PYBIN = str(ROOT / ".venv/bin/python") if (ROOT / ".venv/bin/python").exists() else sys.executable


class Preflight(unittest.TestCase):
    @unittest.skipUnless((ROOT / "work/v3/chunks_s1.jsonl").exists(), "the frozen index is not in this tree (git-ignored)")
    def test_ten_d7_packs_are_recovered_exactly_from_ask_dry_run_json(self):
        p = subprocess.run([PYBIN, "scripts/m7_release_preflight.py", "--n", "10"], cwd=ROOT, capture_output=True, text=True, timeout=3000)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertIn("preflight: 10 of 10 packs recovered exactly", p.stdout)

    def test_it_refuses_hidden_items(self):
        p = subprocess.run([PYBIN, "scripts/m7_release_preflight.py", "--items", "x.jsonl.enc"], cwd=ROOT, capture_output=True, text=True)
        self.assertEqual(p.returncode, 2)


if __name__ == "__main__":
    unittest.main()
