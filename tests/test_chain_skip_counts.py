"""The skip line of the judging stage in the two M5 chains prints the real counts (valid, unjudged), not the expected count."""
import json
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = ("scripts/m5_g_dev.sh", "scripts/m5_r6_run.sh")


class SkipCounts(unittest.TestCase):
    def snippet(self, script):
        line = next(l for l in (ROOT / script).read_text().splitlines() if "skip judging" in l)
        self.assertNotIn("$calls valid rows", line)
        m = re.search(r"python -c '([^']*)'", line)
        self.assertIsNotNone(m)
        return m.group(1)

    def test_real_counts_are_printed(self):
        rows = [{"valid_reply": True}] * 5 + [{"valid_reply": False}]
        with tempfile.TemporaryDirectory() as d:
            f = Path(d) / "j.jsonl"
            f.write_text("".join(json.dumps(r) + "\n" for r in rows))
            for script in SCRIPTS:
                out = subprocess.run([sys.executable, "-c", self.snippet(script), str(f), "6"], capture_output=True, text=True, check=True).stdout
                self.assertEqual(out.strip(), "5 valid rows, 1 unjudged of 6 (expected 6)")


if __name__ == "__main__":
    unittest.main()
