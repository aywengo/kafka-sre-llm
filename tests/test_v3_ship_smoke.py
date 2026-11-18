import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import v3_ship_smoke as sm  # noqa: E402


class Compare(unittest.TestCase):
    rec = {"expansion": "e", "chunk_ids": ["a", "b"], "text": "t"}

    def test_equal_and_different(self):
        c = sm.compare("q", self.rec, {"q": {"expansion": "e"}}, {"q": {"k6": ["a", "b"]}}, {"q": {"text": "x"}})
        self.assertEqual(c, {"expansion_equal": True, "chunk_ids_equal": True, "answer_equal": False})

    def test_order_matters_and_missing(self):
        c = sm.compare("q", self.rec, None, {"q": {"k6": ["b", "a"]}}, {})
        self.assertEqual(c, {"chunk_ids_equal": False, "answer_equal": None})

    def test_args(self):
        import json, tempfile
        with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as f:
            f.write(json.dumps({"question": "q", "x": 1}) + "\n")
        self.assertEqual(sm.load_by_question(f.name)["q"]["x"], 1)
        self.assertEqual(sm.load_by_question(None), {})


if __name__ == "__main__":
    unittest.main()
