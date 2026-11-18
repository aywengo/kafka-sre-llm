import json
import sys
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
import retrieve  # noqa: E402


class RerankerPin(unittest.TestCase):
    def test_constants_match_frozen(self):
        cfg = json.loads((ROOT / "kafka_sre_rag" / "frozen.json").read_text())["retrieval"]["reranker"]
        self.assertEqual(retrieve.RERANKER_MODEL, cfg["model"])
        self.assertEqual(retrieve.RERANKER_REVISION, cfg["revision"])

    def test_revision_passed_to_loader(self):
        retrieve.reranker.cache_clear()
        with mock.patch("sentence_transformers.CrossEncoder") as ce:
            retrieve.reranker()
        retrieve.reranker.cache_clear()
        _, kw = ce.call_args
        self.assertEqual(kw["revision"], retrieve.RERANKER_REVISION)
        self.assertEqual((kw["device"], kw["max_length"]), ("cpu", 512))


if __name__ == "__main__":
    unittest.main()
