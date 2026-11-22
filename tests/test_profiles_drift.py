"""Drift guard: the ordering rules of kafka_sre_rag/profiles.py are a copy of scripts/retrieval_funnel.py (the
specification of R1 and R2). On a synthetic index, with a deterministic stand-in for the reranker, both must give
identical packs (ids and order). If this fails, change the rules in both files."""
import hashlib
import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
import retrieval_funnel as F  # noqa: E402
import retrieve as RT  # noqa: E402
from kafka_sre_rag import retrieval  # noqa: E402


class StubReranker:
    """Deterministic: the score depends on the (query, text) pair only."""
    def predict(self, pairs, batch_size=8):
        return [int(hashlib.sha1((q + "\0" + t).encode()).hexdigest()[:8], 16) / 2 ** 32 for q, t in pairs]


def make_rows():
    words = ["alpha", "beta", "lag", "offset", "broker", "topic", "partition", "consumer", "group", "retention"]
    rows = []
    for i in range(70):
        src = "runbooks" if i % 7 == 0 else ("strimzi" if i % 3 else "apache-kafka")
        w = " ".join(words[(i + j) % 10] for j in range(6))
        title = {5: "Config > max.poll.interval.ms", 9: "Config > auto.offset.reset", 12: "Errors > OFFSET_OUT_OF_RANGE"}.get(i, f"Doc {i} > {words[i % 10]}")
        body = (f"## {words[i % 10]}.setting\n" if i % 4 == 0 else "") + (f"{w} unit{i} " * 150)
        rows.append({"id": f"c{i:03d}", "source": src, "title": title, "path": f"raw/{src}/{i}.md", "text": body})
    return rows


QUESTIONS = [
    ("Consumer lag grows; what does `max.poll.interval.ms` do?", "max.poll.interval.ms, consumer, lag, group, unit5"),
    ("How do I handle offset reset?", "auto.offset.reset, offset, unit9, topic"),
    ("broker partition retention", "retention, broker, unit12, unit30, OFFSET_OUT_OF_RANGE, alpha.setting"),
    ("runbook for beta group", ""),
    ("runbook steps for the group", "unit0, unit7, unit14, unit21, unit28, unit35"),
    ("zzz nothing matches here", "qqq"),
]


class TestProfileDrift(unittest.TestCase):
    def setUp(self):
        if not (ROOT / "runs/v3/INDEX_FROZEN.json").is_file():
            self.skipTest("runs/v3/INDEX_FROZEN.json is not in the repository (it is kept with the run files; no command makes it)")
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, True)
        p = self.tmp / "idx.jsonl"
        p.write_text("\n".join(json.dumps(r) for r in make_rows()))
        self.path = str(p)
        self.model = StubReranker()
        p1 = mock.patch.object(RT, "reranker", lambda: self.model)
        p1.start()
        self.addCleanup(p1.stop)

    def test_r1_r2_packs_equal_funnel(self):
        ix = F.Index(self.path)
        scorer = F.CEScorer(ix.byid, self.tmp / "ce.jsonl", model=self.model)
        scorer.heartbeat = 0
        nonempty = 0
        for q, exp in QUESTIONS:
            st = F.ItemState(ix, q, exp)
            for name in ("r1", "r2"):
                want = F.pack_of(st, F.parse_variant(name)[1], scorer)
                got = [c["id"] for c in retrieval.select(q, exp, profile=name, index_path=self.path)]
                self.assertEqual(got, want, f"{name} drifted from the funnel tool on {q!r}")
                nonempty += bool(got)
        self.assertGreaterEqual(nonempty, 8)   # the test must exercise real packs

    def test_r4_grid_packs_equal_funnel(self):
        """r4 (M5): the eight grid settings, runtime against the funnel's r4_ranking, with the stand-in reranker."""
        from kafka_sre_rag import profiles
        grid = profiles.load_candidate()["r4_grid"]
        ix = F.Index(self.path)
        scorer = F.CEScorer(ix.byid, self.tmp / "ce4.jsonl", model=self.model)
        scorer.heartbeat = 0
        scorer.r4 = F.R4Scorers(ix, self.tmp / "r4", model_of=lambda L: self.model, say=lambda *a: None)
        n = 0
        with mock.patch.object(retrieval, "_reranker_at", lambda L: self.model):
            for q, exp in QUESTIONS:
                st = F.ItemState(ix, q, exp)
                for name, g in grid.items():
                    st.__dict__.pop("_rk_cache", None)
                    want = F.pack_of(st, F.parse_variant(name)[1], scorer)
                    got = [c["id"] for c in retrieval.select(q, exp, profile="r4", index_path=self.path, settings=g)]
                    self.assertEqual(got, want, f"{name} drifted from the funnel tool on {q!r}")
                    n += bool(got)
        self.assertGreaterEqual(n, 40)


if __name__ == "__main__":
    unittest.main()
