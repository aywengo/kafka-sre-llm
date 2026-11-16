"""scripts/gen_questions.py with a small fake chunk file and the script's own dry-run LLM (no network, no model)."""
import json
import math
import os
import random
import subprocess
import sys
import tempfile
import unittest
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
import gen_questions as gq  # noqa: E402

VOCAB = [f"{a}{b}" for a in ("broker", "topic", "lag", "quorum", "segment", "offset", "leader", "isr", "acl", "tls")
         for b in ("alpha", "beta", "gamma", "delta", "sigma", "omega", "kappa", "theta")]


def make_chunks(path):
    """~330 chunks over the sources and paths the seed rules know; every text has its own words."""
    rng, rows = random.Random(7), []
    layout = [("apache-kafka", "docs/operations", 40), ("apache-kafka", "docs/security", 20),
              ("apache-kafka", "docs/design", 15), ("apache-kafka", "docs/streams", 40),
              ("apache-kafka", "docs/configuration", 10), ("apache-kafka-config", "conf", 30),
              ("runbooks", "runbooks/kafka", 12), ("runbooks", "runbooks/kubernetes", 8),
              ("strimzi", "docs/managing", 50), ("strimzi", "docs/misc", 20), ("kubernetes", "docs/tasks", 40),
              ("argo-cd", "docs/operator", 30), ("confluent-schema-registry", "docs/sr", 10),
              ("karapace", "docs/api", 8)]
    n = 0
    for source, folder, count in layout:
        for i in range(count):
            n += 1
            words = " ".join(rng.choice(VOCAB) for _ in range(70))
            rows.append({"id": f"c{n:04d}", "source": source, "license": "x", "title": f"Title {n}",
                         "path": f"raw/{source}/{folder}/file{i // 2}.md",  # two chunks per file
                         "text": f"Section {n} text. {words}. " + "More detail about the cluster. " * 8})
    Path(path).write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    return rows


class Run:
    """One dry-run of the script in a scratch directory."""

    def __init__(self, tmp, name, hashseed="0", **opts):
        self.dir = Path(tmp) / name
        self.dir.mkdir()
        d = self.dir
        self.paths = {k: d / f for k, f in (("raw", "raw.jsonl"), ("out", "q.jsonl"), ("stats", "stats.md"),
                                           ("slice", "slice.jsonl"), ("reserve", "reserve.jsonl"),
                                           ("rejects", "q.rejects.jsonl"))}
        self.args = ["--dry-run", "--chunks", str(Path(tmp) / "chunks.jsonl"), "--eval-glob", str(Path(tmp) / "ev.jsonl"),
                     "--style-file", str(Path(tmp) / "style.jsonl"), "--raw", str(self.paths["raw"]),
                     "--out", str(self.paths["out"]), "--stats", str(self.paths["stats"]),
                     "--eval-slice-out", str(self.paths["slice"]), "--reserve-out", str(self.paths["reserve"])]
        for k, v in opts.items():
            self.args += [f"--{k.replace('_', '-')}", str(v)]
        self.env = {**os.environ, "PYTHONHASHSEED": hashseed}
        self.proc = self.go()

    def go(self):
        p = subprocess.run([sys.executable, str(ROOT / "scripts/gen_questions.py"), *self.args], cwd=ROOT,
                           env=self.env, capture_output=True, text=True)
        assert p.returncode == 0, p.stdout + p.stderr
        return p

    def rows(self, key):
        p = self.paths[key]
        return [json.loads(l) for l in p.read_text().splitlines() if l.strip()]

    def text(self, key):
        return self.paths[key].read_bytes()


OPTS = dict(target=60, over=3, reserve=10, withheld=8, ooc=12)


class GenQuestionsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory()
        t = cls.tmp = cls._tmp.name
        make_chunks(Path(t) / "chunks.jsonl")
        (Path(t) / "ev.jsonl").write_text(json.dumps({"question": "Why is nothing else like this one at all?"}) + "\n")
        (Path(t) / "style.jsonl").write_text(
            "\n".join(json.dumps({"question": f"Style question number {i} about brokers?"}) for i in range(8)) + "\n")
        cls.main = Run(t, "main", **OPTS)
        cls.out, cls.reserve, cls.slice = cls.main.rows("out"), cls.main.rows("reserve"), cls.main.rows("slice")
        cls.bank = [r for r in cls.out if r["origin"] == "seed"]
        cls.ooc = [r for r in cls.out if r["origin"] == "ooc"]

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    # ---- seeds and per-seed numbers
    def test_per_seed_numbers(self):
        def per(source, path):
            return gq.rule_for({"source": source, "path": path})[2]
        for folder in ("operations", "security", "design", "implementation"):
            self.assertEqual(per("apache-kafka", f"raw/apache-kafka/docs/{folder}/x.md"), 3)
        self.assertEqual(per("runbooks", "raw/runbooks/runbooks/kafka/a.md"), 3)
        self.assertEqual(per("runbooks", "raw/runbooks/runbooks/kubernetes/a.md"), 3)
        self.assertEqual(per("confluent-schema-registry", "a"), 3)
        self.assertEqual(per("karapace", "a"), 3)
        self.assertEqual(per("kubernetes", "a"), 2)
        self.assertEqual(per("apache-kafka", "raw/apache-kafka/docs/streams/a.md"), 2)
        # the widened Kafka rule keeps the source+path logic: another source with /security/ is not kafka-ops
        self.assertNotEqual(gq.rule_for({"source": "kubernetes", "path": "docs/security/a.md"})[1], "kafka-ops")

    def test_raw_records_hold_per_seed_types(self):
        by_id = {c["id"]: c for c in map(json.loads, (Path(self.tmp) / "chunks.jsonl").read_text().splitlines())}
        n3 = 0
        for r in self.main.rows("raw"):
            if "chunk_id" in r:
                per = gq.rule_for(by_id[r["chunk_id"]])[2]
                self.assertEqual(len(r["types"]), per)
                n3 += per == 3
        self.assertGreater(n3, 0)

    def test_plan_warning(self):
        def chunk(i, source="runbooks", path="x/kafka/a.md"):
            return {"id": str(i), "source": source, "path": path, "text": "x" * 400}
        plan = [chunk(i) for i in range(10)]  # kafka-ops: 10 seeds x 3 = 30 raw against a quota of 95 at target 380
        lines = gq.plan_lines(plan, 380, 1.6)
        ops = next(l for l in lines if l.strip().startswith("kafka-ops"))
        self.assertIn("expected raw 30 vs quota 95", ops)
        self.assertIn("WARNING", ops)
        enough = gq.plan_lines([chunk(i) for i in range(42)], 380, 1.6)  # 126 raw >= 1.3 x 95
        self.assertNotIn("WARNING", next(l for l in enough if l.strip().startswith("kafka-ops")))

    def test_apportion(self):
        w = {"a": 0.5, "b": 0.3, "c": 0.2}
        self.assertEqual(gq.apportion(10, w, {"a": 99, "b": 99, "c": 99}), {"a": 5, "b": 3, "c": 2})
        # a capped key hands its share to the others; never above the caps; total = min(total, sum of caps)
        got = gq.apportion(10, w, {"a": 1, "b": 99, "c": 99})
        self.assertEqual((got["a"], sum(got.values())), (1, 10))
        self.assertEqual(sum(gq.apportion(100, w, {"a": 1, "b": 2, "c": 3}).values()), 6)
        self.assertEqual(sum(gq.apportion(7, {"a": 1, "b": 1, "c": 1}, {"a": 9, "b": 9, "c": 9}).values()), 7)

    # ---- quota and reserve
    def test_quota_and_reserve(self):
        quota = {c: round(s * OPTS["target"]) for c, s in gq.QUOTA.items()}
        per_cat = Counter(r["category"] for r in self.bank)
        for c, q in quota.items():
            self.assertLessEqual(per_cat[c], q)
        self.assertGreater(sum(per_cat.values()), 0)
        self.assertGreater(len(self.reserve), 0)
        self.assertLessEqual(len(self.reserve), OPTS["reserve"])
        out_ids = {r["id"] for r in self.out}
        res_ids = [r["id"] for r in self.reserve]
        self.assertEqual(len(set(res_ids)), len(res_ids))
        self.assertFalse(out_ids & set(res_ids))
        self.assertFalse(set(res_ids) & {r["id"] for r in self.slice})
        # a reserve question is only taken from a category that was trimmed, so that category is at its quota
        res_cat = Counter(r["category"] for r in self.reserve)
        for c, n in res_cat.items():
            self.assertEqual(per_cat[c], quota[c], c)
        # reserve questions are not in the rejects as over_quota; the others trimmed off are
        rejects = self.main.rows("rejects")
        self.assertFalse(set(res_ids) & {d["id"] for d in rejects if d["reason"] == "over_quota"})
        for r in self.reserve:
            self.assertEqual((r["origin"], r["treatment"]), ("seed", "normal"))
            self.assertIn(r["split"], ("train", "valid"))

    def test_reserve_spread_follows_quota(self):
        trimmed = Counter(r["category"] for r in self.reserve)
        alloc = gq.apportion(OPTS["reserve"], dict(gq.QUOTA), {c: 10 ** 6 for c in gq.QUOTA})
        for c in gq.QUOTA:  # where there is enough surplus the reserve has the proportional number
            if trimmed[c] > 0 and sum(trimmed.values()) == OPTS["reserve"]:
                self.assertLessEqual(abs(trimmed[c] - alloc[c]), 3, c)

    def test_reserve_zero(self):
        r = Run(self.tmp, "noreserve", **{**OPTS, "reserve": 0})
        self.assertEqual(r.rows("reserve"), [])
        self.assertEqual({x["id"] for x in r.rows("out")}, {x["id"] for x in self.out})

    # ---- ooc
    def test_ooc_bypasses_quota_and_is_trimmed(self):
        self.assertEqual(len(self.ooc), OPTS["ooc"])
        for r in self.ooc:
            self.assertEqual((r["treatment"], r["origin"]), ("ooc", "ooc"))
            self.assertIn("topic", r)
            self.assertNotIn("seed_chunk_id", r)
            self.assertIn(r["category"], gq.QUOTA)
        self.assertIn(r["topic"], [t for t, _ in gq.OOC_TOPICS])
        off = Run(self.tmp, "nooc", **{**OPTS, "ooc": 0})
        self.assertEqual([r for r in off.rows("out") if r["origin"] == "ooc"], [])
        self.assertFalse(any("ooc_topic" in r for r in off.rows("raw")))  # no call for a topic
        # the seed part is identical with and without the stream: ooc never takes a place in the quota
        self.assertEqual([r for r in self.out if r["origin"] == "seed"], off.rows("out"))

    def test_ooc_questions_per_call(self):
        n = math.ceil(OPTS["ooc"] * 1.5 / len(gq.OOC_TOPICS))
        recs = [r for r in self.main.rows("raw") if "ooc_topic" in r]
        self.assertEqual(len(recs), len(gq.OOC_TOPICS))
        self.assertTrue(all(r["n"] == n and len(r["types"]) == n for r in recs))

    def test_ooc_topic_list_and_mentions(self):
        self.assertGreaterEqual(len(gq.OOC_TOPICS), 20)
        names = " ".join(t for t, _ in gq.OOC_TOPICS).lower()
        for must in ("ksqldb", "debezium", "msk", "confluent cloud", "flink", "akhq", "cert-manager", "vault",
                     "external secrets", "helm", "flux", "istio", "alertmanager", "grafana", "velero", "terraform",
                     "redpanda", "pulsar", "rabbitmq", "librdkafka", "garbage", "xfs", "dns", "opentelemetry"):
            self.assertIn(must, names)
        m = gq.topic_mentions([{"text": "Use Debezium with ksqlDB."}, {"text": "nothing"}, {"text": "DEBEZIUM"}])
        self.assertEqual(m["Debezium connectors"], 2)
        self.assertEqual(m["ksqlDB"], 1)
        self.assertIn("index chunks mentioning", self.main.text("stats").decode())

    def test_ooc_split_by_topic(self):
        topics = defaultdict(set)
        for r in self.ooc:
            topics[r["topic"]].add(r["split"])
        self.assertTrue(all(len(v) == 1 for v in topics.values()))
        valid = [t for t, v in topics.items() if v == {"valid"}]
        self.assertEqual(len(valid), gq.OOC_VALID_TOPICS)

    # ---- withheld
    def test_withheld(self):
        wh = [r for r in self.out if r["treatment"] == "withheld"]
        self.assertEqual(len(wh), OPTS["withheld"])
        for r in wh:
            self.assertEqual(r["origin"], "seed")
            self.assertIn(r["type"], gq.WITHHOLD_TYPES)
            self.assertEqual(r["split"], "train")  # decision: never valid, eval slice, reserve or ooc
        self.assertTrue(all(r["treatment"] == "normal" for r in self.reserve + self.slice))
        self.assertEqual({r["treatment"] for r in self.out if r["origin"] == "ooc"}, {"ooc"})
        # no concept question and no valid-split question is ever withheld
        for r in self.out:
            if r["type"] == "concept" or r["split"] == "valid":
                self.assertNotEqual(r["treatment"], "withheld")

    def test_withheld_spread_over_categories(self):
        sizes = Counter(r["category"] for r in self.bank)
        wh = Counter(r["category"] for r in self.bank if r["treatment"] == "withheld")
        for c, n in wh.items():  # proportional to category size, within a few of the ideal share
            self.assertLessEqual(abs(n - OPTS["withheld"] * sizes[c] / len(self.bank)), 3, c)
        self.assertGreater(len(wh), 1)

    def test_withheld_more_than_eligible(self):
        r = Run(self.tmp, "manywh", **{**OPTS, "withheld": 10 ** 5})
        rows = r.rows("out")
        n = sum(x["treatment"] == "withheld" for x in rows)
        elig = sum(x["origin"] == "seed" and x["split"] == "train" and x["type"] in gq.WITHHOLD_TYPES for x in rows)
        self.assertEqual(n, elig)

    # ---- split
    def test_split_by_file(self):
        splits = defaultdict(set)
        for r in self.out + self.slice + self.reserve:
            if r["origin"] == "seed":
                splits[r["seed_path"]].add(r["split"])
        self.assertTrue(all(len(v) == 1 for v in splits.values()), "a seed file is in two splits")
        self.assertTrue(all(r["split"] == "evalslice" for r in self.slice))
        self.assertFalse([r for r in self.out if r["split"] == "evalslice"])
        for r in self.out + self.slice + self.reserve:
            if r["origin"] == "seed":
                self.assertEqual(r["split"], gq.split_of(r["seed_path"], 42))

    def test_split_fractions(self):
        paths = [f"raw/x/docs/{i}/file{i}.md" for i in range(20000)]
        got = Counter(gq.split_of(p, 42) for p in paths)
        self.assertAlmostEqual(got["evalslice"] / len(paths), gq.EVAL_SLICE_FRAC, delta=0.01)
        rest = len(paths) - got["evalslice"]
        self.assertAlmostEqual(got["valid"] / rest, gq.VALID_FRAC, delta=0.005)
        # the slice does not move with --seed, valid does
        self.assertEqual({p for p in paths if gq.split_of(p, 1) == "evalslice"},
                         {p for p in paths if gq.split_of(p, 2) == "evalslice"})
        self.assertNotEqual({p for p in paths if gq.split_of(p, 1) == "valid"},
                            {p for p in paths if gq.split_of(p, 2) == "valid"})

    def test_every_record_has_the_fields(self):
        for r in self.out + self.slice + self.reserve:
            self.assertIn(r["origin"], ("seed", "ooc"))
            self.assertIn(r["treatment"], ("normal", "withheld", "ooc"))
            self.assertIn(r["split"], ("train", "valid", "evalslice"))
            self.assertEqual(("seed_chunk_id" in r), r["origin"] == "seed")

    def test_stats_sections(self):
        t = self.main.text("stats").decode()
        for s in ("## Reserve", "## Out-of-corpus stream", "## Withheld", "## Split, origin and treatment",
                  "expected raw", "must be 0"):
            self.assertIn(s, t)
        self.assertIn("(must be 0).", t)
        self.assertRegex(t, r"more than one split: 0 \(must")

    # ---- determinism, resume, dry/real
    def test_deterministic_across_runs_and_hashseeds(self):
        for i, hs in enumerate(("1", "12345")):
            r = Run(self.tmp, f"det{i}", hashseed=hs, **OPTS)
            for k in ("raw", "out", "slice", "reserve", "rejects"):
                if k == "raw":  # completion order of the calls may differ; the set of records may not
                    self.assertEqual(sorted(r.text(k).splitlines()), sorted(self.main.text(k).splitlines()), k)
                else:
                    self.assertEqual(r.text(k), self.main.text(k), f"{k} with PYTHONHASHSEED={hs}")
            norm = lambda x: x.decode().replace(str(r.dir), "D")  # noqa: E731
            norm0 = lambda x: x.decode().replace(str(self.main.dir), "D")  # noqa: E731
            self.assertEqual(norm(r.text("stats")), norm0(self.main.text("stats")))

    def test_resume_generates_nothing_new(self):
        r = Run(self.tmp, "resume", **OPTS)
        before = r.text("raw")
        p = r.go()  # second run on the same raw file
        self.assertIn("0 to process", p.stdout)
        self.assertIn("0 calls to make", p.stdout)
        self.assertEqual(r.text("raw"), before)
        self.assertEqual(r.text("out"), self.main.text("out"))

    def test_limit_zero_redoes_filters_only(self):
        r = Run(self.tmp, "lim0", limit=0, **OPTS)
        self.assertEqual(r.rows("out"), [])  # no raw file, no generation, nothing kept

    def test_dry_run_and_real_raw_do_not_mix(self):
        path = Path(self.tmp) / "mixed.jsonl"
        path.write_text(json.dumps({"chunk_id": "x", "items": [], "dry_run": True}) + "\n")
        with self.assertRaises(SystemExit):
            gq.read_raw(path, False)
        path.write_text(json.dumps({"chunk_id": "x", "items": []}) + "\n")
        with self.assertRaises(SystemExit):
            gq.read_raw(path, True)
        self.assertEqual(len(gq.read_raw(path, False)), 1)

    def test_defaults(self):
        import argparse
        src = (ROOT / "scripts/gen_questions.py").read_text()
        for frag in ('"--target", type=int, default=3800', '"--reserve", type=int, default=600',
                     '"--ooc", type=int, default=250', '"--withheld", type=int, default=400',
                     "work/v3/questions_reserve.jsonl"):
            self.assertIn(frag, src)
        self.assertIsNotNone(argparse)


if __name__ == "__main__":
    unittest.main()
