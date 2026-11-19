"""Retrieval profiles (WP5 T5): frozen is the default and unchanged; r1 and r2 are marked unmeasured."""
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "tests"))
import eval2  # noqa: E402
import test_eval2 as T  # noqa: E402
from kafka_sre_rag import __main__ as cli, config, evalhooks, pipeline, profiles, retrieval, service  # noqa: E402

CHUNKS = [{"id": "a", "source": "apache-kafka", "title": "Consumer configs", "path": "raw/a/b.md",
           "text": "Set max.poll.interval.ms.", "bm25_rank": 1, "ce_score": 0.9}]


class RefGen:
    """Counts as the reference generator, so only the profile can make an answer unmeasured."""
    def __init__(self):
        ref = config.load()["reference_generator"]
        self.identity = {"backend": "mlx", "model": "/x/snapshots/" + ref["snapshot"], "adapter": None}

    def chat(self, messages, max_tokens, on_token=None):
        return "terms, a", 1, False


class TestCandidateFile(unittest.TestCase):
    def test_index_is_the_frozen_one_for_now(self):
        rec = profiles.load_candidate()["index"]
        cfg = config.load()
        self.assertEqual(rec["index_path"], cfg["index"]["index_path"])
        self.assertEqual(rec["sha256"], cfg["index"]["sha256"])
        self.assertEqual(profiles.index_file("r1"), config.index_file())

    def test_settings_hash(self):
        h1, h2 = profiles.settings_sha256("r1"), profiles.settings_sha256("r2")
        self.assertRegex(h1, r"^[0-9a-f]{64}$")
        self.assertNotEqual(h1, h2)
        blob = json.dumps(profiles.settings("r1"), sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        self.assertEqual(h1, hashlib.sha256(blob.encode()).hexdigest())
        cand = profiles.load_candidate()
        cand["profiles"]["r1"]["k"] = 9
        self.assertNotEqual(profiles.settings_sha256("r1", cand), h1)  # any setting changes the hash
        cand = profiles.load_candidate()
        cand["index"]["sha256"] = "0" * 64
        self.assertNotEqual(profiles.settings_sha256("r2", cand), h2)  # so does the index record

    def test_r1_r2_settings(self):
        p = profiles.load_candidate()["profiles"]
        self.assertEqual((p["r1"]["k"], p["r1"]["max_chars"], p["r1"]["lookup"], p["r1"]["runbook_cap"]), (8, 20000, 2, 1))
        self.assertEqual(p["r1"]["fuse"], ["bm25", "terms", "frozen"])
        self.assertEqual(p["r2"]["fuse"], ["bm25", "terms"])
        self.assertEqual(p["r2"]["runbook_cap"], -1)

    def test_frozen_has_no_candidate_settings_and_unknown_profile_fails(self):
        with self.assertRaises(ValueError):
            profiles.settings("frozen")
        with self.assertRaises(ValueError):
            profiles.check("r9")

    def test_manifest_fields(self):
        self.assertEqual(profiles.manifest_fields("frozen"), {})
        f = profiles.manifest_fields("r1")
        self.assertEqual(f, {"retrieval_profile": "r1", "retrieval_settings_sha256": profiles.settings_sha256("r1"),
                             "measured": False})

    def test_verify_index(self):
        profiles.verify_index("frozen")
        if config.index_file().exists():
            profiles.verify_index("r1")


class TestOrdering(unittest.TestCase):
    PROF = {"fuse": ["bm25", "terms", "frozen"], "bm25_depth": 3, "lookup": 0, "lookup_score_depth": 1000,
            "lookup_max_chunks": 12, "runbook_cap": 1}

    def order(self, prof=None, **kw):
        a = dict(bm=["a", "b", "c", "d"], score={}, term_lists=[["d"]], frozen_list=["c", "b"],
                 source_of={"a": "runbooks", "b": "runbooks", "c": "strimzi", "d": "strimzi"},
                 lookup=profiles.Lookup([]))
        a.update(kw)
        return profiles.order(prof or self.PROF, "q", "", **a)

    def test_depth_cap_and_fusion(self):
        rk = self.order()
        self.assertEqual(sum(1 for c in rk if c in ("a", "b")), 1)   # at most one runbook chunk
        self.assertEqual(sorted(rk), ["b", "c", "d"])               # b outranks a in the fusion

    def test_frozen_component_dropped_without_reranker(self):
        self.assertEqual(self.order(frozen_list=None), profiles.order(
            {**self.PROF, "fuse": ["bm25", "terms"]}, "q", "", bm=["a", "b", "c", "d"], score={}, term_lists=[["d"]],
            frozen_list=None, source_of={"a": "runbooks", "b": "runbooks", "c": "strimzi", "d": "strimzi"},
            lookup=profiles.Lookup([])))

    def test_lookup_goes_first(self):
        ch = [{"id": "x", "title": "Config > max.poll.interval.ms", "text": "t", "path": "p"}]
        self.assertEqual(profiles.order({**self.PROF, "lookup": 1, "runbook_cap": -1}, "set `max.poll.interval.ms`", "",
                                        bm=["a"], score={}, term_lists=[], frozen_list=None,
                                        source_of={"a": "kafka", "x": "kafka"}, lookup=profiles.Lookup(ch))[:2], ["x", "a"])

    def test_select_ranked_char_limit(self):
        self.assertEqual(profiles.select_ranked(["a", "b", "c"], {"a": 900, "b": 900, "c": 900}, 8, 1500), ["a", "b"])
        self.assertEqual(profiles.select_ranked(["a", "b"], {"a": 900, "b": 900}, 8, 1300), ["a"])


class TestRetrievalSelect(unittest.TestCase):
    def tiny_index(self):
        d = tempfile.mkdtemp()
        self.addCleanup(__import__("shutil").rmtree, d, True)
        rows = [{"id": f"c{i}", "source": "runbooks" if i < 3 else "strimzi", "title": f"T{i}", "path": f"raw/{i}.md",
                 "text": f"alpha beta lag topic{i} " * 20} for i in range(8)]
        p = Path(d) / "idx.jsonl"
        p.write_text("\n".join(json.dumps(r) for r in rows))
        return str(p)

    def test_r2_without_model(self):
        p = self.tiny_index()
        got = retrieval.select("alpha lag", "topic4, topic5", profile="r2", index_path=p)
        ids = [c["id"] for c in got]
        self.assertLessEqual(len(ids), 8)
        self.assertEqual(len(ids), len(set(ids)))
        self.assertTrue(all("bm25_rank" in c and c["ce_score"] is None for c in got))
        self.assertEqual(ids, [c["id"] for c in retrieval.select("alpha lag", "topic4, topic5", profile="r2", index_path=p)])

    def test_r1_without_reranker_runs_and_caps_runbooks(self):
        p = self.tiny_index()
        got = retrieval.select("alpha lag", "topic0, topic1, topic2", profile="r1", index_path=p, rerank=False)
        self.assertLessEqual(sum(1 for c in got if c["source"] == "runbooks"), 1)

    def test_candidate_needs_k6(self):
        with self.assertRaises(ValueError):
            retrieval.select("q", "", k="k3", profile="r1")

    def test_unknown_profile(self):
        with self.assertRaises(ValueError):
            retrieval.select("q", "", profile="r9")


class TestAsk(unittest.TestCase):
    def run_ask(self, **kw):
        with mock.patch("kafka_sre_rag.retrieval.select", return_value=CHUNKS) as sel:
            ans = pipeline.ask("q?", RefGen(), config.load(), **kw)
        return ans, sel

    def test_default_is_frozen_and_unchanged(self):
        ans, sel = self.run_ask()
        self.assertTrue(ans.measured)
        self.assertEqual(ans.profile_fields, {})
        self.assertEqual(ans.unmeasured_reasons, [])
        self.assertNotIn("profile", sel.call_args.kwargs)      # the frozen call carries no new argument
        ans2, _ = self.run_ask(profile="frozen")
        self.assertTrue(ans2.measured)

    def test_candidates_are_unmeasured_and_recorded(self):
        for name in ("r1", "r2"):
            ans, sel = self.run_ask(profile=name)
            self.assertFalse(ans.measured)
            self.assertEqual(sel.call_args.kwargs["profile"], name)
            self.assertEqual(ans.profile_fields["retrieval_profile"], name)
            self.assertEqual(ans.profile_fields["retrieval_settings_sha256"], profiles.settings_sha256(name))
            self.assertIs(ans.profile_fields["measured"], False)
            self.assertTrue(any(f"retrieval profile {name}" in r for r in ans.unmeasured_reasons))

    def test_unknown_profile(self):
        with self.assertRaises(ValueError):
            self.run_ask(profile="r9")

    def test_replayed_pack_refuses_a_profile(self):
        with self.assertRaises(ValueError):
            evalhooks.run_item("q?", RefGen(), config.load(), pack_ids=["x"], profile="r1")


class TestCli(unittest.TestCase):
    def test_flag_default_and_choices(self):
        for cmd in ("ask q", "chat", "serve"):
            self.assertEqual(cli.build_parser().parse_args(cmd.split()).profile, "frozen")
        self.assertEqual(cli.build_parser().parse_args(["ask", "q", "--profile", "r2"]).profile, "r2")
        with self.assertRaises(SystemExit), mock.patch("sys.stderr"):
            cli.build_parser().parse_args(["ask", "q", "--profile", "r9"])

    def test_dry_run_prints_profile_and_hash(self):
        import io
        buf = io.StringIO()
        sel = mock.Mock(return_value=CHUNKS)
        with mock.patch("kafka_sre_rag.retrieval.select", sel), mock.patch("sys.stdout", buf), \
                mock.patch.object(cli, "make_generator", side_effect=lambda a: cli.StubGenerator(out=buf)), \
                mock.patch("sys.stderr", io.StringIO()):
            rc = cli.main(["ask", "--dry-run", "--profile", "r1", "q?"])
        self.assertEqual(rc, 0)
        out = buf.getvalue()
        self.assertIn("retrieval profile: r1", out)
        self.assertIn(profiles.settings_sha256("r1"), out)
        self.assertIn("Set max.poll.interval.ms.", out)   # the pack is in the printed request

    def test_dry_run_frozen_prints_no_profile_line(self):
        import io
        buf = io.StringIO()
        with mock.patch("kafka_sre_rag.retrieval.select", return_value=CHUNKS), mock.patch("sys.stdout", buf), \
                mock.patch.object(cli, "make_generator", side_effect=lambda a: cli.StubGenerator(out=buf)), \
                mock.patch("sys.stderr", io.StringIO()):
            cli.main(["ask", "--dry-run", "q?"])
        self.assertNotIn("retrieval profile", buf.getvalue())

    def test_json_output_fields(self):
        import io
        buf = io.StringIO()
        with mock.patch("kafka_sre_rag.retrieval.select", return_value=CHUNKS), mock.patch("sys.stdout", buf), \
                mock.patch.object(cli, "make_generator", side_effect=lambda a: cli.StubGenerator(out=buf)), \
                mock.patch("sys.stderr", io.StringIO()):
            cli.main(["ask", "--dry-run", "--json", "--profile", "r2", "q?"])
        d = json.loads(buf.getvalue().splitlines()[-1])
        self.assertEqual((d["retrieval_profile"], d["measured"]), ("r2", False))
        self.assertEqual(d["retrieval_settings_sha256"], profiles.settings_sha256("r2"))
        buf = io.StringIO()
        with mock.patch("kafka_sre_rag.retrieval.select", return_value=CHUNKS), mock.patch("sys.stdout", buf), \
                mock.patch.object(cli, "make_generator", side_effect=lambda a: cli.StubGenerator(out=buf)), \
                mock.patch("sys.stderr", io.StringIO()):
            cli.main(["ask", "--dry-run", "--json", "q?"])
        self.assertNotIn("retrieval_profile", json.loads(buf.getvalue().splitlines()[-1]))


class TestService(unittest.TestCase):
    def answer(self, profile=None):
        kw = {} if profile is None else {"profile": profile}
        svc = service.Service(RefGen(), config.load(), verified=True, **kw)
        with mock.patch("kafka_sre_rag.retrieval.select", return_value=CHUNKS):
            return svc, svc.answer("q?", "default", "k6")[1]

    def test_default(self):
        svc, extra = self.answer()
        self.assertTrue(extra["measured"])
        self.assertNotIn("retrieval_profile", extra)
        self.assertNotIn("retrieval_profile", svc.health())

    def test_candidate(self):
        svc, extra = self.answer("r1")
        self.assertFalse(extra["measured"])
        self.assertEqual(extra["retrieval_profile"], "r1")
        self.assertEqual(extra["retrieval_settings_sha256"], profiles.settings_sha256("r1"))
        self.assertEqual(svc.health()["retrieval_profile"], "r1")


class TestEval2Manifest(T.Base):
    def manifest(self, *extra):
        f, out = self.items_file([T.item(1)]), self.tmp / "a.jsonl"
        self.run_answer(f, out, *extra)
        return eval2.read_manifest(out), eval2.read_jsonl(out)

    def test_default_manifest_has_no_profile_fields(self):
        m, _ = self.manifest()
        self.assertNotIn("retrieval_profile", m)
        self.assertNotIn("retrieval_settings_sha256", m)
        self.assertNotIn("measured", m)
        self.assertEqual(eval2.build_parser().parse_args(
            ["answer", "--items", "i", "--out", "o", "--config", "c"]).profile, "frozen")

    def test_candidate_manifest(self):
        m, rows = self.manifest("--profile", "r2")
        self.assertEqual((m["retrieval_profile"], m["measured"]), ("r2", False))
        self.assertEqual(m["retrieval_settings_sha256"], profiles.settings_sha256("r2"))
        self.assertTrue(any("retrieval profile r2" in r for r in m["unmeasured_reasons"]))
        self.assertIsNone(rows[0]["error"])

    def test_profile_with_replay_is_refused(self):
        f, out = self.items_file([T.item(1)]), self.tmp / "a.jsonl"
        src = self.tmp / "src.jsonl"
        self.run_answer(f, src)
        with self.assertRaises(eval2.EvalError):
            self.run_answer(f, out, "--profile", "r1", "--replay-packs", str(src))


if __name__ == "__main__":
    unittest.main()
