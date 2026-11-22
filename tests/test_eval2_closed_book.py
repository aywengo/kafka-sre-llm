"""E2 build B2: closed-book mode (configuration P0), the M4 reference label in the manifest, and pack replay on hidden
files. Stub generator, synthetic items, temporary key; nothing under eval/hidden/, no model, no server."""
import hashlib
import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "tests"))
import eval2  # noqa: E402
import eval2_judge as ej  # noqa: E402
import eval2_score as sc  # noqa: E402
from kafka_sre_rag import prompts  # noqa: E402
from test_eval2 import Base, Gen, item  # noqa: E402
import test_eval2_hidden_judge as thj  # noqa: E402
import test_eval2_score as tsc  # noqa: E402

NEW_KEYS = {"m4_config", "model_listed", "m4_reference_model", "is_m4_reference", "prompt_hash_form"}
OLD_KEYS = {"harness", "config", "backend", "model_requested", "model_echoed", "model_build", "adapter", "mode", "k",
            "rerank", "extra_body", "max_tokens", "unmeasured_reasons", "index_sha256", "reranker_revision",
            "catalogue_sha256", "runtime_git_commit", "runtime_tree_dirty", "concurrency", "host_class",
            "items_sha256", "items_count", "subset", "replay", "hidden", "started", "ended", "run"}


class ListingGen(Gen):
    """A generator that looks like the server path: backend openai, and a model list the manifest reads."""

    def __init__(self, model, listed):
        super().__init__()
        self.identity = {"backend": "openai", "model": model, "adapter": None}
        self.model, self.headers, self.models_url = model, {}, "http://x/v1/models"
        self.client = mock.Mock()
        self.client.get.return_value.json.return_value = {"data": [{"id": listed}] if listed else []}


class Closed(Base):
    def test_closed_book_calls_only_the_plain_prompt_and_writes_an_empty_pack(self):
        f = self.items_file([item(1), item(2)])
        out, gen = self.tmp / "p0.jsonl", Gen()
        with mock.patch("kafka_sre_rag.retrieval.select") as sel:
            rc, text = self.run_answer(f, out, "--mode", "closed", gen=gen)
        self.assertEqual(rc, 0)
        sel.assert_not_called()
        self.assertEqual(len(gen.calls), 2)                          # one call per item: no search-terms call
        for (msgs, _), it in zip(gen.calls, [item(1), item(2)]):
            self.assertEqual(msgs, [{"role": "system", "content": prompts.SYSTEM_PROMPT},
                                    {"role": "user", "content": it["question"]}])
        rows = eval2.read_jsonl(out)
        self.assertTrue(all(r["pack"] == [] and r["closed_book"] is True and r["search_terms"] is None for r in rows))
        self.assertTrue(all(r["seconds"]["search_terms"] == 0.0 and r["seconds"]["retrieval"] == 0.0 for r in rows))
        m = eval2.read_manifest(out)
        self.assertEqual(m["mode"], "closed")
        self.assertIn("mode closed", m["unmeasured_reasons"])
        self.assertEqual(m["closed_book"]["system_prompt_sha256"],
                         hashlib.sha256(prompts.SYSTEM_PROMPT.encode()).hexdigest())

    def test_closed_book_prompt_is_the_v3_plain_base_prompt(self):
        from build_dataset import SYSTEM_PROMPT   # what eval/compare.py answer() sent for "base"
        self.assertEqual(SYSTEM_PROMPT, prompts.SYSTEM_PROMPT)

    def test_closed_book_tier_b_evidence_is_part_of_the_question(self):
        it = {**item(1, tier="B"), "evidence": "log line"}
        f, out, gen = self.items_file([it]), self.tmp / "p0.jsonl", Gen()
        self.run_answer(f, out, "--mode", "closed", gen=gen)
        self.assertEqual(gen.calls[0][0][1]["content"], it["question"] + "\n\nEvidence:\nlog line")

    def test_closed_refuses_replay_and_retrieval_options(self):
        f = self.items_file([item(1)])
        src = self.tmp / "d.jsonl"
        self.run_answer(f, src)
        for extra in (["--replay-packs", str(src)], ["--profile", "r1"], ["--k", "k3"], ["--no-rerank"]):
            with self.assertRaises(eval2.EvalError, msg=extra):
                self.run_answer(f, self.tmp / "p0.jsonl", "--mode", "closed", *extra)
        self.assertFalse((self.tmp / "p0.jsonl").exists())

    def test_replay_from_a_closed_book_run_is_refused(self):
        f = self.items_file([item(1)])
        p0 = self.tmp / "p0.jsonl"
        self.run_answer(f, p0, "--mode", "closed")
        with self.assertRaises(eval2.EvalError) as cm:
            self.run_answer(f, self.tmp / "s.jsonl", "--mode", "strict", "--replay-packs", str(p0))
        self.assertIn("closed-book", str(cm.exception))
        p0.with_name("p0.jsonl.manifest.json").unlink()          # even without its manifest: empty packs are refused
        with self.assertRaises(eval2.EvalError):
            self.run_answer(f, self.tmp / "s.jsonl", "--mode", "strict", "--replay-packs", str(p0))

    def test_hidden_closed_run_writes_only_enc_and_logs_ids(self):
        marker = "SECRETQ"
        f = self.items_file([item(1, question=f"{marker} one?"), item(2, question=f"{marker} two?")], "h.jsonl.enc")
        out = self.tmp / "p0.jsonl.enc"
        rc, text = self.run_answer(f, out, "--mode", "closed")
        self.assertEqual(rc, 0)
        self.assertNotIn(marker, text)
        self.assertIn("id=i001", text)
        self.assertEqual(sorted(p.name for p in self.tmp.iterdir() if p.name.startswith("p0")),
                         ["p0.jsonl.enc", "p0.jsonl.enc.manifest.json.enc"])
        self.assertNotIn(marker.encode(), out.read_bytes())
        self.assertEqual(eval2.read_jsonl(out)[0]["pack"], [])
        self.assertEqual(eval2.read_manifest(out)["mode"], "closed")


class ReferenceLabel(Base):
    def manifest(self, gen, *extra, name="cfg-q4"):
        f = self.items_file([item(1)])
        out = self.tmp / (name + ".jsonl")
        buf = io.StringIO()
        a = eval2.build_parser().parse_args(["answer", "--items", str(f), "--out", str(out), "--config", name,
                                             "--backend", "stub", *extra])
        with mock.patch.object(eval2, "make_generator", return_value=gen):
            eval2.cmd_answer(a, out=buf)
        return eval2.read_manifest(out)

    def test_reference_when_the_server_lists_the_reference_model(self):
        m = self.manifest(ListingGen("Qwen3.8-27B-4bit", "Qwen3.8-27B-4bit"))
        self.assertEqual((m["m4_config"], m["model_listed"], m["m4_reference_model"], m["is_m4_reference"]),
                         ("cfg-q4", "Qwen3.8-27B-4bit", eval2.M4_REFERENCE_MODEL, True))
        self.assertEqual(eval2.M4_REFERENCE_MODEL, "Qwen3.8-27B-4bit")
        # the v3 label is unchanged: this build is still not the v3 in-process reference
        self.assertIn("generator is not the reference model", m["unmeasured_reasons"])

    def test_other_builds_are_not_the_reference(self):
        for model in ("mlx-community--Qwen3.8-27B-bf16", "Gemma-4-31B"):
            m = self.manifest(ListingGen(model, model), name="x-" + model[-4:])
            self.assertEqual((m["model_listed"], m["is_m4_reference"]), (model, False))

    def test_model_not_listed_or_stub_is_not_the_reference(self):
        m = self.manifest(ListingGen("Qwen3.8-27B-4bit", None))
        self.assertEqual((m["model_listed"], m["is_m4_reference"]), (None, False))
        m = self.manifest(Gen(), name="stubrun")
        self.assertEqual((m["model_listed"], m["is_m4_reference"], m["m4_config"]), (None, False, "stubrun"))

    def test_one_file_manifest_differs_only_by_the_new_fields(self):
        m = self.manifest(Gen())
        self.assertEqual(set(m) - OLD_KEYS, NEW_KEYS)
        self.assertEqual(OLD_KEYS - set(m), set())
        self.assertNotIn("closed_book", m)


class HiddenReplay(Base):
    def test_strict_replays_the_default_packs_on_hidden_files(self):
        marker = "SECRETQ"
        f = self.items_file([item(1, question=f"{marker} one?"), item(3, question=f"{marker} three?")], "h.jsonl.enc")
        d, s = self.tmp / "d.jsonl.enc", self.tmp / "s.jsonl.enc"
        self.run_answer(f, d)
        gen = Gen()
        rc, text = self.run_answer(f, s, "--mode", "strict", "--replay-packs", str(d), gen=gen)
        self.assertEqual(rc, 0)
        self.assertNotIn(marker, text)
        self.assertEqual([r["pack"] for r in eval2.read_jsonl(s)], [r["pack"] for r in eval2.read_jsonl(d)])
        self.assertTrue(all(r["replayed_from"] == eval2.read_manifest(d)["run"] for r in eval2.read_jsonl(s)))
        self.assertEqual(len(gen.calls), 2)                          # no search-terms call on replay
        m = eval2.read_manifest(s)
        self.assertEqual(m["replay"]["run"], eval2.read_manifest(d)["run"])
        self.assertTrue(m["hidden"])
        self.assertEqual(sorted(p.name for p in self.tmp.iterdir() if p.name.startswith("s.")),
                         ["s.jsonl.enc", "s.jsonl.enc.manifest.json.enc"])

    def test_open_replay_source_for_a_hidden_run_is_refused(self):
        fo, fh = self.items_file([item(1)]), self.items_file([item(1)], "h.jsonl.enc")
        d = self.tmp / "d.jsonl"
        self.run_answer(fo, d)
        with self.assertRaises(eval2.EvalError):
            self.run_answer(fh, self.tmp / "s.jsonl.enc", "--replay-packs", str(d))


class OtherStepsAcceptClosedRows(unittest.TestCase):
    """The scorer, the decline outcome, the identifier check and the judge on rows with an empty pack."""

    def test_scorer_and_decline_outcome(self):
        t = tsc.TestDeclineOutcome
        t.setUpClass()
        it = t.cfg_item(t(), )
        for pack in ([], None):
            answers = [{"id": "t-1", "text": "ANSWER: zz.beta.ms", "pack": pack, "closed_book": True}]
            rows, missing, extra = sc.score_files([it], answers, tsc.Catalogues.load(tsc.fixture_catalogues()))
            sc.add_outcomes(rows, [it], answers, t.idx)
            self.assertEqual((missing, extra, rows[0]["outcome"]), (0, 0, "wrong"))
            self.assertFalse(rows[0]["coverage"]["covering_in_pack"])
        ok = [{"id": "t-1", "text": "ANSWER: zz.alpha.ms", "pack": []}]
        rows, _, _ = sc.score_files([it], ok, tsc.Catalogues.load(tsc.fixture_catalogues()))
        sc.add_outcomes(rows, [it], ok, t.idx)
        self.assertEqual(rows[0]["outcome"], "pass")

    def test_identifier_check(self):
        import eval2_idcheck as idc
        if not (ROOT / "work/m4/catalogues/CATALOGUES.json").is_file():
            self.skipTest("catalogues not built")
        rows = [{"id": "a", "text": "Use `kafka-topics.sh --list`.", "pack": [], "closed_book": True, "error": None}]
        out = idc.report_block(rows, True)
        self.assertIn("answers 1  checked 1", out)

    def test_judge_stub_on_open_and_hidden_rows_with_empty_packs(self):
        for hidden in (False, True):
            case = _Judge("runTest")
            case.setUp()
            try:
                d = case.inputs(hidden=hidden, sets=("A",))
                rows = [{**thj.answer(i, "A"), "pack": [], "closed_book": True} for i in range(case.N)]
                d["A"] = case.put("ans_closed", rows, hidden)
                rc, so, se = case.judge(case.run_args(d, sets=("A",)))
                self.assertEqual(rc, 0, se)
                self.assertIn(f"matched A: {case.N} answers", so)
                case.assertNoSecret(so + se)
            finally:
                case.doCleanups()


class _Judge(thj.Base):
    def runTest(self):
        pass


if __name__ == "__main__":
    unittest.main()
