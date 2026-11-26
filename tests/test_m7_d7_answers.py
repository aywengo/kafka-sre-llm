"""M7 I6: the D7 answers chain scripts/m7_d7_answers.sh and its helper scripts/m7_d7_answers_rule.py: the stage-2 rule (the strict edge, a missing measure, one model), the
pack-file and header-before-K refusals on small fixtures, and stub dry runs end to end (both models and one model, `--arms A` first and `--arms K` later in one folder, a second
invocation that changes nothing, the W2 refusal, the W16 unload, a busy server, the request bodies against the reference call). The dry runs need the frozen index and the
chunk file (work/v3, runs/v3) and are skipped when they are not here."""
import contextlib
import hashlib
import io
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "tests"))
sys.path.insert(0, str(ROOT / "tests" / "fixtures" / "kb_i4_i5"))
import fixtures as F  # noqa: E402
import m7_d7_answers_rule as R  # noqa: E402
import test_kb_entries as TKE  # noqa: E402  (its Base builds the entries fixtures and a KbEntries with a stand-in reranker; nothing real is loaded)
from unittest import mock  # noqa: E402

DS = "DeepSeek-V4.1-Flash-1017-oQ4e-mtp"
SIX = ROOT / "work/m7/six_arm"
HAVE = all((ROOT / p).exists() for p in ("work/v3/chunks_s1.jsonl", "runs/v3/INDEX_FROZEN.json", ".venv/bin/python", "work/m4/catalogues/CATALOGUES.json"))
HAVE_SIX = HAVE and (SIX / "ans_d5_A_gemma31.jsonl").exists() and (SIX / "packs_frozen_d5.jsonl").exists() and (ROOT / "eval/m6/d5/items.jsonl").exists()


def run(args):
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = R.main(args)
    return code, out.getvalue(), err.getvalue()


def wl(p, rows):
    Path(p).write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")


def sha(p):
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


class Syntax(unittest.TestCase):
    def test_sh_n(self):
        r = subprocess.run(["sh", "-n", str(ROOT / "scripts/m7_d7_answers.sh")], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stderr)

    def test_rule_text_is_the_designs(self):
        t = R.RULE_TEXT
        for s in ("mean - 1.645 SE) is above -0.04", "sqrt(60)", "0 confirmed new harmful consequences", "Gemma 4 31B alone is the full rule", "W19", "W37", "W39", "not measured",
                  "120 answers", "deepseek-nothink-12k", "2,048-token", "thinking off"):
            self.assertIn(s, t)

    def test_rule_text_does_not_depend_on_the_options(self):
        self.assertEqual(run(["rule-text"])[1].rstrip("\n"), R.RULE_TEXT.rstrip("\n"))
        self.assertEqual(len(R.rule_sha256()), 64)

    def test_rule_text_says_gemma_only_and_the_default_is_gemma(self):
        self.assertEqual(R.DEFAULT_MODELS, "gemma31")
        self.assertIn("Gemma 4 31B 4-bit on frozen packs", R.RULE_TEXT)
        self.assertIn("Qwen3.8-27B is outside this rule", R.RULE_TEXT)
        self.assertNotIn("Both models must", R.RULE_TEXT)

    def test_models_default_order_and_refusals(self):
        self.assertEqual(R.parse_models("qwen,gemma31"), ["gemma31", "qwen"])     # Gemma first, whatever the order given
        self.assertEqual(R.parse_models("gemma31"), ["gemma31"])
        for bad in ("", "gemma26", "qwen,qwen", "qwen,mistral"):
            with self.assertRaises(R.DaError):
                R.parse_models(bad)
        self.assertEqual(run(["parse-models", "--models", "gemma26"])[0], 2)
        self.assertEqual(R.parse_arms("K,A"), ["A", "K"])
        with self.assertRaises(R.DaError):
            R.parse_arms("B")

    def test_call_counts(self):
        self.assertEqual(run(["call-counts", "--nd", "60"])[1].strip(), "ans_A_gemma31=60 ans_K_gemma31=60 judge_A=120 judge_K=120")      # W39: Gemma only by default
        self.assertEqual(run(["call-counts", "--nd", "60", "--models", "qwen,gemma31"])[1].strip(), "ans_A_gemma31=60 ans_A_qwen=60 ans_K_gemma31=60 ans_K_qwen=60 judge_A=240 judge_K=240")
        self.assertEqual(run(["call-counts", "--nd", "60", "--models", "gemma31", "--arms", "A"])[1].strip(), "ans_A_gemma31=60 judge_A=120")


def measure(model, lo_mean, se_=0.02, arms=("A", "K")):
    d = {"mean": lo_mean, "se": se_, "n": 60, "ci90": [lo_mean - R.Z90 * se_, lo_mean + R.Z90 * se_]}
    e = {"model": model, "model_id": R.MODEL_IDS[model], "arms": list(arms), "n": 60, "coverage_A": 0.7, "coverage_K": 0.7 + lo_mean, "rows_A": {"cut": 0, "empty": 0}, "rows_K": {"cut": 0, "empty": 0},
         "seconds_A": {"median": 1.0}, "seconds_K": {"median": 1.0}}
    if "K" in arms:
        e.update({"coverage_diff": d, "coverage_diff_in_slice": d, "coverage_diff_out_of_slice": d, "seconds_diff": {"median": 0.1}})
    return e


class Rule(unittest.TestCase):
    def test_holds_fails_and_the_strict_edge(self):
        self.assertEqual(R.evaluate_model(measure("qwen", 0.0))[0], "holds")
        self.assertEqual(R.evaluate_model(measure("qwen", -0.10))[0], "FAILS")
        e = measure("qwen", -0.04 + R.Z90 * 0.02)                       # the lower end is exactly -0.04: not above it
        self.assertEqual(R.evaluate_model(e)[0], "FAILS")
        self.assertEqual(R.evaluate_model(measure("qwen", -0.04 + R.Z90 * 0.02 + 0.001))[0], "holds")

    def test_missing_measure_is_not_a_pass(self):
        self.assertEqual(R.evaluate_model(measure("qwen", 0.0, arms=("A",)))[0], "not measured")
        e = measure("qwen", 0.0)
        e["coverage_diff"]["ci90"] = None
        self.assertEqual(R.evaluate_model(e)[0], "not measured")
        e["coverage_diff"]["ci90"] = [float("nan"), 0.1]
        self.assertEqual(R.evaluate_model(e)[0], "not measured")

    def rule(self, models, stub=False):
        with tempfile.TemporaryDirectory() as d:
            f = Path(d) / "m.json"
            f.write_text(json.dumps({"stub": stub, "models": models}))
            return run(["rule", "--measures", str(f)])

    def test_gemma_alone_is_the_full_rule_and_is_not_final_without_the_reading(self):
        code, out, _ = self.rule({"gemma31": measure("gemma31", 0.0)})
        self.assertEqual(code, 0)
        last = out.strip().splitlines()[-1]
        self.assertIn("Z4 part 3 NOT FINAL (the coverage condition holds for Gemma 4 31B, the full rule under W39", last)
        self.assertNotIn("NOT MEASURED", last)
        self.assertNotIn("Z4 part 3 MET", last)
        self.assertIn("needs the reading", out)
        self.assertNotIn("qwen", out)

    def test_qwen_is_outside_the_rule_whatever_it_shows(self):
        out = self.rule({"gemma31": measure("gemma31", 0.0), "qwen": measure("qwen", -0.2)})[1]
        self.assertIn("NOT FINAL", out.strip().splitlines()[-1])
        self.assertIn("qwen: outside the fixed rule (W39); reported, decides nothing: coverage FAILS", out)

    def test_qwen_alone_leaves_gemma_not_measured(self):
        code, out, _ = self.rule({"qwen": measure("qwen", 0.0)})
        self.assertEqual(code, 0)
        self.assertIn("gemma31: not measured (not in this run)", out)
        self.assertIn("Z4 part 3 NOT MEASURED (no coverage result for Gemma 4 31B", out.strip().splitlines()[-1])

    def test_gemma_arm_a_only_is_not_measured(self):
        out = self.rule({"gemma31": measure("gemma31", 0.0, arms=("A",))})[1]
        self.assertIn("Z4 part 3 NOT MEASURED", out.strip().splitlines()[-1])

    def test_a_failing_gemma_is_not_met(self):
        out = self.rule({"gemma31": measure("gemma31", -0.2)})[1]
        self.assertIn("Z4 part 3 NOT MET (the coverage condition fails for Gemma 4 31B", out.strip().splitlines()[-1])

    def test_stub_data_is_marked(self):
        self.assertIn("(STUB DATA)", self.rule({"gemma31": measure("gemma31", 0.0)}, stub=True)[1].strip().splitlines()[-1])


# ---------------------------------------------------------------- pack files and the header before K, on small fixtures
class Fixture(unittest.TestCase):
    def setUp(self):
        self._t = tempfile.TemporaryDirectory()
        self.d = Path(self._t.name)
        self.work = self.d / "w"
        self.work.mkdir()
        self.ids = ["x1", "x2", "x3"]
        wl(self.d / "items.jsonl", [{"id": i, "question": "q"} for i in self.ids])
        wl(self.d / "chunks.jsonl", [{"id": f"c{n}"} for n in range(30)])
        self.chunks = str(self.d / "chunks.jsonl")
        self.items = str(self.d / "items.jsonl")

    def tearDown(self):
        self._t.cleanup()

    def packs(self, name, per_id, **man):
        p = self.d / name
        wl(p, [{"id": i, "pack": per_id[i], "error": None} for i in self.ids])
        m = {"kind": "m6-d5-packs", "count": len(self.ids), "pack_file_sha256": sha(p)}
        m.update(man)
        Path(str(p) + ".manifest.json").write_text(json.dumps(m))
        return p

    def a(self):
        return self.packs("packs_A.jsonl", {i: [f"c{n}" for n in range(k * 3, k * 3 + 3)] for k, i in enumerate(self.ids)})

    def k(self, extra=2, fresh=False, **man):
        if fresh:      # a rewritten file keeps its old birth time, so a test that wants a new file removes the old one first
            for f in (self.d / "packs_K.jsonl", self.d / "packs_K.jsonl.manifest.json"):
                f.unlink(missing_ok=True)
        base = {i: [f"c{n}" for n in range(k * 3, k * 3 + 3)] for k, i in enumerate(self.ids)}
        return self.packs("packs_K.jsonl", {i: base[i] + [f"c{20 + k}" for k in range(extra)] for i in self.ids}, **man)

    def check(self, arm, p, a=None, record=False):
        args = ["check-packs", "--arm", arm, "--packs", str(p), "--items", self.items, "--chunks", self.chunks, "--work", str(self.work)]
        if a:
            args += ["--a-packs", str(a)]
        if record:
            args.append("--record")
        return run(args)


class PackFiles(Fixture):
    def test_good_files_pass_and_are_recorded(self):
        a, k = self.a(), self.k()
        self.assertEqual(self.check("A", a, record=True)[0], 0)
        self.assertEqual(self.check("K", k, a, record=True)[0], 0)
        rec = json.loads((self.work / "PACKS_RECORD.json").read_text())
        self.assertEqual(rec["A"]["pack_sha256"], sha(a))
        self.assertEqual(rec["K"]["pack_sha256"], sha(k))

    def test_manifest_hash_that_differs_from_the_file_is_refused(self):
        a = self.a()
        m = Path(str(a) + ".manifest.json")
        d = json.loads(m.read_text())
        d["pack_file_sha256"] = "0" * 64
        m.write_text(json.dumps(d))
        code, _, err = self.check("A", a)
        self.assertEqual(code, 2)
        self.assertIn("differs from the file's", err)

    def test_file_changed_after_it_was_recorded_is_refused(self):
        a, k = self.a(), self.k()
        self.assertEqual(self.check("K", k, a, record=True)[0], 0)
        wl(k, [{"id": i, "pack": ["c1", "c2"], "error": None} for i in self.ids])
        d = json.loads(Path(str(k) + ".manifest.json").read_text())
        d["pack_file_sha256"] = sha(k)                      # the manifest follows the file, so only the record can catch it
        Path(str(k) + ".manifest.json").write_text(json.dumps(d))
        code, _, err = self.check("K", k, a)
        self.assertEqual(code, 2)
        self.assertIn("differs from the one recorded", err)

    def test_manifest_changed_after_it_was_recorded_is_refused(self):
        a = self.a()
        self.assertEqual(self.check("A", a, record=True)[0], 0)
        m = Path(str(a) + ".manifest.json")
        d = json.loads(m.read_text())
        d["note"] = "edited"
        m.write_text(json.dumps(d))
        self.assertEqual(self.check("A", a)[0], 2)

    def test_missing_manifest_missing_scenario_and_error_rows(self):
        a = self.a()
        Path(str(a) + ".manifest.json").unlink()
        self.assertEqual(self.check("A", a)[0], 2)
        wl(a, [{"id": "x1", "pack": ["c1"], "error": None}])
        Path(str(a) + ".manifest.json").write_text(json.dumps({"count": 1, "pack_file_sha256": sha(a)}))
        self.assertEqual(self.check("A", a)[0], 2)
        wl(a, [{"id": i, "pack": ["c1"], "error": "boom" if i == "x2" else None} for i in self.ids])
        Path(str(a) + ".manifest.json").write_text(json.dumps({"count": 3, "pack_file_sha256": sha(a)}))
        self.assertEqual(self.check("A", a)[0], 2)

    def test_an_id_the_replay_cannot_render_is_refused_with_the_reason(self):
        a = self.a()
        base = {i: [f"c{n}" for n in range(k * 3, k * 3 + 3)] for k, i in enumerate(self.ids)}
        k = self.packs("packs_K.jsonl", {i: base[i] + ["entry:e0001"] for i in self.ids})
        code, _, err = self.check("K", k, a)
        self.assertEqual(code, 2)
        self.assertIn("not chunks of the frozen index", err)       # an id that is neither a chunk nor a kbe: entry

    def test_k_must_hold_the_frozen_pack_and_at_most_ten_items(self):
        a = self.a()
        k = self.packs("packs_K.jsonl", {i: ["c25", "c26", "c27"] for i in self.ids})
        self.assertEqual(self.check("K", k, a)[0], 2)
        base = {i: [f"c{n}" for n in range(k * 3, k * 3 + 3)] for k, i in enumerate(self.ids)}
        k = self.packs("packs_K.jsonl", {i: base[i] + [f"c{n}" for n in range(15, 23)] for i in self.ids})      # 11 items
        code, _, err = self.check("K", k, a)
        self.assertEqual(code, 2)
        self.assertIn("more than 10", err)
        self.assertEqual(self.check("K", self.k(), a)[0], 0)


class HeaderBeforeK(Fixture):
    def header(self, text=None):
        h = self.work / "report_header.md"
        h.write_text(text if text is not None else "# header\n\n" + R.RULE_TEXT + "\n")
        return h

    def record(self, h, k=None):
        args = ["header-record", "--work", str(self.work), "--header", str(h), "--models", "gemma31,qwen"]
        if k:
            args += ["--k-packs", str(k)]
        return run(args)

    def order(self, h, k):
        return run(["check-k-order", "--work", str(self.work), "--header", str(h), "--k-packs", str(k)])

    def test_k_created_after_the_header_is_accepted(self):
        a = self.a()
        h = self.header()
        self.assertEqual(self.record(h, self.d / "packs_K.jsonl")[0], 0)
        time.sleep(0.05)
        k = self.k()
        code, out, err = self.order(h, k)
        self.assertEqual(code, 0, err)
        self.assertIn("written before the K pack", out)

    def test_k_that_existed_before_the_header_is_refused_for_good(self):
        a = self.a()
        k = self.k()
        time.sleep(0.05)
        h = self.header()
        code, out, _ = self.record(h, k)
        self.assertEqual(code, 0)
        self.assertIn("ALREADY EXISTED", out)
        code, _, err = self.order(h, k)
        self.assertEqual(code, 2)
        self.assertIn("existed before the header", err)

    def test_k_created_before_the_header_record_without_the_note_is_still_refused_by_time(self):
        k = self.k()
        time.sleep(0.05)
        h = self.header()
        self.assertEqual(self.record(h)[0], 0)                  # the record was made without looking at K (as an older run would)
        code, _, err = self.order(h, k)
        self.assertEqual(code, 2)
        self.assertIn("not after the header", err)

    def test_a_manifest_that_says_it_is_older_is_refused(self):
        h = self.header()
        self.assertEqual(self.record(h)[0], 0)
        time.sleep(0.05)
        k = self.k(created_at="2000-01-01T00:00:00+00:00")
        code, _, err = self.order(h, k)
        self.assertEqual(code, 2)
        self.assertIn("created_at", err)
        k = self.k(created_at="not a time", fresh=True)
        self.assertEqual(self.order(h, k)[0], 2)

    def test_no_record_changed_header_and_a_header_without_the_rule_are_refused(self):
        h = self.header()
        k = self.k()
        self.assertEqual(self.order(h, k)[0], 2)                # no header record yet
        self.assertEqual(self.record(self.header("# header without the rule\n"))[0], 2)
        h = self.header()
        self.assertEqual(self.record(h)[0], 0)
        time.sleep(0.05)
        k = self.k(fresh=True)
        self.assertEqual(self.order(h, k)[0], 0)
        h.write_text(h.read_text() + "edited after it was recorded\n")
        code, _, err = self.order(h, k)
        self.assertEqual(code, 2)
        self.assertIn("hash differs", err)

    def test_touching_an_old_k_file_does_not_make_it_new(self):
        k = self.k()
        time.sleep(0.05)
        h = self.header()
        self.assertEqual(self.record(h)[0], 0)
        os.utime(k, None)                                       # modification time now later than the header; the birth time is still earlier (where the system has it)
        os.utime(str(k) + ".manifest.json", None)
        code, _, _ = self.order(h, k)
        if hasattr(os.stat(k), "st_birthtime"):
            self.assertEqual(code, 2)


# ---------------------------------------------------------------- entries (kbe:) in the K pack, replayed through the runtime's own path
Q = TKE.Q


def ke_err(msg):
    from kafka_sre_rag import kb_entries
    return kb_entries.KbEntriesError(msg)


class KbeInPacks(TKE.Base):
    """I5's K pack holds `kbe:<entry id>` items after the frozen chunks. The chain accepts them when the hash-checked entries index resolves them (kafka_sre_rag.kb_entries.default_kb,
    the path evalhooks.chunks_by_ids takes), and a replayed K item's request body is the one pipeline.ask(..., kb_entries=...) builds for the same terms. Fixtures only: no index, no model."""

    def setUp(self):
        super().setUp()
        self.kb = self.load()
        self.d = self.tmp / "chain"
        self.d.mkdir()
        self.work = self.d / "w"
        self.work.mkdir()
        wl(self.d / "items.jsonl", [{"id": "x1", "question": Q}])
        wl(self.d / "chunks.jsonl", [{"id": c} for c in F.CH])

    def packs(self, name, pack, **man):
        p = self.d / name
        wl(p, [{"id": "x1", "pack": pack, "error": None}])
        m = {"kind": "m7-d7-packs", "count": 1, "pack_file_sha256": sha(p), "created_at": "2026-11-26T20:00:00Z"}
        m.update(man)
        Path(str(p) + ".manifest.json").write_text(json.dumps(m))
        return p

    def frozen_ids(self):
        return [c["id"] for c in self.frozen()]

    def k_ids(self):
        out, info = self.kb.merge(self.frozen(), Q, "terms one, terms two")
        self.assertTrue(info["added"], "the fixture must add at least one entry")
        return [i["id"] for i in out]

    def args(self, arm, p, a=None, dry=False):
        args = ["check-packs", "--arm", arm, "--packs", str(p), "--items", str(self.d / "items.jsonl"), "--chunks", str(self.d / "chunks.jsonl"), "--work", str(self.work)]
        if a:
            args += ["--a-packs", str(a)]
        if dry:
            args.append("--dry")
        return args

    def check(self, arm, p, a=None, dry=False):
        with mock.patch("kafka_sre_rag.kb_entries.default_kb", return_value=self.kb):
            return run(self.args(arm, p, a, dry))

    def test_the_merged_ids_are_kbe_ids_and_the_k_pack_is_accepted(self):
        ids = self.k_ids()
        self.assertTrue(any(i.startswith("kbe:") for i in ids))
        a = self.packs("packs_frozen_d7.jsonl", self.frozen_ids())
        k = self.packs("packs_entries_d7.jsonl", ids)
        self.assertEqual(self.check("A", a)[0], 0)
        code, out, err = self.check("K", k, a)
        self.assertEqual(code, 0, err)
        self.assertIn("start with their frozen pack", out)

    def test_an_entry_the_index_lacks_is_refused(self):
        a = self.packs("packs_frozen_d7.jsonl", self.frozen_ids())
        k = self.packs("packs_entries_d7.jsonl", self.frozen_ids() + ["kbe:kbe-0000000000000000"])
        code, _, err = self.check("K", k, a)
        self.assertEqual(code, 2)
        self.assertIn("not in the entries index", err)

    def test_an_entries_index_that_cannot_be_used_is_refused_with_the_reason(self):
        a = self.packs("packs_frozen_d7.jsonl", self.frozen_ids())
        k = self.packs("packs_entries_d7.jsonl", self.k_ids())
        with mock.patch("kafka_sre_rag.kb_entries.default_kb", side_effect=ke_err("the sha256 of the index differs from the record")):
            code, _, err = run(self.args("K", k, a))
        self.assertEqual(code, 2)
        self.assertIn("entries index cannot be used", err)
        self.assertIn("differs from the record", err)

    def test_the_frozen_pack_holds_no_entries_and_a_stand_in_pack_is_not_for_a_real_run(self):
        a = self.packs("packs_frozen_d7.jsonl", self.k_ids())
        self.assertEqual(self.check("A", a)[0], 2)
        a = self.packs("packs_frozen_d7.jsonl", self.frozen_ids(), stand_in=True)
        code, _, err = self.check("A", a)
        self.assertEqual(code, 2)
        self.assertIn("stand_in", err)
        self.assertEqual(self.check("A", a, dry=True)[0], 0)

    def test_the_created_at_of_a_manifest_in_whole_seconds_is_judged_with_that_tolerance(self):
        import datetime
        h = self.work / "report_header.md"
        h.write_text("# h\n\n" + R.RULE_TEXT)
        self.assertEqual(run(["header-record", "--work", str(self.work), "--header", str(h), "--models", "gemma31"])[0], 0)
        t0 = json.loads((self.work / "HEADER_RECORD.json").read_text())["written_at_epoch"]
        time.sleep(0.05)

        def stamp(t):
            return datetime.datetime.fromtimestamp(t, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        k = self.packs("packs_entries_d7.jsonl", self.k_ids(), created_at=stamp(t0))          # whole seconds: the header's own second is not certainly earlier
        self.assertEqual(run(["check-k-order", "--work", str(self.work), "--header", str(h), "--k-packs", str(k)])[0], 0)
        for f in (k, Path(str(k) + ".manifest.json")):
            f.unlink()
        k = self.packs("packs_entries_d7.jsonl", self.k_ids(), created_at=stamp(t0 - 5))      # seconds before the header: refused
        code, _, err = run(["check-k-order", "--work", str(self.work), "--header", str(h), "--k-packs", str(k)])
        self.assertEqual(code, 2)
        self.assertIn("before the header", err)

    def test_the_request_body_of_a_replayed_k_item_equals_pipelines_with_entries(self):
        import eval2
        from kafka_sre_rag import config, generators, pipeline, retrieval
        ids = self.k_ids()
        with mock.patch("kafka_sre_rag.retrieval.select", return_value=self.frozen()):
            ans = pipeline.ask(Q, TKE.Gen(), config.load(), kb_entries=self.kb)
        self.assertTrue(ans.kb_entries["added"])
        self.assertEqual(ans.kb_entries["added"], [i[len("kbe:"):] for i in ids if i.startswith("kbe:")])      # the same entries, in the same order
        want_body = generators.OpenAIChatGenerator("http://example.invalid/v1", "stub", extra_body=R.EB).request_body(ans.messages, R.SA.ANSWER_LIMIT, False)
        want_parts = generators.body_parts(want_body)
        want_hash = eval2.messages_fingerprint(ans.messages)[0]
        with mock.patch("kafka_sre_rag.kb_entries.default_kb", return_value=self.kb), \
                mock.patch.object(retrieval.RT, "index", return_value=(list(F.CH.values()), None, None)):
            h, parts = R.expected_parts({"id": "x1", "question": Q}, ids, "stub", streamed=False)
            h0, parts0 = R.expected_parts({"id": "x1", "question": Q}, self.frozen_ids(), "stub", streamed=False)
        self.assertEqual(h, want_hash)
        self.assertEqual(parts, want_parts)
        self.assertNotEqual(h0, want_hash)                                                    # the frozen pack alone gives another prompt: the entries are in it
        self.assertEqual({k: v for k, v in parts0.items() if k != "messages_sha256"}, {k: v for k, v in want_parts.items() if k != "messages_sha256"})


# ---------------------------------------------------------------- the request bodies against the reference call
@unittest.skipUnless(HAVE_SIX, "the stored six-arm test (work/m7/six_arm) and the index are not here")
class BodiesAgainstTheSixArmArmA(unittest.TestCase):
    """The stored rows of the six-arm test's arm A (the real call on D5): the request body this chain's reference builds for the same item and pack has the same messages, limit,
    thinking switch and rest (model-free, and with the model's id for the one it was made with)."""

    def test_reference_equals_the_stored_arm_a_rows(self):
        items = {json.loads(l)["id"]: json.loads(l) for l in (ROOT / "eval/m6/d5/items.jsonl").read_text().splitlines() if l.strip()}
        packs = {json.loads(l)["id"]: json.loads(l)["pack"] for l in (SIX / "packs_frozen_d5.jsonl").read_text().splitlines() if l.strip()}
        rows = [json.loads(l) for l in (SIX / "ans_d5_A_gemma31.jsonl").read_text().splitlines()[:8] if l.strip()]
        man = json.loads((SIX / "ans_d5_A_gemma31.jsonl.manifest.json").read_text())
        model = man.get("model_echoed") or man.get("model")
        self.assertTrue(model)
        for r in rows:
            h, want = R.expected_parts(items[r["id"]], packs[r["id"]], model)
            got = r["draft_info"]["request_body_parts"]
            self.assertEqual(r["prompt_sha256"], h)
            for k in ("messages_sha256", "max_tokens", "enable_thinking", "rest_nomodel_sha256"):
                self.assertEqual(got[k], want[k], k)
            self.assertEqual(got["rest_sha256"], want["rest_sha256"], "the whole rest, with the model's id")


# ---------------------------------------------------------------- stub dry runs end to end
def chain(work, *args):
    r = subprocess.run(["sh", "scripts/m7_d7_answers.sh", "--dry-run", "--work", str(work), "--dry-items", "4", *args], cwd=ROOT, capture_output=True, text=True, timeout=900)
    return r.returncode, r.stdout, r.stderr


def tree(work):
    """{relative path: sha256} of every file of a work folder except the stub server's status and the logs (a second invocation changes nothing else)."""
    out = {}
    for p in sorted(Path(work).rglob("*")):
        if p.is_file() and p.name not in ("stub_server_status.json", "check.out") and "logs" not in p.parts[len(Path(work).parts):]:
            out[str(p.relative_to(work))] = sha(p)
    return out


@unittest.skipUnless(HAVE, "the frozen index and the chunk file are not here")
class DryRuns(unittest.TestCase):
    def setUp(self):
        self._t = tempfile.TemporaryDirectory()
        self.w = Path(self._t.name) / "w"

    def tearDown(self):
        self._t.cleanup()

    def test_both_models_a_then_k_in_one_folder(self):
        code, out, err = chain(self.w, "--arms", "A", "--models", "gemma31,qwen")
        self.assertEqual(code, 0, out + err)
        self.assertIn("rule sha256:", out)                                                  # printed for the conductor's ledger
        self.assertTrue((self.w / "D7_LEDGER_rule_text.md").is_file())
        head = (self.w / "report_header.md").read_text()
        self.assertIn(R.RULE_TEXT, head)
        self.assertIn("gemma31, qwen", head)                                                 # the chosen models are in the header
        self.assertIn("Qwen is in this run by explicit request and lies outside the fixed rule", head)   # W39: Qwen only by an explicit --models, recorded as outside the rule
        man = json.loads((self.w / "MANIFEST.json").read_text())
        self.assertEqual(man["options"]["models"], "gemma31,qwen")                           # and in the manifest
        self.assertFalse((self.w / "in/packs_K.jsonl").exists())                              # arm A ran before the entries pack existed
        self.assertTrue((self.w / "ans_d7_A_gemma31.jsonl").is_file() and (self.w / "ans_d7_A_qwen.jsonl").is_file())
        self.assertFalse(list(self.w.glob("ans_d7_K_*")))
        self.assertFalse((self.w / "reading").exists())                                      # no bundles before both arms exist
        self.assertIn("NOT MEASURED", out.strip().splitlines()[-1])
        order = [l for l in (self.w / "HEARTBEAT.md").read_text().splitlines() if "loading" in l]
        self.assertTrue(order[0].find("gemma-4-31b") >= 0 and order[1].find("Qwen3.8") >= 0, order)   # Gemma first, each model alone
        # the later K run resumes in the same folder
        a_before = {f: sha(self.w / f) for f in ("ans_d7_A_gemma31.jsonl", "ans_d7_A_qwen.jsonl", "judge_d7_A.jsonl", "report_header.md")}
        code, out, err = chain(self.w, "--arms", "K", "--models", "gemma31,qwen")
        self.assertEqual(code, 0, out + err)
        self.assertEqual(a_before, {f: sha(self.w / f) for f in a_before})                   # arm A and the header were not touched
        self.assertIn("written before the K pack", out)
        self.assertTrue((self.w / "ans_d7_K_gemma31.jsonl").is_file() and (self.w / "ans_d7_K_qwen.jsonl").is_file())
        self.assertIn("bundles for the readers", out)
        key = json.loads((self.w / "reading/KEY.json").read_text())
        self.assertEqual(key["seed"], R.BUNDLE_SEED)                                          # the seed is recorded
        self.assertEqual(sorted(key["sets"]), ["A_gemma31", "A_qwen", "K_gemma31", "K_qwen"])
        bundles = sorted((self.w / "reading/first").glob("d7-*.md"))
        self.assertEqual(len(bundles), 4)
        self.assertEqual(bundles[0].read_text().count("===== ANSWER"), 8)                    # four answers per scenario (BEGIN and END lines)
        last = out.strip().splitlines()[-1]
        self.assertIn("Z4 part 3 NOT FINAL", last)                                           # stub coverage holds, the harm condition waits for the reading
        self.assertIn("(STUB DATA)", last)
        # a second invocation of each changes nothing
        before = tree(self.w)
        for arm in ("A", "K", "A,K"):
            code, out, err = chain(self.w, "--arms", arm, "--models", "gemma31,qwen")
            self.assertEqual(code, 0, out + err)
        self.assertEqual(before, tree(self.w))

    def test_default_is_gemma_only(self):
        code, out, err = chain(self.w, "--arms", "A")                                          # no --models: W39's default
        self.assertEqual(code, 0, out + err)
        self.assertFalse((self.w / "ans_d7_A_qwen.jsonl").exists())
        head = (self.w / "report_header.md").read_text()
        self.assertIn("**Models of this run (in the order of their answers): gemma31**", head)
        self.assertNotIn("Qwen is in this run", head)
        self.assertEqual(json.loads((self.w / "MANIFEST.json").read_text())["options"]["models"], "gemma31")
        code, out, err = chain(self.w, "--arms", "K")
        self.assertEqual(code, 0, out + err)
        self.assertNotIn("qwen", out.split("DRY RUN")[-1].lower().replace("qwen3.8", ""))
        self.assertIn("Z4 part 3 NOT FINAL (the coverage condition holds for Gemma 4 31B", out.strip().splitlines()[-1])   # Gemma alone is the full rule, not "not measured"
        self.assertEqual(sorted(json.loads((self.w / "reading/KEY.json").read_text())["sets"]), ["A_gemma31", "K_gemma31"])
        # the models are fixed at the first run: another list is refused
        code, out, err = chain(self.w, "--arms", "K", "--models", "gemma31,qwen")
        self.assertEqual(code, 2)
        self.assertIn("option", err)

    def test_k_before_the_header_is_refused(self):
        code, out, err = chain(self.w, "--arms", "A,K", "--plant-k-first")
        self.assertEqual(code, 2, out + err)
        self.assertIn("existed before the header", err + out)
        self.assertFalse(list(self.w.glob("ans_d7_*")))                                     # nothing was answered, not even A
        # and a later --arms K in that folder is refused the same way
        code, out, err = chain(self.w, "--arms", "K")
        self.assertEqual(code, 2, out + err)
        self.assertFalse(list(self.w.glob("ans_d7_K_*")))

    def test_k_without_a_pack_file_is_refused_and_a_alone_does_not_need_it(self):
        code, out, err = chain(self.w, "--arms", "A,K")
        self.assertEqual(code, 2, out + err)
        self.assertIn("K pack file", err)
        self.assertFalse((self.w / "report_header.md").exists())

    def test_k_pack_with_a_wrong_manifest_hash_is_refused(self):
        self.assertEqual(chain(self.w, "--arms", "A")[0], 0)
        time.sleep(0.05)
        r = subprocess.run([sys.executable, "scripts/m7_d7_answers_rule.py", "dry-packs", "--items", str(self.w / "in/items.jsonl"), "--rubrics", str(self.w / "in/rubrics.jsonl"),
                            "--chunks", "work/v3/chunks_s1.jsonl", "--out-dir", str(self.w / "in"), "--arm", "K"], cwd=ROOT, capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stderr)
        m = self.w / "in/packs_K.jsonl.manifest.json"
        d = json.loads(m.read_text())
        d["pack_file_sha256"] = "f" * 64
        m.write_text(json.dumps(d))
        code, out, err = chain(self.w, "--arms", "K")
        self.assertEqual(code, 2, out + err)
        self.assertIn("differs from the file's", err)
        self.assertFalse(list(self.w.glob("ans_d7_K_*")))
        # a repaired manifest is accepted once, then the file is the recorded one: a change of the file after that is refused
        d["pack_file_sha256"] = sha(self.w / "in/packs_K.jsonl")
        m.write_text(json.dumps(d))
        code, out, err = chain(self.w, "--arms", "K")
        self.assertEqual(code, 0, out + err)
        rows = (self.w / "in/packs_K.jsonl").read_text().splitlines()
        (self.w / "in/packs_K.jsonl").write_text("\n".join(rows[::-1]) + "\n")
        d["pack_file_sha256"] = sha(self.w / "in/packs_K.jsonl")
        m.write_text(json.dumps(d))
        code, out, err = chain(self.w, "--arms", "K")
        self.assertEqual(code, 2, out + err)
        self.assertIn("recorded", err)

    def test_w2_a_foreign_model_refuses_and_nothing_is_unloaded(self):
        code, out, err = chain(self.w, "--arms", "A", "--plant-foreign", "some-other-clients-model")
        self.assertEqual(code, 2, out + err)
        self.assertIn("loaded by another client", err)
        self.assertIn("some-other-clients-model", err)
        self.assertFalse(list(self.w.glob("ans_d7_*")))
        st = json.loads((self.w / "stub_server_status.json").read_text())
        self.assertEqual(st["loaded_models"], ["some-other-clients-model"])                  # nothing was unloaded
        self.assertNotIn("unloaded", (self.w / "HEARTBEAT.md").read_text() if (self.w / "HEARTBEAT.md").exists() else "")

    def test_w2_a_foreign_model_that_appears_later_refuses(self):
        code, out, err = chain(self.w, "--arms", "A", "--models", "gemma31,qwen", "--plant-load-before", "ans_A_qwen=some-other-clients-model")
        self.assertEqual(code, 2, out + err)
        self.assertTrue((self.w / "ans_d7_A_gemma31.jsonl").is_file())                      # the first model was answered and unloaded
        self.assertFalse((self.w / "ans_d7_A_qwen.jsonl").exists())
        self.assertIn("loaded by another client", err)

    def test_w16_a_foreign_deepseek_is_unloaded_and_logged(self):
        code, out, err = chain(self.w, "--arms", "A", "--models", "gemma31", "--plant-foreign", DS)
        self.assertEqual(code, 0, out + err)
        hb = (self.w / "HEARTBEAT.md").read_text()
        self.assertIn("W16", hb)
        self.assertIn(f"unloaded {DS}: W16's unload of a foreign model", hb)

    def test_a_busy_server_is_refused(self):
        code, out, err = chain(self.w, "--arms", "A", "--plant-busy", "2")
        self.assertEqual(code, 2, out + err)
        self.assertIn("active or waiting", err)
        self.assertFalse(list(self.w.glob("ans_d7_*")))

    def test_a_dry_run_needs_its_own_folder_and_never_uses_the_real_ones(self):
        r = subprocess.run(["sh", "scripts/m7_d7_answers.sh", "--dry-run"], cwd=ROOT, capture_output=True, text=True)
        self.assertEqual(r.returncode, 2)
        self.assertIn("needs --work", r.stderr)
        for w in ("work/m7/d7_answers", "work/m7/d7/x"):
            r = subprocess.run(["sh", "scripts/m7_d7_answers.sh", "--dry-run", "--work", w], cwd=ROOT, capture_output=True, text=True)
            self.assertEqual(r.returncode, 2)
            self.assertIn("never uses the real work folder", r.stderr)

    def test_request_bodies_equal_the_reference_and_a_planted_difference_fails(self):
        self.assertEqual(chain(self.w, "--arms", "A", "--models", "gemma31")[0], 0)
        base = ["bodies-check", "--items", str(self.w / "items_d7_h.jsonl"), "--packs", str(self.w / "in/packs_A.jsonl"), "--model-id", "stub", "--non-streamed"]
        ans = self.w / "ans_d7_A_gemma31.jsonl"
        code, out, _ = run(base + ["--answers", str(ans)])
        self.assertEqual(code, 0, out)
        self.assertIn("4 of 4 rows carry the request body", out)
        rows = [json.loads(l) for l in ans.read_text().splitlines()]
        for name, edit in (("limit", lambda r: r["draft_info"]["request_body_parts"].__setitem__("max_tokens", 1024)),
                           ("thinking", lambda r: r["draft_info"]["request_body_parts"].__setitem__("enable_thinking", True)),
                           ("messages", lambda r: r["draft_info"]["request_body_parts"].__setitem__("messages_sha256", "0" * 64)),
                           ("prompt", lambda r: r.__setitem__("prompt_sha256", "0" * 64))):
            bad = json.loads(json.dumps(rows))
            edit(bad[1])
            p = self.w.parent / f"planted_{name}.jsonl"
            wl(p, bad)
            code, out, _ = run(base + ["--answers", str(p)])
            self.assertEqual(code, 1, name)
            self.assertIn("3 of 4 rows", out)
        # the K arm of the same call differs from A only by the pack: the chain's own relation checks run in the full run (test_both_models_a_then_k_in_one_folder)


if __name__ == "__main__":
    unittest.main()
