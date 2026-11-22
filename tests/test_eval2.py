import contextlib
import io
import json
import os
import shutil
import stat
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
import eval2  # noqa: E402
from kafka_sre_rag import config, evalhooks, pipeline, prompts  # noqa: E402

NEEDS_OPENSSL = unittest.skipUnless(shutil.which("openssl"), "openssl not on PATH")
CHUNKS = {c["id"]: c for c in [
    {"id": "c1", "source": "apache-kafka", "title": "T1", "path": "raw/a.md", "text": "Set max.poll.interval.ms."},
    {"id": "c2", "source": "strimzi", "title": "", "path": "raw/b.md", "text": "Use kafka-consumer-groups.sh."},
    {"id": "c3", "source": "kubernetes", "title": "T3", "path": "raw/c.md", "text": "Pods restart."},
]}


class Gen:
    """Records every call; expansion call is recognised by its system prompt."""
    identity = {"backend": "stub", "model": "stub", "adapter": None}

    def __init__(self, fail_on=None):
        self.calls, self.fail_on = [], fail_on

    def chat(self, messages, max_tokens, on_token=None):
        self.calls.append((messages, max_tokens))
        if messages[0]["content"] == prompts.EXPAND_SYSTEM:
            return "terms one, terms two", 3, False
        if self.fail_on and self.fail_on in messages[1]["content"]:
            raise RuntimeError("boom " + messages[1]["content"][:60])
        return "answer text", 11, False


def item(i, tier="A", kind="k1", family=None, question=None):
    return {"id": f"i{i:03d}", "tier": tier, "kind": kind, "family": family or f"f{i:03d}", "domain": "kafka",
            "question": question or f"question number {i}?", "extra": {"keep": i}}


def select_stub(question, expansion, **kw):
    return [CHUNKS["c1"], CHUNKS["c2"]] if "3" not in question else [CHUNKS["c3"], CHUNKS["c1"]]


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.key = self.tmp / "key"
        eval2.keygen(self.key)
        p = mock.patch.dict(os.environ, {"EVAL_HIDDEN_KEY": str(self.key)})
        p.start()
        self.addCleanup(p.stop)
        for target, kw in (("kafka_sre_rag.retrieval.select", {"side_effect": select_stub}),
                           ("kafka_sre_rag.evalhooks.chunks_by_ids",
                            {"side_effect": lambda ids, index_path=None: [CHUNKS[i] for i in ids]})):
            m = mock.patch(target, **kw)
            m.start()
            self.addCleanup(m.stop)

    def items_file(self, items, name="items.jsonl"):
        p = self.tmp / name
        eval2.write_jsonl(p, items)
        return p

    def args(self, items, out, *extra, backend="stub"):
        return eval2.build_parser().parse_args(
            ["answer", "--items", str(items), "--out", str(out), "--config", "cfg", "--backend", backend,
             "--host-class", "test box", *extra])

    def run_answer(self, items, out, *extra, gen=None):
        buf = io.StringIO()
        with mock.patch.object(eval2, "make_generator", return_value=gen or Gen()):
            rc = eval2.cmd_answer(self.args(items, out, *extra), out=buf)
        return rc, buf.getvalue()


class TestHooks(Base):
    def test_same_messages_as_pipeline_ask(self):
        g1, g2 = Gen(), Gen()
        ans = pipeline.ask("q 3?", g1, config.load(), mode="strict")
        r = evalhooks.run_item("q 3?", g2, config.load(), mode="strict")
        self.assertEqual(g1.calls, g2.calls)
        self.assertEqual(r["messages"], ans.messages)
        self.assertEqual(r["pack"], ["c3", "c1"])
        self.assertEqual(set(r["seconds"]), {"search_terms", "retrieval", "answer", "total"})

    def test_replay_skips_expansion(self):
        g = Gen()
        r = evalhooks.run_item("q?", g, config.load(), pack_ids=["c2", "c1"])
        self.assertEqual(len(g.calls), 1)
        self.assertEqual(g.calls[0][0], prompts.answer_messages("q?", [CHUNKS["c2"], CHUNKS["c1"]]))
        self.assertIsNone(r["search_terms"])


class TestAnswer(Base):
    def test_rows_manifest_and_progress(self):
        f, out = self.items_file([item(1), item(2)]), self.tmp / "a.jsonl"
        rc, text = self.run_answer(f, out, "--max-tokens", "999")
        self.assertEqual(rc, 0)
        rows = eval2.read_jsonl(out)
        self.assertEqual([r["id"] for r in rows], ["i001", "i002"])
        r = rows[0]
        self.assertEqual(set(r), {"id", "text", "tokens", "truncated", "seconds", "search_terms", "pack", "error", "run",
                                  "prompt_sha256", "prompt_chars", "search_terms_prompt_sha256", "search_terms_prompt_chars"})
        self.assertEqual((r["text"], r["tokens"], r["search_terms"], r["pack"]), ("answer text", 11, "terms one, terms two", ["c1", "c2"]))
        m = json.loads(eval2.manifest_path(out).read_text())
        self.assertEqual(m["run"], r["run"])
        self.assertEqual((m["concurrency"], m["host_class"], m["max_tokens"], m["config"]), (1, "test box", 999, "cfg"))
        self.assertEqual(m["index_sha256"], config.load()["index"]["sha256"])
        self.assertEqual(m["items_sha256"], eval2.sha256_file(f))
        self.assertIsNotNone(m["ended"])
        self.assertEqual(len([l for l in text.splitlines() if l.startswith("[answer] ") and "id=" in l]), 2)

    def test_resume_after_interruption(self):
        f, out = self.items_file([item(i) for i in range(1, 5)]), self.tmp / "a.jsonl"
        self.run_answer(f, out)
        lines = out.read_text().splitlines()
        out.write_text("\n".join(lines[:2]) + "\n")  # as if killed after two items
        g = Gen()
        rc, text = self.run_answer(f, out, gen=g)
        self.assertEqual(rc, 0)
        self.assertEqual([r["id"] for r in eval2.read_jsonl(out)], ["i001", "i002", "i003", "i004"])
        self.assertEqual(len(g.calls), 4)  # two items x (expansion + answer)
        self.assertIn("2 already done", text)

    def test_manifest_mismatch_refused(self):
        f, out = self.items_file([item(1), item(2)]), self.tmp / "a.jsonl"
        self.run_answer(f, out, "--limit", "1")
        with self.assertRaisesRegex(eval2.EvalError, "manifest differs"):
            self.run_answer(f, out, "--limit", "1", "--max-tokens", "5")
        with self.assertRaisesRegex(eval2.EvalError, "manifest differs"):
            self.run_answer(f, out, "--limit", "1", "--mode", "strict")
        self.assertEqual(len(eval2.read_jsonl(out)), 1)

    def test_changed_items_file_refused(self):
        f, out = self.items_file([item(1)]), self.tmp / "a.jsonl"
        self.run_answer(f, out)
        self.items_file([item(1, question="changed?")])
        with self.assertRaisesRegex(eval2.EvalError, "items_sha256"):
            self.run_answer(f, out)

    def test_failed_item_recorded_and_run_continues(self):
        f, out = self.items_file([item(1), item(2, question="BAD one"), item(3)]), self.tmp / "a.jsonl"
        rc, text = self.run_answer(f, out, gen=Gen(fail_on="BAD"))
        rows = eval2.read_jsonl(out)
        self.assertEqual(rc, 0)
        self.assertEqual([bool(r["error"]) for r in rows], [False, True, False])
        self.assertNotIn("\n", rows[1]["error"])
        self.assertIsNone(rows[1]["text"])
        self.assertIn("FAILED", text)
        # a resume retries the failed item only
        g = Gen()
        self.run_answer(f, out, gen=g)
        self.assertEqual([bool(r["error"]) for r in eval2.read_jsonl(out)], [False, False, False])
        self.assertEqual(len(g.calls), 2)

    def test_stops_after_consecutive_errors(self):
        f = self.items_file([item(i, question="BAD") for i in range(1, 5)])
        rc, text = self.run_answer(f, self.tmp / "a.jsonl", "--max-consecutive-errors", "2", gen=Gen(fail_on="BAD"))
        self.assertEqual(rc, 1)
        self.assertEqual(len(eval2.read_jsonl(self.tmp / "a.jsonl")), 2)

    def test_evidence_appended(self):
        it = item(1)
        it["evidence"] = "pasted log"
        g = Gen()
        self.run_answer(self.items_file([it]), self.tmp / "a.jsonl", gen=g)
        self.assertTrue(g.calls[0][0][1]["content"].endswith("question number 1?\n\nEvidence:\npasted log"))

    def test_catalogue_hash_in_manifest(self):
        cat = self.tmp / "cat"
        cat.mkdir()
        (cat / "b.json").write_text("{}")
        (cat / "a.json").write_text("[]")
        f, out = self.items_file([item(1)]), self.tmp / "a.jsonl"
        self.run_answer(f, out, "--catalogue", str(cat))
        self.assertEqual(json.loads(eval2.manifest_path(out).read_text())["catalogue_sha256"], eval2.catalogue_hash(cat))


class TestReplay(Base):
    def test_replay_gives_stored_pack(self):
        f = self.items_file([item(1), item(3)])
        first = self.tmp / "first.jsonl"
        self.run_answer(f, first)
        # the second run would retrieve something else; the stored pack must win
        with mock.patch("kafka_sre_rag.retrieval.select", return_value=[CHUNKS["c3"]]):
            g, out = Gen(), self.tmp / "second.jsonl"
            self.run_answer(f, out, "--replay-packs", str(first), gen=g)
        rows, src = eval2.read_jsonl(out), eval2.read_jsonl(first)
        self.assertEqual([r["pack"] for r in rows], [r["pack"] for r in src])
        self.assertEqual(len(g.calls), 2)  # answer calls only, no expansion
        self.assertEqual(g.calls[1][0], prompts.answer_messages("question number 3?", [CHUNKS["c3"], CHUNKS["c1"]]))
        self.assertTrue(all(r["replayed_from"] == src[0]["run"] and r["search_terms"] is None for r in rows))

    def test_replay_refused_without_stored_pack(self):
        f, first = self.items_file([item(1), item(2)]), self.tmp / "first.jsonl"
        self.run_answer(f, first, "--limit", "1")
        g = Gen()
        with self.assertRaisesRegex(eval2.EvalError, "no stored pack"):
            self.run_answer(f, self.tmp / "second.jsonl", "--replay-packs", str(first), gen=g)
        self.assertEqual(g.calls, [])


class TestSplit(Base):
    def make(self):
        items, n = [], 0
        for tier, kinds in (("A", ["x", "y"]), ("B", ["z"])):
            for kind in kinds:
                for fam in range(10):
                    for _ in range(1 + fam % 3):
                        n += 1
                        items.append(item(n, tier, kind, f"{tier}{kind}{fam}"))
        return items

    def test_deterministic_families_together_share(self):
        items = self.make()
        o1, h1 = eval2.split_families(items, 7)
        o2, h2 = eval2.split_families(items, 7)
        self.assertEqual((o1, h1), (o2, h2))
        self.assertNotEqual([i["id"] for i in h1], [i["id"] for i in eval2.split_families(items, 8)[1]])
        self.assertFalse({i["family"] for i in o1} & {i["family"] for i in h1})
        self.assertEqual(len(o1) + len(h1), len(items))
        tot, hid = eval2.counts_by_cell(items), eval2.counts_by_cell(h1)
        for cell, n in tot.items():
            self.assertLessEqual(abs(hid.get(cell, 0) - n / 2), 1.5, cell)  # largest family is 3 items

    def test_exposed_item_and_its_whole_family_go_open(self):
        items = self.make()
        fams = sorted({i["family"] for i in items})
        exposed_fams = set(fams[::3])
        marked = {}
        for it in items:  # one item per family is marked; the rest of the family is not
            if it["family"] in exposed_fams and it["family"] not in marked:
                it["exposed"] = True
                marked[it["family"]] = it["id"]
        for seed in range(1, 12):
            o, h = eval2.split_families(items, seed)
            self.assertFalse(exposed_fams & {i["family"] for i in h}, seed)
            self.assertTrue(exposed_fams <= {i["family"] for i in o}, seed)
            self.assertFalse({i["family"] for i in o} & {i["family"] for i in h})
            self.assertEqual(len(o) + len(h), len(items))
            tot, hid = eval2.counts_by_cell(items), eval2.counts_by_cell(h)
            for cell, n in tot.items():  # the share is reached with the remaining families
                self.assertLessEqual(abs(hid.get(cell, 0) - n / 2), 3, (seed, cell))
        self.assertEqual(eval2.split_families(items, 4), eval2.split_families(items, 4))

    def test_exposed_false_does_not_force_open(self):
        items = self.make()
        for it in items:
            it["exposed"] = False
        ids = lambda pair: [[i["id"] for i in side] for side in pair]
        self.assertEqual(ids(eval2.split_families(items, 5)), ids(eval2.split_families(self.make(), 5)))

    @NEEDS_OPENSSL
    def test_cli_split_writes_open_clear_hidden_encrypted(self):
        src = self.items_file(self.make())
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = eval2.main(["split", "--items", str(src), "--seed", "3", "--open-out", str(self.tmp / "o.jsonl"),
                             "--hidden-out", str(self.tmp / "h.jsonl.enc")])
        self.assertEqual(rc, 0)
        self.assertNotIn("question number", buf.getvalue())
        self.assertIn(b"question number", (self.tmp / "o.jsonl").read_bytes())
        self.assertNotIn(b"question number", (self.tmp / "h.jsonl.enc").read_bytes())
        self.assertEqual(len(eval2.read_items(self.tmp / "h.jsonl.enc")) + len(eval2.read_items(self.tmp / "o.jsonl")),
                         len(self.make()))


@NEEDS_OPENSSL
class TestHidden(Base):
    def test_round_trip_and_integrity(self):
        blob = eval2.encrypt_bytes(b"secret text")
        self.assertNotIn(b"secret", blob)
        self.assertEqual(eval2.decrypt_bytes(blob), b"secret text")
        self.assertNotEqual(blob, eval2.encrypt_bytes(b"secret text"))  # fresh salt
        with self.assertRaisesRegex(eval2.EvalError, "integrity"):
            eval2.decrypt_bytes(blob[:-1] + bytes([blob[-1] ^ 1]))

    def test_keygen_mode_and_no_overwrite(self):
        k = self.tmp / "k2"
        eval2.keygen(k)
        self.assertEqual(stat.S_IMODE(k.stat().st_mode), 0o600)
        with self.assertRaisesRegex(eval2.EvalError, "refusing"):
            eval2.keygen(k)

    def test_loose_key_refused(self):
        os.chmod(self.key, 0o644)
        with self.assertRaisesRegex(eval2.EvalError, "chmod 600"):
            eval2.encrypt_bytes(b"x")

    def test_hidden_run_prints_and_writes_no_content(self):
        marker = "ZEBRA-QUESTION-MARKER"
        items = [item(1, question=f"{marker} one?"), item(2, question=f"{marker} BAD two?"), item(3, question=f"{marker} three?")]
        f, out = self.items_file(items, "h.jsonl.enc"), self.tmp / "ans.jsonl.enc"
        err, buf = io.StringIO(), io.StringIO()
        with contextlib.redirect_stderr(err), contextlib.redirect_stdout(buf):
            rc, text = self.run_answer(f, out, gen=Gen(fail_on="BAD"))
        shown = text + buf.getvalue() + err.getvalue()
        for leak in (marker, "answer text", "boom", "terms one"):
            self.assertNotIn(leak, shown)
        self.assertIn("id=i001", shown)
        self.assertIn("FAILED", shown)
        for p in (out, eval2.manifest_path(out)):
            self.assertTrue(p.exists())
            self.assertNotIn(b"answer text", p.read_bytes())
            self.assertNotIn(b"terms one", p.read_bytes())
        self.assertEqual(sorted(p.name for p in self.tmp.iterdir() if "ans" in p.name),
                         ["ans.jsonl.enc", "ans.jsonl.enc.manifest.json.enc"])
        rows = eval2.read_jsonl(out)
        self.assertEqual([r["text"] for r in rows], ["answer text", None, "answer text"])
        self.assertIn("boom", rows[1]["error"])  # kept, but only inside the encrypted file

    def test_hidden_items_need_enc_output(self):
        f = self.items_file([item(1)], "h.jsonl.enc")
        with self.assertRaisesRegex(eval2.EvalError, r"\.enc"):
            self.run_answer(f, self.tmp / "clear.jsonl")

    def test_hidden_resume(self):
        f, out = self.items_file([item(1), item(2)], "h.jsonl.enc"), self.tmp / "ans.jsonl.enc"
        self.run_answer(f, out, "--limit", "1")
        with self.assertRaisesRegex(eval2.EvalError, "manifest differs"):
            self.run_answer(f, out)
        self.assertEqual(len(eval2.read_jsonl(out)), 1)

    def test_hidden_report_totals_only(self):
        items = [item(1, question="ZEBRA one?"), item(2, question="ZEBRA two?")]
        f, out = self.items_file(items, "h.jsonl.enc"), self.tmp / "ans.jsonl.enc"
        self.run_answer(f, out)
        eval2.write_jsonl(eval2.score_path(out), [{"id": "i001", "checks_passed": 2, "checks_total": 3, "all_pass": False},
                                                   {"id": "i002", "checks_passed": 3, "checks_total": 3, "all_pass": True}])
        buf = io.StringIO()
        eval2.cmd_report(eval2.build_parser().parse_args(["report", str(out)]), buf)
        text = buf.getvalue()
        self.assertIn("totals only", text)
        self.assertIn("all-pass 1/2", text)
        self.assertNotIn("i001", text)
        self.assertNotIn("i002", text)
        self.assertNotIn("ZEBRA", text)

    def test_hidden_report_outcome_totals_without_ids(self):
        items = [item(1, question="ZEBRA one?"), item(2, question="ZEBRA two?")]
        f, out = self.items_file(items, "h.jsonl.enc"), self.tmp / "ans.jsonl.enc"
        self.run_answer(f, out)
        eval2.write_jsonl(eval2.score_path(out), [
            {"id": "i001", "checks_passed": 2, "checks_total": 3, "all_pass": False, "outcome": "correct_decline",
             "coverage": {"not_in_index": True}},
            {"id": "i002", "checks_passed": 3, "checks_total": 3, "all_pass": True, "outcome": "pass", "coverage": {"not_in_index": False}}])
        buf = io.StringIO()
        eval2.cmd_report(eval2.build_parser().parse_args(["report", str(out)]), buf)
        text = buf.getvalue()
        self.assertIn("all-pass 1/2", text)
        self.assertIn("correct_decline 1/2 = 50.0 %", text)
        self.assertIn("items not in the index 1", text)
        for bad in ("i001", "i002", "ZEBRA"):
            self.assertNotIn(bad, text)


class TestReport(Base):
    def test_open_report_with_scores_errors_timing(self):
        f, out = self.items_file([item(1), item(2, question="BAD"), item(3)]), self.tmp / "a.jsonl"
        self.run_answer(f, out, gen=Gen(fail_on="BAD"))
        rows = eval2.read_jsonl(out)
        for r, t, tk in zip(rows, (1.0, 0.0, 3.0), (10, None, 30)):
            r["seconds"]["total"], r["tokens"] = t, tk
        eval2.write_jsonl(out, rows)
        eval2.write_jsonl(eval2.score_path(out), [{"id": "i001", "score": 0.5}, {"id": "i003", "score": 1.0}])
        buf = io.StringIO()
        eval2.cmd_report(eval2.build_parser().parse_args(["report", str(out)]), buf)
        text = buf.getvalue()
        self.assertIn("errors 1", text)
        self.assertIn("failed ids: i002", text)
        self.assertIn("mean score 0.750", text)
        self.assertIn("total seconds   n=2 median=2.00 min=1.00 max=3.00 p90=2.80", text)
        self.assertIn("answer tokens   n=2 median=20.00", text)

    def report_text(self, out):
        buf = io.StringIO()
        eval2.cmd_report(eval2.build_parser().parse_args(["report", str(out)]), buf)
        return buf.getvalue()

    def test_free_text_answers_get_the_identifier_totals(self):
        import eval2_idcheck
        out = self.tmp / "ft.jsonl"
        self.run_answer(self.items_file([item(1, tier="B"), item(2, tier="B")]), out)
        with mock.patch.object(eval2_idcheck, "report_block", return_value="IDCHECK BLOCK") as rb:
            text = self.report_text(out)
        self.assertIn("IDCHECK BLOCK", text)
        rows, hidden = rb.call_args[0]
        self.assertEqual((len(rows), hidden), (2, False))

    def test_mechanical_answers_keep_their_report_unchanged(self):
        import eval2_idcheck
        out = self.tmp / "mech.jsonl"
        self.run_answer(self.items_file([item(1), item(2), item(3, tier="B")]), out)
        eval2.write_jsonl(eval2.score_path(out), [{"id": "i001", "all_pass": True, "checks_passed": 1, "checks_total": 1},
                                                  {"id": "i002", "all_pass": False, "checks_passed": 0, "checks_total": 1}])
        with mock.patch.object(eval2_idcheck, "report_block", return_value="IDCHECK BLOCK") as rb:
            text = self.report_text(out)
        self.assertEqual([r["id"] for r in rb.call_args[0][0]], ["i003"])   # only the row without a tier A score
        self.assertIn("tier A scored 2  all-pass 1/2", text)
        eval2.write_jsonl(eval2.score_path(out), [{"id": f"i00{n}", "all_pass": True, "checks_passed": 1, "checks_total": 1} for n in (1, 2, 3)])
        with mock.patch.object(eval2_idcheck, "report_block", return_value="IDCHECK BLOCK") as rb:
            text = self.report_text(out)
        rb.assert_not_called()
        self.assertNotIn("IDCHECK", text)

    @NEEDS_OPENSSL
    def test_hidden_file_gets_totals_only(self):
        import eval2_idcheck
        out = self.tmp / "ft.jsonl.enc"
        self.run_answer(self.items_file([item(1, tier="B")], "items.jsonl.enc"), out)
        with mock.patch.object(eval2_idcheck, "report_block", return_value="TOTALS") as rb:
            self.report_text(out)
        self.assertTrue(rb.call_args[0][1])
        # the real function on a hidden run: totals text, no identifier, no id
        text = eval2_idcheck.report_block([{"id": "secret-id-7", "text": "run kafka-nosuchtool.sh --x"}], True, str(ROOT / "work/m4/catalogues")) \
            if (ROOT / "work/m4/catalogues/CATALOGUES.json").exists() else "(hidden: totals only)"
        self.assertIn("(hidden: totals only)", text)
        self.assertNotIn("secret-id-7", text)
        self.assertNotIn("nosuchtool", text)

    def test_missing_catalogues_do_not_fail_the_report(self):
        import eval2_idcheck
        out = self.tmp / "ft.jsonl"
        self.run_answer(self.items_file([item(1, tier="B")]), out)
        with mock.patch.object(eval2_idcheck, "report_block", side_effect=FileNotFoundError("x")):
            self.assertIn("identifier check not run", self.report_text(out))

    def test_percentile(self):
        self.assertEqual(eval2.pct([1, 2, 3, 4], 50), 2.5)
        self.assertIsNone(eval2.pct([], 50))


class TestSmokeAndLegacy(Base):
    def items(self):
        out = []
        for i in range(100):
            out.append(item(i, "A", "ka" if i % 4 else "kb"))
        for i in range(100, 140):
            out.append(item(i, "B", "kc" if i % 2 else "kd"))
        return out

    def test_smoke_stratified_stable_and_protected(self):
        items, sm = self.items(), self.tmp / "m4" / "smoke_ids.json"
        s1 = eval2.smoke_subset(items, sm, 20, 8, 1)
        self.assertEqual((sum(i["tier"] == "A" for i in s1), sum(i["tier"] == "B" for i in s1)), (20, 8))
        kinds = {k: sum(i["kind"] == k for i in s1) for k in ("ka", "kb", "kc", "kd")}
        self.assertEqual(kinds, {"ka": 15, "kb": 5, "kc": 4, "kd": 4})
        text = sm.read_text()
        self.assertEqual([i["id"] for i in eval2.smoke_subset(list(reversed(items)), sm, 20, 8, 1)],
                         [i["id"] for i in reversed(s1)])
        self.assertEqual(sm.read_text(), text)  # not rewritten
        self.assertEqual(eval2.build_smoke(items, 20, 8, 1)["ids"], eval2.build_smoke(items, 20, 8, 1)["ids"])
        with self.assertRaisesRegex(eval2.EvalError, "rebuild-smoke"):
            eval2.smoke_subset(items, sm, 30, 8, 1)
        eval2.smoke_subset(items, sm, 30, 8, 1, rebuild=True)
        self.assertEqual(json.loads(sm.read_text())["smoke_a"], 30)

    def test_smoke_in_answer_run(self):
        f, out = self.items_file(self.items()), self.tmp / "a.jsonl"
        self.run_answer(f, out, "--subset", "smoke", "--smoke-a", "6", "--smoke-b", "4",
                        "--smoke-file", str(self.tmp / "s.json"))
        self.assertEqual(len(eval2.read_jsonl(out)), 10)

    def test_convert_legacy(self):
        src = self.tmp / "q.jsonl"
        src.write_text('{"category": "kafka-ops", "question": "Q1?", "reference": "R"}\n{"category": "x", "question": "Q2?"}\n')
        items = eval2.convert_legacy(src, "real")
        self.assertEqual([i["id"] for i in items], ["real-001", "real-002"])
        self.assertEqual((items[0]["tier"], items[0]["question"], items[0]["reference"]), ("legacy", "Q1?", "R"))

    def test_validate_rejects_duplicates_and_missing(self):
        with self.assertRaisesRegex(eval2.EvalError, "duplicate"):
            eval2.validate_items([item(1), item(1)])
        bad = item(1)
        del bad["family"]
        with self.assertRaisesRegex(eval2.EvalError, "family"):
            eval2.validate_items([bad])


if __name__ == "__main__":
    unittest.main()
