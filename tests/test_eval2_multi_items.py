"""WP1c T10: several --items files in the harness (answer, report, scorer command line), and the a3 open file beside
tier A in retrieval_needs.py and retrieval_funnel.py. Stub backend, synthetic items, no model, no real index."""
import contextlib
import io
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
import eval2_score  # noqa: E402
import retrieval_funnel as RF  # noqa: E402
import retrieval_needs as RN  # noqa: E402
from test_eval2 import Base, Gen, item  # noqa: E402

TAG = "shares_variant_with_published"


def a3(i, kind="client_code", **extra):
    return {**item(i, kind=kind, family=f"g{i:03d}"), "id": f"x{i:03d}", **extra}


class MultiAnswer(Base):
    def two(self, name_a="a.jsonl", name_b="b.jsonl"):
        fa = self.items_file([item(1), item(2)], name_a)
        fb = self.items_file([a3(1), a3(2)], name_b)
        return fa, fb

    def run_multi(self, files, out, *extra, gen=None):
        buf = io.StringIO()
        a = eval2.build_parser().parse_args(
            ["answer", "--items", *map(str, files), "--out", str(out), "--config", "cfg", "--backend", "stub",
             "--host-class", "test box", *extra])
        with mock.patch.object(eval2, "make_generator", return_value=gen or Gen()):
            rc = eval2.cmd_answer(a, out=buf)
        return rc, buf.getvalue()

    def test_two_open_files_in_order_with_every_sha(self):
        fa, fb = self.two()
        out = self.tmp / "ans.jsonl"
        rc, text = self.run_multi([fa, fb], out)
        self.assertEqual(rc, 0)
        self.assertEqual([r["id"] for r in eval2.read_jsonl(out)], ["i001", "i002", "x001", "x002"])
        m = eval2.read_manifest(out)
        self.assertEqual(m["items_count"], 4)
        self.assertEqual(m["items_files"], [
            {"name": "a.jsonl", "sha256": eval2.sha256_file(fa), "count": 2},
            {"name": "b.jsonl", "sha256": eval2.sha256_file(fb), "count": 2}])
        self.assertEqual(m["items_sha256"], eval2.items_digest([fa, fb]))
        self.assertNotEqual(m["items_sha256"], eval2.sha256_file(fa))
        self.assertIn("4 items", text)

    def test_order_matters_for_resume(self):
        fa, fb = self.two()
        out = self.tmp / "ans.jsonl"
        self.run_multi([fa, fb], out)
        with self.assertRaises(eval2.EvalError):
            self.run_multi([fb, fa], out)

    def test_one_file_manifest_has_no_new_field(self):
        fa, _ = self.two()
        out = self.tmp / "ans.jsonl"
        self.run_answer(fa, out)
        m = eval2.read_manifest(out)
        self.assertNotIn("items_files", m)
        self.assertEqual(m["items_sha256"], eval2.sha256_file(fa))
        # the same run given as a one-element list is the same run
        out2 = self.tmp / "ans2.jsonl"
        self.run_multi([fa], out2)
        self.assertEqual(eval2.comparable(eval2.read_manifest(out2)), eval2.comparable(m))
        self.assertEqual(eval2.read_manifest(out2)["run"], m["run"])

    def test_duplicate_id_across_files_refused(self):
        fa = self.items_file([item(1)], "a.jsonl")
        fb = self.items_file([item(1)], "b.jsonl")
        with self.assertRaises(eval2.EvalError):
            self.run_multi([fa, fb], self.tmp / "ans.jsonl")

    def test_open_and_hidden_never_mixed(self):
        fa = self.items_file([item(1)], "a.jsonl")
        fh = self.items_file([a3(1)], "h.jsonl.enc")
        for out in ("ans.jsonl", "ans.jsonl.enc"):
            with self.assertRaises(eval2.EvalError) as cm:
                self.run_multi([fa, fh], self.tmp / out)
            self.assertIn("never mixed", str(cm.exception))

    def test_hidden_run_over_two_files_writes_only_enc_and_logs_ids(self):
        marker = "ZEBRA-QUESTION-MARKER"
        fa = self.items_file([item(1, question=f"{marker} one?"), item(2, question=f"{marker} two?")], "ha.jsonl.enc")
        fb = self.items_file([a3(1, question=f"{marker} three?")], "hb.jsonl.enc")
        out = self.tmp / "ans.jsonl.enc"
        err, buf = io.StringIO(), io.StringIO()
        with contextlib.redirect_stderr(err), contextlib.redirect_stdout(buf):
            rc, text = self.run_multi([fa, fb], out)
        shown = text + buf.getvalue() + err.getvalue()
        self.assertEqual(rc, 0)
        for leak in (marker, "answer text", "terms one"):
            self.assertNotIn(leak, shown)
        self.assertIn("id=x001", shown)
        for p in (out, eval2.manifest_path(out)):
            self.assertNotIn(b"answer text", p.read_bytes())
            self.assertNotIn(marker.encode(), p.read_bytes())
        self.assertEqual(sorted(p.name for p in self.tmp.iterdir() if p.name.startswith("ans")),
                         ["ans.jsonl.enc", "ans.jsonl.enc.manifest.json.enc"])
        self.assertTrue(eval2.read_manifest(out)["hidden"])
        self.assertEqual(len(eval2.read_jsonl(out)), 3)

    def test_hidden_items_need_enc_output_with_several_files(self):
        fa = self.items_file([item(1)], "ha.jsonl.enc")
        fb = self.items_file([a3(1)], "hb.jsonl.enc")
        with self.assertRaises(eval2.EvalError):
            self.run_multi([fa, fb], self.tmp / "ans.jsonl")


class MultiReport(Base):
    def setUp(self):
        super().setUp()
        self.rows_a = [item(1, kind="k1"), item(2, kind="k2")]
        self.rows_b = [a3(1, kind="client_code"), a3(2, kind="client_code", **{TAG: True}),
                       a3(3, kind="not_covered", **{TAG: True}), a3(4, kind="not_covered")]
        self.scores = {"i001": True, "i002": False, "x001": True, "x002": False, "x003": True, "x004": True}

    def report(self, hidden):
        sfx = ".jsonl.enc" if hidden else ".jsonl"
        fa, fb = self.items_file(self.rows_a, "a" + sfx), self.items_file(self.rows_b, "b" + sfx)
        out = self.tmp / ("ans" + sfx)
        self.run_answer(fa, out)
        # the score rows alone drive the report
        eval2.write_jsonl(eval2.score_path(out), [
            {"id": i, "checks_passed": int(v), "checks_total": 1, "all_pass": v} for i, v in self.scores.items()])
        buf = io.StringIO()
        eval2.cmd_report(eval2.build_parser().parse_args(["report", str(out), "--items", str(fa), str(fb)]), buf)
        return buf.getvalue()

    def test_open_totals_per_file_kind_and_tag(self):
        t = self.report(False)
        self.assertIn("items file a.jsonl: 2 items, scored 2, all-pass 1/2", t)
        self.assertIn("items file b.jsonl: 4 items, scored 4, all-pass 3/4", t)
        self.assertIn("  kind client_code: scored 2, all-pass 1/2", t)
        self.assertIn("  kind not_covered: scored 2, all-pass 2/2", t)
        self.assertIn(f"total with the 2 items tagged {TAG}: all-pass 3/4; without them: all-pass 2/2", t)
        self.assertEqual(t.count("tagged " + TAG), 1)             # the untagged file prints no such line

    def test_hidden_totals_without_ids_or_text(self):
        t = self.report(True)
        self.assertIn("totals only", t)
        self.assertIn("items file b.jsonl.enc: 4 items, scored 4, all-pass 3/4", t)
        self.assertIn(f"total with the 2 items tagged {TAG}: all-pass 3/4; without them: all-pass 2/2", t)
        for bad in ("i001", "x001", "question number"):
            self.assertNotIn(bad, t)

    def test_report_without_items_is_unchanged(self):
        fa = self.items_file(self.rows_a, "a.jsonl")
        out = self.tmp / "ans.jsonl"
        self.run_answer(fa, out)
        eval2.write_jsonl(eval2.score_path(out), [{"id": "i001", "checks_passed": 1, "checks_total": 1, "all_pass": True}])
        buf = io.StringIO()
        eval2.cmd_report(eval2.build_parser().parse_args(["report", str(out)]), buf)
        self.assertNotIn("items file", buf.getvalue())

    def test_items_must_match_hiddenness_of_answers(self):
        fa = self.items_file(self.rows_a, "a.jsonl")
        out = self.tmp / "ans.jsonl.enc"
        self.run_answer(self.items_file(self.rows_a, "h.jsonl.enc"), out)
        eval2.write_jsonl(eval2.score_path(out), [{"id": "i001", "checks_passed": 1, "checks_total": 1, "all_pass": True}])
        with self.assertRaises(eval2.EvalError):
            eval2.cmd_report(eval2.build_parser().parse_args(["report", str(out), "--items", str(fa)]), io.StringIO())


class ScorerCli(unittest.TestCase):
    def test_several_items_files_are_parsed_and_enc_or_duplicates_refused(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            (d / "a.jsonl").write_text(json.dumps({"id": "p1", "checks": []}) + "\n")
            (d / "b.jsonl").write_text(json.dumps({"id": "p1", "checks": []}) + "\n")
            (d / "h.jsonl.enc").write_bytes(b"x")
            (d / "ans.jsonl").write_text("")
            for files in (("a.jsonl", "b.jsonl"), ("a.jsonl", "h.jsonl.enc")):
                with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as cm:
                    eval2_score.main(["--items", *[str(d / f) for f in files], "--answers", str(d / "ans.jsonl"),
                                      "--no-outcomes"])
                self.assertEqual(cm.exception.code, 2)

    def test_two_files_are_joined(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            (d / "a.jsonl").write_text(json.dumps({"id": "p1", "checks": [], "kind": "k"}) + "\n")
            (d / "b.jsonl").write_text(json.dumps({"id": "p2", "checks": [], "kind": "k"}) + "\n")
            (d / "ans.jsonl").write_text("")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                eval2_score.main(["--items", str(d / "a.jsonl"), str(d / "b.jsonl"), "--answers", str(d / "ans.jsonl"),
                                  "--no-outcomes"])
            self.assertEqual(json.loads(out.getvalue())["items_scored"], 2)


class NeedsAndFunnel(unittest.TestCase):
    def test_needs_build_reports_tagged_items_apart(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            idx = d / "idx.jsonl"
            idx.write_text(json.dumps({"id": "c1", "path": "raw/k/config.html", "title": "a.b.c", "text": "t",
                                       "source": "s"}) + "\n")
            tier_a = d / "a.jsonl"
            tier_a.write_text(json.dumps({"id": "a1", "kind": "kafka_config", "sources": [
                {"catalogue": "kafka_config", "entry": "broker/a.b.c", "file": "raw/k/config.html"}]}) + "\n")
            a3f = d / "a3.jsonl"
            rows = [{"id": "x1", "kind": "client_code", "not_in_index": True, "sources": [{"catalogue": "pinned_text"}]},
                    {"id": "x2", "kind": "client_code", "not_in_index": False, "sources": [{"catalogue": "pinned_text",
                                                                                           "entry": "e", "file": "raw/z.md"}]},
                    {"id": "x3", "kind": "not_covered", "sources": [{"absent": True}]}]
            a3f.write_text("".join(json.dumps(r) + "\n" for r in rows))
            with mock.patch.object(RN, "check_index_hash", return_value="h"):
                base_rows, base_info = RN.build(str(idx), None, [str(tier_a)])
                one_rows, _ = RN.build(str(idx), None, [str(tier_a)])
                both_rows, info = RN.build(str(idx), None, [str(tier_a), str(a3f)])
            facts = [r for r in both_rows if r["row_type"] == "fact"]
            self.assertEqual([(r["item"], r["status"]) for r in facts], [("a1", "mapped"), ("x1", "not_in_index")])
            self.assertEqual(facts[1]["tags"], ["not_in_index"])
            self.assertEqual(facts[1]["chunks"], [])
            self.assertEqual(info["facts"], 1)
            self.assertEqual(info["not_in_index_tagged"], 1)
            self.assertNotIn("not_in_index_tagged", base_info)
            # tier A alone gives the same rows as before: no row of the tagged kind, no new key
            self.assertEqual([r for r in base_rows if r["row_type"] == "fact"],
                             [r for r in both_rows if r["row_type"] == "fact" and r["item"] == "a1"])
            self.assertEqual(one_rows[0], base_rows[0])

    def test_kafka_config_entry_without_scope_is_unresolved_not_a_crash(self):
        it = {"id": "x", "sources": [{"catalogue": "kafka_config", "entry": "no.scope.key", "file": "raw/k/c.html"}]}
        self.assertEqual(RN.fact_chunks(it, []), (None, "unresolved"))

    def test_funnel_totals_put_tagged_facts_apart(self):
        needs = [{"row_type": "fact", "item": "i1", "chunks": ["c03"], "tags": ["C1"]},
                 {"row_type": "fact", "item": "i2", "chunks": [], "tags": ["not_in_index"], "status": "not_in_index"}]
        stages = {"frozen": {("fact", "i1", None): "in_pack", ("fact", "i2", None): "not_in_index"}}
        tot = RF.totals(needs, stages, "frozen")["frozen"]
        self.assertEqual((tot["facts"]["of"], tot["facts"]["served"]), (1, 1))
        self.assertEqual((tot["facts_not_in_index"]["of"], tot["facts_not_in_index"]["served"]), (1, 0))
        self.assertEqual(tot["not_in_index_fact_stages"], {"not_in_index": 1})
        self.assertEqual(tot["fact_stages"], {"in_pack": 1})
        self.assertIn("apart", RF.table({"frozen": tot}))

    def test_funnel_totals_without_tagged_facts_are_unchanged(self):
        needs = [{"row_type": "fact", "item": "i1", "chunks": ["c03"], "tags": ["C1"]}]
        stages = {"frozen": {("fact", "i1", None): "in_pack"}}
        tot = RF.totals(needs, stages, "frozen")["frozen"]
        self.assertNotIn("facts_not_in_index", tot)
        self.assertNotIn("not_in_index_fact_stages", tot)
        self.assertNotIn("apart", RF.table({"frozen": tot}))

    def test_funnel_loads_several_items_files(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            (d / "needs.jsonl").write_text("")
            (d / "ans.jsonl").write_text("")
            (d / "a.jsonl").write_text(json.dumps({"id": "i1", "question": "q1"}) + "\n")
            (d / "b.jsonl").write_text(json.dumps({"id": "x1", "question": "q2"}) + "\n")
            _, _, items = RF.load_inputs(str(d / "needs.jsonl"), [str(d / "ans.jsonl")], [str(d / "a.jsonl"), str(d / "b.jsonl")])
        self.assertEqual(sorted(items), ["i1", "x1"])


if __name__ == "__main__":
    unittest.main()
