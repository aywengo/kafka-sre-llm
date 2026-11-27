"""M7 KB phase 2, I4: the entries index (scripts/kb_build_index.py), the search and merge (kafka_sre_rag/kb_entries.py) and the
runtime option --kb-entries (EXPERIMENTAL, off by default). Offline: synthetic fixtures (tests/fixtures/kb_i4_i5), a stand-in
reranker, a stub generator; no model, no server, the real reranker is never loaded."""
import contextlib
import copy
import hashlib
import io
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
sys.path.insert(0, str(ROOT / "tests"))
sys.path.insert(0, str(ROOT / "tests" / "fixtures" / "kb_i4_i5"))
import fixtures as F  # noqa: E402
import kb_build_index  # noqa: E402
from kafka_sre_rag import __main__ as cli, config, evalhooks, kb_entries as KE, kb_entry as K, pipeline, prompts, service  # noqa: E402
from test_eval2 import Gen  # noqa: E402

Q = "How often does the Widget operator reconcile a Widget?"


class PosReranker:
    """Scores by the position in the candidate list, reversed: the last candidate is best. Counts its calls."""

    def __init__(self):
        self.calls = []

    def predict(self, pairs, batch_size=8):
        self.calls.append(list(pairs))
        return [float(i) for i in range(len(pairs))]


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.P = F.write_all(self.tmp / "fx")
        self.record = self.tmp / "kb_entries.json"
        rec = kb_build_index.build(self.P["entries_index.jsonl"], self.P["slice.jsonl"], self.P["MANIFEST.json"], file_name="work/x/entries_index.jsonl",
                                   created="2026-11-26T00:00:00Z")
        self.record.write_text(json.dumps(rec))
        self.rec = rec
        # the real frozen reranker must never be loaded by these tests
        p = mock.patch("retrieve.reranker", side_effect=AssertionError("the real reranker was loaded"))
        p.start()
        self.addCleanup(p.stop)

    def load(self, **kw):
        return KE.KbEntries.load(self.record, index_path=self.P["entries_index.jsonl"], reranker=kw.pop("reranker", PosReranker()), **kw)

    def frozen(self, ids=("c05", "c06", "c13", "c10", "c04", "c02")):
        return [dict(F.CH[i], bm25_rank=n + 1, ce_score=1.0) for n, i in enumerate(ids)]


class BuildIndex(Base):
    def test_record_fields(self):
        r = self.rec
        self.assertEqual(r["schema"], 1)
        self.assertEqual(r["entries"], len(F.entry_rows()))
        self.assertEqual(r["sha256"], hashlib.sha256(self.P["entries_index.jsonl"].read_bytes()).hexdigest())
        self.assertEqual(r["slice_sha256"], hashlib.sha256(self.P["slice.jsonl"].read_bytes()).hexdigest())
        self.assertEqual(r["writer"], {"model": F.WRITER, "prompt_version": F.PROMPT_VERSION})
        self.assertEqual(r["checker"], {"model": F.CHECKER, "prompt_version": F.CHECK_VERSION})
        self.assertEqual(r["file"], "work/x/entries_index.jsonl")
        self.assertEqual(set(r), set(KE.RECORD_FIELDS))

    def run_main(self, *extra, manifest=None):
        out = self.tmp / "rec_out.json"
        err = io.StringIO()
        with contextlib.redirect_stderr(err), contextlib.redirect_stdout(io.StringIO()):
            rc = kb_build_index.main(["--index", str(self.P["entries_index.jsonl"]), "--slice", str(self.P["slice.jsonl"]),
                                      "--manifest", str(manifest or self.P["MANIFEST.json"]), "--record", str(out),
                                      "--file-name", "work/x/entries_index.jsonl", "--created", "2026-11-26T00:00:00Z", *extra])
        return rc, out, err.getvalue()

    def test_main_writes_and_is_idempotent_and_refuses_a_change(self):
        rc, out, _ = self.run_main()
        self.assertEqual((rc, json.loads(out.read_text())), (0, self.rec))
        self.assertEqual(self.run_main()[0], 0)
        rows = [json.loads(l) for l in self.P["entries_index.jsonl"].read_text().splitlines()]
        rows[0]["title"] += " x"
        rows[0]["rendered"] = K.render_text(rows[0])
        self.P["entries_index.jsonl"].write_text("".join(K.dumps(r) + "\n" for r in rows))
        rc, out, err = self.run_main()
        self.assertEqual(rc, 2)
        self.assertIn("differs", err)
        self.assertEqual(json.loads(out.read_text()), self.rec)     # not replaced
        self.assertEqual(self.run_main("--force")[0], 0)

    def test_refusals(self):
        rows = [json.loads(l) for l in self.P["entries_index.jsonl"].read_text().splitlines()]
        cases = {
            "unsorted": lambda r: r.reverse(),
            "wrong version": lambda r: r[0].update(prompt_version="kbe/1"),
            "wrong writer": lambda r: r[0].update(writer="another"),
            "stale render": lambda r: r[0].update(rendered=r[0]["rendered"] + "x"),
            "repeated": lambda r: r.insert(1, copy.deepcopy(r[0])),
            "no field": lambda r: r[0].pop("quotes"),
        }
        for name, mut in cases.items():
            rr = copy.deepcopy(rows)
            mut(rr)
            self.P["entries_index.jsonl"].write_text("".join(json.dumps(r, sort_keys=True) + "\n" for r in rr))
            rc, _, err = self.run_main()
            self.assertEqual(rc, 2, name)
            self.assertIn("REFUSED", err, name)
        self.P["entries_index.jsonl"].write_text("")
        self.assertEqual(self.run_main()[0], 2)

    def test_a_file_outside_the_repository_needs_a_recorded_name(self):
        with self.assertRaises(kb_build_index.Refused):
            kb_build_index.build(self.P["entries_index.jsonl"], self.P["slice.jsonl"], self.P["MANIFEST.json"])
        with self.assertRaises(kb_build_index.Refused):
            kb_build_index.build(self.P["entries_index.jsonl"], self.P["slice.jsonl"], self.P["MANIFEST.json"], file_name="/abs/x.jsonl")

    def test_a_manifest_without_the_checker_is_refused(self):
        m = json.loads(self.P["MANIFEST.json"].read_text())
        del m["options"]["checker"]
        bad = self.tmp / "m.json"
        bad.write_text(json.dumps(m))
        self.assertEqual(self.run_main(manifest=bad)[0], 2)


class HashRefusal(Base):
    def refused(self, **kw):
        with self.assertRaises(KE.KbEntriesError) as cm:
            self.load(**kw)
        return str(cm.exception)

    def test_loads_when_everything_matches(self):
        kb = self.load()
        self.assertEqual(len(kb.rows), self.rec["entries"])

    def test_index_changed_after_the_record(self):
        with open(self.P["entries_index.jsonl"], "a") as f:
            f.write("\n")
        self.assertIn("does not match the record", self.refused())

    def test_record_hash_changed(self):
        rec = dict(self.rec, sha256="0" * 64)
        self.record.write_text(json.dumps(rec))
        self.assertIn("does not match the record", self.refused())

    def test_count_differs(self):
        self.record.write_text(json.dumps(dict(self.rec, entries=self.rec["entries"] + 1)))
        self.assertIn("record says", self.refused())

    def test_record_missing_or_a_template(self):
        self.record.unlink()
        self.assertIn("no entries record", self.refused())
        self.record.write_text(json.dumps({k: None for k in KE.RECORD_FIELDS} | {"schema": 1}))
        self.assertIn("template", self.refused())
        self.record.write_text("{")
        self.assertIn("not valid JSON", self.refused())

    def test_index_file_missing(self):
        self.P["entries_index.jsonl"].unlink()
        self.assertIn("is missing", self.refused())

    def test_rendered_text_drift_is_refused_even_with_a_matching_hash(self):
        rows = [json.loads(l) for l in self.P["entries_index.jsonl"].read_text().splitlines()]
        saved = copy.deepcopy(prompts.KB_ENTRY_RENDER)
        self.addCleanup(lambda: (prompts.KB_ENTRY_RENDER.clear(), prompts.KB_ENTRY_RENDER.update(saved)))
        prompts.KB_ENTRY_RENDER["title"] = "Entry: <<TITLE>> (<<PRODUCT>> <<VERSION>>; <<PATHS>>)"
        self.assertIn("rendered text differs", self.refused())
        self.assertEqual(len(rows), self.rec["entries"])

    def test_cli_exits_2_not_a_traceback(self):
        self.record.write_text(json.dumps(dict(self.rec, sha256="0" * 64)))
        err = io.StringIO()
        with contextlib.redirect_stderr(err), mock.patch.object(KE, "_index_path", return_value=self.P["entries_index.jsonl"]):
            rc = cli.main(["ask", "--dry-run", "--no-rerank-in-dry-run", "--kb-entries", "--kb-entries-record", str(self.record), Q])
        self.assertEqual(rc, 2)
        self.assertIn("does not match the record", err.getvalue())
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            rc = cli.main(["ask", "--dry-run", "--kb-entries", "--kb-entries-record", str(self.tmp / "nope.json"), Q])
        self.assertEqual(rc, 2)
        self.assertIn("no entries record", err.getvalue())


class Merge(Base):
    def test_frozen_pack_unchanged_and_first_and_at_most_four_follow(self):
        kb = self.load()
        fz = self.frozen()
        before = copy.deepcopy(fz)
        out, info = kb.merge(fz, Q, "widget operator, reconcile")
        self.assertEqual(fz, before)                                 # the frozen pack is not touched
        self.assertEqual(out[:6], fz)                                # first, in frozen order
        self.assertLessEqual(len(out), 10)
        self.assertEqual(len(out) - 6, len(info["added"]))
        self.assertTrue(0 < len(info["added"]) <= 4)
        self.assertTrue(all(i["id"].startswith("kbe:kbe-") and i["entry"] for i in out[6:]))
        self.assertEqual([i["id"] for i in out[6:]], [KE.PACK_PREFIX + a for a in info["added"]])

    def test_the_item_is_the_rendered_entry(self):
        kb = self.load()
        out, info = kb.merge(self.frozen(), Q, "widget")
        e = kb.byid[info["added"][0]]
        self.assertEqual(prompts.pack_text([out[6]]), e["rendered"])
        self.assertTrue(out[6]["title"].startswith("Fact entry: "))

    def test_ten_items_cap(self):
        kb = self.load()
        fz = [dict(F.CHUNKS[i % 14], id=f"x{i}") for i in range(10)]
        out, info = kb.merge(fz, Q, "widget")
        self.assertEqual(out, fz)
        self.assertEqual(info["added"], [])
        self.assertGreater(info["skipped"]["no_room"], 0)
        out, info = kb.merge(self.frozen(), Q, "widget", max_items=8)
        self.assertEqual(len(out), 8)
        self.assertEqual(len(info["added"]), 2)

    def test_character_cap_counts_the_whole_pack_and_adds_an_entry_whole_or_not_at_all(self):
        kb = self.load()
        fz = self.frozen()
        base = len(prompts.pack_text(fz))
        sizes = {e["id"]: len(prompts.pack_text([kb.item(e["id"])])) for e in kb.rows}
        out, info = kb.merge(fz, Q, "widget", max_chars=base)           # no room at all
        self.assertEqual(out, fz)
        self.assertEqual(info["added"], [])
        # a cap that fits exactly the first entry: that entry (and only an entry that still fits) is added, never a cut one
        first = kb.merge(fz, Q, "widget")[1]["added"][0]
        need = base + 2 + sizes[first]                                   # the separator "\n\n"
        out, info = kb.merge(fz, Q, "widget", max_chars=need)
        self.assertEqual(info["added"][0], first)
        self.assertLessEqual(len(prompts.pack_text(out)), need)
        for it in out[6:]:
            self.assertEqual(prompts.pack_text([it]), kb.byid[it["id"][len(KE.PACK_PREFIX):]]["rendered"])    # whole
        out, info = kb.merge(fz, Q, "widget", max_chars=need - 1)
        self.assertNotIn(first, info["added"])
        self.assertLessEqual(len(prompts.pack_text(out)), need - 1)

    def test_a_pack_already_over_the_cap_gets_nothing_and_loses_nothing(self):
        kb = self.load()
        big = [dict(F.CH["c05"], id=f"b{i}", text="widget " * 800) for i in range(6)]        # about 33,600 characters
        self.assertGreater(len(prompts.pack_text(big)), KE.MAX_CHARS)
        out, info = kb.merge(big, Q, "widget")
        self.assertEqual(out, big)
        self.assertEqual(info["added"], [])

    def test_default_limits_are_the_designs(self):
        self.assertEqual((KE.MAX_ITEMS, KE.MAX_CHARS, KE.POOL), (10, 24000, 24))

    def test_an_entry_whose_sources_are_all_in_the_pack_is_skipped(self):
        kb = self.load()
        e_c01 = next(e for e in kb.rows if [s["id"] for s in e["sources"]] == ["c01"])
        fz = self.frozen(ids=("c01", "c05", "c06", "c13", "c10", "c04"))
        out, info = kb.merge(fz, Q, "widget reconcile interval")
        self.assertNotIn(e_c01["id"], info["added"])
        self.assertGreaterEqual(info["skipped"]["all_sources_in_pack"], 1)
        # a cross entry with one source in the pack and one outside is not skipped
        e_x = next(e for e in kb.rows if len(e["sources"]) == 2)
        fz2 = self.frozen(ids=("c07", "c05", "c06", "c13", "c10", "c04"))
        out2, info2 = kb.merge(fz2, Q + " upgrade rollback bundle", "widget upgrade rollback", max_items=40, max_chars=10 ** 6)
        self.assertIn(e_x["id"], info2["added"])

    def test_a_second_entry_of_the_same_source_set_is_skipped(self):
        rows = [json.loads(l) for l in self.P["entries_index.jsonl"].read_text().splitlines()]
        twin = copy.deepcopy(next(r for r in rows if [s["id"] for s in r["sources"]] == ["c03"]))
        twin["id"] = "kbe-" + "f" * 16
        twin["statement"] = "The Widget operator pool size is limited to nine."
        twin["rendered"] = K.render_text(twin)
        kb = KE.KbEntries(sorted(rows + [twin], key=lambda r: r["id"]), reranker=PosReranker())
        out, info = kb.merge(self.frozen(), Q, "widget pool size", max_items=40, max_chars=10 ** 6)
        same = [i for i in info["added"] if [s["id"] for s in kb.byid[i]["sources"]] == ["c03"]]
        self.assertEqual(len(same), 1)
        self.assertEqual(info["skipped"]["same_source_set"], 1)

    def test_no_entry_is_added_twice_and_ids_are_unique(self):
        kb = self.load()
        out, info = kb.merge(self.frozen(), Q, "widget", max_items=40, max_chars=10 ** 6)
        ids = [i["id"] for i in out]
        self.assertEqual(len(ids), len(set(ids)))

    def test_the_search_uses_the_injected_reranker_with_the_rendered_entries(self):
        rr = PosReranker()
        kb = self.load(reranker=rr)
        ids = kb.ranked(Q, "widget reconcile interval")
        self.assertEqual(len(rr.calls), 1)
        pairs = rr.calls[0]
        self.assertLessEqual(len(pairs), KE.POOL)
        self.assertTrue(all(q == Q and t.startswith("strimzi — Fact entry: ") for q, t in pairs))
        self.assertTrue(all(len(t) <= KE.PASSAGE_CHARS for _, t in pairs))
        self.assertEqual(sorted(ids), sorted(set(ids)))

    def test_rrf_is_the_frozen_expression(self):
        rr = PosReranker()
        kb = self.load(reranker=rr)
        ids = kb.ranked(Q, "widget reconcile interval pool drain labels")
        cand = [i for i, _ in kb.scores(Q + " widget reconcile interval pool drain labels").most_common(KE.POOL)]
        ce = [float(j) for j in range(len(cand))]
        order = sorted(range(len(cand)), key=lambda j: -ce[j])
        pos = {j: r for r, j in enumerate(order)}
        want = sorted(range(len(cand)), key=lambda j: -(1 / (60 + j) + 1 / (60 + pos[j])))
        self.assertEqual(ids, [kb.rows[cand[j]]["id"] for j in want])

    def test_without_rerank_the_bm25_order_stands_and_no_reranker_is_called(self):
        rr = PosReranker()
        kb = self.load(reranker=rr)
        ids = kb.ranked(Q, "widget reconcile", rerank=False)
        self.assertEqual(rr.calls, [])
        cand = [kb.rows[i]["id"] for i, _ in kb.scores(Q + " widget reconcile").most_common(KE.POOL)]
        self.assertEqual(ids, cand)

    def test_no_match_gives_no_entries(self):
        kb = self.load()
        out, info = kb.merge(self.frozen(), "zzzz qqqq", None)
        self.assertEqual(len(out), 6)
        self.assertEqual(info["candidates"], 0)

    def test_resolve_pack_round_trip(self):
        kb = self.load()
        out, info = kb.merge(self.frozen(), Q, "widget")
        ids = [i["id"] for i in out]
        with mock.patch("kafka_sre_rag.evalhooks.chunks_by_ids", side_effect=lambda ids_, ip=None: [F.CH[i] for i in ids_]):
            back = KE.resolve_pack(ids, kb)
        self.assertEqual([prompts.pack_text([x]) for x in back], [prompts.pack_text([x]) for x in out])
        with self.assertRaises(KeyError):
            KE.resolve_pack(["kbe:kbe-0000000000000000"], kb)
        with self.assertRaises(KeyError):
            KE.resolve_pack(["kbe:kbe-0000000000000000"], None)


class OptionOffIsByteIdentical(Base):
    """With the option off nothing of the entries module is imported and every prompt is byte for byte as it was."""

    def fz(self):
        return [F.CH["c05"], F.CH["c06"]]

    def digest(self, msgs):
        return hashlib.sha256(json.dumps(msgs, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()

    def test_phase0_hashes_unchanged(self):
        self.assertEqual(config.prompt_hashes(), {n: config.load()["prompts_sha256"][n] for n in config.PROMPT_NAMES})

    def test_ask_off_equals_ask_without_the_module(self):
        with mock.patch("kafka_sre_rag.retrieval.select", return_value=self.fz()):
            g1, g2, g3 = Gen(), Gen(), Gen()
            a1 = pipeline.ask(Q, g1, config.load())
            a2 = pipeline.ask(Q, g2, config.load(), kb_entries=None)
            with mock.patch.dict(sys.modules, {"kafka_sre_rag.kb_entries": None}):      # importing it would raise
                a3 = pipeline.ask(Q, g3, config.load())
        self.assertEqual(g1.calls, g2.calls)
        self.assertEqual(g1.calls, g3.calls)
        self.assertEqual(self.digest(a1.messages), self.digest(a3.messages))
        self.assertEqual(a1.messages, a2.messages)
        self.assertIsNone(a3.kb_entries)
        self.assertNotIn("knowledge-base", " ".join(a3.unmeasured_reasons))

    def test_evalhooks_and_service_off_do_not_touch_the_module(self):
        with mock.patch("kafka_sre_rag.retrieval.select", return_value=self.fz()):
            with mock.patch.dict(sys.modules, {"kafka_sre_rag.kb_entries": None}):
                g = Gen()
                r = evalhooks.run_item(Q, g, config.load())
                self.assertNotIn("kb_entries", r)
                svc = service.Service(Gen(), config.load(), rerank=False)
                text, extra = svc.answer(Q, "default", "k6")
        self.assertNotIn("kb_entries", extra)
        self.assertNotIn("knowledge-base", " ".join(extra["unmeasured_reasons"]))
        self.assertEqual(r["messages"][1]["content"], prompts.user_message(Q, self.fz()))

    def test_cli_dry_run_off_is_identical_with_and_without_the_module(self):
        def run():
            out, g = io.StringIO(), Gen()
            with mock.patch("kafka_sre_rag.retrieval.select", return_value=self.fz()), contextlib.redirect_stdout(out), \
                    contextlib.redirect_stderr(io.StringIO()), mock.patch.object(cli, "make_generator", return_value=g):
                rc = cli.main(["ask", "--dry-run", "--no-rerank-in-dry-run", Q])
            return rc, out.getvalue(), self.digest([m for call in g.calls for m in call[0]])
        base = run()
        with mock.patch.dict(sys.modules, {"kafka_sre_rag.kb_entries": None}):
            blocked = run()
        self.assertEqual(base, blocked)
        self.assertEqual(base[0], 0)

    def test_the_flag_is_off_by_default_on_ask_chat_and_serve(self):
        p = cli.build_parser()
        for argv in (["ask", "q"], ["chat"], ["serve"]):
            self.assertFalse(p.parse_args(argv).kb_entries, argv)
            self.assertTrue(p.parse_args(argv + ["--kb-entries"]).kb_entries, argv)


class OptionOn(Base):
    def fz(self):
        return self.frozen()

    def test_ask_with_entries_adds_them_after_the_frozen_pack_and_marks_unmeasured(self):
        kb = self.load()
        with mock.patch("kafka_sre_rag.retrieval.select", return_value=self.fz()):
            g_off, g_on = Gen(), Gen()
            off = pipeline.ask(Q, g_off, config.load())
            on = pipeline.ask(Q, g_on, config.load(), kb_entries=kb)
        self.assertIn(pipeline.KB_ENTRIES_REASON, on.unmeasured_reasons)
        self.assertFalse(on.measured)
        off_user, on_user = off.messages[1]["content"], on.messages[1]["content"]
        head = off_user.split("\n\nQuestion:")[0]
        self.assertTrue(on_user.startswith(head), "the frozen excerpts stay first and byte for byte")
        self.assertIn("Fact entry: ", on_user)
        self.assertEqual(off.messages[0], on.messages[0])
        self.assertEqual(len(on.sources), 6 + len(on.kb_entries["added"]))

    def test_the_frozen_profile_only(self):
        kb = self.load()
        with mock.patch("kafka_sre_rag.retrieval.select", return_value=self.fz()):
            with self.assertRaises(ValueError):
                pipeline.ask(Q, Gen(), config.load(), kb_entries=kb, profile="r4")
        with self.assertRaises(ValueError):
            service.Service(Gen(), config.load(), kb_entries=kb, profile="r4")
        with self.assertRaises(ValueError):
            evalhooks.run_item(Q, Gen(), config.load(), kb_entries=kb, pack_ids=["c01"])

    def test_cli_dry_run_with_the_option_and_the_refusals(self):
        with mock.patch.object(KE, "_index_path", return_value=self.P["entries_index.jsonl"]), \
                mock.patch("kafka_sre_rag.retrieval.select", return_value=self.fz()):
            out, err, g = io.StringIO(), io.StringIO(), Gen()
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err), mock.patch.object(cli, "make_generator", return_value=g):
                rc = cli.main(["ask", "--dry-run", "--no-rerank-in-dry-run", "--kb-entries", "--kb-entries-record", str(self.record), Q])
            self.assertEqual(rc, 0, err.getvalue())
            self.assertIn("Fact entry: ", g.calls[1][0][1]["content"])     # the answer call's user message
            self.assertIn("knowledge-base entries", err.getvalue())
            for argv, text in ((["--profile", "r4"], "frozen"), (["--service-url", "http://127.0.0.1:1"], "serve")):
                err = io.StringIO()
                with contextlib.redirect_stderr(err):
                    rc = cli.main(["ask", "--dry-run", "--kb-entries", "--kb-entries-record", str(self.record), *argv, Q])
                self.assertEqual(rc, 2, argv)
                self.assertIn(text, err.getvalue())

    def test_service_with_the_option(self):
        kb = self.load()
        svc = service.Service(Gen(), config.load(), rerank=False, kb_entries=kb)
        with mock.patch("kafka_sre_rag.retrieval.select", return_value=self.fz()):
            text, extra = svc.answer(Q, "default", "k6")
        self.assertIn(pipeline.KB_ENTRIES_REASON, extra["unmeasured_reasons"])
        self.assertFalse(extra["measured"])
        self.assertEqual(extra["chunk_ids"][:6], [c["id"] for c in self.fz()])
        self.assertEqual(extra["chunk_ids"][6:], [KE.PACK_PREFIX + a for a in extra["kb_entries"]["added"]])
        self.assertEqual(len(extra["sources"]), len(extra["chunk_ids"]))


if __name__ == "__main__":
    unittest.main()
