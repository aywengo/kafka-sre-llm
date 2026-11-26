"""M7 KB phase 2, I5: D7 packs, rating bundles and the served count (scripts/m7_d7_served.py). Offline: the synthetic fixtures of
tests/fixtures/kb_i4_i5 (no D7 text or id), a hash stand-in for the reranker; no model, no server, no real D7 pack."""
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
sys.path.insert(0, str(ROOT / "tests" / "fixtures" / "kb_i4_i5"))
import eval2  # noqa: E402
import fixtures as F  # noqa: E402
import kb_build_index  # noqa: E402
import m7_d7_served as S  # noqa: E402
from kafka_sre_rag import kb_entries as KE, prompts  # noqa: E402


def run(argv):
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        rc = S.main(argv)
    return rc, out.getvalue(), err.getvalue()


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.P = F.write_all(self.tmp / "fx")
        self.record = self.tmp / "kb_entries.json"
        self.record.write_text(json.dumps(kb_build_index.build(self.P["entries_index.jsonl"], self.P["slice.jsonl"], self.P["MANIFEST.json"],
                                                               file_name="work/x/e.jsonl", created="2026-11-26T00:00:00Z")))
        p = mock.patch("retrieve.reranker", side_effect=AssertionError("the real reranker was loaded"))
        p.start()
        self.addCleanup(p.stop)
        self.out = self.tmp / "out"
        self.rows = {r["id"]: r for r in map(json.loads, self.P["entries_index.jsonl"].read_text().splitlines())}

    def packs_args(self, **kw):
        a = ["packs", "--items", str(self.P["items.jsonl"]), "--terms", str(self.P["search_terms.jsonl"]), "--kb-record", str(self.record),
             "--kb-index", str(self.P["entries_index.jsonl"]), "--index-path", str(self.P["chunks.jsonl"]), "--out-dir", str(self.out), "--stand-in"]
        return a + sum(([f"--{k.replace('_', '-')}", v] for k, v in kw.items()), [])

    def make_packs(self):
        rc, out, err = run(self.packs_args())
        self.assertEqual(rc, 0, err)
        return out

    def pk(self, name):
        return {r["id"]: r["pack"] for r in map(json.loads, (self.out / f"packs_{name}_d7.jsonl").read_text().splitlines())}

    def bundle_args(self, seed="s1", out="bundles", key="KEY.json"):
        return ["bundles", "--items", str(self.P["items.jsonl"]), "--rubrics", str(self.P["rubrics.jsonl"]),
                "--packs-frozen", str(self.out / "packs_frozen_d7.jsonl"), "--packs-entries", str(self.out / "packs_entries_d7.jsonl"),
                "--kb-record", str(self.record), "--kb-index", str(self.P["entries_index.jsonl"]), "--index-path", str(self.P["chunks.jsonl"]),
                "--seed", seed, "--out-dir", str(self.tmp / out), "--key-out", str(self.tmp / key)]

    def make_bundles(self, **kw):
        self.make_packs()
        rc, out, err = run(self.bundle_args(**kw))
        self.assertEqual(rc, 0, err)
        return json.loads((self.tmp / kw.get("key", "KEY.json")).read_text())

    def count_args(self, rating, key="KEY.json", **kw):
        a = ["count", "--items", str(self.P["items.jsonl"]), "--rubrics", str(self.P["rubrics.jsonl"]), "--slice-tags", str(self.P["slice_tags.json"]),
             "--ledger", str(self.P["D7_LEDGER.md"]), "--denominator", str(self.P["denominator.txt"]),
             "--packs-frozen", str(self.out / "packs_frozen_d7.jsonl"), "--packs-entries", str(self.out / "packs_entries_d7.jsonl"),
             "--packs-top10", str(self.out / "packs_top10_d7.jsonl"), "--packs-r4g2", str(self.out / "packs_r4g2_d7.jsonl"),
             "--key", str(self.tmp / key), "--rating", str(rating)]
        return a + sum(([f"--{k.replace('_', '-')}", v] for k, v in kw.items()), [])

    # ---- an oracle rater: names what truly states each point, as the recorded covering sets say
    def oracle(self, key, *, extra=None):
        """Rating rows. For each point: every covering set whose chunks are all shown (as items) is named, and every shown entry whose
        source set equals a covering set. extra(sid, pid, by_id, rows) may add sets."""
        rows = []
        for r in map(json.loads, self.P["rubrics.jsonl"].read_text().splitlines()):
            sid = r["id"]
            items = key["scenarios"][sid]["items"]
            by_id = {}
            for it in items:
                by_id.setdefault(it["id"], it)
            for m in r["must"]:
                sets = []
                for cs in m["chunk_sets"]:
                    if all(c in by_id for c in cs):
                        sets.append(self.named([by_id[c] for c in cs]))
                    for it in items:
                        if it["type"] == "entry" and it["role"] == "pack" and sorted(s["id"] for s in self.rows[it["id"]]["sources"]) == sorted(cs):
                            sets.append(self.named([it]))
                if extra:
                    extra(sid, m["id"], items, sets)
                rows.append({"id": sid, "point": m["id"], "sets": sets, "rater": "oracle"})
        return rows

    @staticmethod
    def named(its):
        return {"items": [i["label"] for i in its], "quotes": {i["label"]: i["text"].split("\n", 1)[1][:30] for i in its}}

    def write_rating(self, rows, name="rating.jsonl"):
        p = self.tmp / name
        p.write_text("".join(json.dumps(r) + "\n" for r in rows))
        return p

    def expected(self, key):
        """The served flags, computed independently from the packs and the recorded sets (not from the script)."""
        A, K = {s: set(v) for s, v in self.pk("frozen").items()}, {s: set(v) for s, v in self.pk("entries").items()}
        exp = {}
        for r in map(json.loads, self.P["rubrics.jsonl"].read_text().splitlines()):
            for m in r["must"]:
                def srv(pack):
                    for cs in m["chunk_sets"]:
                        if set(cs) <= pack:
                            return True
                        if any(i.startswith("kbe-") and sorted(s["id"] for s in self.rows[i]["sources"]) == sorted(cs) for i in pack):
                            return True
                    return False
                exp[(r["id"], m["id"])] = {"A": srv(A[r["id"]]), "K": srv(K[r["id"]])}
        return exp


class Terms(Base):
    def test_fingerprint_is_evals(self):
        msgs = prompts.expand_messages("q?")
        self.assertEqual(S.messages_fingerprint(msgs), eval2.messages_fingerprint(msgs))
        it = F.ITEMS[0]
        self.assertEqual(S.full_question(it), eval2.full_question(it))

    def test_a_wrong_prompt_hash_ids_or_sha_are_refused(self):
        items = S.read_jsonl(self.P["items.jsonl"])
        by, sha = S.load_terms(self.P["search_terms.jsonl"], items)
        self.assertEqual(len(by), 3)
        rows = [json.loads(l) for l in self.P["search_terms.jsonl"].read_text().splitlines()]
        for name, mut in (("hash", lambda r: r[0].update(prompt_sha256="0" * 64)), ("ids", lambda r: r.pop()), ("dup", lambda r: r.append(r[0])),
                          ("text", lambda r: r[0].update(expansion=None))):
            rr = copy.deepcopy(rows)
            mut(rr)
            bad = self.write_rating(rr, "t.jsonl")
            with self.assertRaises(S.Refused, msg=name):
                S.load_terms(bad, items)
        with self.assertRaises(S.Refused):
            S.load_terms(self.P["search_terms.jsonl"], items, "0" * 64)
        with self.assertRaises(S.Refused):
            S.load_terms(self.tmp / "absent.jsonl", items)


class Packs(Base):
    def test_round_trip_through_the_replay_loader(self):
        self.make_packs()
        items = S.read_jsonl(self.P["items.jsonl"])
        for name in ("frozen", "entries", "top10", "r4g2"):
            path = self.out / f"packs_{name}_d7.jsonl"
            rows = [json.loads(l) for l in path.read_text().splitlines()]
            self.assertTrue(all(set(r) == {"id", "pack", "error"} and r["error"] is None for r in rows))     # the D5 packs' row format
            rep = eval2.load_replay(path, items)
            self.assertEqual(rep, {r["id"]: r["pack"] for r in rows})
            man = eval2.read_manifest(path)
            self.assertEqual((man["kind"], man["count"], man["setting"]), ("m7-d7-packs", 3, name))
            self.assertEqual(man["pack_file_sha256"], hashlib.sha256(path.read_bytes()).hexdigest())
            self.assertIsNone(man.get("mode"))
        self.assertTrue(eval2.read_manifest(self.out / "packs_entries_d7.jsonl")["stand_in"])

    def test_entries_pack_starts_with_the_frozen_pack_and_has_at_most_four_more(self):
        self.make_packs()
        A, K = self.pk("frozen"), self.pk("entries")
        for s in A:
            self.assertEqual(K[s][:len(A[s])], A[s])
            self.assertTrue(len(A[s]) <= len(K[s]) <= len(A[s]) + 4)
            self.assertTrue(all(i.startswith("kbe-") for i in K[s][len(A[s]):]))
            self.assertTrue(all(not i.startswith("kbe-") for i in A[s]))
            self.assertLessEqual(len(self.pk("top10")[s]), 10)

    def test_the_frozen_arm_is_the_runtimes_select_itself(self):
        self.make_packs()
        from kafka_sre_rag import retrieval
        stand = S.StandInReranker()
        terms = {r["id"]: r for r in S.read_jsonl(self.P["search_terms.jsonl"])}
        with mock.patch.object(retrieval.RT, "reranker", lambda: stand):
            for it in S.read_jsonl(self.P["items.jsonl"]):
                want = [c["id"] for c in retrieval.select(S.full_question(it), terms[it["id"]]["expansion"], index_path=str(self.P["chunks.jsonl"]))]
                self.assertEqual(self.pk("frozen")[it["id"]], want)

    def test_replayed_entry_ids_resolve_to_items(self):
        self.make_packs()
        kb = KE.KbEntries.load(self.record, index_path=self.P["entries_index.jsonl"], reranker=S.StandInReranker())
        ids = self.pk("entries")["fx-01"]
        items = KE.resolve_pack(ids, kb, index_path=str(self.P["chunks.jsonl"]))
        self.assertEqual([i["id"] for i in items], ids)

    def test_a_line_per_scenario_a_done_marker_and_no_overwrite(self):
        out = self.make_packs()
        self.assertEqual(sum(l.startswith("[packs]") and " id=fx-" in l for l in out.splitlines()), 3)
        self.assertTrue((self.out / "packs_d7.done").is_file())
        rc, _, err = run(self.packs_args())
        self.assertEqual(rc, 2)
        self.assertIn("exists", err)

    def test_a_stopped_run_resumes_without_rewriting_rows(self):
        self.make_packs()
        rows = self.out / "packs_d7.rows.jsonl"
        text = rows.read_text()
        for p in list(self.out.glob("packs_*_d7.jsonl*")) + [self.out / "packs_d7.done"]:
            p.unlink()
        rows.write_text("".join(text.splitlines(True)[:2]))             # a run that stopped after two scenarios
        rc, out, err = run(self.packs_args())
        self.assertEqual(rc, 0, err)
        self.assertIn("2 already done, 1 to do", out)
        strip = lambda t: [{k: v for k, v in json.loads(l).items() if k != "seconds"} for l in t.splitlines()]  # noqa: E731
        self.assertEqual(rows.read_text().splitlines()[:2], text.splitlines()[:2])        # the two finished rows are untouched
        self.assertEqual(strip(rows.read_text()), strip(text))

    def test_refusals_leave_nothing_written(self):
        bad = self.tmp / "bad_terms.jsonl"
        rows = [json.loads(l) for l in self.P["search_terms.jsonl"].read_text().splitlines()]
        rows[1]["prompt_sha256"] = "0" * 64
        bad.write_text("".join(json.dumps(r) + "\n" for r in rows))
        rc, _, err = run(self.packs_args(terms=str(bad)))
        self.assertEqual(rc, 2)
        self.assertIn("prompt_sha256", err)
        self.assertEqual(list(self.out.glob("packs_*")), [])
        self.record.write_text(json.dumps(dict(json.loads(self.record.read_text()), sha256="0" * 64)))
        rc, _, err = run(self.packs_args())
        self.assertEqual(rc, 2)
        self.assertIn("does not match the record", err)
        rc, _, err = run(self.packs_args(terms=str(self.tmp / "nope.jsonl")))
        self.assertEqual(rc, 2)


class Bundles(Base):
    def test_union_shuffled_unlabelled_with_decoys_and_controls(self):
        key = self.make_bundles()
        K_ = self.pk("entries")
        A = self.pk("frozen")
        for sid, sc in key["scenarios"].items():
            items = sc["items"]
            roles = [i["role"] for i in items]
            pack = [i["id"] for i in items if i["role"] == "pack"]
            self.assertEqual(sorted(pack), sorted(K_[sid]))                       # the union of both packs is the entries pack
            self.assertTrue(set(A[sid]) <= set(pack))
            decoys = [i for i in items if i["role"] == "decoy"]
            self.assertTrue(1 <= len(decoys) <= 3, sid)
            cover = {c for r in map(json.loads, self.P["rubrics.jsonl"].read_text().splitlines()) if r["id"] == sid
                     for m in r["must"] for cs in m["chunk_sets"] for c in cs}
            for d in decoys:
                e = self.rows[d["id"]]
                self.assertNotIn(d["id"], K_[sid])
                self.assertTrue(any(s["id"] in cover for s in e["sources"]))
            for c in (i for i in items if i["role"] == "control"):
                self.assertNotIn(c["id"], K_[sid])
                self.assertIn(c["id"], cover)
            self.assertEqual([i["label"] for i in items], [f"i{n:02d}" for n in range(1, len(items) + 1)])
            self.assertTrue(all(i["text"] == prompts.pack_text([F.CH[i["id"]]]) for i in items if i["type"] == "chunk"))
        # shuffled: in at least one scenario the roles are not grouped
        self.assertTrue(any(sorted(i["role"] for i in sc["items"]) != [i["role"] for i in sc["items"]] for sc in key["scenarios"].values()))

    def test_the_bundle_text_carries_no_arm_role_or_id(self):
        key = self.make_bundles()
        for sid, sc in key["scenarios"].items():
            text = (self.tmp / "bundles" / f"bundle_{sid}.md").read_text()
            for bad in ("decoy", "control", "frozen", "kbe-", "arm ", "K pack", "entries pack", "retrieved"):
                self.assertNotIn(bad, text, (sid, bad))
            for it in sc["items"]:
                self.assertIn(f"### {it['label']}", text)
                self.assertIn(it["text"], text)
            for p in sc["points"]:
                self.assertIn(f"**{p}**", text)

    def test_every_must_point_is_to_be_rated_and_a_second_sample_is_drawn(self):
        key = self.make_bundles()
        tpl = [json.loads(l) for l in (self.tmp / "bundles" / "rating_template.jsonl").read_text().splitlines()]
        self.assertEqual(len(tpl), 11)                                    # all points, with or without a covering set
        self.assertEqual(len(key["second_rater_points"]), 3)              # ceil(20 % of 11)
        self.assertTrue(all(r["sets"] is None for r in tpl))

    def test_deterministic_by_seed_and_never_overwrites(self):
        k1 = self.make_bundles(seed="s1")
        rc, _, err = run(self.bundle_args(seed="s1"))
        self.assertEqual(rc, 2)
        self.assertIn("exist", err)
        rc, _, _ = run(self.bundle_args(seed="s1", out="bundles2", key="KEY2.json"))
        self.assertEqual(json.loads((self.tmp / "KEY2.json").read_text()), k1)
        rc, _, _ = run(self.bundle_args(seed="other", out="bundles3", key="KEY3.json"))
        self.assertNotEqual(json.loads((self.tmp / "KEY3.json").read_text())["scenarios"], k1["scenarios"])

    def test_a_pack_that_does_not_start_with_the_frozen_pack_is_refused(self):
        self.make_packs()
        p = self.out / "packs_entries_d7.jsonl"
        rows = [json.loads(l) for l in p.read_text().splitlines()]
        rows[0]["pack"].reverse()
        p.write_text("".join(json.dumps(r) + "\n" for r in rows))
        rc, _, err = run(self.bundle_args())
        self.assertEqual(rc, 2)
        self.assertIn("start with the frozen pack", err)


class Count(Base):
    def setUp(self):
        super().setUp()
        self.key = self.make_bundles()

    def counted(self, rows, **kw):
        rc, out, err = run(self.count_args(self.write_rating(rows), **kw) + ["--json-out", str(self.tmp / f"r{len(list(self.tmp.glob('r*.json')))}.json")])
        self.assertEqual(rc, 0, err)
        return json.loads(sorted(self.tmp.glob("r*.json"))[-1].read_text()), out

    def test_the_count_equals_an_independent_computation_from_the_packs(self):
        res, out = self.counted(self.oracle(self.key))
        exp = self.expected(self.key)
        den = [tuple(x.split("/")) for x in self.P["denominator.txt"].read_text().split()]
        self.assertEqual(res["bar"]["points"], len(den))
        self.assertEqual(res["bar"]["A"], sum(exp[k]["A"] for k in den))
        self.assertEqual(res["bar"]["K"], sum(exp[k]["K"] for k in den))
        self.assertEqual(res["bar"]["lost"], 0)
        covered = [k for k in exp if k in {(r["id"], m["id"]) for r in map(json.loads, self.P["rubrics.jsonl"].read_text().splitlines())
                                           for m in r["must"] if m["chunk_sets"]}]
        self.assertEqual(res["whole_d7"]["points"], len(covered))
        self.assertEqual(res["whole_d7"]["K"], sum(exp[k]["K"] for k in covered))
        self.assertEqual(res["out_of_slice"]["points"], 3)                  # fx-03's covered points
        self.assertEqual(res["weight_2"]["points"] + res["weight_1"]["points"], len(den))
        self.assertEqual(res["entry_only"]["points_without_covering_set"], 2)
        for part in ("Printed beside the bar", "whole D7", "out-of-slice", "weight 2", "entry-only points", "covering-set count", "second rater",
                     "BAR"):
            self.assertIn(part.lower(), out.lower(), part)
        self.assertEqual(res["controls"]["named_by_the_rater"], res["controls"]["controls"])

    def test_an_arm_is_credited_only_from_the_rating(self):
        res, _ = self.counted([{**r, "sets": []} for r in self.oracle(self.key)])
        self.assertEqual((res["bar"]["A"], res["bar"]["K"], res["whole_d7"]["A"], res["whole_d7"]["K"]), (0, 0, 0, 0))
        self.assertFalse(res["bar"]["met"])
        self.assertGreater(res["covering_set_count"]["denominator"]["frozen"], 0)         # the recorded sets say A holds some, the rating says none

    def test_an_entry_credits_the_k_arm_only(self):
        rows = self.oracle(self.key)
        gained = 0
        for sid, sc in self.key["scenarios"].items():
            ents = [i for i in sc["items"] if i["type"] == "entry" and i["role"] == "pack"]
            gained += len(ents)
        self.assertGreater(gained, 0)
        res, _ = self.counted(rows)
        self.assertGreaterEqual(res["whole_d7"]["K"], res["whole_d7"]["A"])
        exp = self.expected(self.key)
        self.assertEqual(res["whole_d7"]["gained"], sum(1 for k, v in exp.items() if v["K"] and not v["A"] and k in
                                                       {(r["id"], m["id"]) for r in map(json.loads, self.P["rubrics.jsonl"].read_text().splitlines())
                                                        for m in r["must"] if m["chunk_sets"]}))

    def test_a_named_set_with_a_decoy_never_counts(self):
        decoy_rows = []

        def extra(sid, pid, items, sets):
            decoys = [i for i in items if i["role"] == "decoy"]
            pack = [i for i in items if i["role"] == "pack" and i["type"] == "chunk"]
            if pid in ("m3", "m4") and decoys:                              # points the oracle left unserved or covered by a pair
                sets.append(self.named([decoys[0]]))                         # a decoy alone
                sets.append(self.named([decoys[0], pack[0]]))                # a decoy with a pack item
                decoy_rows.append((sid, pid))
        base, _ = self.counted(self.oracle(self.key))
        res, out = self.counted(self.oracle(self.key, extra=extra))
        self.assertTrue(decoy_rows)
        for k in ("A", "K", "points", "gained", "lost"):
            self.assertEqual(res["bar"][k], base["bar"][k], k)
            self.assertEqual(res["whole_d7"][k], base["whole_d7"][k], k)
        self.assertEqual(res["entry_only"], base["entry_only"])
        self.assertEqual(res["dropped_sets"]["decoy"], 2 * len(decoy_rows))
        self.assertIn(f"with a decoy {2 * len(decoy_rows)}", out)

    def test_a_decoy_that_is_also_in_the_pack_still_never_counts(self):
        rows = self.oracle(self.key)                                           # names the retrieved entries for their points
        base, _ = self.counted(rows)
        self.assertGreater(base["whole_d7"]["gained"], 0)
        key = json.loads((self.tmp / "KEY.json").read_text())
        for sc in key["scenarios"].values():
            for it in sc["items"]:
                if it["type"] == "entry":
                    it["role"] = "decoy"                                       # a stale key: the same entries are now marked decoys
        (self.tmp / "KEY.json").write_text(json.dumps(key))
        res, out = self.counted(rows)
        self.assertEqual(res["whole_d7"]["gained"], 0)
        self.assertGreater(res["dropped_sets"]["decoy"], 0)
        self.assertEqual(res["bar"]["K"], res["bar"]["A"])

    def test_a_decoy_set_does_not_count_even_when_it_is_the_only_naming_of_a_point_with_a_covering_set(self):
        def extra(sid, pid, items, sets):
            decoys = [i for i in items if i["role"] == "decoy"]
            if (sid, pid) == ("fx-01", "m1") and decoys:
                sets[:] = [self.named([decoys[0]])]                          # the rater named only a decoy for a point that has a real set
        res, _ = self.counted(self.oracle(self.key, extra=extra))
        base, _ = self.counted(self.oracle(self.key))
        self.assertLess(res["bar"]["K"], base["bar"]["K"])
        self.assertLessEqual(res["bar"]["A"], base["bar"]["A"])

    def test_invalid_sets_are_dropped_and_counted(self):
        def extra(sid, pid, items, sets):
            real = [i for i in items if i["role"] == "pack"]
            if pid == "m1" and len(real) >= 3:
                sets.append({"items": [i["label"] for i in real[:3]], "quotes": {i["label"]: i["text"][:5] for i in real[:3]}})    # three items
                sets.append({"items": ["i99"], "quotes": {"i99": "x"}})                                                           # unknown label
                sets.append({"items": [real[0]["label"]], "quotes": {real[0]["label"]: "this text is not in the item"}})         # not verbatim
                sets.append({"items": [real[0]["label"]], "quotes": {}})                                                          # no quote
        res, out = self.counted(self.oracle(self.key, extra=extra))
        base, _ = self.counted(self.oracle(self.key))
        self.assertEqual(res["dropped_sets"]["shape"], 2 * 3)
        self.assertEqual(res["dropped_sets"]["quote"], 2 * 3)
        self.assertEqual(res["bar"]["K"], base["bar"]["K"])
        self.assertIn("quote not verbatim 6", out)

    def test_an_unrated_point_is_refused(self):
        rows = self.oracle(self.key)
        for name, rr in (("missing row", rows[1:]), ("no sets field", [{**rows[0], "sets": None}] + rows[1:])):
            rc, _, err = run(self.count_args(self.write_rating(rr, "u.jsonl")))
            self.assertEqual(rc, 2, name)
            self.assertIn("unrated", err)
        rc, _, err = run(self.count_args(self.write_rating(rows + [rows[0]], "d.jsonl")))
        self.assertEqual(rc, 2)
        self.assertIn("twice", err)
        rc, _, err = run(self.count_args(self.write_rating(rows[:-1] + [{**rows[-1], "id": "nope"}], "x.jsonl")))
        self.assertEqual(rc, 2)

    def test_unrated_points_outside_the_denominator_are_refused_too(self):
        rows = [r for r in self.oracle(self.key) if (r["id"], r["point"]) != ("fx-03", "m3")]          # fx-03 is out of the slice
        rc, _, err = run(self.count_args(self.write_rating(rows)))
        self.assertEqual(rc, 2)
        self.assertIn("unrated", err)

    def test_a_second_rater_is_reported_and_must_cover_the_sample(self):
        rows = self.oracle(self.key)
        r2 = self.write_rating(rows, "r2.jsonl")
        rc, out, err = run(self.count_args(self.write_rating(rows), rating2=str(r2)))
        self.assertEqual(rc, 0, err)
        self.assertIn("second rater, 3 points: same A-served flag 3, same K-served flag 3, same named sets 3", out)
        part = [r for r in rows if [r["id"], r["point"]] != self.key["second_rater_points"][0]]
        rc, _, err = run(self.count_args(self.write_rating(rows), rating2=str(self.write_rating(part, "r3.jsonl"))))
        self.assertEqual(rc, 2)
        self.assertIn("second rater left", err)

    def test_denominator_hash_refusal_and_the_ledgers_list(self):
        rows = self.write_rating(self.oracle(self.key))
        den = self.P["denominator.txt"]
        den.write_text(den.read_text() + "fx-03/m1\n")                      # a point added after the ledger fixed the list
        rc, _, err = run(self.count_args(rows))
        self.assertEqual(rc, 2)
        self.assertIn("sha256", err)
        den.write_text("".join(x + "\n" for x in F.denominator()))
        self.assertEqual(run(self.count_args(rows))[0], 0)
        led = self.P["D7_LEDGER.md"]
        text = led.read_text()
        led.write_text(text.replace(text.split("sha256 `")[1][:64], "0" * 64, 1))     # two different hashes in the ledger
        rc, _, err = run(self.count_args(rows))
        self.assertEqual(rc, 2)
        self.assertIn("more than one", err)
        led.write_text("# no hash here\n")
        rc, _, err = run(self.count_args(rows))
        self.assertEqual(rc, 2)
        self.assertIn("names no denominator", err)

    def test_a_denominator_point_without_a_covering_set_is_refused(self):
        den = self.P["denominator.txt"]
        den.write_text(den.read_text() + "fx-01/m3\n")
        led = self.P["D7_LEDGER.md"]
        led.write_text(F.ledger_text(hashlib.sha256(den.read_bytes()).hexdigest()))
        rc, _, err = run(self.count_args(self.write_rating(self.oracle(self.key))))
        self.assertEqual(rc, 2)
        self.assertIn("no recorded covering set", err)

    def test_a_key_for_other_packs_is_refused(self):
        rows = self.write_rating(self.oracle(self.key))
        p = self.out / "packs_frozen_d7.jsonl"
        p.write_text(p.read_text() + "\n")
        rc, _, err = run(self.count_args(rows))
        self.assertEqual(rc, 2)
        self.assertIn("key was made for other", err)

    def test_outputs_go_to_new_paths(self):
        rows = self.write_rating(self.oracle(self.key))
        out = self.tmp / "res.json"
        self.assertEqual(run(self.count_args(rows, json_out=str(out)))[0], 0)
        rc, _, err = run(self.count_args(rows, json_out=str(out)))
        self.assertEqual(rc, 2)
        self.assertIn("exists", err)

    def test_the_bar_is_plus_15_points(self):
        res, out = self.counted(self.oracle(self.key))
        self.assertEqual(res["bar"]["met"], res["bar"]["diff_pp"] >= 15.0)
        self.assertIn("BAR MET" if res["bar"]["met"] else "BAR NOT MET", out)
        self.assertEqual(S.BAR_PP, 15.0)


class Pure(unittest.TestCase):
    ITEMS = {"i01": {"label": "i01", "role": "pack", "type": "chunk", "id": "c1", "text": "T\nalpha beta"},
             "i02": {"label": "i02", "role": "pack", "type": "entry", "id": "kbe-1", "text": "T\ngamma  delta"},
             "i03": {"label": "i03", "role": "decoy", "type": "entry", "id": "kbe-2", "text": "T\nalpha beta"},
             "i04": {"label": "i04", "role": "control", "type": "chunk", "id": "c9", "text": "T\nalpha beta"}}

    def sets(self, *items, quotes=None):
        return {"items": list(items), "quotes": quotes or {i: "alpha" if i != "i02" else "gamma delta" for i in items}}

    def test_decoy_rule_first(self):
        ok, drop = S.classify_sets([self.sets("i03"), self.sets("i01", "i03"), self.sets("i01")], self.ITEMS)
        self.assertEqual(ok, [("i01",)])
        self.assertEqual(drop["decoy"], 2)
        self.assertFalse(S.served([], self.ITEMS, {"c1"}))

    def test_whitespace_normalised_verbatim_and_two_item_limit(self):
        ok, drop = S.classify_sets([self.sets("i02")], self.ITEMS)
        self.assertEqual((ok, drop["quote"]), ([("i02",)], 0))
        ok, drop = S.classify_sets([self.sets("i01", "i02", "i04")], self.ITEMS)
        self.assertEqual((ok, drop["shape"]), ([], 1))
        ok, drop = S.classify_sets([{"items": ["i01"], "quotes": {"i01": "ALPHA"}}, "nonsense", {"items": "i01"}], self.ITEMS)
        self.assertEqual((ok, drop["quote"], drop["shape"]), ([], 1, 2))

    def test_served_needs_every_item_of_the_set_in_the_pack(self):
        self.assertTrue(S.served([("i01",)], self.ITEMS, {"c1"}))
        self.assertTrue(S.served([("i01", "i02")], self.ITEMS, {"c1", "kbe-1"}))
        self.assertFalse(S.served([("i01", "i02")], self.ITEMS, {"c1"}))             # an entry credits the arm that holds it
        self.assertFalse(S.served([("i04",)], self.ITEMS, {"c1", "kbe-1"}))           # a control is in no pack

    def test_ledger_parsing(self):
        h = "a" * 64
        self.assertEqual(S.ledger_denominator(f"x `eval/m7/d7/denominator.txt` | `{h}`\nother {'b' * 64}"), ("eval/m7/d7/denominator.txt", h))
        with self.assertRaises(S.Refused):
            S.ledger_denominator("denominator.txt without a hash")
        with self.assertRaises(S.Refused):
            S.ledger_denominator(f"denominator.txt {h}\ndenominator.txt {'c' * 64}")

    def test_clustered_se(self):
        self.assertIsNone(S.clustered_se_pp([(5, 1)]))
        self.assertAlmostEqual(S.clustered_se_pp([(2, 1), (2, 1)]), 0.0)
        self.assertGreater(S.clustered_se_pp([(2, 2), (2, 0), (2, 1)]), 0)

    def test_the_depth_control_is_by_recorded_sets(self):
        pts = {("s", "p"): {"sets": [["c1", "c2"]], "weight": 1}, ("s", "q"): {"sets": [], "weight": 1}}
        self.assertEqual(S.cover_count([("s", "p"), ("s", "q")], pts, {"s": ["c1", "c2", "c3"]}), 1)
        self.assertEqual(S.cover_count([("s", "p"), ("s", "q")], pts, {"s": ["c1"]}), 0)


class RealDenominator(unittest.TestCase):
    """Read-only: D7's frozen denominator file against the ledger's recorded sha256 (skipped where the ledger is not present)."""

    def test_real_denominator_matches_the_ledger(self):
        led = next((b / "runs/m7/D7_LEDGER.md" for b in (ROOT, *ROOT.parents) if (b / "runs/m7/D7_LEDGER.md").is_file()), None)
        den = ROOT / "eval/m7/d7/denominator.txt"
        if led is None or not den.is_file():
            self.skipTest("runs/m7/D7_LEDGER.md or eval/m7/d7/denominator.txt not present")
        pairs, sha = S.read_denominator(led, den)
        self.assertEqual(len(pairs), 185)
        self.assertEqual(sha, "780f169c85b339cadee595f8d672c46fa69241d54c024d807f42234561cf469e")


if __name__ == "__main__":
    unittest.main()
