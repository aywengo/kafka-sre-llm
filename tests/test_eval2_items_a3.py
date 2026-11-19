"""Assembly, split and manifest of the a3 set (WP1c T8): scripts/eval2_items_a3.py.

Runs the real generators with --allow-missing gap (the not-covered generator waits for a decision), regenerates the 496
in memory (about 30 s, once) and writes only to temporary folders. The real outputs (eval/m4/SPLIT_A3.json,
eval/m4/open/tier_a3.jsonl, eval/hidden/tier_a3.jsonl.enc) are never written here. The hidden key is a throw-away key
made for the test. No model, no network."""
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))
import eval2  # noqa: E402
import eval2_items_a3 as asm  # noqa: E402

HAVE = all(p.exists() for p in (ROOT / "work/m4/catalogues", ROOT / "work/m4/catalogues_a3", asm.OPEN_A1, asm.SPLIT_A1))
SKIP = "the catalogues, the a3 catalogues or the 496 files are not on this machine"
GAP_PRESENT = (ROOT / "scripts" / "eval2_items_a3_gap.py").exists()
ALLOW = () if GAP_PRESENT else ("gap",)   # the build allows a module to be missing only while it is
PRESENT_KINDS = {k for m, k in asm.KIND_OF.items() if m not in ALLOW}
MISSING_KINDS = sorted(k for m, k in asm.KIND_OF.items() if m in ALLOW)
_state = {}


def built():
    if "r" not in _state:
        _state["r"] = asm.build(ALLOW)
    return _state["r"]


def grouped(items):
    return [{**i, "family": asm.group_of(i)} for i in items]


@unittest.skipUnless(HAVE, SKIP)
class GeneratorsAndPredecessor(unittest.TestCase):
    def test_absent_gap_needs_the_flag_and_says_so(self):
        if (ROOT / "scripts" / "eval2_items_a3_gap.py").exists():
            self.skipTest("the gap module exists")
        with self.assertRaises(asm.AssemblyError) as c:
            asm.collect(())
        self.assertIn("--allow-missing gap", str(c.exception))
        items, notes = asm.collect(ALLOW)
        self.assertTrue(any("gap is absent" in n and "not_covered" in n for n in notes))
        self.assertIn("generator gap is absent", asm.report(built(), ALLOW))
        self.assertNotIn("not_covered", {i["kind"] for i in items})

    def test_every_present_generator_contributes(self):
        r = built()
        kinds = {i["kind"] for i in r["items"]}
        self.assertEqual(kinds, PRESENT_KINDS)
        for k in kinds:  # a generator may report a derivation failure and make fewer than the plan; never more
            n = sum(1 for i in r["items"] if i["kind"] == k)
            self.assertTrue(2 * asm.MIN_KIND_ITEMS <= n <= asm.PLAN[k], (k, n))
        self.assertEqual([i["id"] for i in r["items"]], [i["id"] for i in sorted(r["items"], key=lambda i: (asm.MODULES.index(
            {v: k for k, v in asm.KIND_OF.items()}[i["kind"]]), i["id"]))])

    def test_the_496_match_split_json_and_no_id_collides(self):
        r = built()
        self.assertEqual(len(r["old_items"]), 496)
        self.assertEqual(asm.sha(asm.serialise(r["old_items"])), json.loads(asm.SPLIT_A1.read_text())["items_sha256"])
        old = {i["id"] for i in r["old_items"]}
        self.assertFalse(old & {i["id"] for i in r["items"]})
        self.assertTrue(all(i["id"].startswith("a3-") for i in r["items"]))

    def test_a_changed_predecessor_stops_the_build(self):
        with tempfile.TemporaryDirectory() as d:
            bad = Path(d) / "SPLIT.json"
            m = json.loads(asm.SPLIT_A1.read_text())
            m["items_sha256"] = "0" * 64
            bad.write_text(json.dumps(m))
            with mock.patch.object(asm, "SPLIT_A1", bad):
                with self.assertRaises(asm.AssemblyError) as c:
                    asm.predecessor()
        self.assertIn("sha256", str(c.exception))

    def test_id_collision_and_duplicates_are_refused(self):
        a = {"id": "a3-x-001", "tier": "A", "kind": "k", "family": "f", "domain": "d", "question": "q"}
        with self.assertRaises(asm.AssemblyError):
            asm.check_ids([a, dict(a)], [])
        with self.assertRaises(asm.AssemblyError):
            asm.check_ids([a], ["a3-x-001"])
        with self.assertRaises(asm.AssemblyError):
            asm.check_ids([{**a, "id": "x-001"}], [])


@unittest.skipUnless(HAVE, SKIP)
class NearDuplicates(unittest.TestCase):
    def test_real_build_has_no_hit_and_reports_every_source(self):
        r = built()
        self.assertEqual([p for p in r["problems"] if p.startswith("near-duplicate")], [])
        self.assertEqual(set(r["dup_report"]), {"data/*.jsonl", "eval/*.jsonl", "open tier_a.jsonl", "the 496 (open and hidden)",
                                                 "work/v3 questions", "a3 items, other templates",
                                                 "a3 items, same template (reported)"})
        for g in ("data/*.jsonl", "eval/*.jsonl", "open tier_a.jsonl", "the 496 (open and hidden)", "work/v3 questions"):
            j, c, _, _ = r["dup_report"][g]
            self.assertLess(j, 0.5, g)
            self.assertLess(c, 0.8, g)

    def test_a_copy_of_an_old_question_is_a_hit(self):
        r = built()
        old = r["old_items"][5]
        copy = {**r["items"][0], "id": "a3-copy-001", "question": old["question"], "template": "copy"}
        rep, hits, pairs, _ = asm.near_duplicates([copy], r["old_items"])
        self.assertTrue(any(h[0] == "a3-copy-001" and h[1] == "the 496 (open and hidden)" for h in hits))

    def test_two_a3_items_of_other_templates_are_a_pair_and_a_template_sibling_is_not_gating(self):
        base = built()["items"][0]
        q = "How many bytes does the broker keep in the page cache for a write rate of 40 MiB per second when the window is 30 seconds"
        mk = lambda n, t: {**base, "id": "a3-t-%03d" % n, "question": q, "template": t}  # noqa: E731
        _, _, pairs, _ = asm.near_duplicates([mk(1, "a"), mk(2, "b")], [])
        self.assertEqual(len(pairs), 1)
        _, _, pairs, _ = asm.near_duplicates([mk(1, "a"), mk(2, "a")], [])
        self.assertEqual(pairs, [])

    def test_core_drops_only_a_known_format_note(self):
        self.assertEqual(asm.core_of({"question": "A b.\n\nFinish with a final line `ANSWER: x`."}), "A b.")
        self.assertEqual(asm.core_of({"question": "A b.\n\nSomething else."}), "A b.\n\nSomething else.")


@unittest.skipUnless(HAVE, SKIP)
class MixAndGuess(unittest.TestCase):
    def test_real_build_meets_its_floors(self):
        r = built()
        self.assertEqual(r["problems"], [])
        self.assertLess(r["guess"], 0.20)
        self.assertLess(r["guess_union"], 0.20)
        text = "\n".join(r["mix_lines"])
        self.assertIn("a3 alone", text)
        self.assertIn("union (", text)
        self.assertIn("not applicable", text)

    def test_guess_of_a_control_and_of_a_chance_item(self):
        self.assertEqual(asm.item_guess({"kind": "valid_but_wrong", "failure_class": "valid_control"}), 0.5)
        self.assertEqual(asm.item_guess({"kind": "valid_but_wrong", "failure_class": "valid_but_wrong"}), 0.0)
        self.assertEqual(asm.item_guess({"kind": "x", "failure_class": "y", "chance": 0.25}), 0.25)
        self.assertEqual(asm.item_guess({"kind": "not_covered", "failure_class": "z"}), 0.5)

    def test_a_blanket_guess_over_the_limit_is_a_problem(self):
        items = [{"id": "a3-%d" % i, "kind": "safe_order", "failure_class": "valid_control", "template": "t", "family": "f%d" % i,
                  "tier": "A"} for i in range(40)]
        _, overall, _ = asm.guess_table(items)
        self.assertGreaterEqual(overall, 0.20)

    def test_floor_problems_name_the_kind(self):
        few = [{"id": "a3-%d" % i, "kind": "safe_order", "family": "f%d" % i, "template": "t"} for i in range(4)]
        p = asm.floor_problems(few, ("gap",))  # explicit: this tests the allow-missing rule, not the build
        self.assertTrue(any(x.startswith("safe_order: 4 items") for x in p))
        self.assertTrue(any("client_code" in x for x in p))
        self.assertFalse(any("not_covered" in x for x in p))
        self.assertTrue(any("not_covered" in x for x in asm.floor_problems(few, ())))

    def test_the_union_mix_is_reported_and_not_forced(self):
        r = built()
        self.assertEqual(r["problems"], [])  # the union misses two lookup floors (see the report); that is not a problem
        self.assertTrue(any("NOT MET" in l for l in r["mix_lines"]))


@unittest.skipUnless(HAVE, SKIP)
class ExposureAndSplit(unittest.TestCase):
    def test_exposed_items_two_per_kind_distinct_groups_all_open(self):
        r = built()
        ex = r["exposed"]
        self.assertEqual(len(ex), 2 * len(PRESENT_KINDS))
        by = {i["id"]: i for i in r["items"]}
        for kind in {i["kind"] for i in r["items"]}:
            self.assertEqual(sum(1 for e in ex if by[e]["kind"] == kind), 2)
            self.assertEqual(len({asm.group_of(by[e]) for e in ex if by[e]["kind"] == kind}), 2)
        self.assertEqual({i["id"] for i in r["items"] if i["exposed"]}, set(ex))
        open_ids = {i["id"] for i in r["open"]}
        self.assertTrue(set(ex) <= open_ids)
        self.assertEqual(asm.choose_exposed(r["items"]), ex)

    def test_embedded_rows_win_and_must_match(self):
        r = built()
        row = {**r["items"][0], "exposed": True}
        with mock.patch.object(asm, "EXPOSED_A3_TEXT", json.dumps(row, sort_keys=True) + "\n"):
            items, ids = asm.mark_exposed(r["items"])
            self.assertEqual(ids, [row["id"]])
            bad = dict(row, question=row["question"] + " changed")
        with mock.patch.object(asm, "EXPOSED_A3_TEXT", json.dumps(bad) + "\n"):
            with self.assertRaises(asm.AssemblyError):
                asm.mark_exposed(r["items"])

    def test_split_is_deterministic_and_covers_every_item_once(self):
        r = built()
        op, hd = asm.split_extend(r["items"])
        self.assertEqual([i["id"] for i in op], [i["id"] for i in r["open"]])
        self.assertEqual([i["id"] for i in hd], [i["id"] for i in r["hidden"]])
        ids = [i["id"] for i in r["open"]] + [i["id"] for i in r["hidden"]]
        self.assertEqual(sorted(ids), sorted(i["id"] for i in r["items"]))
        self.assertEqual(len(ids), len(set(ids)))

    def test_an_item_and_its_control_are_on_one_side(self):
        r = built()
        side = {i["id"]: "o" for i in r["open"]}
        side.update({i["id"]: "h" for i in r["hidden"]})
        by = {}
        for i in r["items"]:
            by.setdefault(asm.group_of(i), set()).add(side[i["id"]])
        self.assertTrue(all(len(s) == 1 for s in by.values()))
        pairs = {}
        for i in r["items"]:
            if i.get("pair"):
                pairs.setdefault(i["pair"], set()).add(side[i["id"]])
        self.assertTrue(pairs)
        self.assertTrue(all(len(v) == 1 for v in pairs.values()))
        # valid-but-wrong items are grouped by their pair, not by mechanism
        self.assertTrue(all(i.get("pair") for i in r["items"] if i["kind"] == "valid_but_wrong"))
        self.assertEqual(asm.group_of({"pair": "x", "family": "f"}), "pair:x")
        self.assertEqual(asm.group_of({"family": "f"}), "f")

    def test_every_template_with_two_or_more_groups_is_on_both_sides(self):
        r = built()
        groups, sides = {}, {}
        for tag, its in (("o", r["open"]), ("h", r["hidden"])):
            for i in its:
                groups.setdefault(i["template"], set()).add(asm.group_of(i))
                sides.setdefault(i["template"], set()).add(tag)
        for t, g in groups.items():
            if len(g) >= 2:
                self.assertEqual(sides[t], {"o", "h"}, t)
        problems, one = asm.template_sides(r["open"], r["hidden"])
        self.assertEqual(problems, [])
        self.assertEqual({t for t, _, _, _ in one}, {t for t, s_ in sides.items() if len(s_) == 1})
        for t, _, n, g in one:
            self.assertEqual(g, 1, t)  # only a template with a single group may sit on one side

    def test_template_sides_reports_a_one_sided_template(self):
        mk = lambda n, t, f: {"id": "a3-%s-%d" % (t, n), "template": t, "family": f}  # noqa: E731
        op = [mk(1, "t1", "f1"), mk(2, "t1", "f2"), mk(3, "t2", "f3")]
        hd = [mk(4, "t3", "f4")]
        problems, one = asm.template_sides(op, hd)
        self.assertEqual(len(problems), 1)
        self.assertIn("t1", problems[0])
        self.assertEqual(sorted(t for t, _, _, _ in one), ["t1", "t2", "t3"])

    def test_the_seed_labels_and_version_are_those_of_version_2(self):
        self.assertEqual(asm.VERSION, "eval2_items_a3/2")
        self.assertNotEqual(asm.SEED, "m4-a3-1")
        self.assertNotEqual(asm.SPLIT_SEED, asm.OLD_SPLIT_SEED)

    def test_manifest_records_file_hashes_not_a_commit_and_what_it_replaces(self):
        m = asm.manifest_of(built())
        self.assertNotIn("generator_commit", m)
        self.assertEqual(set(m["generator_files_sha256"]), set(asm.GENERATOR_FILES))
        for n, h in m["generator_files_sha256"].items():
            self.assertEqual(h, hashlib.sha256((ROOT / "scripts" / n).read_bytes()).hexdigest())
        self.assertEqual(m["replaces"]["items_sha256"], "d425cdada882b81d1124a6dc49d5d43b03ca7af6326882aa9a67e98eaf036ce3")
        self.assertEqual(m["generator"], "eval2_items_a3/2")

    def test_each_kind_has_enough_items_and_families_on_each_side(self):
        r = built()
        self.assertEqual(asm.side_problems(r["open"], r["hidden"], ALLOW), [])
        for kind in {i["kind"] for i in r["items"]}:
            for its in (r["open"], r["hidden"]):
                k = [i for i in its if i["kind"] == kind]
                self.assertGreaterEqual(len(k), asm.MIN_KIND_ITEMS)
                self.assertGreaterEqual(len({i["family"] for i in k}), asm.MIN_KIND_FAMILIES)

    def test_old_ids_stay_on_their_side_in_the_combined_split(self):
        r = built()
        self.assertEqual(asm.old_sides_hold(r["old_items"], r["open"], r["hidden"]), [])
        moved = [{**i, "family": "moved:" + i["family"]} for i in r["old_items"]]
        self.assertTrue(asm.old_sides_hold(moved, r["open"], r["hidden"]))

    def test_split_extend_on_the_496_alone_is_split_families(self):
        old = built()["old_items"]
        op, hd = asm.split_extend(old, asm.OLD_SPLIT_SEED)
        eo, eh = eval2.split_families(old, asm.OLD_SPLIT_SEED, asm.SHARE)
        self.assertEqual([i["id"] for i in op], [i["id"] for i in eo])
        self.assertEqual([i["id"] for i in hd], [i["id"] for i in eh])

    def test_prefix_extension_keeps_every_earlier_id_on_its_side(self):
        r = built()
        prior = {"open": [i["id"] for i in r["open"]], "hidden": [i["id"] for i in r["hidden"]]}
        template = {"tier": "A", "domain": "d", "question": "q", "exposed": False}
        extra = []
        for n in range(1, 13):  # new items of existing kinds and of the missing kind, new families, one new pair
            kind = ("safe_order", "computed_sizing", "valid_but_wrong", "not_covered")[n % 4]
            extra.append({**template, "id": "a3-extra-%03d" % n, "kind": kind, "family": "extra:%d" % n, "template": "extra",
                          **({"pair": "extra-pair"} if n in (3, 7) else {})})
        both = r["items"] + extra
        op, hd = asm.split_extend(both, prior=prior)
        side = {i["id"]: "o" for i in op}
        side.update({i["id"]: "h" for i in hd})
        for i in r["open"]:
            self.assertEqual(side[i["id"]], "o")
        for i in r["hidden"]:
            self.assertEqual(side[i["id"]], "h")
        self.assertEqual(side["a3-extra-003"], side["a3-extra-007"])
        self.assertEqual(len(side), len(both))
        # deterministic
        op2, hd2 = asm.split_extend(both, prior=prior)
        self.assertEqual([i["id"] for i in op], [i["id"] for i in op2])

    def test_prior_group_on_both_sides_is_refused(self):
        r = built()
        g = next(k for k in {asm.group_of(i) for i in r["items"]} if k.startswith("pair:"))
        pair = [i for i in r["items"] if asm.group_of(i) == g]
        prior = {"open": [pair[0]["id"]], "hidden": [pair[1]["id"]]}
        with self.assertRaises(asm.AssemblyError):
            asm.split_extend(r["items"], prior=prior)


@unittest.skipUnless(HAVE, SKIP)
class OutputsAndPrinting(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.key = self.tmp / "key"
        eval2.keygen(self.key)
        p = mock.patch.dict(os.environ, {"EVAL_HIDDEN_KEY": str(self.key)})
        p.start()
        self.addCleanup(p.stop)
        self.addCleanup(self._tmp.cleanup)

    def test_report_prints_no_item_text_and_no_hidden_list(self):
        r = built()
        text = asm.report(r, ALLOW)
        for it in r["hidden"]:
            self.assertNotIn(it["question"][:60], text)
        for it in r["items"]:
            self.assertNotIn(it["question"][:60], text)
        for iid in r["exposed"]:
            self.assertIn(iid, text)

    def test_the_hashes_are_in_the_report_and_match_the_manifest(self):
        r = built()
        m = asm.manifest_of(r)
        text = asm.report(r, ALLOW)
        for h in (m["items_sha256"], m["outputs"]["open"]["sha256"], m["outputs"]["hidden"]["plaintext_sha256"],
                  m["catalogue_hash_a3"]):
            self.assertIn(h, text)
        self.assertEqual(m["items_sha256"], hashlib.sha256(asm.serialise(r["items"]).encode()).hexdigest())
        self.assertEqual(m["items_total"], len(r["items"]))
        self.assertEqual(m["counts"]["open"] + m["counts"]["hidden"], len(r["items"]))
        self.assertEqual(m["kinds_missing"], MISSING_KINDS)
        self.assertEqual(m["predecessor"]["items_sha256"], json.loads(asm.SPLIT_A1.read_text())["items_sha256"])

    def test_write_to_a_temporary_folder_encrypts_the_hidden_half_and_never_overwrites(self):
        r = built()
        o, h, m = self.tmp / "open.jsonl", self.tmp / "hidden.jsonl.enc", self.tmp / "SPLIT_A3.json"
        man = asm.write_outputs(r, o, h, m, self.key, external_ok=True)
        self.assertEqual(sorted(p.name for p in self.tmp.iterdir() if p.name != "key"), ["SPLIT_A3.json", "hidden.jsonl.enc", "open.jsonl"])
        self.assertTrue(h.read_bytes().startswith(eval2.MAGIC))
        plain = h.read_bytes()
        for it in r["hidden"][:5]:
            self.assertNotIn(it["id"].encode(), plain)
            self.assertNotIn(it["id"].encode(), o.read_bytes())
        self.assertEqual(eval2.decrypt_bytes(plain, self.key).decode(), asm.serialise(r["hidden"]))
        self.assertEqual(o.read_text(), asm.serialise(r["open"]))
        self.assertEqual(json.loads(m.read_text())["outputs"]["hidden"]["sha256"], eval2.sha256_bytes(plain))
        self.assertEqual(man["outputs"]["hidden"]["sha256"], eval2.sha256_bytes(plain))
        with self.assertRaises(asm.AssemblyError):
            asm.write_outputs(r, o, self.tmp / "h2.enc", self.tmp / "m2.json", self.key, external_ok=True)
        self.assertFalse((self.tmp / "h2.enc").exists())  # nothing written when one output exists

    def test_manifest_holds_no_absolute_path_and_outside_paths_are_refused(self):
        r = built()
        o, h, m = self.tmp / "open.jsonl", self.tmp / "hidden.jsonl.enc", self.tmp / "SPLIT_A3.json"
        with self.assertRaises(asm.AssemblyError):
            asm.write_outputs(r, o, h, m, self.key)
        self.assertEqual([p.name for p in self.tmp.iterdir() if p.name != "key"], [])
        asm.write_outputs(r, o, h, m, self.key, external_ok=True)
        for text in (m.read_text(), (ROOT / asm.MANIFEST_PATH).read_text() if (ROOT / asm.MANIFEST_PATH).exists() else ""):
            self.assertNotIn(str(Path.home()), text)
            for v in json.loads(text or "{}").get("outputs", {}).values():
                self.assertFalse(os.path.isabs(v["path"]), v["path"])
        self.assertEqual(asm.repo_path(ROOT / asm.OPEN_PATH), asm.OPEN_PATH)

    def test_nothing_is_written_with_problems(self):
        r = dict(built(), problems=["a floor failed"])
        with self.assertRaises(asm.AssemblyError):
            asm.write_outputs(r, self.tmp / "o", self.tmp / "h.enc", self.tmp / "m", self.key)
        self.assertEqual([p.name for p in self.tmp.iterdir() if p.name != "key"], [])

    def test_write_with_allow_missing_is_refused(self):
        with mock.patch.object(asm, "build", side_effect=AssertionError("must not build")), \
                mock.patch("sys.stderr", new_callable=__import__("io").StringIO):
            rc = asm.main(["--allow-missing", "gap", "--write", "--open-out", str(self.tmp / "o"),
                           "--hidden-out", str(self.tmp / "h.enc"), "--manifest-out", str(self.tmp / "m")])
        self.assertEqual(rc, 2)
        self.assertEqual([p.name for p in self.tmp.iterdir() if p.name != "key"], [])

    def test_dry_run_writes_no_file_in_the_repository(self):
        watch = [asm.SPLIT_A1, asm.OPEN_A1] + [ROOT / p for p in (asm.OPEN_PATH, asm.HIDDEN_PATH, asm.MANIFEST_PATH)]
        before = {str(p): (p.stat().st_mtime_ns if p.exists() else None) for p in watch}
        listing = sorted(str(p) for p in (ROOT / "eval").rglob("*"))
        asm.build(ALLOW, with_predecessor=False)
        self.assertEqual(before, {str(p): (p.stat().st_mtime_ns if p.exists() else None) for p in watch})
        self.assertEqual(listing, sorted(str(p) for p in (ROOT / "eval").rglob("*")))


@unittest.skipUnless(HAVE, SKIP)
class Determinism(unittest.TestCase):
    CODE = ("import sys; sys.path.insert(0, %r); import eval2_items_a3 as a; r = a.build(('gap',), with_predecessor=False); "
            "print(a.sha(a.serialise(r['items'])), a.sha(a.serialise(r['open'])), a.sha(a.serialise(r['hidden'])), r['exposed'])"
            % str(ROOT / "scripts"))

    def run_with(self, seed):
        env = dict(os.environ, PYTHONHASHSEED=seed)
        p = subprocess.run([sys.executable, "-c", self.CODE], capture_output=True, text=True, env=env, cwd=str(ROOT), timeout=300)
        self.assertEqual(p.returncode, 0, p.stderr[-800:])
        return p.stdout

    def test_two_hash_seeds_give_the_same_build_and_split(self):
        a, b = self.run_with("1"), self.run_with("2")
        self.assertEqual(a, b)
        r = built()
        self.assertEqual(a.split()[0], asm.sha(asm.serialise(r["items"])))
        self.assertEqual(a.split()[1], asm.sha(asm.serialise(r["open"])))

    def test_the_frozen_files_still_have_their_hashes(self):
        # the dry run never touches them; their values are the ones recorded before the a3 work
        self.assertEqual(eval2.sha256_file(asm.OPEN_A1), "85244d59b2526abc08755afe57230783d128c9a923b417d0d349bade58fe3dd3")
        self.assertEqual(eval2.sha256_file(asm.SPLIT_A1), "17444ff7b682a6e3b00c8da0d8629262e7f0583db65b561d82fa30ebc5cbe7fd")


if __name__ == "__main__":
    unittest.main()
