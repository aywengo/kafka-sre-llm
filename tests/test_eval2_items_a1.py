"""Construction rules of the tier A1 items (scripts/eval2_items_a1.py), checked on the generated file.

Skipped, with a reason, when eval/m4/build/tier_a1.jsonl or the catalogues have not been built.
Also: nothing under kafka_sre_rag/ may import the scorer.

The rebalanced mix (plain lookups a minority; near-miss, scope, confusable and value traps each above its
floor), the 20 exposed items of the first version unchanged, 20 further exposed items, balanced verdicts,
the blind-guess score, and that every near-miss name really is absent from the catalogue are checked on the
generated file. Generating the file takes about 20 s (the near-duplicate scan), so it is not redone here;
a determinism test would need that, run the generator twice and compare instead.
"""
import json
import sys
import unittest
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
import eval2_items_a1 as gen  # noqa: E402
import eval2_score as sc  # noqa: E402
from kafka_sre_rag.catalogue import Catalogues, catalogue_hash, default_dir  # noqa: E402

ITEMS = ROOT / "eval" / "m4" / "build" / "tier_a1.jsonl"
HAVE = ITEMS.is_file() and (default_dir() / "CATALOGUES.json").is_file()


class NoImportTests(unittest.TestCase):
    def test_runtime_does_not_import_the_scorer(self):
        for f in (ROOT / "kafka_sre_rag").glob("*.py"):
            self.assertNotIn("eval2_score", f.read_text(), f.name)

    def test_scorer_imports_nothing_from_the_runtime_but_the_catalogue(self):
        text = (ROOT / "scripts" / "eval2_score.py").read_text()
        imports = [l for l in text.splitlines() if l.startswith(("import ", "from "))]
        for l in imports:
            if "kafka_sre_rag" in l:
                self.assertEqual(l.split()[1], "kafka_sre_rag.catalogue", l)
        self.assertFalse(any("evalhooks" in l or "eval2 " in l or "checker" in l for l in imports))


class HelperTests(unittest.TestCase):
    def test_overlap_and_band(self):
        ov = gen.overlap("default value of max.poll.records", "The maximum number of records returned in poll")
        self.assertEqual(set(ov), {"q_in_desc", "desc_in_q"})
        self.assertIsNone(gen.overlap("x y z", None))
        self.assertEqual(gen.band(None), "none")
        self.assertEqual(gen.band({"q_in_desc": 0.6, "desc_in_q": 0.1}), "high")
        self.assertEqual(gen.band({"q_in_desc": 0.3, "desc_in_q": 0.1}), "mid")
        self.assertEqual(gen.band({"q_in_desc": 0.1, "desc_in_q": 0.1}), "low")

    def test_near_duplicate_rule(self):
        corpus = [("a.jsonl", gen.tokens("Which broker setting controls the log retention time in hours for old segments"))]
        hit, d = gen.near_duplicate("Which broker setting controls the log retention time in hours for old segments?", corpus)
        self.assertTrue(hit)
        hit, d = gen.near_duplicate("What is the default of group.initial.rebalance.delay.ms for the broker?", corpus)
        self.assertFalse(hit)

    def test_excerpt(self):
        self.assertIsNone(gen.excerpt("Short."))
        self.assertTrue(gen.excerpt("This is a first sentence of some length. And this is a second one that follows it.", 30, 80))


class EditTests(unittest.TestCase):
    """The controlled edits, on a small made-up catalogue (no files needed)."""

    class FakeGen:
        vocab = {"log", "retention", "ms", "bytes", "partitions", "partition", "num", "io", "threads", "check", "interval"}
        first_segs = ["log", "logs", "num"]

    def test_edits_cover_the_requested_kinds(self):
        g = self.FakeGen()
        tags = {t for _, t, _ in gen.key_edits(g, "log.retention.check.interval.ms")}
        self.assertTrue({"word", "order", "unit", "drop", "prefix"} <= tags, tags)
        tags = {t for _, t, _ in gen.key_edits(g, "num.partitions")}
        self.assertIn("plural", tags)
        fakes = dict((f, t) for f, t, _ in gen.key_edits(g, "log.retention.bytes"))
        self.assertEqual(fakes.get("log.retention.mb"), "unit")
        self.assertEqual(fakes.get("logs.retention.bytes"), "prefix")

    def test_every_edit_differs_from_the_key(self):
        for key in ("log.retention.ms", "num.io.threads", "group.id"):
            for fake, tag, how in gen.key_edits(self.FakeGen(), key):
                self.assertNotEqual(fake, key)
                self.assertTrue(how)

    def test_error_name_swaps_replace_whole_words_only(self):
        class G:
            err_names = {"UNSUPPORTED_ASSIGNOR"}

            class cats:
                @staticmethod
                def error_name(n):
                    class R:
                        status = "unknown"
                    return R
        fakes = [f for f, _ in gen.err_fakes(G(), "UNSUPPORTED_ASSIGNOR")]
        self.assertFalse(any("UNSUPPANDTED" in f for f in fakes), fakes)
        self.assertNotIn("UNSUPPORTED_ASSIGNOR", fakes)

    def test_canon_and_guess_helpers(self):
        self.assertEqual(gen.canon([{"type": "answer_letter", "expected": "B"}]), "B")
        self.assertEqual(gen.canon([{"type": "answer_set", "accepted": ["b", "A"]}]), "a|b")

    def test_default_buckets(self):
        self.assertEqual([gen.default_bucket(v) for v in ("null", "-1", "0", '""', "9223372036854775807", "30000")],
                         ["null", "-1", "0", "empty", "huge", None])


@unittest.skipUnless(HAVE, "items not generated: run scripts/build_catalogues.py then scripts/eval2_items_a1.py")
class GeneratedFileTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.items = [json.loads(l) for l in ITEMS.read_text().splitlines() if l.strip()]

    def test_fields(self):
        need = {"id", "tier", "kind", "family", "domain", "question", "checks", "reference", "sources", "versions",
                "template", "anchored", "failure_class", "exposed", "overlap", "catalogue_hash", "seed"}
        for it in self.items:
            self.assertTrue(need <= set(it), it["id"])
            self.assertEqual(it["tier"], "A")
            self.assertTrue(it["sources"], it["id"])
            for s in it["sources"]:
                self.assertTrue(s.get("file") and s.get("catalogue"), (it["id"], s))

    def test_ids_and_questions_unique(self):
        self.assertEqual(len({i["id"] for i in self.items}), len(self.items))
        self.assertEqual(len({i["question"] for i in self.items}), len(self.items))

    def test_at_most_ten_per_template_and_is_real_share(self):
        self.assertLessEqual(max(Counter(i["template"] for i in self.items).values()), gen.MAX_PER_TEMPLATE)
        real = sum(i["kind"] == "is_real" for i in self.items)
        self.assertLessEqual(real, 0.10 * len(self.items))

    def test_family_is_template_plus_group(self):
        for it in self.items:
            self.assertTrue(it["family"].startswith(it["template"] + ":"), it["id"])

    def test_catalogue_hash_is_the_current_one(self):
        self.assertEqual({i["catalogue_hash"] for i in self.items}, {catalogue_hash()})

    def test_exposed_count(self):
        self.assertEqual(sum(i["exposed"] for i in self.items), gen.EXPOSED_N + gen.EXPOSED_NEW_N)

    def test_the_first_twenty_exposed_items_are_unchanged(self):
        by_id = {i["id"]: i for i in self.items}
        old = gen.EXPOSED_V1_ROWS()
        self.assertEqual(len(old), gen.EXPOSED_N)
        for row in old:
            self.assertTrue(row["exposed"])
            self.assertEqual(by_id[row["id"]], row, row["id"])

    def test_the_new_exposed_items_are_not_the_old_ones(self):
        old = {r["id"] for r in gen.EXPOSED_V1_ROWS()}
        new = [i for i in self.items if i["exposed"] and i["id"] not in old]
        self.assertEqual(len(new), gen.EXPOSED_NEW_N)

    def test_mix_meets_the_targets(self):
        n = len(self.items)
        cls = Counter(gen.mix_group(i["failure_class"]) for i in self.items)
        self.assertLessEqual(cls["plain_lookup"] / n, gen.MAX_PLAIN)
        for k, lo in gen.MIX_MIN.items():
            self.assertGreaterEqual(cls[k] / n, lo, k)
        self.assertGreaterEqual(n, 280)
        self.assertLessEqual(n, 340)

    def test_argo_items_need_several_flags_often_enough(self):
        argo = [i for i in self.items if i["kind"] == "argocd_command"]
        multi = [i for i in argo if len(i["checks"][1]["required_flags"]) >= 2]
        self.assertGreaterEqual(3 * len(multi), len(argo))

    def test_is_real_is_three_way_and_balanced(self):
        real = [i for i in self.items if i["kind"] == "is_real"]
        v = Counter(i["checks"][1]["expected"] for i in real)
        self.assertEqual(set(v), {"exists", "does not exist", "not in my sources"})
        self.assertLessEqual(max(v.values()) - min(v.values()), 2, v)

    def test_error_items_have_more_than_two_outcomes(self):
        self.assertNotIn("err_retriable", {i["template"] for i in self.items if not i["exposed"]})
        for i in self.items:
            if i["template"] in ("err_cond_mc", "err_which_a", "err_which_b"):
                self.assertEqual(i["checks"][1]["type"], "answer_letter")
                self.assertEqual(i["question"].count(") `"), 4)

    def test_near_miss_names_are_absent_from_every_scope(self):
        cats = Catalogues.load()
        keys = {e["key"] for e in cats.data["kafka_config"]["entries"]}
        names = {e["name"] for e in cats.data["protocol_errors"]["entries"]}
        n = 0
        for it in self.items:
            for s in it["sources"]:
                if "absent" in s and s["catalogue"] == "kafka_config" and "edit" in s:
                    n += 1
                    self.assertNotIn(s["absent"], keys, it["id"])
                if "absent" in s and s["catalogue"] == "protocol_errors" and "edit" in s:
                    n += 1
                    self.assertNotIn(s["absent"], names, it["id"])
        self.assertGreater(n, 40)

    def test_near_miss_name_items_ask_for_the_real_neighbour(self):
        for it in self.items:
            if it["template"].startswith("ns_nm_"):
                real = it["checks"][1]["accepted"][0]
                self.assertNotIn("`%s`" % real, it["question"], it["id"])
                fake = [s["absent"] for s in it["sources"] if "absent" in s][0]
                self.assertIn(fake, it["question"], it["id"])
                self.assertNotEqual(fake, real)

    def test_enumerated_values_items_offer_an_invalid_value(self):
        for it in self.items:
            if it["template"].startswith("vt_values_"):
                valid = set(it["checks"][1]["accepted"])
                self.assertGreaterEqual(len(valid), 2)
                perturbed = set(x.strip() for x in it["perturbed_reference"].split("ANSWER:")[-1].split(","))
                self.assertGreater(len(perturbed - valid), 0, it["id"])

    def test_scope_dependent_items_name_a_key_with_different_defaults(self):
        cats = Catalogues.load()
        by = {}
        for e in cats.data["kafka_config"]["entries"]:
            by.setdefault(e["key"], set()).add(e["default_value"])
        n = 0
        for it in self.items:
            if it["template"].startswith("sd_default_"):
                n += 1
                key = it["sources"][0]["entry"].split("/", 1)[1]
                self.assertGreater(len(by[key]), 1, it["id"])
        self.assertGreaterEqual(n, 20)

    def test_confusable_items_name_neighbours_and_require_one_key(self):
        for it in self.items:
            if it["template"] in ("cn_open", "cn_choice", "cn_default"):
                self.assertGreaterEqual(len(it["sources"]), 2, it["id"])
                self.assertEqual(it["checks"][1]["type"], "answer_equals")
                self.assertEqual(len(it["checks"][1]["accepted"]), 1)

    def test_blind_guess_stays_under_the_limit(self):
        # rebuilds only the bookkeeping the table needs, not the items: shares of the commonest answer per template
        km = gen.kept_modal_shares(self.items)
        from collections import defaultdict

        class G:
            tpl_guess, pool_modal, chances = {}, {}, {}
        # options per template are known from the question: use the number of lettered or listed options
        total = 0.0
        for it in self.items:
            ch = 0.0
            if it["kind"] == "is_real":
                ch = max(1 / 3.0, km["is_real"])
            elif it["checks"][1]["type"] == "answer_letter":
                ch = 0.25
            elif it["template"] in ("cn_choice", "cn_default", "vt_update_b"):
                ch = 1 / 3.0
            elif it["template"] in ("ns_scope_a", "ns_scope_b"):
                ch = 0.1
            elif it["template"] in ("vt_update_a", "vt_default_a", "vt_default_b"):
                ch = km[it["template"]]
            total += ch
        self.assertLess(total / len(self.items), gen.MAX_GUESS)

    def test_references_pass_and_perturbed_references_fail(self):
        cats = Catalogues.load()
        for it in self.items:
            res = sc.run_checks(it, it["reference"], cats)
            self.assertTrue(all(r["pass"] for r in res), (it["id"], res))
            pres = sc.run_checks(it, it["perturbed_reference"], cats)
            self.assertFalse(all(r["pass"] for r in pres), it["id"])
            self.assertFalse(pres[it["perturbed_violates"]]["pass"], it["id"])

    def test_no_kafka_kafka_in_new_questions(self):
        old = {r["id"] for r in gen.EXPOSED_V1_ROWS()}
        for it in self.items:
            if it["id"] not in old:
                self.assertNotIn("Kafka Kafka", it["question"], it["id"])

    def test_every_item_ends_with_an_answer_line_or_asks_for_a_command(self):
        for it in self.items:
            fmt = [c for c in it["checks"] if c.get("format")]
            self.assertEqual(len(fmt), 1, it["id"])
            if fmt[0]["type"] == "answer_line":
                self.assertIn("ANSWER:", it["question"], it["id"])

    def test_no_item_names_its_answer_key_in_a_description_question(self):
        for it in self.items:
            if it["template"].startswith("ns_key_desc"):
                key = it["checks"][1]["accepted"][0]
                self.assertNotIn(key, it["question"], it["id"])

    def test_not_covered_items_are_not_anchored(self):
        for it in self.items:
            if it["failure_class"] == "not_covered":
                self.assertFalse(it["anchored"])
                self.assertEqual(it["checks"][1]["expected"], "not in my sources")

    def test_near_duplicates_of_the_corpus_are_absent(self):
        corpus, _ = gen.corpus_texts()
        for it in self.items:
            core = it["question"].split("\n\n")[0] if it["kind"] != "error_name" or "A)" not in it["question"] else it["question"]
            hit, d = gen.near_duplicate(core, corpus)
            self.assertFalse(hit, (it["id"], d))


if __name__ == "__main__":
    unittest.main()
