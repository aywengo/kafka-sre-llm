"""Construction rules of the tier A1 items (scripts/eval2_items_a1.py), checked on the generated file.

Skipped, with a reason, when eval/m4/build/tier_a1.jsonl or the catalogues have not been built.
Also: nothing under kafka_sre_rag/ may import the scorer.

Slice A2 adds four kinds (write a resource, fix the manifest, write the command, version trap), made by
scripts/eval2_kinds_a2.py: their shares, checks, faults and references are tested here; the scorer's check types
are tested in test_eval2_score.py. The 60 exposed items (20 of the first version, 20 of the second, 20 of the new
kinds) are embedded in the generator: each is kept only if the current catalogues still agree with it, and its
catalogue_hash is then the current one.

The rebalanced mix (plain lookups a minority; near-miss, scope, confusable and value traps each above its
floor), the 20 exposed items of the first version unchanged, 20 further exposed items, balanced verdicts,
the blind-guess score, and that every near-miss name really is absent from the catalogue are checked on the
generated file. Generating the file takes about 20 s (the near-duplicate scan), so it is not redone here;
a determinism test would need that, run the generator twice and compare instead.
"""
import json
import random
import re
import sys
import unittest
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
import eval2_items_a1 as gen  # noqa: E402
import eval2_kinds_a2 as k2  # noqa: E402
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

    def facts(self):
        return gen.facts_view(Catalogues.load())

    def test_exposed_items_survive_only_if_the_catalogues_agree(self):
        emb = gen.EXPOSED_ALL_ROWS()
        self.assertEqual((len(gen.EXPOSED_V1_ROWS()), len(gen.EXPOSED_V2_ROWS()), len(gen.EXPOSED_V3_ROWS())),
                         (gen.EXPOSED_N, gen.EXPOSED_NEW_N, gen.EXPOSED_KINDS_N))
        by_id = {i["id"]: i for i in self.items}
        view = self.facts()
        kept, dropped = [], []
        for row in emb:
            why = gen.exposed_problems(view, row)
            (dropped if why else kept).append(row["id"])
            self.assertEqual(row["id"] in by_id, why is None, (row["id"], why))
        self.assertEqual(sum(i["exposed"] for i in self.items), len(kept))
        self.assertEqual({i["id"] for i in self.items if i["exposed"]}, set(kept))
        self.assertEqual(len(kept) + len(dropped), 60)
        # the one item of the first two versions that the completed catalogues contradict
        self.assertIn("a1-real_flag_nc-002", dropped)

    def test_exposed_items_are_byte_identical_but_for_the_catalogue_hash(self):
        by_id = {i["id"]: i for i in self.items}
        cur = catalogue_hash()
        n = 0
        for row in gen.EXPOSED_ALL_ROWS():
            if row["id"] not in by_id:
                continue
            n += 1
            got = dict(by_id[row["id"]])
            self.assertEqual(got.pop("catalogue_hash"), cur)
            want = dict(row)
            want.pop("catalogue_hash")
            self.assertEqual(got, want, row["id"])
            self.assertTrue(got["exposed"])
        self.assertGreaterEqual(n, 55)

    def test_the_exposed_set_has_all_three_parts(self):
        ids = {i["id"] for i in self.items if i["exposed"]}
        for rows, lo in ((gen.EXPOSED_V1_ROWS(), 19), (gen.EXPOSED_V2_ROWS(), 19), (gen.EXPOSED_V3_ROWS(), 19)):
            self.assertGreaterEqual(sum(r["id"] in ids for r in rows), lo)
        kinds = {r["kind"] for r in gen.EXPOSED_V3_ROWS()}
        self.assertTrue(kinds <= set(gen.NEW_KINDS), kinds)
        self.assertGreaterEqual(len(kinds), 2)

    def test_a_contradicted_exposed_item_is_reported_not_changed(self):
        row = [r for r in gen.EXPOSED_V2_ROWS() if r["id"] == "a1-real_flag_nc-002"][0]
        why = gen.exposed_problems(self.facts(), row)
        self.assertIn("--replicas", why)
        self.assertIn("kafka-topics.sh", why)
        self.assertEqual(row["checks"][1]["expected"], "not in my sources")  # the row itself is as it was

    def test_mix_meets_the_targets(self):
        n = len(self.items)
        cls = Counter(gen.mix_group(i["failure_class"]) for i in self.items)
        self.assertLessEqual(cls["plain_lookup"] / n, gen.MAX_PLAIN)
        for k, lo in gen.MIX_MIN.items():
            self.assertGreaterEqual(cls[k] / n, lo, k)
        self.assertGreaterEqual(n, 400)
        self.assertLessEqual(n, 520)

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
            elif it["template"] in ("cn_choice", "cn_default", "vt_update_b", "vtrap_added_a"):
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

    # ------------------------------------------------------------------ general question rules
    def test_no_question_has_an_immediately_repeated_word(self):
        """Prose only: a manifest in a code block repeats words legitimately (`- Ingress` then `ingress:`)."""
        for it in self.items:
            prose = re.sub(r"```.*?```", " ", it["question"], flags=re.S)
            m = re.search(r"\b(\w+)\s+\1\b", prose, re.I)
            self.assertIsNone(m, (it["id"], m and m.group(0)))

    def test_the_repeated_word_rule_catches_the_old_template_bug(self):
        self.assertTrue(re.search(r"\b(\w+)\s+\1\b", "a Kafka Kafka Streams setting", re.I))
        self.assertEqual(gen.kf("Kafka Streams"), "Kafka Streams")
        self.assertEqual(gen.kf("producer"), "Kafka producer")

    # ------------------------------------------------------------------ the new kinds
    def kind(self, name):
        return [i for i in self.items if i["kind"] == name]

    def test_the_new_kinds_are_at_least_a_third_of_the_target_share(self):
        new = sum(i["kind"] in gen.NEW_KINDS for i in self.items)
        self.assertGreaterEqual(new / len(self.items), gen.NEW_KIND_SHARE)
        for k in gen.NEW_KINDS:
            self.assertGreaterEqual(len(self.kind(k)), 10, k)

    def test_new_items_carry_class_domain_overlap_and_a_source(self):
        for it in self.items:
            if it["kind"] in gen.NEW_KINDS:
                self.assertIn("overlap", it)
                self.assertIn(it["overlap_band"], ("high", "mid", "low", "none"))
                self.assertTrue(it["failure_class"] and it["domain"] and it["family"], it["id"])
                self.assertTrue(it["anchored"], it["id"])

    def test_write_resource_items(self):
        cats = Catalogues.load()
        for it in self.kind("write_resource"):
            checks = {c["type"]: c for c in it["checks"]}
            self.assertEqual(set(checks) - {"yaml_config_keys"},
                             {"yaml_document", "yaml_one_doc", "yaml_strict", "yaml_fields_equal", "yaml_fields_absent"}, it["id"])
            spec_fields = [f for f in checks["yaml_fields_equal"]["fields"] if f["path"][0] == "spec"]
            self.assertTrue(4 <= len(spec_fields) <= 9, (it["id"], len(spec_fields)))
            self.assertTrue(checks["yaml_fields_absent"]["paths"], it["id"])
            self.assertIn("preserves_unknown", it)
            self.assertTrue(all(isinstance(x["preserves_unknown_fields"], bool) for x in it["preserves_unknown"]))
            self.assertIn("strict", it["question"])
            doc = [d for d in sc.answer_yaml_docs(it["reference"])][0]
            self.assertTrue(cats.validate_resource(doc).ok, it["id"])
            self.assertEqual(doc["kind"], checks["yaml_one_doc"]["kind"])
            self.assertEqual(doc["apiVersion"], checks["yaml_one_doc"]["api_version"])
            for c in it["checks"]:
                if c["type"] == "yaml_config_keys":
                    # the free-form config map is named in the question, with the scope of its keys
                    self.assertIn("free-form", it["question"], it["id"])

    def test_write_resource_family_is_kind_plus_top_level_groups(self):
        for it in self.kind("write_resource"):
            tpl, rest = it["family"].split(":", 1)
            self.assertEqual(tpl, it["template"])
            kind, groups = rest.split(":")
            self.assertEqual(kind, it["resource_kind"])
            self.assertEqual(groups.split("+"), sorted(groups.split("+")))

    def test_fix_manifest_items(self):
        cats = Catalogues.load()
        classes = Counter()
        for it in self.kind("fix_manifest"):
            fault = it["fault"]
            classes[fault["class"]] += 1
            faulty = sc.answer_yaml_docs(it["question"])[0]
            fixed = sc.answer_yaml_docs(it["reference"])[0]
            self.assertEqual(sc.answer_yaml_docs(it["perturbed_reference"])[0], faulty)
            v = cats.validate_resource(faulty)
            if fault["class"] == "wrong_scope":
                self.assertTrue(v.ok)  # the schema keeps unknown keys in the free-form map: the key check finds it
                bad = sc.check_yaml_config_keys(
                    [c for c in it["checks"] if c["type"] == "yaml_config_keys"][0], it["question"], cats)
                self.assertFalse(bad[0], it["id"])
            else:
                self.assertFalse(v.ok, it["id"])
            self.assertTrue(cats.validate_resource(fixed).ok, it["id"])
            # exactly one fault: the two documents differ in one leaf value or one leaf place
            a = {(tuple(map(str, p)), repr(x)) for p, x in k2.doc_leaves(faulty)}
            b = {(tuple(map(str, p)), repr(x)) for p, x in k2.doc_leaves(fixed)}
            self.assertEqual(len(a ^ b), 2, (it["id"], sorted(a ^ b)))
            # the injected fault is gone from the reference and the check list names it
            if fault["class"] in ("near_miss_name", "wrong_nesting", "wrong_scope"):
                self.assertTrue(sc.path_get(faulty, fault["faulty_path"])[0], it["id"])
                self.assertFalse(sc.path_get(fixed, fault["faulty_path"])[0], it["id"])
                self.assertIn(fault["faulty_path"], [c for ch in it["checks"] if ch["type"] == "yaml_fields_absent" for c in ch["paths"]])
            self.assertTrue(sc.path_get(fixed, fault["original_path"])[0], it["id"])
            fields = [c for c in it["checks"] if c["type"] == "yaml_fields_equal"][0]["fields"]
            self.assertTrue(any(f["path"] == fault["original_path"] or f["path"][:len(fault["original_path"])] == fault["original_path"]
                                for f in fields), it["id"])
        self.assertEqual(set(classes), {"near_miss_name", "value_type_trap", "wrong_nesting", "wrong_api_version", "wrong_scope"})
        self.assertTrue(all(n >= 4 for n in classes.values()), classes)

    def test_fix_manifest_near_miss_name_is_not_a_sibling_of_the_real_field(self):
        cats = Catalogues.load()
        for it in self.kind("fix_manifest"):
            f = it["fault"]
            if f["class"] == "near_miss_name":
                sch = k2.Schema(cats, it["resource_kind"])
                faulty = sc.answer_yaml_docs(it["question"])[0]
                parent = f["faulty_path"][:-1]
                self.assertNotIn(f["faulty_path"][-1], sch.children(parent), it["id"])
                self.assertIn(f["original_path"][-1], sch.children(parent), it["id"])

    def test_fix_manifest_wrong_api_version_is_not_served(self):
        cats = Catalogues.load()
        for it in self.kind("fix_manifest"):
            if it["fault"]["class"] == "wrong_api_version":
                faulty = sc.answer_yaml_docs(it["question"])[0]
                self.assertEqual(cats.validate_resource(faulty).codes(), ["unknown_api_version"], it["id"])

    def test_write_command_items(self):
        cats = Catalogues.load()
        for it in self.kind("write_command"):
            spec = it["checks"][1]
            tool = spec["tool"]
            self.assertEqual(cats.cli_tool(tool).complete if hasattr(cats.cli_tool(tool), "complete") else True, True)
            ent = [e for e in cats.data["kafka_cli"]["entries"] if e["tool"] == tool][0]
            self.assertTrue(ent["complete"], it["id"])
            for f in list(spec["required_flags"]) + spec["forbidden_flags"]:
                self.assertTrue(cats.cli_option(tool, f).known, (it["id"], f))
            self.assertFalse(set(spec["required_flags"]) & set(spec["forbidden_flags"]), it["id"])
            self.assertEqual(sc.constraint_problems(tool, list(spec["required_flags"]), cats), [], it["id"])
            self.assertTrue(it["sources"] and all(s["catalogue"] == "kafka_cli" for s in it["sources"]), it["id"])
            self.assertIn("bin/" + tool, it["reference"])

    def test_write_command_dry_run_tasks_forbid_execute(self):
        dry = [i for i in self.kind("write_command") if "--dry-run" in i["checks"][1]["required_flags"]]
        self.assertTrue(dry)
        for it in dry:
            self.assertIn("--execute", it["checks"][1]["forbidden_flags"])

    def test_version_trap_items(self):
        cats = Catalogues.load()
        hist = {(e["scope"], e["key"]): e for e in cats.data["kafka_config_history"]["entries"]}
        for it in self.kind("version_trap"):
            acc = it["checks"][1]["accepted"]
            forb = it["checks"][2]
            self.assertEqual(forb["type"], "old_value_forbidden")
            self.assertFalse(set(map(str.lower, acc)) & set(map(str.lower, forb["forbidden"])), it["id"])
            scope, key = it["sources"][0]["entry"].split("/", 1)
            self.assertIn((scope, key), hist, it["id"])
            self.assertEqual(it["versions"], {"kafka": "4.3", "compared_with": "4.1"})
            self.assertEqual(it["failure_class"], "version")
        added = [i for i in self.kind("version_trap") if i["template"] == "vtrap_added_a"]
        self.assertGreaterEqual(len({i["checks"][1]["accepted"][0] for i in added}), 2)  # both 4.2 and 4.3 occur

    def test_not_in_my_sources_items_come_from_what_is_still_incomplete(self):
        cats = Catalogues.load()
        n = Counter()
        for it in self.items:
            if it["kind"] != "is_real" or it["checks"][1]["expected"] != "not in my sources" or it["id"] in {
                    r["id"] for r in gen.EXPOSED_V1_ROWS()}:
                continue
            s = it["sources"][0]
            n[it["template"]] += 1
            if s["catalogue"] == "kafka_cli":
                ent = [e for e in cats.data["kafka_cli"]["entries"] if e["tool"] == s["entry"]][0]
                self.assertIs(ent["complete"], False, it["id"])
                self.assertEqual(cats.cli_option(s["entry"], s["absent"]).status, "not_covered", it["id"])
            elif s["catalogue"] == "kubernetes":
                self.assertEqual(cats.cli_option(s["entry"], s["absent"]).status, "not_covered", it["id"])
            elif s["catalogue"] == "strimzi_annotations":
                self.assertEqual(cats.annotation(s["absent"]).status, "not_covered", it["id"])
            elif s["catalogue"] == "strimzi_resources":
                self.assertEqual(cats.operator_label(s["absent"]).status, "not_covered", it["id"])
            else:
                self.fail((it["id"], s["catalogue"]))
        self.assertGreaterEqual(len(n), 3, n)

    def test_made_up_flags_of_complete_tools_are_unknown_in_the_catalogue(self):
        cats = Catalogues.load()
        got = 0
        for it in self.items:
            if it["template"] == "real_flag" and it["checks"][1]["expected"] == "does not exist":
                s = it["sources"][0]
                self.assertEqual(cats.cli_option(s["entry"], s["absent"]).status, "unknown", it["id"])
                got += 1
        self.assertGreaterEqual(got, 4)

    def test_every_new_item_is_deterministic_in_its_draft(self):
        cats = Catalogues.load()
        for which in ("topic", "user", "connector", "argo", "kafka"):
            a = k2.drafts_write(None, random.Random("t|" + which), cats, which, "a", n_try=3)
            b = k2.drafts_write(None, random.Random("t|" + which), cats, which, "a", n_try=3)
            self.assertEqual([d["core"] for d in a], [d["core"] for d in b])
            self.assertEqual([d["refs"] for d in a], [d["refs"] for d in b])

    def test_baselines_of_the_new_kinds_fail(self):
        """A blind answer (skeleton manifest, the faulty manifest as given, the bare tool) must not pass."""
        cats = Catalogues.load()
        for which in ("topic", "deploy"):
            for d in k2.drafts_write(None, random.Random("b|" + which), cats, which, "a", n_try=3):
                res = sc.run_checks({"id": "x", "checks": d["checks"]}, d["baseline"], cats)
                self.assertFalse(all(r["pass"] for r in res))
        st = Counter()
        for d in k2.drafts_cmd(None, random.Random("c"), cats, st)[:10]:
            res = sc.run_checks({"id": "x", "checks": d["checks"]}, d["baseline"], cats)
            self.assertFalse(all(r["pass"] for r in res))


class KindHelperTests(unittest.TestCase):
    """Helpers of scripts/eval2_kinds_a2.py on the real catalogues (no generation of the whole set)."""

    @classmethod
    def setUpClass(cls):
        cls.cats = Catalogues.load() if HAVE else None

    def setUp(self):
        if not HAVE:
            self.skipTest("catalogues not built")

    def test_pick_value_respects_type_and_range(self):
        rng = random.Random(1)
        ent = [e for e in self.cats.data["kafka_config"]["entries"] if e["scope"] == "topic" and e["key"] == "segment.bytes"][0]
        for _ in range(20):
            v = k2.pick_value(rng, ent)
            self.assertGreaterEqual(v, 1048576)  # the stated range is [1048576,...]
        ent = [e for e in self.cats.data["kafka_config"]["entries"] if e["scope"] == "topic" and e["key"] == "preallocate"][0]
        self.assertIsNot(k2.pick_value(rng, ent), None)
        self.assertEqual(k2.pick_value(rng, {"type": "string", "default_value": "x", "key": "k"}), None)

    def test_config_pool_skips_keys_strimzi_does_not_take(self):
        keys = {e["key"] for e in k2.config_pool(self.cats, "broker")}
        self.assertNotIn("listeners", keys)
        self.assertFalse([k for k in keys if k.startswith(("ssl.", "sasl.", "advertised."))])
        self.assertTrue(keys)

    def test_schema_children_and_open_maps(self):
        sch = k2.Schema(self.cats, "KafkaTopic")
        self.assertEqual(sch.children(["spec"]), ["config", "partitions", "replicas", "topicName"])
        self.assertTrue(sch.open_at(["spec", "config", "retention.ms"]))
        self.assertFalse(sch.open_at(["spec", "partitions"]))
        self.assertTrue(sch.preserves(["spec", "config"]))
        self.assertFalse(sch.preserves(["spec"]))
        self.assertEqual(sch.api, "kafka.strimzi.io/v1")
        self.assertEqual(k2.sp(["spec", "acls", 0, "host"]), "spec.acls[].host")

    def test_each_fault_class_breaks_a_valid_resource(self):
        rng = random.Random(4)
        pairs = k2.scope_pairs(self.cats)
        self.assertTrue(pairs)
        for key, fn in (("near", k2.fault_near_miss), ("enum", k2.fault_enum), ("nest", k2.fault_nest), ("api", k2.fault_api)):
            made = 0
            for _ in range(30):
                r = k2.b_topic(None, rng, self.cats) if key != "enum" else k2.b_user(None, rng, self.cats)
                res = fn(rng, r)
                if res:
                    faulty, fault = res
                    self.assertFalse(self.cats.validate_resource(faulty).ok, (key, fault))
                    self.assertTrue(self.cats.validate_resource(r.doc).ok)
                    made += 1
            self.assertGreater(made, 0, key)

    def test_scope_pairs_are_topic_keys_with_a_log_prefixed_broker_form(self):
        for t, b in k2.scope_pairs(self.cats):
            self.assertEqual(b, "log." + t)
            self.assertEqual(self.cats.config_key(t, "broker").status, "unknown")
            self.assertEqual(self.cats.config_key(b, "topic").status, "unknown")
            self.assertTrue(self.cats.config_key(b, "broker").known and self.cats.config_key(t, "topic").known)

    def test_derivations_are_read_from_the_catalogue(self):
        ok, src = k2.derive_check(self.cats, "kafka-consumer-groups.sh", ("avail", "--members", ["--describe"]))
        self.assertTrue(ok)
        self.assertIn("describe", src["fact"])
        self.assertTrue(src["file"].endswith("ConsumerGroupCommandOptions.java"))
        ok, why = k2.derive_check(self.cats, "kafka-consumer-groups.sh", ("avail", "--members", ["--list"]))
        self.assertFalse(ok)
        ok, _ = k2.derive_check(self.cats, "kafka-leader-election.sh", ("desc", "--partition", "REQUIRED if --topic"))
        self.assertTrue(ok)
        ok, _ = k2.derive_check(self.cats, "kafka-leader-election.sh", ("desc", "--partition", "text that is not there"))
        self.assertFalse(ok)
        ok, why = k2.derive_check(self.cats, "kafka-topics.sh", ("option", "--no-such-flag"))
        self.assertFalse(ok)

    def test_a_task_whose_flag_the_catalogue_does_not_know_is_left_out(self):
        bad = k2.S("kafka-topics.sh", "do something", {"--describe": None, "--no-such": None}, [], [], [{}])
        saved = list(k2.CMD_SPECS)
        try:
            k2.CMD_SPECS[:] = [bad]
            st = Counter()
            self.assertEqual(k2.drafts_cmd(None, random.Random(1), self.cats, st), [])
            self.assertTrue(any(k.startswith("flag_unknown") for k in st), st)
        finally:
            k2.CMD_SPECS[:] = saved

    def test_flag_edits_give_flags_that_are_not_the_original(self):
        for f in ("--partitions", "--under-replicated-partitions", "--replication-factor"):
            ed = gen.flag_edits(f)
            self.assertTrue(ed)
            self.assertNotIn(f, [x for x, _ in ed])


if __name__ == "__main__":
    unittest.main()
