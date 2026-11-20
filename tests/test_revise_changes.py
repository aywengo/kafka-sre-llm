"""WP2b T2b: the acceptance rule (wrapped command as one unit, orphan lines, indentation, YAML, repeated names), the
finding's own line in a wrapped command, the "nearest real" filter and the revise prompt. Synthetic answers only; no
text of any evaluation item. Offline, no model."""
import json
import re
import shutil
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))
from kafka_sre_rag import prompts, revise as rv, service  # noqa: E402
from kafka_sre_rag.checker import Checker  # noqa: E402
from test_ask_check import CHUNKS, make_catalogue_dir  # noqa: E402
from test_revise import Model, reply  # noqa: E402
from test_checker import CATS  # noqa: E402

TOOL = "kafka-streams-application-reset.sh"
WRAPPED = "Run it:\n```bash\n%s --application-id a \\\n  --bootstrap-server b:9092 \\\n  --execute\n```\nDone." % TOOL
# lines: 1 prose, 2 fence open, 3 tool line, 4 bootstrap, 5 --execute (flagged), 6 fence close


class Cat(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dir = make_catalogue_dir()
        cls.ck = service.AnswerChecker(cls.dir)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.dir, ignore_errors=True)

    def rec(self, text, *replies):
        return rv.Reviser(self.ck.check, Model(*replies)).revise(text, CHUNKS, r2=True)


class TestOwnLine(unittest.TestCase):
    def test_flag_in_a_wrapped_command_reports_its_own_line_and_all_lines_are_listed(self):
        res = Checker(CATS).check(WRAPPED)
        f = [f for f in res.findings if f.item.text == "--execute"]
        self.assertEqual(len(f), 1)
        self.assertEqual(f[0].item.line, 5)
        self.assertEqual(f[0].to_dict()["line"], 5)
        self.assertEqual(f[0].to_dict()["lines"], [5])

    def test_one_line_command_keeps_its_line(self):
        res = Checker(CATS).check("```bash\n%s --application-id a --execute\n```" % TOOL)
        self.assertEqual([f.item.line for f in res.findings], [2])

    def test_same_flag_on_two_lines_of_one_command_takes_both(self):
        text = "```bash\n%s --application-id a \\\n  --execute \\\n  --execute\n```" % TOOL
        f = Checker(CATS).check(text).findings[0]
        self.assertEqual(f.item.lines, [3, 4])

    def test_repeated_name_in_other_commands_lists_every_line(self):
        text = "```bash\n%s --execute\n```\nand\n```bash\n%s --execute\n```" % (TOOL, TOOL)
        f = Checker(CATS).check(text).findings[0]
        self.assertEqual(f.to_dict()["lines"], [2, 6])
        self.assertEqual(f.to_dict()["line"], 2)

    def test_findings_are_otherwise_unchanged(self):
        d = Checker(CATS).check(WRAPPED).findings[0].to_dict()
        for k in ("level", "text", "type", "rule", "evidence", "suggestion", "catalogue", "sentence"):
            self.assertIn(k, d)


class TestUnitAndMarks(unittest.TestCase):
    def test_finding_on_any_line_allows_the_whole_command(self):
        for n in (3, 4, 5):
            self.assertEqual(rv.allowed_lines(WRAPPED, [{"level": "invented", "line": n, "text": "x", "type": "cli_flag"}]),
                             {3, 4, 5})

    def test_every_occurrence_line_is_allowed(self):
        text = "a `--x` b\nc\nd `--x` e"
        f = {"level": "invented", "line": 1, "lines": [1, 3], "text": "--x", "type": "cli_flag"}
        self.assertEqual(rv.allowed_lines(text, [f]), {1, 3})

    def test_r1_marks_every_occurrence_in_prose(self):
        text = "Use `--x` here.\nNothing.\nAnd `--x` again."
        f = {"level": "invented", "line": 1, "lines": [1, 3], "text": "--x", "type": "cli_flag", "sentence": "Use `--x` here.",
             "evidence": "not real", "suggestion": ""}
        out = rv.mark_in_place(text, [f]).split("\n")
        self.assertIn("[not confirmed:", out[0])
        self.assertIn("[not confirmed:", out[2])

    def test_r1_marks_a_repeat_inside_one_wrapped_command_once(self):
        text = "```bash\nx \\\n  --a \\\n  --a\n```"
        f = {"level": "invented", "line": 3, "lines": [3, 4], "text": "--a", "type": "cli_flag", "evidence": "no", "suggestion": ""}
        self.assertEqual(rv.mark_in_place(text, [f]).count("not confirmed"), 1)

    def test_r1_mark_for_the_middle_line_of_a_command_goes_after_the_command(self):
        res = Checker(CATS).check(WRAPPED.replace("--execute", "--bogus \\\n  --also"))
        text = WRAPPED.replace("--execute", "--bogus \\\n  --also")
        out = rv.mark_in_place(text, [f.to_dict() for f in res.findings]).split("\n")
        self.assertTrue(out[-3].lstrip().startswith("# not confirmed") or out[-4].lstrip().startswith("# not confirmed"))


class TestAcceptance(Cat):
    def test_flag_on_a_later_line_of_the_command_can_be_fixed(self):
        # the old rule allowed only the named line plus one neighbour; --execute is the last line of the command
        r = self.rec(WRAPPED, reply((5, "  --dry-run")))
        self.assertNotIn("not named", " ".join(r["r2"]["reasons"]))

    def test_finding_on_a_middle_line_allows_changing_the_first_line_too(self):
        text = "```sh\n%s --application-id a \\\n  --execute\n```" % TOOL
        r = self.rec(text, reply((2, "%s --application-id b \\" % TOOL), (3, "  --dry-run")))
        self.assertEqual(r["r2"]["status"], "accepted", r["r2"])

    def test_orphan_continuation_line_is_rejected(self):
        r = self.rec(WRAPPED, reply((3, None)))
        self.assertEqual(r["r2"]["status"], "rejected")
        self.assertTrue(any("first line of the command was removed" in x for x in r["r2"]["reasons"]), r["r2"])
        self.assertEqual(r["final_text"], rv.mark_in_place(WRAPPED, r["check_first"]["findings"]))

    def test_dangling_backslash_is_rejected(self):
        why = rv.structure_problems(WRAPPED, [{"line": 5, "text": None}])
        self.assertTrue(any("dangling backslash" in w for w in why), why)
        why = rv.structure_problems(WRAPPED, [{"line": 5, "text": "  --dry-run \\"}])
        self.assertTrue(any("dangling backslash" in w for w in why), why)

    def test_removing_the_whole_command_is_not_an_orphan(self):
        why = rv.structure_problems(WRAPPED, [{"line": n, "text": None} for n in (3, 4, 5)])
        self.assertEqual(why, [])

    def test_change_of_leading_whitespace_in_a_fence_is_rejected(self):
        why = rv.structure_problems(WRAPPED, [{"line": 5, "text": "   --dry-run"}])
        self.assertEqual(why, ["line 5: the leading whitespace changed"])
        self.assertEqual(rv.structure_problems(WRAPPED, [{"line": 5, "text": "  --dry-run"}]), [])
        r = self.rec(WRAPPED, reply((5, "\t--dry-run")))
        self.assertEqual(r["r2"]["status"], "rejected")

    def test_whitespace_in_prose_is_not_judged(self):
        self.assertEqual(rv.structure_problems("a\nb", [{"line": 2, "text": "  c"}]), [])

    def test_yaml_that_parsed_and_no_longer_does_is_rejected(self):
        old = "```yaml\nkafka:\n  version: 4\n  x: 1\n```"
        bad = "```yaml\nkafka: \\\n  version: 4\n  x: 1\n```"
        self.assertEqual(rv.yaml_regressions(old, bad), ["YAML block 1 parsed before the patch and does not now"])
        self.assertEqual(rv.yaml_regressions(bad, old), [])           # it did not parse before: not held against the patch
        self.assertEqual(rv.yaml_regressions(old, old), [])

    def test_yaml_regression_rejects_through_the_reviser(self):
        text = "```yaml\nspec:\n  a: 1\n  quota: 5\n```"
        f = {"level": "invented", "line": 4, "lines": [4], "text": "quota", "type": "config_key", "evidence": "no", "suggestion": ""}
        new = rv.apply_patches(text, [{"line": 4, "text": "  quota: [5"}])
        self.assertTrue(rv.yaml_regressions(text, new))
        self.assertTrue(f)

    def test_every_line_with_the_same_flag_may_change(self):
        text = "```bash\n%s --execute\n```\nand\n```bash\n%s --execute\n```" % (TOOL, TOOL)
        r = self.rec(text, reply((2, "%s --dry-run" % TOOL), (6, "%s --dry-run" % TOOL)))
        self.assertEqual(r["r2"]["status"], "accepted", r["r2"])

    def test_patch_that_fixes_one_of_two_occurrences_is_rejected_and_marks_stay(self):
        text = "```bash\n%s --execute\n```\nand\n```bash\n%s --execute\n```" % (TOOL, TOOL)
        r = self.rec(text, reply((2, "%s --dry-run" % TOOL)))
        self.assertEqual(r["r2"]["status"], "rejected")
        self.assertTrue(any("--execute" in x for x in r["r2"]["reasons"]))
        self.assertEqual(r["final_text"].count("not confirmed"), 2)       # R1 marks both occurrences
        self.assertEqual(r["final_text"].replace("\n", "").count("--execute"), 2 + 2)

    def test_name_left_in_an_unflagged_prose_line_rejects_the_patch(self):
        text = "Avoid `--execute` here.\n```bash\n%s --execute\n```" % TOOL
        r = self.rec(text, reply((3, "%s --dry-run" % TOOL)))
        self.assertEqual(r["r2"]["status"], "rejected")
        self.assertTrue(any("still in the answer" in x or "flag remains" in x for x in r["r2"]["reasons"]), r["r2"])

    def test_longer_name_that_starts_with_the_flagged_one_is_not_a_survivor(self):
        f = {"level": "invented", "type": "cli_flag", "text": "--topic", "line": 1}
        self.assertEqual(rv.surviving("x --topic-list y", [f]), [])
        self.assertEqual(rv.surviving("x --topic y", [f]), ["--topic"])
        self.assertEqual(rv.surviving("`--topic`.", [f]), ["--topic"])
        k = {"level": "invented", "type": "config_key", "text": "a.b", "line": 1}
        self.assertEqual(rv.surviving("a.b.c is real", [k]), [])
        self.assertEqual(rv.surviving("a.b is not", [k]), ["a.b"])

    def test_a_flag_inside_a_fence_is_left_to_the_recheck_because_it_belongs_to_one_tool(self):
        f = {"level": "invented", "type": "cli_flag", "text": "--topic", "line": 1}
        self.assertEqual(rv.surviving("```sh\nkafka-topics.sh --topic t\n```", [f]), [])
        self.assertEqual(rv.surviving("use `--topic` on it\n```sh\nx\n```", [f]), ["--topic"])

    def test_values_are_not_searched_for_survival(self):
        f = {"level": "wrong_use", "type": "config_value", "text": "true", "line": 1}
        self.assertEqual(rv.surviving("true", [f]), [])


class TestNear(unittest.TestCase):
    def ck(self):
        return Checker(CATS)

    def test_close_sibling_is_kept_and_distant_one_is_dropped(self):
        c = self.ck()
        from kafka_sre_rag.checker import Item
        flag = Item("--execut", "cli_flag", 1, "")
        self.assertEqual(c._keep_near(flag, ["--execute", "--dry-run"]), ["--execute"])
        self.assertEqual(c._keep_near(Item("--topic", "cli_flag", 1, ""), ["--topic-list"]), ["--topic-list"])
        fld = Item("spec.kafka.storage", "field_path", 1, "")
        self.assertEqual(c._keep_near(fld, ["spec.kafka.image", "spec.kafka.tieredStorage"]), [])
        self.assertEqual(c._keep_near(Item("spec.kafkaVersion", "field_path", 1, ""), ["spec.kafka.version"]), [])
        self.assertEqual(c._keep_near(Item("spec.kafka.versoin", "field_path", 1, ""), ["spec.kafka.version"]),
                         ["spec.kafka.version"])

    def test_scope_filter(self):
        from kafka_sre_rag.checker import Item
        c = self.ck()
        it = Item("log.retention.hour", "config_key", 1, "")
        self.assertEqual(c._keep_near(it, ["log.retention.hours"], lambda k: False), [])
        self.assertEqual(c._keep_near(it, ["log.retention.hours"], lambda k: True), ["log.retention.hours"])

    def test_view_without_near_is_as_the_checker_wrote_it(self):
        f = {"evidence": "not a key; nearest real: `x`", "suggestion": "did you mean `x`?"}
        self.assertEqual(rv.finding_view(f), (f["evidence"], f["suggestion"]))

    def test_view_with_empty_near_says_only_that_it_does_not_exist(self):
        f = {"evidence": "not a key of the reference for any scope; nearest real: `x`, `y`", "suggestion": "did you mean `x`?",
             "near": []}
        self.assertEqual(rv.finding_view(f), ("not a key of the reference for any scope", ""))

    def test_view_with_near_lists_only_those(self):
        f = {"evidence": "not a key; nearest real: `x`, `y`", "suggestion": "did you mean `x`?", "near": ["y"]}
        self.assertEqual(rv.finding_view(f), ("not a key; nearest real: `y`", "did you mean `y`?"))

    def test_r1_mark_and_r2_prompt_use_the_view(self):
        f = {"level": "invented", "type": "field_path", "text": "spec.kafka.storage", "line": 1, "lines": [1],
             "evidence": "`spec.kafka.storage` is not a field of Kafka; nearest real: `spec.kafka.image`",
             "suggestion": "did you mean `spec.kafka.image`?", "near": [], "sentence": "spec.kafka.storage"}
        mark = rv.mark_in_place("`spec.kafka.storage`", [f])
        self.assertNotIn("image", mark)
        self.assertIn("is not a field of Kafka", mark)
        msg = rv.build_messages("1| x", CHUNKS, [f])[1]["content"]
        self.assertNotIn("spec.kafka.image", msg)

    def test_the_checker_sets_near_on_real_findings_and_keeps_its_note(self):
        res = Checker(CATS).check("```yaml\napiVersion: kafka.strimzi.io/v1\nkind: KafkaUser\nspec:\n  quotas:\n    consumer: 5\n```")
        f = res.findings[0].to_dict()
        self.assertEqual(f["type"], "field_path")
        self.assertIn("near", f)
        self.assertIn("spec.quotas.consumerByteRate", f["evidence"])   # the checker's own evidence is unchanged


class TestPrompt(unittest.TestCase):
    P = prompts.REVISE_PROMPT

    def test_dispute_sentence_is_gone(self):
        self.assertNotIn("leave its line out", self.P)
        self.assertNotIn("finding is wrong", self.P)

    def test_json_only_and_nearest_real_and_repeated_lines(self):
        self.assertIn("Reply with JSON only", self.P)
        self.assertIn("nothing else", self.P)
        self.assertIn("nearest real", self.P)
        self.assertIn("the excerpts support", self.P)
        self.assertIn("remove the line or reword it", self.P)
        self.assertIn("every one of them", self.P)

    def test_placeholders_and_no_item_id(self):
        for ph in ("<<EXCERPTS>>", "<<ANSWER>>", "<<FINDINGS>>"):
            self.assertEqual(self.P.count(ph), 1)
        self.assertIsNone(re.search(r"\b[bp]\d?-\d\d\b", self.P))

    def test_findings_carry_lines(self):
        f = {"level": "invented", "type": "cli_flag", "text": "--x", "line": 3, "lines": [3, 9], "evidence": "e", "suggestion": ""}
        msg = rv.build_messages("1| x", CHUNKS, [f])[1]["content"]
        self.assertEqual(json.loads([l for l in msg.split("\n") if l.startswith('{"line"')][0])["lines"], [3, 9])


if __name__ == "__main__":
    unittest.main()
