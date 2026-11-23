"""M7 P1: the checking pass (kafka_sre_rag/verify_pass.py), rule by rule, with a stub model and the fixture catalogues.
Every draft here is synthetic: it imitates the shape of the open development item that prompted a rule (named in the
test) and carries none of its text. Offline: no model server, no GPU."""
import json
import shutil
import sys
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))
from kafka_sre_rag import prompts, revise as rv, service, verify_pass as vp  # noqa: E402
from test_ask_check import make_catalogue_dir  # noqa: E402

CHUNKS = [
    {"id": "c1", "source": "apache-kafka", "title": "Consumer configs", "path": "raw/a.md",
     "text": "The max.poll.interval.ms setting is the maximum delay between invocations of poll() when using consumer "
             "group management. Retention is set with log.retention.ms on the broker."},
    {"id": "c2", "source": "apache-kafka", "title": "Topic operations", "path": "raw/b.md",
     "text": "Deleting a topic with kafka-topics.sh is irreversible and causes data loss. Run kafka-topics.sh "
             "--describe --topic orders --exclude-internal to list one topic, or kafka-topics.sh --describe --delete "
             "--topic orders to remove it. Set retention.ms: 604800000 to keep data for seven days."},
]
FILLER = ["Step %d: watch the lag dashboard of the group and note what changes over time." % i for i in range(1, 21)]
DRAFT = "\n".join([
    "## Why the group rebalances",                                                              # 1
    "`max.poll.interval.ms` is the maximum delay between heartbeats sent to the coordinator.",  # 2 contradicted by c1
    "Set `log.retention.hours` on the broker to keep logs longer.",                              # 3 invented (fixture)
    "```sh",                                                                                     # 4
    "kafka-topics.sh --bootstrap-server b:9092 --describe --topic orders",                       # 5
    "```",                                                                                       # 6
    "Never delete the topic: data loss is irreversible.",                                        # 7 a warning
    "Later, `log.retention.hours` can be raised again.",                                         # 8 same item again
] + FILLER)
Q = "Why does my consumer group rebalance and how long are logs kept?"
POLL_QUOTE = "is the maximum delay between invocations of poll() when using consumer group management"
POLL_FIX = "`max.poll.interval.ms` is the maximum delay between invocations of poll()."
FINDING_QUOTE = "nearest real: log.retention.ms"


def rep(*entries, **extra):
    return json.dumps({"replacements": [dict(zip(("line", "quote", "text"), e)) for e in entries]}, **extra)


class Model:
    """Stub model of the checking call: records each call, returns the replies in turn."""
    identity = {"backend": "stub", "model": "stub", "adapter": None}
    backend = "stub"

    def __init__(self, *replies, truncated=False):
        self.replies, self.calls, self.truncated = list(replies), [], truncated

    def chat(self, messages, max_tokens, on_token=None, *, thinking=None, info=None):
        self.calls.append({"messages": messages, "max_tokens": max_tokens, "thinking": thinking})
        r = self.replies.pop(0)
        if isinstance(r, Exception):
            raise r
        return r, 9, self.truncated


class Pass(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dir = make_catalogue_dir()
        cls.ck = service.AnswerChecker(cls.dir)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.dir, ignore_errors=True)

    def run_pass(self, reply, text=DRAFT, chunks=CHUNKS, question=Q, cards=False, truncated=False, model_cut=False):
        m = Model(reply, truncated=model_cut)
        rec = vp.Verifier(self.ck, m, cards=cards).run(question, text, chunks, truncated=truncated)
        return rec, m

    def reasons(self, rec, line):
        p = next(p for p in rec["proposals"] if p["line"] == line)
        return p["stage1"] + p["stage2"]

    def assertUnchanged(self, rec, text=DRAFT):
        self.assertEqual(rec["final_text"], text)
        self.assertEqual(rec["patches"], [])


class TestPromptAndCall(Pass):
    def test_messages_layout_thinking_off_and_record(self):
        rec, m = self.run_pass(rep())
        self.assertEqual(len(m.calls), 1)
        call = m.calls[0]
        self.assertIs(call["thinking"], False)
        self.assertEqual(call["max_tokens"], vp.MAX_TOKENS)
        sys_msg, user = call["messages"]
        self.assertEqual(sys_msg, {"role": "system", "content": prompts.VERIFY_SYSTEM})
        u = user["content"]
        self.assertTrue(u.startswith("Question:\n\n" + Q + "\n\nDocumentation excerpts:\n\n" + prompts.pack_text(CHUNKS)))
        self.assertIn("\n\nDraft answer, one numbered line per line:\n\n" + rv.numbered(DRAFT) + "\n\n", u)
        flagged_lines = [json.loads(l) for l in u.split("(one JSON object per line; empty if none):\n\n")[1]
                         .split("\n\nName a line")[0].split("\n")]
        self.assertEqual([(f["item"], f["lines"]) for f in flagged_lines], [("log.retention.hours", [3, 8])])
        self.assertNotIn("Reference cards", u)
        self.assertEqual(rec["r2"]["status"], "no_patch")
        self.assertUnchanged(rec)
        self.assertEqual(rec["verify_prompt_sha256"], vp.prompt_sha256(False))
        self.assertEqual(rv.public(rec)["r2"]["status"], "no_patch")   # the Reviser's public() applies
        for k in ("check_s", "cards_s", "call_s", "rules_s", "total_s"):
            self.assertIn(k, rec["timings"])

    def test_contradicted_line_is_corrected(self):
        rec, _ = self.run_pass(rep((2, POLL_QUOTE, POLL_FIX)))
        self.assertEqual(rec["r2"]["status"], "accepted", rec["proposals"])
        self.assertEqual(rec["final_text"].split("\n")[1], POLL_FIX)
        self.assertEqual(rec["final_text"].split("\n")[2:], DRAFT.split("\n")[2:])
        self.assertEqual(rec["patches"][0]["quote_found"], "excerpts")
        self.assertEqual(rec["first_pass_text"], DRAFT)
        self.assertLessEqual(rec["limits"]["line_share"], vp.MAX_LINE_SHARE)


class TestA1Reply(Pass):
    """b2-19: a reply cut at the token limit while arguing."""

    def test_reply_that_is_not_json_changes_nothing(self):
        rec, _ = self.run_pass("Line 2 looks wrong to me because ...")
        self.assertEqual(rec["r2"]["status"], "not_json")
        self.assertUnchanged(rec)

    def test_cut_reply_changes_nothing_even_when_it_parses(self):
        rec, _ = self.run_pass(rep((2, POLL_QUOTE, POLL_FIX)), model_cut=True)
        self.assertEqual(rec["r2"]["status"], "cut")
        self.assertUnchanged(rec)

    def test_cut_json_changes_nothing(self):
        rec, _ = self.run_pass(rep((2, POLL_QUOTE, POLL_FIX))[:-30])
        self.assertEqual(rec["r2"]["status"], "not_json")
        self.assertUnchanged(rec)

    def test_a_bad_entry_drops_only_itself(self):
        reply = json.dumps({"replacements": [{"line": 99, "quote": "x y z w", "text": "t"},
                                             {"line": 2, "text": "no quote"},
                                             {"line": 2, "quote": POLL_QUOTE, "text": POLL_FIX, "why": "extra key"},
                                             {"line": 2, "quote": POLL_QUOTE, "text": POLL_FIX}]})
        rec, _ = self.run_pass(reply)
        self.assertEqual(len(rec["dropped"]), 3)
        self.assertEqual(rec["r2"]["status"], "accepted")
        self.assertEqual(rec["final_text"].split("\n")[1], POLL_FIX)

    def test_more_than_six_entries_keep_the_first_six(self):
        entries, dropped, _ = vp.parse_verify_reply(rep(*[(n, "a b c d", "x") for n in range(1, 9)]), 20)
        self.assertEqual([e["line"] for e in entries], [1, 2, 3, 4, 5, 6])
        self.assertEqual([d["entry"]["line"] for d in _], [7, 8])

    def test_fenced_reply_and_bare_list_are_read(self):
        e, why, _ = vp.parse_verify_reply("```json\n" + rep((2, "a b c d", "x")) + "\n```", 5)
        self.assertEqual((e, why), ([{"line": 2, "quote": "a b c d", "text": "x"}], ""))


class TestA2Quote(Pass):
    def test_quote_from_nowhere_is_refused(self):
        # b1-09: the model disputes a true finding from memory
        rec, _ = self.run_pass(rep((3, "log.retention.hours is a valid broker setting",
                                    "Set `log.retention.hours` on the broker to keep logs longer, as before.")))
        self.assertTrue(any(r.startswith("A2: quote not found") for r in self.reasons(rec, 3)))
        self.assertUnchanged(rec)

    def test_quote_too_short_is_refused(self):
        rec, _ = self.run_pass(rep((2, "invocations of poll()", POLL_FIX)))
        self.assertIn("A2: quote too short (3 words)", self.reasons(rec, 2))
        self.assertUnchanged(rec)

    def test_quote_with_markup_and_other_whitespace_is_found(self):
        rec, _ = self.run_pass(rep((2, "is the **maximum delay**  between `invocations` of poll() when using consumer "
                                       "group management", POLL_FIX)))
        self.assertEqual(rec["r2"]["status"], "accepted", rec["proposals"])

    def test_quote_from_the_evidence_of_a_finding_on_the_line(self):
        rec, _ = self.run_pass(rep((3, FINDING_QUOTE, "Set `log.retention.ms` on the broker to keep logs longer."),
                                   (8, FINDING_QUOTE, "Later, `log.retention.ms` can be raised again.")))
        self.assertEqual(rec["r2"]["status"], "accepted", rec["proposals"])
        self.assertEqual({p["quote_found"] for p in rec["patches"]}, {"finding"})
        self.assertNotIn("log.retention.hours", rec["final_text"])
        self.assertEqual(rv.flagged(rec["check_after"]), [])

    def test_evidence_of_a_finding_on_another_line_does_not_count(self):
        rec, _ = self.run_pass(rep((2, FINDING_QUOTE, POLL_FIX)))
        self.assertTrue(any(r.startswith("A2:") for r in self.reasons(rec, 2)))


class TestA3Related(Pass):
    def test_real_quote_used_for_an_unrelated_edit_is_refused(self):
        rec, _ = self.run_pass(rep((2, "Deleting a topic with kafka-topics.sh is irreversible and causes data loss",
                                    "`max.poll.interval.ms` is irreversible.")))
        self.assertIn("A3: the quote shares no checked item and fewer than two content words with the line",
                      self.reasons(rec, 2))
        self.assertUnchanged(rec)


class TestA4Lines(Pass):
    def test_heading_is_never_changed(self):
        rec, _ = self.run_pass(rep((1, "consumer group management. Retention is set", "## Why the group rebalances now")))
        self.assertIn("A4: the line is a heading", self.reasons(rec, 1))
        self.assertUnchanged(rec)

    def test_line_repeating_the_users_configuration_is_never_changed(self):
        # b2-25: the user's own setting was changed
        q = "My producer runs with acks=all and enable.idempotence=false and max.in.flight=5; why do I see duplicates?"
        text = "\n".join(["You run with acks=all and enable.idempotence=false and max.in.flight=5 today."] + FILLER)
        rec, _ = self.run_pass(rep((1, "consumer group management. Retention is set with log.retention.ms",
                                    "You run with acks=all and enable.idempotence=true and max.in.flight=5 today.")),
                               text=text, question=q)
        self.assertTrue(any(r.startswith("A4: the line copies") for r in self.reasons(rec, 1)))
        self.assertUnchanged(rec, text)

    def test_general_knowledge_line_is_never_changed(self):
        # plan section 4: under grounded-g2 the check never edits a line labelled as general knowledge
        text = "\n".join(["General knowledge (not in the excerpts): the delay is between heartbeats."] + FILLER)
        rec, _ = self.run_pass(rep((1, POLL_QUOTE, "General knowledge (not in the excerpts): the delay is between "
                                                   "invocations of poll().")), text=text)
        self.assertIn("A4: a general-knowledge line", self.reasons(rec, 1))
        self.assertUnchanged(rec, text)

    def test_last_line_of_a_cut_draft_is_never_changed(self):
        text = "\n".join(FILLER + ["`max.poll.interval.ms` is the maximum delay between heartbeats sent to the"])
        n = len(text.split("\n"))
        rec, _ = self.run_pass(rep((n, POLL_QUOTE, POLL_FIX)), text=text, truncated=True)
        self.assertIn("A4: the last line of a draft cut at the token limit", self.reasons(rec, n))
        self.assertUnchanged(rec, text)
        rec2, _ = self.run_pass(rep((n, POLL_QUOTE, POLL_FIX)), text=text, truncated=False)
        self.assertNotIn("A4: the last line of a draft cut at the token limit", self.reasons(rec2, n))

    def test_fence_marker_and_table_separator_are_never_changed(self):
        rec, _ = self.run_pass(rep((4, "kafka-topics.sh --describe --topic orders --exclude-internal", "```bash")))
        self.assertIn("A4: the line is a fence marker", self.reasons(rec, 4))
        text = "\n".join(["| key | default |", "|---|---|", "| max.poll.interval.ms | heartbeats |"] + FILLER)
        rec, _ = self.run_pass(rep((2, POLL_QUOTE, "|---|---|---|")), text=text)
        self.assertIn("A4: the line is a table separator", self.reasons(rec, 2))


class TestA5Shape(Pass):
    def test_replacement_longer_than_two_lines_is_refused(self):
        rec, _ = self.run_pass(rep((2, POLL_QUOTE, POLL_FIX + "\nmore\nand more")))
        self.assertIn("A5: line 2: replacement longer than two lines", self.reasons(rec, 2))
        self.assertUnchanged(rec)

    def test_replacement_with_a_fence_marker_is_refused(self):
        rec, _ = self.run_pass(rep((2, POLL_QUOTE, "```\n" + POLL_FIX)))
        self.assertIn("A5: line 2: replacement contains a code fence marker", self.reasons(rec, 2))

    def test_a_prose_line_cannot_become_two(self):
        rec, _ = self.run_pass(rep((2, POLL_QUOTE, POLL_FIX + "\nSee the consumer configs.")))
        self.assertIn("A5: a prose line cannot become two lines", self.reasons(rec, 2))
        self.assertUnchanged(rec)


class TestA6NewItems(Pass):
    def test_new_name_from_memory_is_refused(self):
        rec, _ = self.run_pass(rep((2, POLL_QUOTE, POLL_FIX[:-1] + "; also raise `max.poll.records`.")))
        self.assertIn("A6: new item max.poll.records is in neither the quote nor the excerpts", self.reasons(rec, 2))
        self.assertUnchanged(rec)

    def test_new_number_from_memory_is_refused(self):
        rec, _ = self.run_pass(rep((2, POLL_QUOTE, POLL_FIX[:-1] + " (45000 ms by default).")))
        self.assertIn("A6: new number 45000 is in neither the quote nor the excerpts", self.reasons(rec, 2))
        self.assertUnchanged(rec)


class TestA7Command(Pass):
    def test_scope_flag_removed_from_a_command_is_refused(self):
        # b1-24 run 2: a scope flag was deleted and the reset then hit every partition
        text = "\n".join(["Reset the one partition:", "```sh",
                          "kafka-consumer-groups.sh --bootstrap-server b:9092 --group g --topic orders --partition 3 "
                          "--reset-offsets --to-earliest --execute", "```"] + FILLER)
        chunks = CHUNKS + [{"id": "c3", "source": "apache-kafka", "title": "Groups", "path": "raw/c.md",
                            "text": "kafka-consumer-groups.sh --bootstrap-server b:9092 --group g --topic orders "
                                    "--reset-offsets --to-earliest --execute resets the group."}]
        rec, _ = self.run_pass(rep((3, "kafka-consumer-groups.sh --bootstrap-server b:9092 --group g --topic orders "
                                       "--reset-offsets --to-earliest --execute",
                                    "kafka-consumer-groups.sh --bootstrap-server b:9092 --group g --topic orders "
                                    "--reset-offsets --to-earliest --execute")), text=text, chunks=chunks)
        self.assertIn("A7: flag removed from the command: --partition", self.reasons(rec, 3))
        self.assertUnchanged(rec, text)

    def test_namespace_removed_from_a_command_is_refused(self):
        text = "\n".join(["```sh", "kubectl -n kafka get kafkatopic orders -o yaml", "```"] + FILLER)
        chunks = CHUNKS + [{"id": "c4", "source": "strimzi", "title": "Topics", "path": "raw/d.md",
                            "text": "Check the topic with kubectl get kafkatopic orders -o yaml before a change."}]
        rec, _ = self.run_pass(rep((2, "kubectl get kafkatopic orders -o yaml", "kubectl get kafkatopic orders -o yaml")),
                               text=text, chunks=chunks)
        self.assertIn("A7: namespace removed from the command: kafka", self.reasons(rec, 2))


class TestA8Warnings(Pass):
    QUOTE = "Deleting a topic with kafka-topics.sh is irreversible and causes data loss"

    def test_a_warning_is_kept(self):
        rec, _ = self.run_pass(rep((7, self.QUOTE, "Delete the topic when the data loss does not matter.")))
        self.assertTrue(any(r.startswith("A8: warning dropped:") and "irreversible" in r for r in self.reasons(rec, 7)))
        self.assertUnchanged(rec)

    def test_a_line_with_a_warning_is_not_deleted(self):
        rec, _ = self.run_pass(rep((7, self.QUOTE, None)))
        self.assertIn("A8: a line with a warning is not deleted", self.reasons(rec, 7))
        self.assertUnchanged(rec)

    def test_part_of_a_wrapped_command_is_not_deleted(self):
        text = "\n".join(["```sh", "kafka-topics.sh --bootstrap-server b:9092 \\", "  --describe --topic orders",
                          "```"] + FILLER)
        rec, _ = self.run_pass(rep((3, "kafka-topics.sh --describe --topic orders --exclude-internal", None)), text=text)
        self.assertIn("A8: deletion of part of a command (lines 2-3)", self.reasons(rec, 3))
        self.assertUnchanged(rec, text)


class TestStage2(Pass):
    def test_changes_are_accepted_one_by_one(self):
        # b1-25 D: one bad line rejected six correct changes; here the good change stays
        rec, _ = self.run_pass(rep((2, POLL_QUOTE, POLL_FIX),
                                   (5, "kafka-topics.sh --describe --delete --topic orders",
                                    "kafka-topics.sh --bootstrap-server b:9092 --describe --delete --topic orders")))
        self.assertEqual(rec["r2"]["status"], "accepted")
        self.assertEqual([p["line"] for p in rec["patches"]], [2])
        self.assertTrue(any(r.startswith("new destructive step not warned against: D5") for r in self.reasons(rec, 5)))
        self.assertIn("kafka-topics.sh --bootstrap-server b:9092 --describe --topic orders", rec["final_text"])

    def test_dangling_backslash_is_refused(self):
        text = "\n".join(["```sh", "kafka-topics.sh --bootstrap-server b:9092 \\", "  --describe --topic orders",
                          "```"] + FILLER)
        rec, _ = self.run_pass(rep((3, "kafka-topics.sh --describe --topic orders --exclude-internal",
                                    "  --describe --topic orders --exclude-internal \\")), text=text)
        self.assertEqual(self.reasons(rec, 3),
                         ["lines 2-3: the command is left with an orphan continuation line or a dangling backslash"])
        self.assertUnchanged(rec, text)

    def test_yaml_that_stops_parsing_is_refused(self):
        try:
            import yaml  # noqa: F401
        except ImportError:
            self.skipTest("PyYAML not installed")
        text = "\n".join(["```yaml", "config:", "  retention.ms: 604800000", "```"] + FILLER)
        rec, _ = self.run_pass(rep((3, "Set retention.ms: 604800000 to keep data", "  retention.ms: [604800000")),
                               text=text)
        self.assertIn("YAML block 1 parsed before the patch and does not now", self.reasons(rec, 3))
        self.assertUnchanged(rec, text)

    def test_indentation_change_is_refused(self):
        # b1-25 S: an indentation change inside a block
        text = "\n".join(["```yaml", "config:", "  retention.ms: 604800000", "```"] + FILLER)
        rec, _ = self.run_pass(rep((3, "Set retention.ms: 604800000 to keep data", "    retention.ms: 604800000")),
                               text=text)
        self.assertIn("line 3: the leading whitespace changed", self.reasons(rec, 3))

    def test_item_left_on_another_line_rejects_the_partial_fix(self):
        # b1-23 S, b2-20: the flagged name must not stay elsewhere in the answer
        rec, _ = self.run_pass(rep((3, FINDING_QUOTE, "Set `log.retention.ms` on the broker to keep logs longer.")))
        self.assertIn("flagged name still in the answer: log.retention.hours", self.reasons(rec, 3))
        self.assertUnchanged(rec)

    def test_all_occurrences_go_in_together(self):
        rec, _ = self.run_pass(rep((2, POLL_QUOTE, POLL_FIX),
                                   (3, FINDING_QUOTE, "Set `log.retention.ms` on the broker to keep logs longer."),
                                   (8, FINDING_QUOTE, "Later, `log.retention.ms` can be raised again.")))
        groups = {p["line"]: p["group"] for p in rec["proposals"]}
        self.assertEqual(groups[3], groups[8])
        self.assertNotEqual(groups[2], groups[3])
        self.assertEqual([p["line"] for p in rec["patches"]], [2, 3, 8])
        self.assertEqual(rec["check_after"]["counts"]["invented"], 0)
        self.assertEqual(rec["destructive_after"], rec["destructive_before"])

    def test_flag_still_on_the_changed_line_is_refused(self):
        rec, _ = self.run_pass(rep((3, FINDING_QUOTE, "Set `log.retention.hours` and `log.retention.ms` on the broker."),
                                   (8, FINDING_QUOTE, "Later, `log.retention.ms` can be raised again.")))
        self.assertTrue(any(r == "flag remains on a changed line: log.retention.hours" for r in self.reasons(rec, 3)))
        self.assertUnchanged(rec)


class TestLimits(Pass):
    def test_over_a_quarter_of_the_lines_changes_nothing(self):
        text = "\n".join(["## Poll", DRAFT.split("\n")[1], "Watch the lag."])
        rec, _ = self.run_pass(rep((2, POLL_QUOTE, POLL_FIX)), text=text)
        self.assertEqual(rec["r2"]["status"], "over_limit")
        self.assertGreater(rec["limits"]["line_share"], vp.MAX_LINE_SHARE)
        self.assertUnchanged(rec, text)
        self.assertTrue(self.reasons(rec, 2)[0].startswith("over the limits:"))

    def test_over_15_percent_of_the_characters_changes_nothing(self):
        text = "\n".join([DRAFT.split("\n")[1]] + ["ok"] * 8)
        rec, _ = self.run_pass(rep((1, POLL_QUOTE, POLL_FIX)), text=text)
        self.assertEqual(rec["r2"]["status"], "over_limit")
        self.assertGreater(rec["limits"]["char_share"], vp.MAX_CHAR_SHARE)
        self.assertLessEqual(rec["limits"]["line_share"], vp.MAX_LINE_SHARE)
        self.assertUnchanged(rec, text)

    def test_within_the_limits(self):
        rec, _ = self.run_pass(rep((2, POLL_QUOTE, POLL_FIX)))
        self.assertEqual(rec["limits"]["lines"], 1)
        self.assertEqual(rec["limits"]["max"], {"lines": 6, "line_share": 0.25, "char_share": 0.15})


class TestFailures(Pass):
    def test_failed_call_changes_nothing(self):
        rec, _ = self.run_pass(RuntimeError("server gone"))
        self.assertEqual(rec["r2"], {"status": "error", "reasons": ["model call failed: RuntimeError"]})
        self.assertUnchanged(rec)

    def test_deadline_skips_the_call(self):
        m = Model(rep((2, POLL_QUOTE, POLL_FIX)))
        rec = vp.Verifier(self.ck, m).run(Q, DRAFT, CHUNKS, deadline_at=time.monotonic() - 1)
        self.assertEqual(m.calls, [])
        self.assertEqual(rec["r2"]["status"], "not_checked")
        self.assertUnchanged(rec)
        self.assertIn("not checked", vp.summary_line(rec))


class TestCards(Pass):
    """Decision 2 of 2026-11-23: the pass with the reference cards of the draft's names in its input."""
    TEXT = "\n".join(["The default of `max.poll.interval.ms` is 600000 on the consumer."] + FILLER)
    CHUNKS2 = CHUNKS[1:]      # the excerpts say nothing about the key
    FIX = "The default of `max.poll.interval.ms` is 300000 on the consumer."
    CARD_QUOTE = "type: int default: 300000 (5 minutes)"

    def test_cards_go_into_the_prompt(self):
        rec, m = self.run_pass(rep(), text=self.TEXT, chunks=self.CHUNKS2, cards=True)
        u = m.calls[0]["messages"][1]["content"]
        self.assertIn("Reference cards from the pinned catalogues for names the draft uses (exact facts; empty if none):"
                      "\n\nREFERENCE CARDS from the pinned catalogues", u)
        self.assertIn("config key `max.poll.interval.ms`", u)
        self.assertIn("or a fact on a reference card", u)
        self.assertEqual(rec["cards"]["count"], 1)
        self.assertEqual(rec["verify_prompt_sha256"], vp.prompt_sha256(True))

    def test_quote_from_a_card_is_accepted(self):
        rec, _ = self.run_pass(rep((1, self.CARD_QUOTE, self.FIX)), text=self.TEXT, chunks=self.CHUNKS2, cards=True)
        self.assertEqual(rec["r2"]["status"], "accepted", rec["proposals"])
        self.assertEqual(rec["patches"][0]["quote_found"], "card")
        self.assertEqual(rec["final_text"].split("\n")[0], self.FIX)

    def test_same_quote_without_cards_is_refused(self):
        rec, _ = self.run_pass(rep((1, self.CARD_QUOTE, self.FIX)), text=self.TEXT, chunks=self.CHUNKS2, cards=False)
        self.assertTrue(any(r.startswith("A2: quote not found") for r in self.reasons(rec, 1)))
        self.assertUnchanged(rec, self.TEXT)

    def test_quote_from_nowhere_is_refused_with_cards(self):
        rec, _ = self.run_pass(rep((1, "type: int default: 900000 (15 minutes)", self.FIX)), text=self.TEXT,
                               chunks=self.CHUNKS2, cards=True)
        self.assertTrue(any(r.startswith("A2: quote not found in the excerpts, the cards") for r in self.reasons(rec, 1)))
        self.assertUnchanged(rec, self.TEXT)


class TestProfileDraft(Pass):
    def test_draft_with_thinking_then_check_without(self):
        class G(Model):
            def chat(self, messages, max_tokens, on_token=None, *, thinking=None, info=None):
                self.calls.append({"messages": messages, "max_tokens": max_tokens, "thinking": thinking,
                                   "streamed": on_token is not None})
                if messages[0]["content"] == prompts.VERIFY_SYSTEM:
                    return rep((2, POLL_QUOTE, POLL_FIX)), 5, False
                info.update(reasoning_tokens=321, think_s=4.5)
                return DRAFT, 700, False
        g = G()
        msgs = prompts.answer_messages(Q, CHUNKS)
        th = vp.draft_and_check(Q, msgs, CHUNKS, g, 2048, vp.Profile(cards=False), self.ck)
        self.assertEqual([(c["thinking"], c["max_tokens"]) for c in g.calls], [(True, 2048 + vp.THINKING_TOKENS),
                                                                               (False, vp.MAX_TOKENS)])
        self.assertEqual(g.calls[0]["messages"], msgs)
        self.assertTrue(g.calls[0]["streamed"])
        self.assertEqual(th["first_pass_text"], DRAFT)
        self.assertEqual(th["text"].split("\n")[1], POLL_FIX)
        self.assertEqual(th["seconds"]["thinking"], 4.5)
        self.assertEqual(th["draft_info"]["reasoning_tokens"], 321)

    def test_switches(self):
        g = Model(DRAFT)
        th = vp.draft_and_check(Q, prompts.answer_messages(Q, CHUNKS), CHUNKS, g, 2048, vp.Profile(verify=False), self.ck)
        self.assertIsNone(th["record"])
        self.assertEqual(th["text"], DRAFT)
        self.assertEqual(len(g.calls), 1)
        g = Model(DRAFT, rep())
        th = vp.draft_and_check(Q, prompts.answer_messages(Q, CHUNKS), CHUNKS, g, 2048, vp.Profile(thinking=False), self.ck)
        self.assertEqual([(c["thinking"], c["max_tokens"]) for c in g.calls], [(False, 2048), (False, vp.MAX_TOKENS)])
        self.assertTrue(th["record"]["cards_enabled"])

    def test_deadline_counts_from_the_question(self):
        g = Model(DRAFT)
        th = vp.draft_and_check(Q, prompts.answer_messages(Q, CHUNKS), CHUNKS, g, 2048, vp.Profile(), self.ck,
                                t_start=time.monotonic() - vp.DEADLINE_S - 1)
        self.assertEqual(len(g.calls), 1)
        self.assertEqual(th["record"]["r2"]["status"], "not_checked")
        self.assertEqual(th["text"], DRAFT)


if __name__ == "__main__":
    unittest.main()
