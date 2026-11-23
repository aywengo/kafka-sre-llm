"""M7 P1, after the review (runs/m7/reports/p1_review.md, S1 to S7 and its test gaps): each test here fails when the
rule it names is taken out. Synthetic drafts, stub models, fixture catalogues, httpx.MockTransport. Offline: no model
server, no GPU."""
import json
import shutil
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))
sys.path.insert(0, str(ROOT / "scripts"))
from kafka_sre_rag import generators as G, prompts, verify_pass as vp  # noqa: E402
from test_thinking_request import MSGS, Rec, gen  # noqa: E402
from test_verify_pass import (CHUNKS, DRAFT, FILLER, FINDING_QUOTE, POLL_FIX, POLL_QUOTE, Q, Model, Pass,  # noqa: E402
                              rep)

FIX3 = "Set `log.retention.ms` on the broker to keep logs longer."
FIX8 = "Later, `log.retention.ms` can be raised again."


def chunk(i, text, title="Ops"):
    return {"id": "r%d" % i, "source": "apache-kafka", "title": title, "path": "raw/r%d.md" % i, "text": text}


class Injecting:
    """The fixture checker, with one extra invented item on every check after the first (the re-check of stage 2)."""

    def __init__(self, ck):
        self.ck, self.n = ck, 0
        self.checker, self.cats, self.directory = ck.checker, ck.cats, ck.directory

    def check(self, text, chunks):
        self.n += 1
        r = self.ck.check(text, chunks)
        if self.n > 1:
            r = dict(r, findings=list(r["findings"]) + [{
                "type": "config", "text": "made.up.key", "level": "invented", "line": 2, "lines": [2],
                "evidence": "not in the catalogue", "suggestion": None}])
        return r


class Raising:
    def __init__(self, ck, on_call):
        self.ck, self.n, self.on_call = ck, 0, on_call
        self.checker, self.cats, self.directory = ck.checker, ck.cats, ck.directory

    def check(self, text, chunks):
        self.n += 1
        if self.n == self.on_call:
            raise RuntimeError("checker broke")
        return self.ck.check(text, chunks)


# ----------------------------------------------------------------------------- stage 2's gaps
class TestStage2Gaps(Pass):
    def test_new_invented_item_on_the_re_check_is_refused(self):
        m = Model(rep((2, POLL_QUOTE, POLL_FIX)))
        rec = vp.Verifier(Injecting(self.ck), m).run(Q, DRAFT, CHUNKS)
        self.assertIn("new invented or wrong_use: made.up.key", self.reasons(rec, 2))
        self.assertUnchanged(rec)

    def test_destructive_match_turned_from_skipped_to_not_skipped_is_refused(self):
        def fake(text):
            return [{"id": "D5", "skipped": POLL_FIX not in text}]   # warned against before the change, not after
        with mock.patch.object(vp, "find_destructive", side_effect=fake):
            rec, _ = self.run_pass(rep((2, POLL_QUOTE, POLL_FIX)))
        self.assertIn("new destructive step not warned against: D5", self.reasons(rec, 2))
        self.assertUnchanged(rec)

    def test_a_group_is_rejected_together(self):
        # lines 3 and 8 carry one flagged item; line 8's change leaves the flag on it, so neither change is kept, and
        # both carry the group's reason (taken one by one, line 3 would fail on another reason)
        rec, _ = self.run_pass(rep((3, FINDING_QUOTE, FIX3),
                                   (8, FINDING_QUOTE, "Later, `log.retention.hours` can be raised again, slowly.")))
        g = {p["line"]: p for p in rec["proposals"]}
        self.assertEqual(g[3]["group"], g[8]["group"])
        self.assertEqual(g[3]["stage2"], g[8]["stage2"])
        self.assertIn("flag remains on a changed line: log.retention.hours", g[3]["stage2"])
        self.assertFalse(g[3]["accepted"] or g[8]["accepted"])
        self.assertUnchanged(rec)


# ----------------------------------------------------------------------------- S3
class TestS3FindingOnTheLine(Pass):
    EVIDENCE = "not a key of the Kafka 4.3 reference"   # the fixture checker's evidence for the item on line 3
    def test_finding_elsewhere_in_the_command_does_not_exempt_a3(self):
        # a multi-line command: the invented item is on line 3; the model edits line 2 quoting that finding
        text = "\n".join(["```sh", "kafka-configs.sh --bootstrap-server b:9092 --entity-type topics --entity-name a \\",
                          "  --alter --add-config log.retention.hours=48", "```"] + FILLER)
        rec, _ = self.run_pass(rep((2, self.EVIDENCE, "kafka-configs.sh --bootstrap-server b:9092 --entity-type topics "
                                                      "--entity-name b \\")), text=text)
        r = self.reasons(rec, 2)
        self.assertTrue(any(x.startswith("A2: quote not found") for x in r), r)
        self.assertUnchanged(rec, text)

    def test_finding_on_the_line_itself_still_exempts(self):
        text = "\n".join(["```sh", "kafka-configs.sh --bootstrap-server b:9092 --entity-type topics --entity-name a \\",
                          "  --alter --add-config log.retention.hours=48", "```"] + FILLER)
        rec, _ = self.run_pass(rep((3, self.EVIDENCE, "  --alter --add-config log.retention.ms=172800000")), text=text)
        p = next(p for p in rec["proposals"] if p["line"] == 3)
        self.assertEqual(p["quote_found"], "finding")
        self.assertFalse([x for x in p["stage1"] if x.startswith(("A2", "A3"))], p["stage1"])


# ----------------------------------------------------------------------------- S4
class TestS4CardPath(Pass):
    CARD_QUOTE = "type: int default: 300000 (5 minutes)"
    CHUNKS2 = CHUNKS[1:]

    def test_card_name_inside_a_longer_name_does_not_tie_the_quote(self):
        text = "\n".join(["The default of `my.max.poll.interval.ms` is 600000.",
                          "`max.poll.interval.ms` bounds the time between polls."] + FILLER)
        rec, _ = self.run_pass(rep((1, self.CARD_QUOTE, "The default of `my.max.poll.interval.ms` is 300000.")),
                               text=text, chunks=self.CHUNKS2, cards=True)
        self.assertGreaterEqual(rec["cards"]["count"], 1)
        self.assertIn("A3: the quote shares no checked item and fewer than two content words with the line",
                      self.reasons(rec, 1))
        self.assertUnchanged(rec, text)

    def test_another_keys_value_from_a_card_is_refused(self):
        text = "\n".join(["`max.poll.interval.ms` defaults to 600000 and `rebalance.timeout.ms` to 90000."] + FILLER)
        rec, _ = self.run_pass(rep((1, self.CARD_QUOTE,
                                    "`max.poll.interval.ms` defaults to 600000 and `rebalance.timeout.ms` to 300000.")),
                               text=text, chunks=self.CHUNKS2, cards=True)
        self.assertEqual(rec["cards"]["count"], 2, rec["cards"])
        self.assertIn("A3: the quote shares no checked item and fewer than two content words with the line",
                      self.reasons(rec, 1))
        self.assertUnchanged(rec, text)

    def test_the_nearest_key_takes_its_own_cards_value(self):
        text = "\n".join(["`max.poll.interval.ms` defaults to 600000 and `rebalance.timeout.ms` to 90000."] + FILLER)
        rec, _ = self.run_pass(rep((1, self.CARD_QUOTE,
                                    "`max.poll.interval.ms` defaults to 300000 and `rebalance.timeout.ms` to 90000.")),
                               text=text, chunks=self.CHUNKS2, cards=True)
        self.assertFalse([x for x in self.reasons(rec, 1) if x.startswith("A3")], rec["proposals"])

    def test_boundaries(self):
        self.assertEqual(vp._name_spans("acks", "set backs=1"), [])
        self.assertEqual(vp._name_spans("acks", "set acks=1"), [(4, 8)])
        self.assertEqual(vp._name_spans("max.poll.interval.ms", "max.poll.interval.ms.x"), [])


# ----------------------------------------------------------------------------- S5
class TestS5Tokens(Pass):
    def fenced(self, cmd, chunk_text, quote, new):
        text = "\n".join(["Run this:", "```sh", cmd, "```"] + FILLER)
        rec, _ = self.run_pass(rep((3, quote, new)), text=text, chunks=CHUNKS + [chunk(1, chunk_text)])
        return rec, text

    def test_short_flag_with_its_value_removed(self):
        rec, text = self.fenced("kubectl delete pods -l app=x -n kafka",
                                "To restart the broker pods run kubectl delete pods -n kafka and wait for them.",
                                "run kubectl delete pods -n kafka", "kubectl delete pods -n kafka")
        self.assertIn("A7: part of the command removed or shortened: -l, app=x", self.reasons(rec, 3))
        self.assertUnchanged(rec, text)

    def test_topic_partition_form_shortened(self):
        # b1-24 spelled another way: the partition goes with the topic
        rec, text = self.fenced(
            "kafka-consumer-groups.sh --bootstrap-server b:9092 --group g --reset-offsets --to-earliest --topic orders:3 "
            "--dry-run",
            "Run kafka-consumer-groups.sh --group g --reset-offsets --to-earliest --topic orders to preview a reset.",
            "--reset-offsets --to-earliest --topic orders to preview",
            "kafka-consumer-groups.sh --bootstrap-server b:9092 --group g --reset-offsets --to-earliest --topic orders "
            "--dry-run")
        self.assertIn("A7: part of the command removed or shortened: orders:3", self.reasons(rec, 3))
        self.assertUnchanged(rec, text)

    def test_inline_code_on_a_prose_line(self):
        old = "Preview first with `kafka-consumer-groups.sh --group g --reset-offsets --to-earliest --topic orders --dry-run`."
        text = "\n".join([old] + FILLER)
        ch = CHUNKS + [chunk(1, "Run kafka-consumer-groups.sh --group g --reset-offsets --to-earliest --topic orders "
                                "to preview a reset.")]
        for gone, new in (("--dry-run", old.replace(" --dry-run", "")),
                          ("--topic, orders", old.replace(" --topic orders", ""))):
            rec, _ = self.run_pass(rep((1, "--reset-offsets --to-earliest --topic orders to preview", new)),
                                   text=text, chunks=ch)
            self.assertIn("A7: part of the inline code removed or shortened: " + gone, self.reasons(rec, 1))
            self.assertUnchanged(rec, text)

    def test_partition_flag_in_inline_code(self):
        old = "Reset one partition with `kafka-consumer-groups.sh --group g --topic orders --partition 3 --execute`."
        text = "\n".join([old] + FILLER)
        ch = CHUNKS + [chunk(1, "Use kafka-consumer-groups.sh --group g --topic orders --execute to reset a group.")]
        rec, _ = self.run_pass(rep((1, "kafka-consumer-groups.sh --group g --topic orders --execute",
                                    old.replace(" --partition 3", ""))), text=text, chunks=ch)
        self.assertIn("A7: part of the inline code removed or shortened: --partition, 3", self.reasons(rec, 1))

    def test_a_corrected_value_is_not_a_removal(self):
        self.assertEqual(vp.lost_tokens("kafka-topics.sh --partitions 3", "kafka-topics.sh --partitions 6", set()), [])
        self.assertEqual(vp.lost_tokens("--topic orders:3", "--topic orders", set()), ["orders:3"])
        self.assertEqual(vp.lost_tokens("--topic orders --bad-flag", "--topic orders", {"--bad-flag"}), [])


# ----------------------------------------------------------------------------- S6
class TestS6QuestionCopy(Pass):
    QS = "Our Streams app runs with processing.guarantee: exactly_once and still writes duplicates. Why?"

    def test_short_configuration_line_from_the_question(self):
        # b2-25 type (synthetic line, 34 characters): the user's own setting is not changed
        text = "\n".join(["Your setting:", "```yaml", "processing.guarantee: exactly_once", "```"] + FILLER)
        ch = CHUNKS + [chunk(1, "Set processing.guarantee: exactly_once_v2 on brokers 2.5 or newer.")]
        rec, _ = self.run_pass(rep((3, "Set processing.guarantee: exactly_once_v2 on brokers",
                                    "processing.guarantee: exactly_once_v2")), text=text, chunks=ch, question=self.QS)
        self.assertIn("A4: the line is in the question word for word", self.reasons(rec, 3))
        self.assertUnchanged(rec, text)

    def test_a_pair_from_the_question_inside_a_longer_line(self):
        self.assertEqual(vp.question_copy("- You set `processing.guarantee=exactly_once` today, keep it.", self.QS),
                         "A4: the line carries the question's setting processing.guarantee word for word")
        self.assertIsNone(vp.question_copy("Set `processing.guarantee: exactly_once_v2` instead.", self.QS))


# ----------------------------------------------------------------------------- S2
class TestS2NothingLosesTheDraft(Pass):
    def test_exception_in_the_first_check(self):
        rec = vp.Verifier(Raising(self.ck, 1), Model(rep())).run(Q, DRAFT, CHUNKS)
        self.assertEqual(rec["r2"]["status"], "error")
        self.assertTrue(rec["r2"]["reasons"][0].startswith("the checking pass failed: RuntimeError: checker broke"))
        self.assertUnchanged(rec)
        self.assertIn("the checking pass failed", vp.summary_line(rec))

    def test_exception_in_stage_2(self):
        rec = vp.Verifier(Raising(self.ck, 2), Model(rep((2, POLL_QUOTE, POLL_FIX)))).run(Q, DRAFT, CHUNKS)
        self.assertEqual(rec["r2"]["status"], "error")
        self.assertUnchanged(rec)
        self.assertFalse(any(p["accepted"] for p in rec["proposals"]))

    def test_exception_in_the_cards(self):
        v = vp.Verifier(self.ck, Model(rep()), cards=True)
        with mock.patch.object(vp.Verifier, "cards_for", side_effect=ValueError("bad card")):
            rec = v.run(Q, DRAFT, CHUNKS)
        self.assertEqual(rec["r2"]["status"], "error")
        self.assertUnchanged(rec)

    def test_profile_keeps_the_draft(self):
        g = Model(DRAFT)
        with mock.patch.object(vp.Verifier, "_rules", side_effect=KeyError("x")):
            g.replies.append(rep((2, POLL_QUOTE, POLL_FIX)))
            th = vp.draft_and_check(Q, prompts.answer_messages(Q, CHUNKS), CHUNKS, g, 2048, vp.Profile(cards=False),
                                    self.ck)
        self.assertEqual(th["text"], DRAFT)
        self.assertEqual(th["record"]["r2"]["status"], "error")


# ----------------------------------------------------------------------------- the call's timeout
class TestCallTimeout(Pass):
    def test_the_call_gets_the_time_left(self):
        m = Model(rep())
        vp.Verifier(self.ck, m).run(Q, DRAFT, CHUNKS, call_deadline_at=time.monotonic() + 50)
        self.assertTrue(0 < m.calls[0]["timeout"] <= 50)

    def test_timeout_leaves_the_draft_not_checked(self):
        m = Model(G.CallTimeout("slow"))
        rec = vp.Verifier(self.ck, m).run(Q, DRAFT, CHUNKS, call_deadline_at=time.monotonic() + 50)
        self.assertEqual(rec["r2"]["status"], "not_checked")
        self.assertUnchanged(rec)
        self.assertIn("not checked: the checking call did not finish within the 300 s limit", vp.summary_line(rec))

    def test_profile_bounds_the_call_by_the_total(self):
        g = Model(DRAFT, rep())
        vp.draft_and_check(Q, prompts.answer_messages(Q, CHUNKS), CHUNKS, g, 2048, vp.Profile(cards=False), self.ck,
                           t_start=time.monotonic() - 100)
        self.assertIsNone(g.calls[0]["timeout"])               # the draft is not bounded
        self.assertTrue(190 < g.calls[1]["timeout"] <= vp.TOTAL_S - 100)

    def test_generator_raises_call_timeout_only_with_a_timeout(self):
        def slow(req):
            raise httpx.ReadTimeout("slow", request=req)
        g = gen(Rec(slow))
        with self.assertRaises(G.CallTimeout):
            g.chat(MSGS, 100, timeout=5)
        with self.assertRaises(G.ServerError) as e:
            g.chat(MSGS, 100)
        self.assertNotIsInstance(e.exception, G.CallTimeout)


# ----------------------------------------------------------------------------- S1
def reply(content, finish="length", **msg):
    return lambda req: httpx.Response(200, json={"model": "m1", "choices": [
        {"message": dict(content=content, **msg), "finish_reason": finish}], "usage": {"completion_tokens": 9}})


def sse(*pieces, finish="length"):
    def respond(req):
        lines = ["data: " + json.dumps({"model": "m1", "choices": [{"delta": {"content": p}}]}) for p in pieces]
        lines.append("data: " + json.dumps({"model": "m1", "choices": [{"delta": {}, "finish_reason": finish}],
                                            "usage": {"completion_tokens": 9}}))
        lines.append("data: [DONE]")
        return httpx.Response(200, text="\n\n".join(lines) + "\n\n", headers={"content-type": "text/event-stream"})
    return respond


class TestS1CutWhileThinking(Pass):
    def test_unclosed_think_is_dropped_and_raises_when_nothing_is_left(self):
        for respond, on_token in ((reply("<think>I should look at the poll loop"), None),
                                  (sse("<think>I should", " look"), lambda s: None)):
            info = {}
            with self.assertRaises(G.EmptyAnswerError):
                gen(Rec(respond)).chat(MSGS, 100, on_token, thinking=True, info=info)
            self.assertTrue(info["cut_while_thinking"])

    def test_no_tags_at_the_limit_is_cut_while_thinking(self):
        # the template opened the block in the prompt: the whole content may be reasoning; it is kept, and marked
        for respond, on_token in ((reply("I should look at the poll loop"), None),
                                  (sse("I should", " look at the poll loop"), lambda s: None)):
            info = {}
            out, _, trunc = gen(Rec(respond)).chat(MSGS, 100, on_token, thinking=True, info=info)
            self.assertEqual(out, "I should look at the poll loop")
            self.assertTrue(info["cut_while_thinking"] and trunc)

    def test_text_before_an_unclosed_think_is_the_answer(self):
        info = {}
        out, _, trunc = gen(Rec(reply("Short answer.\n<think>more thoughts"))).chat(MSGS, 100, thinking=True, info=info)
        self.assertEqual(out, "Short answer.")
        self.assertTrue(info["cut_while_thinking"] and trunc)

    def test_closed_think_and_separate_reasoning_are_not_cut(self):
        info = {}
        out, _, _ = gen(Rec(reply("<think>t</think>The answer, cut"))).chat(MSGS, 100, thinking=True, info=info)
        self.assertEqual((out, info["cut_while_thinking"]), ("The answer, cut", False))
        info = {}
        out, _, _ = gen(Rec(reply("The answer, cut", reasoning_content="t"))).chat(MSGS, 100, thinking=True, info=info)
        self.assertEqual((out, info["cut_while_thinking"]), ("The answer, cut", False))

    def test_thinking_off_is_unchanged(self):
        info = {}
        out, _, _ = gen(Rec(reply("<think>kept as today"))).chat(MSGS, 100, info=info)
        self.assertEqual(out, "<think>kept as today")
        self.assertNotIn("cut_while_thinking", info)

    def test_profile_treats_it_as_no_draft(self):
        def cut(messages, info):
            info["cut_while_thinking"] = True
            raise G.EmptyAnswerError("nothing left")
        for draft in (cut, lambda messages, info: (info.update(cut_while_thinking=True), ("Thinking text", 9, True))[1]):
            g = Model(draft)
            th = vp.draft_and_check(Q, prompts.answer_messages(Q, CHUNKS), CHUNKS, g, 2048, vp.Profile(), self.ck)
            self.assertEqual(len(g.calls), 1)                   # nothing is checked
            self.assertEqual(th["text"], vp.NO_DRAFT)
            self.assertTrue(th["cut_while_thinking"])
            self.assertEqual(th["first_pass_text"], "")
            self.assertEqual(th["record"]["r2"]["status"], "no_draft")
            self.assertNotIn("Thinking text", json.dumps(vp.compact(th["record"])))
            self.assertIn("no draft", vp.summary_line(th["record"]))

    def test_without_thinking_an_empty_answer_still_raises(self):
        def empty(messages, info):
            raise G.EmptyAnswerError("nothing")
        with self.assertRaises(G.EmptyAnswerError):
            vp.draft_and_check(Q, prompts.answer_messages(Q, CHUNKS), CHUNKS, Model(empty), 2048,
                               vp.Profile(thinking=False), self.ck)


# ----------------------------------------------------------------------------- S7
class TestS7SameTransport(Pass):
    def test_every_arm_streams_the_draft(self):
        for prof in (vp.Profile(), vp.Profile(thinking=False), vp.Profile(verify=False)):
            g = Model(DRAFT, rep())
            th = vp.draft_and_check(Q, prompts.answer_messages(Q, CHUNKS), CHUNKS, g, 2048, prof, self.ck)
            self.assertTrue(g.calls[0]["streamed"])
            self.assertTrue(th["draft_streamed"])
            if prof.verify:
                self.assertFalse(g.calls[1]["streamed"])


if __name__ == "__main__":
    unittest.main()
