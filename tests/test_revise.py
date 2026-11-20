"""WP2b T2: the revise module (R1 marks, R2 patch with re-check), the streamed gate, ask --check --revise, the service
and the eval2 replay. Offline: stub model, retrieval patched, fixture catalogues. No model server, no GPU."""
import contextlib
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
sys.path.insert(0, str(ROOT / "tests"))
sys.path.insert(0, str(ROOT / "scripts"))
from kafka_sre_rag import __main__ as cli, config, prompts, revise as rv, service  # noqa: E402
from test_ask_check import CHUNKS, make_catalogue_dir  # noqa: E402
from test_service import Running, body, post, sse_events  # noqa: E402

YAML_BAD = "apiVersion: kafka.strimzi.io/v1\nkind: KafkaUser\nspec:\n  quotas:\n    consumer: 5"
ANSWER = "Create the user:\n```yaml\n%s\n```\nThen restart the client." % YAML_BAD       # flagged line: 7
ANSWER_PROSE = "Set `log.retention.hours` on the broker. Then done."                      # flagged line: 1
GOOD_LINE = "    consumerByteRate: 5"


def finding(line, text="--x", level="invented", sentence="", evidence="is not real", type_="cli_flag", suggestion=""):
    return {"level": level, "text": text, "type": type_, "line": line, "sentence": sentence or text, "rule": "r",
            "evidence": evidence, "suggestion": suggestion, "catalogue": None}


def reply(*pairs):
    return json.dumps({"replacements": [{"line": n, "text": t} for n, t in pairs]})


class Model:
    """Stub model for the revise call: records the messages, returns the replies in turn (or raises)."""
    identity = {"backend": "stub", "model": "stub", "adapter": None}
    backend = "stub"

    def __init__(self, *replies):
        self.replies, self.calls = list(replies), []

    def chat(self, messages, max_tokens, on_token=None):
        self.calls.append((messages, max_tokens))
        r = self.replies.pop(0)
        if isinstance(r, Exception):
            raise r
        return r, 7, False


class Cat(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dir = make_catalogue_dir()
        cls.ck = service.AnswerChecker(cls.dir)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.dir, ignore_errors=True)


class TestMarks(unittest.TestCase):
    def test_no_findings_returns_the_same_text(self):
        t = "Plain answer.\n```sh\nls\n```"
        self.assertEqual(rv.mark_in_place(t, []), t)
        self.assertEqual(rv.mark_in_place(t, [finding(1, level="unverified")]), t)

    def test_code_fence_comment_under_the_line_with_its_indent(self):
        t = ANSWER
        out = rv.mark_in_place(t, [finding(7, "spec.quotas.consumer", evidence="`spec.quotas.consumer` is not a field")])
        lines = out.split("\n")
        self.assertEqual(lines[6], "    consumer: 5")
        self.assertEqual(lines[7], "    # not confirmed: 'spec.quotas.consumer' (is not a field)")
        self.assertEqual(lines[8], "```")
        self.assertEqual([l for i, l in enumerate(lines) if i != 7], t.split("\n"))  # nothing else changed

    def test_comment_marker_follows_the_language_and_json_gets_a_note_after_the_fence(self):
        j = "```json\n{\"a\": 1}\n```\nnext"
        out = rv.mark_in_place(j, [finding(2)]).split("\n")
        self.assertEqual(out[:3], ["```json", "{\"a\": 1}", "```"])
        self.assertTrue(out[3].startswith("[not confirmed: `--x`"))
        self.assertEqual(out[4], "next")
        java = "```java\nfoo();\n```"
        self.assertEqual(rv.mark_in_place(java, [finding(2)]).split("\n")[2][:2], "//")

    def test_wrapped_command_gets_the_comment_after_the_whole_command(self):
        t = "```bash\nkafka-topics.sh --bad \\\n  --bootstrap-server b \\\n  --describe\necho done\n```"
        out = rv.mark_in_place(t, [finding(2, "--bad")]).split("\n")
        self.assertEqual(out[1:4], ["kafka-topics.sh --bad \\", "  --bootstrap-server b \\", "  --describe"])
        self.assertTrue(out[4].startswith("# not confirmed: '--bad'"))
        self.assertEqual(out[5], "echo done")

    def test_prose_note_follows_its_sentence(self):
        out = rv.mark_in_place("Set `a.b` now. Then `c` later.", [finding(1, "a.b", sentence="Set `a.b` now.")])
        self.assertEqual(out, "Set `a.b` now. [not confirmed: `a.b` (is not real)] Then `c` later.")

    def test_two_findings_in_one_sentence_share_one_bracket(self):
        s = "Use `--x` and `--y`."
        out = rv.mark_in_place(s, [finding(1, "--x", sentence=s), finding(1, "--y", sentence=s)])
        self.assertEqual(out.count("[not confirmed:"), 1)
        self.assertIn("`--x`", out)
        self.assertIn("`--y`", out)

    def test_table_row_note_goes_under_the_table(self):
        t = "| a | b |\n|---|---|\n| `--x` | y |\nafter"
        out = rv.mark_in_place(t, [finding(3, "--x", sentence="| `--x` | y |")]).split("\n")
        self.assertEqual(out[:3], t.split("\n")[:3])
        self.assertTrue(out[3].startswith("[not confirmed:"))
        self.assertEqual(out[4], "after")


class TestWithCatalogues(Cat):
    def test_r1_on_real_findings(self):
        text = ANSWER + "\n" + ANSWER_PROSE
        res = self.ck.check(text, CHUNKS)
        self.assertEqual(sorted(f["text"] for f in rv.flagged(res)), ["log.retention.hours", "spec.quotas.consumer"])
        out = rv.mark_in_place(text, res["findings"])
        lines = out.split("\n")
        self.assertTrue(lines[7].lstrip().startswith("# not confirmed: 'spec.quotas.consumer'"))
        self.assertIn("on the broker. [not confirmed: `log.retention.hours`", out)

    def test_no_flag_no_model_call_and_text_identical(self):
        m = Model()
        t = "Raise `max.poll.interval.ms` on the consumer."
        rec = rv.Reviser(self.ck.check, m).revise(t, CHUNKS, r2=True)
        self.assertEqual(m.calls, [])
        self.assertEqual(rec["final_text"], t)
        self.assertEqual(rec["r2"]["status"], "not_needed")
        self.assertEqual(rec["first_pass_text"], t)
        self.assertEqual(rec["patches"], [])

    def test_r1_only_makes_no_model_call(self):
        m = Model()
        rec = rv.Reviser(self.ck.check, m).revise(ANSWER, CHUNKS, r2=False)
        self.assertEqual(m.calls, [])
        self.assertEqual(rec["r2"]["status"], "off")
        self.assertEqual(rec["revised_text"], ANSWER)
        self.assertEqual(rec["final_text"], rv.mark_in_place(ANSWER, rec["check_first"]["findings"]))
        self.assertNotEqual(rec["final_text"], ANSWER)
        self.assertEqual(rec["first_pass_text"], ANSWER)

    def test_good_patch_is_accepted(self):
        m = Model(reply((7, GOOD_LINE)))
        rec = rv.Reviser(self.ck.check, m).revise(ANSWER, CHUNKS, r2=True)
        self.assertEqual(rec["r2"]["status"], "accepted", rec["r2"])
        self.assertEqual(rec["final_text"], ANSWER.replace("    consumer: 5", GOOD_LINE))
        self.assertEqual(rec["revised_text"], rec["final_text"])
        self.assertEqual(rec["patches"], [{"line": 7, "was": "    consumer: 5", "text": GOOD_LINE}])
        self.assertEqual(rv.flagged(rec["check_after"]), [])
        self.assertEqual(rec["first_pass_text"], ANSWER)
        self.assertEqual(rec["revise_prompt_sha256"], hashlib.sha256(prompts.REVISE_PROMPT.encode()).hexdigest())
        for k in ("check_s", "r2_s", "recheck_s", "total_s"):
            self.assertIn(k, rec["timings"])
        self.assertEqual(len(m.calls), 1)

    def test_patch_that_adds_an_invented_name_is_rejected_and_r1_is_used(self):
        m = Model(reply((7, "    producerRate: 5")))
        rec = rv.Reviser(self.ck.check, m).revise(ANSWER, CHUNKS, r2=True)
        self.assertEqual(rec["r2"]["status"], "rejected")
        self.assertTrue(any("new invented or wrong_use: spec.quotas.producerRate" in r for r in rec["r2"]["reasons"]))
        self.assertEqual(rec["revised_text"], ANSWER)
        self.assertEqual(rec["final_text"], rv.mark_in_place(ANSWER, rec["check_first"]["findings"]))

    def test_patch_that_leaves_the_flag_is_rejected(self):
        m = Model(reply((7, "    consumer: 6")))
        rec = rv.Reviser(self.ck.check, m).revise(ANSWER, CHUNKS, r2=True)
        self.assertEqual(rec["r2"]["status"], "rejected")
        self.assertTrue(any(r.startswith("flag remains: spec.quotas.consumer") for r in rec["r2"]["reasons"]))
        self.assertIn("# not confirmed", rec["final_text"])

    def test_patch_that_touches_an_unflagged_line_is_rejected_before_the_recheck(self):
        m = Model(reply((7, GOOD_LINE), (2, "kind: KafkaTopic")))
        calls = []
        ck = lambda t, c: (calls.append(t), self.ck.check(t, c))[1]  # noqa: E731
        rec = rv.Reviser(ck, m).revise(ANSWER, CHUNKS, r2=True)
        self.assertEqual(rec["r2"]["status"], "rejected")
        self.assertIn("line 2 is not named by a finding", rec["r2"]["reasons"])
        self.assertEqual(len(calls), 1)  # only the first-pass check: the patch was not even applied
        self.assertEqual(rec["final_text"].count("# not confirmed"), 1)

    def test_malformed_reply_empty_patch_and_model_failure_fall_back_to_r1(self):
        for rep, status, why in (("sorry, here is the fix", "rejected", "not JSON"),
                                 (reply(), "no_patch", None),
                                 ('{"replacements": [{"line": 99, "text": "x"}]}', "rejected", "outside"),
                                 ('{"replacements": [{"line": 7}]}', "rejected", "malformed"),
                                 (RuntimeError("server at http://secret.invalid is down"), "error", "RuntimeError")):
            rec = rv.Reviser(self.ck.check, Model(rep)).revise(ANSWER, CHUNKS, r2=True)
            self.assertEqual(rec["r2"]["status"], status, rep)
            if why:
                self.assertIn(why, " ".join(rec["r2"]["reasons"]))
            self.assertNotIn("secret.invalid", json.dumps(rec["r2"]))
            self.assertEqual(rec["revised_text"], ANSWER)
            self.assertIn("# not confirmed", rec["final_text"])

    def test_reply_in_a_code_fence_is_read(self):
        rec = rv.Reviser(self.ck.check, Model("```json\n" + reply((7, GOOD_LINE)) + "\n```")).revise(
            ANSWER, CHUNKS, r2=True)
        self.assertEqual(rec["r2"]["status"], "accepted")

    def test_prompt_has_excerpts_numbered_answer_and_findings_and_no_other_text(self):
        m = Model(reply((7, GOOD_LINE)))
        rv.Reviser(self.ck.check, m).revise(ANSWER, CHUNKS, r2=True)
        msgs, mx = m.calls[0]
        self.assertEqual(msgs[0], {"role": "system", "content": prompts.SYSTEM_PROMPT})
        u = msgs[1]["content"]
        self.assertIn(CHUNKS[0]["text"], u)
        self.assertIn("7|     consumer: 5", u)
        self.assertIn('"item": "spec.quotas.consumer"', u)
        self.assertIn("consumerByteRate", u)  # the catalogue evidence
        self.assertNotIn("<<", u)
        self.assertEqual(mx, rv.REVISE_MAX_TOKENS)

    def test_record_is_json_serialisable_and_public_drops_texts(self):
        rec = rv.Reviser(self.ck.check, Model(reply((7, GOOD_LINE)))).revise(ANSWER, CHUNKS, r2=True)
        json.dumps(rec)
        pub = rv.public(rec)
        json.dumps(pub)
        for k in ("first_pass_text", "final_text", "revised_text", "check_first"):
            self.assertNotIn(k, pub)
        self.assertEqual(pub["r2"]["status"], "accepted")

    def test_r2_without_a_model_is_an_error(self):
        with self.assertRaises(ValueError):
            rv.Reviser(self.ck.check).revise(ANSWER, CHUNKS, r2=True)


class TestPatchRules(unittest.TestCase):
    CMD = "```sh\nkafka-acls.sh --bad \\\n  --topic t \\\n  --add\nls\n```"

    def test_allowed_lines_are_the_whole_wrapped_command(self):
        self.assertEqual(rv.allowed_lines(self.CMD, [finding(2)]), {2, 3, 4})
        self.assertEqual(rv.allowed_lines(self.CMD, [finding(3)]), {3, 4, 2})
        self.assertEqual(rv.allowed_lines(self.CMD, [finding(5)]), {5})
        self.assertEqual(rv.allowed_lines(self.CMD, [finding(2, level="unverified")]), set())

    def test_only_allowed_lines_may_change(self):
        new = rv.apply_patches(self.CMD, [{"line": 2, "text": "kafka-acls.sh --good \\"}])
        self.assertTrue(rv.only_allowed_changed(self.CMD, new, {2, 3}))
        self.assertFalse(rv.only_allowed_changed(self.CMD, new, {5}))
        new2 = rv.apply_patches(self.CMD, [{"line": 5, "text": "pwd"}])
        self.assertFalse(rv.only_allowed_changed(self.CMD, new2, {2, 3}))

    def test_delete_and_two_line_replacement(self):
        self.assertEqual(rv.apply_patches("a\nb\nc", [{"line": 2, "text": None}]), "a\nc")
        self.assertEqual(rv.apply_patches("a\nb\nc", [{"line": 2, "text": "x\ny"}]), "a\nx\ny\nc")
        self.assertEqual(rv.validate_patches("a\nb", [{"line": 1, "text": "x\ny\nz"}], {1}),
                         ["line 1: replacement longer than two lines"])
        self.assertEqual(rv.validate_patches("a\nb", [{"line": 1, "text": "```"}], {1}),
                         ["line 1: replacement contains a code fence marker"])

    def test_duplicate_line_in_one_reply_is_malformed(self):
        p, why = rv.parse_reply(reply((1, "a"), (1, "b")), 3)
        self.assertIsNone(p)
        self.assertIn("twice", why)


class Gate:
    """A FenceGate wired to a recording check; records what had been shown whenever the check ran."""

    def __init__(self, check_fn=None):
        self.shown, self.at_check, self.checked_texts = [], [], []
        self.fn = check_fn or (lambda t: [])
        self.gate = rv.FenceGate(self.shown.append, self.check, lambda: CHUNKS)

    def check(self, text, chunks):
        self.at_check.append("".join(self.shown))
        self.checked_texts.append(text)
        return {"findings": self.fn(text)}


class TestGate(unittest.TestCase):
    TEXT = "Intro line.\n```sh\nkafka-foo.sh --bad\n```\nOutro."

    def feed(self, g, text, step):
        for i in range(0, len(text), step):
            g.gate.feed(text[i:i + step])
        g.gate.finish()

    def test_fence_is_held_until_its_check_and_never_shown_unchecked(self):
        for step in (1, 2, 3, 7, 1000):
            g = Gate(lambda t: [finding(3, "--bad", sentence="kafka-foo.sh --bad")])
            self.feed(g, self.TEXT, step)
            self.assertEqual(len(g.at_check), 1, step)
            self.assertNotIn("```", g.at_check[0], step)         # nothing of the fence was out when it was checked
            self.assertNotIn("kafka-foo", g.at_check[0], step)
            self.assertIn("Intro line.", g.at_check[0], step)    # prose was already out
            self.assertEqual(g.checked_texts[0], "Intro line.\n```sh\nkafka-foo.sh --bad\n```\n", step)
            self.assertEqual("".join(g.shown),
                             "Intro line.\n```sh\nkafka-foo.sh --bad\n# not confirmed: '--bad' (is not real)\n```\nOutro.",
                             step)

    def test_prose_streams_before_the_answer_ends(self):
        g = Gate()
        g.gate.feed("Hello ")
        g.gate.feed("wor")
        self.assertEqual("".join(g.shown), "Hello wor")
        g.gate.feed("ld\n``")
        self.assertEqual("".join(g.shown), "Hello world\n")   # "``" could still become a fence marker
        g.gate.finish()
        self.assertEqual("".join(g.shown), "Hello world\n``")

    def test_text_without_fences_passes_unchanged_and_is_never_checked(self):
        g = Gate()
        self.feed(g, "a\n\n  b `x` c\n~~ not a fence\n", 3)
        self.assertEqual("".join(g.shown), "a\n\n  b `x` c\n~~ not a fence\n")
        self.assertEqual(g.at_check, [])

    def test_unclosed_fence_is_checked_at_the_end_and_two_fences_two_checks(self):
        g = Gate()
        self.feed(g, "x\n```\none\n```\ny\n~~~yaml\na: b", 4)
        self.assertEqual(len(g.at_check), 2)
        self.assertEqual("".join(g.shown), "x\n```\none\n```\ny\n~~~yaml\na: b")
        self.assertEqual(g.at_check[1], "x\n```\none\n```\ny\n")

    def test_indented_fence_and_line_numbers_of_a_second_fence(self):
        seen = []

        def fn(text):
            seen.append(text)
            return [finding(8, "--bad", sentence="bad line")] if text.count("```") >= 4 else []
        g = Gate(fn)
        self.feed(g, "a\n```\nx\n```\nb\n  ```sh\nok\nbad line\n  ```\nc\n", 5)
        out = "".join(g.shown)
        self.assertIn("bad line\n# not confirmed", out)
        self.assertNotIn("not confirmed: '--bad'", out.split("bad line")[0])

    def test_a_failing_check_withholds_the_fence(self):
        def boom(text, chunks):
            raise RuntimeError("x")
        shown = []
        g = rv.FenceGate(shown.append, boom, lambda: [])
        g.feed("a\n```\nsecret\n```\nb")
        g.finish()
        self.assertEqual("".join(shown), "a\n" + rv.FenceGate.WITHHELD + "b")
        self.assertNotIn("secret", "".join(shown))


class TestCorrections(unittest.TestCase):
    def test_block(self):
        b = rv.corrections_block([{"line": 6, "was": "  a", "text": "  b"}, {"line": 9, "was": "x", "text": None}])
        self.assertEqual(b.split("\n")[1:], ["Line 6 was:    a", "Line 6 now:    b", "Line 9 was:  x", "Line 9 now:  (removed)"])
        self.assertEqual(rv.corrections_block([]), "")


class AnsGen:
    """Search terms, then the answer (streamed in pieces), then the replies of the revise call."""
    identity = {"backend": "stub", "model": "stub", "adapter": None}
    backend = "stub"

    def __init__(self, text, *replies):
        self.text, self.replies, self.calls, self.n = text, list(replies), [], 0

    def chat(self, messages, max_tokens, on_token=None):
        self.n += 1
        self.calls.append(messages)
        if self.n == 1:
            return "terms", 1, False
        if self.n == 2:
            if on_token:
                for i in range(0, len(self.text), 5):
                    on_token(self.text[i:i + 5])
            return self.text, 5, False
        r = self.replies.pop(0)
        if isinstance(r, Exception):
            raise r
        return r, 5, False


@mock.patch("kafka_sre_rag.retrieval.select", return_value=CHUNKS)
class TestServiceRevise(Cat):
    def test_no_flag_no_change_and_no_extra_call(self, sel):
        g = AnsGen(ANSWER)
        with Running(g) as s:
            d = post(s.url, body()).json()
        self.assertEqual(d["choices"][0]["message"]["content"], ANSWER)
        self.assertNotIn("check", d["kafka_sre"])
        self.assertEqual(g.n, 2)

    def test_check_gives_r1_marks_and_no_model_call(self, sel):
        g = AnsGen(ANSWER)
        with Running(g, catalogues=str(self.dir)) as s:
            d = post(s.url, body(check=True)).json()
        c = d["choices"][0]["message"]["content"]
        self.assertEqual(g.n, 2)
        self.assertIn("# not confirmed: 'spec.quotas.consumer'", c)
        self.assertTrue(c.endswith("\n\n" + d["kafka_sre"]["check"]["note"]))
        self.assertNotIn("revise", d["kafka_sre"])

    def test_revise_accepts_a_good_patch_non_streamed(self, sel):
        g = AnsGen(ANSWER, reply((7, GOOD_LINE)))
        with Running(g, catalogues=str(self.dir)) as s:
            d = post(s.url, body(revise=True)).json()
        c = d["choices"][0]["message"]["content"]
        self.assertEqual(g.n, 3)
        self.assertTrue(c.startswith(ANSWER.replace("    consumer: 5", GOOD_LINE)))
        self.assertNotIn("not confirmed", c.split("\n\n")[0])
        self.assertEqual(d["kafka_sre"]["revise"]["r2"]["status"], "accepted")
        self.assertNotIn("final_text", d["kafka_sre"]["revise"])
        self.assertTrue(c.endswith(d["kafka_sre"]["check"]["note"]))  # the note describes the text shown
        self.assertNotIn("Could not confirm", c)

    def test_revise_rejected_patch_keeps_r1(self, sel):
        g = AnsGen(ANSWER, reply((2, "kind: X")))
        with Running(g, catalogues=str(self.dir)) as s:
            d = post(s.url, body(revise=True)).json()
        c = d["choices"][0]["message"]["content"]
        self.assertEqual(d["kafka_sre"]["revise"]["r2"]["status"], "rejected")
        self.assertIn("# not confirmed: 'spec.quotas.consumer'", c)
        self.assertIn("kind: KafkaUser", c)

    def test_revise_request_rules(self, sel):
        with Running(AnsGen(ANSWER), catalogues=str(self.dir)) as s:
            self.assertEqual(post(s.url, body(revise="yes")).status_code, 400)
            r = post(s.url, body(revise=True, check=False))
            self.assertEqual(r.status_code, 400)
            self.assertIn("revise needs the check", r.json()["error"]["message"])

    def test_revise_model_failure_keeps_the_answer(self, sel):
        g = AnsGen(ANSWER, RuntimeError("down"))
        with Running(g, catalogues=str(self.dir)) as s:
            d = post(s.url, body(revise=True)).json()
        self.assertEqual(d["kafka_sre"]["revise"]["r2"]["status"], "error")
        self.assertIn("# not confirmed", d["choices"][0]["message"]["content"])

    def test_serve_revise_default(self, sel):
        g = AnsGen(ANSWER, reply((7, GOOD_LINE)))
        with Running(g, revise=True, catalogues=str(self.dir)) as s:
            d = post(s.url, body()).json()
        self.assertEqual(d["kafka_sre"]["revise"]["r2"]["status"], "accepted")

    def events(self, r):
        ev = sse_events(r.text)
        self.assertEqual(ev[-1], "[DONE]")
        return [json.loads(e) for e in ev[:-1]]

    def test_stream_prose_as_written_fence_with_marks_then_note_then_corrections(self, sel):
        text = ANSWER + "\n" + ANSWER_PROSE
        g = AnsGen(text, reply((7, GOOD_LINE), (10, "Set `log.retention.ms` on the broker. Then done.")))
        with Running(g, catalogues=str(self.dir)) as s:
            r = post(s.url, body(stream=True, revise=True))
        objs = self.events(r)
        pieces = [o["choices"][0]["delta"].get("content") or "" for o in objs]
        streamed = "".join(pieces)
        # the prose of the answer is as written (no inline mark), the fence carries its R1 mark
        self.assertTrue(streamed.startswith(
            "Create the user:\n```yaml\n" + YAML_BAD + "\n    # not confirmed: 'spec.quotas.consumer'"))
        self.assertIn("\n```\nThen restart the client.\n" + ANSWER_PROSE, streamed)
        i_note, i_corr = streamed.index("Could not confirm"), streamed.index("Corrections to the answer above")
        self.assertLess(streamed.index(ANSWER_PROSE), i_note)
        self.assertLess(i_note, i_corr)                                         # the corrections follow the note
        self.assertTrue(streamed.endswith("Line 10 now:  Set `log.retention.ms` on the broker. Then done."))  # last
        self.assertEqual(objs[-1]["choices"][0]["finish_reason"], "stop")
        self.assertEqual(objs[-1]["kafka_sre"]["revise"]["r2"]["status"], "accepted")

    def test_stream_never_shows_an_unchecked_fence(self, sel):
        text = "Intro.\n" + "```yaml\n" + YAML_BAD + "\n```\nEnd."
        checked_at = []
        real = service.AnswerChecker.check

        def spy(self_, t, chunks):
            checked_at.append(len(t))
            return real(self_, t, chunks)
        with mock.patch.object(service.AnswerChecker, "check", spy), Running(AnsGen(text), catalogues=str(self.dir)) as s:
            r = post(s.url, body(stream=True, check=True))
        objs = self.events(r)
        content = [o["choices"][0]["delta"].get("content") or "" for o in objs]
        first_fence_piece = next(i for i, c in enumerate(content) if "```" in c or "kind:" in c)
        # the fence arrives as one piece, with its mark, never in fragments before its check
        self.assertIn("apiVersion: kafka.strimzi.io/v1", content[first_fence_piece])
        self.assertIn("# not confirmed", content[first_fence_piece])
        self.assertTrue(content[first_fence_piece].startswith("```yaml\n"))
        self.assertIn("\n```\n", content[first_fence_piece])  # opening and closing line in the same piece
        self.assertGreaterEqual(len(checked_at), 2)  # the fence check, then the whole answer

    def test_stream_without_revise_has_no_corrections(self, sel):
        with Running(AnsGen(ANSWER), catalogues=str(self.dir)) as s:
            r = post(s.url, body(stream=True, check=True))
        streamed = "".join(o["choices"][0]["delta"].get("content") or "" for o in self.events(r))
        self.assertNotIn("Corrections", streamed)
        self.assertIn("# not confirmed", streamed)

    def test_stream_unchecked_request_is_unchanged(self, sel):
        with Running(AnsGen(ANSWER), catalogues=str(self.dir)) as s:
            r = post(s.url, body(stream=True))
        self.assertEqual("".join(o["choices"][0]["delta"].get("content") or "" for o in self.events(r)), ANSWER)


class RecGen:
    identity = {"backend": "stub", "model": "stub", "adapter": None}
    backend = "stub"

    def __init__(self, answer, *replies):
        self.answer, self.replies, self.calls = answer, list(replies), []

    def chat(self, messages, max_tokens, on_token=None):
        self.calls.append(json.dumps(messages, sort_keys=True))
        n = len(self.calls)
        if n == 1:
            return "terms", 1, False
        if n == 2:
            if on_token:
                on_token(self.answer)
            return self.answer, 5, False
        return self.replies.pop(0), 5, False


class TestCli(Cat):
    def run_cli(self, argv, answer, *replies):
        a = cli.build_parser().parse_args(["ask", "How do I check lag?", *argv])
        gen, out = RecGen(answer, *replies), io.StringIO()
        ck = service.AnswerChecker(a.catalogues) if a.check else None
        with mock.patch("kafka_sre_rag.retrieval.select", return_value=CHUNKS), contextlib.redirect_stdout(out):
            cli.run_one(a.question, gen, config.load(), a, ck)
        return gen, out.getvalue()

    def test_no_flag_prompts_identical(self):
        g0, o0 = self.run_cli([], ANSWER)
        g1, o1 = self.run_cli(["--check", "--catalogues", str(self.dir)], ANSWER)
        self.assertEqual(g0.calls, g1.calls)
        self.assertEqual(o0.strip(), ANSWER)
        self.assertEqual(len(g1.calls), 2)  # R1 makes no model call

    def test_check_gives_r1_marks_inline_and_the_note(self):
        _, o = self.run_cli(["--check", "--catalogues", str(self.dir)], ANSWER)
        self.assertIn("    # not confirmed: 'spec.quotas.consumer'", o)
        self.assertLess(o.index("# not confirmed"), o.index("Could not confirm"))

    def test_check_revise_json_text_is_the_patched_answer(self):
        g, o = self.run_cli(["--check", "--revise", "--json", "--catalogues", str(self.dir)], ANSWER,
                            reply((7, GOOD_LINE)))
        d = json.loads(o)
        self.assertEqual(len(g.calls), 3)
        self.assertEqual(d["text"], ANSWER.replace("    consumer: 5", GOOD_LINE))
        self.assertEqual(d["first_pass_text"], ANSWER)
        self.assertEqual(d["revise"]["r2"]["status"], "accepted")

    def test_live_output_shows_the_first_pass_with_fence_marks_and_the_patch_as_corrections(self):
        g, o = self.run_cli(["--check", "--revise", "--catalogues", str(self.dir)], ANSWER, reply((7, GOOD_LINE)))
        self.assertEqual(len(g.calls), 3)
        self.assertIn("    # not confirmed: 'spec.quotas.consumer'", o)
        self.assertLess(o.index("Could not confirm"), o.index("Corrections to the answer above"))
        self.assertTrue(o.rstrip().endswith("Line 7 now:  " + GOOD_LINE))

    def test_check_revise_json_record(self):
        _, o = self.run_cli(["--check", "--revise", "--json", "--catalogues", str(self.dir)], ANSWER,
                            reply((7, "    producerRate: 5")))
        d = json.loads(o)
        self.assertEqual(d["first_pass_text"], ANSWER)
        self.assertEqual(d["revise"]["r2"]["status"], "rejected")
        self.assertIn("# not confirmed", d["text"])

    def test_stream_holds_the_fence_and_corrections_follow(self):
        _, o = self.run_cli(["--check", "--revise", "--stream", "--catalogues", str(self.dir)], ANSWER,
                            reply((7, GOOD_LINE)))
        self.assertIn("    # not confirmed: 'spec.quotas.consumer'", o)
        self.assertLess(o.index("Then restart the client."), o.index("Could not confirm"))
        self.assertLess(o.index("Could not confirm"), o.index("Corrections to the answer above"))

    def test_revise_needs_check_and_dry_run_is_unchanged(self):
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            self.assertEqual(cli.main(["ask", "--dry-run", "--revise", "q"]), 2)
        self.assertIn("--revise needs --check", err.getvalue())
        a = cli.build_parser().parse_args(["ask", "q"])
        self.assertFalse(a.revise)


class TestEval2Replay(Cat):
    def setUp(self):
        import eval2
        self.eval2 = eval2
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, True)
        items = [{"id": "i1", "tier": "A", "kind": "k", "family": "f1", "domain": "kafka", "question": "q1?"},
                 {"id": "i2", "tier": "A", "kind": "k", "family": "f2", "domain": "kafka", "question": "q2?"}]
        eval2.write_jsonl(self.tmp / "items.jsonl", items)
        rows = [{"id": "i1", "text": ANSWER, "pack": ["a"], "error": None, "run": "r"},
                {"id": "i2", "text": "Raise `max.poll.interval.ms` now.", "pack": ["a"], "error": None, "run": "r"}]
        eval2.write_jsonl(self.tmp / "ans.jsonl", rows)
        m = mock.patch("kafka_sre_rag.evalhooks.chunks_by_ids", side_effect=lambda ids, index_path=None: CHUNKS)
        m.start()
        self.addCleanup(m.stop)

    def run_replay(self, *extra, gen=None):
        a = self.eval2.build_parser().parse_args(
            ["answer", "--items", str(self.tmp / "items.jsonl"), "--config", "rev", "--revise-from",
             str(self.tmp / "ans.jsonl"), "--catalogue", str(self.dir), "--backend", "stub", *extra])
        buf = io.StringIO()
        with mock.patch.object(self.eval2, "make_generator", return_value=gen):
            rc = self.eval2.cmd_answer(a, out=buf)
        return rc, buf.getvalue()

    def test_r1_replay_writes_records_next_to_the_answers_and_leaves_the_answers_alone(self):
        before = (self.tmp / "ans.jsonl").read_bytes()
        rc, out = self.run_replay()
        self.assertEqual(rc, 0, out)
        self.assertEqual((self.tmp / "ans.jsonl").read_bytes(), before)
        recs = self.eval2.read_jsonl(self.tmp / "ans.revise.jsonl")
        self.assertEqual([r["id"] for r in recs], ["i1", "i2"])
        self.assertEqual(recs[0]["first_pass_text"], ANSWER)
        self.assertEqual(recs[0]["flagged"], 1)
        self.assertIn("# not confirmed", recs[0]["final_text"])
        self.assertEqual(recs[1]["final_text"], "Raise `max.poll.interval.ms` now.")
        self.assertEqual(recs[0]["r2"]["status"], "off")
        self.assertIn("[revise] 1/2 id=i1 flagged=1", out)
        man = self.eval2.read_manifest(self.tmp / "ans.revise.jsonl")
        self.assertEqual(man["revise_prompt_sha256"], rv.prompt_sha256())
        self.assertFalse(man["r2"])

    def test_r2_replay_calls_the_model_only_for_flagged_answers_and_resumes(self):
        m = Model(reply((7, GOOD_LINE)))
        rc, out = self.run_replay("--r2", gen=m)
        self.assertEqual(rc, 0, out)
        self.assertEqual(len(m.calls), 1)
        recs = self.eval2.read_jsonl(self.tmp / "ans.revise.jsonl")
        self.assertEqual([r["r2"]["status"] for r in recs], ["accepted", "not_needed"])
        self.assertEqual(recs[0]["final_text"], ANSWER.replace("    consumer: 5", GOOD_LINE))
        rc, out = self.run_replay("--r2", gen=Model())
        self.assertIn("2 already done, 0 to do", out)

    def test_out_may_not_be_the_stored_answers_and_missing_answers_are_refused(self):
        with self.assertRaises(self.eval2.EvalError):
            self.run_replay("--out", str(self.tmp / "ans.jsonl"))
        self.eval2.write_jsonl(self.tmp / "ans.jsonl", [{"id": "i1", "text": ANSWER, "pack": ["a"]}])
        with self.assertRaises(self.eval2.EvalError):
            self.run_replay()


class TestPromptsFrozen(unittest.TestCase):
    def test_measured_prompts_unchanged_and_new_constant_is_separate(self):
        cfg = config.load()
        for n, h in config.prompt_hashes().items():
            self.assertEqual(h, cfg["prompts_sha256"][n])
        self.assertNotIn("REVISE_PROMPT", config.PROMPT_NAMES)
        for placeholder in ("<<EXCERPTS>>", "<<ANSWER>>", "<<FINDINGS>>"):
            self.assertEqual(prompts.REVISE_PROMPT.count(placeholder), 1)


if __name__ == "__main__":
    unittest.main()
