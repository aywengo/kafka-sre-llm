"""M7 P1: thinking per call on the generators. With thinking left at its default the request body is today's, byte for
byte; thinking True or False sets chat_template_kwargs.enable_thinking for that call only; what the server returns
as reasoning stays out of the answer text and its size is recorded. Offline (httpx.MockTransport); no server."""
import json
import sys
import unittest
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from kafka_sre_rag import generators as G  # noqa: E402

MSGS = [{"role": "system", "content": "s — é"}, {"role": "user", "content": "q"}]
EB = {"chat_template_kwargs": {"enable_thinking": False, "other": 1}, "top_p": 1}


def ok(content="Answer.", **extra):
    msg = {"content": content}
    msg.update(extra.pop("message", {}))
    return httpx.Response(200, json={"model": "m1", "choices": [{"message": msg, "finish_reason": "stop"}],
                                     "usage": {"completion_tokens": 7, **extra.pop("usage", {})}})


class Rec:
    def __init__(self, respond=None):
        self.bodies, self.respond = [], respond or (lambda req: ok())

    def __call__(self, req):
        if req.url.path.endswith("/models"):
            return httpx.Response(404)
        self.bodies.append(req.content)
        return self.respond(req)


def gen(rec, extra_body=None):
    g = G.OpenAIChatGenerator("http://example.invalid/v1", "m1", api_key="k", extra_body=extra_body)
    g.client = httpx.Client(transport=httpx.MockTransport(rec))
    return g


def todays_bytes(body):
    """The bytes httpx sends for today's body (the literal the generator built before M7, through the same client)."""
    seen = []
    c = httpx.Client(transport=httpx.MockTransport(lambda req: (seen.append(req.content), httpx.Response(200))[1]))
    c.post("http://example.invalid/v1/chat/completions", json=body)
    return seen[0]


class TodaysRequest(unittest.TestCase):
    def test_default_body_is_todays_byte_for_byte(self):
        for eb in (None, EB, {"chat_template_kwargs": {"thinking": False}}):
            r = Rec()
            gen(r, eb).chat(MSGS, 2048)
            want = todays_bytes({"model": "m1", "messages": MSGS, "max_tokens": 2048, "temperature": 0, **(eb or {})})
            self.assertEqual(r.bodies, [want])

    def test_default_streamed_body_is_todays_byte_for_byte(self):
        def sse(req):
            return httpx.Response(200, text='data: {"model": "m1", "choices": [{"delta": {"content": "A"}, '
                                            '"finish_reason": "stop"}]}\n\ndata: [DONE]\n\n')
        r = Rec(sse)
        out = gen(r, EB).chat(MSGS, 300, on_token=lambda s: None)
        self.assertEqual(out[0], "A")
        want = {"model": "m1", "messages": MSGS, "max_tokens": 300, "temperature": 0, **EB}
        want.update(stream=True, stream_options={"include_usage": True})
        self.assertEqual(r.bodies, [todays_bytes(want)])

    def test_thinking_off_with_the_usual_extra_body_is_todays_request(self):
        # the runs of this repository send {"chat_template_kwargs": {"enable_thinking": false}}; under the profile the
        # search-terms call and the checking call (thinking=False) then send exactly today's bytes
        eb = {"chat_template_kwargs": {"enable_thinking": False}}
        r1, r2 = Rec(), Rec()
        gen(r1, eb).chat(MSGS, 300)
        gen(r2, eb).chat(MSGS, 300, thinking=False)
        self.assertEqual(r1.bodies, r2.bodies)

    def test_info_changes_nothing_in_the_request(self):
        r1, r2 = Rec(), Rec()
        gen(r1, EB).chat(MSGS, 100)
        gen(r2, EB).chat(MSGS, 100, info={})
        self.assertEqual(r1.bodies, r2.bodies)

    def test_request_body_default(self):
        g = gen(Rec(), EB)
        self.assertEqual(g.request_body(MSGS, 9), {"model": "m1", "messages": MSGS, "max_tokens": 9, "temperature": 0, **EB})


class PerCall(unittest.TestCase):
    def test_thinking_on_and_off_per_call_keep_the_other_kwargs(self):
        r = Rec()
        g = gen(r, EB)
        g.chat(MSGS, 10, thinking=True)
        g.chat(MSGS, 10, thinking=False)
        g.chat(MSGS, 10)
        bodies = [json.loads(b) for b in r.bodies]
        self.assertEqual(bodies[0]["chat_template_kwargs"], {"enable_thinking": True, "other": 1})
        self.assertEqual(bodies[1]["chat_template_kwargs"], {"enable_thinking": False, "other": 1})
        self.assertEqual(bodies[2]["chat_template_kwargs"], EB["chat_template_kwargs"])
        self.assertEqual(g.extra_body, EB)   # the generator's own setting is not changed by a call
        self.assertEqual(bodies[0]["top_p"], 1)

    def test_thinking_without_extra_body(self):
        r = Rec()
        gen(r).chat(MSGS, 10, thinking=False)
        self.assertEqual(json.loads(r.bodies[0])["chat_template_kwargs"], {"enable_thinking": False})


class Reasoning(unittest.TestCase):
    def test_reasoning_content_stays_out_and_is_counted(self):
        r = Rec(lambda req: ok("<think>inline</think>\n\nThe answer.", message={"reasoning_content": "long thoughts"},
                               usage={"completion_tokens_details": {"reasoning_tokens": 42}, "prompt_tokens": 5}))
        info = {}
        text, toks, trunc = gen(r).chat(MSGS, 10, thinking=True, info=info)
        self.assertEqual((text, toks, trunc), ("The answer.", 7, False))
        self.assertEqual(info["reasoning_tokens"], 42)
        self.assertEqual(info["reasoning_chars"], len("long thoughts") + len("inline"))
        self.assertTrue(info["reasoning_inline"])
        self.assertIs(info["thinking_requested"], True)
        self.assertIsNone(info["think_s"])   # only a streamed call can time the thinking

    def test_reasoning_tokens_none_when_not_reported(self):
        info = {}
        gen(Rec()).chat(MSGS, 10, info=info)
        self.assertIsNone(info["reasoning_tokens"])
        self.assertEqual(info["reasoning_chars"], 0)

    def test_reply_that_starts_inside_a_think_block(self):
        r = Rec(lambda req: ok("thoughts go here</think>\n\nThe answer."))
        self.assertEqual(gen(r).chat(MSGS, 10, thinking=True)[0], "The answer.")
        # without the per-call switch the text is handled exactly as before (strip_think only)
        self.assertEqual(gen(r).chat(MSGS, 10)[0], G.strip_think("thoughts go here</think>\n\nThe answer."))

    def test_streamed_reasoning_is_timed_and_kept_out(self):
        events = [{"choices": [{"delta": {"reasoning_content": "step one "}}]},
                  {"choices": [{"delta": {"reasoning_content": "step two"}}]},
                  {"choices": [{"delta": {"content": "The "}}]},
                  {"choices": [{"delta": {"content": "answer."}, "finish_reason": "stop"}]},
                  {"choices": [], "usage": {"completion_tokens": 30,
                                            "completion_tokens_details": {"reasoning_tokens": 20}}}]
        body = "".join("data: %s\n\n" % json.dumps(dict(e, model="m1")) for e in events) + "data: [DONE]\n\n"
        shown, info = [], {}
        text, toks, trunc = gen(Rec(lambda req: httpx.Response(200, text=body))).chat(
            MSGS, 10, on_token=shown.append, thinking=True, info=info)
        self.assertEqual((text, toks), ("The answer.", 30))
        self.assertEqual("".join(shown), "The answer.")
        self.assertEqual((info["reasoning_tokens"], info["reasoning_chars"]), (20, len("step one step two")))
        self.assertIsNotNone(info["think_s"])
        self.assertGreaterEqual(info["think_s"], 0)

    def test_empty_answer_after_long_thinking_is_an_error(self):
        r = Rec(lambda req: httpx.Response(200, json={"model": "m1", "choices": [
            {"message": {"content": "", "reasoning_content": "x" * 50}, "finish_reason": "length"}]}))
        with self.assertRaises(G.EmptyAnswerError):
            gen(r).chat(MSGS, 10, thinking=True)


class OpenThink(unittest.TestCase):
    def test_open_think(self):
        self.assertEqual(G.open_think("a</think>b"), "<think>a</think>b")
        self.assertEqual(G.open_think("<think>a</think>b"), "<think>a</think>b")
        self.assertEqual(G.open_think("plain"), "plain")


if __name__ == "__main__":
    unittest.main()
