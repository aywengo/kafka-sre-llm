"""scripts/generate_qa.py pack and pack-verify modes, with a fake LLM or an httpx mock transport."""
import asyncio
import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import httpx

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))
import generate_qa as G  # noqa: E402
from kafka_sre_rag.prompts import pack_text  # noqa: E402

CHUNKS = [{"id": f"c{i}", "source": "src", "license": "x", "path": f"docs/f{i}.md",
           "title": f"Title {i}" if i % 2 else "", "text": f"text of chunk {i}\n\nsecond paragraph {i}"}
          for i in range(4)]
BANK = [
    {"id": "q1", "question": "Why is X slow?", "type": "troubleshooting", "category": "kafka",
     "origin": "seed", "treatment": "normal", "split": "train"},
    {"id": "q2", "question": "How do I set Y?", "type": "howto", "category": "strimzi",
     "origin": "seed", "treatment": "withheld", "split": "train"},
    {"id": "q3", "question": "asdf?", "type": "concept", "category": "kafka",
     "origin": "seed", "treatment": "normal", "split": "valid"},
]
PACKS = [
    {"id": "q1", "k6": ["c0", "c1"], "k3": ["c0"], "reserve": [], "treatment": "normal", "withheld": []},
    {"id": "q2", "k6": ["c2", "c1"], "k3": ["c2"], "reserve": [], "treatment": "withheld", "withheld": ["c3"]},
    {"id": "q3", "k6": ["c0"], "k3": ["c0"], "reserve": [], "treatment": "normal", "withheld": []},
]
GOOD = {"claims": [], "identifiers_not_in_material": [], "causes_with_check_and_fix": 2,
        "coverage": "full", "gap_ok": True, "grounded": True, "reason": "ok"}


class FakeLLM:
    """gen replies by question; verify replies from a list per question (last one repeats)."""

    def __init__(self, verdicts=None, answers=None):
        self.calls = []
        self.verdicts = verdicts or {}
        self.answers = answers or {}

    async def chat_full(self, system, user, **kw):
        self.calls.append((system, user, kw))
        q = user.split("Question: ", 1)[1].split("\n")[0]
        if system == G.PACK_VERIFY_SYSTEM:
            seq = self.verdicts.get(q, [json.dumps(GOOD)])
            n = sum(1 for c in self.calls if c[0] == system and f"Question: {q}\n" in c[1])
            return seq[min(n, len(seq)) - 1], "stop"
        if q == "asdf?":
            return '{"skip": true}', "stop"
        if q in self.answers:
            return self.answers[q]
        return json.dumps({"coverage": "full", "not_covered": "", "answer": f"Answer to {q}"}), "stop"

    async def chat(self, system, user, **kw):
        return (await self.chat_full(system, user, **kw))[0]

    def verify_calls(self):
        return [c for c in self.calls if c[0] == G.PACK_VERIFY_SYSTEM]

    def gen_calls(self):
        return [c for c in self.calls if c[0] == G.PACK_GEN_SYSTEM]


def write_jsonl(path, rows):
    Path(path).write_text("".join(json.dumps(r) + "\n" for r in rows))


def rows_of(path):
    return G.read_jsonl(path)


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        d = Path(self.tmp.name)
        self.d = d
        write_jsonl(d / "index.jsonl", CHUNKS)
        write_jsonl(d / "bank.jsonl", BANK)
        write_jsonl(d / "packs.jsonl", PACKS)

    def args(self, *extra, mode="pack"):
        d = self.d
        base = ["--mode", mode, "--index", str(d / "index.jsonl"), "--out", str(d / "out.jsonl")]
        if mode == "pack":
            base += ["--questions", str(d / "bank.jsonl"), "--packs", str(d / "packs.jsonl")]
        return G.parse_args(base + list(extra))

    def run_pack(self, llm, *extra):
        with contextlib.redirect_stdout(io.StringIO()) as buf:
            asyncio.run(G.run_pack(self.args(*extra), llm, llm))
        return buf.getvalue()


class PackTests(Base):
    def test_generation_message_is_pack_text_exactly(self):
        llm = FakeLLM()
        self.run_pack(llm)
        by_q = {c[1].split("Question: ", 1)[1]: c for c in llm.gen_calls()}
        c = by_q["Why is X slow?"]
        self.assertEqual(c[0], G.PACK_GEN_SYSTEM)
        self.assertEqual(c[1], "<material>\n" + pack_text([CHUNKS[0], CHUNKS[1]]) +
                         "\n</material>\n\nQuestion type: troubleshooting\nQuestion: Why is X slow?")

    def test_verifier_sees_same_material_and_withheld_block_only_on_withheld_rows(self):
        llm = FakeLLM()
        self.run_pack(llm, "--verify")
        v = {c[1].split("\nQuestion: ", 1)[1].split("\n")[0]: c[1] for c in llm.verify_calls()}
        m1 = "<material>\n" + pack_text([CHUNKS[0], CHUNKS[1]]) + "\n</material>\n"
        self.assertEqual(v["Why is X slow?"], m1 + "\nQuestion: Why is X slow?\n\nAnswer:\nAnswer to Why is X slow?")
        gen_material = [c[1] for c in llm.gen_calls() if "Why is X slow?" in c[1]][0]
        self.assertIn(gen_material.split("\n</material>")[0], v["Why is X slow?"])
        self.assertNotIn("<withheld>", v["Why is X slow?"])
        m2 = "<material>\n" + pack_text([CHUNKS[2], CHUNKS[1]]) + "\n</material>\n"
        w = "<withheld>\n" + pack_text([CHUNKS[3]]) + "\n</withheld>\n"
        self.assertEqual(v["How do I set Y?"], m2 + w + "\nQuestion: How do I set Y?\n\nAnswer:\nAnswer to How do I set Y?")
        self.assertEqual(len(v), 2)  # the skip is not verified

    def test_output_rows_resume_and_skip(self):
        llm = FakeLLM()
        self.run_pack(llm, "--verify")
        rows = {r["id"]: r for r in rows_of(self.d / "out.jsonl")}
        self.assertEqual(set(rows), {"q1", "q2", "q3"})
        r = rows["q2"]
        for key in ("id", "question", "type", "category", "origin", "treatment", "split", "k", "chunk_ids",
                    "withheld", "coverage", "not_covered", "answer", "answer_chars", "gen_finish", "skip",
                    "verify"):
            self.assertIn(key, r)
        self.assertEqual((r["k"], r["chunk_ids"], r["withheld"], r["gen_finish"]), ("k6", ["c2", "c1"], ["c3"], "stop"))
        self.assertEqual(r["answer_chars"], len(r["answer"]))
        self.assertTrue(rows["q3"]["skip"])
        self.assertIsNone(rows["q3"]["verify"])
        self.assertEqual(rows["q1"]["verify"], GOOD)
        # resume: nothing new is generated, file unchanged
        llm2 = FakeLLM()
        out = self.run_pack(llm2, "--verify")
        self.assertEqual(llm2.calls, [])
        self.assertIn("0 to process", out)
        self.assertEqual(len(rows_of(self.d / "out.jsonl")), 3)

    def test_resume_processes_only_missing_and_ids_filter(self):
        write_jsonl(self.d / "ids.txt", [])
        (self.d / "ids.txt").write_text("q1\nq2\n")
        llm = FakeLLM()
        self.run_pack(llm, "--ids", str(self.d / "ids.txt"), "--limit", "1")
        self.assertEqual([r["id"] for r in rows_of(self.d / "out.jsonl")], ["q1"])
        self.run_pack(FakeLLM(), "--ids", str(self.d / "ids.txt"))
        self.assertEqual([r["id"] for r in rows_of(self.d / "out.jsonl")], ["q1", "q2"])

    def test_unparsable_verdict_retried_once_then_null(self):
        llm = FakeLLM(verdicts={"Why is X slow?": ["not json", "still not json"],
                                "How do I set Y?": ["garbage", json.dumps(GOOD)]})
        out = self.run_pack(llm, "--verify", "--stats", str(self.d / "stats.md"))
        rows = {r["id"]: r for r in rows_of(self.d / "out.jsonl")}
        self.assertIsNone(rows["q1"]["verify"])
        self.assertEqual(rows["q1"]["verify_error"], "parse")
        self.assertEqual(rows["q2"]["verify"], GOOD)  # second try parsed
        self.assertNotIn("verify_error", rows["q2"])
        self.assertEqual(len([c for c in llm.verify_calls() if "Why is X slow?" in c[1]]), 2)
        self.assertEqual(len([c for c in llm.verify_calls() if "How do I set Y?" in c[1]]), 2)
        self.assertIn("parse failures (verify null, not rejected): 1", out)
        self.assertIn("grounded rate", (self.d / "stats.md").read_text())

    def test_truncation_and_gen_parse_error_recorded(self):
        llm = FakeLLM(answers={"Why is X slow?": ('{"coverage": "full", "answer": "cut o', "length")})
        self.run_pack(llm, "--verify")
        r = {r["id"]: r for r in rows_of(self.d / "out.jsonl")}["q1"]
        self.assertEqual((r["gen_finish"], r["gen_error"], r["answer"]), ("length", "parse", ""))
        self.assertIsNone(r["verify"])
        self.assertEqual(len([c for c in llm.verify_calls() if "Why is X slow?" in c[1]]), 0)

    def test_report_numbers(self):
        out = self.run_pack(FakeLLM(), "--verify")
        self.assertIn("items: 3; skips: 1", out)
        self.assertIn("grounded rate: 2/2", out)
        self.assertIn("verifier coverage agreement: 2/2", out)
        self.assertIn("type=troubleshooting", out)
        self.assertIn("rows with a mechanical problem: 0", out)
        self.assertIn("grounded and mechanical filter passed: 2 (67% of 3 items)", out)

    def test_report_counts_mechanical_problems(self):
        leak = {"Why is X slow?": ('{"coverage": "full", "not_covered": "", "answer": "The documentation says Z."}', "stop"),
                "How do I set Y?": ('{"coverage": "partial", "not_covered": "x", "answer": "Part."}', "stop")}
        out = self.run_pack(FakeLLM(answers=leak), "--verify")
        self.assertIn("rows with a mechanical problem: 2 (gap_sentence_missing=1, leak=1)", out)
        self.assertIn("grounded and mechanical filter passed: 0 (0% of 3 items)", out)
        rows = {r["id"]: r for r in rows_of(self.d / "out.jsonl")}
        self.assertEqual(rows["q1"]["mech"], ["leak:The documentation says"])
        self.assertEqual(rows["q2"]["mech"], ["gap_sentence_missing"])
        self.assertEqual(rows["q3"]["mech"], ["skip"])


class PromptVersionTests(Base):
    def test_new_sentences_in_prompts(self):
        for text in ("If the material contradicts the question's premise",
                     "A premise the material does not mention is not wrong; it is a gap.",
                     "Never say or imply that a field, flag, metric, feature or behaviour",
                     "does not apply, does not propagate or cannot be done",
                     '"there is no ... that I can point to"',
                     "say so only in the gap sentence",
                     "including comments inside code blocks, gives what it names as missing",
                     "the sentence appears exactly once",
                     'a line reading exactly "What it does cover:" followed by those facts',
                     "and stating no technical fact that the material does not support",
                     "direction only (where to look, what kind of thing to check)",
                     "Check and Fix labels are for causes of a symptom only",
                     "steps of a procedure are plain numbered steps",
                     "Number only causes that can produce the symptom",
                     "mention the cause in prose instead of numbering it",
                     "Do not force the troubleshooting shape onto a question that is not about a symptom",
                     "A partial or none answer without that sentence is wrong",
                     "a full answer must not contain it",
                     "never one block of text",
                     '"the docs", "the source", "the sources", "the context", "described here", "given here"',
                     'Return {"skip": true} only if the question is malformed or not a real question',
                     "gets coverage \"none\""):
            self.assertIn(text, " ".join(G.PACK_GEN_SYSTEM.replace("\\\n", "").split()), text)
        self.assertNotIn("is not a real question, return", G.PACK_GEN_SYSTEM)
        v = " ".join(G.PACK_VERIFY_SYSTEM.split())
        for text in ("are not claims",
                     "is not governed by, does not apply, does not propagate to or cannot be done, "
                     "including hedged forms",
                     "they state no technical fact that the material does not support",
                     "refers to the material, the documentation, a table or a source",
                     "or to what is or is not documented or described",
                     "gap_ok is false if what the gap sentence names as missing appears anywhere",
                     "if the answer contains more than one gap sentence",
                     "grounded is false if a numbered cause cannot produce the symptom asked about",
                     "offered to change a resource that another product manages, unless the "
                     "material connects them",
                     "counts only causes that can produce the symptom asked about",
                     "For an answer that is not about a symptom it is 0",
                     'does not contain a sentence of the form "The retrieved documentation does not '
                     'cover ..."'):
            self.assertIn(text, v, text)

    def test_rows_and_report_carry_prompt_version(self):
        self.assertEqual(G.PACK_PROMPT_VERSION, "pack/4")
        out = self.run_pack(FakeLLM(), "--verify", "--stats", str(self.d / "stats.md"))
        rows = rows_of(self.d / "out.jsonl")
        self.assertEqual(len(rows), 3)
        self.assertEqual({r["prompt_version"] for r in rows}, {"pack/4"})
        self.assertTrue(all(isinstance(r["mech"], list) for r in rows))
        self.assertIn("prompt_version: pack/4", (self.d / "stats.md").read_text())

    def test_resume_into_other_version_or_none_exits_and_writes_nothing(self):
        for row in ({"id": "q1", "prompt_version": "pack/1"}, {"id": "q1"}):
            out = self.d / "out.jsonl"
            write_jsonl(out, [row])
            before = out.read_text()
            llm = FakeLLM()
            with self.assertRaises(SystemExit) as cm:
                self.run_pack(llm)
            self.assertIn("prompt_version", str(cm.exception))
            self.assertEqual(llm.calls, [])
            self.assertEqual(out.read_text(), before)


class PackVerifyTests(Base):
    def make_in(self):
        base = {"question": "Q", "chunk_ids": ["c0"], "withheld": [], "skip": False}
        items = [
            {**base, "id": "a", "answer": "A1", "verify": GOOD, "coverage": "full", "type": "howto"},
            {**base, "id": "b", "answer": "A2", "verify": {**GOOD, "grounded": False}, "coverage": "full", "type": "howto"},
            {**base, "id": "c", "answer": "A3", "verify": None, "coverage": "full", "type": "howto"},
            {**base, "id": "d", "answer": "", "verify": None, "skip": True, "coverage": None},
        ]
        write_jsonl(self.d / "in.jsonl", items)

    def run_pv(self, llm, *extra):
        args = G.parse_args(["--mode", "pack-verify", "--in", str(self.d / "in.jsonl"), "--index",
                             str(self.d / "index.jsonl"), "--out", str(self.d / "v2.jsonl"),
                             "--verify-model", "m2", *extra])
        with contextlib.redirect_stdout(io.StringIO()):
            asyncio.run(G.run_pack_verify(args, llm))

    def test_one_row_per_item_resume_and_only_grounded(self):
        self.make_in()
        llm = FakeLLM()
        self.run_pv(llm)
        self.assertEqual([r["id"] for r in rows_of(self.d / "v2.jsonl")], ["a", "b", "c"])  # skip not verified
        self.assertEqual(len(llm.verify_calls()), 3)
        llm2 = FakeLLM()
        self.run_pv(llm2)
        self.assertEqual(llm2.calls, [])
        # only grounded, fresh output
        (self.d / "v2.jsonl").unlink()
        llm3 = FakeLLM()
        self.run_pv(llm3, "--only-grounded")
        self.assertEqual([r["id"] for r in rows_of(self.d / "v2.jsonl")], ["a"])
        self.assertEqual(rows_of(self.d / "v2.jsonl")[0]["verify"], GOOD)

    def test_follow_stops_on_until_file(self):
        self.make_in()
        (self.d / "until").write_text("")
        sleeps = []

        async def fake_sleep(s):
            sleeps.append(s)
        with mock.patch.object(G.asyncio, "sleep", fake_sleep):
            self.run_pv(FakeLLM(), "--follow", "5", "--until-file", str(self.d / "until"))
        self.assertEqual(len(rows_of(self.d / "v2.jsonl")), 3)
        self.assertEqual(sleeps, [])

    def test_follow_waits_then_picks_up_new_rows(self):
        self.make_in()
        items = rows_of(self.d / "in.jsonl")
        sleeps = []

        async def fake_sleep(s):
            sleeps.append(s)
            if len(sleeps) == 1:  # the generator appends an item, then writes the marker
                with open(self.d / "in.jsonl", "a") as f:
                    f.write(json.dumps({**items[0], "id": "e"}) + "\n")
                (self.d / "until").write_text("")
        with mock.patch.object(G.asyncio, "sleep", fake_sleep):
            self.run_pv(FakeLLM(), "--follow", "5", "--until-file", str(self.d / "until"))
        self.assertEqual([r["id"] for r in rows_of(self.d / "v2.jsonl")], ["a", "b", "c", "e"])
        self.assertEqual(sleeps, [5.0])


def llm_with_transport(handler):
    llm = G.LLM("http://secret-host.example:1234/v1", "m", "k", 0.0, 10, {},
                {"calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "reasoning_tokens": 0})
    llm.client = httpx.AsyncClient(base_url="http://secret-host.example:1234/v1",
                                   transport=httpx.MockTransport(handler))
    return llm


class LLMTests(unittest.TestCase):
    def run_chat(self, handler):
        llm = llm_with_transport(handler)
        sleep = mock.AsyncMock()
        buf = io.StringIO()
        with mock.patch.object(G.asyncio, "sleep", sleep), contextlib.redirect_stdout(buf):
            res = asyncio.run(llm.chat_full("s", "u"))
        return res, buf.getvalue(), llm

    def test_retry_log_has_no_url(self):
        n = []

        def handler(req):
            n.append(1)
            return httpx.Response(500, json={}) if len(n) < 3 else httpx.Response(
                200, json={"choices": [{"message": {"content": "hi"}, "finish_reason": "stop"}]})
        (content, finish), log, _ = self.run_chat(handler)
        self.assertEqual((content, finish), ("hi", "stop"))
        self.assertIn("retry after HTTPStatusError 500", log)
        self.assertNotIn("secret-host", log)
        self.assertNotIn("http", log)

    def test_busy_server_is_waited_out(self):
        # 507 (and 429, 503) mean the server is out of capacity for the moment: wait, and do not
        # spend the three ordinary attempts on it
        n = []

        def handler(req):
            n.append(1)
            return httpx.Response(507, json={}) if len(n) <= 5 else httpx.Response(
                200, json={"choices": [{"message": {"content": "hi"}, "finish_reason": "stop"}]})
        (content, finish), log, _ = self.run_chat(handler)
        self.assertEqual((content, finish), ("hi", "stop"))
        self.assertEqual(log.count("server busy (HTTPStatusError 507)"), 5)
        self.assertNotIn("retry after", log)
        self.assertNotIn("secret-host", log)

    def test_busy_server_gives_up_eventually(self):
        def handler(req):
            return httpx.Response(507, json={})
        with self.assertRaises(httpx.HTTPStatusError):
            self.run_chat(handler)

    def test_connect_error_log_has_no_url(self):
        n = []

        def handler(req):
            n.append(1)
            if len(n) < 2:
                raise httpx.ConnectError("boom to http://secret-host.example:1234", request=req)
            return httpx.Response(200, json={"choices": [{"message": {"content": "ok"}}]})
        (content, finish), log, _ = self.run_chat(handler)
        self.assertEqual((content, finish), ("ok", None))
        self.assertIn("retry after ConnectError", log)
        self.assertNotIn("secret-host", log)

    def test_bad_200_bodies_are_retried(self):
        bodies = [httpx.Response(200, content=b"<html>not json</html>"),
                  httpx.Response(200, json={"choices": []}),
                  httpx.Response(200, json={"choices": [{"message": {"content": "fine"}}]})]
        it = iter(bodies)
        (content, _), log, _ = self.run_chat(lambda req: next(it))
        self.assertEqual(content, "fine")
        self.assertEqual(log.count("retry after"), 2)

    def test_bad_body_raises_after_three_attempts_and_chat_returns_string(self):
        n = []

        def handler(req):
            n.append(1)
            return httpx.Response(200, json={"error": "x"})
        llm = llm_with_transport(handler)
        with mock.patch.object(G.asyncio, "sleep", mock.AsyncMock()), contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(KeyError):
                asyncio.run(llm.chat("s", "u"))
        self.assertEqual(len(n), 3)
        llm = llm_with_transport(lambda r: httpx.Response(200, json={"choices": [{"message": {"content": "x"}}]}))
        self.assertEqual(asyncio.run(llm.chat("s", "u")), "x")


class OldModeArgsTests(unittest.TestCase):
    def test_v2_style_command_line_defaults(self):
        a = G.parse_args(["--url", "http://localhost:8080/v1", "--model", "m", "--chunks", "c.jsonl",
                          "--out", "o.jsonl", "--verify", "--concurrency", "4", "--max-per-chunk", "3"])
        self.assertIsNone(a.mode)
        self.assertEqual((a.verify, a.verify_max_tokens, a.max_tokens, a.temperature, a.max_per_chunk),
                         ("batch", 2048, 4096, 0.7, 3))
        self.assertEqual((a.concurrency, a.seed, a.context_neighbours, a.max_reject_rate, a.guard_min_items),
                         (4, 0, 0, 0.25, 100))
        self.assertIsNone(a.reverify)
        self.assertEqual(a.verify_extra_body, a.extra_body)
        self.assertEqual(G.parse_args([]).verify, "none")
        self.assertEqual(G.parse_args(["--verify", "each"]).verify, "each")

    def test_pack_mode_verify_tokens_default_and_override(self):
        self.assertEqual(G.parse_args(["--mode", "pack"]).verify_max_tokens, 4096)
        self.assertEqual(G.parse_args(["--mode", "pack", "--verify-max-tokens", "999"]).verify_max_tokens, 999)
        self.assertEqual(G.parse_args(["--verify-max-tokens", "999"]).verify_max_tokens, 999)


if __name__ == "__main__":
    unittest.main()
