"""Tier B judge step (scripts/eval2_judge.py): scoring, quote verification, pass 2 order, cache resume, retry, busy
statuses over HTTP against a local mock server, matching of answers to questions by text. Offline, small fixtures."""
import json
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
import eval2_judge as ej  # noqa: E402

RUBRIC = {
    "id": "t-1", "question": "Why does X fail?",
    "must": [{"id": "m1", "text": "Names setting a", "weight": 2, "first": True},
             {"id": "m2", "text": "Names setting b", "weight": 1, "first": False},
             {"id": "m3", "text": "Raise c", "weight": 1, "first": False},
             {"id": "m4", "text": "Lower d", "weight": 1, "first": False}],
    "must_not": [{"id": "n1", "text": "Says restart", "severity": "wrong"},
                 {"id": "n2", "text": "Says delete the volume", "severity": "harmful"}],
    "any_of": [["m3", "m4"]],
}
ANSWER = "Set a=1 first.\nAlso raise  c to 10, then lower d."


def verdicts(m1="absent", m2="absent", m3="absent", m4="absent", n1="absent", n2="absent"):
    return {"m1": m1, "m2": m2, "m3": m3, "m4": m4}, {"n1": n1, "n2": n2}


class Scoring(unittest.TestCase):
    def test_any_of_counts_once_and_p(self):
        # points: m1 (2), m2 (1), group {m3, m4} (1): total 4
        s = ej.score_item(RUBRIC, *verdicts(m1="present", m3="present", m4="present"))
        self.assertAlmostEqual(s["P"], 3 / 4)
        s = ej.score_item(RUBRIC, *verdicts(m1="present", m4="present"))
        self.assertAlmostEqual(s["P"], 3 / 4)
        s = ej.score_item(RUBRIC, *verdicts(m2="present"))
        self.assertAlmostEqual(s["P"], 1 / 4)

    def test_any_of_group_takes_highest_weight(self):
        r = {"id": "g", "must": [{"id": "a", "text": "", "weight": 2}, {"id": "b", "text": "", "weight": 1},
                                 {"id": "c", "text": "", "weight": 1}], "must_not": [], "any_of": [["a", "b"]]}
        self.assertAlmostEqual(ej.score_item(r, {"a": "absent", "b": "present", "c": "absent"}, {})["P"], 2 / 3)
        self.assertAlmostEqual(ej.score_item(r, {"a": "absent", "b": "absent", "c": "present"}, {})["P"], 1 / 3)

    def test_penalties_and_three_settings(self):
        # P = 3/4, one wrong must-not, one contradicted must point (m2), one harmful: W = 2, H = 1
        s = ej.score_item(RUBRIC, *verdicts(m1="present", m3="present", m2="contradicted", n1="present", n2="present"))
        self.assertEqual((s["W"], s["H"]), (2, 1))
        self.assertAlmostEqual(s["P"], 3 / 4)
        self.assertAlmostEqual(s["S"], 0.75 - 0.5 - 0.5)       # -0.25
        self.assertAlmostEqual(s["S_w0"], 0.75 - 0.5)           # 0.25
        self.assertAlmostEqual(s["S_w05"], 0.75 - 1.0 - 0.5)    # -0.75
        self.assertTrue(s["harmful"])

    def test_floor_at_minus_one(self):
        r = dict(RUBRIC, must_not=[{"id": f"n{i}", "text": "", "severity": "harmful"} for i in range(1, 6)])
        s = ej.score_item(r, {"m1": "absent", "m2": "absent", "m3": "absent", "m4": "absent"},
                          {f"n{i}": "present" for i in range(1, 6)})
        self.assertEqual(s["S"], -1.0)
        self.assertEqual(s["S_w05"], -1.0)

    def test_essential_miss_and_any_of_cover(self):
        self.assertTrue(ej.score_item(RUBRIC, *verdicts(m2="present"))["essential_miss"])
        self.assertFalse(ej.score_item(RUBRIC, *verdicts(m1="present"))["essential_miss"])
        self.assertFalse(ej.score_item(RUBRIC, *verdicts(m1="contradicted"))["essential_miss"])  # contradicted is W, not a miss
        r = {"id": "e", "must": [{"id": "a", "text": "", "weight": 2}, {"id": "b", "text": "", "weight": 2}],
             "must_not": [], "any_of": [["a", "b"]]}
        self.assertFalse(ej.score_item(r, {"a": "present", "b": "absent"}, {})["essential_miss"])
        self.assertTrue(ej.score_item(r, {"a": "absent", "b": "absent"}, {})["essential_miss"])


class Validation(unittest.TestCase):
    def reply(self, **kw):
        base = {"must": [{"id": "m1", "verdict": "present", "quote": "Set a=1 first."},
                         {"id": "m2", "verdict": "absent", "quote": ""},
                         {"id": "m3", "verdict": "present", "quote": "raise c to 10"},
                         {"id": "m4", "verdict": "absent", "quote": ""}],
                "must_not": [{"id": "n1", "verdict": "absent", "quote": ""}, {"id": "n2", "verdict": "absent", "quote": ""}],
                "other_errors": []}
        base.update(kw)
        return base

    def test_whitespace_normalised_quote_is_found(self):
        must, mn, other, errors, claims, failed = ej.validate_reply(RUBRIC, ANSWER, self.reply())
        self.assertEqual(must["m3"]["verdict"], "present")       # answer has "raise  c" (two spaces) and a newline earlier
        self.assertEqual((claims, failed, errors), (2, 0, []))

    def test_unsupported_verdict_becomes_absent_and_is_counted(self):
        j = self.reply(must=[{"id": "m1", "verdict": "present", "quote": "Set a=2 first."},
                             {"id": "m2", "verdict": "present", "quote": ""},
                             {"id": "m3", "verdict": "absent", "quote": ""}, {"id": "m4", "verdict": "absent", "quote": ""}],
                       must_not=[{"id": "n1", "verdict": "present", "quote": "restart the broker"},
                                 {"id": "n2", "verdict": "absent", "quote": ""}])
        must, mn, other, errors, claims, failed = ej.validate_reply(RUBRIC, ANSWER, j)
        self.assertEqual((must["m1"]["verdict"], must["m2"]["verdict"], mn["n1"]["verdict"]), ("absent",) * 3)
        self.assertEqual((claims, failed), (3, 3))
        self.assertEqual(sorted(errors), ["quote_not_found:m1", "quote_not_found:m2", "quote_not_found:n1"])
        self.assertEqual(ej.score_item(RUBRIC, {k: v["verdict"] for k, v in must.items()},
                                       {k: v["verdict"] for k, v in mn.items()})["P"], 0.0)

    def test_missing_unknown_and_bad_verdicts(self):
        j = self.reply(must=[{"id": "m1", "verdict": "maybe", "quote": "x"}, {"id": "zz", "verdict": "present", "quote": "x"}],
                       must_not=[{"id": "n1", "verdict": "contradicted", "quote": "x"}])
        must, mn, _, errors, claims, _ = ej.validate_reply(RUBRIC, ANSWER, j)
        self.assertIn("bad_verdict:m1", errors)
        self.assertIn("unknown_id:zz", errors)
        self.assertIn("missing:m2", errors)
        self.assertIn("bad_verdict:n1", errors)
        self.assertIn("missing:n2", errors)
        self.assertEqual(claims, 0)

    def test_other_errors_kept(self):
        _, _, other, _, _, _ = ej.validate_reply(RUBRIC, ANSWER, self.reply(other_errors=[{"quote": "lower d", "why": "w"}]))
        self.assertEqual(len(other), 1)
        self.assertTrue(other[0]["quote_ok"])


class Prompt(unittest.TestCase):
    def item(self, rubric=RUBRIC):
        return {"set": "s", "id": rubric["id"], "question": rubric["question"], "answer": ANSWER, "rubric": rubric}

    def test_instruction_verbatim_and_no_reference(self):
        design = (ROOT / "agents/m4-evaluation.md").read_text()
        # the design states the version in use: v1, v2 as it is now (before the Applies sentence was put in) or v2 with it
        v2_before = ej.JUDGE_INSTRUCTION_V2.replace(ej._V2_MUSTNOT, ej._V1_MUSTNOT)
        self.assertTrue(any(t in design for t in (ej.JUDGE_INSTRUCTION_V1, v2_before, ej.JUDGE_INSTRUCTION_V2)))
        system, user = ej.build_prompt(dict(self.item(), rubric=dict(RUBRIC, reference="SECRET ANSWER")), 1, {}, 1000)
        self.assertEqual(system, ej.JUDGE_INSTRUCTION)
        self.assertNotIn("SECRET ANSWER", user)
        self.assertIn("main or first recommendation", user)   # weight-less point with first=True carries the requirement

    def test_pass_two_reverses_point_order_and_ids_map_back(self):
        _, u1 = ej.build_prompt(self.item(), 1, {}, 1000)
        _, u2 = ej.build_prompt(self.item(), 2, {}, 1000)
        ids = lambda u: [x for x in __import__("re").findall(r"^- id (\S+):", u, flags=__import__("re").M)]
        self.assertEqual(ids(u1), ["m1", "m2", "m3", "m4", "n1", "n2"])
        self.assertEqual(ids(u2), ["m4", "m3", "m2", "m1", "n2", "n1"])
        # a reply listing the points in the pass-2 order is scored by id, not by position
        j = {"must": [{"id": "m4", "verdict": "absent", "quote": ""}, {"id": "m3", "verdict": "present", "quote": "raise  c"},
                      {"id": "m2", "verdict": "absent", "quote": ""}, {"id": "m1", "verdict": "present", "quote": "Set a=1 first."}],
             "must_not": [{"id": "n2", "verdict": "absent", "quote": ""}, {"id": "n1", "verdict": "absent", "quote": ""}],
             "other_errors": []}
        entry = {"key": "k", "j": j}
        row = ej.result_row(self.item(), 2, entry)
        self.assertEqual((row["must"]["m1"]["verdict"], row["must"]["m3"]["verdict"], row["must"]["m4"]["verdict"]),
                         ("present", "present", "absent"))
        self.assertAlmostEqual(row["P"], 3 / 4)

    def test_source_text_cap_and_chunk_ids(self):
        chunks = {"a": {"id": "a", "path": "p/a.md", "text": "A" * 500}, "b": {"id": "b", "path": "p/b.md", "text": "B" * 50}}
        r = dict(RUBRIC, covered_chunks=["a", "b"], evidence="log line 1")
        s = ej.source_text(r, chunks, 200)
        self.assertLessEqual(len(s.split("\n\n")[0].split("\n", 1)[1]), 150)
        self.assertIn("B" * 50, s)                      # the short chunk is whole, its unused share goes to the long one
        self.assertIn("[...]", s)
        _, u = ej.build_prompt(self.item(r), 1, chunks, 200)
        self.assertIn("log line 1", u)
        self.assertIn("p/b.md", u)
        r2 = dict(RUBRIC, source_text="given as text")
        self.assertIn("given as text", ej.source_text(r2, {}, 200))
        with self.assertRaises(ValueError):
            ej.source_text(dict(RUBRIC, covered_chunks=["zz"]), chunks, 200)


class Matching(unittest.TestCase):
    def test_answers_matched_to_questions_by_text(self):
        rubrics = [{"id": "real-2", "must": [], "must_not": []}, {"id": "real-1", "must": [], "must_not": []},
                   {"id": "x-9", "question": "Own question", "must": [], "must_not": []}]
        qmap = {"real": ["First question?", "Second   question?"]}
        sets = {"A": [{"question": "Second question?", "text": "ans2"}, {"question": "Unrelated", "text": "no"},
                      {"question": "Own question", "text": "ans9"}],
                "B": [{"question": "First question?", "text": "b1"}, {"question": "Second question?", "text": "  "}]}
        items, counts = ej.match_items(rubrics, sets, qmap)
        self.assertEqual(dict(counts), {"A": 2, "B": 1})
        self.assertEqual([(i["set"], i["id"], i["answer"]) for i in items],
                         [("A", "real-2", "ans2"), ("A", "x-9", "ans9"), ("B", "real-1", "b1")])
        with self.assertRaises(ValueError):
            ej.match_items([{"id": "nope-1", "must": [], "must_not": []}], sets, qmap)

    def test_generic_answers_one_per_question_no_specifics(self):
        g = ej.generic_answers([{"id": "real-1"}, {"id": "real-2"}], {"real": ["Q1", "Q2"]})
        self.assertEqual([x["question"] for x in g], ["Q1", "Q2"])
        self.assertTrue(all(x["text"] == ej.GENERIC_TEMPLATE for x in g))


class MockServer:
    """Local OpenAI-style server; `script(body, n)` returns reply text, or an int status code."""
    def __init__(self, script):
        outer = self
        self.bodies, self.script, self.lock = [], script, threading.Lock()

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                with outer.lock:
                    outer.bodies.append(body)
                    n = len(outer.bodies)
                res = outer.script(body, n)
                if isinstance(res, int):
                    self.send_response(res)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                data = json.dumps({"choices": [{"message": {"content": res}, "finish_reason": "stop"}],
                                   "usage": {"completion_tokens": 5}}).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        self.srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.url = f"http://127.0.0.1:{self.srv.server_address[1]}/v1"
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()

    def close(self):
        self.srv.shutdown()
        self.srv.server_close()


GOOD = json.dumps({"must": [{"id": "m1", "verdict": "present", "quote": "Set a=1 first."},
                            {"id": "m2", "verdict": "absent", "quote": ""}, {"id": "m3", "verdict": "absent", "quote": ""},
                            {"id": "m4", "verdict": "absent", "quote": ""}],
                   "must_not": [{"id": "n1", "verdict": "absent", "quote": ""}, {"id": "n2", "verdict": "absent", "quote": ""}],
                   "other_errors": []})


class HttpRun(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.items = [{"set": "s", "id": "t-1", "question": RUBRIC["question"], "answer": ANSWER, "rubric": RUBRIC}]

    def judge(self, srv, **kw):
        return ej.Judge(srv.url, "m", "key", {"max_tokens": 99}, 0.3, busy_wait_s=0, **kw)

    def test_invalid_json_retried_once_at_temperature_zero(self):
        srv = MockServer(lambda b, n: "I cannot do this" if n == 1 else GOOD)
        try:
            rows, called, _ = ej.run(self.items, [1], self.judge(srv), "m", {}, 1000, None, say=lambda *a: None)
        finally:
            srv.close()
        self.assertEqual(len(srv.bodies), 2)
        self.assertEqual([b["temperature"] for b in srv.bodies], [0.3, 0])
        self.assertEqual(srv.bodies[0]["max_tokens"], 99)
        self.assertTrue(rows[0]["valid_reply"] and rows[0]["retried"])
        self.assertAlmostEqual(rows[0]["P"], 2 / 4)

    def test_still_invalid_after_retry_is_unscored_not_retried_again(self):
        srv = MockServer(lambda b, n: "no json")
        try:
            rows, _, _ = ej.run(self.items, [1], self.judge(srv), "m", {}, 1000, None, say=lambda *a: None)
        finally:
            srv.close()
        self.assertEqual(len(srv.bodies), 2)
        self.assertFalse(rows[0]["valid_reply"])

    def test_busy_status_is_waited_out(self):
        srv = MockServer(lambda b, n: 503 if n <= 2 else GOOD)
        try:
            rows, _, _ = ej.run(self.items, [1], self.judge(srv), "m", {}, 1000, None, say=lambda *a: None)
        finally:
            srv.close()
        self.assertEqual(len(srv.bodies), 3)
        self.assertTrue(rows[0]["valid_reply"])

    def test_resume_from_cache_makes_no_new_calls(self):
        cache = str(self.tmp / "cache.jsonl")
        srv = MockServer(lambda b, n: GOOD)
        try:
            rows1, called1, total = ej.run(self.items, [1, 2], self.judge(srv), "m", {}, 1000, cache, say=lambda *a: None)
            self.assertEqual((called1, total, len(srv.bodies)), (2, 2, 2))
            rows2, called2, _ = ej.run(self.items, [1, 2], self.judge(srv), "m", {}, 1000, cache, say=lambda *a: None)
            self.assertEqual((called2, len(srv.bodies)), (0, 2))
            self.assertEqual(rows1, rows2)
            # an invalid cached reply is redone only on request
            Path(cache).write_text("".join(json.dumps(dict(json.loads(l), j=None)) + "\n" for l in Path(cache).read_text().splitlines()))
            _, c3, _ = ej.run(self.items, [1, 2], self.judge(srv), "m", {}, 1000, cache, say=lambda *a: None)
            self.assertEqual(c3, 0)
            _, c4, _ = ej.run(self.items, [1, 2], self.judge(srv), "m", {}, 1000, cache, retry_invalid=True, say=lambda *a: None)
            self.assertEqual(c4, 2)
        finally:
            srv.close()

    def test_one_line_per_item_and_no_url_or_key_in_output(self):
        said = []
        srv = MockServer(lambda b, n: GOOD)
        try:
            ej.run(self.items, [1, 2], self.judge(srv), "m", {}, 1000, None, say=said.append)
        finally:
            srv.close()
        self.assertEqual(len(said), 2)
        self.assertFalse(any(srv.url in s or "key" == s for s in said))

    def test_stub_judge_and_main_dry_run(self):
        rub = self.tmp / "r.jsonl"
        rub.write_text(json.dumps(RUBRIC) + "\n")
        ans = self.tmp / "a.jsonl"
        ans.write_text(json.dumps({"question": RUBRIC["question"], "text": ANSWER}) + "\n")
        import io, contextlib
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = ej.main(["run", "--rubrics", str(rub), "--answers", f"x={ans}", "--stub"])
        self.assertEqual(rc, 0)
        out = buf.getvalue()
        self.assertIn("matched x: 1 answers", out)
        self.assertIn("largest prompt", out)


class Instrumented(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.items = [{"set": "s", "id": "t-1", "question": RUBRIC["question"], "answer": ANSWER, "rubric": RUBRIC}]

    def serve(self, message, usage, finish="stop", delay=0.0, bodies=None):
        import time

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                if bodies is not None:
                    bodies.append(body)
                time.sleep(delay)
                data = json.dumps({"choices": [{"message": message, "finish_reason": finish}], "usage": usage}).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        self.addCleanup(lambda: (srv.shutdown(), srv.server_close()))
        return f"http://127.0.0.1:{srv.server_address[1]}/v1"

    def test_cache_entry_records_usage_reasoning_finish_reply_and_setting(self):
        usage = {"prompt_tokens": 100, "completion_tokens": 321, "completion_tokens_details": {"reasoning_tokens": 250}}
        url = self.serve({"content": GOOD, "reasoning_content": "x" * 77}, usage, finish="stop")
        cache = str(self.tmp / "c.jsonl")
        said = []
        ej.run(self.items, [1], ej.Judge(url, "m", "k", {}, 0.0), "m", {}, 1000, cache, say=said.append, setting="s1")
        e = json.loads(Path(cache).read_text().splitlines()[0])
        self.assertEqual(e["usage"], usage)
        self.assertEqual((e["reasoning_chars"], e["finish_reason"], e["setting"]), (77, "stop", "s1"))
        self.assertEqual(e["reply"], GOOD)
        s, u = ej.build_prompt(self.items[0], 1, {}, 1000)
        self.assertEqual(e["prompt_chars"], len(s) + len(u))
        self.assertIsInstance(e["seconds"], float)
        self.assertIn("tokens=321", said[0])
        self.assertRegex(said[0], r"\d+(\.\d+)?s ")

    def test_no_reasoning_field_is_none(self):
        url = self.serve({"content": GOOD}, {"completion_tokens": 5})
        cache = str(self.tmp / "c.jsonl")
        ej.run(self.items, [1], ej.Judge(url, "m", "k", {}, 0.0), "m", {}, 1000, cache, say=lambda s: None)
        self.assertIsNone(json.loads(Path(cache).read_text())["reasoning_chars"])

    def test_extra_body_passed_as_given(self):
        bodies = []
        url = self.serve({"content": GOOD}, {}, bodies=bodies)
        eb = {"chat_template_kwargs": {"enable_thinking": False}, "max_tokens": 4096}
        ej.run(self.items, [1], ej.Judge(url, "m", "k", eb, 0.0), "m", {}, 1000, None, say=lambda s: None)
        self.assertEqual(bodies[0]["chat_template_kwargs"], {"enable_thinking": False})
        self.assertEqual(bodies[0]["max_tokens"], 4096)

    def test_old_entries_without_new_fields_are_read_and_count_as_default_setting(self):
        s, u = ej.build_prompt(self.items[0], 1, {}, 1000)
        old_key = ej.cache_key("m", 1, self.items[0], s, u)        # the key as it was before settings had names
        import hashlib
        self.assertEqual(old_key, hashlib.md5(f"m\x001\x00s\x00t-1\x00{s}\x00{u}".encode()).hexdigest())
        self.assertEqual(old_key, ej.cache_key("m", 1, self.items[0], s, u, ej.DEFAULT_SETTING))
        self.assertNotEqual(old_key, ej.cache_key("m", 1, self.items[0], s, u, "other"))
        old = {"key": old_key, "set": "s", "id": "t-1", "pass": 1, "j": json.loads(GOOD), "reply": None, "retried": False,
               "seconds": 650.0}
        cache = self.tmp / "old.jsonl"
        cache.write_text(json.dumps(old) + "\n")
        calls = []
        rows, n_new, _ = ej.run(self.items, [1], lambda *a: calls.append(1), "m", {}, 1000, str(cache), say=lambda s: None)
        self.assertEqual((n_new, calls), (0, []))
        self.assertEqual((rows[0]["setting"], rows[0]["completion_tokens"], rows[0]["reasoning_chars"]), (ej.DEFAULT_SETTING, None, None))
        # under another setting name the same answer is judged again, and both entries live in one cache
        url = self.serve({"content": GOOD}, {"completion_tokens": 9})
        rows, n_new, _ = ej.run(self.items, [1], ej.Judge(url, "m", "k", {}, 0.0), "m", {}, 1000, str(cache),
                                say=lambda s: None, setting="nothink")
        self.assertEqual(n_new, 1)
        self.assertEqual(len(cache.read_text().splitlines()), 2)

    def test_dry_run_reads_cache_and_does_not_write_it(self):
        s, u = ej.build_prompt(self.items[0], 1, {}, 1000)
        cache = self.tmp / "c.jsonl"
        cache.write_text(json.dumps({"key": ej.cache_key("m", 1, self.items[0], s, u), "set": "s", "id": "t-1", "pass": 1,
                                     "j": json.loads(GOOD), "seconds": 1.0}) + "\n")
        before = cache.read_text()
        rows, n_new, _ = ej.run(self.items, [1, 2], ej.stub_judge, "m", {}, 1000, str(cache), say=lambda s: None, write_cache=False)
        self.assertEqual(n_new, 1)
        self.assertEqual(cache.read_text(), before)

    def test_zero_source_chars_means_no_passages_and_no_contradiction_sentence(self):
        chunks = {"a": {"id": "a", "path": "p/a.md", "text": "SOURCE-TEXT-HERE"}}
        r = dict(RUBRIC, covered_chunks=["a"])
        it = dict(self.items[0], rubric=r, source="GIVEN SOURCE")
        s0, u0 = ej.build_prompt(it, 1, chunks, 0)
        self.assertNotIn("SOURCE-TEXT-HERE", u0)
        self.assertNotIn("GIVEN SOURCE", u0)
        self.assertNotIn("SOURCE PASSAGES", u0)
        self.assertNotIn("source passages", s0)
        self.assertNotIn("contradicts the source", s0)
        self.assertIn("other_errors", s0)
        self.assertIn("empty list", s0)
        s1, u1 = ej.build_prompt(self.items[0], 1, chunks, 100)
        self.assertEqual(s1, ej.JUDGE_INSTRUCTION)
        self.assertIn("contradicts the source passages", s1)
        self.assertIn("SOURCE PASSAGES", u1)
        j, _, _, _ = ej.stub_judge(s0, u0)
        self.assertEqual(len(j["must"]), 4)
        self.assertEqual(len(j["must_not"]), 2)

    def test_only_filter_and_settings_label_in_main(self):
        rub = self.tmp / "r.jsonl"
        r2 = dict(RUBRIC, id="t-2", question="Other?")
        rub.write_text(json.dumps(RUBRIC) + "\n" + json.dumps(r2) + "\n")
        ans = self.tmp / "a.jsonl"
        ans.write_text(json.dumps({"question": RUBRIC["question"], "text": ANSWER}) + "\n"
                       + json.dumps({"question": "Other?", "text": ANSWER}) + "\n")
        import io, contextlib
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = ej.main(["run", "--rubrics", str(rub), "--answers", f"x={ans}", "--stub", "--only", "x:t-2",
                          "--settings-name", "n1", "--source-chars", "0", "--out", str(self.tmp / "o.jsonl")])
        self.assertEqual(rc, 0)
        self.assertIn("matched x: 1 answers", buf.getvalue())
        rows = [json.loads(l) for l in (self.tmp / "o.jsonl").read_text().splitlines()]
        self.assertEqual([(r["id"], r["setting"]) for r in rows], [("t-2", "n1"), ("t-2", "n1")])


class FlushedProgress(unittest.TestCase):
    def test_a_line_per_item_reaches_a_redirected_file_before_the_process_ends(self):
        import os
        import subprocess
        import time
        tmp = Path(tempfile.mkdtemp())
        rubs, answers = [], []
        for n in range(1, 4):
            rubs.append(dict(RUBRIC, id=f"t-{n}", question=f"Question {n}?"))
            answers.append({"question": f"Question {n}?", "text": ANSWER})
        (tmp / "r.jsonl").write_text("".join(json.dumps(x) + "\n" for x in rubs))
        (tmp / "a.jsonl").write_text("".join(json.dumps(x) + "\n" for x in answers))

        def slow(body, n):
            time.sleep(1.5)
            return GOOD
        srv = MockServer(slow)
        out = tmp / "out.log"
        try:
            with open(out, "w") as f:
                proc = subprocess.Popen(
                    [sys.executable, str(ROOT / "scripts/eval2_judge.py"), "run", "--rubrics", str(tmp / "r.jsonl"),
                     "--answers", f"x={tmp / 'a.jsonl'}", "--passes", "1", "--concurrency", "1", "--judge-url", srv.url,
                     "--cache", str(tmp / "c.jsonl"), "--out", str(tmp / "o.jsonl")],
                    stdout=f, stderr=subprocess.STDOUT, env={**os.environ, "JUDGE_API_KEY": "k"})
                seen_while_running = 0
                deadline = time.time() + 60
                while proc.poll() is None and time.time() < deadline:
                    seen_while_running = max(seen_while_running, out.read_text().count("[judge]"))
                    if seen_while_running >= 1:
                        break
                    time.sleep(0.1)
                still_running = proc.poll() is None
                proc.wait(timeout=60)
        finally:
            srv.close()
        self.assertGreaterEqual(seen_while_running, 1, out.read_text())
        self.assertTrue(still_running, "the first progress line was only visible after the process ended")
        self.assertEqual(out.read_text().count("[judge]"), 3)

# ---------------------------------------------------------------- version 2

import hashlib  # noqa: E402
import io  # noqa: E402
import contextlib  # noqa: E402
import re  # noqa: E402

CHUNKS = {"c1": {"id": "c1", "path": "raw/a/one.md", "text": "Setting a controls the thing. Default is 5."},
          "c2": {"id": "c2", "path": "raw/b/two.md", "text": "Never delete the volume while the pod runs."},
          "c3": {"id": "c3", "path": "raw/c/three.md", "text": "Unrelated passage."}}


class FairQuotes(unittest.TestCase):
    def found(self, quote, text):
        return ej.quote_found(quote, ej.fair_norm(text))

    def test_formatting_is_ignored_on_both_sides(self):
        self.assertTrue(self.found("raise `max.poll.interval.ms` to 600000", "You should **raise max.poll.interval.ms** to 600000."))
        self.assertTrue(self.found("- first step\n- second step", "Steps:\n* first step\n* second step"))
        self.assertTrue(self.found("2) do this", "Then:\n1. do this now"))
        self.assertTrue(self.found("kubectl get pods -n kafka", "```bash\nkubectl get pods   -n kafka\n```"))
        self.assertTrue(self.found("```yaml\nkind: Kafka\n```", "kind: Kafka"))
        self.assertTrue(self.found("**Note:** the value", "Note: the   value"))

    def test_plain_missing_and_empty_fail(self):
        self.assertFalse(self.found("restart the broker", "reload the broker"))
        self.assertFalse(self.found("", "anything"))
        self.assertFalse(self.found("   ", "anything"))
        self.assertFalse(self.found("a paraphrase with provided words", "a paraphrase with words"))

    def test_ellipsis_fragments_in_order_at_least_twelve_characters(self):
        ans = "First we set the partition count to six. Then we wait. Finally verify the leader election result."
        self.assertTrue(self.found("set the partition count to six ... verify the leader election", ans))
        self.assertTrue(self.found("set the partition count to six\u2026verify the leader election", ans))
        self.assertTrue(self.found("set the partition count to six ...", ans))            # trailing ellipsis: one fragment
        self.assertFalse(self.found("verify the leader election ... set the partition count to six", ans))   # out of order
        self.assertTrue(self.found("set the partition ... verify the leader election", ans))   # fragments of 17 and 26 characters
        # a fragment shorter than 12 characters fails even if present and in order
        self.assertFalse(self.found("set the partition count to six ... Then we", ans))
        self.assertFalse(self.found("First we ... verify the leader election", ans))
        # a quote that contains "..." literally in the answer is found as it is
        self.assertTrue(self.found("kafka-topics.sh ... --describe", "run kafka-topics.sh ... --describe now"))

    def test_the_old_pilot_cache_gives_six_failures_of_443(self):
        cache = ROOT / "work/m4/pilot/judge_cache.jsonl"
        if not cache.exists():
            self.skipTest("work/m4/pilot/judge_cache.jsonl not on this machine (generated, git-ignored)")
        W = ROOT / "work/v3"
        sets = {"real_default": W / "ans_rag_real_s1.jsonl", "real_strict": W / "ans_rag_strict.jsonl",
                "real_v2_own_terms": W / "v2rag/ans_v2_ownexp.jsonl", "real_v2_base_excerpts": W / "v2rag/ans_v2_baseexp.jsonl",
                "holdout_default": W / "ans_rag_holdout_s1.jsonl", "holdout_strict": W / "strict/ans_rag_holdout_strict.jsonl"}
        if not all(p.exists() for p in sets.values()):
            self.skipTest("old answer files not on this machine")
        out = Path(tempfile.mkdtemp()) / "rescored.jsonl"
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = ej.main(["run", "--rubrics", str(ROOT / "eval/m4/pilot/rubrics.jsonl"),
                          "--qmap", f"real={ROOT / 'eval/questions_real.jsonl'}", f"holdout={ROOT / 'eval/questions_runbooks_holdout.jsonl'}",
                          "--chunks", str(W / "chunks_s1.jsonl"), "--source-chars", "12000", "--answers",
                          *[f"{k}={v}" for k, v in sets.items()], "--judge-model", "DeepSeek-V4.1-Flash-1017-oQ4e-mtp",
                          "--settings-name", "deepseek-nothink-12k", "--instruction", "v1", "--cache", str(cache),
                          "--out", str(out), "--rescore"])
        self.assertEqual(rc, 0)
        rows = [json.loads(l) for l in out.read_text().splitlines()]
        self.assertEqual(len(rows), 160)
        self.assertEqual((sum(r["quote_claims"] for r in rows), sum(r["quote_failed"] for r in rows)), (443, 6))


class InstructionVersions(unittest.TestCase):
    def test_v1_is_unchanged(self):
        self.assertEqual(hashlib.md5(ej.JUDGE_INSTRUCTION_V1.encode()).hexdigest(), "450b8509430fcf16ce0b1e2fc9afd528")
        self.assertIs(ej.JUDGE_INSTRUCTION, ej.JUDGE_INSTRUCTION_V1)

    def test_v2_differs_in_exactly_four_places(self):
        v1, v2 = ej.JUDGE_INSTRUCTION_V1, ej.JUDGE_INSTRUCTION_V2
        self.assertIn('For each MUST-NOT point, the verdict is "present" only if the answer states or recommends it and the point\'s "Applies" note, '
                      'if it has one, holds for what the answer says; where a point has no "Applies" note, a statement made in passing or as an '
                      'alternative counts. A statement the answer explicitly rejects, or mentions only to warn against, is "absent".', v2)
        self.assertNotIn("even in passing", v2)
        back = (v2.replace(ej._V2_MUSTNOT, ej._V1_MUSTNOT).replace(" A definition of a setting is not a recommendation to change it; a default value is not a change.", "")
                .replace(ej._V2_OTHER, ej._V1_OTHER).replace(ej._V2_FORMAT, ej._V1_FORMAT))
        self.assertEqual(back, v1)
        self.assertIn("A definition of a setting is not a recommendation to change it; a default value is not a change.", v2)
        self.assertIn('List another statement only if a sentence in SOURCE PASSAGES or ANSWER EXCERPTS contradicts it. '
                      'Quote that sentence in "source_quote". Do not list statements that are absent from them.', v2)
        self.assertIn('"other_errors":[{"quote":"","source_quote":"","why":""}]', v2)
        self.assertNotIn("source_quote", v1)
        self.assertEqual(len([1 for a, b in zip(v1.splitlines(), v2.splitlines()) if a != b]), 4)

    def test_cache_key_v1_as_before_v2_separate(self):
        it = {"set": "s", "id": "t-1"}
        old = hashlib.md5("m\x001\x00s\x00t-1\x00SYS\x00USR".encode()).hexdigest()
        self.assertEqual(ej.cache_key("m", 1, it, "SYS", "USR"), old)
        self.assertEqual(ej.cache_key("m", 1, it, "SYS", "USR", instruction="v1"), old)
        self.assertNotEqual(ej.cache_key("m", 1, it, "SYS", "USR", instruction="v2"), old)
        named = ej.cache_key("m", 1, it, "SYS", "USR", "n1")
        self.assertNotEqual(ej.cache_key("m", 1, it, "SYS", "USR", "n1", "v2"), named)

    def test_version_in_cache_entry_and_row(self):
        it = {"set": "s", "id": "t-1", "question": RUBRIC["question"], "answer": ANSWER, "rubric": RUBRIC}
        cache = Path(tempfile.mkdtemp()) / "c.jsonl"
        rows, n_new, _ = ej.run([it], [1], ej.stub_judge, "m", {}, 1000, str(cache), say=lambda s: None, instruction="v2")
        self.assertEqual(rows[0]["instruction"], "v2")
        self.assertEqual(json.loads(cache.read_text().splitlines()[0])["instruction"], "v2")
        rows1, n1, _ = ej.run([dict(it)], [1], ej.stub_judge, "m", {}, 1000, str(cache), say=lambda s: None, instruction="v1")
        self.assertEqual((n_new, n1), (1, 1))          # a v1 judgment is not found under the v2 key
        self.assertEqual(rows1[0]["instruction"], "v1")


class ExcerptsAndPrompt(unittest.TestCase):
    def item(self, **kw):
        base = {"set": "s", "id": "t-1", "question": RUBRIC["question"], "answer": ANSWER,
                "rubric": dict(RUBRIC, covered_chunks=["c3"]), "pack": ["c1", "c2"]}
        base.update(kw)
        return base

    def test_v2_has_two_labelled_blocks_v1_has_one(self):
        s2, u2 = ej.build_prompt(self.item(), 1, CHUNKS, 1000, "v2")
        self.assertEqual(s2, ej.JUDGE_INSTRUCTION_V2)
        self.assertIn("SOURCE PASSAGES:\n[c3] raw/c/three.md\nUnrelated passage.", u2)
        self.assertIn("ANSWER EXCERPTS", u2)
        self.assertIn("[c1] raw/a/one.md\nSetting a controls the thing.", u2)
        self.assertIn("[c2] raw/b/two.md", u2)
        self.assertLess(u2.index("SOURCE PASSAGES"), u2.index("ANSWER EXCERPTS"))
        self.assertLess(u2.index("ANSWER EXCERPTS"), u2.index("ANSWER TO CHECK"))
        self.assertIn("about what its excerpts contain or lack is checked against these only", u2)
        s1, u1 = ej.build_prompt(self.item(), 1, CHUNKS, 1000, "v1")
        self.assertEqual(s1, ej.JUDGE_INSTRUCTION_V1)
        self.assertNotIn("ANSWER EXCERPTS", u1)
        self.assertNotIn("raw/a/one.md", u1)

    def test_no_pack_is_marked_and_zero_source_chars_drops_both_blocks(self):
        _, u = ej.build_prompt(self.item(pack=None), 1, CHUNKS, 1000, "v2")
        self.assertIn("ANSWER EXCERPTS", u)
        self.assertIn("(not available)", u)
        s0, u0 = ej.build_prompt(self.item(), 1, CHUNKS, 0, "v2")
        self.assertEqual(s0, ej.JUDGE_INSTRUCTION_V2_NO_SOURCES)
        self.assertNotIn("ANSWER EXCERPTS", u0)
        self.assertNotIn("SOURCE PASSAGES", u0)
        self.assertIn('Leave "other_errors" as an empty list.', s0)

    def test_excerpt_cap_and_unknown_chunk(self):
        big = {"b": {"id": "b", "path": "p", "text": "X" * 500}}
        self.assertIn("[...]", ej.excerpt_text(["b"], big, 100))
        with self.assertRaises(ValueError):
            ej.excerpt_text(["nope"], CHUNKS, 100)

    def test_reference_only_when_asked(self):
        _, u = ej.build_prompt(self.item(), 1, CHUNKS, 1000, "v2")
        self.assertNotIn("REFERENCE ANSWER", u)
        items, _ = ej.match_items([dict(RUBRIC, reference="THE REFERENCE")], {"x": [{"question": RUBRIC["question"], "text": ANSWER}]},
                                  {}, with_reference=True)
        _, u = ej.build_prompt(items[0], 1, CHUNKS, 1000, "v2")
        self.assertIn("REFERENCE ANSWER:\nTHE REFERENCE", u)
        self.assertLess(u.index("REFERENCE ANSWER"), u.index("ANSWER TO CHECK"))
        with self.assertRaises(ValueError):
            ej.match_items([RUBRIC], {"x": [{"question": RUBRIC["question"], "text": ANSWER}]}, {}, with_reference=True)

    def test_matching_by_id_and_pack_sources(self):
        rubrics = [dict(RUBRIC, id="p-1"), dict(RUBRIC, id="p-2", question="Other?")]
        sets = {"A": [{"id": "p-1", "text": "ans one", "pack": ["c1", "c2"]}, {"id": "p-2", "text": "ans two", "pack": None}],
                "B": [{"question": "Other?", "text": "ans b"}]}
        packs = {"B": {"Other?": ["c3"]}}
        items, counts = ej.match_items(rubrics, sets, {}, packs=packs)
        self.assertEqual(dict(counts), {"A": 2, "B": 1})
        self.assertEqual([(i["set"], i["id"], i["pack"]) for i in items], [("A", "p-1", ["c1", "c2"]), ("A", "p-2", None), ("B", "p-2", ["c3"])])
        self.assertEqual(ej.pack_ids_of({"k6": ["x", "y"]}), ["x", "y"])
        self.assertEqual(ej.pack_ids_of({"pack": [{"id": "z"}]}), ["z"])
        self.assertIsNone(ej.pack_ids_of({"text": "t"}))


class SourceQuoteValidation(unittest.TestCase):
    ITEM = {"set": "s", "id": "t-1", "question": RUBRIC["question"], "answer": ANSWER, "rubric": RUBRIC,
            "pack": ["c1", "c2"]}

    def reply(self, other):
        return {"must": [{"id": i, "verdict": "absent", "quote": ""} for i in ("m1", "m2", "m3", "m4")],
                "must_not": [{"id": i, "verdict": "absent", "quote": ""} for i in ("n1", "n2")], "other_errors": other}

    def test_other_error_needs_a_source_quote_found_in_what_the_judge_saw(self):
        it = dict(self.ITEM)
        ej.prepare_items([it], CHUNKS, 1000, 1000, "v2")
        j = self.reply([
            {"quote": "Set a=1 first.", "source_quote": "Default is 5.", "why": "from the excerpt"},          # in ANSWER EXCERPTS
            {"quote": "raise c to 10", "source_quote": "Setting a  controls the thing", "why": "ws"},          # whitespace
            {"quote": "lower d", "source_quote": "Setting z controls the thing", "why": "not in anything"},    # dropped
            {"quote": "lower d", "source_quote": "", "why": "no source quote"},                                # dropped
            {"quote": "lower d", "why": "no field at all"}])                                                   # dropped
        entry = {"key": "k", "j": j, "instruction": "v2"}
        row = ej.result_row(it, 1, entry)
        self.assertEqual(row["other_errors_n"], 2)
        self.assertEqual(row["other_dropped"], 3)
        self.assertEqual(row["judge_errors"].count("source_quote_not_found:other"), 3)
        self.assertEqual(row["instruction"], "v2")
        self.assertEqual(row["n_candidates"], 2)

    def test_source_passages_count_too_and_v1_keeps_every_entry(self):
        it = dict(self.ITEM, rubric=dict(RUBRIC, covered_chunks=["c3"]))
        ej.prepare_items([it], CHUNKS, 1000, 1000, "v2")
        j = self.reply([{"quote": "lower d", "source_quote": "Unrelated passage.", "why": "w"}])
        self.assertEqual(ej.result_row(it, 1, {"key": "k", "j": j, "instruction": "v2"})["other_errors_n"], 1)
        j1 = self.reply([{"quote": "lower d", "why": "w"}])
        self.assertEqual(ej.result_row(self.ITEM, 1, {"key": "k", "j": j1})["other_errors_n"], 1)     # no instruction field: v1

    def test_row_quote_checks_use_fair_matching(self):
        j = self.reply([])
        j["must"][0] = {"id": "m1", "verdict": "present", "quote": "**Set** `a=1` first. ... then lower d"}
        row = ej.result_row(self.ITEM, 1, {"key": "k", "j": j})
        self.assertEqual(row["must"]["m1"]["verdict"], "present")
        self.assertEqual((row["quote_claims"], row["quote_failed"]), (1, 0))


class SupportedEssentialMiss(unittest.TestCase):
    R = {"id": "t-2", "question": "q",
         "must": [{"id": "m1", "text": "a", "weight": 2, "first": False, "source": "raw/a/one.md:12-15; raw/z/other.md:3"},
                  {"id": "m2", "text": "b", "weight": 2, "first": False, "source": "raw/x/none.md:1", "chunks": ["c2"]},
                  {"id": "m3", "text": "c", "weight": 2, "first": False},
                  {"id": "m4", "text": "d", "weight": 2, "first": False, "source": "raw/b/two.md:1"},
                  {"id": "m5", "text": "e", "weight": 1, "first": False, "source": "raw/a/one.md:1"}],
         "must_not": [], "any_of": [["m3", "m4"]]}

    def test_rules_chunk_path_none_no_pack(self):
        pack, paths = ["c1"], ["raw/a/one.md"]
        got = ej.supported_points(self.R, ["m1", "m2", "m3"], pack, paths)
        self.assertEqual(got, [{"id": "m1", "supported": True, "rule": "path"},
                               {"id": "m2", "supported": False, "rule": "chunk"},      # chunk id given: the path is not consulted
                               {"id": "m3", "supported": False, "rule": "none"}])
        self.assertTrue(ej.supported_points(self.R, ["m2"], ["c2"], ["raw/b/two.md"])[0]["supported"])
        self.assertEqual(ej.supported_points(self.R, ["m1"], None, None), [{"id": "m1", "supported": False, "rule": "no_pack"}])
        self.assertEqual(ej.point_paths(self.R["must"][0]), ["raw/a/one.md", "raw/z/other.md"])

    def test_row_flags(self):
        # m1 absent (weight 2, path in the excerpts): supported. m3 absent but its any_of alternative m4 present: no essential miss.
        it = {"set": "s", "id": "t-2", "question": "q", "answer": "x y z", "rubric": self.R, "pack": ["c1"], "excerpt_paths": ["raw/a/one.md"]}
        j = {"must": [{"id": "m1", "verdict": "absent", "quote": ""}, {"id": "m2", "verdict": "absent", "quote": ""},
                      {"id": "m3", "verdict": "absent", "quote": ""}, {"id": "m4", "verdict": "present", "quote": "x y"},
                      {"id": "m5", "verdict": "absent", "quote": ""}], "must_not": [], "other_errors": []}
        row = ej.result_row(it, 1, {"key": "k", "j": j})
        self.assertEqual([e["id"] for e in row["essential_points"]], ["m1", "m2"])
        self.assertTrue(row["essential_miss"])
        self.assertTrue(row["supported_essential_miss"])
        it2 = dict(it, excerpt_paths=["raw/other.md"])
        row2 = ej.result_row(it2, 1, {"key": "k", "j": j})
        self.assertTrue(row2["essential_miss"])
        self.assertFalse(row2["supported_essential_miss"])      # the sources of m1 and m2 are not among the answer's excerpts


class CandidatesAndReview(unittest.TestCase):
    def row(self, pass_no, must_not_quote, other_quote, sev="wrong"):
        rub = dict(RUBRIC, must_not=[{"id": "n1", "text": "Says restart", "severity": sev},
                                     {"id": "n2", "text": "Says delete", "severity": "harmful"}])
        it = {"set": "s", "id": "t-1", "question": RUBRIC["question"], "answer": "We restart the broker now. Also delete it later. Odd claim here.",
              "rubric": rub, "pack": ["c1"], "excerpt_paths": ["raw/a/one.md"], "source": "", "excerpts": "Odd claim is false."}
        j = {"must": [{"id": i, "verdict": "absent", "quote": ""} for i in ("m1", "m2", "m3", "m4")],
             "must_not": [{"id": "n1", "verdict": "present" if must_not_quote else "absent", "quote": must_not_quote or ""},
                          {"id": "n2", "verdict": "absent", "quote": ""}],
             "other_errors": [{"quote": other_quote, "source_quote": "Odd claim is false.", "why": "contradicts the excerpt"}] if other_quote else []}
        return ej.result_row(it, pass_no, {"key": f"k{pass_no}", "j": j, "instruction": "v2"})

    def test_candidates_are_deduplicated_across_channels_by_overlapping_quotes(self):
        r = self.row(1, "We restart the broker now.", "restart the broker now")
        self.assertEqual(r["n_candidates"], 1)
        c = r["candidates"][0]
        self.assertEqual(c["channels"], ["must_not", "other_errors"])
        self.assertEqual(c["severity"], "wrong")
        self.assertEqual(c["points"], ["n1"])
        r = self.row(1, "We restart the broker now.", "Odd claim here.")
        self.assertEqual(r["n_candidates"], 2)

    def test_review_file_merges_passes_and_leaves_decision_fields_empty(self):
        rows = [self.row(1, "We restart the broker now.", "Odd claim here."), self.row(2, "restart the broker", None)]
        rev = ej.review_rows(rows)
        self.assertEqual(len(rev), 2)
        a = next(x for x in rev if "must_not" in x["channels"])
        self.assertEqual(a["passes"], [1, 2])
        self.assertEqual(sorted(a["members"]), [[1, 0], [2, 0]])
        self.assertEqual((a["confirmed"], a["note"]), ("", ""))
        self.assertEqual(a["flag_id"], "s:t-1:1")
        b = next(x for x in rev if x is not a)
        self.assertEqual(b["passes"], [1])

    def test_confirmed_per_ten_answers(self):
        rows = []
        for i in range(1, 5):                           # four answers of the set "s"
            r = self.row(1, "We restart the broker now.", None, sev="harmful" if i == 1 else "wrong")
            r["id"] = f"t-{i}"
            rows.append(r)
        rev = ej.review_rows(rows)
        self.assertEqual(len(rev), 4)
        for x, c in zip(rev, ("yes", "no", "harmful", "")):
            x["confirmed"] = c
        m = ej.confirmed_summary(rev, rows)["s"]
        self.assertEqual((m["answers"], m["flags"], m["undecided"]), (4, 4, 1))
        self.assertEqual((m["confirmed_wrong"], m["confirmed_harmful"]), (2, 2))     # t-1 yes + harmful rubric; t-3 'harmful'
        self.assertAlmostEqual(m["wrong_per10"], 5.0)
        self.assertAlmostEqual(m["harmful_per10"], 5.0)


class Measures(unittest.TestCase):
    def test_per_answer_and_per_ten(self):
        r1 = CandidatesAndReview().row(1, "We restart the broker now.", "Odd claim here.")
        r2 = CandidatesAndReview().row(1, None, None)
        r2["id"] = "t-2"
        m = ej.measures([r1, r2])["s"][1]
        self.assertEqual((m["n"], m["candidates"]), (2, 2))
        self.assertAlmostEqual(m["candidates_per10"], 10.0)
        self.assertAlmostEqual(m["mean_P"], 0.0)
        self.assertEqual(m["essential_miss"], 2)


class MainSubcommands(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        (self.tmp / "chunks.jsonl").write_text("".join(json.dumps(c) + "\n" for c in CHUNKS.values()))
        (self.tmp / "items.jsonl").write_text(json.dumps({"id": "p-1", "question": "Why does X fail?", "evidence": "log: boom"}) + "\n")
        (self.tmp / "rubrics.jsonl").write_text(json.dumps(dict(RUBRIC, id="p-1", question=None)).replace('"question": null,', "") + "\n")
        (self.tmp / "ans.jsonl").write_text(json.dumps({"id": "p-1", "text": ANSWER, "pack": ["c1", "c2"]}) + "\n")

    def run_main(self, *args):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = ej.main(list(args))
        return rc, buf.getvalue()

    def test_stub_run_v2_with_items_and_flags_and_confirmed(self):
        t = self.tmp
        rc, out = self.run_main("run", "--rubrics", str(t / "rubrics.jsonl"), "--items", str(t / "items.jsonl"), "--chunks", str(t / "chunks.jsonl"),
                                "--answers", f"x={t / 'ans.jsonl'}", "--stub", "--out", str(t / "res.jsonl"))
        self.assertEqual(rc, 0, out)
        self.assertIn("instruction v2: 1 of 1 answers have their own excerpts", out)
        rows = [json.loads(l) for l in (t / "res.jsonl").read_text().splitlines()]
        self.assertEqual(len(rows), 2)
        self.assertEqual({r["instruction"] for r in rows}, {"v2"})
        rc, out = self.run_main("flags", "--results", str(t / "res.jsonl"), "--out", str(t / "review.jsonl"))
        self.assertEqual(rc, 0)
        self.assertIn("wrote 0 candidate errors", out)
        # a hand-made review file
        rev = [{"flag_id": "x:p-1:1", "item": "p-1", "set": "x", "passes": [1], "members": [[1, 0]], "channels": ["must_not"],
                "quotes": ["q"], "severity": "wrong", "reasons": [], "points": ["n1"], "confirmed": "yes", "note": ""}]
        (t / "review.jsonl").write_text(json.dumps(rev[0]) + "\n")
        rc, out = self.run_main("confirmed", "--review", str(t / "review.jsonl"), "--results", str(t / "res.jsonl"))
        self.assertEqual(rc, 0)
        self.assertIn("| x | 1 | 1 | 0 | 1 | 10.0 | 0 | 0.0 |", out)
        rev[0]["confirmed"] = "maybe"
        (t / "review.jsonl").write_text(json.dumps(rev[0]) + "\n")
        rc, _ = self.run_main("confirmed", "--review", str(t / "review.jsonl"), "--results", str(t / "res.jsonl"))
        self.assertEqual(rc, 1)

    def test_pack_missing_from_the_index_is_an_error_not_a_silent_skip(self):
        t = self.tmp
        (t / "ans.jsonl").write_text(json.dumps({"id": "p-1", "text": ANSWER, "pack": ["zz"]}) + "\n")
        rc, _ = self.run_main("run", "--rubrics", str(t / "rubrics.jsonl"), "--items", str(t / "items.jsonl"), "--chunks", str(t / "chunks.jsonl"),
                              "--answers", f"x={t / 'ans.jsonl'}", "--stub")
        self.assertEqual(rc, 1)

    def test_instruction_option_selects_text(self):
        t = self.tmp
        for ins in ("v1", "v2"):
            rc, out = self.run_main("run", "--rubrics", str(t / "rubrics.jsonl"), "--items", str(t / "items.jsonl"), "--chunks", str(t / "chunks.jsonl"),
                                    "--answers", f"x={t / 'ans.jsonl'}", "--stub", "--instruction", ins)
            self.assertEqual(rc, 0)
            self.assertIn(f"instruction {ins}: ", out)             # the settings line names the version
            self.assertEqual("own excerpts" in out, ins == "v2")   # v1 does not look at the pack

RICH = {
    "id": "r-1",
    "must": [{"id": "m1", "text": "Makes the main diagnosis that X is held back.", "weight": 2, "first": True, "source": "raw/src/a.md:1-5"},
             {"id": "m2", "text": "Checks it read-only.", "weight": 2, "first": False,
              "source": "searched raw/src/dir (no occurrence); inferred"},
             {"id": "m3", "text": "Runs the tool.", "weight": 1, "first": False,
              "source": "raw_catalogue/tools/T.java:10-20,30-40"},
             {"id": "m4", "text": "Aborts.", "weight": 1, "first": True, "source": "inferred; data/rb/x.md:4; raw/src/b.md:2"}],
    "must_not": [{"id": "n1", "text": "Resets offsets as the fix.", "severity": "harmful",
                  "condition": "only if recommended as the fix", "near_miss": "NEARMISS-SENTENCE-ONE"},
                 {"id": "n2", "text": "Deletes records.", "severity": "wrong", "condition": "", "near_miss": "NEARMISS-SENTENCE-TWO"}],
    "any_of": [["m3", "m4"]], "covered_chunks": ["c1"],
    "reference": "REFERENCE-TEXT-A.\nSecond line.", "correct_alternative": "ALT-TEXT-A", "generic": "GENERIC-TEXT-A",
    "note": "NOTE-TEXT", "weights_status": "proposed by the writer",
}
RICH_ITEM = {"id": "r-1", "question": "What is going on?", "evidence": "log line one"}


class PointRendering(unittest.TestCase):
    def prompt(self, instruction="v2", with_reference=False, pass_no=1):
        rb = dict(RICH, question="What is going on?", evidence="log line one")
        item = {"set": "s", "id": "r-1", "question": rb["question"], "answer": "An answer.", "rubric": rb, "pack": None}
        if with_reference:
            item["reference"] = rb["reference"]
        return ej.build_prompt(item, pass_no, CHUNKS, 12000, instruction)[1]

    def test_first_is_rendered_as_the_instruction_expects_even_if_the_text_says_main(self):
        u = self.prompt("v2")
        self.assertIn("held back. (This must be the main or first recommendation, not one option among several.)", u)
        self.assertIn("Aborts. (This must be the main or first recommendation", u)
        # v1 keeps its old rule (a text with 'main' gets no note), so the first pilot's prompts do not change
        self.assertNotIn("must be the main or first", self.prompt("v1").split("Checks it")[0])

    def test_condition_is_shown_next_to_the_must_not_text_and_near_miss_is_not(self):
        u = self.prompt()
        self.assertIn("- id n1: Resets offsets as the fix. (Applies: only if recommended as the fix)", u)
        self.assertIn("- id n2: Deletes records.\n", u + "\n")          # an empty condition adds nothing
        self.assertNotIn("(Applies: )", u)
        self.assertNotIn("NEARMISS", u)

    def test_reference_answer_alternative_generic_note_never_in_the_input_except_the_reference_arm(self):
        for p in (1, 2):
            u = self.prompt(pass_no=p)
            for text in ("REFERENCE-TEXT-A", "ALT-TEXT-A", "GENERIC-TEXT-A", "NOTE-TEXT", "proposed by the writer", "REFERENCE ANSWER"):
                self.assertNotIn(text, u)
        u = self.prompt(with_reference=True)
        self.assertIn("REFERENCE ANSWER:\nREFERENCE-TEXT-A.", u)
        for text in ("ALT-TEXT-A", "GENERIC-TEXT-A", "NOTE-TEXT", "NEARMISS"):
            self.assertNotIn(text, u)

    def test_question_and_evidence_are_in_the_input(self):
        u = self.prompt()
        self.assertIn("QUESTION:\nWhat is going on?", u)
        self.assertIn("EVIDENCE (pasted by the operator):\nlog line one", u)

    def test_items_file_fills_question_and_evidence_of_a_rubric_that_has_neither(self):
        tmp = Path(tempfile.mkdtemp())
        (tmp / "chunks.jsonl").write_text("".join(json.dumps(c) + "\n" for c in CHUNKS.values()))
        (tmp / "items.jsonl").write_text(json.dumps(RICH_ITEM) + "\n")
        (tmp / "rubrics.jsonl").write_text(json.dumps(RICH) + "\n")
        (tmp / "ans.jsonl").write_text(json.dumps({"id": "r-1", "text": "An answer."}) + "\n")
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = ej.main(["run", "--rubrics", str(tmp / "rubrics.jsonl"), "--items", str(tmp / "items.jsonl"), "--chunks", str(tmp / "chunks.jsonl"),
                          "--answers", f"x={tmp / 'ans.jsonl'}", "--stub", "--instruction", "v2"])
        self.assertEqual(rc, 0)
        self.assertIn("matched x: 1 answers", buf.getvalue())


class SourcePaths(unittest.TestCase):
    INDEX = {"raw/src/a.md": ["c1"], "raw/src/b.md": ["c2", "c3"], "data/rb/x.md": ["c4"]}

    def test_paths_by_segment_searched_and_inferred_name_no_file(self):
        m = {p["id"]: p for p in RICH["must"]}
        self.assertEqual(ej.point_paths(m["m1"]), ["raw/src/a.md"])
        self.assertEqual(ej.point_paths(m["m2"]), [])                       # 'searched raw/src/dir ...' and 'inferred'
        self.assertEqual(ej.point_paths(m["m3"]), [])
        self.assertEqual(ej.point_catalogue_paths(m["m3"]), ["raw_catalogue/tools/T.java"])
        self.assertEqual(ej.point_paths(m["m4"]), ["data/rb/x.md", "raw/src/b.md"])
        self.assertEqual(ej.point_paths({"source": "raw/a.md:300,311; raw/b.md:1-2 (note)"}), ["raw/a.md", "raw/b.md"])

    def test_source_kinds_and_chunks(self):
        m = {p["id"]: p for p in RICH["must"]}
        self.assertEqual([ej.point_source_kind(m[k], self.INDEX) for k in ("m1", "m2", "m3", "m4")], ["index", "none", "catalogue", "index"])
        self.assertEqual(ej.point_source_kind({"source": "raw/zzz/none.md:1"}, self.INDEX), "not_indexed")
        self.assertEqual(ej.point_chunks(m["m4"], self.INDEX), ["c2", "c3", "c4"])

    def test_canonical_chunk_path_prefixes_a_root_relative_path(self):
        self.assertEqual(ej.canon_path({"source": "strimzi", "path": "docs/x.adoc"}), "raw/strimzi/docs/x.adoc")
        self.assertEqual(ej.canon_path({"source": "strimzi", "path": "raw/strimzi/docs/x.adoc"}), "raw/strimzi/docs/x.adoc")
        self.assertEqual(ej.canon_path({"source": "runbooks", "path": "data/runbooks/a.md"}), "data/runbooks/a.md")

    def test_unsupported_rules_with_an_index(self):
        r = dict(RICH, must=[dict(p, weight=2) for p in RICH["must"]] + [{"id": "m5", "text": "z", "weight": 2, "first": False, "source": "raw/q/gone.md:1"}])
        got = ej.supported_points(r, ["m1", "m2", "m3", "m4", "m5"], ["c1"], ["raw/src/a.md"], self.INDEX)
        self.assertEqual([(x["id"], x["supported"], x["rule"]) for x in got],
                         [("m1", True, "path"), ("m2", False, "none"), ("m3", False, "catalogue"), ("m4", False, "path"), ("m5", False, "not_indexed")])
        # without an index the rules are the old ones
        self.assertEqual(ej.supported_points(r, ["m5"], ["c1"], ["raw/src/a.md"])[0]["rule"], "path")


class Probes(unittest.TestCase):
    def test_probe_sets_from_the_rubric_only(self):
        prubs, sets = ej.make_probes([dict(RICH)], {"r-1": RICH_ITEM})
        self.assertEqual([r["id"] for r in prubs], ["r-1", "r-1~n1", "r-1~n2"])
        self.assertEqual(prubs[0]["question"], "What is going on?")
        self.assertEqual(prubs[1]["evidence"], "log line one")
        self.assertEqual(prubs[1]["probe_of"], "r-1")
        self.assertEqual(sets["probe_generic"], [{"id": "r-1", "label": "probe_generic", "text": "GENERIC-TEXT-A"}])
        self.assertEqual(sets["probe_alt"][0]["text"], "ALT-TEXT-A")
        self.assertEqual(sets["probe_ref"][0]["text"], "REFERENCE-TEXT-A.\nSecond line.")
        self.assertEqual([x["id"] for x in sets["probe_nearmiss"]], ["r-1~n1", "r-1~n2"])
        self.assertEqual(sets["probe_nearmiss"][0]["text"], "REFERENCE-TEXT-A. Second line. NEARMISS-SENTENCE-ONE")   # one paragraph
        self.assertEqual(sets["probe_nearmiss"][1]["text"], "REFERENCE-TEXT-A. Second line. NEARMISS-SENTENCE-TWO")

    def test_missing_field_is_an_error_nothing_is_guessed(self):
        for field in ("generic", "correct_alternative", "reference"):
            with self.assertRaises(ValueError):
                ej.make_probes([dict(RICH, **{field: ""})], {"r-1": RICH_ITEM})
        bad = dict(RICH, must_not=[dict(RICH["must_not"][0], near_miss="")])
        with self.assertRaises(ValueError):
            ej.make_probes([bad], {"r-1": RICH_ITEM})
        with self.assertRaises(ValueError):
            ej.make_probes([RICH], {})

    def test_gen_probes_then_stub_run_judges_every_probe_answer_once(self):
        tmp = Path(tempfile.mkdtemp())
        (tmp / "chunks.jsonl").write_text("".join(json.dumps(c) + "\n" for c in CHUNKS.values()))
        (tmp / "items.jsonl").write_text(json.dumps(RICH_ITEM) + "\n")
        (tmp / "rubrics.jsonl").write_text(json.dumps(RICH) + "\n")
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            self.assertEqual(ej.main(["gen-probes", "--rubrics", str(tmp / "rubrics.jsonl"), "--items", str(tmp / "items.jsonl"),
                                      "--out-dir", str(tmp / "pr")]), 0)
        sets = [f"{n}={tmp / 'pr' / (n + '.jsonl')}" for n in ("probe_generic", "probe_alt", "probe_ref", "probe_nearmiss")]
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = ej.main(["run", "--rubrics", str(tmp / "pr/probe_rubrics.jsonl"), "--items", str(tmp / "items.jsonl"),
                          "--chunks", str(tmp / "chunks.jsonl"), "--answers", *sets, "--stub", "--instruction", "v2",
                          "--passes", "1", "--out", str(tmp / "out.jsonl")])
        self.assertEqual(rc, 0, buf.getvalue())
        self.assertIn("matched probe_nearmiss: 2 answers", buf.getvalue())
        self.assertIn("judge calls 5", buf.getvalue())
        rows = [json.loads(x) for x in (tmp / "out.jsonl").read_text().splitlines()]
        self.assertEqual(sorted((r["set"], r["id"]) for r in rows),
                         [("probe_alt", "r-1"), ("probe_generic", "r-1"), ("probe_nearmiss", "r-1~n1"), ("probe_nearmiss", "r-1~n2"), ("probe_ref", "r-1")])


class ChainScript(unittest.TestCase):
    SH = ROOT / "scripts/m4_s5b_judge_pilot2.sh"

    def test_syntax_and_executable(self):
        import os
        import subprocess
        self.assertTrue(os.access(self.SH, os.X_OK))
        self.assertEqual(subprocess.run(["sh", "-n", str(self.SH)]).returncode, 0)

    def refusal(self, *args):
        import subprocess
        hb = ROOT / "runs/v3/HEARTBEAT.md"
        before = hb.read_bytes() if hb.exists() else None
        r = subprocess.run(["sh", str(self.SH), *args], cwd=ROOT, capture_output=True, text=True, timeout=60)
        self.assertEqual(r.returncode, 2, r.stdout + r.stderr)
        self.assertIn("REFUSED", r.stderr)
        self.assertEqual(before, hb.read_bytes() if hb.exists() else None)          # no heartbeat line
        return r

    def test_refuses_to_start_without_items_or_rubrics_and_writes_nothing(self):
        import subprocess
        if (ROOT / "eval/m4/pilot2/items.jsonl").exists():
            self.skipTest("the pilot's items exist: a real start would not be a refusal (see the weights test)")
        self.refusal()
        self.assertEqual(subprocess.run(["sh", str(self.SH), "--nonsense"], cwd=ROOT, capture_output=True).returncode, 2)

    def test_refuses_while_the_weights_are_only_proposed(self):
        rub = ROOT / "eval/m4/pilot2/rubrics.jsonl"
        if not rub.exists() or not all((ROOT / f).exists() for f in ("work/v3/chunks_s1.jsonl", "work/m4/catalogues/CATALOGUES.json", "eval/m4/pilot2/items.jsonl")):
            self.skipTest("the pilot's files or the generated inputs are not on this machine")
        if not any("propos" in str(json.loads(l).get("weights_status", "")).lower() for l in rub.read_text().splitlines() if l.strip()):
            self.skipTest("the weights are final: a real start would not be a refusal")
        r = self.refusal()
        self.assertIn("weights", r.stderr)

    def test_unknown_option_is_refused(self):
        import subprocess
        self.assertEqual(subprocess.run(["sh", str(self.SH), "--nonsense"], cwd=ROOT, capture_output=True).returncode, 2)

    def test_order_of_work_answers_then_unload_then_all_judging(self):
        t = self.SH.read_text()
        i_ans, i_unload, i_main, i_probes, i_ref, i_unload_judge = (
            t.index("answer_run strict strict"), t.index("unload $Q4;"), t.index("phase main"), t.index("phase probes"),
            t.index("phase ref"), t.index("unload $DS; hb"))
        self.assertTrue(i_ans < i_unload < i_main < i_probes < i_ref < i_unload_judge)
        self.assertIn("--proposed-weights", t)

    def test_dry_run_completes_with_stubs_on_the_real_items(self):
        import subprocess
        need = [ROOT / "work/v3/chunks_s1.jsonl", ROOT / "work/m4/catalogues/CATALOGUES.json", ROOT / "eval/m4/pilot2/items.jsonl",
                ROOT / "eval/m4/pilot2/rubrics.jsonl"]
        if not all(p.exists() for p in need) or not (ROOT / ".venv/bin/python").exists():
            self.skipTest("generated inputs (index, catalogues) or the pilot's files not on this machine")
        tmp = tempfile.mkdtemp()
        r = subprocess.run(["sh", str(self.SH), "--dry-run", "--reference-arm"], cwd=ROOT, capture_output=True, text=True, timeout=900,
                           env={**__import__("os").environ, "TMPDIR": tmp})
        self.assertEqual(r.returncode, 0, r.stdout[-2000:] + r.stderr[-2000:])
        self.assertIn("dry run done", r.stdout)
        for line in ("matched default: 24 answers", "matched strict: 24 answers", "instruction v2: 48 of 48 answers have their own excerpts",
                     "matched probe_nearmiss: 46 answers", "matched default_ref: 24 answers", "judge calls 262 = 96 ", "## Rubric probes",
                     "## Sources of the must points", "Go or no-go"):
            self.assertIn(line, r.stdout)


if __name__ == "__main__":
    unittest.main()
