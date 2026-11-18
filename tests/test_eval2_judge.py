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
        self.assertIn(ej.JUDGE_INSTRUCTION, design)
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


if __name__ == "__main__":
    unittest.main()
