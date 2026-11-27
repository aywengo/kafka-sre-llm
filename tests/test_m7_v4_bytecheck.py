"""M7 B21: the v4.0.0 byte check (scripts/m7_v4_bytecheck.py) on synthetic data: no D7 text, no server, no model."""
import copy
import json
import sys
import tempfile
import types
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
import m7_v4_bytecheck as B  # noqa: E402
from kafka_sre_rag import generators  # noqa: E402


def side(ids, **over):
    items = {}
    for i in ids:
        bodies = {n: {"plain": "p" + i + n, "stream": "s" + i + n, "keys_plain": ["messages", "model"],
                      "keys_stream": ["messages", "model", "stream"]} for n in ("expand", "answer")}
        items[i] = {"id": i, "expand_sha": "e" + i, "answer_sha": "a" + i, "bodies": bodies, "pack_chars": 100, "n_pack": 2,
                    "cards": {"cards_found": 1, "cards": 1, "chars": 50}, "sources": [["s", "t", "p"], ["s2", "t2", "p2"]]}
    return {"package": "/x", "constants": dict(B.EXPECTED), "items": items, "tools_imported": False, **over}


def rows(ids):
    return [{"id": i, "pack": ["a", "b"], "kb_entries": {"pack_chars": 100}, "lookup": {"cards_found": 1, "cards": 1, "chars": 50},
             "sources": [{"source": "s", "title": "t", "path": "p"}, {"source": "s2", "title": "t2", "path": "p2"}]} for i in ids]


class Compare(unittest.TestCase):
    ids = ["d-1", "d-2", "d-3"]

    def test_identical(self):
        lines, total, ok = B.compare(side(self.ids), side(self.ids), rows(self.ids))
        self.assertTrue(ok)
        self.assertEqual(total, "3/3 identical")
        self.assertEqual(len(lines), 3)

    def test_changed_messages_name_the_item(self):
        new = side(self.ids)
        new["items"]["d-2"]["answer_sha"] = "other"
        lines, total, ok = B.compare(side(self.ids), new, rows(self.ids))
        self.assertFalse(ok)
        self.assertEqual(total, "2/3 identical; differing: d-2")
        self.assertIn("d-2 DIFFERENT: answer messages differ", lines[1])

    def test_changed_request_body(self):
        new = side(self.ids)
        new["items"]["d-3"]["bodies"]["answer"]["plain"] = "other"
        _, total, ok = B.compare(side(self.ids), new, rows(self.ids))
        self.assertFalse(ok)
        self.assertIn("differing: d-3", total)

    def test_both_sides_wrong_the_same_way_is_caught_by_the_recorded_row(self):
        bad = side(self.ids)
        bad["items"]["d-1"]["pack_chars"] = 99
        _, total, ok = B.compare(bad, copy.deepcopy(bad), rows(self.ids))
        self.assertFalse(ok)
        self.assertIn("differing: d-1", total)

    def test_error_in_a_child_is_a_difference(self):
        new = side(self.ids)
        new["items"]["d-1"] = {"id": "d-1", "error": "KeyError: x"}
        _, total, ok = B.compare(side(self.ids), new, rows(self.ids))
        self.assertFalse(ok)
        self.assertIn("d-1", total)

    def test_payload_report(self):
        _, ok = B.payload_report(side(self.ids), side(self.ids))
        self.assertTrue(ok)
        new = side(self.ids)
        new["items"]["d-1"]["bodies"]["answer"]["keys_plain"] = ["messages", "model", "tools"]
        lines, ok = B.payload_report(side(self.ids), new)
        self.assertFalse(ok)
        self.assertTrue(any("tools" in l and "FAIL" in l for l in lines))
        _, ok = B.payload_report(side(self.ids), side(self.ids, tools_imported=True))
        self.assertFalse(ok)

    def test_constants_report(self):
        _, ok = B.constants_report(side(self.ids), side(self.ids))
        self.assertTrue(ok)
        new = side(self.ids)
        new["constants"]["INSTRUCTION"] = "0" * 64
        lines, ok = B.constants_report(side(self.ids), new)
        self.assertFalse(ok)
        self.assertTrue(any("INSTRUCTION" in l and "DIFFERENT" in l for l in lines))

    def test_constants_of_a_changed_prompt(self):
        mod = types.SimpleNamespace(SYSTEM_PROMPT="s", INSTRUCTION="i", EXPAND_SYSTEM="e")
        self.assertNotEqual(B.constants_of(mod), B.EXPECTED)

    def test_full_question(self):
        self.assertEqual(B.full_question({"question": "q"}), "q")
        self.assertEqual(B.full_question({"question": "q", "evidence": "e"}), "q\n\nEvidence:\ne")


class Bodies(unittest.TestCase):
    def test_stub_transport_no_tools_key_and_stable_bytes(self):
        msgs = {"answer": [{"role": "system", "content": "s"}, {"role": "user", "content": "u"}]}
        a = B.capture_bodies(generators, msgs, {"answer": 5})
        b = B.capture_bodies(generators, msgs, {"answer": 5})
        self.assertEqual(a, b)
        self.assertEqual(a["answer"]["keys_plain"], ["chat_template_kwargs", "max_tokens", "messages", "model", "temperature"])
        self.assertNotIn("tools", a["answer"]["keys_stream"])
        c = B.capture_bodies(generators, {"answer": [{"role": "user", "content": "other"}]}, {"answer": 5})
        self.assertNotEqual(a["answer"]["plain"], c["answer"]["plain"])


class Cli(unittest.TestCase):
    def test_check_exit_code_follows_the_result(self):
        with tempfile.TemporaryDirectory() as d:
            ans, items = Path(d) / "a.jsonl", Path(d) / "i.jsonl"
            ans.write_text(json.dumps(rows(["d-1"])[0]) + "\n")
            items.write_text(json.dumps({"id": "d-1", "question": "q"}) + "\n")
            outs = [side(["d-1"], package="/old"), side(["d-1"], package="/new")]
            orig = B.run_emit
            B.run_emit = lambda tree, answers, items: outs.pop(0)
            try:
                import contextlib
                import io
                buf = io.StringIO()
                with contextlib.redirect_stdout(buf):
                    rc = B.main(["check", "--old", "o", "--new", "n", "--answers", str(ans), "--items", str(items)])
                self.assertEqual(rc, 0)
                self.assertIn("(a) 1/1 identical", buf.getvalue())
                outs[:] = [side(["d-1"], package="/old"), side(["d-1"], package="/new")]
                outs[1]["items"]["d-1"]["expand_sha"] = "x"
                buf = io.StringIO()
                with contextlib.redirect_stdout(buf):
                    rc = B.main(["check", "--old", "o", "--new", "n", "--answers", str(ans), "--items", str(items)])
                self.assertEqual(rc, 1)
                self.assertIn("differing: d-1", buf.getvalue())
            finally:
                B.run_emit = orig


if __name__ == "__main__":
    unittest.main()
