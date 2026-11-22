import contextlib
import io
import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "tests"))
import eval2  # noqa: E402
from kafka_sre_rag import prompts  # noqa: E402
from test_eval2 import Base, item, select_stub  # noqa: E402


class TestPromptHash(Base):
    def test_rows_carry_hashes_and_manifest_names_the_form(self):
        f, out = self.items_file([item(1), item(2)]), self.tmp / "a.jsonl"
        rc, _ = self.run_answer(f, out)
        self.assertEqual(rc, 0)
        rows = eval2.read_jsonl(out)
        for r in rows:
            self.assertEqual(len(r["prompt_sha256"]), 64)
            self.assertEqual(len(r["search_terms_prompt_sha256"]), 64)
            self.assertGreater(r["prompt_chars"], 0)
        # the hash is the documented form, computed independently
        msgs = prompts.answer_messages(item(1)["question"], select_stub(item(1)["question"], ""))
        blob = json.dumps([{"role": m["role"], "content": m["content"]} for m in msgs], sort_keys=True,
                          ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.assertEqual(rows[0]["prompt_sha256"], eval2.sha256_bytes(blob))
        self.assertEqual(rows[0]["prompt_chars"], sum(len(m["content"]) for m in msgs))
        self.assertIn("sha256", eval2.read_manifest(out)["prompt_hash_form"])

    def test_closed_book_has_no_search_terms_hash(self):
        f, out = self.items_file([item(1)]), self.tmp / "c.jsonl"
        self.run_answer(f, out, "--mode", "closed")
        r = eval2.read_jsonl(out)[0]
        self.assertIn("prompt_sha256", r)
        self.assertNotIn("search_terms_prompt_sha256", r)

    def test_other_fields_unchanged_apart_from_the_new_ones(self):
        f, out = self.items_file([item(1)]), self.tmp / "d.jsonl"
        self.run_answer(f, out)
        r = eval2.read_jsonl(out)[0]
        for k in ("prompt_sha256", "prompt_chars", "search_terms_prompt_sha256", "search_terms_prompt_chars"):
            r.pop(k)
        self.assertEqual(set(r), {"id", "text", "tokens", "truncated", "seconds", "search_terms", "pack", "error", "run"})
        self.assertEqual(r["text"], "answer text")

    def test_old_manifest_without_the_field_still_resumes(self):
        f, out = self.items_file([item(1), item(2)]), self.tmp / "e.jsonl"
        self.run_answer(f, out, "--limit", "1")
        m = eval2.read_manifest(out)
        m.pop("prompt_hash_form")
        eval2.write_manifest(out, m)
        rc, _ = self.run_answer(f, out, "--limit", "1")
        self.assertEqual(rc, 0)

    def test_compare(self):
        a = [{"id": "a", "prompt_sha256": "x"}, {"id": "b", "prompt_sha256": "y"}, {"id": "c"}, {"id": "d", "prompt_sha256": "z"}]
        b = [{"id": "a", "prompt_sha256": "x"}, {"id": "b", "prompt_sha256": "Y"}, {"id": "c", "prompt_sha256": "q"}, {"id": "e", "prompt_sha256": "z"}]
        r = eval2.compare_prompt_hashes(a, b)
        self.assertEqual((r["common"], r["equal"], r["differ"], r["not_recorded"], r["only_a"], r["only_b"]),
                         (3, ["a"], ["b"], ["c"], ["d"], ["e"]))

    def test_compare_subcommand_prints_counts_first(self):
        pa, pb = self.tmp / "a.jsonl", self.tmp / "b.jsonl"
        eval2.write_jsonl(pa, [{"id": "a", "prompt_sha256": "x"}, {"id": "b", "prompt_sha256": "y"}])
        eval2.write_jsonl(pb, [{"id": "a", "prompt_sha256": "x"}, {"id": "b"}])
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = eval2.main(["compare-prompts", str(pa), str(pb)])
        self.assertEqual(rc, 0)
        lines = buf.getvalue().splitlines()
        self.assertIn("2 ids in both: 1 equal, 0 differ, 1 not recorded", lines[0])
        self.assertIn("not_recorded: b", lines[1])


if __name__ == "__main__":
    unittest.main()
