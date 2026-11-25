"""M7 KB phase 2, I2: the writer and the checker against an in-process stub server (scripts/kb_write_entries.py, scripts/kb_check_entries.py)."""
import contextlib
import hashlib
import io
import json
import os
import re
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
from kafka_sre_rag import prompts as P  # noqa: E402
import kb_slice  # noqa: E402
import kb_write_entries as W  # noqa: E402
import kb_check_entries as C  # noqa: E402

TEXT = "The `{k}` property sets the value of setting {n}. The default is {n}0 and it applies to brokers."


def fixture(d, n=6):
    rows = []
    for i in range(n):
        rows.append({"id": f"c{i:03d}", "source": "strimzi" if i % 2 else "apache-kafka", "path": f"raw/x/page{i // 2}.md",
                     "title": f"Page {i // 2}", "text": TEXT.format(k=f"spec.key{i}", n=i + 1)})
    ch = Path(d) / "chunks.jsonl"
    ch.write_text("".join(json.dumps(r) + "\n" for r in rows))
    rec = Path(d) / "rec.json"
    rec.write_text(json.dumps({"index_path": str(ch), "sha256": hashlib.sha256(ch.read_bytes()).hexdigest(),
                               "source_commits": {"strimzi": "s1", "apache-kafka": "k1"}}))
    sl = Path(d) / "slice.jsonl"
    with contextlib.redirect_stdout(io.StringIO()):
        assert kb_slice.main(["--record", str(rec), "--out", str(sl)]) == 0
    return rec, sl, [r["id"] for r in rows]


def run(fn, args, env=None):
    out, err = io.StringIO(), io.StringIO()
    with mock.patch.dict(os.environ, env or {}, clear=False), contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = fn(args)
    return code, out.getvalue(), err.getvalue()


def rows(p):
    with open(p) as f:
        return [json.loads(l) for l in f if l.strip()]


CLEAN = {k: "" for k in ("KB_STUB_WRITE_BAD", "KB_STUB_WRITE_RETRY", "KB_STUB_WRITE_GATE", "KB_STUB_CHECK_BAD", "KB_STUB_CHECK_PARTIAL", "KB_STUB_STOP_AFTER")}


class Writer(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.TemporaryDirectory()
        self.d = Path(self.td.name)
        self.rec, self.sl, self.ids = fixture(self.d)
        self.out = self.d / "w"
        self.base = ["--slice", str(self.sl), "--record", str(self.rec), "--select", "all", "--out-dir", str(self.out), "--backend", "stub",
                     "--done", str(self.d / "w.done")]

    def tearDown(self):
        self.td.cleanup()

    def test_lines_retry_failed_gate_and_resume(self):
        env = dict(CLEAN, KB_STUB_WRITE_BAD="c001", KB_STUB_WRITE_RETRY="c002", KB_STUB_WRITE_GATE="c003")
        code, out, _ = run(W.main, self.base, env)
        self.assertEqual(code, 0)
        lines = [l for l in out.splitlines() if re.match(r"\[write\] \d+/6 c\d{3} entries=\d+ gate_fail=\d+ [\d.]+s tokens=\d+", l)]
        self.assertEqual(len(lines), 6)                                    # a line per window
        self.assertTrue(any("c001" in l and "FAILED" in l for l in lines))
        self.assertTrue(any("c002" in l and "retried" in l for l in lines))
        wr = {r["window"]: r for r in rows(self.out / "windows.jsonl")}
        self.assertEqual((wr["c001"]["status"], wr["c001"]["attempts"]), ("failed", 2))
        self.assertEqual((wr["c002"]["status"], wr["c002"]["attempts"]), ("ok", 2))
        self.assertEqual(wr["c003"]["gate_rejects"], 1)
        rej = rows(self.out / "entries_gate_rejects.jsonl")
        self.assertIn("'stub.invented.key' is in no quote", " ".join(rej[0]["reasons"]))
        ents = rows(self.out / "entries_raw.jsonl")
        self.assertEqual(len(ents), 5)
        self.assertTrue(all(e["prompt_version"] == P.KB_ENTRY_PROMPT_VERSION and e["writer"] == W.WRITER_MODEL for e in ents))
        self.assertIn("1 failed, 2 retried", out)
        snap = {f: (self.out / f).read_bytes() for f in W.FILES}
        mk = (self.d / "w.done").read_bytes()
        code, out, _ = run(W.main, self.base, CLEAN)                       # resume: nothing new
        self.assertEqual(code, 0)
        self.assertNotIn("to write", out)
        self.assertIn("unchanged (nothing new)", out)
        self.assertEqual({f: (self.out / f).read_bytes() for f in W.FILES}, snap)
        self.assertEqual((self.d / "w.done").read_bytes(), mk)

    def test_stop_midway_and_partial_window_lines(self):
        code, _, err = run(W.main, self.base + ["--concurrency", "1"], dict(CLEAN, KB_STUB_STOP_AFTER="3"))
        self.assertEqual(code, 1)
        self.assertIn("HTTP 500", err)
        self.assertEqual(len(rows(self.out / "windows.jsonl")), 3)
        with open(self.out / "entries_raw.jsonl", "a") as f:               # a line of a window without its row (a stop between the lines)
            f.write(json.dumps({"window": {"target": "c005"}, "id": "kbe-x", "prompt_version": P.KB_ENTRY_PROMPT_VERSION, "writer": W.WRITER_MODEL}) + "\n")
        code, out, _ = run(W.main, self.base, CLEAN)
        self.assertEqual(code, 0)
        self.assertIn("removed 1 line(s)", out)
        self.assertIn("3 window(s) to write of 6 (3 done)", out)
        self.assertEqual(len(rows(self.out / "windows.jsonl")), 6)
        self.assertEqual(len(rows(self.out / "entries_raw.jsonl")), 6)

    def test_another_model_answering_is_a_refusal(self):
        code, out, err = run(W.main, self.base + ["--concurrency", "1"], dict(CLEAN, KB_STUB_WRONG_MODEL_AFTER="2"))
        self.assertEqual(code, 2)
        self.assertIn("another client changed the server", err)
        self.assertIn("SERVER ERROR", out)                                   # the window of the error prints a line
        self.assertEqual(len(rows(self.out / "windows.jsonl")), 2)

    def test_stale_version_and_other_model_refused(self):
        run(W.main, self.base, CLEAN)
        with mock.patch.object(P, "KB_ENTRY_PROMPT_VERSION", "kbe/9"):
            code, _, err = run(W.main, self.base, CLEAN)
        self.assertEqual(code, 2)
        self.assertIn("never mixed", err)
        code, _, err = run(W.main, self.base + ["--model", "Other"], CLEAN)
        self.assertEqual(code, 2)

    def test_reuse(self):
        sub = self.d / "sub.txt"
        sub.write_text("c000\nc003\n")
        first = ["--slice", str(self.sl), "--record", str(self.rec), "--select", str(sub), "--out-dir", str(self.d / "s"), "--backend", "stub"]
        self.assertEqual(run(W.main, first, CLEAN)[0], 0)
        code, out, _ = run(W.main, self.base + ["--reuse", str(self.d / "s")], CLEAN)
        self.assertEqual(code, 0)
        self.assertEqual(out.count(" reused"), 2)
        self.assertIn("4 window(s) to write of 6 (2 done)", out)

    def test_sanity_selection(self):
        ids = [f"id{i}" for i in range(200)]
        s = W.sanity_ids(ids)
        self.assertEqual(len(s), 50)
        self.assertEqual(s, [i for i in ids if i in set(s)])               # index order
        self.assertEqual(set(s), set(sorted(ids, key=lambda i: hashlib.sha256(("kb2-sanity:" + i).encode()).hexdigest())[:50]))

    def test_sanity2_selection(self):
        ids = [f"id{i}" for i in range(400)]
        s1, s2 = W.sanity_ids(ids), W.sanity2_ids(ids)
        self.assertEqual((len(s2), len(set(s1) & set(s2))), (50, 0))          # fresh: disjoint from the first 50
        self.assertEqual(s2, [i for i in ids if i in set(s2)])                # index order
        rest = [i for i in ids if i not in set(s1)]
        self.assertEqual(set(s2), set(sorted(rest, key=lambda i: hashlib.sha256(("kb2-sanity2:" + i).encode()).hexdigest())[:50]))
        self.assertEqual(W.sanity2_ids(ids), s2)                              # deterministic
        self.assertEqual(W.select([{"id": i} for i in ids], "sanity2"), s2)
        self.assertEqual(W.select([{"id": i} for i in ids], "sanity"), s1)    # the first selection is unchanged

    def test_request_body_and_no_address_written(self):
        seen = []

        def handler(body):
            seen.append(body)
            return json.dumps({"entries": []})
        c = W.Client("http://stub.invalid/v1", "secret-key-xyz", "M", transport=W.stub_transport(handler))
        c.chat([{"role": "user", "content": "x"}], 3000)
        self.assertEqual(seen[0]["chat_template_kwargs"], {"thinking_mode": "chat"})
        self.assertEqual((seen[0]["temperature"], seen[0]["max_tokens"], seen[0]["model"]), (0.0, 3000, "M"))
        with mock.patch.dict(os.environ, dict(CLEAN, OMLX_URL="http://omlx-test.invalid:8000/v1", OMLX_API_KEY="secret-key-xyz")):
            real = W.Client
            with mock.patch.object(W, "Client", lambda url, key, model, transport=None: real("http://stub.invalid/v1", key, model,
                                                                                                    transport=W.stub_transport(W.stub_writer(
                                                                                                        {r["id"]: r for r in rows(self.d / "chunks.jsonl")})))):
                code, out, err = run(W.main, self.base[:-2] + ["--backend", "openai", "--done", str(self.d / "w.done")])
        self.assertEqual(code, 0)
        blob = out + err + "".join(p.read_text() for p in self.d.rglob("*") if p.is_file())
        self.assertNotIn("omlx-test.invalid", blob)
        self.assertNotIn("secret-key-xyz", blob)


class Checker(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.TemporaryDirectory()
        self.d = Path(self.td.name)
        self.rec, self.sl, self.ids = fixture(self.d)
        self.wd = self.d / "w"
        run(W.main, ["--slice", str(self.sl), "--record", str(self.rec), "--select", "all", "--out-dir", str(self.wd), "--backend", "stub",
                     "--done", str(self.d / "w.done")], CLEAN)
        self.base = ["--slice", str(self.sl), "--record", str(self.rec), "--write-dir", str(self.wd), "--write-done", str(self.d / "w.done"),
                     "--out-dir", str(self.wd), "--backend", "stub", "--done", str(self.d / "c.done")]

    def tearDown(self):
        self.td.cleanup()

    def test_verdicts_unchecked_lines_and_resume(self):
        code, out, _ = run(C.main, self.base, dict(CLEAN, KB_STUB_CHECK_BAD="c004", KB_STUB_CHECK_PARTIAL="c000"))
        self.assertEqual(code, 0)
        lines = [l for l in out.splitlines() if re.match(r"\[check\] \d+/6 c\d{3} entries=\d+ ok=\d+ partial=\d+ wrong=\d+ unsupported=\d+ unchecked=\d+", l)]
        self.assertEqual(len(lines), 6)
        ch = rows(self.wd / "checks.jsonl")
        self.assertEqual(len(ch), 6)
        v = {r["window"]: r["verdict"] for r in ch}
        self.assertEqual(v["c004"], "unchecked")           # its window and its neighbours' windows show c004 (stub plant by any chunk shown)
        self.assertEqual(v["c000"], "partial")
        self.assertTrue(all(r["check_prompt_version"] == P.KB_CHECK_PROMPT_VERSION and r["checker"] == C.CHECKER_MODEL for r in ch))
        self.assertEqual({r["attempts"] for r in ch if r["verdict"] == "unchecked"}, {2})
        snap = (self.wd / "checks.jsonl").read_bytes()
        code, out, _ = run(C.main, self.base, CLEAN)
        self.assertEqual(code, 0)
        self.assertIn("unchanged (nothing new)", out)
        self.assertEqual((self.wd / "checks.jsonl").read_bytes(), snap)

    def test_refusals(self):
        self.assertEqual(run(C.main, self.base + ["--model", W.WRITER_MODEL], CLEAN)[0], 2)
        self.assertEqual(run(C.main, self.base + ["--model", C.FALLBACK_MODEL], CLEAN)[0], 2)
        self.assertEqual(run(C.main, self.base + ["--model", "Other"], CLEAN)[0], 2)
        b = list(self.base)
        b[b.index("--write-done") + 1] = str(self.d / "missing.done")
        self.assertEqual(run(C.main, b, CLEAN)[0], 2)
        with mock.patch.object(P, "KB_ENTRY_PROMPT_VERSION", "kbe/9"):
            self.assertEqual(run(C.main, self.base, CLEAN)[0], 2)      # stale writer rows
        self.assertEqual(run(C.main, self.base, CLEAN)[0], 0)
        with mock.patch.object(P, "KB_CHECK_PROMPT_VERSION", "kbc/9"):
            self.assertEqual(run(C.main, self.base, CLEAN)[0], 2)      # stale check rows
        code, out, _ = run(C.main, self.base[:-2] + ["--model", C.FALLBACK_MODEL, "--fallback-note", "conductor: oQ8e not available",
                                                      "--out-dir", str(self.d / "c2")], CLEAN)
        self.assertEqual(code, 0)

    def test_title_sent_and_prompt_names_it(self):
        ents = rows(self.wd / "entries_raw.jsonl")
        line = json.loads(C.entry_lines(ents[:1]))
        self.assertEqual(line["title"], ents[0]["title"])
        self.assertIn("each with its title", P.KB_CHECK_USER)
        self.assertIn("every claim of the title, of the statement", P.KB_CHECK_USER)

    def test_retry_never_upgrades_a_verdict(self):
        ents = rows(self.wd / "entries_raw.jsonl")[:2]
        a, b = ents[0]["id"], ents[1]["id"]
        replies = iter([json.dumps({"verdicts": [{"id": a, "verdict": "partial", "reason": "first"}]}),
                        json.dumps({"verdicts": [{"id": a, "verdict": "ok", "reason": "retry"}, {"id": b, "verdict": "ok", "reason": "retry"}]})])
        client = W.Client("http://stub.invalid/v1", None, C.CHECKER_MODEL, transport=W.stub_transport(lambda body: next(replies)))
        got, attempts, _, _ = C.check_window(client, [], ents, 100)
        self.assertEqual(attempts, 2)
        self.assertEqual(got[a], ("partial", "first"))
        self.assertEqual(got[b], ("ok", "retry"))

    def test_another_model_answering_is_a_refusal(self):
        code, out, err = run(C.main, self.base, dict(CLEAN, KB_STUB_WRONG_MODEL_AFTER="2"))
        self.assertEqual(code, 2)
        self.assertIn("REFUSED", err)
        self.assertIn("SERVER ERROR", out)

    def test_parse_verdicts(self):
        self.assertEqual(C.parse_verdicts({"verdicts": [{"id": "a", "verdict": "ok", "reason": "r"}, {"id": "b", "verdict": "maybe"},
                                                        {"id": "z", "verdict": "ok"}]}, {"a", "b"}), {"a": ("ok", "r")})
        self.assertEqual(C.parse_verdicts(None, {"a"}), {})
        self.assertEqual(W.parse_json('```json\n{"verdicts": []}\n```'), {"verdicts": []})
        self.assertIsNone(W.parse_json("no json"))


class NoSecretsInFiles(unittest.TestCase):
    def test_no_address_key_or_home_path(self):
        for f in ("scripts/kb_slice.py", "scripts/kb_write_entries.py", "scripts/kb_check_entries.py", "scripts/m7_kb2.sh", "scripts/m7_kb2_rule.py",
                  "kafka_sre_rag/kb_entry.py", "tests/test_kb_entry.py", "tests/test_kb_write_check.py", "tests/test_m7_kb2_chain.py"):
            t = (ROOT / f).read_text()
            self.assertIsNone(re.search(r"\b(?:\d{1,3}\.){3}\d{1,3}\b(?!\.)", t), f)
            self.assertNotIn("/Users" + "/", t, f)
            self.assertIsNone(re.search(r"sk-[A-Za-z0-9]{16,}", t), f)


if __name__ == "__main__":
    unittest.main()
