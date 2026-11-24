"""M7 KB phase 2, I1: the slice, the entry schema, the rendering and the mechanical gate (kafka_sre_rag/kb_entry.py, scripts/kb_slice.py)."""
import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
from kafka_sre_rag import kb_entry as K, prompts as P  # noqa: E402
import kb_slice  # noqa: E402

T = {"id": "t1", "source": "strimzi", "path": "raw/strimzi/documentation/modules/a.adoc", "title": "Configuring brokers",
     "text": "The `spec.kafka.replicas` property sets the number of broker nodes. The default is 3.\n\nDo not set "
             "min.insync.replicas above the replication factor."}
N1 = {"id": "n1", "source": "strimzi", "path": T["path"], "title": "Configuring brokers", "text": "Use the KafkaNodePool resource "
      "to configure node pools. The maxUnavailable value is 1."}
OTHER = {"id": "o1", "source": "strimzi", "path": "raw/strimzi/documentation/modules/b.adoc", "title": "Other", "text": "Other page."}
WIN = {"t1": T, "n1": N1}


def entry(**kw):
    e = {"kind": "value", "title": "Broker replicas", "statement": "In Strimzi, `spec.kafka.replicas` sets the number of broker nodes; the default is 3.",
         "sources": ["t1"], "quotes": [{"chunk": "t1", "text": "The `spec.kafka.replicas` property sets the number of broker nodes. The default is 3."}]}
    e.update(kw)
    return e


class Slice(unittest.TestCase):
    def test_rule(self):
        self.assertTrue(K.in_slice({"source": "strimzi", "path": "raw/strimzi/documentation/modules/x.adoc"}))
        self.assertTrue(K.in_slice({"source": "apache-kafka", "path": "raw/apache-kafka/docs/operations/x.md"}))
        self.assertFalse(K.in_slice({"source": "strimzi", "path": "raw/strimzi/documentation/contributing/x.adoc"}))
        for s in ("apache-kafka-config", "kubernetes", "runbooks", "argo-cd"):
            self.assertFalse(K.in_slice({"source": s, "path": "raw/x"}), s)

    def _fixture(self, d):
        rows = [T, N1, OTHER, {"id": "c1", "source": "strimzi", "path": "raw/strimzi/documentation/contributing/c.adoc", "title": "", "text": "x"},
                {"id": "k1", "source": "kubernetes", "path": "raw/kubernetes/x.md", "title": "", "text": "y"},
                {"id": "a1", "source": "apache-kafka", "path": "raw/apache-kafka/docs/design/x.md", "title": "", "text": "z"}]
        ch = Path(d) / "chunks.jsonl"
        ch.write_text("".join(json.dumps(r) + "\n" for r in rows))
        rec = Path(d) / "rec.json"
        rec.write_text(json.dumps({"index_path": str(ch), "sha256": hashlib.sha256(ch.read_bytes()).hexdigest(), "source_commits": {}}))
        return ch, rec

    def test_fixture_slice_and_check(self):
        with tempfile.TemporaryDirectory() as d:
            ch, rec = self._fixture(d)
            out = Path(d) / "slice.jsonl"
            self.assertEqual(kb_slice.main(["--record", str(rec), "--out", str(out)]), 0)
            self.assertEqual(kb_slice.load_slice(out), ["t1", "n1", "o1", "a1"])
            self.assertEqual(kb_slice.main(["--record", str(rec), "--check", str(out)]), 0)
            self.assertEqual(kb_slice.main(["--record", str(rec), "--out", str(out)]), 0)     # an equal file is accepted
            self.assertEqual(kb_slice.main(["--record", str(rec), "--out", str(out), "--expect", "5"]), 1)
            out.write_text('{"id": "t1"}\n')
            self.assertEqual(kb_slice.main(["--record", str(rec), "--check", str(out)]), 1)
            self.assertEqual(kb_slice.main(["--record", str(rec), "--out", str(out)]), 2)       # never overwritten
            ch.write_text(ch.read_text() + "\n")
            self.assertEqual(kb_slice.main(["--record", str(rec), "--out", str(Path(d) / "s2.jsonl")]), 2)   # the index's hash differs

    @unittest.skipUnless((ROOT / "work/v3/chunks_s1.jsonl").exists() and (ROOT / "runs/v3/INDEX_FROZEN.json").exists(), "the frozen index is not here")
    def test_real_slice(self):
        chunks, _ = kb_slice.load_index(str(ROOT / "runs/v3/INDEX_FROZEN.json"), str(ROOT / "work/v3/chunks_s1.jsonl"))
        ids = kb_slice.slice_ids(chunks)
        self.assertEqual(len(ids), 1487)
        self.assertEqual(len({c["path"] for c in chunks if K.in_slice(c)}), 478)
        a = hashlib.sha256(kb_slice.slice_bytes(ids)).hexdigest()
        self.assertEqual(a, hashlib.sha256(kb_slice.slice_bytes(kb_slice.slice_ids(chunks))).hexdigest())
        self.assertEqual(a, "d7d1d352ab0e72a624e449154e9fcfdf7890115db48c8bcc6be6c0629efb824e")


class Window(unittest.TestCase):
    def test_neighbours_same_file_only(self):
        sl = [OTHER, T, N1]
        self.assertEqual([c["id"] for c in K.window_of(sl, 1)[1]], ["t1", "n1"])
        self.assertEqual([c["id"] for c in K.window_of(sl, 0)[1]], ["o1"])
        self.assertEqual([c["id"] for c in K.window_of([T, N1, {**OTHER, "path": T["path"]}], 1)[1]], ["t1", "n1", "o1"])

    def test_text_shows_chunks_as_the_model_receives_them(self):
        txt = K.window_text([T, N1], "t1")
        self.assertIn("=== chunk t1 (TARGET) ===\n" + P.pack_text([T]), txt)
        self.assertIn("=== chunk n1 ===\n", txt)
        self.assertNotIn("TARGET", K.window_text([T, N1], None))


class Gate(unittest.TestCase):
    def test_passes(self):
        self.assertEqual(K.gate_entry(entry(), WIN, "t1"), [])

    def test_quote_may_include_the_title_line_and_whitespace_differences(self):
        e = entry(quotes=[{"chunk": "t1", "text": "strimzi — Configuring brokers\nThe `spec.kafka.replicas`   property sets the number of broker nodes. The default is 3."}])
        self.assertEqual(K.gate_entry(e, WIN, "t1"), [])

    def test_not_verbatim(self):
        e = entry(quotes=[{"chunk": "t1", "text": "The spec.kafka.replicas property sets the number of brokers. The default is 3."}])
        self.assertTrue(any("not verbatim" in r for r in K.gate_entry(e, WIN, "t1")))

    def test_window(self):
        e = entry(sources=["t1", "o1"], kind="cross", quotes=entry()["quotes"] + [{"chunk": "o1", "text": "Other page."}])
        r = K.gate_entry(e, WIN, "t1")
        self.assertTrue(any("source o1 is not in the window" in x for x in r))
        self.assertTrue(any("quote of o1: not a chunk of the window" in x for x in r))

    def test_quote_outside_sources_and_source_without_quote(self):
        e = entry(quotes=entry()["quotes"] + [{"chunk": "n1", "text": "Use the KafkaNodePool resource"}])
        self.assertTrue(any("not one of the sources" in x for x in K.gate_entry(e, WIN, "t1")))
        e = entry(kind="cross", sources=["t1", "n1"])
        self.assertTrue(any("source n1 has no quote" in x for x in K.gate_entry(e, WIN, "t1")))

    def test_target_quote(self):
        e = entry(kind="fact", statement="Use the KafkaNodePool resource to configure node pools.", sources=["n1"],
                  quotes=[{"chunk": "n1", "text": "Use the KafkaNodePool resource to configure node pools."}])
        r = K.gate_entry(e, WIN, "t1")
        self.assertIn("the target chunk is not a source", r)
        self.assertIn("no quote from the target chunk", r)

    def test_identifier_and_number(self):
        r = K.gate_entry(entry(statement="In Strimzi, `spec.kafka.replica` sets the broker count; the default is 3."), WIN, "t1")
        self.assertTrue(any("'spec.kafka.replica'" in x for x in r), r)
        r = K.gate_entry(entry(statement="In Strimzi, `spec.kafka.replicas` sets the number of broker nodes; the default is 5."), WIN, "t1")
        self.assertTrue(any("'5'" in x for x in r), r)
        r = K.gate_entry(entry(applies_when="when maxUnavailable is set"), WIN, "t1")
        self.assertTrue(any("applies_when: 'maxUnavailable'" in x for x in r), r)
        r = K.gate_entry(entry(statement="Strimzi 1.3.1 sets broker nodes with `spec.kafka.replicas`; the default is 3."), WIN, "t1")
        self.assertTrue(any("'1.3.1'" in x for x in r), r)

    def test_title_is_gated(self):
        r = K.gate_entry(entry(title="maxUnavailable for brokers"), WIN, "t1")
        self.assertTrue(any("title: 'maxUnavailable'" in x for x in r), r)
        r = K.gate_entry(entry(title="Strimzi 1.3.1 broker replicas"), WIN, "t1")
        self.assertTrue(any("title: '1.3.1'" in x for x in r), r)
        self.assertEqual(K.gate_entry(entry(title="The `spec.kafka.replicas` default"), WIN, "t1"), [])

    def test_whole_tokens_only(self):
        self.assertTrue(K.token_in("spec.kafka.replicas", "The spec.kafka.replicas property."))
        self.assertFalse(K.token_in("spec.kafka.replica", "The spec.kafka.replicas property."))
        self.assertFalse(K.token_in("3", "default is 30"))
        self.assertFalse(K.token_in("3", "version 1.3.1"))
        self.assertTrue(K.token_in("3", "The default is 3."))
        self.assertTrue(K.token_in("kafka-topics.sh", "run `kafka-topics.sh --list`"))

    def test_identifier_tokens(self):
        toks = K.identifier_tokens("Set `min.insync.replicas` (e.g. to 2) on KafkaNodePool, use kafka-topics.sh and /var/lib, see snake_case. Done.")
        self.assertEqual(toks, ["min.insync.replicas", "2", "KafkaNodePool", "kafka-topics.sh", "/var/lib", "snake_case"])
        self.assertEqual(K.identifier_tokens("The broker restarts. It is safe."), [])

    def test_schema_and_sizes(self):
        self.assertTrue(K.gate_entry(entry(kind="tip"), WIN, "t1"))
        self.assertTrue(K.gate_entry(entry(title="x" * 101), WIN, "t1"))
        self.assertTrue(K.gate_entry(entry(statement="x" * 401), WIN, "t1"))
        self.assertTrue(K.gate_entry(entry(applies_when="x" * 201), WIN, "t1"))
        self.assertTrue(K.gate_entry(entry(quotes=[]), WIN, "t1"))
        self.assertTrue(K.gate_entry(entry(quotes=[{"chunk": "t1", "text": "x" * 301}]), WIN, "t1"))
        self.assertTrue(K.gate_entry(entry(quotes=entry()["quotes"] * 5), WIN, "t1"))
        self.assertTrue(K.gate_entry(entry(sources=["t1", "t1"]), WIN, "t1"))
        self.assertTrue(K.gate_entry(entry(extra="x"), WIN, "t1"))
        self.assertIn("a cross entry has one source (needs two or three)", K.gate_entry(entry(kind="cross"), WIN, "t1"))

    def test_reply_schema_and_duplicates(self):
        self.assertIsNotNone(K.check_reply([], [T, N1], "t1")[0])
        self.assertIsNotNone(K.check_reply({"entries": [entry()] * 9}, [T, N1], "t1")[0])
        err, ok, rej = K.check_reply({"entries": [entry(), entry(statement=entry()["statement"].upper().replace("SPEC.KAFKA.REPLICAS", "spec.kafka.replicas")), entry(kind="tip")]}, [T, N1], "t1")
        self.assertIsNone(err)
        self.assertEqual(len(ok), 1)
        self.assertEqual(len(rej), 2)


class Finish(unittest.TestCase):
    def test_fields_id_and_render(self):
        e = K.finish_entry(entry(applies_when=""), [T, N1], "t1", prompt_version="kbe/1", writer="w", created="c", commits={"strimzi": "abc"})
        self.assertEqual(e["scope"], {"product": "Strimzi", "version": "1.3.1"})
        self.assertEqual(e["sources"], [{"id": "t1", "source": "strimzi", "path": T["path"], "commit": "abc"}])
        self.assertNotIn("applies_when", e)
        self.assertEqual(e["id"], K.entry_id("kbe/1", ["t1"], "  in strimzi, `spec.kafka.replicas`  sets the number of broker nodes; the default is 3. "))
        self.assertNotEqual(e["id"], K.entry_id("kbe/2", ["t1"], e["statement"]))
        self.assertRegex(e["id"], r"^kbe-[0-9a-f]{16}$")
        txt = K.render_text(e)
        self.assertTrue(txt.startswith("strimzi — Fact entry: Broker replicas (Strimzi 1.3.1; " + T["path"] + ")\n"))
        self.assertIn("\nSource text:\n- \"The `spec.kafka.replicas` property", txt)
        self.assertEqual(K.render_check(e), [])
        e2 = K.finish_entry(entry(applies_when="KRaft mode"), [T, N1], "t1", prompt_version="kbe/1", writer="w", created="c", commits={})
        self.assertIn("\nApplies when: KRaft mode\nSource text:", K.render_text(e2))
        big = dict(e, quotes=[{"chunk": "t1", "text": "y" * 300}] * 4, statement="s" * 400)
        self.assertTrue(K.render_check(big))

    def test_prompt_texts_are_filled_once(self):
        m = P.kb_writer_messages("WINDOW <<TARGET>>", "t1")
        self.assertEqual(m[0]["content"], P.KB_WRITER_SYSTEM)
        self.assertIn("WINDOW <<TARGET>>", m[1]["content"])        # one pass: text inside the window is not filled again
        self.assertIn("entries for the target chunk t1.", m[1]["content"])
        self.assertNotIn("<<", m[1]["content"].replace("WINDOW <<TARGET>>", ""))
        c = P.kb_check_messages("W", "E")
        self.assertNotIn("<<", c[1]["content"])
        self.assertEqual((P.KB_ENTRY_PROMPT_VERSION, P.KB_CHECK_PROMPT_VERSION), ("kbe/1", "kbc/1"))


class GoldenUntouched(unittest.TestCase):
    def test_golden_prompt_tests_pass(self):
        r = subprocess.run([sys.executable, "-m", "unittest", "tests.test_prompts", "-q"], cwd=ROOT, capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stderr[-2000:])


if __name__ == "__main__":
    unittest.main()
