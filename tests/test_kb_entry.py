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
        self.assertEqual((P.KB_ENTRY_PROMPT_VERSION, P.KB_CHECK_PROMPT_VERSION), ("kbe/3", "kbc/2"))


class GoldenUntouched(unittest.TestCase):
    def test_golden_prompt_tests_pass(self):
        r = subprocess.run([sys.executable, "-m", "unittest", "tests.test_prompts", "-q"], cwd=ROOT, capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stderr[-2000:])


if __name__ == "__main__":
    unittest.main()


class Normalisations(unittest.TestCase):
    """kbe/2: three harmless gate normalisations and the title-line support."""
    TL = {"id": "t2", "source": "strimzi", "path": T["path"], "title": "Using `KafkaNodePool` > Rolling.Updates",
          "text": "Set the value as described. Use bin/kafka-features.sh to inspect features. This is a trade-off between speed and safety."}
    W = {"t2": TL}

    def e(self, **kw):
        d = {"kind": "fact", "title": "Features", "statement": "Set the value.", "sources": ["t2"],
             "quotes": [{"chunk": "t2", "text": "Set the value as described."}]}
        d.update(kw)
        return d

    def test_a_title_line_supports_name_and_is_recorded(self):
        e = self.e(statement="For `KafkaNodePool`, set the value.")
        self.assertEqual(K.gate_entry(e, self.W, "t2"), [])
        self.assertEqual(K.title_support(e, self.W), ["t2"])
        f = K.finish_entry(e, [self.TL], "t2", prompt_version="kbe/2", writer="w", created="c", commits={})
        self.assertEqual(f["quotes"][-1], {"chunk": "t2", "text": "strimzi \u2014 Using `KafkaNodePool` > Rolling.Updates", "title_line": True})
        self.assertIn("Using `KafkaNodePool`", K.render_text(f))
        # separators: dots and ' > ' in the title line
        self.assertEqual(K.gate_entry(self.e(statement="See Rolling.Updates."), self.W, "t2"), [])
        self.assertEqual(K.gate_entry(self.e(statement="See `Rolling/Updates`."), self.W, "t2"), [])

    def test_a_not_for_unlisted_chunk_or_numbers_or_invented(self):
        self.assertTrue(K.gate_entry(self.e(statement="Use `KafkaNodePools`."), self.W, "t2"))      # a plural is another name
        self.assertTrue(K.gate_entry(self.e(statement="Use `KafkaUser`."), self.W, "t2"))
        self.assertTrue(K.gate_entry(self.e(statement="Version 1.3."), self.W, "t2"))
        e = self.e(statement="For `KafkaNodePool`, set the value.", sources=["t2"])
        e2 = dict(e, sources=["t2"])
        self.assertEqual(K.title_support(self.e(), self.W), [])                 # no extra quote when a quote has every name

    def test_b_path_slash(self):
        self.assertTrue(K.token_in("kafka-features.sh", "run bin/kafka-features.sh now"))
        self.assertEqual(K.gate_entry(self.e(statement="Use kafka-features.sh to inspect features.",
                                             quotes=[{"chunk": "t2", "text": "Use bin/kafka-features.sh to inspect features."}]), self.W, "t2"), [])
        self.assertFalse(K.token_in("spec.kafka", "x.spec.kafka"))               # a dot or hyphen before it still refuses
        self.assertFalse(K.token_in("kafka-features.sh", "bin/kafka-features.shx"))
        self.assertFalse(K.token_in("features.sh", "bin/kafka-features.sh"))

    def test_c_case_insensitive_hyphenated_words(self):
        self.assertTrue(K.token_in("Trade-off", "This is a trade-off between"))
        self.assertEqual(K.gate_entry(self.e(statement="A Trade-off applies.",
                                             quotes=[{"chunk": "t2", "text": "This is a trade-off between speed and safety."}]), self.W, "t2"), [])
        self.assertFalse(K.token_in("Spec.Kafka", "a spec.kafka b"))              # not letters and hyphens only: case matters
        self.assertFalse(K.token_in("Trade-off", "tradeoff"))

    def test_still_rejected(self):
        self.assertTrue(K.gate_entry(self.e(quotes=[{"chunk": "t2", "text": "Set the other value."}]), self.W, "t2"))   # not verbatim
        long = "x" * 301
        self.assertTrue(any("longer than 300" in r for r in K.gate_entry(self.e(quotes=[{"chunk": "t2", "text": long}]), self.W, "t2")))
        self.assertTrue(any("longer than 400" in r for r in K.gate_entry(self.e(statement="s" * 401), self.W, "t2")))
        self.assertTrue(K.gate_entry(self.e(statement="Call SinkTask.put now."), self.W, "t2"))            # a built name


class Kbe3(unittest.TestCase):
    """kbe/3: heading lines as support, numbers with their neighbouring word, markup normalised; the limits unchanged."""
    MD = {"id": "m1", "source": "apache-kafka", "path": "raw/apache-kafka/docs/x.md", "title": "Connect > Steps",
          "text": "## KafkaConnectTemplate schema reference\n\nThe pod template sets the labels.\n\n```\n# Not A Heading in code\n```\n\n"
                  "Step 1\n\nUse the secret `_<cluster_name>_-cluster-ca-cert` here.\n\nMirrorMaker 2 is a connector."}
    ADOC = {"id": "a1", "source": "strimzi", "path": "raw/strimzi/documentation/modules/y.adoc", "title": "Using MirrorMaker 2",
            "text": "== Step 1: Create the topic\n\nCreate it with the tool."}
    W = {"m1": MD, "a1": ADOC}

    def e(self, cid="m1", text="The pod template sets the labels.", **kw):
        d = {"kind": "fact", "title": "Pod template", "statement": "The pod template sets the labels.", "sources": [cid],
             "quotes": [{"chunk": cid, "text": text}]}
        d.update(kw)
        return d

    def test_heading_line_supports_a_name_and_is_recorded(self):
        e = self.e(statement="KafkaConnectTemplate: the pod template sets the labels.")
        self.assertEqual(K.gate_entry(e, self.W, "m1"), [])
        f = K.finish_entry(e, [self.MD], "m1", prompt_version="kbe/3", writer="w", created="c", commits={})
        self.assertEqual(f["quotes"][-1], {"chunk": "m1", "text": "## KafkaConnectTemplate schema reference", "heading_line": True})
        self.assertIn("KafkaConnectTemplate schema reference", K.render_text(f))
        e = self.e(cid="a1", text="Create it with the tool.", statement="Create the topic with the tool.", title="Topic")
        e["statement"] = "In Step 1: Create the topic with the tool."
        f = K.finish_entry(e, [self.ADOC], "a1", prompt_version="kbe/3", writer="w", created="c", commits={})
        self.assertEqual(K.gate_entry(e, self.W, "a1"), [])                       # AsciiDoc '=' heading
        self.assertTrue(any(q.get("heading_line") for q in f["quotes"]) or any(q.get("title_line") for q in f["quotes"]))

    def test_heading_rules_negative(self):
        self.assertTrue(K.gate_entry(self.e(statement="KafkaConnectTemplates set the labels."), self.W, "m1"))   # plural: another name
        self.assertTrue(K.gate_entry(self.e(statement="KafkaMirrorMakerTemplate sets the labels."), self.W, "m1"))   # invented
        self.assertEqual(K.heading_lines(self.MD), ["## KafkaConnectTemplate schema reference"])      # no heading line inside a code block
        self.assertTrue(K.gate_entry(self.e(statement="NotAHeadingInCode sets the labels."), self.W, "m1"))
        self.assertEqual(K.heading_lines({"text": "## " + "x" * 130}), [])                                  # too long to be a heading

    def test_number_with_neighbouring_word(self):
        e = self.e(cid="a1", text="Create it with the tool.", title="Topic", statement="MirrorMaker 2 creates it with the tool.")
        self.assertEqual(K.gate_entry(e, self.W, "a1"), [])                                                   # "MirrorMaker 2" in the title line
        e = self.e(cid="a1", text="Create it with the tool.", title="Topic", statement="Step 1 creates it with the tool.")
        self.assertEqual(K.gate_entry(e, self.W, "a1"), [])                                                   # "Step 1" in the heading
        for bad in ("MirrorMaker 3 creates it with the tool.", "Step 2 creates it with the tool.", "Create it with the tool in 7 steps."):
            self.assertTrue(K.gate_entry(self.e(cid="a1", text="Create it with the tool.", title="Topic", statement=bad), self.W, "a1"), bad)

    def test_markup_normalised(self):
        q = "Use the secret _&lt;cluster_name&gt;_-cluster-ca-cert here."
        W = {"t": dict(T, id="t", text=q)}
        e = {"kind": "fact", "title": "Secret", "statement": "Use the secret &lt;cluster_name&gt;-cluster-ca-cert here.", "sources": ["t"],
             "quotes": [{"chunk": "t", "text": q}]}
        self.assertEqual(K.gate_entry(e, W, "t"), [])
        e2 = dict(e, statement="Use the secret `_&lt;cluster_name&gt;_-cluster-ca-cert` here.")
        self.assertEqual(K.gate_entry(e2, W, "t"), [])
        e3 = dict(e, statement="Use the secret &lt;cluster_name&gt;-cluster-ca-certs here.")                  # a plural stays another name
        self.assertTrue(K.gate_entry(e3, W, "t"))
        self.assertEqual(K.unmark("`_a_` and x_y and __z"), "a and x_y and z")

    def test_limits_unchanged(self):
        self.assertEqual((K.QUOTE_MAX, K.STATEMENT_MAX), (300, 400))
        self.assertTrue(any("longer than 300" in r for r in K.gate_entry(self.e(text="x" * 301), self.W, "m1")))
        self.assertTrue(any("longer than 400" in r for r in K.gate_entry(self.e(statement="s" * 401), self.W, "m1")))
        self.assertTrue(any("not verbatim" in r for r in K.gate_entry(self.e(text="The pod template sets all labels."), self.W, "m1")))

    def test_prompt_texts(self):
        w = P.KB_WRITER_USER
        for t in ("Keep each quote under 250 characters: quote one sentence or one table row per quote.",
                  "When the entry's subject is named by a heading line inside the chunk, quote that heading line as well.",
                  "a statement true only under a condition must name it."):
            self.assertIn(t, w)
        self.assertIn("drops a condition that the source (its heading or its context) places on it", P.KB_CHECK_USER)
