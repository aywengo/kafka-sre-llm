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
        self.assertIsNone(K.check_reply({"entries": [entry()] * 12}, [T, N1], "t1")[0])        # kbe/4: the cap is 12
        self.assertIn("13 entries (at most 12)", K.check_reply({"entries": [entry()] * 13}, [T, N1], "t1")[0])
        self.assertEqual(K.ENTRIES_MAX, 12)
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
        self.assertIn("\nApplies when: KRaft mode\nContext: Configuring brokers\nSource text:", K.render_text(e2))
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
        self.assertEqual((P.KB_ENTRY_PROMPT_VERSION, P.KB_CHECK_PROMPT_VERSION), ("kbe/5", "kbc/4"))


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


class Kbe4Trail(unittest.TestCase):
    """kbe/4: the heading trail is derived from the chunks (title line, in-chunk headings, table caption), stored, rendered."""
    MD = {"id": "m1", "source": "apache-kafka", "path": "raw/apache-kafka/docs/x.md", "title": "Monitoring > Streams metrics",
          "text": "# Task metrics\n\nIntro sentence.\n\n## Debug level\n\nThe process-rate metric is only recorded at debug level.\n\n"
                  "```\n# not a heading\n```\n\n## Other\n\nLater text."}
    ADOC = {"id": "a1", "source": "strimzi", "path": "raw/strimzi/documentation/modules/y.adoc", "title": "Doc > Section > Sub",
            "text": "= Operators\n\n== Topic Operator\n\nIntro.\n\n.Operator settings\n[cols=\"1,1\"]\n|===\n|name |value\n|`a` |one\n|===\n\n"
                    "After the table.\n\n== User Operator\n\nUsers."}

    def q(self, cid, text):
        return [{"chunk": cid, "text": text}]

    def test_title_line_and_headings_before_the_quote(self):
        t = K.heading_trail(["m1"], self.q("m1", "The process-rate metric is only recorded at debug level."), {"m1": self.MD})
        self.assertEqual(t, "Monitoring > Streams metrics > Task metrics > Debug level")      # the stack by level, outermost first
        t = K.heading_trail(["m1"], self.q("m1", "Intro sentence."), {"m1": self.MD})
        self.assertEqual(t, "Monitoring > Streams metrics > Task metrics")
        t = K.heading_trail(["m1"], self.q("m1", "Later text."), {"m1": self.MD})
        self.assertTrue(t.endswith("Task metrics > Other"), t)

    def test_adoc_headings_levels_and_table_caption(self):
        b = {"a1": self.ADOC}
        self.assertEqual(K.heading_trail(["a1"], self.q("a1", "Intro."), b), "Doc > Section > Sub > Operators > Topic Operator")
        self.assertEqual(K.heading_trail(["a1"], self.q("a1", "|`a` |one"), b), "Doc > Section > Sub > Operators > Topic Operator > Operator settings")
        self.assertNotIn("Operator settings", K.heading_trail(["a1"], self.q("a1", "After the table."), b))     # outside the table
        self.assertTrue(K.heading_trail(["a1"], self.q("a1", "Users."), b).endswith("Operators > User Operator"))

    def test_code_block_hash_is_no_heading_and_title_only_fallback(self):
        t = K.heading_trail(["m1"], self.q("m1", "# not a heading"), {"m1": self.MD})
        self.assertNotIn("not a heading", t)
        self.assertEqual(K.heading_trail(["m1"], [{"chunk": "m1", "text": "x", "title_line": True}], {"m1": self.MD}), "Monitoring > Streams metrics")
        self.assertEqual(K.heading_trail([], [], {}), "")

    def test_anchor_suffix_is_dropped(self):
        c = dict(self.MD, title="Streams metrics {#kip-1}", text="## Header-aware stores {#kip-1271-headers}\n\nThe quoted sentence.")
        self.assertEqual(K.heading_trail(["m1"], self.q("m1", "The quoted sentence."), {"m1": c}), "Streams metrics > Header-aware stores")

    def test_several_sources_and_quotes_are_joined(self):
        q = [{"chunk": "m1", "text": "Intro sentence."}, {"chunk": "m1", "text": "Later text."}, {"chunk": "a1", "text": "Intro."}]
        t = K.heading_trail(["m1", "a1"], q, {"m1": self.MD, "a1": self.ADOC}, cap=400)
        self.assertEqual(t.count(" | "), 2, t)

    def test_truncation_cuts_the_crumb_middle_and_keeps_the_nearest_heading(self):
        long = {"id": "l1", "source": "strimzi", "path": "raw/p.adoc", "title": " > ".join(f"Segment number {i}" for i in range(12)),
                "text": "== Nearest heading here\n\nThe quoted sentence."}
        t = K.heading_trail(["l1"], self.q("l1", "The quoted sentence."), {"l1": long})
        self.assertLessEqual(len(t), K.TRAIL_MAX)
        self.assertTrue(t.startswith("Segment number 0"), t)
        self.assertTrue(t.endswith("Nearest heading here"), t)
        self.assertIn(K.ELLIPSIS, t)
        self.assertEqual(K.heading_trail(["l1"], self.q("l1", "The quoted sentence."), {"l1": long}, cap=40)[-20:], "Nearest heading here"[-20:])
        # a title of one very long segment is cut in the middle
        one = dict(long, title="T" * 400)
        t = K.heading_trail(["l1"], self.q("l1", "The quoted sentence."), {"l1": one})
        self.assertEqual(len(t), K.TRAIL_MAX)
        self.assertTrue(t.endswith("> Nearest heading here") and K.ELLIPSIS in t)
        # a nearest heading longer than the cap is cut in its middle, never dropped
        big = dict(long, title="Doc", text="== " + "H" * 150 + "\n\nThe quoted sentence.")
        self.assertEqual(len(K.heading_trail(["l1"], self.q("l1", "The quoted sentence."), {"l1": big}, cap=60)), 60)

    def test_finish_entry_stores_and_render_shows_it(self):
        e = {"kind": "fact", "title": "Process rate", "statement": "The process-rate metric is only recorded at debug level.",
             "sources": ["m1"], "quotes": self.q("m1", "The process-rate metric is only recorded at debug level.")}
        f = K.finish_entry(e, [self.MD], "m1", prompt_version="kbe/5", writer="w", created="c", commits={})
        self.assertEqual(f["heading_trail"], "Monitoring > Streams metrics > Task metrics > Debug level")
        self.assertIn("\nContext: Monitoring > Streams metrics > Task metrics > Debug level\nSource text:\n", K.render_text(f))
        self.assertEqual(K.render_check(f), [])
        g = {k: v for k, v in f.items() if k != "heading_trail"}                    # an entry of an earlier version renders as before
        self.assertNotIn("Context:", K.render_text(g))

    def test_checker_and_reading_see_the_trail(self):
        import kb_check_entries as C
        import m7_kb2_rule as R
        e = K.finish_entry({"kind": "fact", "title": "Process rate", "statement": "The process-rate metric is only recorded at debug level.",
                            "sources": ["m1"], "quotes": self.q("m1", "The process-rate metric is only recorded at debug level.")},
                           [self.MD], "m1", prompt_version="kbe/5", writer="w", created="c", commits={})
        e["id"] = "kbe-x"
        row = json.loads(C.entry_lines([e]))
        self.assertEqual(row["heading_trail"], e["heading_trail"])
        self.assertIn("**Heading trail.** " + e["heading_trail"], R._bundle([e], {"m1": self.MD}, "T", "note"))
        self.assertIn("heading trail", P.KB_CHECK_USER)
        self.assertIn("neither in the statement", P.KB_CHECK_USER)
        self.assertIn("added to it automatically", P.KB_WRITER_USER)
        self.assertIn("Write 0 to 12 entries", P.KB_WRITER_USER)


class Kbe5Context(unittest.TestCase):
    """kbe/5: the lead-in sentence and nearby admonitions are derived from the chunk, capped, stored and rendered."""
    def chunk(self, text):
        return {"id": "c1", "source": "strimzi", "path": "raw/strimzi/documentation/modules/z.adoc", "title": "Doc > Page", "text": text}

    def notes(self, text, quote, extra=None, trail="Doc > Page"):
        qs = [{"chunk": "c1", "text": quote}] + (extra or [])
        return K.context_notes(["c1"], qs, {"c1": self.chunk(text)}, trail)

    def test_preceding_sentence_in_the_same_paragraph(self):
        t = "= Page\n\nFirst sentence here is long enough to stand alone as a whole sentence. Second one is also quite long, running on and on and on, and on and on, and on and on again. Quoted fact is here."
        self.assertEqual(self.notes(t, "Quoted fact is here.")[0], "Second one is also quite long, running on and on and on, and on and on, and on and on again.")
        self.assertEqual(K.lead_parts(t, "Second one is also quite long, running on and on and on, and on and on, and on and on again.")[0],
                         "First sentence here is long enough to stand alone as a whole sentence.")

    def test_short_sentence_takes_the_one_before_and_abbreviations_do_not_split(self):
        t = "A message with a key and a null payload is a delete. Such a record is a tombstone. Quoted fact is here."
        self.assertEqual(self.notes(t, "Quoted fact is here.")[0], "A message with a key and a null payload is a delete. Such a record is a tombstone.")
        t = "Use a listener, e.g. an internal one, for this traffic when the cluster is not exposed. Quoted fact is here."
        self.assertEqual(self.notes(t, "Quoted fact is here.")[0], "Use a listener, e.g. an internal one, for this traffic when the cluster is not exposed.")

    def test_quote_starting_a_paragraph_takes_the_paragraph_above(self):
        t = "Intro that is not wanted at all.\n\nIf the Topic Operator is not running, finalizers are not processed by anything.\n\nQuoted fact is here."
        self.assertEqual(self.notes(t, "Quoted fact is here.")[0], "If the Topic Operator is not running, finalizers are not processed by anything.")

    def test_table_lead_in_asciidoc_markdown_and_html(self):
        lead = "The following metrics are recorded at the debug level and only then, never at info:"
        a = f"{lead}\n\n.Metrics\n[cols=\"1,1\"]\n|===\n|name |value\n|`a` |one\n|===\n"
        self.assertEqual(self.notes(a, "|`a` |one")[0], lead)
        m = f"{lead}\n\n| name | value |\n|---|---|\n| a | one |\n"
        self.assertEqual(self.notes(m, "| a | one |")[0], lead)
        h = f"{lead}  \n<table>  \n<tr>\n<td>\n\nprocess-rate\n</td>\n</tr>\n</table>\n"
        self.assertEqual(self.notes(h, "process-rate")[0], lead)

    def test_list_lead_in(self):
        lead = "To handle larger messages, consider adjusting the following configurations:"
        t = f"{lead}\n\n* Adjusting the batch size.\n** Set `a` at the broker level.\n* Increasing the fetch size (`b`).\n* Third item.\n"
        self.assertEqual(self.notes(t, "Increasing the fetch size (`b`).")[0], lead)
        self.assertEqual(self.notes(t, "Set `a` at the broker level.")[0], lead)
        # a step continued over '+' and a code block: the sentence of the step is the lead-in
        s = "== Steps\n\n. Create a `KafkaUser` resource in each namespace of the source and target clusters.\n+\n```\nkubectl apply -f x\n```\n+\nThe User Operator creates the users."
        self.assertEqual(self.notes(s, "The User Operator creates the users.")[0], "Create a `KafkaUser` resource in each namespace of the source and target clusters.")

    def test_paragraph_after_a_list_takes_the_list_lead_in(self):
        t = "You must apply the following annotations to the resource to manage offsets of the connectors:\n\n* `a`\n* `b`\n\nThe `b` annotation must be set to the name."
        self.assertEqual(self.notes(t, "The `b` annotation must be set to the name.")[0],
                         "You must apply the following annotations to the resource to manage offsets of the connectors:")

    def test_admonition_before_and_after(self):
        t = ("NOTE: Only for the legacy mode of operation, which is going away.\n\nThe quoted passage is right here in its own paragraph.\n\n"
             "[WARNING]\n====\nDo not do this on a running cluster.\n====\n\nAfter text that is far away.")
        lead, adm = self.notes(t, "The quoted passage is right here in its own paragraph.")
        self.assertEqual(adm, "NOTE: Only for the legacy mode of operation, which is going away. | WARNING: Do not do this on a running cluster.")
        md = "> **Note**: Applies from version 4.\n\nThe quoted passage.\n\n**Important:** Be careful here.\n"
        self.assertEqual(self.notes(md, "The quoted passage.")[1], "NOTE: Applies from version 4. | IMPORTANT: Be careful here.")
        gh = "> [!CAUTION]\n> Data can be lost.\n\nThe quoted passage."
        self.assertEqual(self.notes(gh, "The quoted passage.")[1], "CAUTION: Data can be lost.")
        far = "NOTE: Far away.\n\nSomething in between.\n\nMore in between.\n\nThe quoted passage."
        self.assertEqual(self.notes(far, "The quoted passage.")[1], "")           # not directly before it

    def test_admonition_not_across_a_heading(self):
        t = "NOTE: In the previous section.\n\n== Next section\n\nThe quoted passage."
        self.assertEqual(self.notes(t, "The quoted passage."), ("", ""))

    def test_quote_at_chunk_start_has_no_lead_in(self):
        self.assertEqual(self.notes("Quoted fact is here. More text after it.", "Quoted fact is here."), ("", ""))
        self.assertEqual(self.notes("== Heading\n\nQuoted fact is here.", "Quoted fact is here."), ("", ""))      # a heading is the trail's
        self.assertEqual(self.notes("```\ncode line.\n```\n\nQuoted fact is here.", "Quoted fact is here."), ("", ""))

    def test_lead_in_inside_the_quote_is_skipped(self):
        t = "The first sentence of the paragraph is a fairly long one indeed. The second one follows it closely enough."
        q2 = "The second one follows it closely enough."
        full = "The first sentence of the paragraph is a fairly long one indeed. The second one follows it closely enough."
        self.assertEqual(self.notes(t, q2)[0], "The first sentence of the paragraph is a fairly long one indeed.")
        self.assertEqual(self.notes(t, q2, extra=[{"chunk": "c1", "text": "The first sentence of the paragraph is a fairly long one indeed."}])[0], "")
        self.assertEqual(self.notes(t, full)[0], "")
        self.assertEqual(self.notes(t, "not in the chunk"), ("", ""))
        self.assertEqual(self.notes(t, q2, extra=[{"chunk": "c1", "text": "Doc", "title_line": True}])[0],
                         "The first sentence of the paragraph is a fairly long one indeed.")

    def test_cap_and_truncation(self):
        lead = "L" * 60 + " " + "x" * 600 + " the nearest words " + "y" * 30 + ":"
        t = f"{lead}\n\nThe quoted passage.\n\nNOTE: " + "N" * 700
        trail = "T" * 200
        l, a = self.notes(t, "The quoted passage.", trail=trail)
        self.assertLessEqual(len(trail) + len(l) + len(a), K.CONTEXT_MAX)
        self.assertTrue(l.startswith(K.ELLIPSIS) and l.endswith("y:"), l)       # the nearest text (the end) of the lead-in stays
        self.assertTrue(a.endswith(K.ELLIPSIS) and a.startswith("NOTE: NNN"), a)     # the start of the note stays
        self.assertGreater(len(l), 60)
        self.assertGreater(len(a), 60)
        l, a = self.notes(t, "The quoted passage.", trail="")
        self.assertLessEqual(len(l) + len(a), K.CONTEXT_MAX)
        # short parts are kept whole
        self.assertEqual(self.notes("A sentence that is long enough to be a lead-in on its own, ending here:\n\nThe quoted passage.", "The quoted passage.")[0],
                         "A sentence that is long enough to be a lead-in on its own, ending here:")

    def test_several_quotes_are_joined(self):
        t = ("First lead sentence, long enough to count as a whole sentence of its own.\n\nQuote one.\n\n"
             "Second lead sentence, long enough to count as a whole sentence of its own.\n\nQuote two.")
        l, _ = self.notes(t, "Quote one.", extra=[{"chunk": "c1", "text": "Quote two."}])
        self.assertEqual(l.count(" | "), 1, l)

    def test_finish_entry_stores_and_render_shows_it(self):
        t = ("The following metrics are only recorded at the debug level, not at info:\n\n|===\n|name |value\n|`rate` |one\n|===\n\n"
             "NOTE: Only when the feature is enabled.")
        c = self.chunk(t)
        e = {"kind": "fact", "title": "Rate", "statement": "The rate metric has the value one.", "sources": ["c1"],
             "quotes": [{"chunk": "c1", "text": "|`rate` |one"}]}
        f = K.finish_entry(e, [c], "c1", prompt_version="kbe/5", writer="w", created="c", commits={})
        self.assertEqual(f["lead_in"], "The following metrics are only recorded at the debug level, not at info:")
        self.assertEqual(f["admonition"], "NOTE: Only when the feature is enabled.")
        r = K.render_text(f)
        self.assertIn("\nContext: Doc > Page\nBefore: The following metrics are only recorded at the debug level, not at info:\n"
                      "Note: NOTE: Only when the feature is enabled.\nSource text:\n", r)
        self.assertEqual(K.render_check(f), [])
        g = {k: v for k, v in f.items() if k not in ("lead_in", "admonition")}          # a kbe/4 entry renders as before
        self.assertNotIn("Before:", K.render_text(g))
        self.assertNotIn("Note:", K.render_text(g))
        h = dict(g, heading_trail="X")
        self.assertIn("\nContext: X\nSource text:", K.render_text(h))

    def test_checker_and_reading_see_the_context(self):
        import kb_check_entries as C
        import m7_kb2_rule as R
        e = {"id": "kbe-x", "kind": "fact", "title": "t", "statement": "s", "sources": [{"id": "c1", "path": "p"}],
             "quotes": [{"chunk": "c1", "text": "q"}], "heading_trail": "A > B", "lead_in": "Lead sentence:", "admonition": "NOTE: n"}
        row = json.loads(C.entry_lines([e]))
        self.assertEqual((row["lead_in"], row["admonition"]), ("Lead sentence:", "NOTE: n"))
        self.assertNotIn("lead_in", json.loads(C.entry_lines([{k: v for k, v in e.items() if k not in ("lead_in", "admonition")}])))
        b = R._bundle([e], {"c1": self.chunk("q")}, "T", "note")
        self.assertIn("**Lead-in (before the quote).** Lead sentence:", b)
        self.assertIn("**Admonition (next to the quote).** NOTE: n", b)
        self.assertIn("lead-in", P.KB_CHECK_USER)
        self.assertIn("sentence just before the quote", P.KB_WRITER_USER)


class W26Fixes(unittest.TestCase):
    """W26: hyphenated title words with a space or no hyphen in the source; no lead-in from a cell or row of the quote's own table."""
    def ch(self, text, title="Doc > Page"):
        return {"id": "c1", "source": "strimzi", "path": "raw/strimzi/documentation/modules/z.adoc", "title": title, "text": text}

    def gate(self, title, quote_text, text=None, statement="Set the value.", **kw):
        c = self.ch(text or quote_text)
        e = {"kind": "fact", "title": title, "statement": statement, "sources": ["c1"], "quotes": [{"chunk": "c1", "text": quote_text}]}
        e.update(kw)
        return K.gate_entry(e, {"c1": c}, "c1")

    def test_hyphenated_title_words_accepted_in_other_forms(self):
        for title, src in (("TLS node-to-node encryption", "Encryption between node to node connections is set here."),
                           ("Self-signed certificates", "Strimzi creates self signed certificates by default."),
                           ("Controller step-down", "The controller does a stepdown when it is told so."),
                           ("Fine-tuning the cache", "Fine tuning the cache is possible."),
                           ("Node-To-Node traffic", "traffic from node to node.")):
            q = src
            self.assertEqual(self.gate(title, q, statement="Set it."), [], title)

    def test_hyphenated_title_words_still_refused_when_absent_or_identifier_like(self):
        self.assertTrue(self.gate("Self-signed certificates", "Strimzi creates certificates by default.", statement="Set it."))
        self.assertTrue(self.gate("Cross-site mirroring", "Mirroring happens here.", statement="Set it."))
        self.assertTrue(self.gate("Option log-dir2 set", "The option is set here.", statement="Set it."))         # a digit
        self.assertTrue(self.gate("Setting `node-to-node` now", "Encryption between node to node.", statement="Set it."))   # backticked in the title
        self.assertTrue(self.gate("Use my-setting.value", "Use my setting value.", statement="Set it."))           # a dot
        self.assertTrue(self.gate("The node-to-node", "See x.node to node.y here.", statement="Set it."))         # part of a longer identifier
        self.assertTrue(self.gate("Use camelCase-word", "Use camel case word.", statement="Set it."))
        # the leniency is for titles only: the statement still needs the exact token
        self.assertTrue(self.gate("Encryption", "Encryption between node to node.", statement="It is node-to-node."))

    LEAD = "The following metrics are recorded at the debug level and only then, never at info:"

    def test_cut_html_table_row_has_no_lead_in_from_a_cell(self):
        t = ("</td> </tr>  \n<tr>  \n<td>\n\nSTREAMS_GROUP_HEARTBEAT (88)\n</td>  \n<td>\n\nDescribe\n</td>  \n<td>\n\nTopic\n</td>  \n<td>\n\n"
             "Not required if internal topics exist.\n</td> </tr>  \n<tr>  \n<td>\n\nSTREAMS_GROUP_DESCRIBE (89)\n</td>  \n<td>\n\nDescribe\n</td>  \n"
             "<td>\n\nTopic\n</td>  \n<td>\n\nRequired for all source topics and internal topics used in the topology of the group.\n</td> </tr>  \n")
        for q in ("STREAMS_GROUP_DESCRIBE (89)", "Required for all source topics and internal topics used in the topology of the group."):
            self.assertEqual(K.lead_parts(t, q), ("", []), q)
            self.assertEqual(K.context_notes(["c1"], [{"chunk": "c1", "text": q}], {"c1": self.ch(t)}, "Doc > Page"), ("", ""))

    def test_whole_table_row_takes_the_sentence_above_the_table(self):
        h = (f"{self.LEAD}  \n<table>  \n<tr>\n<td>\n\nA (1)\n</td>\n<td>\n\nOnly at debug.\n</td>\n</tr>\n<tr>\n<td>\n\nB (2)\n</td>\n<td>\n\n"
             "Other cell sentence.\n</td>\n</tr>\n</table>\n")
        self.assertEqual(K.lead_parts(h, "B (2)")[0], self.LEAD)
        self.assertEqual(K.lead_parts(h, "Other cell sentence.")[0], self.LEAD)
        a = f"{self.LEAD}\n\n|===\n|name |value\n|`a` |Not required.\n|`b` |Required here.\n|===\n"
        self.assertEqual(K.lead_parts(a, "|`b` |Required here.")[0], self.LEAD)
        m = f"{self.LEAD}\n\n| name | value |\n|---|---|\n| a | Not required. |\n| b | Required here. |\n"
        self.assertEqual(K.lead_parts(m, "| b | Required here.")[0], self.LEAD)
        # no sentence above the table: no lead-in, never a cell
        n = "<table>\n<tr>\n<td>\n\nNot required.\n</td>\n<td>\n\nB (2)\n</td>\n</tr>\n</table>\n"
        self.assertEqual(K.lead_parts(n, "B (2)")[0], "")


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
        self.assertIn("a condition from the chunk (a heading, the preceding sentence, a note or warning nearby) is neither in the statement", P.KB_CHECK_USER)


class W27Fixes(unittest.TestCase):
    """W27: a backslash escape in the source is the plain character in the verbatim check; a name in the heading trail is supported."""
    UP = {"id": "u1", "source": "apache-kafka", "path": "raw/apache-kafka/docs/upgrade.md", "title": "Upgrading to 4.0.1 > Notable changes in 4.0.1",
          "text": "The enable.idempotence setting no longer falls back."}
    SK = {"id": "s1", "source": "apache-kafka", "path": "raw/apache-kafka/docs/connect.md", "title": "Developing a Simple Connector > Sink Tasks",
          "text": "The put() method should contain most of the implementation."}
    ES = {"id": "e1", "source": "apache-kafka", "path": "raw/apache-kafka/docs/connect.md", "title": "Connect",
          "text": "  * `group.id` \\- Unique name for the cluster; a \\*star\\*, a snake\\_case and a \\#tag"}

    def e(self, cid, **kw):
        d = {"kind": "fact", "title": "A setting", "statement": "It no longer falls back.", "sources": [cid],
             "quotes": [{"chunk": cid, "text": ""}]}
        d.update(kw)
        return d

    def test_a_escapes(self):
        W = {"e1": self.ES}
        self.assertEqual(K.gate_entry(self.e("e1", title="Escapes", statement="Plain characters.",
                                            quotes=[{"chunk": "e1", "text": "a *star*, a snake_case and a #tag"}]), W, "e1"), [])
        for q in ("`group.id` - Unique name for the cluster", "`group.id` \\- Unique name for the cluster"):
            self.assertEqual(K.gate_entry(self.e("e1", title="group.id", statement="group.id is unique.", quotes=[{"chunk": "e1", "text": q}]), W, "e1"), [], q)
        bad = self.e("e1", title="group.id", statement="group.id is unique.", quotes=[{"chunk": "e1", "text": "`group.id` - Another name"}])
        self.assertTrue(any("not verbatim" in r for r in K.gate_entry(bad, W, "e1")))            # only the escape is forgiven
        bad = self.e("e1", title="group.id", statement="group.id is unique.", quotes=[{"chunk": "e1", "text": "`group.id` -- Unique name"}])
        self.assertTrue(K.gate_entry(bad, W, "e1"))
        self.assertEqual(K.unescape("a\\-b \\* \\_ \\# \\x \\\\"), "a-b * _ # \\x \\\\")

    def test_b_heading_trail(self):
        W = {"u1": self.UP}
        q = [{"chunk": "u1", "text": "The enable.idempotence setting no longer falls back."}]
        ok = self.e("u1", statement="In Apache Kafka 4.0.1, enable.idempotence no longer falls back.", quotes=q)
        self.assertEqual(K.gate_entry(ok, W, "u1"), [])
        bad = self.e("u1", statement="In Apache Kafka 4.0.2, enable.idempotence no longer falls back.", quotes=q)      # another version
        self.assertTrue(any("4.0.2" in r for r in K.gate_entry(bad, W, "u1")))
        bad = self.e("u1", statement="In Apache Kafka 4.0, enable.idempotence no longer falls back.", quotes=q)        # a number of one dot is not a version
        self.assertTrue(K.gate_entry(bad, W, "u1"))
        bad = self.e("u1", statement="In Apache Kafka 4.0.10, enable.idempotence no longer falls back.", quotes=q)     # not a longer number
        self.assertTrue(K.gate_entry(bad, W, "u1"))

    def test_b_no_new_fuzzy_matching(self):
        W = {"s1": self.SK}
        q = [{"chunk": "s1", "text": "The put() method should contain most of the implementation."}]
        for name in ("SinkTask", "Sink-Task", "SinkTasks"):
            self.assertTrue(K.gate_entry(self.e("s1", title=f"{name} put() method", statement="put() holds the implementation.", quotes=q), W, "s1"), name)
        self.assertEqual(K.gate_entry(self.e("s1", title="Sink Tasks put() method", statement="put() holds the implementation.", quotes=q), W, "s1"), [])
        self.assertEqual(K.gate_entry(self.e("s1", statement="`Sink Tasks`: put() holds the implementation.", quotes=q), W, "s1"), [])

    def test_trail_parts_follow_heading_trail(self):
        c = {"id": "h1", "source": "strimzi", "path": "raw/strimzi/x.adoc", "title": "Doc > Section",
             "text": "= " + "Very long heading about KafkaRebalance " * 12 + "\n\nSome text here that is quoted."}
        e = self.e("h1", statement="About KafkaRebalance.", quotes=[{"chunk": "h1", "text": "Some text here that is quoted."}])
        parts = K._trail_parts(e, {"h1": c})
        self.assertTrue(parts and "KafkaRebalance" in parts[0])
        self.assertEqual(K.gate_entry(e, {"h1": c}, "h1"), [])
