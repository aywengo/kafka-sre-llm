"""Not-covered generator of the a3 slice (scripts/eval2_items_a3_gap.py): every gap has an absence record with zero hits
that is recomputed here against the frozen index; every control's fact is in the index and the catalogue; every
reference passes its checks and every perturbed answer fails the named check; a gap and its control share `pair`; the
output does not depend on PYTHONHASHSEED; the index hash is checked. Skipped when the index, the catalogues or the
a3 catalogues are absent."""
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
import eval2_items_a3_gap as gen  # noqa: E402
import eval2_score as sc  # noqa: E402
from kafka_sre_rag.catalogue import Catalogues, default_dir  # noqa: E402

HAVE_INDEX = gen.INDEX_PATH.is_file() and gen.FROZEN_PATH.is_file()
HAVE = (default_dir() / "CATALOGUES.json").is_file() and (ROOT / "work" / "m4" / "catalogues_a3" / "MANIFEST.json").is_file() \
    and HAVE_INDEX and (ROOT / "eval" / "m4" / "open" / "tier_a.jsonl").is_file()
SNIPPET = ("import sys, json; sys.path.insert(0, %r); import eval2_items_a3_gap as g; "
           "print(json.dumps(g.generate(), sort_keys=True))" % str(ROOT / "scripts"))


@unittest.skipUnless(HAVE, "frozen index, catalogues or a3 catalogues not present")
class GapItems(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cats = Catalogues.load()
        cls.idx, cls.sha = gen.load_index()
        cls.items, cls.stats = gen.generate_with_stats(cls.cats, idx=cls.idx)
        cls.gaps = [i for i in cls.items if i["failure_class"] == "not_covered"]
        cls.ctls = [i for i in cls.items if i["failure_class"] != "not_covered"]
        cls.by_pair = {}
        for i in cls.items:
            cls.by_pair.setdefault(i["pair"], []).append(i)

    def test_counts_against_the_plan(self):
        self.assertEqual((len(self.gaps), len(self.ctls)), (12, 12), dict(self.stats))
        self.assertGreaterEqual(len({i["family"] for i in self.items}), 8)
        self.assertEqual(len({i["id"] for i in self.items}), 24)
        per = {}
        for i in self.items:
            per[i["template"]] = per.get(i["template"], 0) + 1
        self.assertLessEqual(max(per.values()), 10)

    def test_shape(self):
        for i in self.items:
            self.assertEqual((i["tier"], i["kind"], i["anchored"], i["exposed"]), ("A", "not_covered", True, False))
            self.assertTrue(i["id"].startswith("a3-nc_"))
            self.assertIn("catalogue_hash_a3", i)
            self.assertEqual(i["perturbed_reference"], i["perturbed"][0]["text"])
            self.assertEqual(i["chance"], 1.0 if i in self.gaps else 0.0)

    def test_pairs_share_a_value(self):
        self.assertEqual(len(self.by_pair), 12)
        for p, two in self.by_pair.items():
            self.assertEqual(sorted(x["failure_class"] for x in two), ["not_covered", "plain_lookup"], p)
            self.assertEqual(two[0]["template"], two[1]["template"])

    def test_index_hash_is_checked(self):
        frozen = json.loads(gen.FROZEN_PATH.read_text())["sha256"]
        self.assertEqual(self.sha, frozen)
        self.assertEqual(hashlib.sha256(gen.INDEX_PATH.read_bytes()).hexdigest(), frozen)
        with tempfile.TemporaryDirectory() as td:
            bad = Path(td) / "chunks.jsonl"
            bad.write_bytes(gen.INDEX_PATH.read_bytes() + b"\n")
            with self.assertRaises(sc.IndexMismatch):
                sc.ChunkIndex.load(bad, gen.FROZEN_PATH)
        for g in self.gaps:
            self.assertEqual(g["sources"][0]["index_sha256"], frozen)

    def test_every_gap_has_zero_hits_recomputed(self):
        """Independent of the generator's Searcher: plain loops over the index file's rows."""
        rows = self.idx.rows
        self.assertEqual(len(rows), json.loads(gen.FROZEN_PATH.read_text())["chunks"])
        for g in self.gaps:
            rec = g["sources"][0]
            self.assertEqual(rec["catalogue"], "frozen_index")
            self.assertEqual(rec["hits"], 0)
            self.assertTrue(rec["terms"])
            self.assertEqual(rec["chunks_searched"], len(rows))
            for term in rec["terms"]:
                tl, tn = term.lower(), re.sub(r"[^a-z0-9]", "", term.lower())
                self.assertTrue(tn)
                n = 0
                for r in rows:
                    for f in (r["text"], r.get("title") or "", r["path"]):
                        if term in f or tl in f.lower() or tn in re.sub(r"[^a-z0-9]", "", f.lower()):
                            n += 1
                self.assertEqual(n, 0, (g["id"], term))
                self.assertEqual(sum(rec["hits_per_term"][term].values()), 0)

    def test_the_search_finds_a_term_that_is_there(self):
        s = gen.Searcher(self.idx.rows)
        h = s.hits("offsets.topic.replication.factor")
        self.assertGreater(h["exact"], 0)
        self.assertGreaterEqual(h["separator_insensitive"], h["case_insensitive"])
        self.assertGreaterEqual(h["case_insensitive"], h["exact"])
        # the separator-insensitive rule also catches other separators
        self.assertGreater(s.hits("offsets_topic_replication_factor")["separator_insensitive"], 0)
        self.assertEqual(s.hits("offsets_topic_replication_factor")["exact"], 0)

    def test_old_version_keys_are_unknown_to_the_current_catalogue(self):
        for g in self.gaps:
            if g["template"] == "nc_old_version":
                src = g["sources"][1]
                self.assertEqual(self.cats.config_key(src["absent"], src["scope"]).status, "unknown", g["id"])

    def test_control_facts_are_in_the_catalogue_and_the_index(self):
        for c in self.ctls:
            src, ix = c["sources"]
            self.assertEqual(src["catalogue"], "kafka_config")
            scope, key = src["entry"].split("/", 1)
            e = next(x for x in self.cats.data["kafka_config"]["entries"] if x["scope"] == scope and x["key"] == key)
            acc = next(k for k in c["checks"] if k["type"] == "answer_equals")["accepted"]
            self.assertEqual(acc[0], e["default_value"])
            chunk = self.idx.chunks[ix["chunk_id"]]
            self.assertEqual(chunk["path"], e["source_file"])
            text = chunk["text"]
            start = text.index("## %s\n" % key) + len(key) + 4 if ("## %s\n" % key) in text else 0
            self.assertEqual(chunk.get("title") == key or start > 0, True, c["id"])
            m = re.search(r"(?m)^Default:\n(.*)$", text[start:])
            self.assertIsNotNone(m, c["id"])
            self.assertEqual(m.group(1).strip(), e["default"], c["id"])
            self.assertEqual(self.cats.config_key(key, scope).status, "known")

    def test_references_pass_their_checks(self):
        for i in self.items:
            res = sc.run_checks(i, i["reference"], self.cats)
            self.assertTrue(all(r["pass"] for r in res), (i["id"], res))

    def test_perturbed_answers_fail_the_named_check(self):
        for i in self.items:
            self.assertEqual(len(i["perturbed"]), 2)
            for p in i["perturbed"]:
                res = sc.run_checks(i, p["text"], self.cats)
                self.assertFalse(res[p["violates"]]["pass"], (i["id"], p["label"]))
                self.assertEqual(res[p["violates"]]["type"], p["check"])
        for g in self.gaps:
            self.assertEqual({p["label"] for p in g["perturbed"]}, {"invented_value", "confident_no_decline"})
            self.assertEqual({p["check"] for p in g["perturbed"]}, {"no_invented_value", "declines"})
        for c in self.ctls:
            self.assertEqual({p["label"] for p in c["perturbed"]}, {"decline", "wrong_value"})

    def test_a_confident_answer_without_the_decline_fails_declines(self):
        for g in self.gaps:
            for text in ("The default is 42.\nANSWER: 42",
                         "It is the usual built-in value.\nANSWER: the usual value",
                         "Set it in the broker file and restart.\n\n```properties\n%s=1\n```" % g["family"].split(":")[-1]):
                res = sc.run_checks(g, text, self.cats)
                by = {r["type"]: r["pass"] for r in res}
                self.assertFalse(by["declines"], (g["id"], text))
            # a decline that still states a number fails no_invented_value
            res = sc.run_checks(g, "The documentation I have does not cover it, but it is 77.\nANSWER: not in my sources", self.cats)
            self.assertFalse({r["type"]: r["pass"] for r in res}["no_invented_value"], g["id"])

    def test_blanket_decline_passes_gaps_and_fails_controls(self):
        text = "The documentation I have does not cover this.\nANSWER: not in my sources"
        for i in self.items:
            ok = all(r["pass"] for r in sc.run_checks(i, text, self.cats))
            self.assertEqual(ok, i in self.gaps, i["id"])

    def test_no_value_in_a_gap_reference_beyond_the_question(self):
        for g in self.gaps:
            nums = re.findall(r"(?<![\w.-])\d[\d,]*(?:\.\d+)*(?![\w-])", g["reference"])
            allowed = set(next(c for c in g["checks"] if c["type"] == "no_invented_value")["allowed_numbers"])
            self.assertTrue(set(nums) <= allowed, (g["id"], nums))

    def test_question_note_is_shared(self):
        for i in self.items:
            self.assertTrue(i["question"].endswith(gen.FORMAT_NOTE))

    def test_identical_under_two_hash_seeds(self):
        outs = []
        for seed in ("1", "2"):
            env = dict(os.environ, PYTHONHASHSEED=seed)
            r = subprocess.run([sys.executable, "-c", SNIPPET], capture_output=True, text=True, env=env, cwd=str(ROOT))
            self.assertEqual(r.returncode, 0, r.stderr[-800:])
            outs.append(r.stdout)
        self.assertEqual(outs[0], outs[1])
        self.assertEqual(json.loads(outs[0]), json.loads(json.dumps(self.items, sort_keys=True)))


@unittest.skipUnless(HAVE_INDEX, "frozen index not present")
class SearcherOnly(unittest.TestCase):
    def test_alnum(self):
        self.assertEqual(gen.alnum("Quota.Consumer-default_1"), "quotaconsumerdefault1")


if __name__ == "__main__":
    unittest.main()
