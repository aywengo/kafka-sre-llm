"""Tests of scripts/eval2_items_a3_vbw.py (WP1c T5): the valid-but-wrong generator. Offline, no model."""
import copy
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
A3_DIR = ROOT / "work" / "m4" / "catalogues_a3"
HAVE = (ROOT / "work" / "m4" / "catalogues").is_dir() and (A3_DIR / "semantic_rules.json").is_file()

if HAVE:
    import eval2_checks_a3 as c3
    import eval2_items_a3_vbw as g
    import eval2_score as sc
    from kafka_sre_rag.catalogue import Catalogues


def norm(s):
    return re.sub(r"\s+", " ", s).strip()


@unittest.skipUnless(HAVE, "catalogues and catalogues_a3 not built")
class Vbw(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.before = sorted(str(p) for p in (ROOT / "eval").rglob("*"))
        cls.items = g.generate()
        cls.cats = Catalogues.load(None)

    def test_counts(self):
        self.assertEqual(len(g.generate.dropped), 0)
        self.assertEqual(len(self.items), 32)
        self.assertEqual(len({i["id"] for i in self.items}), len(self.items))
        for t in {i["template"] for i in self.items}:
            n = [i for i in self.items if i["template"] == t]
            self.assertLessEqual(len(n), 10)
            self.assertGreaterEqual(sum(i["vbw"]["role"] == "control" for i in n), 1)
        self.assertGreaterEqual(len({i["family"] for i in self.items}), 8)

    def test_no_file_written(self):
        self.assertEqual(self.before, sorted(str(p) for p in (ROOT / "eval").rglob("*")))

    def test_each_item(self):
        for it in self.items:
            with self.subTest(it["id"]):
                wrong = it["vbw"]["role"] == "wrong"
                lang = it["vbw"]["artifact_language"]
                shown = re.search(r"```%s\n(.*?)\n```" % lang, it["question"], re.S).group(0)
                # half 1: the shown artifact is valid
                if lang == "yaml":
                    docs = list(sc.yaml.safe_load_all(re.search(r"```yaml\n(.*?)```", shown, re.S).group(1)))
                    self.assertEqual(len(docs), 1)
                    r = self.cats.validate_resource(docs[0])
                    self.assertTrue(r.covered and r.ok, [(v.code, v.path) for v in r.violations])
                else:
                    props = c3.parse_properties(re.search(r"```properties\n(.*?)```", shown, re.S).group(1))
                    self.assertTrue(props)
                # half 2: the oracle fails the shown artifact (wrong) or passes it (control)
                oracle = [c for c in it["checks"] if c["type"] in g.ORACLE_TYPES] if wrong else it["vbw"]["oracle_checks_on_control"]
                res = sc.run_checks({"id": it["id"], "checks": oracle}, shown, self.cats)
                self.assertTrue(res)
                if wrong:
                    self.assertTrue(any(not r["pass"] for r in res), res)
                else:
                    self.assertTrue(all(r["pass"] for r in res), res)
                # the control artifact passes
                ctl = "```%s\n%s\n```" % (lang, it["vbw"]["control_artifact"])
                res = sc.run_checks({"id": it["id"], "checks": oracle}, ctl, self.cats)
                self.assertTrue(all(r["pass"] for r in res), res)
                # reference passes everything, perturbed fails the verdict
                self.assertTrue(all(r["pass"] for r in sc.run_checks(it, it["reference"], self.cats)))
                p = sc.run_checks(it, it["perturbed_reference"], self.cats)
                self.assertTrue(p[0]["pass"] and not p[it["perturbed_violates"]]["pass"])
                if wrong:  # the blind answer "works" and the blind answer "does not work" with the shown artifact both fail
                    blind = sc.run_checks(it, shown + "\nANSWER: does not work", self.cats)
                    self.assertFalse(all(r["pass"] for r in blind))
                else:
                    self.assertTrue(all(r["pass"] for r in sc.run_checks(it, "ANSWER: works", self.cats)))

    def test_sources_exist(self):
        for it in self.items:
            self.assertTrue(it["sources"], it["id"])
            for s in it["sources"]:
                with self.subTest(it["id"], source=s["file"], line=s["line"]):
                    f = ROOT / s["file"]
                    self.assertTrue(f.is_file(), s["file"])
                    lines = f.read_text(errors="replace").split("\n")
                    self.assertTrue(1 <= s["line"] <= len(lines))
                    self.assertIn(norm(s["phrase"]), norm(lines[s["line"] - 1]))

    def test_only_anchored_rules(self):
        st = g.mechanism_status()
        for it in self.items:
            m = it["vbw"]["mechanism"]
            if m != "FK":
                self.assertTrue(st[m][0], it["id"])
            for r in it["vbw"]["rules"]:
                self.assertTrue(c3.rule_anchored(r), (it["id"], r))
        used = {i["vbw"]["mechanism"] for i in self.items}
        self.assertNotIn("V4", used)
        self.assertNotIn("V8", used)
        for r in c3.unanchored_rules():
            self.assertFalse(any(r in i["vbw"]["rules"] for i in self.items))

    def test_deterministic_across_hash_seeds(self):
        code = ("import sys; sys.path.insert(0, %r); import eval2_items_a3_vbw as g; print(g.digest(g.generate(dedup=False)))"
                % str(ROOT / "scripts"))
        outs = []
        for seed in ("1", "2718"):
            env = dict(os.environ, PYTHONHASHSEED=seed)
            outs.append(subprocess.run([sys.executable, "-c", code], env=env, cwd=str(ROOT), capture_output=True,
                                       text=True, check=True).stdout.strip())
        self.assertEqual(outs[0], outs[1])
        self.assertEqual(outs[0], g.digest(g.generate(dedup=False)))


@unittest.skipUnless(HAVE, "catalogues and catalogues_a3 not built")
class Gating(unittest.TestCase):
    """A rule that the table anchors later is picked up by rerunning; valid_but_wrong false keeps a rule out."""

    def make(self, edit):
        d = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, d, True)
        for f in A3_DIR.glob("*.json"):
            shutil.copy(f, d / f.name)
        t = json.loads((d / "semantic_rules.json").read_text())
        edit(t["mechanisms"])
        (d / "semantic_rules.json").write_text(json.dumps(t, indent=1, sort_keys=True))
        return d

    def used(self, d):
        return {m for m, (u, _) in g.mechanism_status(str(d)).items() if u}

    def test_valid_but_wrong_false_is_skipped(self):
        def edit(m):
            m["V8"]["anchored"] = True
            m["V8"]["valid_but_wrong"] = False
        d = self.make(edit)
        self.assertNotIn("V8", self.used(d))
        items = g.generate(catalogues_a3_dir=str(d), dedup=False)
        self.assertFalse(any(i["vbw"]["mechanism"] == "V8" for i in items))

    def test_real_table_skips_v4_and_v8(self):
        used = self.used(A3_DIR)
        self.assertNotIn("V4", used)
        self.assertNotIn("V8", used)
        self.assertFalse(g.mechanism_status()["V8"][0])

    def test_unanchored_table_entry_is_skipped(self):
        def edit(m):
            m["V1"]["anchored"] = False
        self.assertNotIn("V1", self.used(self.make(edit)))

    def test_v8_picked_up_when_valid_but_wrong(self):
        def edit(m):
            m["V8"]["anchored"] = True
            m["V8"]["valid_but_wrong"] = True
            m["V8"]["anchors"] = copy.deepcopy(m["V1"]["anchors"])
        d = self.make(edit)
        items = g.generate(catalogues_a3_dir=str(d), dedup=False)
        v8 = [i for i in items if i["vbw"]["mechanism"] == "V8"]
        self.assertEqual(len(v8), 4)
        self.assertEqual(len(items), 32)  # FK gives up four places

    def test_v4_picked_up_when_both_anchors_flip(self):
        def edit(m):
            m["V4"]["anchored"] = True
            m["V4"]["anchors"] = copy.deepcopy(m["V1"]["anchors"])
        d = self.make(edit)
        self.assertNotIn("V4", self.used(d))  # the scorer module still marks properties.load_semantics unanchored
        old = c3.RULES["properties.load_semantics"]["anchored"]
        c3.RULES["properties.load_semantics"]["anchored"] = True
        try:
            self.assertIn("V4", self.used(d))
            items = g.generate(catalogues_a3_dir=str(d), dedup=False)
        finally:
            c3.RULES["properties.load_semantics"]["anchored"] = old
        self.assertEqual(len([i for i in items if i["vbw"]["mechanism"] == "V4"]), 4)


if __name__ == "__main__":
    unittest.main()
