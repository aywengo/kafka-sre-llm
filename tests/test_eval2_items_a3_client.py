"""Client-code item generator of the a3 slice (WP1c T4): scripts/eval2_items_a3_client.py.

The Java half needs javac, the two pinned jars (raw_catalogue/maven/) and java_api.json, the Python half needs
python_symbols.json, the anchors need the pinned docs and the Kafka sources under raw/ and raw_catalogue/. Whatever is
missing is skipped. javac runs only through the checks of scripts/eval2_checks_a3_client.py (temporary folder, timeout);
nothing compiled is run. No model, no network."""
import hashlib
import json
import os
import shutil
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
import eval2_checks_a3_client as cc  # noqa: E402
import eval2_items_a3_client as gen  # noqa: E402
from kafka_sre_rag import catalogue_a3 as a3  # noqa: E402

REAL = ROOT / "work" / "m4" / "catalogues_a3"
HAVE_CATS = (ROOT / "work" / "m4" / "catalogues").is_dir() and REAL.is_dir()
HAVE_JAVA = bool(shutil.which("javac") and shutil.which("java") and cc.jar_paths()[0]) and (REAL / "java_api.json").is_file()
HAVE_PY = (REAL / "python_symbols.json").is_file()
HAVE_OPEN = (ROOT / "eval" / "m4" / "open" / "tier_a.jsonl").is_file()
NEED = HAVE_CATS and HAVE_OPEN and (HAVE_JAVA or HAVE_PY)

_state = {}


def built():
    """(items, problems): the items of generate(verify=False) and verify_item's verdict per id, computed once."""
    if "items" not in _state:
        items = gen.generate(verify=False)
        from kafka_sre_rag.catalogue import Catalogues
        cats = Catalogues.load()
        problems = {}
        with gen._Memo():
            for it in items:
                problems[it["id"]] = gen.verify_item(it, cats)
        _state["items"], _state["problems"] = items, problems
    return _state["items"], _state["problems"]


def by_lang(lang):
    return [i for i in built()[0] if i["language"] == lang]


class ApplyOpTests(unittest.TestCase):
    SRC = "a\n    b\n    c\nd"

    def test_replace_needs_one_match(self):
        self.assertEqual(gen.apply_op(self.SRC, ("replace", "c", "C")), "a\n    b\n    C\nd")
        with self.assertRaises(AssertionError):
            gen.apply_op("x x", ("replace", "x", "y"))

    def test_remove(self):
        self.assertEqual(gen.apply_op(self.SRC, ("remove", "b")), "a\n    c\nd")

    def test_move_keeps_the_indentation_of_the_target(self):
        self.assertEqual(gen.apply_op(self.SRC, ("move_before", "d", "b")), "a\n    d\n    b\n    c")
        self.assertEqual(gen.apply_op(self.SRC, ("move_after", "a", "c")), "    b\n    c\n    a\nd")

    def test_fill_leaves_nothing_open(self):
        self.assertEqual(gen.fill("x %%a%% y", {"a": 1}), "x 1 y")
        with self.assertRaises(AssertionError):
            gen.fill("x %%b%%", {"a": 1})

    def test_sub_replaces_longest_name_first(self):
        self.assertEqual(gen.sub("%topic-%t", {"t": "T", "topic": "orders"}), "orders-T")

    def test_anchor_check(self):
        ok, why = gen.verify_anchor("tests/test_eval2_items_a3_client.py", 1, "Client-code item generator")
        self.assertTrue(ok, why)
        self.assertFalse(gen.verify_anchor("tests/test_eval2_items_a3_client.py", 1, "no such phrase")[0])
        self.assertFalse(gen.verify_anchor("tests/test_eval2_items_a3_client.py", 99999, "x")[0])
        self.assertFalse(gen.verify_anchor("tests/none.py", 1, "x")[0])

    def test_templates_are_consistent(self):
        # no model and no network in the generator; every template names a probe or config scope it can mutate
        text = (ROOT / "scripts" / "eval2_items_a3_client.py").read_text()
        for word in ("urllib", "import requests", "mlx", "socket", "openai", "anthropic"):
            self.assertNotIn(word, text)
        for t in gen.JAVA_TEMPLATES + gen.PY_TEMPLATES:
            self.assertGreaterEqual(len(t["variants"]), 2, t["template"])
            self.assertTrue(t["anchors"], t["template"])
            self.assertNotIn("rocksdb", t["source"].lower(), t["template"])
        self.assertEqual(sum(len(t["variants"]) for t in gen.JAVA_TEMPLATES), gen.PLAN["java"])
        self.assertEqual(sum(len(t["variants"]) for t in gen.PY_TEMPLATES), gen.PLAN["python"])

    def test_memo_restores_list_calls(self):
        orig = cc.list_calls
        with gen._Memo():
            self.assertIsNot(cc.list_calls, orig)
        self.assertIs(cc.list_calls, orig)


@unittest.skipUnless(NEED, "catalogues, the open file, and the jars or the Python symbol list are missing")
class ItemTests(unittest.TestCase):
    def test_counts_against_the_plan(self):
        nj, npy = len(by_lang("java")), len(by_lang("python"))
        print("\nclient items: %d Java (plan %d), %d Python (plan %d)" % (nj, gen.PLAN["java"], npy, gen.PLAN["python"]))
        if HAVE_JAVA:
            self.assertEqual(nj, gen.PLAN["java"])
        else:
            self.assertEqual(nj, 0)
        if HAVE_PY:
            self.assertEqual(npy, gen.PLAN["python"])

    def test_every_reference_passes_every_check(self):
        from kafka_sre_rag.catalogue import Catalogues
        import eval2_score as sc
        cats = Catalogues.load()
        items, problems = built()
        with gen._Memo():
            for it in items:
                res = sc.run_checks(it, it["reference"], cats)
                self.assertTrue(all(r["pass"] for r in res), (it["id"], [r for r in res if not r["pass"]]))
        self.assertTrue(all(v is None for v in problems.values()), {k: v for k, v in problems.items() if v})

    def test_every_perturbed_program_fails_its_named_check(self):
        from kafka_sre_rag.catalogue import Catalogues
        import eval2_score as sc
        cats = Catalogues.load()
        items, _ = built()
        exact_kinds = {"wrong_order", "invented_key", "bad_import", "invented_method"}
        seen = set()
        with gen._Memo():
            for it in items:
                kinds = [p["kind"] for p in it["perturbed"]]
                self.assertEqual(len(kinds), 6, it["id"])
                self.assertEqual(len(set(kinds)), 6, it["id"])
                for pt in it["perturbed"]:
                    res = sc.run_checks(it, pt["text"], cats)
                    failed = sorted(r["type"] for r in res if not r["pass"])
                    self.assertIn(pt["check"], failed, (it["id"], pt["kind"], failed))
                    self.assertEqual(failed, pt["fails"], (it["id"], pt["kind"]))
                    self.assertEqual(it["checks"][pt["violates"]]["type"], pt["check"])
                    if pt["kind"] in exact_kinds or (pt["kind"] == "wrong_signature" and pt["fails"] == [pt["check"]]):
                        self.assertEqual(failed, [pt["check"]], (it["id"], pt["kind"]))
                    self.assertNotEqual(pt["text"], it["reference"])
                    seen.add(pt["kind"])
        self.assertEqual(seen, set(gen.JAVA_KINDS + gen.PY_KINDS) if (HAVE_JAVA and HAVE_PY) else seen)

    @unittest.skipUnless(HAVE_JAVA, "javac or the jars are missing")
    def test_every_java_skeleton_compiles_against_the_jars(self):
        for it in by_lang("java"):
            ok, detail = cc.check_java_compiles({"timeout": 30}, it["reference"], None)
            self.assertTrue(ok, (it["id"], detail))
            self.assertFalse(detail.startswith("java_template_rocksdb"))
            self.assertNotIn("rocksdb", it["reference"].lower())

    def test_every_cited_source_exists(self):
        n = 0
        for it in built()[0]:
            for s in it["sources"]:
                if s["file"] is None:
                    self.assertIn(s["catalogue"], ("java_api", "python_symbols"))
                    continue
                p = ROOT / s["file"]
                if not p.is_file():
                    self.skipTest("%s is not fetched" % s["file"])
                lines = p.read_text(errors="replace").split("\n")
                self.assertTrue(1 <= s["line"] <= len(lines), (it["id"], s["file"], s["line"]))
                ok, why = gen.verify_anchor(s["file"], s["line"], s["fact"])
                self.assertTrue(ok, (it["id"], why))
                n += 1
        self.assertGreater(n, 0)

    def test_item_fields(self):
        for it in built()[0]:
            self.assertEqual(it["kind"], "client_code")
            self.assertEqual(it["tier"], "A")
            self.assertTrue(it["id"].startswith("a3-client_"))
            self.assertTrue(it["anchored"])
            self.assertEqual(it["not_in_index"], it["language"] == "python")
            self.assertEqual(it["generator"], gen.VERSION)
            self.assertEqual(it["seed"], gen.DEFAULT_SEED)
            self.assertEqual(it["perturbed_reference"], it["perturbed"][0]["text"])
            self.assertEqual(sum(1 for c in it["checks"] if c.get("format")), 1)
            self.assertIn("catalogue_hash_a3", it)
            self.assertNotIn(it["reference"], it["question"])
            fence = "```java" if it["language"] == "java" else "```python"
            self.assertTrue(it["reference"].startswith(fence))
            for pt in it["perturbed"]:
                self.assertTrue(pt["text"].startswith(fence))
        ids = [i["id"] for i in built()[0]]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertEqual(ids, sorted(ids))
        self.assertGreaterEqual(len({i["family"] for i in built()[0] if i["language"] == "java"}), 9 if HAVE_JAVA else 0)

    def test_questions_are_not_near_duplicates(self):
        for it in built()[0]:
            d = it["dedup"]
            self.assertLess(d["max_jaccard"], 0.5, it["id"])
            self.assertLess(d["max_containment"], 0.8, it["id"])

    def test_questions_name_what_the_checks_need(self):
        for it in built()[0]:
            q = it["question"]
            for c in it["checks"]:
                for k, v in (c.get("keys") or {}).items():
                    if k in ("bootstrap.servers", "group.id", "transactional.id", "application.id") and v:
                        self.assertIn(v, q, (it["id"], k))

    def test_no_file_is_written_under_eval(self):
        def snap():
            return sorted((str(p.relative_to(ROOT)), p.stat().st_size, p.stat().st_mtime_ns)
                          for p in (ROOT / "eval").rglob("*") if p.is_file() and "hidden" not in p.parts)
        before = snap()
        gen.generate(verify=False)
        self.assertEqual(before, snap())


@unittest.skipUnless(NEED, "catalogues, the open file, and the jars or the Python symbol list are missing")
class DeterminismTests(unittest.TestCase):
    CODE = ("import sys, json, hashlib; sys.path.insert(0, %r); import eval2_items_a3_client as g; "
            "items = g.generate(verify=False); "
            "print(hashlib.sha256(json.dumps(items, sort_keys=True).encode()).hexdigest(), len(items))" % str(ROOT / "scripts"))

    def run_with(self, seed):
        env = dict(os.environ, PYTHONHASHSEED=seed)
        r = subprocess.run([sys.executable, "-c", self.CODE], env=env, capture_output=True, text=True, cwd=str(ROOT), timeout=300)
        self.assertEqual(r.returncode, 0, r.stderr[-500:])
        return r.stdout.strip()

    def test_identical_under_two_hash_seeds(self):
        a, b = self.run_with("1"), self.run_with("2")
        self.assertEqual(a, b)
        items = built()[0]
        self.assertEqual(a, "%s %d" % (hashlib.sha256(json.dumps(items, sort_keys=True).encode()).hexdigest(), len(items)))


if __name__ == "__main__":
    unittest.main()
