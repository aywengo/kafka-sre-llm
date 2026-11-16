import ast
import hashlib
import importlib.util
import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from kafka_sre_rag import prompts as P  # noqa: E402

PACKS, INDEX = ROOT / "work/v3/rag_packs_base_exp.json", ROOT / "work/v2/chunks.jsonl"
GOLDEN = ROOT / "tests/golden/prompts_phase0.json"


def load_script(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    m = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(ROOT / "scripts"))
    spec.loader.exec_module(m)
    return m


def h(s):
    return {"sha256": hashlib.sha256(s.encode("utf-8")).hexdigest(), "len": len(s)}


CHUNKS = [
    {"source": "apache-kafka", "title": "Consumer configs", "path": "raw/a/b.md", "text": "Set max.poll.interval.ms."},
    {"source": "strimzi", "title": "", "path": "raw/strimzi/docs/rolling.md", "text": "Use maxUnavailable."},
    {"source": "kubernetes", "title": "Probes", "path": "raw/k/p.md", "text": "Tune the liveness probe."},
]
Q = "Why does the group rebalance?"
PACK = ("apache-kafka — Consumer configs\nSet max.poll.interval.ms.\n\n"
        "strimzi — rolling.md\nUse maxUnavailable.\n\n"
        "kubernetes — Probes\nTune the liveness probe.")
HEAD = "Use these documentation excerpts where relevant."


class ToyGolden(unittest.TestCase):
    def test_pack(self):
        self.assertEqual(P.pack_text(CHUNKS), PACK)

    def test_default(self):
        self.assertEqual(P.user_message(Q, CHUNKS), HEAD + "\n\n" + PACK + "\n\nQuestion: " + Q)

    def test_strict(self):
        strict = ("Use only commands, flags, configuration keys, resource fields and annotations that appear in the "
                  "excerpts. If the excerpts do not cover part of the question, say so in one sentence instead of "
                  "filling the gap.")
        self.assertEqual(P.user_message(Q, CHUNKS, strict=True), HEAD + " " + strict + "\n\n" + PACK + "\n\nQuestion: " + Q)

    def test_thorough(self):
        thorough = ("Answer thoroughly: give every likely cause the situation allows, how to check each one "
                    "(exact command, metric or field) and the fix, then what to do if none of them fit.")
        self.assertEqual(P.user_message(Q, CHUNKS, thorough=True), HEAD + "\n\n" + PACK + "\n\nQuestion: " + Q + "\n\n" + thorough)
        self.assertEqual(P.user_message(Q, None, thorough=True), Q + "\n\n" + thorough)

    def test_closed_book(self):
        self.assertEqual(P.user_message(Q, None), Q)

    def test_messages(self):
        m = P.answer_messages(Q, None)
        self.assertEqual(m, [{"role": "system", "content": P.SYSTEM_PROMPT}, {"role": "user", "content": Q}])
        self.assertEqual(P.expand_messages(Q), [{"role": "system", "content": P.EXPAND_SYSTEM}, {"role": "user", "content": Q}])


@unittest.skipUnless(PACKS.exists() and INDEX.exists(), "work/v3/rag_packs_base_exp.json or work/v2/chunks.jsonl absent (work/ is git-ignored)")
class Phase0Contract(unittest.TestCase):
    def test_hashes(self):
        gold = json.loads(GOLDEN.read_text())
        self.assertEqual(h(P.SYSTEM_PROMPT), gold["system_prompt"])
        self.assertEqual(h(P.EXPAND_SYSTEM), gold["expand_system"])
        by_id = {}
        for line in open(INDEX):
            c = json.loads(line)
            by_id[c["id"]] = c
        rows = json.load(open(PACKS))
        self.assertEqual(len(rows), 40)
        checked = 0
        for i, r in enumerate(rows, 1):
            g, q = gold["questions"][str(i)], r["question"]
            k6, k3 = [by_id[x] for x in r["k6"]], [by_id[x] for x in r["k3"]]
            got = {"user_k6": P.user_message(q, k6), "user_k3": P.user_message(q, k3),
                   "user_k6_strict": P.user_message(q, k6, strict=True),
                   "user_k6_thorough": P.user_message(q, k6, thorough=True),
                   "pack_k6": P.pack_text(k6), "pack_k3": P.pack_text(k3)}
            for name, text in got.items():
                self.assertEqual(h(text), g[name], f"question {i} {name}")
                checked += 1
        self.assertEqual(checked, 240)


class Reexports(unittest.TestCase):
    def test_same_strings(self):
        self.assertIs(load_script("build_dataset").SYSTEM_PROMPT, P.SYSTEM_PROMPT)
        self.assertIs(load_script("retrieve").EXPAND_SYSTEM, P.EXPAND_SYSTEM)


class StdlibOnly(unittest.TestCase):
    def test_imports(self):
        tree = ast.parse((ROOT / "kafka_sre_rag/prompts.py").read_text())
        mods = set()
        for n in ast.walk(tree):
            if isinstance(n, ast.Import):
                mods |= {a.name.split(".")[0] for a in n.names}
            elif isinstance(n, ast.ImportFrom):
                mods.add((n.module or "").split(".")[0] if n.level == 0 else "<relative>")
        self.assertLessEqual(mods, set(sys.stdlib_module_names), mods)


if __name__ == "__main__":
    unittest.main()
