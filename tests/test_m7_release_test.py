"""The M7 release test chain (scripts/m7_release_test.sh, scripts/m7_release_test.py): stub dry runs end to end, the h6 hygiene, the refusals.
Offline: a stub ask, a stub judge, a stub server file; a test key in a temp dir; synthetic fixtures (a canary string stands for item text); no real hidden file, no server."""
import copy
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import eval2  # noqa: E402
import m7_release_test as T  # noqa: E402

NEEDS = unittest.skipUnless(shutil.which("openssl") and shutil.which("sh"), "openssl or sh missing")
CANARY = "zebra-canary-h6-text"
CONFIGS = ROOT / "eval/m7/release_test/configs.json"
PYBIN = str(ROOT / ".venv/bin/python") if (ROOT / ".venv/bin/python").exists() else sys.executable
DS = "DeepSeek-V4.1-Flash-1017-oQ4e-mtp"


def jl(rows):
    return "".join(json.dumps(r) + "\n" for r in rows)


class Env:
    """A temp world: key, home, fixtures (open D7 stand-in, hidden h6 stand-in, chunks, ledger)."""

    def __init__(self, n_d7=4, n_h6=4):
        self.d = Path(tempfile.mkdtemp())
        self.key = self.d / "key"
        eval2.keygen(self.key)
        self.home = self.d / "home"
        self.home.mkdir()
        self.env = {"PATH": os.environ["PATH"], "PY": PYBIN, "HOME": str(self.home), "EVAL_HIDDEN_KEY": str(self.key), "M7_STUB_CHUNKS": str(self.d / "chunks.jsonl")}
        chunks = [{"id": f"c{i:02d}", "source": "docs", "license": "x", "path": f"raw/f{i}.md", "title": f"T{i}", "text": f"chunk text {i}"} for i in range(12)]
        (self.d / "chunks.jsonl").write_text(jl(chunks))
        self.d7 = [{"id": f"d7-{i:02d}", "kind": "diagnostics" if i % 2 else "debugging", "family": f"f{i}", "tier": "B", "domain": ["kafka"], "question": f"open question {i}", "evidence": "ev"}
                   for i in range(1, n_d7 + 1)]
        self.rub = lambda items: [{"id": it["id"], "must": [{"id": "m1", "text": "alpha", "weight": 2, "first": True}, {"id": "m2", "text": "beta", "weight": 1}],
                                   "must_not": [{"id": "n1", "text": "bad", "severity": "high"}], "any_of": []} for it in items]
        (self.d / "d7_items.jsonl").write_text(jl(self.d7))
        (self.d / "d7_rubrics.jsonl").write_text(jl(self.rub(self.d7)))
        self.h6 = [{"id": f"h6-{i:02d}", "tier": "B", "kind": "diagnostics", "family": f"g{i}", "domain": "kafka", "question": f"{CANARY} question {i}", "evidence": f"{CANARY} evidence"}
                   for i in range(1, n_h6 + 1)]
        self._enc("h6_items.jsonl.enc", self.h6)
        self._enc("h6_rubrics.jsonl.enc", [dict(r, must=[dict(m, text=f"{CANARY} {m['text']}") for m in r["must"]]) for r in self.rub(self.h6)])
        self.sha = T.sha256_file(CONFIGS)
        (self.d / "ledger.md").write_text(f"| 2026-11-27 | release-look-h6 | configs {self.sha} | rule fixed |\n")

    def _enc(self, name, rows):
        os.environ["EVAL_HIDDEN_KEY"] = str(self.key)
        eval2.write_jsonl(self.d / name, rows)

    def args(self, *extra, sets="d7", arms="R,C", ledger=True):
        a = ["--dry-run", "--work", str(self.d / "w"), "--arms", arms, "--set", sets, "--chunks", str(self.d / "chunks.jsonl"), "--h6-sessions", "2",
             "--d7-items", str(self.d / "d7_items.jsonl"), "--d7-rubrics", str(self.d / "d7_rubrics.jsonl"),
             "--h6-items", str(self.d / "h6_items.jsonl.enc"), "--h6-rubrics", str(self.d / "h6_rubrics.jsonl.enc")]
        if ledger:
            a += ["--ledger", str(self.d / "ledger.md")]
        return a + list(extra)

    def run(self, *args, extra_env=None, cmd="scripts/m7_release_test.sh"):
        env = dict(self.env, **(extra_env or {}))
        p = subprocess.run(["sh", cmd, *args] if cmd.endswith(".sh") else [PYBIN, cmd, *args], cwd=ROOT, env=env, capture_output=True, text=True, timeout=900)
        return p

    def cleanup(self):
        shutil.rmtree(self.d, True)

    def plain_files(self):
        return [f for f in self.d.rglob("*") if f.is_file() and not f.name.endswith(".enc") and f.name not in ("key",)]


@NEEDS
class Arms(unittest.TestCase):
    def test_arms_come_from_configs_and_a_variant_entry_needs_no_code_change(self):
        cfgs = T.load_configs(CONFIGS)
        self.assertEqual(T.arm_names(cfgs), ["R", "C"])
        r = T.arm_info(cfgs, "R")
        self.assertIn("--lookup-cards", r["flags"])
        self.assertEqual(r["model"], "gemma-4-31b-it-4bit")
        c = T.arm_info(cfgs, "C")
        self.assertNotIn("--lookup-cards", c["flags"])
        self.assertEqual(T.ask_argv(c, "Q?", "py")[:5], ["py", "-m", "kafka_sre_rag", "ask", "--backend"])
        self.assertEqual(T.ask_argv(c, "Q?", "py")[-1], "Q?")
        v = copy.deepcopy(cfgs)
        v["R2"] = copy.deepcopy(v["R"])
        v["R2"]["ask_command"] = v["R2"]["ask_command"].replace("--lookup-cards", "--lookup-cards --lookup-kb entries.json")
        self.assertEqual(T.arm_names(v), ["R", "C", "R2"])
        self.assertIn("--lookup-kb", T.arm_info(v, "R2")["flags"])
        with self.assertRaises(T.Refused):
            T.arm_info(cfgs, "X")
        bad = copy.deepcopy(cfgs)
        bad["R"]["ask_command"] = bad["R"]["ask_command"].replace(" --json", "")
        with self.assertRaises(T.Refused):
            T.arm_info(bad, "R")


@NEEDS
class Refusals(unittest.TestCase):
    def setUp(self):
        self.e = Env()
        self.addCleanup(self.e.cleanup)

    def test_ledger_token_refusal(self):
        for text in ("", f"release-look-h6 without the hash\n", f"{self.e.sha} but no token\n"):
            (self.e.d / "ledger.md").write_text(text)
            p = self.e.run(*self.e.args(sets="h6"))
            self.assertEqual(p.returncode, 2, p.stderr)
            self.assertIn("release-look-h6", p.stderr)
            self.assertFalse(list((self.e.d / "w").glob("ans_*")))
        p = self.e.run(*self.e.args(sets="h6", ledger=False), extra_env={})   # the default ledger path does not exist in a temp world
        self.assertEqual(p.returncode, 2)
        # the python answer command checks it as well, before the first item
        p = self.e.run("answer", "--arm", "R", "--set", "h6", "--items", str(self.e.d / "h6_items.jsonl.enc"), "--out", str(self.e.d / "a.jsonl.enc"), "--stub",
                       "--ledger", str(self.e.d / "ledger.md"), cmd="scripts/m7_release_test.py")
        self.assertEqual(p.returncode, 2)
        self.assertFalse((self.e.d / "a.jsonl.enc").exists())
        (self.e.d / "ledger.md").write_text(f"x release-look-h6 {self.e.sha}\n")
        self.assertTrue(T.ledger_ok(self.e.d / "ledger.md", self.e.sha))

    def test_configs_hash_refusal(self):
        p = self.e.run(*self.e.args("--limit", "2"))
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        amended = self.e.d / "configs2.json"
        c = json.loads(CONFIGS.read_text())
        c["fixed"] = "2026-11-27"
        amended.write_text(json.dumps(c))
        p = self.e.run(*self.e.args("--limit", "2", "--configs", str(amended)))
        self.assertEqual(p.returncode, 2)
        self.assertIn("amended configuration means a new manifest", p.stderr)

    def test_w2_foreign_model_is_never_unloaded_and_w16_is_only_the_deepseek_build(self):
        stub = self.e.d / "stub.json"
        self.e.run("stub-status", "--file", str(stub), "--init", "--loaded", "Foreign-Model", cmd="scripts/m7_kb2_rule.py")
        p = self.e.run("server", "gate", "--work", str(self.e.d), "--need", "gemma-4-31b-it-4bit", "--stub-file", str(stub), cmd="scripts/m7_release_test.py")
        self.assertEqual(p.returncode, 2)
        self.assertIn("Foreign-Model", p.stderr)
        self.assertIn("another client", p.stderr)
        self.assertIn("Foreign-Model", json.loads(stub.read_text())["loaded_models"])
        # through the chain: refused, nothing answered, nothing unloaded
        p = self.e.run(*self.e.args("--limit", "2", "--stub-loaded", "Foreign-Model"))
        self.assertEqual(p.returncode, 2, p.stdout + p.stderr)
        self.assertIn("REFUSED", p.stderr)
        self.assertFalse(list((self.e.d / "w").glob("ans_*")))
        self.assertIn("Foreign-Model", json.loads((self.e.d / "w/stub_server.json").read_text())["loaded_models"])
        # the DeepSeek build of another client is unloaded under W16 and the chain goes on
        e2 = Env()
        self.addCleanup(e2.cleanup)
        p = e2.run(*e2.args("--limit", "2", "--arms", "R", "--stub-loaded", DS))
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)

    def test_a_dry_run_never_uses_the_real_hidden_files_and_the_keys_resolver_is_stub_only(self):
        p = self.e.run("check-inputs", "--set", "h6", "--items", str(ROOT / "eval/hidden/tier_b_h6_items_final.jsonl.enc"), "--rubrics", str(ROOT / "eval/hidden/tier_b_h6_rubrics_final.jsonl.enc"),
                       "--stub", cmd="scripts/m7_release_test.py")
        self.assertEqual(p.returncode, 2)
        self.assertIn("eval/hidden", p.stderr)
        p = self.e.run("packs", "--set", "d7", "--arm", "R", "--items", str(self.e.d / "d7_items.jsonl"), "--answers", str(self.e.d / "x.jsonl"), "--resolver", "keys", cmd="scripts/m7_release_test.py")
        self.assertEqual(p.returncode, 2)


@NEEDS
class Chain(unittest.TestCase):
    def test_d7_and_h6_end_to_end_with_r_and_c(self):
        e = Env()
        self.addCleanup(e.cleanup)
        p = e.run(*e.args(sets="d7,h6"))
        out = p.stdout + p.stderr
        self.assertEqual(p.returncode, 0, out)
        w = e.d / "w"
        self.assertTrue((w / "stage_header.done").exists())
        # the header and the manifest hold the arms, the configs hash, the sets and the rule text before the first answer
        head = (w / "report_header.md").read_text()
        for needle in ("Arms: R,C", e.sha, "Sets: d7,h6", "met, not met or not measured", "bundle 20261126"):
            self.assertIn(needle, head)
        man = json.loads((w / "MANIFEST.json").read_text())
        self.assertEqual(man["configs_sha256"], e.sha)
        self.assertEqual(man["arms"], ["R", "C"])
        self.assertLess((w / "stage_header.done").stat().st_mtime_ns, (w / "ans_d7_R.jsonl").stat().st_mtime_ns)
        # row shape of the live runtime's answer, one request per item, resumable (a second run does nothing new)
        rows = [json.loads(l) for l in (w / "ans_d7_R.jsonl").read_text().splitlines()]
        self.assertEqual(len(rows), 4)
        self.assertTrue(all(r["pack"] and r["pack_check"] == "exact" and r["seconds"]["total"] > 0 and r["arm"] == "R" for r in rows))
        self.assertTrue(all("lookup" in r for r in rows))                     # R's flags (--lookup-cards) reached the runtime
        crow = json.loads((w / "ans_d7_C.jsonl").read_text().splitlines()[0])
        self.assertNotIn("lookup", crow)
        self.assertEqual(len(list((w / "reading_d7/export_second_reader").glob("*.md"))) >= 4, True)
        self.assertFalse((w / "reading_d7/export_second_reader/KEY.json").exists())
        # h6: stdout, stderr, logs and every plain file are free of item text; the sealed files exist encrypted
        h6files = [w / "ans_h6_R.jsonl.enc", w / "ans_h6_C.jsonl.enc", w / "judge_h6.jsonl.enc"]
        for f in h6files:
            self.assertTrue(f.exists(), f.name)
            self.assertNotIn(CANARY.encode(), f.read_bytes())
        self.assertNotIn(CANARY, out)
        for f in e.plain_files():
            self.assertNotIn(CANARY, f.read_text(errors="replace"), f.name)
        self.assertNotIn("h6-0", out)
        log = (w / "logs/m7_release_test.ans_h6_R.log").read_text()
        self.assertRegex(log, r"\[answer R h6\] 1/4 ok")
        self.assertNotIn("h6-0", log)
        self.assertNotIn("h6-0", (w / "logs/m7_release_test.judge_h6.log").read_text())
        # reading, unblinding, the adjudication pack and the report
        key = json.loads((w / "reading_d7/KEY.json").read_text())
        names = sorted(key["aid"])
        row = lambda n, kind, **k: dict({"answer": n, "id": n.rsplit("-", 1)[0], "kind": kind, "quote": "Stub answer", "minor": False, "source": "f.md:1", "note": "n"}, **k)
        f1 = [row(names[0], "harmful", consequence="data loss"), row(names[1], "wrong")]
        f2 = [row(names[0], "harmful", consequence="outage of the cluster")]
        (e.d / "f_claude.jsonl").write_text(jl(f1))
        (e.d / "f_codex.jsonl").write_text(jl(f2))
        hdir = w / "reading_h6"
        hk = json.loads(eval2.decrypt_bytes((hdir / "blind_key.json.enc").read_bytes()))
        for b, ns in hk["bundles"].items():
            rows_ = [{"answer": n, "id": n.rsplit("-", 1)[0], "kind": "clean", "quote": "", "point": "", "supported": False, "minor": False, "source": "", "note": ""} for n in ns]
            rows_[0] = {"answer": ns[0], "id": ns[0].rsplit("-", 1)[0], "kind": "harmful", "quote": "Stub answer", "point": "", "supported": False, "minor": False, "source": "s", "note": "n", "consequence": "data loss"}
            eval2.write_jsonl(hdir / f"findings_{b}.jsonl.enc", rows_)
        # --findings-d7 consumes the words after it: it goes last
        a = e.args("--phase", "after-reading", "--h6-dir", str(hdir), "--findings-d7", f"claude={e.d / 'f_claude.jsonl'}", f"codex={e.d / 'f_codex.jsonl'}", sets="d7,h6")
        p = e.run(*a)
        out2 = p.stdout + p.stderr
        self.assertEqual(p.returncode, 0, out2)
        self.assertIn("BARS: B1 not measured; B2 not measured", out2)       # the SRE's verdicts are not in
        self.assertNotIn(CANARY, out2)
        rep = next(w.glob("M7_RELEASE_TEST_from_stub_data.md"))
        text = rep.read_text()
        self.assertIn("hidden: totals only", text)
        self.assertNotIn(CANARY, text)
        for f in e.plain_files():
            self.assertNotIn(CANARY, f.read_text(errors="replace"), f.name)
        # the pack input has the format of scripts/m7_adjudication_pack.py: it builds, and verdicts feed the report
        pi = w / "pack_input_d7"
        flags = [json.loads(l) for l in (pi / "flags.jsonl").read_text().splitlines()]
        self.assertEqual({f["kind"] for f in flags}, {"harm", "material_error"})
        self.assertEqual({f["reader"] for f in flags if f["kind"] == "harm"}, {"claude", "codex"})
        self.assertTrue(all(f["answer_id"].startswith("d7-") for f in flags))
        frozen = e.d / "frozen.json"
        frozen.write_text(json.dumps({"index_path": "x", "source_commits": {}}))
        pk = e.d / "pack"
        b = subprocess.run([PYBIN, "scripts/m7_adjudication_pack.py", "build", "--answers", str(pi / "answers.jsonl"), "--scenarios", str(pi / "scenarios.jsonl"), "--flags", str(pi / "flags.jsonl"),
                            "--out", str(pk), "--shuffle-seed", "20261127", "--sample-seed", "20261128", "--index-frozen", str(frozen), "--chunks", str(e.d / "chunks.jsonl")],
                           cwd=ROOT, capture_output=True, text=True, env=e.env)
        self.assertEqual(b.returncode, 0, b.stdout + b.stderr)
        pkey = json.loads((pk / "key.json").read_text())
        verdicts = {}
        for pid, it in pkey["items"].items():
            s1 = {"harmful": False, "severity": "none", "note": "", "quote": ""}
            verdicts[pid] = {"stage1": s1} if it["kind"] == "sample" else {"stage1": s1, "stage2": {"harmful": False, "severity": "none", "note": "", "decision": "revised"}}
        (e.d / "verdicts.json").write_text(json.dumps({"schema": "m7-adjudication-verdicts-1", "pack_sha256": pkey["pack_sha256"], "verdicts": verdicts}))
        a = e.args("--phase", "after-reading", "--h6-dir", str(hdir), "--verdicts-d7", str(e.d / "verdicts.json"), "--pack-key-d7", str(pk / "key.json"),
                   "--findings-d7", f"claude={e.d / 'f_claude.jsonl'}", f"codex={e.d / 'f_codex.jsonl'}", sets="d7,h6")
        p = e.run(*a)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        text = next(w.glob("M7_RELEASE_TEST_from_stub_data.md")).read_text()
        self.assertIn("0/4 = 0.0 %", text)                                  # H after adjudication (all flags rejected)
        self.assertIn("### Bars", text)

    def test_r_alone_has_no_comparison_and_the_bars_say_so(self):
        e = Env()
        self.addCleanup(e.cleanup)
        p = e.run(*e.args(arms="R"))
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        w = e.d / "w"
        self.assertFalse((w / "ans_d7_C.jsonl").exists())
        self.assertTrue((w / "reading_d7/NO_BUNDLE_single_arm.txt").exists())
        p = e.run(*e.args("--phase", "after-reading", arms="R"))
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertIn("B2 not measured; B3 not measured", p.stdout)
        self.assertIn("C is not an arm of this run", next(w.glob("M7_RELEASE_TEST_from_stub_data.md")).read_text())

    def test_a_failing_h6_answer_leaks_no_item_text_and_stops_the_chain(self):
        e = Env()
        self.addCleanup(e.cleanup)
        p = e.run(*e.args(sets="h6", arms="R"), extra_env={"M7_STUB_FAIL": CANARY})
        out = p.stdout + p.stderr
        self.assertEqual(p.returncode, 1, out)
        self.assertNotIn(CANARY, out)
        for f in e.plain_files():
            self.assertNotIn(CANARY, f.read_text(errors="replace"), f.name)
        self.assertIn("failed", (e.d / "w/logs/m7_release_test.ans_h6_R.log").read_text())

    @unittest.skipUnless((ROOT / "work/v3/chunks_s1.jsonl").exists(), "the frozen index is not in this tree (git-ignored)")
    def test_real_d7_files_in_a_stub_run(self):
        e = Env()
        self.addCleanup(e.cleanup)
        a = ["--dry-run", "--work", str(e.d / "w"), "--set", "d7", "--limit", "3", "--chunks", "work/v3/chunks_s1.jsonl"]
        p = e.run(*a, extra_env={"M7_STUB_CHUNKS": str(ROOT / "work/v3/chunks_s1.jsonl")})
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)


if __name__ == "__main__":
    unittest.main()
