"""The M7 release test chain (scripts/m7_release_test.sh, scripts/m7_release_test.py): stub dry runs end to end, the h6 hygiene, the refusals, the fixes of the review.
Offline: a stub ask, a stub judge, a stub server file; a test key in a temp dir; synthetic fixtures (a canary string stands for item text); no real hidden file, no server."""
import copy
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import eval2  # noqa: E402
import m7_release_rule as RR  # noqa: E402
import m7_release_test as T  # noqa: E402

NEEDS = unittest.skipUnless(shutil.which("openssl") and shutil.which("sh"), "openssl or sh missing")
CANARY = "zebra-canary-h6-text"
CONFIGS = ROOT / "eval/m7/release_test/configs.json"
PYBIN = str(ROOT / ".venv/bin/python") if (ROOT / ".venv/bin/python").exists() else sys.executable
DS = "DeepSeek-V4.1-Flash-1017-oQ4e-mtp"
BY = "the test"


def jl(rows):
    return "".join(json.dumps(r) + "\n" for r in rows)


def rule_sha(mode="full"):
    return RR.rule_text_sha256("R", "C", mode, BY)


class Env:
    """A temp world: key, home, fixtures (open D7 stand-in, hidden h6 stand-in, chunks, ledger). Work folders: w/d7 and w/h6."""

    def __init__(self, n_d7=4, n_h6=4, raw_h6=False):
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
        if raw_h6:   # the frozen rows lack tier, kind, family and domain (the harness adds them)
            self.h6 = [{k: v for k, v in r.items() if k not in ("tier", "kind", "family", "domain")} for r in self.h6]
        self._enc("h6_items.jsonl.enc", self.h6)
        self._enc("h6_rubrics.jsonl.enc", [dict(r, must=[dict(m, text=f"{CANARY} {m['text']}") for m in r["must"]]) for r in self.rub(self.h6)])
        self.sha = T.sha256_file(CONFIGS)
        self.ledger("full")
        self.w = self.d / "w"

    def ledger(self, mode):
        (self.d / "ledger.md").write_text(f"| 2026-11-27 | release-look-h6 | configs {self.sha} | rule {rule_sha(mode)} |\n")

    def _enc(self, name, rows):
        os.environ["EVAL_HIDDEN_KEY"] = str(self.key)
        eval2.write_jsonl(self.d / name, rows)

    def args(self, *extra, sets="d7", arms="R,C", ledger=True, mode="full", configs=None):
        a = ["--dry-run", "--work", str(self.w), "--arms", arms, "--set", sets, "--chunks", str(self.d / "chunks.jsonl"), "--h6-sessions", "2", "--h6-mode", mode, "--h6-chosen-by", BY,
             "--d7-items", str(self.d / "d7_items.jsonl"), "--d7-rubrics", str(self.d / "d7_rubrics.jsonl"),
             "--h6-items", str(self.d / "h6_items.jsonl.enc"), "--h6-rubrics", str(self.d / "h6_rubrics.jsonl.enc")]
        if ledger:
            a += ["--ledger", str(self.d / "ledger.md")]
        if configs:
            a += ["--configs", str(configs)]
        return a + list(extra)

    def run(self, *args, extra_env=None, cmd="scripts/m7_release_test.sh"):
        env = dict(self.env, **(extra_env or {}))
        return subprocess.run(["sh", cmd, *args] if cmd.endswith(".sh") else [PYBIN, cmd, *args], cwd=ROOT, env=env, capture_output=True, text=True, timeout=900)

    def cleanup(self):
        shutil.rmtree(self.d, True)

    def plain_files(self):
        return [f for f in self.d.rglob("*") if f.is_file() and not f.name.endswith(".enc") and f.name not in ("key",)]

    def amended_configs(self, mutate):
        c = json.loads(CONFIGS.read_text())
        mutate(c)
        p = self.d / "configs2.json"
        p.write_text(json.dumps(c))
        return p


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
        argv = T.ask_argv(c, "-Q?", "py")
        self.assertEqual(argv[:5], ["py", "-m", "kafka_sre_rag", "ask", "--backend"])
        self.assertEqual(argv[-2:], ["--", "-Q?"])                                  # the question after `--`
        v = copy.deepcopy(cfgs)
        v["R2"] = copy.deepcopy(v["R"])
        v["R2"]["ask_command"] = v["R2"]["ask_command"].replace("--lookup-cards", "--lookup-cards --lookup-kb entries.json")
        self.assertEqual(T.arm_names(v), ["R", "C", "R2"])
        self.assertIn("--lookup-kb", T.arm_info(v, "R2")["flags"])
        self.assertEqual(T.order_arms(v, ["C", "R2", "R"], "R"), ["R2", "R", "C"])  # all R arms before C
        with self.assertRaises(T.Refused):
            T.arm_info(cfgs, "X")
        bad = copy.deepcopy(cfgs)
        bad["R"]["ask_command"] = bad["R"]["ask_command"].replace(" --json", "")
        with self.assertRaises(T.Refused):
            T.arm_info(bad, "R")

    def test_the_runtime_files_of_configs_are_compared_before_an_arm(self):
        e = Env()
        self.addCleanup(e.cleanup)
        p = e.run("runtime-check", "--arm", "R", cmd="scripts/m7_release_test.py")
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        def m(c):
            c["R"]["runtime_files_sha256"]["kafka_sre_rag/pipeline.py"] = "0" * 64
        p = e.run("runtime-check", "--arm", "R", "--configs", str(e.amended_configs(m)), cmd="scripts/m7_release_test.py")
        self.assertEqual(p.returncode, 2)
        self.assertIn("pipeline.py", p.stderr)
        def m2(c):
            c["R"]["lookup_cards"]["module_sha256"]["kafka_sre_rag/kb_lookup.py"] = "0" * 64
        p = e.run("runtime-check", "--arm", "R", "--configs", str(e.amended_configs(m2)), cmd="scripts/m7_release_test.py")
        self.assertEqual(p.returncode, 2)
        self.assertIn("kb_lookup.py", p.stderr)
        # the chain refuses before any answer
        p = e.run(*e.args("--limit", "2", configs=e.amended_configs(m)))
        self.assertEqual(p.returncode, 2, p.stdout + p.stderr)
        self.assertFalse(list((e.w / "d7").glob("ans_*")))


@NEEDS
class Refusals(unittest.TestCase):
    def setUp(self):
        self.e = Env()
        self.addCleanup(self.e.cleanup)

    def test_ledger_needs_the_token_the_configs_hash_and_the_rule_text_hash(self):
        e = self.e
        rs = rule_sha("full")
        for text in ("", "release-look-h6 without the hashes\n", f"{e.sha} {rs} but no token\n", f"release-look-h6 {e.sha}\n", f"release-look-h6 {rs}\n"):
            (e.d / "ledger.md").write_text(text)
            p = e.run(*e.args(sets="h6"))
            self.assertEqual(p.returncode, 2, p.stderr)
            self.assertIn("release-look-h6", p.stderr)
            self.assertFalse(list((e.w / "h6").glob("ans_*")))
        e.ledger("sample")           # a row for another rule text (mode sample) does not open mode full
        self.assertEqual(e.run(*e.args(sets="h6")).returncode, 2)
        p = e.run(*e.args(sets="h6", ledger=False))      # the default ledger path does not exist in a temp world
        self.assertEqual(p.returncode, 2)
        # the python answer command checks it as well, before the first item
        p = e.run("answer", "--arm", "R", "--set", "h6", "--items", str(e.d / "h6_items.jsonl.enc"), "--out", str(e.d / "a.jsonl.enc"), "--stub", "--ledger", str(e.d / "ledger.md"),
                  "--rule-sha256", rs, cmd="scripts/m7_release_test.py")
        self.assertEqual(p.returncode, 2)
        self.assertFalse((e.d / "a.jsonl.enc").exists())
        e.ledger("full")
        self.assertTrue(T.ledger_ok(e.d / "ledger.md", e.sha, rs))

    def test_an_amended_configs_file_is_accepted_when_every_answered_config_is_byte_identical_and_logged(self):
        e = self.e
        p = e.run(*e.args("--limit", "2"))
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        def other(c):
            c["fixed"] = "2026-11-27"
            c["R2"] = copy.deepcopy(c["R"])               # a new entry: not answered here, so not compared
        p = e.run(*e.args("--limit", "2", configs=e.amended_configs(other)))
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertIn("logged state changed", p.stdout)
        hist = json.loads((e.w / "d7/MANIFEST.json").read_text())["history"]
        self.assertTrue(any("configs_sha256" in h["event"] for h in hist))
        def answered(c):
            c["R"]["ask_command"] = c["R"]["ask_command"].replace("--lookup-cards", "--lookup-cards --lookup-kb x.json")
        p = e.run(*e.args("--limit", "2", configs=e.amended_configs(answered)))
        self.assertEqual(p.returncode, 2)
        self.assertIn("no longer byte-identical", p.stderr)

    def test_manifest_arms_can_be_added_before_the_bundles_stage_and_helper_changes_are_logged_not_refused(self):
        e = self.e
        wd = e.d / "mf"
        base = ["manifest", "--work", str(wd), "--set", "d7", "--candidate", "R", "--control", "C", "--pin", str(e.d / "d7_items.jsonl"), "--log-pin", str(e.d / "helper.py")]
        (e.d / "helper.py").write_text("v1")
        p = e.run(*base, "--arms", "R", cmd="scripts/m7_release_test.py")
        self.assertEqual(p.returncode, 0, p.stderr)
        (e.d / "helper.py").write_text("v2")                                 # a helper fix after the first start: logged, not refused
        p = e.run(*base, "--arms", "R,C", cmd="scripts/m7_release_test.py")
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertIn("arm C added", p.stdout)
        self.assertIn("helper.py", p.stdout)
        self.assertEqual(sorted(json.loads((wd / "MANIFEST.json").read_text())["arms"]), ["C", "R"])
        (e.d / "d7_items.jsonl").write_text((e.d / "d7_items.jsonl").read_text() + "\n")   # a set's items are strict
        self.assertEqual(e.run(*base, "--arms", "R,C", cmd="scripts/m7_release_test.py").returncode, 2)
        (e.d / "d7_items.jsonl").write_text(jl(e.d7))
        (wd / "stage_bundle.done").write_text("{}")
        def add(c):
            c["R2"] = copy.deepcopy(c["R"])
        p = e.run(*base, "--arms", "R,C,R2", "--configs", str(e.amended_configs(add)), cmd="scripts/m7_release_test.py")
        self.assertEqual(p.returncode, 2)
        self.assertIn("reading bundles were already made", p.stderr)

    def test_w2_foreign_model_is_never_unloaded_and_w16_is_only_the_deepseek_build(self):
        e = self.e
        stub = e.d / "stub.json"
        e.run("stub-status", "--file", str(stub), "--init", "--loaded", "Foreign-Model", cmd="scripts/m7_kb2_rule.py")
        p = e.run("server", "gate", "--work", str(e.d), "--need", "gemma-4-31b-it-4bit", "--stub-file", str(stub), cmd="scripts/m7_release_test.py")
        self.assertEqual(p.returncode, 2)
        self.assertIn("Foreign-Model", p.stderr)
        self.assertIn("another client", p.stderr)
        self.assertIn("Foreign-Model", json.loads(stub.read_text())["loaded_models"])
        p = e.run(*e.args("--limit", "2", "--stub-loaded", "Foreign-Model"))
        self.assertEqual(p.returncode, 2, p.stdout + p.stderr)
        self.assertIn("REFUSED", p.stderr)
        self.assertFalse(list((e.w / "d7").glob("ans_*")))
        self.assertIn("Foreign-Model", json.loads((e.w / "stub_server.json").read_text())["loaded_models"])
        e2 = Env()
        self.addCleanup(e2.cleanup)
        p = e2.run(*e2.args("--limit", "2", "--arms", "R", "--stub-loaded", DS))
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)

    def test_a_marker_older_than_the_models_load_is_refused_until_it_is_refreshed(self):
        """The late load: the marker is written at the gate, the server loads the model on its first request, later. Without a refresh the next gate calls the model another client's."""
        e = self.e
        stub, marker = e.d / "stub.json", e.d / "loaded_gemma"
        marker.write_text("")
        old = time.time() - 30
        os.utime(marker, (old, old))
        stub.write_text(json.dumps({"active_requests": 0, "waiting_requests": 0, "loaded_models": ["gemma-4-31b-it-4bit"], "model_load_times": {"gemma-4-31b-it-4bit": time.time()}}))
        args = ["server", "gate", "--work", str(e.d), "--need", "Qwen3.8-27B-4bit", "--stub-file", str(stub), "--marker", f"gemma-4-31b-it-4bit={marker}"]
        p = e.run(*args, cmd="scripts/m7_release_test.py")
        self.assertEqual(p.returncode, 2, p.stdout + p.stderr)
        self.assertIn("another client", p.stderr)
        marker.touch()                                                            # the refresh after the job
        p = e.run(*args, cmd="scripts/m7_release_test.py")
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertIn("UNLOAD gemma-4-31b-it-4bit", p.stdout)                    # only what the gate prints is unloaded

    def test_get_status_merges_the_last_access_of_the_loaded_models_and_survives_a_failing_admin_call(self):
        import types
        from unittest import mock
        a = types.SimpleNamespace(stub_file=None)
        status = json.dumps({"active_requests": 0, "waiting_requests": 0, "loaded_models": ["m1", "m2"]})
        admin = json.dumps({"models": [{"id": "m1", "loaded": True, "is_loading": False, "last_access": 1700000000.5},
                                       {"id": "m2", "loaded": True, "is_loading": False, "last_access": None},
                                       {"id": "m3", "loaded": False, "is_loading": False, "last_access": 1.0}]})
        def fake(method, path, timeout=30):
            self.assertEqual(method, "GET")
            return 200, status if path == "/api/status" else admin
        with mock.patch.object(T, "http", fake):
            st = T.get_status(a)
        self.assertEqual(st["model_last_access"], {"m1": 1700000000.5})            # numbers of loaded models only
        self.assertNotIn("model_load_times", st)
        def failing(method, path, timeout=30):
            if path == "/api/status":
                return 200, status
            raise OSError("admin down")
        with mock.patch.object(T, "http", failing):
            st = T.get_status(a)
        self.assertNotIn("model_last_access", st)                                   # left out, the status still read
        self.assertEqual(st["loaded_models"], ["m1", "m2"])
        def odd(method, path, timeout=30):
            return 200, status if path == "/api/status" else "[1, 2]"
        with mock.patch.object(T, "http", odd):
            self.assertNotIn("model_last_access", T.get_status(a))

    def test_a_dry_run_whose_server_reports_no_load_times_swaps_models_and_reaches_the_judge(self):
        e = Env()
        self.addCleanup(e.cleanup)
        p = e.run(*e.args("--limit", "2", "--stub-no-load-times"))
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertTrue(list((e.w / "d7").glob("stage_judge_*.done")))
        stub = json.loads((e.w / "stub_server.json").read_text())
        self.assertTrue(stub.get("no_load_times"))
        shown = json.loads(e.run("stub-status", "--file", str(e.w / "stub_server.json"), cmd="scripts/m7_kb2_rule.py").stdout)
        self.assertNotIn("model_load_times", shown)
        self.assertIn("model_last_access", shown)
        self.assertNotIn("gemma-4-31b-it-4bit", stub["loaded_models"])             # R's model was unloaded at the swap (without a load time, by its last access)
        self.assertNotIn("gemma-4-31b-it-4bit", stub["model_last_access"])

    def test_a_dry_run_never_uses_the_real_hidden_files_and_the_keys_resolver_is_stub_only(self):
        e = self.e
        p = e.run("check-inputs", "--set", "h6", "--items", str(ROOT / "eval/hidden/tier_b_h6_items_final.jsonl.enc"), "--rubrics", str(ROOT / "eval/hidden/tier_b_h6_rubrics_final.jsonl.enc"),
                  "--stub", cmd="scripts/m7_release_test.py")
        self.assertEqual(p.returncode, 2)
        self.assertIn("eval/hidden", p.stderr)
        p = e.run("packs", "--set", "d7", "--arm", "R", "--items", str(e.d / "d7_items.jsonl"), "--answers", str(e.d / "x.jsonl"), "--resolver", "keys", cmd="scripts/m7_release_test.py")
        self.assertEqual(p.returncode, 2)

    def test_an_exit_by_set_e_leaves_a_failed_marker(self):
        e = self.e
        p = e.run(*e.args("--limit", "2", "--report", "/System/Volumes/nope/x/report.md"))
        self.assertNotEqual(p.returncode, 0)
        self.assertIn("exited with status", (e.w / "logs/m7_release_test.failed").read_text())

    def test_h6_verdicts_and_key_are_accepted_as_enc_only_and_errors_name_the_class_only(self):
        e = self.e
        (e.d / "v.json").write_text("{}")
        (e.d / "k.json").write_text("{}")
        common = ["measures", "--set", "h6", "--arms", "R", "--items", str(e.d / "h6_items.jsonl.enc"), "--rubrics", str(e.d / "h6_rubrics.jsonl.enc"), "--answers", f"R={e.d / 'a.enc'}",
                  "--out", str(e.d / "m.json.enc")]
        p = e.run(*common, "--verdicts", str(e.d / "v.json"), "--pack-key", str(e.d / "k.json"), cmd="scripts/m7_release_test.py")
        self.assertEqual(p.returncode, 2)
        self.assertIn(".enc files only", p.stderr)
        self.assertFalse((e.d / "m.json.enc").exists())
        (e.d / "bad.enc").write_text("not encrypted, mentions " + CANARY)
        p = e.run(*common, "--judge", str(e.d / "bad.enc"), cmd="scripts/m7_release_test.py")
        self.assertEqual(p.returncode, 1)
        self.assertIn("EvalError", p.stderr)
        self.assertNotIn("not an eval2", p.stderr)
        self.assertNotIn(CANARY, p.stdout + p.stderr)


@NEEDS
class Fixes(unittest.TestCase):
    def test_a_pack_that_does_not_match_stops_the_chain_keeps_the_answers_and_is_not_an_answer_error(self):
        e = Env()
        self.addCleanup(e.cleanup)
        p = e.run(*e.args(arms="R"), extra_env={"M7_STUB_BAD_SOURCE": "open question 2"})
        self.assertEqual(p.returncode, 1, p.stdout + p.stderr)
        self.assertIn("packs of R on d7", p.stdout)
        rows = [json.loads(l) for l in (e.w / "d7/ans_d7_R.jsonl").read_text().splitlines()]
        self.assertEqual(len(rows), 4)
        self.assertEqual([r["id"] for r in rows if r.get("pack_failed")], ["d7-02"])
        self.assertTrue(all(not r.get("error") for r in rows))                       # recorded as a failed pack, not as an answer error
        self.assertFalse((e.w / "d7/stage_packs_R.done").exists())
        self.assertFalse((e.w / "d7/judge_d7.jsonl").exists())                       # nothing was judged
        self.assertIn("REFUSED", (e.w / "logs/m7_release_test.packs_d7_R.log").read_text())
        # a restart refuses while a failed pack exists; the answers are not made again
        before = (e.w / "d7/ans_d7_R.jsonl").read_text()
        p = e.run(*e.args(arms="R"))
        self.assertEqual(p.returncode, 1)
        self.assertIn("failed earlier", (e.w / "logs/m7_release_test.packs_d7_R.log").read_text())
        self.assertEqual((e.w / "d7/ans_d7_R.jsonl").read_text(), before)

    def _fake_judge(self, e, fail_always):
        f = e.d / "fake_judge.py"
        f.write_text(f'''import json, subprocess, sys
real = {str(ROOT / "scripts/eval2_judge.py")!r}
args = sys.argv[1:]
rc = subprocess.call([sys.executable, real] + args)
if rc == 0 and ({fail_always!r} or "--retry-invalid" not in args):
    out = args[args.index("--out") + 1]
    rows = [json.loads(l) for l in open(out) if l.strip()]
    rows[0]["valid_reply"] = False
    open(out, "w").write("".join(json.dumps(r) + "\\n" for r in rows))
sys.exit(rc)
''')
        return f

    def test_one_invalid_judge_reply_is_retried_in_a_second_round(self):
        e = Env()
        self.addCleanup(e.cleanup)
        p = e.run(*e.args("--limit", "2"), extra_env={"M7_JUDGE_SCRIPT": str(self._fake_judge(e, False))})
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertIn("round 1: unscored rows remain", p.stdout)
        self.assertTrue((e.w / "logs/m7_release_test.judge_d7_round2.log").exists())
        rows = [json.loads(l) for l in (e.w / "d7/judge_d7.jsonl").read_text().splitlines()]
        self.assertTrue(all(r["valid_reply"] for r in rows))
        self.assertNotIn("--judge-url", (e.w / "logs/m7_release_test.judge_d7_round1.log.guard.json").read_text())

    def test_a_judge_that_stays_invalid_after_the_second_round_stops_the_chain(self):
        e = Env()
        self.addCleanup(e.cleanup)
        p = e.run(*e.args("--limit", "2"), extra_env={"M7_JUDGE_SCRIPT": str(self._fake_judge(e, True))})
        self.assertEqual(p.returncode, 1)
        self.assertIn("not whole after the second round", p.stdout)
        self.assertFalse(list((e.w / "d7").glob("stage_judge_*.done")))

    def test_the_late_load_does_not_stop_the_chain_because_the_marker_is_refreshed_after_every_job(self):
        e = Env()
        self.addCleanup(e.cleanup)
        p = e.run(*e.args("--limit", "2", sets="d7,h6"), extra_env={"M7_STUB_LOAD_DELAY": "2"})
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertTrue(list((e.w / "d7").glob("stage_judge_*.done")) and list((e.w / "h6").glob("stage_judge_*.done")))


@NEEDS
class LookupRow(unittest.TestCase):
    def _row(self, lc):
        out = json.dumps({"text": "an answer", "lookup_cards": lc})
        done = subprocess.CompletedProcess([], 0, stdout=out + "\n", stderr="")
        orig_run, orig_argv = T.subprocess.run, T.ask_argv
        T.subprocess.run, T.ask_argv = (lambda *a, **k: done), (lambda *a, **k: ["x"])
        try:
            row, err = T.run_ask({}, "q", "py", False, {})
        finally:
            T.subprocess.run, T.ask_argv = orig_run, orig_argv
        self.assertIsNone(err)
        return row["lookup"]

    def test_the_row_keeps_the_runtimes_recorded_catalogue_hash_under_catalogue_sha256(self):
        lk = self._row({"cards_found": 3, "cards": 2, "chars": 10, "seconds": 0.5, "catalogue_recorded_sha256": "abc"})
        self.assertEqual(lk["catalogue_sha256"], "abc")
        self.assertEqual((lk["cards_found"], lk["cards"], lk["chars"]), (3, 2, 10))
        self.assertEqual(self._row({"cards_found": 1, "catalogue_sha256": "old"})["catalogue_sha256"], "old")


class Adjudication(unittest.TestCase):
    def test_h_counts_the_harm_the_sre_finds_in_unflagged_answers_and_in_a_full_read(self):
        d = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, d, True)
        key = {"pack_sha256": "x", "denominators": {"d7|R": 6}, "full_sets": ["d7"], "items": {
            "p1": {"answer_id": "d7-R-a", "config": "R", "set": "d7", "kind": "flag"},
            "p2": {"answer_id": "d7-R-b", "config": "R", "set": "d7", "kind": "sample"},
            "p3": {"answer_id": "d7-R-c", "config": "R", "set": "d7", "kind": "read"},
            "p4": {"answer_id": "d7-R-d", "config": "R", "set": "d7", "kind": "sample"}}}
        h = lambda x: {"harmful": x, "severity": "none", "note": "", "quote": ""}
        ver = {"schema": "m7-adjudication-verdicts-1", "pack_sha256": "x", "verdicts": {"p1": {"stage1": h(True), "stage2": dict(h(True), decision="confirmed")}, "p2": {"stage1": h(True)},
                                                                                     "p3": {"stage1": h(True)}, "p4": {"stage1": h(False)}}}
        (d / "k.json").write_text(json.dumps(key))
        (d / "v.json").write_text(json.dumps(ver))
        tot, up = T.adjudication_of(str(d / "v.json"), str(d / "k.json"), "d7")
        r = tot["R"]
        self.assertEqual((r["H"], r["sample_harm"], r["samples_read"], r["pending"], r["full_read"]), (3, 1, 2, 0, True))
        self.assertEqual(sorted(up["R"]), ["d7-R-a", "d7-R-b", "d7-R-c"])


@NEEDS
class HarnessItemsH6(unittest.TestCase):
    """G3: the h6 rows lack tier, kind, family and domain; harness-items handles .enc (counts only) and plain files stay byte-identical."""

    def _run(self, *a, key):
        return subprocess.run([PYBIN, str(ROOT / "scripts/m5_g_rule.py"), "harness-items", *a], capture_output=True, text=True, env={"PATH": os.environ["PATH"], "EVAL_HIDDEN_KEY": str(key)})

    def test_enc_round_trip_adds_domain_keeps_the_rest_and_prints_the_count_only(self):
        e = Env(raw_h6=True)
        self.addCleanup(e.cleanup)
        out = e.d / "h.jsonl.enc"
        p = self._run("--in", str(e.d / "h6_items.jsonl.enc"), "--out", str(out), "--domain", "h6", key=e.key)
        self.assertEqual(p.returncode, 0, p.stderr)
        self.assertNotIn(CANARY, p.stdout + p.stderr)
        self.assertRegex(p.stdout, r"4 harness items")
        self.assertNotIn(CANARY.encode(), out.read_bytes())
        rows = eval2.read_jsonl(out)
        self.assertEqual(len(rows), 4)
        for r, src in zip(rows, e.h6):
            self.assertEqual((r["tier"], r["kind"], r["domain"], r["family"]), ("B", "open", "h6", src["id"]))
            self.assertTrue(all(r[k] == v for k, v in src.items()))
        self.assertEqual(len(eval2.read_items(out)), 4)
        p = self._run("--in", str(e.d / "h6_items.jsonl.enc"), "--out", str(e.d / "plain.jsonl"), "--domain", "h6", key=e.key)   # a mix is refused, nothing written
        self.assertNotEqual(p.returncode, 0)
        self.assertFalse((e.d / "plain.jsonl").exists())

    def test_d7_plain_output_is_byte_identical_to_the_old_code(self):
        e = Env()
        self.addCleanup(e.cleanup)
        old = e.d / "old_rule.py"
        old.write_text(subprocess.run(["git", "show", "bf42ec6:scripts/m5_g_rule.py"], capture_output=True, text=True, cwd=ROOT, check=True).stdout)
        shutil.copy(ROOT / "scripts/m5_g_rule.py", e.d / "new_rule.py")
        outs = {}
        for name in ("old", "new"):
            o = e.d / f"{name}.jsonl"
            p = subprocess.run([PYBIN, str(e.d / f"{name}_rule.py"), "harness-items", "--in", str(e.d / "d7_items.jsonl"), "--out", str(o), "--domain", "d7"], capture_output=True, text=True,
                               env={"PATH": os.environ["PATH"], "PYTHONPATH": str(ROOT / "scripts")})
            self.assertEqual(p.returncode, 0, p.stderr)
            outs[name] = (o.read_bytes(), p.stdout.replace(str(o), "X"))
        self.assertEqual(outs["old"], outs["new"])

    def test_h6_dry_run_with_raw_rows_reaches_answers_judge_and_bundles_and_pins_both_files(self):
        e = Env(raw_h6=True)
        self.addCleanup(e.cleanup)
        p = e.run(*e.args(sets="h6"))
        out = p.stdout + p.stderr
        self.assertEqual(p.returncode, 0, out)
        w = e.w / "h6"
        self.assertTrue((w / "items_h6_h.jsonl.enc").exists())
        for n in ("ans_h6_R.jsonl.enc", "ans_h6_C.jsonl.enc", "judge_h6.jsonl.enc"):
            self.assertTrue((w / n).exists(), n)
        self.assertTrue(list(w.glob("stage_judge_*.done")))
        self.assertTrue((w / "reading").is_dir() and any((w / "reading").iterdir()))
        man = json.loads((w / "MANIFEST.json").read_text())
        pins = man["strict"]["set_pins"] if "strict" in man else man["set_pins"]
        self.assertEqual(sorted(Path(k).name for k in pins), ["h6_items.jsonl.enc", "h6_rubrics.jsonl.enc", "items_h6_h.jsonl.enc"])
        self.assertNotIn(CANARY, out)
        self.assertNotIn("h6-0", out)
        for f in e.plain_files():
            self.assertNotIn(CANARY, f.read_text(errors="replace"), f.name)
        before = (w / "items_h6_h.jsonl.enc").read_bytes()      # a second start keeps the harness copy (the manifest pins it)
        p = e.run(*e.args(sets="h6"))
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertEqual((w / "items_h6_h.jsonl.enc").read_bytes(), before)


class Chain(unittest.TestCase):
    def read_h6(self, e, hdir):
        hk = json.loads(eval2.decrypt_bytes((hdir / "blind_key.json.enc").read_bytes()))
        for b, ns in hk["bundles"].items():
            rows_ = [{"answer": n, "id": n.rsplit("-", 1)[0], "kind": "clean", "quote": "", "point": "", "supported": False, "minor": False, "source": "", "note": ""} for n in ns]
            rows_[0] = {"answer": ns[0], "id": ns[0].rsplit("-", 1)[0], "kind": "harmful", "quote": "Stub answer", "point": "", "supported": False, "minor": False, "source": "s", "note": "n",
                        "consequence": "data loss"}
            eval2.write_jsonl(hdir / f"findings_{b}.jsonl.enc", rows_)

    def build_h6_pack(self, e, full):
        pi = e.w / "h6/pack_input"
        frozen = e.d / "frozen.json"
        frozen.write_text(json.dumps({"index_path": "x", "source_commits": {}}))
        out = e.d / ("pack_h6_full" if full else "pack_h6_sample")
        html = e.d / ("owner_h6_full.html" if full else "owner_h6_sample.html")
        cmd = [PYBIN, "scripts/m7_adjudication_pack.py", "build", "--answers", str(pi / "answers.jsonl.enc"), "--scenarios", str(pi / "scenarios.jsonl.enc"), "--flags", str(pi / "flags.jsonl.enc"),
               "--out", str(out), "--configs", str(CONFIGS), "--shuffle-seed", "20261127", "--sample-seed", "20261128", "--sample-size", "2", "--index-frozen", str(frozen),
               "--chunks", str(e.d / "chunks.jsonl"), "--sealed", "--html-out", str(html)]
        if full:
            cmd += ["--full-sets", "h6"]
        b = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, env=e.env)
        self.assertEqual(b.returncode, 0, b.stdout + b.stderr)
        self.assertNotIn(CANARY, b.stdout + b.stderr)                       # --sealed prints counts only
        self.assertNotIn(str(html), b.stdout)                              # the Owner's copy is never printed
        self.assertTrue((out / "key.json.enc").exists())
        self.assertFalse((out / "key.json").exists())                      # the key of h6 is encrypted
        self.assertIn(CANARY, html.read_text())                            # the one plain output allowed: the Owner's reading copy
        return out, json.loads((out / "manifest.json").read_text())

    def seal_verdicts(self, e, out, name):
        key = json.loads(eval2.decrypt_bytes((out / "key.json.enc").read_bytes()))
        verdicts = {}
        for pid, it in key["items"].items():
            s1 = {"harmful": False, "severity": "none", "note": "", "quote": ""}
            verdicts[pid] = {"stage1": s1} if it["kind"] != "flag" else {"stage1": s1, "stage2": {"harmful": False, "severity": "none", "note": "", "decision": "revised"}}
        plain = e.d / (name + ".json")
        plain.write_text(json.dumps({"schema": "m7-adjudication-verdicts-1", "pack_sha256": key["pack_sha256"], "verdicts": verdicts}))
        b = subprocess.run([PYBIN, "scripts/m7_adjudication_pack.py", "seal", "--in", str(plain), "--out", str(e.d / (name + ".json.enc"))], cwd=ROOT, capture_output=True, text=True, env=e.env)
        self.assertEqual(b.returncode, 0, b.stdout + b.stderr)
        plain.unlink()
        return e.d / (name + ".json.enc"), key

    def test_d7_and_h6_end_to_end_with_r_and_c_in_mode_full(self):
        e = Env()
        self.addCleanup(e.cleanup)
        p = e.run(*e.args(sets="d7,h6"))
        out = p.stdout + p.stderr
        self.assertEqual(p.returncode, 0, out)
        w = e.w
        # one work folder per set; the header (with the mode and who chose it) and the manifest come before the first answer
        for s in ("d7", "h6"):
            self.assertTrue((w / s / "stage_header.done").exists())
            man = json.loads((w / s / "MANIFEST.json").read_text())
            self.assertEqual(man["logged"]["configs_sha256"], e.sha)
            self.assertEqual(sorted(man["arms"]), ["C", "R"])
            self.assertEqual(man["rule_text_sha256"], rule_sha("full"))
            self.assertEqual(man["options"]["judge_concurrency"], "1")
        head = (w / "h6/report_header.md").read_text()
        for needle in ("Arms: R,C", e.sha, "met, not met or not measured", "20261126", f"h6 adjudication mode: `full`**, chosen by {BY}", rule_sha("full"), "concurrency 1"):
            self.assertIn(needle, head)
        self.assertLess((w / "h6/stage_header.done").stat().st_mtime_ns, (w / "h6/ans_h6_R.jsonl.enc").stat().st_mtime_ns)
        rows = [json.loads(l) for l in (w / "d7/ans_d7_R.jsonl").read_text().splitlines()]
        self.assertEqual(len(rows), 4)
        self.assertTrue(all(r["pack"] and r["pack_check"] == "exact" and r["seconds"]["total"] > 0 and r["arm"] == "R" and "lookup" in r for r in rows))
        self.assertNotIn("lookup", json.loads((w / "d7/ans_d7_C.jsonl").read_text().splitlines()[0]))
        self.assertTrue(len(list((w / "d7/reading/export_second_reader").glob("*.md"))) >= 4)
        self.assertFalse((w / "d7/reading/export_second_reader/KEY.json").exists())
        # judge: concurrency 1 and no address on a command line
        self.assertNotIn("--judge-url", out)
        # h6 hygiene
        for f in [w / "h6/ans_h6_R.jsonl.enc", w / "h6/ans_h6_C.jsonl.enc", w / "h6/judge_h6.jsonl.enc"]:
            self.assertTrue(f.exists(), f.name)
            self.assertNotIn(CANARY.encode(), f.read_bytes())
        self.assertNotIn(CANARY, out)
        for f in e.plain_files():
            self.assertNotIn(CANARY, f.read_text(errors="replace"), f.name)
        self.assertNotIn("h6-0", out)
        log = (w / "logs/m7_release_test.ans_h6_R.log").read_text()
        self.assertRegex(log, r"\[answer R h6\] 1/4 ok")
        self.assertNotIn("h6-0", log)
        self.assertNotIn("h6-0", (w / "logs/m7_release_test.judge_h6_round1.log").read_text())
        # reading
        key = json.loads((w / "d7/reading/KEY.json").read_text())
        names = sorted(key["aid"])
        row = lambda n, kind, **k: dict({"answer": n, "id": n.rsplit("-", 1)[0], "kind": kind, "quote": "Stub answer", "minor": False, "source": "f.md:1", "note": "n"}, **k)
        (e.d / "f_claude.jsonl").write_text(jl([row(names[0], "harmful", consequence="data loss"), row(names[1], "wrong")]))
        (e.d / "f_codex.jsonl").write_text(jl([row(names[0], "harmful", consequence="outage of the cluster")]))
        hdir = w / "h6/reading"
        self.read_h6(e, hdir)
        a = e.args("--phase", "after-reading", "--h6-dir", str(hdir), "--findings-d7", f"claude={e.d / 'f_claude.jsonl'}", f"codex={e.d / 'f_codex.jsonl'}", sets="d7,h6")
        p = e.run(*a)
        out2 = p.stdout + p.stderr
        self.assertEqual(p.returncode, 0, out2)
        self.assertIn("BARS: B1 not measured; B2 not measured", out2)
        self.assertNotIn(CANARY, out2)
        text = next(w.glob("M7_RELEASE_TEST_from_stub_data.md")).read_text()
        self.assertIn("hidden: totals only", text)
        self.assertNotIn(CANARY, text)
        # d7 pack input: it builds with configuration names from configs.json; verdicts feed the report
        pi = w / "d7/pack_input"
        flags = [json.loads(l) for l in (pi / "flags.jsonl").read_text().splitlines()]
        self.assertEqual({f["kind"] for f in flags}, {"harm", "material_error"})
        self.assertEqual({f["reader"] for f in flags if f["kind"] == "harm"}, {"claude", "codex"})
        self.assertIn("--configs", p.stdout)
        frozen = e.d / "frozen.json"
        frozen.write_text(json.dumps({"index_path": "x", "source_commits": {}}))
        pk = e.d / "pack"
        b = subprocess.run([PYBIN, "scripts/m7_adjudication_pack.py", "build", "--answers", str(pi / "answers.jsonl"), "--scenarios", str(pi / "scenarios.jsonl"), "--flags", str(pi / "flags.jsonl"),
                            "--out", str(pk), "--configs", str(CONFIGS), "--shuffle-seed", "20261127", "--sample-seed", "20261128", "--index-frozen", str(frozen), "--chunks", str(e.d / "chunks.jsonl")],
                           cwd=ROOT, capture_output=True, text=True, env=e.env)
        self.assertEqual(b.returncode, 0, b.stdout + b.stderr)
        pkey = json.loads((pk / "key.json").read_text())
        verdicts = {}
        for pid, it in pkey["items"].items():
            s1 = {"harmful": False, "severity": "none", "note": "", "quote": ""}
            verdicts[pid] = {"stage1": s1} if it["kind"] == "sample" else {"stage1": s1, "stage2": {"harmful": False, "severity": "none", "note": "", "decision": "revised"}}
        (e.d / "verdicts.json").write_text(json.dumps({"schema": "m7-adjudication-verdicts-1", "pack_sha256": pkey["pack_sha256"], "verdicts": verdicts}))
        # h6 in mode full: the pack input asks for every answer; the sealed pack is built, sealed verdicts feed the report; plain verdicts are refused
        self.assertIn("--full-sets h6", p.stdout)
        out_h, man = self.build_h6_pack(e, True)
        self.assertGreater(man["counts"]["reads"], 0)                          # the unflagged answers beyond the sample are read items
        self.assertEqual(man["counts"]["items"], 8)                            # every h6 answer of R and C
        outs, _ = self.build_h6_pack(e, False)
        self.assertLess(json.loads((outs / "manifest.json").read_text())["counts"]["items"], 8)   # mode sample: flags plus the sample only
        venc, hkey = self.seal_verdicts(e, out_h, "v_h6")
        a = e.args("--phase", "after-reading", "--h6-dir", str(hdir), "--verdicts-d7", str(e.d / "verdicts.json"), "--pack-key-d7", str(pk / "key.json"),
                   "--verdicts-h6", str(venc), "--pack-key-h6", str(out_h / "key.json.enc"), "--findings-d7", f"claude={e.d / 'f_claude.jsonl'}", f"codex={e.d / 'f_codex.jsonl'}", sets="d7,h6")
        p = e.run(*a)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertNotIn(CANARY, p.stdout + p.stderr)
        text = next(w.glob("M7_RELEASE_TEST_from_stub_data.md")).read_text()
        self.assertIn("0/4 = 0.0 %", text)
        self.assertIn("the SRE read every answer of this set", text)            # h6 read in full
        self.assertIn("### Bars", text)
        self.assertNotIn(CANARY, text)
        # plain h6 verdicts are refused by the chain's measures step
        (e.d / "plain_v.json").write_text("{}")
        a = e.args("--phase", "after-reading", "--h6-dir", str(hdir), "--verdicts-h6", str(e.d / "plain_v.json"), "--pack-key-h6", str(out_h / "key.json.enc"), sets="h6")
        p = e.run(*a)
        self.assertNotEqual(p.returncode, 0)

    def test_h6_in_mode_sample_has_its_own_rule_text_and_a_pack_without_the_full_read(self):
        e = Env()
        self.addCleanup(e.cleanup)
        e.ledger("full")
        p = e.run(*e.args(sets="h6", mode="sample"))
        self.assertEqual(p.returncode, 2, p.stderr)          # the ledger row is for the rule text of mode full
        e.ledger("sample")
        p = e.run(*e.args(sets="h6", mode="sample", arms="R"))
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertIn("h6 adjudication mode: `sample**".replace("`sample**", "`sample`"), (e.w / "h6/report_header.md").read_text())
        self.read_h6(e, e.w / "h6/reading")
        p = e.run(*e.args("--phase", "after-reading", "--h6-dir", str(e.w / "h6/reading"), sets="h6", mode="sample", arms="R"))
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertNotIn("--full-sets", p.stdout)
        p = e.run(*e.args("--phase", "after-reading", "--full-sets", "h6", "--h6-dir", str(e.w / "h6/reading"), sets="h6", mode="sample", arms="R"))   # B4's re-read input
        self.assertIn("--full-sets h6", p.stdout)

    def test_r_alone_has_no_comparison_and_the_bars_say_so(self):
        e = Env()
        self.addCleanup(e.cleanup)
        p = e.run(*e.args(arms="R"))
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertFalse((e.w / "d7/ans_d7_C.jsonl").exists())
        self.assertTrue((e.w / "d7/reading/NO_BUNDLE_single_arm.txt").exists())
        p = e.run(*e.args("--phase", "after-reading", arms="R"))
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertIn("B2 not measured; B3 not measured", p.stdout)
        self.assertIn("not an arm of the run", next(e.w.glob("M7_RELEASE_TEST_from_stub_data.md")).read_text())

    def test_a_failing_h6_answer_leaks_no_item_text_and_stops_the_chain(self):
        e = Env()
        self.addCleanup(e.cleanup)
        p = e.run(*e.args(sets="h6", arms="R"), extra_env={"M7_STUB_FAIL": CANARY})
        out = p.stdout + p.stderr
        self.assertEqual(p.returncode, 1, out)
        self.assertNotIn(CANARY, out)
        for f in e.plain_files():
            self.assertNotIn(CANARY, f.read_text(errors="replace"), f.name)
        self.assertIn("failed", (e.w / "logs/m7_release_test.ans_h6_R.log").read_text())

    @unittest.skipUnless((ROOT / "work/v3/chunks_s1.jsonl").exists(), "the frozen index is not in this tree (git-ignored)")
    def test_real_d7_files_in_a_stub_run(self):
        e = Env()
        self.addCleanup(e.cleanup)
        a = ["--dry-run", "--work", str(e.w), "--set", "d7", "--limit", "3", "--chunks", "work/v3/chunks_s1.jsonl"]
        p = e.run(*a, extra_env={"M7_STUB_CHUNKS": str(ROOT / "work/v3/chunks_s1.jsonl")})
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)


if __name__ == "__main__":
    unittest.main()
