"""The M7 release test chain (scripts/m7_release_test.sh, scripts/m7_release_test.py): stub dry runs end to end, the h6 hygiene, the refusals, the fixes of the review.
Offline: a stub ask, a stub judge, a stub server file; a test key in a temp dir; synthetic fixtures (a canary string stands for item text); no real hidden file, no server."""
import copy
import hashlib
import json
import re
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
# The tree on which the release test finished, before the post-measurement runtime change of W46 (the MCP merge and the entries wording). The real pins of configs.json are
# checked against the files of this commit; the v4.0.0 runtime differs from it only in files that scripts/m7_v4_bytecheck.py covers (RE's prompts and request bodies: 60/60 equal).
MEASURED_COMMIT = "a9c0cc0"
PYBIN = str(ROOT / ".venv/bin/python") if (ROOT / ".venv/bin/python").exists() else sys.executable
DS = "DeepSeek-V4.1-Flash-1017-oQ4e-mtp"
BY = "the test"


def jl(rows):
    return "".join(json.dumps(r) + "\n" for r in rows)


def git_show(path, commit=MEASURED_COMMIT):
    """The bytes of `path` at `commit`, or None when the repository has no such history (a git archive, a shallow copy)."""
    try:
        p = subprocess.run(["git", "show", f"{commit}:{path}"], cwd=ROOT, capture_output=True, timeout=60)
    except (OSError, subprocess.SubprocessError):
        return None
    return p.stdout if p.returncode == 0 else None


def working_tree_pins(cfgs):
    """A copy of the configs whose pinned runtime files and lookup modules carry the hashes of the files in this working tree. Used only as the starting state of tests of the
    chain's mechanics; the real configs.json is the measured record and is never changed."""
    cfgs = copy.deepcopy(cfgs)
    for arm in cfgs.values():
        if not isinstance(arm, dict):
            continue
        for pins in (arm.get("runtime_files_sha256"), (arm.get("lookup_cards") or {}).get("module_sha256")):
            for rel in list(pins or {}):
                if (ROOT / rel).is_file():
                    pins[rel] = T.sha256_file(ROOT / rel)
    return cfgs


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
        self.cfg = self.d / "configs_tree.json"      # the real configs with this working tree's pins: the starting state of every test of the chain's mechanics
        self.cfg.write_text(json.dumps(working_tree_pins(json.loads(CONFIGS.read_text()))))
        self.sha = T.sha256_file(self.cfg)
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
        a += ["--configs", str(configs or self.cfg)]
        return a + list(extra)

    def run(self, *args, extra_env=None, cmd="scripts/m7_release_test.sh"):
        env = dict(self.env, **(extra_env or {}))
        return subprocess.run(["sh", cmd, *args] if cmd.endswith(".sh") else [PYBIN, cmd, *args], cwd=ROOT, env=env, capture_output=True, text=True, timeout=900)

    def cleanup(self):
        shutil.rmtree(self.d, True)

    def plain_files(self):
        return [f for f in self.d.rglob("*") if f.is_file() and not f.name.endswith(".enc") and f.name not in ("key",)]

    def amended_configs(self, mutate, name="configs2.json"):
        c = json.loads(self.cfg.read_text())
        mutate(c)
        p = self.d / name
        p.write_text(json.dumps(c))
        return p


@NEEDS
class Arms(unittest.TestCase):
    def test_arms_come_from_configs_and_a_variant_entry_needs_no_code_change(self):
        cfgs = T.load_configs(CONFIGS)
        self.assertEqual(T.arm_names(cfgs), ["R", "C", "RE"])
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
        self.assertEqual(T.arm_names(v), ["R", "C", "RE", "R2"])
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
        p = e.run("runtime-check", "--arm", "R", "--configs", str(e.cfg), cmd="scripts/m7_release_test.py")
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
        base = ["manifest", "--work", str(wd), "--set", "d7", "--candidate", "R", "--control", "C", "--pin", str(e.d / "d7_items.jsonl"), "--log-pin", str(e.d / "helper.py"), "--configs", str(e.cfg)]
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
        pi = e.w / "h6/pack_input_d7-h6_R-C_reading"
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
        text = next(w.glob("M7_RELEASE_TEST_from_stub_data*.md")).read_text()
        self.assertIn("hidden: totals only", text)
        self.assertNotIn(CANARY, text)
        # d7 pack input: it builds with configuration names from configs.json; verdicts feed the report
        pi = w / "d7/pack_input_d7-h6_R-C_reading"
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
        text = next(w.glob("M7_RELEASE_TEST_from_stub_data*.md")).read_text()
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
        self.assertIn("not an arm of the run", next(e.w.glob("M7_RELEASE_TEST_from_stub_data*.md")).read_text())

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
        a = ["--dry-run", "--work", str(e.w), "--set", "d7", "--limit", "3", "--chunks", "work/v3/chunks_s1.jsonl", "--configs", str(e.cfg)]
        p = e.run(*a, extra_env={"M7_STUB_CHUNKS": str(ROOT / "work/v3/chunks_s1.jsonl")})
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)


# ---------------------------------------------------------------- W43: the arm RE (R + the fact entries)

R_SHA = "6ebd600bc5f5596cd8d89c74305fd98a4d4e9613ed8e1d69cb0b0f1c7d3ba182"     # json.dumps(configs["R"], sort_keys=True), as the manifest of the finished D7 folder holds it
C_SHA = "29909e66bcf1403665c8e932c6226448191d58d51532b15d9180dfe73fcbcd97"


def entries_fixture(env, n=8):
    """A small entries index and its record (a synthetic world: the chunks c00..c11 of the Env), and a configs file whose RE points at them."""
    from kafka_sre_rag import kb_entry
    rows = []
    for j in range(n):
        srcs = [{"id": f"c{(j * 5) % 12:02d}", "source": "docs", "path": f"raw/f{(j * 5) % 12}.md"}]
        if j % 3 == 0:
            srcs.append({"id": f"c{(j * 5 + 1) % 12:02d}", "source": "docs", "path": f"raw/f{(j * 5 + 1) % 12}.md"})
        e = {"id": f"kbe-{j:04d}", "kind": "fact", "title": f"Entry {j} about open question", "statement": f"The open question {j} has the answer {j}.", "scope": {"product": "Strimzi", "version": "1.3.1"},
             "sources": srcs, "quotes": [{"text": f"quoted line {j} of the open question"}], "applies_when": "", "prompt_version": "kbe/5", "writer": "w"}
        e["rendered"] = kb_entry.render_text(e)
        rows.append(e)
    idx = env.d / "entries_index.jsonl"
    idx.write_text("".join(json.dumps(r, sort_keys=True) + "\n" for r in rows))
    rec = env.d / "kb_entries_rec.json"
    rec.write_text(json.dumps({"schema": 1, "file": str(idx), "sha256": T.sha256_file(idx), "entries": n, "slice_sha256": "0" * 64, "writer": {"model": "w", "prompt_version": "kbe/5"},
                               "checker": {"model": "c", "prompt_version": "kbc/4"}, "created": "2026-11-27T00:00:00Z"}))

    def mutate(c):
        re_ = c["RE"]
        re_["ask_command"] = re_["ask_command"].replace("--kb-entries ", f"--kb-entries --kb-entries-record {rec} ")
        re_["entries"].update(index=str(idx), sha256=T.sha256_file(idx), record=str(rec), record_sha256=T.sha256_file(rec), entries=n)
    return env.amended_configs(mutate, "configs_re.json"), idx, rec, rows


@NEEDS
class EntriesArm(unittest.TestCase):
    def setUp(self):
        self.e = Env()
        self.addCleanup(self.e.cleanup)
        self.cfg, self.idx, self.rec, self.rows = entries_fixture(self.e)
        self.recv = self.e.d / "received"
        self.recv.mkdir()

    def args(self, *extra, **kw):
        kw.setdefault("arms", "RE,R,C")
        kw.setdefault("configs", self.cfg)
        return self.e.args("--candidate", "RE", *extra, **kw)

    def run_(self, *a, **kw):
        return self.e.run(*a, extra_env={"M7_STUB_RECEIVED": str(self.recv)}, **kw)

    def snapshot(self, d):
        return {str(f.relative_to(d)): T.sha256_file(f) for f in sorted(Path(d).rglob("*")) if f.is_file()}

    # -- the configuration
    def test_r_and_c_are_byte_identical_and_re_is_r_with_the_entries_on(self):
        c = T.load_configs(CONFIGS)
        self.assertEqual(T.sha256_text(json.dumps(c["R"], sort_keys=True)), R_SHA)       # the hash the manifest of the finished D7 folder holds
        self.assertEqual(T.sha256_text(json.dumps(c["C"], sort_keys=True)), C_SHA)
        self.assertEqual(T.config_sha(c, "R"), R_SHA)
        self.assertEqual(T.arm_names(c), ["R", "C", "RE"])
        r, re_ = c["R"], c["RE"]
        changed = sorted(k for k in set(r) | set(re_) if r.get(k) != re_.get(k))
        self.assertEqual(changed, ["ask_command", "ask_command_dry_run", "description", "entries", "name", "prompt_format", "runtime_files_note", "runtime_files_sha256"])
        self.assertEqual(re_["ask_command"].replace(" --kb-entries", ""), r["ask_command"])
        self.assertEqual({k for k in re_["runtime_files_sha256"] if k not in r["runtime_files_sha256"]},
                         {"kafka_sre_rag/kb_entries.py", "kafka_sre_rag/kb_entry.py", "kafka_sre_rag/kb_entries.json"})
        self.assertTrue(re_["entries"]["on"])
        self.assertFalse(r["entries"]["on"])
        self.assertEqual(re_["entries"]["cap"]["MAX_ITEMS"], 10)
        self.assertEqual(re_["entries"]["cap"]["MAX_CHARS"], 24000)
        self.assertEqual((re_["entries"]["entry_prompt_version"], re_["entries"]["check_prompt_version"]), ("kbe/5", "kbc/4"))
        self.assertIn("W43", c["amendments"][-1]["decision"])
        self.assertEqual(c["amendments"][-1]["unchanged"], {"R": R_SHA, "C": C_SHA})
        info = T.arm_info(c, "RE")
        self.assertTrue(info["entries"] and info["entries_record"] == "kafka_sre_rag/kb_entries.json")
        self.assertEqual(info["model"], T.arm_info(c, "R")["model"])
        self.assertEqual(info["flags"], T.arm_info(c, "R")["flags"][:-1] + ["--kb-entries", "--json"])
        self.assertFalse(T.arm_info(c, "R")["entries"])
        self.assertEqual(T.order_arms(c, ["C", "RE", "R"], "RE"), ["RE", "R", "C"])

    def test_the_ask_command_and_the_entries_block_must_agree(self):
        c = T.load_configs(self.cfg)
        bad = copy.deepcopy(c)
        bad["RE"]["entries"]["on"] = False
        with self.assertRaises(T.Refused):
            T.arm_info(bad, "RE")
        bad = copy.deepcopy(c)
        bad["R"]["entries"] = {"on": True}
        with self.assertRaises(T.Refused):
            T.arm_info(bad, "R")
        bad = copy.deepcopy(c)
        bad["RE"]["entries"]["record"] = "elsewhere.json"
        with self.assertRaises(T.Refused):
            T.arm_info(bad, "RE")

    @unittest.skipUnless(git_show("kafka_sre_rag/pipeline.py") is not None, f"the repository has no history with {MEASURED_COMMIT} (a git archive?): the measured files cannot be read")
    def test_the_real_pins_of_r_c_and_re_hold_at_the_measured_commit(self):
        cfgs = json.loads(CONFIGS.read_text())
        checked = 0
        for arm in ("R", "C", "RE"):
            pins = dict(cfgs[arm].get("runtime_files_sha256") or {})
            pins.update((cfgs[arm].get("lookup_cards") or {}).get("module_sha256") or {})
            self.assertTrue(pins, arm)
            for rel, h in sorted(pins.items()):
                blob = git_show(rel)
                self.assertIsNotNone(blob, f"{arm}: {rel} is not in {MEASURED_COMMIT}")
                self.assertEqual(hashlib.sha256(blob).hexdigest(), h, f"{arm}: {rel} at {MEASURED_COMMIT} differs from the pin of configs.json")
                checked += 1
        self.assertGreater(checked, 30)

    @unittest.skipUnless((ROOT / "work/m7/kb/entries_index.jsonl").exists(), "the entries index is not in this tree (git-ignored)")
    def test_the_real_re_entries_pins_hold_in_this_tree_with_the_runtime_file_pins_of_the_tree(self):
        # the entries index and record are data files (git-ignored); their pins are the real ones. Only the runtime file pins are replaced by this tree's hashes.
        p = self.e.run("runtime-check", "--arm", "RE", "--configs", str(self.e.cfg), cmd="scripts/m7_release_test.py")
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        p = self.e.run("arms", "--arms", "RE,R,C", "--configs", str(self.e.cfg), cmd="scripts/m7_release_test.py")
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertIn("fact entries on", p.stdout)
        from kafka_sre_rag.kb_entries import KbEntries
        self.assertEqual(len(KbEntries.load().rows), 7028)

    def test_a_changed_record_index_or_entries_module_is_refused_by_arms_and_runtime_check(self):
        e = self.e
        checks = (("runtime-check", ["--arm", "RE"]), ("arms", ["--arms", "RE"]))
        for cmd, extra in checks:
            p = e.run(cmd, *extra, "--configs", str(self.cfg), cmd="scripts/m7_release_test.py")
            self.assertEqual(p.returncode, 0, (cmd, p.stdout, p.stderr))
        cases = {
            "the entries record": lambda r: r["entries"].update(record_sha256="0" * 64),
            "the entries index": lambda r: r["entries"].update(sha256="0" * 64),
            "kb_entries.py": lambda r: r["runtime_files_sha256"].update({"kafka_sre_rag/kb_entries.py": "0" * 64}),
            "kb_entry.py": lambda r: r["runtime_files_sha256"].update({"kafka_sre_rag/kb_entry.py": "0" * 64}),
            "kb_entries.json": lambda r: r["runtime_files_sha256"].update({"kafka_sre_rag/kb_entries.json": "0" * 64}),
        }
        for what, mutate in cases.items():
            cfg = e.amended_configs(lambda c, m=mutate: m(c["RE"]))
            for cmd, extra in checks:
                p = e.run(cmd, *extra, "--configs", str(cfg), cmd="scripts/m7_release_test.py")
                self.assertEqual(p.returncode, 2, (what, cmd, p.stdout, p.stderr))
                self.assertIn(what, p.stderr)
        # the index file itself changes
        self.idx.write_text(self.idx.read_text() + "\n")
        for cmd, extra in checks:
            p = e.run(cmd, *extra, "--configs", str(self.cfg), cmd="scripts/m7_release_test.py")
            self.assertEqual(p.returncode, 2, (cmd, p.stdout, p.stderr))
            self.assertIn("entries index", p.stderr)
        # and the chain refuses before anything is answered
        p = self.run_(*self.args())
        self.assertEqual(p.returncode, 2, p.stdout + p.stderr)
        self.assertFalse(list(e.w.glob("d7/ans_*")))

    # -- the pack recovery
    def kb(self, reranker=None):
        from kafka_sre_rag.kb_entries import KbEntries
        return KbEntries.load(str(self.rec), reranker=reranker)

    def test_the_keys_resolver_checks_the_added_entries_against_the_index_and_the_sources(self):
        kb = self.kb()
        chunks = [json.loads(l) for l in (self.e.d / "chunks.jsonl").read_text().splitlines()]
        pack = chunks[:3]
        items, info = kb.merge(pack, "open question 1", "stub terms", rerank=False)
        self.assertTrue(info["added"])
        srcs = [{"source": c["source"], "title": c["title"], "path": c["path"]} for c in items]
        res = T.keys_resolver(str(self.e.d / "chunks.jsonl"), kb)
        got = res("q", "t", srcs, info)
        self.assertEqual(got[:3], [c["id"] for c in pack])
        self.assertEqual(got[3:], ["kbe:" + i for i in info["added"]])
        for bad_info in (dict(info, added=info["added"][:-1]), dict(info, added=["kbe-nope"] + info["added"][1:]), None):
            with self.assertRaises(T.Refused):
                res("q", "t", srcs, bad_info)
        with self.assertRaises(T.Refused):                       # an entry source that is not the added entry
            res("q", "t", srcs[:-1] + [dict(srcs[-1], title="Fact entry: another")], info)
        with self.assertRaises(T.Refused):                       # entries recorded for an arm without them
            T.keys_resolver(str(self.e.d / "chunks.jsonl"), None)("q", "t", srcs, info)

    def test_the_retrieval_resolver_recomputes_the_entries_merge_and_refuses_any_difference(self):
        import kafka_sre_rag.retrieval as retrieval
        chunks = [dict(json.loads(l), ce_score=1.0 - i / 10, bm25_rank=i + 1) for i, l in enumerate((self.e.d / "chunks.jsonl").read_text().splitlines())][:4]

        class Fake:
            def predict(self, pairs, batch_size=8):
                return [float(len(p[1]) % 7) + i / 100 for i, p in enumerate(pairs)]
        kb = self.kb(Fake())
        orig = retrieval.select
        retrieval.select = lambda q, exp, k="k6", index_path=None, **kw: [dict(c) for c in chunks]
        self.addCleanup(lambda: setattr(retrieval, "select", orig))
        items, info = kb.merge([dict(c) for c in chunks], "open question 2", "terms", rerank=True)       # what the runtime did
        self.assertTrue(info["added"])
        srcs = [{"source": c["source"], "title": c["title"], "path": c["path"], "bm25_rank": c.get("bm25_rank"), "ce_score": c.get("ce_score")} for c in items]
        res = T.retrieval_resolver(None, kb)
        k = {k: v for k, v in info.items() if k != "seconds"}
        ids = res("open question 2", "terms", srcs, k)
        self.assertEqual(ids, [c["id"] for c in items])
        self.assertEqual([i for i in ids if i.startswith("kbe:")], ["kbe:" + i for i in info["added"]])
        for bad in (dict(k, added=list(reversed(k["added"])) if len(k["added"]) > 1 else []), dict(k, candidates=k["candidates"] + 1), dict(k, pack_chars=k["pack_chars"] + 1),
                    dict(k, skipped=dict(k["skipped"], no_room=k["skipped"]["no_room"] + 1))):
            with self.assertRaises(T.Refused):
                res("open question 2", "terms", srcs, bad)
        with self.assertRaises(T.Refused):                       # another score on an entry
            res("open question 2", "terms", srcs[:-1] + [dict(srcs[-1], ce_score=(srcs[-1]["ce_score"] or 0) + 1)], k)
        with self.assertRaises(T.Refused):                       # a frozen source that differs
            res("open question 2", "terms", [dict(srcs[0], path="raw/x.md")] + srcs[1:], k)
        with self.assertRaises(T.Refused):                       # entries recorded but the arm has none
            T.retrieval_resolver(None, None)("open question 2", "terms", srcs, k)

    # -- the chain
    def test_re_rows_record_the_entries_the_pack_is_frozen_plus_entries_and_the_excerpts_equal_what_the_model_received(self):
        import hashlib
        from kafka_sre_rag import prompts
        e = self.e
        p = self.run_(*self.args())
        out = p.stdout + p.stderr
        self.assertEqual(p.returncode, 0, out)
        self.assertIn("before-reading done", out)
        for bad in ("REFUSED", "STOPPED"):
            self.assertNotIn(bad, out)
        d7 = e.w / "d7"
        re_rows = [json.loads(l) for l in (d7 / "ans_d7_RE.jsonl").read_text().splitlines()]
        self.assertEqual(len(re_rows), 4)
        self.assertTrue(all(r["kb_entries"] is not None for r in re_rows))
        self.assertTrue(all(r["kb_entries"]["n_added"] == len(r["kb_entries"]["added"]) for r in re_rows))
        self.assertGreaterEqual(len([r for r in re_rows if r["kb_entries"]["added"]]), 1)
        for r in re_rows:
            self.assertEqual(r["pack_check"], "exact")
            self.assertEqual([i for i in r["pack"] if i.startswith("kbe:")], ["kbe:" + i for i in r["kb_entries"]["added"]])
            self.assertEqual(len(r["pack"]), len(r["sources"]))
        for arm in ("R", "C"):                                  # the other arms are as they were: no entries key, chunk ids only
            for l in (d7 / f"ans_d7_{arm}.jsonl").read_text().splitlines():
                r = json.loads(l)
                self.assertNotIn("kb_entries", r)
                self.assertFalse(any(i.startswith("kbe:") for i in r["pack"]))
        # the first reader's excerpts of an RE answer are the pack the model received: every item, the entries included, as pack_text renders them
        plus = {c["id"]: c for c in (json.loads(l) for l in (e.w / "chunks_plus_entries.jsonl").read_text().splitlines())}
        key = json.loads((d7 / "reading/KEY.json").read_text())
        items = {it["id"]: it for it in e.d7}
        n_entry_blocks = 0
        for name, arm in key["aid"].items():
            iid = name.rsplit("-", 1)[0]
            row = next(r for r in (re_rows if arm == "RE" else []) if r["id"] == iid) if arm == "RE" else None
            if row is None:
                continue
            q = eval2.full_question(items[iid])
            received = (self.recv / (hashlib.sha256((q + "|kbe").encode()).hexdigest()[:16] + ".txt")).read_text(encoding="utf-8")    # written by the stub model's call
            text = (d7 / "reading/first/excerpts" / f"{name}.md").read_text()
            blocks = text.split("----- chunk ")[1:]
            self.assertEqual(len(blocks), len(row["pack"]))
            rebuilt = []
            for b, cid in zip(blocks, row["pack"]):
                head, _, body = b.partition(" -----\n")
                self.assertTrue(head.startswith(cid + " | "), (head, cid))
                rebuilt.append({"source": plus[cid]["source"], "title": head.split(" | ", 2)[2], "path": plus[cid]["path"], "text": body.rstrip("\n")})
                n_entry_blocks += cid.startswith("kbe:")
            self.assertEqual(prompts.pack_text(rebuilt), received)
        self.assertGreaterEqual(n_entry_blocks, 1)
        # the judge read the same items: the RE rows carry the pack with the entry ids (the chunk file plus the entries was its input; without it the judge cannot resolve them)
        jre = [r for r in (json.loads(l) for l in (d7 / "judge_d7.jsonl").read_text().splitlines()) if r["set"] == "RE"]
        self.assertEqual(len(jre), 8)
        self.assertTrue(any(i.startswith("kbe:") for r in jre for i in r["pack"]))
        self.assertEqual([r["pack"] for r in jre if r["id"] == re_rows[0]["id"]][0], re_rows[0]["pack"])
        self.assertTrue((d7 / "stage_judge_C_R_RE.done").exists())

    def test_the_entries_reach_the_adjudication_pack_and_the_report_has_the_descriptive_line(self):
        e = self.e
        p = self.run_(*self.args())
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        d7 = e.w / "d7"
        key = json.loads((d7 / "reading/KEY.json").read_text())
        row = lambda n, kind, **k: dict({"answer": n, "id": n.rsplit("-", 1)[0], "kind": kind, "quote": "Stub answer", "minor": False, "source": "f.md:1", "note": "n"}, **k)
        re_names = [n for n in sorted(key["aid"]) if key["aid"][n] == "RE"]
        (e.d / "f_claude.jsonl").write_text(jl([row(re_names[0], "harmful", consequence="data loss"), row(re_names[1], "wrong")]))
        p = self.run_(*self.args("--phase", "after-reading", "--findings-d7", f"claude={e.d / 'f_claude.jsonl'}"))
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertIn(f"--chunks {e.d}/chunks.jsonl --entries-index {self.idx}", p.stdout)      # the plain chunk file and the entries index, never the file "plus entries"
        self.assertNotIn("chunks_plus_entries", p.stdout.split("build:", 1)[1])
        answers = [json.loads(l) for l in (d7 / "pack_input_d7_RE-R-C_reading/answers.jsonl").read_text().splitlines()]
        self.assertTrue(any(i.startswith("kbe:") for a in answers if a["config"] == "RE" for i in a["pack_chunk_ids"]))
        self.assertFalse(any(i.startswith("kbe:") for a in answers if a["config"] != "RE" for i in a["pack_chunk_ids"]))
        frozen = e.d / "frozen.json"
        frozen.write_text(json.dumps({"index_path": "x", "source_commits": {}}))
        pi = d7 / "pack_input_d7_RE-R-C_reading"
        b = subprocess.run([PYBIN, "scripts/m7_adjudication_pack.py", "build", "--answers", str(pi / "answers.jsonl"), "--scenarios", str(pi / "scenarios.jsonl"), "--flags", str(pi / "flags.jsonl"),
                            "--out", str(e.d / "pack"), "--configs", str(self.cfg), "--shuffle-seed", "1", "--sample-seed", "2", "--index-frozen", str(frozen),
                            "--chunks", str(e.d / "chunks.jsonl"), "--entries-index", str(self.idx), "--full-sets", "d7"], cwd=ROOT, capture_output=True, text=True, env=e.env)
        self.assertEqual(b.returncode, 0, b.stdout + b.stderr)
        self.assertEqual({a["config"] for a in answers}, {"RE", "R", "C"})
        self.assertIn("Descriptive: RE - R", next(e.w.glob("M7_RELEASE_TEST_from_stub_data*.md")).read_text())

    def test_the_rule_text_for_re_differs_from_the_text_for_r_by_the_arm_names_only(self):
        r = RR.rule_text("R", "C", "full", BY)
        re_ = RR.rule_text("RE", "C", "full", BY)
        self.assertNotEqual(r, re_)
        ta, tb = r.split(), re_.split()
        self.assertEqual(len(ta), len(tb))
        diff = [(x, y) for x, y in zip(ta, tb) if x != y]
        self.assertGreater(len(diff), 3)
        for x, y in diff:                                          # only the candidate's name differs: R becomes RE
            self.assertEqual(y.replace("RE", "R"), x)
            self.assertIn("RE", y)
        self.assertNotEqual(RR.rule_text_sha256("RE", "C", "full", BY), RR.rule_text_sha256("R", "C", "full", BY))

    # -- adding RE to a finished folder
    def test_re_is_added_to_a_finished_d7_folder_without_touching_r_c_their_judge_rows_or_the_first_reading(self):
        e = self.e
        p = e.run(*e.args())                                         # R and C, as the real folder was made
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        d7 = e.w / "d7"
        before = self.snapshot(d7)
        self.assertTrue((d7 / "stage_bundle.done").exists() and (d7 / "reading/KEY.json").exists())
        # without the explicit option the arm is refused (the bundles were made), and nothing changes
        p = self.run_(*self.args("--reading-dir", "reading2", "--bundle-arms", "RE,R,C"))
        self.assertEqual(p.returncode, 2, p.stdout + p.stderr)
        self.assertEqual(self.snapshot(d7), before)
        p = self.run_(*self.args("--add-arms-late", "--reading-dir", "reading2", "--bundle-arms", "RE,R,C"))
        out = p.stdout + p.stderr
        self.assertEqual(p.returncode, 0, out)
        self.assertIn("before-reading done", out)
        for bad in ("REFUSED", "STOPPED"):
            self.assertNotIn(bad, out)
        after = self.snapshot(d7)
        self.assertEqual(sorted(k for k in before if after.get(k) != before[k]), ["MANIFEST.json", "header_arms.txt"])   # everything else of the old folder, reading/ included, is byte-identical
        man = json.loads((d7 / "MANIFEST.json").read_text())
        self.assertEqual(man["candidate"], "R")
        self.assertEqual(man["rule_variants"][0]["candidate"], "RE")
        self.assertEqual(man["rule_variants"][0]["rule_text_sha256"], RR.rule_text_sha256("RE", "C", "full", BY))
        self.assertIn("RE", man["late_arms"])
        self.assertTrue(any("added after the reading bundles" in h["event"] for h in man["history"]))
        self.assertEqual(man["arms"]["R"]["config_sha256"], T.config_sha(T.load_configs(e.cfg), "R"))     # R's entry as the chain started it (the real one with this tree's pins)
        log = (d7 / "header_log.md").read_text()
        self.assertIn("arm RE added", log)
        self.assertIn("`RE`", log)
        # the judge ran for RE only, in its own file; the old judge file is the old one
        jr = [json.loads(l) for l in (d7 / "judge_d7_add_RE.jsonl").read_text().splitlines()]
        self.assertEqual({r["set"] for r in jr}, {"RE"})
        self.assertEqual(len(jr), 8)
        self.assertTrue((d7 / "stage_judge_add_RE.done").exists())
        # reading2: three answers per scenario, a key of its own, the second family's export
        r2 = d7 / "reading2"
        key2 = json.loads((r2 / "KEY.json").read_text())
        self.assertEqual(sorted(set(key2["aid"].values())), ["C", "R", "RE"])
        self.assertEqual(len(key2["aid"]), 12)
        for iid in key2["items"]:
            self.assertEqual(len([n for n in key2["aid"] if n.rsplit("-", 1)[0] == iid]), 3)
            self.assertEqual((r2 / "first" / f"{iid}.md").read_text().count(" BEGIN ====="), 3)
        self.assertTrue(len(list((r2 / "export_second_reader").glob("*.md"))) >= 4)
        self.assertFalse((r2 / "export_second_reader/KEY.json").exists())
        self.assertEqual((d7 / "bundle_arms_reading2.txt").read_text().strip(), "RE,R,C")
        self.assertEqual((d7 / "bundle_arms.txt").read_text().strip(), "C,R")
        # the later invocation needs no flag for what is recorded
        p = self.run_(*self.args("--reading-dir", "reading2", "--bundle-arms", "RE,R,C"))
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        # each reading is unblinded with its own key, into files of its own
        row = lambda n, kind, **k: dict({"answer": n, "id": n.rsplit("-", 1)[0], "kind": kind, "quote": "Stub answer", "minor": False, "source": "f.md:1", "note": "n"}, **k)
        k1 = json.loads((d7 / "reading/KEY.json").read_text())
        (e.d / "f1.jsonl").write_text(jl([row(sorted(k1["aid"])[0], "harmful", consequence="data loss")]))
        (e.d / "f2.jsonl").write_text(jl([row(sorted(key2["aid"])[0], "harmful", consequence="data loss")]))
        reading_before = {k: v for k, v in self.snapshot(d7).items() if k.startswith("reading/")}
        p = e.run(*e.args("--phase", "after-reading", "--candidate", "R", "--control", "C", "--report", str(e.d / "r1.md"), "--findings-d7", f"claude={e.d / 'f1.jsonl'}", arms="R,C", configs=self.cfg))
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        p = self.run_(*self.args("--phase", "after-reading", "--reading-dir", "reading2", "--report", str(e.d / "r2.md"), "--findings-d7", f"claude={e.d / 'f2.jsonl'}"))
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertTrue((d7 / "findings_d7_R-C_reading.jsonl").exists() and (d7 / "findings_d7_RE-R-C_reading2.jsonl").exists())
        m1, m2 = json.loads((d7 / "measures_d7_R-C_reading.json").read_text()), json.loads((d7 / "measures_d7_RE-R-C_reading2.json").read_text())
        self.assertEqual(sorted(m1["arms"]), ["C", "R"])
        self.assertEqual(sorted(m2["arms"]), ["C", "R", "RE"])
        self.assertTrue(all(r["P"] is not None for a in ("C", "R", "RE") for r in m2["arms"][a]["records"]))      # the old judge file (R, C) and the added one (RE) are all read
        self.assertNotIn("Descriptive", (e.d / "r1.md").read_text())
        self.assertIn("Descriptive: RE - R", (e.d / "r2.md").read_text())
        self.assertIn("(descriptive)", (e.d / "r2.md").read_text())
        self.assertEqual({k: v for k, v in self.snapshot(d7).items() if k.startswith("reading/")}, reading_before)   # the old reading is read, never written
        for f in [x for x in ("ans_d7_R.jsonl", "ans_d7_C.jsonl", "judge_d7.jsonl", "judge_cache_d7.jsonl") if x in before]:
            self.assertEqual(self.snapshot(d7)[f], before[f], f)

    def test_the_manifest_rules_for_a_late_arm_a_new_candidate_and_a_dry_run_on_a_copy_of_a_real_folder(self):
        e = self.e
        wd = e.d / "mf"
        base = ["manifest", "--work", str(wd), "--set", "d7", "--pin", str(e.d / "d7_items.jsonl"), "--configs", str(self.cfg), "--opt", "dry=no"]
        run = lambda *a: e.run(*base, *a, cmd="scripts/m7_release_test.py")
        self.assertEqual(run("--arms", "R,C", "--candidate", "R", "--control", "C").returncode, 0)
        (wd / "stage_bundle.done").write_text("{}")
        p = run("--arms", "R,C,RE", "--candidate", "R", "--control", "C")                       # the arm, without the option
        self.assertEqual(p.returncode, 2)
        self.assertIn("--allow-late-arms", p.stderr)
        p = run("--arms", "R,C,RE", "--candidate", "RE", "--control", "C")                      # the candidate, without the option
        self.assertEqual(p.returncode, 2)
        self.assertIn("candidate", p.stderr)
        p = run("--arms", "R,C,RE", "--candidate", "RE", "--control", "R", "--allow-late-arms")   # another control is not a late arm
        self.assertEqual(p.returncode, 2, p.stdout + p.stderr)
        self.assertEqual(sorted(json.loads((wd / "MANIFEST.json").read_text())["arms"]), ["C", "R"])      # nothing was changed by the refusals
        p = run("--arms", "R,C,RE", "--candidate", "RE", "--control", "C", "--allow-late-arms")
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertIn("added after the reading bundles", p.stdout)
        self.assertEqual(run("--arms", "RE,R,C", "--candidate", "RE", "--control", "C").returncode, 0)   # recorded: no option needed
        self.assertEqual(run("--arms", "R,C", "--candidate", "R", "--control", "C").returncode, 0)      # the old candidate still fits
        # a different rule text for the late candidate is refused once its answers exist; the recorded one still fits
        (wd / "stage_ans_RE.done").write_text("{}")
        p = run("--arms", "RE,R,C", "--candidate", "RE", "--control", "C", "--h6-mode", "sample", "--allow-late-arms")
        self.assertEqual(p.returncode, 2, p.stdout + p.stderr)
        self.assertIn("answers of RE exist", p.stderr)
        self.assertEqual(run("--arms", "RE,R,C", "--candidate", "RE", "--control", "C", "--allow-late-arms").returncode, 0)
        # a dry run on a copy of a real folder is accepted; a real run on a dry folder is not
        base2 = [a if a != "dry=no" else "dry=yes" for a in base]
        p = e.run(*base2, "--arms", "R,C", "--candidate", "R", "--control", "C", cmd="scripts/m7_release_test.py")
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        other = ["manifest", "--work", str(e.d / "mf2"), "--set", "d7", "--pin", str(e.d / "d7_items.jsonl"), "--configs", str(self.cfg), "--arms", "R,C", "--candidate", "R", "--control", "C"]
        self.assertEqual(e.run(*other, "--opt", "dry=yes", cmd="scripts/m7_release_test.py").returncode, 0)
        p = e.run(*other, "--opt", "dry=no", cmd="scripts/m7_release_test.py")
        self.assertEqual(p.returncode, 2, p.stdout + p.stderr)
        self.assertIn("options", p.stderr)

    def test_judge_plan_gives_an_added_arm_its_own_stage_and_never_asks_again_for_a_finished_one(self):
        wd = self.e.d / "jp"
        wd.mkdir()
        (wd / "MANIFEST.json").write_text(json.dumps({"arms": {"C": {}, "R": {}}}))
        self.assertEqual(T.judge_plan(wd), ([("judge_C_R", ["C", "R"], "")], False))
        (wd / "stage_judge_C_R.done").write_text("{}")
        self.assertEqual(T.judge_plan(wd), ([], True))
        (wd / "MANIFEST.json").write_text(json.dumps({"arms": {"C": {}, "R": {}, "RE": {}}}))
        self.assertEqual(T.judge_plan(wd), ([("judge_add_RE", ["RE"], "_add_RE")], False))
        (wd / "stage_judge_add_RE.done").write_text("{}")
        self.assertEqual(T.judge_plan(wd), ([], True))
        (wd / "MANIFEST.json").write_text(json.dumps({"arms": {"C": {}, "R": {}, "RE": {}, "RE2": {}}}))
        self.assertEqual(T.judge_plan(wd)[0], [("judge_add_RE2", ["RE2"], "_add_RE2")])

    def test_h6_with_re_and_c_reaches_the_sealed_bundles_and_the_sealed_excerpts_include_the_entries(self):
        e = self.e
        sha = T.sha256_file(self.cfg)
        (e.d / "ledger.md").write_text(f"| 2026-11-27 | release-look-h6 | configs {sha} | rule {RR.rule_text_sha256('RE', 'C', 'full', BY)} |\n")
        p = self.run_(*self.args(sets="h6", arms="RE,C"))
        out = p.stdout + p.stderr
        self.assertEqual(p.returncode, 0, out)
        self.assertIn("before-reading done", out)
        self.assertNotIn(CANARY, out)
        h6 = e.w / "h6"
        rows = eval2.read_jsonl(h6 / "ans_h6_RE.jsonl.enc")
        self.assertTrue(all(r["kb_entries"] is not None and r["pack_check"] == "exact" for r in rows))
        self.assertTrue(any(r["kb_entries"]["added"] for r in rows))
        self.assertFalse((h6 / "ans_h6_RE.jsonl").exists())
        import eval2_sealed_reading as SR
        ex = [r["excerpts"] for f in sorted((h6 / "reading").glob("first_*.jsonl.enc")) for r in SR.read_rows(f)]
        self.assertEqual(len(ex), 8)                                                                       # RE and C, four answers each
        self.assertTrue(any("Fact entry: " in x and "quoted line" in x for x in ex))                       # the entries are in the sealed excerpts
        for f in e.plain_files():                                                                          # no hidden text in any plain file (the chunk file with the entries holds chunks and entries only)
            self.assertNotIn(CANARY, f.read_text(errors="replace"), f.name)
        self.assertTrue((h6 / "stage_judge_C_RE.done").exists())


    def test_every_after_reading_call_has_its_own_files_only_the_final_call_writes_the_done_line_and_the_combined_report_works(self):
        e = self.e
        sha = T.sha256_file(self.cfg)
        (e.d / "ledger.md").write_text(f"| 2026-11-27 | release-look-h6 | configs {sha} | rule {RR.rule_text_sha256('RE', 'C', 'full', BY)} |\n")
        self.assertEqual(e.run(*e.args()).returncode, 0)                                                       # R and C, as the real folder was made
        p = self.run_(*self.args("--add-arms-late", "--reading-dir", "reading2", "--bundle-arms", "RE,R,C"))
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        p = self.run_(*self.args(sets="h6", arms="RE,C"))
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        d7, h6 = e.w / "d7", e.w / "h6"
        row = lambda n, kind, **k: dict({"answer": n, "id": n.rsplit("-", 1)[0], "kind": kind, "quote": "Stub answer", "minor": False, "source": "f.md:1", "note": "n"}, **k)
        k1 = json.loads((d7 / "reading/KEY.json").read_text())
        k2 = json.loads((d7 / "reading2/KEY.json").read_text())
        (e.d / "f1.jsonl").write_text(jl([row(sorted(k1["aid"])[0], "harmful", consequence="data loss")]))
        re_names = [n for n in sorted(k2["aid"]) if k2["aid"][n] == "RE"]
        (e.d / "f2.jsonl").write_text(jl([row(re_names[0], "harmful", consequence="data loss"), row(re_names[1], "wrong")]))
        hk = json.loads(eval2.decrypt_bytes((h6 / "reading/blind_key.json.enc").read_bytes()))
        for b, ns in hk["bundles"].items():                                                                  # the h6 reader's sealed findings (stub)
            rows_ = [{"answer": n, "id": n.rsplit("-", 1)[0], "kind": "clean", "quote": "", "point": "", "supported": False, "minor": False, "source": "", "note": ""} for n in ns]
            rows_[0] = {"answer": ns[0], "id": ns[0].rsplit("-", 1)[0], "kind": "harmful", "quote": "Stub answer", "point": "", "supported": False, "minor": False, "source": "s", "note": "n", "consequence": "data loss"}
            eval2.write_jsonl(h6 / "reading" / f"findings_{b}.jsonl.enc", rows_)
        done = e.w / "logs/m7_release_test.done"
        calls = {
            "old": self.args("--phase", "after-reading", "--candidate", "R", "--control", "C", "--report", str(e.d / "r_old.md"), "--findings-d7", f"claude={e.d / 'f1.jsonl'}", arms="R,C"),
            "re": self.args("--phase", "after-reading", "--pack-arms", "RE,C", "--reading-dir", "reading2", "--report", str(e.d / "r_re.md"), "--findings-d7", f"claude={e.d / 'f2.jsonl'}"),
            "h6": self.args("--phase", "after-reading", "--h6-dir", str(h6 / "reading"), "--report", str(e.d / "r_h6.md"), sets="h6", arms="RE,C"),
            "final": self.args("--phase", "after-reading", "--pack-arms", "RE,C", "--reading-dir", "reading2", "--h6-dir", str(h6 / "reading"), "--report", str(e.d / "r_final.md"), "--findings-d7", f"claude={e.d / 'f2.jsonl'}",
                               sets="d7,h6", arms="RE,C"),
        }
        # MANIFEST.json is the folder's own log (every call checks it and may append a history line when a logged pin changed, e.g. the chunk file with the entries appearing); it is not a call's output
        snap = lambda: {str(f): T.sha256_file(f) for f in e.w.rglob("*") if f.is_file() and f.name not in ("m7_release_test.done", "MANIFEST.json")}
        seen = {}
        for name in ("old", "re", "h6", "final"):
            before = snap()
            p = self.run_(*calls[name])
            out = p.stdout + p.stderr
            self.assertEqual(p.returncode, 0, name + out)
            self.assertNotIn(CANARY, out)
            after = snap()
            self.assertEqual([f for f in before if after.get(f) != before[f]], [], name)                        # no call changes or removes a file an earlier call wrote
            seen[name] = sorted(f.rsplit("/", 1)[1] for f in set(after) - set(before))
            marks = done.read_text().split() if done.exists() else []
            if name != "final":
                self.assertNotIn("M7_RELEASE_TEST_DRY_RUN_DONE", marks, name)                                    # a partial call is not the end of the chain
                self.assertIn("PARTIAL", out)
            else:
                self.assertEqual(marks.count("M7_RELEASE_TEST_DRY_RUN_DONE"), 1)
                self.assertNotIn("PARTIAL", out)
        tags = [(e.d / f"r_{n}.md").read_text().splitlines()[0] for n in ("old", "re", "h6", "final")]
        self.assertEqual(len(set(tags)), 4)
        self.assertIn("findings_d7_R-C_reading.jsonl", seen["old"])
        self.assertIn("findings_d7_RE-R-C_reading2.jsonl", seen["re"])
        self.assertIn("findings_h6_RE-C_reading.jsonl.enc", seen["h6"])
        self.assertIn("findings_d7-h6_RE-C_reading2.jsonl", seen["final"])
        self.assertIn("report_body_d7-h6_RE-C_reading2.md", seen["final"])
        self.assertIn("rule_out_d7-h6_RE-C_reading2.txt", seen["final"])
        self.assertIn("result_d7-h6_RE-C_reading2.json", seen["final"])
        self.assertIn("m7_release_test_after_reading_d7_R-C_reading.partial", seen["old"])
        fin = (e.d / "r_final.md").read_text()
        self.assertIn("Descriptive: RE - R", fin)                                                               # RE - R on D7, from reading2
        self.assertIn("### Set d7", fin)
        self.assertIn("### Set h6 (hidden: totals only", fin)
        self.assertRegex(fin, r"\| RE \| 4 \|")
        self.assertNotIn("not run", fin)                                                                       # h6 has RE and C only: no R row in its table
        self.assertEqual(len(re.findall(r"^\| R \| ", fin, re.M)), 1)                                          # R only in D7's table
        desc = (e.w / "descriptive_d7-h6_RE-C_reading2.md").read_text()
        self.assertIn("Set d7", desc)
        self.assertNotIn("Set h6", desc)
        for call in ("d7_RE-R-C_reading2", "d7-h6_RE-C_reading2"):                                            # W44: no R answer reaches the pack input
            pa = [json.loads(l) for l in (d7 / f"pack_input_{call}/answers.jsonl").read_text().splitlines()]
            self.assertEqual({x["config"] for x in pa}, {"RE", "C"})
            self.assertEqual(len(pa), 8)
            fl = [json.loads(l) for l in (d7 / f"pack_input_{call}/flags.jsonl").read_text().splitlines()]
            self.assertTrue(all(x["answer_id"].split("-")[1] in ("RE", "C") for x in fl))
        pa_old = [json.loads(l) for l in (d7 / "pack_input_d7_R-C_reading/answers.jsonl").read_text().splitlines()]
        self.assertEqual({x["config"] for x in pa_old}, {"R", "C"})                                           # the old reading keeps its arms
        m2 = json.loads((d7 / "measures_d7_RE-R-C_reading2.json").read_text())
        self.assertIn("R", m2["arms"])                                                                        # R's findings still feed RE - R
        self.assertIn("leave out the time to load the entries index", fin)                                      # the labels
        self.assertIn("an entry never serves a point", fin)
        self.assertIn("path (source file), not by its title line", fin)
        self.assertIn("a reader can tell which answers had entries", fin)
        self.assertNotIn(CANARY, fin)
        self.assertNotIn("Descriptive", (e.d / "r_old.md").read_text())
        res = json.loads((e.w / "result_d7-h6_RE-C_reading2.json").read_text())
        self.assertEqual(sorted(res["sets"]), ["d7", "h6"])
        # a report of another call is never overwritten
        keep = (e.d / "r_re.md").read_bytes()
        bad = list(calls["final"])
        bad[bad.index(str(e.d / "r_final.md"))] = str(e.d / "r_re.md")
        p = self.run_(*bad)
        self.assertEqual(p.returncode, 2, p.stdout + p.stderr)
        self.assertIn("is not the report of this call", p.stderr)
        self.assertEqual((e.d / "r_re.md").read_bytes(), keep)
        # without --report each call has its own default report, named from its call
        p = self.run_(*self.args("--phase", "after-reading", "--reading-dir", "reading2", "--findings-d7", f"claude={e.d / 'f2.jsonl'}"))
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertTrue((e.w / "M7_RELEASE_TEST_from_stub_data_d7_RE-R-C_reading2.md").exists())


if __name__ == "__main__":
    unittest.main()
