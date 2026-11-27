"""Offline tests of `scripts/laptop_bench.py --config R` (the release configuration of eval/m7/release_test/configs.json):
the command line, the hash check, the refusals, the reference file, and every phase on the stub backend. No model, no network."""
import contextlib
import io
import json
import shlex
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import laptop_bench as lb  # noqa: E402

CONFIGS = json.loads((ROOT / lb.CONFIGS_FILE).read_text(encoding="utf-8"))
R = CONFIGS["R"]


def bench(*argv):
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
        try:
            rc = lb.main(list(argv))
        except SystemExit as e:
            rc = e.code if isinstance(e.code, int) else 2
            buf.write(str(e.code))
    return rc, buf.getvalue()


class ReleaseConfig(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.t = Path(self.tmp.name)
        self.out = self.t / "R"

    def tearDown(self):
        self.tmp.cleanup()

    def cfg(self, out=None, *extra):
        rc, text = bench("--config", "R", "--stub", "--out", str(out or self.out), "preflight", *extra)
        self.assertEqual(rc, 0, text)
        return json.loads(((out or self.out) / "config.json").read_text())

    def test_pinned_hash_is_the_file(self):
        self.assertEqual(lb.sha256_file(ROOT / lb.CONFIGS_FILE), lb.CONFIGS_SHA256)

    def test_oneshot_command_equals_configs_json(self):
        cfg = self.cfg()
        q = "why is my topic stuck?"
        argv = lb.oneshot_argv(cfg, q, "default", False)
        toks = shlex.split(R["ask_command"])
        expect = ["-m"] + toks[toks.index("kafka_sre_rag"):]       # drops OMLX_URL=<server url> and the interpreter
        expect[-1] = q
        self.assertEqual(argv[1:], expect)
        self.assertEqual(argv[0], lb.py())
        self.assertIn("--lookup-cards", argv)
        self.assertEqual(argv[argv.index("--model") + 1], "gemma-4-31b-it-4bit")
        self.assertEqual(json.loads(argv[argv.index("--extra-body") + 1]), {"chat_template_kwargs": {"enable_thinking": False}})
        for absent in ("--mode", "--k", "--check", "--max-tokens", "--catalogues"):
            self.assertNotIn(absent, argv)
        # the dry-run command of the file is the same line with --dry-run in place of --json
        dry = shlex.split(R["ask_command_dry_run"])
        self.assertEqual([t for t in dry[dry.index("kafka_sre_rag"):] if t != "--dry-run"][:-1],
                         [t for t in toks[toks.index("kafka_sre_rag"):] if t != "--json"][:-1])

    def test_config_re_parses_and_its_command_has_the_entries_while_r_has_not(self):
        out_re = self.t / "RE"
        rc, text = bench("--config", "RE", "--stub", "--out", str(out_re), "preflight")
        self.assertEqual(rc, 0, text)
        cfg_re = json.loads((out_re / "config.json").read_text())
        argv_re = lb.oneshot_argv(cfg_re, "q?", "default", False)
        argv_r = lb.oneshot_argv(self.cfg(), "q?", "default", False)
        self.assertIn("--kb-entries", argv_re)
        self.assertIn("--lookup-cards", argv_re)
        self.assertNotIn("--kb-entries", argv_r)
        self.assertEqual([t for t in argv_re if t != "--kb-entries"], argv_r)

    def test_old_defaults_command_unchanged(self):
        rc, text = bench("--stub", "--out", str(self.t / "old"), "preflight")
        self.assertEqual(rc, 0, text)
        cfg = json.loads((self.t / "old/config.json").read_text())
        self.assertIsNone(cfg["config_name"])
        self.assertEqual(lb.oneshot_argv(dict(cfg, model="m"), "Q", "strict", True),
                         [lb.py(), "-m", "kafka_sre_rag", "ask", "--json", "--mode", "strict", "--catalogues", "work/m4/catalogues",
                          "--check", "--backend", "openai", "--model", "m", "--extra-body",
                          '{"chat_template_kwargs": {"enable_thinking": false}}', "--k", "k6", "Q"])
        self.assertNotIn("--lookup-cards", lb.eval2_answer_cmd(cfg, "i", "a"))

    def test_config_fields_come_from_the_file(self):
        cfg = self.cfg()
        self.assertEqual((cfg["model"], cfg["model_repo"], cfg["model_repo_revision"], cfg["lookup_cards"], cfg["backend"]),
                         ("gemma-4-31b-it-4bit", "mlx-community/gemma-4-31b-it-4bit", R["model"]["hf_repo_revision"], True, "openai"))
        self.assertEqual(cfg["configs_sha256"], lb.CONFIGS_SHA256)
        self.assertEqual(R["model"]["hf_repo_revision"], "696d436c404745a59f30e4939a658162b0a9e57f")
        steps = {s["name"]: s for s in lb.install_steps(cfg)}
        cmd = steps["model"]["cmd"]
        self.assertEqual(cmd[cmd.index("--revision") + 1], R["model"]["hf_repo_revision"])
        self.assertIn("models/gemma-4-31b-it-4bit", cmd)
        old = {s["name"]: s for s in lb.install_steps(dict(lb.DEFAULTS))}["model"]["cmd"]
        self.assertNotIn("--revision", old)
        self.assertIn("models/Qwen3.8-27B-4bit", old)
        cmd = lb.eval2_answer_cmd(cfg, "i", "a")
        self.assertIn("--lookup-cards", cmd)
        self.assertEqual(cmd[cmd.index("--catalogue") + 1], "work/m4/catalogues")
        self.assertNotIn("--answer-profile", cmd)

    def test_hash_checked(self):
        with mock.patch.object(lb, "CONFIGS_SHA256", "0" * 64):
            rc, text = bench("--config", "R", "--stub", "--out", str(self.t / "x"), "preflight")
            self.assertEqual(rc, 2)
            self.assertIn("--configs-sha256", text)
            rc, text = bench("--config", "R", "--configs-sha256", lb.sha256_file(ROOT / lb.CONFIGS_FILE), "--stub",
                             "--out", str(self.t / "x"), "preflight")
            self.assertEqual(rc, 0, text)

    def test_conflicting_options_and_mixed_folders_refused(self):
        for opt in (["--model", "other"], ["--extra-body", "{}"], ["--model-repo", "a/b"], ["--max-tokens", "100"], ["--k", "k3"]):
            rc, text = bench("--config", "R", "--stub", "--out", str(self.t / "c"), *opt, "preflight")
            self.assertEqual(rc, 2, opt)
        self.cfg()
        rc, text = bench("--stub", "--config", "R", "--out", str(self.out), "speed", "--conds", "oneshot-strict")   # not R's condition
        self.assertEqual(rc, 2, text)
        bench("--stub", "--out", str(self.t / "old"), "preflight")
        rc, text = bench("--stub", "--config", "R", "--out", str(self.t / "old"), "preflight")     # old folder, now R
        self.assertEqual(rc, 2)
        self.assertIn("must not mix", text)

    def test_service_conditions_refused_for_r(self):
        self.cfg()
        for conds in ("service-default", "oneshot-strict", "oneshot-default-check"):
            rc, text = bench("--stub", "--out", str(self.out), "speed", "--conds", conds)
            self.assertEqual(rc, 2, conds)
            self.assertIn("no lookup cards", text)
        rc, text = bench("--stub", "--out", str(self.out), "sustained", "--label", "ac", "--runtime", "service", "--force")
        self.assertEqual(rc, 2)
        runner = lb.RealRunner("service", {"lookup_cards": True, "backend": "openai", "port": 1}, self.t, "x")
        with mock.patch.object(lb, "omlx_pid", side_effect=AssertionError("looked for the server")):
            with self.assertRaises(RuntimeError):      # refused before anything touches the server or a process
                runner.start()

    def test_every_phase_runs_with_r_and_with_the_old_defaults(self):
        ref = self.t / "ref_R.json"
        for label, pre, out in (("R", ["--config", "R"], self.t / "r"), ("old", [], self.t / "o")):
            steps = [["preflight"], ["install-check", "--run"], ["speed", "--n-service", "3", "--n-oneshot", "2"],
                     ["speed", "--label", "working-laptop", "--conds", "oneshot-default", "--n-oneshot", "2", "--apps", "browser"],
                     ["sustained", "--label", "ac", "--n", "6", "--force"], ["memory", "--baseline-s", "1"]]
            steps += [["make-reference", "--ref-out", str(ref)], ["score", "--n", "5", "--reference", str(ref)]] if label == "R" else [["score", "--n", "5"]]
            steps.append(["bundle"])
            for st in steps:
                rc, text = bench("--stub", "--out", str(out), *pre, *st)
                self.assertEqual(rc, 0, f"{label} {st}: {text[-400:]}")
            md = (out / "SUMMARY.md").read_text()
            self.assertIn("STUB RUN", md)
            rows = [json.loads(l) for l in (out / "speed/baseline/rows.jsonl").read_text().splitlines()]
            conds = {r["cond"] for r in rows}
            if label == "R":
                self.assertEqual(conds, {"oneshot-default"})
                self.assertIn("one-shot only", md)
                wl = [l for l in md.splitlines() if l.startswith("| A working laptop")]
                self.assertEqual(len(wl), 1)
                self.assertIn("oneshot default", wl[0])
                self.assertNotIn("not run", wl[0])
                self.assertNotIn("service ready", md)
                self.assertIn("First question of the memory phase", md)
                self.assertIn(str(ref), md)
                self.assertTrue((out / "sustained/ac/rows.jsonl").exists())
                cmp = json.loads((out / "score/compare.json").read_text())
                self.assertEqual(cmp["reference_file"]["sha256"], lb.sha256_file(ref))
                self.assertEqual(cmp["n"], 5)
            else:
                self.assertEqual(conds, set(lb.CONDS))
                self.assertIn("eval/m4/laptop/ids.json", md)

    def test_reference_file_rules(self):
        ref = self.t / "ref.json"
        rc, text = bench("--stub", "--config", "R", "--out", str(self.t / "m"), "make-reference", "--ref-out", str(ref))
        self.assertEqual(rc, 0, text)
        r = json.loads(ref.read_text())
        ids = lb.load_ids()
        self.assertEqual((r["kind"], r["config_name"], r["configs_sha256"], r["ids"], r["items_sha256"]),
                         ("laptop-score-reference", "R", lb.CONFIGS_SHA256, ids["score"]["ids"], ids["score"]["items_sha256"]))
        self.assertEqual(len(r["ids"]), 60)
        self.assertEqual(set(r["pass"]), set(r["answer_sha256"]))
        self.assertIn("--lookup-cards", r["command"])
        before = ref.read_bytes()
        rc, text = bench("--stub", "--config", "R", "--out", str(self.t / "m2"), "make-reference", "--ref-out", str(ref))
        self.assertEqual(rc, 2)
        self.assertEqual(ref.read_bytes(), before)            # never overwritten
        # a real run refuses a stub reference, and a reference of another configuration
        cfg = {"stub": False, "config_name": "R", "configs_sha256": lb.CONFIGS_SHA256, "reference": {"path": str(ref)}}
        with self.assertRaises(SystemExit):
            lb.load_reference(cfg, ids)
        other = dict(r, stub=False, config_name=None)
        (self.t / "o.json").write_text(json.dumps(other))
        with self.assertRaises(SystemExit):
            lb.load_reference(dict(cfg, reference={"path": str(self.t / "o.json")}), ids)
        # R without any reference does not fall back to the Qwen reference outside a stub run
        with self.assertRaises(SystemExit):
            lb.load_reference({"stub": False, "config_name": "R", "reference": None}, ids)
        self.assertIsNone(lb.reference_record({"stub": False, "config_name": "R"}, None, None))
        self.assertEqual(lb.reference_record({"stub": False, "config_name": None}, None, None)["path"], lb.EMBEDDED_REF)

    def test_reference_is_recorded_in_config(self):
        ref = self.t / "ref.json"
        bench("--stub", "--config", "R", "--out", str(self.t / "m"), "make-reference", "--ref-out", str(ref))
        cfg = self.cfg(self.t / "s", "--reference", str(ref))
        self.assertEqual(cfg["reference"]["sha256"], lb.sha256_file(ref))
        ref.write_text(ref.read_text() + " ")                    # changed after the folder was started
        rc, text = bench("--stub", "--out", str(self.t / "s"), "preflight")
        self.assertEqual(rc, 2)
        self.assertIn("changed or is missing", text)


if __name__ == "__main__":
    unittest.main()
