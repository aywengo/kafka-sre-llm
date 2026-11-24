"""M7 KB phase 2, I3: the chain scripts/m7_kb2.sh and its helpers (scripts/m7_kb2_rule.py): the server gate (W2, W16), the sanity rule,
the full run's gate, the accepted entries, and a stub dry run end to end (sanity and full, a planted failure, a second invocation)."""
import contextlib
import hashlib
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
import m7_kb2_rule as R  # noqa: E402

DS, GLM8, GLM4 = "DeepSeek-V4.1-Flash-1017-oQ4e-mtp", "GLM-5.3-Flash-oQ8e-mtp", "GLM-5.3-Flash-oQ4e"
HAVE_INDEX = (ROOT / "work/v3/chunks_s1.jsonl").exists() and (ROOT / "runs/v3/INDEX_FROZEN.json").exists() and (ROOT / ".venv/bin/python").exists()


def run(args):
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = R.main(args)
    return code, out.getvalue(), err.getvalue()


class Syntax(unittest.TestCase):
    def test_sh_n(self):
        r = subprocess.run(["sh", "-n", str(ROOT / "scripts/m7_kb2.sh")], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stderr)


class ServerGate(unittest.TestCase):
    def gate(self, status, need, markers=()):
        with tempfile.TemporaryDirectory() as d:
            s = Path(d) / "s.json"
            s.write_text(json.dumps(status))
            margs = []
            for mid in markers:
                p = Path(d) / f"m_{len(margs)}"
                p.write_text("")
                margs += ["--marker", f"{mid}={p}"]
            margs += ["--marker", f"{GLM8}={Path(d) / 'absent'}"]
            return run(["server-gate", "--status", str(s), "--need", need] + margs)

    def test_free(self):
        self.assertEqual(self.gate({"active_requests": 0, "waiting_requests": 0, "loaded_models": []}, DS), (0, "", ""))

    def test_w16_deepseek_of_another_client_is_unloaded(self):
        code, out, _ = self.gate({"active_requests": 0, "waiting_requests": 0, "loaded_models": [DS]}, GLM8)
        self.assertEqual((code, out.strip()), (0, f"W16 {DS}"))
        code, out, _ = self.gate({"active_requests": 0, "waiting_requests": 0, "loaded_models": [DS]}, DS)   # also when the writer needs it: a copy without our marker is foreign
        self.assertEqual((code, out.strip()), (0, f"W16 {DS}"))
        code, out, _ = self.gate({"active_requests": 1, "waiting_requests": 1, "loaded_models": [DS]}, DS)   # busy: the unload waits for idle
        self.assertEqual(code, 0)
        self.assertIn(f"W16 {DS}", out)
        self.assertIn("waits until the server is idle", out)

    def test_other_foreign_model_refused(self):
        for m in (GLM4, GLM8, "Qwen3.8-27B-4bit"):
            code, out, err = self.gate({"active_requests": 0, "waiting_requests": 0, "loaded_models": [m, DS]}, DS)
            self.assertEqual(code, 2, m)
            self.assertIn(f"the model {m} is loaded by another client", err)
            self.assertEqual(out, "")                                        # nothing is unloaded, not even the W16 model

    def test_busy_refused(self):
        code, _, err = self.gate({"active_requests": 1, "waiting_requests": 0, "loaded_models": []}, DS)
        self.assertEqual(code, 2)
        self.assertIn("active or waiting", err)
        code, _, err = self.gate({"active_requests": 0, "waiting_requests": 2, "loaded_models": [GLM8]}, GLM8, markers=[GLM8])
        self.assertEqual(code, 2)

    def test_ours_not_needed_is_unloaded(self):
        code, out, _ = self.gate({"active_requests": 0, "waiting_requests": 0, "loaded_models": [GLM8]}, DS, markers=[GLM8])
        self.assertEqual((code, out.strip()), (0, f"UNLOAD {GLM8}"))
        code, out, _ = self.gate({"active_requests": 0, "waiting_requests": 0, "loaded_models": [DS]}, DS, markers=[DS])
        self.assertEqual((code, out), (0, ""))

    def test_unreadable_status(self):
        with tempfile.TemporaryDirectory() as d:
            (Path(d) / "s.json").write_text("not json")
            self.assertEqual(run(["server-gate", "--status", str(Path(d) / "s.json"), "--need", DS])[0], 2)


class SanityAndFull(unittest.TestCase):
    def write(self, d, windows, checks):
        Path(d, "windows.jsonl").write_text("".join(json.dumps(r) + "\n" for r in windows))
        Path(d, "checks.jsonl").write_text("".join(json.dumps(r) + "\n" for r in checks))

    def test_sanity_rule(self):
        with tempfile.TemporaryDirectory() as d:
            win = [{"window": f"w{i}", "status": "ok", "written": 3, "gate_rejects": 0, "seconds": 30.0} for i in range(50)]
            chk = [{"id": f"e{i}", "verdict": "ok" if i % 10 else "partial"} for i in range(150)]
            self.write(d, win, chk)
            r = R.sanity(d, d, 3)
            self.assertTrue(r["mechanical_pass"], r)
            self.assertAlmostEqual(float(r["items"][4]["value"].split()[0]), 30 / 3 * 1487 / 3600, places=1)
            win[0] = dict(win[0], status="failed")
            win[1] = dict(win[1], status="failed")
            win[2] = dict(win[2], status="failed")
            self.write(d, win, chk)
            self.assertFalse(R.sanity(d, d, 3)["mechanical_pass"])            # 47 of 50 valid < 95 %
            self.write(d, [dict(w, seconds=120.0) for w in win[3:]], chk)
            self.assertFalse(R.sanity(d, d, 3)["items"][4]["pass"])           # 120 s / 3 x 1487 = 16.5 h > 16 h
            self.write(d, win[3:], [dict(c, verdict="ok") for c in chk])
            self.assertFalse(R.sanity(d, d, 3)["items"][3]["pass"])           # accepts 100 % > 98 %
            self.write(d, [dict(w, written=10, gate_rejects=2) for w in win[3:]], chk)
            it = R.sanity(d, d, 3)["items"]
            self.assertFalse(it[1]["pass"])                                    # 20 % gate failures
            self.assertFalse(it[2]["pass"])                                    # median 10

    def test_full_gate(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "r.json"
            self.assertEqual(run(["full-gate", "--sanity-result", str(p), "--read-notok", "0"])[0], 2)
            p.write_text(json.dumps({"mechanical_pass": False}))
            self.assertEqual(run(["full-gate", "--sanity-result", str(p), "--read-notok", "0"])[0], 2)
            p.write_text(json.dumps({"mechanical_pass": True}))
            self.assertEqual(run(["full-gate", "--sanity-result", str(p)])[0], 2)
            self.assertEqual(run(["full-gate", "--sanity-result", str(p), "--read-notok", "5"])[0], 2)
            self.assertEqual(run(["full-gate", "--sanity-result", str(p), "--read-notok", "41"])[0], 2)
            self.assertEqual(run(["full-gate", "--sanity-result", str(p), "--read-notok", "4"])[0], 0)

    def test_draws_are_by_hash(self):
        ids = [f"kbe-{i:016x}" for i in range(500)]
        a = R._draw(ids, "kb2-audit:", 200)
        self.assertEqual(a, sorted(ids, key=lambda i: hashlib.sha256(("kb2-audit:" + i).encode()).hexdigest())[:200])
        self.assertEqual(a, R._draw(list(reversed(ids)), "kb2-audit:", 200))


@unittest.skipUnless(HAVE_INDEX, "the frozen index or the venv is not here")
class DryRun(unittest.TestCase):
    """The chain end to end on the stub server: sanity with plants, a second invocation, the full run with a planted failure and its resume."""

    def sh(self, *args, ok=(0,)):
        r = subprocess.run(["sh", "scripts/m7_kb2.sh", *args], cwd=ROOT, capture_output=True, text=True, timeout=600)
        self.assertIn(r.returncode, ok, r.stdout[-3000:] + r.stderr[-3000:])
        return r

    def tree(self, d):
        return {str(p.relative_to(d)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(Path(d).rglob("*")) if p.is_file()}

    def test_end_to_end(self):
        with tempfile.TemporaryDirectory() as d:
            w = str(Path(d) / "kb")
            r = self.sh("--sanity", "--dry-run", "--work", w, "--plant-write-bad", "69035e6ae7d31ef2", "--plant-check-bad", "6bae04ac43dc5bcc",
                        "--plant-stop", "20", ok=(1,))
            self.assertIn("the writer stopped", r.stdout)
            r = self.sh("--sanity", "--dry-run", "--work", w, "--plant-write-bad", "69035e6ae7d31ef2", "--plant-check-bad", "6bae04ac43dc5bcc")
            self.assertIn("rerun after a stop", r.stdout)
            self.assertIn("SANITY (mechanical parts): PASS", r.stdout)
            self.assertTrue(r.stdout.rstrip().endswith("KB2_PILOT_from_stub_data.md"))
            before = self.tree(w)
            r = self.sh("--sanity", "--dry-run", "--work", w)
            self.assertEqual(self.tree(w), before)                              # a second invocation changes nothing
            r = self.sh("--full", "--dry-run", "--work", w, "--sanity-read-notok", "1", "--fail-after", "index", ok=(1,))
            self.assertIn("planted failure after the stage index", r.stdout)
            wlog = Path(w, "logs/m7_kb2.full.write.log").read_text()
            self.assertEqual(wlog.count(" reused"), 50)
            r = self.sh("--full", "--dry-run", "--work", w, "--sanity-read-notok", "1")
            self.assertIn("5 stage(s) done", r.stdout)                         # slice, header, write, check, index kept
            self.assertNotIn("[write]", r.stdout)
            self.assertIn("stage audit done", r.stdout)
            self.assertEqual(Path(w, "logs/m7_kb2.full.write.log").read_text(), wlog)
            before = self.tree(w)
            self.sh("--full", "--dry-run", "--work", w, "--sanity-read-notok", "1")
            self.assertEqual(self.tree(w), before)
            rep = Path(w, "KB2_PILOT_from_stub_data.md").read_text()
            self.assertIn("## Sanity run (50 windows)", rep)
            self.assertIn("## Full run (1,487 windows)", rep)
            hb = Path(w, "HEARTBEAT.md").read_text()
            self.assertIn(f"loading {DS}", hb)
            self.assertIn(f"unloaded {GLM8}", hb)
            self.assertNotIn("W16", hb)

    def test_server_paths(self):
        with tempfile.TemporaryDirectory() as d:
            w = str(Path(d) / "a")
            self.sh("--sanity", "--dry-run", "--work", w, "--plant-foreign", DS, "--plant-busy", "2")
            hb = Path(w, "HEARTBEAT.md").read_text()
            self.assertIn(f"W16: {DS} is loaded by another client", hb)
            self.assertIn(f"unloaded {DS}: W16's unload of a foreign model", hb)
            for plant in (["--plant-foreign", GLM4], ["--plant-busy", "1"]):
                w = str(Path(d) / ("b" + plant[0][-4:]))
                r = self.sh("--sanity", "--dry-run", "--work", w, *plant, ok=(2,))
                self.assertIn("the server is not free for this chain", r.stderr)
                self.assertEqual(sorted(os.listdir(w)), ["stub_server_status.json"])   # nothing else written
            r = self.sh("--sanity", "--dry-run", "--work", str(Path(d) / "c"), "--checker-model", GLM4, ok=(2,))
            self.assertIn("--fallback-note", r.stderr)
            r = self.sh("--sanity", "--dry-run", "--work", "work/m7/kb", ok=(2,))
            r = self.sh("--sanity", "--plant-busy", "1", ok=(2,))
            self.assertIn("dry runs only", r.stderr)


if __name__ == "__main__":
    unittest.main()
