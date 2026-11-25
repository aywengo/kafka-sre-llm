"""M7 KB phase 2, I3: the chain scripts/m7_kb2.sh and its helpers (scripts/m7_kb2_rule.py): the server gate (W2, W16), the sanity rule,
the full run's gate, the accepted entries, and a stub dry run end to end (sanity and full, a planted failure, a second invocation)."""
import contextlib
import unittest.mock
import hashlib
import io
import json
import os
import signal
import subprocess
import threading
import time
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
    def gate(self, status, need, markers=(), load="before"):
        """markers: models with a marker of this chain; load: the server's load time of those models against the marker's
        time ("before": ours, "after": another client loaded it later, "none": the server reports no load time)."""
        with tempfile.TemporaryDirectory() as d:
            s = Path(d) / "s.json"
            margs, times = [], dict(status.get("model_load_times") or {})
            for mid in markers:
                p = Path(d) / f"m_{len(margs)}"
                p.write_text("")
                margs += ["--marker", f"{mid}={p}"]
                if load != "none":
                    times[mid] = os.path.getmtime(p) + (-10 if load == "before" else 60)
            margs += ["--marker", f"{GLM8}={Path(d) / 'absent'}"]
            s.write_text(json.dumps({**status, "model_load_times": times}))
            return run(["server-gate", "--status", str(s), "--need", need] + margs)

    def test_free(self):
        self.assertEqual(self.gate({"active_requests": 0, "waiting_requests": 0, "loaded_models": []}, DS), (0, "", ""))

    def test_w16_deepseek_of_another_client(self):
        code, out, _ = self.gate({"active_requests": 0, "waiting_requests": 0, "loaded_models": [DS]}, GLM8)    # it blocks the checker: unloaded
        self.assertEqual((code, out.strip()), (0, f"W16 {DS}"))
        code, out, _ = self.gate({"active_requests": 1, "waiting_requests": 1, "loaded_models": [DS]}, GLM8)    # busy: the unload waits for idle
        self.assertEqual(code, 0)
        self.assertIn(f"W16 {DS}", out)
        self.assertIn("waits until the server is idle", out)
        code, out, _ = self.gate({"active_requests": 0, "waiting_requests": 0, "loaded_models": [DS]}, DS)      # the writer needs it: used as it is
        self.assertEqual((code, out.strip()), (0, f"ADOPT {DS}"))
        code, _, err = self.gate({"active_requests": 1, "waiting_requests": 0, "loaded_models": [DS]}, DS)      # ... but not while its client is active
        self.assertEqual(code, 2)
        self.assertIn("active or waiting", err)

    def test_other_foreign_model_refused(self):
        for m in (GLM4, GLM8, "Qwen3.8-27B-4bit"):
            code, out, err = self.gate({"active_requests": 0, "waiting_requests": 0, "loaded_models": [m, DS]}, GLM8 if m != GLM8 else DS)
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

    def test_leftover_marker_never_makes_another_clients_model_ours(self):
        code, out, err = self.gate({"active_requests": 0, "waiting_requests": 0, "loaded_models": [GLM8]}, DS, markers=[GLM8], load="after")
        self.assertEqual((code, out), (2, ""))
        self.assertIn("after this chain's marker", err)
        code, out, err = self.gate({"active_requests": 0, "waiting_requests": 0, "loaded_models": [GLM8]}, DS, markers=[GLM8], load="none")
        self.assertEqual((code, out), (2, ""))
        self.assertIn("cannot be checked", err)
        code, out, err = self.gate({"active_requests": 0, "waiting_requests": 0, "loaded_models": [GLM8],
                                    "model_load_times": {GLM8: "2099-01-01T00:00:00+00:00"}}, DS, markers=[GLM8], load="none")
        self.assertEqual((code, out), (2, ""))                                # an ISO time after the marker: refused
        self.assertIn("after this chain's marker", err)

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

    def test_reading_sample_prefers_judged_rejections(self):
        chk = [{"id": f"a{i}", "verdict": "ok"} for i in range(50)] + [{"id": f"u{i}", "verdict": "unchecked"} for i in range(7)] \
            + [{"id": f"p{i}", "verdict": v} for i, v in enumerate(["partial", "wrong", "unsupported"] * 4)]
        pick = R.pick_reading(chk, "salt", 40, 20)
        self.assertEqual((sum(i[0] == "a" for i in pick), sum(i[0] == "p" for i in pick), sum(i[0] == "u" for i in pick)), (40, 12, 7))
        pick = R.pick_reading(chk, "salt", 40, 10)                                       # the quota is filled by judged ones first
        self.assertEqual((sum(i[0] == "p" for i in pick), sum(i[0] == "u" for i in pick)), (10, 0))
        self.assertEqual(R.pick_reading(chk, "salt", 40, 20), R.pick_reading(chk, "salt", 40, 20))
        only_u = [r for r in chk if r["verdict"] in ("ok", "unchecked")]
        self.assertEqual(sum(i[0] == "u" for i in R.pick_reading(only_u, "salt", 40, 20)), 7)     # unchecked still fill when nothing else

    def test_sanity_rule_exit_and_identity(self):
        with tempfile.TemporaryDirectory() as d:
            self.write(d, [{"window": f"w{i}", "status": "ok", "written": 3, "gate_rejects": 0, "seconds": 30.0} for i in range(50)],
                       [{"id": f"e{i}", "verdict": "ok"} for i in range(150)])
            out = Path(d) / "r.json"
            code, text, _ = run(["sanity-rule", "--write-dir", d, "--check-dir", d, "--writer", DS, "--checker", GLM8, "--out", str(out)])
            self.assertEqual(code, 1)                                          # accepts 100 %: the rule fails, exit 1
            self.assertIn("STOP", text)
            self.assertEqual(R.load_json(out)["identity"], R.run_identity(DS, GLM8))

    def _sanity_dir(self, d, writer=DS, checker=GLM8, mpass=True):
        sd = Path(d) / "sanity"
        (sd / "reading").mkdir(parents=True)
        ident = R.run_identity(writer, checker)
        (sd / "sanity_result.json").write_text(json.dumps({"mechanical_pass": mpass, "identity": ident}))
        (sd / "MANIFEST.json").write_text(json.dumps({"entry_prompt_version": ident["entry_prompt_version"], "check_prompt_version": ident["check_prompt_version"],
                                                     "kb_prompts_sha256": ident["kb_prompts_sha256"], "options": {"writer": writer, "checker": checker}}))
        order = [f"kbe-{i:016x}" for i in range(60)]
        (sd / "reading" / "sanity_reading_key.json").write_text(json.dumps({"order": order, "checker_verdict": {i: ("ok" if n < 40 else "partial") for n, i in enumerate(order)}}))
        return sd, order

    def _reading(self, d, order, notok=0, drop=0, extra=False):
        f = Path(d) / "reading.jsonl"
        rows = [{"id": i, "rating": "partial" if n < notok else "ok", "reason": "r"} for n, i in enumerate(order[drop:])]
        if extra:
            rows.append({"id": "kbe-ffffffffffffffff", "rating": "ok"})
        f.write_text("".join(json.dumps(r) + "\n" for r in rows))
        return f

    def fg(self, sd, reading, writer=DS, checker=GLM8):
        return run(["full-gate", "--sanity-result", str(sd / "sanity_result.json"), "--sanity-dir", str(sd), "--writer", writer, "--checker", checker]
                   + (["--reading", str(reading)] if reading else []))

    def test_full_gate(self):
        with tempfile.TemporaryDirectory() as d:
            sd, order = self._sanity_dir(d)
            code, out, _ = self.fg(sd, self._reading(d, order, notok=4))
            self.assertEqual(code, 0)
            self.assertIn("rated 4 of 40 accepted entries not ok", out)
            self.assertIn("sha256", out)
            self.assertEqual(self.fg(sd, self._reading(d, order, notok=5))[0], 2)
            self.assertEqual(self.fg(sd, None)[0], 2)
            self.assertEqual(self.fg(sd, self._reading(d, order, drop=1))[0], 2)            # ids must equal the sample's
            self.assertEqual(self.fg(sd, self._reading(d, order, extra=True))[0], 2)
            self.assertEqual(self.fg(sd, self._reading(d, order), checker=GLM4)[0], 2)     # another checker than the sanity run's
        with tempfile.TemporaryDirectory() as d:
            sd, order = self._sanity_dir(d, mpass=False)
            self.assertEqual(self.fg(sd, self._reading(d, order))[0], 2)
        with tempfile.TemporaryDirectory() as d:                                          # a sanity result of another prompt version
            sd, order = self._sanity_dir(d)
            r = json.loads((sd / "sanity_result.json").read_text())
            r["identity"]["entry_prompt_version"] = "kbe/0"
            (sd / "sanity_result.json").write_text(json.dumps(r))
            code, _, err = self.fg(sd, self._reading(d, order))
            self.assertEqual(code, 2)
            self.assertIn("entry_prompt_version", err)
        with tempfile.TemporaryDirectory() as d:                                          # a manifest of other KB prompt texts
            sd, order = self._sanity_dir(d)
            m = json.loads((sd / "MANIFEST.json").read_text())
            m["kb_prompts_sha256"] = "0" * 64
            (sd / "MANIFEST.json").write_text(json.dumps(m))
            self.assertEqual(self.fg(sd, self._reading(d, order))[0], 2)

    def test_kb_hash_covers_the_message_builders(self):
        from unittest import mock
        from kafka_sre_rag import prompts as P
        h = R.kb_prompts_sha256()
        with mock.patch.object(P, "KB_CHECK_USER", P.KB_CHECK_USER + " "):
            self.assertNotEqual(R.kb_prompts_sha256(), h)

        def other(window, entries):
            return []
        with mock.patch.object(P, "kb_check_messages", other):
            self.assertNotEqual(R.kb_prompts_sha256(), h)

    def test_draws_are_by_hash(self):
        ids = [f"kbe-{i:016x}" for i in range(500)]
        a = R._draw(ids, "kb2-audit:", 200)
        self.assertEqual(a, sorted(ids, key=lambda i: hashlib.sha256(("kb2-audit:" + i).encode()).hexdigest())[:200])
        self.assertEqual(a, R._draw(list(reversed(ids)), "kb2-audit:", 200))


class NoHangAfterServerError(unittest.TestCase):
    """Regression (2026-11-24, the hang in the main tree): after a server error the writer and the checker must end even while
    windows are still queued. They once cancelled the queued futures (shutdown(cancel_futures=True)), and as_completed then
    waited forever at 0 % CPU; it showed only when queued windows were left, which depends on timing."""

    def _run(self, fn, args, env):
        res = {}

        def target():
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                res["code"] = fn(args)
        old = {k: os.environ.get(k) for k in env}
        os.environ.update(env)
        try:
            t = threading.Thread(target=target, daemon=True)
            t.start()
            t.join(60)
        finally:
            for k, v in old.items():
                os.environ.pop(k, None) if v is None else os.environ.__setitem__(k, v)
        self.assertFalse(t.is_alive(), "the job did not end after a server error (queued windows left waiting)")
        return res["code"]

    def test_writer_and_checker_end_with_windows_queued(self):
        import kb_write_entries as W
        import kb_check_entries as C
        ww, cw = W.write_window, C.check_window

        def slow_w(*a, **k):                    # each window takes a moment, so windows are still queued when the error comes
            time.sleep(0.05)
            return ww(*a, **k)

        def slow_c(*a, **k):
            time.sleep(0.05)
            return cw(*a, **k)
        with tempfile.TemporaryDirectory() as d, unittest.mock.patch.object(W, "write_window", slow_w), \
                unittest.mock.patch.object(C, "check_window", slow_c):
            d = Path(d)
            rows = [{"id": f"c{i:03d}", "source": "strimzi", "path": f"raw/x/p{i // 2}.md", "title": "T", "text": f"The value is {i}0 here."}
                    for i in range(40)]
            ch = d / "chunks.jsonl"
            ch.write_text("".join(json.dumps(r) + "\n" for r in rows))
            rec = d / "rec.json"
            rec.write_text(json.dumps({"index_path": str(ch), "sha256": hashlib.sha256(ch.read_bytes()).hexdigest(), "source_commits": {}}))
            import kb_slice
            with contextlib.redirect_stdout(io.StringIO()):
                kb_slice.main(["--record", str(rec), "--out", str(d / "slice.jsonl")])
            base = ["--slice", str(d / "slice.jsonl"), "--record", str(rec), "--select", "all", "--backend", "stub", "--concurrency", "2"]
            clean = {k: "" for k in ("KB_STUB_WRITE_BAD", "KB_STUB_WRITE_RETRY", "KB_STUB_WRITE_GATE", "KB_STUB_CHECK_BAD", "KB_STUB_WRONG_MODEL_AFTER")}
            self.assertEqual(self._run(W.main, base + ["--out-dir", str(d / "w")], dict(clean, KB_STUB_STOP_AFTER="3")), 1)
            self.assertEqual(self._run(W.main, base + ["--out-dir", str(d / "w")], dict(clean, KB_STUB_STOP_AFTER="")), 0)
            cb = ["--slice", str(d / "slice.jsonl"), "--record", str(rec), "--write-dir", str(d / "w"), "--out-dir", str(d / "c"), "--backend", "stub"]
            self.assertEqual(self._run(C.main, cb, dict(clean, KB_STUB_STOP_AFTER="3")), 1)
            self.assertEqual(self._run(C.main, cb, dict(clean, KB_STUB_WRONG_MODEL_AFTER="")), 0)


def reading_file(w, notok=1):
    """A sanity reading's result for a dry run: every id of the sample, the first NOTOK accepted ones rated partial."""
    key = json.loads(Path(w, "sanity/reading/sanity_reading_key.json").read_text())
    acc = [i for i in key["order"] if key["checker_verdict"][i] == "ok"][:notok]
    f = Path(w).parent / "reading.jsonl"
    f.write_text("".join(json.dumps({"id": i, "rating": "partial" if i in acc else "ok", "reason": "dry"}) + "\n" for i in key["order"]))
    return str(f)


@unittest.skipUnless(HAVE_INDEX, "the frozen index or the venv is not here")
class DryRun(unittest.TestCase):
    """The chain end to end on the stub server: sanity with plants, a second invocation, the full run with a planted failure and its resume."""

    def setUp(self):
        self.home = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.home.cleanup()

    def sh(self, *args, ok=(0,), limit=300):
        """The chain in its own process group, stdin closed, under a time limit that kills the whole group (so a stuck grandchild
        cannot keep the pipes open), with an environment that cannot reach the real server: an empty HOME (no
        ~/.config/kafka-sre-llm), an unresolvable OMLX_URL, no key, no proxies."""
        env = {k: v for k, v in os.environ.items() if not k.upper().endswith("_PROXY") and k not in ("OMLX_API_KEY", "JUDGE_API_KEY")}
        env.update({"HOME": self.home.name, "OMLX_URL": "http://stub.invalid/v1", "OMLX_API_KEY": "", "HF_HUB_OFFLINE": "1",
                    "HF_HOME": os.environ.get("HF_HOME") or str(Path.home() / ".cache" / "huggingface")})
        p = subprocess.Popen(["sh", "scripts/m7_kb2.sh", *args], cwd=ROOT, env=env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, text=True, start_new_session=True)
        try:
            out, err = p.communicate(timeout=limit)
        except subprocess.TimeoutExpired:
            os.killpg(p.pid, signal.SIGKILL)
            out, err = p.communicate()
            self.fail(f"the chain did not finish within {limit} s: {' '.join(args)}\n{out[-2000:]}{err[-2000:]}")
        r = subprocess.CompletedProcess(p.args, p.returncode, out, err)
        self.assertIn(r.returncode, ok, r.stdout[-3000:] + r.stderr[-3000:])
        return r

    def tree(self, d):
        return {str(p.relative_to(d)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(Path(d).rglob("*")) if p.is_file()}

    def test_end_to_end(self):
        with tempfile.TemporaryDirectory() as d:
            w = str(Path(d) / "kb")
            r = self.sh("--sanity", "--dry-run", "--work", w, "--plant-write-bad", "946a872e4a53c017", "--plant-check-bad", "b9ab24266d42b64c",
                        "--plant-stop", "20", ok=(1,))
            self.assertIn("the writer", r.stdout)
            self.assertIn("SERVER ERROR", Path(w, "logs/m7_kb2.sanity.write.log").read_text())   # a window of a server error prints a line
            r = self.sh("--sanity", "--dry-run", "--work", w, "--plant-write-bad", "946a872e4a53c017", "--plant-check-bad", "b9ab24266d42b64c")
            self.assertIn("rerun after a stop", r.stdout)
            self.assertIn("SANITY (mechanical parts): PASS", r.stdout)
            self.assertTrue(r.stdout.rstrip().endswith("KB2_PILOT_from_stub_data.md"))
            self.assertIn("**Title.**", Path(w, "sanity/reading/sanity_reading.md").read_text())
            before = self.tree(w)
            self.sh("--sanity", "--dry-run", "--work", w)
            self.assertEqual(self.tree(w), before)                              # a second invocation changes nothing
            self.sh("--full", "--dry-run", "--work", w, ok=(2,))                 # no reading file
            rf = reading_file(w, notok=5)
            r = self.sh("--full", "--dry-run", "--work", w, "--sanity-reading", rf, ok=(2,))
            self.assertIn("rated 5 of 40", r.stderr)
            rf = reading_file(w, notok=1)
            r = self.sh("--full", "--dry-run", "--work", w, "--sanity-reading", rf, "--fail-after", "index", ok=(1,))
            self.assertIn("planted failure after the stage index", r.stdout)
            wlog = Path(w, "logs/m7_kb2.full.write.log").read_text()
            self.assertEqual(wlog.count(" reused"), 50)
            r = self.sh("--full", "--dry-run", "--work", w, "--sanity-reading", rf)
            self.assertIn("5 stage(s) done", r.stdout)                         # slice, header, write, check, index kept
            self.assertNotIn("[write]", r.stdout)
            self.assertIn("stage audit done", r.stdout)
            self.assertEqual(Path(w, "logs/m7_kb2.full.write.log").read_text(), wlog)
            before = self.tree(w)
            self.sh("--full", "--dry-run", "--work", w, "--sanity-reading", rf)
            self.assertEqual(self.tree(w), before)
            rep = Path(w, "KB2_PILOT_from_stub_data.md").read_text()
            self.assertIn("## Sanity run (50 windows, selection sanity2)", rep)
            self.assertIn("## Full run (1,487 windows)", rep)
            self.assertIn(hashlib.sha256(Path(rf).read_bytes()).hexdigest(), rep)
            self.assertIn("rated 1 of 40 accepted entries not ok", rep)
            hb = Path(w, "HEARTBEAT.md").read_text()
            self.assertIn(f"loading {DS}", hb)
            self.assertIn(f"unloaded {GLM8}", hb)
            self.assertNotIn("W16", hb)
            r = self.sh("--full", "--dry-run", "--work", w, "--sanity-reading", reading_file(w, notok=2), ok=(2,))   # another reading file: refused
            self.assertIn("sanity_reading_sha256", r.stderr)

    def test_failed_sanity_rule_ends_failed(self):
        with tempfile.TemporaryDirectory() as d:
            w = str(Path(d) / "kb")
            r = self.sh("--sanity", "--dry-run", "--work", w, "--plant-write-bad", "946a872e4a53c017,0345ef65d5924a85,efe1a319d6619f88", ok=(1,))
            self.assertIn("the sanity run failed its rule", r.stdout)
            self.assertTrue(Path(w, "logs/m7_kb2.sanity.failed").exists())
            self.assertFalse(Path(w, "logs/m7_kb2.sanity.done").exists())
            self.assertIn("## Sanity run", Path(w, "KB2_PILOT_from_stub_data.md").read_text())
            r = self.sh("--sanity", "--dry-run", "--work", w, ok=(1,))          # it stays failed
            self.assertFalse(Path(w, "logs/m7_kb2.sanity.done").exists())

    def test_server_paths(self):
        with tempfile.TemporaryDirectory() as d:
            w = str(Path(d) / "a")                                              # the writer uses another client's DeepSeek; the checker unloads it (W16)
            self.sh("--sanity", "--dry-run", "--work", w, "--plant-foreign", DS)
            hb = Path(w, "HEARTBEAT.md").read_text()
            self.assertIn("the writer uses that copy as it is, without this chain's marker", hb)
            self.assertNotIn(f"loading {DS}", hb)
            self.assertIn("is left loaded", hb)
            self.assertIn(f"W16: {DS} is loaded by another client", hb)
            self.assertIn(f"unloaded {DS}: W16's unload of a foreign model", hb)
            self.assertLess(hb.index("is left loaded"), hb.index(f"unloaded {DS}: W16"))
            for name, plant in (("b", ["--plant-foreign", GLM4]), ("c", ["--plant-busy", "1"]), ("e", ["--plant-foreign", DS, "--plant-busy", "1"])):
                w = str(Path(d) / name)
                r = self.sh("--sanity", "--dry-run", "--work", w, *plant, ok=(2,))
                self.assertIn("the server is not free for this chain", r.stderr)
                self.assertEqual(sorted(os.listdir(w)), ["stub_server_status.json"])   # nothing else written
            w = str(Path(d) / "f")                                              # another model answers mid-stage: refused, not retried
            r = self.sh("--sanity", "--dry-run", "--work", w, "--plant-wrong-model", "10", ok=(2,))
            self.assertIn("the writer", r.stderr)
            log = Path(w, "logs/m7_kb2.sanity.write.log").read_text()
            self.assertIn("answered with model 'Another-Clients-Model'", log)
            self.assertEqual(log.count("GUARD: ") , 1)                          # one attempt, no restart
            self.assertTrue(Path(w, "logs/m7_kb2.sanity.failed").exists())
            r = self.sh("--sanity", "--dry-run", "--work", str(Path(d) / "g"), "--checker-model", GLM4, ok=(2,))
            self.assertIn("--fallback-note", r.stderr)
            self.sh("--sanity", "--dry-run", "--work", "work/m7/kb", ok=(2,))
            r = self.sh("--sanity", "--dry-run", "--work", str(Path(d) / "with space"), ok=(2,))
            self.assertIn("without white space", r.stderr)
            self.assertFalse(Path(d, "with space").exists())
            r = self.sh("--sanity", "--plant-busy", "1", ok=(2,))
            self.assertIn("dry runs only", r.stderr)


if __name__ == "__main__":
    unittest.main()
