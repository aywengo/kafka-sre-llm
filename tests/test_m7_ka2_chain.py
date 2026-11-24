"""M7 KA2: the Z3 chain scripts/m7_ka2.sh and its helpers (scripts/m7_ka2_rule.py): the rule's rows (each failing alone), the prompt-equality
refusal, the lookup relation, the measures on a synthetic file, and stub dry runs (the W16 paths, a planted failure and its resume, a second
invocation that changes nothing). The dry runs read the stored six-arm arm A (work/m7/six_arm) through a small subset copy; they are skipped
when it is not here."""
import contextlib
import hashlib
import io
import json
import os
import signal
import subprocess
import sys
import tempfile
import unittest
import unittest.mock
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
import m7_ka2_rule as R  # noqa: E402

DS, QID, G31, G26 = "DeepSeek-V4.1-Flash-1017-oQ4e-mtp", "Qwen3.8-27B-4bit", "gemma-4-31b-it-4bit", "gemma-4-26B-A4B-it-QAT-MLX-4bit"
STORED = ROOT / "work/m7/six_arm"
HAVE = all((ROOT / p).exists() for p in ("work/v3/chunks_s1.jsonl", "runs/v3/INDEX_FROZEN.json", ".venv/bin/python", "work/m4/catalogues/CATALOGUES.json",
                                         "work/m4/s6/ans_A1.jsonl", "work/m4/e2/ans_O_Q4D_a3.jsonl")) and (STORED / "ans_d5_A_gemma31.jsonl").exists()


def run(args):
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = R.main(args)
    return code, out.getvalue(), err.getvalue()


def wl(p, rows):
    Path(p).write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")


class Syntax(unittest.TestCase):
    def test_sh_n(self):
        r = subprocess.run(["sh", "-n", str(ROOT / "scripts/m7_ka2.sh")], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stderr)

    def test_rule_text_names_the_bars(self):
        t = R.RULE_TEXT
        for s in ("upper end of the 90 % interval", "below 0", "above -3", "+1 s", "decides nothing about publishing", "STORED arm A", "W20"):
            self.assertIn(s, t)

    def test_call_counts(self):
        self.assertEqual(run(["call-counts", "--nd", "50", "--nma", "96", "--nm3", "20"])[1].strip(), "ans_l_qwen=166 ans_l_gemma31=166 judge=200")


def measure(ident=(-3.0, 0.5), mech=(1.0, 1.0), time_med=0.4):
    """A model's measures with the decided parts set: ident (mean, se) per 10, mech (mean, se) in points, the paired time median."""
    def p(m, s):
        return {"mean": m, "se": s, "n": 50, "ci90": [m - R.Z90 * s, m + R.Z90 * s]}
    rows = {p_: {"cut": 0, "empty": 0} for p_ in R.PARTS}
    return {"model": "qwen", "idents": {"diff_per10": p(*ident)}, "mech": {"n": 116, "diff_pts": p(*mech), "diff_pts_without_lookup": p(0.0, 1.0)},
            "time": {"seconds_diff": {"median": time_med}}, "coverage": {"diff": p(0.01, 0.02)}, "rows": {"A": rows, "L": rows}}


class Rule(unittest.TestCase):
    def test_pass(self):
        r, c = R.evaluate(measure())
        self.assertEqual(r, "PASS")
        self.assertTrue(all(v[0] for v in c.values()))

    def test_each_row_fails_alone(self):
        for name, kw in (("identifiers", {"ident": (-0.5, 0.5)}), ("mechanical", {"mech": (-1.0, 1.5)}), ("time", {"time_med": 1.2})):
            r, c = R.evaluate(measure(**kw))
            self.assertEqual(r, "FAIL", name)
            self.assertEqual([k for k, v in c.items() if not v[0]], [name])

    def test_edges(self):
        self.assertEqual(R.evaluate(measure(ident=(-R.Z90, 1.0)))[0], "FAIL")       # upper end exactly 0: not below 0
        self.assertEqual(R.evaluate(measure(time_med=1.0))[0], "PASS")             # at most +1 s
        self.assertEqual(R.evaluate(measure(mech=(-3.0 + R.Z90, 1.0)))[0], "FAIL")  # lower end exactly -3: not above

    def test_missing_measure_is_an_error(self):
        e = measure()
        e["idents"]["diff_per10"]["ci90"] = None
        with self.assertRaises(R.KaError):
            R.evaluate(e)
        e = measure(time_med=None)
        with self.assertRaises(R.KaError):
            R.evaluate(e)

    def test_rule_command_and_last_line(self):
        with tempfile.TemporaryDirectory() as d:
            f = Path(d) / "m.json"
            g = measure(mech=(-2.0, 1.0))
            g["model"] = "gemma31"
            f.write_text(json.dumps({"stub": False, "models": {"qwen": measure(), "gemma31": g}}))
            code, out, _ = run(["rule", "--measures", str(f)])
            self.assertEqual(code, 0)
            self.assertEqual(out.strip().splitlines()[-1], "M7 KA2 LOOKUP CARDS (Z3) DONE: qwen PASS, gemma31 FAIL; decides nothing about publishing (W10, W13)")
            f.write_text(json.dumps({"stub": False, "models": {"qwen": measure()}}))
            self.assertEqual(run(["rule", "--measures", str(f)])[0], 2)


class RebuildAndRelation(unittest.TestCase):
    def rows(self, n=3, over=None):
        return [dict({"id": f"x{i}", "prompt_sha256": f"h{i}", "pack": [f"c{i}"]}, **(over or {}).get(i, {})) for i in range(n)]

    def test_prompt_equality_refusal(self):
        with tempfile.TemporaryDirectory() as d:
            rb, a, b = Path(d, "rb.jsonl"), Path(d, "a.jsonl"), Path(d, "b.jsonl")
            wl(rb, self.rows())
            wl(a, self.rows())
            wl(b, self.rows())
            self.assertEqual(run(["rebuild-check", "--rebuilt", str(rb), "--stored", str(a), str(b)])[0], 0)
            wl(b, self.rows(over={1: {"prompt_sha256": "other"}}))
            code, out, err = run(["rebuild-check", "--rebuilt", str(rb), "--stored", str(a), str(b)])
            self.assertEqual(code, 2)
            self.assertIn("b.jsonl x1", err)
            self.assertIn("REFUSED", err)
            wl(b, self.rows(over={2: {"pack": ["zz"]}}))
            self.assertEqual(run(["rebuild-check", "--rebuilt", str(rb), "--stored", str(a), str(b)])[0], 2)
            wl(b, self.rows(n=2))
            self.assertEqual(run(["rebuild-check", "--rebuilt", str(rb), "--stored", str(a), str(b)])[0], 2)

    def test_lookup_relation(self):
        def body(h, rest="r"):
            return {"draft_info": {"request_body_parts": {"messages_sha256": h, "rest_sha256": rest, "max_tokens": 2048, "enable_thinking": False}}}
        man = {"config": "a", "mode": "default", "unmeasured_reasons": ["x"], "extra_body": R.EB}
        lman = dict(man, config="l", unmeasured_reasons=["x", "lookup cards (EXPERIMENTAL)"], lookup_cards={"enabled": True, "terms": "question only (replayed packs)"})
        with tempfile.TemporaryDirectory() as d:
            A = [dict({"id": "x0", "prompt_sha256": "a0", "pack": ["c"]}, **body("a0")), dict({"id": "x1", "prompt_sha256": "a1", "pack": ["c"]}, **body("a1"))]
            RB = [{"id": "x0", "prompt_sha256": "l0", "lookup": {"cards": 2}}, {"id": "x1", "prompt_sha256": "a1", "lookup": {"cards": 0}}]
            L = [dict({"id": "x0", "prompt_sha256": "l0", "pack": ["c"], "lookup": {"cards": 2}}, **body("l0")),
                 dict({"id": "x1", "prompt_sha256": "a1", "pack": ["c"], "lookup": {"cards": 0}}, **body("a1"))]
            for n, rows in (("a", A), ("rb", RB), ("l", L)):
                wl(Path(d, f"{n}.jsonl"), rows)
            Path(d, "a.jsonl.manifest.json").write_text(json.dumps(man))
            Path(d, "l.jsonl.manifest.json").write_text(json.dumps(lman))
            args = ["lookup-relation", "--l", str(Path(d, "l.jsonl")), "--rebuilt", str(Path(d, "rb.jsonl")), "--a", str(Path(d, "a.jsonl")),
                    "--body-ref", str(Path(d, "a.jsonl")), "--ref-manifest", str(Path(d, "a.jsonl.manifest.json"))]
            code, out, _ = run(args)
            self.assertEqual(code, 0, out)
            for bad in ({"pack": ["d"]}, {"prompt_sha256": "zz"}, {"lookup": {"cards": 1}}, body("l0", rest="other"),
                        {"draft_info": dict(body("l0")["draft_info"], reasoning_tokens=5)}):
                wl(Path(d, "l.jsonl"), [dict(L[0], **bad), L[1]])
                self.assertEqual(run(args)[0], 1, bad)
            wl(Path(d, "l.jsonl"), L)
            Path(d, "l.jsonl.manifest.json").write_text(json.dumps(dict(lman, mode="strict")))
            code, out, _ = run(args)
            self.assertEqual(code, 1)
            self.assertIn("manifest: mode", out)


class Measures(unittest.TestCase):
    """The measures on a synthetic stored folder and work folder (the judge's coverage per answer is patched: G.judged is tested elsewhere)."""

    def build(self, d, stub=False, drop_tokens=False):
        S, W = Path(d, "stored"), Path(d, "work")
        S.mkdir()
        W.mkdir()
        ids = {"d5": ["d1", "d2", "d3", "d4"], "ma": ["m1", "m2", "m3"], "m3": ["t1"]}
        wl(S / "items_d5_h.jsonl", [{"id": i} for i in ids["d5"]])
        wl(S / "items_ma.jsonl", [{"id": "m1", "kind": "name_setting"}, {"id": "m2", "kind": "cmdline"}, {"id": "m3", "kind": "cmdline"}])
        wl(S / "items_m3.jsonl", [{"id": "t1", "kind": "cmdline"}])
        for model in R.MODELS:
            x = R.sfx(model)
            for p in R.PARTS:
                A, L = [], []
                for k, i in enumerate(ids[p]):
                    A.append({"id": i, "text": "a", "truncated": k == 0, "seconds": {"total": 10.0 + k},
                              "draft_info": {"prompt_tokens": 1000, "ttft_s": 2.0, "cached_prompt_tokens": 0}})
                    di = {"prompt_tokens": 1300, "ttft_s": 2.5, "cached_prompt_tokens": 0}
                    if drop_tokens and k == 0:
                        di["prompt_tokens"] = None
                    L.append({"id": i, "text": "" if (p == "d5" and k == 1) else "l", "truncated": False, "seconds": {"total": 10.5 + k},
                              "draft_info": di, "lookup": {"cards": k, "chars": 100 * k, "seconds": 0.001}})
                wl(S / f"ans_{p}_A{x}.jsonl", A)
                wl(W / f"ans_{p}_L{x}.jsonl", L)
                if p != "d5":
                    wl(S / f"score_{p}_A{x}.jsonl", [{"id": i, "all_pass": i in ("m1",), "format_ok": True} for i in ids[p]])
                    wl(W / f"score_{p}_L{x}.jsonl", [{"id": i, "all_pass": i in ("m1", "m2", "t1"), "format_ok": True} for i in ids[p]])
            (W / f"idcheck_d5_A{x}.json").write_text(json.dumps({"answers": [{"id": i, "nonexistent": n} for i, n in zip(ids["d5"], (2, 1, 0, 1))]}))
            (W / f"idcheck_d5_L{x}.json").write_text(json.dumps({"answers": [{"id": i, "nonexistent": n} for i, n in zip(ids["d5"], (0, 1, 0, 0))]}))
        return S, W

    def judged(self, path, rubrics, name, ids):
        return {i: (0.5 if name.startswith("A") else 0.6) for i in ids}

    def test_synthetic(self):
        with tempfile.TemporaryDirectory() as d, unittest.mock.patch.object(R, "judged_p", self.judged):
            S, W = self.build(d)
            code, out, err = run(["measures", "--work", str(W), "--stored", str(S), "--rubrics", "unused"])
            self.assertEqual(code, 0, err)
            m = json.loads((W / "measures.json").read_text())["models"]["qwen"]
            self.assertAlmostEqual(m["idents"]["A_per10"], 10.0)                    # 4 in 4 answers
            self.assertAlmostEqual(m["idents"]["L_per10"], 2.5)
            self.assertAlmostEqual(m["idents"]["diff_per10"]["mean"], -7.5)         # (-20, 0, 0, -10) / 4
            self.assertAlmostEqual(m["coverage"]["diff"]["mean"], 0.1)
            self.assertEqual((m["mech"]["n"], m["mech"]["A_pass"], m["mech"]["L_pass"]), (4, 1, 3))
            self.assertAlmostEqual(m["mech"]["diff_pts"]["mean"], 50.0)
            self.assertEqual(m["mech"]["n_without_lookup"], 3)                     # m1 is a lookup kind
            self.assertAlmostEqual(m["mech"]["diff_pts_without_lookup"]["mean"], 200.0 / 3)
            self.assertAlmostEqual(m["time"]["seconds_diff"]["median"], 0.5)
            self.assertAlmostEqual(m["time"]["prompt_tokens"]["diff"]["median"], 300)
            self.assertAlmostEqual(m["time"]["ttft_s"]["diff"]["p90"], 0.5)
            self.assertEqual(m["rows"]["A"]["d5"], {"cut": 1, "empty": 0})
            self.assertEqual(m["rows"]["L"]["d5"], {"cut": 0, "empty": 1})
            self.assertEqual(m["cards"]["d5"]["max"], 3)
            self.assertEqual(m["cards"]["all"]["with_cards"], 5)
            self.assertIn("nonexistent identifiers per 10 answers", (W / "measures.md").read_text())
            res, c = R.evaluate(m)                                                   # 4 answers: the identifier interval reaches above 0
            self.assertEqual((res, [k for k, v in c.items() if not v[0]]), ("FAIL", ["identifiers"]))

    def test_missing_prompt_tokens(self):
        with tempfile.TemporaryDirectory() as d, unittest.mock.patch.object(R, "judged_p", self.judged):
            S, W = self.build(d, drop_tokens=True)
            code, _, err = run(["measures", "--work", str(W), "--stored", str(S), "--rubrics", "unused"])
            self.assertEqual(code, 2)                                                # a real run: a missing measure is an error
            self.assertIn("prompt_tokens", err)
            code, _, _ = run(["measures", "--work", str(W), "--stored", str(S), "--rubrics", "unused", "--stub"])
            self.assertEqual(code, 0)                                                # a stub reports none: counted as not reported

    def test_ids_must_match(self):
        with tempfile.TemporaryDirectory() as d, unittest.mock.patch.object(R, "judged_p", self.judged):
            S, W = self.build(d)
            wl(W / "ans_m3_L.jsonl", [])
            self.assertEqual(run(["measures", "--work", str(W), "--stored", str(S), "--rubrics", "unused"])[0], 2)


class ServerGate(unittest.TestCase):
    """The gate is scripts/m7_kb2_rule.py's (tested there); here: it is reached through this chain's helper with W16 and the needs of this chain."""

    def gate(self, loaded, need):
        with tempfile.TemporaryDirectory() as d:
            s = Path(d) / "s.json"
            s.write_text(json.dumps({"active_requests": 0, "waiting_requests": 0, "loaded_models": loaded, "model_load_times": {m: 1.0 for m in loaded}}))
            return run(["server-gate", "--status", str(s), "--need", need, "--marker", f"{QID}={Path(d) / 'absent'}"])

    def test_paths(self):
        self.assertEqual(self.gate([DS], QID)[:2], (0, f"W16 {DS}\n"))            # it blocks an answer stage: W16's unload
        self.assertEqual(self.gate([DS], DS)[:2], (0, f"ADOPT {DS}\n"))           # the judge needs it: used as it is
        code, out, err = self.gate([G26], QID)
        self.assertEqual((code, out), (2, ""))
        self.assertIn("loaded by another client", err)
        self.assertEqual(self.gate([QID], QID)[0], 2)                              # another client's copy of the model we need is foreign too


@unittest.skipUnless(HAVE, "the stored six-arm arm A, the frozen index or the venv is not here")
class DryRun(unittest.TestCase):
    """The chain end to end on stub answers, a stub judge and a stub server status, on a two-item subset of the stored arm A."""

    def setUp(self):
        self.home = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.home.cleanup()

    def sh(self, *args, ok=(0,), limit=400):
        """As tests/test_m7_kb2_chain.py: its own process group, a time limit, an environment that cannot reach the real server."""
        env = {k: v for k, v in os.environ.items() if not k.upper().endswith("_PROXY") and k not in ("OMLX_API_KEY", "JUDGE_API_KEY")}
        env.update({"HOME": self.home.name, "OMLX_URL": "http://stub.invalid/v1", "OMLX_API_KEY": "", "HF_HUB_OFFLINE": "1",
                    "HF_HOME": os.environ.get("HF_HOME") or str(Path.home() / ".cache" / "huggingface")})
        p = subprocess.Popen(["sh", "scripts/m7_ka2.sh", *args], cwd=ROOT, env=env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
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

    def test_resume_and_second_invocation(self):
        with tempfile.TemporaryDirectory() as d:
            w = str(Path(d) / "ka2")
            r = self.sh("--dry-run", "--work", w, "--dry-items", "2", "--fail-after", "ans_l_qwen", ok=(1,))
            self.assertIn("planted failure after the stage ans_l_qwen", r.stdout)
            self.assertIn("calls ans_l_qwen: 6 (expected 6)", r.stdout)
            self.assertLess(r.stdout.index("**The rule of Z3"), r.stdout.index("[answer]") if "[answer]" in r.stdout else r.stdout.index("answers d5 qwen"))
            self.assertTrue(Path(w, "logs/m7_ka2.failed").exists())
            kept = {f: hashlib.sha256(Path(w, f).read_bytes()).hexdigest() for f in ("ans_d5_L.jsonl", "ans_ma_L.jsonl", "ans_m3_L.jsonl", "report_header.md")}
            r = self.sh("--dry-run", "--work", w, "--dry-items", "2")
            self.assertIn("rerun after a stop", r.stdout)
            self.assertIn("3 stage(s) done: ans_l_qwen, header, rebuild", r.stdout)
            self.assertEqual(Path(w, "HEARTBEAT.md").read_text().count("answers d5 of qwen started"), 1)
            self.assertEqual({f: hashlib.sha256(Path(w, f).read_bytes()).hexdigest() for f in kept}, kept)
            self.assertIn("calls judge: 8 (expected 8)", r.stdout)
            self.assertTrue(r.stdout.rstrip().endswith("decides nothing about publishing (W10, W13)"))
            self.assertFalse(Path(w, "logs/m7_ka2.failed").exists())
            self.assertTrue(Path(w, "logs/m7_ka2.done").exists())
            rep = Path(w, "M7_KA2_from_stub_data.md").read_text()
            self.assertTrue(rep.startswith(Path(w, "report_header.md").read_text()))
            self.assertIn("**The rule of Z3", rep)
            before = self.tree(w)
            self.sh("--dry-run", "--work", w, "--dry-items", "2")
            self.assertEqual(self.tree(w), before)                                  # a second invocation changes nothing

    def test_w16_paths(self):
        with tempfile.TemporaryDirectory() as d:
            w = str(Path(d) / "a")                     # a foreign DeepSeek at the start blocks the answers: W16's unload; again before the judge: used as it is
            self.sh("--dry-run", "--work", w, "--dry-items", "2", "--plant-foreign", DS, "--plant-load-before", f"judge={DS}")
            hb = Path(w, "HEARTBEAT.md").read_text()
            self.assertIn(f"W16: {DS} is loaded by another client and blocks this M7 chain", hb)
            self.assertIn(f"unloaded {DS}: W16's unload of a foreign model", hb)
            self.assertIn("the judge uses that copy as it is, without this chain's marker", hb)
            self.assertIn("is left loaded", hb)
            self.assertNotIn(f"loading {DS}", hb)
            self.assertLess(hb.index("W16's unload"), hb.index(f"loading {QID}"))
            self.assertIn(DS, json.loads(Path(w, "stub_server_status.json").read_text())["loaded_models"])
            for name, plant in (("b", ["--plant-foreign", G26]), ("c", ["--plant-busy", "1"])):
                w = str(Path(d) / name)
                r = self.sh("--dry-run", "--work", w, "--dry-items", "2", *plant, ok=(2,))
                self.assertIn("the server is not free for this chain", r.stderr)
                self.assertEqual(sorted(os.listdir(w)), ["stored", "stub_server_status.json"])   # nothing of the run written
            w = str(Path(d) / "e")                     # another client loads the model of the next stage: refused there, the finished stages kept
            r = self.sh("--dry-run", "--work", w, "--dry-items", "2", "--plant-load-before", f"ans_l_gemma31={G31}", ok=(2,))
            self.assertIn(f"the model {G31} is loaded by another client", r.stderr)
            self.assertTrue(Path(w, "stage_ans_l_qwen.done").exists())
            self.assertFalse(Path(w, "ans_d5_L_gemma31.jsonl").exists())
            self.assertTrue(Path(w, "logs/m7_ka2.failed").exists())

    def test_prompt_difference_refuses_before_any_call(self):
        with tempfile.TemporaryDirectory() as d:
            s = Path(d) / "stored"
            self.assertEqual(run(["dry-stored", "--src", str(STORED), "--out", str(s), "--n", "2"])[0], 0)
            f = s / "ans_ma_A_gemma31.jsonl"
            rows = [json.loads(l) for l in f.read_text().splitlines()]
            rows[1]["prompt_sha256"] = "0" * 64
            wl(f, rows)
            w = str(Path(d) / "w")
            r = self.sh("--dry-run", "--work", w, "--stored", str(s), ok=(2,))
            self.assertIn("not rebuilt identically", r.stderr)
            self.assertIn(f"ans_ma_A_gemma31.jsonl {rows[1]['id']}", r.stderr)
            self.assertFalse(list(Path(w).glob("ans_*_L*")))
            self.assertFalse(Path(w, "stage_rebuild.done").exists())
            self.assertFalse(Path(w, "loaded_answer_qwen").exists())

    def test_refusals(self):
        r = self.sh("--plant-busy", "1", ok=(2,))
        self.assertIn("dry runs only", r.stderr)
        r = self.sh("--stored", "x", ok=(2,))
        self.assertIn("dry runs only", r.stderr)
        self.sh("--dry-run", "--work", "work/m7/ka2", ok=(2,))
        r = self.sh("--dry-run", "--work", "/tmp/with space", ok=(2,))
        self.assertIn("without white space", r.stderr)


if __name__ == "__main__":
    unittest.main()
