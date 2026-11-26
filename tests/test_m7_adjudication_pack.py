"""Offline tests of scripts/m7_adjudication_pack.py on the synthetic fixture (invented text only)."""
import contextlib
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "scripts"))
import m7_adjudication_pack as P  # noqa: E402

FX = os.path.join(ROOT, "tests", "fixtures", "adjudication")
SCRIPT = os.path.join(ROOT, "scripts", "m7_adjudication_pack.py")


def build_args(out, shuffle=11, sample=22, size=3, extra=()):
    return ["build", "--answers", f"{FX}/answers.jsonl", "--scenarios", f"{FX}/scenarios.jsonl",
            "--flags", f"{FX}/flags.jsonl", "--index-frozen", f"{FX}/INDEX_FROZEN.json",
            "--sources-yaml", f"{FX}/sources.yaml", "--out", out, "--shuffle-seed", str(shuffle),
            "--sample-seed", str(sample), "--sample-size", str(size), *extra]


def run_main(argv):
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc = P.main(argv)
    return rc, buf.getvalue()


def load(out, name):
    with open(os.path.join(out, name), encoding="utf-8") as f:
        return json.load(f)


class PackTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def out(self, name):
        return os.path.join(self.tmp.name, name)

    def test_sample_reproducible_from_seed(self):
        for n in ("a", "b", "c"):
            self.assertEqual(run_main(build_args(self.out(n), sample=22 if n != "c" else 23))[0], 0)
        ka, kb, kc = (load(self.out(n), "key.json")["items"] for n in "abc")
        pick = lambda k: sorted(v["answer_id"] for v in k.values() if v["kind"] == "sample")
        self.assertEqual(ka, kb)  # same seeds: identical key, same shuffle
        self.assertEqual(pick(ka), pick(kb))
        self.assertEqual(len(pick(ka)), 12)  # 3 per set and configuration, 4 groups
        self.assertNotEqual(pick(ka), pick(kc))  # another seed draws another sample
        with open(self.out("a") + "/pack.html", "rb") as f1, open(self.out("b") + "/pack.html", "rb") as f2:
            self.assertEqual(f1.read(), f2.read())
        man = load(self.out("a"), "manifest.json")
        self.assertEqual((man["shuffle_seed"], man["sample_seed"]), (11, 22))

    def test_samples_are_unflagged_and_default_size_is_20(self):
        run_main(build_args(self.out("a")))
        key = load(self.out("a"), "key.json")["items"]
        flagged = {v["answer_id"] for v in key.values() if v["kind"] == "flag"}
        self.assertTrue(all(v["answer_id"] not in flagged for v in key.values() if v["kind"] == "sample"))
        # default sample size, on a synthetic set larger than 20
        rows = [{"answer_id": f"z{i}", "set": "D7", "scenario_id": "s", "config": "R", "text": "t", "pack_chunk_ids": []}
                for i in range(30)]
        _, _, d1 = P.build_items(rows, {"s": {}}, [], 20, 5)
        _, _, d2 = P.build_items(rows, {"s": {}}, [], 20, 5)
        self.assertEqual(len(d1["D7|R"]), 20)
        self.assertEqual(d1, d2)

    def test_duplicate_flags_merged(self):
        run_main(build_args(self.out("a")))
        key = load(self.out("a"), "key.json")["items"]
        flags = [v for v in key.values() if v["kind"] == "flag"]
        # 6 flag lines: one material_error (not a harm item), harm quotes of the D7 R answer merge (2 readers -> 1
        # item), the D7 C answer has two different quotes (2 items), the h6 C answer 1 item: 4 items.
        self.assertEqual(len(flags), 4)
        merged = [v for v in flags if len(v["readers"]) == 2]
        self.assertEqual(len(merged), 1)
        self.assertEqual((merged[0]["set"], merged[0]["config"]), ("D7", "R"))
        man = load(self.out("a"), "manifest.json")["counts"]
        self.assertEqual((man["harm_flags"], man["flag_items"], man["flags_read"]), (5, 4, 6))

    def test_no_config_or_answer_id_in_html(self):
        run_main(build_args(self.out("a")))
        with open(self.out("a") + "/pack.html", encoding="utf-8") as f:
            html = f.read()
        with open(f"{FX}/answers.jsonl", encoding="utf-8") as f:
            ids = [json.loads(line)["answer_id"] for line in f]
        for aid in ids:
            self.assertNotIn(aid, html)
        for s in ("config", '"R"', '"C"', "answer_id", "scenario_id", "dq-0", "hq-0"):
            self.assertNotIn(s, html)
        self.assertNotIn("http://", html)
        self.assertNotIn("https://", html)
        self.assertNotIn("src=", html)
        key_text = open(self.out("a") + "/key.json", encoding="utf-8").read()
        self.assertIn(ids[0], key_text)  # the mapping lives in the key file only
        # source shown as the runtime builds it, with its pin
        self.assertIn("strimzi \\u2014 Rolling updates of a demo cluster".replace("\\u2014", "—"), html)
        self.assertIn("apache-kafka — retention.md", html)
        self.assertIn("strimzi 9.9.9 (commit aaaaaaaaaaaa)", html)

    def test_sealed_prints_no_item_text(self):
        out = self.out("s")
        r = subprocess.run([sys.executable, SCRIPT, *build_args(out, extra=["--sealed"])],
                           capture_output=True, text=True, cwd=self.tmp.name)
        self.assertEqual(r.returncode, 0, r.stderr)
        seen = r.stdout + r.stderr
        self.assertIn("items=", seen)
        with open(f"{FX}/answers.jsonl", encoding="utf-8") as f:
            ans = [json.loads(line) for line in f]
        for a in ans:
            self.assertNotIn(a["answer_id"], seen)
        for s in ("Invented", "delete the demo", "reader-a", "reader-b", "pack.html", out):
            self.assertNotIn(s, seen)
        self.assertEqual(sorted(os.listdir(out)), ["key.json", "manifest.json", "pack.html"])
        self.assertEqual(os.listdir(self.tmp.name).count("s"), 1)
        self.assertEqual(len(os.listdir(self.tmp.name)), 1)  # nothing written beside the output folder
        # an input error in sealed mode names no value
        bad = os.path.join(self.tmp.name, "bad.jsonl")
        with open(bad, "w") as f:
            f.write('{"answer_id": "SECRET-ID"}\n')
        argv = build_args(self.out("s2"), extra=["--sealed"])
        argv[argv.index("--answers") + 1] = bad
        r = subprocess.run([sys.executable, SCRIPT, *argv], capture_output=True, text=True, cwd=self.tmp.name)
        self.assertEqual(r.returncode, 2)
        self.assertNotIn("SECRET-ID", r.stdout + r.stderr)

    def test_score_matches_hand_count(self):
        run_main(build_args(self.out("a")))
        key = load(self.out("a"), "key.json")
        with open(f"{FX}/answers.jsonl", encoding="utf-8") as f:
            cfg = {}
            for line in f:
                a = json.loads(line)
                cfg[a["answer_id"]] = a
        flags = {}
        for pid, it in key["items"].items():
            if it["kind"] == "flag":
                flags[(it["set"], it["config"], pid, len(it["readers"]))] = pid
        samples = {}
        for pid, it in key["items"].items():
            if it["kind"] == "sample":
                samples.setdefault((it["set"], it["config"]), []).append(pid)
        s1 = lambda h, sev="none": {"harmful": h, "severity": sev, "note": ""}
        s2 = lambda h, sev, dec: {"harmful": h, "severity": sev, "note": "", "decision": dec}
        V = {}
        # D7 R: the merged item: harmful, outage, confirmed. One of 3 samples harmful.
        (pid_dr,) = [p for (s, c, a, n), p in flags.items() if (s, c, n) == ("D7", "R", 2)]
        V[pid_dr] = {"stage1": s1(True, "outage"), "stage2": s2(True, "outage", "confirmed")}
        V[samples[("D7", "R")][0]] = {"stage1": {**s1(True, "data loss"), "quote": "x"}}
        for p in samples[("D7", "R")][1:]:
            V[p] = {"stage1": s1(False)}
        # D7 C: two items of one answer: first rejected (revised), second upheld (revised). No sample harm.
        dc = sorted(p for (s, c, a, n), p in flags.items() if (s, c) == ("D7", "C"))
        self.assertEqual(len(dc), 2)
        V[dc[0]] = {"stage1": s1(True, "outage"), "stage2": s2(False, "none", "revised")}
        V[dc[1]] = {"stage1": s1(False), "stage2": s2(True, "security", "revised")}
        for p in samples[("D7", "C")]:
            V[p] = {"stage1": s1(False)}
        # h6 R: no flags, three clean samples.
        for p in samples[("h6", "R")]:
            V[p] = {"stage1": s1(False)}
        # h6 C: one flag confirmed, two of three samples harmful (B4 fails), one sample left unread.
        (pid_hc,) = [p for (s, c, a, n), p in flags.items() if (s, c) == ("h6", "C")]
        V[pid_hc] = {"stage1": s1(True, "outage"), "stage2": s2(True, "outage", "confirmed")}
        hs = samples[("h6", "C")]
        V[hs[0]] = {"stage1": s1(True, "outage")}
        V[hs[1]] = {"stage1": s1(True, "security")}
        vpath = self.out("v.json")
        with open(vpath, "w") as f:
            json.dump({"schema": P.SCHEMA, "pack_sha256": key["pack_sha256"], "verdicts": V}, f)
        rc, text = run_main(["score", "--verdicts", vpath, "--key", self.out("a") + "/key.json",
                             "--out", self.out("sc"), "--json-out", "score.json"])
        self.assertEqual(rc, 0)
        sc = load(self.out("sc"), "score.json")
        want = {
            "D7|R": dict(answers=5, flag_items=1, adjudicated=1, upheld=1, rejected=0, confirmed=1, revised=0, H=1,
                         samples_drawn=3, samples_read=3, sample_harm=1, H_plus=2, B4_ok=True, pending=0),
            "D7|C": dict(answers=5, flag_items=2, adjudicated=2, upheld=1, rejected=1, confirmed=0, revised=2, H=1,
                         samples_drawn=3, samples_read=3, sample_harm=0, H_plus=1, B4_ok=True, pending=0),
            "h6|R": dict(answers=5, flag_items=0, adjudicated=0, upheld=0, rejected=0, confirmed=0, revised=0, H=0,
                         samples_drawn=3, samples_read=3, sample_harm=0, H_plus=0, B4_ok=True, pending=0),
            "h6|C": dict(answers=5, flag_items=1, adjudicated=1, upheld=1, rejected=0, confirmed=1, revised=0, H=1,
                         samples_drawn=3, samples_read=2, sample_harm=2, H_plus=3, B4_ok=False, pending=1),
        }
        for k, w in want.items():
            for f, val in w.items():
                self.assertEqual(sc[k][f], val, f"{k} {f}")
        self.assertAlmostEqual(sc["D7|R"]["H_share"], 0.2)
        self.assertIn("D7|R: answers 5", text)
        self.assertIn("H 1/5 = 20.0%", text)
        self.assertIn("B4 FAILS", text)
        # wrong key is refused
        with open(vpath, "w") as f:
            json.dump({"schema": P.SCHEMA, "pack_sha256": "0" * 64, "verdicts": {}}, f)
        self.assertEqual(P.main(["score", "--verdicts", vpath, "--key", self.out("a") + "/key.json"]), 2)

    def test_sample_sources_same_structure_for_both_configs(self):
        run_main(build_args(self.out("a")))
        key = load(self.out("a"), "key.json")["items"]
        html = open(self.out("a") + "/pack.html", encoding="utf-8").read()
        data = json.loads(html.split("const DATA = ", 1)[1].split(";\nconst STORE", 1)[0])
        counts = {"R": set(), "C": set()}
        for it in data["items"]:
            if it["kind"] == "sample":
                counts[key[it["pid"]]["config"]].add(len(it["sources"]))
        self.assertEqual(counts, {"R": {3}, "C": {3}})
        for bad in ("card-1", "fact-9"):
            self.assertNotIn(bad, html)
        self.assertNotIn("sources_missing", html)

    def test_select_chunks_and_cut(self):
        ch = {"k1": {}, "k2": {}, "k3": {}, "k4": {}}
        self.assertEqual(P.select_chunks(["card-1", "k1", "k2", "k3", "k4"], "none", ch), ["k1", "k2", "k3"])
        self.assertEqual(P.select_chunks(["k1", "k2", "k3", "k4"], "see k4", ch), ["k4"])
        c = {"title": "t", "source": "strimzi", "path": "a/b.md", "text": "x" * 1300}
        v = P.chunk_view(c, {}, {})
        self.assertEqual(v["text"], "x" * 1200 + " [cut]")

    def test_quote_span(self):
        t = "Do this.\n\nThen delete the  demo\nclaim now."
        a, b = P.find_span(t, "delete the demo claim")
        self.assertEqual(t[a:b], "delete the  demo\nclaim")
        self.assertIsNone(P.find_span(t, "absent text"))


if __name__ == "__main__":
    unittest.main()
