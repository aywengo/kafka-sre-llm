"""Offline tests of scripts/laptop_bench.py: every phase on the stub backend, the parsers on canned macOS output, the
scrubber on synthetic input, resume, and the id file against the question files. No model, no network."""
import contextlib
import io
import json
import os
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import laptop_bench as lb  # noqa: E402

VM_STAT = """Mach Virtual Memory Statistics: (page size of 16384 bytes)
Pages free:                                 100000.
Pages active:                               200000.
Pages inactive:                             150000.
Pages speculative:                           10000.
Pages wired down:                           300000.
Pages purgeable:                             20000.
"Anonymous pages":                          400000.
Pages occupied by compressor:                50000.
Swapins:                                       100.
Swapouts:                                      200.
"""
SWAP = "total = 3072.00M  used = 2063.88M  free = 1008.12M  (encrypted)"
BATT_AC = "Now drawing from 'AC Power'\n -InternalBattery-0 (id=12345)\t85%; charging; 0:40 remaining present: true\n"
BATT_BAT = "Now drawing from 'Battery Power'\n -InternalBattery-0 (id=12345)\t61%; discharging; 3:10 remaining present: true\n"
THERM = "Note: No thermal warning level has been recorded\nNote: No performance warning level has been recorded\nCPU_Speed_Limit \t= 100\n"
def ip(*parts):
    return ".".join(str(x) for x in parts)


LAN, PEER, LOOP = ip(10, 1, 2, 3), ip(93, 184, 216, 34), ip(127, 0, 0, 1)
LSOF = (f"python 10 u IPv4 0x1 0t0 TCP {LOOP}:8765->{LOOP}:50211 (ESTABLISHED)\n"
        f"python 10 u IPv4 0x2 0t0 TCP {LAN}:50000->{PEER}:443 (ESTABLISHED)\n"
        "python 10 u IPv6 0x3 0t0 TCP [::1]:8765->[::1]:50212 (ESTABLISHED)\n")


def fake_sh(cmd, timeout=20):
    c = " ".join(cmd)
    if cmd[0] == "vm_stat":
        return VM_STAT
    if "vm.swapusage" in c:
        return SWAP
    if "pressure_level" in c:
        return "1\n"
    if cmd[0] == "memory_pressure":
        return "System-wide memory free percentage: 64%\n"
    if cmd[:2] == ["pmset", "-g"]:
        return THERM if cmd[2] == "therm" else BATT_AC
    if cmd[0] == "ps":
        return f"1 0 1000 launchd\n{os.getpid()} 1 2097152 python\n"
    if cmd[0] == "lsof":
        return ""
    if cmd[0] == "route":
        return None  # no default route: the network is off
    if "machdep.cpu.brand_string" in c:
        return "Apple M3 Pro\n"
    if "hw.memsize" in c:
        return str(36 * 2 ** 30) + "\n"
    if "hw.ncpu" in c:
        return "12\n"
    if "perflevel0" in c:
        return "6\n"
    if "perflevel1" in c:
        return "6\n"
    if cmd[0] == "sw_vers":
        return "27.2\n"
    return None


TERMS = {"user": ["jdoe"], "host": ["Janes-MacBook-Pro", "Janes-MacBook-Pro.local"],
         "path": ["/Users/jdoe/code/kafka-sre-llm", "/Users/jdoe"], "serial": ["C02ABC123XYZ"], "ssid": ["HomeNet 5G"]}


class Quiet:
    def __enter__(self):
        self.cm = contextlib.redirect_stdout(io.StringIO())
        self.buf = self.cm.__enter__()
        return self.buf

    def __exit__(self, *a):
        self.cm.__exit__(*a)


class Parsers(unittest.TestCase):
    def test_vm_stat(self):
        v = lb.parse_vm_stat(VM_STAT)
        # (400000 - 20000 + 300000 + 50000) pages of 16 KiB
        self.assertAlmostEqual(v["used_gb"], 730000 * 16384 / 2 ** 30, places=2)
        self.assertEqual((v["swapins"], v["swapouts"]), (100, 200))

    def test_swap_pressure_power(self):
        self.assertEqual(lb.parse_swapusage(SWAP), 2063.88)
        self.assertEqual(lb.parse_swapusage("total = 4.00G  used = 1.50G  free = 2.50G"), 1536.0)
        self.assertEqual(lb.parse_free_pct("System-wide memory free percentage: 42%"), 42)
        self.assertEqual(lb.parse_pmset_batt(BATT_AC), {"source": "ac", "percent": 85, "state": "charging"})
        self.assertEqual(lb.parse_pmset_batt(BATT_BAT)["source"], "battery")
        self.assertEqual(lb.parse_pmset_therm(THERM), {"thermal_warning": False, "performance_warning": False, "cpu_speed_limit": 100})
        warn = lb.parse_pmset_therm("Note: Thermal warning level recorded: 70\nCPU_Speed_Limit = 80\n")
        self.assertTrue(warn["thermal_warning"])
        self.assertEqual(warn["cpu_speed_limit"], 80)

    def test_process_tree_and_sockets(self):
        t = lb.parse_ps("1 0 100 launchd\n10 1 2048 python\n11 10 1024 child\n12 1 50 other\n")
        self.assertEqual(lb.tree_rss_mb(t, 10), 3.0)
        self.assertIsNone(lb.tree_rss_mb(t, 99))
        self.assertEqual(lb.external_connections(LSOF), 1)


class Scrubber(unittest.TestCase):
    TEXT = ("host Janes-MacBook-Pro and janes-macbook-pro.local user jdoe at /Users/jdoe/code/kafka-sre-llm/out and /Users/jdoe/.cache; "
            f"serial C02ABC123XYZ ssid HomeNet 5G; ips {ip(192, 168, 1, 23)} {LOOP} fe80::1c2a:3bff:fe4d:5e6f 2001:db8:0:0:0:0:0:1; "
            "mac aa:bb:cc:11:22:33 AA-BB-CC-11-22-33; uuid 123E4567-E89B-12D3-A456-426614174000; tmp /private/var/folders/ab/cd/T/x; "
            "other /Users/someone/else")

    def test_removes_identifying_strings(self):
        sc = lb.Scrubber(TERMS)
        out = sc.text(self.TEXT)
        for bad in ("Janes", "janes", "jdoe", "C02ABC123XYZ", "HomeNet", ip(192, 168), LOOP, "fe80", "2001:db8", "aa:bb",
                    "AA-BB", "123E4567", "var/folders", "someone", "/Users/"):
            self.assertNotIn(bad, out, bad)
        self.assertEqual(sc.leftovers(out), [])
        self.assertTrue(sc.leftovers(self.TEXT))

    def test_leaves_measurements_alone(self):
        sc = lb.Scrubber(TERMS)
        s = "median 41.2 s at 12:34:56, kafka-consumer-groups.sh --describe, version 3.9.0, memory 18.4 GB"
        self.assertEqual(sc.text(s), s)

    def test_objects_and_keys(self):
        sc = lb.Scrubber(TERMS)
        o = sc.obj({"a": ["/Users/jdoe/x", {"b": "ip " + LAN}], "n": 3})
        self.assertNotIn("jdoe", json.dumps(o))
        self.assertNotIn(LAN, json.dumps(o))
        self.assertEqual(o["n"], 3)


class Ids(unittest.TestCase):
    def test_id_lists_match_items(self):
        ids = lb.load_ids()
        for part, n in (("speed", 30), ("score", 60)):
            items = lb.load_items(ids[part]["file"])
            self.assertEqual(len(ids[part]["ids"]), n)
            self.assertEqual(len(set(ids[part]["ids"])), n)
            self.assertTrue(set(ids[part]["ids"]) <= set(items), part)
            self.assertNotIn("hidden", ids[part]["file"])
        ref = ids["score"]["reference"]
        self.assertEqual(set(ref["pass"]), set(ids["score"]["ids"]))
        self.assertEqual(set(ref["answer_sha256"]), set(ids["score"]["ids"]))
        self.assertEqual(ref["all_pass_count"], sum(ref["pass"].values()))
        kinds = {lb.load_items(ids["score"]["file"])[i]["kind"] for i in ids["score"]["ids"]}
        self.assertGreaterEqual(len(kinds), 8)


class Phases(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.out = str(Path(self.tmp.name) / "o")
        self.p = mock.patch.object(lb, "sh", fake_sh)
        self.p.start()

    def tearDown(self):
        self.p.stop()
        self.tmp.cleanup()

    def run_phase(self, *args):
        with Quiet() as buf:
            rc = lb.main(["--stub", "--out", self.out, "--interval", "0.05", *args])
        return rc, buf.getvalue()

    def test_every_phase_and_the_summary(self):
        for args in (["preflight"], ["install-check", "--run"], ["speed", "--n-service", "3", "--n-oneshot", "2"],
                     ["sustained", "--label", "ac", "--n", "12"], ["sustained", "--label", "battery", "--n", "12", "--force"],
                     ["speed", "--label", "working-laptop", "--conds", "service-default", "--n-service", "3", "--apps", "browser, IDE"],
                     ["memory"], ["score"]):
            rc, out = self.run_phase(*args)
            self.assertEqual(rc, 0, (args, out))
        o = Path(self.out)
        self.assertEqual(len(lb.read_jsonl(o / "speed/baseline/rows.jsonl")), 3 * 3 + 3 * 2)
        self.assertEqual(len(lb.read_jsonl(o / "sustained/ac/rows.jsonl")), 12)
        self.assertGreaterEqual(len(lb.read_jsonl(o / "sustained/ac/power.jsonl")), 4)  # before, during x2, after
        self.assertTrue(list((o / "speed/baseline").glob("samples_*.jsonl")))
        cmp = json.loads((o / "score/compare.json").read_text())
        self.assertEqual(cmp["n"], 60)
        rc, out = self.run_phase("bundle", "--scrub-terms", json.dumps(TERMS))
        self.assertEqual(rc, 0, out)
        md = (o / "SUMMARY.md").read_text()
        self.assertIn("STUB RUN", md)
        for name in ("Installation", "Offline", "Memory held", "Time to a complete answer", "Time to the first words", "Sustained use, ac",
                     "Sustained use, battery", "A working laptop", "Same answers as the reference machine", "Cost of --check", "Cold start"):
            self.assertIn(name, md)
        self.assertTrue((o / "send_back.zip").exists())
        with zipfile.ZipFile(o / "send_back.zip") as z:
            self.assertIn("SUMMARY.md", z.namelist())

    def test_resume_skips_finished_items(self):
        args = ["speed", "--conds", "service-default", "--n-service", "3"]
        self.run_phase(*args)
        rows = lb.read_jsonl(Path(self.out) / "speed/baseline/rows.jsonl")
        self.assertEqual(len(rows), 3)
        rc, out = self.run_phase(*args)
        self.assertIn("all 3 done, skipped", out)
        self.assertEqual(len(lb.read_jsonl(Path(self.out) / "speed/baseline/rows.jsonl")), 3)
        rc, out = self.run_phase("speed", "--conds", "service-default", "--n-service", "5")
        self.assertEqual(len(lb.read_jsonl(Path(self.out) / "speed/baseline/rows.jsonl")), 5)
        self.assertEqual(out.count("service-default"), 2)  # only the two new questions ran
        self.run_phase("install-check", "--run")
        rc, out = self.run_phase("install-check", "--run")
        self.assertIn("already done", out)

    def test_bundle_scrubs_planted_identifiers(self):
        self.run_phase("preflight")
        o = Path(self.out)
        (o / "speed").mkdir(exist_ok=True)
        (o / "speed/service_x.log").write_text(f"ready on {ip(192, 168, 1, 5)} for Janes-MacBook-Pro at /Users/jdoe/code/kafka-sre-llm/x aa:bb:cc:11:22:33\n")
        cfg = json.loads((o / "config.json").read_text())
        cfg["weights_dir"] = "/Users/jdoe/models/Qwen"
        (o / "config.json").write_text(json.dumps(cfg))
        rc, _ = self.run_phase("bundle", "--scrub-terms", json.dumps(TERMS))
        self.assertEqual(rc, 0)
        blob = "".join(f.read_text() for f in (o / "send_back").rglob("*") if f.is_file())
        for bad in (ip(192, 168, 1, 5), "Janes", "jdoe", "aa:bb:cc"):
            self.assertNotIn(bad, blob)
        self.assertIn("<ip>", blob)


class Runtime(unittest.TestCase):
    def test_child_processes_run_offline(self):
        env = lb.child_env()
        self.assertEqual(env["HF_HUB_OFFLINE"], "1")
        self.assertEqual(env["TRANSFORMERS_OFFLINE"], "1")

    def test_script_names_no_owner_or_address(self):
        src = (ROOT / "scripts/laptop_bench.py").read_text()
        self.assertNotIn("roman", src.lower())
        import re
        self.assertEqual([m for m in re.findall(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b", src) if m != LOOP], [])


if __name__ == "__main__":
    unittest.main()
