import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import package_adapter as P  # noqa: E402

CONFIG = {"model": "runs/v1/base27b", "adapter_path": "adapters/x", "config": "runs/v2/lora.yaml",
          "data": "data_resume2", "resume_adapter_file": "adapters/y/0001050_adapters.safetensors",
          "fine_tune_type": "lora", "num_layers": -1, "lora_parameters": {"rank": 16, "scale": 20.0, "dropout": 0.05}}


class Fixture(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.t = Path(self._tmp.name)
        self.adir = self.t / "adapter"
        self.adir.mkdir()
        (self.adir / "adapters.safetensors").write_bytes(b"final weights")
        for step in ("0000050", "0000100"):
            (self.adir / f"{step}_adapters.safetensors").write_bytes(b"checkpoint " + step.encode())
        (self.adir / "adapter_config.json").write_text(json.dumps(CONFIG))
        self.card = self.t / "card.md"
        self.card.write_text("# card\nuse with excerpts\n")
        self.notice = self.t / "NOTICE.x"
        self.notice.write_text("notice\n")
        self.lic = self.t / "LICENSE"
        self.lic.write_text("Apache License\n")
        self.secrets = self.t / "secrets"
        self.secrets.mkdir()
        (self.secrets / "judge_key").write_text("sk-test-secret-value\n")
        self.out = self.t / "out" / "pkg"

    def tearDown(self):
        self._tmp.cleanup()

    def build(self, **kw):
        return P.build(self.adir, self.out, self.card, self.notice, self.lic, self.secrets, **kw)


class Build(Fixture):
    def test_only_the_two_adapter_files_are_copied(self):
        lines, changed = self.build()
        self.assertEqual({p.name for p in self.out.iterdir()}, P.ALLOWED)
        self.assertEqual((self.out / "adapters.safetensors").read_bytes(), b"final weights")
        self.assertEqual(len(lines), 5)

    def test_config_is_cleaned_and_the_source_is_untouched(self):
        _, changed = self.build()
        cfg = json.loads((self.out / "adapter_config.json").read_text())
        for k in P.LOCAL_KEYS:
            self.assertNotIn(k, cfg)
            self.assertTrue(any(c.startswith(k) for c in changed))
        self.assertEqual(cfg["model"], P.BASE_MODEL)
        self.assertEqual(cfg["lora_parameters"]["rank"], 16)
        self.assertEqual(json.loads((self.adir / "adapter_config.json").read_text()), CONFIG)

    def test_sha256sums_verify(self):
        self.build()
        lines = (self.out / "SHA256SUMS").read_text().splitlines()
        self.assertEqual(len(lines), 5)
        for line in lines:
            digest, name = line.split("  ", 1)
            self.assertEqual(hashlib.sha256((self.out / name).read_bytes()).hexdigest(), digest)
        self.assertNotIn("SHA256SUMS", (self.out / "SHA256SUMS").read_text())

    def test_extra_file_in_output_is_refused(self):
        self.out.mkdir(parents=True)
        (self.out / "0000050_adapters.safetensors").write_bytes(b"x")
        with self.assertRaises(P.PackageError):
            self.build()

    def test_rebuild_over_own_output_is_fine(self):
        self.build()
        self.build()
        self.assertEqual({p.name for p in self.out.iterdir()}, P.ALLOWED)


class Refusals(Fixture):
    def test_home_path_in_card(self):
        self.card.write_text("see /Users/someone/work\n")
        with self.assertRaises(P.PackageError) as c:
            self.build()
        self.assertIn("home path", str(c.exception))
        self.assertFalse(self.out.exists())

    def test_home_path_in_config_survives_cleaning(self):
        cfg = dict(CONFIG, note="/home/someone/x")
        (self.adir / "adapter_config.json").write_text(json.dumps(cfg))
        with self.assertRaises(P.PackageError):
            self.build()

    def test_ipv4_in_notice(self):
        self.notice.write_text("server 10.1.2.3 here\n")
        with self.assertRaises(P.PackageError) as c:
            self.build()
        self.assertIn("IPv4", str(c.exception))

    def test_version_numbers_are_not_ip_addresses(self):
        self.card.write_text("tag 4.3.2 and 1.3.1 and mlx 0.32.4\n")
        self.build()

    def test_secret_content_refused_and_not_printed(self):
        self.card.write_text("key sk-test-secret-value in text\n")
        with self.assertRaises(P.PackageError) as c:
            self.build()
        self.assertNotIn("sk-test-secret-value", str(c.exception))
        self.assertIn("judge_key", str(c.exception))


if __name__ == "__main__":
    unittest.main()
