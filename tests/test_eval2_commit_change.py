"""eval2.py answer --allow-commit-change: a resume of an unfinished answers file after a commit. Stub generator, mocked git_info."""
import contextlib
import io
import sys
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "tests"))
import eval2  # noqa: E402
from test_eval2 import Base, item  # noqa: E402


class CommitChange(Base):
    def run_at(self, commit, f, out, *extra):
        with mock.patch.object(eval2, "git_info", return_value=(commit, False)):
            return self.run_answer(f, out, *extra)

    def partial(self):
        f, out = self.items_file([item(i) for i in range(1, 5)]), self.tmp / "a.jsonl"
        self.run_at("c1" * 20, f, out)
        lines = out.read_text().splitlines()
        out.write_text("\n".join(lines[:2]) + "\n")   # killed after two items
        return f, out

    def test_refused_without_the_option_and_the_manifest_is_unchanged_when_absent(self):
        f, out = self.partial()
        with self.assertRaisesRegex(eval2.EvalError, r"differs from the existing run in \['runtime_git_commit'\]"):
            self.run_at("c2" * 20, f, out)
        m = eval2.read_manifest(out)
        self.assertNotIn("runtime_git_commits", m)
        self.assertEqual(m["runtime_git_commit"], "c1" * 20)

    def test_allowed_with_the_option_and_both_commits_are_recorded(self):
        f, out = self.partial()
        run1 = eval2.read_manifest(out)["run"]
        rc, text = self.run_at("c2" * 20, f, out, "--allow-commit-change")
        self.assertEqual(rc, 0)
        self.assertEqual(len(eval2.read_jsonl(out)), 4)
        m = eval2.read_manifest(out)
        self.assertEqual(m["runtime_git_commits"], ["c1" * 20, "c2" * 20])
        self.assertEqual(m["run"], run1)
        # a later resume on yet another commit adds it; one on the same commit adds nothing and is not refused
        out.write_text("\n".join(out.read_text().splitlines()[:3]) + "\n")
        self.run_at("c3" * 20, f, out, "--allow-commit-change")
        self.assertEqual(eval2.read_manifest(out)["runtime_git_commits"], ["c1" * 20, "c2" * 20, "c3" * 20])
        out.write_text("\n".join(out.read_text().splitlines()[:3]) + "\n")
        self.run_at("c3" * 20, f, out)
        self.assertEqual(eval2.read_manifest(out)["runtime_git_commits"], ["c1" * 20, "c2" * 20, "c3" * 20])

    def test_still_refused_with_the_option_when_another_field_differs(self):
        f, out = self.partial()
        with self.assertRaisesRegex(eval2.EvalError, "manifest differs"):
            self.run_at("c2" * 20, f, out, "--allow-commit-change", "--host-class", "another host")
        self.assertNotIn("runtime_git_commits", eval2.read_manifest(out))
        with self.assertRaisesRegex(eval2.EvalError, "manifest differs"):
            self.run_at("c1" * 20, f, out, "--allow-commit-change", "--host-class", "another host")


LISTING = {"id": "model-a", "object": "model", "owned_by": "omlx", "model_type": "llm", "max_model_len": 4096}


class ModelReload(Base):
    """M5 F1: a resume survives a reload of the same model (the server's load stamp `created` changes); a change of a field of the model itself refuses."""

    def run_with(self, listing, f, out, *extra, commit="c1" * 20):
        with mock.patch.object(eval2, "git_info", return_value=(commit, False)), mock.patch.object(eval2, "listed_model", return_value=listing):
            return self.run_answer(f, out, *extra)

    def partial(self, stamp=1000):
        f, out = self.items_file([item(i) for i in range(1, 5)]), self.tmp / "a.jsonl"
        self.run_with({**LISTING, "created": stamp}, f, out)
        out.write_text("\n".join(out.read_text().splitlines()[:2]) + "\n")   # killed after two items
        return f, out

    def test_a_different_load_stamp_with_the_same_model_fields_resumes_and_records_the_reload(self):
        f, out = self.partial(1000)
        run1 = eval2.read_manifest(out)["run"]
        self.assertNotIn("reloads", eval2.read_manifest(out))
        rc, text = self.run_with({**LISTING, "created": 2000}, f, out)
        self.assertEqual(rc, 0, text)
        self.assertEqual(len(eval2.read_jsonl(out)), 4)
        self.assertIn('model reloaded: {"created": 1000} -> {"created": 2000}', text)
        m = eval2.read_manifest(out)
        self.assertEqual([(r["old"], r["new"]) for r in m["reloads"]], [('{"created": 1000}', '{"created": 2000}')])
        self.assertEqual(m["run"], run1)
        self.assertEqual(m["model_build"]["created"], 2000)
        # a second reload adds to the list; a resume on the same stamp adds nothing and keeps the list
        out.write_text("\n".join(out.read_text().splitlines()[:3]) + "\n")
        self.run_with({**LISTING, "created": 3000}, f, out)
        self.assertEqual(len(eval2.read_manifest(out)["reloads"]), 2)
        out.write_text("\n".join(out.read_text().splitlines()[:3]) + "\n")
        rc, text = self.run_with({**LISTING, "created": 3000}, f, out)
        self.assertNotIn("model reloaded", text)
        self.assertEqual(len(eval2.read_manifest(out)["reloads"]), 2)

    def test_a_different_field_of_the_model_itself_refuses(self):
        for change in ({"id": "model-b"}, {"model_type": "vlm"}, {"max_model_len": 8192}, {"owned_by": "other"}):
            f, out = self.partial(1000)
            with self.assertRaisesRegex(eval2.EvalError, r"manifest differs from the existing run in \['model_"):
                self.run_with({**LISTING, "created": 2000, **change}, f, out)
            self.assertNotIn("reloads", eval2.read_manifest(out))
            out.unlink()
            Path(str(out) + ".manifest.json").unlink()

    def test_the_same_stamp_is_no_reload(self):
        f, out = self.partial(1000)
        rc, text = self.run_with({**LISTING, "created": 1000}, f, out)
        self.assertEqual(rc, 0)
        self.assertNotIn("model reloaded", text)
        self.assertNotIn("reloads", eval2.read_manifest(out))

    def test_the_run_id_does_not_depend_on_the_load_stamp(self):
        f, out = self.partial(1000)
        m = dict(eval2.read_manifest(out))
        m["model_build"] = {**m["model_build"], "created": 9999}
        self.assertEqual(eval2.run_id(m), eval2.read_manifest(out)["run"])

    def test_a_reload_and_a_commit_change_together_resume_with_the_option(self):
        f, out = self.partial(1000)
        rc, text = self.run_with({**LISTING, "created": 2000}, f, out, "--allow-commit-change", commit="c2" * 20)
        self.assertEqual(rc, 0, text)
        m = eval2.read_manifest(out)
        self.assertEqual(m["runtime_git_commits"], ["c1" * 20, "c2" * 20])
        self.assertEqual(len(m["reloads"]), 1)

    def test_the_chains_runlines_print_the_number_of_reloads(self):
        f, out = self.partial(1000)
        self.run_with({**LISTING, "created": 2000}, f, out)
        import subprocess
        e5 = subprocess.run([sys.executable, str(ROOT / "scripts/m4_e5_screen_helper.py"), "runline", "--name", "x", "--file", str(out)],
                            capture_output=True, text=True, cwd=ROOT)
        self.assertEqual(e5.returncode, 0, e5.stderr)
        self.assertIn("1 model reloads", e5.stdout)
        import m4_e2_manifest as mm
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            mm.cmd_runline(mock.Mock(file=str(out), name="x"))
        self.assertIn("1 model reloads", buf.getvalue())


class ReviseCommitChange(Base):
    """M5 F1 defect 4: the revise step (the inline marks, `answer --revise-from`) takes --allow-commit-change like the answer step."""

    def setUp(self):
        super().setUp()
        self.items = self.items_file([item(i) for i in range(1, 4)])
        eval2.write_jsonl(self.tmp / "ans.jsonl", [{"id": f"i{i:03d}", "text": "t", "pack": ["c1"], "error": None, "run": "r"} for i in range(1, 4)])
        reviser = mock.Mock()
        reviser.revise.return_value = {"flagged": 0, "first_pass_text": "t", "final_text": "t", "r2": {"status": "off"}}
        checker = mock.Mock(catalogue_hash="h")
        for target, kw in (("kafka_sre_rag.service.AnswerChecker", {"return_value": checker}), ("kafka_sre_rag.revise.Reviser", {"return_value": reviser})):
            m = mock.patch(target, **kw)
            m.start()
            self.addCleanup(m.stop)

    def run_rev(self, commit, *extra):
        a = eval2.build_parser().parse_args(["answer", "--items", str(self.items), "--config", "rev", "--revise-from", str(self.tmp / "ans.jsonl"),
                                             "--catalogue", str(self.tmp), "--backend", "stub", *extra])
        buf = io.StringIO()
        with mock.patch.object(eval2, "git_info", return_value=(commit, False)):
            rc = eval2.cmd_answer(a, out=buf)
        return rc, buf.getvalue()

    def partial(self):
        self.run_rev("c1" * 20)
        out = self.tmp / "ans.revise.jsonl"
        out.write_text("\n".join(out.read_text().splitlines()[:1]) + "\n")
        return out

    def test_refused_without_the_option(self):
        self.partial()
        with self.assertRaisesRegex(eval2.EvalError, "manifest differs from the existing records file"):
            self.run_rev("c2" * 20)

    def test_resumes_with_the_option_and_records_both_commits(self):
        out = self.partial()
        run1 = eval2.read_manifest(out)["run"]
        rc, text = self.run_rev("c2" * 20, "--allow-commit-change")
        self.assertEqual(rc, 0, text)
        self.assertEqual(len(eval2.read_jsonl(out)), 3)
        m = eval2.read_manifest(out)
        self.assertEqual(m["runtime_git_commits"], ["c1" * 20, "c2" * 20])
        self.assertEqual(m["run"], run1)

    def test_still_refused_with_the_option_when_another_field_differs(self):
        self.partial()
        with self.assertRaisesRegex(eval2.EvalError, "manifest differs"):
            self.run_rev("c2" * 20, "--allow-commit-change", "--max-tokens", "77")


if __name__ == "__main__":
    unittest.main()
