"""eval2.py answer --allow-commit-change: a resume of an unfinished answers file after a commit. Stub generator, mocked git_info."""
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


if __name__ == "__main__":
    unittest.main()
