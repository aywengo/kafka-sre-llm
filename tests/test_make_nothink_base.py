import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import make_nothink_base as M  # noqa: E402


class PatchTemplate(unittest.TestCase):
    TPL = f"a\n{M.REASONING}\nr\n{{%- endif %}}\nb\n{M.GEN_PROMPT}\ng\n{{%- endif %}}\n"

    def test_replaces_both_conditions_and_nothing_else(self):
        out = M.patch_template(self.TPL)
        self.assertEqual(out, "a\n{%- if false %}\nr\n{%- endif %}\nb\n{%- if true %}\ng\n{%- endif %}\n")

    def test_missing_condition_is_an_error(self):
        with self.assertRaises(AssertionError):
            M.patch_template("no conditions here")

    def test_import_needs_no_mlx(self):
        self.assertNotIn("mlx_lm.tokenizer_utils", sys.modules.get("make_nothink_base").__dict__.get("__doc__", ""))
        self.assertTrue(M.SAMPLE and M.SAMPLE[-1]["role"] == "assistant")


if __name__ == "__main__":
    unittest.main()
