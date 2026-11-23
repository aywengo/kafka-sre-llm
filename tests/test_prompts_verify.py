"""M7 P1: the three constants of the checking pass against tests/golden/prompts_verify.json, their placeholders, the
one difference between the two variants, and that they stay out of the frozen configuration."""
import hashlib
import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from kafka_sre_rag import config, prompts as P  # noqa: E402

GOLDEN = ROOT / "tests/golden/prompts_verify.json"


def h(s):
    return {"sha256": hashlib.sha256(s.encode("utf-8")).hexdigest(), "len": len(s)}


class VerifyPrompts(unittest.TestCase):
    def test_golden_file(self):
        gold = json.loads(GOLDEN.read_text())
        for n in ("VERIFY_SYSTEM", "VERIFY_PROMPT", "VERIFY_PROMPT_CARDS"):
            self.assertEqual(h(getattr(P, n)), gold[n], n)

    def test_placeholders(self):
        for name, t in (("VERIFY_PROMPT", P.VERIFY_PROMPT), ("VERIFY_PROMPT_CARDS", P.VERIFY_PROMPT_CARDS)):
            for ph in ("<<QUESTION>>", "<<EXCERPTS>>", "<<ANSWER>>", "<<FINDINGS>>"):
                self.assertEqual(t.count(ph), 1, (name, ph))
        self.assertNotIn("<<CARDS>>", P.VERIFY_PROMPT)
        self.assertEqual(P.VERIFY_PROMPT_CARDS.count("<<CARDS>>"), 1)

    def test_cards_variant_differs_only_where_the_brief_says(self):
        block = ("Reference cards from the pinned catalogues for names the draft uses (exact facts; empty if none):"
                 "\n\n<<CARDS>>\n\n")
        cards = P.VERIFY_PROMPT_CARDS.replace(block, "", 1)
        cards = cards.replace("a sentence in the excerpts or a fact on a reference card cannot",
                              "a sentence in the excerpts cannot", 1)
        cards = cards.replace("Copy the deciding words of that sentence or fact exactly into \"quote\", from the excerpt "
                              "or the card.", "Copy the deciding words of that sentence exactly into \"quote\".", 1)
        cards = cards.replace("A line that neither the excerpts nor a reference card mention is not contradicted",
                              "A line the excerpts do not mention is not contradicted", 1)
        self.assertEqual(cards, P.VERIFY_PROMPT)
        i = P.VERIFY_PROMPT_CARDS.index("<<FINDINGS>>")
        self.assertLess(i, P.VERIFY_PROMPT_CARDS.index("<<CARDS>>"))

    def test_not_part_of_the_frozen_configuration(self):
        for n in ("VERIFY_SYSTEM", "VERIFY_PROMPT", "VERIFY_PROMPT_CARDS"):
            self.assertNotIn(n, config.PROMPT_NAMES)
            self.assertNotIn(n, config.load()["prompts_sha256"])


if __name__ == "__main__":
    unittest.main()
