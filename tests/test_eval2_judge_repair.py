"""Judge reply repair (M6 F-a): a reply whose list of must verdicts was closed early is accepted only when removing that one
closing sequence leaves exactly the rubric's ids; the row is marked repaired. Synthetic replies of the stored shape."""
import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
import eval2_judge as ej  # noqa: E402

RUBRIC = {"id": "t-1", "question": "q", "must": [{"id": f"m{i}", "text": f"t{i}", "weight": 1, "first": i == 1} for i in range(1, 7)],
          "must_not": [{"id": "n1", "text": "x", "severity": "wrong"}, {"id": "n2", "text": "y", "severity": "wrong"}]}
ANSWER = "Use describeConsumerGroups.\nfor (m : desc.members()) { ownerMap.put(tp, member); }"


def e(i, verdict="absent", quote=""):
    return {"id": i, "verdict": verdict, "quote": quote}


def good_reply():
    return {"must": [e("m1", "present", "Use describeConsumerGroups."), e("m2"), e("m3"),
                     e("m4", "present", "for (m : desc.members()) { ownerMap.put(tp, member); }"), e("m5"), e("m6")],
            "must_not": [e("n1"), e("n2")], "other_errors": []}


def early_close(text, after_id="m4"):
    """The stored shape: the list of must verdicts and the object are closed right after one entry, then the reply goes on."""
    marker = '"id":"%s"' % after_id
    i = text.index(marker)
    end = text.index('"}', i) + 2         # the entry's own closing quote and brace
    return text[:end] + "]}" + text[end:]


class Repair(unittest.TestCase):
    def reply_text(self, j=None):
        return json.dumps(j or good_reply(), separators=(",", ":"))

    def test_the_stored_shape_is_repaired(self):
        bad = early_close(self.reply_text())
        self.assertIsNone(ej.parse_reply(bad))
        j, kind = ej.repair_reply(bad, RUBRIC)
        self.assertEqual(j, good_reply())
        self.assertEqual(kind, "closing_sequence_removed:]}")

    def test_think_block_and_text_around_are_tolerated(self):
        bad = "<think>x {y}</think>\nHere: " + early_close(self.reply_text()) + "\nDone."
        j, _ = ej.repair_reply(bad, RUBRIC)
        self.assertEqual(j, good_reply())

    def test_a_valid_reply_is_not_touched(self):
        self.assertEqual(ej.repair_reply(self.reply_text(), RUBRIC), (None, None))

    def test_missing_id_is_not_repaired(self):
        j = good_reply()
        j["must"] = [x for x in j["must"] if x["id"] != "m6"]
        self.assertEqual(ej.repair_reply(early_close(self.reply_text(j)), RUBRIC), (None, None))

    def test_duplicated_id_is_not_repaired(self):
        j = good_reply()
        j["must"].append(e("m2"))
        self.assertEqual(ej.repair_reply(early_close(self.reply_text(j)), RUBRIC), (None, None))

    def test_extra_id_is_not_repaired(self):
        j = good_reply()
        j["must_not"].append(e("n9"))
        self.assertEqual(ej.repair_reply(early_close(self.reply_text(j)), RUBRIC), (None, None))

    def test_two_misplaced_brackets_are_not_repaired(self):
        bad = early_close(early_close(self.reply_text(), "m4"), "m2")
        self.assertEqual(ej.repair_reply(bad, RUBRIC), (None, None))

    def test_other_damage_is_not_repaired(self):
        t = self.reply_text()
        self.assertEqual(ej.repair_reply(t[:-1], RUBRIC), (None, None))             # cut off
        self.assertEqual(ej.repair_reply(t.replace('"m3"', "m3", 1), RUBRIC), (None, None))   # broken string
        self.assertEqual(ej.repair_reply("no json here", RUBRIC), (None, None))

    def test_result_row_marks_the_repair_and_scores_it(self):
        item = {"set": "s", "id": "t-1", "question": "q", "answer": ANSWER, "rubric": RUBRIC, "pack": None}
        entry = {"key": "k", "j": None, "reply": early_close(self.reply_text()), "retried": True}
        row = ej.result_row(item, 1, entry)
        self.assertTrue(row["valid_reply"])
        self.assertTrue(row["repaired"])
        self.assertEqual(row["repair_kind"], "closing_sequence_removed:]}")
        self.assertEqual(row["must"]["m1"]["verdict"], "present")
        self.assertEqual(row["must"]["m5"]["verdict"], "absent")
        # an unrepairable reply stays invalid, as before
        bad = ej.result_row(item, 1, {"key": "k", "j": None, "reply": "{", "retried": True})
        self.assertFalse(bad["valid_reply"])
        self.assertNotIn("repaired", bad)
        # a valid reply carries no repair field
        ok = ej.result_row(item, 1, {"key": "k", "j": good_reply(), "reply": self.reply_text(), "retried": False})
        self.assertNotIn("repaired", ok)


if __name__ == "__main__":
    unittest.main()
