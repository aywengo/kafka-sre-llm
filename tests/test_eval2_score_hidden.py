"""Hidden path of the tier A scorer (scripts/eval2_score.py): synthetic rows, encrypted with a temporary key in a
temporary folder. Nothing under eval/hidden/ is read. Checks: the round trip, the same totals through the open path,
the refusal to mix, totals only (no id, no answer text in any output), per kind only from 5 items, the tagged items
with and without, not_in_index apart, the private temporary folder for the Java checks, the open path unchanged."""
import contextlib
import io
import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
import eval2  # noqa: E402
import eval2_score as sc  # noqa: E402

FIX = ROOT / "tests" / "fixtures"
SECRET = "SECRETANSWERTEXT"


def item(iid, kind, accepted, tagged=False, sources=None):
    it = {"id": iid, "kind": kind, "domain": "d", "question": "q?", "anchored": True,
          "checks": [{"type": "answer_line"}, {"type": "answer_equals", "accepted": [accepted], "norm": "exact"}],
          "sources": sources or []}
    if tagged:
        it["shares_variant_with_published"] = True
    return it


def answer(iid, value):
    return {"id": iid, "text": "%s\nANSWER: %s" % (SECRET, value), "pack": []}


class Hidden(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dir = Path(tempfile.mkdtemp(prefix="score_hidden_test_"))
        cls.cats = cls.dir / "cats"
        cls.cats.mkdir()
        for sub in ("catalogues", "catalogues_ext"):
            for f in (FIX / sub).glob("*.json"):
                shutil.copy(f, cls.cats)
        cls.key = cls.dir / "key"
        eval2.keygen(cls.key)
        cls.old_key = os.environ.get("EVAL_HIDDEN_KEY")
        os.environ["EVAL_HIDDEN_KEY"] = str(cls.key)
        # file 1: 7 items of kind big (5 pass), 3 of kind small; file 2: 6 items of kind a3 (2 tagged)
        cls.f1 = [item("HIDX-%02d" % i, "big", "v") for i in range(7)] + [item("HIDS-%02d" % i, "small", "v") for i in range(3)]
        cls.f2 = [item("HIDA-%02d" % i, "a3kind", "v", tagged=(i < 2)) for i in range(6)]
        good = {"HIDX-00", "HIDX-01", "HIDX-02", "HIDX-03", "HIDX-04", "HIDS-00", "HIDA-00", "HIDA-02", "HIDA-03"}
        cls.answers = [answer(i["id"], "v" if i["id"] in good else "w") for i in cls.f1 + cls.f2]
        cls.answers = [a for a in cls.answers if a["id"] != "HIDS-02"]          # one missing answer
        cls.answers.append(answer("HIDZ-99", "v"))                               # one answer for an unknown id
        cls.n_ok = len(good)
        cls.idx = cls.dir / "chunks.jsonl"
        cls.idx.write_text(json.dumps({"id": "c1", "path": "p.md", "title": "t", "text": "nothing"}) + "\n")
        import hashlib
        cls.frozen = cls.dir / "frozen.json"
        cls.frozen.write_text(json.dumps({"sha256": hashlib.sha256(cls.idx.read_bytes()).hexdigest()}))
        for name, rows in (("f1", cls.f1), ("f2", cls.f2)):
            eval2.write_jsonl(cls.dir / (name + ".jsonl.enc"), rows)
            (cls.dir / (name + ".jsonl")).write_text("".join(json.dumps(r) + "\n" for r in rows))
        eval2.write_jsonl(cls.dir / "ans.jsonl.enc", cls.answers)
        (cls.dir / "ans.jsonl").write_text("".join(json.dumps(r) + "\n" for r in cls.answers))

    @classmethod
    def tearDownClass(cls):
        if cls.old_key is None:
            os.environ.pop("EVAL_HIDDEN_KEY", None)
        else:
            os.environ["EVAL_HIDDEN_KEY"] = cls.old_key
        shutil.rmtree(cls.dir, ignore_errors=True)

    def run_main(self, argv):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            try:
                code = sc.main(argv)
            except SystemExit as e:
                code = e.code
        return code, out.getvalue(), err.getvalue()

    def common(self, suffix=""):
        return ["--catalogues", str(self.cats), "--index", str(self.idx), "--frozen", str(self.frozen)]

    def hidden_args(self, scores=None, summary=None):
        a = ["--items", str(self.dir / "f1.jsonl.enc"), str(self.dir / "f2.jsonl.enc"), "--answers", str(self.dir / "ans.jsonl.enc")]
        if scores:
            a += ["--scores", str(scores)]
        if summary:
            a += ["--summary", str(summary)]
        return a + self.common()

    def test_round_trip_totals_and_open_equivalence(self):
        sp, su = self.dir / "s.jsonl.enc", self.dir / "s.json"
        code, out, err = self.run_main(self.hidden_args(sp, su))
        self.assertEqual(code, 0, err)
        # the scores file is encrypted, and decrypts to one row per item
        raw = sp.read_bytes()
        self.assertTrue(raw.startswith(eval2.MAGIC))
        self.assertNotIn(b"HIDX", raw)
        rows = eval2.read_jsonl(sp)
        self.assertEqual(len(rows), 16)
        h = json.loads(out)
        self.assertEqual(json.loads(su.read_text()), h)
        self.assertEqual((h["items_scored"], h["missing_answers"], h["answers_for_unknown_ids"]), (16, 1, 1))
        self.assertEqual(h["all_pass"], self.n_ok)
        # the same rows through the open path give the same totals
        osp, osu = self.dir / "o.jsonl", self.dir / "o.json"
        code, oout, oerr = self.run_main(["--items", str(self.dir / "f1.jsonl"), str(self.dir / "f2.jsonl"), "--answers",
                                          str(self.dir / "ans.jsonl"), "--scores", str(osp), "--summary", str(osu)] + self.common())
        self.assertEqual(code, 0, oerr)
        o = json.loads(oout)
        for k in ("items_scored", "missing_answers", "answers_for_unknown_ids", "all_pass", "all_pass_rate", "wilson95",
                  "checks_share", "format_failures", "outcomes"):
            if k == "outcomes":
                for x in sc.OUTCOMES + ("n", "not_in_index"):
                    self.assertEqual(h[k][x], o[k]["total"][x], x)
            else:
                self.assertEqual(h[k], o[k], k)
        for kd in ("big",):
            self.assertEqual(h["by_kind"][kd]["all_pass"], o["by_kind"][kd]["all_pass"])
        ip, oip = h["identifier_precision"], o["identifier_precision"]
        for k in ("known", "unknown", "not_covered", "precision_known_of_classified", "answers_with_an_unknown_identifier"):
            self.assertEqual(ip[k], oip[k], k)
        # the decrypted score rows equal the open rows
        orows = [json.loads(l) for l in osp.read_text().splitlines()]
        self.assertEqual(sorted(rows, key=lambda r: r["id"]), sorted(orows, key=lambda r: r["id"]))

    def test_kind_threshold_and_tagged_totals(self):
        code, out, err = self.run_main(self.hidden_args())
        self.assertEqual(code, 0, err)
        h = json.loads(out)
        self.assertEqual(sorted(h["by_kind"]), ["a3kind", "big"])          # small has 3 items: withheld
        self.assertEqual(h["kinds_withheld_under_5_items"], 1)
        self.assertEqual(h["by_kind"]["big"]["n"], 7)
        self.assertEqual(h["by_kind"]["big"]["all_pass"], 5)
        f1, f2 = h["by_items_file"]
        self.assertEqual((f1["file"], f1["scored"], f1["items"], f1["all_pass"]), ("f1.jsonl.enc", 10, 10, 6))
        self.assertNotIn("with_tagged", f1)
        self.assertEqual(f2["tagged_shares_variant_with_published"], 2)
        self.assertEqual((f2["with_tagged"]["n"], f2["with_tagged"]["all_pass"]), (6, 3))
        self.assertEqual((f2["without_tagged"]["n"], f2["without_tagged"]["all_pass"]), (4, 2))

    def test_not_in_index_apart(self):
        src = [{"catalogue": "kafka_config", "entry": "x/no.such.key", "file": None}]
        its = [item("HIDN-%d" % i, "nn", "v", sources=src) for i in range(2)]
        eval2.write_jsonl(self.dir / "n.jsonl.enc", its)
        eval2.write_jsonl(self.dir / "na.jsonl.enc", [answer("HIDN-0", "v"), answer("HIDN-1", "w")])
        code, out, err = self.run_main(["--items", str(self.dir / "n.jsonl.enc"), "--answers", str(self.dir / "na.jsonl.enc")] + self.common())
        self.assertEqual(code, 0, err)
        h = json.loads(out)
        self.assertEqual(h["not_in_index"]["apart"]["n"], 2)
        self.assertEqual(h["not_in_index"]["all_others"]["n"], 0)
        self.assertEqual(h["outcomes"]["not_in_index"], 2)

    def test_output_has_no_id_and_no_text(self):
        sp, su = self.dir / "t.jsonl.enc", self.dir / "t.json"
        code, out, err = self.run_main(self.hidden_args(sp, su))
        self.assertEqual(code, 0, err)
        for blob in (out, err, su.read_text()):
            self.assertNotIn("HID", blob.replace("HIDDEN", ""))
            self.assertNotIn(SECRET, blob)
            self.assertNotIn("ANSWER", blob)
        for line in err.splitlines():
            self.assertNotRegex(line, r"HID[A-Z]-\d")

    def test_refuses_to_mix_and_plain_outputs(self):
        for items, ans in (([self.dir / "f1.jsonl.enc", self.dir / "f2.jsonl"], "ans.jsonl.enc"),
                           ([self.dir / "f1.jsonl.enc"], "ans.jsonl"), ([self.dir / "f1.jsonl"], "ans.jsonl.enc")):
            code, out, err = self.run_main(["--items"] + [str(p) for p in items] + ["--answers", str(self.dir / ans)] + self.common())
            self.assertNotEqual(code, 0)
            self.assertEqual(out, "")
        code, out, err = self.run_main(self.hidden_args(self.dir / "plain.jsonl"))
        self.assertNotEqual(code, 0)
        self.assertFalse((self.dir / "plain.jsonl").exists())
        code, out, err = self.run_main(["--items", str(self.dir / "f1.jsonl"), "--answers", str(self.dir / "ans.jsonl"),
                                        "--scores", str(self.dir / "x.jsonl.enc")] + self.common())
        self.assertNotEqual(code, 0)
        self.assertFalse((self.dir / "x.jsonl.enc").exists())

    def test_duplicate_ids_and_wrong_key_name_no_values(self):
        eval2.write_jsonl(self.dir / "dup.jsonl.enc", self.f1 + self.f1)
        code, out, err = self.run_main(["--items", str(self.dir / "dup.jsonl.enc"), "--answers", str(self.dir / "ans.jsonl.enc")] + self.common())
        self.assertEqual(code, 2)
        self.assertNotIn("HID", err)
        other = self.dir / "key2"
        eval2.keygen(other)
        old = os.environ["EVAL_HIDDEN_KEY"]
        os.environ["EVAL_HIDDEN_KEY"] = str(other)
        try:
            code, out, err = self.run_main(self.hidden_args())
        finally:
            os.environ["EVAL_HIDDEN_KEY"] = old
        self.assertEqual(code, 2)
        self.assertEqual(out, "")

    def test_scoring_error_names_the_id_only(self):
        orig = sc.score_item

        def boom(it, text, cats):
            raise ValueError(text)          # an exception whose message is the answer text
        sc.score_item = boom
        try:
            code, out, err = self.run_main(self.hidden_args())
        finally:
            sc.score_item = orig
        self.assertEqual(code, 1)
        self.assertNotIn(SECRET, err + out)
        self.assertIn("ValueError", err)

    def test_private_temp_folder_is_used_and_removed(self):
        seen = []
        orig = sc.score_item

        def probe(it, text, cats):
            seen.append(tempfile.gettempdir())
            seen.append(os.environ.get("TMPDIR"))
            with tempfile.TemporaryDirectory(prefix="a3java_") as t:     # what the Java checks do
                seen.append(os.path.dirname(t))
                Path(t, "Answer.java").write_text(text)
            return orig(it, text, cats)
        before = tempfile.gettempdir()
        sc.score_item = probe
        try:
            code, out, err = self.run_main(self.hidden_args())
        finally:
            sc.score_item = orig
        self.assertEqual(code, 0, err)
        private = set(seen)
        self.assertEqual(len(private), 1)
        p = private.pop()
        self.assertNotEqual(p, before)
        self.assertIn("eval2_score_hidden_", p)
        self.assertFalse(Path(p).exists())                       # removed afterwards
        self.assertEqual(tempfile.gettempdir(), before)          # and the process setting restored

    def test_open_path_leaves_temp_alone(self):
        before = tempfile.tempdir
        self.run_main(["--items", str(self.dir / "f1.jsonl"), "--answers", str(self.dir / "ans.jsonl"), "--no-outcomes"] + self.common())
        self.assertEqual(tempfile.tempdir, before)


if __name__ == "__main__":
    unittest.main()
