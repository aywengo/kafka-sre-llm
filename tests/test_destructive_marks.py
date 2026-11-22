"""M6 H1: the destructive-step marker (ask --mark-destructive, experimental). Offline: stub generator, retrieval
patched. A positive and a near negative per pattern; the negation rule; marks only; the option off changes nothing."""
import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))
from kafka_sre_rag import __main__ as cli, checker, config, revise as rv  # noqa: E402
from test_ask_check import CHUNKS, RecGen  # noqa: E402
from unittest import mock  # noqa: E402


def ids(text):
    return [m["id"] for m in checker.find_destructive(text) if not m["skipped"]]


def fence(cmd, lang="bash"):
    return "Run this:\n```%s\n%s\n```\nDone." % (lang, cmd)


# (id, text that must be marked with it, text that must not)
CASES = [
    ("D1", fence("kafka-consumer-groups.sh --bootstrap-server b:9092 --delete --group g"),
     fence("kafka-consumer-groups.sh --bootstrap-server b:9092 --describe --group g")),
    ("D1p", "Delete the consumer group and start again.", "Describe the consumer group and wait."),
    ("D2", fence("kafka-consumer-groups.sh --group g --topic t --reset-offsets --to-earliest --execute"),
     fence("kafka-consumer-groups.sh --group g --topic t --reset-offsets --to-earliest --dry-run")),
    ("D2p", "Then reset the consumer group offsets to the latest.", "The group has committed offsets for this topic."),
    ("D3", fence("kafka-streams-application-reset.sh --application-id app"), fence("kafka-streams-groups.sh --describe")),
    ("D4", fence("kubectl annotate kafkaconnector c strimzi.io/connector-offsets=reset"),
     fence("kubectl annotate kafkaconnector c strimzi.io/connector-offsets=list")),
    ("D5", fence("kafka-topics.sh --bootstrap-server b:9092 --delete --topic t"),
     fence("kafka-topics.sh --bootstrap-server b:9092 --describe --topic t")),
    ("D5p", "As a last resort, recreate the topic.", "Describe the topic first."),
    ("D6", fence("kafka-delete-records.sh --offset-json-file o.json"), fence("kafka-dump-log.sh --files x.log")),
    ("D7", "Set unclean.leader.election.enable=true on the topic.", "unclean.leader.election.enable is false by default."),
    ("D8", fence("argocd app delete kafka-platform"), fence("argocd app delete kafka-platform --cascade=false")),
    ("D8p", "Delete the old Application and create a new one.",
     "Delete the old Application with the non-cascading form."),
    ("D9", fence("kubectl delete pvc data-my-cluster-0 -n kafka"), fence("kubectl delete pod my-cluster-0 -n kafka")),
    ("D9p", "Delete the PVC and let the operator recreate it.", "Check the PVC status."),
    ("D10", fence("kubectl patch app x --type merge -p '{\"metadata\":{\"finalizers\":null}}'"),
     fence("kubectl get app x -o yaml")),
    ("D11", fence("curl -X DELETE http://connect:8083/connectors/c"), fence("curl http://connect:8083/connectors/c")),
    ("D11p", "Delete the existing connectors before applying the new ones.", "List the existing connectors first."),
    ("D12", fence("kubectl scale deployment strimzi-cluster-operator --replicas=0"),
     fence("kubectl scale deployment strimzi-cluster-operator --replicas=1")),
    ("D13", fence("kubectl delete pod p --grace-period=0 --force"), fence("kubectl delete pod p --grace-period=30")),
    ("D14", fence("argocd app sync kafka-platform --prune"), fence("syncPolicy:\n  automated:\n    prune: true", "yaml")),
    ("D15", fence("metadata:\n  annotations:\n    strimzi.io/delete-pod-and-pvc: \"true\"", "yaml"),
     fence("metadata:\n  annotations:\n    strimzi.io/manual-rolling-update: \"true\"", "yaml")),
    ("D16", "Reduce the retention temporarily to force deletion.", "The retention is 7 days."),
    ("D17", "Increase the free-space threshold to a lower percentage.", "The threshold is 10%."),
    ("D18", "Set min.insync.replicas=1 until the broker is back.", "With min.insync.replicas=1 the writes were lost, so this is the cause."),
    ("D19", fence("allow.everyone.if.no.acl.found=true", "properties"), fence("allow.everyone.if.no.acl.found=false", "properties")),
]


class TestPatterns(unittest.TestCase):
    def test_every_pattern_has_a_case(self):
        listed = {p["id"] for p in checker.load_destructive_patterns()}
        self.assertEqual(listed, {c[0] for c in CASES})

    def test_each_pattern_positive_and_near_negative(self):
        for pid, pos, neg in CASES:
            with self.subTest(pid=pid):
                self.assertIn(pid, ids(pos))
                self.assertNotIn(pid, ids(neg))

    def test_data_fields(self):
        for p in checker.load_destructive_patterns():
            self.assertTrue(p["id"] and p["name"] and p["consequence"] and p["applies"] in ("command", "prose", "any"))

    def test_h0_repairs(self):
        self.assertEqual(ids("### Option A: Delete the topic-level override (recommended)"), [])
        self.assertEqual(ids("An unclean shutdown left the log corrupt."), [])
        self.assertEqual(ids("Set `delete.retention.ms` to keep tombstones."), [])
        self.assertEqual(ids("Check log.retention.ms and retention.bytes."), [])
        self.assertEqual(ids("The diagnosis: min.insync.replicas=1 on the topic explains the loss."), [])
        self.assertEqual(ids("Mentions `min.insync.replicas` only by name."), [])
        self.assertEqual(ids("The output should no longer show `min.insync.replicas=1`."), [])
        self.assertEqual(ids("Re-enable pruning on kafka-platform."), ["D14"])
        self.assertEqual(ids("It is documented for `kafka-streams-application-reset.sh` only."), [])


class TestNegation(unittest.TestCase):
    def test_clear_warnings_are_skipped(self):
        for t in ["Do not run `argocd app delete kafka-platform`.",
                  "Never delete the PVC while the replicas are out of sync.",
                  "Avoid deleting the consumer group; wait for it to be Empty.",
                  "Use alter instead of `strimzi.io/connector-offsets=reset`.",
                  "Rather than delete the PVC, fix the disk."]:
            with self.subTest(t=t):
                ms = checker.find_destructive(t)
                self.assertTrue(ms)
                self.assertTrue(all(m["skipped"] for m in ms))

    def test_recommendation_is_marked(self):
        self.assertEqual(ids("If you are in a hurry, delete the group, then reset."), ["D1p"])
        self.assertEqual(ids("Delete the PVC. Do not wait."), ["D9p"])  # the warning is in the next sentence

    def test_doubtful_cases_are_counted(self):
        ms = checker.find_destructive("Deleting the PVC is not recommended.")
        self.assertEqual(len(ms), 1)
        self.assertTrue(ms[0]["skipped"] and ms[0]["doubtful"])
        ms = checker.find_destructive("Do not:\n\n- delete the PVC")
        self.assertTrue(ms[0]["skipped"] and ms[0]["doubtful"])
        ms = checker.find_destructive("### What not to do\n\nDelete the PVC.")
        self.assertTrue(ms[0]["skipped"] and ms[0]["doubtful"])

    def test_fence_after_a_warning_line(self):
        t = "Do not run this:\n```bash\nkubectl delete pvc x\n```"
        self.assertEqual(ids(t), [])
        t = "```bash\n# DON'T do this\nkubectl delete pvc x\n```"
        self.assertEqual(ids(t), [])
        self.assertEqual(ids("Run this:\n```bash\n# delete it\nkubectl delete pvc x\n```"), ["D9"])

    def test_comment_lines_in_a_block_are_not_commands(self):
        self.assertEqual(ids("```bash\n# kubectl delete pvc x\n```"), [])

    def test_wrapped_command_is_one_unit(self):
        ms = checker.find_destructive("```bash\nkafka-consumer-groups.sh \\\n  --group g \\\n  --delete\n```")
        self.assertEqual([(m["line"], m["end"]) for m in ms], [(2, 4)])


class TestMarks(unittest.TestCase):
    TEXT = ("Intro.\n\nIf stuck, delete the PVC and wait. Do not delete the group.\n\n```bash\nkubectl delete pvc x \\\n"
            "  -n kafka\necho ok\n```\n\n```json\n{\"a\": \"curl -X DELETE http://c/connectors/a\"}\n```\n\n"
            "| step | cmd |\n|---|---|\n| 1 | `argocd app delete foo` |\n\nEnd.")

    def test_marks_written_and_nothing_else_changed(self):
        out, n, _ = rv.mark_destructive(self.TEXT)
        self.assertEqual(n, 4)
        self.assertNotEqual(out, self.TEXT)
        self.assertEqual(rv.strip_destructive_marks(out), self.TEXT)
        self.assertIn("[destructive step: deletes a PVC or namespace; the volume's data is lost] Do not", out)
        self.assertIn("  -n kafka\n# destructive step: deletes a resource that holds data", out)
        self.assertIn("```\n[destructive step: deletes a connector", out)
        self.assertIn("| `argocd app delete foo` |\n[destructive step: deletes an Argo CD Application", out.replace("| 1 | ", "| "))

    def test_no_match_no_change(self):
        t = "Describe the group.\n```bash\nkafka-consumer-groups.sh --describe\n```"
        self.assertEqual(rv.mark_destructive(t), (t, 0, []))

    def test_two_patterns_one_sentence_one_mark(self):
        out, n, _ = rv.mark_destructive("Delete the PVC and delete the connector.")
        self.assertEqual(n, 1)
        self.assertEqual(rv.strip_destructive_marks(out), "Delete the PVC and delete the connector.")

    def test_report(self):
        rep = checker.destructive_report({"a": "Delete the PVC.", "b": "Do not delete the PVC.", "c": "Fine."})
        r = rep["patterns"]["D9p"]
        self.assertEqual((r["matches"], r["marked"], r["skipped_negated"]), (2, 1, 1))
        self.assertEqual((rep["answers"], rep["marked_answers"], rep["marks"]), (3, 1, 1))
        self.assertIn("D9p", checker.format_destructive_report(rep))


def run(argv, answer):
    a = cli.build_parser().parse_args(["ask", "How do I recover?", *argv])
    gen, out = RecGen(answer), io.StringIO()
    with mock.patch("kafka_sre_rag.retrieval.select", return_value=CHUNKS), contextlib.redirect_stdout(out):
        cli.run_one(a.question, gen, config.load(), a, None)
    return gen, out.getvalue()


class TestOption(unittest.TestCase):
    ANSWER = "Delete the PVC and wait.\n```bash\nkubectl delete pvc x\n```"

    def test_off_by_default(self):
        a = cli.build_parser().parse_args(["ask", "q"])
        self.assertFalse(a.mark_destructive)
        self.assertFalse(cli.build_parser().parse_args(["chat"]).mark_destructive)
        _, out = run([], self.ANSWER)
        self.assertNotIn("destructive", out)
        self.assertEqual(out, self.ANSWER + "\n")

    def test_on_marks_only(self):
        g0, off = run(["--json"], self.ANSWER)
        g1, on = run(["--json", "--mark-destructive"], self.ANSWER)
        self.assertEqual(g0.calls, g1.calls)                      # the prompts do not change
        o0, o1 = json.loads(off), json.loads(on)
        self.assertNotIn("destructive_marks", o0)
        self.assertEqual(o1["destructive_marks"], 2)
        self.assertEqual(rv.strip_destructive_marks(o1["text"]), o0["text"])
        o1.pop("destructive_marks"), o1.pop("text"), o0.pop("text")
        o0.pop("timings"), o1.pop("timings")
        self.assertEqual(o0, o1)

    def test_plain_output(self):
        _, off = run([], self.ANSWER)
        _, on = run(["--mark-destructive"], self.ANSWER)
        self.assertIn("(destructive-step marks written: 2; experimental)", on)
        body = on.split("\n\n(destructive-step marks written")[0]
        self.assertEqual(rv.strip_destructive_marks(body), off.rstrip("\n"))

    def test_with_stream_is_refused(self):
        buf = io.StringIO()
        with contextlib.redirect_stderr(buf):
            self.assertEqual(cli.main(["ask", "--dry-run", "--stream", "--mark-destructive", "q"]), 2)

    def test_help_says_experimental(self):
        self.assertIn("EXPERIMENTAL", cli.build_parser().format_help() + cli.build_parser()._subparsers._group_actions[0].choices["ask"].format_help())

    def test_serve_has_no_option(self):
        with self.assertRaises(SystemExit):
            with contextlib.redirect_stderr(io.StringIO()):
                cli.build_parser().parse_args(["serve", "--mark-destructive"])


if __name__ == "__main__":
    unittest.main()
