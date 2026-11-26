"""Offline tests of the tool runtime (kafka_sre_rag/tools) and the MCP harness (mcp_harness): no cluster, no server."""
import importlib.util
import io
import json
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from kafka_sre_rag import prompts  # noqa: E402
from kafka_sre_rag.tools import k8s_server as srv  # noqa: E402
from kafka_sre_rag.tools.allowlist import AllowList  # noqa: E402
from kafka_sre_rag.tools.loop import ScriptedModel, Session, parse_text_calls, run  # noqa: E402
from kafka_sre_rag.tools.mcp_client import MCPError, StdioServer, check_local  # noqa: E402
from kafka_sre_rag.tools.schema import validate  # noqa: E402


def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "mcp_harness" / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(ROOT / "mcp_harness"))
    spec.loader.exec_module(mod)
    return mod


SCHEMAS = {t["name"]: t["inputSchema"] for t in srv.TOOLS}


class FakeServer:
    tools = srv.TOOLS

    def __init__(self):
        self.calls = []

    def call(self, name, args):
        self.calls.append((name, args))
        return False, f"output of {name}"

    def close(self):
        pass


class FakeRunner(srv.Runner):
    def __init__(self, outputs=None):
        super().__init__(context="ctx", run=self.fake)
        self.argvs, self.outputs = [], outputs or {}

    def fake(self, argv):
        self.argvs.append(argv)
        for key, out in self.outputs.items():
            if key in argv:
                return 0, out, ""
        return 0, "ok", ""


class SchemaTest(unittest.TestCase):
    def test_kinds_of_errors(self):
        s = SCHEMAS["k8s_logs"]
        self.assertEqual(validate({"namespace": "kafka", "pod": "p-0"}, s), [])
        kinds = {e[0] for e in validate({"namespace": 3, "tail": 900, "x": 1}, s)}
        self.assertEqual(kinds, {"type", "required", "range", "extra"})
        self.assertEqual(validate({"namespace": "kafka", "pod": "Bad_Name"}, s)[0][0], "pattern")
        self.assertEqual(validate({"kind": "secrets"}, SCHEMAS["k8s_get"])[0][0], "enum")
        self.assertEqual(validate({"namespace": "kafka", "pod": "p", "previous": "yes"}, s)[0][0], "type")

    def test_bool_is_not_integer(self):
        self.assertTrue(validate({"namespace": "kafka", "pod": "p", "tail": True}, SCHEMAS["k8s_logs"]))


class AllowListTest(unittest.TestCase):
    def setUp(self):
        self.a = AllowList.load()

    def st(self, name, args):
        return self.a.check(name, args, SCHEMAS).status

    def test_default_list_is_read_only_tools(self):
        self.assertEqual(sorted(self.a.tools), sorted(SCHEMAS))
        for t in srv.TOOLS:
            self.assertTrue(t["annotations"]["readOnlyHint"])
            self.assertFalse(t["annotations"]["destructiveHint"])
        for name in SCHEMAS:
            self.assertFalse(re.search(r"delete|patch|apply|scale|edit|exec|reset|alter|create", name), name)

    def test_ok(self):
        self.assertEqual(self.st("k8s_get", {"kind": "pods", "namespace": "kafka"}), "ok")
        self.assertEqual(self.st("k8s_get", {"kind": "nodes"}), "ok")

    def test_violations(self):
        cases = [
            ("kubectl_delete", {"kind": "pods"}),                                   # unknown tool
            ("k8s_get", {"kind": "secrets", "namespace": "kafka"}),                 # forbidden value
            ("k8s_get", {"kind": "Secret", "namespace": "kafka"}),
            ("k8s_get", {"kind": "pods", "namespace": "kube-system"}),             # namespace outside the session
            ("k8s_get", {"kind": "pods", "all_namespaces": True}),                 # not allowed in this session
            ("k8s_get", {"kind": "pods", "namespace": "kafka", "command": "rm"}),  # argument not taken
            ("k8s_logs", {"namespace": "kafka", "pod": "p; kubectl delete ns kafka"}),  # shell fragment
            ("k8s_get", {"kind": "pods", "namespace": "kafka", "name": "--all"}),  # option fragment
        ]
        for name, args in cases:
            self.assertEqual(self.st(name, args), "violation", (name, args))

    def test_tool_listed_but_not_allowed(self):
        a = AllowList(dict(self.a.data, tools=["k8s_get"]))
        self.assertEqual(a.check("k8s_logs", {"namespace": "kafka", "pod": "p"}, SCHEMAS).status, "violation")

    def test_invalid(self):
        self.assertEqual(self.st("k8s_logs", {"namespace": "kafka", "pod": "Bad_Name"}), "invalid")
        self.assertEqual(self.st("k8s_logs", {"namespace": "kafka"}), "invalid")
        self.assertEqual(self.st("k8s_logs", {"namespace": "kafka", "pod": "p", "tail": 5000}), "invalid")
        self.assertEqual(self.st("k8s_get", None), "invalid")
        self.assertEqual(self.st("k8s_get", {"kind": "pdos", "namespace": "kafka"}), "invalid")

    def test_remote_servers_refused(self):
        for spec in ({"transport": "http", "command": ["x"]}, {"url": "http://localhost:1/mcp", "command": ["x"]},
                     {"transport": "sse"}):
            with self.assertRaises(MCPError):
                check_local(spec)
        self.assertEqual(check_local({"command": ["{python}", "-m", "m"]})[0], sys.executable)
        for spec in AllowList.load().servers.values():
            check_local(spec)


class ServerTest(unittest.TestCase):
    def test_secret_refused_without_running_anything(self):
        r = FakeRunner()
        for name, args in (("k8s_get", {"kind": "secrets", "namespace": "kafka"}),
                           ("k8s_describe", {"kind": "secret", "namespace": "kafka", "name": "x"})):
            err, text = srv.call_tool(r, name, args)
            self.assertTrue(err)
            self.assertIn("Secrets", text)
        self.assertEqual(r.argvs, [])
        self.assertNotIn("secrets", srv.KIND_ALIASES)
        self.assertNotIn("secret", srv.KIND_ALIASES)

    def test_fixed_read_only_argv(self):
        r = FakeRunner()
        srv.call_tool(r, "k8s_get", {"kind": "kafka", "namespace": "kafka", "output": "yaml"})
        srv.call_tool(r, "k8s_logs", {"namespace": "kafka", "pod": "p-0", "tail": 50, "previous": True})
        srv.call_tool(r, "k8s_events", {"namespace": "kafka", "types": "Warning"})
        self.assertEqual(r.argvs[0], ["kubectl", "--context", "ctx", "--request-timeout=20s", "get",
                                      "kafkas.kafka.strimzi.io", "-n", "kafka", "-o", "yaml"])
        self.assertEqual(r.argvs[1][4:], ["logs", "-n", "kafka", "p-0", "--tail=50", "--previous"])
        self.assertEqual(r.argvs[2][4:], ["events", "-n", "kafka", "--types", "Warning"])
        for argv in r.argvs:
            self.assertIn(argv[4], ("get", "describe", "logs", "events"))

    def test_namespace_required_for_namespaced_kinds(self):
        r = FakeRunner()
        err, text = srv.call_tool(r, "k8s_get", {"kind": "pods"})
        self.assertTrue(err)
        self.assertIn("namespace is required", text)
        self.assertEqual(r.argvs, [])

    def test_broker_tools_exec_fixed_describe_commands(self):
        pods = json.dumps({"items": [
            {"metadata": {"name": "kafka-broker-0"}, "status": {"phase": "Pending", "containerStatuses": []}},
            {"metadata": {"name": "kafka-broker-1"}, "status": {"phase": "Running", "containerStatuses": [{"ready": True}]}}]})
        r = FakeRunner({"pods": pods})
        err, text = srv.call_tool(r, "kafka_topics_describe", {"namespace": "kafka", "topic": "orders", "under_replicated": True})
        self.assertFalse(err)
        self.assertIn("kafka-broker-1", text)
        execv = r.argvs[-1]
        self.assertEqual(execv[4:10], ["exec", "-n", "kafka", "kafka-broker-1", "-c", "kafka"])
        self.assertEqual(execv[11:], ["/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092",
                                      "--describe", "--topic", "orders", "--under-replicated-partitions"])
        srv.call_tool(r, "kafka_consumer_groups_describe", {"namespace": "kafka", "group": "g", "mode": "members"})
        self.assertIn("--describe", r.argvs[-1])
        srv.call_tool(r, "kafka_configs_describe", {"namespace": "kafka", "entity_type": "users"})
        self.assertIn("--describe", r.argvs[-1])
        for argv in r.argvs:
            for bad in ("--alter", "--delete", "--reset-offsets", "--execute", "--create"):
                self.assertNotIn(bad, argv)

    def test_redaction(self):
        text = ("ssl.keystore.password=hunter2\npassword: s3cret\n- name: DB_PASSWORD\n  value: abc123\n"
                "-----BEGIN PRIVATE KEY-----\nMIIEv\n-----END PRIVATE KEY-----\n"
                "  user.key: LS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0t\n  secret: app-consumer\n  secretName: app-consumer\n")
        out = srv.redact(text)
        for leaked in ("hunter2", "s3cret", "abc123", "MIIEv", "LS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0t"):
            self.assertNotIn(leaked, out)
        self.assertIn("secret: app-consumer", out)       # a Secret's name is not its value
        self.assertIn("secretName: app-consumer", out)

    def test_output_cap(self):
        r = FakeRunner({"get": "x" * 50000})
        err, text = srv.call_tool(r, "k8s_get", {"kind": "pods", "namespace": "kafka"})
        self.assertLess(len(text), srv.MAX_OUTPUT + 100)
        self.assertIn("truncated", text)

    def test_stdio_protocol(self):
        out = io.StringIO()
        lines = [json.dumps({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}),
                 json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}),
                 json.dumps({"jsonrpc": "2.0", "id": 2, "method": "tools/list"}),
                 json.dumps({"jsonrpc": "2.0", "id": 3, "method": "tools/call",
                             "params": {"name": "k8s_get", "arguments": {"kind": "secrets", "namespace": "kafka"}}}),
                 json.dumps({"jsonrpc": "2.0", "id": 4, "method": "nope"})]
        srv.serve(FakeRunner(), io.StringIO("\n".join(lines) + "\n"), out)
        resp = [json.loads(x) for x in out.getvalue().splitlines()]
        self.assertEqual([r["id"] for r in resp], [1, 2, 3, 4])
        self.assertEqual(resp[0]["result"]["protocolVersion"], srv.PROTOCOL_VERSION)
        self.assertEqual(len(resp[1]["result"]["tools"]), 8)
        self.assertTrue(resp[2]["result"]["isError"])
        self.assertIn("error", resp[3])

    def test_real_subprocess_server(self):
        s = StdioServer("k8s", {"command": ["{python}", "-m", "kafka_sre_rag.tools.k8s_server", "--context", "none"],
                                "cwd": str(ROOT)})
        try:
            self.assertEqual(s.server_info["name"], srv.SERVER_INFO["name"])
            self.assertEqual({t["name"] for t in s.tools}, set(SCHEMAS))
            err, text = s.call("k8s_get", {"kind": "secrets", "namespace": "kafka"})
            self.assertTrue(err)
            self.assertIn("refused", text)
        finally:
            s.close()


class LoopTest(unittest.TestCase):
    def run_loop(self, replies, **kw):
        fake = FakeServer()
        session = Session(AllowList.load(), [fake])
        model = ScriptedModel(replies)
        with tempfile.TemporaryDirectory() as d:
            trace = Path(d) / "t.jsonl"
            res = run("why?", model, session, trace_path=str(trace), on_event=None, **kw)
            events = [json.loads(x) for x in trace.read_text().splitlines()]
        return res, events, fake, model

    def test_forbidden_blocked_and_counted(self):
        res, events, fake, model = self.run_loop([
            {"tool_calls": [{"name": "k8s_get", "arguments": {"kind": "pods", "namespace": "kafka"}},
                            {"name": "k8s_get", "arguments": {"kind": "secrets", "namespace": "kafka"}},
                            {"name": "k8s_delete_pod", "arguments": {"namespace": "kafka", "name": "x"}},
                            {"name": "k8s_logs", "arguments": {"namespace": "kafka", "pod": "Bad_Name"}}]},
            {"content": "the cause"}])
        self.assertEqual(res["calls"], {"ok": 1, "violation": 2, "invalid": 1, "skipped": 0, "error": 0})
        self.assertEqual(fake.calls, [("k8s_get", {"kind": "pods", "namespace": "kafka"})])  # only the allowed call ran
        self.assertEqual((res["stop"], res["turns"], res["answer"]), ("answer", 2, "the cause"))
        tool_msgs = [m for m in model.requests[1]["messages"] if m["role"] == "tool"]
        self.assertEqual(len(tool_msgs), 4)
        self.assertTrue(tool_msgs[1]["content"].startswith("BLOCKED"))
        self.assertTrue(tool_msgs[3]["content"].startswith("REJECTED"))
        self.assertEqual([e["type"] for e in events], ["start", "request", "call", "call", "call", "call", "request", "final"])
        self.assertEqual(events[0]["prompt_version"], prompts.TOOLS_PROMPT_VERSION)

    def test_only_allowed_tools_offered(self):
        fake = FakeServer()
        allow = AllowList(dict(AllowList.load().data, tools=["k8s_get", "k8s_logs"]))
        session = Session(allow, [fake])
        self.assertEqual([t["function"]["name"] for t in session.openai_tools()], ["k8s_get", "k8s_logs"])

    def test_turn_cap_then_final_without_tools(self):
        call = {"tool_calls": [{"name": "k8s_get", "arguments": {"kind": "nodes"}}]}
        res, events, fake, model = self.run_loop([call, call, call, {"content": "final answer"}], max_turns=3)
        self.assertEqual(res["stop"], "turn_cap")
        self.assertEqual(res["turns"], 3)
        self.assertEqual(res["answer"], "final answer")
        self.assertIsNone(model.requests[-1]["tools"])
        self.assertEqual(model.requests[-1]["messages"][-1]["content"], prompts.TOOLS_FINAL)
        self.assertEqual(len(fake.calls), 3)

    def test_time_cap(self):
        call = {"tool_calls": [{"name": "k8s_get", "arguments": {"kind": "nodes"}}]}
        res, *_ = self.run_loop([call] * 5, max_seconds=-1)
        self.assertEqual((res["stop"], res["turns"]), ("time_cap", 0))

    def test_text_fallback(self):
        content = ('Let me look.\n<tool_call>{"name": "k8s_get", "arguments": {"kind": "pods", "namespace": "kafka"}}</tool_call>\n'
                   '<tool_call>{"name": "k8s_get", "arguments": {"kind": "secrets", "namespace": "kafka"}}</tool_call>')
        res, events, fake, model = self.run_loop([{"content": content}, {"content": "done"}])
        calls = [e for e in events if e["type"] == "call"]
        self.assertEqual([c["channel"] for c in calls], ["text", "text"])
        self.assertEqual([c["status"] for c in calls], ["ok", "violation"])
        last = model.requests[1]["messages"][-1]
        self.assertEqual(last["role"], "user")
        self.assertIn("<tool_response", last["content"])

    def test_parse_text_calls(self):
        self.assertEqual(parse_text_calls("no calls"), [])
        out = parse_text_calls('<tool_call>{"name": "a", "arguments": "{\\"x\\": 1}"}</tool_call><tool_call>oops</tool_call>')
        self.assertEqual(out[0][0], "a")
        self.assertEqual(out[0][2], {"x": 1})
        self.assertEqual(out[1][2], None)

    def test_bad_json_arguments_are_invalid(self):
        res, *_ = self.run_loop([{"tool_calls": [{"name": "k8s_get", "arguments": "{not json"}]}, {"content": "x"}])
        self.assertEqual(res["calls"]["invalid"], 1)


class PromptTest(unittest.TestCase):
    def test_version_and_fill(self):
        self.assertEqual(prompts.TOOLS_PROMPT_VERSION, "tools/1")
        msgs = prompts.tools_messages("q", ["argocd", "kafka"], 12)
        self.assertNotIn("<<", msgs[0]["content"])
        self.assertIn("argocd, kafka", msgs[0]["content"])
        self.assertIn("12", msgs[0]["content"])
        text = prompts.tools_messages("q", ["kafka"], 5, text_protocol=True, tools=[{"name": "k8s_get"}])[0]["content"]
        self.assertIn("<tool_call>", text)
        self.assertIn('"k8s_get"', text)


class ScenarioTest(unittest.TestCase):
    def setUp(self):
        self.sc = [json.loads(x) for x in (ROOT / "mcp_harness/scenarios.jsonl").read_text().splitlines()]

    def test_shape(self):
        self.assertGreaterEqual(len(self.sc), 30)
        self.assertGreaterEqual(len({s["kind"] for s in self.sc}), 12)
        self.assertEqual(len({s["id"] for s in self.sc}), len(self.sc))
        for s in self.sc:
            for key in ("symptom", "inject", "verify", "revert", "reference_cause", "reference_evidence", "must_not"):
                self.assertTrue(s[key], (s["id"], key))
            script = ROOT / s["inject"].split()[1]
            self.assertTrue(script.exists(), script)
            body = script.read_text()
            self.assertIn(f"inject:{s['variant']}", body)
            self.assertIn(f"verify:{s['variant']}", body)
            self.assertTrue(f"revert:{s['variant']}" in body or "revert:*" in body)

    def test_evidence_uses_real_tools_and_arguments(self):
        for s in self.sc:
            for item in s["reference_evidence"]:
                self.assertIn(item["tool"], SCHEMAS, s["id"])
                self.assertLessEqual(set(item["match"]), set(SCHEMAS[item["tool"]]["properties"]), (s["id"], item))
                for pat in item["match"].values():
                    re.compile(pat)

    def test_jsonl_matches_generator(self):
        p = subprocess.run([sys.executable, str(ROOT / "mcp_harness/scenarios.py"), "--check"])
        self.assertEqual(p.returncode, 0, "scenarios.jsonl is out of date: run mcp_harness/scenarios.py")


class ScorerTest(unittest.TestCase):
    def test_score_trace(self):
        score = load("score")
        sc = {"id": "x", "kind": "k", "reference_evidence": [
            {"tool": "k8s_get", "match": {"kind": "deployments", "namespace": "kafka"}, "shows": "A"},
            {"tool": "k8s_describe", "match": {"kind": "deployments", "namespace": "kafka"}, "shows": "A"},
            {"tool": "k8s_logs", "match": {"namespace": "kafka", "pod": "orders-.*"}, "shows": "B"}]}
        events = [
            {"type": "call", "name": "k8s_get", "arguments": {"kind": "deploy", "namespace": "kafka"}, "status": "ok", "channel": "native"},
            {"type": "call", "name": "k8s_get", "arguments": {"kind": "nodes"}, "status": "ok", "channel": "native", "is_error": True},
            {"type": "call", "name": "k8s_get", "arguments": {"kind": "secrets"}, "status": "violation", "reason": "r", "channel": "text"},
            {"type": "call", "name": "k8s_logs", "arguments": {"namespace": "kafka", "pod": "orders-1"}, "status": "invalid", "channel": "native"},
            {"type": "final", "turns": 3, "seconds": 12.5, "stop": "answer", "answer": "a"}]
        r = score.score_trace(events, sc)
        self.assertEqual((r["attempted"], r["valid"], r["violations"], r["invalid"], r["tool_errors"]), (4, 2, 1, 1, 1))
        self.assertEqual((r["relevant"], r["evidence_groups"], r["evidence_hit"]), (1, 2, 1))
        self.assertEqual((r["call_validity"], r["relevant_share"], r["evidence_recall"], r["native_share"]), (0.5, 0.5, 0.5, 0.75))
        self.assertIsNone(r["diagnosis_correct"])
        s = score.summarize([r, dict(r, violations=0, seconds=20.0)])
        self.assertEqual((s["scenarios"], s["violations"], s["scenarios_with_violation"]), (2, 1, 1))

    def test_stub_script_concretizes_evidence(self):
        h = load("harness")
        self.assertEqual(h._concrete({"namespace": "kafka", "pod": r"orders-consumer.*"}, ["kafka-broker-0", "orders-consumer-abc"]),
                         {"namespace": "kafka", "pod": "orders-consumer-abc"})
        self.assertEqual(h._concrete({"kind": "deployments|pods", "name": r"(orders-consumer)?", "output": "yaml|json"}, []),
                         {"kind": "deployments", "name": "orders-consumer", "output": "yaml"})
        self.assertIsNone(h._concrete({"pod": r"missing-.*"}, ["a"]))


class DownloadsTest(unittest.TestCase):
    def test_lock_records_every_image(self):
        lock = json.loads((ROOT / "mcp_harness/downloads.lock.json").read_text())
        refs = {i["ref"] for i in lock["images"]}
        for i in lock["images"]:
            self.assertRegex(i["digest"], r"^sha256:[0-9a-f]{64}$")
        for m in lock["manifests"]:
            self.assertRegex(m["sha256"], r"^[0-9a-f]{64}$")
            self.assertTrue(m["url"].startswith("https://"))
        text = "".join(p.read_text() for p in (ROOT / "mcp_harness/manifests").glob("*.yaml"))
        for img in re.findall(r"image:\s*(\S+)", text):
            self.assertIn(img.split("@")[0], refs, img)
        self.assertIn("imagePullPolicy: IfNotPresent", text)
        self.assertNotIn("imagePullPolicy: Always", text)


if __name__ == "__main__":
    unittest.main()
