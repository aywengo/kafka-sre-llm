"""Offline tests of the tool runtime (kafka_sre_rag/tools) and the MCP harness (mcp_harness): no cluster, no server."""
import importlib.util
import io
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import unittest
import unittest.mock
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
        super().__init__(context="ctx", kubeconfig="kc", run=self.fake)
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
        self.assertEqual(r.argvs[0], ["kubectl", "--kubeconfig", "kc", "--context", "ctx", "--request-timeout=20s", "get",
                                      "kafkas.kafka.strimzi.io", "-n", "kafka", "-o", "yaml"])
        self.assertEqual(r.argvs[1][6:], ["logs", "-n", "kafka", "p-0", "--tail=50", "--previous"])
        self.assertEqual(r.argvs[2][6:], ["events", "-n", "kafka", "--types", "Warning"])
        for argv in r.argvs:
            self.assertIn(argv[6], ("get", "describe", "logs", "events"))

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
        self.assertEqual(execv[6:12], ["exec", "-n", "kafka", "kafka-broker-1", "-c", "kafka"])
        self.assertEqual(execv[13:], ["/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092",
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
        kept = "automountServiceAccountToken: false\n- serviceAccountToken:\n    expirationSeconds: 3600\n"
        self.assertEqual(srv.redact(kept), kept)
        self.assertEqual(srv.redact("token: abc.def"), "token: [REDACTED]")

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
        kc = tempfile.NamedTemporaryFile("w", suffix=".kubeconfig", delete=False)
        kc.close()
        self.addCleanup(Path(kc.name).unlink)
        s = StdioServer("k8s", {"command": ["{python}", "-m", "kafka_sre_rag.tools.k8s_server", "--kubeconfig", kc.name,
                                            "--context", "none"], "cwd": str(ROOT)})
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

    def test_openai_client_request_and_native_calls(self):
        import httpx
        from kafka_sre_rag.tools.loop import OpenAIChatModel
        seen = []

        def handler(request):
            seen.append(json.loads(request.content))
            msg = {"role": "assistant", "content": None, "tool_calls": [
                {"id": "c1", "type": "function", "function": {"name": "k8s_get", "arguments": "{\"kind\": \"nodes\"}"}}]}
            return httpx.Response(200, json={"choices": [{"message": msg}]})

        m = OpenAIChatModel("http://127.0.0.1:9/v1", "some-model", transport=httpx.MockTransport(handler))
        tools = Session(AllowList.load(), [FakeServer()]).openai_tools()
        r = m.chat([{"role": "user", "content": "q"}], tools)
        self.assertEqual(r["tool_calls"], [{"id": "c1", "name": "k8s_get", "arguments": "{\"kind\": \"nodes\"}"}])
        body = seen[0]
        self.assertEqual((body["temperature"], body["stream"], body["tool_choice"], body["model"]), (0.0, False, "auto", "some-model"))
        self.assertEqual(len(body["tools"]), 8)
        m.chat([{"role": "user", "content": "q"}], None)
        self.assertNotIn("tools", seen[1])

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
            {"type": "request", "turn": "final", "n_calls": 2},
            {"type": "after_budget_call", "name": "k8s_get", "status": "violation"},
            {"type": "after_budget_call", "name": "k8s_get", "status": "ok"},
            {"type": "final", "turns": 3, "seconds": 12.5, "stop": "answer", "answer": "a"}]
        r = score.score_trace(events, sc)
        self.assertEqual((r["attempted"], r["valid"], r["violations"], r["invalid"], r["tool_errors"]), (4, 2, 1, 1, 1))
        self.assertEqual((r["relevant"], r["evidence_groups"], r["evidence_hit"]), (1, 2, 1))
        self.assertEqual((r["call_validity"], r["relevant_share"], r["evidence_recall"], r["native_share"]), (0.5, 0.5, 0.5, 0.75))
        self.assertIsNone(r["diagnosis_correct"])
        self.assertEqual((r["calls_after_cap"], r["calls_after_cap_outside"]), (2, 1))
        s = score.summarize([r, dict(r, violations=0, seconds=20.0)])
        self.assertEqual((s["scenarios"], s["violations"], s["scenarios_with_violation"]), (2, 1, 1))

    def test_paired_compare(self):
        score = load("score")
        a = [{"id": str(i), "evidence_recall": 0.5} for i in range(10)]
        b = [{"id": str(i), "evidence_recall": 0.5 + (0.1 if i % 2 else 0.0)} for i in range(10)] + [{"id": "x", "evidence_recall": 1.0}]
        c = score.compare(a, b, resamples=2000)
        self.assertEqual((c["pairs"], c["mean_difference"]), (10, 0.05))
        self.assertLessEqual(c["interval_90"][0], 0.05)
        self.assertGreaterEqual(c["interval_90"][1], 0.05)
        self.assertEqual(c, score.compare(a, b, resamples=2000))  # seeded

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


# ---- review fixes (S1 to S7, N1, N2, N4, N5) ----------------------------------------------------------------------

FAKE_URL = "http://made-up-host.invalid:9999/v1"  # a made-up address: it must never reach a trace or a log


class FakeHTTPError(Exception):
    def __init__(self, status):
        super().__init__(f"Server error '{status}' for url '{FAKE_URL}/chat/completions'")
        self.response = type("R", (), {"status_code": status})()


class ReviewLoopTest(unittest.TestCase):
    def run_loop(self, model, **kw):
        fake = FakeServer()
        session = Session(AllowList.load(), [fake])
        with tempfile.TemporaryDirectory() as d:
            trace = Path(d) / "t.jsonl"
            res = run("why?", model, session, trace_path=str(trace), on_event=None, retry_wait=0, **kw)
            text = trace.read_text()
        return res, [json.loads(x) for x in text.splitlines()], fake, text

    def test_s1_a_failed_request_is_retried_once(self):
        model = ScriptedModel([FakeHTTPError(500), {"content": "the cause"}])
        res, events, _, text = self.run_loop(model)
        self.assertEqual((res["stop"], res["answer"]), ("answer", "the cause"))
        errs = [e for e in events if e["type"] == "model_error"]
        self.assertEqual([(e["attempt"], e["error"], e["http_status"]) for e in errs], [(1, "FakeHTTPError", 500)])
        self.assertNotIn("made-up-host", text)

    def test_s1_two_failures_end_the_session_as_failed(self):
        model = ScriptedModel([{"tool_calls": [{"name": "k8s_get", "arguments": {"kind": "nodes"}}]},
                               FakeHTTPError(503), TimeoutError("connect timed out to " + FAKE_URL)])
        res, events, _, text = self.run_loop(model)
        self.assertEqual((res["stop"], res["turns"], res["answer"]), ("failed", 1, None))
        self.assertEqual(res["error"], {"error": "TimeoutError", "http_status": None})
        self.assertEqual(events[-1]["type"], "final")
        self.assertEqual([e["http_status"] for e in events if e["type"] == "model_error"], [503, None])
        self.assertNotIn("made-up-host", text)

    def test_s1_another_exception_aborts_without_a_final_event(self):
        class Broken(FakeServer):
            def call(self, name, args):
                raise RuntimeError(FAKE_URL)
        with tempfile.TemporaryDirectory() as d:
            trace = Path(d) / "t.jsonl"
            with self.assertRaises(RuntimeError):
                run("why?", ScriptedModel([{"tool_calls": [{"name": "k8s_get", "arguments": {"kind": "nodes"}}]}]),
                    Session(AllowList.load(), [Broken()]), trace_path=str(trace), on_event=None, retry_wait=0)
            text = trace.read_text()
        last = json.loads(text.splitlines()[-1])
        self.assertEqual((last["type"], last["error"]), ("aborted", "RuntimeError"))
        self.assertNotIn("made-up-host", text)

    def test_s2_a_ninth_call_is_still_checked(self):
        calls = [{"name": "k8s_get", "arguments": {"kind": "nodes"}}] * 8 + [
            {"name": "k8s_get", "arguments": {"kind": "secrets", "namespace": "kafka"}},
            {"name": "k8s_logs", "arguments": {"namespace": "kafka", "pod": "Bad_Name"}},
            {"name": "k8s_get", "arguments": {"kind": "nodes"}}]
        res, events, fake, _ = self.run_loop(ScriptedModel([{"tool_calls": calls}, {"content": "x"}]))
        self.assertEqual([e["status"] for e in events if e["type"] == "call"][8:], ["violation", "invalid", "skipped"])
        self.assertEqual(res["calls"], {"ok": 8, "violation": 1, "invalid": 1, "skipped": 1, "error": 0})
        self.assertEqual(len(fake.calls), 8)

    def test_s3_a_text_call_that_is_not_json_is_invalid(self):
        content = '<tool_call>{"name": "k8s_get", "arguments": {"kind": </tool_call><tool_call>{"arguments": {}}</tool_call>'
        res, events, fake, _ = self.run_loop(ScriptedModel([{"content": content}, {"content": "done"}]))
        calls = [e for e in events if e["type"] == "call"]
        self.assertEqual([(c["status"], c["name"]) for c in calls], [("invalid", ""), ("invalid", "")])
        self.assertIn("could not be parsed", calls[0]["reason"])
        self.assertEqual((res["calls"]["violation"], res["calls"]["invalid"], fake.calls), (0, 2, []))

    def test_s4_text_calls_after_the_budget_are_checked_not_run_and_no_answer(self):
        first = {"tool_calls": [{"name": "k8s_get", "arguments": {"kind": "nodes"}}]}
        final = ('<tool_call>{"name": "k8s_get", "arguments": {"kind": "secrets", "namespace": "kafka"}}</tool_call>\n'
                 '<tool_call>{"name": "k8s_get", "arguments": {"kind": "pods", "namespace": "kafka"}}</tool_call>')
        res, events, fake, _ = self.run_loop(ScriptedModel([first, {"content": final}]), max_turns=1)
        self.assertEqual((res["stop"], res["answer"]), ("turn_cap", None))
        self.assertEqual(res["after_budget_calls"], {"calls": 2, "violation": 1, "invalid": 0})
        self.assertEqual([e["status"] for e in events if e["type"] == "after_budget_call"], ["violation", "ok"])
        self.assertEqual(len(fake.calls), 1)  # only the call of turn 1 ran
        r = load("score").score_trace(events, {"id": "x", "kind": "k", "reference_evidence": []})
        self.assertEqual((r["attempted"], r["calls_after_cap"], r["calls_after_cap_outside"], r["answered"]), (1, 2, 1, False))
        res, *_ = self.run_loop(ScriptedModel([first, {"content": "The cause is X.\n" + final}]), max_turns=1)
        self.assertEqual(res["answer"], "The cause is X.")

    def test_s6_each_request_gets_the_time_left_and_the_cap_is_time_cap(self):
        model = ScriptedModel([{"tool_calls": [{"name": "k8s_get", "arguments": {"kind": "nodes"}}], "finish_reason": "tool_calls"},
                               {"content": "final", "finish_reason": "length"}])
        res, events, *_ = self.run_loop(model, max_turns=1, max_seconds=30)
        timeouts = [r["timeout"] for r in model.requests]
        self.assertEqual(len(timeouts), 2)  # the final request too
        self.assertTrue(all(0 < t <= 30 for t in timeouts), timeouts)
        self.assertGreaterEqual(timeouts[0], timeouts[1])
        self.assertEqual([e["finish_reason"] for e in events if e["type"] == "request"], ["tool_calls", "length"])

        class Slow:
            identity = {"backend": "stub", "model": "slow"}

            def __init__(self):
                self.timeouts = []

            def chat(self, messages, tools, timeout=None):
                self.timeouts.append(timeout)
                time.sleep(timeout)
                raise TimeoutError("read timed out")
        slow = Slow()
        res, events, *_ = self.run_loop(slow, max_seconds=0.4)
        self.assertEqual((res["stop"], res["turns"], res["answer"]), ("time_cap", 0, None))
        self.assertEqual(len(slow.timeouts), 1)  # no retry at the cap
        self.assertLessEqual(slow.timeouts[0], 0.4)
        self.assertTrue([e for e in events if e["type"] == "model_error"][0]["at_time_cap"])

        class Late(Slow):
            def chat(self, messages, tools, timeout=None):
                time.sleep(timeout + 0.1)
                return {"content": "a late answer", "tool_calls": [], "finish_reason": "stop"}
        res, events, *_ = self.run_loop(Late(), max_seconds=0.3)
        self.assertEqual((res["stop"], res["answer"]), ("time_cap", None))
        self.assertTrue([e for e in events if e["type"] == "request"][0]["late"])

    def test_s6_an_unclosed_tool_call_is_invalid_not_an_answer(self):
        content = 'Let me check.\n<tool_call>{"name": "k8s_get", "arguments": {"kind": "pods"'
        self.assertEqual(parse_text_calls(content)[0][0], "")
        res, events, fake, _ = self.run_loop(ScriptedModel([{"content": content}, {"content": "the cause"}]))
        self.assertEqual((res["stop"], res["turns"], res["answer"]), ("answer", 2, "the cause"))
        self.assertEqual([e["status"] for e in events if e["type"] == "call"], ["invalid"])

    def test_openai_client_timeout_and_finish_reason(self):
        import httpx
        from kafka_sre_rag.tools.loop import OpenAIChatModel
        seen = []

        def handler(request):
            seen.append(request.extensions.get("timeout"))
            return httpx.Response(200, json={"choices": [{"message": {"content": "x"}, "finish_reason": "length"}]})
        m = OpenAIChatModel("http://127.0.0.1:9/v1", "m", transport=httpx.MockTransport(handler))
        self.assertEqual(m.chat([{"role": "user", "content": "q"}], None, timeout=7.5)["finish_reason"], "length")
        self.assertEqual(seen[0]["read"], 7.5)


class ReviewScorerTest(unittest.TestCase):
    def test_s5_an_errored_call_is_no_evidence(self):
        score = load("score")
        sc = {"id": "x", "kind": "k", "reference_evidence": [
            {"tool": "k8s_get", "match": {"kind": "kafkatopics", "namespace": "kafka", "name": "orders", "output": "yaml|json"},
             "shows": "min.insync.replicas: 3"}]}
        call = {"type": "call", "name": "k8s_get", "status": "ok", "channel": "native",
                "arguments": {"kind": "kafkatopics", "namespace": "kafka", "name": "orders", "output": "yaml"}}
        r = score.score_trace([dict(call, is_error=True), {"type": "final", "stop": "answer", "answer": "a"}], sc)
        self.assertEqual((r["valid"], r["tool_errors"], r["relevant"], r["evidence_hit"]), (1, 1, 0, 0))
        r = score.score_trace([call, {"type": "final", "stop": "answer", "answer": "a"}], sc)
        self.assertEqual((r["relevant"], r["evidence_hit"]), (1, 1))
        wide = dict(call, arguments=dict(call["arguments"], output="wide"))
        self.assertEqual(score.score_trace([wide], sc)["evidence_hit"], 0)  # -o wide does not show the config

    def test_s5_facts_wide_does_not_show_need_yaml_or_json(self):
        sc = [json.loads(x) for x in (ROOT / "mcp_harness/scenarios.jsonl").read_text().splitlines()]
        need = re.compile(r"min\.insync|observedGeneration|ComparisonError|operationState|ACL|quotas|status\.secret|"
                          r"jvmOptions|storage class fast|size 2Gi|retention\.ms|listener|warning about")
        for s in sc:
            for it in s["reference_evidence"]:
                if it["tool"] == "k8s_get" and need.search(it["shows"]):
                    self.assertEqual(it["match"].get("output"), "yaml|json", (s["id"], it["shows"]))


class ReviewServerTest(unittest.TestCase):
    def test_s7_redaction(self):
        cases = {
            "json env": '{"name": "DB_PASSWORD", "value": "made-up-pw-1"}',
            "json env escaped": '{\\"name\\":\\"API_TOKEN\\",\\"value\\":\\"made-up-tok-2\\"}',
            "PGPASSWORD": "      PGPASSWORD:  made-up-pw-3",
            "AWS secret": "      AWS_SECRET_ACCESS_KEY:  made-up-aws-4",
            "env line in a log": "export AWS_SECRET_ACCESS_KEY=made-up-aws-5 && run",
            "json map": '{"PGPASSWORD": "made-up-pw-6"}',
            "yaml env": "- name: KAFKA_SASL_JAAS_CONFIG_PASSWORD\n  value: made-up-pw-7",
            "bearer": "Authorization: Bearer made-up-bearer-token-8",
            "bearer in a log": "sending header bearer abcdefgh.made-up-9",
            "basic": "authorization: Basic made-up-basic-10",
        }
        for what, text in cases.items():
            out = srv.redact(text)
            self.assertNotIn("made-up", out, what)
            self.assertIn("[REDACTED]", out, what)
        kept = ("automountServiceAccountToken: false\n      KEYSTORE_MODE:  selfsigned\n      STRIMZI_NAMESPACE:  kafka-prod\n"
                "  secret: app-consumer\n" '{"name": "BOOTSTRAP", "value": "kafka-kafka-bootstrap:9093"}\n'
                "      DB_PASSWORD:  <set to the key 'password' in secret 'db'>  Optional: false")
        self.assertEqual(srv.redact(kept), kept)

    def test_s7_last_applied_configuration_is_dropped(self):
        lac = '{"apiVersion":"apps/v1","spec":{"env":[{"name":"X","value":"made-up-lac"}]}}'
        yaml_text = ("metadata:\n  annotations:\n    deployment.kubernetes.io/revision: \"3\"\n"
                     "    kubectl.kubernetes.io/last-applied-configuration: |\n      " + lac + "\n"
                     "  creationTimestamp: \"2026-11-26T09:45:00Z\"\n  name: orders-consumer\n")
        json_text = json.dumps({"metadata": {"annotations": {"kubectl.kubernetes.io/last-applied-configuration": lac + "\n",
                                                             "other": "kept"}, "name": "orders-consumer"}}, indent=1)
        describe = ("Annotations:      deployment.kubernetes.io/revision: 3\n"
                    "                  kubectl.kubernetes.io/last-applied-configuration:\n                    " + lac + "\n"
                    "Selector:         app=orders-consumer\n")
        for text in (yaml_text, json_text, describe):
            out = srv.redact(text)
            self.assertNotIn("made-up-lac", out)
            self.assertIn("orders-consumer", out)
            self.assertIn("last-applied-configuration", out)
        self.assertEqual(json.loads(srv.redact(json_text))["metadata"]["annotations"]["other"], "kept")
        self.assertIn("creationTimestamp", srv.redact(yaml_text))
        self.assertIn("Selector:", srv.redact(describe))

    def test_n1_no_value_starting_with_a_dash(self):
        r = FakeRunner()
        for name, args in (("kafka_topics_describe", {"namespace": "kafka", "topic": "--delete"}),
                           ("k8s_get", {"kind": "pods", "namespace": "kafka", "selector": "-l"}),
                           ("kafka_configs_describe", {"namespace": "kafka", "entity_type": "users", "entity_name": "--alter"}),
                           ("kafka_consumer_groups_describe", {"namespace": "kafka", "group": "-x"})):
            err, text = srv.call_tool(r, name, args)
            self.assertTrue(err, (name, args))
            self.assertIn("invalid arguments", text)
        self.assertEqual(r.argvs, [])
        self.assertEqual(validate({"namespace": "kafka", "topic": "__consumer_offsets"}, SCHEMAS["kafka_topics_describe"]), [])
        self.assertEqual(validate({"kind": "pods", "selector": "strimzi.io/cluster=kafka,!x"}, SCHEMAS["k8s_get"]), [])
        self.assertEqual(validate({"namespace": "kafka", "entity_type": "users", "entity_name": "CN=app-consumer"},
                                  SCHEMAS["kafka_configs_describe"]), [])

    def test_n2_the_server_survives_bad_json_and_bad_messages(self):
        r = FakeRunner({"pods": "not json", "applications.argoproj.io": "not json either"})
        err, text = srv.call_tool(r, "kafka_topics_describe", {"namespace": "kafka"})
        self.assertTrue(err)
        self.assertIn("could not read the broker pods", text)
        err, text = srv.call_tool(r, "argocd_app_get", {"name": "kafka-topics"})
        self.assertTrue(err)
        self.assertIn("could not read the Application", text)
        out = io.StringIO()
        lines = ["[1, 2]", "{bad", json.dumps({"jsonrpc": "2.0", "id": 5, "method": "tools/call", "params": "oops"}),
                 json.dumps({"jsonrpc": "2.0", "id": 6, "method": "ping"})]
        srv.serve(FakeRunner(), io.StringIO("\n".join(lines) + "\n"), out)
        resp = [json.loads(x) for x in out.getvalue().splitlines()]
        self.assertEqual([x.get("error", {}).get("code") for x in resp], [-32600, -32700, -32603, None])
        self.assertEqual(resp[-1]["result"], {})

    def test_s7_server_refuses_to_start_without_kubeconfig_and_context(self):
        with self.assertRaises(ValueError):
            srv.Runner(None, "kc")
        with self.assertRaises(ValueError):
            srv.Runner("ctx", None)
        base = [sys.executable, "-m", "kafka_sre_rag.tools.k8s_server"]
        for extra in ([], ["--context", "kind-x"], ["--kubeconfig", "/nonexistent/kubeconfig", "--context", "kind-x"]):
            p = subprocess.run(base + extra, cwd=ROOT, capture_output=True, text=True, stdin=subprocess.DEVNULL, timeout=60)
            self.assertEqual(p.returncode, 2, (extra, p.stderr))
        from kafka_sre_rag.tools.cli import session_from
        for kc, ctx in ((None, "ctx"), ("/nonexistent/kubeconfig", "ctx")):
            with self.assertRaises(ValueError):
                session_from(None, kc, ctx)

    def test_s7_diagnose_requires_kubeconfig_and_context(self):
        from kafka_sre_rag import __main__ as cli_main
        for argv in (["diagnose", "q", "--tools", "mcp", "--stub", "x.json"],
                     ["diagnose", "q", "--tools", "mcp", "--stub", "x.json", "--kube-context", "kind-x"],
                     ["diagnose", "q", "--tools", "mcp", "--stub", "x.json", "--kubeconfig", "kc"]):
            with self.assertRaises(SystemExit) as cm, unittest.mock.patch("sys.stderr", io.StringIO()):
                cli_main.main(argv)
            self.assertEqual(cm.exception.code, 2, argv)

    def test_n4_no_secret_env_reaches_the_server_or_kubectl(self):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            kc = d / "kubeconfig"
            kc.write_text("apiVersion: v1\nkind: Config\n")
            dump, argv_dump = d / "env.txt", d / "argv.txt"
            fake = d / "kubectl"
            fake.write_text(f"#!/bin/sh\nenv > '{dump}'\nprintf '%s\\n' \"$@\" > '{argv_dump}'\necho 'NAME  READY'\n")
            fake.chmod(0o755)
            env = {"PATH": f"{d}{os.pathsep}{os.environ.get('PATH', '')}", "OMLX_API_KEY": "made-up-omlx-key",
                   "JUDGE_API_KEY": "made-up-judge-key", "AWS_SECRET_ACCESS_KEY": "made-up-aws", "OMLX_URL": FAKE_URL}
            with unittest.mock.patch.dict(os.environ, env):
                from kafka_sre_rag.tools.cli import session_from
                session = session_from(None, str(kc), "kind-x")
                try:
                    err, text = session.call("k8s_get", {"kind": "pods", "namespace": "kafka"})
                    server_env = session.servers[0].proc.args  # the command line of the server
                finally:
                    session.close()
            self.assertFalse(err, text)
            seen = dump.read_text()
            for secret in ("made-up-omlx-key", "made-up-judge-key", "made-up-aws", "made-up-host"):
                self.assertNotIn(secret, seen)
            self.assertIn(f"KUBECONFIG={kc}", seen)
            argv = argv_dump.read_text().split("\n")
            self.assertEqual(argv[:4], ["--kubeconfig", str(kc), "--context", "kind-x"])
            self.assertIn("--kubeconfig", server_env)
            self.assertEqual(set(srv.minimal_env("k")) - {"HOME"}, {"PATH", "KUBECONFIG"})

    def test_n5_ask_and_verify_do_not_import_the_tools_package(self):
        code = ("import sys; import kafka_sre_rag.__main__ as m; p = m.build_parser(); "
                "p.parse_args(['ask', '--dry-run', 'q']); p.parse_args(['verify']); "
                "print('kafka_sre_rag.tools' in sys.modules)")
        p = subprocess.run([sys.executable, "-c", code], cwd=ROOT, capture_output=True, text=True, timeout=120)
        self.assertEqual((p.returncode, p.stdout.strip()), (0, "False"), p.stderr[-500:])
        p = subprocess.run([sys.executable, "-m", "kafka_sre_rag", "diagnose", "--help"], cwd=ROOT, capture_output=True,
                           text=True, timeout=120)
        self.assertEqual(p.returncode, 0)
        self.assertIn("--kubeconfig", p.stdout)
        p = subprocess.run([sys.executable, "-m", "kafka_sre_rag", "--help"], cwd=ROOT, capture_output=True, text=True, timeout=120)
        self.assertIn("diagnose", p.stdout)


class ReviewHarnessTest(unittest.TestCase):
    """S1 in the harness: restore runs whatever happens, only the exception type is printed, and a trace without a
    final event is moved aside so a rerun repeats the scenario; a "failed" scenario is final."""

    def setUp(self):
        self.h = load("harness")
        self.calls = []
        h = self.h
        sc = {"id": "s1", "symptom": "why?", "verify_timeout": 1, "reference_evidence": []}
        self.patch(h, "pick", lambda ids: [sc])
        self.patch(h, "ensure_kubeconfig", lambda: None)
        self.patch(h, "ensure_healthy", lambda: True)
        self.patch(h, "inject_verified", lambda s: (True, 0.1))
        self.patch(h, "restore", lambda s: self.calls.append("restore") or True)
        self.patch(h, "cluster", lambda action, timeout=0: (0, '{"new_image_refs": 0, "pulling_events": 0}'))
        self.patch(h, "make_session", lambda: Session(AllowList.load(), [FakeServer()]))
        self.patch(h, "still_visible", lambda s: self.calls.append("reverify") or True)
        self.patch(h, "log", lambda msg: self.calls.append(msg))

    def patch(self, obj, name, value):
        p = unittest.mock.patch.object(obj, name, value)
        p.start()
        self.addCleanup(p.stop)

    def args(self, out):
        return type("A", (), dict(model="stub", ids=None, out=out, max_turns=12, max_seconds=600, text_protocol=False,
                                  base_url=None, model_name=None, max_tokens=10, extra_body=None))()

    def test_restore_runs_and_no_url_is_printed(self):
        def boom(*a, **k):
            raise RuntimeError(f"connection refused: {FAKE_URL}")
        self.patch(self.h, "run_loop", boom)
        with tempfile.TemporaryDirectory() as d, self.assertRaises(SystemExit) as cm:
            self.h.cmd_run(self.args(d))
        self.assertIn("restore", self.calls)
        self.assertIn("RuntimeError", str(cm.exception.code))
        self.assertNotIn("made-up-host", str(cm.exception.code) + " ".join(map(str, self.calls)))

    def test_aborted_trace_is_moved_aside_and_failed_is_final(self):
        with tempfile.TemporaryDirectory() as d:
            traces = Path(d) / "traces"
            traces.mkdir()
            (traces / "s1.jsonl").write_text(json.dumps({"type": "start"}) + "\n" + json.dumps({"type": "aborted"}) + "\n")
            self.patch(self.h, "stub_model", lambda sc: ScriptedModel([FakeHTTPError(500), FakeHTTPError(500)]))
            real = self.h.run_loop
            self.patch(self.h, "run_loop", lambda *a, **k: real(*a, **dict(k, on_event=None, retry_wait=0)))
            self.h.cmd_run(self.args(d))
            self.assertTrue((traces / "s1.jsonl.aborted-1").exists())
            final = json.loads((traces / "s1.jsonl").read_text().splitlines()[-1])
            self.assertEqual((final["type"], final["stop"]), ("final", "failed"))
            row = json.loads((Path(d) / "runs.jsonl").read_text().splitlines()[-1])
            self.assertEqual(row["diagnosis"]["stop"], "failed")
            self.assertTrue(row["visible_after"])  # the fault was verified once more after the session
            self.assertLess(self.calls.index("reverify"), self.calls.index("restore"))
            self.calls.clear()
            self.h.cmd_run(self.args(d))  # a rerun: the failed scenario stays as it is
            self.assertTrue(any("skipped" in str(c) for c in self.calls))
            self.assertNotIn("restore", self.calls)

    def test_check_faults_records_whether_the_fault_persists(self):
        self.patch(self.h, "still_visible", lambda s: self.calls.append("reverify") or False)
        self.patch(self.h.time, "sleep", lambda s: self.calls.append(f"hold {s}"))
        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "fc.jsonl"
            self.h.cmd_check_faults(type("A", (), dict(ids=None, out=str(out), hold=150))())
            row = json.loads(out.read_text().splitlines()[-1])
        self.assertEqual((row["visible"], row["persists"], row["healthy_after"]), (True, False, True))
        self.assertLess(self.calls.index("hold 150"), self.calls.index("reverify"))
        self.assertLess(self.calls.index("reverify"), self.calls.index("restore"))


class FaultCheckFixesTest(unittest.TestCase):
    """Fixes from fault check 7: the user operator removes a quota on a user without a KafkaUser (ANONYMOUS) at its
    2-minute timer, so producer_byte_rate sits on the producer's client id; the resize revert pauses reconciliation
    before it corrects the recorded size, so a reconciliation still running with 2Gi cannot write 2Gi back."""

    def test_producer_quota_is_on_the_client_id_in_fault_and_evidence(self):
        q = (ROOT / "mcp_harness/faults/quota.sh").read_text()
        self.assertIn("CLIENT=perf-producer-client", q)
        self.assertIn('--alter --entity-type clients --entity-name "$CLIENT" --add-config producer_byte_rate=300', q)
        self.assertNotIn("--entity-name ANONYMOUS --add-config", q)
        sc = {json.loads(x)["id"]: json.loads(x) for x in (ROOT / "mcp_harness/scenarios.jsonl").read_text().splitlines()}
        ev = [e for e in sc["quota-producer-byte-rate"]["reference_evidence"] if e["tool"] == "kafka_configs_describe"]
        self.assertEqual([(e["match"]["entity_type"], e["match"]["entity_name"]) for e in ev],
                         [("clients", "(perf-producer-client)?")])

    def test_resize_revert_pauses_before_it_corrects_and_unpauses_last(self):
        s = (ROOT / "mcp_harness/faults/storage.sh").read_text()
        r = s[s.index("revert:resize_not_supported)"):]
        r = r[:r.index(";;")]
        order = [r.index(x) for x in ("pause-reconciliation=true", "wait_until", "strimzipodset kafka-broker",
                                      "replace -f")]
        self.assertEqual(order, sorted(order))


if __name__ == "__main__":
    unittest.main()
