"""`python -m kafka_sre_rag diagnose --tools mcp ...`: diagnose a live problem with read-only tools.

EXPERIMENTAL and UNMEASURED (M7, W30/W31). The session allow-list (default: allowlist_readonly.json) names the local
MCP servers and the tools; the runtime enforces it. Every call is shown on stdout and written to the trace.
"""
import copy
import json
import os
import sys

from .allowlist import AllowList
from .loop import OpenAIChatModel, ScriptedModel, Session, run
from .mcp_client import StdioServer


def add_parser(sub):
    d = sub.add_parser("diagnose", help="EXPERIMENTAL: diagnose a live problem with read-only tools (local MCP servers)")
    d.add_argument("question")
    d.add_argument("--tools", choices=["mcp"], required=True, help="tool source: mcp (local MCP servers, stdio)")
    d.add_argument("--allowlist", help="session allow-list JSON (default: kafka_sre_rag/tools/allowlist_readonly.json)")
    d.add_argument("--kube-context", help="kubeconfig context passed to the k8s server (default: the current one)")
    d.add_argument("--base-url", help="OpenAI-compatible server URL (default: environment variable OMLX_URL)")
    d.add_argument("--model", help="model name sent to the server")
    d.add_argument("--max-turns", type=int, default=12)
    d.add_argument("--max-seconds", type=int, default=600)
    d.add_argument("--max-tokens", type=int, default=2048)
    d.add_argument("--text-protocol", action="store_true",
                   help="describe the tools in the system prompt and expect <tool_call> blocks (for a server without native tools)")
    d.add_argument("--extra-body", type=json.loads, default=None, metavar="JSON")
    d.add_argument("--trace", help="append the JSONL trace here")
    d.add_argument("--stub", metavar="JSON_FILE", help="no model: replay scripted replies from this file (a dry run)")
    return d


def session_from(allowlist_path=None, kube_context=None):
    allow = AllowList.load(allowlist_path)
    servers = []
    for name, spec in allow.servers.items():
        spec = copy.deepcopy(spec)
        if kube_context and name == "k8s":
            spec["command"] = spec["command"] + ["--context", kube_context]
        spec.setdefault("cwd", str(__import__("pathlib").Path(__file__).resolve().parents[2]))
        servers.append(StdioServer(name, spec))
    return Session(allow, servers)


def main(a):
    if a.stub:
        model = ScriptedModel(json.load(open(a.stub)))
    else:
        url = a.base_url or os.environ.get("OMLX_URL")
        if not url or not a.model:
            print("diagnose: needs --base-url (or OMLX_URL) and --model, or --stub", file=sys.stderr)
            return 2
        model = OpenAIChatModel(url, a.model, os.environ.get("OMLX_API_KEY"), max_tokens=a.max_tokens,
                                extra_body=a.extra_body)
    session = session_from(a.allowlist, a.kube_context)
    print("EXPERIMENTAL and UNMEASURED: diagnosis with read-only tools; every call is shown below.", file=sys.stderr)
    try:
        res = run(a.question, model, session, a.max_turns, a.max_seconds, a.trace, a.text_protocol)
    finally:
        session.close()
    c = res["calls"]
    print(f"\n[{res['stop']}: {res['turns']} turns, {res['seconds']:.1f} s; calls run {c['ok']}, blocked "
          f"{c['violation']} (violations), rejected {c['invalid']} (invalid), tool errors {c['error']}]")
    print(res["answer"] or "(no answer)")
    return 0
