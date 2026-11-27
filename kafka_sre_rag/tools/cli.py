"""`python -m kafka_sre_rag diagnose --tools mcp --kubeconfig FILE --kube-context NAME ...`: diagnose a live
problem with read-only tools.

EXPERIMENTAL and UNMEASURED (M7, W30/W31). The session allow-list (default: allowlist_readonly.json) names the local
MCP servers and the tools; the runtime enforces it. Every call is shown on stdout and written to the trace.
The kubeconfig file and the context are required: the tools never use the user's default kubeconfig or its
current context.
"""
import copy
import json
import os
import sys
from pathlib import Path

from . import kubectx
from .allowlist import AllowList
from .loop import OpenAIChatModel, ScriptedModel, Session, run
from .mcp_client import StdioServer

ROOT = Path(__file__).resolve().parents[2]


def fill_parser(d):
    """The arguments of `diagnose` (kafka_sre_rag/__main__.py builds this parser only for that command)."""
    d.add_argument("question")
    d.add_argument("--tools", choices=["mcp"], required=True, help="tool source: mcp (local MCP servers, stdio)")
    d.add_argument("--allowlist", help="session allow-list JSON (default: kafka_sre_rag/tools/allowlist_readonly.json)")
    d.add_argument("--kubeconfig", required=True,
                   help="kubeconfig file for the k8s server (required; the default kubeconfig is never used)")
    d.add_argument("--kube-context", required=True, help="context in that kubeconfig (required; no default)")
    d.add_argument("--expect-context", metavar="NAME",
                   help="refuse unless --kube-context equals NAME (the harness passes kind-kafka-sre-mcp)")
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


def session_from(allowlist_path, kubeconfig, kube_context, expect_context=None):
    """The session's servers; the k8s server gets the explicit kubeconfig file and context (both required).
    Refuses (ValueError) before any server starts when the context differs from `expect_context` or is not in the file."""
    if not kubeconfig or not kube_context:
        raise ValueError("a kubeconfig file and a kube context are both required")
    kubeconfig = os.path.abspath(kubeconfig)
    if not os.path.isfile(kubeconfig):
        raise ValueError(f"kubeconfig {kubeconfig}: no such file")
    if expect_context and kube_context != expect_context:
        raise ValueError(f"--kube-context is {kube_context}, expected {expect_context}")
    try:
        kubectx.check_context_exists(kubeconfig, kube_context)
    except kubectx.ContextError as e:
        raise ValueError(str(e))
    allow = AllowList.load(allowlist_path)
    servers = []
    try:
        for name, spec in allow.servers.items():
            spec = copy.deepcopy(spec)
            if name == "k8s":
                spec["command"] = spec["command"] + ["--kubeconfig", kubeconfig, "--context", kube_context,
                                                       "--expect-context", kube_context]
            spec.setdefault("cwd", str(ROOT))
            servers.append(StdioServer(name, spec))
    except BaseException:
        for s in servers:
            s.close()
        raise
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
    try:
        session = session_from(a.allowlist, a.kubeconfig, a.kube_context, a.expect_context)
    except ValueError as e:
        print(f"diagnose: {e}", file=sys.stderr)
        return 2
    print("EXPERIMENTAL and UNMEASURED: diagnosis with read-only tools; every call is shown below.", file=sys.stderr)
    try:
        res = run(a.question, model, session, a.max_turns, a.max_seconds, a.trace, a.text_protocol)
    finally:
        session.close()
    c = res["calls"]
    print(f"\n[{res['stop']}: {res['turns']} turns, {res['seconds']:.1f} s; calls run {c['ok']}, blocked "
          f"{c['violation']} (violations), rejected {c['invalid']} (invalid), tool errors {c['error']}]")
    if res.get("error"):
        print(f"[the model request failed twice: {res['error']['error']}"
              f"{' HTTP ' + str(res['error']['http_status']) if res['error'].get('http_status') else ''}]")
    print(res["answer"] or "(no answer)")
    return 0 if res["stop"] != "failed" else 1
