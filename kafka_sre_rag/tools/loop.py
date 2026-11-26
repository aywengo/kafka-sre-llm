"""The tool-use loop: a model, a session of local MCP servers, the allow-list, a turn and time cap, a trace.

One turn is one request to the model. The model gets the allowed tools as native `tools` (OpenAI chat format).
A reply's calls are taken from `tool_calls` (channel "native"); when a reply has none, <tool_call>{...}</tool_call>
blocks in its text are taken instead (channel "text", the documented fallback; with text_protocol=True the system
prompt also describes it). Every call goes through the allow-list first: only an "ok" call reaches a server;
a "violation" or an "invalid" call is answered with a note to the model and nothing is run. Every call is shown
(on_event) and written to the JSONL trace. When the turn cap is reached, one last request without tools asks
for the answer; the time cap stops the loop where it is.
"""
import json
import re
import time
import uuid

from .. import prompts
from .allowlist import AllowList
from .mcp_client import MCPError, StdioServer

MAX_CALLS_PER_TURN = 8
_TEXT_CALL = re.compile(r"<tool_call>\s*(.*?)\s*</tool_call>", re.S)


def parse_text_calls(content):
    """<tool_call>{"name": ..., "arguments": {...}}</tool_call> blocks -> [(name, raw_arguments, parsed_or_None)]."""
    calls = []
    for block in _TEXT_CALL.findall(content or ""):
        try:
            obj = json.loads(block)
        except ValueError:
            calls.append(("", block, None))
            continue
        if not isinstance(obj, dict):
            calls.append(("", block, None))
            continue
        args = obj.get("arguments", obj.get("parameters", {}))
        if isinstance(args, str):
            try:
                args = json.loads(args)
            except ValueError:
                args = None
        calls.append((str(obj.get("name", "")), json.dumps(obj.get("arguments")), args))
    return calls


def _parse_args(raw):
    if isinstance(raw, dict):
        return raw
    try:
        v = json.loads(raw or "{}")
    except (TypeError, ValueError):
        return None
    return v if isinstance(v, dict) else None


class Session:
    """Starts the allow-list's local servers and routes calls; the allow-list is checked here, not by the model."""

    def __init__(self, allowlist=None, servers=None):
        self.allow = allowlist or AllowList.load()
        self.servers = servers if servers is not None else [StdioServer(n, s) for n, s in self.allow.servers.items()]
        self.route, self.schemas, self.defs = {}, {}, {}
        for srv in self.servers:
            for t in srv.tools:
                self.route.setdefault(t["name"], srv)
                self.schemas.setdefault(t["name"], t.get("inputSchema", {}))
                self.defs.setdefault(t["name"], t)

    def openai_tools(self):
        """Only the allowed tools are offered to the model."""
        return [{"type": "function", "function": {"name": n, "description": self.defs[n].get("description", ""),
                                                  "parameters": self.schemas[n]}}
                for n in self.allow.tools if n in self.defs]

    def check(self, name, args):
        return self.allow.check(name, args, self.schemas)

    def call(self, name, args):
        return self.route[name].call(name, args)

    def close(self):
        for s in self.servers:
            s.close()


class ScriptedModel:
    """A stub model for dry runs and tests: replays a fixed list of replies, one per turn.
    Each reply is {"tool_calls": [{"name": ..., "arguments": {...}}]} or {"content": "..."}, or a function of
    the messages so far that returns one."""
    identity = {"backend": "stub", "model": "scripted"}

    def __init__(self, replies):
        self.replies, self.n, self.requests = list(replies), 0, []

    def chat(self, messages, tools):
        self.requests.append({"messages": [dict(m) for m in messages], "tools": tools})
        if self.n >= len(self.replies):
            return {"content": "[stub: no more scripted replies]", "tool_calls": []}
        r = self.replies[self.n]
        self.n += 1
        if callable(r):  # a reply that depends on the tool outputs so far (the harness's stub)
            r = r(messages)
        calls = [{"id": f"call_{self.n}_{i}", "name": c["name"],
                  "arguments": c["arguments"] if isinstance(c.get("arguments"), str) else json.dumps(c.get("arguments", {}))}
                 for i, c in enumerate(r.get("tool_calls", []))]
        return {"content": r.get("content", ""), "tool_calls": calls}


class OpenAIChatModel:
    """An OpenAI-compatible /v1/chat/completions endpoint (the oMLX server later), non-streaming, temperature 0."""

    def __init__(self, base_url, model, api_key=None, max_tokens=2048, temperature=0.0, timeout=600, extra_body=None):
        import httpx
        self.url = base_url.rstrip("/") + ("/chat/completions" if base_url.rstrip("/").endswith("/v1") else "/v1/chat/completions")
        self.model, self.max_tokens, self.temperature, self.extra = model, max_tokens, temperature, extra_body or {}
        headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        self.http = httpx.Client(timeout=timeout, headers=headers)
        self.identity = {"backend": "openai", "model": model}

    def chat(self, messages, tools):
        body = {"model": self.model, "messages": messages, "temperature": self.temperature,
                "max_tokens": self.max_tokens, "stream": False, **self.extra}
        if tools:
            body["tools"], body["tool_choice"] = tools, "auto"
        r = self.http.post(self.url, json=body)
        r.raise_for_status()
        msg = r.json()["choices"][0]["message"]
        calls = [{"id": c.get("id") or f"call_{uuid.uuid4().hex[:8]}", "name": c["function"]["name"],
                  "arguments": c["function"].get("arguments", "{}")} for c in msg.get("tool_calls") or []]
        return {"content": msg.get("content") or "", "tool_calls": calls}


def _show(ev):
    if ev["type"] == "call":
        res = ev["status"] if ev["status"] != "ok" else ("error" if ev.get("is_error") else "ok")
        extra = f" ({ev['reason']})" if ev.get("reason") else ""
        print(f"[tool {ev['turn']}.{ev['index']}] {ev['name']} {json.dumps(ev['arguments'])} -> {res}{extra}"
              f" [{ev.get('seconds', 0):.1f} s, {ev.get('output_chars', 0)} chars, {ev['channel']}]", flush=True)


def run(question, model, session, max_turns=12, max_seconds=600, trace_path=None, text_protocol=False,
        on_event=_show, scenario=None):
    """Run the loop; returns a summary dict. The trace has one JSON line per event (start, request, call, final)."""
    t0 = time.monotonic()
    trace = open(trace_path, "a") if trace_path else None
    tools = session.openai_tools()

    def emit(ev):
        ev = dict(ev, t=round(time.monotonic() - t0, 3))
        if scenario:
            ev["scenario"] = scenario
        if trace:
            trace.write(json.dumps(ev) + "\n")
            trace.flush()
        if on_event:
            on_event(ev)

    messages = prompts.tools_messages(question, sorted(session.allow.namespaces), max_turns, text_protocol,
                                      [t["function"] for t in tools])
    emit({"type": "start", "prompt_version": prompts.TOOLS_PROMPT_VERSION, "model": model.identity,
          "tools": [t["function"]["name"] for t in tools], "max_turns": max_turns, "max_seconds": max_seconds,
          "text_protocol": text_protocol, "question": question})
    counts = {"ok": 0, "violation": 0, "invalid": 0, "skipped": 0, "error": 0}
    answer, stop, turns = None, "turn_cap", 0
    try:
        for turn in range(1, max_turns + 1):
            if time.monotonic() - t0 > max_seconds:
                stop = "time_cap"
                break
            turns = turn
            ts = time.monotonic()
            reply = model.chat(messages, None if text_protocol else tools)
            native = reply.get("tool_calls") or []
            if native:
                calls = [(c["id"], c["name"], c["arguments"], _parse_args(c["arguments"]), "native") for c in native]
            else:
                calls = [(f"text_{turn}_{i}", n, raw, args, "text")
                         for i, (n, raw, args) in enumerate(parse_text_calls(reply.get("content")))]
            emit({"type": "request", "turn": turn, "seconds": round(time.monotonic() - ts, 3),
                  "content": reply.get("content", ""), "n_calls": len(calls)})
            if not calls:
                answer, stop = reply.get("content", ""), "answer"
                break
            if native:
                messages.append({"role": "assistant", "content": reply.get("content") or None,
                                 "tool_calls": [{"id": c[0], "type": "function",
                                                 "function": {"name": c[1], "arguments": c[2] if isinstance(c[2], str) else json.dumps(c[2])}}
                                                for c in calls]})
            else:
                messages.append({"role": "assistant", "content": reply.get("content", "")})
            text_results = []
            for i, (cid, name, raw, args, channel) in enumerate(calls):
                ev = {"type": "call", "turn": turn, "index": i, "id": cid, "name": name, "channel": channel,
                      "arguments": args if args is not None else raw}
                cs = time.monotonic()
                if i >= MAX_CALLS_PER_TURN:
                    ev.update(status="skipped", reason=f"more than {MAX_CALLS_PER_TURN} calls in one reply")
                    out = prompts.tools_fill(prompts.TOOLS_SKIPPED, N=MAX_CALLS_PER_TURN)
                else:
                    d = session.check(name, args)
                    ev.update(status=d.status, reason=d.reason)
                    if d.status == "violation":
                        out = prompts.tools_fill(prompts.TOOLS_BLOCKED, REASON=d.reason)
                    elif d.status == "invalid":
                        out = prompts.tools_fill(prompts.TOOLS_INVALID, REASON=d.reason)
                    else:
                        try:
                            is_err, out = session.call(name, args)
                        except MCPError as e:
                            is_err, out = True, f"tool failed: {e}"
                        ev.update(is_error=is_err, output=out)
                        if is_err:
                            counts["error"] += 1
                counts[ev["status"]] += 1
                ev.update(seconds=round(time.monotonic() - cs, 3), output_chars=len(out))
                emit(ev)
                if channel == "native":
                    messages.append({"role": "tool", "tool_call_id": cid, "content": out})
                else:
                    text_results.append(prompts.tools_fill(prompts.TOOLS_RESULT_TEXT, NAME=name, TEXT=out))
            if text_results:
                messages.append({"role": "user", "content": "\n".join(text_results)})
        else:
            stop = "turn_cap"
        if answer is None and stop == "turn_cap" and time.monotonic() - t0 <= max_seconds:
            messages.append({"role": "user", "content": prompts.TOOLS_FINAL})
            reply = model.chat(messages, None)
            answer = reply.get("content", "")
            emit({"type": "request", "turn": "final", "content": answer, "n_calls": len(reply.get("tool_calls") or [])})
    finally:
        summary = {"type": "final", "stop": stop, "turns": turns, "seconds": round(time.monotonic() - t0, 3),
                   "calls": counts, "answer": answer}
        emit(summary)
        if trace:
            trace.close()
    return summary
