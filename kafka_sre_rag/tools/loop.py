"""The tool-use loop: a model, a session of local MCP servers, the allow-list, a turn and time cap, a trace.

One turn is one request to the model. The model gets the allowed tools as native `tools` (OpenAI chat format).
A reply's calls are taken from `tool_calls` (channel "native"); when a reply has none, <tool_call>{...}</tool_call>
blocks in its text are taken instead (channel "text", the documented fallback; with text_protocol=True the system
prompt also describes it). A block that is not JSON, has no tool name or is never closed is a malformed call
("invalid"), not an answer. Every call goes through the allow-list first, however many a reply has: only an "ok"
call reaches a server, and only the first MAX_CALLS_PER_TURN of them run (the rest are "skipped"); a "violation"
or an "invalid" call is answered with a note to the model and nothing is run. Every call is shown (on_event) and
written to the JSONL trace.

Caps: each request gets the time left of the session as its timeout (the final one included); a request that
times out at the cap, or a reply that comes after it, ends the session with stop "time_cap". When the turn cap is
reached, one last request without tools asks for the answer; calls the model still makes in that reply (native or
text) are checked against the allow-list, never run, and written as "after_budget_call" events; a reply that is
only calls is no answer. A request that fails (an exception, an HTTP error) is recorded as a "model_error" event
(the exception type and the HTTP status only, never the message, which can carry the server URL) and retried
once; a second failure ends the session with stop "failed". The trace then ends with a "final" event. Any other
exception writes an "aborted" event (its type only) and is raised again, so the trace has no "final" event.
"""
import json
import re
import time
import uuid

from .. import prompts
from .allowlist import AllowList
from .mcp_client import MCPError, StdioServer

MAX_CALLS_PER_TURN = 8
RETRY_WAIT_S = 5.0
CAP_SLACK_S = 0.25  # a request that fails this close to the time cap failed because of the cap
_TEXT_CALL = re.compile(r"<tool_call>\s*(.*?)\s*</tool_call>", re.S)
_OPEN = "<tool_call>"


def parse_text_calls(content):
    """<tool_call>{"name": ..., "arguments": {...}}</tool_call> blocks -> [(name, raw_arguments, parsed_or_None)].
    A block that is not a JSON object gives ("", block, None); an opening tag without a closing one gives
    ("", rest of the text, None). The allow-list rejects both as invalid."""
    calls = []
    text = content or ""
    for block in _TEXT_CALL.findall(text):
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
        calls.append((str(obj.get("name") or ""), json.dumps(obj.get("arguments")), args))
    rest = _TEXT_CALL.sub("", text)
    if _OPEN in rest:
        calls.append(("", rest[rest.index(_OPEN):], None))
    return calls


def strip_text_calls(content):
    """The text of a reply without its <tool_call> blocks (an unclosed block runs to the end)."""
    rest = _TEXT_CALL.sub("", content or "")
    if _OPEN in rest:
        rest = rest[:rest.index(_OPEN)]
    return rest.strip()


def _parse_args(raw):
    if isinstance(raw, dict):
        return raw
    try:
        v = json.loads(raw or "{}")
    except (TypeError, ValueError):
        return None
    return v if isinstance(v, dict) else None


def error_info(exc):
    """What the trace keeps of a failed request: the exception type and the HTTP status, nothing else."""
    status = getattr(getattr(exc, "response", None), "status_code", None)
    return {"error": type(exc).__name__, "http_status": status if isinstance(status, int) else None}


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
    Each reply is {"tool_calls": [{"name": ..., "arguments": {...}}]} or {"content": "..."} (optionally with
    "finish_reason"), a function of the messages so far that returns one, or an exception instance, which is
    raised (a failed request)."""
    identity = {"backend": "stub", "model": "scripted"}

    def __init__(self, replies):
        self.replies, self.n, self.requests = list(replies), 0, []

    def chat(self, messages, tools, timeout=None):
        self.requests.append({"messages": [dict(m) for m in messages], "tools": tools, "timeout": timeout})
        if self.n >= len(self.replies):
            return {"content": "[stub: no more scripted replies]", "tool_calls": [], "finish_reason": "stop"}
        r = self.replies[self.n]
        self.n += 1
        if isinstance(r, BaseException):
            raise r
        if callable(r):  # a reply that depends on the tool outputs so far (the harness's stub)
            r = r(messages)
        calls = [{"id": f"call_{self.n}_{i}", "name": c["name"],
                  "arguments": c["arguments"] if isinstance(c.get("arguments"), str) else json.dumps(c.get("arguments", {}))}
                 for i, c in enumerate(r.get("tool_calls", []))]
        return {"content": r.get("content", ""), "tool_calls": calls,
                "finish_reason": r.get("finish_reason", "tool_calls" if calls else "stop")}


class OpenAIChatModel:
    """An OpenAI-compatible /v1/chat/completions endpoint (the oMLX server later), non-streaming, temperature 0."""

    def __init__(self, base_url, model, api_key=None, max_tokens=2048, temperature=0.0, timeout=600, extra_body=None,
                 transport=None):
        import httpx
        self.url = base_url.rstrip("/") + ("/chat/completions" if base_url.rstrip("/").endswith("/v1") else "/v1/chat/completions")
        self.model, self.max_tokens, self.temperature, self.extra = model, max_tokens, temperature, extra_body or {}
        headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        self.http = httpx.Client(timeout=timeout, headers=headers, transport=transport)
        self.identity = {"backend": "openai", "model": model}

    def chat(self, messages, tools, timeout=None):
        body = {"model": self.model, "messages": messages, "temperature": self.temperature,
                "max_tokens": self.max_tokens, "stream": False, **self.extra}
        if tools:
            body["tools"], body["tool_choice"] = tools, "auto"
        r = self.http.post(self.url, json=body, **({"timeout": timeout} if timeout else {}))
        r.raise_for_status()
        choice = r.json()["choices"][0]
        msg = choice["message"]
        calls = [{"id": c.get("id") or f"call_{uuid.uuid4().hex[:8]}", "name": c["function"]["name"],
                  "arguments": c["function"].get("arguments", "{}")} for c in msg.get("tool_calls") or []]
        return {"content": msg.get("content") or "", "tool_calls": calls, "finish_reason": choice.get("finish_reason")}


def _show(ev):
    if ev["type"] in ("call", "after_budget_call"):
        res = ev["status"] if ev["status"] != "ok" else ("error" if ev.get("is_error") else "ok")
        if ev["type"] == "after_budget_call":
            res = f"after the budget, not run: {res}"
        extra = f" ({ev['reason']})" if ev.get("reason") else ""
        print(f"[tool {ev['turn']}.{ev['index']}] {ev['name']} {json.dumps(ev['arguments'])} -> {res}{extra}"
              f" [{ev.get('seconds', 0):.1f} s, {ev.get('output_chars', 0)} chars, {ev['channel']}]", flush=True)
    elif ev["type"] == "model_error":
        print(f"[model {ev['turn']}] request failed (attempt {ev['attempt']}): {ev['error']}"
              f"{' HTTP ' + str(ev['http_status']) if ev.get('http_status') else ''}"
              f"{' at the time cap' if ev.get('at_time_cap') else ''}", flush=True)


class _Stop(Exception):
    def __init__(self, stop, error=None):
        super().__init__(stop)
        self.stop, self.error = stop, error


def run(question, model, session, max_turns=12, max_seconds=600, trace_path=None, text_protocol=False,
        on_event=_show, scenario=None, retry_wait=RETRY_WAIT_S):
    """Run the loop; returns a summary dict. The trace has one JSON line per event (start, request, call,
    model_error, after_budget_call, final; or aborted)."""
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

    def left():
        return max_seconds - (time.monotonic() - t0)

    def ask(turn, offered):
        """One request with the time left as its timeout; one retry after a failure."""
        for attempt in (1, 2):
            remaining = left()
            if remaining <= 0:
                raise _Stop("time_cap")
            ts = time.monotonic()
            try:
                reply = model.chat(messages, offered, timeout=remaining)
            except Exception as e:  # noqa: BLE001 - every failure of a request is recorded the same way
                at_cap = left() <= CAP_SLACK_S
                emit({"type": "model_error", "turn": turn, "attempt": attempt, **error_info(e), "at_time_cap": at_cap})
                if at_cap:
                    raise _Stop("time_cap")
                if attempt == 2:
                    raise _Stop("failed", error_info(e))
                time.sleep(max(0.0, min(retry_wait, left())))
                continue
            return reply, time.monotonic() - ts, remaining
        raise AssertionError("unreachable")

    messages = prompts.tools_messages(question, sorted(session.allow.namespaces), max_turns, text_protocol,
                                      [t["function"] for t in tools])
    emit({"type": "start", "prompt_version": prompts.TOOLS_PROMPT_VERSION, "model": model.identity,
          "tools": [t["function"]["name"] for t in tools], "max_turns": max_turns, "max_seconds": max_seconds,
          "text_protocol": text_protocol, "question": question})
    counts = {"ok": 0, "violation": 0, "invalid": 0, "skipped": 0, "error": 0}
    after = {"calls": 0, "violation": 0, "invalid": 0}
    answer, stop, turns, error = None, None, 0, None
    try:
        try:
            for turn in range(1, max_turns + 1):
                reply, secs, timeout = ask(turn, None if text_protocol else tools)
                turns = turn
                content = reply.get("content") or ""
                native = reply.get("tool_calls") or []
                if native:
                    calls = [(c["id"], c["name"], c["arguments"], _parse_args(c["arguments"]), "native") for c in native]
                else:
                    calls = [(f"text_{turn}_{i}", n, raw, args, "text")
                             for i, (n, raw, args) in enumerate(parse_text_calls(content))]
                late = left() < 0
                emit({"type": "request", "turn": turn, "seconds": round(secs, 3), "timeout": round(timeout, 1),
                      "finish_reason": reply.get("finish_reason"), "content": content, "n_calls": len(calls),
                      **({"late": True} if late else {})})
                if late:  # the reply came after the time cap: nothing of it is used
                    raise _Stop("time_cap")
                if not calls:
                    answer, stop = content, "answer"
                    break
                if native:
                    messages.append({"role": "assistant", "content": content or None,
                                     "tool_calls": [{"id": c[0], "type": "function",
                                                     "function": {"name": c[1], "arguments": c[2] if isinstance(c[2], str) else json.dumps(c[2])}}
                                                    for c in calls]})
                else:
                    messages.append({"role": "assistant", "content": content})
                text_results = []
                for i, (cid, name, raw, args, channel) in enumerate(calls):
                    ev = {"type": "call", "turn": turn, "index": i, "id": cid, "name": name, "channel": channel,
                          "arguments": args if args is not None else raw}
                    cs = time.monotonic()
                    d = session.check(name, args)  # every call, also those past MAX_CALLS_PER_TURN
                    ev.update(status=d.status, reason=d.reason)
                    if d.status == "violation":
                        out = prompts.tools_fill(prompts.TOOLS_BLOCKED, REASON=d.reason)
                    elif d.status == "invalid":
                        out = prompts.tools_fill(prompts.TOOLS_INVALID, REASON=d.reason)
                    elif i >= MAX_CALLS_PER_TURN:
                        ev.update(status="skipped", reason=f"more than {MAX_CALLS_PER_TURN} calls in one reply")
                        out = prompts.tools_fill(prompts.TOOLS_SKIPPED, N=MAX_CALLS_PER_TURN)
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
                messages.append({"role": "user", "content": prompts.TOOLS_FINAL})
                reply, secs, timeout = ask("final", None)
                content = reply.get("content") or ""
                extra = [(c["name"], c["arguments"], _parse_args(c["arguments"]), "native") for c in reply.get("tool_calls") or []]
                extra += [(n, raw, args, "text") for n, raw, args in parse_text_calls(content)]
                late = left() < 0
                emit({"type": "request", "turn": "final", "seconds": round(secs, 3), "timeout": round(timeout, 1),
                      "finish_reason": reply.get("finish_reason"), "content": content, "n_calls": len(extra),
                      **({"late": True} if late else {})})
                for i, (name, raw, args, channel) in enumerate(extra):  # checked, never run
                    d = session.check(name, args)
                    after["calls"] += 1
                    if d.status in after:
                        after[d.status] += 1
                    emit({"type": "after_budget_call", "turn": "final", "index": i, "name": name, "channel": channel,
                          "arguments": args if args is not None else raw, "status": d.status, "reason": d.reason})
                if late:
                    raise _Stop("time_cap")
                answer = strip_text_calls(content) or None  # a reply that is only calls is no answer
        except _Stop as s:
            stop, error = s.stop, s.error
    except BaseException as e:
        emit({"type": "aborted", "error": type(e).__name__, "turns": turns,
              "seconds": round(time.monotonic() - t0, 3), "calls": counts})
        if trace:
            trace.close()
        raise
    summary = {"type": "final", "stop": stop, "turns": turns, "seconds": round(time.monotonic() - t0, 3),
               "calls": counts, "after_budget_calls": after, "answer": answer}
    if error:
        summary["error"] = error
    emit(summary)
    if trace:
        trace.close()
    return summary
