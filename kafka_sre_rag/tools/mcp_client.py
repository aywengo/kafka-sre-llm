"""A minimal MCP client for local servers: JSON-RPC 2.0 over stdio (newline-delimited), standard library only.

Only the stdio transport exists here: a server is a local command started as a child process. A remote
server (an HTTP or SSE URL) is refused (W31: remote MCP servers are out of scope).
"""
import json
import os
import queue
import subprocess
import sys
import threading

from .k8s_server import PROTOCOL_VERSION

CLIENT_INFO = {"name": "kafka-sre-rag", "version": "tools/1"}


class MCPError(Exception):
    pass


def check_local(spec):
    """A server spec is {"command": [...]} (stdio). Anything with a URL or another transport is refused."""
    if spec.get("transport", "stdio") != "stdio" or any(k in spec for k in ("url", "endpoint", "host")):
        raise MCPError("only local MCP servers (stdio) are allowed; remote servers are out of scope")
    cmd = spec.get("command")
    if not isinstance(cmd, list) or not cmd or not all(isinstance(c, str) for c in cmd):
        raise MCPError("a server needs a command (a list of strings)")
    return [sys.executable if c == "{python}" else c for c in cmd]


class StdioServer:
    def __init__(self, name, spec, timeout=60):
        self.name, self.timeout, self._id = name, timeout, 0
        cmd = check_local(spec)
        env = dict(os.environ, **{k: str(v) for k, v in (spec.get("env") or {}).items()})
        self.proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                     text=True, bufsize=1, env=env, cwd=spec.get("cwd"))
        self._q = queue.Queue()
        threading.Thread(target=self._read, daemon=True).start()
        threading.Thread(target=self._drain_stderr, daemon=True).start()
        init = self.request("initialize", {"protocolVersion": PROTOCOL_VERSION, "capabilities": {},
                                           "clientInfo": CLIENT_INFO})
        self.server_info = init.get("serverInfo", {})
        self.notify("notifications/initialized")
        self.tools = self.request("tools/list", {}).get("tools", [])

    def _read(self):
        for line in self.proc.stdout:
            line = line.strip()
            if line:
                try:
                    self._q.put(json.loads(line))
                except ValueError:
                    pass  # not a JSON-RPC message; ignored
        self._q.put(None)

    def _drain_stderr(self):
        for _ in self.proc.stderr:
            pass

    def _send(self, msg):
        try:
            self.proc.stdin.write(json.dumps(msg) + "\n")
            self.proc.stdin.flush()
        except (BrokenPipeError, OSError) as e:
            raise MCPError(f"server {self.name} is not running: {e}")

    def notify(self, method, params=None):
        self._send({"jsonrpc": "2.0", "method": method, **({"params": params} if params else {})})

    def request(self, method, params, timeout=None):
        self._id += 1
        mid = self._id
        self._send({"jsonrpc": "2.0", "id": mid, "method": method, "params": params})
        while True:
            try:
                msg = self._q.get(timeout=timeout or self.timeout)
            except queue.Empty:
                raise MCPError(f"server {self.name}: no reply to {method} within {timeout or self.timeout} s")
            if msg is None:
                raise MCPError(f"server {self.name} closed its output")
            if msg.get("id") != mid:
                continue  # a notification or a late reply
            if "error" in msg:
                raise MCPError(f"server {self.name}: {msg['error'].get('message')}")
            return msg.get("result", {})

    def call(self, tool, arguments):
        res = self.request("tools/call", {"name": tool, "arguments": arguments})
        text = "\n".join(c.get("text", "") for c in res.get("content", []) if c.get("type") == "text")
        return bool(res.get("isError")), text

    def close(self):
        try:
            self.proc.stdin.close()
            self.proc.wait(timeout=5)
        except Exception:
            self.proc.kill()
            self.proc.wait(timeout=5)
        for f in (self.proc.stdout, self.proc.stderr):
            try:
                f.close()
            except Exception:
                pass
