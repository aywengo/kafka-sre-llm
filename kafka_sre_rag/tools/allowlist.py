"""The session allow-list, enforced by the runtime before a call reaches any MCP server.

A call gets one of three statuses:
  ok         on the allow-list and valid against the tool's schema; it is run.
  violation  outside the allow-list (W31: an attempt counts, it is not run): an unknown tool, a tool not on the
             list, an argument the tool does not take, a forbidden value (the Secret kind), a namespace outside
             the session's namespaces, all_namespaces when the session does not allow it, or a string argument
             that carries a shell or option fragment (whitespace, ; & | ` $ < > or a leading -).
  invalid    on the list, inside the session's scope, but malformed (wrong type, a missing required argument,
             a value that fails the pattern, enum or range of the schema, arguments that are not a JSON
             object); it is not run, and the model is told why.
"""
import json
import re
from pathlib import Path

from .schema import validate

DEFAULT_PATH = Path(__file__).with_name("allowlist_readonly.json")
_INJECTION = re.compile(r"[\s;&|`$<>\\]|^-")


class Decision:
    def __init__(self, status, reason=""):
        self.status, self.reason = status, reason

    @property
    def allowed(self):
        return self.status == "ok"

    def __repr__(self):
        return f"Decision({self.status!r}, {self.reason!r})"


class AllowList:
    def __init__(self, data):
        self.data = data
        self.tools = list(data.get("tools", []))
        self.namespaces = set(data.get("namespaces", []))
        self.all_namespaces = bool(data.get("all_namespaces", False))
        self.forbidden = {k: {str(v).lower() for v in vals} for k, vals in data.get("forbidden_values", {}).items()}
        self.servers = data.get("servers", {})

    @classmethod
    def load(cls, path=None):
        return cls(json.loads(Path(path or DEFAULT_PATH).read_text()))

    def check(self, name, args, schemas):
        """name: the tool the model asked for; args: parsed arguments (or None when they were not JSON);
        schemas: {tool name: inputSchema} of the tools the servers list."""
        if name not in schemas:
            return Decision("violation", f"unknown tool {name!r}")
        if name not in self.tools:
            return Decision("violation", f"tool {name!r} is not on the session allow-list")
        if not isinstance(args, dict):
            return Decision("invalid", "arguments are not a JSON object")
        props = schemas[name].get("properties", {})
        extra = [k for k in args if k not in props]
        if extra:
            return Decision("violation", f"argument(s) {', '.join(map(repr, extra))} not taken by {name}")
        for key, bad in self.forbidden.items():
            if key in args and str(args[key]).lower() in bad:
                return Decision("violation", f"{key}={args[key]!r} is forbidden in this session")
        for key, v in args.items():
            if isinstance(v, str) and _INJECTION.search(v):
                return Decision("violation", f"argument {key}={v!r} carries a shell or option fragment")
        ns = args.get("namespace")
        if isinstance(ns, str) and self.namespaces and ns not in self.namespaces:
            return Decision("violation", f"namespace {ns!r} is outside the session's namespaces "
                                         f"({', '.join(sorted(self.namespaces))})")
        if args.get("all_namespaces") and not self.all_namespaces:
            return Decision("violation", "all_namespaces is not allowed in this session")
        errs = validate(args, schemas[name])
        if errs:
            return Decision("invalid", "; ".join(f"{p}: {m}" for _, p, m in errs))
        return Decision("ok")
