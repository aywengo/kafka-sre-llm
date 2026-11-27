"""The Kubernetes context check of the MCP harness (AGENTS.md, "Kubernetes contexts").

The expected context name is one constant here; mcp_harness/lib.sh holds the same name for the shell scripts (a test
keeps the two equal). It is overridden only by an explicit option or variable (--expect-context, or
MCP_HARNESS_EXPECT_CONTEXT in the shell scripts). The checks read the kubeconfig file only, through
`kubectl config --kubeconfig FILE ...`; they never contact a cluster and never print a server address or credential.
"""
import os
import subprocess

EXPECTED_CONTEXT = "kind-kafka-sre-mcp"


class ContextError(Exception):
    pass


def _config(kubeconfig, *args):
    p = subprocess.run(["kubectl", "config", "--kubeconfig", str(kubeconfig), *args], capture_output=True, text=True,
                       timeout=30, env={"PATH": os.environ.get("PATH", ""), "HOME": os.environ.get("HOME", ""),
                                        "KUBECONFIG": str(kubeconfig)})
    return p.returncode, p.stdout.strip()


def contexts_of(kubeconfig):
    code, out = _config(kubeconfig, "get-contexts", "-o", "name")
    return out.split() if code == 0 else []


def check_context_exists(kubeconfig, context):
    """Raise ContextError unless the file exists and holds a context of that name."""
    if not os.path.isfile(kubeconfig):
        raise ContextError(f"kubeconfig {kubeconfig}: no such file (expected context {context})")
    found = contexts_of(kubeconfig)
    if context not in found:
        raise ContextError(f"kubeconfig {kubeconfig}: no context {context}; found: {', '.join(found) or '(none)'}")


def check_harness_context(kubeconfig, expected=None):
    """The file exists, has the expected context, and its current-context is that name; else ContextError."""
    expected = expected or EXPECTED_CONTEXT
    check_context_exists(str(kubeconfig), expected)
    code, current = _config(kubeconfig, "current-context")
    if code != 0 or current != expected:
        raise ContextError(f"kubeconfig {kubeconfig}: current-context is {current or '(none)'}, expected {expected}")
