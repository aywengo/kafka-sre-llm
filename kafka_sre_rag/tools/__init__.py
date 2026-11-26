"""Tool use for diagnosis (M7, GOAL.md step 6; decisions W30 and W31). EXPERIMENTAL and UNMEASURED.

- k8s_server: a local MCP server (stdio) with read-only Kubernetes, Kafka and Argo CD tools.
- mcp_client: a minimal MCP client (JSON-RPC 2.0 over stdio, standard library only); local servers only.
- allowlist: the session allow-list, enforced by the runtime before any call reaches a server.
- loop: the tool-use loop against an OpenAI-compatible chat endpoint (native `tools` calls, with a text
  fallback), a scripted stub model, and the JSONL trace of every call.
"""
