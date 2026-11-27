"""Local MCP server (stdio) with read-only tools for Kubernetes, Strimzi Kafka and Argo CD.

    python -m kafka_sre_rag.tools.k8s_server --kubeconfig FILE --context KUBE_CONTEXT [--kafka-bootstrap localhost:9092]

Every tool runs one fixed, read-only command (kubectl get/describe/logs/events, or a Kafka CLI tool with
--describe inside a broker pod), built as an argument list (no shell) from arguments checked against the
tool's schema. Nothing that changes state is exposed. Secrets are refused (the Secret kind is not accepted),
the kubectl last-applied-configuration annotation is dropped, and values that look like passwords, tokens or
private keys are redacted from every output. The runtime's session allow-list is checked before a call reaches
this server; this server checks the schema again.
The kubeconfig file and the context are required and passed to every kubectl call: the server never falls back
to the user's current context. kubectl runs with a minimal environment (PATH, HOME for its cache, KUBECONFIG set
to the given file), so no API key or other secret of the caller's environment reaches it. The server makes no
network call of its own beyond kubectl's.
"""
import argparse
import json
import os
import re
import subprocess
import sys

from . import kubectx
from .schema import validate

PROTOCOL_VERSION = "2025-06-18"
SERVER_INFO = {"name": "kafka-sre-k8s-readonly", "version": "1"}
MAX_OUTPUT = 12000
TIMEOUT_S = 30

NAME = r"[a-z0-9]([-a-z0-9.]{0,251}[a-z0-9])?"
NAMESPACE = r"[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?"
# no value may start with "-" (it would reach kubectl or a Kafka tool as an option)
SELECTOR = r"[A-Za-z0-9._/=,!][A-Za-z0-9._/=,!-]{0,199}"
KAFKA_NAME = r"[A-Za-z0-9._][A-Za-z0-9._-]{0,248}"
ENTITY_NAME = r"[A-Za-z0-9._=,][A-Za-z0-9._=,-]{0,248}"

# Plural form -> accepted aliases. Secrets are not here, on purpose.
KINDS = {
    "pods": ["pod", "po"], "deployments": ["deployment", "deploy"], "statefulsets": ["statefulset", "sts"],
    "replicasets": ["replicaset", "rs"], "services": ["service", "svc"], "endpoints": ["endpoint", "ep"],
    "configmaps": ["configmap", "cm"], "persistentvolumeclaims": ["persistentvolumeclaim", "pvc"],
    "persistentvolumes": ["persistentvolume", "pv"], "storageclasses": ["storageclass", "sc"],
    "nodes": ["node", "no"], "namespaces": ["namespace", "ns"], "jobs": ["job"], "events": ["event", "ev"],
    "serviceaccounts": ["serviceaccount", "sa"], "networkpolicies": ["networkpolicy", "netpol"],
    "poddisruptionbudgets": ["poddisruptionbudget", "pdb"], "leases": ["lease"],
    "kafkas": ["kafka"], "kafkanodepools": ["kafkanodepool", "knp"], "kafkatopics": ["kafkatopic", "kt"],
    "kafkausers": ["kafkauser", "ku"], "strimzipodsets": ["strimzipodset", "sps"],
    "kafkarebalances": ["kafkarebalance"], "kafkaconnects": ["kafkaconnect"], "kafkaconnectors": ["kafkaconnector"],
    "kafkamirrormaker2s": ["kafkamirrormaker2"], "kafkabridges": ["kafkabridge"],
    "applications": ["application", "app"], "appprojects": ["appproject"], "applicationsets": ["applicationset"],
}
KIND_ALIASES = {a: plural for plural, al in KINDS.items() for a in [plural] + al}
CLUSTER_SCOPED = {"nodes", "namespaces", "persistentvolumes", "storageclasses"}
FULL_NAMES = {  # kubectl resource names that cannot be confused with another API group
    "kafkas": "kafkas.kafka.strimzi.io", "kafkanodepools": "kafkanodepools.kafka.strimzi.io",
    "kafkatopics": "kafkatopics.kafka.strimzi.io", "kafkausers": "kafkausers.kafka.strimzi.io",
    "strimzipodsets": "strimzipodsets.core.strimzi.io", "kafkarebalances": "kafkarebalances.kafka.strimzi.io",
    "kafkaconnects": "kafkaconnects.kafka.strimzi.io", "kafkaconnectors": "kafkaconnectors.kafka.strimzi.io",
    "kafkamirrormaker2s": "kafkamirrormaker2s.kafka.strimzi.io", "kafkabridges": "kafkabridges.kafka.strimzi.io",
    "applications": "applications.argoproj.io", "appprojects": "appprojects.argoproj.io",
    "applicationsets": "applicationsets.argoproj.io",
}

S_KIND = {"type": "string", "enum": sorted(KIND_ALIASES),
          "description": "Resource kind, plural or singular (pods, deployments, kafkas, kafkanodepools, kafkatopics, "
                         "kafkausers, strimzipodsets, applications, persistentvolumeclaims, ...). Secrets are not available."}
S_NS = {"type": "string", "pattern": NAMESPACE, "description": "Namespace."}
S_NAME = {"type": "string", "pattern": NAME, "description": "Object name."}
S_SEL = {"type": "string", "pattern": SELECTOR, "description": "Label selector, e.g. strimzi.io/cluster=kafka."}
S_CLUSTER = {"type": "string", "pattern": NAME, "description": "Name of the Strimzi Kafka cluster (the Kafka resource). Default: kafka."}

TOOLS = [
    {"name": "k8s_get",
     "description": "kubectl get: list objects of one kind, or one object by name. Read-only.",
     "inputSchema": {"type": "object", "additionalProperties": False, "required": ["kind"], "properties": {
         "kind": S_KIND, "namespace": S_NS, "name": S_NAME, "selector": S_SEL,
         "all_namespaces": {"type": "boolean", "description": "List across all namespaces."},
         "output": {"type": "string", "enum": ["wide", "yaml", "json", "name"],
                    "description": "Output format (default wide; yaml shows spec and status)."}}}},
    {"name": "k8s_describe",
     "description": "kubectl describe: one object by name, or the objects that match a selector, with recent events. Read-only.",
     "inputSchema": {"type": "object", "additionalProperties": False, "required": ["kind"], "properties": {
         "kind": S_KIND, "namespace": S_NS, "name": S_NAME, "selector": S_SEL}}},
    {"name": "k8s_logs",
     "description": "kubectl logs: the last lines of one container's log (at most 500). Read-only.",
     "inputSchema": {"type": "object", "additionalProperties": False, "required": ["namespace", "pod"], "properties": {
         "namespace": S_NS, "pod": S_NAME,
         "container": {"type": "string", "pattern": NAME, "description": "Container name (needed when the pod has several)."},
         "tail": {"type": "integer", "minimum": 1, "maximum": 500, "description": "Number of lines (default 100)."},
         "previous": {"type": "boolean", "description": "Log of the previous, terminated container."},
         "since_seconds": {"type": "integer", "minimum": 1, "maximum": 86400}}}},
    {"name": "k8s_events",
     "description": "kubectl events: recent events of a namespace, optionally for one object (kind/name) or one type. Read-only.",
     "inputSchema": {"type": "object", "additionalProperties": False, "properties": {
         "namespace": S_NS, "all_namespaces": {"type": "boolean"},
         "for_object": {"type": "string", "pattern": r"[a-z0-9.]{1,63}/" + NAME, "description": "kind/name, e.g. pod/kafka-broker-0."},
         "types": {"type": "string", "enum": ["Normal", "Warning"]}}}},
    {"name": "kafka_topics_describe",
     "description": "kafka-topics.sh --describe, run inside a ready broker pod of a Strimzi cluster: partitions, leaders, "
                    "replicas, ISR and topic configuration; optionally only under-replicated, unavailable or at-min-ISR partitions. Read-only.",
     "inputSchema": {"type": "object", "additionalProperties": False, "required": ["namespace"], "properties": {
         "namespace": S_NS, "cluster": S_CLUSTER,
         "topic": {"type": "string", "pattern": KAFKA_NAME},
         "under_replicated": {"type": "boolean"}, "unavailable": {"type": "boolean"}, "at_min_isr": {"type": "boolean"}}}},
    {"name": "kafka_consumer_groups_describe",
     "description": "kafka-consumer-groups.sh --describe, run inside a ready broker pod: per partition the committed offset, "
                    "log end offset and lag (mode offsets), the members (mode members) or the group state (mode state). Read-only.",
     "inputSchema": {"type": "object", "additionalProperties": False, "required": ["namespace"], "properties": {
         "namespace": S_NS, "cluster": S_CLUSTER,
         "group": {"type": "string", "pattern": KAFKA_NAME, "description": "Group id; omit to describe all groups."},
         "mode": {"type": "string", "enum": ["offsets", "members", "state"]}}}},
    {"name": "kafka_configs_describe",
     "description": "kafka-configs.sh --describe, run inside a ready broker pod: the configuration (dynamic and, for brokers, "
                    "all) of a topic, broker, user or client, including quotas. Read-only.",
     "inputSchema": {"type": "object", "additionalProperties": False, "required": ["namespace", "entity_type"], "properties": {
         "namespace": S_NS, "cluster": S_CLUSTER,
         "entity_type": {"type": "string", "enum": ["topics", "brokers", "users", "clients"]},
         "entity_name": {"type": "string", "pattern": ENTITY_NAME,
                         "description": "Name; for a TLS user the principal name, e.g. CN=app-consumer. Omit for all."}}}},
    {"name": "argocd_app_get",
     "description": "Argo CD Application status (through the Kubernetes API): source, sync and health status, conditions, "
                    "the last operation and the managed resources with their status. Without a name, lists the applications. Read-only.",
     "inputSchema": {"type": "object", "additionalProperties": False, "properties": {
         "namespace": {"type": "string", "pattern": NAMESPACE, "description": "Namespace of the Application objects (default argocd)."},
         "name": S_NAME}}},
]
TOOL_BY_NAME = {t["name"]: t for t in TOOLS}
for _t in TOOLS:
    _t["annotations"] = {"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": False}

_PEM = re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----", re.S)
# Keys whose value is a credential. A bare "secret:" (Strimzi's status.secret, a volume's secretName) is the
# name of a Secret, not its value, and is kept.
# The key must start a word (automountServiceAccountToken: false is not a credential) and the value is on the
# same line.
_KV = re.compile(r"(?i)((?<![a-z])(?:password|passwd|token|api[_-]?key|private[_-]?key|client[_-]?secret|secret[_-]?key"
                 r"|secret[_-]?access[_-]?key)"
                 r"\"?[ \t]*[=:][ \t]*\"?)((?!<set to )[^\s\"',}]+)")  # describe's "<set to the key ...>" names no value
# An environment variable whose name says it holds a credential, in every form kubectl prints it: yaml
# (name / value lines), json ("name": ..., "value": ..., also with escaped quotes inside a string) and the
# NAME: value or NAME=value lines of describe and of logs (upper-case names, so automountServiceAccountToken
# and the like are kept).
_CRED = r"(?:PASS|SECRET|TOKEN|API_?KEY|ACCESS_?KEY|PRIVATE_?KEY|CREDENTIAL|JAAS)"
_ENV = re.compile(r"(?im)(name:[ \t]*\S*" + _CRED + r"\S*[ \t]*\n\s*value:[ \t]*)(\S.*)$")
_ENV_JSON = re.compile(r"(?i)(\\?\"name\\?\"\s*:\s*\\?\"[^\"\\]*" + _CRED + r"[^\"\\]*\\?\"\s*,\s*\\?\"value\\?\"\s*:\s*\\?\")"
                       r"((?:[^\"\\]|\\[^\"])*)")
_ENV_LINE = re.compile(r"(?m)(\b[A-Z0-9_]*(?:PASSWORD|PASSWD|PASSPHRASE|SECRET|TOKEN|API_?KEY|ACCESS_?KEY|PRIVATE_?KEY"
                       r"|CREDENTIALS?|JAAS_CONFIG)[A-Z0-9_]*\"?[ \t]*[:=][ \t]*\"?)((?!<set to )[^\s\"',}]+)")
_BEARER = re.compile(r"(?i)(\bbearer[ \t]+)([A-Za-z0-9._~+/=-]{8,})")
_AUTHZ = re.compile(r"(?i)(\bauthorization[\"']?[ \t]*[:=][ \t]*[\"']?(?:basic|token|negotiate|digest)[ \t]+)([^\s\"',}]+)")
_BLOB = re.compile(r"(?m)^(\s*(?:[a-z0-9_.-]+\.(?:key|p12|jks|password)|tls\.key|ca\.key)\s*:\s*)(\S{16,})$")
_LAST_APPLIED = "kubectl.kubernetes.io/last-applied-configuration"
_LAST_APPLIED_JSON = re.compile(r"(\"" + re.escape(_LAST_APPLIED) + r"\"\s*:\s*)\"(?:[^\"\\]|\\.)*\"")


def drop_last_applied(text):
    """Replace the value of the last-applied-configuration annotation (a copy of the whole object as applied,
    env values included) by "[removed]": the one-line json form, and the yaml and describe forms, where the
    value runs on over the following lines that are indented deeper than the annotation's key."""
    text = _LAST_APPLIED_JSON.sub(lambda m: m.group(1) + '"[removed]"', text)
    out, skip_col = [], None
    for line in text.split("\n"):
        if skip_col is not None:
            if line.strip() and len(line) - len(line.lstrip(" ")) > skip_col:
                continue
            skip_col = None
        col = line.find(_LAST_APPLIED + ":")
        if col >= 0:
            out.append(line[:col] + _LAST_APPLIED + ": [removed]")
            skip_col = col
            continue
        out.append(line)
    return "\n".join(out)


def redact(text):
    text = drop_last_applied(text)
    text = _PEM.sub("[REDACTED private key]", text)
    sub = lambda m: m.group(1) + "[REDACTED]"  # noqa: E731
    for pattern in (_ENV, _ENV_JSON, _ENV_LINE, _BLOB, _BEARER, _AUTHZ):
        text = pattern.sub(sub, text)
    return _KV.sub(sub, text)


class ToolError(Exception):
    pass


def minimal_env(kubeconfig=None):
    """The environment of a child process: PATH, HOME (kubectl keeps its discovery cache there) and, when given,
    KUBECONFIG. Nothing else of the caller's environment (no API key, no cloud credential) is passed on."""
    env = {"PATH": os.environ.get("PATH") or "/usr/bin:/bin"}
    if os.environ.get("HOME"):
        env["HOME"] = os.environ["HOME"]
    if kubeconfig:
        env["KUBECONFIG"] = kubeconfig
    return env


class Runner:
    def __init__(self, context, kubeconfig, kafka_bootstrap="localhost:9092", run=None):
        if not context or not kubeconfig:
            raise ValueError("a kubeconfig file and a context are both required (no fallback to the current context)")
        self.context, self.kubeconfig, self.bootstrap = context, kubeconfig, kafka_bootstrap
        self._run = run or self._subprocess

    def _subprocess(self, argv):
        p = subprocess.run(argv, capture_output=True, text=True, timeout=TIMEOUT_S, env=minimal_env(self.kubeconfig),
                           stdin=subprocess.DEVNULL)
        return p.returncode, p.stdout, p.stderr

    def kubectl(self, args):
        argv = ["kubectl", "--kubeconfig", self.kubeconfig, "--context", self.context, "--request-timeout=20s"] + args
        code, out, err = self._run(argv)
        text = out if code == 0 else (out + ("\n" if out else "") + err)
        return code, text

    def broker_pod(self, namespace, cluster):
        code, out = self.kubectl(["get", "pods", "-n", namespace, "-l",
                                  f"strimzi.io/cluster={cluster},strimzi.io/broker-role=true", "-o", "json"])
        if code != 0:
            raise ToolError(out.strip())
        try:
            items = json.loads(out).get("items", [])
        except Exception:  # noqa: BLE001 - a reply that is not the JSON list the server expects
            raise ToolError("could not read the broker pods (kubectl did not return a JSON list)") from None
        for p in items:
            st = p.get("status", {})
            if st.get("phase") == "Running" and any(c.get("ready") for c in st.get("containerStatuses", [])):
                return p["metadata"]["name"]
        raise ToolError(f"no ready broker pod of Kafka cluster {cluster!r} in namespace {namespace!r}")

    def broker_exec(self, args, cmd):
        ns, cluster = args["namespace"], args.get("cluster", "kafka")
        pod = self.broker_pod(ns, cluster)
        code, out = self.kubectl(["exec", "-n", ns, pod, "-c", "kafka", "--"] + cmd)
        return code, f"(run in pod {pod})\n{out}"


def _kind(args):
    plural = KIND_ALIASES[args["kind"]]
    return plural, FULL_NAMES.get(plural, plural)


def _scope(args, plural):
    if plural in CLUSTER_SCOPED:
        return []
    if args.get("all_namespaces"):
        return ["--all-namespaces"]
    if not args.get("namespace"):
        raise ToolError(f"namespace is required for {plural} (or all_namespaces); nothing was run")
    return ["-n", args["namespace"]]


def t_get(r, a):
    plural, res = _kind(a)
    cmd = ["get", res] + ([a["name"]] if a.get("name") else []) + _scope(a, plural)
    if a.get("selector"):
        cmd += ["-l", a["selector"]]
    cmd += ["-o", a.get("output", "wide")]
    return r.kubectl(cmd)


def t_describe(r, a):
    plural, res = _kind(a)
    cmd = ["describe", res] + ([a["name"]] if a.get("name") else []) + _scope(a, plural)
    if a.get("selector"):
        cmd += ["-l", a["selector"]]
    return r.kubectl(cmd)


def t_logs(r, a):
    cmd = ["logs", "-n", a["namespace"], a["pod"], f"--tail={a.get('tail', 100)}"]
    if a.get("container"):
        cmd += ["-c", a["container"]]
    if a.get("previous"):
        cmd.append("--previous")
    if a.get("since_seconds"):
        cmd.append(f"--since={a['since_seconds']}s")
    return r.kubectl(cmd)


def t_events(r, a):
    if not a.get("all_namespaces") and not a.get("namespace"):
        raise ToolError("namespace is required (or all_namespaces); nothing was run")
    cmd = ["events"] + (["--all-namespaces"] if a.get("all_namespaces") else ["-n", a["namespace"]])
    if a.get("for_object"):
        cmd += ["--for", a["for_object"]]
    if a.get("types"):
        cmd += ["--types", a["types"]]
    return r.kubectl(cmd)


def t_topics(r, a):
    cmd = ["/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", r.bootstrap, "--describe"]
    if a.get("topic"):
        cmd += ["--topic", a["topic"]]
    for flag, opt in (("under_replicated", "--under-replicated-partitions"), ("unavailable", "--unavailable-partitions"),
                      ("at_min_isr", "--at-min-isr-partitions")):
        if a.get(flag):
            cmd.append(opt)
    return r.broker_exec(a, cmd)


def t_groups(r, a):
    cmd = ["/opt/kafka/bin/kafka-consumer-groups.sh", "--bootstrap-server", r.bootstrap, "--describe"]
    cmd += ["--group", a["group"]] if a.get("group") else ["--all-groups"]
    mode = a.get("mode", "offsets")
    if mode == "members":
        cmd += ["--members", "--verbose"]
    elif mode == "state":
        cmd.append("--state")
    return r.broker_exec(a, cmd)


def t_configs(r, a):
    cmd = ["/opt/kafka/bin/kafka-configs.sh", "--bootstrap-server", r.bootstrap, "--describe",
           "--entity-type", a["entity_type"]]
    if a.get("entity_name"):
        cmd += ["--entity-name", a["entity_name"]]
    if a["entity_type"] == "brokers" and a.get("entity_name"):
        cmd.append("--all")
    return r.broker_exec(a, cmd)


def _app_summary(app):
    md, spec, st = app.get("metadata", {}), app.get("spec", {}), app.get("status", {})
    out = {"name": md.get("name"), "namespace": md.get("namespace"), "source": spec.get("source") or spec.get("sources"),
           "destination": spec.get("destination"), "syncPolicy": spec.get("syncPolicy"),
           "sync": st.get("sync"), "health": st.get("health"), "conditions": st.get("conditions"),
           "operationState": {k: v for k, v in (st.get("operationState") or {}).items()
                              if k in ("phase", "message", "startedAt", "finishedAt")} or None,
           "resources": [{k: x.get(k) for k in ("kind", "namespace", "name", "status", "health")}
                         for x in st.get("resources") or []]}
    return {k: v for k, v in out.items() if v not in (None, [], {})}


def t_argocd(r, a):
    ns = a.get("namespace", "argocd")
    if not a.get("name"):
        return r.kubectl(["get", "applications.argoproj.io", "-n", ns, "-o",
                          "custom-columns=NAME:.metadata.name,SYNC:.status.sync.status,HEALTH:.status.health.status,"
                          "REVISION:.status.sync.revision,PATH:.spec.source.path,REPO:.spec.source.repoURL"])
    code, out = r.kubectl(["get", "applications.argoproj.io", a["name"], "-n", ns, "-o", "json"])
    if code != 0:
        return code, out
    try:
        return 0, json.dumps(_app_summary(json.loads(out)), indent=1)
    except Exception:  # noqa: BLE001 - a reply that is not the JSON object the server expects
        return 1, "could not read the Application (kubectl did not return a JSON object)"


HANDLERS = {"k8s_get": t_get, "k8s_describe": t_describe, "k8s_logs": t_logs, "k8s_events": t_events,
            "kafka_topics_describe": t_topics, "kafka_consumer_groups_describe": t_groups,
            "kafka_configs_describe": t_configs, "argocd_app_get": t_argocd}


def call_tool(runner, name, args):
    """Run one tool. Returns (is_error, text). Refuses unknown tools, schema errors and Secrets."""
    if name not in TOOL_BY_NAME:
        return True, f"unknown tool {name!r}"
    if not isinstance(args, dict):
        return True, "arguments must be an object"
    if str(args.get("kind", "")).lower() in ("secret", "secrets"):
        return True, "refused: Secrets are not available through these tools"
    errs = validate(args, TOOL_BY_NAME[name]["inputSchema"])
    if errs:
        return True, "invalid arguments: " + "; ".join(f"{p}: {m}" for _, p, m in errs)
    try:
        code, text = HANDLERS[name](runner, args)
    except ToolError as e:
        return True, str(e)
    except subprocess.TimeoutExpired:
        return True, f"timed out after {TIMEOUT_S} s"
    except Exception as e:  # noqa: BLE001 - the server survives any failure of one call
        return True, f"tool failed: {type(e).__name__}"
    text = redact(text)
    if len(text) > MAX_OUTPUT:
        cut = len(text) - MAX_OUTPUT
        text = (text[-MAX_OUTPUT:] if name == "k8s_logs" else text[:MAX_OUTPUT]) + f"\n[output truncated: {cut} characters left out]"
    return code != 0, text or "(no output)"


def handle(runner, msg):
    """One JSON-RPC message -> a response dict, or None for a notification."""
    mid, method, params = msg.get("id"), msg.get("method"), msg.get("params") or {}
    if mid is None:
        return None
    if method == "initialize":
        result = {"protocolVersion": PROTOCOL_VERSION, "capabilities": {"tools": {"listChanged": False}},
                  "serverInfo": SERVER_INFO}
    elif method == "ping":
        result = {}
    elif method == "tools/list":
        result = {"tools": TOOLS}
    elif method == "tools/call":
        err, text = call_tool(runner, params.get("name"), params.get("arguments") or {})
        result = {"content": [{"type": "text", "text": text}], "isError": err}
    else:
        return {"jsonrpc": "2.0", "id": mid, "error": {"code": -32601, "message": f"method not found: {method}"}}
    return {"jsonrpc": "2.0", "id": mid, "result": result}


def serve(runner, stdin=sys.stdin, stdout=sys.stdout):
    for line in stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except Exception:  # noqa: BLE001
            resp = {"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "parse error"}}
        else:
            if not isinstance(msg, dict):
                resp = {"jsonrpc": "2.0", "id": None, "error": {"code": -32600, "message": "invalid request"}}
            else:
                try:
                    resp = handle(runner, msg)
                except Exception as e:  # noqa: BLE001 - the server survives a message it cannot handle
                    resp = {"jsonrpc": "2.0", "id": msg.get("id"),
                            "error": {"code": -32603, "message": f"internal error: {type(e).__name__}"}}
        if resp is not None:
            stdout.write(json.dumps(resp) + "\n")
            stdout.flush()


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--kubeconfig", required=True,
                    help="kubeconfig file (required; the server never uses the default kubeconfig or its current context)")
    ap.add_argument("--context", required=True, help="kubeconfig context (required)")
    ap.add_argument("--expect-context", help="the context name this server must be given and the kubeconfig's "
                    f"current-context must equal (default {kubectx.EXPECTED_CONTEXT})")
    ap.add_argument("--kafka-bootstrap", default="localhost:9092",
                    help="bootstrap address the Kafka tools use inside the broker pod (default localhost:9092)")
    ap.add_argument("--no-current-check", action="store_true",
                    help="do not require the kubeconfig's current-context to equal --context (diagnose on a user's "
                    "own kubeconfig; the context must still exist). The harness's own file keeps the check.")
    a = ap.parse_args(argv)
    if not os.path.isfile(a.kubeconfig):
        ap.error(f"--kubeconfig {a.kubeconfig}: no such file")
    expected = a.expect_context or kubectx.EXPECTED_CONTEXT
    try:
        if a.context != expected:
            raise kubectx.ContextError(f"--context is {a.context}, expected {expected}")
        if a.no_current_check:
            kubectx.check_context_exists(a.kubeconfig, a.context)
        else:
            kubectx.check_harness_context(a.kubeconfig, expected)
    except kubectx.ContextError as e:
        print(f"k8s_server: {e}", file=sys.stderr)
        sys.exit(2)
    serve(Runner(a.context, os.path.abspath(a.kubeconfig), a.kafka_bootstrap))


if __name__ == "__main__":
    main()
