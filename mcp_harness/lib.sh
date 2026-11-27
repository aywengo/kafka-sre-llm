# Shared settings and helpers of the MCP harness scripts (sourced, not run).
HARNESS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HARNESS/.." && pwd)"
CLUSTER=kafka-sre-mcp
# The expected context name (the Python side holds the same constant in kafka_sre_rag/tools/kubectx.py; a test keeps
# them equal). Only MCP_HARNESS_EXPECT_CONTEXT overrides it.
CTX="${MCP_HARNESS_EXPECT_CONTEXT:-kind-kafka-sre-mcp}"
NODE="$CLUSTER-control-plane"
RAW="${MCP_HARNESS_RAW:-$ROOT/raw/mcp_harness}"
GEN="${MCP_HARNESS_GEN:-$ROOT/work/mcp_harness}"
if [ -z "${PY:-}" ]; then
  if [ -x "$ROOT/.venv/bin/python" ]; then PY="$ROOT/.venv/bin/python"
  else PY="$(cd "$ROOT" && git rev-parse --path-format=absolute --git-common-dir)/../.venv/bin/python"; fi
fi
# The harness's own kubeconfig (written by `cluster.sh up` or `cluster.sh kubeconfig`, under the git-ignored work
# area). Every kubectl call names it and the context; KUBECONFIG is exported too, so even a stray kubectl never
# reads the user's default kubeconfig or its current context.
KCFG="${MCP_HARNESS_KUBECONFIG:-$GEN/kubeconfig}"
export KUBECONFIG="$KCFG"
# check_context: the Owner's standing rule (AGENTS.md, "Kubernetes contexts"). The harness kubeconfig must exist, hold
# context $CTX, and have it as current-context; else print the expected and found names (never the server address or
# credentials) and exit 2 before any other command. `kubectl config` reads the file only. Run when lib.sh is sourced,
# so before any kubectl call of any script; a script that writes the file first (cluster.sh up, kubeconfig) sets
# HARNESS_CTX_CHECK=defer before sourcing and calls check_context itself.
check_context() {
  local found cur
  if [ ! -f "$KCFG" ]; then
    echo "harness: kubeconfig $KCFG: no such file (expected context $CTX)" >&2; exit 2
  fi
  found=$(kubectl config --kubeconfig "$KCFG" get-contexts -o name 2>/dev/null | tr '\n' ' ')
  case " $found " in
    *" $CTX "*) ;;
    *) echo "harness: kubeconfig $KCFG: no context $CTX; found: ${found:-(none)}" >&2; exit 2 ;;
  esac
  cur=$(kubectl config --kubeconfig "$KCFG" current-context 2>/dev/null)
  if [ "$cur" != "$CTX" ]; then
    echo "harness: kubeconfig $KCFG: current-context is ${cur:-(none)}, expected $CTX" >&2; exit 2
  fi
}
[ "${HARNESS_CTX_CHECK:-}" = defer ] || check_context
K() { kubectl --kubeconfig "$KCFG" --context "$CTX" "$@"; }
log() { echo "[$(date +%H:%M:%S)] $*"; }

# Images loaded into the kind node (the node image itself is not loaded).
IMAGES=(
  quay.io/strimzi/operator:1.3.1
  quay.io/strimzi/kafka:1.3.1-kafka-4.3.2
  quay.io/argoproj/argocd:v3.6.1
  public.ecr.aws/docker/library/redis:8.10.1-alpine
)

# wait_until TIMEOUT_S DESCRIPTION CMD...: run CMD every 5 s until it succeeds; print one line per try.
wait_until() {
  local timeout=$1 what=$2; shift 2
  local start=$SECONDS
  while true; do
    if "$@" >/dev/null 2>&1; then log "ok: $what ($((SECONDS - start)) s)"; return 0; fi
    if (( SECONDS - start >= timeout )); then log "TIMEOUT: $what after ${timeout} s"; return 1; fi
    log "waiting: $what ($((SECONDS - start)) s)"
    sleep 5
  done
}

# A running broker pod of cluster kafka (the first Ready one).
broker_pod() {
  K -n kafka get pods -l strimzi.io/cluster=kafka,strimzi.io/broker-role=true \
    -o jsonpath='{range .items[?(@.status.containerStatuses[0].ready==true)]}{.metadata.name}{"\n"}{end}' | head -1
}
# broker_exec CMD...: run a Kafka CLI tool in a broker against its plain listener.
broker_exec() {
  local p; p=$(broker_pod)
  [ -n "$p" ] || { echo "no ready broker pod" >&2; return 1; }
  K -n kafka exec "$p" -c kafka -- "$@"
}
group_lag() {  # total lag of a consumer group (empty when unknown)
  broker_exec /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group "$1" 2>/dev/null \
    | awk 'NR>1 && $6 ~ /^[0-9]+$/ {s+=$6; n++} END {if (n) print s}'
}
urp_count() {
  broker_exec /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --under-replicated-partitions 2>/dev/null \
    | grep -c "Partition:"
}
gitserver_exec() {  # run a shell snippet in the GitOps repository of the git daemon pod
  local p; p=$(K -n argocd get pods -l app=gitserver -o jsonpath='{.items[0].metadata.name}')
  K -n argocd exec "$p" -c daemon -- sh -c "cd /repos/gitops && $1"
}
op_env() {  # op_env NAME VALUE: set an env var of the cluster operator to a plain value (rolls the operator)
  K -n kafka get deploy strimzi-cluster-operator -o json | "$PY" "$HARNESS/jsonedit.py" env "$1" "$2" | K replace -f - >/dev/null
}
op_wait_down() {  # wait (up to 90 s) until no cluster operator pod is ready, so a later spec change is not reconciled
  local i
  for i in $(seq 1 45); do
    [ -z "$(K -n kafka get pods -l name=strimzi-cluster-operator -o jsonpath='{range .items[?(@.status.containerStatuses[0].ready==true)]}{.metadata.name}{end}')" ] && return 0
    sleep 2
  done
  return 1
}
kafka_spec_bump() {  # change the Kafka spec (a dynamic cluster-wide setting), so generation moves past observedGeneration
  K -n kafka patch kafka kafka --type merge -p '{"spec":{"kafka":{"config":{"log.retention.ms":259200000}}}}' >/dev/null
}
kafka_generation_lags() {
  local g o; g=$(K -n kafka get kafka kafka -o jsonpath='{.metadata.generation}'); o=$(K -n kafka get kafka kafka -o jsonpath='{.status.observedGeneration}')
  [ -n "$g" ] && [ -n "$o" ] && [ "$o" -lt "$g" ]
}
pod_waiting_reason() {  # pod_waiting_reason SELECTOR: waiting reasons of the first container of matching pods
  K -n kafka get pods -l "$1" -o jsonpath='{range .items[*]}{.status.containerStatuses[0].state.waiting.reason}{" "}{.status.containerStatuses[0].lastState.terminated.reason}{"\n"}{end}'
}
pod_logs_match() {  # pod_logs_match SELECTOR REGEX: the recent log of a matching pod (current or previous) matches
  # FAULT_SINCE (RFC 3339, set by harness.py at inject time) keeps out lines an earlier scenario left in the log
  local p since=(); [ -n "${FAULT_SINCE:-}" ] && since=(--since-time="$FAULT_SINCE")
  for p in $(K -n kafka get pods -l "$1" -o name); do
    { K -n kafka logs "$p" --tail=300 "${since[@]}" 2>/dev/null; K -n kafka logs "$p" --previous --tail=300 "${since[@]}" 2>/dev/null; } | grep -Eq "$2" && return 0
  done; return 1
}
group_has_members() {
  broker_exec /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group "$1" --members 2>/dev/null \
    | awk -v g="$1" '$1 == g' | grep -q .
}
argo_refresh() { K -n argocd annotate application kafka-topics argocd.argoproj.io/refresh=hard --overwrite >/dev/null; }
app_field() { K -n argocd get application kafka-topics -o jsonpath="$1"; }
deploy_ready() {  # deploy_ready NS NAME: rolled out (current generation), all replicas updated and ready, at least one
  local r; r=$(K -n "$1" get deploy "$2" -o jsonpath='{.spec.replicas} {.status.readyReplicas} {.status.updatedReplicas} {.status.replicas} {.metadata.generation} {.status.observedGeneration}')
  set -- $r
  [ "$#" = 6 ] && [ "$1" != 0 ] && [ "$1" = "$2" ] && [ "$1" = "$3" ] && [ "$1" = "$4" ] && [ "$5" = "$6" ]
}
