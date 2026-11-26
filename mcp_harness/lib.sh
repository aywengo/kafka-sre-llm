# Shared settings and helpers of the MCP harness scripts (sourced, not run).
HARNESS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HARNESS/.." && pwd)"
CLUSTER=kafka-sre-mcp
CTX="kind-$CLUSTER"
NODE="$CLUSTER-control-plane"
RAW="${MCP_HARNESS_RAW:-$ROOT/raw/mcp_harness}"
GEN="${MCP_HARNESS_GEN:-$ROOT/work/mcp_harness}"
if [ -z "${PY:-}" ]; then
  if [ -x "$ROOT/.venv/bin/python" ]; then PY="$ROOT/.venv/bin/python"
  else PY="$(cd "$ROOT" && git rev-parse --path-format=absolute --git-common-dir)/../.venv/bin/python"; fi
fi
K() { kubectl --context "$CTX" "$@"; }
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
  local p; for p in $(K -n kafka get pods -l "$1" -o name); do
    { K -n kafka logs "$p" --tail=300 2>/dev/null; K -n kafka logs "$p" --previous --tail=300 2>/dev/null; } | grep -Eq "$2" && return 0
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
