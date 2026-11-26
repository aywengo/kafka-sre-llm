#!/bin/bash
# Fault kinds argocd_out_of_sync and argocd_degraded: the Application kafka-topics (automated sync, prune on,
# selfHeal off) that manages topics payments and inventory and user app-payments from the in-cluster git daemon.
#   argocd.sh inject|verify|revert live_edit|wrong_path|wrong_revision|topic_rf_too_high|invalid_manifest
set -u  # no pipefail: a predicate ending in grep -q must not fail on SIGPIPE upstream
source "$(dirname "$0")/../lib.sh"
action=$1 variant=$2
commit() {  # commit FILE CONTENT-FILE MESSAGE: add a file to the GitOps repository and commit (idempotent)
  local p; p=$(K -n argocd get pods -l app=gitserver -o jsonpath='{.items[0].metadata.name}')
  K -n argocd exec -i "$p" -c daemon -- sh -c "cat > /repos/gitops/kafka/$1" < "$2"
  gitserver_exec "git add -A && (git diff --cached --quiet || git commit -q -m '$3')"
  argo_refresh
}
cond_types() { app_field '{.status.conditions[*].type}'; }
case "$action:$variant" in
  inject:live_edit)
    K -n kafka patch kafkatopic payments --type merge -p '{"spec":{"config":{"retention.ms":"3600000"}}}' >/dev/null ;;
  verify:live_edit) [ "$(app_field '{.status.sync.status}')" = OutOfSync ] ;;
  inject:wrong_path)
    K -n argocd patch application kafka-topics --type merge -p '{"spec":{"source":{"path":"kafka-prod"}}}' >/dev/null; argo_refresh ;;
  inject:wrong_revision)
    K -n argocd patch application kafka-topics --type merge -p '{"spec":{"source":{"targetRevision":"release-1.0"}}}' >/dev/null; argo_refresh ;;
  verify:wrong_path|verify:wrong_revision) cond_types | grep -q ComparisonError ;;
  inject:topic_rf_too_high)
    cat > "$GEN/fault-topic.yaml" <<'EOF'
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: inventory-archive
  namespace: kafka
  labels:
    strimzi.io/cluster: kafka
spec:
  partitions: 3
  replicas: 5
EOF
    commit inventory-archive.yaml "$GEN/fault-topic.yaml" "add topic inventory-archive" ;;
  verify:topic_rf_too_high) [ "$(app_field '{.status.health.status}')" = Degraded ] ;;
  inject:invalid_manifest)
    cat > "$GEN/fault-topic.yaml" <<'EOF'
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: payments-dlq
  namespace: kafka
  labels:
    strimzi.io/cluster: kafka
spec:
  partitions: 0
  replicas: 3
EOF
    commit payments-dlq.yaml "$GEN/fault-topic.yaml" "add topic payments-dlq" ;;
  verify:invalid_manifest)
    { app_field '{.status.operationState.message}'; app_field '{.status.conditions[*].message}'; } | grep -qi "invalid\|greater than or equal" ;;
  revert:*)
    K replace -f "$ROOT/mcp_harness/manifests/argocd.yaml" >/dev/null
    gitserver_exec "git reset -q --hard base"
    argo_refresh
    K -n kafka delete kafkatopic inventory-archive payments-dlq --ignore-not-found >/dev/null
    K -n argocd patch application kafka-topics --type merge \
      -p '{"operation":{"initiatedBy":{"username":"harness-revert"},"sync":{"revision":"main","prune":true}}}' >/dev/null ;;
  *) echo "usage: $0 inject|verify|revert live_edit|wrong_path|wrong_revision|topic_rf_too_high|invalid_manifest" >&2; exit 2 ;;
esac
