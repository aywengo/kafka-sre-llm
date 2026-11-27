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
# Automated sync without self-heal leaves drift in the cluster alone only when a sync to the current revision is on
# record (status.operationState). `cluster.sh reset` replaces the Application, which drops its status and that
# record, and then the first drift is synced back at once (seen 2026-11-26: OutOfSync for under a second).
sync_on_record() {
  [ "$(app_field '{.status.operationState.phase}')" = Succeeded ] &&
    [ -n "$(app_field '{.status.sync.revision}')" ] &&
    [ "$(app_field '{.status.operationState.syncResult.revision}')" = "$(app_field '{.status.sync.revision}')" ]
}
dlq_pending() {  # the commit with payments-dlq is in Argo CD's target state, and the topic was never created
  [ "$(app_field '{.status.sync.status}')" = OutOfSync ] &&
    app_field '{.status.resources[?(@.name=="payments-dlq")].status}' | grep -q OutOfSync &&
    ! K -n kafka get kafkatopic payments-dlq >/dev/null 2>&1
}
case "$action:$variant" in
  inject:live_edit)
    if ! sync_on_record; then
      K -n argocd patch application kafka-topics --type merge \
        -p '{"operation":{"initiatedBy":{"username":"harness-inject"},"sync":{"revision":"main"}}}' >/dev/null
      wait_until 120 "a sync to the current revision on record" sync_on_record >/dev/null || exit 1
    fi
    K -n kafka patch kafkatopic payments --type merge -p '{"spec":{"config":{"retention.ms":"3600000"}}}' >/dev/null; argo_refresh ;;
  verify:live_edit)  # OutOfSync, the edit still live, and nothing that would sync it back
    [ "$(app_field '{.status.sync.status}')" = OutOfSync ] && sync_on_record &&
      [ "$(K -n kafka get kafkatopic payments -o jsonpath='{.spec.config.retention\.ms}')" = 3600000 ] ;;
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
    # Argo CD retries the failed automated sync (limit 5, with backoff); while an attempt runs the message is briefly
    # "Retrying operation. Attempt #N" or "waiting for healthy state of ..." instead of the failure (seen 2026-11-26),
    # so such a message counts once a retry is on record (retryCount >= 1: a sync has failed), not during the first
    # attempt. payments-dlq pending in the target state keeps out a message from before the commit.
    msg=$({ app_field '{.status.operationState.message}'; app_field '{.status.conditions[*].message}'; })
    retries=$(app_field '{.status.operationState.retryCount}')
    dlq_pending && { grep -qi "invalid\|greater than or equal" <<<"$msg" ||
      { [ "${retries:-0}" -ge 1 ] && grep -qi "Retrying operation\|waiting for healthy state" <<<"$msg"; }; } ;;
  revert:*)
    K replace -f "$ROOT/mcp_harness/manifests/argocd.yaml" >/dev/null
    gitserver_exec "git reset -q --hard base"
    argo_refresh
    K -n kafka delete kafkatopic inventory-archive payments-dlq --ignore-not-found >/dev/null
    K -n argocd patch application kafka-topics --type merge \
      -p '{"operation":{"initiatedBy":{"username":"harness-revert"},"sync":{"revision":"main","prune":true}}}' >/dev/null ;;
  *) echo "usage: $0 inject|verify|revert live_edit|wrong_path|wrong_revision|topic_rf_too_high|invalid_manifest" >&2; exit 2 ;;
esac
