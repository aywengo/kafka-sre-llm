#!/bin/bash
# Fault kind storage: broker storage that cannot be provisioned or resized.
#   storage.sh inject|verify|revert new_pool_missing_storage_class|resize_not_supported
set -u  # no pipefail: a predicate ending in grep -q must not fail on SIGPIPE upstream
source "$(dirname "$0")/../lib.sh"
action=$1 variant=$2
paused() { [ "$(K -n kafka get kafka kafka -o jsonpath='{.status.conditions[?(@.type=="ReconciliationPaused")].status}')" = True ]; }
case "$action:$variant" in
  inject:new_pool_missing_storage_class)
    K apply -f - >/dev/null <<'EOF'
apiVersion: kafka.strimzi.io/v1
kind: KafkaNodePool
metadata:
  name: pool-b
  namespace: kafka
  labels:
    strimzi.io/cluster: kafka
    mcp-harness/fault: "true"
spec:
  replicas: 1
  roles: [broker]
  storage:
    type: persistent-claim
    size: 1Gi
    class: fast-ssd
    deleteClaim: true
  resources:
    requests: {cpu: 100m, memory: 640Mi}
    limits: {memory: 1Gi}
EOF
    ;;
  verify:new_pool_missing_storage_class)
    K -n kafka get pvc -l strimzi.io/pool-name=pool-b -o jsonpath='{.items[*].status.phase}' | grep -q Pending ;;
  revert:new_pool_missing_storage_class)
    K -n kafka delete kafkanodepool pool-b --ignore-not-found >/dev/null
    K -n kafka delete pod,pvc -l strimzi.io/pool-name=pool-b --ignore-not-found --wait=false >/dev/null ;;
  inject:resize_not_supported)
    K -n kafka patch kafkanodepool broker --type merge -p '{"spec":{"storage":{"size":"2Gi"}}}' >/dev/null ;;
  verify:resize_not_supported)  # the operator's warning in the Kafka status (reset leaves no Warning condition there)
    K -n kafka get kafka kafka -o jsonpath='{.status.conditions[?(@.reason=="PvcResizingWarning")].message}' | grep -q "does not support resizing" ;;
  revert:resize_not_supported)
    # Strimzi keeps the size it was asked for in the StrimziPodSet's storage annotation and refuses a decrease, so
    # the recorded size must go back to 1Gi too. A reconciliation still running with the 2Gi spec can write 2Gi back
    # after that (seen 2026-11-26), so pause reconciliation first and wait until the operator says it is paused,
    # then correct the pool and the record; replacing the Kafka resource drops the pause annotation again.
    K -n kafka annotate kafka kafka strimzi.io/pause-reconciliation=true --overwrite >/dev/null
    wait_until 300 "Kafka reconciliation paused" paused >/dev/null
    K -n kafka patch kafkanodepool broker --type merge -p '{"spec":{"storage":{"size":"1Gi"}}}' >/dev/null
    K -n kafka annotate strimzipodset kafka-broker --overwrite \
      'strimzi.io/storage={"type":"persistent-claim","size":"1Gi","deleteClaim":true}' >/dev/null
    K -n kafka replace -f "$ROOT/mcp_harness/manifests/kafka.yaml" >/dev/null ;;
  *) echo "usage: $0 inject|verify|revert new_pool_missing_storage_class|resize_not_supported" >&2; exit 2 ;;
esac
