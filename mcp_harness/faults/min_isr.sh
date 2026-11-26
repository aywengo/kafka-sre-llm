#!/bin/bash
# Fault kind min_isr: producer writes with acks=all fail because min.insync.replicas cannot be met.
#   min_isr.sh inject|verify|revert above_replication_factor|equal_to_rf_broker_down
set -u  # no pipefail: a predicate ending in grep -q must not fail on SIGPIPE upstream
source "$(dirname "$0")/../lib.sh"
action=$1 variant=$2
topic_minisr() {
  K -n kafka patch kafkatopic orders --type merge -p "{\"spec\":{\"config\":{\"min.insync.replicas\":\"$1\"}}}" >/dev/null
}
case "$action:$variant" in
  inject:above_replication_factor) topic_minisr 4 ;;
  verify:above_replication_factor) pod_logs_match app=orders-producer "NotEnoughReplicas" ;;
  inject:equal_to_rf_broker_down)
    topic_minisr 3
    K cordon "$NODE" >/dev/null
    [ "$(K -n kafka get pod kafka-broker-0 -o jsonpath='{.status.phase}' 2>/dev/null)" = Pending ] || K -n kafka delete pod kafka-broker-0 --wait=true >/dev/null ;;
  verify:equal_to_rf_broker_down)
    [ "$(K -n kafka get pod kafka-broker-0 -o jsonpath='{.status.phase}')" = Pending ] && pod_logs_match app=orders-producer "NotEnoughReplicas" ;;
  revert:*)
    K uncordon "$NODE" >/dev/null
    K -n kafka replace -f "$ROOT/mcp_harness/manifests/kafka.yaml" >/dev/null
    K -n kafka rollout restart deploy/orders-producer >/dev/null ;;  # a fresh log, so the next check is not misled
  *) echo "usage: $0 inject|verify|revert above_replication_factor|equal_to_rf_broker_down" >&2; exit 2 ;;
esac
