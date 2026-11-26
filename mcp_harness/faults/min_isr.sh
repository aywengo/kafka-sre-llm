#!/bin/bash
# Fault kind min_isr: producer writes with acks=all fail because topic orders has min.insync.replicas=3, equal to its
# replication factor, and one broker is out of the ISR.
#   min_isr.sh inject|verify|revert equal_to_rf_broker_pending|equal_to_rf_broker_crashloop
# (min.insync.replicas above the replication factor is no fault here: on Kafka 4.3.2 writes to orders with
# min.insync.replicas=4 and 3 in-sync replicas succeeded, observed 2026-11-26.)
set -u  # no pipefail: a predicate ending in grep -q must not fail on SIGPIPE upstream
source "$(dirname "$0")/../lib.sh"
action=$1 variant=$2
topic_minisr() {
  K -n kafka patch kafkatopic orders --type merge -p "{\"spec\":{\"config\":{\"min.insync.replicas\":\"$1\"}}}" >/dev/null
}
rejected() { pod_logs_match app=orders-producer "NOT_ENOUGH_REPLICAS|NotEnoughReplicas"; }
crashing() { pod_waiting_reason strimzi.io/pool-name=broker | grep -Eq "CrashLoopBackOff|Error"; }
case "$action:$variant" in
  inject:equal_to_rf_broker_pending)
    topic_minisr 3
    K cordon "$NODE" >/dev/null
    [ "$(K -n kafka get pod kafka-broker-0 -o jsonpath='{.status.phase}' 2>/dev/null)" = Pending ] || K -n kafka delete pod kafka-broker-0 --wait=true >/dev/null ;;
  verify:equal_to_rf_broker_pending)
    [ "$(K -n kafka get pod kafka-broker-0 -o jsonpath='{.status.phase}')" = Pending ] && rejected ;;
  inject:equal_to_rf_broker_crashloop)
    # the broken roll first: Strimzi does not roll a broker while that would take partitions below min.insync.replicas
    K -n kafka patch kafkanodepool broker --type merge -p '{"spec":{"jvmOptions":{"-XX":{"UseStringDeduplicationX":"true"}}}}' >/dev/null
    wait_until 360 "a broker crash-looping" crashing >/dev/null || exit 1
    topic_minisr 3 ;;
  verify:equal_to_rf_broker_crashloop) crashing && rejected ;;
  revert:*)
    K uncordon "$NODE" >/dev/null
    K -n kafka replace -f "$ROOT/mcp_harness/manifests/kafka.yaml" >/dev/null
    [ "$variant" = equal_to_rf_broker_crashloop ] && K -n kafka delete pod -l name=strimzi-cluster-operator --wait=false >/dev/null
    K -n kafka rollout restart deploy/orders-producer >/dev/null ;;  # a fresh log, so the next check is not misled
  *) echo "usage: $0 inject|verify|revert equal_to_rf_broker_pending|equal_to_rf_broker_crashloop" >&2; exit 2 ;;
esac
