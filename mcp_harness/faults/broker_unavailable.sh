#!/bin/bash
# Fault kind broker_unavailable: one broker is not running, so partitions are under-replicated.
#   broker_unavailable.sh inject|verify|revert pod_pending_node_cordoned|pod_missing_operator_down
set -u  # no pipefail: a predicate ending in grep -q must not fail on SIGPIPE upstream
source "$(dirname "$0")/../lib.sh"
action=$1 variant=$2
case "$action:$variant" in
  inject:pod_pending_node_cordoned)
    K cordon "$NODE" >/dev/null
    [ "$(K -n kafka get pod kafka-broker-2 -o jsonpath='{.status.phase}' 2>/dev/null)" = Pending ] || K -n kafka delete pod kafka-broker-2 --wait=true >/dev/null ;;
  verify:pod_pending_node_cordoned)
    [ "$(K -n kafka get pod kafka-broker-2 -o jsonpath='{.status.phase}')" = Pending ] && [ "$(urp_count)" -gt 0 ] ;;
  revert:pod_pending_node_cordoned) K uncordon "$NODE" >/dev/null ;;
  inject:pod_missing_operator_down)
    K -n kafka scale deploy strimzi-cluster-operator --replicas=0 >/dev/null
    K -n kafka wait --for=delete pod -l name=strimzi-cluster-operator --timeout=120s >/dev/null 2>&1
    K -n kafka delete pod kafka-broker-1 --ignore-not-found --wait=true >/dev/null ;;
  verify:pod_missing_operator_down)
    ! K -n kafka get pod kafka-broker-1 >/dev/null 2>&1 && [ "$(urp_count)" -gt 0 ] ;;
  revert:pod_missing_operator_down)
    K -n kafka scale deploy strimzi-cluster-operator --replicas=1 >/dev/null
    # the restarted StrimziPodSet controller does not recreate a pod deleted while it was down until the
    # StrimziPodSet changes; a harmless annotation makes it reconcile now
    wait_until 180 "operator ready" deploy_ready kafka strimzi-cluster-operator
    K -n kafka annotate strimzipodset kafka-broker mcp-harness/nudge="$(date +%s)" --overwrite >/dev/null ;;
  *) echo "usage: $0 inject|verify|revert pod_pending_node_cordoned|pod_missing_operator_down" >&2; exit 2 ;;
esac
