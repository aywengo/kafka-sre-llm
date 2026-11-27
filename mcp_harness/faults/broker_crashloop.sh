#!/bin/bash
# Fault kind broker_crashloop: a change to the node pool makes the rolled broker fail at start; the operator stops
# the rolling update after the first pod, so one broker crash-loops and the other two keep serving.
#   broker_crashloop.sh inject|verify|revert bad_jvm_option|memory_limit_below_heap
set -u  # no pipefail: a predicate ending in grep -q must not fail on SIGPIPE upstream
source "$(dirname "$0")/../lib.sh"
action=$1 variant=$2
crashing() { pod_waiting_reason strimzi.io/pool-name=broker | grep -Eq "CrashLoopBackOff|Error|OOMKilled"; }
case "$action:$variant" in
  inject:bad_jvm_option)
    K -n kafka patch kafkanodepool broker --type merge -p '{"spec":{"jvmOptions":{"-XX":{"UseStringDeduplicationX":"true"}}}}' >/dev/null ;;
  inject:memory_limit_below_heap)  # the heap is committed at start (AlwaysPreTouch) and exceeds the container limit
    K -n kafka patch kafkanodepool broker --type merge \
      -p '{"spec":{"resources":{"requests":{"memory":"256Mi"},"limits":{"memory":"256Mi"}},"jvmOptions":{"-Xms":"512m","-XX":{"AlwaysPreTouch":"true"}}}}' >/dev/null ;;
  verify:bad_jvm_option) crashing && pod_logs_match strimzi.io/pool-name=broker "Unrecognized VM option" ;;
  verify:memory_limit_below_heap) pod_waiting_reason strimzi.io/pool-name=broker | grep -q OOMKilled ;;
  revert:*)
    K -n kafka replace -f "$ROOT/mcp_harness/manifests/kafka.yaml" >/dev/null
    # restart the operator so its next reconciliation (which rolls the failing pod back) starts now
    K -n kafka delete pod -l name=strimzi-cluster-operator --wait=false >/dev/null ;;
  *) echo "usage: $0 inject|verify|revert bad_jvm_option|memory_limit_below_heap" >&2; exit 2 ;;
esac
