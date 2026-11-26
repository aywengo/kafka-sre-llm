#!/bin/bash
# Fault kind operator: the Strimzi cluster operator does not reconcile. Each variant also changes the Kafka spec
# (log.retention.ms), so the symptom is a change that is never applied (generation ahead of observedGeneration).
#   operator.sh inject|verify|revert scaled_to_zero|crashloop_bad_env|wrong_watch_namespace
set -uo pipefail
source "$(dirname "$0")/../lib.sh"
action=$1 variant=$2
case "$action:$variant" in
  inject:scaled_to_zero) K -n kafka scale deploy strimzi-cluster-operator --replicas=0 >/dev/null && kafka_spec_bump ;;
  verify:scaled_to_zero) [ -z "$(K -n kafka get pods -l name=strimzi-cluster-operator -o name)" ] && kafka_generation_lags ;;
  inject:crashloop_bad_env) op_env STRIMZI_FULL_RECONCILIATION_INTERVAL_MS two-minutes && kafka_spec_bump ;;
  verify:crashloop_bad_env) pod_waiting_reason name=strimzi-cluster-operator | grep -Eq "CrashLoopBackOff|Error" && kafka_generation_lags ;;
  inject:wrong_watch_namespace) op_env STRIMZI_NAMESPACE kafka-prod && kafka_spec_bump ;;
  verify:wrong_watch_namespace)
    [ "$(K -n kafka get deploy strimzi-cluster-operator -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="STRIMZI_NAMESPACE")].value}')" = kafka-prod ] \
      && [ -z "$(K -n kafka get pods -l name=strimzi-cluster-operator -o jsonpath='{.items[?(@.metadata.deletionTimestamp)].metadata.name}')" ] \
      && sleep 20 && kafka_generation_lags ;;
  revert:*)
    K -n kafka replace -f "$ROOT/mcp_harness/manifests/kafka.yaml" >/dev/null
    K replace -f "$GEN/operator-deployment.yaml" >/dev/null
    K -n kafka scale deploy strimzi-cluster-operator --replicas=1 >/dev/null ;;
  *) echo "usage: $0 inject|verify|revert scaled_to_zero|crashloop_bad_env|wrong_watch_namespace" >&2; exit 2 ;;
esac
