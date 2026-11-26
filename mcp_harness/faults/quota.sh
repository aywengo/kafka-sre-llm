#!/bin/bash
# Fault kind quota: a client quota throttles a client.
#   quota.sh inject|verify|revert consumer_byte_rate|producer_byte_rate
# consumer_byte_rate is set on KafkaUser app-consumer (the user operator applies it); producer_byte_rate is set
# with kafka-configs.sh on the producer's client id perf-producer-client, outside any KafkaUser. (A quota on a user
# that has no KafkaUser, such as ANONYMOUS, does not last: the user operator's periodic reconciliation, every 2
# minutes, removes it; seen 2026-11-26.)
set -u  # no pipefail: a predicate ending in grep -q must not fail on SIGPIPE upstream
source "$(dirname "$0")/../lib.sh"
action=$1 variant=$2
KC=(/opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092)
CLIENT=perf-producer-client  # kafka-producer-perf-test's client.id
has_quota() { broker_exec "${KC[@]}" --describe --entity-type "$1" --entity-name "$2" 2>/dev/null | grep -q producer_byte_rate; }
case "$action:$variant" in
  inject:consumer_byte_rate)
    K -n kafka patch kafkauser app-consumer --type merge -p '{"spec":{"quotas":{"consumerByteRate":512}}}' >/dev/null ;;
  verify:consumer_byte_rate)
    v=$(group_lag orders-consumer); [ -n "$v" ] && [ "$v" -gt 400 ] \
      && broker_exec "${KC[@]}" --describe --entity-type users --entity-name CN=app-consumer 2>/dev/null | grep -q consumer_byte_rate ;;
  inject:producer_byte_rate)
    broker_exec "${KC[@]}" --alter --entity-type clients --entity-name "$CLIENT" --add-config producer_byte_rate=300 >/dev/null ;;
  verify:producer_byte_rate)
    has_quota clients "$CLIENT" \
      && pod_logs_match app=orders-producer " [0-9]\.[0-9] records/sec" ;;  # throttled to under 10 records a second
  revert:consumer_byte_rate) K -n kafka replace -f "$ROOT/mcp_harness/manifests/kafka.yaml" >/dev/null ;;
  revert:producer_byte_rate)  # also the user ANONYMOUS, where an earlier version of this fault put the quota
    if has_quota clients "$CLIENT"; then
      broker_exec "${KC[@]}" --alter --entity-type clients --entity-name "$CLIENT" --delete-config producer_byte_rate >/dev/null
    fi
    if has_quota users ANONYMOUS; then
      broker_exec "${KC[@]}" --alter --entity-type users --entity-name ANONYMOUS --delete-config producer_byte_rate >/dev/null
    fi ;;
  *) echo "usage: $0 inject|verify|revert consumer_byte_rate|producer_byte_rate" >&2; exit 2 ;;
esac
