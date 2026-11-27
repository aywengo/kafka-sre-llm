#!/bin/bash
# Fault kind consumer_lag: the consumer group orders-consumer falls behind the producer.
#   consumer_lag.sh inject|verify|revert consumer_stopped|slow_processing
set -u  # no pipefail: a predicate ending in grep -q must not fail on SIGPIPE upstream
source "$(dirname "$0")/../lib.sh"
action=$1 variant=$2
lag_over() { local v; v=$(group_lag orders-consumer); [ -n "$v" ] && [ "$v" -gt "$1" ]; }
case "$action:$variant" in
  inject:consumer_stopped) K -n kafka scale deploy orders-consumer --replicas=0 >/dev/null ;;
  verify:consumer_stopped) lag_over 400 && ! group_has_members orders-consumer ;;
  inject:slow_processing) K -n kafka set env deploy/orders-consumer PROCESS_DELAY=0.2 >/dev/null ;;
  verify:slow_processing) lag_over 400 && group_has_members orders-consumer ;;
  revert:*) K -n kafka replace -f "$ROOT/mcp_harness/manifests/clients.yaml" >/dev/null ;;
  *) echo "usage: $0 inject|verify|revert consumer_stopped|slow_processing" >&2; exit 2 ;;
esac
