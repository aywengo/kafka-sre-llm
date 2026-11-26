#!/bin/bash
# Fault kind bootstrap: the producer's bootstrap address is wrong.
#   bootstrap.sh inject|verify|revert misnamed_service|wrong_namespace|wrong_port
set -uo pipefail
source "$(dirname "$0")/../lib.sh"
action=$1 variant=$2
case "$action:$variant" in
  inject:misnamed_service) K -n kafka set env deploy/orders-producer BOOTSTRAP=kafka-bootstrap:9092 >/dev/null ;;
  inject:wrong_namespace) K -n kafka set env deploy/orders-producer BOOTSTRAP=kafka-kafka-bootstrap.kafka-prod.svc:9092 >/dev/null ;;
  inject:wrong_port) K -n kafka set env deploy/orders-producer BOOTSTRAP=kafka-kafka-bootstrap:9095 >/dev/null ;;
  verify:misnamed_service|verify:wrong_namespace) pod_logs_match app=orders-producer "resolvable|DNS resolution failed|UnknownHost" ;;
  verify:wrong_port) pod_logs_match app=orders-producer "could not be established|disconnected|not present in metadata|TimeoutException" ;;
  revert:*) K -n kafka replace -f "$ROOT/mcp_harness/manifests/clients.yaml" >/dev/null ;;
  *) echo "usage: $0 inject|verify|revert misnamed_service|wrong_namespace|wrong_port" >&2; exit 2 ;;
esac
