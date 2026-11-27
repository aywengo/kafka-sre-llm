#!/bin/bash
# Fault kind client_listener: a client's security protocol does not match the listener it connects to.
#   client_listener.sh inject|verify|revert plaintext_to_tls_port|ssl_to_plain_port
set -u  # no pipefail: a predicate ending in grep -q must not fail on SIGPIPE upstream
source "$(dirname "$0")/../lib.sh"
action=$1 variant=$2
case "$action:$variant" in
  inject:plaintext_to_tls_port) K -n kafka set env deploy/orders-producer BOOTSTRAP=kafka-kafka-bootstrap:9093 >/dev/null ;;
  verify:plaintext_to_tls_port) pod_logs_match app=orders-producer "disconnected|not present in metadata|TimeoutException" ;;
  inject:ssl_to_plain_port) K -n kafka set env deploy/orders-consumer BOOTSTRAP=kafka-kafka-bootstrap:9092 >/dev/null ;;
  verify:ssl_to_plain_port)  # not the bare word SSL: the client's start line prints security.protocol=SSL
    pod_logs_match app=orders-consumer "SSLHandshakeException|SSL handshake failed|Unrecognized SSL message|disconnected" \
      && ! group_has_members orders-consumer ;;
  revert:*) K -n kafka replace -f "$ROOT/mcp_harness/manifests/clients.yaml" >/dev/null ;;
  *) echo "usage: $0 inject|verify|revert plaintext_to_tls_port|ssl_to_plain_port" >&2; exit 2 ;;
esac
