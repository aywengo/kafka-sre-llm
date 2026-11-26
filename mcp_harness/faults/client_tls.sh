#!/bin/bash
# Fault kind client_tls: the consumer's TLS material is wrong.
#   client_tls.sh inject|verify|revert truststore_wrong_ca|selfsigned_client_cert|user_secret_missing
set -uo pipefail
source "$(dirname "$0")/../lib.sh"
action=$1 variant=$2
vol_patch() {  # vol_patch INDEX SECRET: point volume INDEX of the consumer at another secret
  K -n kafka patch deploy orders-consumer --type json \
    -p "[{\"op\":\"replace\",\"path\":\"/spec/template/spec/volumes/$1/secret/secretName\",\"value\":\"$2\"}]" >/dev/null
}
case "$action:$variant" in
  inject:truststore_wrong_ca) vol_patch 1 kafka-clients-ca-cert ;;  # the clients CA instead of the cluster CA
  verify:truststore_wrong_ca) pod_logs_match app=orders-consumer "PKIX|SSLHandshake|certificate" ;;
  inject:selfsigned_client_cert) K -n kafka set env deploy/orders-consumer KEYSTORE_MODE=selfsigned >/dev/null ;;
  verify:selfsigned_client_cert) pod_logs_match app=orders-consumer "SSL|authentication|bad_certificate|certificate" && ! group_has_members orders-consumer ;;
  inject:user_secret_missing) vol_patch 2 app-consumer-tls ;;
  verify:user_secret_missing)
    K -n kafka get events --field-selector reason=FailedMount -o jsonpath='{.items[*].message}' | grep -q app-consumer-tls ;;
  revert:*) K -n kafka replace -f "$ROOT/mcp_harness/manifests/clients.yaml" >/dev/null ;;
  *) echo "usage: $0 inject|verify|revert truststore_wrong_ca|selfsigned_client_cert|user_secret_missing" >&2; exit 2 ;;
esac
