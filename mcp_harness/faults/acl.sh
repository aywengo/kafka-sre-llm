#!/bin/bash
# Fault kind acl: KafkaUser app-consumer lacks an ACL the consumer needs.
#   acl.sh inject|verify|revert group_acl_misnamed|topic_acl_missing
set -uo pipefail
source "$(dirname "$0")/../lib.sh"
action=$1 variant=$2
acls() { K -n kafka patch kafkauser app-consumer --type merge -p "{\"spec\":{\"authorization\":{\"type\":\"simple\",\"acls\":$1}}}" >/dev/null; }
TOPIC='{"resource":{"type":"topic","name":"orders","patternType":"literal"},"operations":["Read","Describe"]}'
case "$action:$variant" in
  inject:group_acl_misnamed)
    acls "[$TOPIC,{\"resource\":{\"type\":\"group\",\"name\":\"orders-consumers\",\"patternType\":\"literal\"},\"operations\":[\"Read\"]}]" ;;
  verify:group_acl_misnamed) pod_logs_match app=orders-consumer "GroupAuthorization|Not authorized to access group" ;;
  inject:topic_acl_missing)
    acls '[{"resource":{"type":"group","name":"orders-consumer","patternType":"literal"},"operations":["Read"]}]' ;;
  verify:topic_acl_missing) pod_logs_match app=orders-consumer "TopicAuthorization|Not authorized to access topics" ;;
  revert:*)
    K -n kafka replace -f "$ROOT/mcp_harness/manifests/kafka.yaml" >/dev/null
    K -n kafka rollout restart deploy/orders-consumer >/dev/null ;;
  *) echo "usage: $0 inject|verify|revert group_acl_misnamed|topic_acl_missing" >&2; exit 2 ;;
esac
