#!/bin/bash
# The local kind cluster of the MCP harness (CPU and Docker only).
#   cluster.sh up        create cluster kafka-sre-mcp from the images and manifests fetched once (fetch.sh)
#   cluster.sh down      delete the cluster
#   cluster.sh reset     restore the healthy state (between scenarios), then wait until healthy
#   cluster.sh health    one line per check; exit 0 when all pass
#   cluster.sh pulls     image pulls since `up` (node image list against the baseline, and Pulling events); exit 1 if any
#   cluster.sh kubeconfig  (re)write the harness's own kubeconfig file from kind (work/mcp_harness/kubeconfig)
# Needs no network after fetch.sh: images are loaded into the node, every pull policy is IfNotPresent.
# Never touches the user's default kubeconfig: kind writes the cluster's context into the harness's own file, and
# every kubectl call names that file and the context (lib.sh).
set -uo pipefail
HARNESS_CTX_CHECK=defer  # the file may not exist yet (up, kubeconfig); check_context runs before the first kubectl call
source "$(dirname "$0")/lib.sh"

write_kubeconfig() {  # the cluster's kubeconfig into the harness's own file (mode 600), never the default one
  mkdir -p "$(dirname "$KCFG")"
  ( umask 077; kind get kubeconfig --name "$CLUSTER" > "$KCFG.tmp" ) && mv "$KCFG.tmp" "$KCFG" \
    || { rm -f "$KCFG.tmp"; echo "cannot write $KCFG: is cluster $CLUSTER up?" >&2; return 1; }
}

need_kubeconfig() { { [ -s "$KCFG" ] || write_kubeconfig; } && check_context; }

have_images() {
  local i
  for i in "${IMAGES[@]}" kindest/node:v1.32.2; do
    docker image inspect "$i" >/dev/null 2>&1 || { echo "missing image $i: run mcp_harness/fetch.sh first" >&2; return 1; }
  done
}

node_images() {  # the image references the kind node's containerd holds, one per line, sorted
  docker exec "$NODE" crictl images -o json | "$PY" -c '
import json, sys
for i in json.load(sys.stdin)["images"]:
    for r in (i.get("repoTags") or []) + (i.get("repoDigests") or []):
        print(r)' | sort -u
}

gitops_configmap() {
  K -n argocd create configmap gitops-content --from-file="$HARNESS/manifests/gitops/kafka" --dry-run=client -o yaml | K apply -f - >/dev/null
}

replace_or_create() {  # replace whole objects (removes fields a fault added), create the missing ones
  K replace -f "$1" >/dev/null 2>&1 || K apply -f "$1" >/dev/null
}

healthy() {
  local ok=0 v
  check() { if eval "$2" >/dev/null 2>&1; then echo "  ok   $1"; else echo "  FAIL $1"; ok=1; fi; }
  check "node schedulable" "[ -z \"\$(K get node $NODE -o jsonpath='{.spec.unschedulable}')\" ]"
  check "operator ready" "deploy_ready kafka strimzi-cluster-operator"
  check "operator watches kafka" "[ \"\$(K -n kafka get deploy strimzi-cluster-operator -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name==\"STRIMZI_NAMESPACE\")].valueFrom.fieldRef.fieldPath}')\" = metadata.namespace ]"
  check "Kafka kafka and pool broker without Warning conditions" "[ -z \"\$(K -n kafka get kafka kafka -o jsonpath='{.status.conditions[?(@.type==\"Warning\")].reason}')\$(K -n kafka get kafkanodepool broker -o jsonpath='{.status.conditions[?(@.type==\"Warning\")].reason}')\" ]"
  check "Kafka kafka Ready" "[ \"\$(K -n kafka get kafka kafka -o jsonpath='{.status.conditions[?(@.type==\"Ready\")].status}')\" = True ]"
  check "3 broker pods ready" "[ \"\$(K -n kafka get pods -l strimzi.io/pool-name=broker -o jsonpath='{range .items[*]}{.status.containerStatuses[0].ready}{\"\\n\"}{end}' | grep -c true)\" = 3 ]"
  check "only pool broker" "[ \"\$(K -n kafka get kafkanodepools -o name | wc -l | tr -d ' ')\" = 1 ]"
  check "entity operator ready" "deploy_ready kafka kafka-entity-operator"
  check "producer ready" "deploy_ready kafka orders-producer"
  check "consumer ready" "deploy_ready kafka orders-consumer"
  check "no under-replicated partitions" "[ \"\$(urp_count)\" = 0 ]"
  v=$(group_lag orders-consumer); check "orders-consumer lag below 300 (now ${v:-unknown})" "[ -n \"$v\" ] && [ \"$v\" -lt 300 ]"
  check "git daemon ready" "deploy_ready argocd gitserver"
  check "Argo CD app Synced" "[ \"\$(app_field '{.status.sync.status}')\" = Synced ]"
  check "Argo CD app Healthy" "[ \"\$(app_field '{.status.health.status}')\" = Healthy ]"
  check "topics orders, payments, inventory Ready" "[ \"\$(K -n kafka get kafkatopics orders payments inventory -o jsonpath='{range .items[*]}{.status.conditions[?(@.type==\"Ready\")].status}{\"\\n\"}{end}' | grep -c True)\" = 3 ]"
  check "users app-consumer, app-payments Ready" "[ \"\$(K -n kafka get kafkausers app-consumer app-payments -o jsonpath='{range .items[*]}{.status.conditions[?(@.type==\"Ready\")].status}{\"\\n\"}{end}' | grep -c True)\" = 2 ]"
  return $ok
}

cmd_up() {
  have_images || exit 1
  "$PY" "$HARNESS/render.py" "$RAW" "$GEN" || exit 1
  mkdir -p "$GEN"
  if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then log "cluster $CLUSTER exists"; write_kubeconfig || exit 1
  else ( umask 077; kind create cluster --config "$HARNESS/manifests/kind-config.yaml" --kubeconfig "$KCFG" ) || exit 1; fi
  check_context  # before the first kubectl call
  # `kind load docker-image` imports all platforms and fails on Docker's containerd image store when only
  # linux/arm64 was pulled; a single-platform archive loads cleanly.
  mkdir -p "$GEN/images"
  for i in "${IMAGES[@]}"; do
    local tar="$GEN/images/$(echo "$i" | tr '/:' '__').tar"
    [ -f "$tar" ] || docker save --platform linux/arm64 -o "$tar" "$i" || exit 1
    log "loading $i into the node"; kind load image-archive --name "$CLUSTER" "$tar" || exit 1
  done
  K create namespace kafka --dry-run=client -o yaml | K apply -f - >/dev/null
  K create namespace argocd --dry-run=client -o yaml | K apply -f - >/dev/null
  log "Strimzi 1.3.1 operator"
  K apply --server-side --force-conflicts -n kafka -f "$GEN/strimzi-operator.yaml" >/dev/null || exit 1
  wait_until 300 "operator ready" deploy_ready kafka strimzi-cluster-operator || exit 1
  log "Kafka cluster"
  K apply -f "$HARNESS/manifests/kafka.yaml" >/dev/null || exit 1
  log "Argo CD v3.6.1 core"
  K apply --server-side --force-conflicts -n argocd -f "$GEN/argocd-core.yaml" >/dev/null || exit 1
  gitops_configmap
  wait_until 300 "Argo CD repo server ready" deploy_ready argocd argocd-repo-server || exit 1
  K apply -f "$HARNESS/manifests/argocd.yaml" >/dev/null || exit 1
  wait_until 900 "Kafka kafka Ready" sh -c "[ \"\$(kubectl --kubeconfig '$KCFG' --context $CTX -n kafka get kafka kafka -o jsonpath='{.status.conditions[?(@.type==\"Ready\")].status}')\" = True ]" || exit 1
  wait_until 300 "secret app-consumer" K -n kafka get secret app-consumer || exit 1
  K apply -f "$HARNESS/manifests/clients.yaml" >/dev/null || exit 1
  wait_until 600 "healthy" healthy || { healthy; exit 1; }
  mkdir -p "$GEN"
  node_images > "$GEN/node-images.baseline"
  date -u +%Y-%m-%dT%H:%M:%SZ > "$GEN/up.time"
  log "up: done; $(wc -l < "$GEN/node-images.baseline" | tr -d ' ') image references in the node (baseline written)"
}

cmd_reset() {
  log "reset: restore the healthy state"
  K uncordon "$NODE" >/dev/null 2>&1
  K -n kafka delete kafkanodepools,kafkatopics,kafkausers,pvc,configmaps,secrets -l mcp-harness/fault=true --ignore-not-found --wait=false >/dev/null 2>&1
  for f in kafka.yaml clients.yaml; do replace_or_create "$HARNESS/manifests/$f"; done
  bash "$HARNESS/faults/quota.sh" revert producer_byte_rate >/dev/null 2>&1  # a quota set outside any KafkaUser
  replace_or_create "$GEN/operator-deployment.yaml"
  K -n kafka scale deploy strimzi-cluster-operator --replicas=1 >/dev/null 2>&1
  K -n kafka annotate kafka kafka strimzi.io/pause-reconciliation- >/dev/null 2>&1
  gitops_configmap
  # after a restart of the node the git daemon's pod can stay in phase Unknown (its repository lives in an
  # emptyDir); a new pod rebuilds the repository from the ConfigMap at the base commit
  if ! deploy_ready argocd gitserver; then
    K -n argocd delete pod -l app=gitserver --wait=false >/dev/null 2>&1
    wait_until 120 "git daemon ready" deploy_ready argocd gitserver
  fi
  gitserver_exec "git reset -q --hard base && git log --oneline -1" >/dev/null 2>&1
  replace_or_create "$HARNESS/manifests/argocd.yaml"
  argo_refresh
  # direct edits of Argo-managed objects: sync them back from git (selfHeal is off on purpose)
  if [ "$(app_field '{.status.sync.status}')" != "Synced" ]; then
    K -n argocd patch application kafka-topics --type merge -p '{"operation":{"initiatedBy":{"username":"harness-reset"},"sync":{"revision":"main","prune":true}}}' >/dev/null 2>&1
  fi
  wait_until "${RESET_TIMEOUT:-900}" "healthy" healthy || { healthy; return 1; }
}

cmd_pulls() {
  local base="$GEN/node-images.baseline" since n_new n_ev
  [ -f "$base" ] || { echo "no baseline: run cluster.sh up" >&2; return 2; }
  n_new=$(comm -13 "$base" <(node_images) | tee "$GEN/node-images.new" | wc -l | tr -d ' ')
  n_ev=$(K get events -A --field-selector reason=Pulling -o name 2>/dev/null | wc -l | tr -d ' ')
  echo "{\"new_image_refs\": $n_new, \"pulling_events\": $n_ev}"
  [ "$n_new" = 0 ] && [ "$n_ev" = 0 ]
}

case "${1:-}" in
  up) cmd_up ;;
  down) kind delete cluster --name "$CLUSTER" --kubeconfig "$KCFG" ;;
  reset) need_kubeconfig && cmd_reset ;;
  health) need_kubeconfig && healthy ;;
  pulls) need_kubeconfig && cmd_pulls ;;
  kubeconfig) write_kubeconfig && check_context ;;
  *) echo "usage: $0 up|down|reset|health|pulls|kubeconfig" >&2; exit 2 ;;
esac
