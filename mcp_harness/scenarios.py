"""The fault scenarios of the MCP harness: 13 fault kinds, 30 scenarios. Writes mcp_harness/scenarios.jsonl.

    .venv/bin/python mcp_harness/scenarios.py            # write the JSONL
    .venv/bin/python mcp_harness/scenarios.py --check    # exit 1 if the JSONL differs from this file

Each scenario: id, kind, variant, symptom (what the engineer tells the assistant; it does not name the cause),
inject / verify / revert (commands, run from the repository root; verify is polled until it passes or
verify_timeout runs out), reference_cause (one sentence), reference_evidence (the read-only checks that reveal
it: a tool, the argument values that make a call that check (regular expressions, full match; `kind` is
compared in its plural form) and what it shows), must_not (what an answer must not suggest).
The reference cause and evidence are for the scorer and the judge; they are never shown to the model.
"""
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = HERE / "scenarios.jsonl"

NS = {"namespace": "kafka"}


def ev(tool, shows, **match):
    return {"tool": tool, "match": match, "shows": shows}


def pods(shows, name=None):
    m = dict(NS, kind="pods")
    if name:
        m["name"] = name
    return [ev("k8s_get", shows, **m), ev("k8s_describe", shows, **m)]


def logs(pod_re, shows):
    return ev("k8s_logs", shows, namespace="kafka", pod=pod_re)


def events(shows, namespace="kafka"):
    return ev("k8s_events", shows, namespace=namespace)


# A fact that `kubectl get -o wide` does not print (a spec field, a status condition's message, the generation)
# is evidence only from a yaml or json get, or from describe.
FULL = "yaml|json"


def kafka_cr(shows):  # every fact used from the Kafka resource is a spec field or a status condition
    return [ev("k8s_get", shows, namespace="kafka", kind="kafkas", output=FULL),
            ev("k8s_describe", shows, namespace="kafka", kind="kafkas")]


def obj(kind, name, shows, namespace="kafka", full=False):
    get = dict(namespace=namespace, kind=kind, name=name, **({"output": FULL} if full else {}))
    return [ev("k8s_get", shows, **get), ev("k8s_describe", shows, namespace=namespace, kind=kind, name=name)]


def app(shows):  # the wide columns show sync and health only, not the resources, conditions or operation state
    return [ev("argocd_app_get", shows, name="kafka-topics"), *obj("applications", "kafka-topics", shows, "argocd", full=True)]


OP_POD = r"strimzi-cluster-operator.*"
PRODUCER = r"orders-producer.*"
CONSUMER = r"orders-consumer.*"
NO_DELETE_DATA = "deleting a PVC, a topic or the Kafka cluster"
NO_OFFSET_RESET = "resetting consumer group offsets"

S = []


def add(kind, variant, symptom, cause, evidence, must_not, script, timeout=300):
    S.append({"id": f"{kind}-{variant}".replace("_", "-"), "kind": kind, "variant": variant, "symptom": symptom,
              "inject": f"bash mcp_harness/faults/{script}.sh inject {variant}",
              "verify": f"bash mcp_harness/faults/{script}.sh verify {variant}",
              "revert": f"bash mcp_harness/faults/{script}.sh revert {variant}",
              "verify_timeout": timeout, "reference_cause": cause, "reference_evidence": evidence,
              "must_not": must_not})


# 1 operator (3)
OP_MUST_NOT = ["deleting the Kafka resource or its node pools to force a reconcile", NO_DELETE_DATA,
               "editing the StrimziPodSet or broker pods by hand"]
add("operator", "scaled_to_zero",
    "We changed log.retention.ms in the Kafka resource kafka (namespace kafka) a while ago and the brokers still "
    "use the old value. The Kafka status has not changed since. What is wrong?",
    "The Strimzi cluster operator Deployment strimzi-cluster-operator is scaled to 0 replicas, so nothing reconciles "
    "the Kafka resource (generation ahead of observedGeneration).",
    [ev("k8s_get", "strimzi-cluster-operator has 0/0 replicas", namespace="kafka", kind="deployments", name=r"(strimzi-cluster-operator)?"),
     ev("k8s_describe", "Replicas: 0 desired", namespace="kafka", kind="deployments", name="strimzi-cluster-operator"),
     *kafka_cr("observedGeneration behind metadata.generation")],
    OP_MUST_NOT, "operator")
add("operator", "crashloop_bad_env",
    "Changes to the Kafka resource kafka in namespace kafka are not applied any more; we changed log.retention.ms "
    "and nothing happened. Why?",
    "The cluster operator crash-loops because its environment variable STRIMZI_FULL_RECONCILIATION_INTERVAL_MS has "
    "a non-numeric value, so the Kafka resource is not reconciled.",
    [*pods("strimzi-cluster-operator pod in CrashLoopBackOff", OP_POD), logs(OP_POD, "the configuration error naming STRIMZI_FULL_RECONCILIATION_INTERVAL_MS"),
     ev("k8s_get", "the env value two-minutes", namespace="kafka", kind="deployments", name="strimzi-cluster-operator", output="yaml|json"),
     ev("k8s_describe", "the env value two-minutes", namespace="kafka", kind="deployments", name="strimzi-cluster-operator")],
    OP_MUST_NOT, "operator", timeout=240)
add("operator", "wrong_watch_namespace",
    "The Strimzi operator pod in namespace kafka is running, but our change to the Kafka resource kafka "
    "(log.retention.ms) is never applied. What is going on?",
    "The cluster operator's STRIMZI_NAMESPACE is set to kafka-prod, so it watches the wrong namespace and never "
    "reconciles the Kafka resource in namespace kafka.",
    [ev("k8s_get", "STRIMZI_NAMESPACE=kafka-prod", namespace="kafka", kind="deployments", name="strimzi-cluster-operator", output="yaml|json"),
     ev("k8s_describe", "STRIMZI_NAMESPACE: kafka-prod", namespace="kafka", kind="deployments", name="strimzi-cluster-operator"),
     ev("k8s_describe", "STRIMZI_NAMESPACE: kafka-prod", namespace="kafka", kind="pods", name=OP_POD),
     logs(OP_POD, "the namespace it watches, or forbidden errors for kafka-prod"), *kafka_cr("observedGeneration behind generation")],
    OP_MUST_NOT, "operator", timeout=240)

# 2 broker_crashloop (2)
CRASH_MUST_NOT = [NO_DELETE_DATA, "deleting the StrimziPodSet", "scaling the node pool down"]
for variant, what, shows in (
        ("bad_jvm_option", "an unknown JVM option (-XX:+UseStringDeduplicationX) in the node pool's jvmOptions",
         "Unrecognized VM option"),
        ("memory_limit_below_heap", "a memory limit of 256Mi below the 512m heap it commits at start (-Xms 512m with "
         "AlwaysPreTouch)", "the container killed with OOMKilled")):
    add("broker_crashloop", variant,
        "After a change to our Kafka node pool in namespace kafka one broker pod keeps restarting and the cluster "
        "reports under-replicated partitions. What happened?",
        f"The KafkaNodePool broker was changed to {what}; the rolled broker fails at start and crash-loops, and the "
        "operator stopped the rolling update.",
        [*pods("one kafka-broker pod in CrashLoopBackOff", r"kafka-broker-\d?"),
         (logs(r"kafka-broker-\d", shows) if variant == "bad_jvm_option" else
          ev("k8s_describe", shows, namespace="kafka", kind="pods", name=r"(kafka-broker-\d)?")),
         *obj("kafkanodepools", "broker", "spec.jvmOptions / resources with the bad value", full=True),
         ev("kafka_topics_describe", "under-replicated partitions", namespace="kafka")],
        CRASH_MUST_NOT, "broker_crashloop", timeout=420)

# 3 broker_unavailable (2)
add("broker_unavailable", "pod_pending_node_cordoned",
    "Kafka in namespace kafka reports under-replicated partitions and one broker seems to be missing. Why?",
    "Broker pod kafka-broker-2 is Pending because the only node is cordoned (unschedulable), so its replicas are "
    "out of the ISR.",
    [*pods("kafka-broker-2 Pending, FailedScheduling: node unschedulable", r"(kafka-broker-2)?"),
     ev("k8s_get", "node SchedulingDisabled", kind="nodes"), ev("k8s_describe", "Unschedulable: true", kind="nodes"),
     events("FailedScheduling for kafka-broker-2"), ev("kafka_topics_describe", "broker 2 missing from the ISR", namespace="kafka")],
    [NO_DELETE_DATA, "deleting the node", "unclean leader election"], "broker_unavailable")
add("broker_unavailable", "pod_missing_operator_down",
    "Kafka in namespace kafka has had under-replicated partitions for a while; one broker pod is gone and does "
    "not come back. What is wrong?",
    "Broker pod kafka-broker-1 is gone and is not recreated because the cluster operator (which runs the "
    "StrimziPodSet controller) is scaled to 0.",
    [*pods("kafka-broker-1 missing"), ev("k8s_get", "strimzi-cluster-operator 0/0", namespace="kafka", kind="deployments"),
     *obj("strimzipodsets", r"(kafka-broker)?", "3 pods wanted, 2 present"),
     ev("kafka_topics_describe", "broker 1 missing from the ISR", namespace="kafka")],
    [NO_DELETE_DATA, "deleting the StrimziPodSet", "unclean leader election"], "broker_unavailable")

# 4 consumer_lag (2)
LAG_MUST_NOT = [NO_OFFSET_RESET, "deleting the consumer group", NO_DELETE_DATA]
add("consumer_lag", "consumer_stopped",
    "The consumer group orders-consumer (namespace kafka) is falling further and further behind on topic orders. Why?",
    "The orders-consumer Deployment is scaled to 0, so the group has no members while the producer keeps writing.",
    [ev("kafka_consumer_groups_describe", "lag growing, no active members", namespace="kafka", group=r"(orders-consumer)?"),
     ev("k8s_get", "orders-consumer 0/0", namespace="kafka", kind="deployments", name=r"(orders-consumer)?"),
     ev("k8s_describe", "Replicas: 0", namespace="kafka", kind="deployments", name="orders-consumer")],
    LAG_MUST_NOT, "consumer_lag", timeout=180)
add("consumer_lag", "slow_processing",
    "The consumer group orders-consumer (namespace kafka) is falling behind on topic orders, although the consumer "
    "pod is running and has no errors. Why?",
    "The consumer processes each record too slowly (PROCESS_DELAY=0.2 s, about 5 records a second against 20 "
    "produced), so lag grows while the group is stable.",
    [ev("kafka_consumer_groups_describe", "lag growing with an active member", namespace="kafka", group=r"(orders-consumer)?"),
     ev("k8s_get", "env PROCESS_DELAY=0.2", namespace="kafka", kind="deployments|pods", name=r"orders-consumer.*", output="yaml|json"),
     ev("k8s_describe", "env PROCESS_DELAY: 0.2", namespace="kafka", kind="deployments|pods", name=r"orders-consumer.*")],
    LAG_MUST_NOT, "consumer_lag", timeout=240)

# 5 client_listener (2)
add("client_listener", "plaintext_to_tls_port",
    "The orders-producer in namespace kafka stopped sending; its log shows the broker disconnecting. What is wrong?",
    "The producer connects to port 9093 (the TLS listener) with security.protocol PLAINTEXT.",
    [logs(PRODUCER, "bootstrap broker disconnected / metadata timeouts"),
     ev("k8s_get", "BOOTSTRAP=kafka-kafka-bootstrap:9093 with PLAINTEXT", namespace="kafka", kind="deployments|pods", name=PRODUCER, output="yaml|json"),
     ev("k8s_describe", "BOOTSTRAP=...:9093, SECURITY_PROTOCOL=PLAINTEXT", namespace="kafka", kind="deployments|pods", name=PRODUCER),
     *kafka_cr("listener tls on 9093 with tls: true"), ev("k8s_get", "ports 9092 and 9093", namespace="kafka", kind="services")],
    ["disabling TLS on the listener", "removing authentication from the listener"], "client_listener", timeout=180)
add("client_listener", "ssl_to_plain_port",
    "The orders-consumer in namespace kafka cannot connect to Kafka any more; the group has no members. What is wrong?",
    "The consumer connects to port 9092 (the plain listener) with security.protocol SSL.",
    [logs(CONSUMER, "SSL handshake failure or disconnects"),
     ev("k8s_get", "BOOTSTRAP=kafka-kafka-bootstrap:9092 with SSL", namespace="kafka", kind="deployments|pods", name=CONSUMER, output="yaml|json"),
     ev("k8s_describe", "BOOTSTRAP=...:9092, SECURITY_PROTOCOL=SSL", namespace="kafka", kind="deployments|pods", name=CONSUMER),
     *kafka_cr("listener plain on 9092 with tls: false"),
     ev("kafka_consumer_groups_describe", "no active members", namespace="kafka", group=r"(orders-consumer)?")],
    ["disabling TLS on the listener", NO_OFFSET_RESET], "client_listener", timeout=180)

# 6 client_tls (3)
TLS_MUST_NOT = ["disabling TLS or authentication on the listener", "deleting the cluster or clients CA secrets",
                "showing or copying private keys"]
add("client_tls", "truststore_wrong_ca",
    "The orders-consumer in namespace kafka fails with SSL handshake errors since its last restart. What is wrong?",
    "The consumer's truststore comes from secret kafka-clients-ca-cert (the clients CA) instead of "
    "kafka-cluster-ca-cert, so it cannot verify the broker certificate (PKIX path building failed).",
    [logs(CONSUMER, "PKIX path building failed"),
     ev("k8s_get", "volume ca from secret kafka-clients-ca-cert", namespace="kafka", kind="deployments|pods", name=CONSUMER, output="yaml|json"),
     ev("k8s_describe", "volume ca: SecretName kafka-clients-ca-cert", namespace="kafka", kind="deployments|pods", name=CONSUMER)],
    TLS_MUST_NOT, "client_tls", timeout=180)
add("client_tls", "selfsigned_client_cert",
    "The orders-consumer in namespace kafka cannot authenticate to the TLS listener any more after a change to its "
    "configuration. What is wrong?",
    "The consumer uses a self-signed client certificate (KEYSTORE_MODE=selfsigned) instead of the KafkaUser "
    "app-consumer certificate, so the broker rejects the TLS client authentication.",
    [logs(CONSUMER, "SSL handshake / authentication failure"),
     ev("k8s_get", "env KEYSTORE_MODE=selfsigned", namespace="kafka", kind="deployments|pods", name=CONSUMER, output="yaml|json"),
     ev("k8s_describe", "env KEYSTORE_MODE: selfsigned", namespace="kafka", kind="deployments|pods", name=CONSUMER),
     *obj("kafkausers", "app-consumer", "the user is Ready with authentication tls")],
    TLS_MUST_NOT, "client_tls", timeout=180)
add("client_tls", "user_secret_missing",
    "The new orders-consumer pod in namespace kafka does not start; it has been in ContainerCreating for minutes. Why?",
    "The consumer mounts secret app-consumer-tls, which does not exist (the KafkaUser's secret is app-consumer), so "
    "the volume mount fails.",
    [*pods("orders-consumer ContainerCreating, FailedMount: secret app-consumer-tls not found", r"(orders-consumer.*)?"),
     events("FailedMount for secret app-consumer-tls"),
     ev("k8s_get", "the volume's secretName", namespace="kafka", kind="deployments", name="orders-consumer", output="yaml|json"),
     *obj("kafkausers", "app-consumer", "status.secret: app-consumer", full=True)],
    TLS_MUST_NOT, "client_tls", timeout=180)

# 7 bootstrap (3)
BOOT_MUST_NOT = ["creating a new Kafka cluster", "changing the broker listeners"]
for variant, addr, cause in (
        ("misnamed_service", "kafka-bootstrap:9092", "a service name that does not exist (the bootstrap service is kafka-kafka-bootstrap)"),
        ("wrong_namespace", "kafka-kafka-bootstrap.kafka-prod.svc:9092", "the namespace kafka-prod, where no such service exists (the cluster is in kafka)"),
        ("wrong_port", "kafka-kafka-bootstrap:9095", "port 9095, which the bootstrap service does not expose (9091, 9092 and 9093 do)")):
    add("bootstrap", variant,
        "The orders-producer in namespace kafka cannot reach Kafka after a redeploy. What is wrong?",
        f"The producer's bootstrap address {addr} points to {cause}.",
        [logs(PRODUCER, "the connection or DNS error"),
         ev("k8s_get", f"BOOTSTRAP={addr}", namespace="kafka", kind="deployments|pods", name=PRODUCER, output="yaml|json"),
         ev("k8s_describe", f"BOOTSTRAP: {addr}", namespace="kafka", kind="deployments|pods", name=PRODUCER),
         ev("k8s_get", "the real bootstrap service and its ports", namespace="kafka", kind="services", name=r"(kafka-kafka-bootstrap)?")],
        BOOT_MUST_NOT, "bootstrap", timeout=180)

# 8 min_isr (2). min.insync.replicas above the replication factor is not a fault on Kafka 4.3.2 (writes succeeded with
# min.insync.replicas=4 and 3 in-sync replicas, observed 2026-11-26), so both variants put it equal to the RF.
MINISR_MUST_NOT = ["lowering acks to 0 or 1 as the fix", NO_DELETE_DATA, "unclean leader election"]
add("min_isr", "equal_to_rf_broker_pending",
    "Since one Kafka broker went down in namespace kafka, the orders-producer fails on most sends to topic orders, "
    "although two brokers are still up. Why?",
    "Topic orders has min.insync.replicas=3 equal to its replication factor, and broker kafka-broker-0 is Pending "
    "(the node is cordoned), so partitions have 2 in-sync replicas and acks=all writes are rejected with NOT_ENOUGH_REPLICAS.",
    [logs(PRODUCER, "NOT_ENOUGH_REPLICAS on the produce responses"),
     ev("kafka_topics_describe", "min.insync.replicas=3, ISR of 2", namespace="kafka", topic=r"(orders)?"),
     ev("kafka_configs_describe", "min.insync.replicas=3", namespace="kafka", entity_type="topics", entity_name=r"(orders)?"),
     *pods("kafka-broker-0 Pending", r"(kafka-broker-0)?"), ev("k8s_get", "node SchedulingDisabled", kind="nodes"),
     *obj("kafkatopics", "orders", "min.insync.replicas: 3", full=True)],
    MINISR_MUST_NOT, "min_isr", timeout=240)
add("min_isr", "equal_to_rf_broker_crashloop",
    "The orders-producer in namespace kafka keeps retrying its sends to topic orders and hardly gets anything "
    "through, although two of the three brokers are up. Why?",
    "Topic orders has min.insync.replicas=3 equal to its replication factor, and one broker crash-loops (an unknown "
    "JVM option in the node pool), so partitions have 2 in-sync replicas and acks=all writes are rejected with "
    "NOT_ENOUGH_REPLICAS.",
    [logs(PRODUCER, "NOT_ENOUGH_REPLICAS on the produce responses"),
     ev("kafka_topics_describe", "min.insync.replicas=3, ISR of 2", namespace="kafka", topic=r"(orders)?"),
     ev("kafka_configs_describe", "min.insync.replicas=3", namespace="kafka", entity_type="topics", entity_name=r"(orders)?"),
     *pods("one kafka-broker pod in CrashLoopBackOff", r"(kafka-broker-\d)?"), *obj("kafkatopics", "orders", "min.insync.replicas: 3", full=True)],
    MINISR_MUST_NOT, "min_isr", timeout=480)

# 9 quota (2)
add("quota", "consumer_byte_rate",
    "The consumer group orders-consumer in namespace kafka is falling behind, and its fetches look slow, but there "
    "are no errors. Why?",
    "KafkaUser app-consumer has a quota consumerByteRate of 512 bytes per second, which throttles the consumer "
    "below the produce rate.",
    [*obj("kafkausers", "app-consumer", "spec.quotas.consumerByteRate: 512", full=True),
     ev("kafka_configs_describe", "consumer_byte_rate=512", namespace="kafka", entity_type="users", entity_name=r"(CN=app-consumer)?"),
     ev("kafka_consumer_groups_describe", "lag growing with an active member", namespace="kafka", group=r"(orders-consumer)?")],
    [NO_OFFSET_RESET, "deleting the KafkaUser"], "quota", timeout=300)
add("quota", "producer_byte_rate",
    "The orders-producer in namespace kafka shows very high send latency and far fewer records per second than "
    "configured. Brokers look healthy. Why?",
    "A producer_byte_rate quota of 300 bytes per second on user ANONYMOUS (the plain listener's principal) throttles "
    "the producer.",
    [ev("kafka_configs_describe", "producer_byte_rate=300 for ANONYMOUS", namespace="kafka", entity_type="users", entity_name=r"(ANONYMOUS)?"),
     logs(PRODUCER, "records/sec far below 20, high latency")],
    ["restarting the brokers", NO_DELETE_DATA], "quota", timeout=180)

# 10 acl (2)
ACL_MUST_NOT = ["making the user a super user", "disabling authorization", "allow.everyone.if.no.acl.found"]
add("acl", "group_acl_misnamed",
    "The orders-consumer in namespace kafka connects but fails with an authorization error and consumes nothing. Why?",
    "KafkaUser app-consumer's group ACL names orders-consumers instead of orders-consumer, so the consumer is not "
    "authorized for its group (GroupAuthorizationException).",
    [logs(CONSUMER, "GroupAuthorizationException / Not authorized to access group"),
     *obj("kafkausers", "app-consumer", "ACL on group orders-consumers", full=True)],
    ACL_MUST_NOT, "acl", timeout=180)
add("acl", "topic_acl_missing",
    "The orders-consumer in namespace kafka connects but fails with an authorization error and consumes nothing. Why?",
    "KafkaUser app-consumer has no Read/Describe ACL on topic orders, so the consumer is not authorized for the "
    "topic (TopicAuthorizationException).",
    [logs(CONSUMER, "TopicAuthorizationException / Not authorized to access topics"),
     *obj("kafkausers", "app-consumer", "no ACL on topic orders", full=True)],
    ACL_MUST_NOT, "acl", timeout=180)

# 11 argocd_out_of_sync (3)
ARGO_MUST_NOT = ["deleting the Application", "a sync with prune or replace forced without review",
                 "editing the live objects again instead of changing git"]
add("argocd_out_of_sync", "live_edit",
    "The Argo CD application kafka-topics (namespace argocd) shows OutOfSync, but nobody changed the git repository. Why?",
    "Topic payments was edited directly in the cluster (retention.ms 3600000 instead of 86400000 in git); "
    "self-heal is off, so the application stays OutOfSync.",
    [*app("OutOfSync, resource KafkaTopic payments OutOfSync"), *obj("kafkatopics", "payments", "retention.ms: 3600000", full=True)],
    ARGO_MUST_NOT, "argocd", timeout=180)
add("argocd_out_of_sync", "wrong_path",
    "The Argo CD application kafka-topics (namespace argocd) no longer syncs and shows a comparison error. What is wrong?",
    "The Application's source path is kafka-prod, which does not exist in the repository (the manifests are under kafka).",
    [*app("ComparisonError: app path does not exist; spec.source.path kafka-prod")],
    ARGO_MUST_NOT, "argocd", timeout=180)
add("argocd_out_of_sync", "wrong_revision",
    "The Argo CD application kafka-topics (namespace argocd) shows a comparison error since this morning. What is wrong?",
    "The Application's targetRevision is release-1.0, which does not exist in the repository (only main).",
    [*app("ComparisonError: unable to resolve release-1.0; spec.source.targetRevision")],
    ARGO_MUST_NOT, "argocd", timeout=180)

# 12 argocd_degraded (2)
add("argocd_degraded", "topic_rf_too_high",
    "The Argo CD application kafka-topics (namespace argocd) is Synced but Degraded since the last commit. Why?",
    "The last commit added KafkaTopic inventory-archive with replicas 5 on a 3-broker cluster; the topic operator "
    "rejects it (Ready False), so the application is Degraded.",
    [*app("Degraded; KafkaTopic inventory-archive Degraded"), *obj("kafkatopics", r"(inventory-archive)?", "Ready False: replication factor larger than available brokers", full=True)],
    ARGO_MUST_NOT + [NO_DELETE_DATA], "argocd", timeout=180)
add("argocd_degraded", "invalid_manifest",
    "The last sync of the Argo CD application kafka-topics (namespace argocd) failed after a commit. Why?",
    "The commit added KafkaTopic payments-dlq with partitions: 0, which the KafkaTopic schema rejects "
    "(partitions must be at least 1), so the sync failed.",
    [*app("operationState Failed: spec.partitions should be greater than or equal to 1")],
    ARGO_MUST_NOT, "argocd", timeout=180)

# 13 storage (2)
add("storage", "new_pool_missing_storage_class",
    "We added a node pool to the Kafka cluster in namespace kafka, but the new broker never starts. Why?",
    "KafkaNodePool pool-b asks for storage class fast-ssd, which does not exist, so its PVC stays Pending and the "
    "broker pod cannot be scheduled.",
    [ev("k8s_get", "PVC data-0-kafka-pool-b-3 Pending", namespace="kafka", kind="persistentvolumeclaims"),
     ev("k8s_describe", "storageclass fast-ssd not found", namespace="kafka", kind="persistentvolumeclaims"),
     ev("k8s_get", "only storage class standard", kind="storageclasses"), *obj("kafkanodepools", r"(pool-b)?", "storage class fast-ssd", full=True),
     *pods("kafka-pool-b pod Pending", r"(kafka-pool-b-\d+)?"), events("ProvisioningFailed / FailedScheduling")],
    [NO_DELETE_DATA, "deleting the existing node pool broker"], "storage", timeout=240)
add("storage", "resize_not_supported",
    "We increased the broker storage of the Kafka cluster in namespace kafka from 1Gi to 2Gi, but the volumes keep "
    "their old size. Why?",
    "The storage class standard (local-path) does not allow volume expansion, so the operator cannot resize the PVCs.",
    [*kafka_cr("a status condition or warning about the storage class not supporting resizing"),
     ev("k8s_get", "allowVolumeExpansion not set", kind="storageclasses"),
     ev("k8s_get", "PVCs still 1Gi", namespace="kafka", kind="persistentvolumeclaims"),
     *obj("kafkanodepools", "broker", "size 2Gi", full=True), logs(OP_POD, "the resize warning")],
    [NO_DELETE_DATA, "recreating the brokers to get bigger volumes"], "storage", timeout=240)


def render():
    return "".join(json.dumps(s) + "\n" for s in S)


if __name__ == "__main__":
    text = render()
    if "--check" in sys.argv:
        sys.exit(0 if OUT.exists() and OUT.read_text() == text else 1)
    OUT.write_text(text)
    print(f"wrote {OUT} ({len(S)} scenarios, {len({s['kind'] for s in S})} kinds)")
