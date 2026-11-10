# Recovering from expired Cluster Operator-managed CA certificates

| Field | Value |
|---|---|
| Product | Strimzi, KRaft |
| Doc version | Strimzi 1.3.1, "Deploying and Managing Strimzi" (§18.4.6, §18.3.3) |
| Sources | https://strimzi.io/docs/operators/1.3.1/deploying#proc-recovering-expired-ca-certs-str ; https://strimzi.io/docs/operators/1.3.1/deploying#cluster_ca_secrets ; repo `documentation/modules/security/proc-recovering-expired-ca-certs.adoc` @ tag 1.3.1 (commit 555c3e0f07c203911bc3cb7d13ac085bfaa81b50) |
| License | Derived from Strimzi documentation, © the Strimzi authors, CC BY 4.0 (https://creativecommons.org/licenses/by/4.0/). Prose reworded; commands quoted verbatim. |
| Retrieved | 2026-11-10 |

> **[DESTRUCTIVE]** This procedure deletes secrets and all Kafka pods. Kafka is not available until the pods are recreated.

## Symptoms / trigger

The Cluster Operator renews the cluster and clients CA certificates automatically when their renewal period starts. Long Cluster Operator downtime or an unavailable Kafka cluster can prevent the renewal. If the CA certificates expire, Kafka components can no longer talk to each other, and the Cluster Operator cannot renew the CA certificates without manual help.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-recovering-expired-ca-certs-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Diagnosis steps (prerequisites)

1. Check the prerequisites: the Cluster Operator is deployed; the Kafka cluster has CA certificates and private keys installed; OpenSSL is available to check certificate validity. The example uses cluster `my-cluster` in namespace `my-project`.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-recovering-expired-ca-certs-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. If you use **your own** CA certificates, this procedure does not apply as written. The process is similar, but you renew your CA certificates yourself instead of letting the Cluster Operator generate them.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-recovering-expired-ca-certs-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Remediation steps

Do the steps in the order given.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-recovering-expired-ca-certs-str (Strimzi 1.3.1, retrieved 2026-11-10)

1. **[DESTRUCTIVE]** Delete the secret that contains the expired CA certificate.
   Cluster CA:
   ```
   kubectl delete secret my-cluster-cluster-ca-cert -n my-project
   ```
   Clients CA:
   ```
   kubectl delete secret my-cluster-clients-ca-cert -n my-project
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-recovering-expired-ca-certs-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. Wait for the Cluster Operator to generate new certificates. It recreates them in secrets with the same names: `my-cluster-cluster-ca-cert` (verifies broker identity) and `my-cluster-clients-ca-cert` (verifies user identity).
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-recovering-expired-ca-certs-str (Strimzi 1.3.1, retrieved 2026-11-10)
3. Check the validity period of the new CA certificates:
   ```
   kubectl get secret my-cluster-cluster-ca-cert -n my-project -o=jsonpath='{.data.ca\.crt}' | base64 -d | openssl x509 -noout -dates
   ```
   ```
   kubectl get secret my-cluster-clients-ca-cert -n my-project -o=jsonpath='{.data.ca\.crt}' | base64 -d | openssl x509 -noout -dates
   ```
   The output shows `notBefore` and `notAfter`, the start and end of the certificate's validity.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-recovering-expired-ca-certs-str (Strimzi 1.3.1, retrieved 2026-11-10)
4. **[DESTRUCTIVE]** Delete the component pods and secrets that use the CA certificates:
   1. Delete the Kafka secret. The procedure does not name this secret. The cluster CA secrets reference in the same guide lists per-node secrets named `<cluster_name>-<pool_name>-<node_id>` (see FINDINGS.md).
   2. Wait for the Cluster Operator to detect the missing Kafka secret and recreate it.
   3. Delete all Kafka pods.

   If you only recover the clients CA certificate, you only need to delete the Kafka secret and the Kafka pods.
   To find the resources, and later to confirm they are gone, use:
   ```
   kubectl get <resource_type> --all-namespaces | grep <kafka_cluster_name>
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-recovering-expired-ca-certs-str (Strimzi 1.3.1, retrieved 2026-11-10); https://strimzi.io/docs/operators/1.3.1/deploying#cluster_ca_secrets (Strimzi 1.3.1, retrieved 2026-11-10)
5. Wait for the Cluster Operator to detect the missing Kafka pods and recreate them with the updated CA certificates. During reconciliation it also updates the other components to trust the new CA certificates.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-recovering-expired-ca-certs-str (Strimzi 1.3.1, retrieved 2026-11-10)
6. Update client configurations so clients trust the new cluster CA certificate. The source points to its sections on configuring internal and external clients to trust the cluster CA.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-recovering-expired-ca-certs-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Verification

- Check that the Cluster Operator log shows no certificate validation problems.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-recovering-expired-ca-certs-str (Strimzi 1.3.1, retrieved 2026-11-10)
- The `openssl x509 -noout -dates` output from step 3 shows the new validity window.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-recovering-expired-ca-certs-str (Strimzi 1.3.1, retrieved 2026-11-10)
- Restarts caused by CA changes appear as restart events with reasons such as `CaCertRenewed` or `CaCertHasOldGeneration`; see [kafka-restart-events.md](kafka-restart-events.md).
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#ref-operator-restart-events-reasons-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Rollback or caveats

- After recovery, clients must trust the new cluster CA. Only the `<cluster_name>-cluster-ca-cert` secret is needed by clients.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#cluster_ca_secrets (Strimzi 1.3.1, retrieved 2026-11-10)
- NO OFFICIAL SOURCE FOUND for a rollback. The deleted secrets are replaced, not restored.

## Related runbooks

- [kafka-restart-events.md](kafka-restart-events.md)
- [prometheus-alert-rules.md](prometheus-alert-rules.md) (certificate expiry alert)
- [operator-logs.md](operator-logs.md)
