# Recovering from expired Cluster Operator-managed CA certificates

| Field | Value |
|---|---|
| Product | Strimzi, KRaft |
| Doc version | Strimzi 1.3.1, "Deploying and Managing Strimzi" (§18.4.6, §18.3.3) |
| Sources | https://strimzi.io/docs/operators/1.3.1/deploying#proc-recovering-expired-ca-certs-str ; https://strimzi.io/docs/operators/1.3.1/deploying#cluster_ca_secrets ; repo `documentation/modules/security/proc-recovering-expired-ca-certs.adoc` @ tag 1.3.1 (commit 555c3e0f07c203911bc3cb7d13ac085bfaa81b50) |
| License | Derived from Strimzi documentation, © the Strimzi authors, CC BY 4.0 (https://creativecommons.org/licenses/by/4.0/). Prose reworded; commands quoted verbatim. |
| Retrieved | 2026-11-10 |

> **[DESTRUCTIVE]** This procedure deletes secrets and all Kafka pods. Compiler's inference, not from the source: Kafka is not available until the pods are recreated.
>
> **Run each CA deletion only for a CA that has expired** (Diagnosis step 3). The source says to delete "the secret containing the expired CA certificate".
>
> **Step 4.1 cannot be completed from official docs.** The source says "Delete the Kafka secret" without naming it (FINDINGS F8). Do not guess the name; see step 4.

## Symptoms / trigger

The Cluster Operator renews the cluster and clients CA certificates automatically when their renewal period starts. Long Cluster Operator downtime or an unavailable Kafka cluster can prevent the renewal. If the CA certificates expire, Kafka components can no longer talk to each other, and the Cluster Operator cannot renew the CA certificates without manual help.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-recovering-expired-ca-certs-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Diagnosis steps (prerequisites)

1. Check the prerequisites: the Cluster Operator is deployed; the Kafka cluster has CA certificates and private keys installed; OpenSSL is available to check certificate validity. The example uses cluster `my-cluster` in namespace `my-project`.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-recovering-expired-ca-certs-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. If you use **your own** CA certificates, this procedure does not apply as written. The process is similar, but you renew your CA certificates yourself instead of letting the Cluster Operator generate them.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-recovering-expired-ca-certs-str (Strimzi 1.3.1, retrieved 2026-11-10)
3. Find out which CA has expired. Check the validity period of each CA certificate:
   ```
   kubectl get secret my-cluster-cluster-ca-cert -n my-project -o=jsonpath='{.data.ca\.crt}' | base64 -d | openssl x509 -noout -dates
   ```
   ```
   kubectl get secret my-cluster-clients-ca-cert -n my-project -o=jsonpath='{.data.ca\.crt}' | base64 -d | openssl x509 -noout -dates
   ```
   The output shows `notBefore` and `notAfter`, the start and end of the certificate's validity. A CA whose `notAfter` date has passed is expired.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-recovering-expired-ca-certs-str (Strimzi 1.3.1, retrieved 2026-11-10). The source lists OpenSSL as a prerequisite "to check the period of validity for CA certificates" and gives these commands for the new certificates (Remediation step 3). Compiler's note, not from the source: running them before step 1 is this runbook's use of the same commands.

## Remediation steps

Do the steps in the order given.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-recovering-expired-ca-certs-str (Strimzi 1.3.1, retrieved 2026-11-10)

1. **[DESTRUCTIVE]** Delete the secret that contains the expired CA certificate. Run only the command for the CA that Diagnosis step 3 shows as expired.
   Cluster CA (only if the cluster CA has expired):
   ```
   kubectl delete secret my-cluster-cluster-ca-cert -n my-project
   ```
   Clients CA (only if the clients CA has expired):
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
   1. Delete the Kafka secret. **This step cannot be completed from official docs**: the procedure does not name this secret, and no other section of the guide says which secret it means (FINDINGS F8). Do not guess. Confirm the secret name with the Strimzi maintainers before you run this procedure. NO OFFICIAL SOURCE FOUND for the name.
   2. Wait for the Cluster Operator to detect the missing Kafka secret and recreate it.
   3. Delete all Kafka pods.

   If you only recover the clients CA certificate, you only need to delete the Kafka secret and the Kafka pods.
   To find the resources, and later to confirm they are gone, use:
   ```
   kubectl get <resource_type> --all-namespaces | grep <kafka_cluster_name>
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-recovering-expired-ca-certs-str (Strimzi 1.3.1, retrieved 2026-11-10)
5. Wait for the Cluster Operator to detect the missing Kafka pods and recreate them with the updated CA certificates. During reconciliation it also updates the other components to trust the new CA certificates.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-recovering-expired-ca-certs-str (Strimzi 1.3.1, retrieved 2026-11-10)
6. Update client configurations so clients trust the new cluster CA certificate. The source points to its sections on configuring internal and external clients to trust the cluster CA.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-recovering-expired-ca-certs-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Verification

- Check that the Cluster Operator log shows no certificate validation problems.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-recovering-expired-ca-certs-str (Strimzi 1.3.1, retrieved 2026-11-10)
- The `openssl x509 -noout -dates` output from step 3 shows the new validity window.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-recovering-expired-ca-certs-str (Strimzi 1.3.1, retrieved 2026-11-10)
- Restart reasons such as `CaCertRenewed` and `CaCertHasOldGeneration` describe restarts that the Cluster Operator performs during CA renewal. This procedure deletes the Kafka pods by hand, and the source does not say which restart events, if any, it produces. Do not use these events as a check for this procedure. See [kafka-restart-events.md](kafka-restart-events.md).
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#ref-operator-restart-events-reasons-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Rollback or caveats

- After recovery, clients must trust the new cluster CA. Only the `<cluster_name>-cluster-ca-cert` secret is needed by clients.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#cluster_ca_secrets (Strimzi 1.3.1, retrieved 2026-11-10)
- NO OFFICIAL SOURCE FOUND for a rollback. The deleted secrets are replaced, not restored.

## Related runbooks

- [kafka-restart-events.md](kafka-restart-events.md)
- [prometheus-alert-rules.md](prometheus-alert-rules.md) (certificate expiry alert)
- [operator-logs.md](operator-logs.md)
