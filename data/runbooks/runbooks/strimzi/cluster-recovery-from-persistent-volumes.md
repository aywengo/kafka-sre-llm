# Recovering a deleted Kafka cluster from persistent volumes

| Field | Value |
|---|---|
| Product | Strimzi, KRaft |
| Doc version | Strimzi 1.3.1, "Deploying and Managing Strimzi" (chapter 33) |
| Sources | https://strimzi.io/docs/operators/1.3.1/deploying#con-cluster-recovery-scenarios-str ; https://strimzi.io/docs/operators/1.3.1/deploying#recovering_from_namespace_deletion ; https://strimzi.io/docs/operators/1.3.1/deploying#recovering_from_cluster_loss ; https://strimzi.io/docs/operators/1.3.1/deploying#proc-cluster-recovery-volume-str ; https://strimzi.io/docs/operators/1.3.1/deploying#recovery_approach ; https://strimzi.io/docs/operators/1.3.1/deploying#con-custom-resources-status-str ; repo `documentation/modules/managing/con-cluster-recovery-scenarios.adoc`, `documentation/modules/managing/proc-cluster-recovery-volume.adoc` @ tag 1.3.1 (commit 555c3e0f07c203911bc3cb7d13ac085bfaa81b50) |
| License | Derived from: Strimzi documentation, © Strimzi a Series of LF Projects, LLC; licence unconfirmed. Prose reworded; commands quoted verbatim. |
| Retrieved | 2026-11-10 |

> **Precondition: the namespace was deleted or the Kubernetes cluster was lost.** The source describes this procedure as recovering "a deleted Kafka cluster", for two scenarios only: unintentional deletion of a namespace, and loss of an entire Kubernetes cluster while the PVs remain. Do not use it on a live cluster whose PVCs still exist.
> — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-cluster-recovery-scenarios-str ; https://strimzi.io/docs/operators/1.3.1/deploying#proc-cluster-recovery-volume-str (Strimzi 1.3.1, retrieved 2026-11-10)
>
> **Ambiguity in the source.** Strimzi's broker storage section sends issues "such as failed mounts or unavailable volumes" to this procedure (`documentation/modules/configuring/con-kafka-recovering-brokers.adoc:27`, https://strimzi.io/docs/operators/1.3.1/deploying#recovery_approach). The procedure itself covers only a deleted namespace or a lost cluster. Compiler's note, not from the source: a failed mount on a running cluster is not one of those two scenarios, so do not start this procedure until the original PVCs are actually gone.
>
> **[DESTRUCTIVE if skipped] Users are deleted.** If the User Operator is enabled and you do not recreate the Kafka users, the users are deleted from the Kafka cluster immediately after recovery (step 8).
> — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-cluster-recovery-volume-str (Strimzi 1.3.1, retrieved 2026-11-10)
>
> **Data-corruption risk.** Each PV must be mounted into the correct PVC. Check every mapping yourself. The example values in the source do not match each other (see FINDINGS.md), so do not copy them.

## Symptoms / trigger

The source supports recovery from PVs in two cases:
- A namespace was deleted by accident. This deletes all PVCs, pods and services in it, but PVs with reclaim policy `Retain` keep their data.
- The whole Kubernetes cluster was lost, but the physical storage behind the PVs is intact. You then set up a new cluster and reconfigure the PVs by hand to use the existing storage.

In both cases, recovery means recreating the original PVCs.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-cluster-recovery-scenarios-str ; https://strimzi.io/docs/operators/1.3.1/deploying#recovering_from_namespace_deletion ; https://strimzi.io/docs/operators/1.3.1/deploying#recovering_from_cluster_loss (Strimzi 1.3.1, retrieved 2026-11-10)

Recovery from namespace deletion is possible only if the PV has `persistentVolumeReclaimPolicy: Retain`, or inherits `reclaimPolicy: Retain` from its storage class through `storageClassName`.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#recovering_from_namespace_deletion (Strimzi 1.3.1, retrieved 2026-11-10)

## Diagnosis steps

1. List the PVs:
   ```
   kubectl get pv
   ```
   `NAME` is the PV name, `RECLAIMPOLICY` should show `Retain`, and `CLAIM` shows the original PVC, for example `myproject/data-0-my-cluster-broker-0`.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-cluster-recovery-volume-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. Write down the PV `NAME` for each original PVC. You need it for `volumeName` in step 4, and the `volumeName` must match the PV name.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-cluster-recovery-volume-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Remediation steps

3. Recreate the original namespace:
   ```
   kubectl create namespace myproject
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-cluster-recovery-volume-str (Strimzi 1.3.1, retrieved 2026-11-10)
4. **[DATA-CORRUPTION RISK: a PV mounted into the wrong PVC]** Recreate each original PVC and link it to its PV through `spec.volumeName`. The source's example is a PVC named `data-0-my-cluster-broker-0` with `accessModes: [ReadWriteOnce]`, `storage: 100Gi`, `storageClassName: gp2-retain`, `volumeMode: Filesystem` and `volumeName: <PV name>`.
   The source requires that PVs are mounted into the correct PVC to avoid data corruption, and that `volumeName` matches the PV name.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-cluster-recovery-volume-str (Strimzi 1.3.1, retrieved 2026-11-10)
5. **[DATA-CORRUPTION RISK: edits the binding of each PV]** Edit each PV and delete the `claimRef` block that bound it to the original PVC (`apiVersion`, `kind`, `name`, `namespace`, `resourceVersion`, `uid`).
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-cluster-recovery-volume-str (Strimzi 1.3.1, retrieved 2026-11-10)
6. Deploy the Cluster Operator:
   ```
   kubectl create -f install/cluster-operator -n myproject
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-cluster-recovery-volume-str (Strimzi 1.3.1, retrieved 2026-11-10)
7. If the Topic Operator and User Operator are deployed, recreate all `KafkaTopic` resources **with the same configuration as before**. Otherwise the Topic Operator tries to update the topics in Kafka.
   ```
   kubectl apply -f <topic_configuration_file> -n myproject
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-cluster-recovery-volume-str (Strimzi 1.3.1, retrieved 2026-11-10)
8. **[DESTRUCTIVE if skipped]** If the Topic Operator and User Operator are deployed, recreate all `KafkaUser` resources. If the User Operator is enabled and the users are not recreated, the users are deleted from the Kafka cluster immediately after recovery.
   1. To keep existing passwords and certificates, recreate the user secrets first, with exactly the same name, labels and fields as the originals. If you skip this, the User Operator generates new credentials.
   2. Apply the users:
      ```
      kubectl apply -f <user_configuration_file> -n myproject
      ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-cluster-recovery-volume-str (Strimzi 1.3.1, retrieved 2026-11-10)
9. Add the annotation `strimzi.io/pause-reconciliation="true"` to the original `Kafka` resource configuration, then deploy it:
   ```
   kubectl apply -f <kafka_resource_configuration>.yaml -n myproject
   ```
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-cluster-recovery-volume-str (Strimzi 1.3.1, retrieved 2026-11-10)
10. Recover the original `clusterId` from logs or from a copy of the Kafka custom resource. Otherwise, read it from a volume with a temporary pod:
    ```
    PVC_NAME="data-0-my-cluster-broker-0"
    COMMAND="grep cluster.id /disk/kafka-log*/meta.properties | awk -F'=' '{print \$2}'"
    kubectl run tmp -itq --rm --restart "Never" --image "foo" --overrides "{\"spec\":
      {\"containers\":[{\"name\":\"busybox\",\"image\":\"busybox\",\"command\":[\"/bin/sh\",
      \"-c\",\"$COMMAND\"],\"volumeMounts\":[{\"name\":\"disk\",\"mountPath\":\"/disk\"}]}],
      \"volumes\":[{\"name\":\"disk\",\"persistentVolumeClaim\":{\"claimName\":
      \"$PVC_NAME\"}}]}}" -n myproject
    ```
    — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-cluster-recovery-volume-str (Strimzi 1.3.1, retrieved 2026-11-10)
11. **[Edits the `status` subresource]** Set `.status.clusterId` on the Kafka resource to the recovered value. Compiler's note, not from the source: check the value against the one recovered in step 10 before saving, because the source gives no way to undo this edit.
    ```
    kubectl edit kafka <cluster-name> --subresource status -n myproject
    ```
    — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-cluster-recovery-volume-str (Strimzi 1.3.1, retrieved 2026-11-10)
12. Resume reconciliation:
    ```
    kubectl annotate kafka my-cluster strimzi.io/pause-reconciliation=false \
      --overwrite -n myproject
    ```
    — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-cluster-recovery-volume-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Verification

1. Topics:
   ```
   kubectl get kafkatopics -o wide -w -n myproject
   ```
   A topic is recovered when `READY` shows `True`.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-cluster-recovery-volume-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. Users:
   ```
   kubectl get kafkausers -o wide -w -n myproject
   ```
   A user is recovered when `READY` shows `True`.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-cluster-recovery-volume-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Rollback or caveats

- **[DESTRUCTIVE if skipped]** If the User Operator is enabled and you do not recreate the Kafka users, the users are deleted from the Kafka cluster immediately after recovery (also stated at the top and in step 8).
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-cluster-recovery-volume-str (Strimzi 1.3.1, retrieved 2026-11-10)
- With reclaim policy `Retain`, PVs must be deleted by hand if you really want to delete the whole cluster.
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#recovering_from_namespace_deletion (Strimzi 1.3.1, retrieved 2026-11-10)
- Step 11 edits the `status` subresource, while §1.1.3 of the same guide says `status` cannot be edited through `kubectl edit` (see FINDINGS.md).
  — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#proc-cluster-recovery-volume-str ; https://strimzi.io/docs/operators/1.3.1/deploying#con-custom-resources-status-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Related runbooks

- [broker-storage-issues.md](broker-storage-issues.md)
- [delete-kafka-node-annotation.md](delete-kafka-node-annotation.md)
- [topic-not-ready.md](topic-not-ready.md), [user-not-ready.md](user-not-ready.md)
