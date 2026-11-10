# Cluster Operator lock acquisition warnings

| Field | Value |
|---|---|
| Product | Strimzi, KRaft |
| Doc version | Strimzi 1.3.1, "Deploying and Managing Strimzi" |
| Sources | https://strimzi.io/docs/operators/1.3.1/deploying#con-failed-lock-warnings-str ; repo `documentation/modules/configuring/con-failed-lock-warnings.adoc` @ tag 1.3.1 (commit 555c3e0f07c203911bc3cb7d13ac085bfaa81b50) |
| License | Derived from Strimzi documentation, © the Strimzi authors, CC BY 4.0 (https://creativecommons.org/licenses/by/4.0/). Prose reworded; log lines quoted verbatim. |
| Retrieved | 2026-11-10 |

## Symptoms / trigger

The Cluster Operator uses locks so that only one operation runs at a time per cluster. Operations include cluster creation, rolling updates, scaling up and scaling down. An operation that tries to start while the lock is held waits for the current one to finish.
— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-failed-lock-warnings-str (Strimzi 1.3.1, retrieved 2026-11-10)

The source describes two symptoms:

- **Lock timeout.** If acquiring the lock takes longer than `STRIMZI_OPERATION_TIMEOUT_MS`, the operator logs a DEBUG message:
  ```
  DEBUG AbstractOperator:406 - Reconciliation #55(timer) Kafka(myproject/my-cluster): Failed to acquire lock lock::myproject::Kafka::my-cluster within 10000ms.
  ```
- **Possible lock release error.** An INFO message that keeps appearing with the same reconciliation number:
  ```
  INFO  AbstractOperator:399 - Reconciliation #1(watch) Kafka(myproject/my-cluster): Reconciliation is in progress
  ```

— *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-failed-lock-warnings-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Diagnosis steps

1. Look for the messages above in the Cluster Operator log. The source shows the messages but gives no command for reading the log. For general log access see [operator-logs.md](operator-logs.md).
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-failed-lock-warnings-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. Tell the two cases apart:
   - A DEBUG "Failed to acquire lock" message only means a timeout. The operation is retried at the next periodic reconciliation, whose interval is `STRIMZI_FULL_RECONCILIATION_INTERVAL_MS` (default 120 seconds).
   - The same reconciliation number repeating in the INFO "Reconciliation is in progress" message may mean the lock was not released.

   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-failed-lock-warnings-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Remediation steps

1. For a plain lock timeout, no action is described. Wait for the retry at the next periodic reconciliation.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-failed-lock-warnings-str (Strimzi 1.3.1, retrieved 2026-11-10)
2. For a suspected lock release error, restart the Cluster Operator. The source says this "can resolve" the issue. It does not give the restart command.
   — *Source:* https://strimzi.io/docs/operators/1.3.1/deploying#con-failed-lock-warnings-str (Strimzi 1.3.1, retrieved 2026-11-10)

## Verification

NO OFFICIAL SOURCE FOUND for a verification step.

## Rollback or caveats

NO OFFICIAL SOURCE FOUND.

## Related runbooks

- [operator-logs.md](operator-logs.md)
- [cr-status-and-conditions.md](cr-status-and-conditions.md)
