# MCP harness: how reliably a model calls read-only tools

EXPERIMENTAL (M7, decisions W30 and W31 in `MILESTONE.md`; `GOAL.md`, "Tools, MCP and skills", step 6).
A local kind cluster with injected faults, a local MCP server with read-only tools, the runtime's tool-use loop
and a mechanical scorer. CPU and Docker only. Nothing here starts a model; a run against a model needs the
measurement rule in `MILESTONE.md` first.

## Downloads (once, at install)

`fetch.sh` downloads exactly what `downloads.lock.json` records and checks every sha256 and image digest. After
that a run makes no network call: the images are loaded into the kind node, every pull policy is
`IfNotPresent`, and `cluster.sh pulls` fails if the node's image list grew or any `Pulling` event exists.

| What | Version | Pin |
|---|---|---|
| Strimzi install YAML | 1.3.1 (matches `sources.yaml`) | sha256 `557c1452…1931` |
| Argo CD `core-install.yaml` | v3.6.1 (matches `sources.yaml`) | sha256 `0b38189d…3c` |
| `kindest/node` | v1.32.2 (kind v0.27.0 default) | `sha256:142f5435…950b` |
| `quay.io/strimzi/operator` | 1.3.1 | `sha256:55460ee2…7926` |
| `quay.io/strimzi/kafka` | 1.3.1-kafka-4.3.2 (brokers and the clients) | `sha256:bc23e15a…1013` |
| `quay.io/argoproj/argocd` | v3.6.1 (Argo CD core and the git daemon) | `sha256:296e3d74…5c54` |
| `public.ecr.aws/docker/library/redis` | 8.10.1-alpine (Argo CD's cache) | `sha256:ef216d85…3b79` |

No pip package: the MCP client and server use the standard library only.

## Use

    bash mcp_harness/fetch.sh                  # once; the only step that uses the network
    bash mcp_harness/cluster.sh up             # kind cluster kafka-sre-mcp: Strimzi, Kafka (3 nodes, KRaft), Argo CD, clients
    bash mcp_harness/cluster.sh health         # one line per check
    .venv/bin/python mcp_harness/harness.py check-faults --out work/mcp_harness/fault_check.jsonl
    .venv/bin/python mcp_harness/harness.py run --model stub --ids ID ... --out work/mcp_harness/dry_run
    bash mcp_harness/cluster.sh down

The harness never uses your default kubeconfig or its current context. `cluster.sh up` has kind write the
cluster's context into the harness's own file `work/mcp_harness/kubeconfig` (git-ignored, mode 600; for a cluster
created earlier, `cluster.sh kubeconfig` writes it), and every kubectl call names that file and the context
`kind-kafka-sre-mcp`. `diagnose` and the MCP server refuse to start without an explicit `--kubeconfig` and context:

    .venv/bin/python -m kafka_sre_rag diagnose --tools mcp --kubeconfig work/mcp_harness/kubeconfig \
        --kube-context kind-kafka-sre-mcp --stub STUB.json "QUESTION"

`scenarios.jsonl` (built by `scenarios.py`) has 30 scenarios of 13 fault kinds; each has inject, verify and
revert commands (`faults/<kind>.sh`), the symptom the engineer reports, the reference cause, the reference
evidence (which read-only calls on which objects reveal it) and a must-not list. `cluster.sh reset` restores
the healthy state between scenarios.

The runtime side lives in `kafka_sre_rag/tools/` (`python -m kafka_sre_rag diagnose --tools mcp ...`); the tool
prompt is `TOOLS_SYSTEM` in `kafka_sre_rag/prompts.py`, version `TOOLS_PROMPT_VERSION`.
