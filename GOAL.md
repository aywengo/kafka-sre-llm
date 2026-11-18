# Goal

**Status: agreed with the Owner on 2026-11-17, except the items marked "proposed" and the
list under "Open decisions".**

## The goal in one sentence

A local AI assistant that fits on a working laptop and is a competent colleague for an
engineer who builds and runs Apache Kafka on Kubernetes under GitOps: it writes and reviews
code and configuration, diagnoses and debugs problems with tools, and advises on streaming
architecture, with answers and actions that can be trusted.

"Assistant" means the whole thing that runs on the laptop: a model, a documentation index,
tools, MCP servers, skills and the runtime that puts them together. The weights alone are
not the product.

## Priorities: intelligence and privacy first

When two aims pull apart, these two win, in this order of thought:

1. **Intelligence.** The quality of the answer comes before speed, size and convenience.
   Within the limits below we choose the most capable configuration that fits, not the
   smallest or the fastest. An answer that is quick and wrong is a failure.
2. **Privacy.** Nothing the user types, pastes or lets a tool read leaves the machine. No
   hosted model, no telemetry, no network call at answer time. A feature that needs data to
   leave the laptop is not built.

Fit, speed and ease of installation are limits to stay inside, not things to optimise at
the cost of these two.

## Who it is for

An engineer (SRE, platform or application developer) who runs Apache Kafka themselves: on
Kubernetes with Strimzi, deployed with Argo CD, or as plain Apache Kafka. They know
the basics. They need exact flags, fields and procedures, a second opinion on a design, and
help reading a failure, often with cluster details they cannot paste into a hosted service.

## What "fits on a working laptop" means

A working laptop also runs an IDE, a browser, containers and often a local cluster. The
assistant may use part of the memory, not all of it.

| | Reference (must work) | Stretch (should work) |
|---|---|---|
| Machine | Apple Silicon laptop, 32 to 36 GB memory | 16 to 24 GB memory |
| Memory the assistant may hold | about 20 GB | about 10 GB |
| Model | 27B class at 4-bit (about 15 to 17 GB, an estimate to verify) | a smaller model, to be chosen |
| Network | none needed after installation | same |
| Time to a complete answer | median under 60 s | same |
| Installation | one documented command sequence, under 30 minutes | same |

Platforms:

- **Apple Silicon** is the reference. The recommended setup serves the model with oMLX on
  the same machine; the runtime talks to it over the local OpenAI-compatible endpoint.
- **Linux and Windows laptops** are supported through GGUF builds (llama.cpp and tools
  built on it), over the same kind of local endpoint. Their reference machine has 32 GB of
  memory and a GPU. The GPU requirement is the one of the Qwen3.8-27B model at the
  quantisation we ship (about 15 to 17 GB for the 4-bit weights, an estimate to verify,
  plus room for the context).

The stretch tier never lowers what the reference tier delivers.

## What it must be able to do

Domains:

- **Apache Kafka:** brokers, KRaft, clients, topics, consumer groups, replication, security,
  quotas.
- **Kafka Streams and Kafka Connect**, and Schema Registry.
- **Strimzi:** custom resources, operators, upgrades, rebalancing, MirrorMaker 2.
- **Kubernetes:** workloads, scheduling, storage, networking, access control, resources.
- **GitOps with Argo CD:** applications, sync, rollback, secrets, Helm and Kustomize as the
  rendering layer.

Code is written and reviewed in **Java and Python**, plus the configuration languages of
the domains (YAML manifests and custom resources, properties files, shell, PromQL).

| Task | The user brings | The assistant returns |
|---|---|---|
| **Coding** | A requirement or existing code | Client code (producer, consumer, Streams topology), connector configuration, manifests, custom resources, Argo CD applications, shell and query one-liners; or a review of what was given |
| **Configuring** | A requirement, or a configuration to check | A minimal correct configuration with every non-default setting explained; or what is wrong or risky in the one given |
| **Diagnostics** | Symptoms, and access to the cluster through tools | Ranked hypotheses, the evidence it gathered or the read-only commands that tell them apart |
| **Debugging** | Logs, errors, `describe` output, stack traces, pasted or read with tools | The cause, the fix, how to verify it, how to roll it back |
| **Streaming architecture** | A use case and its numbers | A design (topics, keys, partitions, delivery guarantees, retention, schema evolution, scaling, multi-cluster) with the trade-offs, and which missing numbers the answer depends on |

## Tools, MCP and skills

The assistant gathers evidence and acts; it does not only advise.

- **Tools.** It reads (list, get, describe, logs, events, status, metrics) and it may make
  changes (apply, patch, restart, sync, commit), through `kubectl`, the Kafka command-line
  tools, the Argo CD client and Git.
- **MCP.** The runtime is a Model Context Protocol client. The user connects MCP servers
  (for example for Kubernetes, Kafka, Git or metrics) and the assistant uses their tools
  under the same rules as its own.
- **Skills.** A skill is a packaged procedure (instructions, checks, scripts) for a
  recurring job, such as a rolling upgrade or a consumer-lag investigation. Skills are
  local files, loaded when the task calls for them. The project ships a starting set and
  the user adds their own.
- **Approval once per session.** A session has an allow-list of commands and tools. The
  user approves it once; the assistant then works inside it without asking again and never
  outside it.
- **The default allow-list is read-only.** The user adds state-changing commands for a
  session when they want the assistant to make changes.
- **Destructive commands are confirmed each time** (delete, offset reset, forced
  operations), even when they are on the allow-list.
- **MCP servers run on the same machine by default.** A remote server receives what the
  assistant sends it, so it is off by default, enabled only by the user's explicit choice,
  and named as remote whenever it is used.
- **GitOps first.** Where Argo CD manages a resource, a change is a change in Git (a commit
  or a pull request), not a direct edit of the cluster.
- **The user's own access.** Tools run locally with the user's current context and
  credentials. The assistant gets no access of its own.
- **Visible.** Every command and tool call is shown, and the answer says which outputs it
  rests on.
- **Private.** Tool output stays on the machine like everything else. Secret values are not
  read unless the user asks for it.
- **Optional.** Everything works without tools, from what the user pastes.

## How it must behave

These matter as much as being right on average. An answer an operator pastes into a cluster
is judged by its worst line.

1. **Nothing invented.** Every command, flag, configuration key, resource field, annotation,
   metric and API path exists in the version it speaks for.
2. **Grounded and sourced.** Answers rest on the pinned documentation in the index and on
   what the tools returned, and can show which pages and outputs they used.
3. **Honest about gaps.** When the documentation it has does not cover the question it says
   so, and it does not decline when the documentation does cover it.
4. **Version-aware.** It states the versions it speaks for and flags behaviour that changed.
5. **Safe by default.** Read-only diagnosis before any change; disruptive or destructive
   steps (offset resets, deletions, forced operations) are marked; a change comes with a
   verification and a way back, whether the engineer or the assistant carries it out.
6. **Complete enough to act on.** The essential step is never the one left out.
7. **Short.** As long as the task needs and no longer.
8. **Private.** It talks only to a model endpoint on the same machine and makes no other
   network call at answer time.

## How we will know

Each bar is measured on the reference laptop configuration, on questions that were not used
to build or tune the assistant.

| Property | Measure | Bar | Status |
|---|---|---|---|
| Nothing invented | A reviewer's reading of answers: invented or wrong items | At most 2 per 10 answers, none that would do damage | agreed |
| Fast enough | Time to a complete answer | Median under 60 s | agreed |
| Fits | Memory held, network off | The table above | agreed |
| Helps | Judged score against the same model without the index, per task type | A gain beyond noise on every task type | agreed |
| Costs little to shrink | The same evaluation for the shipped quantisation and for the full-precision 27B, same index, same serving path | Loss inside a margin fixed before the run (5 points on the mechanical tier of the M4 evaluation), or a stated and accepted loss | agreed; wording amended 2026-11-18 |
| Artefacts are valid | Generated manifests and resources checked against pinned schemas; configuration keys against the real key lists; code that compiles | 95 % valid resources, 100 % existing keys | agreed |
| Honest about gaps | Wrong refusals where the excerpts cover the question | At most 1 in 10 | agreed |
| Complete | Answers that drop an essential, supported step | At most 1 in 10 | agreed |
| Diagnoses and fixes with tools | Faults injected into a local test cluster, with tools, MCP servers and skills: cause found, fix correct | To be set after a first measurement | agreed |
| Tools stay inside the approval | Commands and tool calls issued during those runs | None outside the session's allow-list, ever | proposed (replaces the read-only bar) |
| Private | Network traffic during use | None beyond the local endpoint | agreed |

Rules that already hold in this project apply here too: bars are fixed before the run, every
number is given with its size, and a difference under the noise level is reported as no
difference.

## What we know so far (2026-11-17)

- **Retrieval is what helps.** The base model with the documentation index scores +1.48
  over the plain base model (40 real questions, two judge passes, standard error 0.21).
- **Fine-tuning alone did not help.** The v1 and v2 adapters without retrieval score about
  zero against the base model (+0.08 and -0.18).
- **An adapter trained on retrieved excerpts was stopped.** A stronger teacher model beat
  base + retrieval by only +0.24, under the +0.3 bar fixed in advance.
- **The invention bar is not met in the default mode.** A reading of ten answers found 17
  invented or wrong items in the default mode and 1 in the strict mode (one reader).
- **The speed bar is not met yet.** The one end-to-end measurement took 112, 151 and 170
  seconds per answer (3 questions, default mode, full-precision 27B on a 512 GB machine).
- **The evidence is narrow.** It covers question answering on operations and configuration.
  There is no evaluation yet for coding, for debugging from logs, for architecture or for
  tool use, and none on a laptop-sized model.
- **The runtime has no tools yet.** It answers from the index only: no tool calling, no MCP
  client and no skills exist today, and the model's tool calling has not been measured.

## From here to the goal

In order. Each step is a question to answer by measurement, not a commitment to a method.

1. **Ship v3.0.0:** the retrieval runtime and the documentation index on the base model.
2. **Laptop fit:** the same retrieval with a 4-bit 27B served by oMLX, and with GGUF builds,
   measured for quality, memory and speed on the reference laptop. Because intelligence
   comes first, the question is how much quality the shrink costs, before how fast it is.
3. **Evaluation for the whole goal:** fresh question sets per task type, with mechanical
   checks where they exist (schema validation, key lists, code that compiles) and a
   reviewer's reading where they do not.
4. **Fewer invented items and shorter answers:** the default mode decision, a mechanical
   check of identifiers in an answer against the index and schemas, sources shown with
   every answer. Shorter answers also serve the 60-second bar.
5. **Coverage for the new tasks:** Java and Python client material for coding, Helm and
   Kustomize, and more failure material for debugging. Every source has its licence
   checked before it enters the index.
6. **Tools, MCP and skills:** first measure how reliably the model calls tools; then the
   tool set and the session allow-list, the MCP client, a starting set of skills, a local
   test cluster with injected faults to measure them, and proof that nothing leaves the
   machine. Read-only tools come before tools that change things.
7. **Fine-tuning, only where measurement shows a gap retrieval cannot close.** The likely
   places are a smaller or quantised model that uses excerpts less well than the 27B does,
   and tool use. That is a hypothesis, not a result.

## Not the goal

- A general chat or coding assistant outside these domains.
- A hosted service, or anything that sends the user's data off the machine.
- An unattended agent. The assistant acts only inside a session the engineer started and
  within what they approved for it.
- Managed Kafka services and vendor distributions. The scope is Apache Kafka and Strimzi.
- GitOps tools other than Argo CD, and stream processors other than Kafka Streams and
  Kafka Connect (for example Flux and Flink).
- Client languages other than Java and Python.
- A replacement for the documentation, or for testing a change before production.
- Training a base model.

## Open decisions (the Owner's)

1. **The bar for work with tools,** after a first measurement.
2. **The allow-list bar** ("no command or tool call outside the session's allow-list,
   ever"), which replaced the earlier read-only bar and is still marked proposed.

## How this file relates to the others

`GOAL.md` says where the project is going and how success is judged; it changes rarely and
only by the Owner's decision. `AGENTS.md` holds the working rules. `agents/` holds one brief
per version, each a step toward this goal. `runs/v3/STATE.md` says where things stand today.
