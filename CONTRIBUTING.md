# Contributing

Thank you for looking at kafka-sre-llm. This is a Kafka / Strimzi / Kubernetes / Argo CD SRE
assistant that answers from retrieved documentation.

## In brief

- Most welcome: runbooks that were proven in real operation, and remediations of real incidents.
- Open an issue or a discussion first, say what you want to add, and wait for a go.
- Then fork, work on a branch of your fork, and open a pull request against `main`.
- Scrub everything: no host names, IP addresses, account or cluster names, credentials or
  customer names. This is a public repository.
- Small fixes (a typo, a wrong command in a runbook) may go straight to a pull request.

## What is most welcome

Runbooks that you or your team proved in a real environment, and write-ups of real incident
remediations, for Apache Kafka (KRaft), Strimzi, Kubernetes and Argo CD. For an incident:
what the symptoms were, what was wrong, what fixed it, and what made it worse.

Why this matters here: the assistant answers from retrieved documentation. The project's
measurements show that retrieval is what helps (see "Status" in `README.md`), so a good runbook
improves answers directly. Real incidents also become evaluation scenarios.

## How to contribute

1. Open an [issue](https://github.com/aywengo/kafka-sre-llm/issues) or start a
   [discussion](https://github.com/aywengo/kafka-sre-llm/discussions). Describe what you want
   to add. Wait for a go.
2. Fork the repository and make your changes on a branch of your fork.
3. Open a pull request against `main`.

The maintainer named in `CODEOWNERS` reviews every pull request. Small fixes may go straight
to a pull request without the first step.

## What a runbook contribution looks like

Runbooks live in `data/runbooks/runbooks/<area>/` (today `kafka`, `kubernetes` and `strimzi`).
Read two of them first, for example `kafka/kraft-quorum-debugging.md` and
`strimzi/kafkarebalance-tracking-and-stopping.md`, and `data/runbooks/ATTRIBUTION.md`.

Layout:

- A title, then a header table with the rows Product, Doc version, Sources, License and
  Retrieved.
- The sections Symptoms / trigger, Diagnosis steps, Remediation steps, Verification,
  Rollback or caveats, and Related runbooks.

Requirements:

- **Versions.** State the versions the runbook was proven on. The project pins Kafka 4.3
  (docs source tag 4.3.2), Strimzi 1.3.1 and Argo CD 3.6.1; the Kubernetes documentation is
  pinned to a commit (see `sources.yaml`). Where your versions differ, say so.
- **Checked commands.** Check every command and setting against the upstream documentation
  and link the page. Where the documentation does not cover a step, say so.
- **Your own words.** Do not copy vendor text. The License row names the sources you used and
  their licences.
- **Proven.** It means you or your team ran it in a real environment. Say in one line what
  kind of environment it was (for example "three-broker test cluster"), without naming it.

## Incident remediations

A short write-up is enough to start, in the issue or discussion:

- the symptoms;
- the evidence (logs or command output, shortened);
- the root cause;
- the fix;
- what you tried that did not help or made things worse;
- the versions involved.

The maintainers help turn it into a runbook or an evaluation scenario.

## Scrub before you post

Remove or replace with neutral placeholders: host names, IP addresses, account and cluster
names, credentials, tokens, customer and company names, and e-mail addresses. Do not post
anything covered by an NDA. Logs and command output need the same check.

## What not to change, and what happens after a merge

The documentation index, the measured prompts and the evaluation sets are frozen (see "Things
that are frozen" in `AGENTS.md`). In a runbook pull request:

- do not rebuild the index;
- do not change `kafka_sre_rag/prompts.py`, anything in `tests/golden/`, or anything under
  `eval/`.

An accepted runbook enters the index at the maintainers' next index build. Some runbooks are
held out for evaluation (`data/runbooks/holdout.txt`).

## Code contributions

Code changes are also welcome, after an issue. Set up the environment as in `README.md`
("Install"): a virtual environment in `.venv` and `pip install -r requirements.txt`. Then run:

```bash
.venv/bin/python -m unittest discover -s tests
.venv/bin/python -m kafka_sre_rag verify
```

The unit tests are offline. `verify` needs the documentation index; see "Get the index" in
`README.md`. Keep changes small.

## Licence

The repository is under the Apache License 2.0 (`LICENSE`). Contributions are accepted under
it (section 5 of the licence). A runbook also carries the licences of the sources it is
derived from. There is no CLA and no sign-off requirement.
