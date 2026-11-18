# M4, work package 1: a new evaluation

**Status: design by the conductor, revised 2026-11-18 after review (`runs/m4/reports/evaluation_design_review.md`). Items marked "waits for the Owner" are not yet agreed.**
Tracking: `MILESTONE.md`. Goal and bars: `GOAL.md`.

## Why

The 40 real questions have been used for every decision since v1, including tuning of the
index. Their standard error is about 0.2, so they cannot show an improvement smaller than
about 0.4, and part of the last headline figure is fitted to them. They cover operations
and configuration only. Every later step of M4 (answer checker, speed, another base model,
4-bit against full precision) has to be judged on all five task types of `GOAL.md`, with
the smallest visible difference of each tier stated (see "Statistics"), and without a judge
model's noise where that can be avoided.

## Design in one paragraph

Three tiers. **Tier A** is large and scored by a program, with no judge: tasks whose
answers can be checked against schemas and catalogues built from the pinned sources.
**Tier B** is smaller and scored by a judge against a written list of required points,
then partly read. **Tier C** is a live cluster with injected faults and belongs to the
tools milestone; it is only specified here. Each tier has a hidden part that nobody reads
during development. The old 40 questions are kept as a legacy set for continuity; that
they are no longer the gate set waits for the Owner (see "GOAL.md bars: tier and metric").

## Tier A: mechanical (target 300 or more items)

An item is a question plus a machine-checkable expectation. No reference prose.

| Kind | Example | Check |
|---|---|---|
| Write a resource | "Write a KafkaTopic with 12 partitions, 3 replicas, 7 days retention, compaction off." | The answer contains one YAML document that validates against the pinned schema; listed fields have the listed values |
| Write a resource with a trap | "…for a KRaft cluster using node pools" | As above, plus fields that must be absent |
| Name the setting | "Which broker setting limits the time a follower may lag before leaving the ISR, and its default?" | The answer names the catalogue key; the default matches the catalogue |
| Is this real | "Does `kafka-topics.sh` have an `--unparsed` flag?" (half of these are real, half are not) | Three-way answer (exists / does not exist / not in my sources) matches the catalogue; false "exists" and false "does not exist" are reported separately. At most 10 % of tier A |
| Fix the manifest | A manifest with one schema error or one wrong field | The corrected document validates and the named field is changed |
| Write the command | "Describe consumer group `orders` with members and lag." | Tool and flags exist in the catalogue; required flags present; forbidden ones absent |
| Write the client code | "A Java consumer with manual commit after processing." / the same in Python | It compiles (Java) or passes a syntax and import check (Python); listed API calls present; listed config keys exist |

Five further kinds:

- **Version trap:** a default or API that changed.
- **Valid but wrong:** it validates but does nothing or the wrong thing, such as a
  NetworkPolicy with a podSelector only for clients in another namespace,
  `strimzi.io/restart` without `includeTasks`, a setting equal to its default, or `#`
  comments inside `.properties` lines.
- **Safe order:** a dry run or `--describe` before `--execute`.
- **Computed sizing:** a number to get within a tolerance.
- **Not covered:** an honest gap.

Construction rules:

- Ground truth comes from the catalogues or from source passages, never from a model's
  reference alone. Each expected fact cites a pinned file and line; facts without a local
  source are marked unanchored and are not counted in decisions.
- Items are generated from the catalogues and the pinned documentation by templates and by
  a model that is neither the answering model nor the judge.
- An item whose model-written reference fails goes to repair, not to the bin. Drop and
  repair rates are reported per kind.
- No item is kept or dropped because of how any candidate configuration does on it.
- At most 10 items per template, and a quota per failure class (version, near-miss name,
  valid but wrong, no-op default, condition or negation, real but not documented, needs two
  pages, not covered).
- For each item the overlap of words with its seed chunk is recorded, along with the seed
  chunk's retrieval rank. Results are reported for the two groups (seed retrieved or not).
- Every item records its source (catalogue entry or document), the template, and the
  versions it is true for.

Catalogue rules:

- Every catalogue entry records its provenance: *exists* (source code, `--help` output or a
  schema), *documented* (in the index) or both. Each catalogue is marked complete or
  incomplete per tool or resource. "Not real" items and "nonexistent identifier" penalties
  come only from catalogues marked complete; elsewhere a miss is *unknown*.
- Schema validation rejects unknown fields; the API server's silent pruning is not a pass.
  Free-form maps (`spec.kafka.config`, `spec.config` and the like) are checked against the
  key catalogue and against Strimzi's forbidden prefixes.
- Catalogues are frozen and hashed like the index.
- Every download and install needed for catalogues and checks waits for the Owner
  (recommendation: yes): Kafka 4.3.2 binary or tool source (flags), Strimzi 1.3.1 CRDs,
  Kubernetes OpenAPI at a pinned version, Argo CD 3.6.1 CRDs, earlier Kafka config pages,
  the kafka-clients jar, and `jsonschema` and a Python Kafka client.

Scoring rules:

- Scoring is pass or fail per check, reported per kind and per domain, with intervals as
  in "Statistics". The main score is "all checks pass"; partial credit (the share of checks
  passed) is secondary.
- Every YAML document of the requested kind must pass.
- At least one command line must meet the specification, and no line may use a
  nonexistent flag of a tool whose catalogue is complete.
- The "name the setting" and "is this real" kinds end with a required `ANSWER:` line, and
  missing lines are counted as format failures.
- "Is this real" items must be balanced, so that answering "exists" to everything scores
  50 % on them.
- The tier A scorer and the runtime answer checker (WP2b) share only the frozen, hashed
  catalogues, not the code that extracts and decides. The scorer runs checks the checker
  does not: strict schema validation, value checks and meaning checks. E3 is measured by a
  reading, as `GOAL.md` requires ("A reviewer's reading"). Scorer counts are reported
  beside the reading, split into errors the checker can see and errors it cannot. A checker
  change is never accepted on the scorer's numbers alone.

## Tier B: judged (target 120 or more items)

For diagnostics, debugging from pasted evidence, and streaming architecture, where a
program cannot decide. An item is a scenario, optional pasted evidence (log lines,
`describe` output, a stack trace), and a rubric:

- **must** points (3 to 6): facts or steps a correct answer contains;
- **must not** points (0 to 3): statements that would be wrong or harmful here;
- a short reference answer, for the reader, not for the score.

Answers are scored alone, not head to head. Each must point carries a weight: 2 if it is
essential, otherwise 1. Each must-not point has a severity: *wrong* or *harmful*. Per item:

- P = (sum of the weights of must points marked present) / (sum of all must weights);
- W = (must-not points of severity *wrong* that are present) + (must points marked
  *contradicted*);
- H = must-not points of severity *harmful* that are present;
- **S = max(-1, P - 0.25·W - 0.5·H).**

The configuration score is the mean of S, with each item's two passes averaged first. Also
report:

- the essential-miss rate (the GOAL "Complete" bar);
- the harmful rate ("none that would do damage");
- the judge's other-errors count (outside S, sample-read);
- the nonexistent identifiers the scorer finds, per answer.

Decisions are also reported with the W penalty at 0 and at 0.5; a decision that flips
between these is reported as depending on the penalty.

The judge sees the question, any pasted evidence, the rubric and the item's source
passages. It does not see the reference answer. One call is made per answer. Pass 2 lists
the points in reverse order. A verdict whose quote is not found verbatim in the answer
counts as absent and is logged as a judge error.

The judge instruction (exact text):

```
You check one answer from an assistant for Kafka, Strimzi, Kubernetes and Argo CD operators against a list of points. Judge each point on its own. Do not grade style, length or tone.
For each MUST point, the verdict is "present" only if the answer states the point explicitly and specifically enough that an engineer following the answer would act on it correctly. A setting, flag, field, annotation, metric, API path or HTTP method counts only if it is named as in the point. A vague mention ("check the configuration", "look at the logs") is "absent". If the answer states the point and contradicts it elsewhere, the verdict is "contradicted". If the point says it must be the main or first recommendation and the answer gives it only as one option among several, the verdict is "absent".
For each MUST-NOT point, the verdict is "present" if the answer states or recommends it, even in passing or as an alternative, and "absent" otherwise. A statement the answer explicitly rejects is "absent".
For every "present" or "contradicted" verdict, quote the shortest passage of the answer that shows it. If you cannot quote it, the verdict is "absent".
Then list any other statement in the answer that contradicts the source passages below, with a quote. Do not list statements that are only missing from the sources.
Reply only with JSON: {"must":[{"id":"","verdict":"present|absent|contradicted","quote":""}],"must_not":[{"id":"","verdict":"present|absent","quote":""}],"other_errors":[{"quote":"","why":""}]}
```

### Streaming architecture items

Each item states its numbers or deliberately leaves one out. The rubric then needs:

- "names the missing numbers the design depends on" (a `GOAL.md` requirement);
- one or two computed points (partitions, storage, consumer count) scored mechanically
  within a tolerance;
- "any of" groups for designs that are equally valid.

Architecture rubrics need the Owner's review; with no Owner review, the 25-item quota
drops to 15.

### Sources of scenarios, in order of trust

1. The Owner's real questions with correct answers, as they arrive.
2. Scenarios written from the held-out runbooks and from pinned documentation by a model
   that is neither answerer nor judge, each rubric checked against the documents by a
   reviewer on a Claude model.
3. Public questions (Stack Overflow, issue trackers), only if the Owner allows it and a
   researcher has cleared the licence. These licence questions wait for the Owner; they are
   questions for the researcher and the Owner, not conclusions:
   - Stack Overflow (CC BY-SA 4.0) and issue-tracker text as item material;
   - publishing an open eval set derived from Kubernetes docs (CC BY 4.0);
   - any use of `raw/confluent-schema-registry`.

The 42 holdout questions stay a legacy set and do not count toward the tier B quota: they
are lookups, and base + retrieval already scores about 4.4 to 4.5 of 5 on them. The v3
bank may supply a topic only if that bank question and its near duplicates are moved to
`work/v3/eval_exclusions.jsonl` and never used for training. All new items are
deduplicated by near-duplicate matching, not exact text, against:

- `eval/*.jsonl`;
- `work/v3/questions*.jsonl`;
- `data/{train,valid,test}.jsonl`;
- the pilot prompts.

Task-type quota for the 120: diagnostics 40, debugging from evidence 40, architecture 25,
configuration reasoning 15. Coding is covered by tier A.

### Rubric pilot

Tier B rubrics wait for its result. Use the labelled answers of the three v3 readings: 10
real questions times 4 answer sets, plus 20 holdout questions times 2 sets, about 80
answers on 30 questions. First check the reading labels that rest on memory against the
sources. Then a writer who has not seen the answers or the readings writes one rubric per
question. The judge scores every answer twice with the instruction above. Go only if all
four hold:

- per-point agreement between passes has Cohen's kappa of at least 0.7;
- the item score's pass-to-pass reliability (ICC) is higher than that of the 1 to 5 mark
  on the same answers (from the existing judge caches);
- at least 80 % of the reader's essential omissions are marked absent;
- at most 5 % of quotes fail to be found in the answer.

This leans on `questions_real` and the holdout again. It calibrates the judging method; it
is not a claim about the system.

## Tier C: live (specified only)

A local cluster with Strimzi and one injected fault per case (a full disk, a wrong
listener, a missing secret, a crash-looping pod, a stuck rebalance, an out-of-sync
application). The assistant has read-only tools. Scored on cause found and fix correct.
Built in the tools milestone; nothing is installed for it in M4.

## The hidden part

Split by family, not by item: all items from one template, one source page or one scenario
go to the same side.

- Hidden share 50 % in both tiers, fixed by seed at build time. Waits for the Owner
  (recommendation: 50 %).
- The hidden part is used for exit criteria and for decisions (base model, release); the
  open part is for development. Deciding on the hidden part waits for the Owner
  (recommendation: decide on the hidden part).
- Hidden items, answers and per-item results are stored encrypted. The key lives in
  `~/.config/kafka-sre-llm/`; the harness decrypts in memory. Encrypting the hidden part
  waits for the Owner (recommendation: yes).
- For hidden items, logs and progress lines show item ids only.
- Every hidden run is written in `runs/m4/HIDDEN_LEDGER.md`, together with the decision
  rule written before the run. The budget is one hidden run per decision listed in
  `MILESTONE.md` (E2, E3, E5, release), at most 6 in M4. When the budget is spent, a new
  hidden part is built.
- Tier B rubrics on the hidden side are checked by a separate reviewer session that
  returns pass or fail per item id only.

## Rules against fitting

Any change to the index, a catalogue, a prompt or the checker that was prompted by
particular items lists those item ids. Its headline figure excludes them and comes from the
hidden part. The open and hidden results are reported side by side; a gap larger than 2
standard errors marks the change as fitted. Every reported configuration has a complete
answer set and a manifest: model build, server, concurrency, mode, index hash, catalogue
hash, runtime commit.

## Reference configuration

Unless the Owner decides otherwise: the 4-bit base model served by the local model server,
frozen index, default and strict mode.

The cost of 4-bit is measured with both builds on the same server path. The primary run is
end to end. A second run gives both builds the same stored excerpts ("pack replay"), to
separate the cost in search terms from the cost in writing the answer. The in-process path
is a continuity check only, until D1 explains the difference between paths. Loading the
full-precision and 4-bit 27B builds on the model server for this comparison waits for the
Owner (recommendation: go).

## Statistics

Smallest visible difference (paired, two-sided α 0.05, 80 % power): MDE = 2.80 · SD_d / √n.

- SD_d is measured, by tier, as the standard deviation of per-item paired differences from
  an A/A test: the reference configuration answered twice end to end, and on tier B judged
  twice.
- Confidence intervals come from a paired bootstrap clustered by item family. The report
  gives the effective item count.
- A no-loss claim (4-bit, speed changes) needs the 90 % interval to lie inside a margin
  fixed before the run: 5 percentage points on tier A all-pass rate, 0.10 on tier B S.
  "Within noise" is not enough. The `GOAL.md` wording on the cost of 4-bit is amended to
  this margin; that waits for the Owner (see "GOAL.md bars: tier and metric").
- Results per task type or per kind are descriptive unless their own MDE is stated.

## What each tier can show

Smallest visible difference at the sizes of this design (z sum = 1.96 + 0.84 = 2.80).

**Tier B**, in units of the 1 to 5 scale equivalent, for SD_d of the paired item
differences of 1.3 and 1.07 (measured at 1.05 to 1.34 on the real questions and 0.94 to
1.01 on the holdout), and in units of SD_d:

| n | SD_d 1.3 | SD_d 1.07 | in SD_d units |
|---|---|---|---|
| 120 (all) | 0.33 | 0.27 | 0.26 |
| 84 (70 % open) | 0.40 | 0.33 | 0.31 |
| 36 (30 % hidden) | 0.61 | 0.50 | 0.47 |
| 60 (50 % hidden) | 0.47 | 0.39 | 0.36 |
| 200 | 0.26 | 0.21 | 0.20 |

**Tier A**, binary all-pass: MDE ≈ 2.80 · √(p_d / n), where p_d is the share of items on
which the two configurations disagree. The real p_d is unknown until the A/A test.

| n | p_d 0.10 | p_d 0.15 | p_d 0.25 |
|---|---|---|---|
| 300 | 5.1 pp | 6.3 pp | 8.1 pp |
| 210 (70 % open) | 6.1 | 7.5 | 9.7 |
| 90 (30 % hidden) | 9.3 | 11.4 | 14.8 |
| 150 (50 % hidden) | 7.2 | 8.9 | 11.4 |

Consequence: tier B supports decisions of about 0.4 and above on the 1 to 5 scale
equivalent. The fine decisions (the cost of 4-bit, the answer checker) rest on tier A and
on readings.

## Noise

Before any comparison is believed:

- Run the reference configuration twice on tier A. Decoding is greedy, so differences
  between the two runs measure the server's own non-repeatability; the discordance is a
  floor for p_d and is measured at the concurrency used in evaluation runs.
- Judge tier B twice. Report per-point agreement between passes and the standard error of
  the item score.
- Have a reviewer read 20 judged items and mark each point; report agreement with the
  judge. The Owner reads 10 of those (item D2 of `MILESTONE.md`).
- State the smallest difference each tier can show, from the measured SD_d and p_d (see
  "Statistics").

## Harness

One entry point, `scripts/eval2.py`, with steps that resume from their output files and
print a line per item (item ids only for hidden items): `answer` (through the runtime, any
backend), `score` (tier A), `judge` (tier B), `report` (totals with intervals, hidden part
as totals only). Long steps run under `scripts/run_guarded.py`. It also records seconds per
stage and tokens, so every evaluation run is a speed measurement too. It logs the excerpts
of every answer and can replay stored packs.

Run policy, three run sizes:

- smoke: 60 tier A and 24 tier B items from the open part, stratified, fixed; for every
  change; it shows breakage only;
- open full: per candidate;
- hidden: per decision only.

Speed figures come only from runs with one request at a time and no other client on the
server. A change that should not alter answers is accepted by byte-identical answers on the
smoke set, not by scores. All answering finishes before the judge is loaded.

## Cost of a run

Measured rates on the 4-bit oMLX path, one request at a time, on the Mac Studio (10
questions per mode): median 70.8 s default, 47.2 s strict. bf16 on the server: 134 to 212
s (3 questions). Judge: the brief's figure of 1.5 minutes per item with three in parallel;
v3's 1,290 judge calls give a median of 160 s per head-to-head call and a 90th percentile
of 441 s.

| Run | Answers | Judge (2 passes) | Total |
|---|---|---|---|
| Tier A 300, 4-bit default | 5.9 h (strict 3.9 h) | none | 5.9 h |
| Tier B 120, 4-bit default | 2.4 h (strict 1.6 h) | 4 to 6 h | 6.4 to 8.4 h |
| One configuration, one mode, both tiers | 8.3 h | 4 to 6 h | 12 to 14 h |
| E2: plain base, bf16 + retrieval, 4-bit default, 4-bit strict, A/A repeat | about 44 to 54 h | 20 to 30 h | **about 64 to 84 h** of exclusive machine time |
| Smoke (60 A + 24 B, one judge pass) | 1.65 h | 0.6 h | **about 2.3 h** |

Run sizes (see "Harness"):

| Size | Items | Cost |
|---|---|---|
| Smoke | 60 tier A and 24 tier B from the open part | about 2.3 h; shows only breakage (about 14 pp on tier A at p_d 0.15, about 0.74 on tier B) |
| Open full | the open part, per candidate | the rows above at the size of the open part |
| Hidden | the hidden part, per decision only | the rows above at the size of the hidden part |

E2 is affordable once. A full run is not affordable for every candidate change. Tier A
answers may be shorter than the real-question medians; that was not measured.

## GOAL.md bars: tier and metric

| GOAL bar | Tier and metric |
|---|---|
| 100 % existing keys | Tier A: identifier precision over all generated answers, against complete catalogues |
| 95 % valid resources | Tier A: all-pass share on the resource kinds |
| Nothing invented | A reading; the scorer's nonexistent identifiers per answer are reported beside it |
| Complete | Tier B: essential-miss rate |
| None that would do damage | Tier B: harmful rate |
| Honest about gaps | Wrong refusals, counted with the logged excerpts and the item's covering chunk ids |

The design makes `questions_real` a legacy set and not the gate set. That contradicts
`AGENTS.md` ("`questions_real` is the gate set"), so it needs the Owner's amendment, not a
silent change. Amending `AGENTS.md` that way, and amending the `GOAL.md` wording on the
cost of 4-bit to a non-inferiority margin fixed before the run, wait for the Owner
(recommendation: amend both). Until then `AGENTS.md` stands.

## First slice and build order

Build the harness skeleton, with excerpt logging and pack replay, together with the first
slice. Run the rubric pilot and the A/A test next. The catalogues of work package 1a are
already started. The slice needs no downloads, and its truth is programmatic.

| Task | Role | Files | Inputs → outputs | Acceptance | Needs | Time |
|---|---|---|---|---|---|---|
| S1 Harness skeleton | implementer | `scripts/eval2.py` (new), `tests/test_eval2.py` | config → answers with pack ids, timing, manifest; pack replay; hidden ids only | offline tests pass; replay reproduces stored packs on 3 legacy questions; golden tests still pass | CPU | 1 to 1.5 days |
| S2 Config, REST and Argo CD items | implementer | `scripts/eval2_items_a1.py`, `eval/m4/` | generated config HTML, `connect_rest.yaml`, argocd command pages → about 200 items, grouped 50/50 split | every reference passes, a perturbed one fails; near-duplicate check against all earlier sets clean | CPU | 1 day |
| S3 Scorer for these kinds plus identifier precision | implementer | `scripts/eval2_score.py` | answers → per-check results | hand-written pass and fail answers behave as expected; `--cancel`-type misses reported as unknown | CPU | 1 day |
| S4 Rubric pilot rubrics and label check | complex-reviewer, plus researcher for source checks | report only | 30 questions, sources → rubrics with file and line | every point anchored to a source | Claude, CPU | half a day |
| S5 Judge pilot | job-operator | — | 80 answers × 2 passes | rubric pilot thresholds computed | model server (GLM oQ8e) | 3 to 4 h |
| S6 A/A and 4-bit against bf16, same server | job-operator | — | about 200 items: 4-bit twice, bf16 once, plus pack replay | p_d and the non-inferiority interval reported | model server, 27B builds (loading them waits for the Owner) | 15 to 20 h |

After the first slice: the remaining tier A kinds and the tier B items are built only after
the rubric pilot and the A/A test have reported. Then the starting-point measurement
(exit criterion E2).

## Acceptance of this work package

- 300 or more tier A items and 120 or more tier B items, each with its source recorded.
- Every tier A item's reference solution passes its own checks; a deliberately wrong
  solution fails them.
- The noise section is filled with measured values.
- No item of the new sets appears in any prompt used to generate another item's
  reference, and none is among the old 40.
- The old 40 and the 42 holdout questions still run through the new harness, for
  continuity with v3.

## Review

The review is `runs/m4/reports/evaluation_design_review.md` (verdict: build with these
changes). All 15 of its changes are in the text above; it holds the reasoning for each.
Its statistics workings, cost workings and the list of what the reviewer could not verify
are there too.
