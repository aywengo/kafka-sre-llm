# M4, work package 1: a new evaluation

**Status: design by the conductor, revised 2026-11-18 after review and 2026-11-19 after the first judging pilot failed (`runs/m4/reports/s5_pilot_analysis.md`), and again on 2026-11-19 after the second pilot failed (`runs/m4/reports/s5b_pilot2_analysis.md`; see "Revisions"). The Owner's decisions of both days are in `MILESTONE.md`.**
Items marked "waits for the Owner" are not yet agreed.
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
**Tier B** is smaller; a judge reports how many of a written list of required points an
answer covers, and blind readings with the sources give the rates of wrong, invented and
harmful statements. **Tier C** is a live cluster with injected faults and belongs to the
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
| Is this real | "Does `kafka-topics.sh` have an `--unparsed` flag?" (half of these are real, half are not) | Three-way answer (exists / does not exist / not in my sources) matches the catalogue; false "exists" and false "does not exist" are reported separately. An answer "not in my sources" on an item whose expected answer is "does not exist" (an invented name) still fails the all-pass check and is also reported as an abstention on an invented name (see "Decline outcomes"). At most 10 % of tier A |
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
  change is never accepted on the scorer's numbers alone. Since 2026-11-19 (J5) the scorer
  also reads the frozen index, read-only and with its hash checked against
  `runs/v3/INDEX_FROZEN.json`, to find which chunks cover an item's fact. That is new: the
  scorer and the runtime's checker still share only the catalogues, and nothing else.

Decline outcomes (J5, decided 2026-11-19). On the open half, strict mode scored 174 of 251
against 194 of 251 for default, 8.0 points lower, and the scorer has no outcome for a
decline, so a correct decline scores like an invented answer. Of the 26 items default
passed and strict failed, 19 were declines where the excerpts lacked the fact (10 in the
index but not retrieved, 6 not in the index, 3 abstentions on an invented name) and 7 were
strict faults; crediting the 19 would leave a gap of 0.4 points (`runs/m4/reports/s6_strict_loss_analysis.md`,
one reader, all 26 read).

- The scorer reports, per item and per kind, one of four outcomes beside the all-pass
  rate: **pass**; **correct decline** (the answer declines and the item's covering chunk
  ids are not in the pack the answer was given); **wrong decline** (it declines and they
  are in the pack); **wrong** (any other failure). The all-pass rate itself is unchanged
  and stays comparable with S6.
- The covering chunk ids of an item come from its recorded source (file and entry) in the
  frozen index.
- For the `is_real` kind, an abstention ("not in my sources") on an item whose expected
  answer is "does not exist" is reported apart, as an abstention on an invented name.
- Items whose fact is not in the index (the Argo CD command pages, which `sources.yaml`
  excludes, and Kafka tool options that exist only in source code) are tagged and reported
  as their own subset.
- A rise in correct declines is read together with the wrong declines, so that declining
  more is not rewarded.

## Tier B: judged (target 120 or more items)

For diagnostics, debugging from pasted evidence, and streaming architecture, where a
program cannot decide. An item is a scenario, optional pasted evidence (log lines,
`describe` output, a stack trace), and a rubric:

- **must** points (3 to 6): facts or steps a correct answer contains;
- **must not** points (0 to 3): statements that would be wrong or harmful here;
- a short reference answer, for the reader, not for the score.

Rubric rules:

- the weights of essential points are set by a second reviewer, against the documented
  procedure, before any answers exist;
- the cause point of a "what is going on" item is marked as the one that must come first;
- must points name an action ("recommends raising X above the processing time"), not just
  a name;
- each must-not states its condition and gives one correct near-miss that must not trigger
  it;
- no must-not may catch a documented, correct alternative;
- the rubric is probed with a generic answer (which must score about 0) and with a "correct
  alternative" answer (which must trigger no must-not);
- the Owner reviews which points are essential on his own questions.

Rules added on 2026-11-19 from the readers' remarks on the second pilot:

- every must point carries the chunk ids of the passages in the index that contain its
  fact, recorded by the rubric author before any answer exists; a second reviewer checks
  them, against the chunks, before answers exist;
- no bundled points: one fact or one step per point;
- alternatives that are equally correct go into an any_of group, not into separate points;
- a point that rests on a source outside the index is allowed but marked as such; it
  tests retrieval coverage, not the answer, and is never "supported" (see below).
- how B1's second reviewers read two of these rules, recorded before any answer existed:
  "3 to 6 must points" counts units, an any_of group once; in the chunk check, a point
  whose fact is an order the sources imply but do not state counts as partial.

Answers are scored alone, not head to head. Each must point carries a weight: 2 if it is
essential, otherwise 1. Each must-not point has a severity: *wrong* or *harmful*. Per item:

- P = (sum of the weights of must points marked present) / (sum of all must weights);
- W = (must-not points of severity *wrong* that are present) + (must points marked
  *contradicted*);
- H = must-not points of severity *harmful* that are present;
- **S = max(-1, P - 0.25·W - 0.5·H).**

The judge's role (J1, decided 2026-11-19): it reports rubric coverage only. The judge's
result for a configuration is:

- coverage P;
- essential-miss units: a weight-2 point with no verdict "present", where an any_of group
  counts once, as one unit that is missed if no member is present;
- supported essential-miss units: essential-miss units that the answer's own excerpts
  could have answered, by the chunk-id rule below;
- must-not statements present.

Each is given per 10 answers with its smallest visible difference. The wrong, invented
and harmful rates do not come from the judge (see "Wrong, invented and harmful: where the
rates come from"). S is secondary; it is the mean over items, with each item's two passes
averaged first, and its W counts only must-not points of severity *wrong* and contradicted
must points. The judge's "other errors" are kept as leads for a reader and are never
counted toward any bar. Also report:

- the judge's other-errors count (leads only, outside S);
- the identifier check's totals (see "Wrong, invented and harmful: where the rates come
  from").

Supported, decided by chunk ids (replaces the file-level rule of the second pilot):

- A must point is *supported* in an answer if one of the chunk ids that the rubric author
  recorded for it is among the answer's logged excerpts. An any_of group is supported if
  any member is. A point that rests on a source outside the index is marked as such and
  is never supported.
- The file-level rule (any chunk of the point's file counts) is dropped: in the second
  pilot it counted 15 flags whose chunk did not contain the fact and missed cases where
  the fact was in a file the rubric did not cite.

Decisions are also reported with the W penalty at 0 and at 0.5; a decision that flips
between these is reported as depending on the penalty.

The judge sees the question, any pasted evidence, the rubric and the item's source
passages. It does not see the reference answer. One call is made per answer. Pass 2 lists
the points in reverse order. The judge also gets the answer's own logged excerpts. A
statement about what the excerpts contain is checked against those excerpts only.

A "present" or "contradicted" verdict whose quote is not found in the answer becomes
"unverified" and is logged as a judge error. An unverified verdict is counted neither as
present nor as a miss; the share of unverified verdicts is reported. A quote is found after removing markdown marks, code-fence tags and list markers and
normalising whitespace; fragments joined by "..." must each be at least 12 characters and
appear in order. Entries of `other_errors` whose `source_quote` is not found in the source
passages or the answer's excerpts are dropped and counted. The entries that remain are
leads for a reader (J1), not findings.

The judge instruction, version 2 (exact text; it is version 1 with two changes and the
`source_quote` field):

```
You check one answer from an assistant for Kafka, Strimzi, Kubernetes and Argo CD operators against a list of points. Judge each point on its own. Do not grade style, length or tone.
For each MUST point, the verdict is "present" only if the answer states the point explicitly and specifically enough that an engineer following the answer would act on it correctly. A setting, flag, field, annotation, metric, API path or HTTP method counts only if it is named as in the point. A vague mention ("check the configuration", "look at the logs") is "absent". A definition of a setting is not a recommendation to change it; a default value is not a change. If the answer states the point and contradicts it elsewhere, the verdict is "contradicted". If the point says it must be the main or first recommendation and the answer gives it only as one option among several, the verdict is "absent".
For each MUST-NOT point, the verdict is "present" only if the answer states or recommends it and the point's "Applies" note, if it has one, holds for what the answer says; where a point has no "Applies" note, a statement made in passing or as an alternative counts. A statement the answer explicitly rejects, or mentions only to warn against, is "absent".
For every "present" or "contradicted" verdict, quote the shortest passage of the answer that shows it. If you cannot quote it, the verdict is "absent".
List another statement only if a sentence in SOURCE PASSAGES or ANSWER EXCERPTS contradicts it. Quote that sentence in "source_quote". Do not list statements that are absent from them.
Reply only with JSON: {"must":[{"id":"","verdict":"present|absent|contradicted","quote":""}],"must_not":[{"id":"","verdict":"present|absent","quote":""}],"other_errors":[{"quote":"","why":"","source_quote":""}]}
```

Version 1, as used in the first pilot (2026-11-18), kept for the record:

```
You check one answer from an assistant for Kafka, Strimzi, Kubernetes and Argo CD operators against a list of points. Judge each point on its own. Do not grade style, length or tone.
For each MUST point, the verdict is "present" only if the answer states the point explicitly and specifically enough that an engineer following the answer would act on it correctly. A setting, flag, field, annotation, metric, API path or HTTP method counts only if it is named as in the point. A vague mention ("check the configuration", "look at the logs") is "absent". If the answer states the point and contradicts it elsewhere, the verdict is "contradicted". If the point says it must be the main or first recommendation and the answer gives it only as one option among several, the verdict is "absent".
For each MUST-NOT point, the verdict is "present" if the answer states or recommends it, even in passing or as an alternative, and "absent" otherwise. A statement the answer explicitly rejects is "absent".
For every "present" or "contradicted" verdict, quote the shortest passage of the answer that shows it. If you cannot quote it, the verdict is "absent".
Then list any other statement in the answer that contradicts the source passages below, with a quote. Do not list statements that are only missing from the sources.
Reply only with JSON: {"must":[{"id":"","verdict":"present|absent|contradicted","quote":""}],"must_not":[{"id":"","verdict":"present|absent","quote":""}],"other_errors":[{"quote":"","why":""}]}
```

### The judge

- **Model and setting:** DeepSeek-V4.1-Flash without thinking, with source passages. On 16
  answers it took 22 seconds a call against 691 for the earlier judge (GLM, thinking), and
  caught more of the labelled errors (`runs/m4/S5A_JUDGE_SETTINGS.md`). The earlier judge's
  thinking cannot be switched off on this server.
- **Cautions:** it wrote the project's earlier training data, so it is never the only judge
  where the v2 adapter's answers are compared. In the first pilot it was harsh on strict and
  documentation-citing answers; the answer's own excerpts are meant to fix that.
- **Cross-check:** the earlier judge model re-judges a fixed 24-item subset on every
  decision run, and the agreement is reported.
- **Role (J1):** coverage only. Instruction version 2 stays as it is, with thinking off.
  In the second pilot the judge caught 58 of the 111 wrong, harmful or invented
  statements the readers found (same statement, at least one pass), and about 18 of the
  53 it missed were contradicted by a sentence it was shown; so it is not an error finder.
  There is no third pilot of the judge as an error finder. Claim-by-claim verification
  (each statement of an answer checked against retrieved passages) is parked as a research
  item; it could only be tested on fresh items, because the 111 findings have been used.
- **Not to be trusted for:** the wrong, invented or harmful rate, or any bar built from
  its other-errors output; ranking configurations by S; made-up identifiers, which belong
  to the identifier check.

### Wrong, invented and harmful: where the rates come from

Two sources, neither of them the judge (J1).

- **The identifier check.** The evaluation's own check of identifiers against the frozen
  catalogues: `scripts/eval2_idcheck.py`. It is separate code from the runtime answer
  checker and shares only the catalogue files with it. It runs on all answers, including
  those of the hidden half, and the harness prints totals only. It must raise no false
  "invented" finding on the 244 stored answers and the 48 pilot answers.
- **Blind readings.** A strong reader (a Claude model) reads answers with the pinned
  sources. The rates of wrong, invented and harmful statements are the reading's, with the
  identifier check's totals beside them.

Reading protocol:

1. The reader is blind to the configuration (mode, model, limit) of every answer.
2. Stage 1, the reader's own findings, before it sees anything from the judge or a second
   reader. Each finding has a kind: *wrong*, *invented*, *harmful* or *omission* (an
   omission names the rubric point id and `supported`, by the rule above; a point counts
   as stated by the same standard as in the judge's instruction, so that both count the
   same thing: explicit and specific enough to act on, named as in the point, first where
   the point says first, not contradicted elsewhere); a `minor` mark
   (true or false); a verbatim quote from the answer; and the source file and lines. A
   statement the sources do not settle is not counted; it is listed as "could not
   verify".
3. Stage 2 only after stage 1 is fixed: comparison with a second reader's findings or with
   the judge's leads.
4. One row per distinct statement.
5. `supported` means that the passage itself is in the answer's excerpts. For an any_of
   group it is true if any member's passage is.
6. A second reader reads about 25 % of the answers, drawn by a fixed seed (J3), with the
   same protocol and without seeing the first reader's findings until stage 2.
7. If the two readers' totals of wrong + invented + harmful on the shared answers differ
   by more than 30 %, the readings are not used for a bar until the protocol is fixed.
8. When two configurations are within what the readers' disagreement allows, the reading
   does not decide between them.

What was measured on the six pilot items both readers read (12 answers,
`runs/m4/reports/s5b_second_reading.md`): errors 27 by the first reader and 33 by the
second, 26 found by both; omissions 18 and 16, 16 by both; `supported` agreed on 13 of 16
shared omissions; the readers agreed on which answer had fewer errors for 5 of 6 items.
The second reader's threshold was lower for small side errors, so a gap of 2 errors or
less between two answers flips with the reader.

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

   Working rule from the licence check (`runs/m4/reports/question_sources_licences.md`):
   public posts are used as ideas only, collected by hand, with nothing verbatim; rubrics
   come from the pinned documentation and never from posted answers; each item has a
   private provenance record; chat archives and the Confluent forum are left out.

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

### First pilot (2026-11-18): no-go

The first pilot judged about 80 labelled answers on 30 questions twice with instruction
version 1 (`runs/m4/S5_JUDGE_PILOT.md`). One of four criteria passed:

- per-point agreement between passes, kappa 0.81 (needed 0.7 or more): pass;
- reliability of S against the 1 to 5 mark, ICC 0.82 against 0.85: fail;
- the reader's essential omissions marked absent, 14 of 18 (77.8 %, needed 80 %): fail;
- quotes not found, 35 of 443 (7.9 %, needed at most 5 %): fail.

The analysis (`runs/m4/reports/s5_pilot_analysis.md`) found three things that matter. The
score did not order answers as the reader did: S agreed with the reader on 20 of 42 pairs
and disagreed on 18, and only the old mark was clearly above chance. About 1 in 6 of the
judge's flags outside the reader's labels was a real error. With a same-statement rule for
a catch, the judge caught 14 of the reader's 30 wrong items, not 25. The first pilot stays
a no-go; re-scoring it afterwards with the new quote matching is not a go.

### Second pilot (2026-11-19): no-go

What was run:

- 24 fresh open items, written under the rubric rules above;
- each answered in default and strict mode on the reference configuration (48 answers);
- judged in two passes with instruction version 2 (96 judgments);
- two readers read the 48 answers blind to the mode, with the excerpts: each reader took
  half of the items (24 answers each), in one pass. One reviewer did not read all 48, so
  reader and item half cannot be told apart in the readings;
- an optional arm that gave the judge the reference answer.

The criteria, fixed before the run. Go only if all of the first six hold (the pilot's
report counts precision and recall as one criterion, so it lists five):

- kappa of at least 0.7 between passes;
- the judge's share of the variance of a paired difference at most 1/3 (this replaces the
  ICC comparison);
- quote failures at most 5 % under the new matching;
- raw flag precision of at least 0.5, decided by the reviewer by substance, not by quote
  overlap;
- semantic-error recall of at least 0.7 in at least one pass;
- supported essential omissions: at least 80 % flagged, and at least 60 % of those flags
  confirmed;
- pair ordering is reported descriptively (it is unpowered at this size).

Result (`runs/m4/S5B_JUDGE_PILOT2.md`): **NO-GO, 3 of 5 criteria passed.**

| Criterion | Result |
|---|---|
| Kappa between passes | 0.74 on 334 point pairs: pass |
| Judge's share of the variance of a paired difference | S 0.19, P 0.26 (24 items): pass |
| Quote failures | 4.7 % (11 of 234 present or contradicted verdicts): pass |
| Flag precision and semantic-error recall | precision 0.78 and 0.81 (49 of 63, 56 of 69 flags confirmed); recall 0.43 and 0.49 (48 and 54 of 111 findings, same statement only): fail |
| Supported essential omissions | 37.5 % of the reviewers' 16 supported trials flagged, 15.8 % of the judge's 38 flags confirmed (6 of 38): fail |

The pilot stays a no-go. Re-scoring it afterwards with the chunk-id rule or the
"unverified" rule is not a go; the first pilot's rule applies. The analysis
(`runs/m4/reports/s5b_pilot2_analysis.md`) found why the two criteria failed. A perfect
judge with the input it had would reach about 76 of 111 findings (0.68), below the 0.7
bar. Of the 32 unconfirmed supported-miss flags, 23 came from the file-level rule and 9
from verdicts turned into misses by quote validation. Ignoring "supported", judge and
readers agreed on essential-miss units with kappa 0.75 and 0.73 (126 units per pass), but
that was found after the fact, on the same items.

What must not be concluded from this pilot:

- that the judge cannot detect omissions;
- that re-scoring this pilot with a new attribution rule makes it a go;
- that a better prompt with the same input reaches 0.7 recall;
- that the judge's flags measure the wrong rate, the harm rate or the direction between
  default and strict;
- any bar status from these 24 items, which were written to be hard, as the product's
  rate;
- anything about the cost of 4-bit.

The 24 items have been used for the pilot, the readings and the analysis; a change that
they prompt lists their ids.

### First fresh batch (B1): confirms the coverage role

The judge's coverage role (J1) is confirmed, or not, on a first batch of fresh items. The
agreement found in the second pilot was found after the fact, so it needs confirming.

What is run:

- about 30 fresh open scenarios across the five task types of `GOAL.md`, with rubrics
  under the rules above (chunk ids, no bundled points, any_of groups, outside-index
  marks), checked by a second reviewer before any answer exists;
- each answered on the reference configuration in default mode, in strict mode and (J4)
  in default mode with the answer limit raised from 2,048 to 4,096 tokens;
- judged in two passes with instruction version 2;
- the identifier check on all answers;
- a blind reading of the default and strict answers, with a second reader on about 25 %.

The raised limit is descriptive on this batch. In the second pilot 11 of 24 default
answers and 5 of 24 strict answers were cut at 2,048 tokens. B1 compares the share of
answers cut off, the median time per answer, coverage P and essential-miss units, each
against default at 2,048. The limit itself stays frozen until a decision run.

The criteria, fixed before the run. Go only if all six hold:

1. Essential-miss units, judge against blind reader, an any_of group as one unit: kappa of
   at least 0.6, and precision and recall of at least 0.75, in each pass, on at least 100
   weight-2 units.
2. A reviewer checks the rubric chunk ids before answers exist: the chunk contains the
   point in at least 95 % of points. Of the judge's supported-miss flags, at least 60 %
   are confirmed.
3. "Unverified" verdicts are at most 3 % of must verdicts, and none is counted as absent.
4. Kappa between the judge's two passes of at least 0.7.
5. A second reader reads at least 25 % of the answers. If the two readers' wrong +
   invented totals on the shared answers differ by more than 30 %, readings are not used
   for a bar until the protocol is fixed.
6. The identifier check raises zero false "invented" findings on the 244 stored answers
   and the 48 pilot answers.

Cost (inferred from the pilot's rates, not measured on B1): about 90 answers; at the
pilot's medians of 82 s (default) and 67 s (strict) per answer, 30 answers take about 41
and 33 minutes, and the raised limit takes longer than the first; about 180 judgments, at
the first pilot's rate of 190 in 25 minutes about 25 minutes; the identifier check is
CPU only. The pilot's 48 answers took two parallel readers about 20 minutes of wall time;
tokens were not recorded, and B1 records them. It runs on the model server.

The judged tier's items are written at scale only after B1 reports go.

## Tier C: live (specified only)

A local cluster with Strimzi and one injected fault per case (a full disk, a wrong
listener, a missing secret, a crash-looping pod, a stuck rebalance, an out-of-sync
application). The assistant has read-only tools. Scored on cause found and fix correct.
Built in the tools milestone; nothing is installed for it in M4.

## The hidden part

Split by family, not by item: all items from one template, one source page or one scenario
go to the same side.

Two limits of the hidden half of the **mechanical** tier, stated so that nobody relies on
more than it gives:

- A family there is a template plus one catalogue group, so the same template appears on
  both sides with different catalogue entries. The hidden half detects fitting to
  particular entries and items; it does not detect fitting to a template's wording. The
  judged tier splits by scenario and source page, as the rule above says.
- The mechanical items come from a deterministic generator in this repository, so the
  hidden half can be regenerated by anyone who runs it. Encryption stops accidental reading
  and searching; the rule that nobody opens it is what protects it. The generator's plain
  build output is deleted after each split and is not kept in scratch folders.

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
- Readings on the hidden half (J2, decided 2026-11-19). A sealed reader session may read
  hidden-half answers, with the protocol of "Wrong, invented and harmful: where the rates
  come from", and returns totals only: no item text, no per-item results and no quotes.
  Each such reading is written in `runs/m4/HIDDEN_LEDGER.md` before it runs, with its
  decision rule, as part of the decision run it belongs to; it is not a further run. The
  conductor and all other sessions never see its transcript. This sends hidden items to a
  hosted model, which the Owner accepted. Without such a reading, the reading-based bars
  would rest on the open half.

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

## Retrieval and index coverage (J6)

A separate work package builds a candidate index and candidate retrieval settings next to
the frozen ones. It is judged by one hidden-half run with a rule written beforehand; the
frozen index and settings are not changed until then. Its plan is a separate document.

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

**Tier B**, from the first pilot's 80 answers (`runs/m4/reports/s5_pilot_analysis.md`).
SD_d is the median across pairs of answer sets, each measured on only 10 to 20 questions,
so it is itself uncertain by about 25 %.

| Measure | SD_d | 120 items | 200 | 330 | 120 items, 4 passes |
|---|---|---|---|---|---|
| S | 0.33 | 0.084 | 0.065 | 0.051 | 0.079 |
| P | 0.18 to 0.21 | 0.047 to 0.052 | 0.036 to 0.041 | 0.028 to 0.032 | 0.044 to 0.047 |
| Old mark (1 to 5) | 1.24 | 0.32 | 0.24 | 0.19 | 0.31 |

In units of SD_d the smallest visible difference is the same for every measure: 0.26, 0.20
and 0.15 at 120, 200 and 330 items.

**Tier A**, binary all-pass: MDE ≈ 2.80 · √(p_d / n), where p_d is the share of items on
which the two configurations disagree. The real p_d is unknown until the A/A test.

| n | p_d 0.10 | p_d 0.15 | p_d 0.25 |
|---|---|---|---|
| 300 | 5.1 pp | 6.3 pp | 8.1 pp |
| 210 (70 % open) | 6.1 | 7.5 | 9.7 |
| 90 (30 % hidden) | 9.3 | 11.4 | 14.8 |
| 150 (50 % hidden) | 7.2 | 8.9 | 11.4 |

Consequence: cheap judging makes extra passes almost free, but they cut the smallest
visible difference by only about 6 %; items and answering are what cost. The fine decisions
(the cost of 4-bit, the answer checker) rest on tier A and on readings.

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
s (3 questions). Judge (DeepSeek without thinking, see "The judge"): the first pilot's 190
judgments took 25 minutes, so about half an hour per configuration of 120 items in two
passes. The earlier judge's figures (the brief's 1.5 minutes per item; v3's median of 160 s
per head-to-head call) no longer apply.

| Run | Answers | Judge (2 passes) | Total |
|---|---|---|---|
| Tier A 300, 4-bit default | 5.9 h (strict 3.9 h) | none | 5.9 h |
| Tier B 120, 4-bit default | 2.4 h (strict 1.6 h) | about 0.5 h | about 2.9 h |
| One configuration, one mode, both tiers | 8.3 h | about 0.5 h | about 8.8 h |
| E2: plain base, bf16 + retrieval, 4-bit default, 4-bit strict, A/A repeat | about 44 to 54 h | about 2.5 h (5 runs) | **about 47 to 57 h** of exclusive machine time |
| Smoke (60 A + 24 B, one judge pass) | 1.65 h | about 0.1 h | **about 1.8 h** |

Run sizes (see "Harness"):

| Size | Items | Cost |
|---|---|---|
| Smoke | 60 tier A and 24 tier B from the open part | about 1.8 h; shows only breakage (about 14 pp on tier A at p_d 0.15, about 0.74 on tier B) |
| Open full | the open part, per candidate | the rows above at the size of the open part |
| Hidden | the hidden part, per decision only | the rows above at the size of the hidden part |

Reading time (see "Wrong, invented and harmful: where the rates come from"): the pilot's
48 answers took two parallel readers about 20 minutes of wall time (inferred from the
run log; tokens were not recorded), plus about a quarter more for the second reader. The
reviewer no longer confirms the judge's flags one by one. The earlier judge's re-judging of the 24-item
subset adds about 15 minutes per decision run (an estimate, not measured with that judge).
These are not in the table.

E2 is affordable once. A full run is not affordable for every candidate change. Tier A
answers may be shorter than the real-question medians; that was not measured.

## GOAL.md bars: tier and metric

| GOAL bar | Tier and metric |
|---|---|
| 100 % existing keys | Tier A: identifier precision over all generated answers, against complete catalogues |
| 95 % valid resources | Tier A: all-pass share on the resource kinds |
| Nothing invented | A reading; the identifier check's totals are reported beside it |
| Complete | Tier B: supported essential-miss units, decided by chunk ids; confirmed on B1 |
| None that would do damage | A reading (not the judge's flags) |
| Honest about gaps | Wrong declines: from the tier A scorer's outcomes (decline outcomes, with the item's covering chunk ids and the pack's excerpts) and from readings |

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
| S2 Config, REST and Argo CD items | implementer | `scripts/eval2_items_a1.py`, `eval/m4/` | generated config HTML, `connect_rest.yaml`, argocd command pages → 496 items built, grouped 50/50 split (251 open, 245 hidden) | every reference passes, a perturbed one fails; near-duplicate check against all earlier sets clean | CPU | 1 day |
| S3 Scorer for these kinds plus identifier precision | implementer | `scripts/eval2_score.py` | answers → per-check results | hand-written pass and fail answers behave as expected; `--cancel`-type misses reported as unknown | CPU | 1 day |
| S4 Rubric pilot rubrics and label check | complex-reviewer, plus researcher for source checks | report only | 30 questions, sources → rubrics with file and line | every point anchored to a source | Claude, CPU | half a day |
| S5 Judge pilot | job-operator | — | 80 answers × 2 passes | rubric pilot thresholds computed | model server (DeepSeek without thinking, chosen in S5a) | 3 to 4 h |
| S6 A/A and 4-bit against bf16, same server | job-operator | — | 251 open items: 4-bit twice, bf16 once, plus pack replay | p_d and the non-inferiority interval reported | model server, 27B builds (loading them waits for the Owner) | 15 to 20 h |

After the first slice: the remaining tier A kinds are built after the A/A test has
reported, and the tier B items at scale only after B1 reports go. Then the starting-point measurement
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

## Revisions

- 2026-11-18: design written; revised after the review (15 changes) and after the Owner's
  decisions of that day (`MILESTONE.md`).
- 2026-11-19, first revision: after the first judging pilot failed, judge instruction
  version 2, fair quote matching, the answer's own excerpts, the second pilot's criteria.
- 2026-11-19, second revision, after the second pilot failed (3 of 5); the Owner agreed to
  the recommended J1 to J6:
  - J1: the judge reports coverage only; wrong, invented and harmful rates come from the
    identifier check and blind readings; no third pilot of the judge as an error finder;
    B1 confirms the coverage role.
  - J2: a sealed reader session may read hidden-half answers and return totals only, each
    reading logged in `runs/m4/HIDDEN_LEDGER.md` before it runs.
  - J3: a second reader reads about 25 % of the answers.
  - J4: B1 measures default mode with the answer limit raised from 2,048 to 4,096 tokens,
    descriptively; the limit stays frozen.
  - J5: the tier A scorer reports pass, correct decline, wrong decline and wrong beside
    the unchanged all-pass rate, reads the frozen index read-only, and tags items whose
    fact is not in the index.
  - J6: a separate work package for retrieval and index coverage, judged by one hidden-half
    run.
- 2026-11-19, night: the Owner agreed with the recommendations K1 to K6 (`MILESTONE.md`):
  - K1: the judge's coverage role is confirmed on B1. For every later batch the rubric
    chunk check runs after the second review and its completions, and must reach 95 %
    before any answer exists.
  - K2: the judged tier is written at scale to 120 or more items, the Owner's questions
    first, in batches of 30 with the same writing, second review and chunk check.
  - K3: retrieval candidate R3 (frozen pack first, then the exact-heading lookup and one
    runbook, up to 10 chunks and 24,000 characters) is tested once on B2 by the rule in
    `MILESTONE.md`, fixed before B2 exists.
  - K4: for E3, a statement marked "not confirmed" still counts as an error; minor items
    are reported beside the bar and not counted; "checked before the user sees it" means
    that prose streams and each code block is held until its check, with corrections to
    prose after the answer; E3 is measured on the hidden judged half after E2; a grounded
    default prompt variant is a separate item with its own pilot; no catalogue change.
  - K5: the not-covered generator of tier A may read the frozen index, read-only, with its
    hash checked.
  - K6: the Owner's 34 questions are reconstructions of public incidents (source 3). They
    are admitted under the working rule for public posts: ideas only, rubrics from the
    pinned documentation and never from the posted fix, the source link as the private
    provenance record, a point that only the incident shows marked as resting on a source
    outside the index. All 34 are hidden. Sealed sessions write and check their rubrics
    and adapt or drop items that do not apply to the pinned versions, and return results
    per id only. The plain copy is kept outside the repository.
