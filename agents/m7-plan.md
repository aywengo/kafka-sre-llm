# M7 plan: measure first, then build

Written on 2026-11-23 by a planning session from the Owner's two ideas of that day (a
longer time budget; a knowledge base) and the conductor's answer to them. It is a brief:
where this text and `MILESTONE.md` differ, `MILESTONE.md` wins. The Owner's decisions on
section 9 are recorded in `MILESTONE.md`, not here. The numbers of M6's informative D5
run and of its reading were not known when this was written; they are in
`milestones/M6.md` once M6 is closed.

## 1. Recommendation

- **Run the four-way test on D5 first**, as six arms. Two of the extra arms come almost free: the thinking draft is a by-product of the thorough arm, and the checking pass can be run on drafts that already exist.
- **The test is a screen.** It decides whether work continues, not what ships. 50 worn scenarios cannot show a change in wrong statements smaller than about 3 per 10.
- **Start building the exact-lookup knowledge base now**, offline and in new files only. It needs no model.
- **Model-written fact entries, and the thorough profile as a product, wait** for the test and then for B4.
- **Nothing that the running DA chain pins gets edited until the chain is done.** It pins every `kafka_sre_rag/*.py` and rechecks the pins before each stage, so an edit would stop it.

## 2. What the evidence says

| Measure | Value | Size, source |
|---|---|---|
| Must points that reach the model (strict count) | frozen 69 of 227 (30.4 %); r4g2 112 (49.3 %) | D5, 50 scenarios (`runs/m6/D5_FUNNEL.md`) |
| Same, out of sample | frozen 50 of 140; r4g2 65 (+10.7 points) | B4, 30 (`runs/m5/B4_CHECK.md`) |
| Points no pack from today's index can serve | 29 not indexed, 13 with no covering set (42 of 269) | D5 `CHUNK_CHECK.json` |
| Facts spread over two chunks | 17 points reachable only through a set of two | D5 |
| Ranking against search | perfect ordering of the 24 candidates +103 points; better search at most +15 | M5, 421 development points |
| Answers cut at 2,048 tokens | default 13 of 41, g2 1 of 41 (T); 29 of 54 against 1 of 54 (M5 development); 39 of 60 (E4) | as named |
| Non-minor wrong statements per 10 answers | hidden default 17.2 (61); B1 default 18.7, strict 11.3 (30, one reader); development default 19.6, G2 15.7 (54); T default 15.1, g2 13.9 (41) | readings |
| Harmful statements | hidden 13 in 61; development 5 and 6 in 54; T 14 and 14 in 41 (T is built to tempt) | readings |
| Errors the excerpts seem to support, or reasoning about the user's case | 13 of 56 (23 %) | B1, one classifier |
| Nonexistent identifiers per 10 answers | development 5.37 against G2 2.22; T 1.46 against 0.73 | identifier check, not read |
| Median time with search | T default 72.9 s, g2 42.7 s; E4 judged scenarios 90.3 s | measured |
| Parts of that time | search terms 6.2 s (50 calls); frozen retrieval 4.0 to 4.3 s; r4g2 second call 2.25 s (20 calls); decoding about 32 tokens/s with 10 to 12.5 s fixed per call (B1 linear fit) | measured |

**A longer limit alone (M4, B1, 30 scenarios, two judge passes, `work/m4/b1/comparison.md`):** at 4,096 tokens cut answers fell from 19 to 1, at +6.7 s (SE 3.4). Coverage moved by only +0.01 (SE 0.02). Nonexistent identifiers went from 10 to 20, not verified by reading. So length alone buys nothing.

**The checker and the repair steps, as measured:**
- The patch step R2 removed no counted error on 90 answers, net 0.0 to −0.3 per 10. A second run gave net +0.3, and one accepted patch was harmful.
- 6 of the checker's 22 flags on fresh B1 answers were false.
- The destructive-step marking caught 7 of 15 destructive steps on T.
- The closest test of a claim-against-excerpts pass (S5b) used a stronger model. It caught 0.43 to 0.49 of the readers' errors, and 0.78 to 0.81 of its flags were real.
- On B1 (one classifier), even a perfect check of identifiers, contradictions and catalogue facts leaves default at 11.3 per 10.

**Thinking:** the only measurement is v2 with thinking, closed book, on the 40 legacy questions: −0.12, one pass, 8 of 40 cut at 4,096. That leans on the legacy set again. Thinking with retrieval on the base model has never been measured.

**Runbooks:** 32 runbook chunks are indexed. Their own effect on answers is not measured. R3's runbook slot was measured only as served points, +7.4 on B2 against a bar of +15.

**My offline count on D5 (CPU, seconds):**
- A perfect pack (one covering set per point, fewest characters) is a median of 3 chunks (range 1 to 6) and 6,239 characters (maximum 19,404). Of its 175 chunks, 37 are already in the frozen pack and 107 are not in r4g2.
- I read the 42 point texts with no covering set once. Most are arithmetic, missing numbers or code design, and only 5 name a catalogued Kafka key. A knowledge base cannot serve them; thinking might.

**Expected gain from the perfect pack (inferred):** +0.11 (R6's slope, +0.040 for 21 points more served) to +0.23 (M5's 0.8 against 0.4 rule) in judged coverage.

## 3. The four-way test on D5

**Arms.** All arms use the default prompt and the frozen search terms.

| Arm | Pack | Call | Cost |
|---|---|---|---|
| A | frozen (the shipped default) | today's call | new draft |
| B | frozen | thinking on (the draft of D) | new draft |
| C | frozen | checking pass on A | one check call |
| D | frozen | thinking, then checking | check on B |
| E | perfect | today's call | new draft |
| F | perfect | thinking, then checking (its draft is arm F′) | new draft and check |

**Perfect pack.**
- Covering sets come first: for each indexed point, the set that adds the fewest characters.
- Frozen-pack chunks follow in frozen order, within the frozen cap: max(6, set chunks) chunks and 14,000 characters (`select_ranked`). One scenario's sets go over that cap and are kept whole.
- The result is a median of 6 chunks, about 2 of them from the frozen pack, so pack length matches the frozen pack and mainly delivery differs. An unpadded variant is equally defensible: it adds less noise but changes the length.
- The 42 points with no set stay in the rubric. Coverage is reported over all 269 points and over the 227 and the 42 separately.

**The checking pass** (a new `VERIFY_PROMPT` in `prompts.py`; do not reuse the existing `THOROUGH` constant, which asks for longer answers):
- Input: the question, the excerpts, the draft with numbered lines (`revise.numbered`) and the checker's findings.
- Reply: the JSON form of `REVISE_PROMPT`, `{"replacements":[{"line","text","quote"}]}`. It covers only lines that an excerpt contradicts or that a finding names. A deletion is allowed only when the line is contradicted and no correct form exists. No new steps.
- The runtime accepts a patch only if all of these hold, otherwise the draft stands with R1 marks:
  - every quote is a substring of the pack;
  - it passes `parse_reply`, `validate_patches`, `only_allowed_changed`, `structure_problems` and `yaml_regressions`;
  - the checker finds no new invented or wrong_use item;
  - at most 25 % of lines change.
- The prompt is written on worn B1 and B2, never on D5.

**Budget per answer at 180 s (inferred from the measured rates):**

| Part | Tokens | Seconds |
|---|---|---|
| Search terms and retrieval | – | 10 |
| Draft: prompt, thinking at most 1,500, answer at most 2,048 | | 11 + ≤47 + ≤64 |
| Check, thinking off: prompt about 7k, output at most 600 | | 14 + ≤19 |
| **Median / 90th percentile** | | about 150 to 165 / about 190 to 210 |

The thinking cap is the open part. A total `max_tokens` cannot keep long thinking from cutting the answer. A pilot measures this first (task K3).

**What is measured:**
- judged coverage (two passes);
- cut answers and empty answers;
- time per part, with the median and the 90th percentile;
- nonexistent identifiers;
- the 116 mechanical items for A and D;
- a blind reading of A, D, E and F (200 answers, all four answers of a scenario in one bundle): about 13 first-reader and 4 second-reader sessions. B, C and F′ are judged only.

**D5 as data.** After M6's DA run and its reading, D5 is worn development data. That allows comparisons inside this run, with the reference answered again in the same run, and a choice among arms declared beforehand. It does not allow out-of-sample claims or tuning on D5 items.

**What 50 scenarios can show:**
- Paired coverage has a standard error of about 0.025 to 0.04, so differences under about 0.06 are not resolved.
- Wrong statements have a standard error of about 1.8 per 10, so a true −2 passes "upper end below 0" only about 30 % of the time.
- Five harmful statements in 50 answers has a 90 % interval of 2.0 to 10.5.
- A 90th percentile from 50 answers is the fifth largest, so the measured value must be at most 270 s.

**What each outcome means (fixed beforehand; decides only whether work continues):**

| Result | Recommendation |
|---|---|
| E − A at least +0.10, lower end above +0.04 | Build the knowledge base: (a), then a pilot of (b) |
| E − A between +0.04 and +0.10 | (a) only |
| E − A below +0.04 | No knowledge base for coverage. (a) only if it lowers nonexistent identifiers |
| D − A wrong statements at most −2.0 per 10, coverage at least −0.04, harmful at most A + 2, cut at most 10 %, median at most 180 s and measured 90th percentile at most 270 s | Thorough profile goes on to B4 |
| Errors improve but time fails | One g2-based thorough arm on worn data, then B4 |
| Both of the above | Both, (a) first, because it feeds the check |
| Neither | Stop both; record it |

B against C shows whether thinking or checking carries an effect. That comparison is descriptive and corrected for two comparisons.

## 4. The thorough profile (if the test supports it)

- **In the runtime:** `--answer-profile thorough` (the name "profile" already means retrieval in `profiles.py`). It is off by default and marked unmeasured, as `pipeline.py:41-54` does for other options.
- **Code changes:**
  - per-call thinking on `OpenAIChatGenerator`, so the search terms and the check stay off;
  - the `enable_thinking=False` that is hard-coded at `generators.py:203` becomes a parameter;
  - `frozen.json` values stay unchanged.
- **How it composes:** with r4g2 or the knowledge base on the pack side, and with g2 on the prompt side. Under g2 the check never edits lines labelled "General knowledge".
- **The 300 s gate:** a wall-clock deadline. At 240 s the check is skipped and the draft comes back with R1 marks and "not checked". The answer is held, not streamed, while it is being checked. The 90th percentile is measured over judged answers with search.
- **Laptop:** decoding is probably 1.5 to 2 times slower (inferred, not measured). Until the laptop run, the bars are Mac Studio figures.
- **Proposed wording for `GOAL.md`** (lines 49 and 149; the Owner's to make): "Time to a complete answer: fast profile (default) median under 60 s; thorough profile (opt-in, extra time spent on checking, not longer answers) median at most 180 s, 90th percentile at most 300 s."
- **Said plainly:** the fast default does not meet 60 s on judged scenarios today (72.9 s on T, 90.3 s in E4).

## 5. The knowledge base, in stages

**(a) Exact lookup from the catalogues** (`work/m4/catalogues`, hash `e319653a…`):
- **What it holds:**
  - Kafka 4.3: 1,177 configuration keys with default, type, valid values and description; 99 history entries for 4.1 to 4.3; 135 protocol errors; 28 Connect REST entries.
  - 43 Kafka command-line tools with 603 options (catalogue marked incomplete).
  - Strimzi 1.3.1: 10 kinds and 5,002 field paths (types and enums, no descriptions); 35 annotations.
  - Kubernetes 1.37.2: 255 resources.
  - Argo CD 3.6.1: 179 commands.
- **How it is used:** identifiers in the question and the search terms (`profiles.Lookup.identifiers`), and in the thorough profile also in the draft (`Checker.extract`), become short reference cards with their pin. The cards go into a block placed after the frozen pack. No tool calling is needed.
- **Expected effect:** fewer nonexistent identifiers. But tier A's "name the setting" items become lookups, so the mechanical tier partly measures the lookup itself (`agents/m4-evaluation.md:98-106`). It is reported with and without the lookup.

**(b) Fact entries** for facts that are spread over chunks, need the title, or cross pages:
- **Writers:** the local teacher keeps everything local. Claude workers are faster; the docs are public, but the text goes to a hosted service.
- **Checking:** each entry is checked against the pinned source by an independent checker before it enters, at the 95 % bar D5 used.
- **Expected error rate:** first independent counts of model-written covering sets were 82.4 % right (D5), 84.6 % (h6) and 64 % (B4). The failures were incomplete sets, not wrong ones.
- **Start small:** a pilot slice of Strimzi and Kafka operations, about 1,500 chunks; roughly 8 or more server hours (inferred).

**(c) Ranking entries with chunks:**
- The frozen pack stays first and untouched.
- Then entries from a separately hashed index, within 10 items and 24,000 characters.
- The index, the reranker and the golden hashes stay frozen.
- Entries record the chunk ids they were derived from. A point counts as served by an entry only after an independent checker rates that entry for that point. Chunk-based covering sets of B4, D5 and h6 stay as they are; h6 needs sealed checkers for this.

**Replacing classic retrieval** would first have to show, on a fresh batch and then on the hidden part:
- entries alone serve at least what frozen plus r4g2 serve;
- coverage and wrong statements are not worse;
- a sampled audit shows the entries cover the indexed facts.

The plan does not start there because the entries are model-written, roughly 15 % fail a first check, `GOAL.md` rule 2 asks for pinned sources, and the whole evaluation is tied to chunk ids.

## 6. Evaluation

| Data | State | Use in M7 |
|---|---|---|
| D5 | worn after DA | the four-way screen |
| B4 (30 scenarios) | only a retrieval count; never answered | one out-of-sample answer check of the thorough profile, rule fixed first (needs the Owner, decision 6) |
| T (41) | answered and read | harm comparison of thorough against default, described as built to tempt |
| h6 (60), four looks | unused | one look for thorough (with (a) as a second arm, corrected for two candidates); keep at least two looks |
| New D7 | does not exist | needed before (b) can be judged fairly |

## 7. Exit criteria (proposed)

| # | Criterion | Bar |
|---|---|---|
| Z1 | Four-way test | Run, readers within 30 %, outcome table applied |
| Z2 | Thorough profile | h6 look with M6's rule (wrong statements: upper end below 0; coverage at least −0.04 point and lower end above −0.10; harmful at most R + 3; mechanical lower end above −3; wrong declines at most 1 in 10), plus median at most 180 s, 90th percentile at most 300 s, cut at most 10 % |
| Z3 | Lookup (a) | Nonexistent identifiers below the arm without it; mechanical lower end above −3; at most +1 s |
| Z4 | Entries (b), conditional | 95 % independent check; strict served count +15 points over frozen on D7, nothing lost |
| Z5 | `GOAL.md` | Wording put to the Owner |

**Out of reach even if everything works:**
- 2 wrong statements per 10 (the inferred floor is about 3.6);
- zero harm (cannot be shown);
- laptop bars, without the laptop run;
- "helps on every task type".

## 8. Tasks

| Task | Role | Files | Output and acceptance | Needs | Time |
|---|---|---|---|---|---|
| K0 perfect packs | implementer | new `scripts/m7_perfect_packs.py` and a test | `work/m7/packs_perfect.jsonl`; every indexed point has a whole set; medians equal section 3 | CPU, now | 0.5 d |
| K1 check-pass design | complex-reviewer | none | prompt text and acceptance rules, tried offline on stored B1/B2 answers | CPU, now | 0.5 d |
| K7 lookup module | implementer | new `kafka_sre_rag/kb_lookup.py` and tests | cards; resolve rate on stored answers | CPU, now | 1.5 d |
| K2 thorough code | implementer | `generators.py`, `prompts.py`, `revise.py`, `pipeline.py` | golden tests pass; full suite passes | CPU, after DA | 2 d |
| K3 thinking pilot | job-operator | `work/m7/pilot` | thinking-token distribution, empty and cut answers on 20 B1/B2 scenarios; sets the cap | server 1 h | 0.5 d |
| K4 chain | implementer | new `scripts/m7_four_way.sh` | rule in the header; stub dry run; resume changes nothing | CPU | 1.5 d |
| K5 run | job-operator | `work/m7/` | 7 judged sets, 700 judge calls | server 10 to 14 h | 1 d |
| K6 reading | readers | bundles | totals; readers within 30 % | about 17 sessions | 1 d |
| K9 / K10 | job-operator, sealed readers | – | B4 check; h6 look | server 4 h and 10 to 12 h; about 15 sessions | conditional |

**First slice** (needs only the go for the plan): K0, K1, K7.

**Totals:** about 25 to 31 server hours, 9 to 12 worker-days, about 32 reader sessions.

**Risks and the first sign of each:**
- **Thinking runs long or the switch is ignored:** K3 shows a median above 1,500 thinking tokens, or no reasoning tokens at all. A GLM thinking switch once had no effect (`HEARTBEAT.md`).
- **The check makes things worse, as R2 did:** readers find errors on patched lines.
- **The check catches little:** fewer than 20 % of answers get an accepted patch.
- **Smaller effect on B4:** as with r4g2 (+18.9 on D5, +10.7 on B4).
- **Readers can tell the arms apart:** their notes guess the arm.
- **The DA chain stops:** an edit to a pinned file makes its recheck refuse.

## 9. Decisions for the Owner

1. **Go for the plan and the 4-bit load for M7?** Yes. Without it nothing can be measured.
2. **Change `GOAL.md` now?** Recommend recording the wording as "proposed" now and "agreed" after Z2. Agreeing now would let an unmeasured profile ship.
3. **Six arms or four?** Six. The extra cost is about 1.5 server hours, and they are judged only.
4. **Pad the perfect pack?** Yes. An unpadded arm adds about 1.3 server hours.
5. **Thorough on the default prompt or on g2?** Default, as you specified; g2 only if time fails. Adding a g2 arm now costs about 2 server hours and 4 reader sessions.
6. **Answer B4, against `AGENTS.md` "used once"?** Yes. The alternative is writing D7 for answers too: 2 to 3 worker-days.
7. **Catalogues at answer time, against `m4-evaluation.md:98-106`?** Yes, with tier A reported both ways. The cost is that the mechanical tier partly stops testing the model.
8. **Entries (b): who writes them, and the licences?** Decide only if the test supports (b). For you to decide: Kubernetes docs are CC-BY-4.0; the terms on the teacher's outputs; publishing entries in the dataset.
9. **One h6 look?** Yes, after B4.

**Brief lines this plan departs from, so they can be amended openly:**
- `GOAL.md:49` and `:149` (the 60 s row);
- `AGENTS.md` "B4 is used once";
- `AGENTS.md` "Do not rebuild it" (the index), for (c);
- `AGENTS.md` `PACK_PROMPT_VERSION` and bulk generation, for (b).

No download or install is needed (`agents/train-v3.md:87`, `:156`).

**Not verified:** whether the 4-bit build honours thinking on oMLX; every time figure in section 3; laptop speed; the expected gains; my one-pass reading of the 42 point texts. The D5 results of M6's informative run are still to be added. Only the thinking measurement leans on the 40 legacy questions.
