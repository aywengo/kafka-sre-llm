# Changelog

Numbers are given with their size. "Hidden half" means items of the new evaluation that were never used to build or
tune the assistant; the harness prints totals only. The longer description is in `README.md` ("Status").

## v3.2.0 (2026-11-22)

Milestone M5: retrieval ranking and fewer wrong statements. Both criteria were **not met**; each candidate was stopped by a
rule fixed before its result existed. The release carries the candidates as experimental options, off by default, and
the repairs. The default answer path is that of `v3.1.0`: `scripts/retrieve.py` and the existing golden files are
unchanged, `prompts.py` only gained lines, `frozen.json` only gained the three hashes of the new prompts, and `verify`
matches. The fixes of `v3.1.1` are included. All numbers below are development data (open scenarios), not the hidden
half; no hidden run was used in M5 (4 of 6 remain).

### Added

- `--mode grounded-g2` (`ask`, `chat`, `scripts/eval2.py answer`): an experimental grounded prompt. On 54 development
  scenarios against default: judged coverage 0.476 against 0.545; 15.7 against 19.6 non-minor wrong, invented or
  harmful statements per 10 answers in a blind reading of 108 answers (paired difference -3.89, 90 % interval -6.85
  to -0.56); harmful statements 6 against 5 in total; nonexistent identifiers 2.22 against 5.37 per 10 answers;
  median 902 tokens and 42.4 s with search against 2,048 tokens and 75.6 s. It failed the gate to become the default.
- `--mode grounded-g1` and `--mode grounded-g3`, checked on the same 54 scenarios (judged coverage 0.423 and 0.493;
  G3 also failed on supported essential misses, 2.50 against 1.00 per 10 on 30 scenarios).
- `--profile r4`, an experimental retrieval profile (the frozen six chunks, then up to four more ordered by a second
  reranker call), with its eight grid settings in `kafka_sre_rag/candidate.json`. On 84 development scenarios the best
  setting gained 48 of 202 points on half S (+23.8 percentage points; bar +25) and 37 of 219 on half C (+16.9; bar
  +18), with no point lost.
- `scripts/r5_qwen.py`, an offline test of a second reranker (Qwen3-Reranker-0.6B), not a runtime option: +3 points over
  the best `r4` setting on S (bar +20), +22 on C, 6.7 GB of added memory against a limit of 3.5 (not reproduced: 3.2
  to 3.4 GB when measured again in fresh processes; the difference is not explained, and the gain on S was under its
  bar either way).
- The open batch B4 (`eval/m4/b4/`): 30 scenarios with covering sets for each must point (170 must points, 140 with a
  covering set), for one out-of-sample check of a retrieval candidate. It was not spent. Its sets were checked on the
  chunks' text without their title lines, which is stricter than what the model is given; the dropped chunks are
  recounted before the batch is used.
- The mechanical development sample `eval/m4/g_dev_ids.json`, and the development check chain for the grounded variants.

### Changed

- Help texts of `ask`, `serve` and `scripts/eval2.py answer` mark the grounded modes and the retrieval candidates
  EXPERIMENTAL.
- Tests that need built or git-ignored files (the index file, `runs/` pins, `work/` answers and packs) skip with the name
  of the missing file; the tests added in M5 now do this too.

### Fixed

- The E2 chain checks a stored manifest and markers at its start; the E5 chain stops when a step refuses; a resume
  survives a reload of the same model; the inline-marks step takes `--allow-commit-change`.

### Not met

- Ranking: both candidates (`r4`, the second reranker) were stopped by their rules; the batch B4 was not spent.
- Fewer wrong statements: all three grounded variants failed the development check on judged coverage (bar: not more
  than 0.05 under default); none reached the hidden half. Harmful statements were not reduced.
- Repairs not done: the answer checker still misses names in prose and comments; the scorer's index-aware declines.

## v3.1.1 (2026-11-22)

A patch release: the published setup now works from a fresh clone. Nothing that was measured changed: the prompts,
the retrieval settings, the index and the default answer path are those of `v3.1.0`.

### Fixed

- `scripts/build_catalogues.py` read `runs/v3/INDEX_FROZEN.json`, a file that is not in the repository, so the
  catalogues could not be built from a fresh clone and `ask --check` could not be set up from the README. It now reads
  the pins from the tracked `kafka_sre_rag/frozen.json`; if `runs/v3/INDEX_FROZEN.json` exists it must agree, or the
  build stops and names the difference. The catalogue hash is unchanged (`e319653a5432...`).
- The source paths that enter the catalogue files are now made relative lexically, so the hash does not depend on
  whether `raw/` and `raw_catalogue/` are symlinks (with symlinks, `v3.1.0` gave another hash).
- Tests that need built or downloaded files (the a3 catalogues, the built catalogues, the index) or a `.venv` failed
  or errored in a fresh clone (9 failures, 15 errors). They now skip with a reason that names the file and the
  command that makes it, and tests that start Python use the running interpreter. No assertion was changed.

### Changed

- `README.md`: closed-book judged coverage corrected to 0.39 (two-pass mean 0.387); the 10 s first-words bar is
  attributed to the milestone, not to `GOAL.md`; the fresh-clone test is stated as what it was.

## v3.1.0 (2026-11-22)

The retrieval runtime on the unmodified base model, with an answer checker, a streaming service and the open half of a
new evaluation. The measured reference configuration is Qwen3.8-27B at 4-bit, served through a local
OpenAI-compatible server, default mode, on one Mac Studio. The default answer path (prompts, retrieval settings, index)
is the one that was measured in the evaluation below.

### Added

- **Answer checker** (`ask --check`, `serve --check`): marks, in place, identifiers that do not exist in the pinned
  versions (Kafka 4.3.2, Strimzi 1.3.1, Argo CD v3.6.1, Kubernetes v1.37.2) against catalogues built with
  `scripts/fetch_catalogue_sources.py` and `scripts/build_catalogues.py`. `verify` shows the catalogue hash.
- **Local service** (`serve`, `ask --service-url`, `ask --stream`): OpenAI-compatible, streaming, loopback only; keeps the
  index and the reranker loaded (about 3.8 GB idle).
- **Evaluation**: 644 mechanical items (339 open, 305 hidden) and 121 judged scenarios (60 open, 61 hidden), with the
  harness (`scripts/eval2*.py`), the mechanical scorer and the identifier check. The hidden half is committed
  encrypted. The open half is in `eval/m4/`.
- **Closed-book mode** of the harness (the plain base model without retrieval) for comparison.
- `agents/laptop-tests.md` and `scripts/laptop_bench.py`: the plan and the script for measurements on a 32 to 36 GB
  laptop (not run yet).
- `CONTRIBUTING.md`.

### Measured (hidden half, 4-bit unless stated)

- Mechanical, all checks of an item pass, 305 items: 81.0 % (247) with retrieval, default mode; 75.7 % (231) in strict
  mode; 52.8 % (161) closed book; 82.6 % (252) with retrieval at full precision.
- 4-bit minus full precision: -1.64 points, 90 % interval -5.10 to +1.90. The margin fixed in advance (lower end above
  -5) was missed by 0.10; the loss was accepted and 4-bit stays the reference.
- Judged coverage of required points, 61 scenarios: 0.55 with retrieval, 0.38 closed book, strict 0.53 to 0.55 by pass
  (difference to default -0.02).
- Median time per answer through the streamed service: 24.9 s on the 305 mechanical items and 81.8 s on the 61
  scenarios; on open questions 26.1 s (60 short) and 90.3 s (60 scenarios), first words 19.2 s and 22.9 s.
- Bake-off of other base models on the open half (339 items, 60 scenarios each): Gemma 4 26B-A4B, GLM-4.7-Flash and
  Granite 4.2 30B were dropped by rules fixed in advance; Gemma 4 31B was left out for memory. Qwen3.8-27B at 4-bit
  stays (`runs/m4/reports/e5_screen.md`).

### Not met

- Wrong statements: a blind reading of 61 default answers found 17.2 wrong, invented or harmful statements, minor ones
  not counted, per 10 answers (90 % interval 14.4 to 20.0; goal: at most 2) and 13 harmful ones (goal: none).
- Speed: median above 60 s for long scenarios (81.8 s and 90.3 s; 36 of 61 and 39 of 60 answers hit the 2,048-token
  limit); first words after about 20 s on all 120 open questions, none within 10 s (goal: 10 s).
- Not measured: a laptop. Everything above is from the Mac Studio.

### Experimental (in the code, off by default)

- `--revise` (a model patch step after the check): on a reading of 30 open answers it removed no counted error in the
  first reading and 2 (with 1 introduced, a harmful command) in the second; not tested on the hidden half.
- `--profile r1|r2|r3` (candidate retrieval settings): none passed its acceptance rule; `frozen` is the default.

### Unchanged

- The published index dataset (`aywengo/kafka-sre-docs-index-v3`, the frozen 6,444-chunk file) and the v2
  retrieval-only adapter (`aywengo/kafka-sre-27b-v2-retrieval-only`).
- The prompts, the frozen retrieval settings, and the measured prompt hashes (`tests/golden/`).

## v3.0.1 (2026-11-18)

Fixes from the end-to-end test of v3.0.0 (`agents/e2e-v3.0.0.md`):

- The server backend checks the `--model` name against the server's model list and stops with a clear error instead of
  letting the server answer with another model; clearer errors from the server backend.
- A server on the same machine is reached without the proxy environment variables.
- README: the index download route and the Ollama steps in working order.

## v3.0.0 (2026-11-18)

The measured release of the retrieval runtime on the base model.

- Retrieval runtime `kafka_sre_rag/` (`ask`, `chat`, `verify`): the model writes search terms, BM25 and a
  cross-encoder reranker pick up to 6 excerpts, the model answers from them; a default and a strict mode.
- Result on the 40 real questions (a legacy set, two judge passes): base model plus retrieval scores +1.48 against the
  plain base model on the shipped index (standard error 0.21; 33 wins, 5 ties, 2 losses), +1.19 on the earlier index.
  The fine-tuned adapters alone add nothing (v1 +0.08, v2 -0.18).
- The documentation index as a dataset (6,444 chunks) and the v2 adapter as a retrieval-only model.
- The adapter stage was stopped after a ceiling check; v3 ships as retrieval on the base model.
