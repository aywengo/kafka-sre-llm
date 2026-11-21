# Changelog

Numbers are given with their size. "Hidden half" means items of the new evaluation that were never used to build or
tune the assistant; the harness prints totals only. The longer description is in `README.md` ("Status").

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
