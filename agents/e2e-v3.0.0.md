# End-to-end test of release v3.0.0

**What this is:** a test of the release as a new user meets it, from the published artefacts
only, on two serving paths the Owner named on 2026-11-18: a model downloaded from Hugging
Face and served by oMLX, and a model served by Ollama. It is a test of the path (does it
install, run, answer, stay local, and how long does it take), not a measurement of answer
quality. Nothing here changes a frozen thing, a published repository or the tag.

**Published artefacts under test**

- Code: the Git tag `v3.0.0` of this repository on GitHub.
- Documentation index: the Hugging Face dataset `kafka-sre-docs-index-v3`.
- Adapter: the Hugging Face model `kafka-sre-27b-v2-retrieval-only`.
- Namespace: the account in the repository links of `README.md`.

## Rules

1. **Published artefacts only.** Work in a clean directory outside this working tree (the
   session's scratch directory), from a fresh clone of the tag, a fresh virtual environment
   and files downloaded from the Hub. Do not copy anything from this working tree into the
   test (no `work/`, `runs/`, `adapters/`, `out/`), with one exception named in T5.
2. **Follow the documents literally first.** Do what `README.md` at the tag and the two
   cards on the Hub say, in their order. Every place where a literal reading fails, is
   ambiguous or omits a step is a finding: record the instruction, what happened, and the
   smallest wording or code change that would fix it. Then work around it and continue.
3. **Do not fix the repository.** No commit, no edit of a tracked file in this working tree,
   no upload, no change to either Hub repository. Fixes are proposed in the report.
4. **Downloads and installs allowed by this brief:** Python packages into the test's own
   virtual environment; the dataset and the adapter; the reranker; one MLX 4-bit build of
   the base model through oMLX; one GGUF build of the base model at Q4_K_M (or the nearest
   available 4-bit quantisation) through Ollama. Ollama is already installed; do not upgrade
   it. Anything else waits for the Owner. Use no `sudo`. Delete nothing that the test did
   not create, and keep the downloaded models (list them with sizes so the Owner can decide).
5. **The machine's rules still hold** (`AGENTS.md`): one model set in memory at a time;
   before any load or unload on the model server check that it has no active or waiting
   request, and wait if another client is active; unload only what the test loaded; never
   run the model server and Ollama with models loaded at the same time; note every load,
   unload and download in `runs/v3/HEARTBEAT.md`. Stop Ollama's server at the end if the
   test started it.
6. **Privacy.** The server address and key stay in `~/.config/kafka-sre-llm/` or the
   environment and are never printed or written to a file. No personal detail goes into any
   request. Hub downloads of public files need no token; do not print one.
7. **Look things up.** How oMLX downloads a model from the Hub, and how Ollama turns
   thinking off and sets the context length for a model, are to be read from each tool's
   own documentation or help output, not guessed. Record what you read and where.

## The fixed question sample

The first-3 check uses questions 1 to 3 of `eval/questions_real.jsonl` (in the clone).
The timing sample uses ten questions by position: 1, 2, 3, 5, 7, 18, 26, 31, 36, 39 (the ten
whose measured answers a reviewer has read), each in default mode and in `--mode strict`.

## Tests

### T0. Clean install from the tag
Clone the tag, create the virtual environment and install as the README says. Record the
Python version, the versions of mlx, mlx-lm, sentence-transformers and huggingface_hub that
were installed, and the time taken. Compare mlx and mlx-lm with the versions the README
says the checks were run with (0.32.4 and 0.32.2).

### T1. Unit tests in a fresh clone
Run the test command from `AGENTS.md`. Record pass, fail and skip counts. A test that fails
only because a git-ignored file is absent is a finding (the suite is described as offline).

### T2. The index from the Hub
Download the dataset, check `SHA256SUMS`, and make `python -m kafka_sre_rag verify` pass
using only what the README and the dataset card say. If the README only explains how to
build the index from sources and not how to use the downloaded one, that is a finding;
then place the file where `frozen.json` expects it and continue. Use a separate, empty
Hugging Face cache for this test so that the reranker is really downloaded at its pinned
revision. Record what `verify` checks and prints. Then run the README's `--dry-run`
command and confirm it prints a prompt with six excerpts.

### T3. The adapter from the Hub, in-process (mlx)
Download the adapter repository, check `SHA256SUMS`, and follow its card to answer
questions 1 to 3 with the runtime: base model as the card describes it (the non-thinking
wrapper), `--adapter` pointing at the download. The card says how to get the base model;
if the full-precision base is already in the machine's normal Hugging Face cache, use it
from there and do not download 54 GB again (say so in the report). Expected: answers that
are not empty and on topic. Compare with the stored answers the Owner's tree holds
(`work/v3/v2rag/ans_v2_ownexp.jsonl`, read-only, comparison only: this is the one
exception to rule 1) and record equal or the first difference.

### T4. Model downloaded from the Hub and served by oMLX
1. Find on the Hub the MLX 4-bit build of the base model (Qwen3.8-27B). Record the
   repository, revision, size and licence tag. If several exist, take the plain 4-bit one
   from the same publisher as the full-precision build the project uses; say why.
2. Have oMLX download it from the Hub by oMLX's own documented way, and load it. Record the
   download time, the size on disk and the memory the server reports for the loaded model.
3. With the runtime's server backend and thinking off, run the first-3 check and then the
   timing sample (20 answers). Confirm on the first response that thinking is really off
   (no reasoning text, no empty answer).
4. Record per answer: seconds for search terms, retrieval and answer; answer tokens;
   tokens per second if reported; whether the answer is empty or cut off.
5. Unload the model.

### T5. Model served by Ollama
1. Start Ollama's server if it is not running. Get a GGUF build of the base model at
   Q4_K_M (Ollama's own library if it has the model, otherwise a GGUF repository on the
   Hub). Record the exact name, digest, size and licence.
2. Find and apply the documented way to turn thinking off and to set a context length that
   holds the whole prompt. The prompts carry six excerpts of up to 4,000 characters each
   (two runbook sources up to 12,000), so Ollama's default context length is likely too
   small, and a prompt that is cut off silently is the most likely hidden failure of this
   path. Prove the prompt was not cut off: compare the prompt token count Ollama reports
   with the context length for at least the longest of the ten prompts.
3. With the runtime's server backend pointed at Ollama's OpenAI-compatible endpoint, run
   the first-3 check and the timing sample, recording the same fields as T4, plus the
   memory `ollama ps` shows.
4. Stop the model (and the server if the test started it).

### T6. Nothing leaves the machine
With the model already downloaded and loaded, run one question on each serving path with
Hub access switched off (`HF_HUB_OFFLINE=1`) and with the proxy variables pointing at a
closed local port, so that any outbound HTTP request other than the one to the local model
endpoint fails. Expected: the answer still comes. Record anything that tried to reach the
network (the reranker's "could not reach the Hub" notice is one known candidate).

### T7. Failure messages
One run each, expecting a clear error and a non-zero exit code: the index file absent; the
index file altered by one byte; the server endpoint closed; a model name the server does
not have; (Ollama) the context length left at its default, if step T5.2 showed it cuts the
prompt. Record the message the user would see.

## What to record

`runs/v3/E2E_V3_0_0.md` (in this working tree; `runs/` is git-ignored), with:

1. A result line: how many tests passed, how many failed, how many findings.
2. A table: test, pass or fail, one line of what happened.
3. Timing, per serving path and mode, over the ten questions: median, minimum and maximum
   of the total seconds per answer and of the answer tokens; memory held; tokens per
   second. Next to them, for reference, the figures already known for the full-precision
   model (`README.md`: 134 to 212 s default, 77 to 117 s strict, 3 questions). State
   against the bar in `GOAL.md` (median under 60 s; about 20 GB held) whether each path
   met it on this machine, and say plainly that this machine is not the reference laptop.
4. Findings, most serious first: the instruction or behaviour, what happened, the proposed
   fix (exact wording or the file to change), and whether a user would be blocked by it.
5. What was downloaded, where it lives and how large it is.
6. What could not be tested and why.

The answers of the timing sample are kept under `runs/v3/e2e/<path>/` as JSON, one file per
question and mode, so that a reviewer can read them later. No judgement of their quality is
part of this test beyond "not empty, on topic, not cut off".

## Stop conditions

Stop and report, without working around it, if: a download would exceed 25 GB; a tool asks
for elevated rights; the model server stays busy with another client for more than 30
minutes; a published artefact fails its checksum; or anything would require changing a
published repository.
