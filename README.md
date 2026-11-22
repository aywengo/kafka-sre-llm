# kafka-sre-llm

A Kafka / Strimzi / Kubernetes / Argo CD SRE assistant on an open-weight 27B model
(Qwen3.8-27B), on Apple Silicon with mlx-lm. This repository holds two things:

- the **retrieval runtime** `kafka_sre_rag/` (v3.2.0), which is what currently works;
- the **fine-tuning pipeline** that produced the v1 and v2 adapters (described at the end).

## Status

**v3.2.0** (2026-11-22) is `v3.1.1` plus the experimental options of milestone M5, which are off by default
(see "M5: what was tried and what was not met" below). **v3.1.0** is the retrieval runtime on the unmodified base model, with an answer checker and a streaming
service. The measured reference configuration is Qwen3.8-27B at 4-bit (`mlx-community/Qwen3.8-27B-4bit`),
served through a local OpenAI-compatible server, default mode, thinking off, on one Apple Silicon machine
(a Mac Studio, not a laptop). The default answer path (prompts, retrieval, index) did not change after the
evaluation below was run; v3.1.0 only changed help texts in `kafka_sre_rag/__main__.py` after it. `v3.1.1` is `v3.1.0` with the setup fixed for a fresh clone (the catalogue build and the tests; see
`CHANGELOG.md`); nothing that was measured changed.

**Read every answer before you act on it.** In the blind reading below, the default answers held on average
about 1.7 non-minor wrong, invented or harmful statements each (17.2 per 10 answers). The assistant is a way to
find the right documentation page quickly, not an authority.

What was measured, on the new evaluation's **hidden half** (items that were never used to build or tune the
assistant; the harness prints totals only; 2 of 6 allowed hidden runs were used before this release):

- **Mechanical items** (305 items, all checks of an item must pass; a program scores them, no model judges):
  81.0 % (247 of 305) with retrieval at 4-bit in default mode; 52.8 % (161 of 305) closed book (the same
  base model, no retrieval); 82.6 % (252 of 305) with retrieval at full precision. 4-bit minus full
  precision is -1.64 points (90 % interval -5.10 to +1.90). The margin fixed in advance was a lower end
  above -5; it was **missed by 0.10** and the loss was accepted by the project owner, so 4-bit stays the
  reference. Strict mode scored 75.7 % (231 of 305), 5.25 points below default (interval -8.54 to -2.01).
- **Judged scenarios** (61 scenarios, a judge model scores the share of required points that an answer
  covers, two passes): coverage 0.55 with retrieval and 0.39 closed book (two-pass mean 0.387). Strict mode: about the same as
  default (difference -0.02, interval -0.06 to +0.03).

What is **not met** (the goals are in `GOAL.md`):

- **Wrong statements.** A blind reading of 61 default answers (readers saw the answers without knowing the
  configuration; two readers agreed within 2 % on the 32 answers both read) found **17.2 wrong, invented or
  harmful statements, not counting minor ones, per 10 answers** (90 % interval 14.4 to 20.0; goal: at most
  2), and **13 harmful ones** in total (goal: none). Minor statements (65 more) are not in that figure. Strict
  mode: 14.6 per 10 and 11 harmful; the difference to default (-2.62 per 10, interval -5.57 to +0.33) is not
  clearly different from zero. The answer checker (below) marks nonexistent names only; most of the counted
  errors are wrong statements about behaviour, which it cannot see (an inference from the open reading; the
  hidden reading returns totals only).
- **Speed.** Through the streamed service, 4-bit: a short mechanical question takes about 25 s at the median
  (26.1 s on 60 open questions, 24.9 s on the 305 hidden ones), a long scenario 80 to 90 s (90.3 s on 60 open
  scenarios, 81.8 s on the 61 hidden ones; 39 of 60 and 36 of 61 answers hit the 2,048-token limit). The
  goal is under 60 s. The first words appear after about 20 s (19.2 s on short questions, 22.9 s on scenarios,
  60 open questions each; none of the 120 within the 10 s goal). Another client's model was loaded on the
  server during the open timing run; the effect is not known.
- **Laptop.** Everything above is from a Mac Studio. The measurements on a 32 to 36 GB laptop (memory,
  speed, offline use) are not done yet; `agents/laptop-tests.md` is the plan.

What comes next, in one line: a combined candidate (the grounded mode together with better ranking), chosen on
a fresh development batch, checked once on the batch B4 and then on the hidden half; harmful statements, which
the grounded mode did not reduce; the leftover repairs.

### M5: what was tried and what was not met

Milestone M5 (2026-11-22) aimed at two things: a better ranking of the retrieved excerpts, and fewer wrong
statements. **Both criteria were not met.** Each candidate was stopped by a rule that was fixed before its result
existed, and no hidden run was used (4 of 6 remain). The numbers below are development data (open scenarios used
to build and tune), not the hidden half. Nothing here replaces the v3.1.0 headline numbers above.

- **Ranking, not met.** On 84 development scenarios (loose count of served points) two candidates were stopped.
  `r4` (the frozen six chunks, then up to four more ordered by a second call of the same reranker): the best
  setting gained 48 of 202 points on half S (+23.8 percentage points; bar +25) and 37 of 219 on half C (+16.9;
  bar +18), with no point lost. A second reranker, Qwen3-Reranker-0.6B, on the same candidates: +3 points over
  the best `r4` setting on S (bar +20) and +22 on C (about three standard errors), and it added 6.7 GB of memory
  against a limit of 3.5. The out-of-sample batch B4 was not spent. Of 421 indexed development points, the frozen
  retrieval serves 125 and the best `r4` setting 210; a perfect ordering of the same 24 scored candidates would
  serve 103 more (the milestone's count; no model was run for it).
- **Fewer wrong statements, not met.** Three grounded prompt variants failed the development check on judged
  coverage (54 scenarios, two judge passes; the bar was not more than 0.05 under default). G3 also failed on
  supported essential misses (2.50 per 10 on 30 scenarios against 1.00 for default; the bar was at most 2.0) and
  missed the coverage bar by 0.002. None went on to the reading that this check gates (on the unread batch B2) or
  to the hidden half.

| 54 development scenarios | default | G1 | G2 | G3 |
|---|---|---|---|---|
| Judged coverage | 0.545 | 0.423 | 0.476 | 0.493 |
| Supported essential misses per 10 (30 scenarios) | 1.00 | 1.50 | 2.00 | 2.50 |
| Median answer tokens | 2,048 | 789 | 902 | 1,139 |
| Answers cut at 2,048 tokens | 29 of 54 | 1 of 54 | 1 of 54 | 1 of 54 |
| Median seconds with search | 75.6 | 41.0 | 42.4 | 51.1 |

- **A reading of G2 (informative, it decided nothing).** A blind reading of 108 existing answers (54 development
  scenarios, nine reader sessions, readers 6 % apart): G2 made 15.7 non-minor wrong, invented or harmful
  statements per 10 answers against 19.6 for default, paired difference -3.89 (90 % interval -6.85 to -0.56).
  Harmful statements were not reduced (6 against 5 in total), and omissions rose (31.3 against 26.9 per 10).
  Nonexistent identifiers fell from 5.37 to 2.22 per 10 answers; judged coverage fell by 0.069.
- **More excerpts served gave some coverage back.** G2 on `r4`'s packs against G2 on the frozen packs, same 54
  scenarios: 146 against 88 of 272 points served, judged coverage 0.513 against 0.473 (+0.040, standard error
  0.025).
- **Repairs.** The chain defects found in M4 are fixed (the E2 chain checks a stored manifest and markers at its
  start, the E5 chain stops on a refused step, a resume survives a reload of the same model, the inline-marks
  step takes `--allow-commit-change`). Not done: the answer checker still misses names in prose and comments, and
  the scorer's index-aware declines (needed only for an index change).

**The default answer path is unchanged since v3.1.0.** Checked on 2026-11-22 by comparing with the tag `v3.1.0`:
`scripts/retrieve.py` and the existing golden files of the measured prompts in `tests/golden/` are unchanged;
`kafka_sre_rag/prompts.py` only gained lines (68 added, none removed: the grounded variants); `frozen.json` only
gained the three hashes of those variants, and the hashes of the default and strict prompts are the same;
`python -m kafka_sre_rag verify` reports the index, the prompts, the reranker and the catalogues as matching; the
golden tests pass. This checks the prompts and the retrieval settings. No answer was regenerated for it.

History, on a **legacy** question set (the 40 real questions of v3, reused many times; it is no longer a
gate and nothing is tuned on it):

- **v1** is published as an experimental preview. It is a fine-tuned model that did not pass
  its eval gate.
- **v2** is released as a retrieval-only adapter (see "Published models"): it is for use with
  retrieved excerpts and its own search terms, not on its own. Base model plus retrieval stays
  the recommended default.
- **v3 (legacy set):** retrieval on the unmodified base model is what works. On 40 real questions, scored
  by a local LLM judge in two passes, base model plus retrieval on the shipped index scores
  **+1.48** against the plain base model (standard error 0.21; 33 wins, 5 ties, 2 losses). The
  earlier measurement on the previous index was +1.19 (26 / 8 / 6). The fine-tuned adapters
  without retrieval score about zero (v1 +0.08, v2 -0.18). An adapter trained with retrieved
  excerpts in the prompt was stopped after a ceiling check. v3 therefore ships as retrieval on
  the base model; the v2 adapter is an option on top of it, not the default. The v3.1.0 evaluation above
  replaces this set as the measure; v3.1.0 does not change the index or the adapter.
- The size of the result: 40 questions, two judge passes, one judge model, standard error
  0.21. The +1.19 was measured on the previous version of the documentation index (6,292
  chunks); +1.48 is on the shipped one (6,444 chunks). On the 34 questions whose answers are
  identical in both measurements the two agree (+1.41 in each; per pass the same answers moved by +0.12 and -0.12, the judge's own variation). The whole +0.29 difference
  comes from the 6 questions whose retrieved excerpts changed with the index (-0.08 before,
  +1.83 now). Documentation was added to the index after some of these same questions showed
  no matching content, so that part of the gain is fitted to this question set: it shows the
  index now covers those topics, and is not evidence that retrieval improved in general.

## Published models

### v1 (experimental preview)

v1 is out as an experimental preview. Its automatic eval gate failed; read the model card
before using it, and verify every config and command before applying it to a cluster. The v1
weights are not what the v3 result is about: the v3 number is for the base model with retrieval.

- Article: [Kafka SRE agent, part 1](https://huggingface.co/blog/aywengo/kafka-sre-agent-part-1)
- MLX 4-bit: [aywengo/kafka-sre-27b-v1-preview-MLX-4bit](https://huggingface.co/aywengo/kafka-sre-27b-v1-preview-MLX-4bit)
- GGUF (Q4_K_M now; Q5_K_M and Q8_0 to follow): [aywengo/kafka-sre-27b-v1-preview-GGUF](https://huggingface.co/aywengo/kafka-sre-27b-v1-preview-GGUF)
- bf16, Hugging Face layout (upload in progress): [aywengo/kafka-sre-27b-v1-preview](https://huggingface.co/aywengo/kafka-sre-27b-v1-preview)

### v2 (retrieval-only adapter)

- Adapter only (LoRA weights, no base model):
  [aywengo/kafka-sre-27b-v2-retrieval-only](https://huggingface.co/aywengo/kafka-sre-27b-v2-retrieval-only)
- **The one rule of use: only with retrieved excerpts, and with its own search terms** (the
  runtime with `--adapter` does both). Without retrieval it scores -0.18 against the plain base
  model (40 questions, two judge passes).
- Result, from `runs/v3/V2RAG_CHECK.md` (40 real questions, two judge passes, one judge model,
  standard error 0.21): with its own search terms it scores -0.07 against base plus retrieval
  (15 wins, 6 ties, 19 losses), which is the same score within noise; with the base model's
  search terms it is behind, -0.29 (9 / 7 / 24). Its answers are about a quarter the length
  (median 1,208 against 4,714 characters). A reviewer's reading of ten answers (one reader, one
  pass) found an essential step dropped in 3 to 4 of them. Pick it for short answers, not for
  better ones; base plus retrieval remains the default. This leans on the 40 real questions
  again.

### Dataset

- Documentation index (the frozen 6,444-chunk file, with its attribution and checksums):
  [aywengo/kafka-sre-docs-index-v3](https://huggingface.co/datasets/aywengo/kafka-sre-docs-index-v3),
  published with v3.0.0 and unchanged in v3.1.0.

Feedback and review are welcome.

## Run with retrieval

What it does, for each question:

1. The model writes a line of search terms (config keys, tool names, error names) for the question.
2. BM25 searches the documentation index with the question plus those terms and keeps 40 candidates.
3. A cross-encoder reranks the candidates on CPU.
4. The top chunks (up to 6, within 14,000 characters) go into the prompt, and the model answers from them.

### Install

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

The reranker is `BAAI/bge-reranker-v2-m3`. It is downloaded from Hugging Face on first use
(through sentence-transformers) and runs on CPU. The runtime loads it at the revision in
`kafka_sre_rag/frozen.json`; `verify` compares that with your local cache.

### Get the index

The index is not in this repository (`work/` is git-ignored). Two routes.

Download the frozen index (about 10 MB) and check it:

```bash
hf download aywengo/kafka-sre-docs-index-v3 --repo-type dataset --local-dir idx
(cd idx && shasum -a 256 -c SHA256SUMS)
mkdir -p work/v3 && cp idx/chunks.jsonl work/v3/chunks_s1.jsonl
python -m kafka_sre_rag verify
```

Or build it from `sources.yaml`:

```bash
python scripts/fetch_sources.py                              # shallow clones into raw/
python scripts/chunk_docs.py --out work/v3/chunks_s1.jsonl   # the path frozen.json expects
python -m kafka_sre_rag verify
```

`verify` checks the index hash, the four prompt hashes and the reranker revision in your
cache against `frozen.json`. A real `ask` refuses to run if they do not match. On the
machine where the index was built, `verify` passes, and the downloaded index passes `verify` in
a fresh clone of the tag (end-to-end test, 2026-11-18). Not yet verified: that a fresh fetch and
chunk on another machine gives the same hash. If it does not, `verify` fails and you should not
trust the numbers below for your index.

The unit tests (`python -m unittest discover -s tests`) skip four tests that need the index when
`work/v3/chunks_s1.jsonl` is absent.

### Ask a question

In-process with mlx-lm (the path of the v3 measurements on the legacy set; the model is the bf16 base model wrapped
so that its chat template is non-thinking, see "How v1 was trained", section 4):

```bash
export BASE_PATH=$PWD/runs/v1/base27b
python -m kafka_sre_rag ask --sources "How do I check consumer lag?"
```

`--model` takes the model directory and defaults to `BASE_PATH`. Runtime check: on 3 of the 40
questions, the real path (mlx, base model, frozen index, k6, default mode) reproduced the stored
search terms, chunk ids and answers exactly. The same holds with the v2 adapter
(`--adapter <adapter directory>` on the same base model): on 3 questions it reproduced the stored
search terms, chunk ids and answers. These checks were run with mlx-lm 0.32.2 and mlx 0.32.4
(`requirements.txt` does not pin them).

With an OpenAI-compatible server (a `/v1/chat/completions` endpoint):

```bash
export OMLX_URL=<your server>/v1          # or pass --base-url
python -m kafka_sre_rag ask --backend openai --model <model name on the server> "How do I check consumer lag?"
```

`OMLX_API_KEY` is sent as a bearer token if it is set. The client sends temperature 0 and strips
a `<think>` block from the reply, but it cannot switch thinking off for you: pass the model's
switch with `--extra-body`, for the base model `--extra-body '{"chat_template_kwargs": {"enable_thinking": false}}'`.
A reasoning model that is left thinking needs a larger `--max-tokens` instead (the run is then
unmeasured), and when the model returns no answer text the runtime exits with an error that says so.
The server backend was run end to end as a path check with timing, not a quality measurement: base
model `mlx-community--Qwen3.8-27B-bf16` served by the local model server (full-precision build),
thinking off, 3 questions. They took 134 to 212 s per question in default mode and 77 to 117 s in
strict mode. The output was **not** identical to the mlx path: the search terms diverge after 260
to 440 characters; that changes the order of the excerpts (on one of 3 questions also two of
them), and the answers differ from the first sentence. The measured results
of v3 (the +1.48 and +1.19 on the 40 legacy questions) therefore apply to the mlx path. The v3.1.0
evaluation (see "Status") was run through the server path, with the 4-bit build of the base model
(`Qwen3.8-27B-4bit`), thinking off, and that is the measured reference configuration of v3.1.0. Because
`frozen.json` still names the bf16 model as the reference of the v3 runtime, a run on the 4-bit build prints
`UNMEASURED CONFIGURATION: generator is not the reference model`; that line is expected there and is not a
fault. An earlier path check with another model (GLM-5.3-Flash-oQ8e-mtp, `--max-tokens 16000`) also produced answers.

A second path check, from the published artefacts in a fresh clone of the tag (2026-11-18): the MLX
4-bit build of the base model, `mlx-community/Qwen3.8-27B-4bit` (about 16 GB), downloaded and
served by the local model server, thinking off, ten questions per mode. The median was 70.8 s per
answer in default mode (65.0 to 88.9 s) and 47.2 s in strict mode (36.8 to 64.6 s), and the server
held about 16.9 GB by its own estimate. 20 of 20 answers were non-empty and not cut off. This is a
path check with timing on a large machine, not a quality measurement and not the reference laptop.
If the server lists its models, the client checks the `--model` name against the list and stops
with an error when the server does not have it, instead of letting the server answer with another
model. For a server on the same machine (`localhost`, an address in 127.0.0.0/8, or `::1`) the runtime
ignores the proxy environment variables (`HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY`), whatever `NO_PROXY`
says, so a prompt never goes to a proxy; for a server elsewhere on your network, add its host to `NO_PROXY`.

**Ollama.** Tested with Ollama 0.40.16, which works; 0.14.2 cannot load Qwen3.8 (the library model's own
metadata says it needs 0.32.12 or later, which was not tested, so the minimum version is not established).
Do the steps in this order. First start the server, with a context that holds the prompt and the answer (a
prompt is up to about 4,100 tokens and the answer up to 2,048). If an Ollama server is already running, stop
it first, so that the context setting takes effect:

```bash
OLLAMA_CONTEXT_LENGTH=16384 ollama serve
```

Then, in a second terminal, pull the base model by its exact name and ask (`ollama pull` needs the running
server):

```bash
ollama pull qwen3.8:27b                     # 18.2 GB, Apache-2.0
ollama ps                                   # after the first answer: the CONTEXT column shows 16384
export OMLX_URL=http://localhost:11434/v1
python -m kafka_sre_rag ask --backend openai --model qwen3.8:27b --extra-body '{"reasoning_effort": "none"}' "How do I check consumer lag?"
```

At the time of the test the library model `qwen3.8:27b` is an NVFP4 build on Ollama's MLX runner, not a
Q4_K_M GGUF. A Q4_K_M GGUF from the Hub, `ollama pull hf.co/lmstudio-community/Qwen3.8-27B-GGUF:Q4_K_M`
(17.7 GB, llama.cpp runner), also loads and answered through the runtime once, but was not timed. Ollama's
documented default context is 4,096 on machines under 24 GiB of video memory. With that context the two
runners behaved differently in the test: the llama.cpp runner enforced it and cut an answer off at the limit
(finish_reason `length`; a longer prompt got a clear HTTP 400), the MLX runner did not enforce it. Do not
rely on that; set the context. Use the exact name that `ollama list` shows.

`reasoning_effort: "none"` turns thinking off on Ollama's OpenAI endpoint; `{"think": false}` is ignored
there. The runtime refuses a model name that `ollama list` does not show: the short alias `qwen3.8`, which
Ollama itself resolves, is refused with a message that lists the available names; use `qwen3.8:27b`.
Path check with timing, not a quality measurement: ten questions per mode with `qwen3.8:27b` and a
16,384 context, median 38.6 s per answer in default mode (31.0 to 46.1 s) and 26.7 s in strict mode
(18.3 to 32.1 s). `ollama ps` showed about 19 GB idle and 27 GB as the largest value seen during answers,
which is above the 20 GB bar in `GOAL.md` (the 27 GB is the largest of about 30 samples, not a measured
peak). One strict answer was a one-sentence refusal. Answers on this path were not repeatable at
temperature 0 in one observed case (one strict run of one question gave a 28-token refusal, two further runs
467 and 528 tokens). Quality was not measured. The machine is not the reference laptop, and the Ollama and
the oMLX path above use different 4-bit builds, so the figures are not a comparison of the two servers.

`chat` is the same loop, one question at a time. Each turn is retrieved and answered on its
own; no conversation history is sent, and multi-turn use is not measured.

To see exactly what would be sent, without a model:

```bash
python -m kafka_sre_rag ask --dry-run --no-rerank-in-dry-run "How do I check consumer lag?"
```

This was run and works: it prints the search-term request and the answer request with the
retrieved excerpts. `--no-rerank-in-dry-run` skips the cross-encoder, so the excerpts shown
are in BM25 order.

Other options: `--k k3` (3 chunks, shorter prompt), `--json` (one JSON object with the text,
sources, search terms, timings and the reasons the run is unmeasured), `--adapter <dir>`
(mlx only).

### Run as a local service

`ask` starts a process, loads the index, builds the BM25 structures and loads the reranker for every question.
`serve` does that once and keeps it:

```bash
# OMLX_URL set as in the server-backend part above
python -m kafka_sre_rag serve --backend openai --model qwen3.8:27b --extra-body '{"reasoning_effort": "none"}'
# in another terminal
python -m kafka_sre_rag ask --service-url http://127.0.0.1:8765 --stream --sources "How do I check consumer lag?"
curl -s http://127.0.0.1:8765/healthz
```

The service takes the same model options as `ask` (`--backend`, `--model`, `--adapter`, `--base-url`,
`--extra-body`, `--max-tokens`, `--mode`, `--k`), fixed at start. It speaks the OpenAI chat format, so a chat
front end or an editor plug-in can point at `http://127.0.0.1:8765/v1`: `POST /v1/chat/completions` (also with
`"stream": true`, server-sent events), `GET /v1/models` (`kafka-sre` and `kafka-sre-strict`; a request can also
carry `"mode": "strict"`) and `GET /healthz` (index hash, reranker revision, backend, model, whether `verify`
passed at start). The last user message is the question; every request is retrieved and answered on its own and
no conversation history is sent (the response says so in the `X-Kafka-Sre-History` header and the `kafka_sre`
field, which also carries the sources and chunk ids). One request at a time reaches the model; the rest wait.

**Loopback only.** The service binds to `127.0.0.1` and refuses any other address; `--allow-non-local` overrides
that and prints a warning, because `GOAL.md` requires that nothing leaves the machine. Requests whose `Host`
header is not a loopback name are refused too.

`ask --stream` prints the answer as it is generated, with or without a service, and then the sources; a
`<think>` block is not shown. The final text is the same as without `--stream`.

What the service keeps loaded: the index with its BM25 structures and the reranker (about 3.8 GB resident
when idle, after 11 requests, `ps` rss, not a peak; 7.1 s to start; retrieval inside a running service takes
about 4.4 s per question against about 9.5 s for a fresh `ask`, 10 questions, no model; `runs/m4/reports/wp3a_service.md`).
The model itself stays in the model server (the 4-bit build held about 16.9 GB there). Times per answer
through the service with a real model are in "Status" (speed) and "Timings".

### Checking an answer

`ask --check` marks, in place, identifiers in the answer that do not exist in the pinned versions. It is
off by default. It checks against **catalogues** built from the pinned sources (Apache Kafka 4.3.2 configuration
reference, command-line tools and protocol errors; Strimzi 1.3.1 custom resource schemas; Argo CD v3.6.1; the
Kubernetes v1.37.2 API) and against the excerpts the answer was written from. It looks up names; it **cannot see a
wrong statement about behaviour** that uses real names.

The catalogues are not in the repository (`raw_catalogue/` and `work/` are git-ignored). Build them once, after
fetching the documentation sources (see "Get the index"); the second fetch clones five public repositories
(about 10.5 MB, no token needed) and the build reads only files on disk:

```bash
python scripts/fetch_sources.py                 # if raw/ is not there yet
python scripts/fetch_catalogue_sources.py       # the catalogue sources of catalogue_sources.yaml -> raw_catalogue/
python scripts/build_catalogues.py              # -> work/m4/catalogues/ (CATALOGUES.json holds the hash)
python -m kafka_sre_rag verify                  # its last line shows the catalogue hash, or "catalogues: not built"
python -m kafka_sre_rag ask --check --sources "How do I check consumer lag?"
python -m kafka_sre_rag serve --check ...       # the same for every request; or {"check": true} in one request body
```

The catalogues built here had the hash `e319653a5432...` (first 12 characters) in the evaluation. The build was tested from a fresh
clone at `v3.1.1`: it gave the same hash on the same machine, in another directory. That a build on another
machine gives the same hash was not checked. The sources (`raw/`, `raw_catalogue/`) are real directories under
the repository; the build reads them by lexical path, so symlinks do not change the hash. `--check` stops with an error that says how to
build the catalogues when the directory is missing.

`--revise` (with `--check`) is a model patch step: one more call to the model that rewrites the lines the check
flagged. It exists, is **off by default and experimental**. It was read on the 30 open default answers of one
batch (development data, so the figures flatter it): the first reading found that it removed no counted
(non-minor) error; after changes, a second reading found 2 removed and 1 introduced (net +0.3 per 10 answers),
and the one it introduced was a harmful command (it deleted a flag that narrowed a reset to one partition). It
was not tested on the hidden half (`runs/m4/reports/wp2b_t4_patch_reading.md`). Do not use it for answers you will
act on.

After the answer is complete, the checker looks up the names in it (configuration keys and their values, fields of
Strimzi resources, command-line tools and flags, REST paths, error names, annotations) in the catalogues and in the
excerpts the answer was written from, and prints a short note under the answer. The note has three kinds of lines:

- `Could not confirm: ...` for a name that does not exist (`invented`: the line gives the nearest real name when there
  is one) or a real name used wrongly (`wrong_use`: for example a value or an HTTP method the reference does not allow).
- `Not checked, no reference covers them: ...` for names the catalogues cannot judge (`unverified`). This is
  information, not an accusation.
- When there is nothing to report, one line says that the names were confirmed and how many were checked, so that
  silence is not mistaken for "not checked".

The answer and the prompt are not changed; the note is separate (in `--json` output it is the `check` field with the
findings and the catalogue hash; the service appends it to the answer text after a blank line, so any chat front end
shows it, and also returns it in the `kafka_sre` field). `--catalogues DIR` points at another catalogue directory;
`--check` stops with an error that says how to build them when the directory is missing, and `verify` shows the
catalogue hash, or `catalogues: not built`.

What it can and cannot do: it catches made-up names and wrong values; it does not catch a wrong statement that uses
real names. On the 30 errors that readers had found in stored answers (a set the checker's rules were developed on) it
caught 7, reported 5 as unverified and did not catch 18, of which 15 were wrong statements that use real names and 3
were names a catalogue could in principle judge but this one does not. It also gave no false `invented` finding on 244
stored answers, again the set it was developed on. It was not measured separately on fresh answers; its effect on the number of wrong statements is not
established, and the blind reading in "Status" (17.2 per 10 answers) was made on the answers as first written, before any
mark (`runs/m4/reports/wp2b_checker.md`). On 60 open judged default answers the same identifier lookup, run as a
separate program, found 21 nonexistent identifiers in 11 answers (3.5 per 10 answers; `runs/m4/reports/e5_screen.md`);
identifiers it cannot decide are not counted.

### Modes

- **default:** the prompt that was measured, and the default. Its answers are longer and
  invent or misstate more (table below).
- **strict** (`--mode strict`): adds one sentence to the user message: "Use only commands,
  flags, configuration keys, resource fields and annotations that appear in the excerpts. If
  the excerpts do not cover part of the question, say so in one sentence instead of filling the
  gap."
- **Prompted answer rules:** adding answer rules to the prompt without an adapter scored -0.15
  against base plus retrieval on the 40 questions (standard error 0.16; 9 wins, 16 ties, 15
  losses), so no such prompt ships.

Evidence on the two modes, first from the v3 readings on legacy sets (one reader, one pass; invented or wrong
items are counted from a reviewer's reading against the excerpts, not by a program), then from the v3.1.0 hidden half.

| | Default | Strict |
|---|---|---|
| Invented or wrong items, 10 real questions read in full | 17 in 7 answers | 1 in 1 answer |
| Invented or wrong items, 10 holdout questions, ten fixed positions (1, 5, 10, 15, 21, 24, 28, 31, 37, 41), apart from the ten shortest strict answers | 3 in 1 answer | 0 |
| Essential omissions, wrong refusals (10 real) | not assessed | 2 omissions (1 more partial), 1 wrong refusal |
| Essential omissions, wrong refusals (10 holdout) | 0 (1 partial), 0 | 0 (2 partial), 0 |
| Judged score of strict minus default | - | -0.11 on the 40 real questions (Phase 0), -0.05 on the 42 holdout questions (standard error 0.14); both within noise |
| Mean answer length | 1,217 tokens (40 real), 459 (42 holdout) | 516 tokens (40 real), 160 (42 holdout) |

What this means. The project's bars (`GOAL.md`, on questions not used to build or tune the
assistant): at most 2 invented or wrong items per 10 answers and none that would do damage; at
most 1 wrong refusal and at most 1 dropped essential step per 10. On the 10 real questions (a
set reused many times): default has 17 items in 7 answers, one of them damaging; strict has 1
item, but drops an essential step in 2 answers (1 more partly) and wrongly refuses 1. On the 10
holdout questions: default has 3 items in 1 answer, one probably damaging; strict has none, and
no omissions (2 partial). Those readings are small (10 questions each) and the first set was reused many times.
The larger, blind reading of the v3.1.0 hidden half (61 scenarios, 4-bit) found a smaller gap: strict 14.6 against
default 17.2 non-minor wrong, invented or harmful statements per 10 answers (difference -2.62, 90 % interval -5.57
to +0.33, so not clearly different from zero), 11 harmful against 13. On the 305 hidden mechanical items strict
passed 5.25 points fewer (75.7 % against 81.0 %), and judged coverage was the same within noise (-0.02). So strict
may lessen invented names but does not make an answer safe: **check every answer, in either mode, and check
that no step is missing.** The price of strict is that when retrieval misses the page, it can give a non-answer
("the excerpts do not cover this") where default writes something.
Default stays the default because it is the mode every headline number was measured in. Sources: `runs/v3/reports/`
(`strict_mode_invention_reading.md`, `strict_holdout_reading.md`) and `runs/v3/STRICT_HOLDOUT_CHECK.md`.

### Experimental options (off by default)

These came out of milestone M5. None of them is the default and every answer from them is marked unmeasured.

- **`--mode grounded-g2`.** It replaces the answer instruction with one that asks the model to base every statement
  on the excerpts or the question, to write names as they appear there, and to say in one fixed sentence what the
  excerpts do not cover; general knowledge may follow only on lines marked as such. It needs excerpts and does not
  work with `--service-url` or `serve`. The measured trade on 54 development scenarios (development data, not the
  hidden half): about 0.07 less judged coverage (0.476 against 0.545); 3.9 fewer non-minor wrong, invented or
  harmful statements per 10 answers (90 % interval -6.85 to -0.56, from a blind reading of 108 answers); about half
  the invented identifiers (2.22 against 5.37 per 10); answers of about 900 tokens (median 902) in about 42 s with
  search, against 2,048 tokens and 76 s; harmful statements were not reduced (6 against 5 in total). **It failed the
  gate to become the default** (coverage more than 0.05 under default) and is not the default.
- **`--mode grounded-g1`, `--mode grounded-g3`.** Two other variants that were also checked on the same 54
  scenarios: G1 lost 0.122 coverage, G3 lost 0.052 and also failed on supported essential misses (2.50 per 10 against 1.00 for default).
- **`--profile r4`.** An experimental retrieval profile: the frozen six chunks, then up to four more ordered by a
  second reranker call (10 chunks, up to 24,000 characters). It failed its rule fixed beforehand (see M5 above), so
  `frozen` stays the default. `--profile r4` runs the placeholder values (grid setting G4 in
  `kafka_sre_rag/candidate.json`); the best grid setting of the development run, `r4g2`, is in the same file under
  `r4_grid` and is used by the funnel scripts.
- **The second reranker's offline test** is a script, `scripts/r5_qwen.py`, not a runtime option; it needs the
  Qwen3-Reranker-0.6B model, which is not part of the install.

### Sources

`--sources` prints the retrieved chunks under the answer: source, title, file path, BM25 rank
and reranker score. Read them. If the excerpts are not about your question, the answer is
not grounded in them.

### UNMEASURED CONFIGURATION

If a run differs from the configuration behind the headline number, a line
`UNMEASURED CONFIGURATION: <reasons>` is printed (and `measured: false` is in the JSON). The
reasons are: mode other than default, `--k k3`, a generator that is not the reference model
(the bf16 base model, no adapter; see the note on the 4-bit build above), a retrieval profile other than `frozen`,
a different index file, or no reranker. It does not mean
the run is broken. For `k3` it means the number was measured on an earlier
index, not on this exact setup. For `strict`, the 40-question figure is from the earlier index;
on the shipped index it was measured on the 42 holdout questions only. For another model, a 4-bit or GGUF build, or a server, it
means nobody has measured it.

### Timings

Through the streamed service, 4-bit build on the model server, Mac Studio, default mode, per question (median of
the whole answer, including search terms, retrieval and answering; `runs/m4/reports/e4_first_words_run.md` for the
open runs, `runs/m4/E2_STARTING_POINT.md` for the hidden ones):

| Questions | Median per answer | Median to the first words | Cut at 2,048 tokens |
|---|---|---|---|
| 60 open short (mechanical) questions | 26.1 s | 19.2 s | 3 of 60 |
| 60 open judged scenarios | 90.3 s | 22.9 s | 39 of 60 |
| 305 hidden mechanical items (no first-word timing) | 24.9 s | - | 8 of 305 |
| 61 hidden judged scenarios (no first-word timing) | 81.8 s | - | 36 of 61 |

An answer that runs to the 2,048-token limit needs about 70 s at the observed 29 tokens per second (inferred from the
rate), which is why long scenarios miss the 60 s goal. Full precision on the 305 hidden mechanical items: 48.2 s
median, about twice the 4-bit time. Another client's model was loaded on the server during the open timing run
(effect not known).

Phase 0 of v3 (bf16 model through mlx-lm on the same kind of machine, 40 legacy questions): search terms about 8 s,
BM25 and rerank on CPU about 6 s, answer about 1.6 minutes (3 chunks) to 1.9 minutes (6 chunks); about 2 minutes in
total. No laptop was measured.

### What to expect

- It still invents some flags, fields and commands in the default mode. Verify every command
  and every config key before you run it on a cluster.
- It gets worse on questions the documentation index does not cover. On the three test
  questions the earlier index could not answer (29, 37, 38), retrieval scored 1.0 below the
  plain base model. Pages on these topics were then added; on the shipped index the same three
  score +2.17 on average (2 passes). That gain is fitted to these questions. On a topic the
  index does not cover, expect answers from unrelated excerpts or from memory. The index covers
  Apache Kafka, Strimzi, Argo CD and the Kubernetes concepts and tasks pages, plus a few
  Schema Registry, cAdvisor and runbook pages. It has little on Schema Registry and on CPU
  throttling.
- Answers in the default mode are long (about 1,200 tokens on average on the 40 legacy questions, 2 of 40 at
  the 2,048-token limit); on the judged scenarios of v3.1.0, 36 of 61 hidden and 39 of 60 open answers were cut at
  that limit.
- Some default-mode answers carried wrong advice, found by a reviewer's reading of 20 answers: a
  statement that the offset-reset time is read in the broker's timezone (in Kafka 4.3.2 a
  `--to-datetime` value without an offset is read as UTC; write the offset explicitly, for example
  `2026-11-18T09:00:00.000+01:00`); a statement that NetworkPolicies are additive in the sense that one policy that
  does not allow the traffic blocks it (the reverse holds: allowed traffic is the union of what every policy selecting the pod
  allows; the source pod's egress must allow it too); and clearing a replication throttle by
  setting the rates to 0 (0 is the tightest throttle, not a clear: the rate is an upper bound in
  bytes per second for the throttled replicas; the Kafka documentation removes the throttle with
  `kafka-reassign-partitions.sh --verify` after the reassignment completes, or remove the
  throttle configs with `kafka-configs.sh --alter --delete-config`). Check such details.
- Retrieval missed the source page on at least 5 of the 42 holdout questions (13, 14, 22, 34,
  41): the source page was not among the excerpts. Strict mode then can give a non-answer (questions 13, 14, 34);
  default may fill the gap from memory.
- Time per answer: at 4-bit through the service, about 25 s for a short question and 80 to 90 s for a long
  scenario, first words after about 20 s (see "Timings"); about 2 minutes in Phase 0 with bf16;
  112, 151 and 170 s in the one end-to-end check (3 questions, default mode, bf16 27B through mlx-lm).
  `GOAL.md` asks for a median under 60 s; that is not met for long scenarios. The first words within 10 s
  was the bar of the milestone, not of `GOAL.md`, and it is not met at all. Strict mode is shorter and quicker (77 to 117 s on the server path check).
  On the 4-bit server path check (10 questions per mode, large machine): median 70.8 s in default mode,
  47.2 s in strict mode.
- The documentation snapshots are fixed (Kafka 4.3.2, Strimzi 1.3.1, Argo CD v3.6.1).
- English only. Tested on short questions and on judged scenarios of a paragraph or two, not on long incident threads.

### The evaluation, and how to run its open half

v3.1.0 comes with a new evaluation of 644 mechanical items (a program scores each: commands, manifests, error
names, numbers) and 121 judged scenarios (a judge model scores coverage of required points). Half of it is hidden.

In the repository:

- **Open half:** `eval/m4/open/tier_a.jsonl` and `eval/m4/open/tier_a3.jsonl` (251 and 88 mechanical items, 339 in
  all) and `eval/m4/b1/` and `eval/m4/b2/` (30 judged scenarios each, items, rubrics and second-review changes).
  These were used to build and tune the runtime, so they are development data: numbers on them are not the gate.
- **Hidden half:** 305 mechanical items and 61 scenarios, committed **encrypted** under `eval/hidden/`. They cannot
  be read without a key that is not in the repository; the harness prints totals only. They are not described here.
- **Harness:** `scripts/eval2.py` (answers and manifest), `scripts/eval2_score.py` (mechanical scorer),
  `scripts/eval2_idcheck.py` (nonexistent identifiers), `scripts/eval2_judge.py` and `scripts/eval2_compare.py`
  (judged scenarios and the decision rules; the judge needs a second, stronger model on a server and is not
  covered here), `scripts/eval2_first_words.py` (timing through the service).

- **Open batch B4:** `eval/m4/b4/` holds 30 new judged scenarios with items and rubrics, written for one
  out-of-sample check of a retrieval candidate; it was not used in M5. Each must point records its covering sets: one
  or two chunks that together state the whole fact (170 must points: 140 with at least one covering set, 14 with no
  covering set in the index, 16 not indexed). The strict count calls a point served only when a whole covering set
  is in the pack; the loose count of the earlier batches B1 and B2 calls it served when any recorded chunk is, so
  it overstates (on B4 an independent chunk-by-chunk check found 98 of 154 points fully covered by each recorded
  chunk, which is why the sets were added). Its rule is fixed beforehand and it is used once.

To run the mechanical open half on your own setup (needs the index, the catalogues and a model server as above; the
answers file must be a new path, an existing one is resumed):

```bash
export OMLX_URL=<your server>/v1
python scripts/eval2.py answer --items eval/m4/open/tier_a.jsonl eval/m4/open/tier_a3.jsonl \
  --out out/my_run/answers.jsonl --config my-run --backend openai --model <model name on the server> \
  --extra-body '{"chat_template_kwargs": {"enable_thinking": false}}' --catalogue work/m4/catalogues
python scripts/eval2_score.py --items eval/m4/open/tier_a.jsonl eval/m4/open/tier_a3.jsonl \
  --answers out/my_run/answers.jsonl --scores out/my_run/scores.jsonl --summary out/my_run/summary.json
python scripts/eval2_idcheck.py --answers out/my_run/answers.jsonl --totals-only
```

The first command writes one line per answer as it goes and can be stopped and resumed (339 answers at the
24.9 s median measured on the hidden mechanical items come to about 2.3 hours on the Mac Studio, an estimate). The
second prints the share of items where every check passes. The commands were checked against the scripts' options
with the stub backend on three items (no model); a full run with a model was not repeated for this README.

**Retrieval profiles.** `--profile r1`, `r2`, `r3` and `r4` (on `ask`, `chat`, `serve` and `eval2.py answer`) select
candidate retrieval settings (`kafka_sre_rag/candidate.json`). They are **experimental**: none passed its
acceptance rule fixed in advance (R3 gained 7.4 points on one open batch against the 15 required, the earlier
check of R2 failed, and R4 stopped short of its bars on both development halves, see "M5" above), so `frozen` stays the default and every answer with a candidate profile is marked unmeasured.

### Other base models tried

A bake-off of other base models ran on the open half only (339 mechanical items and 60 judged scenarios per model,
the same prompts, retrieval, greedy decoding and 2,048-token limit; no hidden run; `runs/m4/reports/e5_screen.md`).
The reference on the same items: 255 of 339 and judged coverage 0.563 (60 scenarios), median 86.0 s per judged answer.
None replaced Qwen3.8-27B at 4-bit:

- **Gemma 4 26B-A4B** (4-bit, 15.26 GB loaded): 247 of 339 (-2.36 points) and coverage 0.463 (-0.10 against the
  allowed -0.08), but 24.6 s per judged answer and no answer cut at the limit. Dropped on coverage. It is the candidate
  for a faster setup later.
- **GLM-4.7-Flash** (4-bit, 16.48 GB): 202 of 339 (-15.63 points), coverage 0.214 (-0.349), 21 of 339 answers cut
  at the limit, 6 of 20 repeat answers differed. Dropped.
- **Granite 4.2 30B:** median 100.4 s per judged answer against the limit of 98.9 s (1.15 times the reference's), 51 of
  60 answers cut at the limit. Dropped on speed; its mechanical run was not made.
- **Gemma 4 31B** was left out: loaded alone it holds 18.0 GB, above the 16.7 GB the project allowed for the model.

### Laptop

The measurements on a 32 to 36 GB laptop (memory, speed, offline use, install time) are **not done yet**. The plan,
with the script that does them, is `agents/laptop-tests.md`. All figures in this README are from a Mac Studio.

## How v1 was trained (fine-tuning pipeline)

The sections below describe the fine-tuning pipeline that produced the v1 adapter. v3 does
not use an adapter.

```
sources.yaml ─► fetch_sources.py ─► chunk_docs.py ─► generate_qa.py ─► build_dataset.py
                  raw/                work/chunks.jsonl  work/raw_qa.jsonl   data/*.jsonl
                                                                                 │
              publish.sh ◄── eval/compare.py ◄── mlx_lm.lora -c configs/lora.yaml ┘
```

### 0. Setup

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
hf auth login                      # write token from huggingface.co/settings/tokens
```

For GGUF export, clone and build llama.cpp separately (`cmake -B build && cmake --build build -j`).

### 1. Collect and chunk docs

```bash
python scripts/fetch_sources.py            # sparse, shallow clones into raw/
python scripts/chunk_docs.py               # -> work/chunks.jsonl
```

Put your own runbooks/postmortems in `private/runbooks/` (scrub secrets, hostnames and
customer data first). They are usually the highest-value data you have.

### 2. Generate training data

Serve a strong instruct model locally as the "teacher". With 512 GB you can run a
large one; generation quality matters far more than speed here.

```bash
mlx_lm.server --model <teacher-model> --port 8080
# test on a small slice first and read the output by hand
python scripts/generate_qa.py --limit 30 --verify
less work/raw_qa.jsonl
# then the full run (resumable — rerun to continue)
python scripts/generate_qa.py --verify --concurrency 2
```

Check the teacher model's license: some forbid using outputs to train other models.
LM Studio (`--url http://localhost:1234/v1`) or Ollama (`--url http://localhost:11434/v1
--model <name>`) work too.

### 3. Build the dataset

```bash
python scripts/build_dataset.py                       # -> data/{train,valid,test}.jsonl
# optional: mix in general chat data to limit forgetting
python scripts/build_dataset.py --general general.jsonl --general-ratio 0.15
```

It prints a suggested `iters` value; put it in `configs/lora.yaml` (both `iters` and the
second number in `lr_schedule.arguments`).

### 4. Train

```bash
mlx_lm.lora -c configs/lora.yaml
mlx_lm.lora -c configs/lora.yaml --train false --test   # test-set loss
```

Watch validation loss. If it bottoms out and climbs, stop and use the best checkpoint
(`adapters/kafka-sre-v1/<step>_adapters.safetensors`).

`BASE_PATH` is the base model directory. v1 uses mlx-community/Qwen3.8-27B-bf16 at
revision 6f265714824f3c38d4452baa1628aef3d9b9aae9, wrapped so that the chat template is
always non-thinking (weights are symlinked, only `chat_template.jinja` is patched):

```bash
SNAP=~/.cache/huggingface/hub/models--mlx-community--Qwen3.8-27B-bf16/snapshots/6f265714824f3c38d4452baa1628aef3d9b9aae9
python scripts/make_nothink_base.py "$SNAP" runs/v1/base27b
export BASE_PATH=$PWD/runs/v1/base27b
```

Training (`runs/v1/lora.yaml`), `eval/compare.py --model` and `scripts/publish.sh`
(`BASE=$BASE_PATH`) all use this directory.

Quick manual check:

```bash
mlx_lm.generate --model "$BASE_PATH" --adapter-path adapters/kafka-sre-v1 \
  --prompt "Strimzi rolling update is stuck because of under-replicated partitions. What now?"
```

### 5. Evaluate

Extend `eval/questions.jsonl` to ~200 questions written by you (not generated from the
same docs), then:

```bash
python eval/compare.py --model "$BASE_PATH" --adapter adapters/kafka-sre-v1 \
  --judge-url http://localhost:8080/v1
```

Read the answers yourself too — judge scores are a rough signal, invented config keys
are what matters most in this domain.

### 6. Publish

Fill in `MODEL_CARD.md` (commits are in `raw/*/.source-commit`, eval scores, counts), then:

```bash
HF_USER=<you> NAME=kafka-sre-14b-v1 bash scripts/publish.sh
```

This uploads three repos: full-precision safetensors, an MLX 4-bit variant and GGUF
quants (Q4_K_M, Q5_K_M, Q8_0).

### Rough budget

| Step | Time on M3 Ultra |
|---|---|
| Generation, ~20k chunks with verify pass | 1–3 days, depends on teacher size |
| LoRA, 14B bf16, ~20k examples, 2 epochs | several hours to ~1 day |
| LoRA, 32B | roughly 2–3× the 14B time |

Start with a small run end to end (a few hundred examples, 200 iters) to shake out the
pipeline before the long runs.

## Contributing

Proven runbooks and remediations of real incidents are most welcome. Open an issue or a
discussion first; see [CONTRIBUTING.md](CONTRIBUTING.md).

## Licence

The code in this repository is licensed under the Apache License 2.0 (see `LICENSE`).

The documentation index is a mixed-licence collection: Apache-2.0 for nine of the sources (eight upstream projects and the project's runbooks) and
CC BY 4.0 for the Kubernetes documentation. The attribution is in `NOTICE`.

The base model's weights are under their own licence; see the model card (`MODEL_CARD.md`).
