# Laptop tests: plan for the Owner

Purpose: measure the real bars of `GOAL.md` ("What fits on a working laptop means") on a 32 to 36 GB Apple
Silicon laptop. You run the steps; the script `scripts/laptop_bench.py` times, samples memory, writes one folder, and
`bundle` builds a scrubbed folder (and `SUMMARY.md`) to send back.

The question sets are **open development items** (`eval/m4/laptop/ids.json`: the 30 judged scenarios of
`eval/m4/b1/items.jsonl`, and 60 open mechanical items of `eval/m4/open/tier_a.jsonl`, drawn over the nine kinds
among the items the Mac Studio's 4-bit run answered). Nothing under `eval/hidden/` is used. Laptop numbers on these
items are speed, memory and scorer-agreement measurements, not quality decisions.

## What is measured, and the bar

| # | Question | Measured by | Bar (`GOAL.md`) |
|---|---|---|---|
| 1 | Install | `install-check --run`, per step; the model download is reported apart, with its size | one command sequence, under 30 min |
| 2 | Offline | every child process runs with `HF_HUB_OFFLINE=1` and `TRANSFORMERS_OFFLINE=1`; non-local sockets of our processes are looked for with `lsof` during the runs; the default route is recorded (Wi-Fi off) | nothing needs the network after install |
| 3 | Memory | resident memory (`ps`) of oMLX, the service or the one-shot process; system used memory (`vm_stat`), swap (`sysctl vm.swapusage`), pressure (`sysctl kern.memorystatus_vm_pressure_level`, `memory_pressure`); weights on disk | about 20 GB held; weights 15 to 17 GB (an estimate to verify) |
| 4 | Speed | wall time per answer, time to the first words (service, streamed), tokens per second, the split search terms / retrieval and rerank / answering; default and strict; with and without `--check`; cold and warm; service against one-shot | median under 60 s in default mode; first words reported, not gated |
| 5 | Sustained use | 30 questions in a row, on power and on battery; `pmset -g batt` and `pmset -g therm` before, every 5 questions, and after | no gate in `GOAL.md`; the script flags a last-10 median more than 20 % above the first-10 median (a threshold set here) |
| 6 | A working laptop | the same short set while your usual applications are open | same 60 s bar |
| 7 | Same answers as the reference machine | the mechanical scorer on 60 open items against the stored result of the Mac Studio's 4-bit run (`s6-A1`) | difference under 5 points is flagged as a pass; a larger one means the build or the quantisation differs |

For comparison, on the Mac Studio (oMLX 0.8.0, `Qwen3.8-27B-4bit`, 3 to 10 questions per figure, not the reference
laptop): load 6.4 s; the server held 15.7 GB with the model loaded alone (its own estimate; 16.9 GB in a later
check); answer step median 34.9 s at 22 to 25 tokens/s; retrieval and rerank 8.5 to 11 s per question in a fresh
`ask`, 4.4 s when the index and reranker stay loaded; median 70.8 s per answer in default mode and 47.2 s in strict
mode (10 questions each, one-shot `ask`). The retrieval and the model are the same on the laptop; the laptop will be
slower per token.

## Before you start

- Apple Silicon Mac, 32 to 36 GB, macOS recent, Python 3.10 or newer (`python3 --version`), `git`, 45 GB free disk.
- Power adapter connected, battery above 90 %, the laptop idle and cool, no models loaded by other tools.
- A choice of serving option (A or B below). You can do both; use a separate `--out` folder for each.
- On the Mac Studio, once (the catalogues are not published; they are needed for `--check` and for the scorer):
  `tar czf catalogues.tgz -C work/m4 catalogues`. Copy that file to the laptop. Compare the hash line printed by
  `python -m kafka_sre_rag verify` with `work/m4/catalogues/CATALOGUES.json` there.
- The files of this work package (`agents/laptop-tests.md`, `scripts/laptop_bench.py`, `tests/test_laptop_bench.py`,
  `eval/m4/laptop/ids.json`) must be in the checkout you use (committed by the conductor, or copied).

All commands run from the repository root. Use one output folder for the whole session; the options you give to the
first command are stored in `<out>/config.json` and the later phases reuse them. Below, `OUT=out/laptop/run1`.

## Serving options

**A. oMLX with the 4-bit model (the reference setup of `GOAL.md`).** The runtime's OpenAI-compatible backend talks to
oMLX at the address from the environment.

- Install and start oMLX as its own documentation says (not documented in this repository, not verified here), with
  its model directory pointing at the folder that holds the 4-bit model (step 3 downloads it to
  `models/Qwen3.8-27B-4bit`).
- `export OMLX_URL=http://localhost:<port>/v1` (the port oMLX shows) and, if oMLX requires one, `export OMLX_API_KEY=...`.
  Neither value is written to any file.
- The model name is the id oMLX lists: `curl -s $OMLX_URL/models` (add `-H "Authorization: Bearer $OMLX_API_KEY"` if
  needed). On the Mac Studio it was `Qwen3.8-27B-4bit`; on your laptop it may differ, so it is the parameter
  `--model`. The runtime itself stops with a list of names when the name is not on the server.
- Thinking is switched off through the request: the script's default is
  `--extra-body '{"chat_template_kwargs": {"enable_thinking": false}}'` (the form used in the S6 runs). Pass another
  `--extra-body` only if your oMLX needs it.
- Parameters: `--backend openai --model <name> --weights-dir models/Qwen3.8-27B-4bit`.

**B. In-process mlx-lm (no server).** The model is a local folder; `--model` is that path.

- Parameters: `--backend mlx --model models/Qwen3.8-27B-4bit --weights-dir models/Qwen3.8-27B-4bit`.
  mlx-lm is installed by `requirements.txt` (the runtime's `MLXGenerator` calls `mlx_lm.load` and
  `stream_generate`, and builds the prompt with `enable_thinking=False`).
- Use a local folder, not a Hugging Face name: a name makes mlx-lm look on the network.
- The model then lives in the process of `ask` (loaded again for every one-shot question) or of `serve` (once).
  This path was measured only with the full-precision base model; with the 4-bit build it is untested.

Both options use the same model repository for the download, `mlx-community/Qwen3.8-27B-4bit`
(`--model-repo` changes it).

## Steps

Times are ranges, with the basis in the last section. Each step prints one line per item and can be run again: a
finished item is skipped.

1. **Clone, with the network on.** `time git clone <your remote> kafka-sre-llm && cd kafka-sre-llm` (note the time;
   add it to the install figure by hand). Put `catalogues.tgz` in place:
   `mkdir -p work/m4 && tar xzf /path/to/catalogues.tgz -C work/m4`. 1 to 2 min.

2. **Preflight** (no model, no network): what is present and what is missing.
   `python3 scripts/laptop_bench.py --out OUT preflight` (with your serving parameters from above on this first
   command). Missing items that are listed as required must be fixed after step 3, then run it again. 1 min.

3. **Install, timed.** `python3 scripts/laptop_bench.py --out OUT [serving parameters] install-check --run`.
   The sequence, each step timed (`OUT/install/steps.jsonl`):
   `python3 -m venv .venv`; `.venv/bin/pip install -r requirements.txt`; `hf download aywengo/kafka-sre-docs-index-v3
   --repo-type dataset --local-dir idx`, checksum, copy to `work/v3/chunks_s1.jsonl`; `hf download BAAI/bge-reranker-v2-m3
   --revision <frozen revision>`; `hf download mlx-community/Qwen3.8-27B-4bit --local-dir models/Qwen3.8-27B-4bit`;
   `python -m kafka_sre_rag verify`. The pip step counts as install time; the three downloads are reported as
   download time with their sizes. Bar: install time (without the downloads) under 30 min. After it, start oMLX (option
   A) and make sure it lists the model. 15 to 40 min, mostly the download (about 16 GB for the model).

4. **Wi-Fi off.** Turn Wi-Fi off in the menu bar and unplug any network cable. Check that there is no route:
   `route -n get default` must fail ("not in table"). Keep it off until step 11. (The reranker and the model are
   already on disk; the environment variables `HF_HUB_OFFLINE=1` and `TRANSFORMERS_OFFLINE=1` make any Hugging Face
   lookup an error instead of a silent download. If you start anything by hand, export both first.)

5. **Preflight again**, with the serving parameters: `python3 scripts/laptop_bench.py --out OUT [serving parameters]
   preflight --strict` must exit 0.

6. **Memory and cold start.** Quit oMLX and start it again (option A), and do not send it a request. Then
   `.venv/bin/python scripts/laptop_bench.py --out OUT memory`. It samples a 20 s baseline (oMLX idle, nothing of ours),
   starts `serve`, and asks three questions: the first one is the cold start of the whole assistant (model load
   included). Writes the peak resident memory per process, system memory, swap and pressure. 5 to 12 min.

7. **Speed.** `.venv/bin/python scripts/laptop_bench.py --out OUT speed`. Six conditions, same questions in the same
   order: service and one-shot, each in default mode, strict mode, default with `--check`; 10 questions per service
   condition and 5 per one-shot condition (`--n-service`, `--n-oneshot`); then the retrieval alone (BM25 and
   cross-encoder, no model). The service is started and stopped by the script for each condition, so the first
   question of each is the first after launch. 1.5 to 3 h (use `--conds service-default,service-strict` first if short
   of time: about 25 to 40 min).

8. **Sustained, on power.** Charger connected, laptop cool.
   `.venv/bin/python scripts/laptop_bench.py --out OUT sustained --label ac`. 30 questions in default mode through one
   service; `pmset` is read before, after questions 5, 10, ..., 30, and after. 30 to 60 min.

9. **Sustained, on battery.** Disconnect the charger with the battery at 90 % or more, let the laptop cool for 10
   minutes, then `... sustained --label battery`. The script refuses when the power state does not match the label.
   Note the battery percentage at the end (in `power.jsonl`). 30 to 60 min.

10. **A working laptop.** Reconnect the charger. Open what you usually have open: IDE, browser with its usual tabs,
    containers or a local cluster, chat and mail. Then
    `... speed --label working-laptop --conds service-default --n-service 10 --apps "IDE, browser 25 tabs, Docker with 6 containers"`
    (write what was open, in your words). The script records the total memory in use at the start and nothing else
    about the machine. 10 to 20 min.

11. **Same answers as the reference machine.** `.venv/bin/python scripts/laptop_bench.py --out OUT score` answers the 60
    open items in default mode through `scripts/eval2.py answer` and scores them with `scripts/eval2_score.py` (needs
    the catalogues), then compares with the stored `s6-A1` result. It is resumable: run it again after an interruption.
    45 to 90 min. Turn Wi-Fi on again after this step.

12. **Bundle.** `.venv/bin/python scripts/laptop_bench.py --out OUT bundle` writes `OUT/SUMMARY.md` (every bar, the measured
    value, its size, pass or fail), `OUT/send_back/` (scrubbed copies of the result files) and `OUT/send_back.zip`.
    Read `SUMMARY.md` and look at `ls -R OUT/send_back` before sending: the scrubber is a check by pattern and by the
    names it finds on this machine, not a guarantee. Send `send_back.zip`. 1 min.

Total working time: about 5 to 8 hours of running, which fits two evenings (steps 1 to 7, then 8 to 12). Between the
evenings the folder can stay as it is; run each phase again to resume.

## What the bundle contains, and what it never contains

Result files (`json`, `jsonl`, `md`, short `log` tails): timings, memory samples, power and thermal readings, the answers
and scores of the 60 open items, and the machine described by chip, core counts, memory, macOS version and power
state. The scrubber removes host names, the user name, home and repository paths, serial numbers and hardware ids,
the Wi-Fi name, IPv4 and IPv6 addresses, MAC addresses and `.local` style names; a file that still shows any of them
after scrubbing is left out and named in the output. No server address and no key is ever written.

## What this does not measure

- GGUF and llama.cpp builds, Ollama, Linux and Windows laptops.
- The stretch machine (16 to 24 GB, about 10 GB held) and a smaller model.
- Quality on the hidden items (`eval/hidden/` is not read). The 60-item comparison can show a gross difference between
  the laptop build and the reference build; it cannot judge answer quality.
- The invention, completeness and refusal bars (they need readings), battery energy use beyond the percentage drop,
  concurrent requests, multi-turn use, tool use, longer contexts.
- Whether DNS or other traffic is attempted for a moment between two `lsof` looks (every 10 s); the offline
  variables and the Wi-Fi-off step are what make that a failure instead of a silent success.
- GPU memory as such: the resident memory of a process may miss it, so the system's used memory is reported beside it.
  The peaks are the largest of samples taken every 2 s.

## How long, and why

- Install: the pip step is 3 to 10 min (torch and sentence-transformers wheels); the model is about 16 GB and the
  reranker a few GB, so the downloads take 5 min at 500 Mbit/s and about 40 min at 60 Mbit/s.
- Per answer: the Mac Studio's 4-bit medians were 35 s for the answer step plus 4 s for the search terms and 4 to 9 s
  for retrieval, so about 45 to 55 s in default mode and 30 to 40 s in strict; a 32 to 36 GB laptop has a lower
  memory bandwidth, so the ranges assume 1 to 2 times that, 45 to 110 s. Service conditions: 10 questions; one-shot
  conditions add the process start (index, reranker, and in option B the 16 GB model) to every question.
- Scorer items: the 251-item run took 1 h 57 min on the Mac Studio, 28 s per item; the laptop range uses 1 to 2.5 times
  that for 60 items.
- These are estimates before any laptop run; replace them with the measured times in the report.
