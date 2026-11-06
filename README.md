# kafka-sre-llm

Fine-tune an open-weight model into a Kafka / Strimzi / Kubernetes / GitOps SRE
assistant on Apple Silicon with mlx-lm, then publish it to Hugging Face.

```
sources.yaml ─► fetch_sources.py ─► chunk_docs.py ─► generate_qa.py ─► build_dataset.py
                  raw/                work/chunks.jsonl  work/raw_qa.jsonl   data/*.jsonl
                                                                                 │
              publish.sh ◄── eval/compare.py ◄── mlx_lm.lora -c configs/lora.yaml ┘
```

## 0. Setup

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
hf auth login                      # write token from huggingface.co/settings/tokens
```

For GGUF export, clone and build llama.cpp separately (`cmake -B build && cmake --build build -j`).

## 1. Collect and chunk docs

```bash
python scripts/fetch_sources.py            # sparse, shallow clones into raw/
python scripts/chunk_docs.py               # -> work/chunks.jsonl
```

Put your own runbooks/postmortems in `private/runbooks/` (scrub secrets, hostnames and
customer data first). They are usually the highest-value data you have.

## 2. Generate training data

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

## 3. Build the dataset

```bash
python scripts/build_dataset.py                       # -> data/{train,valid,test}.jsonl
# optional: mix in general chat data to limit forgetting
python scripts/build_dataset.py --general general.jsonl --general-ratio 0.15
```

It prints a suggested `iters` value; put it in `configs/lora.yaml` (both `iters` and the
second number in `lr_schedule.arguments`).

## 4. Train

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

## 5. Evaluate

Extend `eval/questions.jsonl` to ~200 questions written by you (not generated from the
same docs), then:

```bash
python eval/compare.py --model "$BASE_PATH" --adapter adapters/kafka-sre-v1 \
  --judge-url http://localhost:8080/v1
```

Read the answers yourself too — judge scores are a rough signal, invented config keys
are what matters most in this domain.

## 6. Publish

Fill in `MODEL_CARD.md` (commits are in `raw/*/.source-commit`, eval scores, counts), then:

```bash
HF_USER=<you> NAME=kafka-sre-14b-v1 bash scripts/publish.sh
```

This uploads three repos: full-precision safetensors, an MLX 4-bit variant and GGUF
quants (Q4_K_M, Q5_K_M, Q8_0).

## Rough budget

| Step | Time on M3 Ultra |
|---|---|
| Generation, ~20k chunks with verify pass | 1–3 days, depends on teacher size |
| LoRA, 14B bf16, ~20k examples, 2 epochs | several hours to ~1 day |
| LoRA, 32B | roughly 2–3× the 14B time |

Start with a small run end to end (a few hundred examples, 200 iters) to shake out the
pipeline before the long runs.
