#!/usr/bin/env bash
# Fuse the adapter, build MLX and GGUF variants, and upload to Hugging Face.
# Usage: HF_USER=yourname NAME=kafka-sre-14b-v1 bash scripts/publish.sh
# Check flags against `mlx_lm.fuse --help` / `mlx_lm.convert --help` for your version.
set -euo pipefail

BASE="${BASE:-Qwen/Qwen3-14B}"          # full-precision base the adapter was trained on
ADAPTER="${ADAPTER:-adapters/kafka-sre-v1}"
NAME="${NAME:-kafka-sre-14b-v1}"
HF_USER="${HF_USER:?set HF_USER}"
LLAMA_CPP="${LLAMA_CPP:-$HOME/src/llama.cpp}"
OUT="out/$NAME"

mkdir -p "$OUT"

echo "== 1. fuse adapter into base weights (Hugging Face safetensors layout)"
# If you trained on a 4-bit model, add --dequantize so the fused model is full precision.
mlx_lm.fuse --model "$BASE" --adapter-path "$ADAPTER" --save-path "$OUT/fused"
cp MODEL_CARD.md "$OUT/fused/README.md"

echo "== 2. MLX 4-bit variant"
mlx_lm.convert --hf-path "$OUT/fused" --mlx-path "$OUT/mlx-4bit" -q --q-bits 4
cp MODEL_CARD.md "$OUT/mlx-4bit/README.md"

echo "== 3. GGUF variants (for Ollama / LM Studio / llama.cpp)"
python "$LLAMA_CPP/convert_hf_to_gguf.py" "$OUT/fused" --outtype f16 --outfile "$OUT/$NAME-f16.gguf"
for q in Q4_K_M Q5_K_M Q8_0; do
  "$LLAMA_CPP/build/bin/llama-quantize" "$OUT/$NAME-f16.gguf" "$OUT/$NAME-$q.gguf" "$q"
done
mkdir -p "$OUT/gguf" && mv "$OUT"/*-Q*.gguf "$OUT/gguf/" && cp MODEL_CARD.md "$OUT/gguf/README.md"

echo "== 4. smoke test"
mlx_lm.generate --model "$OUT/mlx-4bit" --max-tokens 300 \
  --prompt "How do I check consumer group lag for a Strimzi-managed cluster?"

echo "== 5. upload (requires: hf auth login)"
hf upload "$HF_USER/$NAME"          "$OUT/fused"    . --repo-type model
hf upload "$HF_USER/$NAME-MLX-4bit" "$OUT/mlx-4bit" . --repo-type model
hf upload "$HF_USER/$NAME-GGUF"     "$OUT/gguf"     . --repo-type model
echo "done: https://huggingface.co/$HF_USER/$NAME"
