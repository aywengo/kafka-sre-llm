#!/usr/bin/env bash
# Fuse the adapter, build MLX and GGUF variants, and upload to Hugging Face.
# Usage: HF_USER=yourname BASE=<BASE_PATH> NAME=kafka-sre-27b-v1 [UPLOAD=1] bash scripts/publish.sh
# Check flags against `mlx_lm.fuse --help` / `mlx_lm.convert --help` for your version.
set -euo pipefail

BASE="${BASE:?set BASE to the base model snapshot path (see README)}"  # full-precision base the adapter was trained on
ADAPTER="${ADAPTER:-adapters/kafka-sre-v1}"
NAME="${NAME:-kafka-sre-27b-v1}"
HF_USER="${HF_USER:?set HF_USER}"
LLAMA_CPP="${LLAMA_CPP:-$HOME/src/llama.cpp}"
PYTHON="${PYTHON:-python}"   # needs the packages from llama.cpp requirements/requirements-convert_hf_to_gguf.txt
export DYLD_LIBRARY_PATH="$LLAMA_CPP/build/bin${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
OUT="out/$NAME"

mkdir -p "$OUT"

echo "== 1. fuse adapter into base weights (Hugging Face safetensors layout)"
# If you trained on a 4-bit model, add --dequantize so the fused model is full precision.
mlx_lm.fuse --model "$BASE" --adapter-path "$ADAPTER" --save-path "$OUT/fused"

echo "== 2. MLX 4-bit variant"
mlx_lm.convert --hf-path "$OUT/fused" --mlx-path "$OUT/mlx-4bit" -q --q-bits 4

echo "== 3. GGUF variants (for Ollama / LM Studio / llama.cpp)"
# llama.cpp expects the Hugging Face weight layout, not the mlx-lm layout of the fused model.
"$PYTHON" scripts/mlx_to_hf_layout.py "$OUT/fused" "$OUT/bf16"
"$PYTHON" "$LLAMA_CPP/convert_hf_to_gguf.py" "$OUT/bf16" --outtype f16 --outfile "$OUT/$NAME-f16.gguf"
for q in Q4_K_M Q5_K_M Q8_0; do
  "$LLAMA_CPP/build/bin/llama-quantize" "$OUT/$NAME-f16.gguf" "$OUT/$NAME-$q.gguf" "$q"
done
mkdir -p "$OUT/gguf" && mv "$OUT"/*-Q*.gguf "$OUT/gguf/"

echo "== 4. smoke test"
mlx_lm.generate --model "$OUT/mlx-4bit" --max-tokens 300 \
  --prompt "How do I check consumer group lag for a Strimzi-managed cluster?"

echo "== READMEs (one card, title and library_name per repo)"
sed 's/^library_name: .*/library_name: transformers/' MODEL_CARD.md > "$OUT/bf16/README.md"
sed -e "s/^# $NAME\$/# $NAME-MLX-4bit/" -e "s|^base_model: .*|base_model: $HF_USER/$NAME\nbase_model_relation: quantized|" MODEL_CARD.md > "$OUT/mlx-4bit/README.md"
sed -e "s/^# $NAME\$/# $NAME-GGUF/" -e 's/^library_name: .*/library_name: gguf/' -e "s|^base_model: .*|base_model: $HF_USER/$NAME\nbase_model_relation: quantized|" MODEL_CARD.md > "$OUT/gguf/README.md"

[ "${UPLOAD:-0}" = 1 ] || { echo "== steps 1-4 done; not uploading (set UPLOAD=1 to run step 5)"; exit 0; }

echo "== 5. upload (requires: hf auth login)"
hf upload "$HF_USER/$NAME"          "$OUT/bf16"     . --repo-type model --private
hf upload "$HF_USER/$NAME-MLX-4bit" "$OUT/mlx-4bit" . --repo-type model --private
hf upload "$HF_USER/$NAME-GGUF"     "$OUT/gguf"     . --repo-type model --private
echo "done: https://huggingface.co/$HF_USER/$NAME"
