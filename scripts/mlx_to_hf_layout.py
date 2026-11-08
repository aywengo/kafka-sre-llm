#!/usr/bin/env python3
"""Turn a fused mlx-lm Qwen3.5/3.6/3.8 model into a Hugging Face layout checkpoint.

  python scripts/mlx_to_hf_layout.py FUSED_DIR OUT_DIR [UPSTREAM_ID]

mlx-lm saves a different weight layout than Hugging Face: 1-D RMSNorm weights shifted by +1,
the linear-attention conv1d weight as (out, kernel, 1) instead of (out, 1, kernel),
`language_model.model.*` key names, and no vision tower. A GGUF or transformers model made
straight from the fused directory is broken (doubled norm weights, wrong conv1d: gibberish).
This writes a copy that undoes the layout changes (the inverse of Model.sanitize in
mlx_lm/models/qwen3_5.py), renames the keys to the upstream names, adds the unchanged vision
tower from UPSTREAM_ID, and takes config, tokenizer and processor files from upstream. The
chat template stays the one in FUSED_DIR. The MTP head is not included (it was not fine-tuned).
Needs torch, safetensors and huggingface_hub.
"""
import json
import shutil
import sys
from pathlib import Path

from huggingface_hub import hf_hub_download
from safetensors import safe_open
from safetensors.torch import save_file

NORM_SUFFIXES = (".input_layernorm.weight", ".post_attention_layernorm.weight",
                 "model.norm.weight", ".q_norm.weight", ".k_norm.weight")
UPSTREAM_FILES = ["config.json", "generation_config.json", "tokenizer.json", "tokenizer_config.json",
                  "vocab.json", "merges.txt", "preprocessor_config.json",
                  "video_preprocessor_config.json", "LICENSE"]


def upstream_name(key: str) -> str:
    if key.startswith("language_model.model."):
        return "model.language_model." + key[len("language_model.model."):]
    if key.startswith("language_model.lm_head."):
        return "lm_head." + key[len("language_model.lm_head."):]
    raise KeyError(key)


def main(src: Path, dst: Path, upstream: str):
    dst.mkdir(parents=True, exist_ok=True)
    weight_map = {}
    fused = json.loads((src / "model.safetensors.index.json").read_text())["weight_map"]
    n_norm = n_conv = 0
    for shard in sorted(set(fused.values())):
        tensors = {}
        with safe_open(str(src / shard), "pt") as sf:
            for k in sf.keys():
                t = sf.get_tensor(k)
                if "conv1d.weight" in k and t.ndim == 3 and t.shape[-1] == 1:
                    t, n_conv = t.moveaxis(1, 2).contiguous(), n_conv + 1
                elif t.ndim == 1 and k.endswith(NORM_SUFFIXES):
                    t, n_norm = (t.float() - 1.0).to(t.dtype), n_norm + 1
                tensors[upstream_name(k)] = t
        save_file(tensors, str(dst / shard), metadata={"format": "pt"})
        weight_map.update({k: shard for k in tensors})
        print(f"{shard}: {len(tensors)} tensors", flush=True)
    print(f"reversed {n_norm} norm weights and {n_conv} conv1d weights")

    up_index = json.loads(Path(hf_hub_download(upstream, "model.safetensors.index.json")).read_text())["weight_map"]
    visual = {k: f for k, f in up_index.items() if k.startswith("model.visual.")}
    (up_shard,) = set(visual.values())
    tensors = {}
    with safe_open(hf_hub_download(upstream, up_shard), "pt") as sf:
        for k in visual:
            tensors[k] = sf.get_tensor(k)
    save_file(tensors, str(dst / "model-vision.safetensors"), metadata={"format": "pt"})
    weight_map.update({k: "model-vision.safetensors" for k in tensors})
    print(f"model-vision.safetensors: {len(tensors)} tensors")

    total = sum(f.stat().st_size for f in dst.glob("*.safetensors"))
    (dst / "model.safetensors.index.json").write_text(
        json.dumps({"metadata": {"total_size": total}, "weight_map": weight_map}, indent=2))
    for name in UPSTREAM_FILES:
        shutil.copy(hf_hub_download(upstream, name), dst / name)
    shutil.copy(src / "chat_template.jinja", dst / "chat_template.jinja")


if __name__ == "__main__":
    main(Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3] if len(sys.argv) > 3 else "Qwen/Qwen3.8-27B")
