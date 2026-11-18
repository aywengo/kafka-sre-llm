#!/usr/bin/env python3
"""Build a base-model directory whose chat template is always non-thinking.

  python scripts/make_nothink_base.py SNAPSHOT_DIR OUT_DIR

OUT_DIR symlinks every file of SNAPSHOT_DIR except chat_template.jinja, which is a
patched copy: no reasoning-effort preamble and a generation prompt that ends with
an empty <think></think> block. mlx_lm forces enable_thinking=True for templates that
mention <think>, so the template itself has to be non-thinking.
"""
import os
import sys
from pathlib import Path

REASONING = "{%- if enable_thinking is undefined or enable_thinking is true %}"
GEN_PROMPT = "{%- if enable_thinking is defined and enable_thinking is false %}"

# Fixed sample for the self-check (the script does not need any training file).
SAMPLE = [
    {"role": "system", "content": "You are an expert Site Reliability Engineer for Apache Kafka."},
    {"role": "user", "content": "How do I check consumer lag?"},
    {"role": "assistant", "content": "Use kafka-consumer-groups.sh with --describe and --group."},
]


def patch_template(tpl: str) -> str:
    for old in (REASONING, GEN_PROMPT):
        assert tpl.count(old) == 1, f"template condition not found once: {old}"
    return tpl.replace(REASONING, "{%- if false %}").replace(GEN_PROMPT, "{%- if true %}")


def main(snapshot: Path, out: Path):
    from mlx_lm.tokenizer_utils import load as load_tokenizer  # imported here: patch_template needs no mlx

    tpl = (snapshot / "chat_template.jinja").read_text()
    patched = patch_template(tpl)

    out.mkdir(parents=True, exist_ok=True)
    for f in snapshot.iterdir():
        dst = out / f.name
        if f.name != "chat_template.jinja" and not dst.exists():
            os.symlink(f.resolve(), dst)
    (out / "chat_template.jinja").write_text(patched)

    row = SAMPLE
    orig, new = load_tokenizer(snapshot), load_tokenizer(out)
    expect = orig.apply_chat_template(row, tokenize=False, return_dict=False, enable_thinking=False)
    assert new.apply_chat_template(row, tokenize=False, return_dict=False) == expect
    prompt = new.apply_chat_template(row[:-1], add_generation_prompt=True, tokenize=False, return_dict=False)
    assert prompt.endswith("<think>\n\n</think>\n\n")
    print(f"ok: {out} renders non-thinking by default")


if __name__ == "__main__":
    main(Path(sys.argv[1]), Path(sys.argv[2]))
