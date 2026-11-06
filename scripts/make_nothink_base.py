#!/usr/bin/env python3
"""Build a base-model directory whose chat template is always non-thinking.

  python scripts/make_nothink_base.py SNAPSHOT_DIR OUT_DIR

OUT_DIR symlinks every file of SNAPSHOT_DIR except chat_template.jinja, which is a
patched copy: no reasoning-effort preamble and a generation prompt that ends with
an empty <think></think> block. mlx_lm forces enable_thinking=True for templates that
mention <think>, so the template itself has to be non-thinking.
"""
import json
import os
import sys
from pathlib import Path

from mlx_lm.tokenizer_utils import load as load_tokenizer

REASONING = "{%- if enable_thinking is undefined or enable_thinking is true %}"
GEN_PROMPT = "{%- if enable_thinking is defined and enable_thinking is false %}"


def main(snapshot: Path, out: Path):
    tpl = (snapshot / "chat_template.jinja").read_text()
    for old in (REASONING, GEN_PROMPT):
        assert tpl.count(old) == 1, f"template condition not found once: {old}"
    patched = tpl.replace(REASONING, "{%- if false %}").replace(GEN_PROMPT, "{%- if true %}")

    out.mkdir(parents=True, exist_ok=True)
    for f in snapshot.iterdir():
        dst = out / f.name
        if f.name != "chat_template.jinja" and not dst.exists():
            os.symlink(f.resolve(), dst)
    (out / "chat_template.jinja").write_text(patched)

    row = json.loads(Path("data/train.jsonl").open().readline())["messages"]
    orig, new = load_tokenizer(snapshot), load_tokenizer(out)
    expect = orig.apply_chat_template(row, tokenize=False, return_dict=False, enable_thinking=False)
    assert new.apply_chat_template(row, tokenize=False, return_dict=False) == expect
    prompt = new.apply_chat_template(row[:-1], add_generation_prompt=True, tokenize=False, return_dict=False)
    assert prompt.endswith("<think>\n\n</think>\n\n")
    print(f"ok: {out} renders non-thinking by default")


if __name__ == "__main__":
    main(Path(sys.argv[1]), Path(sys.argv[2]))
