"""Chat generators with one interface: chat(messages, max_tokens, on_token=None) -> (text, tokens, truncated).
Importing this module never loads mlx."""
import json
import os
import re

import httpx

_THINK = re.compile(r"<think>.*?</think>", re.S)


def strip_think(text):
    return _THINK.sub("", text).strip()


class OpenAIChatGenerator:
    """Any OpenAI-compatible /v1/chat/completions server. Temperature 0. Thinking must be switched off per
    server through extra_body; a <think> block in the reply is stripped in any case."""
    backend = "openai"

    def __init__(self, base_url=None, model=None, api_key=None, extra_body=None, transport=None, timeout=600):
        base_url = base_url or os.environ.get("OMLX_URL")
        if not base_url:
            raise ValueError("base_url missing: pass it or set OMLX_URL")
        if not model:
            raise ValueError("model missing")
        base = base_url.rstrip("/")
        self.url = base + ("/chat/completions" if base.endswith("/v1") else "/v1/chat/completions")
        self.model, self.extra_body = model, dict(extra_body or {})
        key = api_key or os.environ.get("OMLX_API_KEY")
        self.headers = {"Authorization": f"Bearer {key}"} if key else {}
        self.client = httpx.Client(transport=transport, timeout=timeout)
        self.adapter = None

    @property
    def identity(self):
        return {"backend": self.backend, "model": self.model, "adapter": None}

    def chat(self, messages, max_tokens, on_token=None):
        body = {"model": self.model, "messages": messages, "max_tokens": max_tokens, "temperature": 0,
                **self.extra_body}
        if on_token is None:
            r = self.client.post(self.url, json=body, headers=self.headers)
            r.raise_for_status()
            d = r.json()
            ch = d["choices"][0]
            toks = (d.get("usage") or {}).get("completion_tokens", 0)
            return strip_think(ch["message"].get("content") or ""), toks, ch.get("finish_reason") == "length"
        body.update(stream=True, stream_options={"include_usage": True})
        text, toks, finish = "", 0, None
        with self.client.stream("POST", self.url, json=body, headers=self.headers) as r:
            r.raise_for_status()
            for line in r.iter_lines():
                if not line.startswith("data:") or line[5:].strip() == "[DONE]":
                    continue
                d = json.loads(line[5:])
                if d.get("usage"):
                    toks = d["usage"].get("completion_tokens", toks)
                for ch in d.get("choices") or []:
                    piece = (ch.get("delta") or {}).get("content") or ""
                    if piece:
                        text += piece
                        on_token(piece)
                    finish = ch.get("finish_reason") or finish
        return strip_think(text), toks, finish == "length"


class MLXGenerator:
    """In-process mlx-lm, the measured path. Prompt building as scripts/rag_answers.py gen(): chat template,
    add_generation_prompt, enable_thinking=False, greedy (no sampler)."""
    backend = "mlx"

    def __init__(self, model_path, adapter=None):
        from mlx_lm import load, stream_generate  # imported here: the module itself never loads mlx
        self._stream = stream_generate
        self.model_path, self.adapter = model_path, adapter
        self.model, self.tok = load(model_path, adapter_path=adapter)

    @property
    def identity(self):
        return {"backend": self.backend, "model": self.model_path, "adapter": self.adapter}

    def chat(self, messages, max_tokens, on_token=None):
        prompt = self.tok.apply_chat_template(messages, add_generation_prompt=True, tokenize=False,
                                              enable_thinking=False)
        text, last = "", None
        for last in self._stream(self.model, self.tok, prompt=prompt, max_tokens=max_tokens):
            text += last.text
            if on_token and last.text:
                on_token(last.text)
        return strip_think(text), last.generation_tokens, last.finish_reason == "length"
