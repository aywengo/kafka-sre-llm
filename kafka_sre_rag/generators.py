"""Chat generators with one interface: chat(messages, max_tokens, on_token=None) -> (text, tokens, truncated).
Importing this module never loads mlx."""
import json
import os
import re
import ipaddress
from urllib.parse import urlsplit

import httpx

_THINK = re.compile(r"<think>.*?</think>", re.S)


class EmptyAnswerError(RuntimeError):
    """The server returned no answer text (typically a reasoning model that used the whole token budget)."""


class ServerError(RuntimeError):
    """The server could not be reached, refused the request, or answered with another model than requested."""


def _error_text(r):
    """The server's own error text from an error response (first 300 characters)."""
    text = r.text
    try:
        d = r.json()
        e = d.get("error", d.get("detail", d.get("message"))) if isinstance(d, dict) else None
        if isinstance(e, dict):
            e = e.get("message")
        if isinstance(e, str) and e:
            text = e
    except ValueError:
        pass
    return " ".join(text.split())[:300]


_PROXY_VARS = ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY")


def is_loopback_host(base_url):
    """True when the URL's host is localhost, an address in 127.0.0.0/8, or ::1."""
    try:
        host = (urlsplit(base_url).hostname or "").lower().rstrip(".")
    except ValueError:
        return False
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def proxy_configured():
    """True when a proxy variable is set in the environment (the value is never read out)."""
    return any(os.environ.get(v) or os.environ.get(v.lower()) for v in _PROXY_VARS)


def _empty_error(max_tokens, truncated, reasoning):
    cut = "the response was cut off by the token limit (finish_reason length)" if truncated \
        else "the response was not cut off by the token limit"
    why = "the response carried reasoning content" if reasoning else "no reasoning content was exposed"
    return EmptyAnswerError(
        f"the model returned no answer text: {cut}, {why} (max_tokens {max_tokens}). Remedies: raise the token "
        "limit with --max-tokens, or turn the model's thinking off with --extra-body "
        "(for example '{\"chat_template_kwargs\": {\"thinking\": false}}'; the key depends on the model).")


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
        self.models_url = base + ("/models" if base.endswith("/v1") else "/v1/models")
        self.base = base
        # A server on this machine is never reached through a proxy: the prompt carries the question and
        # the retrieved text. For loopback hosts the client ignores the proxy variables and NO_PROXY.
        self.local = is_loopback_host(base)
        self.client = httpx.Client(transport=transport, timeout=timeout, trust_env=not self.local)
        self.adapter = None
        self._model_checked = False

    def _unreachable(self, e):
        hint = ""
        if not self.local and proxy_configured():
            hint = (" - a proxy is configured in the environment; add the server's host to NO_PROXY "
                    "if it is local to your network")
        return ServerError(f"cannot reach {self.base} ({type(e).__name__}){hint}")

    def _check_status(self, r):
        if r.status_code >= 400:
            r.read()
            raise ServerError(f"server {self.base} returned HTTP {r.status_code}: {_error_text(r)}")

    def _check_model_listed(self):
        """Once, before the first chat call: the requested name must be in GET <base>/models, when the server
        offers that list. A missing or failing list endpoint skips the check."""
        if self._model_checked:
            return
        self._model_checked = True
        try:
            r = self.client.get(self.models_url, headers=self.headers)
            if r.status_code >= 400:
                return
            data = r.json().get("data")
            names = [m.get("id") for m in data if isinstance(m, dict) and m.get("id")] \
                if isinstance(data, list) else []
        except (httpx.HTTPError, ValueError, AttributeError, TypeError):
            return
        if names and self.model not in names:
            raise ServerError(f"model {self.model!r} is not on the server; available (first 20): "
                              f"{', '.join(names[:20])}")

    def _check_answer_model(self, got):
        if got and got != self.model:
            raise ServerError(f"the server answered with model {got!r}, not the requested {self.model!r}; "
                              "the answer is not used")

    @property
    def identity(self):
        return {"backend": self.backend, "model": self.model, "adapter": None}

    def chat(self, messages, max_tokens, on_token=None):
        self._check_model_listed()
        try:
            return self._chat(messages, max_tokens, on_token)
        except httpx.HTTPError as e:
            raise self._unreachable(e) from None

    def _chat(self, messages, max_tokens, on_token):
        body = {"model": self.model, "messages": messages, "max_tokens": max_tokens, "temperature": 0,
                **self.extra_body}
        if on_token is None:
            r = self.client.post(self.url, json=body, headers=self.headers)
            self._check_status(r)
            d = r.json()
            self._check_answer_model(d.get("model"))
            ch = d["choices"][0]
            toks = (d.get("usage") or {}).get("completion_tokens", 0)
            msg = ch["message"]
            out = strip_think(msg.get("content") or "")
            trunc = ch.get("finish_reason") == "length"
            if not out:
                raise _empty_error(max_tokens, trunc, msg.get("reasoning_content") or msg.get("reasoning"))
            return out, toks, trunc
        body.update(stream=True, stream_options={"include_usage": True})
        text, toks, finish, reasoning = "", 0, None, False
        with self.client.stream("POST", self.url, json=body, headers=self.headers) as r:
            self._check_status(r)
            for line in r.iter_lines():
                if not line.startswith("data:") or line[5:].strip() == "[DONE]":
                    continue
                d = json.loads(line[5:])
                self._check_answer_model(d.get("model"))
                if d.get("usage"):
                    toks = d["usage"].get("completion_tokens", toks)
                for ch in d.get("choices") or []:
                    delta = ch.get("delta") or {}
                    reasoning = reasoning or bool(delta.get("reasoning_content") or delta.get("reasoning"))
                    piece = delta.get("content") or ""
                    if piece:
                        text += piece
                        on_token(piece)
                    finish = ch.get("finish_reason") or finish
        out = strip_think(text)
        if not out:
            raise _empty_error(max_tokens, finish == "length", reasoning)
        return out, toks, finish == "length"


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


class ThinkFilter:
    """Streaming counterpart of strip_think(): feed() the pieces of a reply as they arrive, print what it returns;
    finish() returns the rest. The concatenation of everything returned equals strip_think(whole reply): no
    <think>...</think> block, no leading or trailing whitespace. An unterminated <think> is kept, as strip_think
    keeps it. Text is held back only while it could still be the start of a tag, or is trailing whitespace."""
    OPEN, CLOSE = "<think>", "</think>"

    def __init__(self):
        self.buf = ""          # unprocessed input
        self.inside = False
        self.started = False   # a non-whitespace character has been emitted
        self.ws = ""           # whitespace held back (emitted only when more text follows)
        self.block = ""        # raw text of an open think block (kept if it never closes)

    def _emit(self, text):
        if not text:
            return ""
        if not self.started:
            text = text.lstrip()
            if not text:
                return ""
        out = self.ws + text.rstrip()
        tail = text[len(text.rstrip()):]
        if text.rstrip():
            self.started = True
            self.ws = tail
            return out
        self.ws += text
        return ""

    def feed(self, piece):
        self.buf += piece
        out = ""
        while True:
            tag = self.CLOSE if self.inside else self.OPEN
            i = self.buf.find(tag)
            if i >= 0:
                head, self.buf = self.buf[:i], self.buf[i + len(tag):]
                if self.inside:
                    self.block = ""
                    self.inside = False
                else:
                    out += self._emit(head)
                    self.inside, self.block = True, ""
                continue
            # keep a possible partial tag at the end of the buffer
            keep = 0
            for n in range(min(len(tag) - 1, len(self.buf)), 0, -1):
                if tag.startswith(self.buf[-n:]):
                    keep = n
                    break
            head, self.buf = self.buf[:len(self.buf) - keep], self.buf[len(self.buf) - keep:]
            if self.inside:
                self.block += head
            else:
                out += self._emit(head)
            return out

    def finish(self):
        out = ""
        if self.inside:  # never closed: strip_think leaves the text as it is
            out = self._emit(self.OPEN + self.block + self.buf)
        else:
            out = self._emit(self.buf)
        self.buf, self.block, self.inside = "", "", False
        return out
