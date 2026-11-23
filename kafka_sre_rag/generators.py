"""Chat generators with one interface: chat(messages, max_tokens, on_token=None) -> (text, tokens, truncated).
Importing this module never loads mlx."""
import json
import os
import re
import ipaddress
import time
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


def open_think(text):
    """A reply that starts inside a think block (the chat template opened it in the prompt) ends it with </think> and
    has no <think>: the opening tag is put back so strip_think removes the block. Used only for calls with thinking
    on; any other text is returned unchanged."""
    i = text.find("</think>")
    if i >= 0 and "<think>" not in text[:i]:
        return "<think>" + text
    return text


class CallTimeout(ServerError):
    """A call given its own timeout (chat(..., timeout=s)) did not finish within it."""


def thinking_answer(raw, cut_by_limit, separate_reasoning):
    """The answer of a call with thinking on, and whether it was cut while thinking (M7 P1, review S1). raw: the
    content as received. A think block that is never closed is reasoning, not answer: everything from the unclosed
    <think> to the end is dropped and the call counts as cut while thinking. A reply with no </think> that stopped at
    the token limit also counts as cut while thinking (the template opened the block in the prompt, so the whole
    content may be reasoning), unless the server returned the reasoning in its own field and the content is the
    answer. Returns (answer, cut)."""
    t = open_think(raw)
    out = strip_think(t)
    i = out.find("<think>")
    if i >= 0:
        return out[:i].strip(), True
    cut = bool(cut_by_limit and "</think>" not in t and not (separate_reasoning and out))
    return out, cut


def request_info(info, usage, reasoning_text, raw_content, finish_reason, thinking, seconds, think_seconds):
    """Fill `info` (a dict) with what one call reported. The reasoning is never part of the answer text; only its size
    is kept. reasoning_tokens is the server's own count (usage.completion_tokens_details.reasoning_tokens, the OpenAI
    convention) or None when the server does not report one. think_s: seconds from the first reasoning piece to the
    first answer piece; only a streamed call can measure it (None otherwise)."""
    usage = usage or {}
    details = usage.get("completion_tokens_details") or {}
    inline = "".join(m[len("<think>"):-len("</think>")] for m in _THINK.findall(raw_content or ""))
    info.update({
        "thinking_requested": thinking,
        "completion_tokens": usage.get("completion_tokens"),
        "prompt_tokens": usage.get("prompt_tokens"),
        "reasoning_tokens": details.get("reasoning_tokens") if isinstance(details, dict) else None,
        "reasoning_chars": len(reasoning_text or "") + len(inline),
        "reasoning_inline": bool(inline),
        "finish_reason": finish_reason,
        "seconds": seconds,
        "think_s": think_seconds,
    })
    return info


def body_sha256(body):
    """sha256 of a request body in a canonical JSON form (M7 K4: recorded in the call's info, never sent)."""
    import hashlib
    return hashlib.sha256(json.dumps(body, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")).hexdigest()


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

    def chat(self, messages, max_tokens, on_token=None, *, thinking=None, info=None, timeout=None):
        """thinking None (the default) sends today's request unchanged: whatever extra_body says. True or False sets
        chat_template_kwargs.enable_thinking for this call only (the key the Qwen3 chat template reads; the scripts of
        this repository send the same key through --extra-body). info: a dict that receives what the server reported
        about the call (see request_info); passing it changes nothing in the request. With thinking True, a reply cut
        before its </think> is handled by thinking_answer() and info["cut_while_thinking"] says so. timeout: seconds
        for this call only (None: the client's timeout); when it runs out, CallTimeout is raised."""
        self._check_model_listed()
        try:
            return self._chat(messages, max_tokens, on_token, thinking, info, timeout)
        except httpx.TimeoutException as e:
            if timeout is not None:
                raise CallTimeout(f"the call to {self.base} did not finish within {timeout:.0f} s") from None
            raise self._unreachable(e) from None
        except httpx.HTTPError as e:
            raise self._unreachable(e) from None

    def request_body(self, messages, max_tokens, thinking=None):
        """The JSON body of a non-streamed call. thinking None: exactly today's body."""
        body = {"model": self.model, "messages": messages, "max_tokens": max_tokens, "temperature": 0,
                **self.extra_body}
        if thinking is not None:  # M7 P1: per call; the rest of chat_template_kwargs from extra_body is kept
            kw = dict(body.get("chat_template_kwargs") or {})
            kw["enable_thinking"] = bool(thinking)
            body["chat_template_kwargs"] = kw
        return body

    def _chat(self, messages, max_tokens, on_token, thinking=None, info=None, timeout=None):
        body = self.request_body(messages, max_tokens, thinking)
        t_start = time.monotonic()
        tkw = {} if timeout is None else {"timeout": timeout}  # None: the request is sent exactly as before
        if on_token is None:
            if info is not None:  # M7 K4: the hash of the body as sent (a record for the reader; nothing is sent differently)
                info["request_body_sha256"] = body_sha256(body)
            r = self.client.post(self.url, json=body, headers=self.headers, **tkw)
            self._check_status(r)
            d = r.json()
            self._check_answer_model(d.get("model"))
            ch = d["choices"][0]
            toks = (d.get("usage") or {}).get("completion_tokens", 0)
            msg = ch["message"]
            raw = msg.get("content") or ""
            sep = msg.get("reasoning_content") or msg.get("reasoning")
            trunc = ch.get("finish_reason") == "length"
            cut = False
            if thinking:
                raw = open_think(raw)
                out, cut = thinking_answer(raw, trunc, bool(sep))
            else:
                out = strip_think(raw)
            if info is not None:
                request_info(info, d.get("usage"), sep or "", raw,
                             ch.get("finish_reason"), thinking, time.monotonic() - t_start, None)
                if thinking:
                    info["cut_while_thinking"] = cut
            if not out:
                raise _empty_error(max_tokens, trunc, sep)
            return out, toks, trunc
        body.update(stream=True, stream_options={"include_usage": True})
        if info is not None:
            info["request_body_sha256"] = body_sha256(body)
        text, toks, finish, reasoning = "", 0, None, False
        rtext, usage, t_r0, t_c0 = "", None, None, None
        with self.client.stream("POST", self.url, json=body, headers=self.headers, **tkw) as r:
            self._check_status(r)
            for line in r.iter_lines():
                if not line.startswith("data:") or line[5:].strip() == "[DONE]":
                    continue
                d = json.loads(line[5:])
                self._check_answer_model(d.get("model"))
                if d.get("usage"):
                    toks = d["usage"].get("completion_tokens", toks)
                    usage = d["usage"]
                for ch in d.get("choices") or []:
                    delta = ch.get("delta") or {}
                    rpiece = delta.get("reasoning_content") or delta.get("reasoning")
                    reasoning = reasoning or bool(rpiece)
                    if info is not None and rpiece:
                        rtext += rpiece
                        t_r0 = time.monotonic() if t_r0 is None else t_r0
                    piece = delta.get("content") or ""
                    if piece:
                        text += piece
                        on_token(piece)
                        # the answer starts: at once when the server sends reasoning apart, else after </think>
                        if info is not None and t_c0 is None and (
                                t_r0 is not None or "</think>" in text
                                or not (thinking or text.lstrip().startswith("<think>"))):
                            t_c0 = time.monotonic()
                    finish = ch.get("finish_reason") or finish
        if info is not None:
            t_end = time.monotonic()
            think_s = None
            if t_r0 is not None:
                think_s = (t_c0 or t_end) - t_r0
            elif "</think>" in text or text.lstrip().startswith("<think>"):  # inline: from the start to </think>
                think_s = (t_c0 or t_end) - t_start
            request_info(info, usage, rtext, open_think(text) if thinking else text, finish, thinking,
                         t_end - t_start, think_s)
        if thinking:
            out, cut = thinking_answer(text, finish == "length", reasoning)
            if info is not None:
                info["cut_while_thinking"] = cut
        else:
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

    def chat(self, messages, max_tokens, on_token=None, *, thinking=None, info=None, timeout=None):
        """thinking None or False: today's prompt (enable_thinking=False); True: enable_thinking=True for this call.
        timeout: accepted for the common interface; an in-process call is not bounded by it."""
        prompt = self.tok.apply_chat_template(messages, add_generation_prompt=True, tokenize=False,
                                              enable_thinking=bool(thinking))
        text, last = "", None
        t0 = time.monotonic()
        t_c0 = None
        for last in self._stream(self.model, self.tok, prompt=prompt, max_tokens=max_tokens):
            text += last.text
            if on_token and last.text:
                on_token(last.text)
            if info is not None and t_c0 is None and "</think>" in text:
                t_c0 = time.monotonic()
        if thinking and prompt.rstrip().endswith("<think>") and "<think>" not in text:
            text = "<think>" + text  # the template opened the block in the prompt: the reply starts inside it
        if info is not None:
            t_end = time.monotonic()
            think_s = ((t_c0 or t_end) - t0) if text.lstrip().startswith("<think>") else None
            request_info(info, {"completion_tokens": last.generation_tokens,
                                "prompt_tokens": getattr(last, "prompt_tokens", None)},
                         "", text, last.finish_reason, thinking, t_end - t0, think_s)
        trunc = last.finish_reason == "length"
        if not thinking:
            return strip_think(text), last.generation_tokens, trunc
        out, cut = thinking_answer(text, trunc, False)
        if info is not None:
            info["cut_while_thinking"] = cut
        if not out:
            raise _empty_error(max_tokens, trunc, True)
        return out, last.generation_tokens, trunc


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
