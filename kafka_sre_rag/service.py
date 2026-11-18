"""python -m kafka_sre_rag serve: one process that keeps the index, the BM25 structures and the reranker loaded and
answers OpenAI-style chat requests on the loopback interface. Standard library only (http.server).

Every request is retrieved and answered on its own, as `chat` does: no conversation history is sent to the model.
The steps are those of evalhooks.run_item() (expansion -> retrieval -> answer); prompts and retrieval settings are
the frozen ones. One request at a time reaches the model (a lock); the others wait."""
import ipaddress
import json
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import config, evalhooks, pipeline
from .generators import EmptyAnswerError, ServerError, ThinkFilter

MODEL_DEFAULT = "kafka-sre"
MODEL_STRICT = "kafka-sre-strict"
MODELS = (MODEL_DEFAULT, MODEL_STRICT)
HISTORY_NOTE = "not sent: each request is retrieved and answered on its own (only the last user message is used)"


def is_loopback_bind(host):
    h = (host or "").strip("[]").lower()
    if h == "localhost":
        return True
    try:
        return ipaddress.ip_address(h).is_loopback
    except ValueError:
        return False


class BadRequest(ValueError):
    pass


def parse_request(body, default_mode):
    """-> (question, mode, k, stream). The question is the last user message."""
    if not isinstance(body, dict):
        raise BadRequest("the request body must be a JSON object")
    msgs = body.get("messages")
    if not isinstance(msgs, list) or not msgs:
        raise BadRequest("messages must be a non-empty list")
    question = None
    for m in reversed(msgs):
        if isinstance(m, dict) and m.get("role") == "user":
            c = m.get("content")
            if isinstance(c, list):  # content parts: join the text parts
                c = "".join(p.get("text", "") for p in c if isinstance(p, dict) and p.get("type") == "text")
            if isinstance(c, str) and c.strip():
                question = c.strip()
            break
    if not question:
        raise BadRequest("the last user message must contain text")
    mode = default_mode
    model = body.get("model")
    if isinstance(model, str) and model:
        mode = "strict" if model == MODEL_STRICT else ("default" if model == MODEL_DEFAULT else default_mode)
    if "mode" in body:
        mode = body["mode"]
        if mode not in ("default", "strict"):
            raise BadRequest("mode must be 'default' or 'strict'")
    k = body.get("k", "k6")
    if k not in ("k6", "k3"):
        raise BadRequest("k must be 'k6' or 'k3'")
    return question, mode, k, bool(body.get("stream"))


class _Tee:
    """Wraps the generator for one request. evalhooks.run_item() calls chat() twice (search terms, answer) and
    passes no on_token; the second call gets the streaming callback here. Nothing else is changed."""

    def __init__(self, gen, on_text):
        self.gen, self.on_text, self.n = gen, on_text, 0
        self.identity = getattr(gen, "identity", None)
        self.backend = getattr(gen, "backend", None)

    def chat(self, messages, max_tokens, on_token=None):
        self.n += 1
        if self.n == 2 and self.on_text is not None:
            flt = ThinkFilter()
            res = self.gen.chat(messages, max_tokens, lambda p: self.on_text(flt.feed(p)))
            self.on_text(flt.finish())
            return res
        return self.gen.chat(messages, max_tokens, None)


class Service:
    def __init__(self, gen, cfg=None, *, mode="default", k="k6", rerank=True, max_tokens=None, verified=None):
        self.gen, self.cfg = gen, cfg or config.load()
        self.mode, self.k, self.rerank, self.max_tokens = mode, k, rerank, max_tokens
        self.verified = verified
        self.lock = threading.Lock()
        self.load_seconds = None

    def warm_up(self):
        """Load the index, the BM25 structures and the reranker (no model call). Returns the seconds taken."""
        from . import retrieval
        t = time.monotonic()
        retrieval.select("warm up", "", k=self.k, rerank=self.rerank)
        self.load_seconds = time.monotonic() - t
        return self.load_seconds

    def health(self):
        ident = getattr(self.gen, "identity", None) or {}
        rr = self.cfg["retrieval"]["reranker"]
        return {"status": "ok", "index_sha256": self.cfg["index"]["sha256"], "reranker_revision": rr["revision"],
                "backend": ident.get("backend"), "model": ident.get("model"), "adapter": ident.get("adapter"),
                "verify_passed": self.verified, "default_mode": self.mode,
                "load_seconds": self.load_seconds, "history": HISTORY_NOTE}

    def answer(self, question, mode, k, on_text=None):
        """Retrieve and answer one question, one at a time (the lock). Returns (text, extra) where extra is the
        dict that travels in the response's `kafka_sre` field."""
        from . import retrieval
        with self.lock:
            seen = []
            real_select = retrieval.select

            def capture(*a, **kw):  # run_item() returns only chunk ids; keep the chunks for the sources
                chunks = real_select(*a, **kw)
                seen.append(chunks)
                return chunks
            retrieval.select = capture
            try:
                r = evalhooks.run_item(question, _Tee(self.gen, on_text), self.cfg, k=k, mode=mode,
                                       rerank=self.rerank, max_tokens=self.max_tokens)
            finally:
                retrieval.select = real_select
        chunks = seen[0] if seen else []
        reasons = []
        if mode != "default":
            reasons.append(f"mode {mode}")
        if k != "k6":
            reasons.append(f"k {k}")
        if not pipeline.is_reference(self.gen, self.cfg):
            reasons.append("generator is not the reference model")
        if not self.rerank:
            reasons.append("no reranker")
        if self.max_tokens is not None:
            reasons.append(f"max_tokens override {self.max_tokens}")
        sec = r["seconds"]
        extra = {"question": question, "mode": mode, "k": k, "history": HISTORY_NOTE,
                 "expansion": r["search_terms"], "chunk_ids": r["pack"],
                 "sources": [{"source": c["source"], "title": c["title"], "path": c["path"],
                              "bm25_rank": c.get("bm25_rank"), "ce_score": c.get("ce_score")} for c in chunks],
                 "timings": {"expand_s": sec["search_terms"], "retrieve_s": sec["retrieval"],
                             "answer_s": sec["answer"]},
                 "measured": not reasons, "unmeasured_reasons": reasons, "tokens": r["tokens"],
                 "truncated": r["truncated"]}
        return r["text"], extra


def _error_body(message, kind):
    return {"error": {"message": message, "type": kind}}


def make_handler(svc, allow_non_local):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        server_version = "kafka-sre-rag"

        def log_message(self, fmt, *args):  # one short line on stderr per request; never the question text
            import sys
            print(f"{self.address_string()} {fmt % args}", file=sys.stderr)

        def _host_ok(self):
            if allow_non_local:
                return True
            host = (self.headers.get("Host") or "").strip()
            host = host.rsplit("]", 1)[0].lstrip("[") if host.startswith("[") else host.rsplit(":", 1)[0]
            return is_loopback_bind(host)

        def _send_json(self, code, obj, extra_headers=None):
            data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("X-Kafka-Sre-History", "not-sent")
            for k, v in (extra_headers or {}).items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            if not self._host_ok():
                return self._send_json(403, _error_body("host not allowed", "forbidden"))
            if self.path == "/healthz":
                return self._send_json(200, svc.health())
            if self.path == "/v1/models":
                return self._send_json(200, {"object": "list", "data": [
                    {"id": m, "object": "model", "created": 0, "owned_by": "kafka-sre-rag"} for m in MODELS]})
            self._send_json(404, _error_body("not found", "not_found"))

        def do_POST(self):
            if not self._host_ok():
                return self._send_json(403, _error_body("host not allowed", "forbidden"))
            if self.path != "/v1/chat/completions":
                return self._send_json(404, _error_body("not found", "not_found"))
            try:
                n = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(n) or b"null")
                question, mode, k, stream = parse_request(body, svc.mode)
            except (ValueError, UnicodeDecodeError) as e:  # BadRequest is a ValueError
                return self._send_json(400, _error_body(f"bad request: {e}", "invalid_request_error"))
            model = body.get("model") if isinstance(body.get("model"), str) and body.get("model") \
                else (MODEL_STRICT if mode == "strict" else MODEL_DEFAULT)
            rid, created = "chatcmpl-" + uuid.uuid4().hex[:24], int(time.time())
            if not stream:
                try:
                    text, extra = svc.answer(question, mode, k)
                except EmptyAnswerError as e:
                    return self._send_json(502, _error_body(str(e), "empty_answer"))
                except ServerError as e:
                    return self._send_json(502, _error_body(str(e), "model_server_error"))
                return self._send_json(200, {
                    "id": rid, "object": "chat.completion", "created": created, "model": model,
                    "choices": [{"index": 0, "message": {"role": "assistant", "content": text},
                                 "finish_reason": "length" if extra["truncated"] else "stop"}],
                    "usage": {"completion_tokens": extra["tokens"]}, "kafka_sre": extra})
            self._stream(question, mode, k, model, rid, created)

        def _chunk(self, rid, created, model, delta, finish=None, extra=None):
            d = {"id": rid, "object": "chat.completion.chunk", "created": created, "model": model,
                 "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]}
            if extra is not None:
                d["kafka_sre"] = extra
            return d

        def _write_event(self, obj):
            data = obj if isinstance(obj, str) else json.dumps(obj, ensure_ascii=False)
            self.wfile.write(f"data: {data}\n\n".encode("utf-8"))
            self.wfile.flush()

        def _stream(self, question, mode, k, model, rid, created):
            started = []

            def begin():  # headers go out with the first token, so an error before it is a plain HTTP error
                if started:
                    return
                started.append(1)
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("X-Kafka-Sre-History", "not-sent")
                self.send_header("Connection", "close")
                self.end_headers()
                self.close_connection = True
                self._write_event(self._chunk(rid, created, model, {"role": "assistant", "content": ""}))

            def on_text(piece):
                if piece:
                    begin()
                    self._write_event(self._chunk(rid, created, model, {"content": piece}))
            try:
                text, extra = svc.answer(question, mode, k, on_text=on_text)
            except (EmptyAnswerError, ServerError) as e:
                kind = "empty_answer" if isinstance(e, EmptyAnswerError) else "model_server_error"
                if not started:
                    return self._send_json(502, _error_body(str(e), kind))
                self._write_event(_error_body(str(e), kind))
                return self._write_event("[DONE]")
            except (BrokenPipeError, ConnectionResetError):
                return
            try:
                begin()
                self._write_event(self._chunk(rid, created, model, {},
                                              "length" if extra["truncated"] else "stop", extra))
                self._write_event("[DONE]")
            except (BrokenPipeError, ConnectionResetError):
                pass
    return Handler


def make_server(svc, host="127.0.0.1", port=8765, allow_non_local=False):
    """Bind the server. A non-loopback address is refused unless allow_non_local (nothing may leave the machine)."""
    if not is_loopback_bind(host):
        if not allow_non_local:
            raise ValueError(f"refusing to bind to {host!r}: the service answers on the loopback interface only. "
                             "Pass --allow-non-local to override (nothing is then protected by the machine boundary).")
    srv = ThreadingHTTPServer((host, port), make_handler(svc, allow_non_local))
    srv.daemon_threads = True
    return srv
