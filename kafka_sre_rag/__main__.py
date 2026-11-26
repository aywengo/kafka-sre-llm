"""python -m kafka_sre_rag ask|chat|verify"""
import argparse
import json
import os
import sys

from . import config, profiles, revise as revise_mod, service
from .generators import EmptyAnswerError, ServerError, ThinkFilter

DRY_EXPANSION = "consumer group, kafka-consumer-groups.sh, --describe, --group, LAG, max.poll.interval.ms"


class StubGenerator:
    """--dry-run: fixed expansion, echoes the request. Prints the exact messages it receives."""
    backend = "stub"
    identity = {"backend": "stub", "model": "stub", "adapter": None}

    def __init__(self, out=sys.stdout):
        self.out, self.n = out, 0

    def chat(self, messages, max_tokens, on_token=None, **kw):
        from . import prompts
        if messages and messages[0]["content"] == prompts.VERIFY_SYSTEM:  # EXPERIMENTAL thorough profile: the check
            print(f"===== CHECK REQUEST (max_tokens {max_tokens}, thinking off) =====", file=self.out)
            for m in messages:
                print(f"--- {m['role']} ---\n{m['content']}", file=self.out)
            self.out.flush()
            return '{"replacements": []}', 0, False
        self.n += 1
        label = "EXPANSION REQUEST" if self.n % 2 == 1 else "ANSWER REQUEST"
        if kw.get("thinking") is not None:  # only under the thorough profile
            label += " (thinking %s)" % ("on" if kw["thinking"] else "off")
        print(f"===== {label} (max_tokens {max_tokens}) =====", file=self.out)
        for m in messages:
            print(f"--- {m['role']} ---\n{m['content']}", file=self.out)
        self.out.flush()  # so the notice on stderr cannot land in the middle of a line
        text = DRY_EXPANSION if self.n % 2 == 1 else "[dry run: no model was called]"
        return text, 0, False


def mark_option(p):
    p.add_argument("--mark-destructive", action="store_true",
                   help="EXPERIMENTAL, off by default: mark each place where the answer recommends or shows as a command "
                        "a destructive step (delete a consumer group, a topic, a PVC or an Argo CD Application, reset "
                        "offsets, force, shorten retention, ...) with a bracketed note that names the step and its "
                        "consequence; a step the sentence warns against is not marked. Nothing is removed or reworded. "
                        "It marks what the text says; it cannot tell whether a claim about behaviour is true, and it is "
                        "no measure of harm. The answer is printed when it is complete (not streamed)")


def lookup_option(p):
    p.add_argument("--lookup-cards", action="store_true",
                   help="EXPERIMENTAL and UNMEASURED, off by default (M7 KA1): add reference cards to the prompt, one block "
                        "after the excerpts: the exact facts of the pinned catalogues (type, default, valid values, "
                        "options, fields, errors) for the names in the question and in the search terms, with their pin. "
                        "No extra model call; the lookup takes milliseconds. Needs the catalogues (--catalogues DIR or "
                        "work/m4/catalogues; python scripts/build_catalogues.py). Not available with --service-url or serve")


def answer_profile_options(p):
    p.add_argument("--answer-profile", choices=["thorough"], default=None,
                   help="EXPERIMENTAL and UNMEASURED, off by default: thorough = the draft is written with thinking on, "
                        "then one more call (thinking off) checks it against the excerpts, the catalogue findings and "
                        "the reference cards of its names; a line is changed only with a word-for-word quote and under "
                        "mechanical rules, each change accepted by itself. Needs the catalogues. The answer is printed "
                        "when it is complete (not streamed); after 240 s the check is skipped and the answer says "
                        "'not checked'. Not available with --stream, --service-url or serve")
    p.add_argument("--no-thinking", action="store_true",
                   help="with --answer-profile thorough: the draft without thinking (check only; experimental)")
    p.add_argument("--no-verify", action="store_true",
                   help="with --answer-profile thorough: no checking pass (thinking only; experimental)")
    p.add_argument("--no-cards", action="store_true",
                   help="with --answer-profile thorough: no reference cards in the check (experimental)")


def thorough_profile(a):
    """The verify_pass.Profile of the options, or None when the profile is off."""
    if not getattr(a, "answer_profile", None):
        return None
    from .verify_pass import Profile
    return Profile(thinking=not a.no_thinking, verify=not a.no_verify, cards=not a.no_cards)


def answer_profile_error(a):
    """The refusal for a bad combination of the answer-profile options, or None."""
    sub = [o for o, v in (("--no-thinking", getattr(a, "no_thinking", False)),
                          ("--no-verify", getattr(a, "no_verify", False)),
                          ("--no-cards", getattr(a, "no_cards", False))) if v]
    if not getattr(a, "answer_profile", None):
        return f"{', '.join(sub)} needs --answer-profile thorough" if sub else None
    if a.cmd == "serve":
        return "--answer-profile cannot be used with serve: the service has no answer profile"
    if getattr(a, "service_url", None):
        return "--answer-profile cannot be used with --service-url: the service has no answer profile"
    if getattr(a, "stream", False):
        return "--answer-profile thorough holds the answer until it is checked; do not combine it with --stream"
    if getattr(a, "revise", False):
        return "--answer-profile thorough has its own checking pass; do not combine it with --revise"
    if a.no_thinking and a.no_verify:
        return "--no-thinking with --no-verify leaves nothing of the answer profile"
    if a.no_cards and a.no_verify:
        return "--no-cards applies to the checking pass; it cannot be combined with --no-verify"
    return None


def build_parser():
    ap = argparse.ArgumentParser(prog="python -m kafka_sre_rag", description="Retrieval-augmented Kafka SRE answers.")
    sub = ap.add_subparsers(dest="cmd", required=True)

    def common(p):
        p.add_argument("--k", choices=["k6", "k3"], default="k6")
        p.add_argument("--profile", choices=list(profiles.PROFILES), default="frozen",
                       help="retrieval profile: frozen (default, the measured one) or the candidates r1, r2, r3 and r4 "
                            "(kafka_sre_rag/candidate.json; every answer is marked unmeasured). "
                            "EXPERIMENTAL: r1 to r4 are candidates; none of them passed its acceptance rule, so frozen stays the default")
        p.add_argument("--mode", choices=["default", "strict", "grounded-g1", "grounded-g2", "grounded-g3"], default="default",
                       help="default (the measured prompt), strict, or grounded-g1 / grounded-g2 / grounded-g3. "
                            "EXPERIMENTAL: the grounded modes were checked on development data only and failed the gate to become the default; "
                            "every answer is marked unmeasured; "
                            "they need excerpts and cannot be used with --service-url")
        p.add_argument("--backend", choices=["mlx", "openai"], default="mlx")
        p.add_argument("--model", help="mlx: model directory; openai: model name sent to the server")
        p.add_argument("--adapter", help="mlx only: LoRA adapter directory")
        p.add_argument("--base-url", help="openai: server URL (default: environment variable OMLX_URL)")
        p.add_argument("--extra-body", type=json.loads, default=None, metavar="JSON",
                       help="openai: JSON merged into the request body, e.g. "
                            "'{\"chat_template_kwargs\": {\"enable_thinking\": false}}' to switch thinking off")
        p.add_argument("--max-tokens", type=int, default=None, metavar="N",
                       help="token limit for both model calls (default: the frozen limits, 300 and 2048). "
                            "A reasoning model on the server needs a larger value; the run is then unmeasured")
        p.add_argument("--sources", action="store_true", help="print the retrieved sources under the answer")
        p.add_argument("--json", action="store_true", help="print one JSON object")
        p.add_argument("--check", action="store_true",
                       help="check the names in the answer (config keys, fields, flags, errors) against the "
                            "catalogues of what exists and print a note under the answer; the answer itself and "
                            "the prompt are not changed. Needs the catalogues (python scripts/build_catalogues.py). "
                            "R1: each item the catalogues contradict is also marked at its line in the answer "
                            "(a comment line in a code block, a bracketed note in prose); no model call")
        p.add_argument("--revise", action="store_true",
                       help="EXPERIMENTAL, off by default (on open data it removed hardly any counted error and "
                            "once produced a harmful command). With --check: R2, one more call to the same model "
                            "that replaces the flagged lines; the replacement is used only if the check passes it "
                            "again, otherwise the R1 marks stay. Costs about 15 to 20 s on an answer with a "
                            "contradicted item")
        p.add_argument("--catalogues", metavar="DIR", default=None,
                       help="catalogues for --check (default: work/m4/catalogues)")
        p.add_argument("--dry-run", action="store_true",
                       help="no model: stub generator, prints the exact messages that would be sent")
        p.add_argument("--no-rerank-in-dry-run", action="store_true",
                       help="with --dry-run, skip the cross-encoder (BM25 order; fast, unmeasured)")

    a = sub.add_parser("ask", help="answer one question")
    a.add_argument("question")
    common(a)
    mark_option(a)
    lookup_option(a)
    answer_profile_options(a)
    a.add_argument("--stream", action="store_true",
                   help="print the answer as it is generated (a <think> block is not shown); the sources follow")
    a.add_argument("--service-url", metavar="URL",
                   help="send the question to a running `serve` (for example http://127.0.0.1:8765) instead of "
                        "loading the index and the model here; model options are those of the service")
    c = sub.add_parser("chat", help="interactive loop; each turn is retrieved and answered independently "
                                    "(no conversation history is sent; multi-turn is unmeasured)")
    common(c)
    mark_option(c)
    lookup_option(c)
    answer_profile_options(c)
    sv = sub.add_parser("serve", help="long-running local service, OpenAI-compatible, loopback only")
    common(sv)
    answer_profile_options(sv)
    sv.add_argument("--host", default="127.0.0.1", help="bind address (default 127.0.0.1; loopback only)")
    sv.add_argument("--port", type=int, default=8765)
    sv.add_argument("--allow-non-local", action="store_true",
                    help="allow a bind address that is not loopback (prints a warning; GOAL.md: nothing leaves "
                         "the machine)")
    v = sub.add_parser("verify", help="check the index sha256 and the prompt hashes against frozen.json; "
                                      "reports the catalogue hash")
    v.add_argument("--catalogues", metavar="DIR", default=None, help="default: work/m4/catalogues")
    from .tools import cli as tools_cli  # EXPERIMENTAL (M7, W30): diagnose with read-only tools
    tools_cli.add_parser(sub)
    return ap


def make_generator(a):
    if a.dry_run:
        return StubGenerator()
    if a.backend == "openai":
        from .generators import OpenAIChatGenerator
        if not a.model:
            sys.exit("--model is required for the openai backend")
        return OpenAIChatGenerator(a.base_url or os.environ.get("OMLX_URL"), a.model,
                                   extra_body=a.extra_body)
    from .generators import MLXGenerator
    model = a.model or os.environ.get("BASE_PATH")
    if not model:
        sys.exit("--model (or BASE_PATH) is required for the mlx backend")
    return MLXGenerator(model, a.adapter)


def print_sources(sources):
    print("\nSources:")
    for s in sources:
        ce = "n/a" if s["ce_score"] is None else f"{s['ce_score']:.3f}"
        print(f"  [{s['bm25_rank']}] {s['source']} - {s['title'] or s['path'].rsplit('/', 1)[-1]} ({s['path']}) ce={ce}")


def ask_service(a):
    """ask --service-url: one request to a running service; the output is that of a local ask."""
    import httpx
    from .generators import is_loopback_host
    base = a.service_url.rstrip("/")
    url = base + ("/chat/completions" if base.endswith("/v1") else "/v1/chat/completions")
    body = {"model": "kafka-sre-strict" if a.mode == "strict" else "kafka-sre", "k": a.k, "stream": bool(a.stream),
            "messages": [{"role": "user", "content": a.question}]}
    if a.check:
        body["check"] = True
    if a.revise:
        body["revise"] = True
    local = is_loopback_host(base)
    flt, extra, text = ThinkFilter(), None, ""
    try:
        with httpx.Client(timeout=900, trust_env=not local) as c, c.stream("POST", url, json=body) as r:
            if r.status_code >= 400:
                r.read()
                try:
                    msg = r.json()["error"]["message"]
                except (ValueError, KeyError, TypeError):
                    msg = " ".join(r.text.split())[:300]
                raise ServerError(f"service {base} returned HTTP {r.status_code}: {msg}")
            if not a.stream:
                r.read()
                d = r.json()
                text, extra = d["choices"][0]["message"]["content"], d["kafka_sre"]
            else:
                for line in r.iter_lines():
                    if not line.startswith("data:") or line[5:].strip() == "[DONE]":
                        continue
                    d = json.loads(line[5:])
                    if "error" in d:
                        raise ServerError(f"service {base}: {d['error'].get('message')}")
                    for ch in d.get("choices") or []:
                        piece = (ch.get("delta") or {}).get("content") or ""
                        if piece and not a.json:
                            print(piece, end="", flush=True)
                        text += piece
                    extra = d.get("kafka_sre") or extra
    except httpx.HTTPError as e:
        raise ServerError(f"cannot reach the service at {base} ({type(e).__name__})") from None
    check = extra.get("check")
    cut = text.rfind("\n\n" + check["note"]) if check else -1
    if cut >= 0:  # the service appended the note (and, when streamed, a corrections block) to the text
        answer_text = text[:cut]
    else:
        answer_text = text
    if a.json:
        out = {"question": a.question, "text": answer_text, "sources": extra["sources"],
               "expansion": extra["expansion"], "timings": extra["timings"],
               "measured": extra["measured"], "tokens": extra["tokens"],
               "truncated": extra["truncated"], "unmeasured_reasons": extra["unmeasured_reasons"]}
        out.update({k: extra[k] for k in ("retrieval_profile", "retrieval_settings_sha256") if k in extra})
        if check:
            out["check"] = check
        print(json.dumps(out, ensure_ascii=False))
        return
    banner = config.unmeasured_banner(extra["unmeasured_reasons"])
    if a.stream:
        print()
    sys.stdout.flush()
    if banner:
        print(banner, file=sys.stderr)
    if not a.stream:
        print(text)  # with --check the service has appended the note after a blank line
    if a.sources:
        print_sources(extra["sources"])
    if extra["truncated"]:
        print("(answer cut at max_tokens)", file=sys.stderr)


def serve(a, cfg, checker=None):
    from . import service
    if not service.is_loopback_bind(a.host):
        if not a.allow_non_local:
            print(f"error: refusing to bind to {a.host!r}: the service answers on the loopback interface only "
                  "(pass --allow-non-local to override)", file=sys.stderr)
            return 2
        print(f"WARNING: binding to {a.host!r}, which is not loopback: anyone who can reach this address can "
              "send questions and read the retrieved sources. GOAL.md requires that nothing leaves the machine.",
              file=sys.stderr)
    verified = False
    if not a.dry_run:
        try:
            config.verify(cfg=cfg)
            profiles.verify_index(a.profile)
            verified = True
        except config.ConfigError as e:
            print(e, file=sys.stderr)
            return 1
    gen = make_generator(a)
    svc = service.Service(gen, cfg, mode=a.mode, k=a.k, profile=a.profile, rerank=not (a.dry_run and a.no_rerank_in_dry_run),
                          max_tokens=a.max_tokens, verified=verified, check=a.check,
                          catalogues=a.catalogues, checker=checker, revise=a.revise)
    print("loading the index, the BM25 structures and the reranker ...", file=sys.stderr, flush=True)
    secs = svc.warm_up()
    srv = service.make_server(svc, a.host, a.port, a.allow_non_local)
    print(f"ready in {secs:.1f} s; listening on http://{a.host}:{srv.server_address[1]}/v1 "
          f"(models: {', '.join(service.MODELS)})", file=sys.stderr, flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print(file=sys.stderr)
    finally:
        srv.server_close()
    return 0


def run_one(question, gen, cfg, a, checker=None):
    from .pipeline import ask
    flt = ThinkFilter() if getattr(a, "stream", False) and not (a.json or a.dry_run) else None
    gate = None
    seen_now = []
    mark_d = getattr(a, "mark_destructive", False)
    thorough = getattr(a, "thorough_profile", None)  # EXPERIMENTAL answer profile; None = off
    live = not (a.json or a.dry_run or mark_d or thorough)  # the answer is printed while it is generated
    if live and checker is not None:  # decision O2, option c: prose as written, a code block only after its check
        gate = revise_mod.FenceGate(lambda s: print(s, end="", flush=True), checker.check,
                                    lambda: seen_now[0] if seen_now else [])
        stream = (lambda t: gate.feed(flt.feed(t))) if flt else gate.feed  # noqa: E731
    elif flt:
        stream = lambda t: print(flt.feed(t), end="", flush=True)  # noqa: E731
    else:
        stream = None if not live else (lambda t: print(t, end="", flush=True))
    kw = dict(k=a.k, mode=a.mode, on_token=stream, rerank=not (a.dry_run and a.no_rerank_in_dry_run),
              max_tokens=a.max_tokens)
    if getattr(a, "lookup_cards", False):  # off: the call stays exactly as it was
        kw.update(lookup_cards=True, lookup_kb=a.lookup_kb)
    if thorough is not None:  # off: the call stays exactly as it was
        if thorough.verify and thorough.cards and getattr(a, "thorough_kb", None) is None:  # loaded once (chat)
            from .kb_lookup import KbLookup
            a.thorough_kb = KbLookup(a.thorough_checker.cats, a.thorough_checker.directory, verify=True)
        kw.update(thorough=thorough, thorough_checker=a.thorough_checker, kb=getattr(a, "thorough_kb", None))
    if a.profile != "frozen":  # the frozen call stays exactly as it was
        kw["profile"] = a.profile
        if not a.json:
            f = profiles.manifest_fields(a.profile)
            print(f"retrieval profile: {f['retrieval_profile']}  settings sha256: {f['retrieval_settings_sha256']}  "
                  f"measured: false", flush=True)
    check = rec = None
    shown = None
    if checker is None:
        ans = ask(question, gen, cfg, **kw)
    else:  # the check runs after the answer is complete, on the excerpts it was written from
        with service.capture_chunks() as seen:
            seen_now = seen
            ans = ask(question, gen, cfg, **kw)
        if gate:  # the end of the stream: a code block still held is checked and shown before anything else runs
            if flt:
                gate.feed(flt.finish())
            gate.finish()
        chunks = seen[0] if seen else []
        check = checker.check(ans.text, chunks)
        rec = revise_mod.Reviser(checker.check, gen if getattr(a, "revise", False) else None, a.max_tokens).revise(
            ans.text, chunks, r2=getattr(a, "revise", False), first=check)
        accepted = rec["r2"]["status"] == "accepted"
        shown = rec["final_text"]
        if accepted:
            check = rec["check_after"]
    n_marks = 0
    if mark_d:  # the answer as it is, with a bracketed note after each destructive step
        shown, n_marks, _ = revise_mod.mark_destructive(ans.text if shown is None else shown)
    if a.json:
        out = {"question": question, "text": ans.text if shown is None else shown, "sources": ans.sources,
               "expansion": ans.expansion, "timings": ans.timings, "measured": ans.measured, "tokens": ans.tokens,
               "truncated": ans.truncated, "unmeasured_reasons": ans.unmeasured_reasons}
        out.update(ans.profile_fields)  # nothing for frozen
        if mark_d:
            out["destructive_marks"] = n_marks
        if ans.lookup is not None:
            out["lookup_cards"] = ans.lookup
        if ans.thorough is not None:
            from . import verify_pass
            # one name per text (review note): first_pass_text is the draft, as in the harness's rows; checked_text is
            # the text after the checking pass (what --check then checks and marks)
            out["first_pass_text"] = ans.thorough["first_pass_text"]
            out["answer_profile"] = dict(thorough.manifest(), measured=False, checked_text=ans.text,
                                         note=verify_pass.summary_line(ans.thorough["record"]),
                                         check=verify_pass.compact(ans.thorough["record"]),
                                         draft_info=ans.thorough["draft_info"], seconds=ans.thorough["seconds"],
                                         cut_while_thinking=ans.thorough["cut_while_thinking"],
                                         draft_streamed=ans.thorough["draft_streamed"])
        if check is not None:
            out["check"] = check
            if ans.thorough is None:  # unchanged without the profile
                out["first_pass_text"] = ans.text
            if getattr(a, "revise", False):
                out["revise"] = revise_mod.public(rec)
        print(json.dumps(out, ensure_ascii=False))
        return
    banner = config.unmeasured_banner(ans.unmeasured_reasons)
    if flt and not gate:
        print(flt.finish(), end="")
    if stream:
        print()  # end the streamed answer's line before the notice
    sys.stdout.flush()
    if banner:
        print(banner, file=sys.stderr)
    if not stream:
        print(ans.text if shown is None else shown)
    if check is not None:
        print("\n" + (rec["check_first"]["note"] if gate else check["note"]))
        if gate and rec["r2"]["status"] == "accepted":  # streamed: the corrections follow the answer
            print("\n" + revise_mod.corrections_block(rec["patches"]))
    if mark_d:
        print("\n(destructive-step marks written: %d; experimental)" % n_marks)
    if ans.thorough is not None:
        from . import verify_pass
        print("\n(%s)" % verify_pass.summary_line(ans.thorough["record"]))
    if a.sources:
        print_sources(ans.sources)
    if ans.truncated:
        print("(answer cut at max_tokens)", file=sys.stderr)


def main(argv=None):
    a = build_parser().parse_args(argv)
    if a.cmd == "diagnose":
        from .tools import cli as tools_cli
        return tools_cli.main(a)
    if a.cmd != "verify":
        why = answer_profile_error(a)
        if why:
            print(f"error: {why}", file=sys.stderr)
            return 2
    if a.cmd == "verify":
        try:
            rep = config.verify()
        except config.ConfigError as e:
            print(e, file=sys.stderr)
            return 1
        i = rep["index"]
        print(f"index    {'match' if i['match'] else 'MISMATCH'}  {i['sha256']}")
        for n, p in rep["prompts"].items():
            print(f"prompt   {'match' if p['match'] else 'MISMATCH'}  {n}  {p['sha256']}")
        r = rep["reranker"]
        print(f"reranker revision {r['revision']}  local cache: "
              f"{'not found' if r['cached'] is None else ('match' if r['match'] else 'MISMATCH')}")
        print(config.catalogues_line(a.catalogues))
        return 0
    if a.cmd == "serve" and a.mode.startswith("grounded"):
        print(f"error: --mode {a.mode} cannot be used with serve: the service has no grounded mode "
              "(a request would be answered in the default mode)", file=sys.stderr)
        return 2
    if a.cmd == "ask" and a.service_url and a.lookup_cards:
        print("error: --lookup-cards is not available with --service-url: the service has no lookup cards", file=sys.stderr)
        return 2
    if a.cmd == "ask" and a.service_url and a.mark_destructive:
        print("error: --mark-destructive is not available with --service-url", file=sys.stderr)
        return 2
    if a.cmd == "ask" and a.service_url:
        if a.mode.startswith("grounded"):
            print(f"error: --mode {a.mode} cannot be used with --service-url: the service has no grounded mode "
                  "and would answer in the default mode", file=sys.stderr)
            return 2
        if a.profile != "frozen":
            print("error: with --service-url the retrieval profile is that of the service (serve --profile ...)",
                  file=sys.stderr)
            return 2
        if a.dry_run or a.model or a.adapter or a.base_url or a.extra_body or a.max_tokens is not None:
            print("error: with --service-url the model options are those of the service; "
                  "do not combine it with --dry-run, --model, --adapter, --base-url, --extra-body, --max-tokens",
                  file=sys.stderr)
            return 2
        try:
            ask_service(a)
        except ServerError as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        return 0
    if getattr(a, "revise", False) and not a.check:
        print("error: --revise needs --check", file=sys.stderr)
        return 2
    if getattr(a, "mark_destructive", False) and getattr(a, "stream", False):
        print("error: --mark-destructive prints the answer when it is complete; do not combine it with --stream",
              file=sys.stderr)
        return 2
    checker = None
    if a.check:  # before anything slow is loaded
        try:
            checker = service.AnswerChecker(a.catalogues)
        except service.CatalogueError as e:
            print(f"error: {e}", file=sys.stderr)
            return 2
    a.thorough_profile = thorough_profile(a)
    a.thorough_checker = None
    if a.thorough_profile is not None:  # EXPERIMENTAL: the checking pass needs the catalogues
        try:
            a.thorough_checker = checker or service.AnswerChecker(a.catalogues)
        except service.CatalogueError as e:
            print(f"error: {e}", file=sys.stderr)
            return 2
    a.lookup_kb = None
    if getattr(a, "lookup_cards", False):  # EXPERIMENTAL: the lookup needs the catalogues; loaded once (hash checked)
        try:
            from .kb_lookup import CatalogueHashError, KbLookup
            ck = checker or a.thorough_checker or service.AnswerChecker(a.catalogues)
            a.lookup_kb = KbLookup(ck.cats, ck.directory, verify=True)
        except (service.CatalogueError, CatalogueHashError) as e:
            print(f"error: {e}", file=sys.stderr)
            return 2
    cfg = config.load()
    if a.cmd == "serve":
        return serve(a, cfg, checker)
    if not a.dry_run:  # a real run must be on the frozen index and prompts
        try:
            config.verify(cfg=cfg)
            profiles.verify_index(a.profile)
        except config.ConfigError as e:
            print(e, file=sys.stderr)
            return 1
    gen = make_generator(a)
    if a.cmd == "ask":
        try:
            run_one(a.question, gen, cfg, a, checker)
        except EmptyAnswerError as e:
            print(f"error: {e}", file=sys.stderr)
            return 2
        except ServerError as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        return 0
    try:  # chat
        while True:
            q = input("question> ").strip()
            if q in ("", "exit", "quit"):
                if q:
                    break
                continue
            try:
                run_one(q, gen, cfg, a, checker)
            except (EmptyAnswerError, ServerError) as e:
                print(f"error: {e}", file=sys.stderr)
    except (EOFError, KeyboardInterrupt):
        print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
