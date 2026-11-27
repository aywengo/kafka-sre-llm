#!/usr/bin/env python
"""M7 B21: byte check for the v4.0.0 runtime merge (the MCP tools branch and the corrected entries wording).

Proves, for the release configuration RE (Gemma 4 31B 4-bit, --lookup-cards, --kb-entries), that everything the model receives is the same
with two runtime trees: the measured one (main at 57eae61, exported with `git archive`) and the merged one.

  check --old TREE --new TREE [--answers FILE] [--items FILE]
      runs `emit` for each tree in its own process (so each side really imports its own kafka_sre_rag) and compares:
      (a) per answer of the D7 RE file: the expansion messages, the answer messages, and the four request bodies that would be sent
          for them (expansion and answer, streamed and not), each as a sha256; the answer messages are rebuilt from the recorded
          pack ids, the recorded search terms and the question of the item, with the runtime's own builders (no retrieval, no model);
          each tree's rebuild is also anchored to the recorded row (pack characters, cards, entries added, sources);
      (b) the request body keys with the tools off (no `tools` key) and that `kafka_sre_rag.tools` is not imported by ask or serve;
      (c) the sha256 of the system prompt, the instruction and EXPAND_SYSTEM against the values of eval/m7/release_test/configs.json.
      One line per item, then "N/N identical" or the differing ids. Exit 0 only if everything matches.
  emit --tree TREE [--answers FILE] [--items FILE]
      the child of check: prints one JSON line with what this tree produces.

Nothing is sent: the HTTP client is given a stub transport. The two trees need the git-ignored data under their own work/ and runs/v3
(symlinks to the owner's data are enough): work/v3/chunks_s1.jsonl, work/m7/kb, work/m4/catalogues, runs/v3/INDEX_FROZEN.json.
"""
import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent

# eval/m7/release_test/configs.json, arm RE (prompt_format and query_expansion)
EXPECTED = {
    "SYSTEM_PROMPT": "766ff0b0e2c61316acc1239425c33f299c2f1136814e2ed6c7135f7dda20da15",
    "INSTRUCTION": "65fe40116e6723ecc5128222fbe5530e7009e79f1cb7a696b12a78138d7f9307",
    "EXPAND_SYSTEM": "a1fb0eefabcd052fad39ac975e1b99ff3282285903af2410e1d9d3c4ce639de9",
}
RE_MODEL = "gemma-4-31b-it-4bit"
RE_EXTRA_BODY = {"chat_template_kwargs": {"enable_thinking": False}}
STUB_URL = "http://127.0.0.1:9"          # never contacted: the transport is a stub
DEFAULT_ANSWERS = "work/m7/release_test/d7/ans_d7_RE.jsonl"
DEFAULT_ITEMS = "eval/m7/d7/items.jsonl"


def sha(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha_json(obj):
    return sha(json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":")))


def read_rows(path):
    return [json.loads(l) for l in Path(path).read_text(encoding="utf-8").splitlines() if l.strip()]


def full_question(item):
    """As scripts/eval2.py full_question: the question, then the evidence when the item has one."""
    q = item["question"]
    if item.get("evidence"):
        q += "\n\nEvidence:\n" + item["evidence"]
    return q


def constants_of(prompts):
    return {name: sha(getattr(prompts, name)) for name in EXPECTED}


# ---------------------------------------------------------------- request bodies (stubbed HTTP)

def capture_bodies(generators, messages_by_name, max_tokens_by_name, model=RE_MODEL, extra_body=None):
    """The raw request bytes that the runtime's generator would POST for each call, with a stub transport.
    Returns {name: {"plain": sha256, "stream": sha256, "keys_plain": [...], "keys_stream": [...]}}."""
    import httpx
    seen = []

    def handler(request):
        if request.method == "GET":
            return httpx.Response(200, json={"data": [{"id": model}]})
        seen.append(request.content)
        body = json.loads(request.content)
        if body.get("stream"):
            chunk = json.dumps({"model": model, "choices": [{"delta": {"content": "x"}, "finish_reason": "stop"}],
                                "usage": {"completion_tokens": 1}})
            return httpx.Response(200, headers={"content-type": "text/event-stream"},
                                  content=f"data: {chunk}\n\ndata: [DONE]\n\n".encode())
        return httpx.Response(200, json={"model": model, "choices": [{"message": {"content": "x"}, "finish_reason": "stop"}],
                                         "usage": {"completion_tokens": 1}})

    gen = generators.OpenAIChatGenerator(base_url=STUB_URL, model=model, extra_body=extra_body or RE_EXTRA_BODY,
                                         transport=httpx.MockTransport(handler))
    out = {}
    for name, msgs in messages_by_name.items():
        n = max_tokens_by_name[name]
        seen.clear()
        gen.chat(msgs, n, None)
        gen.chat(msgs, n, lambda piece: None)
        plain, stream = seen[0], seen[1]
        out[name] = {"plain": hashlib.sha256(plain).hexdigest(), "stream": hashlib.sha256(stream).hexdigest(),
                     "keys_plain": sorted(json.loads(plain)), "keys_stream": sorted(json.loads(stream))}
    return out


# ---------------------------------------------------------------- the child: what one tree produces

def emit(tree, answers, items):
    tree = Path(tree).resolve()
    sys.path.insert(0, str(tree))
    import kafka_sre_rag
    where = Path(kafka_sre_rag.__file__).resolve()
    if tree not in where.parents:
        raise SystemExit(f"emit: kafka_sre_rag was imported from {where}, not from the tree {tree}")
    from kafka_sre_rag import __main__ as cli, config, generators, kb_lookup, prompts, service
    from kafka_sre_rag.kb_entries import KbEntries, resolve_pack
    cfg = config.load()
    exp_max, ans_max = cfg["expansion"]["max_tokens"], cfg["answer"]["max_tokens"]
    by_id = {it["id"]: it for it in read_rows(items)}
    kb = KbEntries.load(None)
    ck = service.AnswerChecker(None)
    lookup_kb = kb_lookup.KbLookup(ck.cats, ck.directory, verify=True)
    out = {"tree": str(tree), "package": str(where), "constants": constants_of(prompts), "items": {}}
    for row in read_rows(answers):
        rid = row["id"]
        res = {"id": rid}
        try:
            q = full_question(by_id[rid])
            exp_msgs = prompts.expand_messages(q)
            chunks = resolve_pack(row["pack"], kb=kb)
            cards_text, lk = kb_lookup.answer_block(q, row.get("search_terms") or "", kb=lookup_kb)
            ans_msgs = prompts.answer_messages(q, chunks, cards_text=cards_text or None)
            res.update(expand_sha=sha_json(exp_msgs), answer_sha=sha_json(ans_msgs),
                       bodies=capture_bodies(generators, {"expand": exp_msgs, "answer": ans_msgs},
                                             {"expand": exp_max, "answer": ans_max}),
                       pack_chars=len(prompts.pack_text(chunks)), n_pack=len(chunks),
                       cards={"cards_found": lk["cards_found"], "cards": lk["cards"], "chars": lk["chars"]},
                       sources=[[c["source"], c["title"], c["path"]] for c in chunks])
        except Exception as e:  # recorded, so that the parent names the item
            res["error"] = f"{type(e).__name__}: {e}"
        out["items"][rid] = res
    # (b) the tools stay out of ask and serve
    for argv in (["ask", "q"], ["serve"]):
        cli.build_parser().parse_args(argv)
    out["tools_imported"] = any(m == "kafka_sre_rag.tools" or m.startswith("kafka_sre_rag.tools.") for m in sys.modules)
    print(json.dumps(out, sort_keys=True))


# ---------------------------------------------------------------- the parent: compare two trees

def anchor_problems(res, row):
    """The rebuild must agree with what the answer recorded (so that old == new is not two wrong values that agree)."""
    bad = []
    ke = row.get("kb_entries") or {}
    if ke.get("pack_chars") is not None and res["pack_chars"] != ke["pack_chars"]:
        bad.append(f"pack_chars {res['pack_chars']} != recorded {ke['pack_chars']}")
    if res["n_pack"] != len(row["pack"]):
        bad.append("pack size")
    lk = row.get("lookup") or {}
    for k in ("cards_found", "cards", "chars"):
        if k in lk and res["cards"][k] != lk[k]:
            bad.append(f"cards.{k} {res['cards'][k]} != recorded {lk[k]}")
    rec = [[s["source"], s["title"], s["path"]] for s in row.get("sources") or []]
    if rec and res["sources"] != rec:
        bad.append("sources differ from the recorded ones")
    return bad


def compare(old, new, rows):
    """old, new: the emit outputs; rows: the recorded answer rows. Returns (lines, summary, ok)."""
    lines, same, differ, ok = [], 0, [], True
    for row in rows:
        rid = row["id"]
        o, n = old["items"].get(rid, {"error": "missing"}), new["items"].get(rid, {"error": "missing"})
        why = []
        if o.get("error") or n.get("error"):
            why.append(f"error old={o.get('error')} new={n.get('error')}")
        else:
            if o["expand_sha"] != n["expand_sha"]:
                why.append("expansion messages differ")
            if o["answer_sha"] != n["answer_sha"]:
                why.append("answer messages differ")
            if o["bodies"] != n["bodies"]:
                why.append("request bodies differ")
            for side, r in (("old", o), ("new", n)):
                why += [f"{side} anchor: {p}" for p in anchor_problems(r, row)]
        if why:
            differ.append(rid)
            lines.append(f"{rid} DIFFERENT: {'; '.join(why)}")
        else:
            same += 1
            lines.append(f"{rid} identical: expansion {n['expand_sha'][:12]}, answer {n['answer_sha'][:12]}, "
                         f"bodies {n['bodies']['answer']['plain'][:12]}/{n['bodies']['answer']['stream'][:12]}, "
                         f"pack {n['n_pack']} items {n['pack_chars']} chars, cards {n['cards']['cards']}")
    total = f"{same}/{len(rows)} identical" + (f"; differing: {', '.join(differ)}" if differ else "")
    ok = not differ
    return lines, total, ok


def payload_report(old, new):
    """(b): the key set of every request body, the same in both trees, no tools key, tools not imported."""
    lines, ok = [], True
    keys = set()
    for side, t in (("old", old), ("new", new)):
        for it in t["items"].values():
            for b in (it.get("bodies") or {}).values():
                keys.update(b["keys_plain"] + b["keys_stream"])
    bad_tools = sorted(k for k in keys if k.startswith("tool"))
    lines.append(f"(b) request body keys over all calls: {sorted(keys)}")
    if bad_tools:
        ok = False
        lines.append(f"(b) FAIL: tool keys in a request body: {bad_tools}")
    if old["tools_imported"] or new["tools_imported"]:
        ok = False
        lines.append(f"(b) FAIL: kafka_sre_rag.tools imported by ask/serve parsing (old {old['tools_imported']}, new {new['tools_imported']})")
    else:
        lines.append("(b) kafka_sre_rag.tools is not imported by the ask and serve parsers in either tree")
    lines.append("(b) " + ("no tools key and no tools import; the bodies' sha256 are compared per item in (a)" if ok else "FAIL"))
    return lines, ok


def constants_report(old, new):
    lines, ok = [], True
    for name, want in EXPECTED.items():
        o, n = old["constants"][name], new["constants"][name]
        good = o == n == want
        ok &= good
        lines.append(f"(c) {name} {n} {'unchanged' if good else 'DIFFERENT (old ' + o + ', expected ' + want + ')'}")
    return lines, ok


def run_emit(tree, answers, items):
    p = subprocess.run([sys.executable, str(HERE / "m7_v4_bytecheck.py"), "emit", "--tree", str(tree), "--answers", str(answers),
                        "--items", str(items)], capture_output=True, text=True, cwd=str(tree))
    if p.returncode != 0:
        raise SystemExit(f"emit failed for {tree}:\n{p.stderr[-2000:]}")
    return json.loads(p.stdout.strip().splitlines()[-1])


def check(args):
    answers, items = Path(args.answers), Path(args.items)
    rows = read_rows(answers)
    old, new = run_emit(args.old, answers, items), run_emit(args.new, answers, items)
    if old["package"] == new["package"]:
        raise SystemExit("the two trees import the same package; nothing would be compared")
    lines, total, ok_a = compare(old, new, rows)
    print("\n".join(lines))
    print(f"(a) {total}")
    pl, ok_b = payload_report(old, new)
    cl, ok_c = constants_report(old, new)
    print("\n".join(pl + cl))
    ok = ok_a and ok_b and ok_c
    print(f"TOTAL {'PASS' if ok else 'FAIL'}: (a) {total}; (b) {'ok' if ok_b else 'FAIL'}; (c) {'ok' if ok_c else 'FAIL'}")
    return 0 if ok else 1


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("check", "emit"):
        s = sub.add_parser(name)
        s.add_argument("--answers", default=str(ROOT / DEFAULT_ANSWERS))
        s.add_argument("--items", default=str(ROOT / DEFAULT_ITEMS))
        if name == "check":
            s.add_argument("--old", required=True, help="the measured tree (git archive of 57eae61)")
            s.add_argument("--new", default=str(ROOT), help="the merged tree (default: this one)")
        else:
            s.add_argument("--tree", required=True)
    a = ap.parse_args(argv)
    if a.cmd == "emit":
        emit(a.tree, a.answers, a.items)
        return 0
    return check(a)


if __name__ == "__main__":
    sys.exit(main())
