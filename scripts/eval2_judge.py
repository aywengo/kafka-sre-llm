#!/usr/bin/env python3
"""Tier B judge step of the new evaluation (agents/m4-evaluation.md, "Tier B: judged" and "Rubric pilot").

One call to the judge per answer and pass. The judge gets the instruction of the design, the question, any
evidence, the rubric points with their ids and the item's source passages. It never gets a reference answer.
The reply is validated (every point has a verdict; a "present" or "contradicted" verdict needs a quote that
occurs in the answer after whitespace normalisation, otherwise it becomes "absent" and is counted as a judge
error) and turned into the item score of the design.

  scripts/eval2_judge.py run --rubrics eval/m4/pilot/rubrics.jsonl \
      --qmap real=eval/questions_real.jsonl holdout=eval/questions_runbooks_holdout.jsonl \
      --chunks work/v3/chunks_s1.jsonl \
      --answers real_default=work/v3/ans_rag_real_s1.jsonl holdout_default=... \
      --cache work/m4/pilot/judge_cache.jsonl --out work/m4/pilot/judge_results.jsonl \
      --judge-url "$OMLX_URL/v1" --judge-model GLM-5.3-Flash-oQ8e-mtp --extra-body '{"max_tokens": 32768}'
  scripts/eval2_judge.py run ... --stub            # dry run: no server, stub replies, prints counts and an estimate
  scripts/eval2_judge.py run ... --rescore         # no calls: rebuild --out from --cache
  scripts/eval2_judge.py gen-generic --rubrics ... --qmap ... --out eval/m4/pilot/generic_answers.jsonl

Data formats (so that tier B items of the new evaluation use the same code):
  rubric row   {"id", "question" (optional if the id is "<prefix>-<n>" and --qmap has the prefix), "evidence" (optional),
                "must": [{"id","text","weight","first"}], "must_not": [{"id","text","severity"}], "any_of": [[ids]],
                "covered_chunks": [chunk ids] and/or "source_text": str or [str]}
  answer file  JSONL rows with "question" and "text"; rows are matched to rubrics by the question text
               (whitespace-normalised). An answer set is given as NAME=PATH.
  chunks file  JSONL rows with "id" and "text" (--chunks); optional, only for rubrics that give chunk ids.

Scoring (per answer and pass), exactly as the design states:
  P = (weights of must points present) / (all must weights); W = must-not "wrong" present + must "contradicted";
  H = must-not "harmful" present; S = max(-1, P - 0.25 W - 0.5 H); S with the W penalty 0 and 0.5 as well.
  An any_of group counts as ONE point: weight = the highest weight among its members, present if any member is
  present. Groups that share a member are merged. A contradicted member still counts in W.
  Essential miss: a weight-2 must point is absent and no member of its any_of group is present.

Judge call: OpenAI-compatible chat completions; URL and key from --judge-url / OMLX_URL and JUDGE_API_KEY /
OMLX_API_KEY / ~/.config/kafka-sre-llm/. Busy statuses (429, 503, 507) are waited out as in scripts/generate_qa.py.
Neither URL nor key is ever printed.
"""
import argparse
import hashlib
import json
import math
import os
import re
import sys
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

JUDGE_INSTRUCTION = """You check one answer from an assistant for Kafka, Strimzi, Kubernetes and Argo CD operators against a list of points. Judge each point on its own. Do not grade style, length or tone.
For each MUST point, the verdict is "present" only if the answer states the point explicitly and specifically enough that an engineer following the answer would act on it correctly. A setting, flag, field, annotation, metric, API path or HTTP method counts only if it is named as in the point. A vague mention ("check the configuration", "look at the logs") is "absent". If the answer states the point and contradicts it elsewhere, the verdict is "contradicted". If the point says it must be the main or first recommendation and the answer gives it only as one option among several, the verdict is "absent".
For each MUST-NOT point, the verdict is "present" if the answer states or recommends it, even in passing or as an alternative, and "absent" otherwise. A statement the answer explicitly rejects is "absent".
For every "present" or "contradicted" verdict, quote the shortest passage of the answer that shows it. If you cannot quote it, the verdict is "absent".
Then list any other statement in the answer that contradicts the source passages below, with a quote. Do not list statements that are only missing from the sources.
Reply only with JSON: {"must":[{"id":"","verdict":"present|absent|contradicted","quote":""}],"must_not":[{"id":"","verdict":"present|absent","quote":""}],"other_errors":[{"quote":"","why":""}]}"""

BUSY_STATUS = (429, 503, 507)
BUSY_RETRIES, BUSY_WAIT_S = 20, 60
MUST_VERDICTS = ("present", "absent", "contradicted")
MUSTNOT_VERDICTS = ("present", "absent")
DEFAULT_SOURCE_CHARS = 12000
SECONDS_PER_ITEM_DESIGN = 90      # design, "Cost of a run": 1.5 minutes per item with three in parallel
SECONDS_PER_CALL_V3_MEDIAN = 160  # v3: median per head-to-head call (sequential; divide by the concurrency)


def rd(path):
    return [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()]


def norm_ws(s):
    return re.sub(r"\s+", " ", s or "").strip()


# ---------------------------------------------------------------- items

def load_qmap(specs):
    """['real=eval/questions_real.jsonl', ...] -> {'real': [question, ...]} (1-based positions)."""
    out = {}
    for spec in specs or []:
        name, _, path = spec.partition("=")
        out[name] = [r["question"] for r in rd(path)]
    return out


def rubric_question(rubric, qmap):
    if rubric.get("question"):
        return rubric["question"]
    m = re.fullmatch(r"(.+)-(\d+)", rubric["id"])
    if m and m.group(1) in qmap and 1 <= int(m.group(2)) <= len(qmap[m.group(1)]):
        return qmap[m.group(1)][int(m.group(2)) - 1]
    raise ValueError(f"rubric {rubric['id']}: no question text (no 'question' field and no --qmap entry)")


def source_text(rubric, chunks, cap):
    """Source passages of an item: 'covered_chunks' ids looked up in the chunk index, plus 'source_text'.
    Total length capped at `cap` characters: each passage gets an equal share of the cap, shares that a short
    passage does not use go to the longer ones, a cut passage ends with ' [...]'."""
    parts = []
    for cid in rubric.get("covered_chunks") or []:
        if cid not in chunks:
            raise ValueError(f"rubric {rubric['id']}: chunk {cid} not in the chunk file")
        c = chunks[cid]
        head = c.get("path") or c.get("title") or cid
        parts.append((f"[{cid}] {head}", c["text"].strip()))
    st = rubric.get("source_text")
    if st:
        for i, t in enumerate([st] if isinstance(st, str) else st):
            parts.append((f"[text {i + 1}]", t.strip()))
    if not parts:
        return ""
    budget = cap
    order = sorted(range(len(parts)), key=lambda i: len(parts[i][1]))
    share = {}
    left = len(parts)
    for i in order:
        s = max(budget // left, 0)
        share[i] = min(len(parts[i][1]), s)
        budget -= share[i]
        left -= 1
    out = []
    for i, (head, text) in enumerate(parts):
        t = text if share[i] >= len(text) else text[:max(share[i] - 6, 0)].rstrip() + " [...]"
        out.append(f"{head}\n{t}")
    return "\n\n".join(out)


def point_text(p):
    t = p["text"].strip()
    if p.get("first") and "first" not in t.lower() and "main" not in t.lower():
        t += " (This must be the main or first recommendation, not one option among several.)"
    return t


def build_prompt(item, pass_no, chunks, cap):
    """-> (system, user). Pass 2 lists the points (must and must-not) in reverse order."""
    r = item["rubric"]
    must, must_not = list(r["must"]), list(r.get("must_not") or [])
    if pass_no == 2:
        must, must_not = must[::-1], must_not[::-1]
    lines = [f"QUESTION:\n{item['question'].strip()}"]
    if r.get("evidence"):
        lines.append(f"EVIDENCE (pasted by the operator):\n{r['evidence'].strip()}")
    lines.append("MUST points:\n" + "\n".join(f"- id {p['id']}: {point_text(p)}" for p in must))
    lines.append("MUST-NOT points:\n" + ("\n".join(f"- id {p['id']}: {point_text(p)}" for p in must_not) or "(none)"))
    src = item.get("source")
    if src is None:
        src = source_text(r, chunks, cap)
    lines.append("SOURCE PASSAGES:\n" + (src or "(none)"))
    lines.append("ANSWER TO CHECK:\n<<<\n" + item["answer"].strip() + "\n>>>")
    return JUDGE_INSTRUCTION, "\n\n".join(lines)


def match_items(rubrics, answer_sets, qmap):
    """Match the answers of each set to the rubrics by question text. -> (items, counts per set)."""
    items, counts = [], OrderedDict()
    for name, rows in answer_sets.items():
        by_q = {}
        for row in rows:
            by_q.setdefault(norm_ws(row.get("question")), row)
        counts[name] = 0
        for r in rubrics:
            q = rubric_question(r, qmap)
            row = by_q.get(norm_ws(q))
            if row is None or not (row.get("text") or "").strip():
                continue
            items.append({"set": name, "id": r["id"], "question": q, "answer": row["text"], "rubric": r})
            counts[name] += 1
    return items, counts


# ---------------------------------------------------------------- scoring

def groups_of(rubric):
    """any_of groups with overlapping groups merged: list of sets of must ids."""
    groups = []
    for g in rubric.get("any_of") or []:
        g = set(g)
        for old in [x for x in groups if x & g]:
            g |= old
            groups.remove(old)
        groups.append(g)
    return groups


def score_item(rubric, must_v, mustnot_v):
    """must_v: {id: 'present'|'absent'|'contradicted'}; mustnot_v: {id: 'present'|'absent'}. -> dict of numbers."""
    weight = {p["id"]: p["weight"] for p in rubric["must"]}
    groups = groups_of(rubric)
    in_group = {i: g for g in groups for i in g if i in weight}
    units = []  # (weight, present)
    for p in rubric["must"]:
        if p["id"] not in in_group:
            units.append((p["weight"], must_v.get(p["id"]) == "present"))
    for g in groups:
        members = [i for i in g if i in weight]
        if members:
            units.append((max(weight[i] for i in members), any(must_v.get(i) == "present" for i in members)))
    total = sum(w for w, _ in units)
    P = sum(w for w, pres in units if pres) / total if total else 0.0
    sev = {p["id"]: p["severity"] for p in rubric.get("must_not") or []}
    W = sum(1 for i, v in mustnot_v.items() if v == "present" and sev.get(i) == "wrong") \
        + sum(1 for i, v in must_v.items() if v == "contradicted")
    H = sum(1 for i, v in mustnot_v.items() if v == "present" and sev.get(i) == "harmful")
    s = lambda pen: max(-1.0, P - pen * W - 0.5 * H)
    essential_miss = False
    for p in rubric["must"]:
        if p["weight"] == 2 and must_v.get(p["id"]) == "absent":
            g = in_group.get(p["id"])
            if g and any(must_v.get(i) == "present" for i in g if i != p["id"]):
                continue
            essential_miss = True
    return {"P": P, "W": W, "H": H, "S": s(0.25), "S_w0": s(0.0), "S_w05": s(0.5),
            "essential_miss": essential_miss, "harmful": H > 0}


def validate_reply(rubric, answer, j):
    """Check a parsed reply against the rubric and the answer.
    -> (must {id: {verdict, quote, quote_ok, judge_verdict}}, must_not {...}, other_errors [...], errors [str], n_claims, n_failed).
    A point without a usable verdict, or a present/contradicted verdict whose quote is not in the answer, is "absent"."""
    ans = norm_ws(answer)
    errors, n_claims, n_failed = [], 0, 0

    def one(points, reply_list, allowed, kind):
        nonlocal n_claims, n_failed
        got = {}
        for e in reply_list if isinstance(reply_list, list) else []:
            if isinstance(e, dict) and isinstance(e.get("id"), str):
                got.setdefault(e["id"], e)
        for e in got:
            if e not in {p["id"] for p in points}:
                errors.append(f"unknown_id:{e}")
        out = {}
        for p in points:
            e = got.get(p["id"])
            if e is None:
                errors.append(f"missing:{p['id']}")
                out[p["id"]] = {"verdict": "absent", "quote": "", "quote_ok": None, "judge_verdict": None}
                continue
            v, q = e.get("verdict"), e.get("quote") if isinstance(e.get("quote"), str) else ""
            if v not in allowed:
                errors.append(f"bad_verdict:{p['id']}")
                out[p["id"]] = {"verdict": "absent", "quote": q, "quote_ok": None, "judge_verdict": v}
                continue
            if v == "absent":
                out[p["id"]] = {"verdict": "absent", "quote": q, "quote_ok": None, "judge_verdict": v}
                continue
            n_claims += 1
            ok = bool(norm_ws(q)) and norm_ws(q) in ans
            if not ok:
                n_failed += 1
                errors.append(f"quote_not_found:{p['id']}")
            out[p["id"]] = {"verdict": v if ok else "absent", "quote": q, "quote_ok": ok, "judge_verdict": v}
        return out

    must = one(rubric["must"], j.get("must"), MUST_VERDICTS, "must")
    must_not = one(rubric.get("must_not") or [], j.get("must_not"), MUSTNOT_VERDICTS, "must_not")
    other = []
    for e in j.get("other_errors") or []:
        if isinstance(e, dict) and isinstance(e.get("quote"), str):
            other.append({"quote": e["quote"], "why": e.get("why") if isinstance(e.get("why"), str) else "",
                          "quote_ok": norm_ws(e["quote"]) in ans and bool(norm_ws(e["quote"]))})
    return must, must_not, other, errors, n_claims, n_failed


def result_row(item, pass_no, entry):
    """One output row from a cache entry (reply parsed or None)."""
    base = {"set": item["set"], "id": item["id"], "question": item["question"], "pass": pass_no,
            "answer": item["answer"], "key": entry["key"], "seconds": entry.get("seconds"),
            "retried": bool(entry.get("retried"))}
    j = entry.get("j")
    if not isinstance(j, dict):
        return {**base, "valid_reply": False, "judge_errors": ["invalid_json"]}
    must, must_not, other, errors, n_claims, n_failed = validate_reply(item["rubric"], item["answer"], j)
    sc = score_item(item["rubric"], {k: v["verdict"] for k, v in must.items()},
                    {k: v["verdict"] for k, v in must_not.items()})
    return {**base, "valid_reply": True, "must": must, "must_not": must_not, "other_errors": other,
            "other_errors_n": len(other), "judge_errors": errors, "quote_claims": n_claims, "quote_failed": n_failed, **sc}


# ---------------------------------------------------------------- judge call

def parse_reply(content):
    txt = re.sub(r"<think>.*?</think>", "", content or "", flags=re.S)
    m = re.search(r"\{.*\}", txt, flags=re.S)
    if not m:
        return None
    try:
        j = json.loads(m.group(0))
    except json.JSONDecodeError:
        return None
    return j if isinstance(j, dict) else None


class Judge:
    def __init__(self, url, model, api_key=None, extra_body=None, temperature=0.0, client=None,
                 busy_wait_s=BUSY_WAIT_S, busy_retries=BUSY_RETRIES):
        import httpx
        self.httpx = httpx
        self.url, self.model, self.extra_body, self.temperature = url.rstrip("/"), model, extra_body or {}, temperature
        self.client = client or httpx.Client(timeout=1800, headers={"Authorization": f"Bearer {api_key}"} if api_key else {})
        self.busy_wait_s, self.busy_retries = busy_wait_s, busy_retries

    def chat(self, system, user, temperature):
        body = {**self.extra_body, "model": self.model, "temperature": temperature,
                "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}]}
        attempt = busy = 0
        while True:
            try:
                r = self.client.post(f"{self.url}/chat/completions", json=body)
                r.raise_for_status()
                data = r.json()
                return data["choices"][0]["message"].get("content") or "", data.get("usage", {}).get("completion_tokens")
            except (self.httpx.HTTPError, ValueError, KeyError, IndexError, TypeError, AttributeError) as e:
                what = type(e).__name__   # class and status only: the error text carries the server address
                code = e.response.status_code if isinstance(e, self.httpx.HTTPStatusError) else None
                if code:
                    what += f" {code}"
                if code in BUSY_STATUS and busy < self.busy_retries:
                    busy += 1
                    print(f"  server busy ({what}), waiting {self.busy_wait_s}s ({busy}/{self.busy_retries})", flush=True)
                    time.sleep(self.busy_wait_s)
                    continue
                if attempt == 2:
                    raise
                time.sleep(2 ** attempt if self.busy_wait_s >= 1 else 0)
                attempt += 1
                print(f"  retry after {what}", flush=True)

    def __call__(self, system, user):
        """-> (parsed reply or None, raw content, retried). A reply that is not valid JSON is retried once at temperature 0."""
        content, _ = self.chat(system, user, self.temperature)
        j = parse_reply(content)
        if j is not None:
            return j, content, False
        content2, _ = self.chat(system, user, 0)
        return parse_reply(content2), content2, True


def stub_judge(system, user):
    """Dry run: every point absent. Reads the ids from the prompt."""
    must = re.findall(r"^- id (\S+):", user.split("MUST-NOT points:")[0], flags=re.M)
    mn = re.findall(r"^- id (\S+):", user.split("MUST-NOT points:")[1].split("SOURCE PASSAGES:")[0], flags=re.M)
    j = {"must": [{"id": i, "verdict": "absent", "quote": ""} for i in must],
         "must_not": [{"id": i, "verdict": "absent", "quote": ""} for i in mn], "other_errors": []}
    return j, json.dumps(j), False


def cache_key(model, pass_no, item, system, user):
    return hashlib.md5(f"{model}\0{pass_no}\0{item['set']}\0{item['id']}\0{system}\0{user}".encode()).hexdigest()


def load_cache(path, retry_invalid=False):
    cache = {}
    if path and Path(path).exists():
        for line in Path(path).read_text().splitlines():
            if line.strip():
                c = json.loads(line)
                if retry_invalid and not isinstance(c.get("j"), dict):
                    continue
                cache[c["key"]] = c
    return cache


def run(items, passes, judge, model, chunks, cap, cache_path, concurrency=1, retry_invalid=False, calls=True, say=print):
    """Judge every (item, pass) not in the cache. -> list of result rows in a fixed order."""
    cache = load_cache(cache_path, retry_invalid)
    jobs = []
    for it in items:
        for p in passes:
            system, user = build_prompt(it, p, chunks, cap)
            jobs.append((it, p, system, user, cache_key(model, p, it, system, user)))
    lock = threading.Lock()

    def do(job):
        it, p, system, user, key = job
        t0 = time.time()
        j, raw, retried = judge(system, user)
        entry = {"key": key, "set": it["set"], "id": it["id"], "pass": p, "j": j, "reply": raw if j is None else None,
                 "retried": retried, "seconds": round(time.time() - t0, 1)}
        with lock:
            cache[key] = entry
            if cache_path:
                Path(cache_path).parent.mkdir(parents=True, exist_ok=True)
                with open(cache_path, "a") as f:
                    f.write(json.dumps(entry, ensure_ascii=False) + "\n")
            row = result_row(it, p, entry)
            say(f"[judge] {len(cache)} cached  {it['set']} {it['id']} p{p} valid={row['valid_reply']}"
                + (f" quotes_failed={row['quote_failed']}/{row['quote_claims']} S={row['S']:.2f}" if row["valid_reply"] else "")
                + f" {entry['seconds']}s")

    todo = [j for j in jobs if j[4] not in cache]
    if calls and todo:
        with ThreadPoolExecutor(concurrency) as ex:
            list(ex.map(do, todo))
    rows = []
    for it, p, _, _, key in jobs:
        if key in cache:
            rows.append(result_row(it, p, cache[key]))
    return rows, len(todo) if calls else 0, len(jobs)


# ---------------------------------------------------------------- generic answers

GENERIC_TEMPLATE = (
    "This is a common situation and it usually has a straightforward explanation. First, review the relevant "
    "configuration and the recent changes to your environment, and check the logs of the components involved for "
    "errors or warnings. Make sure all components are healthy and that the settings are appropriate for your "
    "workload. If something looks wrong, adjust the configuration carefully, test the change in a non-production "
    "environment first, and monitor the system afterwards. Consult the official documentation for the exact steps "
    "for your version, and consider contacting your platform team or support if the problem continues. Keeping "
    "good monitoring, regular backups and a tested rollback plan will help to prevent similar issues in the future."
)


def generic_answers(rubrics, qmap):
    """One bland answer per rubric question, from a fixed template (it names nothing specific)."""
    return [{"label": "generic", "question": rubric_question(r, qmap), "text": GENERIC_TEMPLATE} for r in rubrics]


# ---------------------------------------------------------------- main

def api_key():
    k = os.environ.get("JUDGE_API_KEY") or os.environ.get("OMLX_API_KEY")
    if not k:
        p = Path.home() / ".config/kafka-sre-llm/judge_key"
        k = p.read_text().strip() if p.exists() else None
    return k


def estimate(n_calls, concurrency):
    lo = n_calls * SECONDS_PER_CALL_V3_MEDIAN / max(concurrency, 1) / 3600
    design = n_calls * SECONDS_PER_ITEM_DESIGN / 3600
    return lo, design


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--rubrics", required=True)
    common.add_argument("--qmap", nargs="*", default=[], help="PREFIX=questions.jsonl; rubric id PREFIX-N is the Nth question")
    g = sub.add_parser("gen-generic", parents=[common], help="write the generic-answer probe file")
    g.add_argument("--out", required=True)
    r = sub.add_parser("run", parents=[common], help="judge answers against rubrics")
    r.add_argument("--answers", nargs="+", required=True, metavar="NAME=PATH")
    r.add_argument("--chunks", help="JSONL of chunks (id, text) for rubrics that give covered_chunks")
    r.add_argument("--source-chars", type=int, default=DEFAULT_SOURCE_CHARS, help="cap on the source passages of one item, in characters")
    r.add_argument("--passes", type=int, nargs="+", default=[1, 2], choices=[1, 2])
    r.add_argument("--cache", help="JSONL of judge replies; written per call and reused on rerun")
    r.add_argument("--out", help="JSONL of validated per-point results and scores, rewritten from the cache at the end")
    r.add_argument("--judge-url", default=None, help="default $OMLX_URL/v1")
    r.add_argument("--judge-model", default="GLM-5.3-Flash-oQ8e-mtp")
    r.add_argument("--judge-extra-body", type=json.loads, default=None)
    r.add_argument("--temperature", type=float, default=0.0)
    r.add_argument("--concurrency", type=int, default=3)
    r.add_argument("--retry-invalid", action="store_true", help="redo cached calls whose reply was not valid JSON")
    r.add_argument("--stub", action="store_true", help="dry run: stub judge, no server; prints matches, largest prompt, estimate")
    r.add_argument("--rescore", action="store_true", help="no calls: rebuild --out from --cache")
    a = ap.parse_args(argv)

    rubrics = rd(a.rubrics)
    qmap = load_qmap(a.qmap)
    if a.cmd == "gen-generic":
        rows = generic_answers(rubrics, qmap)
        Path(a.out).parent.mkdir(parents=True, exist_ok=True)
        Path(a.out).write_text("".join(json.dumps(x, ensure_ascii=False) + "\n" for x in rows))
        print(f"wrote {len(rows)} generic answers to {a.out}")
        return 0

    sets = OrderedDict()
    for spec in a.answers:
        name, _, path = spec.partition("=")
        if not path:
            ap.error(f"--answers {spec!r}: expected NAME=PATH")
        sets[name] = rd(path)
    chunks = {c["id"]: c for c in rd(a.chunks)} if a.chunks else {}
    items, counts = match_items(rubrics, sets, qmap)
    for name, n in counts.items():
        print(f"matched {name}: {n} answers", flush=True)
    if not items:
        print("no answer matched a rubric", file=sys.stderr)
        return 1
    if a.stub or a.rescore:
        url = None
        judge = stub_judge
    else:
        url = a.judge_url or (os.environ.get("OMLX_URL", "").rstrip("/") + "/v1" if os.environ.get("OMLX_URL") else None)
        if not url:
            print("no judge URL: pass --judge-url or set OMLX_URL", file=sys.stderr)
            return 2
        judge = Judge(url, a.judge_model, api_key(), a.judge_extra_body, a.temperature)
    cache_path = None if a.stub else a.cache
    sizes = [len(s) + len(u) for it in items for p in a.passes for s, u in [build_prompt(it, p, chunks, a.source_chars)]]
    n_calls = len(items) * len(a.passes)
    lo, design = estimate(n_calls, a.concurrency)
    print(f"items {len(items)}, passes {a.passes}, judge calls {n_calls}, largest prompt {max(sizes)} characters "
          f"(system + user), mean {sum(sizes) // len(sizes)}", flush=True)
    print(f"judge time estimate for {n_calls} calls: {lo:.1f} h (v3 median {SECONDS_PER_CALL_V3_MEDIAN} s per call, "
          f"{a.concurrency} in parallel) to {design:.1f} h (design: {SECONDS_PER_ITEM_DESIGN} s per item)", flush=True)
    rows, n_new, n_jobs = run(items, a.passes, judge, a.judge_model, chunks, a.source_chars, cache_path,
                              a.concurrency, a.retry_invalid, calls=not a.rescore)
    invalid = sum(not r["valid_reply"] for r in rows)
    print(f"{n_jobs} judgments, {n_new} called now, {len(rows) - invalid} valid replies, {invalid} unscored", flush=True)
    if a.out and not a.stub:
        Path(a.out).parent.mkdir(parents=True, exist_ok=True)
        tmp = a.out + ".tmp"
        Path(tmp).write_text("".join(json.dumps(x, ensure_ascii=False) + "\n" for x in rows))
        os.replace(tmp, a.out)
        print(f"wrote {a.out}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
