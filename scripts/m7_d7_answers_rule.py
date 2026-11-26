#!/usr/bin/env python3
"""M7 I6 helper: the D7 answers chain (scripts/m7_d7_answers.sh), Z4 stage 2, "nothing lost" on answers (runs/m7/reports/kb2_design.md sections 6, 9 row I6 and 10;
MILESTONE.md W19, W33, W37). Pure Python, CPU only; no model server, no model.

The rule is fixed in RULE_TEXT, written into the chain's report header (and printed for the conductor to copy into runs/m7/D7_LEDGER.md) at the first run, before any
D7 pack with entries exists. The code applies only the coverage condition; the harm condition is "needs the reading" and the code never prints "met". W37: the
result of Z4 is reported as met, not met or not measured; the rule itself is unchanged.

  rule-text | code-pins | parse-models --models L | parse-arms --arms L
  gate-inputs --items F --rubrics F --chunk-check F --slice-tags F [--dry]        D7's files are the 60 scenarios the chunk check names
  check-packs --arm A|K --packs F --items F --chunks F --work D [--a-packs F] [--record]
                                                  a pack file: its manifest's hash equals the file's and equals the one recorded earlier, one pack per scenario, every
                                                  id renderable by the replay, for K the frozen pack inside it; --record writes the hashes once (PACKS_RECORD.json)
  header-record --work D --header F [--k-packs F] the header's time and hash, once (HEADER_RECORD.json); notes a K pack that already exists
  check-k-order --work D --header F --k-packs F   refuses arm K unless the header (with the rule text) was written before the K pack and its manifest were created
  dry-packs --items F --rubrics F --chunks F --out-dir D --arm A|K     stand-in packs for a dry run (real chunk ids, replay format with a manifest)
  bodies-check --answers F --items F --packs F [--model-id ID]         the request body of every row equals the one built from the prompt and the replayed pack
  call-counts, stage-calls, measures, rule, reading-sets, manifest, start-check, mark, server-gate, stub-*

Exit codes: 0 done; 1 a check failed; 2 refused or an input is missing (a missing input is never a pass).
"""
import argparse
import datetime
import hashlib
import json
import math
import os
import statistics
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(ROOT))
import m5_b4_served as S  # noqa: E402  (point_sets, the chunk check's gate: called, not copied)
import m5_g_rule as G  # noqa: E402  (judged(): the judge's coverage per answer)
import m6_d5_rule as R6  # noqa: E402  (write_pack_file: the replay format)
import m7_ka2_rule as KA  # noqa: E402  (stage-calls, the state helpers, the stub server)
import m7_kb2_rule as KB  # noqa: E402  (the server gate with W16 and the stub server)
import m7_six_arm_rule as SA  # noqa: E402  (Z90, ANSWER_LIMIT, CODE_PINS, row_counts)

SCHEMA = 1
MODELS = ("gemma31", "qwen")                   # the order of the answers: Gemma first, each model alone on the server
MODEL_IDS = {"gemma31": "gemma-4-31b-it-4bit", "qwen": "Qwen3.8-27B-4bit"}
ARMS = ("A", "K")
DEFAULT_MODELS = "gemma31"                      # W39: stage 2 runs on Gemma 4 31B only; Qwen only by an explicit --models naming it (outside the fixed rule)
DECIDED_MODEL = "gemma31"                      # the model the rule decides on
N_SCENARIOS = 60
Z90 = SA.Z90
COV_LOWER_MIN = -0.04                          # the lower end of the 90 % interval of the paired coverage difference K - A must be above this
BUNDLE_SEED = 20261126                         # drawn before any answer exists; it is in the header; the blind letters come from it
MAX_PACK_ITEMS = 10                            # the merge of section 5: at most 10 items in an entries pack
EB = {"chat_template_kwargs": {"enable_thinking": False}}
CODE_FILES = ("scripts/m7_d7_answers.sh", "scripts/m7_d7_answers_rule.py", "scripts/m7_ka2_rule.py", "scripts/m7_kb2_rule.py")
PACK_PREFIX = "kbe:"                           # kafka_sre_rag/kb_entries.PACK_PREFIX: an entry in a stored pack

RULE_TEXT = """\
## The rule of Z4 part 3, "nothing lost" on D7's answers (fixed before any D7 pack with entries exists; amended by W39 before any answer)

Source: `runs/m7/reports/kb2_design.md` section 6 (stage 2: the conductor's proposal) as amended by section 10; `MILESTONE.md` W19 (nothing lost over all 60 scenarios),
W33 and W37 (the release no longer waits on the bars: the result of Z4 is reported as met, not met or not measured, with its number and denominator; the rule itself is
unchanged except as W39 says) and **W39 (the Owner, before any D7 answer: stage 2 runs on Gemma 4 31B only; Qwen's arms are not run, and the entries are documented as measured on
Gemma only)**. This text is written into the chain's header and into `runs/m7/D7_LEDGER.md` before any D7 pack with entries exists; it is not changed after that.

**The measurement.** Gemma 4 31B 4-bit on frozen packs (arm A) and frozen + entries packs (arm K), the default prompt, temperature 0, a 2,048-token answer limit, thinking off,
the packs replayed from stage 1 (no search call in an answer), one request at a time, all 60 D7 scenarios (120 answers). Two judge passes (the M7 judge setting
`deepseek-nothink-12k`, coverage role, instruction v2) against `eval/m7/d7/rubrics.jsonl`; a blind reading of the D7 answers by Claude readers (W18), the answers of a scenario in
one bundle under blind letters. The mechanical items are not part of this stage. Qwen3.8-27B is outside this rule: a run that names it explicitly records that in its header, its
numbers are reported and decide nothing.

**The rule, Gemma 4 31B, K against A, over all 60 scenarios.**
1. Judged coverage (the mean of the two passes per answer), paired by scenario. The standard error is the standard deviation of the 60 paired differences divided by
   sqrt(60). **The lower end of the 90 % interval (mean - 1.645 SE) is above -0.04.**
2. **0 confirmed new harmful consequences** in K (W6: a harmful statement of K is new when no harmful statement in that scenario's A answer has the same consequence; confirmed
   by the blind reading).
Gemma 4 31B alone is the full rule. Wrong statements per 10 answers (paired, 90 % interval), the in-slice (40) and out-of-slice (20) parts and the other measures printed
beside are reported, not decided.

**What the code applies and what it leaves.** The code applies condition 1. Condition 2 needs the blind reading: the code prints "needs the reading" and never "met". A run
without Gemma 4 31B, an arm that was not answered, or a missing measure is "not measured", never a pass.

**The judge.** The M7 judge, DeepSeek, is also the entry writer; a judge may prefer text in its own wording, so judged coverage of K could be biased upwards. The served count
of stage 1 is rated by Claude readers, not by DeepSeek, and the blind reading is the second measure of the answers. The result names this risk beside the coverage number.
"""


class DaError(RuntimeError):
    pass


def rd(p):
    p = Path(p)
    return [json.loads(l) for l in p.read_text(encoding="utf-8").splitlines() if l.strip()]


def load_json(p):
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def sha256_file(p):
    return KB.sha256_file(p)


def sha256_text(s):
    return KB.sha256_text(s)


def num(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x)


def mean(xs):
    return sum(xs) / len(xs) if xs else None


def median(xs):
    xs = [x for x in xs if num(x)]
    return statistics.median(xs) if xs else None


def se(xs):
    return statistics.stdev(xs) / math.sqrt(len(xs)) if len(xs) > 1 else None


def paired(d, z=Z90):
    m, s = mean(d), se(d)
    return {"mean": m, "se": s, "n": len(d), "ci90": None if s is None else [m - z * s, m + z * s]}


def rule_text():
    return RULE_TEXT


def rule_sha256():
    return sha256_text(RULE_TEXT)


# ---------------------------------------------------------------- the options
def parse_models(s):
    """The models of the run in the order of their answers (gemma31, then qwen), each once. The default is both (the rule as fixed)."""
    keys = [x.strip() for x in str(s if s is not None else "").split(",") if x.strip()]
    if not keys:
        raise DaError(f"--models names no model (a comma list from {', '.join(MODELS)})")
    bad = [k for k in keys if k not in MODELS]
    if bad:
        raise DaError(f"--models: unknown model {bad[0]!r} (one of {', '.join(MODELS)})")
    if len(set(keys)) != len(keys):
        raise DaError("--models names a model twice")
    return [m for m in MODELS if m in keys]


def parse_arms(s):
    keys = [x.strip() for x in str(s if s is not None else "").split(",") if x.strip()]
    if not keys:
        raise DaError("--arms names no arm (A, K or A,K)")
    bad = [k for k in keys if k not in ARMS]
    if bad:
        raise DaError(f"--arms: unknown arm {bad[0]!r} (A or K)")
    if len(set(keys)) != len(keys):
        raise DaError("--arms names an arm twice")
    return [k for k in ARMS if k in keys]


def cmd_parse_models(a):
    try:
        print(",".join(parse_models(a.models)))
    except DaError as e:
        print(f"REFUSED: {e}", file=sys.stderr)
        return 2
    return 0


def cmd_parse_arms(a):
    try:
        print(",".join(parse_arms(a.arms)))
    except DaError as e:
        print(f"REFUSED: {e}", file=sys.stderr)
        return 2
    return 0


def cmd_rule_text(a):
    print(RULE_TEXT)
    return 0


def cmd_code_pins(a):
    pins = [p for p in SA.CODE_PINS if p != "scripts/m7_four_way.sh"] + list(CODE_FILES)
    seen = []
    for p in pins:
        if p not in seen:
            seen.append(p)
    print("\n".join(seen))
    return 0


# ---------------------------------------------------------------- the inputs
def cmd_gate_inputs(a):
    try:
        items, rubs = rd(a.items), rd(a.rubrics)
        ids = [r["id"] for r in items]
        if sorted(ids) != sorted(r["id"] for r in rubs) or len(set(ids)) != len(ids):
            raise DaError("items and rubrics do not hold the same scenarios")
        if not a.dry and len(ids) != N_SCENARIOS:
            raise DaError(f"D7 has {len(ids)} scenarios; {N_SCENARIOS} expected")
        if not a.dry and S.cmd_gate_files(argparse.Namespace(items=a.items, rubrics=a.rubrics, chunk_check=a.chunk_check)) != 0:
            raise DaError("the D7 files differ from the hashes in the chunk check")
        tags = load_json(a.slice_tags)
        have = {s["id"] for s in tags.get("scenarios") or []}
        if not set(ids) <= have:
            raise DaError("the slice tags do not name every D7 scenario")
    except (DaError, OSError, ValueError, KeyError) as e:
        print(f"REFUSED: {e}", file=sys.stderr)
        return 2
    print(f"gate: D7 {len(ids)} scenarios; items and rubrics{'' if a.dry else ' equal the chunk check'}; slice tags complete")
    return 0


# ---------------------------------------------------------------- the pack files (arms A and K)
def _packs_record_path(work):
    return Path(work) / "PACKS_RECORD.json"


def _pack_rows(path):
    return {r["id"]: r for r in rd(path)}


def cmd_check_packs(a):
    """A pack file in the replay format (work/m7/six_arm/packs_frozen_d5.jsonl and its manifest). Refused (exit 2) on: a missing file or manifest, a manifest whose hash of the
    pack file differs from the file's (or from the one this chain recorded when it first accepted the file), a wrong count, a scenario without a pack or with an error row, an id the
    replay cannot render (not a chunk of the frozen index), for arm K a pack that does not hold its scenario's frozen pack or holds more than 10 items."""
    try:
        p = Path(a.packs)
        mp = Path(str(p) + ".manifest.json")
        if not p.is_file() or not mp.is_file():
            raise DaError(f"arm {a.arm}: the pack file {p} or its manifest {mp} is missing")
        man = load_json(mp)
        h = sha256_file(p)
        if man.get("pack_file_sha256") != h:
            raise DaError(f"arm {a.arm}: the manifest's hash of the pack file ({str(man.get('pack_file_sha256'))[:12]}) differs from the file's ({h[:12]})")
        rec_path = _packs_record_path(a.work)
        rec = load_json(rec_path) if rec_path.is_file() else {}
        cur = {"pack_sha256": h, "manifest_sha256": sha256_file(mp)}
        if a.arm in rec:     # first of all: a file or manifest that is not the one this chain accepted before
            for key, what in (("pack_sha256", "pack file"), ("manifest_sha256", "manifest")):
                if rec[a.arm].get(key) != cur[key]:
                    raise DaError(f"arm {a.arm}: the {what} differs from the one recorded when this chain first accepted it "
                                  f"(hash {str(rec[a.arm].get(key))[:12]} recorded, now {cur[key][:12]}); nothing is answered")
        ids = [r["id"] for r in rd(a.items)]
        rows = rd(p)
        by = {r["id"]: r for r in rows}
        if len(rows) != len(by) or sorted(by) != sorted(ids):
            raise DaError(f"arm {a.arm}: the pack file does not hold exactly one pack for each of the {len(ids)} scenarios")
        if man.get("count") is not None and man["count"] != len(rows):
            raise DaError(f"arm {a.arm}: the manifest counts {man['count']} packs, the file holds {len(rows)}")
        bad = [i for i in ids if by[i].get("error") or not isinstance(by[i].get("pack"), list) or not by[i]["pack"]
               or not all(isinstance(c, str) and c for c in by[i]["pack"]) or len(set(by[i]["pack"])) != len(by[i]["pack"])]
        if bad:
            raise DaError(f"arm {a.arm}: {len(bad)} scenarios have an error row, an empty pack or a repeated id (first: {bad[0]})")
        if man.get("stand_in") and not a.dry:
            raise DaError(f"arm {a.arm}: the pack manifest says stand_in: the packs were made with a stand-in reranker or stand-in search terms; a real run does not answer them")
        chunk_ids = {json.loads(l)["id"] for l in Path(a.chunks).read_text(encoding="utf-8").splitlines() if l.strip()}
        kbe = sorted({c for r in rows for c in r["pack"] if c.startswith(PACK_PREFIX)})
        unknown = sorted({c for r in rows for c in r["pack"] if not c.startswith(PACK_PREFIX) and c not in chunk_ids})
        if unknown:
            raise DaError(f"arm {a.arm}: {len(unknown)} pack ids are not chunks of the frozen index (first: {unknown[0]})")
        if kbe:     # entries of a K pack: replayed through the runtime's own path (evalhooks.chunks_by_ids -> kb_entries.resolve_pack with the hash-checked record)
            if a.arm == "A":
                raise DaError("arm A: the frozen packs hold no entries (kbe: ids)")
            from kafka_sre_rag import kb_entries
            try:
                kb = kb_entries.default_kb()
            except kb_entries.KbEntriesError as e:
                raise DaError(f"arm {a.arm}: the packs name entries, and the entries index cannot be used: {e}")
            missing = [c for c in kbe if c[len(PACK_PREFIX):] not in kb.byid]
            if missing:
                raise DaError(f"arm {a.arm}: {len(missing)} entry ids are not in the entries index of the record (first: {missing[0]})")
        note = ""
        if a.arm == "K":
            if not a.a_packs:
                raise DaError("arm K: --a-packs is needed (the entries pack holds the frozen pack)")
            ar = _pack_rows(a.a_packs)
            outside = [i for i in ids if not set(ar[i]["pack"]) <= set(by[i]["pack"])]
            long_ = [i for i in ids if len(by[i]["pack"]) > MAX_PACK_ITEMS]
            if outside:
                raise DaError(f"arm K: {len(outside)} entries packs do not hold their frozen pack (first: {outside[0]})")
            if long_:
                raise DaError(f"arm K: {len(long_)} packs have more than {MAX_PACK_ITEMS} items (first: {long_[0]})")
            notprefix = [i for i in ids if by[i]["pack"][:len(ar[i]["pack"])] != ar[i]["pack"]]
            note = f"; {len(ids) - len(notprefix)} of {len(ids)} start with their frozen pack" + (f" (the other {len(notprefix)} hold it in another order)" if notprefix else "")
        if a.arm not in rec and a.record:
            rec[a.arm] = {**cur, "path": str(p), "count": len(rows)}
            Path(a.work).mkdir(parents=True, exist_ok=True)
            rec_path.write_text(json.dumps(rec, indent=1, sort_keys=True) + "\n", encoding="utf-8")
            note += "; hashes recorded"
    except (DaError, OSError, ValueError, KeyError, TypeError) as e:
        print(f"REFUSED: {e}", file=sys.stderr)
        return 2
    print(f"packs {a.arm}: {len(rows)} scenarios, manifest hash equal to the file's{note}")
    return 0


# ---------------------------------------------------------------- the header before the entries pack (arm K)
def _created(path):
    """When a file was created: the earlier of its birth time (macOS and others that report it) and its modification time, so that neither a later touch nor a later rewrite
    can make an old file look new. -> (epoch, evidence kind). Weak evidence: a file copied later looks new (a copy has a new birth time)."""
    st = os.stat(path)
    birth = getattr(st, "st_birthtime", None)
    if birth:
        return min(birth, st.st_mtime), "birth time and modification time"
    return st.st_mtime, "modification time only (no birth time on this system)"


def _epoch(v):
    if num(v):
        return float(v)
    try:
        return datetime.datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp()
    except (ValueError, TypeError):
        return None


def _header_path(work):
    return Path(work) / "HEADER_RECORD.json"


def cmd_header_record(a):
    p = _header_path(a.work)
    if p.exists():
        print(f"header record exists: {p}")
        return 0
    h = Path(a.header)
    text = h.read_text(encoding="utf-8")
    if RULE_TEXT not in text:
        print("REFUSED: the header does not contain the rule text", file=sys.stderr)
        return 2
    existed, ev = False, None
    if a.k_packs:
        for f in (Path(a.k_packs), Path(str(a.k_packs) + ".manifest.json")):
            if f.exists():
                existed = True
                ev = f"{f} existed when the header was written"
    now = datetime.datetime.now().astimezone()
    rec = {"schema": SCHEMA, "written_at_epoch": now.timestamp(), "written_at": now.isoformat(timespec="microseconds"), "header_sha256": sha256_file(h),
           "rule_sha256": rule_sha256(), "models": a.models, "bundle_seed": BUNDLE_SEED, "k_pack_existed_at_header": existed, "k_pack_evidence": ev}
    Path(a.work).mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(rec, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    print(f"header record written: {p} (header {rec['header_sha256'][:12]}, rule {rec['rule_sha256'][:12]}, at {rec['written_at']}"
          + ("; THE K PACK ALREADY EXISTED: arm K is refused for good" if existed else "") + ")")
    return 0


def cmd_check_k_order(a):
    """Arm K is answered only when the report header with the rule text was written before the K pack file (and its manifest) was created: the recorded header time and hash
    against the K files. Unknown is a refusal."""
    try:
        rp = _header_path(a.work)
        if not rp.is_file():
            raise DaError("there is no header record: the header with the rule text has not been written yet, so arm K cannot be answered")
        rec = load_json(rp)
        h = Path(a.header)
        if not h.is_file():
            raise DaError(f"the header file {h} is missing")
        if sha256_file(h) != rec.get("header_sha256"):
            raise DaError("the header's hash differs from the recorded one: the header changed after it was recorded")
        if RULE_TEXT not in h.read_text(encoding="utf-8") or rec.get("rule_sha256") != rule_sha256():
            raise DaError("the rule text in the header (or its recorded hash) is not this chain's rule text")
        if rec.get("k_pack_existed_at_header"):
            raise DaError(f"the K pack file existed before the header with the rule text was written ({rec.get('k_pack_evidence')}); arm K is refused for good in this work folder")
        kp = Path(a.k_packs)
        mp = Path(str(kp) + ".manifest.json")
        t0 = float(rec["written_at_epoch"])
        for f in (kp, mp):
            if not f.is_file():
                raise DaError(f"{f} is missing")
            t, kind = _created(f)
            if not t > t0:
                raise DaError(f"{f.name} was created at {datetime.datetime.fromtimestamp(t).astimezone().isoformat(timespec='seconds')}, not after the header ({rec['written_at']}): "
                              f"the rule text must come before the K pack")
        man = load_json(mp)
        for k in ("created_at", "created", "written_at"):
            if k in man:
                t = _epoch(man[k])
                if t is None:
                    raise DaError(f"the K manifest's {k} cannot be read")
                if t < math.floor(t0):     # I5 writes whole seconds (UTC): the true time lies in [t, t + 1 s), so only a value before the header's second is certainly earlier
                    raise DaError(f"the K manifest says it was {k} {man[k]}, before the header ({rec['written_at']})")
    except (DaError, OSError, ValueError, KeyError) as e:
        print(f"REFUSED: {e}", file=sys.stderr)
        return 2
    print(f"k order: the header ({rec['written_at']}, rule {rec['rule_sha256'][:12]}) was written before the K pack and its manifest were created ({kind})")
    return 0


# ---------------------------------------------------------------- stand-in packs for a dry run
def cmd_dry_packs(a):
    items = [r["id"] for r in rd(a.items)]
    rubs = {r["id"]: r for r in rd(a.rubrics)}
    chunk_ids = [json.loads(l)["id"] for l in Path(a.chunks).read_text(encoding="utf-8").splitlines() if l.strip()]
    out = Path(a.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    def frozen(i, n):
        head = []
        for pt in rubs[i]["must"]:
            for s in (S.point_sets(pt) or [[]])[:1]:
                for c in s:
                    if c not in head:
                        head.append(c)
        k = items.index(i)
        for c in chunk_ids[k * 7:] + chunk_ids:
            if len(head) >= n:
                break
            if c not in head:
                head.append(c)
        return head[:n]

    entry_ids = []
    if a.arm == "K":     # real entries when the record and its index are usable here (replayed through the runtime's own path); else chunk ids stand in for them
        try:
            from kafka_sre_rag import kb_entries
            entry_ids = [PACK_PREFIX + e["id"] for e in kb_entries.default_kb().rows]
        except Exception:
            entry_ids = []
    rows = []
    for k, i in enumerate(items):
        base = frozen(i, 6)
        if a.arm == "A":
            rows.append((i, base))
        elif entry_ids:
            rows.append((i, base + [entry_ids[(k * 3 + j) % len(entry_ids)] for j in range(2 + k % 3)]))   # 2 to 4 entries
        else:
            extra = [c for c in chunk_ids[1000 + k * 5:] if c not in base][:2 + k % 3]   # stand-ins for 2 to 4 entries
            rows.append((i, base + extra))
    path = out / f"packs_{a.arm}.jsonl"
    R6.write_pack_file(path, rows, {"source": "a dry run's stand-in (real chunk ids)", "setting": "frozen" if a.arm == "A" else "frozen+entries (stand-in)",
                                    "created_at": datetime.datetime.now().astimezone().isoformat(timespec="microseconds")})
    print(f"dry packs {a.arm}: wrote {path} ({len(rows)} scenarios)")
    return 0


# ---------------------------------------------------------------- the request bodies of arm A and K against the reference call
def expected_parts(question_item, pack, model, max_tokens=SA.ANSWER_LIMIT, streamed=True):
    """The reference call: the default prompt on the replayed pack (prompts.answer_messages on the chunks), and the body the server generator builds for it with the thinking switch
    off and the answer limit (the call of the six-arm test's arm A: `eval2.py answer --answer-profile thorough --no-thinking --no-verify --replay-packs`)."""
    import eval2
    from kafka_sre_rag import evalhooks, generators, prompts
    msgs = prompts.answer_messages(eval2.full_question(question_item), evalhooks.chunks_by_ids(pack))
    body = generators.OpenAIChatGenerator("http://example.invalid/v1", model, extra_body=EB).request_body(msgs, max_tokens, False)
    if streamed:     # the profile streams the draft: the generator adds these two keys before it records the body's parts (generators.py, OpenAIChatGenerator._chat)
        body.update(stream=True, stream_options={"include_usage": True})
    return eval2.messages_fingerprint(msgs)[0], generators.body_parts(body)


def cmd_bodies_check(a):
    items = {r["id"]: r for r in rd(a.items)}
    packs = {r["id"]: r["pack"] for r in rd(a.packs)}
    rows = rd(a.answers)
    bad = []
    for r in rows:
        i = r["id"]
        why = []
        if r.get("pack") != packs.get(i):
            why.append("the row's pack is not the replayed pack")
        else:
            want_h, want = expected_parts(items[i], packs[i], a.model_id, streamed=not a.non_streamed)
            got = (r.get("draft_info") or {}).get("request_body_parts")
            if r.get("prompt_sha256") != want_h:
                why.append("the prompt hash differs from the default prompt on the pack")
            if not got:
                why.append("no request body recorded")
            else:
                for k in ("messages_sha256", "max_tokens", "enable_thinking", "rest_sha256", "rest_nomodel_sha256"):
                    if got.get(k) != want[k]:
                        why.append(f"request body part {k} differs")
        if why:
            bad.append((i, "; ".join(why)))
    for i, w in bad[:5]:
        print(f"bodies check {i}: {w}")
    print(f"bodies check {Path(a.answers).name}: {len(rows) - len(bad)} of {len(rows)} rows carry the request body built from the default prompt on their replayed pack "
          f"(thinking off, {SA.ANSWER_LIMIT} tokens, temperature 0, {'non-streamed: the stub backend' if a.non_streamed else 'streamed'})")
    return 1 if bad else 0


# ---------------------------------------------------------------- manifest, start check (the options exclude the arms: the A run and the later K run share one manifest)
def state(a):
    missing = [p for p in a.pin if not Path(p).is_file()]
    if missing:
        raise ValueError(f"pinned file missing: {missing[0]}")
    return {"schema": SCHEMA, "kind": "dry" if a.dry else "real", "rule_sha256": rule_sha256(), "options": KB.kv(a.opt), "info": KB.kv(a.info),
            "pins": {p: sha256_file(p) for p in sorted(a.pin)}}


def cmd_manifest(a):
    m = Path(a.work) / "MANIFEST.json"
    if m.exists():
        print(f"manifest exists: {m}")
        return 0
    m.write_text(json.dumps(state(a), indent=1, sort_keys=True) + "\n", encoding="utf-8")
    print(f"manifest written: {m}")
    return 0


def cmd_start_check(a):
    saved = KB.state
    KB.state = state
    try:
        return KB.cmd_start_check(a)
    finally:
        KB.state = saved


# ---------------------------------------------------------------- the files of a run
def sfile(work, arm, model):
    return Path(work) / f"ans_d7_{arm}_{model}.jsonl"


def jfile(work, arm):
    return Path(work) / f"judge_d7_{arm}.jsonl"


def set_name(arm, model):
    return f"{arm}_{model}"


def expected_calls(n, models, arms):
    out = {f"ans_{arm}_{m}": n for arm in arms for m in models}
    for arm in arms:
        out[f"judge_{arm}"] = len(models) * n * 2
    return out


def cmd_call_counts(a):
    try:
        c = expected_calls(a.nd, parse_models(a.models), parse_arms(a.arms))
    except DaError as e:
        print(f"REFUSED: {e}", file=sys.stderr)
        return 2
    print(" ".join(f"{k}={v}" for k, v in c.items()))
    return 0


def cmd_reading_sets(a):
    """NAME=PATH of every answer set present, in the order arm, model: the sets of the blind bundles."""
    models = parse_models(a.models)
    out = []
    for arm in ARMS:
        for m in models:
            f = sfile(a.work, arm, m)
            if f.is_file():
                out.append(f"{set_name(arm, m)}={f}")
    print("\n".join(out))
    return 0


# ---------------------------------------------------------------- the measures and the rule
def coverage(work, rubrics, arm, model, ids):
    j = G.judged(str(jfile(work, arm)), rubrics, {set_name(arm, model)})
    out = {}
    for i in ids:
        if (set_name(arm, model), i) not in j:
            raise DaError(f"no valid judge result for {set_name(arm, model)} {i}")
        out[i] = j[(set_name(arm, model), i)][0]
    return out


def ident_counts(path):
    return {r["id"]: int(r["nonexistent"]) for r in load_json(path)["answers"]}


def model_measures(a, model, tags):
    W = Path(a.work)
    rubs = rd(a.rubrics)
    ids = sorted(r["id"] for r in rubs)
    present = [arm for arm in ARMS if sfile(W, arm, model).is_file() and jfile(W, arm).is_file()]
    out = {"model": model, "model_id": MODEL_IDS[model], "arms": present, "n": len(ids)}
    rows, cov = {}, {}
    for arm in present:
        rows[arm] = {r["id"]: r for r in rd(sfile(W, arm, model))}
        if sorted(rows[arm]) != ids:
            raise DaError(f"{model} arm {arm}: the answers do not hold the {len(ids)} scenarios")
        cov[arm] = coverage(W, a.rubrics, arm, model, ids)
        out[f"coverage_{arm}"] = mean(list(cov[arm].values()))
        c = SA.row_counts(list(rows[arm].values()))
        out[f"rows_{arm}"] = {"cut": c["cut"], "empty": c["empty"]}
        out[f"seconds_{arm}"] = {"median": median([(r.get("seconds") or {}).get("total") for r in rows[arm].values()])}
    if "A" in present and "K" in present:
        d = [cov["K"][i] - cov["A"][i] for i in ids]
        out["coverage_diff"] = paired(d)
        in_slice = {s["id"]: s.get("code_tag") == "slice" for s in tags["scenarios"]}
        out["coverage_diff_in_slice"] = paired([cov["K"][i] - cov["A"][i] for i in ids if in_slice.get(i)])
        out["coverage_diff_out_of_slice"] = paired([cov["K"][i] - cov["A"][i] for i in ids if not in_slice.get(i)])
        out["seconds_diff"] = {"median": median([((rows["K"][i].get("seconds") or {}).get("total") or 0) - ((rows["A"][i].get("seconds") or {}).get("total") or 0) for i in ids])}
        same = sum(1 for i in ids if rows["A"][i].get("pack") == rows["K"][i].get("pack"))
        out["same_pack"] = same
        ia, ik = W / f"idcheck_d7_A_{model}.json", W / f"idcheck_d7_K_{model}.json"
        if ia.is_file() and ik.is_file():
            ca, ck = ident_counts(ia), ident_counts(ik)
            if sorted(ca) == sorted(ck) == ids:
                out["idents_diff_per10"] = paired([10.0 * (ck[i] - ca[i]) for i in ids])
                out["idents_per10"] = {"A": 10 * sum(ca.values()) / len(ids), "K": 10 * sum(ck.values()) / len(ids)}
    return out


def cmd_measures(a):
    try:
        models = parse_models(a.models)
        tags = load_json(a.slice_tags)
        m = {"schema": SCHEMA, "stub": bool(a.stub), "models_in_run": models, "models": {}}
        for model in models:
            m["models"][model] = model_measures(a, model, tags)
    except (DaError, OSError, KeyError, ValueError, TypeError, G.RuleError) as e:
        print(f"the measures could not be computed: {type(e).__name__}: {e}", file=sys.stderr)
        return 2
    W = Path(a.work)
    (W / "measures.json").write_text(json.dumps(m, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    (W / "measures.md").write_text(measures_md(m), encoding="utf-8")
    print(measures_md(m))
    return 0


def f(x, k=3):
    return "n/a" if x is None else f"{x:+.{k}f}" if isinstance(x, float) else str(x)


def iv(p, k=3):
    c = p.get("ci90")
    s_ = "n/a" if p["se"] is None else format(p["se"], f".{k}f")
    c_ = "n/a" if c is None else format(c[0], f"+.{k}f") + " to " + format(c[1], f"+.{k}f")
    return f"{f(p['mean'], k)} (SE {s_}, 90 % interval {c_}, n {p['n']})"


def measures_md(m):
    L = [f"### Measures{' (from stub data: not a measurement)' if m['stub'] else ''}", ""]
    for model, e in m["models"].items():
        L.append(f"**{model}** (`{e['model_id']}`; D7 {e['n']} scenarios; arms answered and judged: {', '.join(e['arms']) or 'none'})")
        L.append("")
        L += ["| Measure | A | K | K - A |", "|---|---|---|---|"]
        cov = lambda arm: "n/a" if f"coverage_{arm}" not in e else f"{e[f'coverage_{arm}']:.3f}"  # noqa: E731
        L.append(f"| judged coverage (mean of two passes, all {e['n']}) | {cov('A')} | {cov('K')} | {iv(e['coverage_diff']) if 'coverage_diff' in e else 'not measured (arm K missing)'} |")
        if "coverage_diff" in e:
            L.append(f"| in-slice scenarios (reported, not decided) | | | {iv(e['coverage_diff_in_slice'])} |")
            L.append(f"| out-of-slice scenarios (reported, not decided) | | | {iv(e['coverage_diff_out_of_slice'])} |")
        if "idents_diff_per10" in e:
            L.append(f"| nonexistent identifiers per 10 answers (reported) | {e['idents_per10']['A']:.2f} | {e['idents_per10']['K']:.2f} | {iv(e['idents_diff_per10'], 2)} |")
        rw = lambda arm: "n/a" if f"rows_{arm}" not in e else f"{e[f'rows_{arm}']['cut']} / {e[f'rows_{arm}']['empty']}"  # noqa: E731
        L.append(f"| cut / empty answers | {rw('A')} | {rw('K')} | |")
        sc = lambda arm: "n/a" if f"seconds_{arm}" not in e else f(e[f"seconds_{arm}"]["median"], 1)  # noqa: E731
        L.append(f"| seconds per answer (median; paired difference median) | {sc('A')} | {sc('K')} | {f(e['seconds_diff']['median'], 1) if 'seconds_diff' in e else 'n/a'} |")
        L.append("")
    return "\n".join(L) + "\n"


def evaluate_model(e):
    """-> (word, text). Condition 1 only: "holds", "FAILS" or "not measured"; a missing interval is not a pass."""
    p = e.get("coverage_diff")
    if p is None or p.get("ci90") is None or not num(p["ci90"][0]):
        return "not measured", f"{e['model']}: not measured (arm {'K' if 'K' not in e['arms'] else 'A'} was not answered and judged here, or the interval cannot be computed)"
    lo = p["ci90"][0]
    ok = lo > COV_LOWER_MIN + 1e-9
    return ("holds" if ok else "FAILS"), f"{e['model']}: judged coverage K - A, lower end of the 90 % interval {lo:+.3f} (needs above {COV_LOWER_MIN:+.2f}), n {p['n']}"


def cmd_rule(a):
    m = load_json(a.measures)
    try:
        if DECIDED_MODEL not in (m.get("models") or {}):
            word = "not measured"
            print(f"{DECIDED_MODEL}: not measured (not in this run)")
        else:
            e = m["models"][DECIDED_MODEL]
            word, txt = evaluate_model(e)
            print(f"{DECIDED_MODEL}: coverage condition {word}")
            print(f"  {txt}")
            print("  harm condition (0 confirmed new harmful consequences in K): needs the reading")
            if "coverage_diff" in e:
                print(f"  reported: in-slice {iv(e['coverage_diff_in_slice'])}; out-of-slice {iv(e['coverage_diff_out_of_slice'])}; cut K {e['rows_K']['cut']}, A {e['rows_A']['cut']}")
        if "qwen" in (m.get("models") or {}):
            e = m["models"]["qwen"]
            w, txt = evaluate_model(e)
            print(f"qwen: outside the fixed rule (W39); reported, decides nothing: coverage {w}; {txt}")
    except (KeyError, TypeError) as err:
        print(f"the rule could not be applied: {type(err).__name__}: {err}", file=sys.stderr)
        return 2
    if word == "FAILS":
        verdict = "Z4 part 3 NOT MET (the coverage condition fails for Gemma 4 31B, the full rule under W39)"
    elif word == "not measured":
        verdict = "Z4 part 3 NOT MEASURED (no coverage result for Gemma 4 31B; the harm condition needs the reading)"
    else:
        verdict = "Z4 part 3 NOT FINAL (the coverage condition holds for Gemma 4 31B, the full rule under W39; the harm condition needs the blind reading, so nothing is called met yet)"
    print(f"M7 D7 ANSWERS (Z4 part 3, nothing lost) DONE{' (STUB DATA)' if m.get('stub') else ''}: {verdict}; per W37 the result is reported as met, not met or not measured, with its number and denominator")
    return 0


# ---------------------------------------------------------------- command line
def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] in ("server-gate", "stub-status", "stub-load", "stub-unload", "stage-calls", "mark"):
        return KA.main(argv)
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("manifest", "start-check"):
        p = sub.add_parser(name)
        p.add_argument("--work", required=True)
        p.add_argument("--pin", action="append", default=[])
        p.add_argument("--opt", action="append", default=[])
        p.add_argument("--info", action="append", default=[])
        p.add_argument("--dry", action="store_true")
    sub.add_parser("rule-text")
    sub.add_parser("code-pins")
    p = sub.add_parser("parse-models")
    p.add_argument("--models", required=True)
    p = sub.add_parser("parse-arms")
    p.add_argument("--arms", required=True)
    p = sub.add_parser("gate-inputs")
    for x in ("--items", "--rubrics", "--chunk-check", "--slice-tags"):
        p.add_argument(x, required=True)
    p.add_argument("--dry", action="store_true")
    p = sub.add_parser("check-packs")
    p.add_argument("--arm", choices=ARMS, required=True)
    for x in ("--packs", "--items", "--chunks", "--work"):
        p.add_argument(x, required=True)
    p.add_argument("--a-packs", default=None)
    p.add_argument("--record", action="store_true")
    p.add_argument("--dry", action="store_true", help="a dry run's stand-in packs are accepted")
    p = sub.add_parser("header-record")
    p.add_argument("--work", required=True)
    p.add_argument("--header", required=True)
    p.add_argument("--k-packs", default=None)
    p.add_argument("--models", default="")
    p = sub.add_parser("check-k-order")
    for x in ("--work", "--header", "--k-packs"):
        p.add_argument(x, required=True)
    p = sub.add_parser("dry-packs")
    for x in ("--items", "--rubrics", "--chunks", "--out-dir"):
        p.add_argument(x, required=True)
    p.add_argument("--arm", choices=ARMS, required=True)
    p = sub.add_parser("bodies-check")
    for x in ("--answers", "--items", "--packs"):
        p.add_argument(x, required=True)
    p.add_argument("--model-id", default="stub")
    p.add_argument("--non-streamed", action="store_true", help="the stub backend of a dry run records the non-streamed body; a real run streams the draft")
    p = sub.add_parser("call-counts")
    p.add_argument("--nd", type=int, required=True)
    p.add_argument("--models", default=DEFAULT_MODELS)
    p.add_argument("--arms", default=",".join(ARMS))
    p = sub.add_parser("reading-sets")
    p.add_argument("--work", required=True)
    p.add_argument("--models", default=DEFAULT_MODELS)
    p = sub.add_parser("measures")
    for x in ("--work", "--rubrics", "--slice-tags"):
        p.add_argument(x, required=True)
    p.add_argument("--models", default=DEFAULT_MODELS)
    p.add_argument("--stub", action="store_true")
    p = sub.add_parser("rule")
    p.add_argument("--measures", required=True)
    a = ap.parse_args(argv)
    return {"manifest": cmd_manifest, "start-check": cmd_start_check, "rule-text": cmd_rule_text, "code-pins": cmd_code_pins, "parse-models": cmd_parse_models,
            "parse-arms": cmd_parse_arms, "gate-inputs": cmd_gate_inputs, "check-packs": cmd_check_packs, "header-record": cmd_header_record,
            "check-k-order": cmd_check_k_order, "dry-packs": cmd_dry_packs, "bodies-check": cmd_bodies_check, "call-counts": cmd_call_counts,
            "reading-sets": cmd_reading_sets, "measures": cmd_measures, "rule": cmd_rule}[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())
