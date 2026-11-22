#!/usr/bin/env python3
"""M5 R5: the offline test of Qwen3-Reranker-0.6B as a second scorer for r4g2's candidates.

Design: runs/m5/reports/r5_design.md (sections 1, 2, 4); the rule as the conductor fixed it: MILESTONE.md, section "R5"
(where the two differ, MILESTONE.md wins). The pin: runs/m5/RERANKER_R5.json.

Each of the four settings r5a..r5d replaces only r4g2's second reranker call. Everything else is r4g2 as scripts/retrieval_funnel.py
builds it (imported, not copied): the frozen six, the scored set of 24, the passage (title + "\n" + text)[:1500], the pack
rule (10 chunks, 24,000 characters, runbook cap 1). The query is Qb exactly as r4g2 builds it (r5a, r5b) or Qh, the head
of the question, 800 characters, by r4's `_head` rule (r5c, r5d). The instruction is the card's default (r5a, r5c) or the
design's task text (r5b, r5d). Model: CPU, fp32, max_length 1,024, offline, one predict() call per question over the 24
candidates in the scored set's order, a count of truncated pairs (must be 0).

  pin         write the pin file (sha256 of model.safetensors, the four settings and their hashes)
  check-pin   the cache holds the pinned files
  check-R     the scored sets rebuilt through this code equal R.json (r4g2) on every item; no model
  standin     a scorer replaying r4g2's cached bge scores through this code reproduces r4g2 exactly; no model
  smoke       r5a on two questions, live (loads the model; prints timing and pack sizes only)
  time-r5     the 20 timing questions, each setting's call live and uncached; memory against the bge reranker alone
  run         the four settings over the development scenarios and the facts; the paired difference against r4g2
  choose-r5   the rule of MILESTONE.md section "R5"; the verdict line first

Every command that loads the model runs under scripts/run_guarded.py and prints a line per question. No server call.
"""
import argparse
import collections
import dataclasses
import hashlib
import json
import math
import os
import resource
import sys
import time
from pathlib import Path

os.environ.setdefault("HF_HUB_OFFLINE", "1")
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
import retrieval_funnel as F  # noqa: E402

MODEL_ID = "Qwen/Qwen3-Reranker-0.6B"
REVISION = "e61197ed45024b0ed8a2d74b80b4d909f1255473"
PIN_FILE = "runs/m5/RERANKER_R5.json"
R_JSON = "runs/m5/reports/scripts/r2/R.json"
BASE_PRESET = "r4g2"
MAX_LENGTH = 1024
PASSAGE_CHARS = 1500
SCORED_SET = 24
BATCH_SIZE = 8
QH_CHARS = 800
DEFAULT_INSTRUCTION = "Given a web search query, retrieve relevant passages that answer the query"
TASK_INSTRUCTION = ("Given an operations question about Kafka, Strimzi, Kubernetes or Argo CD, or search terms taken from one, "
                    "retrieve documentation passages that state a fact needed to answer it")
SETTINGS = {   # name: (query kind, instruction name)
    "r5a": ("Qb", "default"), "r5b": ("Qb", "task"), "r5c": ("Qh", "default"), "r5d": ("Qh", "task"),
}
INSTRUCTIONS = {"default": DEFAULT_INSTRUCTION, "task": TASK_INSTRUCTION}

# the rule (MILESTONE.md, section "R5"), in points on the indexed must points (S 202, C 219)
GAIN_S, GAIN_C = 20, 15              # over r4g2, in points
SE_MULT = 2                          # and at least this many paired standard errors of the total
VS_FROZEN_PP = {"S": 25, "C": 25}    # over the frozen retrieval, percentage points
TIME_MEDIAN, TIME_P90 = 6.0, 9.0     # seconds, the new call on the 20 timing questions
MEMORY_ADDED_MB = 3500               # 3.5 GB (decimal) added to the process peak with the bge reranker alone
CHARS_MEDIAN, CHARS_P90 = 11000, 16000
TIE_POINTS = 4


# ------------------------------------------------------------------ settings and the pin
@dataclasses.dataclass(frozen=True)
class R5Variant(F.Variant):
    """r4g2 with a name: the funnel's r4 code builds the scored set and the pack; only the scorer differs."""
    setting: str = ""


def variant(name):
    return R5Variant(**dataclasses.asdict(F.PRESETS[BASE_PRESET]), setting=name)


def setting_params(name):
    """Every parameter of a setting, as one dict (its hash is in the pin)."""
    kind, instr = SETTINGS[name]
    base = dataclasses.asdict(F.PRESETS[BASE_PRESET])
    return {"name": name, "model": MODEL_ID, "revision": REVISION, "query": kind,
            "query_rule": ("r4g2's Qb: the search terms joined, at most %d characters (the question head if there are none)"
                           % base["terms_chars"]) if kind == "Qb" else
            "Qh: the head of the full question (with its evidence), %d characters, r4's _head rule" % QH_CHARS,
            "instruction_name": instr, "instruction": INSTRUCTIONS[instr], "max_length": MAX_LENGTH,
            "passage": "(title + newline + text)[:%d]" % PASSAGE_CHARS, "scored_set": SCORED_SET, "batch_size": BATCH_SIZE,
            "device": "cpu", "dtype": "float32", "loader": "sentence_transformers.CrossEncoder, one predict() per question",
            "base_preset": BASE_PRESET, "base_preset_params": base}


def settings_hash(name):
    return hashlib.sha256(json.dumps(setting_params(name), sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def cache_name(instruction_name):
    i8 = hashlib.sha1(INSTRUCTIONS[instruction_name].encode()).hexdigest()[:8]
    return f"qwen3r_{REVISION[:12]}_{MAX_LENGTH}_{PASSAGE_CHARS}_{i8}_fp32.jsonl"


def snapshot_dir():
    from huggingface_hub import snapshot_download
    return Path(snapshot_download(MODEL_ID, revision=REVISION, local_files_only=True))


def file_sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1 << 22), b""):
            h.update(b)
    return h.hexdigest()


def build_pin(safetensors_sha256):
    return {"purpose": "M5 R5: the offline test of a second reranker for r4g2's candidates; the rule is in MILESTONE.md, section R5",
            "model": MODEL_ID, "revision": REVISION, "hub_cache": "the default hub cache",
            "model_safetensors_sha256": safetensors_sha256,
            "license": "apache-2.0", "license_note": "no licence file in the repository",
            "license_source": "the licence field of the model card (README.md front matter) at this revision",
            "run": {"device": "cpu", "dtype": "float32", "offline": True, "max_length": MAX_LENGTH, "batch_size": BATCH_SIZE},
            "settings": {n: {**setting_params(n), "sha256": settings_hash(n)} for n in SETTINGS}}


def load_pin(path=PIN_FILE):
    p = Path(path)
    return json.loads((p if p.is_absolute() else ROOT / p).read_text())


def check_pin(path=PIN_FILE, say=F.say_flush):
    """The pin file equals the code; the cache holds the pinned revision and the pinned weights. Returns True."""
    pin = load_pin(path)
    want = json.loads(json.dumps(build_pin(pin["model_safetensors_sha256"])))
    ok = pin == want
    say(f"pin file equals the settings in the code: {ok}")
    snap = snapshot_dir()
    say(f"snapshot directory name equals the pinned revision: {snap.name == REVISION}")
    ok = ok and snap.name == REVISION
    sha = file_sha256(snap / "model.safetensors")
    say(f"sha256 of model.safetensors equals the pin: {sha == pin['model_safetensors_sha256']}")
    return ok and sha == pin["model_safetensors_sha256"]


# ------------------------------------------------------------------ the model
def messages(instruction, query, passage):
    """The messages the model's chat template renders (the system role carries the instruction)."""
    return [{"role": "system", "content": instruction}, {"role": "query", "content": query},
            {"role": "document", "content": passage}]


def count_over(lengths, max_length):
    """Pairs whose untruncated token count exceeds the window."""
    return sum(1 for n in lengths if n > max_length)


def peak_rss_mb():
    r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return r / 1e6 if sys.platform == "darwin" else r / 1e3     # bytes on macOS, kilobytes on Linux


class QwenModel:
    """The pinned model on the CPU in fp32. predict(pairs, batch_size, instruction) -> scores (raw logit differences, the
    card's sentence-transformers path); `last_truncated` is the number of pairs of the last call that did not fit the window."""

    def __init__(self):
        self._m = None
        self.last_truncated = 0
        self.truncated = 0
        self.pairs = 0

    @property
    def model(self):
        if self._m is None:
            import torch
            from sentence_transformers import CrossEncoder
            snap = snapshot_dir()
            if snap.name != REVISION:
                raise SystemExit("the cache does not hold the pinned revision")
            m = CrossEncoder(MODEL_ID, revision=REVISION, device="cpu", max_length=MAX_LENGTH,
                             model_kwargs={"torch_dtype": torch.float32})
            if next(m.parameters()).dtype != torch.float32:
                raise SystemExit("the model did not load in float32")
            if (m.prompts or {}).get("query") != DEFAULT_INSTRUCTION:
                raise SystemExit("the shipped default instruction differs from the card's text in this file")
            self._m = m
        return self._m

    def lengths(self, pairs, instruction):
        tok = self.model.tokenizer
        out = []
        for q, d in pairs:
            ids = tok.apply_chat_template(messages(instruction, q, d), tokenize=True)
            out.append(len(ids["input_ids"]) if hasattr(ids, "keys") else len(ids))
        return out

    def predict(self, pairs, batch_size=BATCH_SIZE, instruction=DEFAULT_INSTRUCTION):
        self.last_truncated = count_over(self.lengths(pairs, instruction), MAX_LENGTH)
        self.truncated += self.last_truncated
        self.pairs += len(pairs)
        return [float(x) for x in self.model.predict(pairs, batch_size=batch_size, prompt=instruction)]


def qwen_query(setting, query, question):
    """Qb is the query r4's code built (passed in); Qh is the head of the full question (r4's _head rule)."""
    kind = SETTINGS[setting][0]
    if kind == "Qb":
        return query
    if kind == "Qh":
        return F._head(question, QH_CHARS)
    raise ValueError(kind)


class Bound:
    """The model with one instruction: what R4Scorer calls as `model.predict(pairs, batch_size=...)`."""

    def __init__(self, qwen, instruction):
        self.qwen, self.instruction = qwen, instruction

    def predict(self, pairs, batch_size=BATCH_SIZE):
        return self.qwen.predict(pairs, batch_size, self.instruction)

    @property
    def last_truncated(self):
        return self.qwen.last_truncated


# ------------------------------------------------------------------ the scorers
class R5Scorer(F.R4Scorer):
    """R4Scorer with the number of truncated pairs stored in each cache record (a rerun costs nothing)."""

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.trunc = {}
        if self.path.exists():
            for l in self.path.read_text().splitlines():
                if l.strip():
                    r = json.loads(l)
                    self.trunc[(r["q"], r["ids"])] = r.get("trunc")

    def scores(self, query, ids):
        k = self.key(query, ids)
        self.last = (query, tuple(ids))
        if k in self.mem:
            return self.mem[k], "cached", 0.0
        sc, secs = self.predict(query, ids)
        trunc = getattr(self.model, "last_truncated", 0)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "a") as f:
            f.write(json.dumps({"q": k[0], "ids": k[1], "n": len(ids), "trunc": trunc, "s": sc}) + "\n")
        self.mem[k], self.trunc[k] = sc, trunc
        return sc, "computed", secs


class R5Scorers:
    """The `scorer.r4` object of the funnel for the r5 variants. A plain Variant (r4g2) goes to `bge` (an F.R4Scorers over
    the old cache); an R5Variant goes to the Qwen scorer of its instruction. `questions` maps an item id to its full question."""

    def __init__(self, ix, cache_dir, questions, qwen=None, bge=None, say=F.say_flush):
        d = Path(cache_dir)
        self.dir = d if d.is_absolute() else ROOT / d
        self.ix, self.questions, self.qwen, self.bge, self.say = ix, questions, qwen, bge, say
        self.by, self.used, self.computed = {}, {}, 0

    def of(self, instruction_name):
        if instruction_name not in self.by:
            if self.qwen is None:
                self.qwen = QwenModel()
            sc = R5Scorer(self.ix.byid, self.dir / cache_name(instruction_name), PASSAGE_CHARS, BATCH_SIZE,
                          model=Bound(self.qwen, INSTRUCTIONS[instruction_name]))
            sc.say = self.say
            self.by[instruction_name] = sc
        return self.by[instruction_name]

    def query_of(self, v, query, item):
        return qwen_query(v.setting, query, self.questions[item])

    def scores(self, v, query, ids, item=None):
        if not getattr(v, "setting", ""):
            return self.bge.scores(v, query, ids, item)
        q = self.query_of(v, query, item)
        sc = self.of(SETTINGS[v.setting][1])
        s, how, secs = sc.scores(q, ids)
        k = sc.key(q, ids)
        self.used[(v.setting, item)] = (len(ids), sc.trunc.get(k))
        if how == "computed":
            self.computed += len(ids)
        self.say(f"  {v.setting} {item} {len(ids)} pairs {how} {secs:.1f}s")
        return s, how, secs

    def truncated_total(self):
        """(pairs scored, truncated pairs) over the calls this run used; None when a cached record lacks the count."""
        pairs = sum(n for n, _ in self.used.values())
        if any(t is None for _, t in self.used.values()):
            return pairs, None
        return pairs, sum(t for _, t in self.used.values())


class Replay:
    """The stand-in: serves r4g2's cached bge scores for (query, ordered ids) through the same interface and the same
    dispatch. Only the Qb settings have such scores (Qh queries were never scored by bge); a miss is an error."""

    def __init__(self, ix, bge_cache_file, questions):
        self.bge = F.R4Scorer(ix.byid, bge_cache_file, F.PRESETS[BASE_PRESET].ce_chunk_chars,
                              model=_NoModel())
        self.questions = questions
        self.used = {}
        self.computed = 0

    def scores(self, v, query, ids, item=None):
        q = qwen_query(v.setting, query, self.questions[item]) if getattr(v, "setting", "") else query
        sc, how, secs = self.bge.scores(q, ids)
        self.used[(getattr(v, "setting", "r4g2"), item)] = (len(ids), 0)
        return sc, how, secs


class _NoModel:
    def predict(self, *a, **k):
        raise SystemExit("a score is missing from the cache and this run may not load a model")


def frozen_scorer(ix, cache_dir):
    """The funnel's scorer over the old cache for the frozen call: a miss is an error, the old files are never written."""
    sc = F.open_scorer(ix, cache_dir, model=_NoModel())
    return sc


def bge_scorers(ix, cache_dir, say=F.say_flush):
    return F.R4Scorers(ix, cache_dir, model_of=lambda L: _NoModel(), say=say)


# ------------------------------------------------------------------ paired difference, proxy, rule
def paired_diff(needs, stages, label, ref_label, item_ids, group):
    """Served points of `label` against `ref_label` on the same items, with the item-clustered standard error of the
    total (as runs/m5/reports/scripts/r5_pair.py): the per-item differences of served counts over ALL the items given
    (items without points count as 0), SE of the total = sqrt(n * sample variance)."""
    rows = [r for r in F.group_rows(needs, group) if r["item"] in item_ids]
    key = lambda r: (r["row_type"], r["item"], r.get("point"))  # noqa: E731
    a, b = collections.Counter(), collections.Counter()
    for r in rows:
        a[r["item"]] += stages[label].get(key(r)) == "in_pack"
        b[r["item"]] += stages[ref_label].get(key(r)) == "in_pack"
    ds = [a[i] - b[i] for i in sorted(item_ids)]
    n, m = len(ds), sum(ds)
    var = sum((x - m / n) ** 2 for x in ds) / (n - 1) if n > 1 else 0.0
    se = math.sqrt(n * var)
    pts = len(rows)
    return {"items": n, "points": pts, "served": sum(a.values()), "ref_served": sum(b.values()), "diff": m,
            "se_points": se, "se_pp": 100 * se / pts if pts else 0.0, "items_differing": sum(1 for x in ds if x)}


def proxy_served(needs, packs, label, item_ids, group):
    """The proxy: points (or facts) whose recorded chunks ALL lie in the pack. Reported, decides nothing."""
    rows = [r for r in F.group_rows(needs, group) if r["item"] in item_ids and r.get("chunks")]
    ok = sum(1 for r in rows if set(r["chunks"]) <= set(packs[label][r["item"]]))
    return {"points": len(rows), "all_chunks_in_pack": ok}


def choose_r5(table, paired, time_s, memory, truncated, labels=tuple(SETTINGS)):
    """The rule of MILESTONE.md section "R5" as a pure function. Inputs (a missing one is an error, ValueError):
      table[lab]  = {"S": stats, "C": stats, "facts": {"lost": n}}, stats = half_stats against the FROZEN retrieval
                    (points, served, base, lost, added_chars on S);
      paired[lab] = {"S": d, "C": d}, d = paired_diff against r4g2 (points, served, ref_served, diff, se_points, items);
      time_s[lab] = seconds of the new call on the timing questions; memory = {"added_mb": ...}; truncated = pairs cut.
    Eligible: no frozen-served point lost on S, C or the facts; time median <= 6.0 s and p90 <= 9.0 s; added memory
    <= 3.5 GB; added characters on S median <= 11,000 and p90 <= 16,000. Choice: most served on S; within 4 points of the
    best, the smaller median added characters, then the lower median seconds. The chosen one passes only if: S gain over
    r4g2 >= 20 points and >= 2 SE; C gain >= 15 points and >= 2 SE; over the frozen retrieval >= +25 pp on S and on C
    (net * 100 >= 25 * points). Returns {"verdict": "PASS" | "FAIL", "chosen", "conditions", "settings", "reasons", "near"}."""
    miss = []
    for lab in labels:
        t = table.get(lab)
        if t is None:
            miss.append(f"{lab}: no funnel table")
            continue
        for h in ("S", "C"):
            if h not in t or any(k not in t[h] for k in ("points", "served", "base", "lost")):
                miss.append(f"{lab}: funnel half {h} incomplete")
            if h not in (paired.get(lab) or {}) or any(k not in paired[lab][h] for k in ("diff", "se_points", "points", "ref_served")):
                miss.append(f"{lab}: paired difference on {h} missing")
        if "lost" not in (t.get("facts") or {}):
            miss.append(f"{lab}: facts missing")
        if not (t.get("S") or {}).get("added_chars"):
            miss.append(f"{lab}: added characters missing")
        if not (time_s or {}).get(lab):
            miss.append(f"{lab}: no timing")
    if not memory or memory.get("added_mb") is None:
        miss.append("no memory figure")
    if truncated is None:
        miss.append("no truncated-pair count")
    if miss:
        raise ValueError("missing input: " + "; ".join(miss))
    rows = {}
    for lab in labels:
        S, C, Fa = table[lab]["S"], table[lab]["C"], table[lab]["facts"]
        ts, ac = time_s[lab], table[lab]["S"]["added_chars"]
        checks = []
        lost = S["lost"] + C["lost"] + Fa["lost"]
        checks.append((lost == 0, f"E1 no frozen-served point lost: S {S['lost']}, C {C['lost']}, facts {Fa['lost']}"))
        checks.append((F.pctl(ts, 50) <= TIME_MEDIAN and F.pctl(ts, 90) <= TIME_P90,
                       f"E2 the new call on {len(ts)} timing questions: median {F.pctl(ts, 50):.1f} s (at most {TIME_MEDIAN}), "
                       f"p90 {F.pctl(ts, 90):.1f} s (at most {TIME_P90})"))
        checks.append((memory["added_mb"] <= MEMORY_ADDED_MB,
                       f"E3 added memory {memory['added_mb']:.0f} MB (at most {MEMORY_ADDED_MB})"))
        checks.append((F.pctl(ac, 50) <= CHARS_MEDIAN and F.pctl(ac, 90) <= CHARS_P90,
                       f"E4 added characters on S ({len(ac)} items): median {F.pctl(ac, 50):.0f} (at most {CHARS_MEDIAN}), "
                       f"p90 {F.pctl(ac, 90):.0f} (at most {CHARS_P90})"))
        rows[lab] = {"eligible": all(c[0] for c in checks), "checks": checks, "why_not": [c[1] for c in checks if not c[0]],
                     "S_served": S["served"], "S_net": S["served"] - S["base"], "add_med": F.pctl(ac, 50),
                     "sec_med": F.pctl(ts, 50)}
    elig = [l for l in labels if rows[l]["eligible"]]
    if not elig:
        why = "; ".join(f"{l}: " + " / ".join(rows[l]["why_not"]) for l in labels)
        return {"verdict": "FAIL", "chosen": None, "conditions": [], "settings": rows, "near": [],
                "reasons": [f"no setting is eligible ({why})"]}
    best = max(rows[l]["S_served"] for l in elig)
    near = [l for l in elig if rows[l]["S_served"] >= best - TIE_POINTS]
    chosen = min(near, key=lambda l: (rows[l]["add_med"], rows[l]["sec_med"], -rows[l]["S_served"], l))
    S, C = table[chosen]["S"], table[chosen]["C"]
    pS, pC = paired[chosen]["S"], paired[chosen]["C"]
    conds = []
    for h, need, p in (("S", GAIN_S, pS), ("C", GAIN_C, pC)):
        conds.append((p["diff"] >= need,
                      f"gain over r4g2 on {h}: {p['diff']:+d} of {p['points']} points (r4g2 {p['ref_served']}, "
                      f"{chosen} {p['ref_served'] + p['diff']}), at least +{need}"))
        conds.append((p["diff"] >= SE_MULT * p["se_points"],
                      f"gain over r4g2 on {h} at least {SE_MULT} paired standard errors: {p['diff']:+d} points against "
                      f"{SE_MULT} x {p['se_points']:.1f} = {SE_MULT * p['se_points']:.1f} ({p.get('items', '?')} items)"))
    for h, st in (("S", S), ("C", C)):
        net = st["served"] - st["base"]
        conds.append((net * 100 >= VS_FROZEN_PP[h] * st["points"],
                      f"gain over the frozen retrieval on {h}: {net:+d} of {st['points']} points = {100 * net / st['points']:+.1f} pp "
                      f"(frozen {st['base']}, {chosen} {st['served']}), at least +{VS_FROZEN_PP[h]} pp"))
    conds += [(ok, txt) for ok, txt in rows[chosen]["checks"]]
    conds.append((truncated == 0, f"validity: truncated pairs {truncated} (must be 0)"))
    reasons = [t for ok, t in conds if not ok]
    return {"verdict": "FAIL" if reasons else "PASS", "chosen": chosen, "conditions": conds, "settings": rows, "near": near,
            "reasons": reasons}


def choose_report(res, table=None, paired=None):
    """The verdict line first, then PASS or FAIL per condition with the numbers and their sizes."""
    if res["verdict"] == "PASS":
        head = f"VERDICT: PASS, chosen {res['chosen']}: every condition holds (R5 may go on to B4 by the Owner's decision)"
    else:
        head = "VERDICT: FAIL, R5 ENDS WITH NO PROFILE AND NO B4: " + "; ".join(res["reasons"])
    out = [head, ""]
    for lab, r in res["settings"].items():
        out.append(f"{lab}: " + ("eligible" if r["eligible"] else "NOT eligible") + f"; served on S {r['S_served']} "
                   f"(net over frozen {r['S_net']:+d}), added characters median {r['add_med']:.0f}, seconds median {r['sec_med']:.1f}")
        for ok, txt in r["checks"]:
            out.append(f"  {'PASS' if ok else 'FAIL'} {txt}")
    if res["chosen"]:
        out += ["", f"chosen {res['chosen']} by the tie-break among {', '.join(res['near'])} (within {TIE_POINTS} points of the best "
                    f"eligible on S): conditions"]
        out += [f"  {'PASS' if ok else 'FAIL'} {txt}" for ok, txt in res["conditions"]]
    else:
        out += ["", "no eligible setting: the conditions on the gains are not evaluated"]
    return "\n".join(out)


# ------------------------------------------------------------------ commands
def load_states(ix, a, ids=None):
    needs, answers, items = F.load_inputs(a.needs, a.answers, a.items)
    states = F.make_states(ix, ids if ids is not None else sorted({r["item"] for r in needs}), answers, items)
    return needs, states


def new_index(a):
    ix = F.Index(a.index)
    if not ix.frozen_ok:
        raise SystemExit("the index is not the frozen one")
    return ix


def cmd_pin(a, say=F.say_flush):
    snap = snapshot_dir()
    sha = file_sha256(snap / "model.safetensors")
    F.new_json(a.out, build_pin(sha))
    say(f"written {a.out}; sha256 of model.safetensors {sha[:12]}...")


def cmd_check_pin(a, say=F.say_flush):
    if not check_pin(a.pin, say):
        raise SystemExit("check-pin FAILED")
    say("check-pin PASS")


def variants_of(names=tuple(SETTINGS)):
    return [(n, variant(n)) for n in names]


def cmd_check_R(a, say=F.say_flush):
    """Rebuild the scored sets through this code (a zero scorer, no model) and compare with R.json (r4g2), 84 of 84."""
    ix = new_index(a)
    needs, states = load_states(ix, a, F.load_split(a.split)[0] | F.load_split(a.split)[1])
    sc = frozen_scorer(ix, a.cache_dir)
    R = json.loads((ROOT / R_JSON).read_text())["res"]

    class Zero:
        def scores(self, v, query, ids, item=None):
            return [0.0] * len(ids), "zero", 0.0

    sc.r4 = Zero()
    same = bad = 0
    qb_equal = qh_equal = 0
    for n, (i, st) in enumerate(sorted(states.items())):
        F.ranking(st, F.PRESETS[BASE_PRESET], sc)
        mine = st.r4info
        ok = mine["R"] == R[i]["r4g2"]["R"] and mine["frozen_pack"] == R[i]["frozen_pack"]
        r5 = R5Scorers(ix, "work/m5/r5/none", {i: st.q})
        qb = r5.query_of(variant("r5a"), mine["query"], i) == mine["query"]
        qh = r5.query_of(variant("r5c"), mine["query"], i) == F._head(st.q, QH_CHARS)
        same += ok
        bad += not ok
        qb_equal += qb
        qh_equal += qh
        say(f"[check-R {n + 1}/{len(states)}] {i} R equal {ok} ({len(mine['R'])} ids) Qb as r4g2 {qb} Qh as head {qh}")
    say(f"rebuilt scored sets equal R.json (r4g2): {same} of {len(states)}; Qb equal {qb_equal}; Qh equal {qh_equal}")
    if same != len(states) or len(states) != 84:
        raise SystemExit("check-R FAILED")


def evaluate_all(ix, needs, states, labels_variants, scorer, say):
    info = {}
    stages, skipped = F.evaluate(ix, needs, states, labels_variants, scorer, say, info)
    return stages, skipped, info


def cmd_standin(a, say=F.say_flush):
    """Acceptance: the replaying scorer reproduces r4g2 exactly through this code."""
    ix = new_index(a)
    needs, states = load_states(ix, a)
    sc = frozen_scorer(ix, a.cache_dir)
    questions = {i: st.q for i, st in states.items()}
    bge_file = Path(a.cache_dir) / f"ce_{ix.sha[:12]}_{F.PRESETS[BASE_PRESET].ce_max_length}_{PASSAGE_CHARS}_frozenfmt_r4.jsonl"
    bge_file = bge_file if bge_file.is_absolute() else ROOT / bge_file
    sc.r4 = Replay(ix, bge_file, questions)
    lv = [("frozen", F.FROZEN_V), ("r5a", variant("r5a"))]
    stages, skipped, info = evaluate_all(ix, needs, states, lv, sc, say)
    ref = json.loads(Path(a.reference).read_text())
    refst = ref["per_need_stage"]["r4g2"]
    mine = {"|".join(str(x) for x in k if x is not None): v for k, v in stages["r5a"].items()}
    diff = [k for k in refst if mine.get(k) != refst[k]]
    extra = [k for k in mine if k not in refst]
    split = F.load_split(a.split)
    res = {}
    for h, items in zip(("S", "C"), split):
        res[h] = F.half_stats(needs, stages, "r5a", "frozen", items, "points", info)["served"]
    fs = F.half_stats(needs, stages, "r5a", "frozen", None, "facts", info)["served"]
    tot = F.totals(needs, stages, "frozen")["r5a"]
    reftot = ref["totals"]["r4g2"]
    stage_eq = all(tot[k] == reftot[k] for k in ("point_stages", "point_stages_weight2", "fact_stages"))
    say(f"stand-in (r5a, replaying r4g2's cached bge scores): needs compared {len(refst)}, different {len(diff)}, extra {len(extra)}")
    say(f"served on S {res['S']}, on C {res['C']} (r4g2 stored: 108 and 102), facts {fs}; every stage count equal to r4g2: {stage_eq}")
    say(f"point stages {tot['point_stages']}")
    ok = not diff and not extra and stage_eq and (res["S"], res["C"]) == (108, 102)
    say("standin PASS" if ok else f"standin FAILED: {diff[:10]}")
    if not ok:
        raise SystemExit(1)


def smoke_ids(ix, a):
    S, _ = F.load_split(a.split)
    tier = [it["id"] for it in F.rd("eval/m4/open/tier_a.jsonl")]
    return F.timing_ids(S, tier)[:1] + F.timing_ids(S, tier)[10:11]


def cmd_smoke(a, say=F.say_flush):
    """r5a live on two questions: loader, template, cache. Prints timing and pack sizes, never served counts."""
    ix = new_index(a)
    ids = smoke_ids(ix, a)
    needs, states = load_states(ix, a, ids)
    sc = frozen_scorer(ix, a.cache_dir)
    questions = {i: st.q for i, st in states.items()}
    qwen = QwenModel()
    check_pin(a.pin, say)
    sc.r4 = R5Scorers(ix, a.qwen_cache_dir, questions, qwen=qwen, say=say)
    v = variant("r5a")
    t0 = time.time()
    qwen.model.predict([("warm up", "warm up passage")] * 2, batch_size=2)
    say(f"model loaded and warmed in {time.time() - t0:.1f}s, peak memory {peak_rss_mb():.0f} MB")
    for n, i in enumerate(ids):
        st = states[i]
        st.__dict__.pop("_rk_cache", None)
        t = time.time()
        rk, _ = F.ranking(st, v, sc)
        pack = F.selected(st, v, rk)
        info = st.r4info
        pairs = sc.r4.of("default").pairs(info["query"], info["R"])
        lens = qwen.lengths(pairs, DEFAULT_INSTRUCTION)
        feats = [int(qwen.model.preprocess([p], prompt=DEFAULT_INSTRUCTION)["attention_mask"].sum()) for p in pairs[:3]]
        say(f"[smoke {n + 1}/{len(ids)}] {i} pairs {info['pairs']} (call {info['how']}, {info['seconds']:.1f}s), "
            f"pack {len(pack)} chunks / {info['pack_chars']} characters (frozen {len(info['frozen_pack'])} / "
            f"{info['frozen_chars']}), longest pair {max(lens)} tokens, over the window {count_over(lens, MAX_LENGTH)}, "
            f"template length equals the loader's for the first 3 pairs: {lens[:3] == feats}, {time.time() - t:.1f}s")
    say(f"peak memory {peak_rss_mb():.0f} MB; cache {a.qwen_cache_dir}")


def cmd_time_r5(a, say=F.say_flush):
    ix = new_index(a)
    ref = json.loads(Path(a.reference).read_text())
    ids = ref["items"]
    needs, states = load_states(ix, a, ids)
    sc = frozen_scorer(ix, a.cache_dir)
    check_pin(a.pin, say)
    # phase 1: the bge reranker alone (the frozen call of each timing question, live): the memory baseline
    import retrieve as RT
    bge = RT.reranker()
    bge.predict([("warm up", "warm up passage")] * 2, batch_size=2)
    bge_s = []
    for n, i in enumerate(ids):
        st = states[i]
        pool = st.head(F.FROZEN_EXACT)
        pairs = [(st.q, (ix.byid[c]["title"] + "\n" + ix.byid[c]["text"])[:1500]) for c in pool]
        t = time.time()
        bge.predict(pairs, batch_size=8)
        bge_s.append(time.time() - t)
        say(f"[time bge {n + 1}/{len(ids)}] {i} frozen call {len(pairs)} pairs {bge_s[-1]:.1f}s computed")
    peak_bge = peak_rss_mb()
    say(f"peak memory with the bge reranker alone: {peak_bge:.0f} MB")
    # phase 2: Qwen added, each setting's call live and uncached (never stored)
    qwen = QwenModel()
    r5 = R5Scorers(ix, a.qwen_cache_dir, {i: st.q for i, st in states.items()}, qwen=qwen, say=lambda *x: None)
    qwen.model.predict([("warm up", "warm up passage")] * 2, batch_size=2)
    say(f"Qwen loaded and warmed, peak memory {peak_rss_mb():.0f} MB")

    class Timed:
        def scores(self_, v, query, ids_, item=None):
            q = r5.query_of(v, query, item)
            sc_ = r5.of(SETTINGS[v.setting][1])
            s, secs = sc_.predict(q, ids_)
            return s, "timed", secs

    sc.r4 = Timed()
    labels = a.settings or list(SETTINGS)
    res = {l: {"seconds": [], "pairs": [], "items": [], "truncated": []} for l in labels}
    for n, i in enumerate(ids):
        st = states[i]
        for lab in labels:
            st.__dict__.pop("_rk_cache", None)
            F.ranking(st, variant(lab), sc)
            r = res[lab]
            r["seconds"].append(st.r4info["seconds"])
            r["pairs"].append(st.r4info["pairs"])
            r["items"].append(i)
            r["truncated"].append(qwen.last_truncated)
            say(f"[time {n + 1}/{len(ids)}] {i} {lab} {st.r4info['pairs']} pairs {st.r4info['seconds']:.1f}s computed, "
                f"truncated {qwen.last_truncated}")
    peak_all = peak_rss_mb()
    mem = {"peak_bge_alone_mb": peak_bge, "peak_with_qwen_mb": peak_all, "added_mb": peak_all - peak_bge}
    for lab in labels:
        sec = res[lab]["seconds"]
        say(f"{lab}: median {F.pctl(sec, 50):.1f} s, p90 {F.pctl(sec, 90):.1f} s over {len(sec)} items "
            f"(stays if median <= {TIME_MEDIAN} and p90 <= {TIME_P90})")
    say(f"bge frozen call (reference): median {F.pctl(bge_s, 50):.1f} s, p90 {F.pctl(bge_s, 90):.1f} s")
    say(f"memory: peak with bge alone {peak_bge:.0f} MB, with Qwen added {peak_all:.0f} MB, added {mem['added_mb']:.0f} MB "
        f"(stays if at most {MEMORY_ADDED_MB})")
    trunc = sum(sum(r["truncated"]) for r in res.values())
    say(f"truncated pairs: {trunc} of {sum(sum(r['pairs']) for r in res.values())} (must be 0)")
    F.new_json(a.out, {"items": ids, "settings": res, "memory": mem, "bge_frozen_call_seconds": bge_s, "truncated_pairs": trunc,
                       "pin": {n: settings_hash(n) for n in SETTINGS}})


def cmd_run(a, say=F.say_flush):
    ix = new_index(a)
    needs, states = load_states(ix, a)
    sc = frozen_scorer(ix, a.cache_dir)
    check_pin(a.pin, say)
    questions = {i: st.q for i, st in states.items()}
    r5 = R5Scorers(ix, a.qwen_cache_dir, questions, qwen=QwenModel(), bge=bge_scorers(ix, a.cache_dir, say), say=say)
    sc.r4 = r5
    names = list(SETTINGS)
    lv = [("frozen", F.FROZEN_V), (BASE_PRESET, F.PRESETS[BASE_PRESET])] + variants_of(names)
    say("development set: numbers below are fitted to these items; not evidence of improvement")
    stages, skipped, info = evaluate_all(ix, needs, states, lv, sc, say)
    tot = F.totals(needs, stages, "frozen")
    split = F.load_split(a.split)
    S, C = split
    labels = [BASE_PRESET] + names
    time_s = None
    if a.time:
        time_s = {lab: x["seconds"] for lab, x in json.loads(Path(a.time).read_text())["settings"].items()}
    r4 = F.r4_tables(needs, stages, info, labels, split, time_s)
    for grp, title in (("points", "indexed must points"), ("weight2", "weight-2 points"), ("facts", "mechanical facts")):
        say("")
        say(F.r4_table(r4[grp], time_s, f"r5: {title} (S, C; frozen is the base)"))
    fact_items = {r["item"] for r in needs if r["row_type"] == "fact"}
    halves = {"S": (S & set(states), "points"), "C": (C & set(states), "points"), "facts": (fact_items & set(states), "facts")}
    paired = {lab: {h: paired_diff(needs, stages, lab, BASE_PRESET, ids, grp) for h, (ids, grp) in halves.items()}
              for lab in names}
    say("")
    say("paired difference against r4g2 (served points; SE of the total over the items, item-clustered)")
    for lab in names:
        for h in ("S", "C", "facts"):
            p = paired[lab][h]
            say(f"  {lab} {h}: {p['diff']:+d} of {p['points']} points (r4g2 {p['ref_served']}, {lab} {p['served']}), "
                f"SE {p['se_points']:.1f} points ({p['se_pp']:.1f} pp), {p['items']} items, {p['items_differing']} differ")
    packs = {lab: {} for lab, _ in lv}
    for n, (i, st) in enumerate(states.items()):
        for lab, v in lv:
            st.__dict__.pop("_rk_cache", None)
            packs[lab][i] = F.pack_of(st, v, sc)
        say(f"[packs {n + 1}/{len(states)}] {i}")
    proxy = {lab: {h: proxy_served(needs, packs, lab, ids, grp) for h, (ids, grp) in halves.items()} for lab, _ in lv}
    say("")
    say("proxy 'all recorded chunks in the pack' (reported, decides nothing; the strict count cannot be built here)")
    for lab, _ in lv:
        say(f"  {lab}: " + ", ".join(f"{h} {proxy[lab][h]['all_chunks_in_pack']} of {proxy[lab][h]['points']}" for h in halves))
    pairs, trunc = r5.truncated_total()
    say(f"pairs scored by the r5 settings: {pairs}; truncated pairs: {trunc} (must be 0)")
    refs = F.chunkref_stages(needs, states, F.FROZEN_V, sc)
    per_need = {lab: {"|".join(str(x) for x in k if x is not None): v for k, v in st.items()} for lab, st in stages.items()}
    F.new_json(a.json, {"totals": tot, "per_need_stage": per_need, "chunk_refs_frozen": refs, "skipped": skipped, "r4": r4,
                        "variants": {lab: dataclasses.asdict(v) for lab, v in lv},
                        "r5": {"pin": {n: settings_hash(n) for n in SETTINGS}, "paired_vs_r4g2": paired,
                               "proxy_all_recorded_chunks_in_pack": proxy, "pairs_scored": pairs, "truncated_pairs": trunc}})
    say(f"written {a.json}")


def cmd_choose_r5(a, say=F.say_flush):
    for p in (a.funnel, a.time):
        if not Path(p).exists():
            raise SystemExit(f"missing input: {p}")
    f = json.loads(Path(a.funnel).read_text())
    t = json.loads(Path(a.time).read_text())
    pin = {n: settings_hash(n) for n in SETTINGS}
    try:
        if f["r5"]["pin"] != pin or t["pin"] != pin:
            raise SystemExit("the funnel or timing file was made with other settings than the code and the pin")
        table = {lab: {"S": f["r4"]["points"][lab]["S"], "C": f["r4"]["points"][lab]["C"],
                       "facts": f["r4"]["facts"][lab]["facts"]} for lab in SETTINGS}
        paired = f["r5"]["paired_vs_r4g2"]
        time_s = {lab: t["settings"][lab]["seconds"] for lab in SETTINGS}
        mem = t["memory"]
        trunc = f["r5"]["truncated_pairs"]
        if trunc is not None:
            trunc = trunc + t["truncated_pairs"]
        res = choose_r5(table, paired, time_s, mem, trunc)
    except (KeyError, ValueError, TypeError) as e:
        raise SystemExit(f"missing or malformed input: {e}")
    say(choose_report(res, table, paired))
    if a.out:
        F.new_json(a.out, {"verdict": res["verdict"], "chosen": res["chosen"], "reasons": res["reasons"],
                           "conditions": [[ok, txt] for ok, txt in res["conditions"]]})
    return res


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    def common(p, qwen=True):
        p.add_argument("--needs", nargs="+", default=F.R4_NEEDS)
        p.add_argument("--answers", nargs="+", default=F.R4_ANSWERS)
        p.add_argument("--items", nargs="+", default=F.R4_ITEM_FILES)
        p.add_argument("--index", default=F.INDEX)
        p.add_argument("--cache-dir", default="work/m5/r4/cache", help="the old cache: frozen and bge scores, read only")
        p.add_argument("--split", default=F.R4_SPLIT)
        p.add_argument("--pin", default=PIN_FILE)
        if qwen:
            p.add_argument("--qwen-cache-dir", default="work/m5/r5/cache")

    p = sub.add_parser("pin")
    p.add_argument("--out", required=True, help="a new file")
    p = sub.add_parser("check-pin")
    p.add_argument("--pin", default=PIN_FILE)
    p = sub.add_parser("check-R")
    common(p, qwen=False)
    p = sub.add_parser("standin")
    common(p, qwen=False)
    p.add_argument("--reference", default="work/m5/r4/funnel_r4.json", help="the stored r4 funnel (r4g2 stages)")
    p = sub.add_parser("smoke")
    common(p)
    p = sub.add_parser("time-r5")
    common(p)
    p.add_argument("--reference", default="work/m5/r4/time_r4.json", help="its items are the 20 timing questions")
    p.add_argument("--settings", nargs="*", help=" ".join(SETTINGS))
    p.add_argument("--out", required=True, help="a new file")
    p = sub.add_parser("run")
    common(p)
    p.add_argument("--time", help="time-r5 output (shown in the tables)")
    p.add_argument("--json", required=True, help="a new file")
    p = sub.add_parser("choose-r5")
    p.add_argument("--funnel", required=True, help="run --json output")
    p.add_argument("--time", required=True, help="time-r5 output")
    p.add_argument("--out", help="write the verdict to this new file")
    a = ap.parse_args(argv)
    {"pin": cmd_pin, "check-pin": cmd_check_pin, "check-R": cmd_check_R, "standin": cmd_standin, "smoke": cmd_smoke,
     "time-r5": cmd_time_r5, "run": cmd_run, "choose-r5": cmd_choose_r5}[a.cmd](a)


if __name__ == "__main__":
    main()
