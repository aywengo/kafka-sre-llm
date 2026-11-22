"""Retrieval profiles: `frozen` (the default, frozen.json) and the candidates `r1` to `r4` (candidate.json).

Everything here is pure (no scripts/retrieve.py, no model); kafka_sre_rag/retrieval.py feeds it the BM25 lists.
The ordering rules are those of scripts/retrieval_funnel.py (the funnel tool's simulation), written out here so
the runtime does not import an analysis tool. Candidate output is always marked unmeasured."""
import collections
import hashlib
import json
import re

from . import config

FROZEN = "frozen"
CANDIDATES = ("r1", "r2", "r3", "r4")
PROFILES = (FROZEN,) + CANDIDATES
CANDIDATE_FILE = config.PKG / "candidate.json"


def check(profile):
    if profile not in PROFILES:
        raise ValueError(f"profile must be one of {', '.join(PROFILES)}, got {profile!r}")
    return profile


def load_candidate():
    with open(CANDIDATE_FILE, encoding="utf-8") as f:
        return json.load(f)


def grid_settings(name, cand=None):
    """The settings of one named grid setting of r4 (candidate.json `r4_grid`: r4g1 to r4g8)."""
    cand = cand or load_candidate()
    if name not in cand.get("r4_grid", {}):
        raise ValueError(f"unknown r4 grid setting {name!r}; known: {', '.join(cand.get('r4_grid', {}))}")
    return cand["r4_grid"][name]


def settings(profile, cand=None, cfg=None, grid=None):
    """The complete settings of a candidate profile: its own, the candidate index record and the frozen retrieval
    block it inherits (reranker, BM25, pool). Raises for `frozen` (its settings are frozen.json itself). `grid`: the
    name of an r4 grid setting (r4g1 to r4g8); its values stand in for the profile's own (profile must be r4)."""
    check(profile)
    if profile == FROZEN:
        raise ValueError("the frozen profile has no candidate settings; see frozen.json")
    cand = cand or load_candidate()
    cfg = cfg or config.load()
    own = cand["profiles"][profile]
    if grid is not None:
        if profile != "r4":
            raise ValueError("grid settings exist for r4 only")
        own = grid_settings(grid, cand)
    return {"profile": profile, "index": cand["index"], "profile_settings": own,
            "inherited_frozen_retrieval": {k: cfg["retrieval"][k] for k in ("reranker", "bm25", "pool", "chunk_chars",
                                                                           "rrf", "rrf_constant")}}


def settings_sha256(profile, cand=None, cfg=None, grid=None):
    """sha256 over the canonical JSON (sorted keys, no spaces, UTF-8) of settings(profile). frozen.json carries no
    settings hash of its own (only the index and prompt hashes), so there is no existing recipe to follow."""
    blob = json.dumps(settings(profile, cand, cfg, grid), sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def index_file(profile=FROZEN, cfg=None):
    """Index path (absolute) of a profile: the frozen index, or the one named in candidate.json."""
    if check(profile) == FROZEN:
        return config.index_file(cfg)
    return config.ROOT / load_candidate()["index"]["index_path"]


def verify_index(profile):
    """Raise ConfigError if a candidate's index file does not hash to the sha256 recorded in candidate.json."""
    if check(profile) == FROZEN:
        return
    rec = load_candidate()["index"]
    p = config.ROOT / rec["index_path"]
    if not p.exists():
        raise config.ConfigError(f"candidate index not found: {p}")
    got = config.sha256_file(p)
    if got != rec["sha256"]:
        raise config.ConfigError(f"candidate index sha256 mismatch for {p}: got {got}, candidate.json {rec['sha256']}")


def manifest_fields(profile):
    """Fields recorded in every answer or manifest made with a candidate profile; {} for frozen, so frozen
    output stays byte-identical."""
    if check(profile) == FROZEN:
        return {}
    return {"retrieval_profile": profile, "retrieval_settings_sha256": settings_sha256(profile), "measured": False}


def unmeasured_reason(profile):
    return None if check(profile) == FROZEN else f"retrieval profile {profile} (candidate, unmeasured)"


# ---------------------------------------------------------------- ordering rules
# DRIFT GUARD: the rules below (rrf, cap_runbooks, Lookup, select_ranked, order) are a copy of scripts/retrieval_funnel.py,
# the specification of R1 and R2. tests/test_profiles_drift.py asserts identical packs; change both files together.
def select_ranked(ranked, lens, k, max_chars):
    """The selection rule of retrieve.retrieve(): the first k that fit, stop at less than 500 characters of room."""
    out, used = [], 0
    for c in ranked[:k]:
        room = max_chars - used
        if room < 500:
            break
        out.append(c)
        used += min(lens[c], room)
    return out


def rrf(lists):
    sc = collections.Counter()
    for L in lists:
        for r, c in enumerate(L):
            sc[c] += 1 / (60 + r)
    return [c for c, _ in sc.most_common()]


def cap_runbooks(ranked, source_of, cap):
    out, n = [], 0
    for c in ranked:
        if source_of[c] == "runbooks":
            n += 1
            if n > cap:
                continue
        out.append(c)
    return out


_ERR = re.compile(r"^([A-Z][A-Z0-9_]{3,})\n-?\d+\n(True|False)\n", re.M)
_KEYHEAD = re.compile(r"^## (.+)$", re.M)
_IDRE = re.compile(r"^(--?[a-z][\w-]+|[\w-]+(\.[\w-]+)+|[A-Z][A-Z0-9]*(_[A-Z0-9]+)+|([A-Z][a-z0-9]+){2,}|[\w.-]+/[\w./-]+)$")


def _norm(s):
    return s.strip().strip("`").strip().lower()


class Lookup:
    """Names (key, error, endpoint, kind) -> chunks whose title or inline '## heading' (or error-table row) is that name."""

    def __init__(self, chunks):
        self.keys = collections.defaultdict(set)
        for c in chunks:
            t = c["title"].split(" > ")[-1] if c["title"] else ""
            t = re.sub(r"\s*(schema reference|schema properties|command reference)$", "", _norm(t))
            if t:
                self.keys[_norm(t)].add(c["id"])
            for h in _KEYHEAD.findall(c["text"]):
                self.keys[_norm(h.split(" > ")[-1])].add(c["id"])
            if c["path"].endswith("protocol_errors.html"):
                for m in _ERR.finditer(c["text"]):
                    self.keys[m.group(1).lower()].add(c["id"])

    @staticmethod
    def identifiers(q, exp):
        out = [_norm(s) for s in re.findall(r"`([^`\n]{3,80})`", q) + re.findall(r"[\w./-]+", q) if _IDRE.match(s.strip())]
        out += [_norm(t) for t in exp.split(",") if _IDRE.match(t.strip())]
        return list(dict.fromkeys(out))

    def hits(self, q, exp, n, score_of, max_chunks=12):
        """Up to n chunk ids; ambiguous names (more than max_chunks chunks) are skipped; ties go to the smaller id."""
        out = []
        for ident in self.identifiers(q, exp):
            cands = self.keys.get(ident, ())
            if not cands or len(cands) > max_chunks:
                continue
            best = max(sorted(cands), key=lambda c: score_of(c))
            if best not in out:
                out.append(best)
            if len(out) >= n:
                break
        return out


def additive_pack(prof, question, exp, *, frozen_list, bm, score, source_of, lens, lookup):
    """Profile r3 (order "additive"): the frozen pack exactly (the first base_k of frozen_list that fit base_chars,
    by select_ranked), then the exact-heading lookup hits, then the rest of frozen_list in its order; at most
    `added_runbook_cap` runbook chunks ADDED after the frozen pack (runbook chunks already in the frozen pack are
    kept and not counted), at most k chunks and max_chars characters (select_ranked's accounting, continued from the frozen pack). Added
    chunks are dropped when a cap is hit; a frozen chunk is never displaced, dropped or reordered. Returns
    (pack ids, ids refused by the runbook cap). frozen_list: the frozen ordering of the pool (best first)."""
    pack = select_ranked(frozen_list, lens, prof["base_k"], prof["base_chars"])
    used = 0
    for c in pack:
        used += min(lens[c], prof["base_chars"] - used)
    rank = {c: r + 1 for r, c in enumerate(bm)}
    depth = prof["lookup_score_depth"]
    lk = lookup.hits(question, exp, prof["lookup"],
                     lambda c: score.get(c, 0.0) if rank.get(c, 10 ** 9) <= depth else 0.0, prof["lookup_max_chunks"])
    inpack = set(pack)
    runbooks = 0   # runbook chunks added so far
    capped = []
    for c in list(dict.fromkeys(lk + frozen_list)):
        if c in inpack:
            continue
        if len(pack) >= prof["k"] or prof["max_chars"] - used < 500:
            break
        if source_of[c] == "runbooks":
            if runbooks >= prof["added_runbook_cap"]:
                capped.append(c)
                continue
            runbooks += 1
        pack.append(c)
        inpack.add(c)
        used += min(lens[c], prof["max_chars"] - used)
    return pack, capped


def order(prof, question, exp, *, bm, score, term_lists, frozen_list, source_of, lookup):
    """Candidate ranking, best first (chunk ids). bm: ids with a nonzero BM25 score, best first; score: id -> BM25
    score; term_lists: one best-first id list per search term (already cut to per_term_n); frozen_list: the frozen
    reranked order of the pool, or None when the profile does not use it (or no reranker runs)."""
    rank = {c: r + 1 for r, c in enumerate(bm)}
    per = {}
    for L in term_lists:
        for r, c in enumerate(L):
            per[c] = min(per.get(c, 99), r)
    terms = sorted(per, key=lambda c: per[c])
    lists = []
    for comp in prof["fuse"]:
        if comp == "bm25":
            lists.append(bm[:prof["bm25_depth"]])
        elif comp == "terms":
            lists.append(terms)
        elif comp == "frozen":
            if frozen_list is not None:
                lists.append(frozen_list)
        else:
            raise ValueError(f"unknown fusion component {comp!r}")
    rk = rrf(lists)
    if prof["lookup"]:
        depth = prof["lookup_score_depth"]
        lk = lookup.hits(question, exp, prof["lookup"],
                         lambda c: score.get(c, 0.0) if rank.get(c, 10 ** 9) <= depth else 0.0,
                         prof["lookup_max_chunks"])
        rk = lk + [c for c in rk if c not in lk]
    if prof["runbook_cap"] >= 0:
        rk = cap_runbooks(rk, source_of, prof["runbook_cap"])
    return rk


# ---------------------------------------------------------------- profile r4 (M5)
# DRIFT GUARD: clean_terms, question_identifiers, r4_terms, r4_rerank_set, r4_query, r4_pack are copied in
# scripts/retrieval_funnel.py (r4_ranking); tests/test_profile_r4.py asserts identical packs. Change both together.
_FENCE = re.compile(r"```.*?(```|\Z)", re.S)
_DROP_LINE = (re.compile(r"^\s*ANSWER\s*:", re.I), re.compile(r"^\s*[-#>]"), re.compile(r"^\s*[A-Za-z_][\w.\-]*\s*:(\s|$)"))


def clean_terms(exp):
    """(terms, malformed): the expansion's search terms with fenced blocks, ANSWER lines, bullet / heading / quote
    lines and YAML key lines removed; terms over 80 characters or 8 words dropped; duplicates removed
    case-insensitively (first kept); at most 40; malformed when fewer than 3 remain."""
    text = _FENCE.sub("", exp or "")
    lines = [l for l in text.split("\n") if not any(p.match(l) for p in _DROP_LINE)]
    out, seen = [], set()
    for t in ", ".join(lines).split(","):
        t = t.strip(" `'\"\t\r\n")
        if not t or len(t) > 80 or len(t.split()) > 8 or t.lower() in seen:
            continue
        seen.add(t.lower())
        out.append(t)
    out = out[:40]
    return out, len(out) < 3


def question_identifiers(q):
    """The identifier rule of Lookup applied to the question only."""
    return Lookup.identifiers(q, "")


def _dedupe_ci(seq, n):
    out, seen = [], set()
    for t in seq:
        if t.lower() not in seen:
            seen.add(t.lower())
            out.append(t)
    return out[:n]


def r4_terms(prof, question, exp):
    """(terms, malformed) for prof["terms_source"]: E (cleaned expansion terms; question identifiers instead when the
    expansion is malformed), EI (question identifiers, then cleaned expansion terms) or I (question identifiers).
    `malformed` is the flag of clean_terms(exp), whatever the source."""
    n = prof["max_terms"]
    ex, bad = clean_terms(exp)
    ids = question_identifiers(question)
    src = prof["terms_source"]
    if src == "E":
        return (_dedupe_ci(ids, n) if bad else ex[:n]), bad
    if src == "EI":
        return _dedupe_ci(ids + ex, n), bad
    if src == "I":
        return _dedupe_ci(ids, n), bad
    raise ValueError(f"unknown terms_source {src!r}")


def term_order(term_lists):
    """Chunks of the per-term lists, each at its best rank, sorted as in order() (stable)."""
    per = {}
    for L in term_lists:
        for r, c in enumerate(L):
            per[c] = min(per.get(c, 99), r)
    return sorted(per, key=lambda c: per[c])


def r4_rerank_set(prof, *, bm, term_list, frozen_list, lookup_hits, frozen_pack):
    """The scored set R: lookup hits first, then the RRF (constant 60) of bm[:bm25_depth], the term list and the
    frozen list; without the frozen-pack chunks; the first rerank_set kept."""
    pre = rrf([bm[:prof["bm25_depth"]], term_list, frozen_list])
    inpack = set(frozen_pack)
    return [c for c in dict.fromkeys(list(lookup_hits) + pre) if c not in inpack][:prof["rerank_set"]]


def _head(s, n):
    if len(s) <= n:
        return s
    cut = s[:n]
    i = max(cut.rfind(ch) for ch in " \n\t\r")
    return cut[:i] if i > 0 else cut


def _join_terms(terms, n):
    out, used = [], 0
    for t in terms:
        add = len(t) + (2 if out else 0)
        if used + add > n:
            break
        out.append(t)
        used += add
    return ", ".join(out)


def r4_query(prof, question, terms):
    """The reranker query of r4: Qf (the question), Qa (question head + "Search terms: " + terms) or Qb (terms only;
    the question head when there are none). Qa without terms is the question head alone."""
    q = prof["query"]
    if q == "Qf":
        return question
    head = _head(question, prof["q_head_chars"])
    joined = _join_terms(terms, prof["terms_chars"])
    if q == "Qa":
        return head + "\nSearch terms: " + joined if joined else head
    if q == "Qb":
        return joined or head
    raise ValueError(f"unknown query {q!r}")


def r4_passage(prof, chunk):
    """(title + newline + text) cut to ce_chunk_chars, the frozen passage format."""
    return (chunk["title"] + "\n" + chunk["text"])[:prof["ce_chunk_chars"]]


def r4_pack(prof, *, frozen_list, order_r, source_of, lens):
    """The r4 pack: the frozen pack exactly (select_ranked over frozen_list with base_k / base_chars), then the scored
    chunks in `order_r` (best first): a chunk already in the pack is skipped; filling stops at k chunks or when
    max_chars minus the accounted usage is under 500; at most added_runbook_cap runbook chunks are added (the frozen
    pack's are kept and not counted); a chunk that crosses the limit is delivered whole. Returns
    (pack, capped, rest, stop): capped = chunks refused by the runbook cap, rest = chunks of order_r not reached
    (not in the pack, not capped), stop = "k", "chars" or "end"."""
    pack = select_ranked(frozen_list, lens, prof["base_k"], prof["base_chars"])
    used = 0
    for c in pack:
        used += min(lens[c], prof["base_chars"] - used)
    inpack = set(pack)
    runbooks, capped, rest, stop = 0, [], [], "end"
    for j, c in enumerate(order_r):
        if c in inpack:
            continue
        if len(pack) >= prof["k"]:
            stop = "k"
        elif prof["max_chars"] - used < 500:
            stop = "chars"
        if stop != "end":
            rest = [x for x in order_r[j:] if x not in inpack]
            break
        if source_of[c] == "runbooks":
            if runbooks >= prof["added_runbook_cap"]:
                capped.append(c)
                continue
            runbooks += 1
        pack.append(c)
        inpack.add(c)
        used += min(lens[c], prof["max_chars"] - used)
    return pack, capped, rest, stop
