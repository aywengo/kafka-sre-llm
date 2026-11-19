"""Retrieval profiles: `frozen` (the default, frozen.json) and the candidates `r1` and `r2` (candidate.json).

Everything here is pure (no scripts/retrieve.py, no model); kafka_sre_rag/retrieval.py feeds it the BM25 lists.
The ordering rules are those of scripts/retrieval_funnel.py (the funnel tool's simulation), written out here so
the runtime does not import an analysis tool. Candidate output is always marked unmeasured."""
import collections
import hashlib
import json
import re

from . import config

FROZEN = "frozen"
CANDIDATES = ("r1", "r2")
PROFILES = (FROZEN,) + CANDIDATES
CANDIDATE_FILE = config.PKG / "candidate.json"


def check(profile):
    if profile not in PROFILES:
        raise ValueError(f"profile must be one of {', '.join(PROFILES)}, got {profile!r}")
    return profile


def load_candidate():
    with open(CANDIDATE_FILE, encoding="utf-8") as f:
        return json.load(f)


def settings(profile, cand=None, cfg=None):
    """The complete settings of a candidate profile: its own, the candidate index record and the frozen retrieval
    block it inherits (reranker, BM25, pool). Raises for `frozen` (its settings are frozen.json itself)."""
    check(profile)
    if profile == FROZEN:
        raise ValueError("the frozen profile has no candidate settings; see frozen.json")
    cand = cand or load_candidate()
    cfg = cfg or config.load()
    return {"profile": profile, "index": cand["index"], "profile_settings": cand["profiles"][profile],
            "inherited_frozen_retrieval": {k: cfg["retrieval"][k] for k in ("reranker", "bm25", "pool", "chunk_chars",
                                                                           "rrf", "rrf_constant")}}


def settings_sha256(profile, cand=None, cfg=None):
    """sha256 over the canonical JSON (sorted keys, no spaces, UTF-8) of settings(profile). frozen.json carries no
    settings hash of its own (only the index and prompt hashes), so there is no existing recipe to follow."""
    blob = json.dumps(settings(profile, cand, cfg), sort_keys=True, separators=(",", ":"), ensure_ascii=False)
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
