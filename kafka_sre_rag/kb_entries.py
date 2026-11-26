"""M7 KB phase 2, I4 (EXPERIMENTAL, UNMEASURED): the entries index and the runtime option `--kb-entries`.

Spec: runs/m7/reports/kb2_design.md section 5. Nothing here is imported unless the option is on, so the frozen path, its
prompts and its retrieval stay byte for byte as they were.

  KbEntries.load(record)             the accepted entries of the chain (work/m7/kb/entries_index.jsonl) after a hash check
                                     against the tracked record kafka_sre_rag/kb_entries.json; refuses on any mismatch
  KbEntries.ranked(question, terms)  BM25 over the entries (retrieve.tokens, K1, B, title twice, path words once), the top 24,
                                     the already loaded frozen cross-encoder on (question, rendered entry), RRF as in rank()
  KbEntries.merge(frozen, ...)       the frozen pack first, unchanged and in order; then entries in ranked order, whole or not
                                     at all, within MAX_ITEMS items and MAX_CHARS characters of pack_text; an entry whose source
                                     chunks are all in the pack, or a second entry of the same source-chunk set, is skipped
  resolve_pack(ids, ...)             pack ids (chunk ids or `kbe-` entry ids) to pack items, for a replay of a stored pack

An entry is an item of the pack with the title line of prompts.KB_ENTRY_RENDER (kb_entry.render); no prompt text is added.
No model is called here; the reranker is the frozen instance of scripts/retrieve.py unless one is passed in.
"""
import hashlib
import json
import math
import sys
import time
from collections import Counter
from pathlib import Path

from . import config, kb_entry, prompts

SCHEMA = 1
RECORD = config.PKG / "kb_entries.json"
MAX_ITEMS, MAX_CHARS = 10, 24000        # the whole pack after the merge (section 5)
POOL = 24                                # BM25 candidates that are reranked
PASSAGE_CHARS = 1500                     # a reranker passage, as for chunks
RRF_K = 60
RECORD_FIELDS = ("schema", "file", "sha256", "entries", "slice_sha256", "writer", "checker", "created")


class KbEntriesError(RuntimeError):
    """The entries index cannot be used (record missing or a template, hash or count differs, a row is not as recorded)."""


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def read_record(path=None):
    p = Path(path or RECORD)
    try:
        rec = json.loads(p.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise KbEntriesError(f"no entries record at {p.name}: build the index first (scripts/kb_build_index.py)")
    except ValueError as e:
        raise KbEntriesError(f"the entries record {p.name} is not valid JSON: {e}")
    missing = [k for k in RECORD_FIELDS if rec.get(k) in (None, "")]
    if missing:
        raise KbEntriesError(f"the entries record {p.name} is incomplete or a template (missing: {', '.join(missing)})")
    if rec["schema"] != SCHEMA:
        raise KbEntriesError(f"the entries record {p.name} has schema {rec['schema']}, expected {SCHEMA}")
    return rec


def rows_of(path):
    return [json.loads(l) for l in Path(path).read_text(encoding="utf-8").splitlines() if l.strip()]


def _index_path(rec, record_path):
    p = Path(rec["file"])
    return p if p.is_absolute() else config.ROOT / p


def verify_index(rec, path):
    """Hash and count of the index file against the record. Raises KbEntriesError."""
    path = Path(path)
    if not path.is_file():
        raise KbEntriesError(f"the entries index {rec['file']} is missing")
    got = sha256_file(path)
    if got != rec["sha256"]:
        raise KbEntriesError(f"the entries index {rec['file']} does not match the record: sha256 {got}, record {rec['sha256']}")


def _retrieve():
    scripts = str(config.ROOT / "scripts")
    if scripts not in sys.path:
        sys.path.insert(0, scripts)
    import retrieve as RT
    return RT


def passage(entry):
    """What the reranker and the model see of an entry: the rendered item (title line and text)."""
    return entry["rendered"]


def check_rendered(rows):
    """Each row's stored rendered text must be what the code renders now (prompts.KB_ENTRY_RENDER unchanged since the build)."""
    for e in rows:
        if e.get("rendered") != kb_entry.render_text(e):
            raise KbEntriesError(f"entry {e.get('id')}: the stored rendered text differs from kb_entry.render_text "
                                 "(KB_ENTRY_RENDER or the render code changed since the index was built)")


class KbEntries:
    def __init__(self, rows, *, record=None, reranker=None):
        """rows: the index rows (dicts with the entry fields and `rendered`). reranker: an object with predict(pairs,
        batch_size) or None for the frozen instance (scripts/retrieve.py), loaded on first use."""
        RT = _retrieve()
        self.rows, self.record, self._reranker = list(rows), record, reranker
        self.byid = {e["id"]: e for e in self.rows}
        if len(self.byid) != len(self.rows):
            raise KbEntriesError("the entries index has a repeated id")
        self.docs, self.lens = [], []
        for e in self.rows:
            words = []
            for s in e["sources"]:
                words += s["path"].rsplit("/", 2)[-2:] if s.get("path") else []
            d = Counter(RT.tokens(e["rendered"]) + RT.tokens(e["title"]) * RT.TITLE_W + RT.tokens(" ".join(words)))
            self.docs.append(d)
            self.lens.append(sum(d.values()))
        n = len(self.docs)
        df = Counter(t for d in self.docs for t in d)
        self.idf = {t: math.log(1 + (n - f + 0.5) / (f + 0.5)) for t, f in df.items()}
        self.avg = sum(self.lens) / n if n else 0.0
        self.inv = {}
        for i, d in enumerate(self.docs):
            for t in d:
                self.inv.setdefault(t, []).append(i)

    @classmethod
    def load(cls, record_path=None, *, index_path=None, reranker=None):
        """Read the record, check the index file's hash against it (refuses on a mismatch), its count, and the rendered texts."""
        rec = read_record(record_path)
        path = Path(index_path) if index_path else _index_path(rec, record_path)
        verify_index(rec, path)
        rows = rows_of(path)
        if len(rows) != rec["entries"]:
            raise KbEntriesError(f"the entries index has {len(rows)} entries, the record says {rec['entries']}")
        check_rendered(rows)
        return cls(rows, record=rec, reranker=reranker)

    # ------------------------------------------------------------ search
    def scores(self, query):
        RT = _retrieve()
        s = Counter()
        for t in set(RT.tokens(query)):
            for i in self.inv.get(t, ()):
                tf = self.docs[i][t]
                s[i] += self.idf[t] * tf * (RT.K1 + 1) / (tf + RT.K1 * (1 - RT.B + RT.B * self.lens[i] / self.avg))
        return s

    def reranker(self):
        return self._reranker if self._reranker is not None else _retrieve().reranker()

    def ranked(self, question, expansion=None, rerank=True):
        """Entry ids, best first: BM25 on question + terms, top POOL, rerank, RRF with the BM25 rank as in retrieve.rank().
        rerank=False: BM25 order only (unmeasured; dry runs and tests)."""
        query = question + " " + expansion if expansion else question
        cand = [i for i, _ in self.scores(query).most_common(POOL)]
        if not cand:
            return []
        if not rerank:
            return [self.rows[i]["id"] for i in cand]
        ce = self.reranker().predict([(question, passage(self.rows[i])[:PASSAGE_CHARS]) for i in cand], batch_size=8)
        order = sorted(range(len(cand)), key=lambda j: -ce[j])
        pos = {j: r for r, j in enumerate(order)}
        order = sorted(range(len(cand)), key=lambda j: -(1 / (RRF_K + j) + 1 / (RRF_K + pos[j])))
        self._last = {self.rows[cand[j]]["id"]: (j + 1, round(float(ce[j]), 6)) for j in range(len(cand))}
        return [self.rows[cand[j]]["id"] for j in order]

    # ------------------------------------------------------------ merge
    def item(self, entry_id, bm25_rank=None, ce_score=None):
        e = self.byid[entry_id]
        it = kb_entry.render(e)
        it["id"] = e["id"]                 # so that a pack lists entries by id like chunks (the item has no other use of it)
        it.update(bm25_rank=bm25_rank, ce_score=ce_score, entry=True)
        return it

    def merge(self, frozen, question, expansion=None, rerank=True, *, max_items=MAX_ITEMS, max_chars=MAX_CHARS):
        """frozen: the frozen pack (chunk dicts, best first). Returns (items, info): the frozen pack unchanged, then the entries
        that fit; info has the added ids and the counts of what was skipped and why. Characters are those of pack_text of the
        whole pack."""
        t0 = time.monotonic()
        ids = self.ranked(question, expansion, rerank)
        last = getattr(self, "_last", {}) if rerank else {}
        out = list(frozen)
        in_pack = {c["id"] for c in frozen}
        seen_sets, added, skipped = set(), [], {"all_sources_in_pack": 0, "same_source_set": 0, "no_room": 0}
        for eid in ids:
            e = self.byid[eid]
            srcs = frozenset(s["id"] for s in e["sources"])
            if srcs <= in_pack:
                skipped["all_sources_in_pack"] += 1
                continue
            if srcs in seen_sets:
                skipped["same_source_set"] += 1
                continue
            it = self.item(eid, *last.get(eid, (None, None)))
            if len(out) + 1 > max_items or len(prompts.pack_text(out + [it])) > max_chars:
                skipped["no_room"] += 1
                continue
            out.append(it)
            seen_sets.add(srcs)
            added.append(eid)
        return out, {"added": added, "skipped": skipped, "candidates": len(ids), "pack_chars": len(prompts.pack_text(out)),
                     "seconds": time.monotonic() - t0}


# ---------------------------------------------------------------- stored packs
def resolve_pack(ids, kb=None, index_path=None):
    """Pack items for a stored pack (replay): a chunk id from the chunk index, a `kbe-` id from the entries (kb: a KbEntries).
    Raises KeyError for an id that is in neither. evalhooks.chunks_by_ids does the chunk part only."""
    from . import evalhooks
    chunk_ids = [i for i in ids if not str(i).startswith("kbe-")]
    chunks = dict(zip(chunk_ids, evalhooks.chunks_by_ids(chunk_ids, index_path))) if chunk_ids else {}
    out = []
    for i in ids:
        if str(i).startswith("kbe-"):
            if kb is None or i not in kb.byid:
                raise KeyError(i)
            out.append(kb.item(i))
        else:
            out.append(chunks[i])
    return out
