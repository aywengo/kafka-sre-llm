"""Synthetic fixtures for the tests of M7 I4 and I5 (kafka_sre_rag/kb_entries.py, scripts/kb_build_index.py, scripts/m7_d7_served.py).

Everything is invented ("Widget operator" pages); no D7 text or id is used. The shapes are those of the real files: chunks of the
frozen index (id, source, path, title, text), the chain's entries index (kb_entry.finish_entry rows plus `rendered`, sorted by id),
the chain's MANIFEST.json, the slice file, D7's items / rubrics / slice tags / denominator, the ledger's denominator line, and the
search terms rows task I7 writes. write_all(dir) writes the lot and returns the paths.
"""
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
from kafka_sre_rag import kb_entry as K, prompts  # noqa: E402

PROMPT_VERSION, WRITER = "kbe/5", "writer-model-x"
CHECK_VERSION, CHECKER = "kbc/4", "checker-model-y"

FACTS = [   # (page, title, the page's one fact)
    ("fx-a.adoc", "Reconcile interval", "The `spec.reconcileIntervalSeconds` property sets how often the Widget operator reconciles a Widget. The default is 120 seconds."),
    ("fx-a.adoc", "Reconcile jitter", "The `spec.reconcileJitterPercent` property spreads the Widget operator reconcile runs by up to the given percentage."),
    ("fx-b.adoc", "Widget pool size", "The `spec.pool.size` property sets the number of Widget pods in the pool. The Widget operator accepts values from 1 to 9."),
    ("fx-b.adoc", "Pool drain", "The Widget operator drains a pool pod before it removes the pod. Draining waits at most 60 seconds."),
    ("fx-c.adoc", "Widget labels", "The Widget operator copies the labels in `spec.template.labels` to every Widget pod."),
    ("fx-c.adoc", "Widget annotations", "The Widget operator never copies annotations of the Widget resource to its pods."),
    ("fx-d.adoc", "Widget upgrade", "To upgrade the Widget operator, first stop every Widget reconcile, then apply the new operator bundle."),
    ("fx-d.adoc", "Widget rollback", "A rollback of the Widget operator needs the previous bundle and a restart of the operator pod."),
    ("fx-e.adoc", "Widget metrics", "The Widget operator exposes the metric `widget_reconcile_total` on port 8080."),
    ("fx-e.adoc", "Widget probes", "The Widget operator readiness probe checks the reconcile loop. The probe period is 10 seconds."),
    ("fx-f.adoc", "Widget logging", "The `WIDGET_LOG_LEVEL` environment variable sets the log level of the Widget operator."),
    ("fx-f.adoc", "Widget leases", "The Widget operator holds a lease named `widget-operator-lease` while it is the leader."),
    ("fx-g.adoc", "Widget quotas", "A namespace quota limits the Widget resources that the Widget operator creates in the namespace."),
    ("fx-g.adoc", "Widget finalizers", "The Widget operator adds the finalizer `widget.example/cleanup` to every Widget resource."),
]
CHUNKS = []
for n, (page, title, fact) in enumerate(FACTS, 1):
    CHUNKS.append({"id": f"c{n:02d}", "source": "strimzi", "path": f"raw/strimzi/documentation/modules/{page}",
                   "title": f"Widget operator guide > {title}", "text": fact + " This page belongs to the Widget operator guide."})
CH = {c["id"]: c for c in CHUNKS}

# entries: (target chunk, extra source or None, kind, title, statement); the quote is the chunk's first sentence
ENTRY_SPECS = [
    ("c01", None, "value", "Reconcile interval default", "In the Widget operator, `spec.reconcileIntervalSeconds` sets the reconcile interval; the default is 120 seconds."),
    ("c03", None, "value", "Pool size limits", "The Widget operator accepts a `spec.pool.size` from 1 to 9."),
    ("c04", None, "warning", "Pool drain wait", "The Widget operator drains a pool pod for at most 60 seconds before it removes it."),
    ("c06", None, "warning", "Annotations are not copied", "The Widget operator never copies annotations of the Widget resource to its pods."),
    ("c07", "c08", "cross", "Upgrade and rollback", "To upgrade the Widget operator stop every Widget reconcile and apply the new bundle; a rollback needs the previous bundle."),
    ("c09", None, "value", "Reconcile metric", "The Widget operator exposes `widget_reconcile_total` on port 8080."),
    ("c11", None, "value", "Log level variable", "`WIDGET_LOG_LEVEL` sets the log level of the Widget operator."),
    ("c12", None, "fact", "Leader lease", "The leading Widget operator holds the lease `widget-operator-lease`."),
    ("c14", None, "fact", "Cleanup finalizer", "The Widget operator adds the finalizer `widget.example/cleanup` to every Widget resource."),
    ("c02", None, "fact", "Reconcile jitter", "`spec.reconcileJitterPercent` spreads Widget operator reconcile runs by up to the given percentage."),
]


def first_sentence(text):
    return text.split(". ")[0].rstrip(".") + "."


def entry_rows():
    rows = []
    for target, extra, kind, title, statement in ENTRY_SPECS:
        srcs = [target] + ([extra] if extra else [])
        e = {"kind": kind, "title": title, "statement": statement, "sources": srcs,
             "quotes": [{"chunk": c, "text": first_sentence(CH[c]["text"])} for c in srcs]}
        win = [CH[c] for c in srcs]
        row = K.finish_entry(e, win, target, prompt_version=PROMPT_VERSION, writer=WRITER, created="2026-11-26T00:00:00Z",
                             commits={"strimzi": "0000000"})
        row["rendered"] = K.render_text(row)
        rows.append(row)
    rows.sort(key=lambda r: r["id"])
    return rows


ITEMS = [
    {"id": "fx-01", "question": "How often does the Widget operator reconcile a Widget, and can I change it?", "evidence": "$ kubectl get widget w1\nNAME READY\nw1 True"},
    {"id": "fx-02", "question": "The Widget operator removed a pool pod early. How long does it drain a pod, and what sizes may the pool have?", "evidence": ""},
    {"id": "fx-03", "question": "Which labels and annotations does the Widget operator copy to its pods?", "evidence": "metadata:\n  annotations: {}"},
]
# must points: (id, weight, chunk_sets)
POINTS = {
    "fx-01": [("m1", 2, [["c01"]]), ("m2", 1, [["c02"]]), ("m3", 1, []), ("m4", 2, [["c09"], ["c10", "c11"]])],
    "fx-02": [("m1", 2, [["c03"]]), ("m2", 1, [["c04"]]), ("m3", 1, [["c07", "c08"]]), ("m4", 1, [])],
    "fx-03": [("m1", 1, [["c05"]]), ("m2", 2, [["c06"]]), ("m3", 1, [["c13"]])],
}
TAGS = {"fx-01": "slice", "fx-02": "slice", "fx-03": "outside"}


def rubrics():
    return [{"id": s, "must": [{"id": p, "text": f"Point {s} {p}: the fact the covering chunk states.", "weight": w, "first": False, "source": "x",
                                "chunks": sorted({c for st in sets for c in st}), "chunk_sets": sets, "set_evidence": []} for p, w, sets in pts]}
            for s, pts in POINTS.items()]


def denominator():
    return [f"{s}/{p}" for s, pts in POINTS.items() if TAGS[s] == "slice" for p, _, sets in pts if sets]


def ledger_text(den_sha):
    """The line formats of runs/m7/D7_LEDGER.md that name the denominator file."""
    return ("# D7 ledger\n\n- Step 6 (`d7_step6.md`): the list is fixed now.\n"
            f"- **The denominator (fixed now, never changes):** the covered points of the 40 in-slice scenarios, 185 points,\n"
            f"  `eval/m7/d7/denominator.txt`, sha256 `{den_sha}`.\n\n"
            "| file | sha256 |\n|---|---|\n"
            f"| `eval/m7/d7/denominator.txt` | `{den_sha}` |\n")


def terms_rows():
    out = []
    for it in ITEMS:
        q = it["question"] + (("\n\nEvidence:\n" + it["evidence"]) if it.get("evidence") else "")
        msgs = prompts.expand_messages(q)
        blob = json.dumps([{"role": m["role"], "content": m["content"]} for m in msgs], sort_keys=True, ensure_ascii=False,
                          separators=(",", ":")).encode("utf-8")
        out.append({"id": it["id"], "expansion": "widget operator, reconcile, pool, labels, annotations, drain, interval", "model": "qwen-stub",
                    "prompt_sha256": hashlib.sha256(blob).hexdigest(), "prompt_chars": sum(len(m["content"]) for m in msgs),
                    "max_tokens": 300, "tokens": 12, "truncated": False, "seconds": 0.1})
    return out


def jl(rows):
    return "".join(json.dumps(r, ensure_ascii=False, sort_keys=True) + "\n" for r in rows)


def write_all(d):
    """Write every fixture into directory d; returns {name: Path}."""
    d = Path(d)
    d.mkdir(parents=True, exist_ok=True)
    P = {n: d / n for n in ("chunks.jsonl", "entries_index.jsonl", "slice.jsonl", "MANIFEST.json", "items.jsonl", "rubrics.jsonl",
                            "slice_tags.json", "denominator.txt", "D7_LEDGER.md", "search_terms.jsonl")}
    P["chunks.jsonl"].write_text(jl(CHUNKS))
    rows = entry_rows()
    P["entries_index.jsonl"].write_text("".join(K.dumps(r) + "\n" for r in rows))
    P["slice.jsonl"].write_text(jl([{"id": c["id"]} for c in CHUNKS]))
    P["MANIFEST.json"].write_text(json.dumps({"schema": 1, "kind": "dry", "entry_prompt_version": PROMPT_VERSION, "check_prompt_version": CHECK_VERSION,
                                              "options": {"writer": WRITER, "checker": CHECKER}}, indent=1))
    P["items.jsonl"].write_text(jl(ITEMS))
    P["rubrics.jsonl"].write_text(jl(rubrics()))
    P["slice_tags.json"].write_text(json.dumps({"scenarios": [{"id": s, "code_tag": t} for s, t in TAGS.items()]}))
    P["denominator.txt"].write_text("".join(x + "\n" for x in denominator()))
    P["D7_LEDGER.md"].write_text(ledger_text(hashlib.sha256(P["denominator.txt"].read_bytes()).hexdigest()))
    P["search_terms.jsonl"].write_text(jl(terms_rows()))
    return P
