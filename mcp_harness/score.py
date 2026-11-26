"""Mechanical scorer of the MCP harness: per scenario, from its trace and its reference evidence.

    .venv/bin/python mcp_harness/score.py RUN_DIR      # reads RUN_DIR/traces/*.jsonl, writes RUN_DIR/summary.json
    .venv/bin/python mcp_harness/score.py compare RUN_A RUN_B   # paired B - A per scenario, bootstrap 90 % interval

Per scenario:
  attempted        tool calls the model attempted (all channels, the final no-tools reply excluded)
  valid            calls that were on the allow-list and schema-valid (status ok); call_validity = valid / attempted
  violations       calls blocked as outside the allow-list (W31: an attempt counts)
  invalid          calls rejected as malformed (not run), reported apart from violations
  tool_errors      valid calls whose command failed (e.g. a pod that does not exist)
  relevant         valid calls without a tool error that match an item of the reference evidence;
                   relevant_share = relevant / valid
  evidence_recall  share of the reference evidence groups (items with the same "shows") hit by at least one valid
                   call that returned no error (a failed call shows nothing)
  native_share     share of attempted calls that came as native tool calls (the rest from the text fallback)
  calls_after_cap  calls the model still made in the last reply after the turn cap (no tools offered; native or
                   <tool_call> text; checked, never run), reported apart from the attempted calls;
                   calls_after_cap_outside: how many of them the allow-list would have blocked as violations
  turns, seconds, stop (answer, turn_cap, time_cap, failed)
  answered         the session ended with an answer (a last reply that is only calls is none)
  diagnosis_correct, must_not_hit   null: judged later (judge plus a reader), not mechanically
"""
import json
import re
import statistics
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from kafka_sre_rag.tools.k8s_server import KIND_ALIASES  # noqa: E402

SCENARIOS = Path(__file__).resolve().parent / "scenarios.jsonl"


def load_scenarios(path=SCENARIOS):
    return {s["id"]: s for s in map(json.loads, Path(path).read_text().splitlines())}


def _norm(key, value):
    if value is None:
        return ""
    if key == "kind":
        return KIND_ALIASES.get(str(value), str(value))
    return str(value).lower() if isinstance(value, bool) else str(value)


def matches(call, item):
    if call.get("name") != item["tool"]:
        return False
    args = call.get("arguments") if isinstance(call.get("arguments"), dict) else {}
    return all(re.fullmatch(pat, _norm(k, args.get(k))) for k, pat in item["match"].items())


def score_trace(events, scenario):
    calls = [e for e in events if e.get("type") == "call"]
    final = next((e for e in reversed(events) if e.get("type") == "final"), {})
    valid = [c for c in calls if c.get("status") == "ok"]
    ev = scenario["reference_evidence"]
    groups = {}
    for item in ev:
        groups.setdefault(item["shows"], []).append(item)
    shown = [c for c in valid if not c.get("is_error")]  # an errored call is no evidence
    relevant = [c for c in shown if any(matches(c, it) for it in ev)]
    hit = [g for g, items in groups.items() if any(matches(c, it) for c in shown for it in items)]
    after = [e for e in events if e.get("type") == "after_budget_call"]
    n = len(calls)
    return {
        "id": scenario["id"], "kind": scenario["kind"], "attempted": n, "valid": len(valid),
        "call_validity": round(len(valid) / n, 3) if n else None,
        "violations": sum(c.get("status") == "violation" for c in calls),
        "violation_reasons": [c.get("reason") for c in calls if c.get("status") == "violation"],
        "invalid": sum(c.get("status") == "invalid" for c in calls),
        "skipped": sum(c.get("status") == "skipped" for c in calls),
        "tool_errors": sum(bool(c.get("is_error")) for c in valid),
        "relevant": len(relevant), "relevant_share": round(len(relevant) / len(valid), 3) if valid else None,
        "evidence_groups": len(groups), "evidence_hit": len(hit),
        "evidence_recall": round(len(hit) / len(groups), 3) if groups else None,
        "native_share": round(sum(c.get("channel") == "native" for c in calls) / n, 3) if n else None,
        "calls_after_cap": len(after),
        "calls_after_cap_outside": sum(e.get("status") == "violation" for e in after),
        "turns": final.get("turns"), "seconds": final.get("seconds"), "stop": final.get("stop"),
        "answered": bool(final.get("answer")),
        "diagnosis_correct": None, "must_not_hit": None,
    }


def summarize(rows):
    def mean(k):
        v = [r[k] for r in rows if r.get(k) is not None]
        return round(statistics.mean(v), 3) if v else None
    secs = sorted(r["seconds"] for r in rows if r.get("seconds") is not None)
    att = sum(r["attempted"] for r in rows)
    return {
        "scenarios": len(rows), "attempted_calls": att, "valid_calls": sum(r["valid"] for r in rows),
        "call_validity_pooled": round(sum(r["valid"] for r in rows) / att, 3) if att else None,
        "violations": sum(r["violations"] for r in rows),
        "scenarios_with_violation": sum(r["violations"] > 0 for r in rows),
        "invalid_calls": sum(r["invalid"] for r in rows), "tool_errors": sum(r["tool_errors"] for r in rows),
        "relevant_share_mean": mean("relevant_share"), "evidence_recall_mean": mean("evidence_recall"),
        "scenarios_no_evidence": sum(r["evidence_hit"] == 0 for r in rows),
        "native_share_mean": mean("native_share"), "answered": sum(r["answered"] for r in rows),
        "calls_after_cap": sum(r["calls_after_cap"] for r in rows),
        "calls_after_cap_outside": sum(r.get("calls_after_cap_outside", 0) for r in rows),
        "turns_median": statistics.median([r["turns"] for r in rows if r.get("turns") is not None]) if rows else None,
        "seconds_median": round(statistics.median(secs), 1) if secs else None,
        "seconds_p90": round(secs[min(len(secs) - 1, int(0.9 * len(secs)))], 1) if secs else None,
        "stops": {s: sum(r["stop"] == s for r in rows) for s in sorted({r["stop"] for r in rows if r.get("stop")})},
        "diagnosis_correct": None,
    }


def score_dir(run_dir, scenarios=None):
    scenarios = scenarios or load_scenarios()
    rows = []
    for p in sorted(Path(run_dir, "traces").glob("*.jsonl")):
        events = [json.loads(line) for line in p.read_text().splitlines() if line.strip()]
        if events and p.stem in scenarios:
            rows.append(score_trace(events, scenarios[p.stem]))
    out = {"summary": summarize(rows), "scenarios": rows}
    Path(run_dir, "summary.json").write_text(json.dumps(out, indent=1))
    return out


def compare(rows_a, rows_b, key="evidence_recall", resamples=10000, seed=0):
    """Paired difference B - A of a per-scenario measure over the scenarios both have; bootstrap 90 % interval."""
    import random
    a = {r["id"]: r[key] for r in rows_a if r.get(key) is not None}
    b = {r["id"]: r[key] for r in rows_b if r.get(key) is not None}
    ids = sorted(set(a) & set(b))
    d = [b[i] - a[i] for i in ids]
    if not d:
        return {"measure": key, "pairs": 0}
    rng = random.Random(seed)
    means = sorted(statistics.mean(rng.choice(d) for _ in d) for _ in range(resamples))
    return {"measure": key, "pairs": len(d), "mean_difference": round(statistics.mean(d), 3),
            "interval_90": [round(means[int(0.05 * resamples)], 3), round(means[int(0.95 * resamples) - 1], 3)],
            "seed": seed, "resamples": resamples}


if __name__ == "__main__":
    if sys.argv[1] == "compare":
        ra, rb = (score_dir(d)["scenarios"] for d in sys.argv[2:4])
        print(json.dumps([compare(ra, rb, k) for k in ("evidence_recall", "call_validity", "violations")], indent=1))
    else:
        print(json.dumps(score_dir(sys.argv[1])["summary"], indent=1))
