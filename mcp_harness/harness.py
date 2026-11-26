"""Runner of the MCP harness on the local kind cluster (CPU and Docker only; no network after fetch.sh).

    .venv/bin/python mcp_harness/harness.py check-faults [--ids ID ...] --out FILE.jsonl
        per scenario: inject (twice: idempotent), poll verify until the fault is visible, revert, reset to healthy
    .venv/bin/python mcp_harness/harness.py run --model stub [--ids ID ...] --out RUN_DIR
    .venv/bin/python mcp_harness/harness.py run --model openai --model-name NAME [--base-url URL] --out RUN_DIR
        per scenario: healthy check, inject, verify, the diagnosis loop (trace RUN_DIR/traces/<id>.jsonl), revert,
        reset, the image-pull check; then the mechanical score (RUN_DIR/summary.json). Revert and reset run
        whatever happens in between. Resumable: a scenario whose trace ends with a final event is skipped (this
        includes stop "failed": two failed requests in a row, kept in the denominator); a trace without one (an
        aborted attempt) is moved aside to <id>.jsonl.aborted-N and the scenario runs again. Any other error stops
        the run after the restore, printing only the exception's type. The model server URL comes from --base-url
        or OMLX_URL and the key from OMLX_API_KEY; nothing is printed of either.

All kubectl calls use the harness's own kubeconfig (work/mcp_harness/kubeconfig, or MCP_HARNESS_KUBECONFIG),
written by `cluster.sh up` or `cluster.sh kubeconfig`, and context kind-kafka-sre-mcp; never the user's default
kubeconfig or its current context.

The stub model (--model stub) replays a script built from the scenario's reference evidence, plus two calls
outside the allow-list (a Secret read and an unknown destructive tool) and one malformed call, so a dry run shows
calls run, blocked and rejected. It says nothing about any model.
"""
import argparse
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT))

from kafka_sre_rag.tools.cli import session_from  # noqa: E402
from kafka_sre_rag.tools.loop import OpenAIChatModel, ScriptedModel, run as run_loop  # noqa: E402

import score  # noqa: E402

CONTEXT = "kind-kafka-sre-mcp"
KUBECONFIG = Path(os.environ.get("MCP_HARNESS_KUBECONFIG") or ROOT / "work" / "mcp_harness" / "kubeconfig")
STUB_ANSWER = "[dry run: scripted stub model; no model was called]"


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def sh(cmd, timeout=1200, quiet=True):
    env = dict(os.environ, PY=sys.executable, KUBECONFIG=str(KUBECONFIG), MCP_HARNESS_KUBECONFIG=str(KUBECONFIG))
    p = subprocess.run(cmd, shell=True, cwd=ROOT, env=env, capture_output=quiet, text=True, timeout=timeout)
    return p.returncode, (p.stdout or "") + (p.stderr or "")


def cluster(action, timeout=1200):
    return sh(f"bash mcp_harness/cluster.sh {action}", timeout)


def wait_verify(sc):
    start = time.monotonic()
    while True:
        code, _ = sh(sc["verify"], timeout=120)
        el = time.monotonic() - start
        if code == 0:
            return True, round(el, 1)
        if el > sc["verify_timeout"]:
            return False, round(el, 1)
        log(f"  {sc['id']}: fault not visible yet ({el:.0f} s)")
        time.sleep(10)


def inject_verified(sc):
    # log lines older than the inject do not count for verify (lib.sh pod_logs_match)
    os.environ["FAULT_SINCE"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    for i in (1, 2):  # twice: an inject must be idempotent
        code, out = sh(sc["inject"], timeout=300)
        log(f"  inject {i}: exit {code}" + (f" {out.strip()[-300:]}" if code else ""))
        if code:
            return False, 0.0
    return wait_verify(sc)


def restore(sc):
    code, out = sh(sc["revert"], timeout=300)
    log(f"  revert: exit {code}" + (f" {out.strip()[-300:]}" if code else ""))
    code, out = cluster("reset", timeout=1500)
    log(f"  reset: {'healthy' if code == 0 else 'NOT HEALTHY'}")
    if code:
        print(out[-2000:], flush=True)
    return code == 0


# ---- the stub model ----------------------------------------------------------------------------------------------

def _simplify(pat):
    m = re.fullmatch(r"\((.*)\)\?", pat)
    pat = m.group(1) if m else pat
    return pat.split("|")[0]


def _concrete(match, names):
    args = {}
    for k, pat in match.items():
        if k in ("pod", "name") and re.search(r"[.*\\\[\]+?]", _simplify(pat)):
            hit = next((n for n in names if re.fullmatch(pat, n)), None)
            if hit is None:
                if re.fullmatch(pat, ""):
                    continue  # optional: leave the name out
                return None
            args[k] = hit
            continue
        v = _simplify(pat)
        if v == "" and re.fullmatch(pat, ""):
            continue
        args[k] = v
    for k, pat in match.items():
        if not re.fullmatch(pat, str(args.get(k, ""))):
            return None
    return args


def _pod_names(messages):
    names = []
    for m in messages:
        if m.get("role") == "tool":
            for line in (m.get("content") or "").splitlines()[1:]:
                if line.strip():
                    names.append(line.split()[0])
    return names


def stub_model(sc):
    def turn1(_messages):
        return {"content": "", "tool_calls": [
            {"name": "k8s_get", "arguments": {"kind": "pods", "namespace": "kafka"}},
            {"name": "k8s_get", "arguments": {"kind": "secrets", "namespace": "kafka"}}]}  # forbidden: a violation

    def turn2(messages):
        names, calls, seen = _pod_names(messages), [], set()
        for item in sc["reference_evidence"]:
            if item["shows"] in seen or len(calls) >= 4:
                continue
            args = _concrete(item["match"], names)
            if args is not None:
                seen.add(item["shows"])
                calls.append({"name": item["tool"], "arguments": args})
        calls.append({"name": "k8s_delete", "arguments": {"kind": "pods", "namespace": "kafka", "name": "kafka-broker-0"}})
        calls.append({"name": "k8s_logs", "arguments": {"namespace": "kafka", "pod": "Not_A_Pod"}})  # malformed
        return {"content": "", "tool_calls": calls}

    return ScriptedModel([turn1, turn2, {"content": STUB_ANSWER}])


# ---- commands ----------------------------------------------------------------------------------------------------

def pick(ids):
    scs = list(score.load_scenarios().values())
    if ids:
        known = {s["id"] for s in scs}
        missing = [i for i in ids if i not in known]
        if missing:
            sys.exit(f"unknown scenario ids: {missing}")
        scs = [s for s in scs if s["id"] in ids]
    return scs


def ensure_kubeconfig():
    if not KUBECONFIG.is_file():
        code, out = cluster("kubeconfig", 60)
        if code:
            sys.exit(f"no harness kubeconfig at {KUBECONFIG} and none could be written: {out.strip()[-300:]}")
        log(f"wrote the harness kubeconfig {KUBECONFIG}")


def ensure_healthy():
    code, out = cluster("health", 300)
    if code == 0:
        return True
    log("cluster not healthy; reset")
    code, out = cluster("reset", 1500)
    if code:
        print(out[-2000:], flush=True)
    return code == 0


def cmd_check_faults(a):
    out = Path(a.out)
    if out.exists():
        sys.exit(f"{out} exists; give a new path")
    out.parent.mkdir(parents=True, exist_ok=True)
    ensure_kubeconfig()
    for sc in pick(a.ids):
        log(f"{sc['id']}: start")
        if not ensure_healthy():
            sys.exit("cluster not healthy; stopped")
        t = time.monotonic()
        ok, secs, aborted = False, 0.0, None
        try:
            ok, secs = inject_verified(sc)
            log(f"  verify: {'VISIBLE' if ok else 'NOT VISIBLE'} after {secs} s")
        except Exception as e:  # noqa: BLE001 - only the type is printed
            aborted = type(e).__name__
            log(f"  aborted: {aborted}")
        finally:
            healthy = restore(sc)
        row = {"id": sc["id"], "visible": ok, "verify_seconds": secs, "healthy_after": healthy,
               "seconds": round(time.monotonic() - t, 1), **({"aborted": aborted} if aborted else {})}
        with out.open("a") as f:
            f.write(json.dumps(row) + "\n")
        if aborted:
            sys.exit(f"stopped: {aborted} in {sc['id']}")
        if not healthy:
            sys.exit("cluster not healthy after the revert; stopped")
    code, txt = cluster("pulls", 120)
    log(f"image pulls: {txt.strip()} ({'none' if code == 0 else 'PULLS SEEN'})")


def make_session():
    return session_from(None, str(KUBECONFIG), CONTEXT)


def move_aside(trace):
    """A trace without a final event (an aborted attempt) is kept, renamed, and the scenario runs again."""
    n = 1
    while trace.with_name(f"{trace.name}.aborted-{n}").exists():
        n += 1
    aside = trace.with_name(f"{trace.name}.aborted-{n}")
    trace.rename(aside)
    return aside


def cmd_run(a):
    run_dir = Path(a.out)
    (run_dir / "traces").mkdir(parents=True, exist_ok=True)
    if a.model == "openai":
        url = a.base_url or os.environ.get("OMLX_URL")
        if not url or not a.model_name:
            sys.exit("--model openai needs --model-name and --base-url or OMLX_URL")
    ensure_kubeconfig()
    for sc in pick(a.ids):
        trace = run_dir / "traces" / f"{sc['id']}.jsonl"
        if trace.exists():
            events = [json.loads(line) for line in trace.read_text().splitlines() if line.strip()]
            if events and events[-1].get("type") == "final":
                log(f"{sc['id']}: done before (stop {events[-1].get('stop')}); skipped")
                continue
            log(f"{sc['id']}: an earlier attempt has no final event; kept as {move_aside(trace).name}, run again")
        log(f"{sc['id']}: start")
        if not ensure_healthy():
            sys.exit("cluster not healthy; stopped")
        row, aborted = {"id": sc["id"]}, None
        try:
            ok, secs = inject_verified(sc)
            log(f"  verify: {'VISIBLE' if ok else 'NOT VISIBLE'} after {secs} s")
            row.update(visible=ok, verify_seconds=secs)
            if ok:
                model = stub_model(sc) if a.model == "stub" else OpenAIChatModel(
                    a.base_url or os.environ["OMLX_URL"], a.model_name, os.environ.get("OMLX_API_KEY"),
                    max_tokens=a.max_tokens, temperature=0.0, extra_body=json.loads(a.extra_body) if a.extra_body else None)
                session = make_session()
                try:
                    res = run_loop(sc["symptom"], model, session, a.max_turns, a.max_seconds, str(trace),
                                   text_protocol=a.text_protocol, scenario=sc["id"])
                finally:
                    session.close()
                row["diagnosis"] = {k: res[k] for k in ("stop", "turns", "seconds", "calls", "after_budget_calls")}
                if res.get("error"):
                    row["diagnosis"]["error"] = res["error"]
                log(f"  diagnosis: {res['stop']}, {res['turns']} turns, calls {res['calls']}, "
                    f"after the budget {res['after_budget_calls']}")
        except Exception as e:  # noqa: BLE001 - only the type is printed: a message can carry the server URL
            aborted = row["aborted"] = type(e).__name__
            log(f"  aborted: {aborted}")
        finally:
            row["healthy_after"] = restore(sc)
            code, txt = cluster("pulls", 120)
            row["pulls"] = json.loads(txt.strip().splitlines()[-1]) if txt.strip() else None
            row["no_pulls"] = code == 0
            log(f"  image pulls: {txt.strip()}")
            with (run_dir / "runs.jsonl").open("a") as f:
                f.write(json.dumps(row) + "\n")
        if aborted:
            sys.exit(f"stopped: {aborted} in {sc['id']}; the trace has no final event, so a rerun repeats the scenario")
        if not row["healthy_after"]:
            sys.exit("cluster not healthy after the revert; stopped")
    s = score.score_dir(run_dir)
    print(json.dumps(s["summary"], indent=1))


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("check-faults")
    c.add_argument("--ids", nargs="*")
    c.add_argument("--out", required=True)
    r = sub.add_parser("run")
    r.add_argument("--model", choices=["stub", "openai"], required=True)
    r.add_argument("--model-name")
    r.add_argument("--base-url")
    r.add_argument("--ids", nargs="*")
    r.add_argument("--out", required=True)
    r.add_argument("--max-turns", type=int, default=12)
    r.add_argument("--max-seconds", type=int, default=600)
    r.add_argument("--max-tokens", type=int, default=2048)
    r.add_argument("--extra-body", help="JSON merged into each request body")
    r.add_argument("--text-protocol", action="store_true")
    a = ap.parse_args(argv)
    (cmd_check_faults if a.cmd == "check-faults" else cmd_run)(a)


if __name__ == "__main__":
    main()
