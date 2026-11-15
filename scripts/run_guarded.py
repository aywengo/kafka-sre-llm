#!/usr/bin/env python3
"""Run a long job and restart it automatically when it dies of a GPU or memory problem.

  scripts/run_guarded.py --log runs/v3/logs/gen.log -- .venv/bin/python eval/compare.py ...

What it does:
  * runs the command with stdout+stderr appended to --log;
  * exit 0                      -> done, exits 0;
  * died of a GPU/memory cause  -> waits --wait-min (20) minutes, restarts; at most
                                   --max-restarts (6) restarts in total, then gives up (exit 75);
  * any other failure           -> no restart, exits with the command's code
                                   (unless --other-retries N is set; those wait --other-wait-s).

A failure counts as GPU/memory when one of these holds:
  * the output of that attempt matches a known pattern ([METAL], kIOGPU..., GPU Hang,
    Insufficient Memory, out of memory, MemoryError, bad_alloc, failed to allocate, ...);
  * the process was killed with SIGKILL (on macOS that is what the kernel does to a process
    when memory runs out);
  * --stall-min N is set and the log did not grow for N minutes (a silent GPU hang): the guard
    kills the job and treats it as a GPU failure.

The restart runs the same command again, so the command must resume from its own output
(generate_qa.py, compare.py --cache / --judge-cache do). Training does NOT: mlx_lm.lora would
start again at iteration 0 and overwrite the adapter. For such jobs pass --resume-cmd, a shell
command that is run instead of the original on every restart (it has to build the resume
config from the last checkpoint itself). Without --resume-cmd the guard refuses to restart a
command that contains "mlx_lm.lora" / "mlx_lm lora".

The child sees GUARD_ATTEMPT (1 = first run) and GUARD_REASON (gpu|memory|stall|other) in its
environment. Every restart and the final give-up are written to the log as lines starting with
"GUARD:", to --status (JSON) and, if given, appended to --heartbeat as a short entry.

The job runs in its own process group, and the guard always signals the whole group, so a
trainer started by a wrapper script or --resume-cmd (shell) is stopped with it. Before a
restart the guard checks that no process of the old group is left (a GPU-hung process may not
die); if one is, it does not restart (exit 75), so two GPU jobs never run side by side.

To stop a guarded job, send SIGTERM to the GUARD's pid (it stops the job and does not
restart). Never kill the job itself: a SIGKILL looks like an out-of-memory kill and is restarted.
"""
import argparse
import datetime as dt
import json
import os
import re
import signal
import subprocess
import sys
import time

GROUP_EXIT_WAIT_S = 30

GPU_PATTERNS = [
    r"\[METAL\]",
    r"kIOGPU\w*",
    r"GPU Hang",
    r"Command buffer execution failed",
    r"GPU Timeout Error",
    r"MTLCommandBuffer\w* .*(error|fail)",
]
MEMORY_PATTERNS = [
    r"Insufficient Memory",
    r"out of memory",
    r"\bOOM\b",
    r"MemoryError",
    r"std::bad_alloc",
    r"failed to allocate",
    r"unable to allocate",
    r"cannot allocate memory",
    r"\bENOMEM\b",
    r"malloc: .*(fail|error)",
    r"jetsam",
]
TRAIN_HINT = re.compile(r"mlx_lm[. ]lora")
GIVE_UP_EXIT = 75


def now():
    return dt.datetime.now().astimezone().isoformat(timespec="seconds")


def classify(returncode, text, stalled, extra):
    """-> (kind, detail); kind is gpu | memory | stall | other."""
    if stalled:
        return "stall", "log did not grow"
    for kind, pats in (("gpu", GPU_PATTERNS + extra), ("memory", MEMORY_PATTERNS)):
        for p in pats:
            m = re.search(p, text, re.I)
            if m:
                return kind, f"matched '{m.group(0)}'"
    if returncode == -signal.SIGKILL:
        return "memory", "killed with SIGKILL (the kernel does this when memory runs out)"
    return "other", f"exit {returncode}"


def group_alive(pgid):
    try:
        os.killpg(pgid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def signal_group(pgid, sig):
    """Signal a process group. -> False if there was nothing to signal.
    macOS answers EPERM (PermissionError), not ESRCH, when the group holds only a zombie,
    i.e. the job has exited but was not reaped yet; that is not an error here."""
    try:
        os.killpg(pgid, sig)
        return True
    except (ProcessLookupError, PermissionError):
        return False


def stop_group(proc, wait_s=GROUP_EXIT_WAIT_S):
    """SIGTERM the job's whole process group, then SIGKILL after wait_s. -> True if the group is gone."""
    pgid = proc.pid
    for sig in (signal.SIGTERM, signal.SIGKILL):
        if not signal_group(pgid, sig):
            break
        deadline = time.time() + wait_s
        while time.time() < deadline:
            if proc.poll() is not None and not group_alive(pgid):
                break
            time.sleep(0.5)
        if not group_alive(pgid):
            break
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass
    return not group_alive(pgid)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--log", required=True, help="job output is appended here")
    ap.add_argument("--wait-min", type=float, default=20, help="minutes to wait before a restart (20)")
    ap.add_argument("--max-restarts", type=int, default=6, help="restarts after GPU/memory failures (6)")
    ap.add_argument("--resume-cmd", help="shell command run instead of the original on restarts")
    ap.add_argument("--stall-min", type=float, default=0,
                    help="kill and restart if the log does not grow for this many minutes (0 = off)")
    ap.add_argument("--other-retries", type=int, default=0, help="retries for non GPU/memory failures (0)")
    ap.add_argument("--other-wait-s", type=float, default=60, help="seconds to wait before those (60)")
    ap.add_argument("--pattern", action="append", default=[], help="extra regex that counts as a GPU failure")
    ap.add_argument("--status", help="JSON status file (default: LOG + .guard.json)")
    ap.add_argument("--heartbeat", help="markdown file to append restart / give-up entries to")
    ap.add_argument("--name", help="job name for messages (default: log file name)")
    ap.add_argument("cmd", nargs=argparse.REMAINDER, help="-- command ...")
    a = ap.parse_args()
    cmd = a.cmd[1:] if a.cmd[:1] == ["--"] else a.cmd
    if not cmd:
        ap.error("no command given (put it after --)")
    name = a.name or os.path.basename(a.log)
    status_path = a.status or a.log + ".guard.json"
    os.makedirs(os.path.dirname(os.path.abspath(a.log)), exist_ok=True)
    is_training = bool(TRAIN_HINT.search(" ".join(cmd)))

    state = {"name": name, "cmd": cmd, "resume_cmd": a.resume_cmd, "started": now(),
             "attempt": 0, "restarts": 0, "other_retries": 0, "events": [], "state": "RUNNING"}
    child = None
    stopping = False

    def note(msg):
        with open(a.log, "a") as f:
            f.write(f"GUARD: {now()} {msg}\n")

    def save():
        state["updated"] = now()
        tmp = status_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(state, f, indent=2)
        os.replace(tmp, status_path)

    def heartbeat(title, lines, needs_human):
        if not a.heartbeat:
            return
        with open(a.heartbeat, "a") as f:
            f.write(f"\n### {now()} — {name}: {title}\n")
            for line in lines:
                f.write(line + "\n")
            f.write(f"needs_human: {'yes' if needs_human else 'no'}\n")

    def on_signal(signum, _frame):
        nonlocal stopping
        stopping = True
        if child is not None:
            signal_group(child.pid, signal.SIGTERM)

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    def run_once(reason):
        """Run one attempt. -> (returncode, text of this attempt, stalled)"""
        nonlocal child
        state["attempt"] += 1
        env = dict(os.environ, GUARD_ATTEMPT=str(state["attempt"]), GUARD_REASON=reason)
        use_resume = state["attempt"] > 1 and a.resume_cmd
        start = os.path.getsize(a.log) if os.path.exists(a.log) else 0
        with open(a.log, "a") as out:
            if use_resume:
                child = subprocess.Popen(a.resume_cmd, shell=True, stdout=out, stderr=subprocess.STDOUT, env=env,
                                         start_new_session=True)
            else:
                child = subprocess.Popen(cmd, stdout=out, stderr=subprocess.STDOUT, env=env,
                                         start_new_session=True)
        state["pid"] = child.pid
        save()
        stalled = False
        last_size, last_change = start, time.time()
        while child.poll() is None:
            time.sleep(1 if a.wait_min < 1 else 5)
            if stopping:
                # the handler sent SIGTERM; make sure the group really goes (SIGKILL after the wait)
                stop_group(child)
                break
            if a.stall_min > 0:
                size = os.path.getsize(a.log)
                if size != last_size:
                    last_size, last_change = size, time.time()
                elif time.time() - last_change > a.stall_min * 60:
                    stalled = True
                    note(f"no output for {a.stall_min:g} min, killing process group {child.pid}")
                    stop_group(child)
                    break
        if child.returncode is None:
            # never wait without a limit: a GPU-hung process can survive SIGKILL
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                note(f"pid {child.pid} did not exit after SIGKILL")
        with open(a.log, "rb") as f:
            f.seek(start)
            text = f.read().decode("utf-8", "replace")
        # judge the end of the attempt, where the error is; skip our own lines
        text = "\n".join(l for l in text[-200_000:].splitlines() if not l.startswith("GUARD:"))
        return child.returncode, text, stalled

    reason = "first"
    while True:
        rc, text, stalled = run_once(reason)
        if rc == 0 and not stalled:
            state["state"] = "DONE"
            save()
            if state["restarts"] or state["other_retries"]:
                note(f"finished after {state['restarts']} GPU/memory restart(s), "
                     f"{state['other_retries']} other retry(ies)")
            return 0
        if stopping:
            state["state"] = "STOPPED"
            save()
            note("stopped by signal, not restarting"
                 + (f"; processes of group {child.pid} are STILL ALIVE" if group_alive(child.pid) else ""))
            return 130
        kind, detail = classify(rc, text, stalled, a.pattern)
        last_line = next((l for l in reversed(text.splitlines()) if l.strip()), "")[-300:]
        event = {"ts": now(), "attempt": state["attempt"], "kind": kind, "detail": detail,
                 "returncode": rc, "last_line": last_line}
        state["events"].append(event)

        if kind == "other":
            if state["other_retries"] < a.other_retries:
                state["other_retries"] += 1
                note(f"attempt {state['attempt']} failed ({detail}), not GPU/memory; "
                     f"retry {state['other_retries']}/{a.other_retries} in {a.other_wait_s:g} s")
                save()
                time.sleep(a.other_wait_s)
                reason = "other"
                continue
            state["state"] = "FAILED"
            save()
            note(f"attempt {state['attempt']} failed ({detail}); not a GPU/memory failure, NOT restarting")
            heartbeat("FAILED (not GPU/memory), not restarted",
                      [f"event:     attempt {state['attempt']}, {detail}", f"last line: {last_line}",
                       f"log:       {a.log}"], True)
            return rc if 0 < rc < 126 else 1

        # GPU / memory / stall
        if is_training and not a.resume_cmd:
            state["state"] = "FAILED"
            save()
            note(f"attempt {state['attempt']} failed ({kind}: {detail}); this is a training command and "
                 f"no --resume-cmd was given, NOT restarting (a rerun would start at iteration 0)")
            heartbeat(f"{kind.upper()} failure in training, not restarted (no --resume-cmd)",
                      [f"event:     {detail}", f"last line: {last_line}", f"log:       {a.log}"], True)
            return GIVE_UP_EXIT
        if state["restarts"] >= a.max_restarts:
            state["state"] = "GAVE_UP"
            save()
            note(f"attempt {state['attempt']} failed ({kind}: {detail}); "
                 f"GIVING UP after {a.max_restarts} restarts")
            heartbeat(f"GAVE UP after {a.max_restarts} restarts ({kind})",
                      [f"event:     {detail}", f"last line: {last_line}",
                       f"history:   " + "; ".join(f"{e['ts'][11:16]} {e['kind']}" for e in state["events"]),
                       f"log:       {a.log}"], True)
            return GIVE_UP_EXIT
        if group_alive(child.pid) and not stop_group(child):
            state["state"] = "GAVE_UP"
            save()
            note(f"attempt {state['attempt']} failed ({kind}: {detail}); processes of group {child.pid} "
                 f"are still alive after SIGKILL, NOT restarting (a second GPU job would run beside them)")
            heartbeat(f"{kind.upper()} failure, old processes survived SIGKILL, not restarted",
                      [f"event:     {detail}", f"group:     {child.pid} still alive (check ps; may need a reboot)",
                       f"log:       {a.log}"], True)
            return GIVE_UP_EXIT
        state["restarts"] += 1
        at = (dt.datetime.now().astimezone() + dt.timedelta(minutes=a.wait_min)).strftime("%H:%M")
        state["state"] = "WAITING"
        state["next_restart"] = at
        save()
        note(f"attempt {state['attempt']} failed ({kind}: {detail}); "
             f"restart {state['restarts']}/{a.max_restarts} at {at}")
        heartbeat(f"{kind.upper()} failure, restart {state['restarts']}/{a.max_restarts} at {at}",
                  [f"event:     {detail}", f"last line: {last_line}", f"log:       {a.log}"], False)
        deadline = time.time() + a.wait_min * 60
        while time.time() < deadline and not stopping:
            time.sleep(min(5, max(0.05, deadline - time.time())))
        if stopping:
            state["state"] = "STOPPED"
            save()
            note("stopped by signal while waiting, not restarting")
            return 130
        state["state"] = "RUNNING"
        reason = kind


if __name__ == "__main__":
    sys.exit(main())
