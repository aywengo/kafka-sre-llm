#!/usr/bin/env python3
"""Laptop tests of GOAL.md ("what fits on a working laptop"): install, offline, memory, speed, sustained use,
a working laptop, and the scorer on a fixed open subset. The plan is agents/laptop-tests.md.

One command per phase, all output under out/laptop/<date>/ (git-ignored), resumable by output files:

  preflight        what is present and missing; writes preflight.json and config.json (no model, no network)
  install-check    state of the installation; with --run it executes the documented sequence, timed per step,
                   the model download counted separately
  speed            time to a complete answer, time to the first words, tokens per second, the split, for the
                   conditions service|oneshot x default|strict|default+check; --label names a series
                   (baseline, working-laptop)
  sustained        30 questions in a row; pmset before, during and after; --label ac|battery
  memory           baseline, idle with the service loaded, three questions; peaks over the whole folder
  score            60 open mechanical items through scripts/eval2.py and eval2_score.py, next to the stored
                   result of the Mac Studio (eval/m4/laptop/ids.json)
  bundle           SUMMARY.md and the scrubbed folder to send back (send_back/ and send_back.zip)
  make-ids         (reference machine only) writes eval/m4/laptop/ids.json from work/m4/s6/

The runtime is not changed or imported by the phases: the service (`python -m kafka_sre_rag serve`) is used through
HTTP, one-shot through `ask --json`. Only the retrieval probe imports kafka_sre_rag.retrieval, in its own process.
The server address and key come from OMLX_URL and OMLX_API_KEY (environment); they are never written anywhere.
Every child process gets HF_HUB_OFFLINE=1 and TRANSFORMERS_OFFLINE=1.

--stub replaces every model, service and network call by a deterministic fake; its output is marked STUB.
"""
import argparse
import datetime
import getpass
import hashlib
import json
import os
import random
import re
import shutil
import socket
import statistics
import subprocess
import sys
import threading
import time
import zipfile
from pathlib import Path
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parent.parent
IDS_FILE = ROOT / "eval/m4/laptop/ids.json"
VENV_PY = ROOT / ".venv/bin/python"
OFFLINE_ENV = {"HF_HUB_OFFLINE": "1", "TRANSFORMERS_OFFLINE": "1", "HF_DATASETS_OFFLINE": "1",
               "HF_HUB_DISABLE_TELEMETRY": "1", "DO_NOT_TRACK": "1"}
DEFAULTS = {"backend": "openai", "model": None, "extra_body": '{"chat_template_kwargs": {"enable_thinking": false}}',
            "k": "k6", "max_tokens": None, "catalogues": "work/m4/catalogues", "weights_dir": None,
            "model_repo": "mlx-community/Qwen3.8-27B-4bit", "port": 8765, "interval": 2.0}
BARS = {"install_s": 1800, "median_answer_s": 60, "memory_gb": 20, "weights_gb": (15, 17), "drift_ratio": 1.2,
        "score_margin_points": 5}
CONDS = ["service-default", "service-strict", "service-default-check",
         "oneshot-default", "oneshot-strict", "oneshot-default-check"]


# ------------------------------------------------------------------ small helpers

def py():
    return str(VENV_PY if VENV_PY.exists() else sys.executable)


def now():
    return time.time()


def sh(cmd, timeout=20):
    """stdout of a command, or None when it is missing, fails or times out. Never raises."""
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, cwd=ROOT)
        return r.stdout if r.returncode == 0 else None
    except (OSError, subprocess.SubprocessError):
        return None


def read_jsonl(path):
    p = Path(path)
    if not p.exists():
        return []
    return [json.loads(l) for l in p.read_text(encoding="utf-8").splitlines() if l.strip()]


def append_jsonl(path, row):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())


def write_json(path, obj):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(obj, indent=1, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")


def read_json(path, default=None):
    p = Path(path)
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else default


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def med(xs):
    xs = [x for x in xs if x is not None]
    return statistics.median(xs) if xs else None


def fmt(x, nd=1):
    return "-" if x is None else f"{x:.{nd}f}"


def dir_size_bytes(path):
    total = 0
    for dp, _, fs in os.walk(path):
        for f in fs:
            try:
                total += os.path.getsize(os.path.join(dp, f))
            except OSError:
                pass
    return total


def child_env(extra=None):
    env = dict(os.environ)
    env.update(OFFLINE_ENV)
    env.update(extra or {})
    return env


# ------------------------------------------------------------------ parsers of the macOS tools

def parse_vm_stat(text):
    """vm_stat -> GB figures and the swap counters. used_gb follows Activity Monitor: anonymous minus purgeable,
    plus wired, plus the compressor."""
    if not text:
        return None
    m = re.search(r"page size of (\d+) bytes", text)
    page = int(m.group(1)) if m else 16384
    v = {}
    for line in text.splitlines():
        mm = re.match(r'"?([^":]+?)"?:\s+(\d+)\.?\s*$', line.strip())
        if mm:
            v[mm.group(1)] = int(mm.group(2))
    gb = lambda key: v.get(key, 0) * page / 2 ** 30  # noqa: E731
    if "Anonymous pages" in v:
        used = gb("Anonymous pages") - gb("Pages purgeable") + gb("Pages wired down") + gb("Pages occupied by compressor")
    else:
        used = gb("Pages active") + gb("Pages wired down") + gb("Pages occupied by compressor")
    return {"used_gb": round(used, 3), "free_gb": round(gb("Pages free"), 3), "wired_gb": round(gb("Pages wired down"), 3),
            "compressed_gb": round(gb("Pages occupied by compressor"), 3), "swapins": v.get("Swapins", 0),
            "swapouts": v.get("Swapouts", 0)}


def parse_swapusage(text):
    if not text:
        return None
    m = re.search(r"used = ([\d.]+)([MG])", text)
    if not m:
        return None
    return round(float(m.group(1)) * (1024 if m.group(2) == "G" else 1), 2)


def parse_free_pct(text):
    m = re.search(r"free percentage:\s*(\d+)%", text or "")
    return int(m.group(1)) if m else None


def parse_pmset_batt(text):
    """Power source, battery percent and state. No battery id, no serial."""
    if not text:
        return {"source": None, "percent": None, "state": None}
    src = None
    if "'AC Power'" in text:
        src = "ac"
    elif "'Battery Power'" in text:
        src = "battery"
    pct = state = None
    for line in text.splitlines():
        if "InternalBattery" in line:
            m = re.search(r"(\d+)%;\s*([^;]+)", line)
            if m:
                pct, state = int(m.group(1)), m.group(2).strip()
    return {"source": src, "percent": pct, "state": state}


def parse_pmset_therm(text):
    """Thermal and performance warning flags and the CPU speed limit (100 = no throttling)."""
    if not text:
        return {"thermal_warning": None, "performance_warning": None, "cpu_speed_limit": None}
    out = {"thermal_warning": None, "performance_warning": None, "cpu_speed_limit": None}
    for line in text.splitlines():
        low = line.lower()
        if "thermal warning level" in low:
            out["thermal_warning"] = "no thermal warning" not in low
        if "performance warning level" in low:
            out["performance_warning"] = "no performance warning" not in low
        m = re.search(r"CPU_Speed_Limit\s*=\s*(\d+)", line)
        if m:
            out["cpu_speed_limit"] = int(m.group(1))
    return out


def parse_ps(text):
    """ps -axo pid=,ppid=,rss=,comm= -> {pid: (ppid, rss_kb)}"""
    out = {}
    for line in (text or "").splitlines():
        p = line.split(None, 3)
        if len(p) >= 3 and all(x.isdigit() for x in p[:3]):
            out[int(p[0])] = (int(p[1]), int(p[2]))
    return out


def tree_rss_mb(table, root):
    """RSS in MB of a process and all its descendants."""
    if root not in table:
        return None
    kids = {}
    for pid, (ppid, _) in table.items():
        kids.setdefault(ppid, []).append(pid)
    total, stack, seen = 0, [root], set()
    while stack:
        p = stack.pop()
        if p in seen:
            continue
        seen.add(p)
        total += table[p][1]
        stack.extend(kids.get(p, []))
    return round(total / 1024, 1)


def external_connections(lsof_text):
    """Number of sockets whose remote end is not this machine. Addresses are never kept."""
    n = 0
    for line in (lsof_text or "").splitlines():
        m = re.search(r"->(\[?[^\]\s]+\]?):\d+", line)
        if not m:
            continue
        host = m.group(1).strip("[]").lower()
        if host == "localhost" or host.startswith("127.") or host == "::1":
            continue
        n += 1
    return n


def default_route_present():
    out = sh(["route", "-n", "get", "default"], 5)
    return out is not None and ("gateway" in out or "interface" in out)


def read_system():
    vm = parse_vm_stat(sh(["vm_stat"], 5)) or {}
    return {"vm": vm, "swap_used_mb": parse_swapusage(sh(["sysctl", "-n", "vm.swapusage"], 5)
                                                      or sh(["sysctl", "vm.swapusage"], 5)),
            "pressure_level": _int(sh(["sysctl", "-n", "kern.memorystatus_vm_pressure_level"], 5))}


def _int(s):
    try:
        return int((s or "").strip())
    except ValueError:
        return None


def omlx_pid(base_url):
    """PID listening on the port of a server on this machine (none for another host)."""
    if not base_url:
        return None
    try:
        u = urlsplit(base_url)
        host, port = (u.hostname or ""), u.port or (443 if u.scheme == "https" else 80)
    except ValueError:
        return None
    if host not in ("localhost", "127.0.0.1", "::1"):
        return None
    out = sh(["lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN", "-t"], 10)
    return int(out.split()[0]) if out and out.split() and out.split()[0].isdigit() else None


def machine_info():
    g = lambda k: (sh(["sysctl", "-n", k], 5) or "").strip() or None  # noqa: E731
    mem = g("hw.memsize")
    from importlib import metadata

    def ver(n):
        try:
            return metadata.version(n)
        except metadata.PackageNotFoundError:
            return None
    commit = (sh(["git", "-C", str(ROOT), "rev-parse", "--short", "HEAD"], 5) or "").strip() or None
    dirty = bool((sh(["git", "-C", str(ROOT), "status", "--porcelain", "--untracked-files=no"], 10) or "").strip())
    batt = parse_pmset_batt(sh(["pmset", "-g", "batt"], 5))
    return {"chip": g("machdep.cpu.brand_string"), "cpu_total": _int(g("hw.ncpu")),
            "cpu_performance": _int(g("hw.perflevel0.physicalcpu")), "cpu_efficiency": _int(g("hw.perflevel1.physicalcpu")),
            "memory_gb": round(int(mem) / 2 ** 30, 1) if mem and mem.isdigit() else None,
            "macos": (sh(["sw_vers", "-productVersion"], 5) or "").strip() or None,
            "power_source": batt["source"], "battery_percent": batt["percent"],
            "python": sys.version.split()[0], "mlx_lm": ver("mlx-lm"), "mlx": ver("mlx"),
            "sentence_transformers": ver("sentence-transformers"), "torch": ver("torch"),
            "runtime_commit": commit, "runtime_tree_dirty": dirty}


def power_snapshot(when, i=None):
    b = parse_pmset_batt(sh(["pmset", "-g", "batt"], 5))
    t = parse_pmset_therm(sh(["pmset", "-g", "therm"], 5))
    return {"when": when, "i": i, "ts": now(), **b, **t}


# ------------------------------------------------------------------ memory sampler

class Sampler(threading.Thread):
    """Background sampling while questions run: RSS of each registered process tree (ps), the system's used memory
    and swap (vm_stat, sysctl), the pressure level, every few ticks the free percentage (memory_pressure) and the
    number of non-local sockets of the registered processes (lsof). The peak is the largest sample (interval
    default 2 s): a shorter peak can be missed."""

    def __init__(self, path, pids_fn, interval=2.0, label=""):
        super().__init__(daemon=True)
        self.path, self.pids_fn, self.interval, self.label = path, pids_fn, interval, label
        self._stop_ev = threading.Event()
        self.n, self.peak_rss, self.peak_total, self.peak_used = 0, {}, 0.0, 0.0
        self.max_swap, self.max_pressure, self.external, self.min_free_pct = None, None, 0, None

    def tick(self):
        pids = {k: v for k, v in (self.pids_fn() or {}).items() if v}
        table = parse_ps(sh(["ps", "-axo", "pid=,ppid=,rss=,comm="], 10))
        rss = {k: tree_rss_mb(table, p) for k, p in pids.items()}
        rss = {k: v for k, v in rss.items() if v is not None}
        total = round(sum(rss.values()), 1)
        s = read_system()
        rec = {"ts": round(now(), 2), "label": self.label, "rss_mb": rss, "total_rss_mb": total,
               "used_gb": (s["vm"] or {}).get("used_gb"), "swap_used_mb": s["swap_used_mb"],
               "swapouts": (s["vm"] or {}).get("swapouts"), "pressure_level": s["pressure_level"]}
        if self.n % 5 == 0:
            rec["free_pct"] = parse_free_pct(sh(["memory_pressure"], 15))
            if pids:
                ext = external_connections(sh(["lsof", "-nP", "-i", "-a", "-p", ",".join(str(p) for p in pids.values())], 15))
                rec["external_sockets"] = ext
                self.external += ext
            if rec["free_pct"] is not None:
                self.min_free_pct = rec["free_pct"] if self.min_free_pct is None else min(self.min_free_pct, rec["free_pct"])
        for k, v in rss.items():
            self.peak_rss[k] = max(self.peak_rss.get(k, 0), v)
        self.peak_total = max(self.peak_total, total)
        if rec["used_gb"] is not None:
            self.peak_used = max(self.peak_used, rec["used_gb"])
        if rec["swap_used_mb"] is not None:
            self.max_swap = rec["swap_used_mb"] if self.max_swap is None else max(self.max_swap, rec["swap_used_mb"])
        if rec["pressure_level"] is not None:
            self.max_pressure = rec["pressure_level"] if self.max_pressure is None else max(self.max_pressure, rec["pressure_level"])
        self.n += 1
        append_jsonl(self.path, rec)

    def run(self):
        while not self._stop_ev.is_set():
            try:
                self.tick()
            except Exception as e:  # sampling must never stop a run
                append_jsonl(self.path, {"ts": round(now(), 2), "sampler_error": type(e).__name__})
            self._stop_ev.wait(self.interval)

    def stop(self):
        self._stop_ev.set()
        self.join(timeout=30)
        return {"samples": self.n, "peak_rss_mb": self.peak_rss, "peak_total_rss_mb": self.peak_total,
                "peak_used_gb": self.peak_used or None, "max_swap_used_mb": self.max_swap,
                "max_pressure_level": self.max_pressure, "min_free_pct": self.min_free_pct,
                "external_sockets": self.external}


# ------------------------------------------------------------------ runners

class Runner:
    """start() -> info dict; ask(question, mode, check) -> row dict; pids() -> {label: pid}; stop()."""
    kind = "?"

    def start(self):
        return {}

    def pids(self):
        return {}

    def stop(self):
        pass


class StubRunner(Runner):
    def __init__(self, kind, **_):
        self.kind = kind

    def start(self):
        return {"startup_s": 0.01 if self.kind == "service" else None}

    def pids(self):
        return {"stub": os.getpid()}

    def ask(self, question, mode, check, iid=""):
        h = int(hashlib.sha256((iid + mode + str(check)).encode()).hexdigest()[:6], 16)
        base = 30 + (h % 100) / 10 + (8 if mode == "default" else 0) + (1 if check else 0)
        search, retr = 3.0, 4.5
        ans = base - search - retr
        toks = 600 + h % 200
        return {"wall_s": round(base + (15 if self.kind == "oneshot" else 0), 2),
                "ttft_s": round(search + retr + 1.5, 2) if self.kind == "service" else None,
                "search_s": search, "retrieve_s": retr, "answer_s": round(ans, 2), "tokens": toks,
                "tok_per_s": round(toks / ans, 2), "truncated": False, "chars": 2000 + h % 500, "error": None}


class RealRunner(Runner):
    def __init__(self, kind, cfg, out_dir, tag):
        self.kind, self.cfg, self.out, self.tag = kind, cfg, Path(out_dir), tag
        self.proc = None
        self.child = None
        self.base = os.environ.get("OMLX_URL")
        self.omlx = None
        self.logf = None

    def model_args(self):
        c = self.cfg
        a = ["--backend", c["backend"]]
        if c.get("model"):
            a += ["--model", c["model"]]
        if c.get("extra_body"):
            a += ["--extra-body", c["extra_body"]]
        if c.get("max_tokens"):
            a += ["--max-tokens", str(c["max_tokens"])]
        a += ["--k", c["k"]]
        return a

    def pids(self):
        d = {}
        if self.omlx:
            d["omlx"] = self.omlx
        if self.proc and self.proc.poll() is None:
            d["service"] = self.proc.pid
        if self.child:
            d["ask"] = self.child
        return d

    def start(self):
        if self.cfg["backend"] == "openai":
            self.omlx = omlx_pid(self.base)
        if self.kind != "service":
            return {"startup_s": None, "omlx_pid_found": bool(self.omlx)}
        port = int(self.cfg["port"])
        self.out.mkdir(parents=True, exist_ok=True)
        self.logf = open(self.out / f"service_{self.tag}.log", "w", encoding="utf-8")
        cmd = [py(), "-m", "kafka_sre_rag", "serve", "--port", str(port), "--catalogues", self.cfg["catalogues"]] \
            + self.model_args()
        t0 = time.monotonic()
        self.proc = subprocess.Popen(cmd, cwd=ROOT, env=child_env(), stdout=self.logf, stderr=subprocess.STDOUT)
        import httpx
        url = f"http://127.0.0.1:{port}/healthz"
        while time.monotonic() - t0 < 1800:
            if self.proc.poll() is not None:
                raise RuntimeError(f"the service exited with code {self.proc.returncode} before it was ready; "
                                   f"see {self.logf.name}")
            try:
                if httpx.get(url, timeout=3, trust_env=False).status_code == 200:
                    return {"startup_s": round(time.monotonic() - t0, 2), "omlx_pid_found": bool(self.omlx)}
            except httpx.HTTPError:
                pass
            time.sleep(1)
        raise RuntimeError("the service was not ready after 30 minutes")

    def ask(self, question, mode, check, iid=""):
        return self._ask_service(question, mode, check) if self.kind == "service" else self._ask_oneshot(question, mode, check)

    def _ask_service(self, question, mode, check):
        import httpx
        body = {"model": "kafka-sre-strict" if mode == "strict" else "kafka-sre", "k": self.cfg["k"], "stream": True,
                "messages": [{"role": "user", "content": question}]}
        if check:
            body["check"] = True
        url = f"http://127.0.0.1:{int(self.cfg['port'])}/v1/chat/completions"
        t0, ttft, text, extra, err = time.monotonic(), None, "", None, None
        try:
            with httpx.Client(timeout=1800, trust_env=False) as c, c.stream("POST", url, json=body) as r:
                if r.status_code >= 400:
                    r.read()
                    err = f"HTTP {r.status_code}"
                else:
                    for line in r.iter_lines():
                        if not line.startswith("data:") or line[5:].strip() == "[DONE]":
                            continue
                        d = json.loads(line[5:])
                        if "error" in d:
                            err = str(d["error"].get("message", "error"))[:200]
                            continue
                        for ch in d.get("choices") or []:
                            piece = (ch.get("delta") or {}).get("content") or ""
                            if piece and ttft is None:
                                ttft = time.monotonic() - t0
                            text += piece
                        extra = d.get("kafka_sre") or extra
        except httpx.HTTPError as e:
            err = type(e).__name__
        return self._row(time.monotonic() - t0, ttft, extra, len(text), err)

    def _ask_oneshot(self, question, mode, check):
        cmd = [py(), "-m", "kafka_sre_rag", "ask", "--json", "--mode", mode, "--catalogues", self.cfg["catalogues"]]
        cmd += (["--check"] if check else []) + self.model_args() + [question]
        t0 = time.monotonic()
        p = subprocess.Popen(cmd, cwd=ROOT, env=child_env(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.child = p.pid
        out, errtxt = p.communicate()
        self.child = None
        wall = time.monotonic() - t0
        extra, err = None, None
        try:
            extra = json.loads(out.strip().splitlines()[-1])
        except (ValueError, IndexError):
            err = ("exit " + str(p.returncode) + ": " + " ".join(errtxt.split())[-200:]) if p.returncode else "no JSON"
        return self._row(wall, None, extra, len((extra or {}).get("text", "")), err)

    @staticmethod
    def _row(wall, ttft, extra, chars, err):
        t = (extra or {}).get("timings") or {}
        toks = (extra or {}).get("tokens")
        a = t.get("answer_s")
        return {"wall_s": round(wall, 2), "ttft_s": None if ttft is None else round(ttft, 2),
                "search_s": _r(t.get("expand_s")), "retrieve_s": _r(t.get("retrieve_s")), "answer_s": _r(a),
                "tokens": toks, "tok_per_s": round(toks / a, 2) if toks and a else None,
                "truncated": bool((extra or {}).get("truncated")), "chars": chars, "error": err}

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=60)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        if self.logf:
            self.logf.close()


def _r(x):
    return None if x is None else round(x, 2)


def make_runner(kind, cfg, out_dir, tag):
    return StubRunner(kind) if cfg.get("stub") else RealRunner(kind, cfg, out_dir, tag)


# ------------------------------------------------------------------ config and ids

def load_ids():
    return json.loads(IDS_FILE.read_text(encoding="utf-8"))


def load_items(rel):
    return {r["id"]: r for r in read_jsonl(ROOT / rel)}


def full_question(item):
    q = item["question"]
    return q + ("\n\nEvidence:\n" + item["evidence"] if item.get("evidence") else "")


def resolve(a):
    """Command line over the stored config.json over the defaults. Stores the result for the later phases."""
    out = Path(getattr(a, "out", None)) if getattr(a, "out", None) else ROOT / "out/laptop" / (getattr(a, "date", None) or datetime.date.today().isoformat())
    if not out.is_absolute():
        out = ROOT / out
    stored = read_json(out / "config.json", {})
    cfg = dict(DEFAULTS)
    cfg.update(stored)
    for k in ("backend", "model", "extra_body", "k", "max_tokens", "catalogues", "weights_dir", "model_repo", "port", "interval"):
        v = getattr(a, k, None)
        if v is not None:
            cfg[k] = v
    cfg["stub"] = bool(getattr(a, "stub", False) or stored.get("stub"))
    if cfg["stub"]:
        cfg["interval"] = min(float(cfg["interval"]), 0.05)
    if cfg["backend"] == "mlx":
        cfg["model"] = cfg.get("model") or os.environ.get("BASE_PATH")
        cfg["extra_body"] = None
    cfg["omlx_url_set"] = bool(os.environ.get("OMLX_URL"))
    cfg["omlx_url_local"] = bool(os.environ.get("OMLX_URL")) and urlsplit(os.environ["OMLX_URL"]).hostname in (
        "localhost", "127.0.0.1", "::1")
    persist = {k: v for k, v in cfg.items() if k not in ("omlx_url_set", "omlx_url_local")}
    out.mkdir(parents=True, exist_ok=True)
    write_json(out / "config.json", persist)
    return out, cfg


def log(phase, msg):
    print(f"[{phase}] {msg}", flush=True)


# ------------------------------------------------------------------ preflight

def cmd_preflight(a):
    out, cfg = resolve(a)
    checks = []

    def add(name, ok, detail="", required=True):
        checks.append({"name": name, "ok": bool(ok), "detail": detail, "required": required})
        log("preflight", f"{'present' if ok else 'MISSING'} {name}" + (f"  {detail}" if detail else ""))
    mi = machine_info()
    add("Apple Silicon Mac", sys.platform == "darwin" and (mi["chip"] or "").startswith("Apple"), f"{mi['chip']}", required=not cfg["stub"])
    add("memory of at least 32 GB", (mi["memory_gb"] or 0) >= 31, f"{mi['memory_gb']} GB", required=False)
    add("python >= 3.10", sys.version_info >= (3, 10), mi["python"])
    add(".venv of the repository", VENV_PY.exists(), str(VENV_PY.relative_to(ROOT)))
    from importlib import util
    for mod in ("httpx", "yaml", "sentence_transformers", "torch") + (("mlx_lm",) if cfg["backend"] == "mlx" else ()):
        add(f"python module {mod}", util.find_spec(mod) is not None, "", required=not cfg["stub"])
    fr = read_json(ROOT / "kafka_sre_rag/frozen.json", {})
    idx_rel = (fr.get("index") or {}).get("index_path", "work/v3/chunks_s1.jsonl")
    idx = ROOT / idx_rel
    ok_hash = idx.exists() and sha256_file(idx) == (fr.get("index") or {}).get("sha256")
    add("frozen index with the right hash", ok_hash, idx_rel + ("" if idx.exists() else " (absent)"))
    rev = ((fr.get("retrieval") or {}).get("reranker") or {}).get("revision")
    home = Path(os.environ.get("HF_HOME", Path.home() / ".cache/huggingface"))
    rr = Path(os.environ.get("HF_HUB_CACHE", home / "hub")) / "models--BAAI--bge-reranker-v2-m3/snapshots" / str(rev)
    add("reranker in the local cache at the frozen revision", rr.is_dir(), str(rev)[:12])
    cat = ROOT / cfg["catalogues"] / "CATALOGUES.json"
    add("catalogues (for --check and score)", cat.exists(), cfg["catalogues"], required=False)
    ids_ok = IDS_FILE.exists()
    add("id lists eval/m4/laptop/ids.json", ids_ok)
    if ids_ok:
        ids = load_ids()
        for part in ("speed", "score"):
            items = load_items(ids[part]["file"])
            miss = [i for i in ids[part]["ids"] if i not in items]
            add(f"{part} ids exist in {ids[part]['file']}", not miss, f"{len(ids[part]['ids'])} ids, {len(miss)} missing")
    add("OMLX_URL set" if cfg["backend"] == "openai" else "BASE_PATH or --model set",
        cfg["omlx_url_set"] if cfg["backend"] == "openai" else bool(cfg.get("model")), "value not shown",
        required=not cfg["stub"])
    if cfg["backend"] == "openai":
        add("server is on this machine (the bar needs that)", cfg["omlx_url_local"], "", required=False)
        add("--model given", bool(cfg.get("model")), "", required=not cfg["stub"])
    wd = cfg.get("weights_dir")
    add("weights directory given and present", bool(wd) and Path(wd).expanduser().is_dir(),
        f"{round(dir_size_bytes(Path(wd).expanduser()) / 1e9, 2)} GB" if wd and Path(wd).expanduser().is_dir() else "give --weights-dir",
        required=False)
    for tool in ("pmset", "vm_stat", "memory_pressure", "sysctl", "lsof", "ps", "route"):
        add(f"tool {tool}", shutil.which(tool) is not None, required=False)
    du = shutil.disk_usage(ROOT)
    add("free disk >= 40 GB", du.free >= 40e9, f"{du.free / 1e9:.0f} GB", required=False)
    add("default route present now (Wi-Fi on: needed only for the install)", default_route_present(), "info", required=False)
    miss = [c["name"] for c in checks if c["required"] and not c["ok"]]
    write_json(out / "preflight.json", {"machine": mi, "checks": checks, "missing_required": miss, "stub": cfg["stub"]})
    log("preflight", f"{len(checks) - len([c for c in checks if not c['ok']])} of {len(checks)} present; "
                     f"required and missing: {len(miss)}" + (": " + "; ".join(miss) if miss else ""))
    return 1 if (miss and a.strict) else 0


# ------------------------------------------------------------------ install-check

def install_steps(cfg):
    """The documented sequence (agents/laptop-tests.md, step 3). kind 'download' is reported separately."""
    fr = read_json(ROOT / "kafka_sre_rag/frozen.json", {})
    rev = ((fr.get("retrieval") or {}).get("reranker") or {}).get("revision", "")
    wd = cfg.get("weights_dir") or "models/Qwen3.8-27B-4bit"
    return [
        {"name": "venv", "kind": "install", "cmd": ["python3", "-m", "venv", ".venv"], "skip_if": VENV_PY.exists()},
        {"name": "pip", "kind": "install", "cmd": [".venv/bin/pip", "install", "-r", "requirements.txt"]},
        {"name": "index", "kind": "download", "cmd": ["sh", "-c",
            ".venv/bin/hf download aywengo/kafka-sre-docs-index-v3 --repo-type dataset --local-dir idx && (cd idx && shasum -a 256 -c SHA256SUMS)"
            " && mkdir -p work/v3 && cp idx/chunks.jsonl work/v3/chunks_s1.jsonl"], "bytes_of": "idx"},
        {"name": "reranker", "kind": "download", "cmd": [".venv/bin/hf", "download", "BAAI/bge-reranker-v2-m3", "--revision", rev]},
        {"name": "model", "kind": "download", "cmd": [".venv/bin/hf", "download", cfg["model_repo"], "--local-dir", wd], "bytes_of": wd},
        {"name": "verify", "kind": "install", "cmd": [".venv/bin/python", "-m", "kafka_sre_rag", "verify"]},
    ]


def cmd_install(a):
    out, cfg = resolve(a)
    steps_file = out / "install/steps.jsonl"
    done = {r["name"]: r for r in read_jsonl(steps_file) if r.get("rc") == 0}
    if a.run:
        for st in install_steps(cfg):
            if st["name"] in done:
                log("install", f"{st['name']} already done ({done[st['name']]['seconds']} s), skipped")
                continue
            if st.get("skip_if") and not cfg["stub"]:
                row = {"name": st["name"], "kind": st["kind"], "seconds": 0.0, "rc": 0, "skipped": "already present", "bytes": None}
            else:
                cmd = ["true"] if cfg["stub"] else st["cmd"]
                t0 = time.monotonic()
                r = subprocess.run(cmd, cwd=ROOT, env=dict(os.environ), capture_output=True, text=True)
                sec = round(time.monotonic() - t0, 1)
                size = None
                if st.get("bytes_of") and not cfg["stub"] and (ROOT / st["bytes_of"]).exists():
                    size = dir_size_bytes(ROOT / st["bytes_of"])
                row = {"name": st["name"], "kind": st["kind"], "seconds": sec, "rc": r.returncode, "bytes": size,
                       "tail": " ".join((r.stderr or r.stdout or "").split())[-300:] if r.returncode else ""}
            append_jsonl(steps_file, row)
            log("install", f"{st['name']} {row['seconds']} s rc={row['rc']} kind={st['kind']}"
                           + (f" {row['bytes'] / 1e9:.2f} GB" if row.get("bytes") else ""))
            if row["rc"] != 0:
                log("install", "FAILED; the sequence stops here: " + row.get("tail", ""))
                return 1
    rows = read_jsonl(steps_file)
    last = {r["name"]: r for r in rows if r.get("rc") == 0}
    install_s = round(sum(r["seconds"] for r in last.values() if r["kind"] == "install"), 1)
    dl_s = round(sum(r["seconds"] for r in last.values() if r["kind"] == "download"), 1)
    wd = cfg.get("weights_dir")
    weights = dir_size_bytes(Path(wd).expanduser()) if wd and Path(wd).expanduser().is_dir() and not cfg["stub"] else (
        last.get("model", {}).get("bytes"))
    ver = sh([py(), "-m", "kafka_sre_rag", "verify"], 120) if not cfg["stub"] else "stub"
    res = {"steps_run": len(last), "install_seconds": install_s, "download_seconds": dl_s,
           "download_bytes": {n: r.get("bytes") for n, r in last.items() if r["kind"] == "download"},
           "weights_bytes": weights, "verify_ok": ver is not None, "complete": len(last) == len(install_steps(cfg)),
           "stub": cfg["stub"]}
    write_json(out / "install_check.json", res)
    log("install", f"install {install_s} s (bar {BARS['install_s']} s), downloads {dl_s} s apart; "
                   f"weights {fmt((weights or 0) / 1e9, 2) if weights else '-'} GB; verify {'ok' if ver is not None else 'FAILED'}")
    return 0 if ver is not None else 1


# ------------------------------------------------------------------ speed

def questions_for(ids_part, n):
    ids = load_ids()
    items = load_items(ids[ids_part]["file"])
    return [(i, full_question(items[i])) for i in ids[ids_part]["ids"][:n]]


def run_condition(out, cfg, phase, label, cond, qs, extra_row=None, power_every=0, session=1):
    """One condition of a series: start the runner, sample memory, ask the questions in order, resume by row key."""
    kind, rest = cond.split("-", 1)
    mode, check = ("strict" if rest.startswith("strict") else "default"), rest.endswith("check")
    rows_file = out / phase / label / "rows.jsonl"
    done = {(r["cond"], r["id"]) for r in read_jsonl(rows_file) if not r.get("error")}
    todo = [(i, q) for i, q in qs if (cond, i) not in done]
    if not todo:
        log(phase, f"{label} {cond}: all {len(qs)} done, skipped")
        return
    runner = make_runner(kind, cfg, out / phase / label, cond)
    info = runner.start()
    sampler = Sampler(out / phase / label / f"samples_{cond}.jsonl", runner.pids, float(cfg["interval"]), f"{label}/{cond}")
    net = {"default_route": default_route_present() if not cfg["stub"] else False}
    sampler.start()
    power_file = out / phase / label / "power.jsonl"
    if power_every:
        append_jsonl(power_file, {**power_snapshot("before"), "cond": cond})
    try:
        for n, (iid, q) in enumerate(todo, 1):
            row = runner.ask(q, mode, check, iid=iid)
            row.update({"cond": cond, "id": iid, "label": label, "i": len(qs) - len(todo) + n, "session": session,
                        "cold": (n == 1 and kind == "service"), "ts": round(now(), 1), **(extra_row or {})})
            if n == 1 and info.get("startup_s") is not None:
                row["startup_s"] = info["startup_s"]
            append_jsonl(rows_file, row)
            log(phase, f"{label} {cond} {row['i']}/{len(qs)} id={iid} wall={fmt(row['wall_s'])}s ttft={fmt(row['ttft_s'])}s "
                       f"tokens={row['tokens']} {'FAILED ' + str(row['error']) if row['error'] else 'ok'}")
            if power_every and row["i"] % power_every == 0:
                append_jsonl(power_file, {**power_snapshot("during", row["i"]), "cond": cond})
    finally:
        if power_every:
            append_jsonl(power_file, {**power_snapshot("after"), "cond": cond})
        stats = sampler.stop()
        runner.stop()
        append_jsonl(out / phase / label / "memory_summary.jsonl", {"cond": cond, **stats, **net, **info})


def cmd_speed(a):
    out, cfg = resolve(a)
    label = a.label or "baseline"
    conds = [c.strip() for c in a.conds.split(",")] if a.conds else list(CONDS)
    bad = [c for c in conds if c not in CONDS]
    if bad:
        log("speed", f"unknown condition {bad}; choose from {CONDS}")
        return 2
    extra = {}
    if a.apps:
        write_json(out / "speed" / label / "apps_note.json", {"apps_open": a.apps, "used_gb_at_start": (read_system()["vm"] or {}).get("used_gb")})
    for cond in conds:
        n = a.n_service if cond.startswith("service") else a.n_oneshot
        run_condition(out, cfg, "speed", label, cond, questions_for("speed", n), extra)
    probe(out, cfg, a.n_service)
    return 0


def probe(out, cfg, n):
    """Retrieval alone (BM25, then the cross-encoder on the 40 candidates), no model: cold and warm."""
    f = out / "speed" / "retrieval_probe.json"
    if f.exists():
        return
    qs = questions_for("speed", n)
    if cfg["stub"]:
        write_json(f, {"stub": True, "index_load_s": 1.0, "reranker_load_s": 3.0, "bm25_s_median": 0.05,
                       "rerank_s_median": 3.5, "n": len(qs), "peak_rss_mb": 2500.0})
        return
    qf = out / "speed" / "probe_questions.json"
    write_json(qf, [q for _, q in qs])
    r = subprocess.run([py(), str(Path(__file__).resolve()), "_probe", str(qf), str(f)], cwd=ROOT, env=child_env(),
                       capture_output=True, text=True)
    log("speed", f"retrieval probe rc={r.returncode}" + (": " + " ".join(r.stderr.split())[-200:] if r.returncode else ""))


def probe_main(qfile, outfile):
    import resource
    sys.path.insert(0, str(ROOT))
    from kafka_sre_rag import config, retrieval
    RT = retrieval.RT
    cfg = config.load()
    path = str(config.index_file(cfg))
    t = time.monotonic()
    chunks = RT.index(path)[0]
    t_idx = time.monotonic() - t
    t = time.monotonic()
    model = RT.reranker()
    t_rr = time.monotonic() - t
    bm, rk = [], []
    for q in json.loads(Path(qfile).read_text()):
        t = time.monotonic()
        cand = [i for i, _ in RT.scores(q, path).most_common(cfg["retrieval"]["pool"])]
        bm.append(time.monotonic() - t)
        t = time.monotonic()
        model.predict([(q, (chunks[i]["title"] + "\n" + chunks[i]["text"])[:1500]) for i in cand], batch_size=8)
        rk.append(time.monotonic() - t)
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024 * 1024)  # bytes on macOS
    write_json(outfile, {"index_load_s": round(t_idx, 2), "reranker_load_s": round(t_rr, 2), "bm25_s_median": round(statistics.median(bm), 3),
                         "rerank_s_median": round(statistics.median(rk), 2), "rerank_s_first": round(rk[0], 2), "n": len(rk),
                         "peak_rss_mb": round(peak, 0)})


# ------------------------------------------------------------------ sustained

def cmd_sustained(a):
    out, cfg = resolve(a)
    label = a.label
    detected = parse_pmset_batt(sh(["pmset", "-g", "batt"], 5))["source"]
    if not cfg["stub"] and detected and detected != label and not a.force:
        log("sustained", f"label {label} but the Mac is on {detected}; fix the power or pass --force")
        return 2
    qs = questions_for("speed", a.n)
    cond = f"{a.runtime}-default"
    prior = read_jsonl(out / "sustained" / label / "rows.jsonl")
    session = 1 + max(r.get("session", 1) for r in prior) if prior and len(prior) < len(qs) else 1
    run_condition(out, cfg, "sustained", label, cond, qs, {"power_label": label}, power_every=5, session=session)
    return 0


# ------------------------------------------------------------------ memory

def cmd_memory(a):
    out, cfg = resolve(a)
    d = out / "memory"
    f = d / "memory_phase.json"
    if f.exists() and not a.force:
        log("memory", "already done (memory/memory_phase.json); skipped")
        return 0
    base_pids = {}
    if cfg["backend"] == "openai" and not cfg["stub"]:
        p = omlx_pid(os.environ.get("OMLX_URL"))
        if p:
            base_pids["omlx"] = p
    s0 = Sampler(d / "samples_baseline.jsonl", lambda: base_pids or ({"stub": os.getpid()} if cfg["stub"] else {}),
                 float(cfg["interval"]), "baseline")
    s0.start()
    time.sleep(2 if cfg["stub"] else a.baseline_s)
    base = s0.stop()
    log("memory", f"baseline {base['samples']} samples, used {fmt(base['peak_used_gb'], 1)} GB, oMLX rss peak {base['peak_rss_mb']}")
    qs = questions_for("speed", 3)
    run_condition(out, cfg, "memory", "loaded", "service-default", qs)
    status = None
    if cfg["backend"] == "openai" and not cfg["stub"] and os.environ.get("OMLX_URL"):
        status = server_status(os.environ["OMLX_URL"])
    write_json(f, {"baseline": base, "server_status": status, "weights_bytes": (read_json(out / "install_check.json", {}) or {}).get("weights_bytes")})
    return 0


def server_status(base):
    """Best effort: the memory fields of the model server's own status page. The address and the key stay out."""
    try:
        import httpx
        u = urlsplit(base)
        key = os.environ.get("OMLX_API_KEY")
        r = httpx.get(f"{u.scheme}://{u.netloc}/api/status", timeout=5, trust_env=False,
                      headers={"Authorization": f"Bearer {key}"} if key else {})
        d = r.json()
        return {k: v for k, v in d.items() if "memory" in k.lower() and isinstance(v, (int, float, str))}
    except Exception:
        return None


# ------------------------------------------------------------------ score

def cmd_score(a):
    out, cfg = resolve(a)
    ids = load_ids()
    items = load_items(ids["score"]["file"])
    sub = out / "score"
    sub.mkdir(parents=True, exist_ok=True)
    items_f = sub / "items_subset.jsonl"
    items_f.write_text("".join(json.dumps(items[i], ensure_ascii=False) + "\n" for i in ids["score"]["ids"]), encoding="utf-8")
    ans, scores, summ = sub / "answers.jsonl", sub / "scores.jsonl", sub / "summary.json"
    pids = {}
    if cfg["backend"] == "openai" and not cfg["stub"]:
        p = omlx_pid(os.environ.get("OMLX_URL"))
        pids = {"omlx": p} if p else {}
    sampler = Sampler(sub / "samples.jsonl", lambda: dict(pids), float(cfg["interval"]), "score")
    sampler.start()
    try:
        if cfg["stub"]:
            have = {r["id"] for r in read_jsonl(ans)}
            for i in ids["score"]["ids"]:
                if i in have:
                    log("score", f"{i} already done, skipped")
                    continue
                h = int(hashlib.sha256(i.encode()).hexdigest()[:4], 16)
                append_jsonl(ans, {"id": i, "text": "stub", "seconds": {"total": 30.0 + h % 10}, "tokens": 500, "error": None})
                append_jsonl(scores, {"id": i, "all_pass": h % 4 != 0, "checks_passed": 1, "checks_total": 1})
                log("score", f"{i} stub")
        else:
            cmd = [py(), "scripts/eval2.py", "answer", "--items", str(items_f), "--out", str(ans), "--config", "laptop",
                   "--backend", cfg["backend"], "--host-class", "laptop", "--k", cfg["k"]]
            if cfg.get("model"):
                cmd += ["--model", cfg["model"]]
            if cfg.get("extra_body") and cfg["backend"] == "openai":
                cmd += ["--extra-body", cfg["extra_body"]]
            proc = subprocess.Popen(cmd, cwd=ROOT, env=child_env())  # one progress line per item goes to the terminal
            pids["eval2"] = proc.pid
            rc = proc.wait()
            pids.pop("eval2", None)
            if rc != 0:
                log("score", f"answers stopped with code {rc}; run the command again to resume")
                return 1
            r = subprocess.run([py(), "scripts/eval2_score.py", "--items", str(items_f), "--answers", str(ans), "--scores", str(scores),
                                "--summary", str(summ), "--catalogues", cfg["catalogues"]], cwd=ROOT, env=child_env())
            if r.returncode != 0:
                log("score", f"scorer stopped with code {r.returncode} (catalogues present at {cfg['catalogues']}?)")
                return 1
    finally:
        stats = sampler.stop()
    write_json(sub / "memory_summary.json", stats)
    cmp = compare_scores(ids, read_jsonl(scores), read_jsonl(ans))
    cmp["stub"] = cfg["stub"]
    write_json(sub / "compare.json", cmp)
    log("score", f"laptop {cmp['laptop_pass']}/{cmp['n']} pass, reference {cmp['reference_pass']}/{cmp['n']}; "
                 f"discordant {cmp['discordant']}; identical answers {cmp['identical_text']}/{cmp['n']}")
    return 0


def compare_scores(ids, score_rows, answer_rows):
    ref = ids["score"]["reference"]
    got = {r["id"]: bool(r.get("all_pass")) for r in score_rows}
    txt = {r["id"]: hashlib.sha256((r.get("text") or "").encode()).hexdigest() for r in answer_rows if not r.get("error")}
    common = [i for i in ids["score"]["ids"] if i in got]
    lp = sum(got[i] for i in common)
    rp = sum(bool(ref["pass"][i]) for i in common)
    disc = [i for i in common if got[i] != bool(ref["pass"][i])]
    ident = sum(1 for i in common if txt.get(i) == ref["answer_sha256"].get(i))
    return {"n": len(common), "n_expected": len(ids["score"]["ids"]), "laptop_pass": lp, "reference_pass": rp,
            "discordant": len(disc), "laptop_only": sum(1 for i in disc if got[i]), "reference_only": sum(1 for i in disc if not got[i]),
            "identical_text": ident, "reference_run": ref.get("run"), "discordant_ids": disc}


# ------------------------------------------------------------------ summary

def collect(out):
    """Everything the summary needs, read from the folder."""
    out = Path(out)
    rows = []
    for f in sorted((out / "speed").glob("*/rows.jsonl")):
        rows += read_jsonl(f)
    sust = {}
    for f in sorted((out / "sustained").glob("*/rows.jsonl")):
        sust[f.parent.name] = read_jsonl(f)
    mems = []
    for f in sorted(out.glob("*/*/memory_summary.jsonl")):
        mems += [{**r, "_file": f.parent.parent.name + "/" + f.parent.name} for r in read_jsonl(f)]
    for f in (out / "score/memory_summary.json", ):
        if f.exists():
            mems.append({**read_json(f), "cond": "score", "_file": "score"})
    return {"rows": rows, "sustained": sust, "cold": read_jsonl(out / "memory/loaded/rows.jsonl"), "mems": mems, "install": read_json(out / "install_check.json"),
            "preflight": read_json(out / "preflight.json"), "probe": read_json(out / "speed/retrieval_probe.json"),
            "memory_phase": read_json(out / "memory/memory_phase.json"), "compare": read_json(out / "score/compare.json"),
            "config": read_json(out / "config.json", {})}


def slope(ys):
    n = len(ys)
    if n < 3:
        return None
    xs = list(range(n))
    mx, my = sum(xs) / n, sum(ys) / n
    den = sum((x - mx) ** 2 for x in xs)
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / den if den else None


def bars(d):
    """List of (name, measured, size, bar, verdict). Verdict: pass, FAIL, reported, not run."""
    B = []
    ok_rows = [r for r in d["rows"] if not r.get("error")]

    def cond_rows(c, label="baseline"):
        return [r for r in ok_rows if r["cond"] == c and r.get("label", "baseline") == label]
    ins = d["install"]
    if ins and ins.get("complete"):
        B.append(("Installation (without model download)", f"{ins['install_seconds'] / 60:.1f} min; downloads {ins['download_seconds'] / 60:.1f} min apart",
                  "6 steps", "under 30 min", "pass" if ins["install_seconds"] < BARS["install_s"] else "FAIL"))
    else:
        B.append(("Installation (without model download)", "-", "-", "under 30 min", "not run (install-check --run)"))
    mems = [m for m in d["mems"] if m.get("samples")]
    ext = sum(m.get("external_sockets", 0) for m in mems)
    routes = [m.get("default_route") for m in mems if "default_route" in m]
    errs = [r for r in d["rows"] if r.get("error")]
    if mems and routes:
        off = not any(routes)
        B.append(("Offline", f"non-local sockets seen {ext}; default route absent in {sum(1 for x in routes if not x)} of {len(routes)} runs; "
                             f"errors {len(errs)}", f"{len(mems)} runs",
                  "no network needed after install", ("pass" if ext == 0 and not errs else "FAIL") if off else "reported (Wi-Fi was on: not an offline test)"))
    else:
        B.append(("Offline", "-", "-", "no network needed after install", "not run"))
    if mems:
        peak = max(m["peak_total_rss_mb"] for m in mems) / 1024
        used = max((m.get("peak_used_gb") or 0) for m in mems)
        swap = max((m.get("max_swap_used_mb") or 0) for m in mems)
        pres = max((m.get("max_pressure_level") or 1) for m in mems)
        mp = d["memory_phase"] or {}
        bu = (mp.get("baseline") or {}).get("peak_used_gb")
        delta = f"; system used {used:.1f} GB (baseline {fmt(bu)} GB)" if used else ""
        B.append(("Memory held (sum of resident memory of the assistant's processes)", f"peak {peak:.1f} GB{delta}; swap up to {swap:.0f} MB; "
                  f"pressure level up to {pres} (1 normal, 2 warning, 4 critical)", f"{sum(m['samples'] for m in mems)} samples",
                  "about 20 GB", ("pass" if peak <= BARS["memory_gb"] and pres < 4 else "FAIL")))
    else:
        B.append(("Memory held", "-", "-", "about 20 GB", "not run"))
    wb = (ins or {}).get("weights_bytes")
    if wb:
        g = wb / 1e9
        B.append(("4-bit weights on disk", f"{g:.1f} GB", "1 model", "15 to 17 GB (estimate to verify)",
                  "reported: inside the estimate" if BARS["weights_gb"][0] <= g <= BARS["weights_gb"][1] else "reported: outside the estimate"))
    else:
        B.append(("4-bit weights on disk", "-", "-", "15 to 17 GB", "not run (--weights-dir)"))
    sd = cond_rows("service-default")
    od = cond_rows("oneshot-default")
    if sd or od:
        main = sd or od
        m = med([r["wall_s"] for r in main])
        B.append(("Time to a complete answer, default mode, " + ("service" if sd else "one-shot"), f"median {m:.1f} s (min {min(r['wall_s'] for r in main):.1f}, max {max(r['wall_s'] for r in main):.1f})",
                  f"{len(main)} questions", "median under 60 s", "pass" if m < BARS["median_answer_s"] else "FAIL"))
        if sd:
            B.append(("Time to the first words (service, streamed)", f"median {fmt(med([r['ttft_s'] for r in sd]))} s; first question after launch {fmt(sd[0]['ttft_s'])} s",
                      f"{len(sd)} questions", "reported on the laptop (10 s on the reference machine)", "reported"))
        tok = med([r["tok_per_s"] for r in main])
        sp = {k: med([r[k] for r in main]) for k in ("search_s", "retrieve_s", "answer_s")}
        B.append(("Split of an answer", f"search terms {fmt(sp['search_s'])} s, retrieval and rerank {fmt(sp['retrieve_s'])} s, answering {fmt(sp['answer_s'])} s; {fmt(tok)} tokens/s",
                  f"{len(main)} questions", "reported", "reported"))
    else:
        B.append(("Time to a complete answer", "-", "-", "median under 60 s", "not run"))
    parts = []
    for c in CONDS:
        r = cond_rows(c)
        if r:
            parts.append(f"{c} {fmt(med([x['wall_s'] for x in r]))} s (n={len(r)})")
    B.append(("Modes and service against one-shot (median seconds)", "; ".join(parts) or "-", "see n", "reported", "reported" if parts else "not run"))
    if sd and (cond_rows("service-default-check")):
        ck = {r["id"]: r["wall_s"] for r in cond_rows("service-default-check")}
        pair = [(r["wall_s"], ck[r["id"]]) for r in sd if r["id"] in ck]
        if pair:
            B.append(("Cost of --check (service, paired)", f"median {fmt(med([b - a for a, b in pair]), 2)} s per answer", f"{len(pair)} questions", "reported", "reported"))
    if sd and len(sd) > 2:
        B.append(("Cold start against warm (service)", f"first question {fmt(sd[0]['wall_s'])} s, median of the others {fmt(med([r['wall_s'] for r in sd[1:]]))} s; service ready in {fmt(sd[0].get('startup_s'))} s",
                  f"{len(sd)} questions", "reported", "reported"))
    cold = [r for r in d.get("cold", []) if not r.get("error")]
    if cold:
        B.append(("Cold start of the whole assistant (first question after the server was restarted, memory phase)",
                  f"{fmt(cold[0]['wall_s'])} s, first words {fmt(cold[0]['ttft_s'])} s; service ready in {fmt(cold[0].get('startup_s'))} s; later two questions {fmt(med([r['wall_s'] for r in cold[1:]]))} s",
                  f"{len(cold)} questions", "reported", "reported"))
    pr = d["probe"]
    if pr:
        B.append(("Retrieval alone (no model)", f"index {fmt(pr.get('index_load_s'))} s + reranker load {fmt(pr.get('reranker_load_s'))} s once; per question BM25 {fmt(pr.get('bm25_s_median'), 3)} s, rerank {fmt(pr.get('rerank_s_median'))} s; process peak {fmt(pr.get('peak_rss_mb'), 0)} MB",
                  f"{pr.get('n')} questions", "reported", "reported"))
    for lab, rs in sorted(d["sustained"].items()):
        rs = [r for r in rs if not r.get("error")]
        t = [r["wall_s"] for r in rs]
        if len(t) >= 10:
            f10, l10 = med(t[:10]), med(t[-10:])
            sl = slope(t)
            pw = read_jsonl(Path(d["_out"]) / "sustained" / lab / "power.jsonl") if d.get("_out") else []
            th = sum(1 for p in pw if p.get("thermal_warning") or p.get("performance_warning"))
            lim = min([p["cpu_speed_limit"] for p in pw if p.get("cpu_speed_limit") is not None], default=None)
            B.append((f"Sustained use, {lab}", f"first 10 median {f10:.1f} s, last 10 median {l10:.1f} s, ratio {l10 / f10:.2f}; slope {fmt(sl, 2)} s per question; "
                      f"thermal or performance warnings in {th} of {len(pw)} readings; lowest CPU speed limit {fmt(lim, 0)}",
                      f"{len(t)} questions", f"ratio under {BARS['drift_ratio']} (threshold set in the plan, not in GOAL.md)",
                      "pass" if l10 / f10 < BARS["drift_ratio"] else "FAIL"))
        elif t:
            B.append((f"Sustained use, {lab}", f"only {len(t)} questions", f"{len(t)}", "30 questions", "not run in full"))
    if not d["sustained"]:
        B.append(("Sustained use", "-", "-", "30 in a row, on power and on battery", "not run"))
    wl = [r for r in ok_rows if r.get("label") == "working-laptop" and r["cond"] == "service-default"]
    if wl and sd:
        B.append(("A working laptop (apps open), service default", f"median {fmt(med([r['wall_s'] for r in wl]))} s against {fmt(med([r['wall_s'] for r in sd]))} s with nothing else open",
                  f"{len(wl)} and {len(sd)} questions", "median under 60 s", "pass" if med([r["wall_s"] for r in wl]) < BARS["median_answer_s"] else "FAIL"))
    else:
        B.append(("A working laptop (apps open)", "-", "-", "median under 60 s", "not run (speed --label working-laptop)"))
    c = d["compare"]
    if c:
        pts = 100 * abs(c["laptop_pass"] - c["reference_pass"]) / max(c["n"], 1)
        B.append(("Same answers as the reference machine (mechanical scorer, open subset)", f"laptop {c['laptop_pass']} of {c['n']} all-pass, reference {c['reference_pass']} of {c['n']}; "
                  f"{c['discordant']} items differ ({c['laptop_only']} only the laptop passes, {c['reference_only']} only the reference machine); identical answer text {c['identical_text']} of {c['n']}",
                  f"{c['n']} items", f"difference under {BARS['score_margin_points']} points (flag, not a quality gate: open development items)",
                  "pass" if pts < BARS["score_margin_points"] else "FAIL (a build or quantisation difference may show)"))
    else:
        B.append(("Same answers as the reference machine", "-", "-", "difference under 5 points", "not run"))
    return B


def summary_md(out):
    d = collect(out)
    d["_out"] = str(out)
    stub = (d["config"] or {}).get("stub")
    mi = (d["preflight"] or {}).get("machine") or machine_info()
    L = ["# Laptop test summary", ""]
    if stub:
        L += ["**STUB RUN: no model, no real measurement. This file only shows that the scripts work.**", ""]
    L += [f"Machine: {mi.get('chip')}, {mi.get('cpu_total')} cores ({mi.get('cpu_performance')} performance, {mi.get('cpu_efficiency')} efficiency), "
          f"{mi.get('memory_gb')} GB memory, macOS {mi.get('macos')}, power {mi.get('power_source')}.",
          f"Runtime commit {mi.get('runtime_commit')} (tree changed: {mi.get('runtime_tree_dirty')}); mlx-lm {mi.get('mlx_lm')}, mlx {mi.get('mlx')}.",
          f"Serving: backend {(d['config'] or {}).get('backend')}, model {(d['config'] or {}).get('model')}.",
          "The questions are open development items. These numbers are speed, memory and scorer-agreement measurements, not quality decisions.", "",
          "| Bar | Measured | Size | Bar value | Result |", "|---|---|---|---|---|"]
    for n, m, s, b, v in bars(d):
        L.append(f"| {n} | {m} | {s} | {b} | {v} |")
    L += ["", "Memory peaks are the largest of the samples taken every few seconds; RSS may miss GPU memory, which is why the system's used memory is shown beside it."]
    return "\n".join(L) + "\n"


# ------------------------------------------------------------------ scrubber and bundle

class Scrubber:
    """Removes what identifies the machine or its owner from text. Terms are looked up on this machine
    (host names, user name, home and repository path, serial number, hardware UUID, Wi-Fi name) and can be given
    for a test. Patterns: IPv4, IPv6, MAC, *.local style host names, /Users/<name>, temporary folders."""
    IPV4 = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
    IPV6 = re.compile(r"(?<![\w:])(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}(?![\w:])|(?<![\w:])[0-9a-fA-F:]*::[0-9a-fA-F:]*(?![\w:])")
    MAC = re.compile(r"\b(?:[0-9a-fA-F]{2}[:-]){5}[0-9a-fA-F]{2}\b")
    HOST = re.compile(r"\b[\w-]+(?:\.[\w-]+)*\.(?:local|lan|home|internal|localdomain)\b", re.I)
    USERPATH = re.compile(r"/(?:Users|home)/[^/\s\"'<>]+")
    TMP = re.compile(r"/(?:private/)?var/folders/\S+")
    UUID = re.compile(r"\b[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}\b")

    def __init__(self, terms=None):
        self.terms = terms if terms is not None else self.lookup()
        self.words = []  # (word, label), longest first
        for key, val in self.terms.items():
            for v in ([val] if isinstance(val, str) else list(val or [])):
                if v and len(v) >= 2:
                    self.words.append((v, key))
        self.words.sort(key=lambda x: -len(x[0]))

    @staticmethod
    def lookup():
        t = {"user": [], "host": [], "path": [], "serial": [], "ssid": []}
        try:
            t["user"].append(getpass.getuser())
        except Exception:
            pass
        t["path"] += [str(Path.home()), str(ROOT)]
        t["user"].append(Path.home().name)
        hn = socket.gethostname()
        t["host"] += [hn, hn.split(".")[0]]
        for k in ("ComputerName", "LocalHostName", "HostName"):
            v = (sh(["scutil", "--get", k], 5) or "").strip()
            if v:
                t["host"].append(v)
        io = sh(["ioreg", "-c", "IOPlatformExpertDevice", "-d", "2"], 10) or ""
        for m in re.finditer(r'"(?:IOPlatformSerialNumber|IOPlatformUUID|serial-number)"\s*=\s*"?([^"\s>]+)', io):
            t["serial"].append(m.group(1))
        for dev in ("en0", "en1"):
            v = sh(["networksetup", "-getairportnetwork", dev], 5) or ""
            m = re.search(r"Network:\s*(.+)", v)
            if m:
                t["ssid"].append(m.group(1).strip())
        pref = sh(["networksetup", "-listpreferredwirelessnetworks", "en0"], 5) or ""
        t["ssid"] += [l.strip() for l in pref.splitlines()[1:] if l.strip()]
        return t

    def text(self, s):
        if not isinstance(s, str):
            return s
        for w, key in self.words:
            if key == "path":
                s = s.replace(w, "<path>")
        for w, key in self.words:
            if key != "path":
                s = re.sub(r"(?<![A-Za-z0-9])" + re.escape(w) + r"(?![A-Za-z0-9])", f"<{key}>", s, flags=re.I)
        s = self.TMP.sub("<tmp>", s)
        s = self.USERPATH.sub("/<user-home>", s)
        s = self.MAC.sub("<mac>", s)
        s = self.IPV6.sub("<ip>", s)
        s = self.IPV4.sub("<ip>", s)
        s = self.UUID.sub("<id>", s)
        s = self.HOST.sub("<host>", s)
        return s

    def obj(self, o):
        if isinstance(o, str):
            return self.text(o)
        if isinstance(o, list):
            return [self.obj(x) for x in o]
        if isinstance(o, dict):
            return {self.text(k) if isinstance(k, str) else k: self.obj(v) for k, v in o.items()}
        return o

    def leftovers(self, s):
        """Terms or patterns still present in s (labels only, never the values)."""
        bad = []
        low = s.lower()
        for w, key in self.words:
            if key == "path":
                if w.lower() in low:
                    bad.append(key)
            elif re.search(r"(?<![A-Za-z0-9])" + re.escape(w) + r"(?![A-Za-z0-9])", s, re.I):
                bad.append(key)
        for name, rx in (("ipv4", self.IPV4), ("ipv6", self.IPV6), ("mac", self.MAC), ("host", self.HOST), ("userpath", self.USERPATH)):
            if rx.search(s):
                bad.append(name)
        return sorted(set(bad))


SEND_SUFFIXES = {".json", ".jsonl", ".md", ".log", ".txt"}
SKIP_NAMES = {"items_subset.jsonl", "probe_questions.json"}


def cmd_bundle(a):
    out, cfg = resolve(a)
    sc = Scrubber(json.loads(a.scrub_terms) if a.scrub_terms else None)
    md = summary_md(out)
    (out / "SUMMARY.md").write_text(md, encoding="utf-8")
    dest = out / "send_back"
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)
    n, leaks = 0, []
    for f in sorted(out.rglob("*")):
        if not f.is_file() or "send_back" in f.parts or f.suffix not in SEND_SUFFIXES or f.name in SKIP_NAMES:
            continue
        raw = f.read_text(encoding="utf-8", errors="replace")
        if f.suffix == ".log":
            raw = raw[-200000:]
        cleaned = sc.text(raw)  # text level: the same result for json, jsonl, md and logs
        left = sc.leftovers(cleaned)
        if left:
            leaks.append((str(f.relative_to(out)), left))
            continue
        t = dest / f.relative_to(out)
        t.parent.mkdir(parents=True, exist_ok=True)
        t.write_text(cleaned, encoding="utf-8")
        n += 1
    if leaks:
        for name, left in leaks:
            log("bundle", f"NOT included, still holds {left}: {name}")
    z = out / "send_back.zip"
    with zipfile.ZipFile(z, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in sorted(dest.rglob("*")):
            if f.is_file():
                zf.write(f, f.relative_to(dest))
    log("bundle", f"{n} files scrubbed into send_back/ and {z.name}; {len(leaks)} left out; SUMMARY.md written")
    print("\n" + sc.text(md))
    return 1 if leaks else 0


# ------------------------------------------------------------------ make-ids (reference machine)

def cmd_make_ids(a):
    """eval/m4/laptop/ids.json: the 30 judged scenarios in file order, and 60 open mechanical items drawn round-robin over
    the kinds (seeded) among the ones the Mac Studio's 4-bit run (s6-A1) answered without error, with that run's pass
    flag and the sha256 of its answer text."""
    s6 = ROOT / "work/m4/s6"
    answers = {r["id"]: r for r in read_jsonl(s6 / "ans_A1.jsonl")}
    scores = {r["id"]: r for r in read_jsonl(s6 / "ans_A1.scores.jsonl")}
    man = read_json(s6 / "ans_A1.jsonl.manifest.json", {})
    tier_f = ROOT / "eval/m4/open/tier_a.jsonl"
    items = read_jsonl(tier_f)
    eligible = [it for it in items if it["id"] in answers and not answers[it["id"]].get("error") and it["id"] in scores]
    by = {}
    for it in eligible:
        by.setdefault(it["kind"], []).append(it["id"])
    rng = random.Random(a.seed)
    for k in by:
        by[k].sort()
        rng.shuffle(by[k])
    kinds = sorted(by)
    chosen = []
    while len(chosen) < a.n_score and any(by[k] for k in kinds):
        for k in kinds:
            if by[k] and len(chosen) < a.n_score:
                chosen.append(by[k].pop())
    chosen.sort()
    b1 = [r["id"] for r in read_jsonl(ROOT / "eval/m4/b1/items.jsonl")]
    ref = {"run": man.get("run"), "model": man.get("model_requested"), "mode": man.get("mode"),
           "pass": {i: bool(scores[i]["all_pass"]) for i in chosen},
           "answer_sha256": {i: hashlib.sha256(answers[i]["text"].encode()).hexdigest() for i in chosen}}
    ref["all_pass_count"] = sum(ref["pass"].values())
    obj = {"version": 1, "seed": a.seed,
           "note": "Open development items. Laptop numbers on them are speed and memory measurements, not quality decisions. Nothing here comes from eval/hidden/.",
           "speed": {"file": "eval/m4/b1/items.jsonl", "ids": b1},
           "score": {"file": "eval/m4/open/tier_a.jsonl", "items_sha256": sha256_file(tier_f), "ids": chosen, "reference": ref}}
    IDS_FILE.parent.mkdir(parents=True, exist_ok=True)
    write_json(IDS_FILE, obj)
    from collections import Counter
    log("make-ids", f"{len(b1)} speed ids, {len(chosen)} score ids {dict(Counter(next(i['kind'] for i in items if i['id'] == c) for c in chosen))}, "
                    f"reference pass {ref['all_pass_count']}/{len(chosen)}")
    return 0


# ------------------------------------------------------------------ cli

def _common_args(ap, suppress):
    """The options every phase takes. Given on the subcommand they also work (suppressed defaults do not overwrite)."""
    kw = {"default": argparse.SUPPRESS} if suppress else {}
    ap.add_argument("--out", help="output folder (default out/laptop/<date>)", **kw)
    ap.add_argument("--date", help="folder name under out/laptop (default today)", **kw)
    ap.add_argument("--stub", action="store_true", help="fake backend, for tests and for trying the scripts", **kw)
    ap.add_argument("--backend", choices=["openai", "mlx"], help="openai: oMLX or another server (OMLX_URL); mlx: in-process mlx-lm", **kw)
    ap.add_argument("--model", help="openai: the model name on the server; mlx: the model directory", **kw)
    ap.add_argument("--extra-body", help="JSON for the server request (default switches thinking off for Qwen3.8)", **kw)
    ap.add_argument("--k", choices=["k6", "k3"], **kw)
    ap.add_argument("--max-tokens", type=int, **kw)
    ap.add_argument("--catalogues", **kw)
    ap.add_argument("--weights-dir", help="folder holding the model weights, to measure their size", **kw)
    ap.add_argument("--model-repo", help="Hugging Face repository of the 4-bit model (install-check --run)", **kw)
    ap.add_argument("--port", type=int, help="port of the service the script starts (default 8765)", **kw)
    ap.add_argument("--interval", type=float, help="seconds between memory samples (default 2)", **kw)


def build_parser():
    ap = argparse.ArgumentParser(prog="laptop_bench.py", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    _common_args(ap, False)
    common = argparse.ArgumentParser(add_help=False)
    _common_args(common, True)
    sub = ap.add_subparsers(dest="cmd", required=True)

    def add(name):
        return sub.add_parser(name, parents=[common])
    p = add("preflight")
    p.add_argument("--strict", action="store_true", help="exit 1 when a required item is missing")
    p = add("install-check")
    p.add_argument("--run", action="store_true", help="execute the documented sequence, timed per step")
    p = add("speed")
    p.add_argument("--label", default="baseline")
    p.add_argument("--conds", help="comma list of " + ", ".join(CONDS) + " (default all six)")
    p.add_argument("--n-service", type=int, default=10)
    p.add_argument("--n-oneshot", type=int, default=5)
    p.add_argument("--apps", help="working-laptop series: what is open (free text)")
    p = add("sustained")
    p.add_argument("--label", choices=["ac", "battery"], required=True)
    p.add_argument("--n", type=int, default=30)
    p.add_argument("--runtime", choices=["service", "oneshot"], default="service")
    p.add_argument("--force", action="store_true")
    p = add("memory")
    p.add_argument("--baseline-s", type=int, default=20)
    p.add_argument("--force", action="store_true")
    add("score")
    p = add("bundle")
    p.add_argument("--scrub-terms", help="JSON {label: [terms]} instead of looking them up (tests)")
    p = add("make-ids")
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--n-score", type=int, default=60)
    sub.add_parser("_probe").add_argument("files", nargs=2)
    return ap


def main(argv=None):
    a = build_parser().parse_args(argv)
    if a.cmd == "_probe":
        probe_main(*a.files)
        return 0
    fn = {"preflight": cmd_preflight, "install-check": cmd_install, "speed": cmd_speed, "sustained": cmd_sustained,
          "memory": cmd_memory, "score": cmd_score, "bundle": cmd_bundle, "make-ids": cmd_make_ids}[a.cmd]
    return fn(a)


if __name__ == "__main__":
    sys.exit(main())
