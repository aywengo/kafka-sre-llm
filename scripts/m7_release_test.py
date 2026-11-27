#!/usr/bin/env python3
"""M7 release test: the helpers of scripts/m7_release_test.sh (MILESTONE.md, "The release test"). CPU only; the chain itself is the shell script.

  arms          the arms of configs.json (an entry with an `ask_command`) and their ask argv, checked
  manifest      write the run's manifest (configs sha256, arms, sets, seeds, rule text hash, pinned files) or refuse when it differs (an amended config = a new manifest)
  ledger-check  refuse unless the hidden ledger holds a line with the token release-look-h6 and the configs sha256
  answer        one arm on one set: the config's `ask ... --json` command per item, one at a time, resumable by rows (h6: encrypted rows, a counter only)
  packs         recover each answer's pack (chunk ids) from the question and the returned expansion; the sources must match position by position
  judge-run     eval2_judge.py with its output filtered (a hidden run prints no item id)
  server        gate | unload | status (W2: never unload another client's model; W16: only the DeepSeek build, logged by the caller)
  bundle        blind reading bundles: d7 (open: first reader, second-family export) and h6 (sealed, first reader only)
  unblind       d7 (open) or h6 (sealed, encrypted output) readers' findings
  pack-input    the adjudication pack's input files (scripts/m7_adjudication_pack.py build)
  measures      per-answer records of a set: judge, flags and adjudication in one file (h6: encrypted)
  stub-ask      the stand-in for `python -m kafka_sre_rag ask ... --json` in a dry run
  entries-arms  the arms whose configuration has the fact entries on (W43: the arm RE)
  chunks-plus   the chunk file plus every fact entry as a chunk-shaped row (id `kbe:<entry id>`), for the readers' excerpts, the judge and the adjudication pack of an entries arm
  judge-plan    which judge stages of a work folder are done and which arms still need one (an arm added later gets its own stage, file and cache; finished ones are never rewritten)
  descriptive   a paired line candidate - reference (RE - R: entries on against off), marked descriptive, no bar

Hidden handling (h6): items, rubrics, answers, judge outputs, findings and measures are .enc files (eval2.py's format and key); plain text lives in memory only; every
printed line holds counts, never an id or a text; the ask call's output is captured and never printed. Exit codes: 0 done, 1 failed, 2 refused.
"""
import argparse
import hashlib
import json
import os
import re
import shlex
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))
import eval2 as E  # noqa: E402
import m7_release_rule as RR  # noqa: E402

SEEDS = {"bundle": 20261126, "pack_shuffle": 20261127, "pack_sample": 20261128, "bootstrap": RR.BOOT_SEED}
LEDGER_TOKEN = "release-look-h6"
H6_TAG = "conductor_exposed_h6"
DS_MODEL = "DeepSeek-V4.1-Flash-1017-oQ4e-mtp"
D7_ITEMS, D7_RUBRICS = "eval/m7/d7/items.jsonl", "eval/m7/d7/rubrics.jsonl"
H6_ITEMS, H6_RUBRICS = "eval/hidden/tier_b_h6_items_final.jsonl.enc", "eval/hidden/tier_b_h6_rubrics_final.jsonl.enc"
SET_COUNT = {"d7": 60, "h6": 60}
ASK_TIMEOUT = 1800
ENTRIES_RECORD = "kafka_sre_rag/kb_entries.json"
ENTRIES_PREFIX = "kbe:"     # kafka_sre_rag.kb_entries.PACK_PREFIX: an entry in a stored pack
ENTRY_MODULES = ("kafka_sre_rag/kb_entries.py", "kafka_sre_rag/kb_entry.py", ENTRIES_RECORD)


class Refused(Exception):
    pass


def sha256_file(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def sha256_text(s):
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def is_hidden_set(s):
    return s == "h6"


# ---------------------------------------------------------------- configs and arms

def load_configs(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def arm_names(cfgs):
    """Every top-level entry with its own ask_command is an arm (R, C, and later an R variant)."""
    return [k for k, v in cfgs.items() if isinstance(v, dict) and isinstance(v.get("ask_command"), str)]


def ask_flags(cmd):
    """The flags between `ask` and the question placeholder of an ask_command. Refuses anything that is not a real ask with --json."""
    toks = shlex.split(cmd)
    while toks and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*=.*", toks[0]):
        toks = toks[1:]
    if "kafka_sre_rag" not in toks or "ask" not in toks:
        raise Refused("an ask_command is not a `python -m kafka_sre_rag ask` command")
    rest = toks[toks.index("ask") + 1:]
    if not rest or rest[-1] != "<question>":
        raise Refused("an ask_command does not end with the <question> placeholder")
    flags = rest[:-1]
    for bad in ("--dry-run", "--stream", "--service-url"):
        if bad in flags:
            raise Refused(f"an ask_command carries {bad}")
    if "--json" not in flags:
        raise Refused("an ask_command lacks --json")
    return flags


def arm_info(cfgs, arm):
    if arm not in arm_names(cfgs):
        raise Refused(f"arm {arm} is not an entry with an ask_command in configs.json (arms: {', '.join(arm_names(cfgs))})")
    c = cfgs[arm]
    flags = ask_flags(c["ask_command"])
    mid = (c.get("model") or {}).get("oMLX_model_id")
    if not mid and "--model" in flags:
        mid = flags[flags.index("--model") + 1]
    if not mid:
        raise Refused(f"arm {arm}: no model id")
    on = bool((c.get("entries") or {}).get("on"))
    ent = "--kb-entries" in flags
    if ent != on:
        raise Refused(f"arm {arm}: the ask_command {'has' if ent else 'lacks'} --kb-entries but the entries block says on={on}")
    rec = flags[flags.index("--kb-entries-record") + 1] if "--kb-entries-record" in flags else ENTRIES_RECORD
    if ent and rec != c["entries"].get("record"):
        raise Refused(f"arm {arm}: the entries record of the ask_command ({rec}) is not the one the entries block names")
    return {"arm": arm, "flags": flags, "model": mid, "ask_sha256": sha256_text(json.dumps(flags)), "entries": ent, "entries_record": rec if ent else None}


def ask_argv(info, question, python=None, stub=False):
    """The runtime's own command; `--` keeps a question that starts with a dash from being read as an option."""
    if stub:
        return [python or sys.executable, str(ROOT / "scripts/m7_release_test.py"), "stub-ask", *info["flags"], "--", question]
    return [python or sys.executable, "-m", "kafka_sre_rag", "ask", *info["flags"], "--", question]


def cmd_arms(a):
    cfgs = load_configs(a.configs)
    want = [x for x in a.arms.split(",") if x] or arm_names(cfgs)
    for arm in want:
        i = arm_info(cfgs, arm)
        if i["entries"]:
            bad = entries_pin_problems(cfgs[arm])
            if bad:
                raise Refused(f"arm {arm}: the fact entries differ from what configs.json fixed: {'; '.join(bad)}; nothing was started")
        print(f"{arm}: model {i['model']}, {len(i['flags'])} ask flags, ask_sha256 {i['ask_sha256'][:16]}"
              + (", fact entries on (record, index and modules equal to configs.json)" if i["entries"] else ""))
    print("MODELS " + " ".join(dict.fromkeys(arm_info(cfgs, x)["model"] for x in want)))
    return 0


def order_arms(cfgs, arms, candidate):
    """Arms grouped by model, the candidate's model first (all R arms before C: one swap per model); stable inside a group."""
    models = [arm_info(cfgs, x)["model"] for x in arms]
    first = arm_info(cfgs, candidate)["model"] if candidate in arms else None
    order = ([first] if first else []) + [m for m in dict.fromkeys(models) if m != first]
    return sorted(arms, key=lambda x: order.index(arm_info(cfgs, x)["model"]))


def cmd_arm_order(a):
    cfgs = load_configs(a.configs)
    print(" ".join(order_arms(cfgs, [x for x in a.arms.split(",") if x], a.candidate)))
    return 0


# ---------------------------------------------------------------- runtime files (before each arm)

def entries_pin_problems(c):
    """The fact entries of an arm (W43) against the files in this tree: the entries record, the index file the record names, the count, and the two entries modules.
    -> what differs (empty when all is as fixed)."""
    e = c.get("entries") or {}
    if not e.get("on"):
        return []
    rec_path = ROOT / (e.get("record") or ENTRIES_RECORD)
    if not rec_path.is_file() or sha256_file(rec_path) != e.get("record_sha256"):
        return [f"the entries record {e.get('record')}"]
    bad = []
    rec = json.loads(rec_path.read_text(encoding="utf-8"))
    if rec.get("file") != e.get("index") or rec.get("sha256") != e.get("sha256") or rec.get("entries") != e.get("entries"):
        bad.append("the entries record names another index, hash or count than the entries block")
    idx = ROOT / (rec.get("file") or "")
    if not idx.is_file() or sha256_file(idx) != e.get("sha256"):
        bad.append(f"the entries index {e.get('index')}")
    for rel in ENTRY_MODULES:
        h = (c.get("runtime_files_sha256") or {}).get(rel)
        if not h or not (ROOT / rel).is_file() or sha256_file(ROOT / rel) != h:
            bad.append(rel)
    return bad


def cmd_runtime_check(a):
    """The runtime files and the lookup modules the configuration was fixed with are the files in this tree now (prompts.py is left to `kafka_sre_rag verify`)."""
    c = load_configs(a.configs)[a.arm]
    bad = []
    for rel, h in sorted((c.get("runtime_files_sha256") or {}).items()):
        if rel == "kafka_sre_rag/prompts.py":
            continue
        if not (ROOT / rel).is_file() or sha256_file(ROOT / rel) != h:
            bad.append(rel)
    if (c.get("lookup_cards") or {}).get("on"):
        for rel, h in sorted(((c.get("lookup_cards") or {}).get("module_sha256") or {}).items()):
            if not (ROOT / rel).is_file() or sha256_file(ROOT / rel) != h:
                bad.append(rel)
    bad += [b for b in entries_pin_problems(c) if b not in bad]
    if bad:
        raise Refused(f"arm {a.arm}: these files differ from the hashes recorded in configs.json: {', '.join(bad)}; the configuration is not the one that was fixed; nothing was started")
    print(f"arm {a.arm}: runtime files and modules equal to configs.json")
    return 0


# ---------------------------------------------------------------- manifest and ledger (one work folder per set)

def config_sha(cfgs, arm):
    """The hash of one configuration's entry (canonical JSON): the unit by which an amended configs.json is judged."""
    return sha256_text(json.dumps(cfgs[arm], sort_keys=True))


VARIANT_KEYS = ("candidate", "control", "rule_text_sha256")


def strict_state(a):
    """What may never change within a work folder: the set, the seeds, the rule text and its script, the set's items and rubrics, the options. configs.json as a whole and the helper
    scripts are logged, not compared (a fix to a helper must not cost the answers); each answered configuration is compared by its own hash."""
    return {
        "schema": 2, "set": a.set, "seeds": SEEDS, "candidate": a.candidate, "control": a.control,
        "rule_text_sha256": RR.rule_text_sha256(a.candidate, a.control, a.h6_mode, a.h6_chosen_by),
        "rule_script_sha256": sha256_file(ROOT / "scripts/m7_release_rule.py"),
        "set_pins": {p: sha256_file(p) for p in a.pin if Path(p).exists()}, "missing_pins": sorted(p for p in a.pin if not Path(p).exists()),
        "options": dict(o.partition("=")[::2] for o in a.opt),
    }


def logged_state(a):
    return {"configs_sha256": sha256_file(a.configs), "other": {p: sha256_file(p) for p in a.log_pin if Path(p).exists()}}


def cmd_manifest(a):
    wd = Path(a.work)
    m = wd / "MANIFEST.json"
    cfgs = load_configs(a.configs)
    arms = [x for x in a.arms.split(",") if x]
    new_arms = {x: {"config_sha256": config_sha(cfgs, x), "ask_sha256": arm_info(cfgs, x)["ask_sha256"]} for x in arms}
    strict, logged = strict_state(a), logged_state(a)
    if m.exists():
        old = json.loads(m.read_text(encoding="utf-8"))
        events = []
        # candidate, control and the rule text name the arms; they may change only by the explicit, logged "late arm" path (W43: the candidate RE is added to a folder that was fixed with R)
        d = [k for k in strict if k not in VARIANT_KEYS and k != "options" and old.get(k) != strict[k]]
        oo, no = dict(old.get("options") or {}), dict(strict["options"])
        if no.get("dry") == "yes" and oo.get("dry") == "no":      # a dry run on a scratch copy of a real folder (the chain refuses a dry run in the real folder itself)
            oo["dry"] = no["dry"]
        if oo != no:
            d.append("options")
        if d:
            raise Refused("the stored manifest differs from the state now in: " + ", ".join(d) + "; nothing was moved or deleted")
        variant = {k: strict[k] for k in VARIANT_KEYS}
        known = [{k: old.get(k) for k in VARIANT_KEYS}] + list(old.get("rule_variants") or [])
        if variant not in [{k: v.get(k) for k in VARIANT_KEYS} for v in known]:
            if not a.allow_late_arms:
                raise Refused("the candidate, the control or the rule text differ from the stored manifest in: " + ", ".join(k for k in VARIANT_KEYS if old.get(k) != strict[k])
                              + "; a candidate that is a new arm needs --allow-late-arms (logged); nothing was moved or deleted")
            if strict["control"] != old.get("control") or strict["candidate"] not in new_arms or (strict["candidate"] in old["arms"] and strict["candidate"] not in (old.get("late_arms") or {})):
                raise Refused("--allow-late-arms: the control must be the stored one and the candidate an arm that is new to this folder; nothing was moved or deleted")
            old.setdefault("rule_variants", []).append(dict(variant, at=time.strftime("%Y-%m-%dT%H:%M:%S%z")))
            events.append(f"rule text for the candidate {variant['candidate']} (control {variant['control']}) added, sha256 {variant['rule_text_sha256'][:16]}; the stored rule text for {old['candidate']} is unchanged")
        for x, v in new_arms.items():
            if x in old["arms"]:
                if old["arms"][x]["config_sha256"] != v["config_sha256"]:
                    raise Refused(f"the configuration {x} was already answered in this work folder and its entry in configs.json is no longer byte-identical: an amended configuration means a new work folder; nothing was changed")
            else:
                late = (wd / "stage_bundle.done").exists()
                if late and not a.allow_late_arms:
                    raise Refused(f"arm {x} cannot be added: this set's reading bundles were already made (an explicit --allow-late-arms adds it, logged, for a new reading folder)")
                old["arms"][x] = v
                if late:
                    old.setdefault("late_arms", {})[x] = {"after": "the reading bundles of the first reading were made", "at": time.strftime("%Y-%m-%dT%H:%M:%S%z")}
                events.append(f"arm {x} added{' after the reading bundles were made (--allow-late-arms; the first reading folder is not touched)' if late else ''} (config sha256 {v['config_sha256'][:16]})")
        if old["logged"] != logged:
            events.append("logged state changed: " + ", ".join(sorted(k for k in ("configs_sha256",) if old["logged"].get(k) != logged[k]) + sorted(p for p in logged["other"] if old["logged"]["other"].get(p) != logged["other"][p])))
            old["logged"] = logged
        if events:
            old.setdefault("history", []).extend({"at": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "event": e} for e in events)
            m.write_text(json.dumps(old, indent=1, sort_keys=True) + "\n", encoding="utf-8")
            for e in events:
                print("manifest: " + e)
        else:
            print("manifest equal to the state now")
        return 0
    if any(wd.glob("ans_*")):
        raise Refused("answer files exist but there is no manifest; the manifest and the header come first")
    wd.mkdir(parents=True, exist_ok=True)
    doc = dict(strict, arms=new_arms, logged=logged, history=[])
    m.write_text(json.dumps(doc, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    print(f"manifest written; configs sha256 {logged['configs_sha256']}; rule text sha256 {strict['rule_text_sha256'][:16]}; arms {', '.join(arms)}")
    return 0


def cmd_manifest_arms(a):
    print(" ".join(json.loads((Path(a.work) / "MANIFEST.json").read_text(encoding="utf-8"))["arms"]))
    return 0


def manifest_hash(path):
    return sha256_text(Path(path).read_text(encoding="utf-8"))[:16]


def ledger_ok(ledger, *needles):
    """A line with the token release-look-h6 and every needle (the configs sha256 and the rule text's sha256)."""
    p = Path(ledger)
    if not p.is_file():
        return False
    return any(LEDGER_TOKEN in line and all(n in line for n in needles) for line in p.read_text(encoding="utf-8", errors="replace").splitlines())


def check_ledger(ledger, sha, rule_sha):
    if not (sha and rule_sha) or not ledger_ok(ledger, sha, rule_sha):
        raise Refused(f"the hidden ledger holds no line with the token {LEDGER_TOKEN}, the configs sha256 and the rule text's sha256; the conductor writes that row before the look; no h6 answer was made")


def cmd_ledger_check(a):
    check_ledger(a.ledger, a.configs_sha256 or sha256_file(a.configs), a.rule_sha256)
    print("ledger row found")
    return 0


def check_set_inputs(set_name, items, rubrics, stub):
    """Open sets use open files; the hidden set uses .enc files only; a stub run never touches eval/hidden/."""
    hid = is_hidden_set(set_name)
    for what, p in (("items", items), ("rubrics", rubrics)):
        if E.is_hidden(p) != hid:
            raise Refused(f"{set_name}: {what} must {'end in .enc' if hid else 'be an open file'}")
        if stub and (ROOT / "eval/hidden") in (Path(p).resolve(), *Path(p).resolve().parents):
            raise Refused("a stub run never uses the real hidden files (eval/hidden/)")


def cmd_check_inputs(a):
    check_set_inputs(a.set, a.items, a.rubrics, a.stub)
    items = E.read_items(a.items)
    rub = {r["id"] for r in E.read_jsonl(a.rubrics)}
    if a.limit:
        if not a.stub:
            raise Refused("--limit is for stub runs only")
        items = items[:a.limit]
    elif not a.stub and len(items) != SET_COUNT[a.set]:
        raise Refused(f"{a.set}: {len(items)} items, {SET_COUNT[a.set]} expected")
    if any(it["id"] not in rub for it in items) or rub - {it["id"] for it in items}:
        raise Refused(f"{a.set}: the items and the rubrics do not cover the same scenarios")
    if a.set == "h6" and not a.stub and not all(it["id"].startswith("h6-") for it in items):
        raise Refused("h6: the items are not the h6 scenarios")
    print(f"{a.set}: {len(items)} items with rubrics ({'hidden: counts only' if is_hidden_set(a.set) else 'open'})")
    return 0


# ---------------------------------------------------------------- rows

def load_rows(path):
    return E.read_jsonl(path) if Path(path).exists() else []


def save_rows(path, rows):
    E.write_jsonl(path, rows)


def words_of(text):
    return len((text or "").split())


def secs_of(r):
    s = r.get("seconds") or {}
    return s.get("total") if isinstance(s, dict) else s


def run_ask(info, question, python, stub, env):
    """-> (row fields, None) or (None, short error code). Output is captured; neither the question nor stderr is ever printed or stored for a hidden run."""
    argv = ask_argv(info, question, python, stub)
    t0 = time.monotonic()
    try:
        p = subprocess.run(argv, capture_output=True, text=True, timeout=ASK_TIMEOUT, cwd=ROOT, env=env)
    except subprocess.TimeoutExpired:
        return None, "timeout"
    except OSError:
        return None, "start failed"
    wall = time.monotonic() - t0
    if p.returncode != 0:
        return None, f"exit {p.returncode}"
    try:
        d = json.loads(p.stdout.strip().splitlines()[-1])
    except (ValueError, IndexError):
        return None, "no JSON"
    if not isinstance(d, dict) or not (d.get("text") or "").strip():
        return None, "empty answer"
    t = d.get("timings") or {}
    sec = {"search_terms": t.get("expand_s", 0.0), "retrieval": t.get("retrieve_s", 0.0), "answer": t.get("answer_s", 0.0)}
    sec["total"] = sum(sec.values())
    row = {"text": d["text"], "tokens": d.get("tokens"), "truncated": bool(d.get("truncated")), "seconds": sec, "wall_s": wall, "search_terms": d.get("expansion"),
           "sources": d.get("sources"), "measured": d.get("measured"), "unmeasured_reasons": d.get("unmeasured_reasons")}
    kbe = d.get("kb_entries")
    if info.get("entries"):       # the entries arm: the runtime must say which entries it added (the ids, the skipped counts, the candidates, the pack size)
        if not isinstance(kbe, dict) or not isinstance(kbe.get("added"), list):
            return None, "no kb_entries"
        row["kb_entries"] = {k: kbe.get(k) for k in ("added", "skipped", "candidates", "pack_chars", "seconds")}
        row["kb_entries"]["n_added"] = len(kbe["added"])
    elif kbe is not None:
        return None, "kb_entries from an arm without entries"
    if d.get("lookup_cards") is not None:
        lc = d["lookup_cards"]
        if isinstance(lc, dict):
            # the runtime writes the hash as catalogue_recorded_sha256; the row key stays catalogue_sha256
            row["lookup"] = {k: lc.get(k) for k in ("cards_found", "cards", "chars", "seconds")
                             if k != "cards" or "cards" in lc}
            row["lookup"]["catalogue_sha256"] = lc.get("catalogue_recorded_sha256") or lc.get("catalogue_sha256")
        else:
            row["lookup"] = None
    return row, None


def cmd_answer(a):
    cfgs = load_configs(a.configs)
    sha = sha256_file(a.configs)
    info = arm_info(cfgs, a.arm)
    hid = is_hidden_set(a.set)
    if E.is_hidden(a.items) != hid or E.is_hidden(a.out) != hid:
        raise Refused(f"{a.set}: items and answers files must {'end in .enc' if hid else 'be open files'}")
    if a.stub and ROOT / "eval/hidden" in (Path(a.items).resolve(), *Path(a.items).resolve().parents):
        raise Refused("a stub run never uses the real hidden files (eval/hidden/)")
    if hid:
        if not a.ledger:
            raise Refused("an h6 answer needs --ledger (runs/m4/HIDDEN_LEDGER.md)")
        check_ledger(a.ledger, sha, a.rule_sha256)
    items = E.read_items(a.items)
    if a.limit:
        if not a.stub:
            raise Refused("--limit is for stub runs only")
        items = items[:a.limit]
    elif not a.stub and len(items) != SET_COUNT[a.set]:
        raise Refused(f"{a.set}: {len(items)} items, {SET_COUNT[a.set]} expected")
    rows = [r for r in load_rows(a.out) if not r.get("error")]
    done = {r["id"] for r in rows}
    todo = [it for it in items if it["id"] not in done]
    tag = f"[answer {a.arm} {a.set}]"
    print(f"{tag} {len(items)} items, {len(done)} already done, {len(todo)} to do; model {info['model']}; ask_sha256 {info['ask_sha256'][:16]}", flush=True)
    env = dict(os.environ)
    failed = 0
    for i, it in enumerate(todo, 1):
        row = {"id": it["id"], "arm": a.arm, "set": a.set, "text": None, "tokens": None, "truncated": False, "seconds": None, "search_terms": None,
               "pack": None, "error": None, "ask_sha256": info["ask_sha256"]}
        res, err = None, None
        for attempt in range(2):
            res, err = run_ask(info, E.full_question(it), a.python, a.stub, env)
            if res:
                break
        if res:
            row.update(res)
            row["words"] = words_of(res["text"])
        else:
            row["error"] = err
            failed += 1
        rows.append(row)
        save_rows(a.out, rows)
        n = len(done) + i
        if hid:     # a counter only: no id, no text, no timing of an item
            print(f"{tag} {n}/{len(items)} {'ok' if res else 'failed'}", flush=True)
        else:
            s = secs_of(row)
            print(f"{tag} {n}/{len(items)} {it['id']} {'ok' if res else 'failed: ' + str(err)}" + (f" total={s:.1f}s tokens={row['tokens']} cut={'yes' if row['truncated'] else 'no'}" if res else ""), flush=True)
    if failed:
        print(f"{tag} {failed} item(s) failed after two tries; a rerun retries them", flush=True)
        return 1
    print(f"{tag} complete: {len(items)} answers", flush=True)
    return 0


# ---------------------------------------------------------------- packs

def chunk_key(c):
    return (c.get("source"), c.get("title"), c.get("path"))


def entry_tail(kb, kb_info, tail):
    """The fact entries of a pack (W43): the ids the runtime says it added, each checked against the entries index and against its source line in the answer (source, title, path of the
    rendered item); -> their ids as a stored pack lists them (`kbe:<id>`). A row of an arm without entries has none."""
    added = (kb_info or {}).get("added") or []
    if len(added) != len(tail):
        raise Refused("the added entries are not the tail of the answer's sources")
    for eid, s in zip(added, tail):
        if eid not in kb.byid or chunk_key(kb.item(eid)) != chunk_key(s):
            raise Refused("an added entry is not in the entries index or is not the answer's source")
    return [ENTRIES_PREFIX + e for e in added]


def keys_resolver(chunks_path, kb=None):
    """Dry runs only: a source maps to a chunk by (source, title, path); an ambiguous key takes its first chunk. The entries are checked against the index."""
    by = {}
    for c in (json.loads(l) for l in Path(chunks_path).read_text(encoding="utf-8").splitlines() if l.strip()):
        by.setdefault(chunk_key(c), []).append(c["id"])

    def resolve(question, expansion, sources, kb_info=None):
        if (kb is None) != (kb_info is None):
            raise Refused("the answer's kb_entries and the arm's entries setting disagree")
        n_ent = len((kb_info or {}).get("added") or [])
        ids = []
        for s in sources[:len(sources) - n_ent]:
            c = by.get(chunk_key(s)) or []
            if not c:
                raise Refused("a source maps to no chunk (keys resolver)")
            ids.append(c[0])     # an ambiguous key takes its first chunk: a stub run only; the real resolver recomputes the pack
        return ids + (entry_tail(kb, kb_info, sources[len(sources) - n_ent:]) if kb else [])
    return resolve


def retrieval_resolver(chunks_path, kb=None):
    """The real pack: the frozen retrieval on the question and the returned expansion, as `ask` does it (CPU, the reranker), then for an entries arm the entries merge with the frozen
    reranker (KbEntries.merge, as the runtime does it). Must equal the sources position by position, and the added entries, the skipped counts, the candidates and the pack size
    must equal what the answer recorded."""
    from kafka_sre_rag import retrieval

    def same(got, srcs):
        for g, s in zip(got, srcs):
            if chunk_key(g) != chunk_key(s):
                raise Refused("the recomputed pack differs from the answer's sources")
            if g.get("ce_score") is not None and s.get("ce_score") is not None and abs(float(g["ce_score"]) - float(s["ce_score"])) > 1e-3:
                raise Refused("the recomputed pack's scores differ from the answer's")

    def resolve(question, expansion, sources, kb_info=None):
        if (kb is None) != (kb_info is None):
            raise Refused("the answer's kb_entries and the arm's entries setting disagree")
        n_ent = len((kb_info or {}).get("added") or [])
        nf = len(sources) - n_ent
        got = retrieval.select(question, expansion, k="k6", index_path=chunks_path) if chunks_path else retrieval.select(question, expansion, k="k6")
        if len(got) != nf:
            raise Refused("the recomputed pack has another size than the answer's sources")
        same(got, sources[:nf])
        if kb is None:
            return [g["id"] for g in got]
        items, info = kb.merge(got, question, expansion, rerank=True)
        if info["added"] != list((kb_info or {}).get("added") or []):
            raise Refused("the recomputed entries differ from the entries the answer recorded")
        for k in ("skipped", "candidates", "pack_chars"):
            if info.get(k) != kb_info.get(k):
                raise Refused(f"the recomputed entries merge differs from the recorded one ({k})")
        same(items[nf:], sources[nf:])
        entry_tail(kb, kb_info, sources[nf:])
        return [g["id"] for g in got] + [it["id"] for it in items[nf:]]
    return resolve


def cmd_packs(a):
    """Recover each answer's pack. A pack that does not match is recorded as `pack_failed` on its row (the answer itself is not an error and is not redone); the stage then
    refuses (exit 2: the guard is run without retries) and refuses again on the next start while one exists, until the cause is understood and --retry-failed is given."""
    hid = a.set == "h6"
    if a.resolver == "keys" and not a.stub:
        raise Refused("the keys resolver is for stub runs only")
    items = {it["id"]: it for it in E.read_items(a.items)}
    rows = load_rows(a.answers)
    earlier = [r for r in rows if r.get("pack_failed")]
    if earlier and not a.retry_failed:
        raise Refused(f"{len(earlier)} pack(s) of this answer file failed earlier (the answers are kept); find the cause, then run with --retry-failed; nothing was done")
    for r in earlier:
        r["pack_failed"] = None
    kb = None
    if a.configs:
        info = arm_info(load_configs(a.configs), a.arm)
        if info["entries"]:
            from kafka_sre_rag.kb_entries import KbEntries, KbEntriesError
            try:
                kb = KbEntries.load(str(ROOT / info["entries_record"]))
            except KbEntriesError as e:
                raise Refused(f"the entries of {a.arm} cannot be loaded: {e}") from None
    resolve = keys_resolver(a.chunks, kb) if a.resolver == "keys" else retrieval_resolver(a.chunks if a.chunks_override else None, kb)
    tag = f"[packs {a.arm} {a.set}]"
    todo = [r for r in rows if not r.get("error") and not r.get("pack")]
    print(f"{tag} {len(rows)} rows, {len(todo)} to resolve", flush=True)
    bad = 0
    for i, r in enumerate(todo, 1):
        try:
            r["pack"] = resolve(E.full_question(items[r["id"]]), r.get("search_terms") or "", r.get("sources") or [], r.get("kb_entries"))
            r["pack_check"] = "exact"
        except Refused as e:
            r["pack_failed"] = "pack not reproduced"
            r["pack_failed_detail"] = None if hid else str(e)
            bad += 1
        save_rows(a.answers, rows)
        print(f"{tag} {i}/{len(todo)} {'ok' if not r.get('pack_failed') else 'failed'}" + ("" if hid else f" {r['id']}"), flush=True)
    if bad:
        print(f"{tag} REFUSED: {bad} pack(s) were not recovered exactly; the answers are kept and not marked as errors; the chain stops here", flush=True)
        return 2
    return 0


# ---------------------------------------------------------------- judge wrapper

SAFE_JUDGE = re.compile(r"^(matched |instruction |items |settings |\d+ judgments|judge totals|wrote |judge time|  server busy|  retry)")
JUDGE_LINE = re.compile(r"^\[judge\] (\d+) cached\s+\S+ \S+ p(\d) valid=(\w+)")


def cmd_judge_run(a):
    hid = a.hidden
    script = str(ROOT / "scripts/eval2_judge.py")
    if "--stub" in a.rest and os.environ.get("M7_JUDGE_SCRIPT"):      # a stub run only: the tests plant invalid replies through a wrapper of the judge
        script = os.environ["M7_JUDGE_SCRIPT"]
    cmd = [a.python, script, "run", *a.rest]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, cwd=ROOT)
    withheld = 0
    for line in p.stdout:
        line = line.rstrip("\n")
        if not hid:
            print(line, flush=True)
            continue
        m = JUDGE_LINE.match(line)
        if m:
            print(f"[judge] {m.group(1)} calls done, pass {m.group(2)}, valid={m.group(3)}", flush=True)
        elif SAFE_JUDGE.match(line):
            print(line, flush=True)
        else:
            withheld += 1
    rc = p.wait()
    if hid and withheld:
        print(f"[judge] {withheld} output line(s) withheld (hidden run)", flush=True)
    return rc


def cmd_judge_check(a):
    rows = E.read_jsonl(a.out) if Path(a.out).exists() else []
    valid = sum(1 for r in rows if r.get("valid_reply"))
    print(f"judge rows: {len(rows)} (expected {a.expect}), valid {valid}, unscored {len(rows) - valid}")
    return 0 if len(rows) == a.expect and valid == len(rows) else 1


# ---------------------------------------------------------------- server

def server_base():
    u = (os.environ.get("OMLX_URL") or "").rstrip("/")
    if not u:
        raise Refused("no server address (OMLX_URL)")
    return u[:-3] if u.endswith("/v1") else u


def http(method, path, timeout=30):
    import urllib.request
    req = urllib.request.Request(server_base() + path, method=method, headers={"Authorization": "Bearer " + os.environ.get("OMLX_API_KEY", "")})
    with urllib.request.urlopen(req, timeout=timeout) as r:   # noqa: S310 (the Owner's own server)
        return r.status, r.read().decode("utf-8", "replace")


def kb2(*args):
    p = subprocess.run([sys.executable, str(ROOT / "scripts/m7_kb2_rule.py"), *args], capture_output=True, text=True)
    return p.returncode, p.stdout, p.stderr


def get_status(a):
    if a.stub_file:
        rc, out, _ = kb2("stub-status", "--file", a.stub_file)
        return json.loads(out)
    try:
        st, body = http("GET", "/api/status")
        status = json.loads(body)
    except Exception as e:  # noqa: BLE001
        raise Refused(f"the server status is unreadable ({type(e).__name__})") from None
    try:      # /api/status has no load times; the admin list has each loaded model's last access (epoch seconds). Never fatal: without it the gate refuses a model it cannot date.
        _, body = http("GET", "/admin/api/models")
        access = last_access_of(json.loads(body))
        if access is not None:
            status["model_last_access"] = access
    except Exception:  # noqa: BLE001
        pass
    return status


def last_access_of(admin):
    """{id: last_access} of the loaded models whose last_access is a number, from the /admin/api/models document; None when it has not the expected shape."""
    models = admin.get("models") if isinstance(admin, dict) else None
    if not isinstance(models, list):
        return None
    return {m["id"]: float(m["last_access"]) for m in models
            if isinstance(m, dict) and m.get("loaded") and isinstance(m.get("id"), str)
            and isinstance(m.get("last_access"), (int, float)) and not isinstance(m.get("last_access"), bool)}


def cmd_server(a):
    if a.action == "status":
        st = get_status(a)
        print(json.dumps({k: st.get(k) for k in ("active_requests", "waiting_requests", "loaded_models")}))
        return 0
    if a.action == "gate":
        st = get_status(a)
        tmp = Path(a.work) / "server_status.json"
        tmp.write_text(json.dumps(st), encoding="utf-8")
        args = ["server-gate", "--status", str(tmp), "--need", a.need, "--w16", a.w16 or DS_MODEL]
        for m in a.marker:
            args += ["--marker", m]
        rc, out, err = kb2(*args)
        sys.stdout.write(out)
        sys.stderr.write(err)
        return rc
    if a.action == "unload":
        n = 0
        while True:       # no active or waiting request, then the unload (never for another client's model: the gate decided which ids come here)
            st = get_status(a)
            if int(st.get("active_requests", 1)) + int(st.get("waiting_requests", 0)) == 0:
                break
            n += 1
            if n > 120:
                raise Refused("the server is still busy after 60 minutes; nothing was unloaded")
            time.sleep(0 if a.stub_file else 30)
        if a.stub_file:
            kb2("stub-unload", "--file", a.stub_file, "--model", a.model)
            print(f"unloaded {a.model} (stub server)")
            return 0
        code, body = http("POST", f"/admin/api/models/{a.model}/unload", timeout=300)
        if not 200 <= code < 300:
            raise Refused(f"the server answered {code} to the unload of {a.model}")
        w = 0
        while a.model in (get_status(a).get("loaded_models") or []):
            time.sleep(5)
            w += 5
            if w > 600:
                raise Refused(f"{a.model} is still loaded 600 s after the unload was accepted")
        print(f"unloaded {a.model}; the server answered {code}; gone after {w} s")
        return 0
    if a.action == "load":     # the stub server loads a model on the first request: after the marker was written (M7_STUB_LOAD_DELAY seconds later, for the late-load test)
        if a.stub_file:
            time.sleep(float(os.environ.get("M7_STUB_LOAD_DELAY") or 0))
            kb2("stub-load", "--file", a.stub_file, "--model", a.model)
        return 0
    return 1


# ---------------------------------------------------------------- blind reading bundles

def leak_terms(cfgs, arms):
    t = set()
    for x in arms:
        c = cfgs[x]
        m = c.get("model") or {}
        for k in ("oMLX_model_id", "hf_repo", "base_model"):
            if m.get(k):
                t.add(m[k])
    return sorted(t)


def leak_check(dirs, terms):
    hits = 0
    for d in dirs:
        for f in Path(d).rglob("*"):
            if f.is_file():
                txt = f.read_text(encoding="utf-8", errors="replace").lower()
                hits += sum(1 for t in terms if t.lower() in txt)
    return hits


def run_py(args, hidden=False, quiet=False):
    p = subprocess.run([sys.executable, *args], capture_output=True, text=True, cwd=ROOT)
    if not (hidden or quiet):
        sys.stdout.write(p.stdout)
        sys.stderr.write(p.stderr)
    elif p.returncode != 0:      # hidden: the helper scripts' own messages name counts and classes only, but nothing is forwarded but the exit code
        print(f"(a sealed helper step failed with exit {p.returncode}; its output is withheld)", file=sys.stderr)
    return p


def cmd_bundle(a):
    cfgs = load_configs(a.configs)
    arms = [x for x in a.arms.split(",") if x]
    sets = {x: p for x, p in (s.partition("=")[::2] for s in a.answers)}
    if a.set == "d7":
        out = Path(a.out)
        first, second, export = out / "first", out / "second", out / "export_second_reader"
        for d in (first, second, export):
            if d.exists() and any(d.iterdir()):
                raise Refused(f"{d.name} exists and is not empty; not overwriting")
        key = out / "KEY.json"
        if len(sets) < 2:     # eval2_blind.py needs two or more answer sets: with one arm there is nothing to shuffle across configurations
            out.mkdir(parents=True, exist_ok=True)
            (out / "NO_BUNDLE_single_arm.txt").write_text("One arm only: scripts/eval2_blind.py needs two or more answer sets, so no blind bundle was made.\n", encoding="utf-8")
            print("d7: one arm only; eval2_blind.py makes no bundle for a single answer set (nothing to shuffle across configurations); no reading bundle was made")
            return 0
        p = run_py([str(ROOT / "scripts/eval2_blind.py"), "bundle", "--items", a.items, "--rubrics", a.rubrics, "--answers", *a.answers, "--chunks", a.chunks,
                    "--out", str(first), "--key", str(key), "--seed", str(SEEDS["bundle"]), "--second-reader", str(a.second_fraction), "--second-out", str(second),
                    "--harm-consequences"], quiet=True)
        sys.stdout.write("\n".join(l for l in p.stdout.splitlines() if l.startswith(("bundle", "second bundle"))) + "\n")
        if p.returncode:
            sys.stderr.write(p.stderr)
            return 1
        shutil_copytree(second, export)
        terms = leak_terms(cfgs, arms)
        n = leak_check([first, second, export], terms)
        if n:
            print(f"REFUSED: {n} bundle file(s) name a model; remove the bundles before reading", file=sys.stderr)
            return 2
        print(f"d7 bundles: first reader {first}, second family's export (D7 only; no key, no answers file) {export}; seed {SEEDS['bundle']}; key {key} (no reader opens it)")
        return 0
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    p = run_py([str(ROOT / "scripts/eval2_sealed_reading.py"), "bundle", "--items", a.items, "--rubrics", a.rubrics, "--answers", *a.answers, "--chunks", a.chunks,
                "--out-dir", str(out), "--seed", str(SEEDS["bundle"]), "--first-sessions", str(a.sessions), "--second-sessions", "0"], hidden=True)
    if p.returncode:
        return 1
    n_b = len(list(out.glob("first_*.jsonl.enc")))
    print(f"h6 sealed bundles: {n_b} first-reader bundle(s), no export, no second family; seed {SEEDS['bundle']}")
    return 0


def shutil_copytree(src, dst):
    import shutil
    shutil.copytree(src, dst, dirs_exist_ok=True)


# ---------------------------------------------------------------- unblinding

def cmd_unblind(a):
    if a.set == "d7":
        readers = [f"{n}={p}" for n, p in (s.partition("=")[::2] for s in a.findings)]
        args = [str(ROOT / "scripts/eval2_blind.py"), "unblind", "--key", a.key, "--rubrics", a.rubrics, "--answers", *a.answers, "--findings", *readers,
                "--out", a.out, "--reads-out", a.reads_out, "--require-consequence"]
        if a.scope:
            args += ["--scope", *a.scope]
        p = run_py(args)
        return p.returncode
    # sealed: the findings of the first reader, validated as the sealed totals do; one encrypted row per finding with the arm and the scenario
    import eval2_sealed_reading as SR
    d = Path(a.dir)
    key = json.loads(E.decrypt_bytes((d / SR.KEY_NAME).read_bytes()).decode("utf-8"))
    text_of = {}
    for b in key["bundles"]:
        for r in SR.read_rows(d / f"{b}.jsonl.enc"):
            text_of[r["id"]] = r["answer"]
    out, rejected, seen = [], 0, set()
    for b, names in key["bundles"].items():
        f = d / f"findings_{b}.jsonl.enc"
        if not f.exists():
            print(f"no findings file for {len(key['bundles'])} bundle(s) expected", file=sys.stderr)
            return 1
        covered = set()
        for r in SR.read_rows(f):
            if r.get("answer") in names:
                covered.add(r["answer"])
            if r.get("kind") == "clean":
                continue
            why = SR.validate(r, set(names), key, text_of)
            sig = (r.get("answer"), r.get("kind"), SR.norm(r.get("quote")), r.get("point") or "")
            if why is None and sig in seen:
                why = "duplicate"
            seen.add(sig)
            if why:
                rejected += 1
                continue
            out.append({"set": key["aid"][r["answer"]], "id": r["id"], "reader": a.reader, "kind": r["kind"], "quote": r.get("quote") or "", "minor": bool(r.get("minor")),
                        "note": r.get("note") or "", "consequence": r.get("consequence") or "", "source": r.get("source") or ""})
        if len(set(names) - covered):
            print(f"{len(set(names) - covered)} answer(s) of a bundle have neither a finding nor a clean row", file=sys.stderr)
            return 1
    E.write_jsonl(a.out, out)
    print(f"h6: {len(out)} findings valid, {rejected} rejected (counts only); written encrypted")
    return 0


# ---------------------------------------------------------------- the adjudication pack's input

def cmd_pack_input(a):
    cfgs = load_configs(a.configs)
    for x in [x for x in a.arms.split(",") if x]:
        arm_info(cfgs, x)
    hid = a.set == "h6"
    items = {it["id"]: it for it in E.read_items(a.items)}
    answers, flags = [], []
    flag_ids = None
    for arm, path in (s.partition("=")[::2] for s in a.answers):
        for r in load_rows(path):
            if r.get("error"):
                continue
            answers.append({"answer_id": f"{a.set}-{arm}-{r['id']}", "set": a.set, "scenario_id": r["id"], "config": arm, "text": r["text"], "pack_chunk_ids": r.get("pack") or []})
    ids = {x["answer_id"] for x in answers}
    for f in E.read_jsonl(a.findings):
        aid = f"{a.set}-{f['set']}-{f['id']}"
        if aid not in ids or f["kind"] not in ("harmful", "wrong", "invented"):
            continue
        if f["kind"] == "harmful":
            kind = "harm"
        elif f.get("minor"):
            continue
        else:
            kind = "material_error"
        flags.append({"answer_id": aid, "reader": f["reader"], "kind": kind, "quote": f["quote"], "rationale": f.get("note") or "",
                      "plausible_consequence": f.get("consequence") or "", "source_chunk_ids": []})
    scen = [{"scenario_id": i, "question": it["question"], "evidence": it.get("evidence") or ""} for i, it in items.items()]
    outd = Path(a.out_dir)
    outd.mkdir(parents=True, exist_ok=True)
    ext = ".enc" if hid else ""
    for name, rows in (("answers", answers), ("scenarios", scen), ("flags", flags)):
        E.write_jsonl(outd / f"{name}.jsonl{ext}", rows)
    n_h = sum(1 for f in flags if f["kind"] == "harm")
    print(f"pack input for {a.set}: {len(answers)} answers, {len(scen)} scenarios, {n_h} harm flags, {len(flags) - n_h} material-error flags; seeds shuffle {SEEDS['pack_shuffle']} sample {SEEDS['pack_sample']}")
    full = f" --full-sets {a.full_sets}" if a.full_sets else ""
    if hid:
        print("h6: the files are encrypted; the sealed pack is built with `--sealed` (counts only; the key is written as key.json.enc; the HTML is the Owner's reading copy, written to --html-out):")
    print(f"build: scripts/m7_adjudication_pack.py build --answers {outd}/answers.jsonl{ext_of(hid)} --scenarios {outd}/scenarios.jsonl{ext_of(hid)} --flags {outd}/flags.jsonl{ext_of(hid)} "
          f"--out DIR --configs {a.configs} --shuffle-seed {SEEDS['pack_shuffle']} --sample-seed {SEEDS['pack_sample']}{full}" + (f" --chunks {a.chunks}" if a.chunks else "") + (" --sealed --html-out PATH" if hid else ""))
    return 0


def ext_of(hid):
    return ".enc" if hid else ""


# ---------------------------------------------------------------- measures

def adjudication_of(verdicts_path, key_path, set_name):
    """-> (totals {config: {...}}, harmful answer ids {config: set}) for one set, from the Owner's verdicts and the pack's key; None when absent.
    H counts every answer the SRE found harmful: the upheld flags, the harm in the sampled unflagged answers and the harm in the unflagged answers read in full (H_plus of the pack's scorer).
    h6 verdicts and key carry quotes and notes: they are accepted as .enc files only."""
    if set_name == "h6":
        for pth in (verdicts_path, key_path):
            if pth and not str(pth).endswith(".enc"):
                raise Refused("h6 verdicts and the pack key are accepted as .enc files only (they hold quotes and notes); seal the Owner's export with scripts/m7_adjudication_pack.py seal")
    if not (verdicts_path and key_path and Path(verdicts_path).exists() and Path(key_path).exists()):
        return None, {}
    v = json.loads(E.read_text(verdicts_path))
    key = json.loads(E.read_text(key_path))
    verdicts = v.get("verdicts", {})
    tot, upheld = {}, {}
    for k, n in key["denominators"].items():
        s, _, c = k.partition("|")
        if s == set_name:
            tot[c] = {"answers": n, "H": 0, "pending": 0, "samples_drawn": 0, "samples_read": 0, "sample_harm": 0, "full_read": set_name in (key.get("full_sets") or [])}
            upheld[c] = set()
    for pid, it in key["items"].items():
        if it["set"] != set_name:
            continue
        t, vd = tot[it["config"]], verdicts.get(pid, {})
        if it["kind"] == "flag":
            s2 = vd.get("stage2")
            if not s2:
                t["pending"] += 1
            elif s2["harmful"]:
                upheld[it["config"]].add(it["answer_id"])
        elif it["kind"] == "read":
            s1 = vd.get("stage1")
            if not s1:
                t["pending"] += 1
            elif s1["harmful"]:
                upheld[it["config"]].add(it["answer_id"])
        else:
            t["samples_drawn"] += 1
            s1 = vd.get("stage1")
            if not s1:
                t["pending"] += 1
            else:
                t["samples_read"] += 1
                if s1["harmful"]:
                    t["sample_harm"] += 1
                    upheld[it["config"]].add(it["answer_id"])
    for c in tot:
        tot[c]["H"] = len(upheld[c])
        tot[c]["upheld_ids"] = sorted(upheld[c])
    return tot, upheld


def cmd_measures(a):
    import eval2_judge as EJ
    arms = [x for x in a.arms.split(",") if x]
    items = E.read_items(a.items)
    ids = [it["id"] for it in items]
    rub = {r["id"]: r for r in E.read_jsonl(a.rubrics)}
    judge = [r for j in a.judge if Path(j).exists() for r in E.read_jsonl(j) if r.get("valid_reply")]
    findings = E.read_jsonl(a.findings) if a.findings and Path(a.findings).exists() else None
    if a.set == "h6":
        for pth in (*a.judge, a.findings, a.out):
            if pth and not str(pth).endswith(".enc"):
                raise Refused("h6: the judge output, the findings and the measures are .enc files only")
    adj, _ = adjudication_of(a.verdicts, a.pack_key, a.set)
    out = {"set": a.set, "arms": {}, "adjudication": None, "reading_complete": findings is not None, "first_reader": a.first_reader}
    for arm, path in (s.partition("=")[::2] for s in a.answers):
        if arm not in arms:
            continue
        ans = {r["id"]: r for r in load_rows(path)}
        recs = []
        for iid in ids:
            r = ans.get(iid)
            rec = {"id": iid, "error": None, "words": 0, "seconds": None, "cut": False, "P": None, "essential_missed": None, "points_absent": None,
                   "wrong": None, "material": None, "harm_flag": None, "harm_upheld": None}
            if r is None or r.get("error") or not (r.get("text") or "").strip():
                rec["error"] = (r or {}).get("error") or "missing"
            else:
                rec.update(words=words_of(r["text"]), seconds=secs_of(r), cut=bool(r.get("truncated")))
                js = [x for x in judge if x["set"] == arm and x["id"] == iid]
                if js:
                    cs = [EJ.coverage_row(x, rub[iid]) for x in js]
                    rec["P"] = sum(c["P"] for c in cs) / len(cs)
                    rec["essential_missed"] = sum(sum(1 for u in c["units"] if u["status"] == "missed") for c in cs) / len(cs)
                    rec["points_absent"] = sum(sum(1 for e in x["must"].values() if e["verdict"] == "absent") for x in js) / len(js)
                    rec["passes_valid"] = len(js)
                if findings is not None:
                    fs = [f for f in findings if f["set"] == arm and f["id"] == iid]
                    rec["wrong"] = sum(1 for f in fs if f["reader"] == a.first_reader and f["kind"] in ("wrong", "invented") and not f.get("minor"))
                    rec["material"] = any(f["kind"] in ("wrong", "invented") and not f.get("minor") for f in fs)
                    rec["harm_flag"] = any(f["kind"] == "harmful" for f in fs)
                if adj is not None and arm in adj:
                    rec["harm_upheld"] = f"{a.set}-{arm}-{iid}" in set(adj[arm]["upheld_ids"])
            recs.append(rec)
        out["arms"][arm] = {"records": recs}
    if adj is not None:
        out["adjudication"] = {c: {k: v for k, v in t.items() if k != "upheld_ids"} for c, t in adj.items()}
    if a.tag_file and Path(a.tag_file).exists():
        tags = json.loads(Path(a.tag_file).read_text(encoding="utf-8")).get("tags", {})
        if a.set == "h6" and H6_TAG in tags:
            out["tag_exclude"] = {H6_TAG: [i for i in tags[H6_TAG]["ids"] if i in set(ids)]}
    blob = (json.dumps(out, indent=1) + "\n").encode()
    E.write_atomic(a.out, E.encrypt_bytes(blob) if E.is_hidden(a.out) else blob)
    n = {k: len(v["records"]) for k, v in out["arms"].items()}
    print(f"measures {a.set}: {n} records; judged {sum(1 for v in out['arms'].values() for r in v['records'] if r['P'] is not None)}; reading {'in' if findings is not None else 'pending'}; "
          f"adjudication {'in' if adj is not None else 'pending'}")
    return 0


# ---------------------------------------------------------------- stub ask (dry runs)

def cmd_stub_ask(a, rest):
    """Stands in for `python -m kafka_sre_rag ask <flags> --json QUESTION`: the same arguments, a deterministic JSON line. M7_STUB_CHUNKS names the chunk file;
    M7_STUB_FAIL names a substring of a question whose call fails (exit 1, and the question is echoed to stderr, as a bad runtime might)."""
    flags, q = [f for f in rest[:-1] if f != "--"], rest[-1]
    fail = os.environ.get("M7_STUB_FAIL")
    if fail and fail in q:
        print("stub failure for: " + q, file=sys.stderr)
        return 1
    model = flags[flags.index("--model") + 1] if "--model" in flags else "stub"
    bad = os.environ.get("M7_STUB_BAD_SOURCE")
    chunks_path = os.environ.get("M7_STUB_CHUNKS") or str(ROOT / "work/v3/chunks_s1.jsonl")
    chunks = [json.loads(l) for l in Path(chunks_path).read_text(encoding="utf-8").splitlines() if l.strip()]
    seen, srcs, pack = set(), [], []
    h = int(hashlib.sha256((model + q).encode()).hexdigest(), 16)
    start = int(hashlib.sha256(q.encode()).hexdigest(), 16) % len(chunks)
    for j in range(min(6, len(chunks))):
        c = chunks[(start + j) % len(chunks)]
        if chunk_key(c) in seen:
            continue
        seen.add(chunk_key(c))
        pack.append(c)
        srcs.append({"source": c["source"], "title": c["title"], "path": c["path"], "bm25_rank": j + 1, "ce_score": round(1.0 - j * 0.1, 3)})
    kb_info = None
    if "--kb-entries" in flags:       # the runtime's own merge (KbEntries.merge), BM25 order only (no reranker is loaded in a stub run)
        from kafka_sre_rag.kb_entries import KbEntries
        kb = KbEntries.load(str(ROOT / flags[flags.index("--kb-entries-record") + 1]) if "--kb-entries-record" in flags else None)
        pack, kb_info = kb.merge(pack, q, "stub terms", rerank=False)
        srcs += [{"source": c["source"], "title": c["title"], "path": c["path"], "bm25_rank": c.get("bm25_rank"), "ce_score": c.get("ce_score")} for c in pack[len(srcs):]]
    if os.environ.get("M7_STUB_RECEIVED"):      # tests: the pack text the stub model "received", by question hash
        from kafka_sre_rag import prompts
        Path(os.environ["M7_STUB_RECEIVED"], hashlib.sha256((q + ("|kbe" if kb_info is not None else "")).encode()).hexdigest()[:16] + ".txt").write_text(prompts.pack_text(pack), encoding="utf-8")
    if bad and bad in q and srcs:      # a source that is in no chunk: the pack cannot be recovered
        srcs[0] = dict(srcs[0], path="raw/does-not-exist.md")
    text = f"Stub answer {h % 1000} to: {q[:40]}. " + " ".join(f"word{k}" for k in range(20 + h % 50))
    out = {"question": q, "text": text, "sources": srcs, "expansion": "stub terms", "timings": {"expand_s": 0.1, "retrieve_s": 0.2, "answer_s": 1.0 + (h % 7)},
           "measured": False, "tokens": 40 + h % 30, "truncated": False, "unmeasured_reasons": ["stub"]}
    if "--lookup-cards" in flags:
        out["lookup_cards"] = {"cards_found": 1, "chars": 100, "seconds": 0.1, "catalogue_sha256": "stub"}
    if kb_info is not None:
        out["kb_entries"] = kb_info
    print(json.dumps(out, ensure_ascii=False))
    return 0


# ---------------------------------------------------------------- the fact entries (W43), the judge stages of a folder that grows, the descriptive line

def cmd_entries_arms(a):
    cfgs = load_configs(a.configs)
    print(" ".join(x for x in a.arms.split(",") if x and arm_info(cfgs, x)["entries"]))
    return 0


def cmd_chunks_plus(a):
    """The chunk file as it is, then every entry of the entries index as a chunk-shaped row: id `kbe:<entry id>`, and source, title, path and text exactly as the runtime puts the entry in the
    prompt (kb_entry.render: the title line and the text), so that the judge, the readers' excerpts and the adjudication pack show an RE answer's whole pack. The rows do not depend on any
    answer (h6 included). An existing file is kept when it is the same, refused when it differs."""
    from kafka_sre_rag.kb_entries import KbEntries, KbEntriesError
    info = arm_info(load_configs(a.configs), a.arm)
    if not info["entries"]:
        raise Refused(f"arm {a.arm} has no fact entries")
    try:
        kb = KbEntries.load(str(ROOT / info["entries_record"]))
    except KbEntriesError as e:
        raise Refused(f"the entries cannot be loaded: {e}") from None
    lines = [l for l in Path(a.chunks).read_text(encoding="utf-8").splitlines() if l.strip()]
    for eid in sorted(kb.byid):
        it = kb.item(eid)
        lines.append(json.dumps({"id": it["id"], "source": it["source"], "title": it["title"], "path": it["path"], "text": it["text"], "entry_id": eid, "entry": True}, ensure_ascii=False))
    blob = ("\n".join(lines) + "\n").encode("utf-8")
    out = Path(a.out)
    if out.exists():
        if out.read_bytes() != blob:
            raise Refused(f"{out} exists and differs from the chunk file plus the entries now (the chunk file or the entries changed); nothing was overwritten")
        print(f"chunks plus entries: {out.name} unchanged ({len(kb.byid)} entries, sha256 {hashlib.sha256(blob).hexdigest()[:16]})")
        return 0
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(out.name + ".tmp")
    tmp.write_bytes(blob)
    tmp.replace(out)
    print(f"chunks plus entries: {out.name} written ({len(lines) - len(kb.byid)} chunks, {len(kb.byid)} entries, sha256 {hashlib.sha256(blob).hexdigest()[:16]})")
    return 0


def judge_plan(work):
    """-> (groups, complete). A judge stage is done by its marker stage_judge_<arms joined by _>.done (the first group of a folder: every arm it had then) or stage_judge_add_<arm>.done (an arm
    added later: its own stage, files judge_<set>_add_<arm>.jsonl and cache, so that the finished judge rows are never rewritten). groups: [(stage, [arms], file suffix)] still to run."""
    wd = Path(work)
    marms = list(json.loads((wd / "MANIFEST.json").read_text(encoding="utf-8"))["arms"])
    if any("_" in x for x in marms):
        raise Refused("an arm name with an underscore cannot be tracked by the judge stage markers")
    covered, first = set(), False
    for f in sorted(wd.glob("stage_judge_*.done")):
        name = f.name[len("stage_judge_"):-len(".done")]
        if name.startswith("add_"):
            covered.add(name[4:])
        else:
            covered |= set(name.split("_"))
            first = True
    pending = [x for x in marms if x not in covered]
    if not pending:
        return [], True
    if not first:
        return [("judge_" + "_".join(pending), pending, "")], False
    return [(f"judge_add_{x}", [x], f"_add_{x}") for x in pending], False


def cmd_judge_plan(a):
    groups, complete = judge_plan(a.work)
    for stage, arms, suffix in groups:
        print(f"GROUP {stage} {','.join(arms)} {suffix or '-'}")
    print("COMPLETE " + ("yes" if complete else "no"))
    return 0


def cmd_descriptive(a):
    """Descriptive paired lines candidate - reference per set (RE - R: the entries on against off): coverage, essential points missed, seconds. No bar reads them."""
    L = []
    for spec in a.measures:
        name, _, path = spec.partition("=")
        m = RR.load_measures(path)
        rows = {x: v["records"] for x, v in m["arms"].items()}
        if a.candidate not in rows or a.reference not in rows:
            continue
        for key, label in (("P", "judged coverage"), ("essential_missed", "essential points missed per answer"), ("seconds", "seconds")):
            d, nsc, miss = RR.paired(rows[a.candidate], rows[a.reference], key)
            if d and len(miss) <= RR.MAX_MISSING:
                mean, lo, hi = RR.bootstrap_ci(d, RR.RESAMPLES)
                L.append(f"- Set {name}, paired {a.candidate} - {a.reference}, {label}: {RR.f3(mean)}, 90 % interval [{RR.f3(lo)}, {RR.f3(hi)}] over {len(d)} scenarios (descriptive).")
            else:
                L.append(f"- Set {name}, paired {a.candidate} - {a.reference}, {label}: not measured ({len(miss)} of {nsc} scenarios incomplete).")
    out = ["### Descriptive: " + f"{a.candidate} - {a.reference} (the fact entries on against off; descriptive only, no bar reads it)", ""] + (L or ["- not measured: both arms are not in this reading."]) + [""]
    Path(a.out).write_text("\n".join(out), encoding="utf-8")
    print(f"descriptive {a.candidate} - {a.reference}: {len(L)} line(s)")
    return 0


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] == "stub-ask":
        return cmd_stub_ask(None, argv[1:])
    if argv and argv[0] == "judge-run":       # judge-run [--hidden] [--python PY] -- ARGS OF eval2_judge.py run
        if "--" not in argv:
            print("usage: judge-run [--hidden] [--python PY] -- ARGS", file=sys.stderr)
            return 2
        i = argv.index("--")
        ns = argparse.Namespace(hidden="--hidden" in argv[1:i], python=(argv[argv.index("--python") + 1] if "--python" in argv[1:i] else sys.executable), rest=argv[i + 1:])
        return cmd_judge_run(ns)
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)

    def configs(p):
        p.add_argument("--configs", default=str(ROOT / "eval/m7/release_test/configs.json"))
        p.add_argument("--arms", default="R,C")

    p = sub.add_parser("arms"); configs(p)
    p = sub.add_parser("arm-order"); configs(p)
    p.add_argument("--candidate", default="R")
    p = sub.add_parser("runtime-check"); configs(p)
    p.add_argument("--arm", required=True)
    p = sub.add_parser("entries-arms"); configs(p)
    p = sub.add_parser("chunks-plus"); configs(p)
    p.add_argument("--arm", required=True)
    p.add_argument("--chunks", required=True)
    p.add_argument("--out", required=True)
    p = sub.add_parser("judge-plan")
    p.add_argument("--work", required=True)
    p = sub.add_parser("descriptive")
    p.add_argument("--measures", nargs="+", required=True, metavar="SET=PATH")
    p.add_argument("--candidate", default="RE")
    p.add_argument("--reference", default="R")
    p.add_argument("--out", required=True)
    p = sub.add_parser("manifest-arms")
    p.add_argument("--work", required=True)
    p = sub.add_parser("manifest"); configs(p)
    p.add_argument("--work", required=True)
    p.add_argument("--set", required=True, choices=sorted(SET_COUNT))
    p.add_argument("--candidate", default="R")
    p.add_argument("--control", default="C")
    p.add_argument("--h6-mode", default="full", choices=list(RR.H6_MODES))
    p.add_argument("--h6-chosen-by", default="")
    p.add_argument("--allow-late-arms", action="store_true", help="W43: an arm may be added after the reading bundles were made, and its candidate rule text recorded (logged in the manifest)")
    p.add_argument("--pin", action="append", default=[], help="strict: a set's items and rubrics")
    p.add_argument("--log-pin", action="append", default=[], help="logged, not compared: helper scripts, the chunk file")
    p.add_argument("--opt", action="append", default=[])
    p = sub.add_parser("ledger-check"); configs(p)
    p.add_argument("--ledger", required=True)
    p.add_argument("--configs-sha256")
    p.add_argument("--rule-sha256", required=True)
    p = sub.add_parser("answer"); configs(p)
    p.add_argument("--arm", required=True)
    p.add_argument("--set", required=True, choices=sorted(SET_COUNT))
    p.add_argument("--items", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--ledger")
    p.add_argument("--rule-sha256", default="")
    p.add_argument("--python")
    p.add_argument("--stub", action="store_true")
    p.add_argument("--limit", type=int, default=0)
    p = sub.add_parser("check-inputs")
    p.add_argument("--set", required=True, choices=sorted(SET_COUNT))
    p.add_argument("--items", required=True)
    p.add_argument("--rubrics", required=True)
    p.add_argument("--stub", action="store_true")
    p.add_argument("--limit", type=int, default=0)
    p = sub.add_parser("packs")
    p.add_argument("--set", required=True)
    p.add_argument("--arm", required=True)
    p.add_argument("--items", required=True)
    p.add_argument("--answers", required=True)
    p.add_argument("--chunks", default="work/v3/chunks_s1.jsonl")
    p.add_argument("--chunks-override", action="store_true")
    p.add_argument("--resolver", choices=["retrieval", "keys"], default="retrieval")
    p.add_argument("--configs", default=None, help="configs.json: an arm with the fact entries on has its entries merged in the recovery")
    p.add_argument("--retry-failed", action="store_true")
    p.add_argument("--stub", action="store_true")
    p = sub.add_parser("judge-check")
    p.add_argument("--out", required=True)
    p.add_argument("--expect", type=int, required=True)
    p = sub.add_parser("server")
    p.add_argument("action", choices=["gate", "unload", "status", "load"])
    p.add_argument("--work", default=".")
    p.add_argument("--need", default="")
    p.add_argument("--model", default="")
    p.add_argument("--marker", action="append", default=[])
    p.add_argument("--w16", default=None)
    p.add_argument("--stub-file", default=None)
    p = sub.add_parser("bundle"); configs(p)
    p.add_argument("--set", required=True, choices=sorted(SET_COUNT))
    p.add_argument("--items", required=True)
    p.add_argument("--rubrics", required=True)
    p.add_argument("--answers", nargs="+", required=True, metavar="ARM=PATH")
    p.add_argument("--chunks", default="work/v3/chunks_s1.jsonl")
    p.add_argument("--out", required=True)
    p.add_argument("--second-fraction", type=float, default=1.0)
    p.add_argument("--sessions", type=int, default=6)
    p = sub.add_parser("unblind")
    p.add_argument("--set", required=True, choices=sorted(SET_COUNT))
    p.add_argument("--key")
    p.add_argument("--rubrics")
    p.add_argument("--answers", nargs="*", default=[])
    p.add_argument("--findings", nargs="*", default=[], metavar="READER=PATH")
    p.add_argument("--scope", nargs="*")
    p.add_argument("--out", required=True)
    p.add_argument("--reads-out")
    p.add_argument("--dir")
    p.add_argument("--reader", default="claude")
    p = sub.add_parser("pack-input"); configs(p)
    p.add_argument("--set", required=True, choices=sorted(SET_COUNT))
    p.add_argument("--items", required=True)
    p.add_argument("--answers", nargs="+", required=True, metavar="ARM=PATH")
    p.add_argument("--findings", required=True)
    p.add_argument("--out-dir", required=True)
    p.add_argument("--full-sets", default="")
    p.add_argument("--chunks", default="", help="the chunk file for the pack build (an entries arm: the chunk file plus the entries, so the pinned source can show every item of the pack)")
    p = sub.add_parser("measures")
    p.add_argument("--set", required=True, choices=sorted(SET_COUNT))
    p.add_argument("--arms", default="R,C")
    p.add_argument("--items", required=True)
    p.add_argument("--rubrics", required=True)
    p.add_argument("--answers", nargs="+", required=True, metavar="ARM=PATH")
    p.add_argument("--judge", nargs="*", default=[], help="the judge files of the set (the first group's and any added arm's)")
    p.add_argument("--findings")
    p.add_argument("--first-reader", default="claude")
    p.add_argument("--verdicts")
    p.add_argument("--pack-key")
    p.add_argument("--tag-file", default=str(ROOT / "eval/m4/HIDDEN_TAGS.json"))
    p.add_argument("--out", required=True)
    a = ap.parse_args(argv)
    hidden = getattr(a, "set", None) == "h6" or any(str(x).endswith(".enc") for x in argv)
    try:
        return {"arms": cmd_arms, "manifest": cmd_manifest, "ledger-check": cmd_ledger_check, "answer": cmd_answer, "packs": cmd_packs, "judge-run": cmd_judge_run,
                "arm-order": cmd_arm_order, "runtime-check": cmd_runtime_check, "manifest-arms": cmd_manifest_arms, "server": cmd_server, "bundle": cmd_bundle, "unblind": cmd_unblind, "pack-input": cmd_pack_input, "measures": cmd_measures, "check-inputs": cmd_check_inputs, "judge-check": cmd_judge_check,
                "entries-arms": cmd_entries_arms, "chunks-plus": cmd_chunks_plus, "judge-plan": cmd_judge_plan, "descriptive": cmd_descriptive}[a.cmd](a)
    except Refused as e:
        print(f"REFUSED: {e}", file=sys.stderr)
        return 2
    except E.EvalError as e:
        print(f"error: {type(e).__name__ if hidden else e}", file=sys.stderr)
        return 1
    except Exception as e:  # noqa: BLE001
        if hidden:     # nothing of a hidden run is printed but the class (the guard copies the last line into HEARTBEAT.md)
            print(f"failed: {type(e).__name__} (details withheld for hidden input)", file=sys.stderr)
            return 1
        raise


if __name__ == "__main__":
    sys.exit(main())
