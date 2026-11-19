#!/usr/bin/env python3
"""Assembly, split and manifest of the a3 set of tier A (WP1c T8).

Collects the items of the a3 generators (client code, valid but wrong, safe order, computed sizing, and the
not-covered generator when it exists), checks them against the 496 items of eval2_items_a1 (regenerated in memory,
never read from disk) and against every question text the repository holds, builds the mix and blind-guess tables,
marks the exposed items, splits whole groups to the open and the hidden side, and writes the manifest.

Nothing is written without --write. The default is a dry run: it prints counts, tables, maxima and the hashes the
real run would write, and writes no file. With --write the outputs go to --open-out, --hidden-out (encrypted with
eval2.encrypt_bytes) and --manifest-out; an existing output is never overwritten. The plain build (hidden half
included) only exists in memory; no plain hidden item is written anywhere.

Output is ids and counts only for the hidden half; the item text of a hidden item is never printed.

Split rule: scripts/eval2.py split_families, on the a3 items alone, with the seed and share of the 496 (20261118,
0.5). Items that belong together (an item and its control: an item may carry `pair`; the valid-but-wrong items are
paired by mechanism, or for the forbidden-key template by resource kind) form one group and go to one side. A group
is one family for the split. `split_extend` keeps every id of an earlier split on its side when items are added
later (new groups are decided on the remaining targets), and equals split_families when there is no earlier split.
"""
import argparse
import contextlib
import hashlib
import io
import json
import os
import subprocess
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT))

import eval2  # noqa: E402
import eval2_items_a1 as a1  # noqa: E402

VERSION = "eval2_items_a3/1"
SEED = "m4-a3-1"
SPLIT_SEED = 20261118
SHARE = 0.5
EXPOSED_PER_KIND = 2
OPEN_PATH = "eval/m4/open/tier_a3.jsonl"
HIDDEN_PATH = "eval/hidden/tier_a3.jsonl.enc"
MANIFEST_PATH = "eval/m4/SPLIT_A3.json"
SPLIT_A1 = ROOT / "eval" / "m4" / "SPLIT.json"
OPEN_A1 = ROOT / "eval" / "m4" / "open" / "tier_a.jsonl"

# module, entry point; the order is the order of the build and of the output. client and vbw are keyword-compatible.
MODULES = ("client", "vbw", "order", "sizing", "gap")
KIND_OF = {"client": "client_code", "vbw": "valid_but_wrong", "order": "safe_order", "sizing": "computed_sizing",
           "gap": "not_covered"}
PLAN = {"client_code": 24, "valid_but_wrong": 32, "safe_order": 16, "computed_sizing": 24, "not_covered": 24}
MIN_KIND_ITEMS = 8        # items of a kind on each side, and families of a kind on each side
MIN_KIND_FAMILIES = 8
# Blind guess of an item that carries no `chance`: a control is passed by a blanket "works" or "not in my sources".
DEFAULT_GUESS = {"valid_control": 0.5, "not_covered": 0.5}
LOOKUP_FLOORS = ("near_miss_name", "scope", "confusable_neighbour", "value_type_trap")  # A1's floors on the lookup classes
EXPOSED_A3_TEXT = r'''
'''  # the exposed rows, embedded verbatim after the first real build (empty until then), one JSON object per line


class AssemblyError(RuntimeError):
    pass


# ------------------------------------------------------------------ generators
def _quiet(fn, *a, **k):
    with contextlib.redirect_stdout(io.StringIO()):
        return fn(*a, **k)


def _load(name):
    import importlib
    return importlib.import_module("eval2_items_a3_" + name)


def collect(allow_missing=(), verify=False):
    """(items, notes): the items of every generator module present, in MODULES order, each sorted by id."""
    items, notes = [], []
    for name in MODULES:
        try:
            mod = _load(name)
        except ModuleNotFoundError as e:
            if e.name != "eval2_items_a3_" + name:
                raise
            if name not in allow_missing:
                raise AssemblyError("generator module eval2_items_a3_%s is absent; pass --allow-missing %s to build "
                                    "without it" % (name, name))
            notes.append("generator %s is absent (--allow-missing %s): kind %s is not in this build" % (name, name, KIND_OF[name]))
            continue
        part = _quiet(mod.generate, verify=verify) if name == "client" else _quiet(mod.generate)
        part = sorted(part, key=lambda i: i["id"])
        for it in part:
            if it.get("kind") != KIND_OF[name]:
                raise AssemblyError("%s: item %s has kind %r, expected %r" % (name, it.get("id"), it.get("kind"), KIND_OF[name]))
        items += part
    return items, notes


def serialise(items):
    return "".join(json.dumps(i, sort_keys=True, ensure_ascii=False) + "\n" for i in items)


def sha(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# ------------------------------------------------------------------ predecessor
def predecessor():
    """The 496 items of eval2_items_a1, regenerated in memory, and the generator object (for its guess tables).
    Raises when their sha256 differs from items_sha256 of SPLIT.json."""
    from kafka_sre_rag.catalogue import Catalogues, catalogue_hash, default_dir
    manifest = json.loads(SPLIT_A1.read_text())
    cats = Catalogues.load()
    chash = catalogue_hash(default_dir())
    g = _quiet(a1.generate, cats, a1.DEFAULT_SEED, chash)
    got = sha(serialise(g.items))
    if got != manifest["items_sha256"]:
        raise AssemblyError("the 496 regenerated in memory have sha256 %s, SPLIT.json records %s" % (got, manifest["items_sha256"]))
    if chash != manifest["catalogue_hash"]:
        raise AssemblyError("catalogue hash %s differs from SPLIT.json (%s)" % (chash, manifest["catalogue_hash"]))
    return g, manifest


def check_ids(a3, old_ids):
    ids = [i["id"] for i in a3]
    dup = [k for k, n in Counter(ids).items() if n > 1]
    if dup:
        raise AssemblyError("duplicate a3 ids: %s" % dup[:5])
    bad = [i for i in ids if not i.startswith("a3-")]
    if bad:
        raise AssemblyError("a3 ids must start with 'a3-': %s" % bad[:5])
    clash = sorted(set(ids) & set(old_ids))
    if clash:
        raise AssemblyError("a3 ids collide with existing ids: %s" % clash[:5])
    eval2.validate_items(a3)


# ------------------------------------------------------------------ near duplicates
NOTE_STARTS = ("Give the commands in the order", "Finish with a final line", "Give one complete")  # format notes of the generators


def core_of(it):
    """The question without its closing format note (the generators compare the core, as A1 does)."""
    head, sep, last = it["question"].rpartition("\n\n")
    return head if sep and last.startswith(NOTE_STARTS) else it["question"]


def _source_of(rel):
    if rel.startswith("eval/m4/open/"):
        return "open tier_a.jsonl"
    if rel.startswith("eval/"):
        return "eval/*.jsonl"
    if rel.startswith("data/"):
        return "data/*.jsonl"
    if rel.startswith("work/"):
        return "work/v3 questions"
    return rel


def near_duplicates(a3, old_items):
    """(report, hits). report: per source group the highest (jaccard, containment, item id, nearest file) of any
    a3 question against that group; the groups are the eval files, the open tier_a.jsonl, the training data, the 496
    regenerated (open and hidden, as one group, ids not shown), and the a3 items among themselves (other items only).
    hits: the (a3 id, group, jaccard, containment) over the plan's thresholds (Jaccard 0.5, containment 0.8, at least
    8 tokens) against anything outside a3; they stop the build. pairs: a3 items of different templates over the same
    thresholds (a, b, jaccard, containment); reported as a warning, because the generators were never asked to avoid
    them (a Java task and its Python restatement are such a pair by design). The a3 items of one template are
    parameter variants and are only reported (highest value)."""
    corpus, files = a1.corpus_texts()
    seen = {tuple(sorted(tk)) for _, tk in corpus}
    for line in OPEN_A1.read_text().splitlines():
        if line.strip():
            tk = a1.tokens(json.loads(line)["question"])
            if tuple(sorted(tk)) not in seen:
                corpus.append(("eval/m4/open/tier_a.jsonl", tk))
    groups = defaultdict(list)
    for rel, tk in corpus:
        groups[_source_of(rel)].append((rel, tk))
    groups["the 496 (open and hidden)"] = [("a1", a1.tokens(i["question"])) for i in old_items]
    toks = {i["id"]: a1.tokens(core_of(i)) for i in a3}
    report, hits, pairs = {}, [], set()
    for gname in sorted(groups):
        best = (0.0, 0.0, None, None)
        for it in a3:
            hit, dd = a1.near_duplicate(core_of(it), groups[gname])
            if max(dd["max_jaccard"], dd["max_containment"]) > max(best[0], best[1]):
                best = (dd["max_jaccard"], dd["max_containment"], it["id"], dd["nearest"])
            if hit:
                hits.append((it["id"], gname, dd["max_jaccard"], dd["max_containment"]))
        report[gname] = best
    # the a3 items among themselves: items of one template are parameter variants of one wording and are reported
    # only; items of different templates must not be near-duplicates (a failure)
    for label, same in (("a3 items, other templates", False), ("a3 items, same template (reported)", True)):
        best = (0.0, 0.0, None, None)
        for it in a3:
            others = [(o["id"], toks[o["id"]]) for o in a3
                      if o["id"] != it["id"] and (o["template"] == it["template"]) == same]
            hit, dd = a1.near_duplicate(core_of(it), others)
            if max(dd["max_jaccard"], dd["max_containment"]) > max(best[0], best[1]):
                best = (dd["max_jaccard"], dd["max_containment"], it["id"], dd["nearest"])
            if hit and not same:
                pairs.add(tuple(sorted((it["id"], dd["nearest"]))) + (dd["max_jaccard"], dd["max_containment"]))
        report[label] = best
    return report, sorted(hits), sorted(pairs), len(corpus)


# ------------------------------------------------------------------ mix and guess
def item_guess(it):
    c = it.get("chance")
    if c is None:
        c = DEFAULT_GUESS.get(it["failure_class"], DEFAULT_GUESS.get(it["kind"], 0.0))
    return float(c)


def guess_table(a3, old_g=None):
    """(lines, a3_overall, union_overall); the union uses A1's own guess of the 496 (kept modal shares, chances)."""
    n = len(a3)
    gs = [item_guess(i) for i in a3]
    lines = ["score of a blind guess (chance over the listed options; a control is passed by the blanket answer: %s):" % (
        ", ".join("%s %.2f" % kv for kv in sorted(DEFAULT_GUESS.items())))]
    by = defaultdict(list)
    for it, g in zip(a3, gs):
        by[it["kind"]].append(g)
    for k in sorted(by):
        lines.append("  %-18s %3d items  guess %5.1f %%" % (k, len(by[k]), 100.0 * sum(by[k]) / len(by[k])))
    overall = sum(gs) / n if n else 0.0
    lines.append("  a3 alone: overall guess over %d items: %.1f %% (limit %.0f %%)  %s" % (
        n, 100 * overall, 100 * a1.MAX_GUESS, "met" if overall < a1.MAX_GUESS else "NOT MET"))
    union = overall
    if old_g is not None:
        km = a1.kept_modal_shares(old_g.items)
        og = [a1.item_guess(old_g, i, km) for i in old_g.items]
        union = (sum(og) + sum(gs)) / (len(og) + n)
        lines.append("  union (%d + %d = %d items): overall guess %.1f %% (limit %.0f %%, reported)  %s" % (
            len(og), n, len(og) + n, 100 * union, 100 * a1.MAX_GUESS, "met" if union < a1.MAX_GUESS else "NOT MET"))
    return lines, overall, union


def mix_lines(a3, old_items):
    """Mix of the failure classes against A1's targets on the a3 build alone and on the union (reported)."""
    out = ["mix against the targets (a3 alone: floors that apply; union: reported, not forced):"]
    problems = []
    for title, items in (("a3 alone", a3), ("union", list(old_items) + list(a3))):
        n = len(items)
        grp = Counter(a1.mix_group(i["failure_class"]) for i in items)
        plain = grp["plain_lookup"] / n
        out.append("  %s (%d items)" % (title, n))
        ok = plain <= a1.MAX_PLAIN
        out.append("    %-34s %3d  %5.1f %%  (at most %.0f %%)  %s" % ("plain lookup", grp["plain_lookup"], 100 * plain,
                                                                      100 * a1.MAX_PLAIN, "met" if ok else "NOT MET"))
        if title == "a3 alone" and not ok:
            problems.append("plain lookup share %.1f %% above %.0f %%" % (100 * plain, 100 * a1.MAX_PLAIN))
        for k, lo in a1.MIX_MIN.items():
            share = grp[k] / n
            if title == "a3 alone":
                state = "not applicable: the a3 kinds have no %s items by construction" % a1.MIX_NAMES[k]
            else:
                state = "met" if share >= lo else "NOT MET"
            out.append("    %-34s %3d  %5.1f %%  (at least %.0f %%)  %s" % (a1.MIX_NAMES[k], grp[k], 100 * share, 100 * lo, state))
        new = sum(1 for i in items if i["kind"] in a1.NEW_KINDS or i["kind"] in PLAN)
        out.append("    new kinds (A2 kinds and a3 kinds) %d of %d = %.1f %% (at least %.0f %%)  %s" % (
            new, n, 100.0 * new / n, 100 * a1.NEW_KIND_SHARE, "met" if new >= a1.NEW_KIND_SHARE * n else "NOT MET"))
        real = sum(1 for i in items if i["kind"] == "is_real")
        out.append("    is_real %d of %d = %.1f %% (at most %.0f %%)  %s" % (real, n, 100.0 * real / n, 100 * a1.REAL_SHARE,
                                                                           "met" if real <= a1.REAL_SHARE * n else "NOT MET"))
        if title == "a3 alone" and real > a1.REAL_SHARE * n:
            problems.append("is_real share above %.0f %%" % (100 * a1.REAL_SHARE))
    return out, problems


def floor_problems(a3, allow_missing):
    """Floors of the plan on the a3 build alone: items and families per kind, items per template."""
    out = []
    for kind in PLAN:
        if kind == "not_covered" and "gap" in allow_missing:
            continue
        its = [i for i in a3 if i["kind"] == kind]
        if len(its) < MIN_KIND_ITEMS * 2:
            out.append("%s: %d items, at least %d needed" % (kind, len(its), MIN_KIND_ITEMS * 2))
        fams = {i["family"] for i in its}
        if len(fams) < MIN_KIND_FAMILIES:
            out.append("%s: %d families, at least %d needed" % (kind, len(fams), MIN_KIND_FAMILIES))
    for t, n in sorted(Counter(i["template"] for i in a3).items()):
        if n > a1.MAX_PER_TEMPLATE:
            out.append("template %s: %d items, at most %d" % (t, n, a1.MAX_PER_TEMPLATE))
    return out


# ------------------------------------------------------------------ exposure
def exposure_key(iid):
    return hashlib.sha256(("%s|exposed|%s" % (SEED, iid)).encode()).hexdigest()


def group_of(it):
    """The split group of an item: items that belong together (an item and its control) share one."""
    if it.get("pair"):
        return "pair:%s" % it["pair"]
    v = it.get("vbw")
    if v:
        if v.get("mechanism") == "FK":
            return "pair:" + ":".join(it["family"].split(":")[:2])
        return "pair:vbw:%s" % v["mechanism"]
    return it["family"]


def choose_exposed(items):
    """EXPOSED_PER_KIND items of each kind, by seed, from different groups; their groups go open as a whole."""
    chosen = []
    for kind in sorted({i["kind"] for i in items}):
        seen = set()
        for it in sorted((i for i in items if i["kind"] == kind), key=lambda i: exposure_key(i["id"])):
            g = group_of(it)
            if g in seen:
                continue
            seen.add(g)
            chosen.append(it["id"])
            if len(seen) == EXPOSED_PER_KIND:
                break
    return sorted(chosen)


def embedded_rows():
    return [json.loads(l) for l in EXPOSED_A3_TEXT.splitlines() if l.strip()]


def mark_exposed(items):
    """Returns (items with `exposed` set, exposed ids). Embedded rows (EXPOSED_A3_TEXT) win over a new choice: they
    must equal the built items, else the generator changed an item people have seen."""
    emb = embedded_rows()
    if emb:
        by = {i["id"]: i for i in items}
        for row in emb:
            cur = by.get(row["id"])
            if cur is None or {**cur, "exposed": True} != {**row, "exposed": True}:
                raise AssemblyError("embedded exposed item %s differs from the built item" % row["id"])
        ids = sorted(r["id"] for r in emb)
    else:
        ids = choose_exposed(items)
    out = [{**i, "exposed": i["id"] in set(ids)} for i in items]
    return out, ids


# ------------------------------------------------------------------ split
def split_extend(items, seed=SPLIT_SEED, share=SHARE, prior=None):
    """(open, hidden). Same method as eval2.split_families, with groups as families. prior = {"open": ids,
    "hidden": ids} of an earlier split: its groups stay on their side, the others are decided on the remaining
    targets. With no prior it returns exactly what split_families returns for the grouped items."""
    fam = defaultdict(list)
    for it in items:
        fam[group_of(it)].append(it)
    forced_open = {f for f, its in fam.items() if any(it.get("exposed") is True for it in its)}
    cells_total = Counter((it["tier"], it["kind"]) for it in items)
    target = {c: share * n for c, n in cells_total.items()}
    got = Counter()
    hidden = set()
    fixed = set()
    if prior:
        po, ph = set(prior["open"]), set(prior["hidden"])
        for f, its in fam.items():
            ids = {i["id"] for i in its}
            side_h, side_o = ids & ph, ids & po
            if side_h and side_o:
                raise AssemblyError("group %s was split over both sides before" % f)
            if side_h:
                if f in forced_open:
                    raise AssemblyError("group %s is hidden in the earlier split but now exposed" % f)
                hidden.add(f)
                got.update(Counter((it["tier"], it["kind"]) for it in its))
                fixed.add(f)
            elif side_o:
                fixed.add(f)
    for f in sorted((f for f in fam if f not in forced_open and f not in fixed), key=lambda f: eval2._order_key(seed, f)):
        add = Counter((it["tier"], it["kind"]) for it in fam[f])
        before = sum(abs(got[c] - target[c]) for c in add)
        after = sum(abs(got[c] + n - target[c]) for c, n in add.items())
        if after < before:
            hidden.add(f)
            got.update(add)
    return ([it for it in items if group_of(it) not in hidden], [it for it in items if group_of(it) in hidden])


def side_problems(op, hd, allow_missing):
    out = []
    for kind in PLAN:
        if kind == "not_covered" and "gap" in allow_missing:
            continue
        for side, its in (("open", op), ("hidden", hd)):
            k = [i for i in its if i["kind"] == kind]
            if len(k) < MIN_KIND_ITEMS:
                out.append("%s, %s side: %d items, at least %d needed" % (kind, side, len(k), MIN_KIND_ITEMS))
            f = {i["family"] for i in k}
            if len(f) < MIN_KIND_FAMILIES:
                out.append("%s, %s side: %d families, at least %d needed" % (kind, side, len(f), MIN_KIND_FAMILIES))
    return out


def old_sides_hold(old_items, a3_open, a3_hidden, seed=SPLIT_SEED, share=SHARE):
    """In-memory proof that adding the a3 items to the 496 moves no old id and no a3 id: split_families over both
    gives the open ids of tier_a.jsonl on the old side and the a3 split on the new side. Returns the problems."""
    old_open = {json.loads(l)["id"] for l in OPEN_A1.read_text().splitlines() if l.strip()}
    both = list(old_items) + [{**i, "family": group_of(i)} for i in a3_open + a3_hidden]
    op, hd = eval2.split_families(both, seed, share)
    op_ids = {i["id"] for i in op}
    out = []
    old_ids = {i["id"] for i in old_items}
    if {i for i in op_ids if i in old_ids} != old_open:
        out.append("an old id moved: the open side of the 496 differs from eval/m4/open/tier_a.jsonl")
    if {i for i in op_ids if i not in old_ids} != {i["id"] for i in a3_open}:
        out.append("the a3 part of the combined split differs from the a3-alone split")
    return out


# ------------------------------------------------------------------ build
def git_commit():
    return subprocess.run(["git", "-C", str(ROOT), "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip() or None


def build(allow_missing=(), verify=False, with_predecessor=True):
    """Everything the dry run and the real run share. Returns a dict; raises AssemblyError when a floor fails."""
    items, notes = collect(allow_missing, verify)
    r = {"notes": notes, "problems": []}
    old_g = old_items = None
    if with_predecessor:
        old_g, _ = predecessor()
        old_items = old_g.items
        check_ids(items, [i["id"] for i in old_items])
    else:
        check_ids(items, [])
    items, exposed = mark_exposed(items)
    r.update(items=items, exposed=exposed, old_g=old_g, old_items=old_items or [])
    r["problems"] += floor_problems(items, allow_missing)
    if with_predecessor:
        rep, hits, pairs, ncorpus = near_duplicates(items, old_items)
        r.update(dup_report=rep, dup_corpus=ncorpus, dup_pairs=pairs)
        r["problems"] += ["near-duplicate: %s against %s (jaccard %.3f, containment %.3f)" % h for h in hits]
    r["guess_lines"], r["guess"], r["guess_union"] = guess_table(items, old_g)
    if r["guess"] >= a1.MAX_GUESS:
        r["problems"].append("blind guess of the a3 build %.1f %% not below %.0f %%" % (100 * r["guess"], 100 * a1.MAX_GUESS))
    r["mix_lines"], mp = mix_lines(items, old_items or [])
    r["problems"] += mp
    op, hd = split_extend(items)
    r["open"], r["hidden"] = op, hd
    r["problems"] += side_problems(op, hd, allow_missing)
    if with_predecessor:
        r["problems"] += old_sides_hold(old_items, op, hd)
    return r


def manifest_of(r, enc_sha=None):
    items, op, hd = r["items"], r["open"], r["hidden"]
    from kafka_sre_rag import catalogue_a3
    from kafka_sre_rag.catalogue import catalogue_hash, default_dir
    cell = lambda its: dict(sorted(Counter(i["kind"] for i in its).items()))  # noqa: E731
    return {
        "generator": VERSION, "generator_commit": git_commit(), "seed": SPLIT_SEED, "share": SHARE, "exposure_seed": SEED,
        "catalogue_hash": catalogue_hash(default_dir()), "catalogue_hash_a3": catalogue_a3.manifest()["catalogue_hash"],
        "predecessor": {"items_sha256": json.loads(SPLIT_A1.read_text())["items_sha256"], "items": 496},
        "items_total": len(items), "items_sha256": sha(serialise(items)),
        "counts": {"open": len(op), "hidden": len(hd), "exposed": len(r["exposed"]),
                   "families_open": len({i["family"] for i in op}), "families_hidden": len({i["family"] for i in hd})},
        "per_kind": {k: {"open": cell(op).get(k, 0), "hidden": cell(hd).get(k, 0), "total": cell(items)[k]} for k in cell(items)},
        "exposed_ids": r["exposed"],
        "outputs": {"open": {"path": OPEN_PATH, "sha256": sha(serialise(op)), "items": len(op)},
                    "hidden": {"path": HIDDEN_PATH, "plaintext_sha256": sha(serialise(hd)), "items": len(hd),
                               "sha256": enc_sha,
                               "note": "encrypted with a random salt: the ciphertext differs on every run; the plaintext "
                                       "(jsonl, sort_keys, in the order of the items) is reproduced exactly"}},
        "rule": "groups on one side (an item and its control together); families with any item exposed:true go open; "
                "share per (tier, kind) cell reached with the remaining groups (eval2_items_a3.split_extend, the method "
                "of scripts/eval2.py split_families)",
        "grouping": "valid_but_wrong by mechanism (forbidden-key template by resource kind); items with `pair` by it; "
                    "all other items by their own family",
        "kinds_missing": sorted(k for k in PLAN if k not in cell(items)),
    }


def report(r, allow_missing):
    items, op, hd = r["items"], r["open"], r["hidden"]
    out = ["%s  seed %s  split seed %d share %.2f" % (VERSION, SEED, SPLIT_SEED, SHARE)]
    out += r["notes"]
    out.append("a3 items: %d  (families %d, templates %d, exposed %d)" % (
        len(items), len({i["family"] for i in items}), len({i["template"] for i in items}), len(r["exposed"])))
    out.append("per kind and side (open / hidden / total; families open / hidden):")
    for k in sorted({i["kind"] for i in items}):
        o, h = [i for i in op if i["kind"] == k], [i for i in hd if i["kind"] == k]
        out.append("  %-16s %3d / %3d / %3d   families %3d / %3d" % (k, len(o), len(h), len(o) + len(h),
                                                                     len({i["family"] for i in o}), len({i["family"] for i in h})))
    out.append("  %-16s %3d / %3d / %3d   (split groups %d, hidden %d)" % (
        "all", len(op), len(hd), len(items), len({group_of(i) for i in items}), len({group_of(i) for i in hd})))
    out.append("per template: %s" % ", ".join("%s %d" % kv for kv in sorted(Counter(i["template"] for i in items).items())))
    out.append("per failure class: %s" % ", ".join("%s %d" % kv for kv in sorted(Counter(i["failure_class"] for i in items).items())))
    out.append("exposed (open by construction; the list for T9, ids only): %s" % ", ".join(r["exposed"]))
    out += r["mix_lines"]
    out += r["guess_lines"]
    if "dup_report" in r:
        out.append("near-duplicates: %d texts in the corpus (eval, work/v3, data, open tier_a) + the 496 + the a3 items; thresholds "
                   "Jaccard >= %.2f or containment >= %.2f (both sets >= %d tokens)" % (
                       r["dup_corpus"], a1.JACCARD_T, a1.CONTAIN_T, a1.MIN_TOKENS))
        out.append("  highest values (jaccard, containment, a3 item, nearest):")
        for g, b in r["dup_report"].items():
            out.append("    %-28s %.3f  %.3f  %s  %s" % (g, b[0], b[1], b[2], b[3]))
    if r.get("dup_pairs"):
        out.append("warning, not gating: %d pairs of a3 items of different templates over the thresholds (ids, jaccard, containment):"
                   % len(r["dup_pairs"]))
        for a, b, j, c in r["dup_pairs"]:
            out.append("    %s  %s  %.3f  %.3f" % (a, b, j, c))
    m = manifest_of(r)
    out.append("hashes the real run would write:")
    out.append("  items_sha256 (plain build, in memory only)  %s" % m["items_sha256"])
    out.append("  %s  sha256 %s" % (OPEN_PATH, m["outputs"]["open"]["sha256"]))
    out.append("  %s  plaintext sha256 %s (the file itself differs on every run: random salt)" % (HIDDEN_PATH, m["outputs"]["hidden"]["plaintext_sha256"]))
    out.append("  catalogue_hash %s  catalogue_hash_a3 %s" % (m["catalogue_hash"], m["catalogue_hash_a3"]))
    out.append("problems: %s" % ("none" if not r["problems"] else ""))
    out += ["  " + p for p in r["problems"]]
    return "\n".join(out)


def write_outputs(r, open_out, hidden_out, manifest_out, key_file=None):
    """The real run: three new files; an existing output stops it before anything is written."""
    for p in (open_out, hidden_out, manifest_out):
        if Path(p).exists():
            raise AssemblyError("%s exists; the a3 split is made once and never overwritten" % p)
    if r["problems"]:
        raise AssemblyError("not writing: %d problem(s)" % len(r["problems"]))
    blob = eval2.encrypt_bytes(serialise(r["hidden"]).encode("utf-8"), key_file)
    m = manifest_of(r, enc_sha=eval2.sha256_bytes(blob))
    m["outputs"]["open"]["path"], m["outputs"]["hidden"]["path"] = str(open_out), str(hidden_out)
    eval2.write_atomic(hidden_out, blob)
    eval2.write_atomic(open_out, serialise(r["open"]).encode("utf-8"))
    eval2.write_atomic(manifest_out, (json.dumps(m, indent=1, sort_keys=True) + "\n").encode("utf-8"))
    return m


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--allow-missing", action="append", default=[], choices=list(MODULES),
                    help="build without this generator module when it does not exist (the run says so)")
    ap.add_argument("--verify", action="store_true", help="let the client generator run its per-item verification (slow)")
    ap.add_argument("--write", action="store_true", help="write the three outputs (real run; never overwrites)")
    ap.add_argument("--open-out", default=str(ROOT / OPEN_PATH))
    ap.add_argument("--hidden-out", default=str(ROOT / HIDDEN_PATH))
    ap.add_argument("--manifest-out", default=str(ROOT / MANIFEST_PATH))
    args = ap.parse_args(argv)
    try:
        if args.write and args.allow_missing:
            raise AssemblyError("--write with --allow-missing: the real split is made once, with all five modules")
        r = build(tuple(args.allow_missing), verify=args.verify or args.write)
        print(report(r, args.allow_missing))
        if args.write:
            write_outputs(r, args.open_out, args.hidden_out, args.manifest_out)
            print("wrote %s, %s (encrypted), %s" % (args.open_out, args.hidden_out, args.manifest_out))
        else:
            print("dry run: nothing written")
        return 1 if r["problems"] else 0
    except AssemblyError as e:
        print("FAIL: %s" % e, file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
