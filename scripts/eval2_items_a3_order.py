#!/usr/bin/env python3
"""Safe-order items of the a3 slice of tier A (WP1c T6): "run the safe step first, then the mutating one".

Each item asks for a short procedure (a preview before the change, `--generate` before `--execute` before
`--verify`). The check is `cmd_order` of scripts/eval2_checks_a3.py, next to the flag checks of the scorer
(`has_kafka_command`, `kcmd_known_flags`, `kcmd_constraints`; `has_command` and `no_invented_cli` for Argo CD).

Truth, no model (D6):
  - every ordering constraint is a passage of a pinned file (file, line, quoted phrase). The phrase is looked up in
    the cited line at build time; a spec whose passage is not on that line is a failed derivation;
  - every flag of a command line must exist in the tool's catalogue entry (kafka_cli, argocd; complete option
    lists only), and the reference's flag combination must satisfy the tool's own constraints;
  - the reference must pass every check of the item, and each of the three perturbed orders must fail `cmd_order`.
    A spec for which one of these does not hold is counted and skipped, never repaired by hand.

Perturbed orders, one of each per item (`perturbed`: label, text, check):
  swap       the mutating line before the safe one (for `--generate/--execute/--verify`: execute before generate)
  missing    a required step left out (the safe line; for the reassignment the verify line)
  forbidden  the line that must not appear: the safe or mutating step carries a flag the task rules out, which the
             step's `flags_forbidden` rejects (the task states the exclusion in the question)

Candidates without an ordering passage in the pinned text (describe before alter for kafka-topics and
kafka-configs; the preview of `kafka-streams-groups.sh --delete`) are counted as skipped, not filled in.

Version 3 (fix round 2): N6 and N7 changed the checks of order_sg_delete_offsets and order_reassign (new ids from 301); N1 adds one
parameter set (`fresh`, ids from 201) each for order_app_reset, order_argo_sync and order_reassign, for the hidden side.
Version 3, revision 2: the three order_reassign items save the proposed block of the `--generate` output, not a redirect of the whole
output (comment line after the generate line, DOC_OPS 398); new ids 401, 402 and 251.
Deterministic: one random stream per template, seeded with seed|catalogue hash|template; no set iteration.
`generate()` returns the items and writes no file. Run: .venv/bin/python scripts/eval2_items_a3_order.py
"""
import json
import random
import re
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import eval2_score as sc  # noqa: E402
from eval2_checks_a3 import check_cmd_order  # noqa: E402
from kafka_sre_rag.catalogue import KNOWN, Catalogues, catalogue_hash, default_dir  # noqa: E402

VERSION = "eval2_items_a3_order/3"
DEFAULT_SEED = "m4-a3-1"
BS = "kafka:9092"
KIND = "safe_order"
# Version 2: the items of these templates changed (their steps or their wording), so their numbers start at 101 and
# no version-1 id is reused.
V2_CHANGED = ("order_argo_sync",)
V2_ID_BASE = 100
# Version 3 (round 2, N6 and N7): the checks of these templates changed again, so their numbers start at 301 and no
# earlier id is reused. The `fresh` specs (round 2, N1: items for the hidden side that were never in an open file) have
# their own numbers from 201.
V3_CHANGED = ("order_sg_delete_offsets", "order_reassign")
V3_ID_BASE = 300
FRESH_ID_BASE = 200
# Revision 2 of version 3: `--generate` prints two labelled JSON blocks (the current and the proposed assignment); the docs say
# to save the proposed one in a json file for `--execute` (DOC_OPS 398). A redirect of the whole output is not a plan file, so the
# reference puts a comment line after the generate line. Comment lines are not commands for cmd_order.
SAVE_NOTE = '\n# save the "Proposed partition reassignment configuration" block of this output as {r} (the current assignment block is only for a rollback)'
V3R2_ID_BASE = 400       # order_reassign items of the revision: new ids (301, 302 are not reused)
FRESH_R2_ID_BASE = 250   # the fresh order_reassign item of the revision (201 is not reused)
FORMAT_NOTE = ("Give the commands in the order they should be run, in one code block, one command per line; "
               "use only options that exist for the tool.")

DOC_OPS = "raw/apache-kafka/docs/operations/basic-kafka-operations.md"
DOC_SG = "raw/apache-kafka/docs/streams/developer-guide/kafka-streams-group-sh.md"
DOC_RESET = "raw/apache-kafka/docs/streams/developer-guide/app-reset-tool.md"
DOC_ARGO = "raw/argo-cd/docs/user-guide/commands/argocd_app_sync.md"
DOC_ARGO_DIFF = "raw/argo-cd/docs/user-guide/commands/argocd_app_diff.md"
SRC_SG_OPTS = "raw_catalogue/apache-kafka-tools/tools/src/main/java/org/apache/kafka/tools/streams/StreamsGroupCommandOptions.java"

# Tool rules the catalogue does not carry (Q5: enforced here, the frozen kafka_cli catalogue does not change):
# for kafka-streams-groups.sh, `--dry-run` and `--execute` are accepted only together with `--reset-offsets`.
TOOL_RULES = {"kafka-streams-groups.sh": {
    "only_with": {"--dry-run": "--reset-offsets", "--execute": "--reset-offsets"},
    "anchors": [(SRC_SG_OPTS, 60, "Only show results without executing changes on streams group. Supported operations: reset-offsets."),
                (SRC_SG_OPTS, 61, "Execute operation. Supported operations: reset-offsets."),
                (SRC_SG_OPTS, 236, '"Only Option " + resetOffsetsOpt + " accepts "')]}}


# ------------------------------------------------------------------ anchors
def verify_anchor(file, line, phrase, root=ROOT):
    """(True, "") when `file` exists, `line` (1-based) is in range and that line contains `phrase`
    (whitespace-normalised, backslash escapes of markdown dropped); else (False, reason)."""
    p = Path(root) / file
    if not p.is_file():
        return False, "file %s not present" % file
    lines = p.read_text(errors="replace").split("\n")
    if not 1 <= line <= len(lines):
        return False, "%s has %d lines, not %d" % (file, len(lines), line)
    norm = lambda s: re.sub(r"\s+", " ", s.replace("\\", "")).strip()  # noqa: E731
    if norm(phrase) not in norm(lines[line - 1]):
        return False, "%s:%d does not contain %r" % (file, line, phrase)
    return True, ""


def anchor(file, line, phrase):
    return {"file": file, "line": line, "phrase": phrase}


# ------------------------------------------------------------------ the specs
# A step is {role, tool, words, flags_all, flags_forbidden}; `lines` builds the command text of a role from the params.
def kbin(tool):
    return "bin/" + tool


def kline(tool, flags, p, extra=()):
    toks = [kbin(tool), "--bootstrap-server", BS]
    for f, v in flags:
        toks.append(f)
        if v is not None:
            toks.append(v.format(**p))
    toks += list(extra)
    return " ".join(toks)


def reset_flags(spec_flag, p, scope):
    return [("--reset-offsets", None), ("--group", "{g}")] + scope + [spec_flag]


RESET_VARIANTS = {
    "earliest": (("--to-earliest", None), "to the earliest offset"),
    "latest": (("--to-latest", None), "to the latest offset"),
    "datetime": (("--to-datetime", "{d}"), "to the time `{d}`"),
    "shift": (("--shift-by", "-{n}"), "back by {n} offsets"),
}


def specs():
    """The candidate specs, in a fixed order. `anchors` give the ordering rule; `params` the parameter sets."""
    s = []
    # --- consumer groups: the reset only displays unless --execute is given
    for var, params in (("earliest", {"g": "orders", "t": "orders-events"}),
                        ("datetime", {"g": "billing", "t": "billing-events", "d": "2026-11-01T00:00:00.000"}),
                        ("shift", {"g": "search-indexer", "t": "search-index", "n": "500"})):
        flag, phrase = RESET_VARIANTS[var]
        scope = [("--topic", "{t}")]
        s.append({
            "template": "order_cg_reset", "group": "kafka-consumer-groups:%s" % var, "tool": "kafka-consumer-groups.sh",
            "domain": "kafka", "params": params, "forbidden": "--all-topics", "forbidden_role": "mutate",
            "task": "reset the offsets of the consumer group `{g}` on the topic `%s` %s" % ("{t}", phrase),
            "limit": "Only the topic `{t}` may be reset, not all topics of the group.",
            "safe": reset_flags(flag, params, scope) + [("--dry-run", None)],
            "mutate": reset_flags(flag, params, scope) + [("--execute", None)],
            "safe_step": {"flags_all": ["--reset-offsets"], "flags_forbidden": ["--execute"]},
            "mutate_step": {"flags_all": ["--reset-offsets", "--execute"]},
            "flags": ["--reset-offsets", "--group", "--topic", "--dry-run", "--execute", flag[0], "--all-topics"],
            "anchors": [anchor(DOC_OPS, 230, "(default) to display which offsets to reset"),
                        anchor(DOC_OPS, 231, "to execute --reset-offsets process")],
        })
    # --- streams groups: preview, then apply (the passage says it twice)
    for var, params in (("earliest", {"g": "orders-streams"}),
                        ("datetime", {"g": "fraud-streams", "d": "2026-10-15T12:30:00.000"}),
                        ("shift", {"g": "billing-streams", "n": "200"})):
        flag, phrase = RESET_VARIANTS[var]
        scope = [("--all-input-topics", None)]
        s.append({
            "template": "order_sg_reset", "group": "kafka-streams-groups:%s" % var, "tool": "kafka-streams-groups.sh",
            "domain": "kafka", "params": params, "forbidden": "--delete", "forbidden_role": "mutate",
            "task": "reset the input topic offsets of the Streams group `{g}` %s (the application instances are stopped)" % phrase,
            "limit": "Keep the group: it must not be deleted.",
            "safe": reset_flags(flag, params, scope) + [("--dry-run", None)],
            "mutate": reset_flags(flag, params, scope) + [("--execute", None)],
            "safe_step": {"flags_all": ["--reset-offsets", "--dry-run"]},
            "mutate_step": {"flags_all": ["--reset-offsets", "--execute"]},
            "flags": ["--reset-offsets", "--group", "--all-input-topics", "--dry-run", "--execute", flag[0], "--delete"],
            "anchors": [anchor(DOC_SG, 90, "Always preview changes with `--dry-run` before using `--execute`"),
                        anchor(DOC_SG, 172, "Preview changes with `--dry-run`")],
        })
    # --- streams groups: deleting offsets. The pinned tool source (not the doc page, which says to preview every
    # mutating operation with --dry-run) accepts --dry-run and --execute only with --reset-offsets; so the safe step is
    # describing the committed offsets and the mutating step is --delete-offsets itself.
    for params in ({"g": "orders-streams"}, {"g": "clicks-streams"}):
        s.append({
            "template": "order_sg_delete_offsets", "group": "kafka-streams-groups:delete-offsets:%s" % params["g"],
            "tool": "kafka-streams-groups.sh", "domain": "kafka", "params": params, "forbidden": "--delete",
            "forbidden_role": "mutate",
            "task": "delete the committed offsets of all input topics of the Streams group `{g}`, so that it re-reads them on restart (the instances are stopped), after looking at the offsets the group has committed",
            "limit": "Keep the group: it must not be deleted.",
            "safe": [("--describe", None), ("--offsets", None), ("--group", "{g}")],
            "mutate": [("--delete-offsets", None), ("--group", "{g}"), ("--all-input-topics", None)],
            # N6: --offsets is the default sub-action (StreamsGroupCommandOptions.java:51-52), so describing the group is the
            # safe step; --members and --state describe something else (they exclude --offsets, lines 250-253); the delete
            # step needs --group and one scope (lines 261-263)
            "safe_step": {"flags_all": ["--describe", "--group"], "flags_forbidden": ["--members", "--state"]},
            "mutate_step": {"flags_all": ["--delete-offsets", "--group"], "flags_any": ["--all-input-topics", "--input-topic"],
                            "flags_forbidden": ["--dry-run", "--execute"]},
            "flags": ["--describe", "--offsets", "--delete-offsets", "--group", "--all-input-topics", "--delete"],
            "note": ("The doc page kafka-streams-group-sh.md (lines 35, 114, 165) says to preview with --dry-run before every "
                     "mutating operation, but the pinned tool source accepts --dry-run and --execute only with --reset-offsets "
                     "(StreamsGroupCommandOptions.java:60-61, 235-236). The item follows the tool: --describe --offsets first, "
                     "then --delete-offsets without --dry-run or --execute."),
            "anchors": [anchor(DOC_SG, 35, "Mutating operations (offset resets/deletes, group deletion)")] + [
                anchor(*a) for a in TOOL_RULES["kafka-streams-groups.sh"]["anchors"]],
        })
    # --- application reset tool: one dry run before the irreversible reset
    for params in ({"a": "orders-app", "t": "orders-events"}, {"a": "fraud-detector", "t": "payments"}):
        s.append({
            "template": "order_app_reset", "group": "kafka-streams-application-reset:%s" % params["a"],
            "tool": "kafka-streams-application-reset.sh", "domain": "kafka", "params": params, "forbidden": "--force",
            "forbidden_role": "mutate",
            "task": "reset the Kafka Streams application `{a}` to the beginning of its input topic `{t}` (all instances are stopped)",
            "limit": "Do not use `--force`.",
            "safe": [("--application-id", "{a}"), ("--input-topics", "{t}"), ("--dry-run", None)],
            "mutate": [("--application-id", "{a}"), ("--input-topics", "{t}")],
            "safe_step": {"flags_all": ["--application-id", "--dry-run"]},
            "mutate_step": {"flags_all": ["--application-id"], "flags_forbidden": ["--dry-run"]},
            "flags": ["--application-id", "--input-topics", "--dry-run", "--force"],
            "anchors": [anchor(DOC_RESET, 65, "run this once with `--dry-run` to preview your changes before making them")],
        })
    # --- partition reassignment: generate, execute, verify
    for params, thr in (({"f": "topics-to-move.json", "b": "5,6", "r": "expand-cluster-reassignment.json"}, False),
                        ({"f": "move-orders.json", "b": "4,5,6", "r": "bigger-cluster.json", "v": "50000000"}, True)):
        exe = [("--execute", None), ("--reassignment-json-file", "{r}")] + ([("--throttle", "{v}")] if thr else [])
        anchors = [anchor(DOC_OPS, 348, "the tool generates a candidate reassignment"),
                   anchor(DOC_OPS, 349, "the tool kicks off the reassignment of partitions"),
                   anchor(DOC_OPS, 350, "the tool verifies the status of the reassignment"),
                   anchor(DOC_OPS, 398, "The new assignment should be saved in a json file")]
        if thr:
            anchors.append(anchor(DOC_OPS, 605, "the throttle will be removed via the --verify command"))
        s.append({
            "template": "order_reassign", "group": "kafka-reassign-partitions:%s" % ("throttle" if thr else "plain"),
            "tool": "kafka-reassign-partitions.sh", "domain": "kafka", "params": params,
            "forbidden": "--disable-rack-aware", "forbidden_role": "safe", "verify": True,
            "task": "move all partitions of the topics listed in `{f}` to the brokers {b}%s, and confirm that the move has finished. Generate the candidate assignment and save the proposed assignment as `{r}`" % (
                " with the data movement throttled to {v} bytes per second" if thr else ""),
            "limit": "Keep the rack-aware replica assignment.",
            "safe": [("--generate", None), ("--topics-to-move-json-file", "{f}"), ("--broker-list", "{b}")],
            "safe_suffix": SAVE_NOTE,   # revision 2: --generate prints two blocks; the docs say to save the proposed one
            "mutate": exe, "verify": [("--verify", None), ("--reassignment-json-file", "{r}")],
            "safe_step": {"flags_all": ["--generate"]},
            # N7: the question asks for this throttle, so the execute step checks it
            "mutate_step": {"flags_all": ["--execute"], **({"values": {"--throttle": params["v"]}} if thr else {})},
            "verify_step": {"flags_all": ["--verify"]},
            "flags": ["--generate", "--topics-to-move-json-file", "--broker-list", "--execute", "--reassignment-json-file",
                      "--verify", "--disable-rack-aware"] + (["--throttle"] if thr else []),
            "anchors": anchors,
        })
    # --- share groups
    for params in ({"g": "my-share-group", "t": "topic1"}, {"g": "audit-share", "t": "audit-log"}):
        s.append({
            "template": "order_shg_reset", "group": "kafka-share-groups:%s" % params["g"],
            "tool": "kafka-share-groups.sh", "domain": "kafka", "params": params, "forbidden": "--all-topics",
            "forbidden_role": "mutate",
            "task": "reset the offsets of the share group `{g}` on the topic `{t}` to the earliest offset",
            "limit": "Only the topic `{t}` may be reset, not all topics of the group.",
            "safe": [("--reset-offsets", None), ("--group", "{g}"), ("--topic", "{t}"), ("--to-earliest", None), ("--dry-run", None)],
            "mutate": [("--reset-offsets", None), ("--group", "{g}"), ("--topic", "{t}"), ("--to-earliest", None), ("--execute", None)],
            "safe_step": {"flags_all": ["--reset-offsets", "--dry-run"]},
            "mutate_step": {"flags_all": ["--reset-offsets", "--execute"]},
            "flags": ["--reset-offsets", "--group", "--topic", "--to-earliest", "--dry-run", "--execute", "--all-topics"],
            "anchors": [anchor(DOC_OPS, 302, "--dry-run: to display which offsets to reset"),
                        anchor(DOC_OPS, 303, "to execute --reset-offsets process")],
        })
    # --- Argo CD: preview the sync, then sync
    for params in ({"a": "billing"}, {"a": "web-frontend"}):
        s.append({
            "template": "order_argo_sync", "group": "argocd-app-sync:%s" % params["a"], "tool": "argocd", "domain": "argocd",
            "params": params, "forbidden": "--prune", "forbidden_role": "mutate", "argocd": "argocd app sync",
            "task": "sync the Argo CD application `{a}`, after checking what the sync would do without changing the cluster",
            "limit": "Do not prune resources.",
            "safe": "argocd app sync {a} --dry-run", "mutate": "argocd app sync {a}",
            "safe_step": {"words": ["app", "sync"], "flags_all": ["--dry-run"]},
            "safe_alt_steps": [{"words": ["app", "diff"]}],
            "mutate_step": {"words": ["app", "sync"], "flags_forbidden": ["--dry-run"], "flags_forbidden_true": ["--prune"]},
            "forbidden_bool": True,
            "flags": ["--dry-run", "--prune"],
            "anchors": [anchor(DOC_ARGO, 49, "Preview apply without affecting cluster"),
                        anchor(DOC_ARGO_DIFF, 5, "Perform a diff against the target and live state.")],
        })
    # --- round 2 (N1): one further parameter set for each of three templates; never in an open file, so they can be hidden
    s += [dict(x, fresh=True) for x in fresh_specs()]
    return s


def fresh_specs():
    out = []
    for params in ({"a": "stock-sync", "t": "stock-levels"},):
        out.append({
            "template": "order_app_reset", "group": "kafka-streams-application-reset:%s" % params["a"],
            "tool": "kafka-streams-application-reset.sh", "domain": "kafka", "params": params, "forbidden": "--force",
            "forbidden_role": "mutate",
            "task": "reset the Kafka Streams application `{a}` to the beginning of its input topic `{t}` (all instances are stopped)",
            "limit": "Do not use `--force`.",
            "safe": [("--application-id", "{a}"), ("--input-topics", "{t}"), ("--dry-run", None)],
            "mutate": [("--application-id", "{a}"), ("--input-topics", "{t}")],
            "safe_step": {"flags_all": ["--application-id", "--dry-run"]},
            "mutate_step": {"flags_all": ["--application-id"], "flags_forbidden": ["--dry-run"]},
            "flags": ["--application-id", "--input-topics", "--dry-run", "--force"],
            "anchors": [anchor(DOC_RESET, 65, "run this once with `--dry-run` to preview your changes before making them")],
        })
    for params in ({"a": "payments-api"},):
        out.append({
            "template": "order_argo_sync", "group": "argocd-app-sync:%s" % params["a"], "tool": "argocd", "domain": "argocd",
            "params": params, "forbidden": "--prune", "forbidden_role": "mutate", "argocd": "argocd app sync",
            "task": "sync the Argo CD application `{a}`, after checking what the sync would do without changing the cluster",
            "limit": "Do not prune resources.",
            "safe": "argocd app sync {a} --dry-run", "mutate": "argocd app sync {a}",
            "safe_step": {"words": ["app", "sync"], "flags_all": ["--dry-run"]},
            "safe_alt_steps": [{"words": ["app", "diff"]}],
            "mutate_step": {"words": ["app", "sync"], "flags_forbidden": ["--dry-run"], "flags_forbidden_true": ["--prune"]},
            "forbidden_bool": True,
            "flags": ["--dry-run", "--prune"],
            "anchors": [anchor(DOC_ARGO, 49, "Preview apply without affecting cluster"),
                        anchor(DOC_ARGO_DIFF, 5, "Perform a diff against the target and live state.")],
        })
    for params in ({"f": "move-ledger.json", "b": "7,8", "r": "ledger-plan.json", "v": "20000000"},):
        exe = [("--execute", None), ("--reassignment-json-file", "{r}"), ("--throttle", "{v}")]
        anchors = [anchor(DOC_OPS, 348, "the tool generates a candidate reassignment"),
                   anchor(DOC_OPS, 349, "the tool kicks off the reassignment of partitions"),
                   anchor(DOC_OPS, 350, "the tool verifies the status of the reassignment"),
                   anchor(DOC_OPS, 398, "The new assignment should be saved in a json file"),
                   anchor(DOC_OPS, 605, "the throttle will be removed via the --verify command")]
        out.append({
            "template": "order_reassign", "group": "kafka-reassign-partitions:throttle:%s" % params["r"].split(".")[0],
            "tool": "kafka-reassign-partitions.sh", "domain": "kafka", "params": params,
            "forbidden": "--disable-rack-aware", "forbidden_role": "safe", "verify": True,
            "task": "move all partitions of the topics listed in `{f}` to the brokers {b} with the data movement throttled to {v} bytes per second, and confirm that the move has finished. Generate the candidate assignment and save the proposed assignment as `{r}`",
            "limit": "Keep the rack-aware replica assignment.",
            "safe": [("--generate", None), ("--topics-to-move-json-file", "{f}"), ("--broker-list", "{b}")],
            "safe_suffix": SAVE_NOTE,
            "mutate": exe, "verify": [("--verify", None), ("--reassignment-json-file", "{r}")],
            "safe_step": {"flags_all": ["--generate"]},
            "mutate_step": {"flags_all": ["--execute"], "values": {"--throttle": params["v"]}},
            "verify_step": {"flags_all": ["--verify"]},
            "flags": ["--generate", "--topics-to-move-json-file", "--broker-list", "--execute", "--reassignment-json-file",
                      "--verify", "--disable-rack-aware", "--throttle"],
            "anchors": anchors,
        })
    return out


# Candidates the pinned text gives no ordering passage for: counted and skipped (reason printed).
UNANCHORED = [
    ("order_topics_alter", "kafka-topics.sh --describe before --alter",
     "no passage of the pinned documentation says to describe before altering; the page only shows `--describe` after the change"),
    ("order_configs_alter", "kafka-configs.sh --describe before --alter",
     "no passage of the pinned documentation says to describe before altering"),
    ("order_sg_delete", "kafka-streams-groups.sh --delete previewed with --dry-run",
     "line 35 names group deletion, but the catalogue and the examples of the page show no --dry-run/--execute pair for --delete"),
]


# ------------------------------------------------------------------ building one item
def lines_for(spec, role, p, extra=()):
    ln = spec[role]
    if spec.get("argocd"):
        return ln.format(**p) + (" " + " ".join(extra) if extra else "")
    return kline(spec["tool"], ln, p, extra) + (spec["safe_suffix"].format(**p) if role == "safe" and spec.get("safe_suffix") else "")


def steps_for(spec):
    tool = spec["tool"]
    steps = []
    for role in ("safe", "mutate", "verify"):
        st = spec.get(role + "_step")
        if st is None:
            continue
        st = {"role": role, "tool": tool, **st}
        st["flags_forbidden"] = list(st.get("flags_forbidden", []))
        if role == spec["forbidden_role"] and not spec.get("forbidden_bool"):
            st["flags_forbidden"].append(spec["forbidden"])
        steps.append(st)
        if role == "safe":
            steps += [{"role": "safe", "tool": tool, "flags_forbidden": [], **alt} for alt in spec.get("safe_alt_steps", [])]
    return steps


def block(cmds):
    return "```bash\n%s\n```" % "\n".join(cmds)


def perturbations(spec, p):
    safe = lines_for(spec, "safe", p)
    mut = lines_for(spec, "mutate", p)
    ver = lines_for(spec, "verify", p) if spec.get("verify") else None
    ref = [safe, mut] + ([ver] if ver else [])
    out = []
    swap = [mut, safe] + ([ver] if ver else [])
    out.append(("swap", block(swap)))
    # missing: the safe step; for the three-step reassignment the verify step (the task asks to confirm the move)
    miss = [safe, mut] if ver else [mut] + ([ver] if ver else [])
    out.append(("missing", block(miss)))
    bad = lines_for(spec, spec["forbidden_role"], p, extra=[spec["forbidden"]])
    forb = [bad if spec["forbidden_role"] == "safe" else safe, bad if spec["forbidden_role"] == "mutate" else mut]
    if ver:
        forb.append(ver)
    out.append(("forbidden", block(forb)))
    return ref, out


PHRASINGS = [
    "I need to {task}. {limit} What exact commands do I run, and in what order? Kafka 4.3, bootstrap server `%s`.",
    "Write the procedure to {task}. {limit} Kafka 4.3 command-line tools; bootstrap server `%s`.",
    "On a broker host I have to {task}. {limit} Give the commands in the order I should run them (bootstrap server `%s`).",
]
PHRASINGS_ARGO = [
    "I need to {task}. {limit} What exact commands do I run, and in what order? `argocd` is already logged in.",
    "Write the procedure to {task}. {limit} Use the `argocd` CLI (Argo CD v3.6), already logged in.",
    "On a CI runner where `argocd` is logged in I have to {task}. {limit} Give the commands in the order I should run them.",
]


def build_item(spec, cats, chash, seed, n, rng, corpus, stats):
    p = spec["params"]
    tool = spec["tool"]
    # anchors
    for a in spec["anchors"]:
        ok, why = verify_anchor(a["file"], a["line"], a["phrase"])
        if not ok:
            stats["derivation_failed:%s: anchor: %s" % (spec["template"], why)] += 1
            return None
    # flags exist in the catalogue; the argocd command exists
    if spec.get("argocd"):
        if cats.argocd_command(spec["argocd"]).status != KNOWN:
            stats["derivation_failed:%s: command %s not in the catalogue" % (spec["template"], spec["argocd"])] += 1
            return None
        flag_tool = spec["argocd"]
    else:
        ent = [e for e in cats.data["kafka_cli"]["entries"] if e["tool"] == tool]
        if not ent or ent[0].get("complete") is not True:
            stats["derivation_failed:%s: no complete option list for %s" % (spec["template"], tool)] += 1
            return None
        flag_tool = tool
    for f in spec["flags"]:
        if cats.cli_option(flag_tool, f).status != KNOWN:
            stats["derivation_failed:%s: option %s not in %s" % (spec["template"], f, flag_tool)] += 1
            return None
    ref, perts = perturbations(spec, p)
    if not spec.get("argocd"):
        for role in ("safe", "mutate", "verify"):
            if spec.get(role):
                flags = [f for f, _ in spec[role]] + ["--bootstrap-server"]
                probs = sc.constraint_problems(tool, flags, cats)
                rule = TOOL_RULES.get(tool)
                for f, need in (rule["only_with"].items() if rule else ()):
                    if f in flags and need not in flags:
                        probs.append("%s is accepted only with %s (%s:%d)" % (f, need, rule["anchors"][-1][0], rule["anchors"][-1][1]))
                if probs:
                    stats["derivation_failed:%s: constraint of %s: %s" % (spec["template"], tool, "; ".join(probs))] += 1
                    return None
    steps = steps_for(spec)
    order_chk = {"type": "cmd_order", "steps": steps}
    if spec.get("verify"):
        order_chk["require_verify"] = True
    if spec.get("argocd"):
        checks = [{"type": "has_command", "format": True}, order_chk, {"type": "no_invented_cli", "tool": "argocd"}]
    else:
        checks = [{"type": "has_kafka_command", "format": True}, order_chk,
                  {"type": "kcmd_known_flags", "tool": tool}, {"type": "kcmd_constraints", "tool": tool}]
    oi = 1
    tmpl = (PHRASINGS_ARGO if spec.get("argocd") else PHRASINGS)[rng.randrange(3)]
    core = tmpl.format(task=spec["task"].format(**p), limit=spec["limit"].format(**p))
    if not spec.get("argocd"):
        core = core % BS
    question = core + "\n\n" + FORMAT_NOTE
    # near-duplicates of any eval, training or earlier question
    hit, dd = near_duplicate_of(core, corpus)
    if hit:
        stats["dropped:near_duplicate:%s" % spec["template"]] += 1
        return None
    item = {
        "id": "a3-%s-%03d" % (spec["template"], n), "tier": "A", "kind": KIND, "family": "%s:%s" % (spec["template"], spec["group"]),
        "domain": spec["domain"], "question": question, "checks": checks, "reference": block(ref),
        "sources": [{"catalogue": "pinned_text", "entry": spec["template"], "fact": a["phrase"], "file": a["file"],
                     "line": a["line"]} for a in spec["anchors"]] + [
            {"catalogue": "argocd" if spec.get("argocd") else "kafka_cli", "entry": flag_tool,
             "fact": "every option on the lines exists for the tool: %s" % ", ".join(spec["flags"]),
             "file": None, "line": None}],
        "versions": {"argo-cd": "v3.6.1"} if spec.get("argocd") else {"kafka": "4.3"},
        "template": spec["template"], "anchored": True, "failure_class": "safe_order", "exposed": False,
        "catalogue_hash": chash, "seed": seed, "generator": VERSION, "dedup": dd, "chance": 0.0,
        "perturbed_reference": perts[0][1], "perturbed_violates": oi,
        **({"note": spec["note"]} if spec.get("note") else {}),
        "perturbed": [{"label": lab, "text": txt, "violates": oi, "check": "cmd_order"} for lab, txt in perts],
    }
    # the reference passes every check; every perturbed order fails cmd_order
    res = sc.run_checks(item, item["reference"], cats)
    bad = [r["type"] for r in res if not r["pass"]]
    if bad:
        stats["derivation_failed:%s: the reference fails %s" % (spec["template"], ",".join(bad))] += 1
        return None
    for lab, txt in perts:
        ok, _ = check_cmd_order(order_chk, txt, cats)
        if ok:
            stats["derivation_failed:%s: the %s order passes cmd_order" % (spec["template"], lab)] += 1
            return None
    return item


# ------------------------------------------------------------------ near-duplicate scan (the matcher of the A1 generator)
_CORPUS = None


def load_corpus():
    """Tokenised questions of eval/*.jsonl, work/v3/questions*.jsonl, data/{train,valid,test}.jsonl (the A1 corpus)
    and eval/m4/open/tier_a.jsonl (read only; the hidden file is never opened)."""
    global _CORPUS
    if _CORPUS is None:
        import eval2_items_a1 as a1
        corpus, _ = a1.corpus_texts()
        f = ROOT / "eval" / "m4" / "open" / "tier_a.jsonl"
        for line in f.read_text(errors="replace").splitlines():
            if line.strip():
                row = json.loads(line)
                if isinstance(row.get("question"), str):
                    corpus.append(("eval/m4/open/tier_a.jsonl", a1.tokens(row["question"])))
        _CORPUS = corpus
    return _CORPUS


def near_duplicate_of(core, corpus):
    import eval2_items_a1 as a1
    return a1.near_duplicate(core, corpus)


# ------------------------------------------------------------------ generate
def generate_with_stats(cats=None, seed=DEFAULT_SEED, corpus=None):
    cats = cats or Catalogues.load()
    chash = catalogue_hash(default_dir())
    corpus = load_corpus() if corpus is None else corpus
    stats = Counter()
    items, counters, streams = [], Counter(), {}
    for tid, label, why in UNANCHORED:
        stats["derivation_failed:%s: %s" % (tid, why)] += 1
    for spec in specs():
        t = spec["template"]
        if t not in streams:
            streams[t] = random.Random("%s|%s|%s" % (seed, chash, t))
        key = (t, bool(spec.get("fresh")))
        counters[key] += 1
        base = FRESH_ID_BASE if spec.get("fresh") else V3_ID_BASE if t in V3_CHANGED else V2_ID_BASE if t in V2_CHANGED else 0
        if t == "order_reassign":
            base = FRESH_R2_ID_BASE if spec.get("fresh") else V3R2_ID_BASE
        item = build_item(spec, cats, chash, seed, counters[key] + base, streams[t], corpus, stats)
        if item is None:
            counters[key] -= 1
            continue
        items.append(item)
    items.sort(key=lambda i: i["id"])
    stats["items"] = len(items)
    return items, stats


def generate(cats=None, seed=DEFAULT_SEED):
    return generate_with_stats(cats, seed)[0]


def max_similarity(items, corpus):
    best = (0.0, 0.0, None, None)
    for it in items:
        d = it["dedup"]
        if max(d["max_jaccard"], d["max_containment"]) > max(best[0], best[1]):
            best = (d["max_jaccard"], d["max_containment"], d["nearest"], it["id"])
    return best


def main():
    items, stats = generate_with_stats()
    print("%s: %d items made (plan: 16)" % (VERSION, len(items)))
    for k in sorted(stats):
        if k != "items":
            print("  %s x%d" % (k, stats[k]))
    best = max_similarity(items, None)
    print("highest similarity to an eval, training or open question: jaccard %.3f, containment %.3f (%s, item %s)" % best)
    print("families: %d; templates: %s" % (len({i["family"] for i in items}), sorted({i["template"] for i in items})))


if __name__ == "__main__":
    main()
