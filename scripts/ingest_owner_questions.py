#!/usr/bin/env python3
"""Seal the Owner's questions (eval/m4/inbox/owner_questions.yaml) into the encrypted hidden store.

The script never prints item text. Errors name field names and case numbers only.
Each item must be preceded by a numbered comment line ("# 12"); its id is oq-012.
"""
import argparse
import hashlib
import json
import re
import sys
from collections import Counter
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
import eval2  # noqa: E402

KINDS = {"diagnostics", "debugging", "configuration", "coding", "architecture"}
SIDES = {"open", "hidden"}
FIELDS = ["kind", "versions", "question", "evidence", "what_was_wrong", "what_fixed_it",
          "essential", "dangerous", "scrub", "side", "source", "note"]
PROVENANCE = ("source", "note")
CASE_RE = re.compile(r"^\s*#\s*(?:case\s*)?#?\s*(\d+)\b", re.I)


class IntakeError(Exception):
    pass


def _empty(v):
    return v is None or (hasattr(v, "__len__") and len(v) == 0) or (isinstance(v, str) and not v.strip())


def parse_inbox(text):
    """Return [(case_number, item_dict)]; raises IntakeError without quoting item text."""
    lines = text.splitlines()
    comments = [(i + 1, int(m.group(1))) for i, l in enumerate(lines) if (m := CASE_RE.match(l))]
    try:
        root = yaml.compose(text)
    except yaml.YAMLError:
        raise IntakeError("inbox is not valid YAML") from None
    if not isinstance(root, yaml.MappingNode):
        raise IntakeError("inbox top level is not a mapping")
    seq = next((v for k, v in root.value if k.value == "questions"), None)
    if not isinstance(seq, yaml.SequenceNode):
        raise IntakeError("inbox has no 'questions' list")
    data = yaml.safe_load(text)["questions"]
    starts = [n.start_mark.line + 1 for n in seq.value]
    if len(comments) != len(starts):
        raise IntakeError(f"{len(comments)} numbered comments but {len(starts)} items")
    nums = [n for _, n in comments]
    dup = sorted({n for n in nums if nums.count(n) > 1})
    if dup:
        raise IntakeError(f"duplicate case numbers: {dup}")
    out = []
    prev = 0
    for idx, (start, item) in enumerate(zip(starts, data)):
        above = [c for c in comments if prev < c[0] <= start]
        if not above:
            raise IntakeError(f"item {idx + 1} has no numbered comment above it")
        out.append((above[-1][1], item))
        prev = start
    if len({c for c, _ in out}) != len(out):
        raise IntakeError("a case number was assigned to two items")
    return out


def validate(case, item):
    if not isinstance(item, dict):
        raise IntakeError(f"case {case}: item is not a mapping")
    missing = [f for f in FIELDS if f not in item]
    if missing:
        raise IntakeError(f"case {case}: missing fields {missing}")
    if item["kind"] not in KINDS:
        raise IntakeError(f"case {case}: field 'kind' not an allowed value")
    if item["side"] not in SIDES:
        raise IntakeError(f"case {case}: field 'side' not an allowed value")
    for f in ("question", "essential"):
        if _empty(item[f]):
            raise IntakeError(f"case {case}: field '{f}' is empty")


def build_rows(parsed, intake_sha):
    rows = []
    for case, item in parsed:
        validate(case, item)
        row = {"id": f"oq-{case:03d}"}
        row.update({k: v for k, v in item.items() if k not in PROVENANCE})
        row["provenance"] = {k: item[k] for k in PROVENANCE}
        row["intake_sha256"] = intake_sha
        rows.append(row)
    return rows


def manifest_for(parsed, rows, inbox_sha, enc_sha):
    per = {}
    for (case, item), row in zip(parsed, rows):
        per[row["id"]] = {"case": case, "kind": item["kind"], "side": item["side"],
                          "versions_empty": _empty(item["versions"]), "scrub": item["scrub"]}
    return {"inbox_sha256": inbox_sha, "enc_sha256": enc_sha, "total": len(rows),
            "by_kind": dict(Counter(v["kind"] for v in per.values())),
            "by_side": dict(Counter(v["side"] for v in per.values())),
            "versions_empty": sum(v["versions_empty"] for v in per.values()),
            "items": per}


def summary(rows, parsed):
    ck = Counter(i["kind"] for _, i in parsed)
    cs = Counter(i["side"] for _, i in parsed)
    print(f"items: {len(rows)}")
    print("by kind: " + ", ".join(f"{k}={v}" for k, v in sorted(ck.items())))
    print("by side: " + ", ".join(f"{k}={v}" for k, v in sorted(cs.items())))
    print(f"empty versions: {sum(_empty(i['versions']) for _, i in parsed)}")
    print("ids: " + " ".join(r["id"] for r in rows))


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--inbox", default=str(ROOT / "eval/m4/inbox/owner_questions.yaml"))
    ap.add_argument("--out", default=str(ROOT / "eval/hidden/tier_b_owner.jsonl.enc"))
    ap.add_argument("--manifest", default=str(ROOT / "runs/m4/owner_questions_manifest.json"))
    ap.add_argument("--key-file", default=None)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--force", action="store_true")
    a = ap.parse_args(argv)
    out, man = Path(a.out), Path(a.manifest)
    try:
        raw = Path(a.inbox).read_bytes()
        inbox_sha = hashlib.sha256(raw).hexdigest()
        parsed = parse_inbox(raw.decode("utf-8"))
        rows = build_rows(parsed, inbox_sha)
        summary(rows, parsed)
        print(f"inbox sha256: {inbox_sha}")
        if a.dry_run:
            print("dry run: nothing written")
            return 0
        if out.exists() and not a.force:
            raise IntakeError("output exists; use --force to overwrite")
        data = "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows).encode("utf-8")
        blob = eval2.encrypt_bytes(data, a.key_file)
        eval2.write_atomic(out, blob)
        back = [json.loads(l) for l in eval2.decrypt_bytes(out.read_bytes(), a.key_file)
                .decode("utf-8").splitlines() if l.strip()]
        if [r["id"] for r in back] != [r["id"] for r in rows]:
            raise IntakeError("round trip failed: ids differ")
        enc_sha = hashlib.sha256(out.read_bytes()).hexdigest()
        eval2.write_atomic(man, (json.dumps(manifest_for(parsed, rows, inbox_sha, enc_sha),
                                            indent=2) + "\n").encode())
        print(f"round trip ok: {len(back)} rows, ids match")
        print(f"enc sha256: {enc_sha}")
        print(f"wrote {out} and {man}")
        return 0
    except (IntakeError, eval2.EvalError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    except (OSError, UnicodeDecodeError, KeyError, TypeError) as e:
        print(f"error: {type(e).__name__}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
