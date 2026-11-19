#!/usr/bin/env python3
"""Read and write tool for sealed sessions (hidden judged items, rubrics, reviews).

Uses encrypt_bytes / decrypt_bytes of eval2.py (same key file, same format). Plain text
exists only in memory and on stdout/stdin: no temporary file is ever written. Error
messages name ids, line numbers and field names only, never row text.

  show FILE.enc [--ids a,b]        print the rows (sealed sessions only)
  seal OUT.enc [--schema S] [--append]   read JSONL from stdin, validate, encrypt
  ids FILE.enc                     print ids and count
  stats FILE.enc --schema rubric   counts only

Key: EVAL_HIDDEN_KEY or ~/.config/kafka-sre-llm/eval_hidden.key.
"""
import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from eval2 import EvalError, decrypt_bytes, encrypt_bytes  # noqa: E402

BANNER = "SEALED CONTENT - do not copy into a report or reply"
VERDICTS = ("accept", "change", "drop")
REQUIRED = {
    "rubric": ("id", "must", "must_not", "any_of"),
    "item": ("id", "tier", "kind", "family", "domain", "question", "evidence",
             "failure_classes", "versions"),
    "review": ("id", "verdict", "changes"),
}


class SealedError(Exception):
    pass


def parse_rows(text):
    rows = []
    for n, line in enumerate(text.splitlines(), 1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except ValueError:
            raise SealedError(f"line {n}: not valid JSON") from None
        if not isinstance(row, dict):
            raise SealedError(f"line {n}: not a JSON object")
        if not isinstance(row.get("id"), str) or not row["id"]:
            raise SealedError(f"line {n}: missing field 'id'")
        rows.append(row)
    return rows


def check_schema(rows, schema):
    seen = set()
    for row in rows:
        rid = row["id"]
        if rid in seen:
            raise SealedError(f"duplicate id {rid} in input")
        seen.add(rid)
        if not schema:
            continue
        for field in REQUIRED[schema]:
            if field not in row:
                raise SealedError(f"id {rid}: missing field '{field}'")
        if schema == "review" and row["verdict"] not in VERDICTS:
            raise SealedError(f"id {rid}: field 'verdict' not in {{accept, change, drop}}")
        if schema == "rubric":
            for field in ("must", "must_not", "any_of"):
                if not isinstance(row[field], list):
                    raise SealedError(f"id {rid}: field '{field}' is not a list")


def load(path):
    blob = Path(path).read_bytes()
    return parse_rows(decrypt_bytes(blob).decode("utf-8"))


def dump(rows):
    return "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows).encode("utf-8")


def write_blob(path, blob, replace):
    """Atomic: ciphertext goes to a sibling file, then is linked or renamed into place."""
    path = Path(path)
    tmp = path.with_name(path.name + f".tmp{os.getpid()}")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(blob)
            f.flush()
            os.fsync(f.fileno())
        if replace:
            os.replace(tmp, path)
        else:
            os.link(tmp, path)  # fails if the file exists: no overwrite
    finally:
        if tmp.exists():
            tmp.unlink()


def cmd_show(a):
    rows = load(a.file)
    if a.ids:
        want = [x for x in a.ids.split(",") if x]
        have = {r["id"] for r in rows}
        missing = [x for x in want if x not in have]
        if missing:
            raise SealedError("unknown ids: " + ",".join(missing))
        rows = [r for r in rows if r["id"] in want]
    print(BANNER)
    for r in rows:
        print(json.dumps(r, ensure_ascii=False))


def cmd_seal(a):
    rows = parse_rows(sys.stdin.read())
    if not rows:
        raise SealedError("no rows on stdin")
    check_schema(rows, a.schema)
    out = Path(a.out)
    if a.append:
        if not out.exists():
            raise SealedError("--append: file does not exist")
        old = load(out)
        dup = sorted({r["id"] for r in rows} & {r["id"] for r in old})
        if dup:
            raise SealedError("duplicate ids already in file: " + ",".join(dup))
        allrows = old + rows
    else:
        if out.exists():
            raise SealedError("output exists; use --append or choose a new path")
        allrows = rows
    blob = encrypt_bytes(dump(allrows))
    write_blob(out, blob, replace=a.append)
    print(f"written {len(rows)} rows ({len(allrows)} in file)")
    print("ids " + ",".join(r["id"] for r in rows))
    print("sha256 " + hashlib.sha256(blob).hexdigest())


def cmd_ids(a):
    rows = load(a.file)
    print(f"count {len(rows)}")
    for r in rows:
        print(r["id"])


def _counts(row):
    must = row.get("must") or []
    return {
        "must": len(must),
        "weight2": sum(1 for m in must if isinstance(m, dict) and m.get("weight") == 2),
        "any_of": len(row.get("any_of") or []),
        "must_not": len(row.get("must_not") or []),
        "not_indexed": sum(1 for m in must if isinstance(m, dict) and m.get("not_indexed")),
    }


def cmd_stats(a):
    rows = load(a.file)
    total = dict.fromkeys(("must", "weight2", "any_of", "must_not", "not_indexed"), 0)
    for r in rows:
        c = _counts(r)
        print(r["id"] + " " + " ".join(f"{k}={v}" for k, v in c.items()))
        for k, v in c.items():
            total[k] += v
    print("total " + " ".join(f"{k}={v}" for k, v in total.items()) + f" rows={len(rows)}")


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("show")
    s.add_argument("file")
    s.add_argument("--ids")
    s.set_defaults(fn=cmd_show)
    s = sub.add_parser("seal")
    s.add_argument("out")
    s.add_argument("--schema", choices=sorted(REQUIRED))
    s.add_argument("--append", action="store_true")
    s.set_defaults(fn=cmd_seal)
    s = sub.add_parser("ids")
    s.add_argument("file")
    s.set_defaults(fn=cmd_ids)
    s = sub.add_parser("stats")
    s.add_argument("file")
    s.add_argument("--schema", choices=["rubric"], required=True)
    s.set_defaults(fn=cmd_stats)
    a = p.parse_args(argv)
    try:
        a.fn(a)
    except (SealedError, EvalError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    except OSError as e:
        print(f"error: {type(e).__name__}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
