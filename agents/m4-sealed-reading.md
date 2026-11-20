# Brief: sealed blind reading of hidden answers (M4 evaluation, J2)

Template. The conductor fills in the two values in angle brackets and sends it unchanged
otherwise. It is the open readers' brief (B1) adapted to a sealed session.

You are a blind reader of answers that an assistant for Apache Kafka 4.3, Strimzi 1.3.1,
Kubernetes 1.37 and Argo CD 3.6.1 gave to scenarios. The rates of wrong, invented and harmful
statements come from your findings. Read-only for the repository, CPU only, no model server.
Working directory: the repository root.

Your bundle: `<BUNDLE FILE, e.g. DIR/first_3.jsonl.enc>`. Your findings file:
`<DIR>/findings_<bundle name>.jsonl.enc` (for `first_3.jsonl.enc` it is `findings_first_3.jsonl.enc`).

## Sealed rules
- This content is sealed. Read the bundle only with
  `.venv/bin/python scripts/sealed_io.py show <bundle> --ids <one row id>`, one row per
  call. `show` without `--ids` on the whole bundle is not allowed; `ids <bundle>` lists the
  row ids.
- Write findings only with
  `.venv/bin/python scripts/sealed_io.py seal <findings file> --schema finding`
  (the first call) and `... --schema finding --append` (every later call), JSON Lines on
  standard input (a heredoc), one call per answer or per few answers. Never write a file,
  never a temporary file, never a copy of any text anywhere.
- Never open, list or decrypt anything else under `eval/hidden/`, `work/m4/` (except
  `work/m4/catalogues/`), `runs/`, `eval/m4/`, `MILESTONE.md`, or another reader's bundle or
  findings. Do not look for the key file. Do not try to find out how an answer was made
  (mode, model, settings); read each answer on its own.
- You may open for checking facts: `raw/`, `raw_catalogue/`, `work/m4/catalogues/`,
  `work/v3/chunks_s1.jsonl` (to look up a chunk by id), `data/runbooks/`, `sources.yaml`.
- Read the protocol in `agents/m4-evaluation.md` lines 266-306 ("Wrong, invented and harmful:
  where the rates come from"). Nothing else of that file is needed.

## A row of the bundle
`id` (the answer's blind name, `<item>-<letter>`), `item`, `question`, `evidence` (pasted by
the user, may be empty), `points` (the rubric's must points with ids and weights, the any_of
groups, the must-not points with their conditions and near-misses), `reference` (for you, not
for a score), `answer`, `excerpts` (the passages the assistant was given for this answer).

## What to do, per answer of every row in your bundle
1. **Must points.** For every must point (an any_of group once, as `m3|m4`), decide whether
   the answer states it. Use the judge's standard, so that both count the same thing:
   stated only if the answer states the point explicitly and specifically enough that an
   engineer following the answer would act on it correctly; a setting, flag, field,
   annotation, metric, API path or HTTP method counts only if named as in the point; a vague
   mention ("check the configuration", "look at the logs") is not stated; if the point says
   it must be the main or first recommendation and the answer gives it only as one option
   among several, it is not stated; if the answer states it and contradicts it elsewhere,
   it is not stated (write "contradicted" in the note). Every point not stated is an
   `omission` row with `point` and `supported`.
2. **`supported`** (omissions only): true if a passage in that row's `excerpts` contains the
   point's fact itself (the rubric's chunk ids are a hint, not the rule). For an any_of
   group, true if any member's fact is there. A point marked "rests on a source outside the
   index" is never supported.
3. **Wrong, invented, harmful.** Every distinct statement of the answer that is false for
   the pinned versions (`wrong`), names an identifier that does not exist in the pinned
   versions (`invented`: a configuration key, resource field, annotation, command, tool
   option, API, metric, error name or similar; check the catalogues and the sources), or
   recommends something that would lose data, cause an outage or open a security hole in
   this scenario (`harmful`). Not only statements the rubric foresees. A statement that a
   must-not point describes is one of these kinds; name the must-not id in the note
   ("n1: ..."), and respect its condition and near-miss. Give a verbatim `quote` (copy it
   exactly; the totals tool matches it against the answer after whitespace normalisation; a
   quote that is not found is not counted), the `source` file and lines that show it is
   wrong, and `minor` true only for a small side error that would not mislead an engineer.
4. **Open the source for every finding.** Do not write from memory. A statement the sources do
   not settle is not a finding.
5. One row per distinct statement. If the same false statement appears twice in one answer,
   one row.

## Findings row (all nine fields, every row)
`{"answer": "<item>-A", "id": "<item>", "kind": "wrong"|"invented"|"harmful"|"omission",
"quote": "...", "point": "", "supported": false, "minor": false, "source": "...", "note": "..."}`
- `answer` is the row's `id`; `id` is the row's `item`.
- Omission: `quote` is `""`, `point` is `m2` or `m3|m4`, `supported` is true or false.
- Other kinds: `quote` is not empty, `point` is `""`, `supported` is false.
- An answer you read and found nothing on needs a **clean row**:
  `{"answer": "<item>-A", "id": "<item>", "kind": "clean", "quote": "", "point": "",
  "supported": false, "minor": false, "source": "", "note": ""}`. Every row of your bundle
  must end with at least one finding row or one clean row, never both; the totals tool
  stops with an error on an answer that has neither, so an unread answer cannot pass as a
  clean one.
- If `seal` reports an error, fix the rows it names and call it again.

## Reply
Nothing in plain text anywhere: no item text, quote, note, identifier, finding or judgement,
in the reply or in any file. The reply holds only:
1. the number of findings rows written (the sum of the `written N rows` lines, clean rows included);
2. the ids of all bundle rows you read (the blind answer names), as one comma-separated line;
3. the sha256 line that the last `seal` call printed.
A bundle with no finding still gets its findings file, made of clean rows.
