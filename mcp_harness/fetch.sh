#!/bin/bash
# The one download of the MCP harness: the two pinned manifests and the five pinned images in downloads.lock.json.
# Skips what is already present and checks every sha256 and image digest against the lock. Nothing else downloads.
set -euo pipefail
HARNESS_CTX_CHECK=defer  # fetch.sh calls no kubectl
source "$(dirname "$0")/lib.sh"
LOCK="$HARNESS/downloads.lock.json"
mkdir -p "$RAW"

"$PY" - "$LOCK" <<'EOF' > "$RAW/.fetch-plan"
import json, sys
lock = json.load(open(sys.argv[1]))
for m in lock["manifests"]:
    print("M", m["url"], m["path"].split("/")[-1], m["sha256"])
for i in lock["images"]:
    print("I", i["ref"], i["digest"])
EOF

while read -r t a b c; do
  if [ "$t" = M ]; then
    if [ ! -f "$RAW/$b" ]; then log "download $a"; curl -sSLf -o "$RAW/$b" "$a"; fi
    got=$(shasum -a 256 "$RAW/$b" | cut -d' ' -f1)
    [ "$got" = "$c" ] || { echo "sha256 mismatch for $b: $got" >&2; exit 1; }
    log "ok $b $got"
  else
    if ! docker image inspect "$a" >/dev/null 2>&1; then log "pull $a"; docker pull -q --platform linux/arm64 "$a"; fi
    got=$(docker image inspect --format '{{range .RepoDigests}}{{.}} {{end}}' "$a")
    case "$got" in *"$b"*) log "ok $a $b" ;; *) echo "digest mismatch for $a: $got" >&2; exit 1 ;; esac
  fi
done < "$RAW/.fetch-plan"
