#!/usr/bin/env bash
# Fetch the pinned sources used by check_runbooks.py into a cache directory.
#
# Usage: tools/fetch_sources.sh <cache_dir>
#
# Layout created under <cache_dir>:
#   strimzi/        strimzi/strimzi-kafka-operator at tag 1.3.1
#   drain-cleaner/  strimzi/drain-cleaner at 70f1bc25bc14aacd15eb478768db6566bde783e3
#   kafka43/        apache/kafka tag 4.3.2, docs/ only (sparse)
#   web/            the published pages listed in sources.md
#
# The script fails if a commit or a page hash differs from the pinned value.
# Page hashes are in tools/pinned_pages.txt, which check_runbooks.py also reads.
set -euo pipefail

CACHE="${1:?usage: fetch_sources.sh <cache_dir>}"
PAGES="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/pinned_pages.txt"
mkdir -p "$CACHE"
cd "$CACHE"

STRIMZI_SHA=555c3e0f07c203911bc3cb7d13ac085bfaa81b50
DRAIN_SHA=70f1bc25bc14aacd15eb478768db6566bde783e3
KAFKA_SHA=0a6c96d3b61167f1141df2032f25f10fd6e118fe

check_sha() {
  local dir="$1" want="$2" got
  got="$(git -C "$dir" rev-parse HEAD)"
  if [ "$got" != "$want" ]; then
    echo "FAIL: $dir is at $got, expected $want" >&2
    exit 1
  fi
  # HEAD alone does not prove the bytes: refuse modified or untracked files
  local dirty
  dirty="$(git -C "$dir" status --porcelain --untracked-files=all --ignored=matching)"
  if [ -n "$dirty" ]; then
    echo "FAIL: $dir working tree differs from $want:" >&2
    echo "$dirty" | head -10 >&2
    exit 1
  fi
  echo "ok   $dir $got (clean)"
}

[ -d strimzi ] || git clone -q --depth 1 --branch 1.3.1 https://github.com/strimzi/strimzi-kafka-operator.git strimzi
check_sha strimzi "$STRIMZI_SHA"

if [ ! -d drain-cleaner ]; then
  git clone -q https://github.com/strimzi/drain-cleaner.git drain-cleaner
  git -C drain-cleaner checkout -q "$DRAIN_SHA"
fi
check_sha drain-cleaner "$DRAIN_SHA"

if [ ! -d kafka43 ]; then
  git clone -q --depth 1 --branch 4.3.2 --filter=blob:none --sparse https://github.com/apache/kafka.git kafka43
  git -C kafka43 sparse-checkout set docs
fi
check_sha kafka43 "$KAFKA_SHA"

mkdir -p web
# file  url  sha256-prefix, shared with check_runbooks.py
fail=0
while read -r f url want; do
  case "$f" in ''|'#'*) continue ;; esac
  [ -s "web/$f" ] || curl -sSfL -A "Mozilla/5.0" -o "web/$f" "$url"
  got="$(shasum -a 256 "web/$f" | cut -c1-16)"
  if [ "$got" != "$want" ]; then
    echo "FAIL: web/$f sha256 prefix $got, pinned $want (page changed since retrieval, or a bad cached copy)" >&2
    fail=1
  else
    echo "ok   web/$f $got"
  fi
done < "$PAGES"
exit "$fail"
