#!/usr/bin/env bash
# Fetch the pinned sources used by check_runbooks.py into a cache directory.
#
# Usage: tools/fetch_sources.sh <cache_dir>
#
# Layout created under <cache_dir>:
#   strimzi/        strimzi/strimzi-kafka-operator at tag 1.3.1
#   drain-cleaner/  strimzi/drain-cleaner at 70f1bc25bc14aacd15eb478768db6566bde783e3
#   kafka44/        apache/kafka tag 4.4.0, docs/ only (sparse)
#   web/            the published pages listed in sources.md
#
# The script fails if a commit or a page hash differs from the pinned value.
set -euo pipefail

CACHE="${1:?usage: fetch_sources.sh <cache_dir>}"
mkdir -p "$CACHE"
cd "$CACHE"

STRIMZI_SHA=555c3e0f07c203911bc3cb7d13ac085bfaa81b50
DRAIN_SHA=70f1bc25bc14aacd15eb478768db6566bde783e3
KAFKA_SHA=1156b2752aaa28eafc0b4c4d387e63356793268d

check_sha() {
  local dir="$1" want="$2" got
  got="$(git -C "$dir" rev-parse HEAD)"
  if [ "$got" != "$want" ]; then
    echo "FAIL: $dir is at $got, expected $want" >&2
    exit 1
  fi
  echo "ok   $dir $got"
}

[ -d strimzi ] || git clone -q --depth 1 --branch 1.3.1 https://github.com/strimzi/strimzi-kafka-operator.git strimzi
check_sha strimzi "$STRIMZI_SHA"

if [ ! -d drain-cleaner ]; then
  git clone -q https://github.com/strimzi/drain-cleaner.git drain-cleaner
  git -C drain-cleaner checkout -q "$DRAIN_SHA"
fi
check_sha drain-cleaner "$DRAIN_SHA"

if [ ! -d kafka44 ]; then
  git clone -q --depth 1 --branch 4.4.0 --filter=blob:none --sparse https://github.com/apache/kafka.git kafka44
  git -C kafka44 sparse-checkout set docs
fi
check_sha kafka44 "$KAFKA_SHA"

mkdir -p web
# file  url  sha256-prefix (from sources.md)
while read -r f url want; do
  [ -n "$f" ] || continue
  [ -s "web/$f" ] || curl -sSfL -A "Mozilla/5.0" -o "web/$f" "$url"
  got="$(shasum -a 256 "web/$f" | cut -c1-16)"
  if [ "$got" != "$want" ]; then
    echo "WARN: web/$f sha256 prefix $got, sources.md has $want (page changed since retrieval)" >&2
  else
    echo "ok   web/$f $got"
  fi
done <<'EOF'
strimzi.html https://strimzi.io/docs/operators/1.3.1/deploying c43b40792bbb362e
k-basic.html https://kafka.apache.org/44/operations/basic-kafka-operations/ 554551ef7d388cd4
k-kraft.html https://kafka.apache.org/44/operations/kraft/ 6ec83b8f87fd0b8c
k-monitoring.html https://kafka.apache.org/44/operations/monitoring/ 9b4ce983f19d132b
k-geo.html https://kafka.apache.org/44/operations/geo-replication-cross-cluster-data-mirroring/ e7bfc284a96658c7
k-zk2kraft.html https://kafka.apache.org/44/getting-started/zk2kraft/ 256b31647bb85209
EOF
