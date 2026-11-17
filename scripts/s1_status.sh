#!/bin/sh
# One short status report for stage 1 (at most about 15 lines): running jobs, markers, the latest progress line,
# guard events, loaded models. Meant for cheap checks and as the last command of a waiting loop.
cd "$(dirname "$0")/.." || exit 1
L=runs/v3/logs
echo "time     $(date '+%Y-%m-%d %H:%M')"
echo "jobs     $(pgrep -fl 'scripts/s1_[a-z0-9_]*\.sh' | awk '{print $NF}' | sort -u | tr '\n' ' ')"
echo "guarded  $(pgrep -f run_guarded.py | wc -l | tr -d ' ') step(s) running"
echo "done     $(ls -t $L/*.done 2>/dev/null | head -4 | xargs -n1 basename 2>/dev/null | tr '\n' ' ')"
[ -n "$(ls $L/*.failed 2>/dev/null)" ] && echo "FAILED   $(ls $L/*.failed | xargs -n1 basename | tr '\n' ' ')"
N=$(ls -t $L/*.log 2>/dev/null | head -1)
[ -n "$N" ] && echo "log      $(basename $N) ($(( ($(date +%s) - $(stat -f %m $N)) / 60 )) min ago): $(tail -1 $N | cut -c1-110)"
echo "guard    $(grep -h 'GUARD:' $L/*.log 2>/dev/null | grep -c "$(date +%Y-%m-%d)") event(s) today; last: $(grep -h 'GUARD:' $L/*.log 2>/dev/null | tail -1 | cut -c8-100)"
URL=${OMLX_URL:-$(cat "$HOME/.config/kafka-sre-llm/omlx_url" 2>/dev/null)}; KEY=$(cat "$HOME/.config/kafka-sre-llm/judge_key" 2>/dev/null)
echo "server   $(curl -s -m 10 -H "Authorization: Bearer $KEY" "$URL/api/status" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(",".join(d.get("loaded_models") or ["none"]), "| active", d.get("active_requests"), "waiting", d.get("waiting_requests"))' 2>/dev/null || echo unreachable)"
echo "state    $(python3 -c 'import json; d=json.load(open("runs/v3/status.json")); print(d.get("state"), "|", d.get("phase","")[:90], "| needs_human", d.get("needs_human"))' 2>/dev/null)"
echo "last hb  $(grep '^### ' runs/v3/HEARTBEAT.md | tail -1 | cut -c1-140)"
