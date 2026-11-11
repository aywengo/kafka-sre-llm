#!/bin/sh
# usage: run_retry.sh LOG cmd... ; reruns on failure (commands resume from their output files)
log=$1; shift
for i in 1 2 3 4 5 6 7 8 9 10; do
  "$@" >> "$log" 2>&1 && exit 0
  echo "retry $i after exit $?" >> "$log"; sleep 60
done
exit 1
