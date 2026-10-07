#!/usr/bin/env bash
# Collects the JSON lines a k6 matrix run printed into sqlmeasure-compatible JSONL and
# recomputes their metrics from Prometheus.
#
#   tools/sqlmeasure/k6/collect.sh <kube-context> <namespace> <job-name> <out.jsonl>
#   go run ./tools/sqlmeasure -context dev -recompute <out.jsonl> -out <final.jsonl>
#
# k6 wraps console output as `time=... level=info msg="SQLMEASURE {...}"` with the inner
# quotes escaped; this unwraps it.
set -euo pipefail
ctx=$1; ns=$2; name=$3; out=$4
kubectl --context "$ctx" -n "$ns" logs -l "job-name=$name" --tail=-1 --all-containers=true \
  | python3 -c '
import re, sys, json
for line in sys.stdin:
    m = re.search(r"msg=\"SQLMEASURE (\{.*\})\"(\s+\w+=\S*)*\s*$", line.rstrip())
    if not m:
        continue
    raw = m.group(1).encode().decode("unicode_escape")
    try:
        json.loads(raw)
    except Exception:
        continue
    print(raw)
' > "$out"
echo "$(wc -l < "$out") runs written to $out"
