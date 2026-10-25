#!/usr/bin/env bash
set -euo pipefail

REPEATS=1
VARIANTS=(json-infer json-schema parquet)
OUT=results/query0/filters
mkdir -p "$OUT"

for run in $(seq 1 "$REPEATS"); do
  for variant in "${VARIANTS[@]}"; do
    echo "run $run: $variant"
    sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'
    spark-submit \
      --master spark://$(hostname):7077 \
      --driver-memory 2g \
      --conf spark.eventLog.enabled=false \
      --executor-cores 1 \
      --executor-memory 2g \
      --total-executor-cores 4 \
      ../src/query0.py --explain-only "$variant" \
      > "$OUT/${variant}_run${run}.txt" 2> "$OUT/${variant}_run${run}.log"
  done
done
