#!/usr/bin/env bash
set -euo pipefail

REPEATS=3
VARIANTS=(0 1)

for run in $(seq 1 "$REPEATS"); do
  for variant in "${VARIANTS[@]}"; do
    echo "run $run: $variant"

    sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'

    # Variant 0: no cache flag
    # Variant 1: enable cache
    args=()
    if [[ "$variant" -eq 1 ]]; then
      args+=(--use_cache)
    fi

    spark-submit \
      --master "spark://$(hostname):7077" \
      --driver-memory 2g \
      --executor-cores 1 \
      --executor-memory 2g \
      --total-executor-cores 4 \
      ../src/query1.py \
      "${args[@]}" \
      > "results/query1/DF/${variant}_run${run}.txt" \
      2> "results/query1/DF/${variant}_run${run}.log"
  done
done

# THEN ALSO IMPLEMENT A LOOP FOR SQL APIs METHOD
