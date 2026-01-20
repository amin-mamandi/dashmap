#!/usr/bin/env bash
set -euo pipefail

BIN=./target/release/dashmap_ycsb

# Workloads to test (must match clap ValueEnum)
WORKLOADS=(a b c d e f)

# Thread counts to test
THREADS=(1 2 4 8 16 32)

# Fixed YCSB-like parameters
RECORDS=6500000
OPS=200000000
ZIPFIAN=true
ZIPF_FLAG=""
if [ "$ZIPFIAN" = true ]; then
  ZIPF_FLAG="--zipfian"
fi
ZIPF_S=1.03

OUTCSV=results.csv

# Write CSV header
echo "workload,threads,records,ops,dist,zipf_s,elapsed_sec,throughput_Mops" > "$OUTCSV"

echo "Starting sweep at $(date)"
echo "Writing results to $OUTCSV"
echo

for wl in "${WORKLOADS[@]}"; do
  for th in "${THREADS[@]}"; do
    echo "=== workload=$wl threads=$th ==="

    # Run benchmark and capture output
    OUT=$(
        $BIN \
        --workload "$wl" \
        --threads "$th" \
        --recordcount "$RECORDS" \
        --operationcount "$OPS" \
        $ZIPF_FLAG \
        --zipf-s "$ZIPF_S"
    )

    echo "$OUT"

    # Example line:
    # Completed workloada in 8.701s | throughput = 22.99 Mops/s
    LINE=$(echo "$OUT" | grep "Completed")

    # Parse fields
    ELAPSED=$(echo "$LINE" | sed -E 's/.*in ([0-9.]+)s.*/\1/')
    THR=$(echo "$LINE" | sed -E 's/.*throughput = ([0-9.]+) Mops\/s.*/\1/')

    DIST=$( [ "$ZIPFIAN" = true ] && echo "zipfian" || echo "uniform" )

    # Append CSV row
    echo "$wl,$th,$RECORDS,$OPS,$DIST,$ZIPF_S,$ELAPSED,$THR" >> "$OUTCSV"

    echo "→ elapsed=${ELAPSED}s throughput=${THR} Mops/s"
    echo
    sleep 2   # small cooldown (optional)
  done
done

echo "Sweep finished at $(date)"
echo "CSV written to $OUTCSV"
