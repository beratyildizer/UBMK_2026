#!/bin/bash

set -e

BENCHMARK="./queue"

FILE=QUEUE_IO_LOAD
NUM_MESSAGES=1000000
CONSUMER_PRIORITY=98
PRODUCER_PRIORITY=98

PAYLOADS=(100 1000 5000 10000)
RUNS=5

STRESS_TIMEOUT=180
STABILIZATION_TIME=15
COOLDOWN_TIME=10

STRESS_PID=""

cleanup() {
    if [ -n "$STRESS_PID" ] && kill -0 "$STRESS_PID" 2>/dev/null; then
        echo "Cleaning up stress-ng PID: $STRESS_PID"
        kill "$STRESS_PID" 2>/dev/null || true
        wait "$STRESS_PID" 2>/dev/null || true
    fi
}

trap cleanup EXIT INT TERM

OUT_DIR="${FILE}_results_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUT_DIR"

echo "Output directory: $OUT_DIR"

if [ ! -x "$BENCHMARK" ]; then
    echo "Error: Benchmark executable not found or not executable: $BENCHMARK"
    echo "Try: chmod +x pipe"
    exit 1
fi

if ! command -v stress-ng >/dev/null 2>&1; then
    echo "Error: stress-ng is not installed or not found."
    echo "Install it with: sudo apt install stress-ng"
    exit 1
fi

for payload in "${PAYLOADS[@]}"; do
    for run in $(seq 1 "$RUNS"); do

        LOG_FILE="$OUT_DIR/${FILE}_${payload}B_run${run}.log"
        STRESS_LOG="$OUT_DIR/stress_ng_${payload}B_run${run}.log"

        echo "--------------------------------------------------"
        echo "Starting io load for payload=${payload} bytes, run=${run}..."
		
		stress-ng --iomix 4 --timeout ${STRESS_TIMEOUT}s --metrics-brief > "$STRESS_LOG" &

        STRESS_PID=$!

        echo "stress-ng PID: $STRESS_PID"
        echo "Waiting ${STABILIZATION_TIME} seconds for load stabilization..."
        sleep "$STABILIZATION_TIME"

        if ! kill -0 "$STRESS_PID" 2>/dev/null; then
            echo "ERROR: stress-ng stopped before benchmark started!"
            echo "This run is invalid because io load was not active at benchmark start."
            echo "Payload: ${payload} bytes, run: ${run}"
            echo "Check stress-ng log: $STRESS_LOG"
            exit 1
        fi

        echo "stress-ng is active. Starting benchmark..."
        echo "Running benchmark payload=${payload} bytes, run=${run}..."

        "$BENCHMARK" "$NUM_MESSAGES" "$payload" "$CONSUMER_PRIORITY" "$PRODUCER_PRIORITY" > "$LOG_FILE"

        echo "Benchmark finished for payload=${payload} bytes, run=${run}."

        if ! kill -0 "$STRESS_PID" 2>/dev/null; then
            echo "ERROR: stress-ng stopped before benchmark finished!"
            echo "This run is invalid because the benchmark may not have been fully executed under io load."
            echo "Payload: ${payload} bytes, run: ${run}"
            echo "Check stress-ng log: $STRESS_LOG"
            exit 1
        fi

        cleanup
        STRESS_PID=""

        echo "Cooling down for ${COOLDOWN_TIME} seconds..."
        sleep "$COOLDOWN_TIME"

    done
done

echo "--------------------------------------------------"
echo "All tests completed."
echo "Results are saved in: $OUT_DIR"
