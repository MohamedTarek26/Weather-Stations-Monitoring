#!/bin/bash
#
# BitCask Client — CLI tool for querying the Central Station's BitCask store.
#
# Usage:
#   ./bitcask_client.sh --view-all                 Print all keys+values to <timestamp>.csv
#   ./bitcask_client.sh --view --key=SOME_KEY       Print value for a specific key
#   ./bitcask_client.sh --perf --clients=100        Stress test with N concurrent threads
#
# Configuration:
#   CENTRAL_HOST  - Central station hostname (default: localhost)
#   CENTRAL_PORT  - Central station API port (default: 8080)

CENTRAL_HOST="${CENTRAL_HOST:-localhost}"
CENTRAL_PORT="${CENTRAL_PORT:-8080}"
BASE_URL="http://${CENTRAL_HOST}:${CENTRAL_PORT}"

# ─── Parse arguments ─────────────────────────────────────────

MODE=""
KEY=""
CLIENTS=100

for arg in "$@"; do
    case "$arg" in
        --view-all)  MODE="view-all" ;;
        --view)      MODE="view" ;;
        --perf)      MODE="perf" ;;
        --key=*)     KEY="${arg#--key=}" ;;
        --clients=*) CLIENTS="${arg#--clients=}" ;;
    esac
done

# ─── view-all: Fetch all keys → write to timestamped CSV ─────

do_view_all() {
    local output_dir="./data/client_output"
    mkdir -p "$output_dir"

    local timestamp
    timestamp=$(date +%s)
    local filename="${output_dir}/${timestamp}${1}.csv"  # $1 is suffix (empty or _thread_N)

    echo "key,value" > "$filename"

    # Fetch all key-value pairs as JSON
    local response
    response=$(curl -s "${BASE_URL}/view-all")

    if [ $? -ne 0 ] || [ -z "$response" ]; then
        echo "ERROR: Could not connect to Central Station at ${BASE_URL}"
        return 1
    fi

    # Parse JSON object {"key1":"val1","key2":"val2"} → CSV lines
    # Using python for reliable JSON parsing
    echo "$response" | python3 -c '
import json, sys, csv
data = json.load(sys.stdin)
writer = csv.writer(sys.stdout)
for k, v in data.items():
    writer.writerow([k, v])
' >> "$filename"

    echo "Written to: $filename ($(wc -l < "$filename") lines)"
}

# ─── view: Fetch a single key ────────────────────────────────

do_view() {
    if [ -z "$KEY" ]; then
        echo "ERROR: --key=SOME_KEY is required with --view"
        echo "Usage: $0 --view --key=SOME_KEY"
        exit 1
    fi

    local response
    response=$(curl -s "${BASE_URL}/view?key=${KEY}")

    if [ $? -ne 0 ]; then
        echo "ERROR: Could not connect to Central Station at ${BASE_URL}"
        exit 1
    fi

    echo "$response"
}

# ─── perf: Stress test with N concurrent threads ─────────────

do_perf() {
    echo "Starting performance test with ${CLIENTS} clients..."

    for i in $(seq 1 "$CLIENTS"); do
        (
            do_view_all "_thread_${i}"
        ) &
    done

    echo "Waiting for all ${CLIENTS} threads to complete..."
    wait
    echo "Performance test complete. Check *_thread_*.csv files."
}

# ─── Main ─────────────────────────────────────────────────────

case "$MODE" in
    view-all) do_view_all "" ;;
    view)     do_view ;;
    perf)     do_perf ;;
    *)
        echo "BitCask Client — Query the Central Station's BitCask store"
        echo ""
        echo "Usage:"
        echo "  $0 --view-all                  Dump all keys to <timestamp>.csv"
        echo "  $0 --view --key=SOME_KEY        Get a specific key's value"
        echo "  $0 --perf --clients=100         Stress test with 100 threads"
        echo ""
        echo "Environment:"
        echo "  CENTRAL_HOST=${CENTRAL_HOST}  CENTRAL_PORT=${CENTRAL_PORT}"
        exit 1
        ;;
esac
