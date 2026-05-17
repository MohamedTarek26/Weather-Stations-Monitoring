#!/bin/bash
#
# JFR Profiling Script — Records a 60-second Java Flight Recorder session
# of the Central Station and generates a profiling report.
#
# Usage:
#   ./jfr_profile.sh              Local mode (start central-station with JFR)
#   ./jfr_profile.sh k8s          K8s mode (profile the running central-station pod)
#
# Output:
#   central_station_recording.jfr   Raw JFR recording
#   jfr_report.txt                  Parsed profiling report
#
# Required JFR metrics (from project spec):
#   1. Top 10 classes by total memory allocation
#   2. GC pause count
#   3. GC max pause duration
#   4. I/O operations list

set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
RECORDING_FILE="$PROJECT_DIR/central_station_recording.jfr"
REPORT_FILE="$PROJECT_DIR/jfr_report.txt"
DURATION=60

echo "╔══════════════════════════════════════════════╗"
echo "║  JFR Profiling — Central Station             ║"
echo "║  Duration: ${DURATION}s                              ║"
echo "╚══════════════════════════════════════════════╝"

if [ "$1" = "k8s" ]; then
    # ─── K8s Mode: Profile the running pod ────────────────────
    NAMESPACE="weather-monitoring"
    POD=$(kubectl get pods -n $NAMESPACE -l app=central-station -o jsonpath='{.items[0].metadata.name}')

    if [ -z "$POD" ]; then
        echo "ERROR: Central station pod not found. Is the cluster running?"
        exit 1
    fi

    echo "Profiling pod: $POD"
    echo ""

    # Copy custom JFR settings into the pod
    echo "[1/4] Copying JFR settings to pod..."
    kubectl cp "$PROJECT_DIR/jfr_custom.jfc" "$NAMESPACE/$POD:/app/jfr_custom.jfc"

    # Start JFR recording inside the pod
    echo "[2/4] Starting JFR recording (${DURATION}s)..."
    kubectl exec -n $NAMESPACE "$POD" -- \
        jcmd 1 JFR.start name=profile duration=${DURATION}s filename=/app/recording.jfr settings=/app/jfr_custom.jfc

    echo "[3/4] Waiting ${DURATION} seconds for recording to complete..."
    sleep $((DURATION + 5))

    # Copy the recording file out
    echo "[4/4] Copying recording from pod..."
    kubectl cp "$NAMESPACE/$POD:/app/recording.jfr" "$RECORDING_FILE"

else
    # ─── Local Mode: Start central-station with JFR ──────────
    JAR="$PROJECT_DIR/central-station/build/libs/central-station.jar"

    if [ ! -f "$JAR" ]; then
        echo "Building central-station JAR..."
        cd "$PROJECT_DIR" && ./gradlew :central-station:shadowJar --no-daemon
    fi

    echo "[1/2] Starting Central Station with JFR (${DURATION}s recording)..."
    echo "  The app will run for ${DURATION}s then JFR dumps the recording."
    echo ""

    java \
        -XX:StartFlightRecording=duration=${DURATION}s,filename="$RECORDING_FILE",settings="$PROJECT_DIR/jfr_custom.jfc" \
        -jar "$JAR" &
    APP_PID=$!

    echo "  Central Station PID: $APP_PID"
    echo "[2/2] Waiting ${DURATION}s for recording..."
    sleep $((DURATION + 5))

    # Graceful shutdown: SIGINT triggers the JVM shutdown hook
    echo "  Stopping Central Station..."
    kill -INT $APP_PID 2>/dev/null || true
    # Wait up to 10s for graceful shutdown
    for i in $(seq 1 10); do
        if ! kill -0 $APP_PID 2>/dev/null; then
            break
        fi
        sleep 1
    done
    # Force kill if still running
    kill -9 $APP_PID 2>/dev/null || true
    wait $APP_PID 2>/dev/null || true
fi

# ─── Analyze the recording ────────────────────────────────────

if [ ! -f "$RECORDING_FILE" ]; then
    echo "ERROR: Recording file not found at $RECORDING_FILE"
    exit 1
fi

echo ""
echo "=== Analyzing JFR Recording ==="
echo "Recording: $RECORDING_FILE ($(du -h "$RECORDING_FILE" | cut -f1))"
echo ""

{
    echo "═══════════════════════════════════════════════"
    echo "  JFR Profiling Report — Central Station"
    echo "  Date: $(date)"
    echo "  Duration: ${DURATION}s"
    echo "═══════════════════════════════════════════════"
    echo ""

    # ── METRIC 1: Top 10 Classes by Total Memory ─────────────
    echo "─── 1. Top 10 Classes by Total Memory Allocation ─────"
    echo ""
    jfr print --events jdk.ObjectAllocationInNewTLAB,jdk.ObjectAllocationOutsideTLAB \
        "$RECORDING_FILE" 2>/dev/null | \
        grep -oP 'objectClass = \K\S+' | \
        sort | uniq -c | sort -rn | head -10 || \
        echo "(No allocation events — try 'profile' settings for more detail)"
    echo ""

    # ── METRIC 2: GC Pause Count ─────────────────────────────
    echo "─── 2. GC Pause Count ────────────────────────────────"
    echo ""
    GC_COUNT=$(jfr print --events jdk.GarbageCollection "$RECORDING_FILE" 2>/dev/null | \
        grep -c "jdk.GarbageCollection" || echo "0")
    echo "  Total GC pause events: $GC_COUNT"
    echo ""
    echo "  GC Events by type:"
    jfr print --events jdk.GarbageCollection "$RECORDING_FILE" 2>/dev/null | \
        grep -oP 'name = \K.*' | sort | uniq -c | sort -rn || \
        echo "  (No GC events captured)"
    echo ""

    # ── METRIC 3: GC Max Pause Duration ──────────────────────
    echo "─── 3. GC Max Pause Duration ─────────────────────────"
    echo ""
    jfr print --events jdk.GCPhasePause "$RECORDING_FILE" 2>/dev/null | \
        grep -oP 'duration = \K[^\s]+' | \
        sort -t' ' -k1 -rn | head -1 | \
        while read dur; do echo "  Max GC pause: $dur"; done
    if [ $? -ne 0 ] || [ -z "$(jfr print --events jdk.GCPhasePause "$RECORDING_FILE" 2>/dev/null | grep duration)" ]; then
        # Try alternative event
        echo "  GC Pause durations from GarbageCollection events:"
        jfr print --events jdk.GarbageCollection "$RECORDING_FILE" 2>/dev/null | \
            grep -oP 'duration = \K[^\s]+' | \
            sort -t' ' -k1 -rn | head -5 || echo "  (No GC pause data)"
    fi
    echo ""

    # ── METRIC 4: I/O Operations List ────────────────────────
    echo "─── 4. I/O Operations ────────────────────────────────"
    echo ""
    echo "  File I/O (reads/writes):"
    jfr print --events jdk.FileRead,jdk.FileWrite "$RECORDING_FILE" 2>/dev/null | \
        grep -E '(path|bytesRead|bytesWritten|duration)' | head -40 || \
        echo "  (No file I/O events captured)"
    echo ""
    echo "  Socket I/O (network):"
    jfr print --events jdk.SocketRead,jdk.SocketWrite "$RECORDING_FILE" 2>/dev/null | \
        grep -E '(host|port|bytesRead|bytesWritten|duration)' | head -40 || \
        echo "  (No socket I/O events captured)"
    echo ""

    # ── BONUS: CPU & Thread Overview ─────────────────────────
    echo "─── 5. CPU Usage (bonus) ─────────────────────────────"
    echo ""
    jfr print --events jdk.CPULoad "$RECORDING_FILE" 2>/dev/null | \
        grep -E '(jvmUser|jvmSystem|machineTotal)' | tail -6 || \
        echo "  (No CPU events captured)"
    echo ""

    echo "─── 6. Thread Activity (bonus) ───────────────────────"
    echo ""
    jfr print --events jdk.ThreadStart "$RECORDING_FILE" 2>/dev/null | \
        grep -oP 'thread = "\K[^"]+' | sort | uniq -c | sort -rn | head -10 || \
        echo "  (No thread events captured)"
    echo ""

    echo "═══════════════════════════════════════════════"
    echo "  Full recording: $RECORDING_FILE"
    echo "  Open in JDK Mission Control: jmc $RECORDING_FILE"
    echo "  Or view raw: jfr print $RECORDING_FILE"
    echo "═══════════════════════════════════════════════"
} | tee "$REPORT_FILE"

echo ""
echo "Report saved to: $REPORT_FILE"
