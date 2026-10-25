#!/usr/bin/env bash
# Stop the ivm demo started by start.sh. With --clean, also delete its data
# directory (upstream cluster included, so no replication slot is left behind).
set -euo pipefail

REPO=$(cd "$(dirname "$0")/../.." && pwd)
DEMO_DIR=${DEMO_DIR:-$REPO/.scratch/demo}
PG_BIN=${PG_BIN:-/usr/lib/postgresql/17/bin}

stop_pid() {
    local f=$1
    [ -f "$f" ] || return 0
    local pid
    pid=$(cat "$f")
    if kill -0 "$pid" 2>/dev/null; then
        kill "$pid"
        for _ in $(seq 100); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
    fi
    rm -f "$f"
}

for f in "$DEMO_DIR"/subscribe-*.pid "$DEMO_DIR/loadgen.pid" "$DEMO_DIR/ivm.pid"; do
    stop_pid "$f"
done
if [ -d "$DEMO_DIR/pg" ] && "$PG_BIN/pg_ctl" -D "$DEMO_DIR/pg" status >/dev/null 2>&1; then
    "$PG_BIN/pg_ctl" -D "$DEMO_DIR/pg" -m fast -w stop >/dev/null
fi
if [ "${1:-}" = "--clean" ]; then
    rm -rf "$DEMO_DIR"
    echo "demo stopped and its data deleted"
else
    echo "demo stopped (data kept in $DEMO_DIR; start.sh resumes it)"
fi
