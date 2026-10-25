#!/usr/bin/env bash
# Start the ivm demo:
# - an upstream PostgreSQL with a small shop schema (databases shop and sinkdb)
# - an ivm engine with the web UI, replicating shop and maintaining views
# - a load generator streaming changes upstream
# - SUBSCRIBE clients following two views
# Everything lives in $DEMO_DIR (default: .scratch/demo in the repository).
# Re-running continues an existing demo; stop it with stop.sh.
set -euo pipefail

REPO=$(cd "$(dirname "$0")/../.." && pwd)
HERE=$REPO/examples/demo
DEMO_DIR=${DEMO_DIR:-$REPO/.scratch/demo}
PG_BIN=${PG_BIN:-/usr/lib/postgresql/17/bin}
PG_PORT=${PG_PORT:-54600}
IVM=${IVM:-$REPO/target/release/ivm}
IVM_LISTEN=${IVM_LISTEN:-127.0.0.1:6875}
HTTP_LISTEN=${HTTP_LISTEN:-127.0.0.1:6876}
RATE=${RATE:-20}
LOADGEN_ARGS=${LOADGEN_ARGS:-}   # extra loadgen.py options, e.g. "--glitch-every 60"

SOCK=$DEMO_DIR/sock
IVM_HOST=${IVM_LISTEN%:*}
IVM_PORT=${IVM_LISTEN##*:}
mkdir -p "$DEMO_DIR" "$SOCK"

running() { [ -f "$1" ] && kill -0 "$(cat "$1")" 2>/dev/null; }

if [ ! -x "$IVM" ]; then
    echo "building ivm (release)..."
    (cd "$REPO" && cargo build --release -p ivm --bin ivm)
fi

# upstream PostgreSQL
if [ ! -d "$DEMO_DIR/pg" ]; then
    "$PG_BIN/initdb" -D "$DEMO_DIR/pg" -U postgres --auth=trust -E UTF8 --locale=C >"$DEMO_DIR/initdb.log"
    cat >>"$DEMO_DIR/pg/postgresql.conf" <<CONF
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
listen_addresses = ''
unix_socket_directories = '$SOCK'
port = $PG_PORT
timezone = 'UTC'
CONF
fi
if ! "$PG_BIN/pg_ctl" -D "$DEMO_DIR/pg" status >/dev/null 2>&1; then
    "$PG_BIN/pg_ctl" -D "$DEMO_DIR/pg" -l "$DEMO_DIR/postgres.log" -w start >/dev/null
fi
PSQL_UP=(psql -h "$SOCK" -p "$PG_PORT" -U postgres -X -q -v ON_ERROR_STOP=1)
if ! "${PSQL_UP[@]}" -d postgres -Atc "SELECT 1 FROM pg_database WHERE datname = 'shop'" | grep -q 1; then
    "${PSQL_UP[@]}" -d postgres -c "CREATE DATABASE shop" -c "CREATE DATABASE sinkdb"
    "${PSQL_UP[@]}" -d shop -f "$HERE/upstream.sql"
fi
UPSTREAM="host=$SOCK port=$PG_PORT user=postgres dbname=shop"
SINKCONN="host=$SOCK port=$PG_PORT user=postgres dbname=sinkdb"

# ivm with the web UI
if ! running "$DEMO_DIR/ivm.pid"; then
    HTTP_ARGS=()
    # (ivm --help exits non-zero, so don't pipe it under pipefail)
    IVM_HELP=$("$IVM" --help 2>&1 || true)
    if grep -q -- --http-listen <<<"$IVM_HELP"; then
        HTTP_ARGS=(--http-listen "$HTTP_LISTEN")
    else
        echo "note: this ivm build has no web UI (--http-listen); starting without it"
        HTTP_LISTEN=
    fi
    nohup "$IVM" --data-dir "$DEMO_DIR/ivm-data" --listen "$IVM_LISTEN" "${HTTP_ARGS[@]}" \
        >>"$DEMO_DIR/ivm.log" 2>&1 &
    echo $! >"$DEMO_DIR/ivm.pid"
fi
PSQL_IVM=(psql -h "$IVM_HOST" -p "$IVM_PORT" -U ivm -X -q)
for _ in $(seq 100); do
    "${PSQL_IVM[@]}" -Atc "SELECT 1" >/dev/null 2>&1 && break
    sleep 0.2
done
if ! "${PSQL_IVM[@]}" -Atc "SHOW SOURCES" | grep -q '^shop|'; then
    "${PSQL_IVM[@]}" -v ON_ERROR_STOP=1 -v upstream="$UPSTREAM" -v sinkconn="$SINKCONN" -f "$HERE/views.sql"
fi

# streaming upstream changes
if ! running "$DEMO_DIR/loadgen.pid"; then
    # shellcheck disable=SC2086  # LOADGEN_ARGS is a list of options
    nohup python3 "$HERE/loadgen.py" --conninfo "$UPSTREAM" --rate "$RATE" $LOADGEN_ARGS \
        >>"$DEMO_DIR/loadgen.log" 2>&1 &
    echo $! >"$DEMO_DIR/loadgen.pid"
fi

# SUBSCRIBE clients
for v in top_products order_status_counts; do
    if ! running "$DEMO_DIR/subscribe-$v.pid"; then
        nohup "${PSQL_IVM[@]}" -c "COPY (SUBSCRIBE TO $v WITH (PROGRESS)) TO STDOUT" \
            >>"$DEMO_DIR/subscribe-$v.log" 2>&1 &
        echo $! >"$DEMO_DIR/subscribe-$v.pid"
    fi
done

echo
echo "ivm demo running (data in $DEMO_DIR)"
[ -n "$HTTP_LISTEN" ] && echo "  web UI:    http://$HTTP_LISTEN/"
echo "  SQL:       psql -h $IVM_HOST -p $IVM_PORT -U ivm"
echo "  upstream:  psql -h $SOCK -p $PG_PORT -U postgres shop"
echo "  logs:      $DEMO_DIR/{ivm,loadgen,subscribe-*}.log"
echo "  stop:      $HERE/stop.sh   (add --clean to delete all demo data)"
