#!/bin/bash
# Reconnect scenarios that need to drive `docker stop/start` from outside
# the C process. Runs a series of probes; each verifies a scenario by
# checking the client's behaviour after a broker restart.

set -e
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

BROKER=arbitro-bench
IMG=arbitro:latest

restart_broker() {
    docker restart "$BROKER" >/dev/null 2>&1 || \
        docker run -d --name "$BROKER" -p 9898:9898 "$IMG" >/dev/null
    sleep 1
}

kill_broker() {
    docker stop "$BROKER" >/dev/null 2>&1 || true
}

need_probe() {
    if [ ! -f "$1" ]; then
        echo "SKIP: missing probe $1 — run cmake --build build-wsl first"
        return 1
    fi
    return 0
}

echo "=== Phase 10.7 Reconnect (shell-driven) ==="

# 10.7.1: subscribe, kill broker, restart, verify next publish arrives
echo -n "[1] test_reconnect_replays_subscriptions ... "
restart_broker
# The connect+subscribe+kill+restart+publish sequence needs a dedicated
# C probe with reconnect enabled. Marked pending — requires probes/reconnect_probe.c.
echo "SKIP — needs probes/reconnect_probe.c (dedicated harness)"

# 10.7.2: verify metrics.reconnects bumped after N restarts
echo -n "[2] test_reconnect_metrics_bumped ... "
echo "SKIP — same dedicated probe requirement"

# 10.7.3: kill broker, verify give-up after reconnect_max
echo -n "[3] test_reconnect_max_attempts ... "
kill_broker
echo "SKIP — needs probe with reconnect_max=3 and dead broker"
restart_broker

# 10.7.4: backoff timing
echo -n "[4] test_reconnect_backoff_respected ... "
echo "SKIP — timing assertion requires probe"

# 10.7.5: in-flight sync fails on disconnect
echo -n "[5] test_in_flight_sync_fails_on_disconnect ... "
echo "SKIP — race-window probe not implemented"

# 10.7.6: reconnect recreates default_svc (Phase 9 P1 fix)
echo -n "[6] test_reconnect_recreates_default_svc ... "
echo "SKIP — needs probe that does arbitro_request → kill → restart → arbitro_request"

echo ""
echo "=== Reconnect shell harness: 0/6 executed (all pending dedicated probes) ==="
echo "Rationale: exhaustive reconnect testing needs probes/reconnect_probe.c"
echo "which drives client + broker orchestration in tight loops. Marked"
echo "as follow-up work for a dedicated PR."

exit 0
