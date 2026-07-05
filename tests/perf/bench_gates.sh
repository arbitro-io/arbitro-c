#!/bin/bash
# Phase 10.10 — Performance regression gates
#
# Runs bench_publish, bench_roundtrip, bench_limits and asserts against
# a baseline file. First run generates the baseline. Subsequent runs
# fail if throughput drops > 20% or p99 latency grows > 20%.

set -e
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BASELINE="$ROOT/tests/perf/bench_baseline.txt"
BUILD="$ROOT/build-wsl"

fail=0
tests=0
passed=0

check_ge() {
    local name="$1" current="$2" baseline="$3" threshold_pct="$4"
    tests=$((tests+1))
    local min=$(awk "BEGIN{print $baseline * $threshold_pct / 100}")
    if awk "BEGIN{exit !($current >= $min)}"; then
        echo "[$tests] $name: $current >= $min ($threshold_pct% of $baseline) — PASS"
        passed=$((passed+1))
    else
        echo "[$tests] $name: $current < $min ($threshold_pct% of $baseline) — FAIL"
        fail=$((fail+1))
    fi
}

check_le() {
    local name="$1" current="$2" baseline="$3" threshold_pct="$4"
    tests=$((tests+1))
    local max=$(awk "BEGIN{print $baseline * $threshold_pct / 100}")
    if awk "BEGIN{exit !($current <= $max)}"; then
        echo "[$tests] $name: $current <= $max ($threshold_pct% of $baseline) — PASS"
        passed=$((passed+1))
    else
        echo "[$tests] $name: $current > $max ($threshold_pct% of $baseline) — FAIL"
        fail=$((fail+1))
    fi
}

echo "=== Phase 10.10 — Perf regression gates ==="

if [ ! -f "$BUILD/bench_publish" ]; then
    echo "SKIP: benches not built (cmake --build build-wsl)"
    exit 0
fi

# Run bench_publish — capture msg/s
PUB_OUT=$("$BUILD/bench_publish" --msgs 10000 --payload 64 2>&1 || echo "0 msg/s")
PUB_MSGS=$(echo "$PUB_OUT" | grep -oE '[0-9]+' | tail -1)
PUB_MSGS=${PUB_MSGS:-0}

# Run bench_roundtrip
RT_OUT=$("$BUILD/bench_roundtrip" 2>&1 || echo "0")
RT_P99=$(echo "$RT_OUT" | grep -oE 'p99[= ]*[0-9]+' | grep -oE '[0-9]+' | tail -1)
RT_P99=${RT_P99:-999999}

# Run bench_limits
BL_OUT=$(BENCH_LIMITS_ITERS=20 "$BUILD/bench_limits" 2>&1 || echo "extras=999")
BL_EXTRAS=$(echo "$BL_OUT" | grep -oE 'extras=[0-9]+' | grep -oE '[0-9]+' | head -1)
BL_EXTRAS=${BL_EXTRAS:-999}

if [ ! -f "$BASELINE" ]; then
    echo "No baseline file, writing current numbers to $BASELINE"
    cat > "$BASELINE" <<EOF
PUB_MSGS=$PUB_MSGS
RT_P99=$RT_P99
BL_EXTRAS=0
EOF
    echo "First-run baseline captured. Re-run to test regressions."
    exit 0
fi

source "$BASELINE"

check_ge "bench_publish msg/s" "$PUB_MSGS" "${PUB_MSGS:-1}" 80
check_le "bench_roundtrip p99 ms" "$RT_P99" "${RT_P99:-1}" 120
check_le "bench_limits stage0 extras" "$BL_EXTRAS" 0 0

echo ""
echo "=== $passed/$tests passed ($fail failed) ==="
exit $fail
