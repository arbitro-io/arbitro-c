# Testing Rules

## Structure

- `tests/unit/` — no broker needed, test frame encoding/decoding, buffer logic
- `tests/integration/` — requires broker on ARBITRO_ADDR (default 127.0.0.1:9898)
- `benchmarks/` — throughput and latency measurements

## Framework

- Use a minimal single-header test framework (e.g. `greatest.h` or custom assert macros).
- No heavy test dependencies. The test binary must compile in <2 seconds.

## Unit Tests (always run)

```bash
cmake --build build --target unit_tests && ./build/tests/unit_tests
```

Must cover:
- Frame header encode/decode (both standard and envelope formats)
- Publish frame encoding
- Ack/BatchAck encoding
- RepBatch entry parsing
- JSON cold-path emission (byte-array format)
- Ring buffer operations
- Subject hash echo correctness

## Integration Tests (broker required)

```bash
ARBITRO_ADDR=127.0.0.1:9898 ./build/tests/integration
```

Must cover:
- Connect + handshake
- Create stream → publish → subscribe → deliver → ack round-trip
- Batch publish + batch ack
- Request/reply (service pattern)
- Timeout on request with no responder
- Reconnect after disconnect
- Multiple consumers on same stream

## Benchmarks

```bash
./build/benchmarks/bench_publish --msgs 100000 --payload 64
./build/benchmarks/bench_roundtrip --msgs 10000
```

Report: msgs/sec, bytes/sec, p50/p99 latency.

## CI

- Unit tests: every commit (no external deps)
- Integration tests: on PR + main (Docker broker)
- Sanitizers: ASan + UBSan on every CI run
- Valgrind: weekly or on request
