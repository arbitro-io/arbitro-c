# Performance Rules

## Hot Path (publish, deliver dispatch, ack)

1. ZERO heap allocations. Stack buffers with compile-time bounds only.
2. No `snprintf`, no `strlen` on the hot path — lengths are always known.
3. No function pointers in inner loops (inline or direct call).
4. Frame header: write directly into send buffer at known offsets. No intermediate struct copy.
5. Batch acks: accumulate in a ring buffer, flush every N acks or on timer.
6. Use `writev`/`WSASend` to send header+body without memcpy concatenation.
7. Deliver callback receives a zero-copy view into the read buffer — no alloc, no copy.
8. Subject matching: caller registers by exact subject or prefix — O(1) dispatch via hash or direct consumer_id lookup.

## Cold Path (create_stream, create_consumer, subscribe)

1. May allocate. Must free before returning or document caller-free contract.
2. JSON emission: `snprintf` into a stack buffer (cold-path bodies are bounded <2KB).
3. Blocking wait for RepOk is acceptable (one outstanding management request at a time).

## Syscalls

1. One `send()` per publish (header+body in single buffer) or `writev` for batch.
2. Read loop: single `recv()` into a 64KB ring buffer. Parse frames in-place.
3. No `select()`/`poll()` in the default blocking mode. Optional non-blocking via `arbitro_client_set_nonblock()`.
4. TCP_NODELAY always on. Nagle kills latency.

## Memory Layout

1. `arbitro_client_t` is one contiguous allocation: connection state + read buffer + ack batch buffer.
2. No linked lists. Arrays with known max capacity.
3. Frame buffer: 64KB default, configurable at connect time.

## Measurements

Before any performance claim, measure with:
```bash
./build/benchmarks/bench_publish    # msgs/sec, p99 latency
./build/benchmarks/bench_roundtrip  # publish→deliver→ack cycle
```
