# arbitro-c TODO

Single-file C99 client for the Arbitro message broker. Two deliverable files: `include/arbitro/arbitro.h` (public API) and `src/arbitro.c` (implementation). Internal helpers use `arb__` prefix and `static` linkage inside `src/arbitro.c`.

**Complexity tags:** `[S]` small (Sonnet) · `[M]` medium (Opus 4.6) · `[L]` large/critical (Opus 4.7)

**Invariants for every item** (from `.claude/rules/`):
- C99, zero external deps, POSIX + Winsock2 via `#ifdef _WIN32`.
- Hot path (publish, deliver dispatch, ack) is ZERO heap allocation. Cold path (management JSON) may allocate or use stack `snprintf` (bodies bounded < 2KB).
- All wire integers are **little-endian**. Frame header is 16 bytes.
- Public API: `arbitro_<noun>_<verb>()`, types `arbitro_<noun>_t`, constants `ARBITRO_<NAME>`. Errors: `int`, `ARBITRO_OK == 0`, failures `< 0`.
- No comments unless the WHY is non-obvious. Functions ≤ 40 lines.
- Wire source of truth: `arbitro-proto` crate in the broker repo (NOT `.claude/rules/wire-protocol.md`, which was found stale — see Phase 9).

---

## Status snapshot (2026-07-05)

**Working end-to-end against the Docker broker (verified by probes):**
- Connect, hello handshake, standard + envelope frame parse
- Publish (fire-and-forget, sync, with_id, batch, with_headers, **delayed** via real `PublishDelayed 0x0801` frame, **with reply** via `PublishWithReply 0x0104` frame)
- Subscribe / Deliver dispatch (RepBatch entry parse with `data_len = subj + reply + payload`)
- Consumer create / stream upsert / subject limits enforced
- Ack / batch ack / nack
- **Service RPC end-to-end** (probe: `request "hello" → reply "reply-to-hello"` OK)
- **Delayed publish** (probe: `delay_ms=800` → delivered at 803ms)
- Reconnect skeleton, TLS behind `#ifdef`, metrics, non-blocking mode

**Verified regressions passing:**
- `bench_limits`: cap enforcement 100/100 exact, 0 extras; stage 2 isolation ratio 2.09x; stage 4 1000/1000 delivered
- `cap_probe`: publish 3 to one subject with cap=1 → exactly 1 delivered, 2 blocked
- `service_probe`: full request → dispatch → reply → waiter routing round-trip
- `delayed_probe`: `publish_delayed(800ms)` delivered at 803ms

**Root-cause bugs fixed during audit** (all landed in `src/arbitro.c`):
- `ARBITRO_ACK_EXPLICIT` / `ARBITRO_ACK_NONE` values were **swapped** — was silently sending `AckPolicy::None`, which makes the broker ignore `max_subject_inflight` and never require acks.
- RepBatch entry `data_len` was misinterpreted — broker packs `data_len = subj_len + reply_len + payload_len` (all variable bytes), client was treating it as payload-only, double-counting subject+reply.
- Reply IDs from RepOk (stream_id, consumer_id) come as `u64 LE`, client was reading `u32`.
- Subscribe cold body JSON was wrong shape — client sent `{consumer_id, stream_id}`, broker expects `{consumer_id, subscription_id, filters:[byte_array]}`.
- `publish_delayed` was faking it with a `delay-ms` TLV header on a normal Publish frame — broker never parses that header, message delivered immediately. Now uses real `PublishDelayed` frame (0x0801) with `delay_ms u64` in the 16B fixed body.
- `arbitro_request` never subscribed to a reply inbox — replies had nowhere to go. Now lazily creates a per-client default service that receives correlated replies, and `arb__svc_dispatch` routes `_svc.<self>._r.<corr_id>` to the waiter table.

---

## Phase 0 — Foundation

- [x] [S] Public header skeleton — `include/arbitro/arbitro.h`
- [x] [S] Error codes + `arbitro_err_str`
- [x] [S] Wire constants (action codes, magic, flags)
- [x] [S] Public tunables (port, buf sizes)
- [x] [S] Little-endian codec helpers (`arb__put/get_u16/u32/u64`)
- [x] [M] Platform/socket abstraction (`arb_sock_t`, POSIX + Winsock2)
- [x] [M] `struct arbitro_client` — single contiguous allocation
- [x] [S] `arbitro_msg_t` public view struct
- [x] [S] `arbitro_opts_t` + `arbitro_opts_init`

## Phase 1 — Connection

- [x] [S] Hello handshake (8B `[magic][version][role][pad]`)
- [x] [S] Standard frame header writer `arb__hdr_write`
- [x] [M] Dual-header parser `arb__hdr_parse` (envelope branch for `0x0205`/`0x0207`)
- [x] [M] `arbitro_client_connect` + close + is_connected
- [x] [M] Pending-reply correlation table `arb_pending_t pending[64]`
- [x] [L] Blocking read loop `arb__read_frame` + `arbitro_client_poll`
- [x] [M] Sync management round-trip helper `arb__request_ok`

## Phase 2 — Publish

- [x] [M] Publish body encoder `arb__publish_body`
- [x] [M] Fire-and-forget `arbitro_publish` (hot path, zero alloc)
- [x] [M] Sync `arbitro_publish_sync` with RepOk wait
- [x] [S] Publish with dedup msg_id `arbitro_publish_with_id`
- [x] [L] Publish with headers (TLV) — `arbitro_publish_with_headers` (ExtendedPayload wire format)
- [x] [S] Delayed publish `arbitro_publish_delayed` — **FIXED** now uses real `PublishDelayed 0x0801` frame with `[stream_id u32][subject_len u16][msg_id_len u16][delay_ms u64][subject][payload]`
- [x] [M] Batch publish `arbitro_publish_batch` (sync via ACK_REQ + wait)
- [x] [S] `arbitro_publish_batch_sync` alias (already sync)
- [x] [S] `arbitro_publish_sync_with_id`
- [x] [S] Stream-ID resolution cache `arbitro_resolve_stream_id`

## Phase 3 — Subscribe

- [x] [M] Consumer create `arbitro_consumer_create` (cold path JSON, includes `subject_limits`)
- [x] [M] Subscribe + unsubscribe (Subscribe body: `{consumer_id, subscription_id:0, filters:[]}`)
- [x] [L] Deliver dispatch `arb__dispatch_batch_body` (RepBatch entry stride = `24 + data_len`; `payload_len = data_len - subj_len - reply_len`)
- [x] [M] Single ack/nack (echo subject_hash from delivery)
- [x] [M] Batched acks — ring accumulator + `arb__ack_flush`
- [x] [S] Message copy escape hatch `arbitro_msg_copy` / `arbitro_msg_owned_free`
- [x] [M] Callback run loop `arbitro_client_run` + `arbitro_client_fetch`

## Phase 4 — Management

- [x] [M] Cold-path JSON emitter (byte-array format for `Vec<u8>` fields)
- [x] [M] Create stream `arbitro_stream_create` (0x0401)
- [x] [S] Delete stream/consumer
- [x] [M] Info/list queries (stream_info, stream_list, consumer_info, consumer_list, stream_exists)
- [x] [S] Purge / drain / delete-message
- [x] [S] Upsert convenience (`arbitro_stream_upsert`, `arbitro_consumer_upsert`)

## Phase 5 — Service / RPC

- [x] [S] Reply-to codec `arb__replyto_encode` / `arb__replyto_decode` (magic `0xFF`)
- [x] [M] Message reply `arbitro_msg_reply` (hot path)
- [x] [L] Correlated request `arbitro_request` — **FIXED** now uses `PublishWithReply 0x0104` frame + lazy per-client default service for reply inbox
- [x] [L] Service builder `arbitro_service_create` / `_handle` / `_destroy`
- [x] [L] **Reply routing fix** — `arb__svc_dispatch` now recognizes `_svc.<name>._r.<corr_id>` subjects and routes payload to waiter table
- [x] [S] `arbitro_service_request` / `arbitro_service_send` (service_request routes through caller's service, not default)

## Phase 6 — Advanced

- [x] [S] Metrics counters + `arbitro_client_metrics`
- [x] [S] Keepalive skeleton (Ping action code — see Phase 9 bug)
- [x] [M] Non-blocking mode + `arbitro_client_fd`
- [x] [L] Reconnect with backoff + subscription replay (see Phase 9: doesn't re-establish `default_svc`)
- [x] [L] TLS behind `#ifdef ARBITRO_TLS`

## Phase 7 — Testing

- [x] [S] Minimal test harness (custom `ARB_TEST`/`ARB_ASSERT_EQ` macros)
- [x] [M] Unit tests — frame codec (standard + envelope branch)
- [x] [M] Unit tests — body codecs (publish, ack, BatchAck, RepBatch, reply-to)
- [x] [M] Unit tests — JSON emitter byte-array + ack ring buffer
- [x] [M] Integration tests — connect/pub/sub/ack round-trip
- [x] [M] Benchmarks — `bench_publish`, `bench_roundtrip`, `bench_limits`
- [x] [S] CMake/CTest + `ARBITRO_SANITIZERS=ON`

## Phase 8 — Polish

- [x] [S] Example `pubsub.c`
- [x] [S] Example `service.c` (calculator/add)
- [x] [S] Plain `Makefile`
- [x] [M] CI workflow
- [x] [S] pkg-config template
- [x] [M] Single-header amalgamation generator
- [x] [M] README.md

---

## Phase 9 — Robustness fixes

### P0 — Correctness / silent-corruption risk (5/5 DONE)

- [x] [M] Move `arb__waiters[32]` into `arbitro_client_t.waiters[]` — `src/arbitro.c:435`
- [x] [S] Fix `ARB_ACT_PING = 0x0601`, add `ARB_ACT_PONG = 0x0602`, `ARB_ACT_HELLO = 0x0001` — `src/arbitro.c:45-48`
- [x] [M] `_r.<corr>` parser rejects non-numeric via `valid` flag — `src/arbitro.c:2210`
- [x] [S] `arb__do_request` bounds-checks all `snprintf` via return-value inspection — `src/arbitro.c:2144`
- [x] [S] `arb__replyto_encode(dst, dst_cap, ...)` returns `ARBITRO_ERR_TOOLARGE` on overflow — `src/arbitro.c:1504`

### P1 — Reconnect / lifecycle (3/3 DONE)

- [x] [M] `arb__reconnect` destroys `default_svc` + fixes Subscribe body JSON (`consumer_id`, `subscription_id:0`, `filters:[]`) — `src/arbitro.c:2432`
- [x] [S] `arb__ensure_default_svc` caches failure until `now + request_timeout_ms` — `src/arbitro.c:2116`
- [x] [M] Unique default-svc name = `pid ^ time ^ /dev/urandom u64 ^ counter` — `src/arbitro.c:2103`

### P1 — Build / CI (1/2, 1 deferred)

- [x] [S] `#define _POSIX_C_SOURCE 200112L` at top of `arbitro.c`; shared lib now builds on WSL — `src/arbitro.c:1-5`
- [ ] [S] Regenerate `.claude/rules/wire-protocol.md` from `arbitro-proto` — DEFERRED to CI job

### P2 — API completeness (3/3 DONE)

- [x] [S] `arbitro_request_with_id` — `PublishWithReply` frame with `msg_id_len` + `msg_id` tail — `src/arbitro.c:2280`
- [x] [S] `arbitro_service_name(svc)`, `arbitro_service_stream_id(svc)` — `src/arbitro.c:2413`
- [x] [S] `metrics.requests_sent`, `metrics.replies_recv`, `arbitro_client_stats()` — `src/arbitro.c:1607, 1620`

### P2 — Test coverage gaps (8/8 DONE — 8/8 pass)

`tests/integration/phase9_p2.c` — all pass against Docker broker AND under ASan+UBSan.

### P3 — Nice-to-have (3/3 DONE)

- [x] [S] `ARBITRO_ERR_POOL = -13` returned from waiter-exhaustion — `arbitro.h:34`, `arbitro.c:2264`
- [x] [S] `arbitro_client_stats()` implemented — `src/arbitro.c:1620`
- [x] [M] Amalgamation regenerated to `dist/arbitro_single.h` (2850 lines, smoke-compile clean)

### P0 — Independent verification (3/5, 2 BLOCKED)

- [ ] [L] **BLOCKED** — External audit by second agent / `code-review ultra` — requires user-triggered cloud review, not runnable in-band from this session
- [x] [M] Fuzz frame parser — `tests/fuzz/fuzz_frame.c` — libFuzzer+ASan+UBSan, 37,588,237 exec/30s, 0 crash/OOB/error
- [x] [M] ASan+UBSan probes — build-asan/, 0 findings on `cap_probe`, `service_probe`, `phase9_p2` (8/8 pass)
- [x] [M] Valgrind — 0 bytes definitely lost on `service_probe` (272B possibly lost = thread stack, expected)
- [ ] [L] **BLOCKED** — Windows MSVC `/W4 /WX` + macOS clang — requires Windows CI runner / macOS machine, not accessible from this session

---

## Phase 10 — Full test coverage matrix (needed for production-grade)

Phase 7 marks "unit + integration tests" done, but reviewing what actually exists vs what a robust client requires, most feature-level tests are **absent**. The 4 hand-written probes (`bench_limits`, `cap_probe`, `service_probe`, `delayed_probe`) prove the happy path only. Below is the exhaustive matrix a robust client must pass. Each entry is one test case unless noted; group into `tests/integration/` files by area.

### 10.1 — Publish family (against broker)

- [ ] [S] `test_publish_fire_forget` — 100 msgs sent, all persist in stream (verify via stream_info) — `tests/integration/publish.c`
- [ ] [S] `test_publish_sync_returns_seq` — 10 sync publishes, `out_seq` strictly increasing — `tests/integration/publish.c`
- [ ] [M] `test_publish_with_id_dedup_within_window` — publish twice with same `msg_id` inside idempotency window → second returns same `seq`, only 1 stored — `tests/integration/publish.c`
- [ ] [M] `test_publish_with_id_no_dedup_after_window` — publish, wait > window, publish same `msg_id` → new seq assigned — `tests/integration/publish.c`
- [ ] [S] `test_publish_sync_with_id` — mirror of the two above via the `_sync_with_id` API path — `tests/integration/publish.c`
- [ ] [M] `test_publish_delayed_arrives_late` — `delay_ms=500`, ensure delivery timestamp ≥ publish + 450 (allow slop). Already covered by `delayed_probe` but promote to CI — `tests/integration/publish_delayed.c`
- [ ] [M] `test_publish_delayed_zero_ms` — `delay_ms=0` behaves like normal publish, arrives ≤ 100ms — `tests/integration/publish_delayed.c`
- [ ] [S] `test_publish_delayed_large_ms` — `delay_ms=10000`, cancel the wait after 1s and verify no delivery yet — `tests/integration/publish_delayed.c`
- [ ] [M] `test_publish_with_headers_survives_roundtrip` — publish with 3 headers, consumer receives with headers stripped from payload (broker strips per contract) but no corruption; also verify `msg-id` header dedup works — `tests/integration/publish_headers.c`
- [ ] [M] `test_publish_batch_atomic` — 100 entries, all persisted or none (kill broker mid-send test — hard, may skip) — `tests/integration/publish_batch.c`
- [ ] [S] `test_publish_batch_returns_first_seq` — batch of 10, `first_seq` matches seq of first delivered entry — `tests/integration/publish_batch.c`
- [ ] [M] `test_publish_batch_mixed_dedup` — some entries with `msg_id`, some without; correct dedup behavior — `tests/integration/publish_batch.c`
- [ ] [S] `test_publish_empty_payload` — `payload_len=0` succeeds and delivers — `tests/integration/publish.c`
- [ ] [S] `test_publish_max_subject_len` — 255-byte subject succeeds; 256 rejected client-side — `tests/integration/publish.c`
- [ ] [S] `test_publish_rejects_toolarge` — payload > `frame_buf_size` returns `ARBITRO_ERR_TOOLARGE` without touching the socket — `tests/integration/publish.c`

### 10.2 — Subscribe / Deliver / Consumer config

- [ ] [S] `test_subscribe_delivers_all_pending` — `deliver_policy=All`, publish 10 before subscribe, receive all 10 — `tests/integration/subscribe.c`
- [ ] [S] `test_subscribe_deliver_new_only` — `deliver_policy=New`, publish 10 before subscribe, receive 0; then publish 5 more, receive those 5 — `tests/integration/subscribe.c`
- [ ] [M] `test_subscribe_deliver_by_start_seq` — publish 20, subscribe with `start_seq=10`, receive 11 msgs (10..20) — `tests/integration/subscribe.c`
- [ ] [M] `test_subscribe_wildcard_single` — filter `orders.*`, publish `orders.a`, `orders.b`, `orders.a.b` → receive first 2 only — `tests/integration/subscribe.c`
- [ ] [M] `test_subscribe_wildcard_multi` — filter `orders.>`, publish `orders.a`, `orders.a.b.c` → receive both — `tests/integration/subscribe.c`
- [ ] [M] `test_max_inflight_enforced` — `max_inflight=10`, publish 20 without ack, receive exactly 10; ack one → receive one more — `tests/integration/subscribe.c`
- [ ] [M] `test_ack_wait_redelivery` — `ack_wait_ms=1000`, receive but don't ack; after 1.2s receive again with same seq — `tests/integration/subscribe.c`
- [ ] [M] `test_max_deliver_dlq` — `max_deliver=3`, receive same message 3 times without ack, on 4th the broker stops delivering (goes to DLQ) — `tests/integration/subscribe.c`
- [ ] [M] `test_two_consumers_same_stream` — two consumers, both receive every published message (independent queues) — `tests/integration/subscribe.c`
- [ ] [M] `test_group_consumers_load_balance` — 3 consumers in same `group`, publish 30, each receives ~10 (allow ±3 tolerance) — `tests/integration/subscribe.c`
- [ ] [M] `test_fanout_mode` — `deliver_mode=Fanout`, 3 consumers each receive 100% of messages — `tests/integration/subscribe.c`
- [ ] [S] `test_subscribe_wildcard_no_match` — publish to non-matching subject, consumer receives 0 — `tests/integration/subscribe.c`
- [ ] [S] `test_unsubscribe_stops_delivery` — subscribe, receive some, unsubscribe, publish more, receive nothing — `tests/integration/subscribe.c`
- [ ] [M] `test_subject_limits_multiple_patterns` — 2 patterns with different caps (basic=1, premium=100), each enforced independently — expansion of `cap_probe` — `tests/integration/subscribe.c`
- [ ] [M] `test_subject_limit_ignored_with_ack_none` — `ack_policy=None` + `subject_limits` → broker silently drops the limits (spec behavior), all 100 published delivered — `tests/integration/subscribe.c`

### 10.3 — Ack / Nack

- [ ] [S] `test_single_ack_credits_inflight` — max_inflight=1, receive → ack → receive next; loop 100 times — `tests/integration/ack.c`
- [ ] [M] `test_batch_ack_flushes_at_cap` — 256 acks accumulated → automatic flush; verify broker sees them — `tests/integration/ack.c`
- [ ] [M] `test_batch_ack_flushes_on_consumer_change` — ack consumer A × 5, then ack consumer B → A's 5 flushed first — `tests/integration/ack.c`
- [ ] [S] `test_arbitro_client_flush_acks_manual` — accumulate 3 acks, call `flush_acks`, verify sent immediately — `tests/integration/ack.c`
- [ ] [S] `test_nack_requeues_immediately` — nack → receive same msg again within 100ms — `tests/integration/ack.c`
- [ ] [M] `test_nack_with_delay` — nack with delay_ms, verify redelivery happens after delay — `tests/integration/ack.c`
- [ ] [S] `test_ack_unknown_seq_ignored` — ack with a seq that was never delivered → broker doesn't crash, no reply — `tests/integration/ack.c`
- [ ] [S] `test_ack_after_unsubscribe_ignored` — unsubscribe then ack, no error — `tests/integration/ack.c`

### 10.4 — Stream management

- [ ] [M] `test_create_stream_all_config` — every field: subject_filter, max_msgs, max_bytes, max_age_ms, replicas, journal, idempotency_window_ms → stream_info shows them back — `tests/integration/stream_mgmt.c`
- [ ] [S] `test_create_stream_duplicate` — second create with same name → RepError; upsert path returns OK — `tests/integration/stream_mgmt.c`
- [ ] [S] `test_stream_upsert_idempotent` — call 3 times, same stream_id returned — `tests/integration/stream_mgmt.c`
- [ ] [M] `test_delete_stream_removes_messages` — create, publish 10, delete → subsequent stream_info returns NOTFOUND; recreate → 0 messages — `tests/integration/stream_mgmt.c`
- [ ] [M] `test_delete_stream_keep_data` — with `keep_data=1`, recreated stream sees old messages — `tests/integration/stream_mgmt.c`
- [ ] [S] `test_stream_list_pagination` — create 20 streams, list all, verify count and names — `tests/integration/stream_mgmt.c`
- [ ] [S] `test_stream_exists` — returns 1 for existing, 0 for missing, negative on error — `tests/integration/stream_mgmt.c`
- [ ] [M] `test_purge_stream` — publish 100, purge, out_purged=100, stream_info shows 0 msgs — `tests/integration/stream_mgmt.c`
- [ ] [M] `test_drain_subject_wildcard` — publish 50 across 3 subjects, drain `orders.canceled.>`, verify only matching drained — `tests/integration/stream_mgmt.c`
- [ ] [M] `test_delete_message_tombstone` — publish 10, delete seq=5, subscribe with deliver=All → receive 9 (5 skipped) — `tests/integration/stream_mgmt.c`
- [ ] [S] `test_stream_info_reports_bytes` — publish known payloads, verify byte count reasonable — `tests/integration/stream_mgmt.c`

### 10.5 — Consumer management

- [ ] [M] `test_create_consumer_all_config` — every field returned by `consumer_info` — `tests/integration/consumer_mgmt.c`
- [ ] [S] `test_consumer_upsert_idempotent` — call 3 times, same consumer_id — `tests/integration/consumer_mgmt.c`
- [ ] [M] `test_consumer_upsert_config_change_rejected` — upsert with different `filter` → broker errors (stale config check) — `tests/integration/consumer_mgmt.c`
- [ ] [S] `test_delete_consumer` — create, subscribe, delete, subscribe again fails — `tests/integration/consumer_mgmt.c`
- [ ] [S] `test_consumer_list_per_stream` — create 5 consumers on stream, list returns exactly those 5 — `tests/integration/consumer_mgmt.c`
- [ ] [M] `test_pause_resume_consumer` — subscribe, pause, publish 10, verify 0 delivered; resume, verify 10 arrive — needs new `arbitro_consumer_pause/resume` API in header — `tests/integration/consumer_mgmt.c`
- [ ] [M] `test_consumer_stats_pending` — subscribe, publish 5, don't ack; consumer_stats shows pending=5 — `tests/integration/consumer_mgmt.c`

### 10.6 — Service / RPC

- [ ] [S] `test_request_reply_basic` — currently covered by `service_probe` — promote to CI — `tests/integration/service.c`
- [ ] [M] `test_request_timeout_no_responder` — request to service with no `handle`, waits `timeout_ms`, returns `ARBITRO_ERR_TIMEOUT`, waiter slot recovers — `tests/integration/service.c`
- [ ] [M] `test_request_timeout_slot_leak` — run 100 timeouts back-to-back, then verify a fresh request succeeds (proves slot pool doesn't leak) — `tests/integration/service.c`
- [ ] [M] `test_service_multiple_handlers` — service with `add`, `sub`, `mul` methods; 3 concurrent requests routed correctly — `tests/integration/service.c`
- [ ] [M] `test_service_two_clients` — client A hosts service, client B requests; already in probe — promote — `tests/integration/service.c`
- [ ] [M] `test_service_send_fire_forget` — `arbitro_service_send`, handler receives, no reply expected, no waiter registered — `tests/integration/service.c`
- [ ] [M] `test_service_send_no_reply_expected` — verify sender's default_svc doesn't accumulate waiters when using `_send` — `tests/integration/service.c`
- [ ] [L] `test_two_clients_concurrent_requests` — same process, 2 clients, each makes 20 requests concurrently → all responses correctly routed (**requires P0 per-client waiters fix**) — `tests/integration/service_concurrent.c`
- [ ] [M] `test_service_handler_crash_returns_timeout` — handler calls `abort()` or doesn't reply; requester gets timeout, not garbage — `tests/integration/service.c`
- [ ] [M] `test_request_disconnects_mid_flight` — start request, kill requester's socket, service worker survives without hang — `tests/integration/service.c`
- [ ] [M] `test_request_target_stream_missing` — request to a service that has no stream → `ARBITRO_ERR_NOTFOUND` — `tests/integration/service.c`

### 10.7 — Reconnect / lifecycle

- [ ] [L] `test_reconnect_replays_subscriptions` — subscribe, kill broker, wait for reconnect, verify next publish arrives to callback — `tests/integration/reconnect.c`
- [ ] [L] `test_reconnect_recreates_default_svc` — call `arbitro_request` (creates default_svc), kill broker, reconnect, next request succeeds (**requires P1 default_svc reconnect fix**) — `tests/integration/reconnect.c`
- [ ] [M] `test_reconnect_backoff_respected` — verify delay grows from 500ms → 1s → 2s (up to cap) between attempts — `tests/integration/reconnect.c`
- [ ] [M] `test_reconnect_max_attempts` — set `reconnect_max=3`, keep broker dead, verify give-up after 3 attempts with `ARBITRO_ERR_CLOSED` — `tests/integration/reconnect.c`
- [ ] [M] `test_reconnect_metrics_bumped` — after N reconnects, `metrics.reconnects == N` — `tests/integration/reconnect.c`
- [ ] [S] `test_in_flight_sync_fails_on_disconnect` — sync publish or request in progress when socket dies → immediate `ARBITRO_ERR_CLOSED`, no hang — `tests/integration/reconnect.c`

### 10.8 — Lifecycle / edge cases

- [ ] [S] `test_double_close_safe` — call `arbitro_client_close` twice, no crash — `tests/integration/lifecycle.c`
- [ ] [S] `test_close_before_connect_result` — connect with wrong host/port, close returned partial state — `tests/integration/lifecycle.c`
- [ ] [S] `test_null_client_all_apis` — every public API with NULL client returns `ARBITRO_ERR_ARG`, no crash — `tests/integration/lifecycle.c`
- [ ] [S] `test_null_output_params` — sync publish with `out_seq=NULL`, subscribe with `cb=NULL` → `ARBITRO_ERR_ARG` — `tests/integration/lifecycle.c`
- [ ] [M] `test_rapid_connect_close_loop` — 100 iterations of connect → publish → close; no fd leak (verify via `/proc/self/fd` count on Linux) — `tests/integration/lifecycle.c`
- [ ] [M] `test_client_stop_from_callback` — receive msg, call `arbitro_client_stop` in the callback, `arbitro_client_run` returns cleanly — `tests/integration/lifecycle.c`
- [ ] [S] `test_arbitro_client_fetch_pull_mode` — publish 10, `fetch(consumer_id, 5, timeout)` returns exactly 5 — `tests/integration/lifecycle.c`

### 10.9 — Adversarial / fuzz-adjacent

- [ ] [M] `test_broker_sends_unknown_action` — mock broker sends action=0xFFFF, client skips body, keeps parsing next frame — `tests/unit/adversarial.c`
- [ ] [M] `test_broker_sends_truncated_frame` — send 10-byte frame (< 16 header), client returns `ARBITRO_ERR_PROTOCOL`, doesn't crash — `tests/unit/adversarial.c`
- [ ] [M] `test_broker_sends_oversized_msg_len` — mock frame with msg_len=`frame_buf_size + 1` → `ARBITRO_ERR_TOOLARGE`, connection can recover — `tests/unit/adversarial.c`
- [ ] [M] `test_repbatch_entry_size_overflow` — synthetic RepBatch with `data_len = 0xFFFFFFFF` → parser rejects with `ARBITRO_ERR_PROTOCOL`, does not overflow `entry_size` computation — `tests/unit/adversarial.c`
- [ ] [M] `test_repbatch_count_zero` — empty RepBatch (count=0) → no callback invoked, no error — `tests/unit/adversarial.c`
- [ ] [S] `test_replyto_bad_magic` — feed reply_to bytes without `0xFF` prefix to `arbitro_msg_reply` → `ARBITRO_ERR_PROTOCOL` — `tests/unit/adversarial.c`
- [ ] [S] `test_replyto_empty` — reply_to len=0 → `ARBITRO_ERR_STATE`, no crash — `tests/unit/adversarial.c`
- [ ] [M] `test_svc_dispatch_reply_non_numeric_corr` — subject `_svc.foo._r.abc` should NOT route to any waiter (**requires P0 fix**) — `tests/unit/adversarial.c`
- [ ] [M] `test_stream_name_too_long_rejected_client_side` — > 64 char stream name to `stream_upsert` → `ARBITRO_ERR_ARG`, not truncated silently (**requires P0 bounds check**) — `tests/unit/adversarial.c`

### 10.10 — Performance regression gates

- [ ] [M] `bench_publish_regression` — CI job runs `bench_publish --msgs 100000 --payload 64` and asserts throughput ≥ 80% of baseline stored in `bench_baseline.json` — `.github/workflows/perf.yml`
- [ ] [M] `bench_roundtrip_regression` — same, asserts p99 ≤ 1.2 × baseline — `.github/workflows/perf.yml`
- [ ] [M] `bench_limits_regression` — stage 0 must show 0 extras; stage 2 ratio ≤ 3x baseline; stage 4 delivers 100% — `.github/workflows/perf.yml`
- [ ] [S] `bench_ack_batching_regression` — 100k acks issued, verify ≤ 500 syscalls to send (proves batching still works) — `.github/workflows/perf.yml`

### 10.11 — Memory / leak tests (valgrind + ASan required)

- [ ] [M] `valgrind_connect_close_no_leak` — connect → close, valgrind reports 0 bytes definitely lost — `tests/valgrind/`
- [ ] [M] `valgrind_service_lifecycle` — service_create → handle → destroy → client_close, 0 leaks — `tests/valgrind/`
- [ ] [M] `valgrind_default_svc_cleanup` — arbitro_request → client_close, 0 leaks (proves default_svc cleanup path) — `tests/valgrind/`
- [ ] [M] `valgrind_reconnect_no_leak` — 10 reconnect cycles, 0 leaks (proves subscription re-registration doesn't leak old slots) — `tests/valgrind/`
- [ ] [M] `asan_hot_path_no_oob` — bench_publish + bench_roundtrip under ASan, 0 reports — `tests/asan/`

### 10.12 — Platform matrix (CI)

- [ ] [M] Linux gcc 11/13 — `.github/workflows/ci.yml`
- [ ] [M] Linux clang 15/17 — `.github/workflows/ci.yml`
- [ ] [M] macOS clang (latest Xcode) — `.github/workflows/ci.yml`
- [ ] [L] Windows MSVC `/W4 /WX` — full test suite must pass — `.github/workflows/ci.yml`
- [ ] [M] Windows MinGW gcc — link `ws2_32`, run same suite — `.github/workflows/ci.yml`
- [ ] [S] Alpine musl (static builds) — smoke test only — `.github/workflows/ci.yml`
- [ ] [S] FreeBSD (via cirrus-ci or self-hosted) — smoke test only — `.github/workflows/ci.yml`

---

## Coverage summary (updated after dony run)

| Category | Tests needed | Written | Pass | Fail | Skip |
|---|---:|---:|---:|---:|---:|
| Publish family (10.1) | 15 | 15 | 12 | 3 | 0 |
| Subscribe / deliver (10.2) | 15 | 15 | 8 | 7 | 0 |
| Ack / Nack (10.3) | 8 | 8 | 7 | 0 | 1 |
| Stream mgmt (10.4) | 11 | 11 | 7 | 4 | 0 |
| Consumer mgmt (10.5) | 7 | 7 | 2 | 3 | 2 |
| Service / RPC (10.6) | 11 | 11 | 10 | 0 | 1 |
| Reconnect (10.7) | 6 | 0 | 0 | 0 | 6 |
| Lifecycle / edge (10.8) | 7 | 7 | 5 | 2 | 0 |
| Adversarial (10.9) | 9 | 9 | 2 | 0 | 7 |
| Performance regression (10.10) | 4 | 0 | 0 | 0 | 4 |
| Valgrind / ASan (10.11) | 5 | 3 | 3 | 0 | 2 |
| Platform matrix (10.12) | 7 | 2 | 2 | 0 | 5 |
| **Total** | **105** | **88** | **58** | **19** | **28** |

**Tests written:** `tests/integration/phase9_p2.c`, `phase10_core.c`, `phase10_publish.c`, `phase10_sub_ack.c`, `phase10_mgmt_svc.c`, `phase10_edge.c`, `tests/fuzz/fuzz_frame.c`.

**Real gaps surfaced by tests (client/broker bugs, not test bugs):**
1. `publish_sync_with_id` dedup path — 2nd call returns `ERR_BROKER` (needs Rust cross-check of RepOk-on-duplicate shape)
2. Consumer `filter` / `group` / `deliver_mode` / `deliver_policy` fields not honored by broker — likely JSON serialization bug in client cold-path
3. 5 mgmt cold-path RPCs return `ERR_BROKER` (drain, message_delete, consumer_info, consumer_delete, consumer_list) — likely wrong action code or JSON body
4. 7 public APIs SEGV on NULL client → NULL guards added this session (verify with re-run)
5. `stream_upsert` accepts 100-char names silently → length bounds check added

---

## Explicit non-goals for v0.1 (documented parity gaps vs Go/TS clients)

- **Workflow/Saga orchestration** — pure client-side layer over streams; heavy closure/state machinery is a poor fit for C99 v0.1.
- **Cron scheduling** — same rationale; both re-evaluated after the core is benchmarked and stable.
- **Typed codecs / lazy decode** — not applicable; C callers get raw byte views by design.
- **Auth (0x0002)** — frame code reserved in constants; implement when the broker auth story lands.

## Cross-cutting verification checklist (apply to every PR)

- [ ] Hot-path functions (`arbitro_publish*`, `arb__dispatch_batch`, `arbitro_msg_ack`, `arb__ack_flush`, `arbitro_msg_reply`) contain no `malloc`/`free`/`snprintf`/`strlen`.
- [ ] Every wire write goes through `arb__put_*` (no struct casts, no bitfields on wire data).
- [ ] `subject_hash` is only ever ECHOED from a delivery, never computed.
- [ ] Read-loop msg_len offset branch (4 vs 8) covered by a unit test.
- [ ] Builds clean with `-Wall -Wextra -Wpedantic -Werror` (GCC/Clang) and `/W4 /WX` (MSVC).
- [ ] **NEW**: If the change touches wire encoding, cross-reference `arbitro-proto` NOT `.claude/rules/wire-protocol.md` (the doc is stale — Phase 9 P1 to regenerate).
- [ ] **NEW**: If the change adds a new public API, add at least one integration test that exercises it against the Docker broker.
