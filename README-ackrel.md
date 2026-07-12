# Ack-reliability hot tier

The C client tracks deferred/unacknowledged sequences in memory
(`arb_ackrel_t`, one instance per `arbitro_client_t`) so that a batch-ack
flush failure or a disconnect does not silently lose ack state. On
reconnect, the client replays an `AckStateReq` (0x0A01) per active
consumer with a bumped generation, and a background sweep (every ~100ms)
resends outstanding sequences via `AckBatch` (0x0A03) until the broker
confirms them with `AckBatchResp` (0x0A04) or reports the durable cursor
via `AckStateRep` (0x0A02).

This hot tier survives **disconnect + reconnect** within the lifetime of
a single process. It does **not** survive a process restart.

## Cold tier — intentionally out of scope

A cold tier (persisting the ack-reliability state to disk so it survives
a client process restart) is intentionally **not implemented** in this
client. Rationale:

- The C client is meant to be a thin, embeddable, zero-dependency layer.
  Adding a persistence backend (file format, fsync policy, corruption
  recovery) would materially grow the surface area and dependency set of
  a single-file library.
- Applications that need ack durability across a full process restart
  are better served by designing **idempotent message handlers** — the
  broker will redeliver anything the client didn't explicitly ack before
  it went away, and an idempotent handler makes redelivery safe by
  construction.
- Applications with stricter requirements can drive their own external
  persistence (e.g. record `consumer_id`/`seq` pairs to a local store as
  part of the handler, and skip re-processing on restart) without any
  help from this library.

If a future version of this client adds a cold tier, it should be
opt-in and behind a compile-time flag, mirroring the Rust client's
`ack-persistence` feature.
