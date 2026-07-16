# Wire Protocol Reference

Source of truth: `arbitro-proto` crate in the broker repo, specifically
`arbitro-proto/src/action.rs` for action codes and
`arbitro-client-tokio/src/consume/demux.rs` for the Deliver/RepBatch wire
shapes. Regenerate this file whenever those change.

## Connection

1. TCP connect to broker (default 127.0.0.1:9898)
2. Send 8-byte Hello: `[magic:u32 LE = 0x32425241][version:u8 = 2][role:u8 = 0][pad:u16 = 0]`
3. No HelloAck — immediately start framing

## Frame Header (16 bytes, little-endian)

Standard header (most frames):
```
[0:2]  action    u16   — frame type
[2:3]  flags     u8    — bit0 = ACK_REQ
[3:4]  entry_flags u8  — bit4 = HAS_HEADERS
[4:8]  msg_len   u32   — body length after header
[8:16] seq       u64   — correlation ID (client-chosen, echoed in replies)
```

Envelope header (RepBatch 0x0205, FanoutBatch 0x0207 only):
```
[0:2]  action    u16
[2:3]  flags     u8
[3:4]  rsv       u8
[4:8]  stream_id u32
[8:12] msg_len   u32   — body length (NOTE: offset 8, not 4!)
[12:16] env_seq  u32
```

CRITICAL: The read loop MUST branch on action to determine msg_len offset.

## Action Codes

Matches `arbitro-proto/src/action.rs::Action` exactly. Direction is
relative to the client.

| Code   | Name             | Direction      |
|--------|------------------|----------------|
| 0x0001 | Hello            | client→server (pre-Header, 8B HelloFrame) |
| 0x0002 | Auth             | client→server  |
| 0x0101 | Publish          | client→server  |
| 0x0103 | PublishBatch     | client→server  |
| 0x0104 | PublishWithReply | client→server  |
| 0x0200 | Deliver          | server→client  |
| 0x0201 | Ack              | client→server  |
| 0x0202 | Nack             | client→server  |
| 0x0203 | RepOk            | server→client  |
| 0x0204 | RepError         | server→client  |
| 0x0205 | RepBatch         | server→client  |
| 0x0206 | BatchAck         | client→server  |
| 0x0207 | FanoutBatch      | server→client  |
| 0x020A | BatchNack        | client→server  |
| 0x020B | AckTerm          | client→server  |
| 0x0301 | Subscribe        | client→server  |
| 0x0302 | Unsubscribe      | client→server  |
| 0x0401 | CreateStream     | client→server  |
| 0x0402 | DeleteStream     | client→server  |
| 0x0403 | GetStream (StreamInfo) | client→server |
| 0x0404 | ListStreams      | client→server  |
| 0x0405 | PurgeStream      | client→server  |
| 0x0406 | DrainSubject     | client→server  |
| 0x0407 | DeleteMessage    | client→server  |
| 0x0501 | CreateConsumer   | client→server  |
| 0x0502 | DeleteConsumer   | client→server  |
| 0x0503 | GetConsumer (ConsumerInfo) | client→server |
| 0x0504 | ListConsumers    | client→server  |
| 0x0505 | ConsumerStats    | client→server  |
| 0x0506 | PauseConsumer    | client→server  |
| 0x0507 | ResumeConsumer   | client→server  |
| 0x0601 | Ping             | server→client  |
| 0x0602 | Pong             | client→server  |
| 0x0605 | Disconnect       | server→client  |
| 0x0701 | CreateCron       | client→server  |
| 0x0702 | DeleteCron       | client→server  |
| 0x0703 | ListCrons        | client→server  |
| 0x0704 | CronFire         | server→client  |
| 0x0705 | CronAck          | client→server  |
| 0x0801 | PublishDelayed   | client→server  |
| 0x0A01 | AckStateReq      | client→server  |
| 0x0A02 | AckStateRep      | server→client  |
| 0x0A03 | AckBatch         | client→server  |
| 0x0A04 | AckBatchResp     | server→client  |

Reserved / deleted slots — do not reuse: `0x0102`, `0x0105`, `0x0106`,
`0x0208`, `0x0209`, `0x0603`, `0x0604`, `0x0901..=0x0908`.

The C client (`arbitro.c`) currently implements the hot-path subset
(`ARB_ACT_*` macros at `arbitro.c:51-80`) plus the cold-path management
codes it needs (Subscribe, CreateStream/DeleteStream/StreamInfo/
ListStreams/PurgeStream/DrainSubject/DeleteMessage,
CreateConsumer/DeleteConsumer/ConsumerInfo/ListConsumers/PauseConsumer/
ResumeConsumer). Cron (0x0701 CreateCron, 0x0702 DeleteCron, 0x0704
CronFire, 0x0705 CronAck) is now implemented — `arbitro_cron_create` /
`arbitro_cron_delete` with a fixed-size in-client registry and CronFire→
CronAck dispatch in the read loop (see the Cron Frames section below).
Workflow actions remain intentionally out of scope for an embeddable C
client. AckState (0x0A01-0x0A04) is fully supported since Wave4a — see the
ackrel section below.

## Cron Frames

CreateCron (0x0701) body is cold-path JSON matching `wire/cron.rs`
`CreateCronBody`: `{"name":String,"every":String,"tz":String?,"timeout_ms":u32,"overlap":bool}`.
`name`/`every`/`tz` are JSON strings (NOT byte arrays); `tz` is omitted
when empty. The C client always emits `timeout_ms:0` and `overlap:false`.
Reply is binary RepOk/RepError.

DeleteCron (0x0702) body is the raw cron name bytes (no JSON).

CronFire (0x0704, server→client) fixed LE body:
```
[0:2]  name_len  u16
[2:10] fire_time_ms u64
[10:18] fire_count u64
[18:..] name[name_len]
```

CronAck (0x0705, client→server) fixed LE body:
```
[0:2] name_len u16
[2:3] status   u8   (0=ok, 1=error)
[3:..] name[name_len]
```

## Publish Body (after standard 16B header)

```
[0:4]  stream_id    u32
[4:6]  subject_len  u16
[6:8]  msg_id_len   u16
[8:..]  subject[subject_len] + msg_id[msg_id_len] + payload[remainder]
```

## Deliver Body (single delivery, action 0x0200)

Uses the **standard 16B header** (not Envelope) — `seq` at header offset
`[8:16]` is the delivery sequence. Body immediately after the header is
a fixed 12-byte `DeliverBody`, then the subject, then the payload:

```
[0:4]   consumer_id   u32
[4:8]   subject_hash  u32
[8:10]  subject_len   u16
[10:12] pad           u16
[12:..]  subject[subject_len] + payload[remainder]
```

Single Deliver does not carry a `reply_to` — that field is empty on the
client-side message view.

## RepBatch / FanoutBatch Entry (inside RepBatch/FanoutBatch body)

Uses the **Envelope 16B header** — body starts with `[count:u16][pad:u16]`,
then N entries:
```
[0:4]   consumer_id   u32
[4:12]  seq           u64
[12:14] subject_len   u16
[14:16] reply_len     u16
[16:20] data_len      u32
[20:24] subject_hash  u32
[24:..]  subject[subject_len] + reply_to[reply_len] + payload[data_len]
```

## Ack Body

```
[0:4]  consumer_id   u32
[4:8]  subject_hash  u32  — echo from delivery, not computed by client
[8:16] ack_seq       u64
```

## BatchAck Body

```
[0:4]  consumer_id  u32
[4:8]  count        u32
[8:..]  N × [seq:u64][subject_hash:u32][pad:u32]
```

## Cold Path JSON

Field values that are `Vec<u8>` in Rust serialize as **arrays of numbers**, not strings:
```json
{"name":[111,114,100,101,114,115],"filter":[111,114,100,101,114,115,46,62]}
```

The C client must emit this format. No JSON parsing is needed (replies are binary RepOk/RepError).

## Reply-to Encoding (for Service/RPC)

```
[0]    magic    0xFF
[1:5]  stream_id  u32 LE
[5:..]  reply_subject bytes
```
