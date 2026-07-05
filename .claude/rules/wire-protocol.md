# Wire Protocol Reference

Source of truth: `arbitro-proto` crate in the broker repo.

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

## Action Codes (hot path)

| Code   | Name          | Direction      |
|--------|---------------|----------------|
| 0x0101 | Publish       | client→server  |
| 0x0103 | PublishBatch  | client→server  |
| 0x0200 | Deliver       | server→client  |
| 0x0201 | Ack           | client→server  |
| 0x0202 | Nack          | client→server  |
| 0x0203 | RepOk         | server→client  |
| 0x0204 | RepError      | server→client  |
| 0x0205 | RepBatch      | server→client  |
| 0x0206 | BatchAck      | client→server  |
| 0x0207 | FanoutBatch   | server→client  |
| 0x0001 | Ping          | server→client  |
| 0x0002 | Auth          | client→server  |

## Action Codes (cold path — JSON body)

| Code   | Name            |
|--------|-----------------|
| 0x0301 | CreateStream    |
| 0x0302 | DeleteStream    |
| 0x0303 | CreateConsumer  |
| 0x0304 | DeleteConsumer  |
| 0x0305 | Subscribe       |
| 0x0306 | Unsubscribe     |
| 0x0310 | ListStreams     |
| 0x0311 | StreamInfo      |
| 0x0320 | ListConsumers   |
| 0x0321 | ConsumerInfo    |
| 0x0400 | PurgeStream     |
| 0x0401 | DrainSubject    |
| 0x0402 | DeleteMessage   |

## Publish Body (after standard 16B header)

```
[0:4]  stream_id    u32
[4:6]  subject_len  u16
[6:8]  msg_id_len   u16
[8:..]  subject[subject_len] + msg_id[msg_id_len] + payload[remainder]
```

## RepBatch Entry (inside RepBatch body)

Body starts with `[count:u16][pad:u16]`, then N entries:
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
