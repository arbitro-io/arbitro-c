# arbitro-c

Official C client for the [Arbitro](https://github.com/arbitro-io/arbitro) message broker.

Single-file, zero-dependency, embeddable in any C/C++ project. Targets C99 with POSIX and Windows (Winsock2) support.

## Features

- Zero heap allocations on the hot path (publish, deliver, ack)
- Full feature parity with Rust, TypeScript, and Go clients
- Single-header option via amalgamation script
- Cross-platform: Linux, macOS, Windows, FreeBSD

## Requirements

- C99 compiler (GCC, Clang, MSVC)
- Arbitro broker reachable on `127.0.0.1:9898`

## Install

### CMake

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
sudo cmake --install build
```

### Makefile (no CMake needed)

```bash
make
sudo make install
```

### Single-file embed

```bash
cp include/arbitro/arbitro.h your_project/
cp src/arbitro.c your_project/
# Add to your build: cc -std=c99 -D_POSIX_C_SOURCE=200112L -c arbitro.c
```

### Single-header amalgamation

```bash
python scripts/amalgamate.py > arbitro_single.h
```

Then in exactly one `.c` file:

```c
#define ARBITRO_IMPLEMENTATION
#include "arbitro_single.h"
```

## Run the Broker

```bash
docker run --rm -p 9898:9898 ghcr.io/arbitro-io/arbitro-server:latest
```

## Quick Start

```c
#include <arbitro/arbitro.h>
#include <stdio.h>

void on_msg(arbitro_msg_t *msg, void *ud) {
    printf("%.*s: %.*s\n",
        (int)msg->subject_len, msg->subject,
        (int)msg->data_len, msg->data);
    arbitro_msg_ack(msg);
    arbitro_client_stop(msg->client);
}

int main(void) {
    arbitro_client_t *c;
    uint32_t stream_id, consumer_id;
    arbitro_stream_cfg_t scfg = {0};
    arbitro_consumer_cfg_t ccfg = {0};

    arbitro_client_connect("127.0.0.1", ARBITRO_DEFAULT_PORT, NULL, &c);

    scfg.subject_filter = "orders.>";
    arbitro_stream_upsert(c, "orders", &scfg, &stream_id);

    ccfg.name = "workers";
    ccfg.filter = "orders.>";
    ccfg.ack_policy = ARBITRO_ACK_EXPLICIT;
    ccfg.max_inflight = 100;
    arbitro_consumer_upsert(c, "orders", &ccfg, &consumer_id);

    arbitro_subscribe(c, stream_id, consumer_id, on_msg, NULL);
    arbitro_publish(c, stream_id,
                    (const uint8_t *)"orders.new", 10,
                    (const uint8_t *)"hello", 5);
    arbitro_client_run(c);
    arbitro_client_close(c);
    return 0;
}
```

## Publish

```c
// Fire-and-forget (zero alloc, hot path)
arbitro_publish(c, stream_id, subject, subject_len, payload, payload_len);

// Synchronous — waits for broker ack, returns assigned sequence
uint64_t seq;
arbitro_publish_sync(c, stream_id, subject, subject_len, payload, payload_len, &seq);

// With dedup message ID
arbitro_publish_with_id(c, stream_id, subject, subject_len,
                        msg_id, msg_id_len, payload, payload_len);

// Batch publish
arbitro_batch_entry_t entries[] = {
    { .subject = (const uint8_t *)"a.x", .subject_len = 3,
      .payload = buf_a, .payload_len = len_a },
    { .subject = (const uint8_t *)"a.y", .subject_len = 3,
      .payload = buf_b, .payload_len = len_b },
};
uint64_t first_seq;
arbitro_publish_batch(c, stream_id, entries, 2, &first_seq);

// With TLV headers
arbitro_header_t hdrs[] = {
    { .key = (const uint8_t *)"trace-id", .key_len = 8,
      .val = (const uint8_t *)"abc123", .val_len = 6 },
};
arbitro_publish_with_headers(c, stream_id, subject, subject_len,
                             hdrs, 1, payload, payload_len);

// Delayed publish (delivers after N ms)
arbitro_publish_delayed(c, stream_id, subject, subject_len,
                        payload, payload_len, 5000);
```

## Subscribe

```c
// Register callback (zero-copy: msg points into read buffer)
void on_msg(arbitro_msg_t *msg, void *userdata) {
    process(msg->data, msg->data_len);
    arbitro_msg_ack(msg);
}
arbitro_subscribe(c, stream_id, consumer_id, on_msg, NULL);

// Run the event loop (blocks until arbitro_client_stop)
arbitro_client_run(c);

// Or poll manually
arbitro_client_poll(c, 1000); // timeout_ms

// Nack for redelivery
arbitro_msg_nack(msg);

// Copy message for out-of-callback use
arbitro_msg_owned_t owned;
arbitro_msg_copy(msg, &owned);
// ... use later ...
arbitro_msg_owned_free(&owned);
```

## Service (Request/Reply RPC)

```c
// Server side: register handlers
arbitro_service_t *svc;
arbitro_service_create(c, "calc", 10, &svc);
arbitro_service_handle(svc, "add", on_add, NULL);
arbitro_client_run(c);
arbitro_service_destroy(svc);

// Handler replies via arbitro_msg_reply
void on_add(arbitro_msg_t *msg, void *ud) {
    int32_t result = compute(msg->data, msg->data_len);
    arbitro_msg_reply(msg, (const uint8_t *)&result, sizeof(result));
}

// Client side: send request with timeout
uint8_t resp[256];
uint32_t resp_len;
int rc = arbitro_request(c, "calc", "add",
                         payload, payload_len,
                         5000,
                         resp, sizeof(resp), &resp_len);

// Service-scoped request (auto-resolve stream)
arbitro_service_request(svc, "other-service", "method",
                        payload, len, 5000,
                        resp, sizeof(resp), &resp_len);

// Fire-and-forget to another service
arbitro_service_send(svc, "other-service", "notify", payload, len);
```

## Stream Management

```c
arbitro_stream_cfg_t cfg = {
    .subject_filter = "orders.>",
    .max_msgs = 1000000,
    .max_bytes = 1ULL << 30,
    .max_age_ms = 86400000,
    .replicas = 1,
    .journal = 1,
    .idempotency_window_ms = 5000,
};

uint32_t stream_id;
arbitro_stream_create(c, "orders", &cfg, &stream_id);
arbitro_stream_upsert(c, "orders", &cfg, &stream_id);  // create-or-update
arbitro_stream_delete(c, "orders", 0);                   // 0=drop data, 1=keep

uint64_t purged;
arbitro_stream_purge(c, "orders", &purged);

uint64_t drained;
arbitro_subject_drain(c, "orders", "orders.cancelled.>", &drained);

arbitro_message_delete(c, "orders", 42);

int exists = arbitro_stream_exists(c, "orders"); // 1 or 0

arbitro_stream_info_t info;
arbitro_stream_info(c, "orders", &info);

arbitro_stream_info_t list[32];
size_t n;
arbitro_stream_list(c, list, 32, &n);
```

## Consumer Management

```c
arbitro_consumer_cfg_t cfg = {
    .name = "workers",
    .filter = "orders.>",
    .ack_policy = ARBITRO_ACK_EXPLICIT,
    .max_inflight = 1000,
    .ack_wait_ms = 30000,
    .max_deliver = 5,
};

uint32_t consumer_id;
arbitro_consumer_create(c, "orders", &cfg, &consumer_id);
arbitro_consumer_upsert(c, "orders", &cfg, &consumer_id);
arbitro_consumer_delete(c, "orders", "workers");

arbitro_consumer_info_t info;
arbitro_consumer_info(c, "orders", "workers", &info);

arbitro_consumer_info_t list[32];
size_t n;
arbitro_consumer_list(c, "orders", list, 32, &n);
```

## Connection Options

```c
arbitro_opts_t opts;
arbitro_opts_init(&opts);
opts.connect_timeout_ms = 5000;
opts.request_timeout_ms = 5000;
opts.frame_buf_size = 64 * 1024;
opts.reconnect = 1;
opts.reconnect_max = 10;
opts.reconnect_delay_ms = 500;

arbitro_client_connect("127.0.0.1", ARBITRO_DEFAULT_PORT, &opts, &c);
```

## Utilities

```c
// Resolve stream name to ID (cached)
uint32_t id;
arbitro_resolve_stream_id(c, "orders", &id);

// Flush batched acks immediately
arbitro_client_flush_acks(c);

// Ping broker (keepalive)
arbitro_client_ping(c);

// Non-blocking mode (for custom event loops)
arbitro_client_set_nonblock(c, 1);
int fd = arbitro_client_fd(c);

// Metrics snapshot
arbitro_metrics_t m;
arbitro_client_metrics(c, &m);
printf("published: %llu, delivered: %llu\n",
       (unsigned long long)m.publishes_sent,
       (unsigned long long)m.deliveries_recv);
```

## Error Handling

All functions return `int` — `ARBITRO_OK` (0) on success, negative error codes on failure.

```c
int rc = arbitro_publish(c, stream_id, subj, subj_len, data, data_len);
if (rc != ARBITRO_OK) {
    fprintf(stderr, "publish failed: %s\n", arbitro_err_str(rc));
}
```

| Code | Constant | Meaning |
|------|----------|---------|
| 0 | `ARBITRO_OK` | Success |
| -1 | `ARBITRO_ERR_SOCKET` | Socket creation failed |
| -2 | `ARBITRO_ERR_CONNECT` | Connection failed |
| -3 | `ARBITRO_ERR_HANDSHAKE` | Handshake rejected |
| -4 | `ARBITRO_ERR_CLOSED` | Connection closed |
| -5 | `ARBITRO_ERR_TIMEOUT` | Operation timed out |
| -6 | `ARBITRO_ERR_PROTOCOL` | Protocol violation |
| -7 | `ARBITRO_ERR_TOOLARGE` | Frame exceeds buffer |
| -8 | `ARBITRO_ERR_NOMEM` | Allocation failed |
| -9 | `ARBITRO_ERR_BROKER` | Broker returned error |
| -10 | `ARBITRO_ERR_ARG` | Invalid argument |
| -11 | `ARBITRO_ERR_NOTFOUND` | Resource not found |
| -12 | `ARBITRO_ERR_STATE` | Invalid state |

## Build Options

| CMake Option | Default | Description |
|---|---|---|
| `ARBITRO_BUILD_TESTS` | ON | Build unit + integration tests |
| `ARBITRO_BUILD_EXAMPLES` | ON | Build examples |
| `ARBITRO_BUILD_BENCHMARKS` | ON | Build benchmarks |
| `ARBITRO_SANITIZERS` | OFF | Enable ASan + UBSan |
| `ARBITRO_TLS` | OFF | Enable TLS (requires OpenSSL) |

## Testing

```bash
# Unit tests (no broker needed)
make clean all test

# Integration tests (broker on 127.0.0.1:9898)
make build/integration
ARBITRO_ADDR=127.0.0.1:9898 ./build/integration

# With sanitizers
make CFLAGS="-std=c99 -Wall -Wextra -Wpedantic -Werror -fsanitize=address,undefined -fno-omit-frame-pointer" clean all test
```

## Benchmarks

```bash
make benchmarks

# Basic publish throughput (fire-and-forget)
./build/bench_publish --msgs 1000 --payload 64

# Publish→deliver→ack latency
./build/bench_roundtrip --msgs 1000 --payload 64

# Full throughput: single + sync + batch modes with MB/s reporting
./build/bench_throughput --msgs 1000 --batch 64 --payload 64

# Per-subject inflight cap isolation + deliver+ack throughput
./build/bench_limits

# Reconnect correctness under chaos (kill/restart broker during run)
./build/bench_chaos
```

## License

MIT
