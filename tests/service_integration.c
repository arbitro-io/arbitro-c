#ifndef _WIN32
#define _POSIX_C_SOURCE 200112L
#endif

#include "test_harness.h"
#include "arbitro/arbitro.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* Mirrors arbitro-ts/tests/29-service-queue.test.ts. Runs against a real
   broker -- compiling is not evidence that two instances share a load. */

static const char *svc_addr(void) {
    static char host[128];
    const char *addr = getenv("ARBITRO_ADDR");
    if (!addr) return "127.0.0.1";
    {
        const char *colon = strchr(addr, ':');
        if (colon) {
            size_t n = (size_t)(colon - addr);
            if (n >= sizeof(host)) n = sizeof(host) - 1;
            memcpy(host, addr, n);
            host[n] = '\0';
            return host;
        }
    }
    return addr;
}

static uint16_t svc_port(void) {
    const char *addr = getenv("ARBITRO_ADDR");
    if (addr) {
        const char *colon = strchr(addr, ':');
        if (colon) return (uint16_t)atoi(colon + 1);
    }
    return ARBITRO_DEFAULT_PORT;
}

static int svc_connect(arbitro_client_t **out) {
    arbitro_opts_t opts;
    arbitro_opts_init(&opts);
    opts.connect_timeout_ms = 2000;
    opts.request_timeout_ms = 5000;
    return arbitro_client_connect(svc_addr(), svc_port(), &opts, out);
}

static uint64_t svc_now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000u + (uint64_t)ts.tv_nsec / 1000000u;
}

/* The broker is shared and keeps no per-run isolation: a leftover
   `_svc-<name>` stream would hand this run an earlier run's consumers. */
static void svc_unique(char *dst, size_t cap, const char *tag) {
    static unsigned seq = 0;
    snprintf(dst, cap, "%s%llu%u", tag,
             (unsigned long long)(svc_now_ms() % 100000000ull), ++seq);
}

#define MI_REQUESTS 12

typedef struct {
    arbitro_client_t  *client;
    arbitro_service_t *svc;
    pthread_t          th;
    volatile int       quit;
    int                tag;
    /* Written only by this instance's own thread and read after join, so a
       double delivery cannot be swallowed by a lost update. */
    unsigned char      seen[MI_REQUESTS];
    int                hits;
} instance_t;

static void *instance_pump(void *arg) {
    instance_t *in = (instance_t *)arg;
    while (!in->quit) {
        int rc = arbitro_client_poll(in->client, 50);
        if (rc != ARBITRO_OK && rc != ARBITRO_ERR_TIMEOUT) break;
    }
    return NULL;
}

static int mi_work(const arbitro_request_t *req, uint8_t *out, uint32_t cap,
                   uint32_t *out_len, void *ud) {
    instance_t *in = (instance_t *)ud;
    int idx = (req->payload_len >= 1) ? (int)req->payload[0] : -1;

    if (idx >= 0 && idx < MI_REQUESTS) in->seen[idx]++;
    in->hits++;

    if (cap >= 2) {
        out[0] = (uint8_t)in->tag;
        out[1] = (uint8_t)(idx < 0 ? 0xFF : idx);
        *out_len = 2;
    }
    return ARBITRO_OK;
}

static int instance_start(instance_t *in, const char *svc_name, int tag) {
    int rc;
    memset(in, 0, sizeof(*in));
    in->tag = tag;
    rc = svc_connect(&in->client);
    if (rc != ARBITRO_OK) return rc;
    /* Built on the caller's thread: the instance-id counter is a plain
       static, so two threads racing here could share one id. */
    rc = arbitro_service_create(in->client, svc_name, 64, &in->svc);
    if (rc != ARBITRO_OK) return rc;
    rc = arbitro_service_handle(in->svc, "work", mi_work, in);
    if (rc != ARBITRO_OK) return rc;
    if (pthread_create(&in->th, NULL, instance_pump, in) != 0)
        return ARBITRO_ERR_STATE;
    return ARBITRO_OK;
}

static void instance_stop(instance_t *in) {
    if (!in->client) return;
    in->quit = 1;
    pthread_join(in->th, NULL);
    if (in->svc) arbitro_service_destroy(in->svc);
    arbitro_client_close(in->client);
    in->client = NULL;
}

/* Every worker subscription must be live before the first request, or an
   early one lands on whichever instance registered first and the split looks
   lopsided for reasons unrelated to the queue. */
static void svc_settle(void) {
    struct timespec ts;
    ts.tv_sec = 0;
    ts.tv_nsec = 400000000L;
    nanosleep(&ts, NULL);
}

static void svc_drop_stream(arbitro_client_t *c, const char *svc_name) {
    char stream[128];
    snprintf(stream, sizeof(stream), "_svc-%s", svc_name);
    arbitro_stream_delete(c, stream, 0);
}

#define MI_CHECK(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "  FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond); \
        failed = 1; \
    } \
} while (0)

ARB_TEST(test_service_request_reply_roundtrip) {
    instance_t in;
    arbitro_client_t *caller;
    char name[64];
    uint8_t out[8];
    uint32_t out_len = 0;
    uint8_t payload = 7;
    int failed = 0;
    int rc;

    svc_unique(name, sizeof(name), "csvcrt");

    rc = svc_connect(&caller);
    if (rc == ARBITRO_ERR_CONNECT) {
        fprintf(stderr, "  SKIP %s (broker unreachable)\n", arb__test_current);
        arb__test_pass++;
        return;
    }
    ARB_ASSERT_EQ(rc, ARBITRO_OK);

    rc = instance_start(&in, name, 'A');
    if (rc != ARBITRO_OK) {
        fprintf(stderr, "  FAIL instance_start rc=%d\n", rc);
        arb__test_fail++;
        instance_stop(&in);
        arbitro_client_close(caller);
        return;
    }
    svc_settle();

    rc = arbitro_request(caller, name, "work", &payload, 1, 5000,
                         out, sizeof(out), &out_len);
    MI_CHECK(rc == ARBITRO_OK);
    MI_CHECK(out_len == 2);
    if (out_len == 2) {
        MI_CHECK(out[0] == 'A');
        MI_CHECK(out[1] == 7);
    }

    instance_stop(&in);
    MI_CHECK(in.hits == 1);

    svc_drop_stream(caller, name);
    arbitro_client_close(caller);
    if (failed) { arb__test_fail++; return; }
    ARB_PASS();
}

/* Two instances of one service, N requests: each handled exactly once and
   neither instance starved. Two independent failures are covered --
   deliver_mode Fanout (the broker discards the group and cannot round-robin)
   and a service-wide worker consumer NAME (every instance collapses onto one
   consumer id, and the broker allows one subscription per id, so the last to
   subscribe retires its siblings and takes 100% of the traffic). */
ARB_TEST(test_service_two_instances_share_load) {
    instance_t a, b;
    arbitro_client_t *caller;
    arbitro_consumer_info_t consumers[16];
    char stream[128];
    char name[64];
    size_t n_consumers = 0;
    int replies_ok = 0;
    int exactly_once = 1;
    int failed = 0;
    int i, total = 0;
    int rc;

    svc_unique(name, sizeof(name), "csvcq");

    rc = svc_connect(&caller);
    if (rc == ARBITRO_ERR_CONNECT) {
        fprintf(stderr, "  SKIP %s (broker unreachable)\n", arb__test_current);
        arb__test_pass++;
        return;
    }
    ARB_ASSERT_EQ(rc, ARBITRO_OK);

    memset(&a, 0, sizeof(a));
    memset(&b, 0, sizeof(b));
    rc = instance_start(&a, name, 'A');
    if (rc == ARBITRO_OK) rc = instance_start(&b, name, 'B');
    if (rc != ARBITRO_OK) {
        fprintf(stderr, "  FAIL instance_start rc=%d\n", rc);
        arb__test_fail++;
        instance_stop(&a);
        instance_stop(&b);
        arbitro_client_close(caller);
        return;
    }
    svc_settle();

    /* Asked of the broker rather than inferred: two instances must leave FOUR
       durable consumers on the service stream -- a worker and a reply
       consumer each. Three means both collapsed onto one worker consumer. */
    snprintf(stream, sizeof(stream), "_svc-%s", name);
    rc = arbitro_consumer_list(caller, stream, consumers, 16, &n_consumers);
    fprintf(stderr, "  consumer_list rc=%d n=%d on %s\n",
            rc, (int)n_consumers, stream);
    MI_CHECK(rc == ARBITRO_OK);
    MI_CHECK((int)n_consumers == 4);

    for (i = 0; i < MI_REQUESTS; i++) {
        uint8_t payload = (uint8_t)i;
        uint8_t out[8];
        uint32_t out_len = 0;
        rc = arbitro_request(caller, name, "work", &payload, 1, 5000,
                             out, sizeof(out), &out_len);
        if (rc == ARBITRO_OK && out_len == 2 &&
            (out[0] == 'A' || out[0] == 'B') && out[1] == (uint8_t)i) {
            replies_ok++;
        } else {
            fprintf(stderr, "  request %d: rc=%d out_len=%u\n", i, rc, out_len);
        }
    }

    instance_stop(&a);
    instance_stop(&b);

    for (i = 0; i < MI_REQUESTS; i++) {
        int n = (int)a.seen[i] + (int)b.seen[i];
        if (n != 1) {
            fprintf(stderr, "  request %d handled %d times (A=%d B=%d)\n",
                    i, n, (int)a.seen[i], (int)b.seen[i]);
            exactly_once = 0;
        }
        total += n;
    }
    fprintf(stderr, "  split: A=%d B=%d replies_ok=%d/%d\n",
            a.hits, b.hits, replies_ok, MI_REQUESTS);

    MI_CHECK(exactly_once == 1);
    MI_CHECK(total == MI_REQUESTS);
    MI_CHECK(a.hits + b.hits == MI_REQUESTS);
    MI_CHECK(a.hits > 0);
    MI_CHECK(b.hits > 0);
    MI_CHECK(replies_ok == MI_REQUESTS);

    svc_drop_stream(caller, name);
    arbitro_client_close(caller);
    if (failed) { arb__test_fail++; return; }
    ARB_PASS();
}

int main(void) {
    ARB_RUN_TESTS();
    return arb__test_fail ? 1 : 0;
}
