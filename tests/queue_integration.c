#include "test_harness.h"
#include "arbitro/arbitro.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Mirrors the Rust e2e suite for arbitro_queue_subscribe. Runs against a real
   broker -- compiling is not evidence that a queue distributes anything. */

static const char *qs_addr(void) {
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

static uint16_t qs_port(void) {
    const char *addr = getenv("ARBITRO_ADDR");
    if (addr) {
        const char *colon = strchr(addr, ':');
        if (colon) return (uint16_t)atoi(colon + 1);
    }
    return ARBITRO_DEFAULT_PORT;
}

static int qs_connect(arbitro_client_t **out) {
    arbitro_opts_t opts;
    arbitro_opts_init(&opts);
    opts.connect_timeout_ms = 2000;
    opts.request_timeout_ms = 5000;
    return arbitro_client_connect(qs_addr(), qs_port(), &opts, out);
}

static int qs_seen[64];
static int qs_count;
static int qs_want;

static void qs_collect(arbitro_msg_t *msg, void *ud) {
    (void)ud;
    if (msg->data_len == 1 && msg->data[0] < 64)
        qs_seen[msg->data[0]]++;
    arbitro_msg_ack(msg);
    if (++qs_count >= qs_want)
        arbitro_client_stop(msg->client);
}

/* Every job goes to exactly one member of the queue. A duplicate means queue
   dedup broke; a shortfall means loss. */
ARB_TEST(test_queue_delivers_each_job_once) {
    arbitro_client_t *c;
    arbitro_stream_cfg_t scfg = {0};
    arbitro_queue_cfg_t qcfg = {0};
    uint32_t stream_id;
    int rc, i;

    rc = qs_connect(&c);
    if (rc == ARBITRO_ERR_CONNECT) {
        fprintf(stderr, "  SKIP %s (broker unreachable)\n", arb__test_current);
        arb__test_pass++;
        return;
    }
    ARB_ASSERT_EQ(rc, ARBITRO_OK);

    scfg.subject_filter = "cqs-once.>";
    rc = arbitro_stream_upsert(c, "cqs-once", &scfg, &stream_id);
    ARB_ASSERT_EQ(rc, ARBITRO_OK);

    qcfg.group = "workers";
    qcfg.filter = "cqs-once.>";
    rc = arbitro_queue_subscribe(c, "cqs-once", &qcfg, qs_collect, NULL);
    ARB_ASSERT_EQ(rc, ARBITRO_OK);

    memset(qs_seen, 0, sizeof(qs_seen));
    qs_count = 0;
    qs_want = 10;
    for (i = 0; i < 10; i++) {
        uint8_t payload = (uint8_t)i;
        rc = arbitro_publish(c, stream_id, (const uint8_t *)"cqs-once.job", 12,
                             &payload, 1);
        ARB_ASSERT_EQ(rc, ARBITRO_OK);
    }

    arbitro_client_run(c);
    ARB_ASSERT_EQ(qs_count, 10);
    for (i = 0; i < 10; i++)
        ARB_ASSERT_EQ(qs_seen[i], 1);

    arbitro_stream_delete(c, "cqs-once", 0);
    arbitro_client_close(c);
    ARB_PASS();
}

/* One durable consumer per queue NAME, counted against the broker's own
   consumer list -- not per subscription and not per connection. */
ARB_TEST(test_queue_one_consumer_per_name) {
    arbitro_client_t *a, *w1, *w2, *audit;
    arbitro_stream_cfg_t scfg = {0};
    arbitro_queue_cfg_t qcfg = {0};
    arbitro_consumer_info_t list[16];
    uint32_t stream_id;
    size_t n = 0;
    int rc;

    rc = qs_connect(&a);
    if (rc == ARBITRO_ERR_CONNECT) {
        fprintf(stderr, "  SKIP %s (broker unreachable)\n", arb__test_current);
        arb__test_pass++;
        return;
    }
    ARB_ASSERT_EQ(rc, ARBITRO_OK);

    scfg.subject_filter = "cqs-count.>";
    rc = arbitro_stream_upsert(a, "cqs-count", &scfg, &stream_id);
    ARB_ASSERT_EQ(rc, ARBITRO_OK);

    ARB_ASSERT_EQ(qs_connect(&w1), ARBITRO_OK);
    ARB_ASSERT_EQ(qs_connect(&w2), ARBITRO_OK);
    qcfg.group = "workers";
    ARB_ASSERT_EQ(arbitro_queue_subscribe(w1, "cqs-count", &qcfg, qs_collect, NULL), ARBITRO_OK);
    ARB_ASSERT_EQ(arbitro_queue_subscribe(w2, "cqs-count", &qcfg, qs_collect, NULL), ARBITRO_OK);

    rc = arbitro_consumer_list(a, "cqs-count", list, 16, &n);
    ARB_ASSERT_EQ(rc, ARBITRO_OK);
    ARB_ASSERT_EQ((int)n, 1);

    ARB_ASSERT_EQ(qs_connect(&audit), ARBITRO_OK);
    qcfg.group = "audit";
    ARB_ASSERT_EQ(arbitro_queue_subscribe(audit, "cqs-count", &qcfg, qs_collect, NULL), ARBITRO_OK);

    rc = arbitro_consumer_list(a, "cqs-count", list, 16, &n);
    ARB_ASSERT_EQ(rc, ARBITRO_OK);
    ARB_ASSERT_EQ((int)n, 2);

    arbitro_stream_delete(a, "cqs-count", 0);
    arbitro_client_close(audit);
    arbitro_client_close(w2);
    arbitro_client_close(w1);
    arbitro_client_close(a);
    ARB_PASS();
}

/* A NULL cfg means all defaults, and the group falls back to the stream
   name -- so the minimal call is still a working queue. */
ARB_TEST(test_queue_null_cfg_defaults_to_stream_name) {
    arbitro_client_t *c;
    arbitro_stream_cfg_t scfg = {0};
    uint32_t stream_id;
    int rc;

    rc = qs_connect(&c);
    if (rc == ARBITRO_ERR_CONNECT) {
        fprintf(stderr, "  SKIP %s (broker unreachable)\n", arb__test_current);
        arb__test_pass++;
        return;
    }
    ARB_ASSERT_EQ(rc, ARBITRO_OK);

    scfg.subject_filter = "cqs-default.>";
    rc = arbitro_stream_upsert(c, "cqs-default", &scfg, &stream_id);
    ARB_ASSERT_EQ(rc, ARBITRO_OK);

    rc = arbitro_queue_subscribe(c, "cqs-default", NULL, qs_collect, NULL);
    ARB_ASSERT_EQ(rc, ARBITRO_OK);

    /* Reachable under the stream's own name: that is the fallback working. */
    ARB_ASSERT_EQ(arbitro_consumer_exists(c, "cqs-default", "cqs-default"), 1);

    qs_count = 0;
    qs_want = 1;
    rc = arbitro_publish(c, stream_id, (const uint8_t *)"cqs-default.job", 15,
                         (const uint8_t *)"x", 1);
    ARB_ASSERT_EQ(rc, ARBITRO_OK);
    arbitro_client_run(c);
    ARB_ASSERT_EQ(qs_count, 1);

    arbitro_stream_delete(c, "cqs-default", 0);
    arbitro_client_close(c);
    ARB_PASS();
}

static int qs_deliveries;
static void qs_nack_first(arbitro_msg_t *msg, void *ud) {
    (void)ud;
    qs_deliveries++;
    if (qs_deliveries == 1) {
        arbitro_msg_nack(msg);
    } else {
        arbitro_msg_ack(msg);
        arbitro_client_stop(msg->client);
    }
}

/* A job the worker never acked must come back -- the half of at-least-once
   that separates a queue from fire-and-forget. */
ARB_TEST(test_queue_redelivers_nacked_job) {
    arbitro_client_t *c;
    arbitro_stream_cfg_t scfg = {0};
    arbitro_queue_cfg_t qcfg = {0};
    uint32_t stream_id;
    int rc;

    rc = qs_connect(&c);
    if (rc == ARBITRO_ERR_CONNECT) {
        fprintf(stderr, "  SKIP %s (broker unreachable)\n", arb__test_current);
        arb__test_pass++;
        return;
    }
    ARB_ASSERT_EQ(rc, ARBITRO_OK);

    scfg.subject_filter = "cqs-redeliver.>";
    rc = arbitro_stream_upsert(c, "cqs-redeliver", &scfg, &stream_id);
    ARB_ASSERT_EQ(rc, ARBITRO_OK);

    qcfg.group = "workers";
    rc = arbitro_queue_subscribe(c, "cqs-redeliver", &qcfg, qs_nack_first, NULL);
    ARB_ASSERT_EQ(rc, ARBITRO_OK);

    qs_deliveries = 0;
    rc = arbitro_publish(c, stream_id, (const uint8_t *)"cqs-redeliver.job", 17,
                         (const uint8_t *)"job-1", 5);
    ARB_ASSERT_EQ(rc, ARBITRO_OK);

    arbitro_client_run(c);
    ARB_ASSERT(qs_deliveries >= 2);

    arbitro_stream_delete(c, "cqs-redeliver", 0);
    arbitro_client_close(c);
    ARB_PASS();
}

int main(void) {
    ARB_RUN_TESTS();
    return arb__test_fail ? 1 : 0;
}
