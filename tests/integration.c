#include "test_harness.h"
#include "arbitro/arbitro.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char *get_addr(void) {
    const char *addr = getenv("ARBITRO_ADDR");
    return addr ? addr : "127.0.0.1";
}

static uint16_t get_port(void) {
    const char *addr = getenv("ARBITRO_ADDR");
    if (addr) {
        const char *colon = strchr(addr, ':');
        if (colon) return (uint16_t)atoi(colon + 1);
    }
    return ARBITRO_DEFAULT_PORT;
}

static int try_connect(arbitro_client_t **out) {
    arbitro_opts_t opts;
    arbitro_opts_init(&opts);
    opts.connect_timeout_ms = 2000;
    opts.request_timeout_ms = 5000;
    return arbitro_client_connect(get_addr(), get_port(), &opts, out);
}

ARB_TEST(test_connect_hello) {
    arbitro_client_t *c;
    int rc = try_connect(&c);
    if (rc == ARBITRO_ERR_CONNECT) {
        fprintf(stderr, "  SKIP %s (broker unreachable)\n", arb__test_current);
        arb__test_pass++;
        return;
    }
    ARB_ASSERT_EQ(rc, ARBITRO_OK);
    ARB_ASSERT(arbitro_client_is_connected(c));
    arbitro_client_close(c);
    ARB_PASS();
}

static int delivered = 0;
static void roundtrip_cb(arbitro_msg_t *msg, void *ud) {
    (void)ud;
    arbitro_msg_ack(msg);
    delivered++;
    if (delivered >= 5)
        arbitro_client_stop(msg->client);
}

ARB_TEST(test_pub_sub_roundtrip) {
    arbitro_client_t *c;
    uint32_t stream_id, consumer_id;
    arbitro_stream_cfg_t scfg = {0};
    arbitro_consumer_cfg_t ccfg = {0};
    int rc, i;

    rc = try_connect(&c);
    if (rc == ARBITRO_ERR_CONNECT) {
        fprintf(stderr, "  SKIP %s (broker unreachable)\n", arb__test_current);
        arb__test_pass++;
        return;
    }
    ARB_ASSERT_EQ(rc, ARBITRO_OK);

    scfg.subject_filter = "integ-test.>";
    rc = arbitro_stream_upsert(c, "integ-test", &scfg, &stream_id);
    ARB_ASSERT_EQ(rc, ARBITRO_OK);

    ccfg.name = "integ-worker";
    ccfg.filter = "integ-test.>";
    ccfg.ack_policy = ARBITRO_ACK_EXPLICIT;
    ccfg.max_inflight = 100;
    rc = arbitro_consumer_upsert(c, "integ-test", &ccfg, &consumer_id);
    ARB_ASSERT_EQ(rc, ARBITRO_OK);

    rc = arbitro_subscribe(c, stream_id, consumer_id, roundtrip_cb, NULL);
    ARB_ASSERT_EQ(rc, ARBITRO_OK);

    delivered = 0;
    for (i = 0; i < 5; i++) {
        rc = arbitro_publish(c, stream_id,
                             (const uint8_t *)"integ-test.a", 12,
                             (const uint8_t *)"hello", 5);
        ARB_ASSERT_EQ(rc, ARBITRO_OK);
    }

    rc = arbitro_client_run(c);
    ARB_ASSERT_EQ(delivered, 5);

    arbitro_client_close(c);
    ARB_PASS();
}

ARB_TEST(test_publish_sync_seq) {
    arbitro_client_t *c;
    uint32_t stream_id;
    arbitro_stream_cfg_t scfg = {0};
    uint64_t seq1 = 0, seq2 = 0;
    int rc;

    rc = try_connect(&c);
    if (rc == ARBITRO_ERR_CONNECT) {
        fprintf(stderr, "  SKIP %s (broker unreachable)\n", arb__test_current);
        arb__test_pass++;
        return;
    }
    ARB_ASSERT_EQ(rc, ARBITRO_OK);

    scfg.subject_filter = "integ-sync.>";
    rc = arbitro_stream_upsert(c, "integ-sync", &scfg, &stream_id);
    ARB_ASSERT_EQ(rc, ARBITRO_OK);

    rc = arbitro_publish_sync(c, stream_id,
                              (const uint8_t *)"integ-sync.a", 12,
                              (const uint8_t *)"d1", 2, &seq1);
    ARB_ASSERT_EQ(rc, ARBITRO_OK);

    rc = arbitro_publish_sync(c, stream_id,
                              (const uint8_t *)"integ-sync.a", 12,
                              (const uint8_t *)"d2", 2, &seq2);
    ARB_ASSERT_EQ(rc, ARBITRO_OK);
    ARB_ASSERT(seq2 > seq1);

    arbitro_client_close(c);
    ARB_PASS();
}

ARB_TEST(test_batch_publish) {
    arbitro_client_t *c;
    uint32_t stream_id;
    arbitro_stream_cfg_t scfg = {0};
    arbitro_batch_entry_t entries[3];
    uint64_t first_seq = 0;
    int rc;

    rc = try_connect(&c);
    if (rc == ARBITRO_ERR_CONNECT) {
        fprintf(stderr, "  SKIP %s (broker unreachable)\n", arb__test_current);
        arb__test_pass++;
        return;
    }
    ARB_ASSERT_EQ(rc, ARBITRO_OK);

    scfg.subject_filter = "integ-batch.>";
    rc = arbitro_stream_upsert(c, "integ-batch", &scfg, &stream_id);
    ARB_ASSERT_EQ(rc, ARBITRO_OK);

    entries[0].subject = (const uint8_t *)"integ-batch.a";
    entries[0].subject_len = 13;
    entries[0].msg_id = NULL;
    entries[0].msg_id_len = 0;
    entries[0].payload = (const uint8_t *)"p1";
    entries[0].payload_len = 2;

    entries[1].subject = (const uint8_t *)"integ-batch.b";
    entries[1].subject_len = 13;
    entries[1].msg_id = NULL;
    entries[1].msg_id_len = 0;
    entries[1].payload = (const uint8_t *)"p2";
    entries[1].payload_len = 2;

    entries[2].subject = (const uint8_t *)"integ-batch.c";
    entries[2].subject_len = 13;
    entries[2].msg_id = NULL;
    entries[2].msg_id_len = 0;
    entries[2].payload = (const uint8_t *)"p3";
    entries[2].payload_len = 2;

    rc = arbitro_publish_batch(c, stream_id, entries, 3, &first_seq);
    ARB_ASSERT_EQ(rc, ARBITRO_OK);
    ARB_ASSERT(first_seq > 0);

    arbitro_client_close(c);
    ARB_PASS();
}

ARB_TEST(test_stream_purge) {
    arbitro_client_t *c;
    uint32_t stream_id;
    arbitro_stream_cfg_t scfg = {0};
    uint64_t purged = 0;
    int rc;

    rc = try_connect(&c);
    if (rc == ARBITRO_ERR_CONNECT) {
        fprintf(stderr, "  SKIP %s (broker unreachable)\n", arb__test_current);
        arb__test_pass++;
        return;
    }
    ARB_ASSERT_EQ(rc, ARBITRO_OK);

    scfg.subject_filter = "integ-purge.>";
    rc = arbitro_stream_upsert(c, "integ-purge", &scfg, &stream_id);
    ARB_ASSERT_EQ(rc, ARBITRO_OK);

    rc = arbitro_publish_sync(c, stream_id,
                              (const uint8_t *)"integ-purge.a", 13,
                              (const uint8_t *)"x", 1, NULL);
    ARB_ASSERT_EQ(rc, ARBITRO_OK);

    rc = arbitro_stream_purge(c, "integ-purge", &purged);
    ARB_ASSERT_EQ(rc, ARBITRO_OK);

    arbitro_client_close(c);
    ARB_PASS();
}

ARB_TEST(test_metrics) {
    arbitro_client_t *c;
    uint32_t stream_id;
    arbitro_stream_cfg_t scfg = {0};
    arbitro_metrics_t m;
    int rc;

    rc = try_connect(&c);
    if (rc == ARBITRO_ERR_CONNECT) {
        fprintf(stderr, "  SKIP %s (broker unreachable)\n", arb__test_current);
        arb__test_pass++;
        return;
    }
    ARB_ASSERT_EQ(rc, ARBITRO_OK);

    scfg.subject_filter = "integ-metrics.>";
    rc = arbitro_stream_upsert(c, "integ-metrics", &scfg, &stream_id);
    ARB_ASSERT_EQ(rc, ARBITRO_OK);

    rc = arbitro_publish(c, stream_id,
                         (const uint8_t *)"integ-metrics.a", 15,
                         (const uint8_t *)"x", 1);
    ARB_ASSERT_EQ(rc, ARBITRO_OK);

    arbitro_client_metrics(c, &m);
    ARB_ASSERT(m.publishes_sent >= 1);

    arbitro_client_close(c);
    ARB_PASS();
}

static int pending_delivered = 0;
static void pending_cb(arbitro_msg_t *msg, void *ud) {
    (void)msg;
    (void)ud;
    pending_delivered++;
}

ARB_TEST(test_get_pending) {
    arbitro_client_t *c;
    uint32_t stream_id, consumer_id;
    arbitro_stream_cfg_t scfg = {0};
    arbitro_consumer_cfg_t ccfg = {0};
    uint64_t pending = 0;
    int rc;

    rc = try_connect(&c);
    if (rc == ARBITRO_ERR_CONNECT) {
        fprintf(stderr, "  SKIP %s (broker unreachable)\n", arb__test_current);
        arb__test_pass++;
        return;
    }
    ARB_ASSERT_EQ(rc, ARBITRO_OK);

    scfg.subject_filter = "integ-pending.>";
    rc = arbitro_stream_upsert(c, "integ-pending", &scfg, &stream_id);
    ARB_ASSERT_EQ(rc, ARBITRO_OK);

    ccfg.name = "integ-pending-worker";
    ccfg.filter = "integ-pending.>";
    ccfg.ack_policy = ARBITRO_ACK_EXPLICIT;
    ccfg.max_inflight = 100;
    rc = arbitro_consumer_upsert(c, "integ-pending", &ccfg, &consumer_id);
    ARB_ASSERT_EQ(rc, ARBITRO_OK);

    rc = arbitro_subscribe(c, stream_id, consumer_id, pending_cb, NULL);
    ARB_ASSERT_EQ(rc, ARBITRO_OK);

    pending_delivered = 0;
    rc = arbitro_publish_sync(c, stream_id,
                              (const uint8_t *)"integ-pending.a", 15,
                              (const uint8_t *)"x", 1, NULL);
    ARB_ASSERT_EQ(rc, ARBITRO_OK);

    rc = arbitro_client_poll(c, 2000);
    ARB_ASSERT(rc == ARBITRO_OK || rc == ARBITRO_ERR_TIMEOUT);
    ARB_ASSERT_EQ(pending_delivered, 1);

    rc = arbitro_get_pending(c, consumer_id, &pending);
    ARB_ASSERT_EQ(rc, ARBITRO_OK);
    ARB_ASSERT(pending >= 1);

    arbitro_client_close(c);
    ARB_PASS();
}

int main(void) {
    fprintf(stdout, "arbitro-c integration tests\n");
    fprintf(stdout, "broker: %s:%u\n\n", get_addr(), (unsigned)get_port());
    ARB_RUN_TESTS();
}
