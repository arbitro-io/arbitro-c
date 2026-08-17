/* arbitro_subscribe_batch — N subscriptions in ONE round-trip.
 *
 * The sibling fan-out itself is pinned elsewhere; what is new here is that
 * opening the siblings in a single frame reaches the same place as opening
 * them one at a time, and that a filter outside the consumer's slice comes
 * back naming ITS entry while its peers stay open. */

#ifndef _WIN32
#define _POSIX_C_SOURCE 200112L
#endif

#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include "arbitro/arbitro.h"
#include "test_addr.h"

static int total = 0, pass = 0, fail = 0;

#define T(name) do { total++; printf("[%d] %s ... ", total, name); fflush(stdout); } while(0)
#define OK() do { pass++; printf("PASS\n"); } while(0)
#define FAIL(fmt, ...) do { fail++; printf("FAIL — " fmt "\n", ##__VA_ARGS__); } while(0)

/* Wire code for a subscription filter that escapes its consumer's slice. */
#define ERRCODE_INVALID_SUBSCRIPTION_FILTER 0x0306

static uint64_t nowms(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static int n_orders = 0, n_pay = 0, n_all = 0;
static void on_orders(arbitro_msg_t *m, void *ud) { (void)ud; n_orders++; arbitro_msg_ack(m); }
static void on_pay   (arbitro_msg_t *m, void *ud) { (void)ud; n_pay++;    arbitro_msg_ack(m); }
static void on_all   (arbitro_msg_t *m, void *ud) { (void)ud; n_all++;    arbitro_msg_ack(m); }
static void on_noop  (arbitro_msg_t *m, void *ud) { (void)ud; arbitro_msg_ack(m); }

/* Three filtered siblings in one frame split as three separate subscribes
 * would, and the entry with no filter inherits the consumer's. */
static void test_batch_splits_identically(void) {
    arbitro_client_t *c;
    arbitro_stream_cfg_t scfg = {0};
    arbitro_consumer_cfg_t ccfg = {0};
    uint32_t sid = 0, cid = 0;
    char sname[64], f_ord[96], f_pay[96];
    arbitro_sub_entry_t entries[3];
    arbitro_sub_result_t results[3];
    int rc, i;
    uint64_t t0;

    snprintf(sname, sizeof(sname), "cbatch_%llu", (unsigned long long)nowms());
    T("test_batch_splits_identically");
    n_orders = n_pay = n_all = 0;

    if (arbitro_client_connect(arb_test_addr(), arb_test_port(), NULL, &c) != ARBITRO_OK) {
        FAIL("connect"); return;
    }
    scfg.subject_filter = arb_slice(sname);
    arbitro_stream_upsert(c, sname, &scfg, &sid);

    ccfg.name = "w"; ccfg.group = "w"; ccfg.fanout = 1;
    ccfg.ack_policy = 1; ccfg.max_inflight = 1000; ccfg.ack_wait_ms = 30000;
    ccfg.filter = arb_slice(sname);
    if (arbitro_consumer_create(c, sname, &ccfg, &cid) != ARBITRO_OK) {
        FAIL("consumer_create"); arbitro_client_close(c); return;
    }

    snprintf(f_ord, sizeof(f_ord), "%s.orders.*", sname);
    snprintf(f_pay, sizeof(f_pay), "%s.payments.*", sname);
    entries[0].filter = (const uint8_t *)f_ord;
    entries[0].filter_len = arb_slen(f_ord);
    entries[0].cb = on_orders; entries[0].userdata = NULL;
    entries[1].filter = (const uint8_t *)f_pay;
    entries[1].filter_len = arb_slen(f_pay);
    entries[1].cb = on_pay; entries[1].userdata = NULL;
    /* No filter — inherits the consumer's. */
    entries[2].filter = NULL; entries[2].filter_len = 0;
    entries[2].cb = on_all; entries[2].userdata = NULL;

    rc = arbitro_subscribe_batch(c, sid, cid, entries, 3, results);
    if (rc != ARBITRO_OK) {
        FAIL("subscribe_batch rc=%d (codes %u/%u/%u)", rc,
             results[0].code, results[1].code, results[2].code);
        arbitro_client_close(c); return;
    }
    /* Distinct non-zero ids: zero is the broker's "unnamed" sentinel and
       equal ids would leave the siblings indistinguishable on ack. */
    if (results[0].sub_id == 0 ||
        results[0].sub_id == results[1].sub_id ||
        results[1].sub_id == results[2].sub_id) {
        FAIL("subscription ids collided or were zero: %u/%u/%u",
             results[0].sub_id, results[1].sub_id, results[2].sub_id);
        arbitro_client_close(c); return;
    }

    for (i = 0; i < 3; i++) {
        char subj[128];
        snprintf(subj, sizeof(subj), "%s.orders.%d", sname, i);
        arbitro_publish_sync(c, sid, (const uint8_t *)subj, arb_slen(subj),
                             (const uint8_t *)"o", 1, NULL);
    }
    for (i = 0; i < 2; i++) {
        char subj[128];
        snprintf(subj, sizeof(subj), "%s.payments.%d", sname, i);
        arbitro_publish_sync(c, sid, (const uint8_t *)subj, arb_slen(subj),
                             (const uint8_t *)"p", 1, NULL);
    }
    {
        const char *subj = arb_scoped(sname, "audit.trail");
        arbitro_publish_sync(c, sid, (const uint8_t *)subj, arb_slen(subj),
                             (const uint8_t *)"x", 1, NULL);
    }

    t0 = nowms();
    while (nowms() - t0 < 2000) arbitro_client_poll(c, 100);

    if (n_orders == 3 && n_pay == 2 && n_all == 6) OK();
    else FAIL("orders=%d (want 3) pay=%d (want 2) all=%d (want 6)", n_orders, n_pay, n_all);
    arbitro_client_close(c);
}

/* A filter outside the consumer's slice is refused ALONE, by entry, and its
 * legal peers stay open. */
static void test_batch_refuses_one_entry(void) {
    arbitro_client_t *c;
    arbitro_stream_cfg_t scfg = {0};
    arbitro_consumer_cfg_t ccfg = {0};
    uint32_t sid = 0, cid = 0;
    char sname[64], narrow[96], f_ok1[96], f_bad[96], f_ok2[96];
    arbitro_sub_entry_t entries[3];
    arbitro_sub_result_t results[3];
    int rc;

    snprintf(sname, sizeof(sname), "cbadf_%llu", (unsigned long long)nowms());
    T("test_batch_refuses_one_entry");

    if (arbitro_client_connect(arb_test_addr(), arb_test_port(), NULL, &c) != ARBITRO_OK) {
        FAIL("connect"); return;
    }
    scfg.subject_filter = arb_slice(sname);
    arbitro_stream_upsert(c, sname, &scfg, &sid);

    /* Deliberately narrow: a payments.* sibling escapes orders.>. */
    snprintf(narrow, sizeof(narrow), "%s.orders.>", sname);
    ccfg.name = "n"; ccfg.group = "n"; ccfg.fanout = 1;
    ccfg.ack_policy = 1; ccfg.max_inflight = 100; ccfg.ack_wait_ms = 30000;
    ccfg.filter = narrow;
    if (arbitro_consumer_create(c, sname, &ccfg, &cid) != ARBITRO_OK) {
        FAIL("consumer_create"); arbitro_client_close(c); return;
    }

    snprintf(f_ok1, sizeof(f_ok1), "%s.orders.a", sname);
    snprintf(f_bad, sizeof(f_bad), "%s.payments.*", sname);
    snprintf(f_ok2, sizeof(f_ok2), "%s.orders.b", sname);
    entries[0].filter = (const uint8_t *)f_ok1; entries[0].filter_len = arb_slen(f_ok1);
    entries[0].cb = on_noop; entries[0].userdata = NULL;
    entries[1].filter = (const uint8_t *)f_bad; entries[1].filter_len = arb_slen(f_bad);
    entries[1].cb = on_noop; entries[1].userdata = NULL;
    entries[2].filter = (const uint8_t *)f_ok2; entries[2].filter_len = arb_slen(f_ok2);
    entries[2].cb = on_noop; entries[2].userdata = NULL;

    rc = arbitro_subscribe_batch(c, sid, cid, entries, 3, results);

    if (rc != ARBITRO_ERR_BROKER) {
        FAIL("a filter outside the consumer was accepted: rc=%d", rc);
    } else if (results[1].code != ERRCODE_INVALID_SUBSCRIPTION_FILTER) {
        FAIL("entry 1 code=0x%04x, want 0x%04x", results[1].code,
             ERRCODE_INVALID_SUBSCRIPTION_FILTER);
    } else if (results[0].code != 0 || results[2].code != 0) {
        /* Per entry, not all-or-nothing. */
        FAIL("a single bad entry took its peers down: codes %u/%u",
             results[0].code, results[2].code);
    } else {
        OK();
    }
    arbitro_client_close(c);
}

int main(void) {
    printf("=== subscribe_batch tests ===\n");
    test_batch_splits_identically();
    test_batch_refuses_one_entry();
    printf("=== %d/%d passed (%d failed) ===\n", pass, total, fail);
    return fail ? 1 : 0;
}
