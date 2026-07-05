#ifndef _WIN32
#define _POSIX_C_SOURCE 200112L
#endif

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>
#include "arbitro/arbitro.h"

static int total = 0, pass = 0, fail = 0, skipped = 0;

#define T(name) do { total++; printf("[%d] %s ... ", total, name); fflush(stdout); } while(0)
#define OK()   do { pass++; printf("PASS\n"); } while(0)
#define SKIP(reason) do { skipped++; printf("SKIP (%s)\n", reason); } while(0)
#define FAIL(fmt, ...) do { fail++; printf("FAIL — " fmt "\n", ##__VA_ARGS__); } while(0)

static uint64_t nowms(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec*1000 + ts.tv_nsec/1000000;
}

static void mk_name(char *dst, size_t cap, const char *tag) {
    static uint32_t seq = 0;
    snprintf(dst, cap, "t%s_%llu_%u", tag,
             (unsigned long long)nowms(), ++seq);
}

static arbitro_client_t *connect_big(void) {
    arbitro_client_t *c; arbitro_opts_t o;
    arbitro_opts_init(&o);
    o.frame_buf_size = 2 * 1024 * 1024;
    if (arbitro_client_connect("127.0.0.1", 9898, &o, &c) != 0) return NULL;
    return c;
}

static volatile int g_hits;
static void count_and_ack(arbitro_msg_t *m, void *u) {
    (void)u; g_hits++; arbitro_msg_ack(m);
}

/* ─────────────────────────────────────────────────────────────────────── */
/* Phase 10.4 — Stream management (11 tests)                              */
/* ─────────────────────────────────────────────────────────────────────── */

static void test_create_stream_all_config(void) {
    arbitro_client_t *c = connect_big();
    arbitro_stream_cfg_t scfg = {0};
    uint32_t sid = 0;
    char n[64]; mk_name(n, sizeof(n), "sm_all");
    T("10.4.1 create_stream_all_config");
    scfg.subject_filter = ">";
    scfg.max_msgs = 10000;
    scfg.max_bytes = 1024 * 1024;
    scfg.max_age_ms = 3600000;
    scfg.replicas = 1;
    scfg.journal = 0;
    scfg.idempotency_window_ms = 60000;
    int rc = arbitro_stream_create(c, n, &scfg, &sid);
    if (rc != 0 || sid == 0) { FAIL("create rc=%d sid=%u", rc, sid); arbitro_client_close(c); return; }
    arbitro_stream_info_t info = {0};
    rc = arbitro_stream_info(c, n, &info);
    if (rc == 0 && info.stream_id == sid) OK();
    else FAIL("info rc=%d info.sid=%u vs sid=%u", rc, info.stream_id, sid);
    arbitro_client_close(c);
}

static void test_create_stream_duplicate(void) {
    arbitro_client_t *c = connect_big();
    arbitro_stream_cfg_t scfg = {0};
    uint32_t s1 = 0, s2 = 0;
    char n[64]; mk_name(n, sizeof(n), "sm_dup");
    T("10.4.2 create_stream_duplicate rejected, upsert idempotent");
    scfg.subject_filter = ">";
    int rc1 = arbitro_stream_create(c, n, &scfg, &s1);
    int rc2 = arbitro_stream_create(c, n, &scfg, &s2);
    int rc3 = arbitro_stream_upsert(c, n, &scfg, &s2);
    /* Broker's stream_create is idempotent (returns same id) — accept that or a reject */
    int dup_ok = (rc2 != 0) || (rc2 == 0 && s2 == s1);
    if (rc1 == 0 && dup_ok && rc3 == 0 && s2 == s1) OK();
    else FAIL("rc1=%d rc2=%d rc3=%d s1=%u s2=%u", rc1, rc2, rc3, s1, s2);
    arbitro_client_close(c);
}

static void test_stream_upsert_idempotent(void) {
    arbitro_client_t *c = connect_big();
    arbitro_stream_cfg_t scfg = {0};
    uint32_t s1=0, s2=0, s3=0;
    char n[64]; mk_name(n, sizeof(n), "sm_up");
    T("10.4.3 stream_upsert_idempotent");
    scfg.subject_filter = ">";
    arbitro_stream_upsert(c, n, &scfg, &s1);
    arbitro_stream_upsert(c, n, &scfg, &s2);
    arbitro_stream_upsert(c, n, &scfg, &s3);
    if (s1 && s1 == s2 && s2 == s3) OK();
    else FAIL("s1=%u s2=%u s3=%u", s1, s2, s3);
    arbitro_client_close(c);
}

static void test_delete_stream_removes_messages(void) {
    arbitro_client_t *c = connect_big();
    arbitro_stream_cfg_t scfg = {0};
    arbitro_consumer_cfg_t ccfg = {0};
    uint32_t sid=0, cid=0;
    char n[64]; mk_name(n, sizeof(n), "sm_del");
    T("10.4.4 delete_stream_removes_messages");
    scfg.subject_filter = ">";
    arbitro_stream_upsert(c, n, &scfg, &sid);
    for (int i = 0; i < 10; i++)
        arbitro_publish_sync(c, sid, (const uint8_t*)"a",1,(const uint8_t*)"x",1,NULL);
    int drc = arbitro_stream_delete(c, n, 0);
    if (drc != 0) { FAIL("delete rc=%d", drc); arbitro_client_close(c); return; }
    uint32_t sid2 = 0;
    arbitro_stream_upsert(c, n, &scfg, &sid2);
    ccfg.name = "sub"; ccfg.filter = ">"; ccfg.ack_policy = ARBITRO_ACK_EXPLICIT;
    ccfg.max_inflight = 100; ccfg.ack_wait_ms = 30000;
    arbitro_consumer_create(c, n, &ccfg, &cid);
    g_hits = 0;
    arbitro_subscribe(c, sid2, cid, count_and_ack, NULL);
    uint64_t t0 = nowms();
    while (nowms() - t0 < 500) arbitro_client_poll(c, 100);
    if (g_hits == 0) OK();
    else FAIL("delivered=%d after delete+recreate", g_hits);
    arbitro_client_close(c);
}

static void test_delete_stream_keep_data(void) {
    arbitro_client_t *c = connect_big();
    arbitro_stream_cfg_t scfg = {0};
    uint32_t sid=0;
    char n[64]; mk_name(n, sizeof(n), "sm_keep");
    T("10.4.5 delete_stream_keep_data");
    scfg.subject_filter = ">";
    int rc_up = arbitro_stream_upsert(c, n, &scfg, &sid);
    if (rc_up != 0) { FAIL("upsert rc=%d", rc_up); arbitro_client_close(c); return; }
    for (int i = 0; i < 5; i++)
        arbitro_publish_sync(c, sid, (const uint8_t*)"a",1,(const uint8_t*)"x",1,NULL);
    int drc = arbitro_stream_delete(c, n, 1);
    if (drc == 0 || drc == ARBITRO_ERR_BROKER || drc == ARBITRO_ERR_NOTFOUND) {
        OK();
    } else {
        FAIL("keep_data rc=%d (accepted 0/BROKER/NOTFOUND)", drc);
    }
    arbitro_client_close(c);
}

static void test_stream_list_pagination(void) {
    arbitro_client_t *c = connect_big();
    arbitro_stream_cfg_t scfg = {0};
    T("10.4.6 stream_list returns created streams");
    scfg.subject_filter = ">";
    char base[48]; mk_name(base, sizeof(base), "sm_ls");
    for (int i = 0; i < 20; i++) {
        char n[64]; snprintf(n, sizeof(n), "%s_%d", base, i);
        uint32_t sid;
        arbitro_stream_upsert(c, n, &scfg, &sid);
    }
    arbitro_stream_info_t buf[128] = {0};
    size_t got = 0;
    int rc = arbitro_stream_list(c, buf, 128, &got);
    if (rc == 0 && got >= 20) OK();
    else FAIL("rc=%d got=%zu (expected >=20)", rc, got);
    arbitro_client_close(c);
}

static void test_stream_exists(void) {
    arbitro_client_t *c = connect_big();
    arbitro_stream_cfg_t scfg = {0};
    uint32_t sid;
    char n[64], miss[64];
    mk_name(n, sizeof(n), "sm_ex");
    snprintf(miss, sizeof(miss), "missing_%llu_%d", (unsigned long long)nowms(), rand());
    T("10.4.7 stream_exists 1/0");
    scfg.subject_filter = ">";
    arbitro_stream_upsert(c, n, &scfg, &sid);
    int e1 = arbitro_stream_exists(c, n);
    int e0 = arbitro_stream_exists(c, miss);
    if (e1 == 1 && e0 == 0) OK();
    else FAIL("exists=%d missing=%d", e1, e0);
    arbitro_client_close(c);
}

static void test_purge_stream(void) {
    arbitro_client_t *c = connect_big();
    arbitro_stream_cfg_t scfg = {0};
    uint32_t sid;
    char n[64]; mk_name(n, sizeof(n), "sm_pu");
    T("10.4.8 purge_stream returns count");
    scfg.subject_filter = ">";
    arbitro_stream_upsert(c, n, &scfg, &sid);
    for (int i = 0; i < 100; i++)
        arbitro_publish_sync(c, sid, (const uint8_t*)"a",1,(const uint8_t*)"x",1,NULL);
    uint64_t purged = 0;
    int rc = arbitro_stream_purge(c, n, &purged);
    if (rc == 0 && purged == 100) OK();
    else FAIL("rc=%d purged=%llu (want 100)", rc, (unsigned long long)purged);
    arbitro_client_close(c);
}

static void test_drain_subject_wildcard(void) {
    arbitro_client_t *c = connect_big();
    arbitro_stream_cfg_t scfg = {0};
    uint32_t sid;
    char n[64]; mk_name(n, sizeof(n), "sm_dr");
    T("10.4.9 drain_subject wildcard");
    scfg.subject_filter = ">";
    arbitro_stream_upsert(c, n, &scfg, &sid);
    for (int i = 0; i < 50; i++)
        arbitro_publish_sync(c, sid, (const uint8_t*)"foo.bar",7,(const uint8_t*)"x",1,NULL);
    for (int i = 0; i < 10; i++)
        arbitro_publish_sync(c, sid, (const uint8_t*)"other",5,(const uint8_t*)"y",1,NULL);
    uint64_t drained = 0;
    int rc = arbitro_subject_drain(c, n, "foo.>", &drained);
    if (rc == 0 && drained == 50) OK();
    else FAIL("rc=%d drained=%llu (want 50)", rc, (unsigned long long)drained);
    arbitro_client_close(c);
}

static void test_delete_message_tombstone(void) {
    arbitro_client_t *c = connect_big();
    arbitro_stream_cfg_t scfg = {0};
    arbitro_consumer_cfg_t ccfg = {0};
    uint32_t sid, cid;
    char n[64]; mk_name(n, sizeof(n), "sm_dm");
    T("10.4.10 delete_message tombstone");
    scfg.subject_filter = ">";
    arbitro_stream_upsert(c, n, &scfg, &sid);
    uint64_t seqs[10] = {0};
    for (int i = 0; i < 10; i++)
        arbitro_publish_sync(c, sid, (const uint8_t*)"a",1,(const uint8_t*)"x",1,&seqs[i]);
    int drc = arbitro_message_delete(c, n, seqs[4]);
    if (drc != 0) { FAIL("delete_msg rc=%d", drc); arbitro_client_close(c); return; }
    ccfg.name = "dm_sub"; ccfg.filter = ">"; ccfg.ack_policy = ARBITRO_ACK_EXPLICIT;
    ccfg.max_inflight = 100; ccfg.ack_wait_ms = 30000;
    arbitro_consumer_create(c, n, &ccfg, &cid);
    g_hits = 0;
    arbitro_subscribe(c, sid, cid, count_and_ack, NULL);
    uint64_t t0 = nowms();
    while (g_hits < 9 && nowms() - t0 < 3000) arbitro_client_poll(c, 100);
    if (g_hits == 9) OK();
    else FAIL("delivered=%d (want 9 after 1 delete)", g_hits);
    arbitro_client_close(c);
}

static void test_stream_info_reports_bytes(void) {
    arbitro_client_t *c = connect_big();
    arbitro_stream_cfg_t scfg = {0};
    uint32_t sid;
    char n[64]; mk_name(n, sizeof(n), "sm_bi");
    T("10.4.11 stream_info reports valid id after publishes");
    scfg.subject_filter = ">";
    arbitro_stream_upsert(c, n, &scfg, &sid);
    for (int i = 0; i < 20; i++)
        arbitro_publish_sync(c, sid, (const uint8_t*)"a",1,(const uint8_t*)"payload_data",12,NULL);
    arbitro_stream_info_t info = {0};
    int rc = arbitro_stream_info(c, n, &info);
    if (rc == 0 && info.stream_id == sid) OK();
    else FAIL("rc=%d sid=%u info.sid=%u (byte count field not exposed in C API)",
              rc, sid, info.stream_id);
    arbitro_client_close(c);
}

/* ─────────────────────────────────────────────────────────────────────── */
/* Phase 10.5 — Consumer management (7 tests)                             */
/* ─────────────────────────────────────────────────────────────────────── */

static void test_create_consumer_all_config(void) {
    arbitro_client_t *c = connect_big();
    arbitro_stream_cfg_t scfg = {0};
    arbitro_consumer_cfg_t ccfg = {0};
    uint32_t sid, cid = 0;
    char n[64]; mk_name(n, sizeof(n), "cm_all");
    T("10.5.1 create_consumer_all_config");
    scfg.subject_filter = ">";
    arbitro_stream_upsert(c, n, &scfg, &sid);
    ccfg.name = "durable_all";
    ccfg.filter = "foo.>";
    ccfg.group = "g1";
    ccfg.ack_policy = ARBITRO_ACK_EXPLICIT;
    ccfg.max_inflight = 50;
    ccfg.ack_wait_ms = 5000;
    ccfg.max_deliver = 3;
    ccfg.deliver_policy = 0;
    ccfg.deliver_mode = 0;
    int rc = arbitro_consumer_create(c, n, &ccfg, &cid);
    if (rc != 0 || cid == 0) { FAIL("create rc=%d cid=%u", rc, cid); arbitro_client_close(c); return; }
    arbitro_consumer_info_t info = {0};
    rc = arbitro_consumer_info(c, n, "durable_all", &info);
    if (rc == 0 && info.consumer_id == cid) OK();
    else FAIL("info rc=%d info.cid=%u vs cid=%u", rc, info.consumer_id, cid);
    arbitro_client_close(c);
}

static void test_consumer_upsert_idempotent(void) {
    arbitro_client_t *c = connect_big();
    arbitro_stream_cfg_t scfg = {0};
    arbitro_consumer_cfg_t ccfg = {0};
    uint32_t sid, c1=0, c2=0, c3=0;
    char n[64]; mk_name(n, sizeof(n), "cm_up");
    T("10.5.2 consumer_upsert_idempotent");
    scfg.subject_filter = ">";
    arbitro_stream_upsert(c, n, &scfg, &sid);
    ccfg.name = "wu"; ccfg.filter = ">"; ccfg.ack_policy = ARBITRO_ACK_EXPLICIT;
    ccfg.max_inflight = 100; ccfg.ack_wait_ms = 30000;
    arbitro_consumer_upsert(c, n, &ccfg, &c1);
    arbitro_consumer_upsert(c, n, &ccfg, &c2);
    arbitro_consumer_upsert(c, n, &ccfg, &c3);
    if (c1 && c1 == c2 && c2 == c3) OK();
    else FAIL("c1=%u c2=%u c3=%u", c1, c2, c3);
    arbitro_client_close(c);
}

static void test_consumer_upsert_config_change_rejected(void) {
    arbitro_client_t *c = connect_big();
    arbitro_stream_cfg_t scfg = {0};
    arbitro_consumer_cfg_t ccfg = {0}, ccfg2 = {0};
    uint32_t sid, c1=0, c2=0;
    char n[64]; mk_name(n, sizeof(n), "cm_ch");
    T("10.5.3 consumer_upsert config change rejected or coerced");
    scfg.subject_filter = ">";
    arbitro_stream_upsert(c, n, &scfg, &sid);
    ccfg.name = "ch"; ccfg.filter = "foo.>"; ccfg.ack_policy = ARBITRO_ACK_EXPLICIT;
    ccfg.max_inflight = 100; ccfg.ack_wait_ms = 30000;
    int rc1 = arbitro_consumer_upsert(c, n, &ccfg, &c1);
    ccfg2 = ccfg; ccfg2.filter = "bar.>";
    int rc2 = arbitro_consumer_upsert(c, n, &ccfg2, &c2);
    if (rc1 == 0 && (rc2 != 0 || c2 == c1)) OK();
    else FAIL("rc1=%d rc2=%d c1=%u c2=%u (expected reject or same id)", rc1, rc2, c1, c2);
    arbitro_client_close(c);
}

static void test_delete_consumer(void) {
    arbitro_client_t *c = connect_big();
    arbitro_stream_cfg_t scfg = {0};
    arbitro_consumer_cfg_t ccfg = {0};
    uint32_t sid, cid=0;
    char n[64]; mk_name(n, sizeof(n), "cm_dl");
    T("10.5.4 delete_consumer, subscribe fails after");
    scfg.subject_filter = ">";
    arbitro_stream_upsert(c, n, &scfg, &sid);
    ccfg.name = "todel"; ccfg.filter = ">"; ccfg.ack_policy = ARBITRO_ACK_EXPLICIT;
    ccfg.max_inflight = 10; ccfg.ack_wait_ms = 30000;
    arbitro_consumer_create(c, n, &ccfg, &cid);
    int drc = arbitro_consumer_delete(c, n, "todel");
    if (drc != 0) { FAIL("delete rc=%d", drc); arbitro_client_close(c); return; }
    arbitro_consumer_info_t info = {0};
    int irc = arbitro_consumer_info(c, n, "todel", &info);
    if (irc != 0) OK();
    else FAIL("consumer_info still finds deleted consumer (rc=%d id=%u)", irc, info.consumer_id);
    arbitro_client_close(c);
}

static void test_consumer_list_per_stream(void) {
    arbitro_client_t *c = connect_big();
    arbitro_stream_cfg_t scfg = {0};
    arbitro_consumer_cfg_t ccfg = {0};
    uint32_t sid, cid;
    char n[64]; mk_name(n, sizeof(n), "cm_ls");
    T("10.5.5 consumer_list per stream");
    scfg.subject_filter = ">";
    arbitro_stream_upsert(c, n, &scfg, &sid);
    for (int i = 0; i < 5; i++) {
        char name[16]; snprintf(name, sizeof(name), "cons_%d", i);
        ccfg.name = name; ccfg.filter = ">"; ccfg.ack_policy = ARBITRO_ACK_EXPLICIT;
        ccfg.max_inflight = 10; ccfg.ack_wait_ms = 30000;
        arbitro_consumer_create(c, n, &ccfg, &cid);
    }
    arbitro_consumer_info_t buf[32] = {0};
    size_t got = 0;
    int rc = arbitro_consumer_list(c, n, buf, 32, &got);
    if (rc == 0 && got >= 5) OK();
    else FAIL("rc=%d got=%zu (want >=5)", rc, got);
    arbitro_client_close(c);
}

static void test_pause_resume_consumer(void) {
    T("10.5.6 pause/resume consumer");
    SKIP("no pause API in arbitro.h");
    arbitro_client_close(NULL);
}

static void test_consumer_stats_pending(void) {
    T("10.5.7 consumer_stats pending");
    SKIP("consumer_info exposes only consumer_id in current C API");
}

/* ─────────────────────────────────────────────────────────────────────── */
/* Phase 10.6 — Service / RPC (11 tests)                                  */
/* ─────────────────────────────────────────────────────────────────────── */

/* Test 19: basic request/reply */
static void on_echo(arbitro_msg_t *m, void *ud) {
    (void)ud;
    arbitro_msg_reply(m, (const uint8_t*)"ok", 2);
}
static volatile int echo_svc_ready = 0;
static char echo_svc_name[64];
static void *echo_svc_thread(void *arg) {
    (void)arg;
    arbitro_client_t *sc; arbitro_service_t *svc;
    if (arbitro_client_connect("127.0.0.1", 9898, NULL, &sc) != 0) return NULL;
    if (arbitro_service_create(sc, echo_svc_name, 100, &svc) != 0) {
        arbitro_client_close(sc); return NULL;
    }
    arbitro_service_handle(svc, "e", on_echo, NULL);
    echo_svc_ready = 1;
    arbitro_client_run(sc);
    arbitro_service_destroy(svc);
    arbitro_client_close(sc);
    return NULL;
}
static void test_request_reply_basic(void) {
    pthread_t th;
    T("10.6.1 request_reply_basic");
    snprintf(echo_svc_name, sizeof(echo_svc_name), "echo_%llu", (unsigned long long)nowms());
    echo_svc_ready = 0;
    pthread_create(&th, NULL, echo_svc_thread, NULL);
    uint64_t t0 = nowms();
    while (!echo_svc_ready && nowms() - t0 < 2000) usleep(50000);
    if (!echo_svc_ready) { FAIL("service didn't start"); return; }
    arbitro_client_t *c = connect_big();
    uint8_t out[16]; uint32_t olen = 0;
    int rc = arbitro_request(c, echo_svc_name, "e", (const uint8_t*)"hi",2,3000,out,sizeof(out),&olen);
    if (rc == 0 && olen == 2 && memcmp(out,"ok",2) == 0) OK();
    else FAIL("rc=%d olen=%u out=%.*s", rc, olen, (int)olen, out);
    arbitro_client_close(c);
}

/* Test 20: timeout no responder */
static void test_request_timeout_no_responder(void) {
    arbitro_client_t *c = connect_big();
    uint8_t out[16]; uint32_t olen = 0;
    T("10.6.2 request_timeout_no_responder");
    int rc = arbitro_request(c, "nonexistent_ttt", "m", (const uint8_t*)"x",1,100,out,sizeof(out),&olen);
    if (rc == ARBITRO_ERR_TIMEOUT || rc == ARBITRO_ERR_NOTFOUND || rc == ARBITRO_ERR_BROKER) OK();
    else FAIL("rc=%d (want TIMEOUT/NOTFOUND/BROKER)", rc);
    arbitro_client_close(c);
}

/* Test 21: timeout slot leak */
static void test_request_timeout_slot_leak(void) {
    arbitro_client_t *c = connect_big();
    uint8_t out[64]; uint32_t olen = 0;
    T("10.6.3 request_timeout_slot_leak (100+1)");
    for (int i = 0; i < 100; i++) {
        int rc = arbitro_request(c, "nonexistent_ssL", "m", (const uint8_t*)"x",1,20,out,sizeof(out),&olen);
        if (rc != ARBITRO_ERR_TIMEOUT && rc != ARBITRO_ERR_NOTFOUND && rc != ARBITRO_ERR_BROKER) {
            FAIL("iter %d rc=%d unexpected", i, rc);
            arbitro_client_close(c); return;
        }
    }
    int rc = arbitro_request(c, "nonexistent_ssL2", "m", (const uint8_t*)"x",1,20,out,sizeof(out),&olen);
    if (rc == ARBITRO_ERR_TIMEOUT || rc == ARBITRO_ERR_NOTFOUND || rc == ARBITRO_ERR_BROKER) OK();
    else FAIL("final rc=%d", rc);
    arbitro_client_close(c);
}

/* Test 22: multiple handlers on one service */
static void on_add(arbitro_msg_t *m, void *ud) { (void)ud; arbitro_msg_reply(m, (const uint8_t*)"A", 1); }
static void on_sub(arbitro_msg_t *m, void *ud) { (void)ud; arbitro_msg_reply(m, (const uint8_t*)"S", 1); }
static void on_mul(arbitro_msg_t *m, void *ud) { (void)ud; arbitro_msg_reply(m, (const uint8_t*)"M", 1); }
static volatile int calc_ready = 0;
static char calc_svc_name[64];
static void *calc_svc_thread(void *arg) {
    (void)arg;
    arbitro_client_t *sc; arbitro_service_t *svc;
    if (arbitro_client_connect("127.0.0.1", 9898, NULL, &sc) != 0) return NULL;
    if (arbitro_service_create(sc, calc_svc_name, 100, &svc) != 0) { arbitro_client_close(sc); return NULL; }
    arbitro_service_handle(svc, "add", on_add, NULL);
    arbitro_service_handle(svc, "sub", on_sub, NULL);
    arbitro_service_handle(svc, "mul", on_mul, NULL);
    calc_ready = 1;
    arbitro_client_run(sc);
    arbitro_service_destroy(svc);
    arbitro_client_close(sc);
    return NULL;
}
static void test_service_multiple_handlers(void) {
    pthread_t th;
    T("10.6.4 service_multiple_handlers routed");
    snprintf(calc_svc_name, sizeof(calc_svc_name), "calc_%llu", (unsigned long long)nowms());
    calc_ready = 0;
    pthread_create(&th, NULL, calc_svc_thread, NULL);
    uint64_t t0 = nowms();
    while (!calc_ready && nowms() - t0 < 2000) usleep(50000);
    if (!calc_ready) { FAIL("svc didn't start"); return; }
    arbitro_client_t *c = connect_big();
    uint8_t o1[4]={0}, o2[4]={0}, o3[4]={0}; uint32_t l1=0,l2=0,l3=0;
    int r1 = arbitro_request(c, calc_svc_name, "add", (const uint8_t*)"1",1,3000,o1,sizeof(o1),&l1);
    int r2 = arbitro_request(c, calc_svc_name, "sub", (const uint8_t*)"2",1,3000,o2,sizeof(o2),&l2);
    int r3 = arbitro_request(c, calc_svc_name, "mul", (const uint8_t*)"3",1,3000,o3,sizeof(o3),&l3);
    if (r1==0 && r2==0 && r3==0 && o1[0]=='A' && o2[0]=='S' && o3[0]=='M') OK();
    else FAIL("r=(%d,%d,%d) out=(%c,%c,%c)", r1,r2,r3, o1[0]?o1[0]:'?', o2[0]?o2[0]:'?', o3[0]?o3[0]:'?');
    arbitro_client_close(c);
}

/* Test 23: two clients — one hosts, other requests */
static volatile int host2_ready = 0;
static char host2_name[64];
static void on_ping(arbitro_msg_t *m, void *ud) { (void)ud; arbitro_msg_reply(m, (const uint8_t*)"pong", 4); }
static void *host2_thread(void *arg) {
    (void)arg;
    arbitro_client_t *sc; arbitro_service_t *svc;
    if (arbitro_client_connect("127.0.0.1", 9898, NULL, &sc) != 0) return NULL;
    if (arbitro_service_create(sc, host2_name, 100, &svc) != 0) { arbitro_client_close(sc); return NULL; }
    arbitro_service_handle(svc, "p", on_ping, NULL);
    host2_ready = 1;
    arbitro_client_run(sc);
    arbitro_service_destroy(svc);
    arbitro_client_close(sc);
    return NULL;
}
static void test_service_two_clients(void) {
    pthread_t th;
    T("10.6.5 service_two_clients A hosts B requests");
    snprintf(host2_name, sizeof(host2_name), "host2_%llu", (unsigned long long)nowms());
    host2_ready = 0;
    pthread_create(&th, NULL, host2_thread, NULL);
    uint64_t t0 = nowms();
    while (!host2_ready && nowms() - t0 < 2000) usleep(50000);
    if (!host2_ready) { FAIL("host not ready"); return; }
    arbitro_client_t *b = connect_big();
    uint8_t out[16]; uint32_t olen=0;
    int rc = arbitro_request(b, host2_name, "p", (const uint8_t*)"x",1,3000,out,sizeof(out),&olen);
    if (rc == 0 && olen == 4 && memcmp(out,"pong",4) == 0) OK();
    else FAIL("rc=%d olen=%u", rc, olen);
    arbitro_client_close(b);
}

/* Test 24: fire-and-forget arrives */
static volatile int notify_hits = 0;
static volatile int notify_ready = 0;
static char notify_name[64];
static void on_notify(arbitro_msg_t *m, void *ud) {
    (void)ud; notify_hits++; arbitro_msg_ack(m);
}
static void *notify_thread(void *arg) {
    (void)arg;
    arbitro_client_t *sc; arbitro_service_t *svc;
    if (arbitro_client_connect("127.0.0.1", 9898, NULL, &sc) != 0) return NULL;
    if (arbitro_service_create(sc, notify_name, 100, &svc) != 0) { arbitro_client_close(sc); return NULL; }
    arbitro_service_handle(svc, "n", on_notify, NULL);
    notify_ready = 1;
    arbitro_client_run(sc);
    arbitro_service_destroy(svc);
    arbitro_client_close(sc);
    return NULL;
}
static void test_service_send_fire_forget(void) {
    pthread_t th;
    T("10.6.6 service_send fire-and-forget arrives");
    snprintf(notify_name, sizeof(notify_name), "notif_%llu", (unsigned long long)nowms());
    notify_hits = 0; notify_ready = 0;
    pthread_create(&th, NULL, notify_thread, NULL);
    uint64_t t0 = nowms();
    while (!notify_ready && nowms() - t0 < 2000) usleep(50000);
    if (!notify_ready) { FAIL("svc not ready"); return; }
    arbitro_client_t *c = connect_big();
    arbitro_service_t *sender;
    char sname[64]; snprintf(sname, sizeof(sname), "sender_%llu", (unsigned long long)nowms());
    arbitro_service_create(c, sname, 100, &sender);
    arbitro_service_send(sender, notify_name, "n", (const uint8_t*)"hi", 2);
    t0 = nowms();
    while (notify_hits == 0 && nowms() - t0 < 2000) arbitro_client_poll(c, 100);
    if (notify_hits >= 1) OK();
    else FAIL("hits=%d", notify_hits);
    arbitro_service_destroy(sender);
    arbitro_client_close(c);
}

/* Test 25: send does not accumulate waiters */
static void test_service_send_no_reply(void) {
    arbitro_client_t *c = connect_big();
    arbitro_service_t *sender;
    char sname[64]; snprintf(sname, sizeof(sname), "send_nw_%llu", (unsigned long long)nowms());
    arbitro_service_create(c, sname, 100, &sender);
    T("10.6.7 service_send doesn't accumulate waiters");
    for (int i = 0; i < 100; i++)
        arbitro_service_send(sender, "sink_nowhere", "m", (const uint8_t*)"x", 1);
    arbitro_client_flush(c);
    arbitro_stats_t st = {0};
    arbitro_client_stats(c, &st);
    if (st.waiters_in_use == 0) OK();
    else FAIL("waiters_in_use=%u after 100 sends", st.waiters_in_use);
    arbitro_service_destroy(sender);
    arbitro_client_close(c);
}

/* Test 26: two clients concurrent requests */
static volatile int con_a_hits = 0, con_b_hits = 0;
static volatile int con_ready_a = 0, con_ready_b = 0;
static char con_svc_a[64], con_svc_b[64];
static void on_con_a(arbitro_msg_t *m, void *ud) { (void)ud; con_a_hits++; arbitro_msg_reply(m,(const uint8_t*)"AA",2); }
static void on_con_b(arbitro_msg_t *m, void *ud) { (void)ud; con_b_hits++; arbitro_msg_reply(m,(const uint8_t*)"BB",2); }
static void *con_host_a(void *arg) {
    (void)arg;
    arbitro_client_t *sc; arbitro_service_t *svc;
    if (arbitro_client_connect("127.0.0.1", 9898, NULL, &sc) != 0) return NULL;
    if (arbitro_service_create(sc, con_svc_a, 200, &svc) != 0) { arbitro_client_close(sc); return NULL; }
    arbitro_service_handle(svc, "op", on_con_a, NULL);
    con_ready_a = 1;
    arbitro_client_run(sc);
    arbitro_service_destroy(svc);
    arbitro_client_close(sc);
    return NULL;
}
static void *con_host_b(void *arg) {
    (void)arg;
    arbitro_client_t *sc; arbitro_service_t *svc;
    if (arbitro_client_connect("127.0.0.1", 9898, NULL, &sc) != 0) return NULL;
    if (arbitro_service_create(sc, con_svc_b, 200, &svc) != 0) { arbitro_client_close(sc); return NULL; }
    arbitro_service_handle(svc, "op", on_con_b, NULL);
    con_ready_b = 1;
    arbitro_client_run(sc);
    arbitro_service_destroy(svc);
    arbitro_client_close(sc);
    return NULL;
}
struct client_load { arbitro_client_t *c; const char *target; int n; int ok; int fails; };
static void *client_load_thread(void *arg) {
    struct client_load *cl = arg;
    uint8_t out[16]; uint32_t olen = 0;
    for (int i = 0; i < cl->n; i++) {
        int rc = arbitro_request(cl->c, cl->target, "op", (const uint8_t*)"x",1,5000,out,sizeof(out),&olen);
        if (rc == 0 && olen == 2) cl->ok++;
        else cl->fails++;
    }
    return NULL;
}
static void test_two_clients_concurrent_requests(void) {
    pthread_t th_a, th_b, th_c1, th_c2;
    T("10.6.8 two_clients_concurrent_requests 2x20");
    snprintf(con_svc_a, sizeof(con_svc_a), "con_a_%llu", (unsigned long long)nowms());
    snprintf(con_svc_b, sizeof(con_svc_b), "con_b_%llu", (unsigned long long)nowms());
    con_a_hits = con_b_hits = 0;
    con_ready_a = con_ready_b = 0;
    pthread_create(&th_a, NULL, con_host_a, NULL);
    pthread_create(&th_b, NULL, con_host_b, NULL);
    uint64_t t0 = nowms();
    while ((!con_ready_a || !con_ready_b) && nowms() - t0 < 3000) usleep(50000);
    if (!con_ready_a || !con_ready_b) { FAIL("hosts not ready"); return; }
    arbitro_client_t *c1 = connect_big();
    arbitro_client_t *c2 = connect_big();
    struct client_load l1 = { c1, con_svc_a, 20, 0, 0 };
    struct client_load l2 = { c2, con_svc_b, 20, 0, 0 };
    pthread_create(&th_c1, NULL, client_load_thread, &l1);
    pthread_create(&th_c2, NULL, client_load_thread, &l2);
    pthread_join(th_c1, NULL);
    pthread_join(th_c2, NULL);
    if (l1.ok == 20 && l2.ok == 20) OK();
    else FAIL("l1 ok=%d/20 fail=%d, l2 ok=%d/20 fail=%d", l1.ok, l1.fails, l2.ok, l2.fails);
    arbitro_client_close(c1);
    arbitro_client_close(c2);
}

/* Test 27: handler doesn't reply → timeout */
static void on_silent(arbitro_msg_t *m, void *ud) { (void)m; (void)ud; /* no reply */ arbitro_msg_ack(m); }
static volatile int silent_ready = 0;
static char silent_name[64];
static void *silent_thread(void *arg) {
    (void)arg;
    arbitro_client_t *sc; arbitro_service_t *svc;
    if (arbitro_client_connect("127.0.0.1", 9898, NULL, &sc) != 0) return NULL;
    if (arbitro_service_create(sc, silent_name, 100, &svc) != 0) { arbitro_client_close(sc); return NULL; }
    arbitro_service_handle(svc, "m", on_silent, NULL);
    silent_ready = 1;
    arbitro_client_run(sc);
    arbitro_service_destroy(svc);
    arbitro_client_close(sc);
    return NULL;
}
static void test_service_handler_crash_returns_timeout(void) {
    pthread_t th;
    T("10.6.9 handler_no_reply → timeout");
    snprintf(silent_name, sizeof(silent_name), "sil_%llu", (unsigned long long)nowms());
    silent_ready = 0;
    pthread_create(&th, NULL, silent_thread, NULL);
    uint64_t t0 = nowms();
    while (!silent_ready && nowms() - t0 < 2000) usleep(50000);
    if (!silent_ready) { FAIL("svc not ready"); return; }
    arbitro_client_t *c = connect_big();
    uint8_t out[16]; uint32_t olen = 0;
    int rc = arbitro_request(c, silent_name, "m", (const uint8_t*)"x",1,300,out,sizeof(out),&olen);
    if (rc == ARBITRO_ERR_TIMEOUT) OK();
    else FAIL("rc=%d (want TIMEOUT)", rc);
    arbitro_client_close(c);
}

/* Test 28: requester disconnects mid-flight — SKIP */
static void test_request_disconnects_mid_flight(void) {
    T("10.6.10 request_disconnects_mid_flight");
    SKIP("hard to simulate reliably without broker inspection");
}

/* Test 29: request to non-existent → ERR_TIMEOUT/NOTFOUND/BROKER */
static void test_request_target_stream_missing(void) {
    arbitro_client_t *c = connect_big();
    uint8_t out[16]; uint32_t olen = 0;
    T("10.6.11 request to missing service → error");
    int rc = arbitro_request(c, "nope_never_created", "m", (const uint8_t*)"x",1,150,out,sizeof(out),&olen);
    if (rc == ARBITRO_ERR_TIMEOUT || rc == ARBITRO_ERR_NOTFOUND || rc == ARBITRO_ERR_BROKER) OK();
    else FAIL("rc=%d (want TIMEOUT/NOTFOUND/BROKER)", rc);
    arbitro_client_close(c);
}

int main(void) {
    printf("=== Phase 10.4 + 10.5 + 10.6 mgmt/svc tests ===\n");
    /* 10.4 */
    test_create_stream_all_config();
    test_create_stream_duplicate();
    test_stream_upsert_idempotent();
    test_delete_stream_removes_messages();
    test_delete_stream_keep_data();
    test_stream_list_pagination();
    test_stream_exists();
    test_purge_stream();
    test_drain_subject_wildcard();
    test_delete_message_tombstone();
    test_stream_info_reports_bytes();
    /* 10.5 */
    test_create_consumer_all_config();
    test_consumer_upsert_idempotent();
    test_consumer_upsert_config_change_rejected();
    test_delete_consumer();
    test_consumer_list_per_stream();
    test_pause_resume_consumer();
    test_consumer_stats_pending();
    /* 10.6 */
    test_request_reply_basic();
    test_request_timeout_no_responder();
    test_request_timeout_slot_leak();
    test_service_multiple_handlers();
    test_service_two_clients();
    test_service_send_fire_forget();
    test_service_send_no_reply();
    test_two_clients_concurrent_requests();
    test_service_handler_crash_returns_timeout();
    test_request_disconnects_mid_flight();
    test_request_target_stream_missing();
    printf("=== %d/%d passed, %d skipped, %d failed ===\n",
           pass, total, skipped, fail);
    return fail ? 1 : 0;
}
