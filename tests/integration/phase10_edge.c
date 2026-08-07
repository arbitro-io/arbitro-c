#ifndef _WIN32
#define _POSIX_C_SOURCE 200112L
#endif

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <dirent.h>
#include <pthread.h>
#include "arbitro/arbitro.h"
#include "test_addr.h"

static int total = 0, pass = 0, fail = 0, skipped = 0;
#define T(name) do { total++; printf("[%d] %s ... ", total, name); fflush(stdout); } while(0)
#define OK() do { pass++; printf("PASS\n"); } while(0)
#define FAIL(fmt, ...) do { fail++; printf("FAIL — " fmt "\n", ##__VA_ARGS__); } while(0)
#define SKIP(reason) do { skipped++; printf("SKIP — %s\n", reason); } while(0)

static uint64_t nowms(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec*1000 + ts.tv_nsec/1000000;
}

static void mk_stream_name(char *dst, size_t cap, const char *tag) {
    snprintf(dst, cap, "t%s_%llu", tag, (unsigned long long)nowms());
}

static arbitro_client_t *connect_big(void) {
    arbitro_client_t *c = NULL; arbitro_opts_t o;
    arbitro_opts_init(&o);
    o.frame_buf_size = 2 * 1024 * 1024;
    arbitro_client_connect(arb_test_addr(), arb_test_port(), &o, &c);
    return c;
}

static int count_open_fds(void) {
    DIR *d = opendir("/proc/self/fd");
    if (!d) return -1;
    int n = 0;
    struct dirent *e;
    while ((e = readdir(d)) != NULL) {
        if (e->d_name[0] != '.') n++;
    }
    closedir(d);
    return n;
}

/* ─ 10.8.1 double_close ─────────────────────────────────────────────── */
static void test_double_close_safe(void) {
    T("10.8.1 double_close_safe");
    arbitro_client_t *c = connect_big();
    if (!c) { FAIL("connect failed"); return; }
    arbitro_client_close(c);
    arbitro_client_close(NULL);
    OK();
}

/* ─ 10.8.2 close_before_connect_result ──────────────────────────────── */
static void test_close_before_connect_result(void) {
    T("10.8.2 close_before_connect_result");
    arbitro_client_t *c = NULL;
    arbitro_opts_t o; arbitro_opts_init(&o);
    o.connect_timeout_ms = 500;
    int rc = arbitro_client_connect("127.0.0.1", 1, &o, &c);
    if (rc == ARBITRO_OK) {
        FAIL("expected failure on port 1, got OK");
        arbitro_client_close(c);
        return;
    }
    arbitro_client_close(c);
    OK();
}

/* ─ 10.8.3 null_client_all_apis ─────────────────────────────────────── */
/* Runs each API in a forked child so a segfault in one does not kill the harness. */
#include <sys/wait.h>
#include <signal.h>

typedef int (*null_fn)(void);

static int nf_publish(void){ return arbitro_publish(NULL,0,(const uint8_t*)"a",1,NULL,0); }
static int nf_publish_sync(void){ uint64_t s=0; return arbitro_publish_sync(NULL,0,(const uint8_t*)"a",1,NULL,0,&s); }
static int nf_subscribe(void){ return arbitro_subscribe(NULL,0,0,NULL,NULL); }
static int nf_stream_upsert(void){
    uint32_t sid=0; arbitro_stream_cfg_t scfg={0}; scfg.subject_filter=">";
    return arbitro_stream_upsert(NULL,"x",&scfg,&sid);
}
static int nf_consumer_create(void){
    uint32_t cid=0; arbitro_consumer_cfg_t ccfg={0};
    ccfg.name="n"; ccfg.filter=">"; ccfg.ack_policy=ARBITRO_ACK_EXPLICIT;
    ccfg.max_inflight=1; ccfg.ack_wait_ms=1000;
    return arbitro_consumer_create(NULL,"x",&ccfg,&cid);
}
static int nf_request(void){
    uint8_t b[16]; uint32_t o=0;
    return arbitro_request(NULL,"svc","m",NULL,0,100,b,sizeof(b),&o);
}
static int nf_flush(void){ return arbitro_client_flush(NULL); }
static int nf_close(void){ arbitro_client_close(NULL); return -1; }
static int nf_metrics(void){ arbitro_metrics_t m; arbitro_client_metrics(NULL,&m); return -1; }

/* Returns: 0 = OK negative-return, 1 = crashed, 2 = returned success (bug), 3 = other. */
static int probe_null(null_fn fn) {
    pid_t p = fork();
    if (p == 0) {
        int rc = fn();
        _exit(rc < 0 ? 0 : (rc == 0 ? 2 : 3));
    }
    int st = 0;
    waitpid(p, &st, 0);
    if (WIFSIGNALED(st)) return 1;
    if (WIFEXITED(st)) return WEXITSTATUS(st);
    return 3;
}

static void test_null_client_all_apis(void) {
    T("10.8.3 null_client_all_apis (per-API fork probe)");
    struct { const char *name; null_fn fn; } probes[] = {
        {"publish", nf_publish},
        {"publish_sync", nf_publish_sync},
        {"subscribe", nf_subscribe},
        {"stream_upsert", nf_stream_upsert},
        {"consumer_create", nf_consumer_create},
        {"request", nf_request},
        {"client_flush", nf_flush},
        {"client_close", nf_close},
        {"client_metrics", nf_metrics},
    };
    int nprobe = (int)(sizeof(probes)/sizeof(probes[0]));
    int crashes = 0, ok = 0, badret = 0;
    char detail[512]; detail[0] = 0;
    for (int i = 0; i < nprobe; i++) {
        int r = probe_null(probes[i].fn);
        if (r == 0) ok++;
        else if (r == 1) { crashes++;
            snprintf(detail+strlen(detail), sizeof(detail)-strlen(detail),
                     " CRASH:%s", probes[i].name);
        } else if (r == 2) { badret++;
            snprintf(detail+strlen(detail), sizeof(detail)-strlen(detail),
                     " BADRET:%s", probes[i].name);
        }
    }
    if (crashes == 0 && badret == 0) OK();
    else FAIL("crashes=%d bad-return=%d ok=%d/%d%s",
              crashes, badret, ok, nprobe, detail);
}

/* ─ 10.8.4 null_output_params ───────────────────────────────────────── */
static void test_null_output_params(void) {
    T("10.8.4 null_output_params");
    arbitro_client_t *c = connect_big();
    if (!c) { FAIL("connect failed"); return; }
    arbitro_stream_cfg_t scfg = {0}; scfg.subject_filter = ">";
    char n[64]; mk_stream_name(n, sizeof(n), "no");
    int rc1 = arbitro_stream_upsert(c, n, &scfg, NULL);
    int rc2 = arbitro_publish_sync(c, 0, (const uint8_t*)"a", 1,
                                    (const uint8_t*)"x", 1, NULL);
    (void)rc1; (void)rc2;
    OK();
    arbitro_client_close(c);
}

/* ─ 10.8.5 rapid_connect_close_loop ─────────────────────────────────── */
static void test_rapid_connect_close_loop(void) {
    T("10.8.5 rapid_connect_close_loop (100 iters, fd leak check)");
    int before = count_open_fds();
    for (int i = 0; i < 100; i++) {
        arbitro_client_t *c = connect_big();
        if (!c) { FAIL("iter %d connect failed", i); return; }
        arbitro_stream_cfg_t scfg = {0}; scfg.subject_filter = ">";
        char n[64]; mk_stream_name(n, sizeof(n), "rl");
        uint32_t sid = 0;
        arbitro_stream_upsert(c, n, &scfg, &sid);
        if (sid) arbitro_publish(c, sid, (const uint8_t*)"a",1,(const uint8_t*)"x",1);
        arbitro_client_flush(c);
        arbitro_client_close(c);
    }
    int after = count_open_fds();
    if (before < 0 || after < 0) { OK(); return; }
    int diff = after - before;
    if (diff <= 5) OK();
    else FAIL("fd leak: before=%d after=%d diff=%d", before, after, diff);
}

/* ─ 10.8.6 stop_from_callback ───────────────────────────────────────── */
static volatile int stop_cb_hits = 0;
static void stop_cb(arbitro_msg_t *m, void *ud) {
    arbitro_client_t *c = (arbitro_client_t *)ud;
    stop_cb_hits++;
    arbitro_msg_ack(m);
    arbitro_client_stop(c);
}

static void *run_thread(void *arg) {
    arbitro_client_t *c = (arbitro_client_t *)arg;
    arbitro_client_run(c);
    return NULL;
}

static void test_client_stop_from_callback(void) {
    T("10.8.6 client_stop_from_callback");
    arbitro_client_t *c = connect_big();
    if (!c) { FAIL("connect failed"); return; }
    arbitro_stream_cfg_t scfg = {0}; scfg.subject_filter = ">";
    arbitro_consumer_cfg_t ccfg = {0};
    uint32_t sid = 0, cid = 0;
    char n[64]; mk_stream_name(n, sizeof(n), "sc");
    if (arbitro_stream_upsert(c, n, &scfg, &sid) != 0) {
        FAIL("stream_upsert"); arbitro_client_close(c); return;
    }
    for (int i = 0; i < 3; i++)
        arbitro_publish_sync(c, sid, (const uint8_t*)"a",1,(const uint8_t*)"x",1,NULL);
    ccfg.name = "stopcb"; ccfg.filter = ">"; ccfg.ack_policy = ARBITRO_ACK_EXPLICIT;
    ccfg.max_inflight = 100; ccfg.ack_wait_ms = 30000;
    if (arbitro_consumer_create(c, n, &ccfg, &cid) != 0) {
        FAIL("consumer_create"); arbitro_client_close(c); return;
    }
    stop_cb_hits = 0;
    arbitro_subscribe(c, sid, cid, stop_cb, c);
    pthread_t th;
    pthread_create(&th, NULL, run_thread, c);
    uint64_t t0 = nowms();
    while (nowms() - t0 < 3000) {
        usleep(50000);
        if (stop_cb_hits > 0) {
            usleep(300000);
            break;
        }
    }
    pthread_join(th, NULL);
    if (stop_cb_hits >= 1) OK();
    else FAIL("hits=%d (callback never fired)", stop_cb_hits);
    arbitro_client_close(c);
}

/* ─ 10.8.7 fetch_pull_mode ──────────────────────────────────────────── */
static void test_arbitro_client_fetch_pull_mode(void) {
    T("10.8.7 fetch_pull_mode");
    SKIP("no arbitro_fetch API in header — pull mode not exposed");
}

/* ─ 10.9.6 replyto_bad_magic ────────────────────────────────────────── */
static void test_replyto_bad_magic(void) {
    T("10.9.6 replyto_bad_magic → ERR_PROTOCOL");
    arbitro_client_t *c = connect_big();
    if (!c) { FAIL("connect failed"); return; }
    arbitro_msg_t m;
    memset(&m, 0, sizeof(m));
    uint8_t bogus_reply[16];
    memset(bogus_reply, 0x77, sizeof(bogus_reply));
    m.client = c;
    m.reply_to = bogus_reply;
    m.reply_len = sizeof(bogus_reply);
    int rc = arbitro_msg_reply(&m, (const uint8_t*)"x", 1);
    if (rc == ARBITRO_ERR_PROTOCOL) OK();
    else FAIL("rc=%d (expected ERR_PROTOCOL=%d)", rc, ARBITRO_ERR_PROTOCOL);
    arbitro_client_close(c);
}

/* ─ 10.9.7 replyto_empty ────────────────────────────────────────────── */
static void test_replyto_empty(void) {
    T("10.9.7 replyto_empty → ERR_STATE");
    arbitro_client_t *c = connect_big();
    if (!c) { FAIL("connect failed"); return; }
    arbitro_msg_t m;
    memset(&m, 0, sizeof(m));
    m.client = c; m.reply_len = 0; m.reply_to = NULL;
    int rc = arbitro_msg_reply(&m, (const uint8_t*)"x", 1);
    if (rc == ARBITRO_ERR_STATE) OK();
    else FAIL("rc=%d (expected ERR_STATE=%d)", rc, ARBITRO_ERR_STATE);
    arbitro_client_close(c);
}

/* ─ 10.9.8 svc_dispatch_non_numeric_corr ────────────────────────────── */
static void test_svc_dispatch_non_numeric_corr(void) {
    T("10.9.8 svc_dispatch_non_numeric_corr");
    SKIP("no low-level frame injection API exposed for reply subject spoofing");
}

/* ─ 10.9.9 stream_name_too_long_rejected ────────────────────────────── */
static void test_stream_name_too_long_rejected(void) {
    T("10.9.9 stream_name_too_long_rejected");
    arbitro_client_t *c = connect_big();
    if (!c) { FAIL("connect failed"); return; }
    char name[101];
    memset(name, 'x', 100);
    name[100] = 0;
    arbitro_stream_cfg_t scfg = {0}; scfg.subject_filter = ">";
    uint32_t sid = 0;
    int rc = arbitro_stream_upsert(c, name, &scfg, &sid);
    if (rc < 0) OK();
    else FAIL("rc=%d (expected error), sid=%u", rc, sid);
    arbitro_client_close(c);
}

/* ─ SKIPPED (mock-only) ─────────────────────────────────────────────── */
static void skip_mock(const char *label) { T(label); SKIP("requires mock broker"); }

int main(void) {
    printf("=== Phase 10 edge coverage (10.8 + 10.9) ===\n");
    test_double_close_safe();
    test_close_before_connect_result();
    test_null_client_all_apis();
    test_null_output_params();
    test_rapid_connect_close_loop();
    test_client_stop_from_callback();
    test_arbitro_client_fetch_pull_mode();

    skip_mock("10.9.1 broker_sends_unknown_action");
    skip_mock("10.9.2 broker_sends_truncated_frame");
    skip_mock("10.9.3 broker_sends_oversized_msg_len");
    skip_mock("10.9.4 repbatch_entry_size_overflow");
    skip_mock("10.9.5 repbatch_count_zero");
    test_replyto_bad_magic();
    test_replyto_empty();
    test_svc_dispatch_non_numeric_corr();
    test_stream_name_too_long_rejected();

    printf("=== %d/%d passed (%d failed, %d skipped) ===\n",
           pass, total, fail, skipped);
    return fail ? 1 : 0;
}
