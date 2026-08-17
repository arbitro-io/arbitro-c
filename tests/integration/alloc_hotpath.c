#ifndef _WIN32
#define _POSIX_C_SOURCE 200112L
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include "arbitro/arbitro.h"
#include "test_addr.h"

/* Linked with -Wl,--wrap=malloc,--wrap=calloc,--wrap=realloc so every
   allocation the client makes is counted. The hot path (publish, deliver
   dispatch, ack) is documented as zero-alloc; this measures it instead of
   trusting the comment. */

static volatile unsigned long g_allocs = 0;
static int g_counting = 0;

void *__real_malloc(size_t);
void *__real_calloc(size_t, size_t);
void *__real_realloc(void *, size_t);

void *__wrap_malloc(size_t n) {
    if (g_counting) g_allocs++;
    return __real_malloc(n);
}
void *__wrap_calloc(size_t n, size_t s) {
    if (g_counting) g_allocs++;
    return __real_calloc(n, s);
}
void *__wrap_realloc(void *p, size_t n) {
    if (g_counting) g_allocs++;
    return __real_realloc(p, n);
}

static uint64_t nowms(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec*1000 + ts.tv_nsec/1000000;
}

#define N 2000

static volatile int hits = 0;
static void ack_cb(arbitro_msg_t *m, void *ud) {
    (void)ud; hits++; arbitro_msg_ack(m);
}

int main(void) {
    arbitro_client_t *c = NULL;
    arbitro_opts_t o;
    arbitro_stream_cfg_t scfg = {0};
    arbitro_consumer_cfg_t ccfg = {0};
    uint32_t sid, cid;
    char n[64];
    int i;

    arbitro_opts_init(&o);
    o.frame_buf_size = 2 * 1024 * 1024;
    if (arbitro_client_connect(arb_test_addr(), arb_test_port(), &o, &c) != 0) {
        printf("FAIL — connect\n");
        return 1;
    }
    snprintf(n, sizeof(n), "alloc_hp_%llu", (unsigned long long)nowms());
    scfg.subject_filter = arb_slice(n);
    arbitro_stream_upsert(c, n, &scfg, &sid);
    ccfg.name = "cHp"; ccfg.filter = arb_slice(n); ccfg.ack_policy = ARBITRO_ACK_EXPLICIT;
    ccfg.max_inflight = 60000; ccfg.ack_wait_ms = 30000;
    arbitro_consumer_create(c, n, &ccfg, &cid);
    arbitro_subscribe(c, sid, cid, ack_cb, NULL);

    const char *subj = arb_scoped(n, "hp.a");
    uint16_t subj_len = arb_slen(subj);

    /* Warm every lazy path (stream-id cache, pending slots) before counting. */
    for (i = 0; i < 10; i++)
        arbitro_publish(c, sid, (const uint8_t *)subj, subj_len,
                        (const uint8_t *)"xxxxxxxx", 8);
    arbitro_client_flush(c);
    {
        uint64_t t0 = nowms();
        while (hits < 10 && nowms() - t0 < 3000) arbitro_client_poll(c, 50);
    }
    arbitro_client_flush_acks(c);
    hits = 0;

    g_allocs = 0;
    g_counting = 1;

    for (i = 0; i < N; i++)
        arbitro_publish(c, sid, (const uint8_t *)subj, subj_len,
                        (const uint8_t *)"xxxxxxxx", 8);
    arbitro_client_flush(c);
    {
        uint64_t t0 = nowms();
        while (hits < N && nowms() - t0 < 15000) arbitro_client_poll(c, 50);
    }
    arbitro_client_flush_acks(c);

    g_counting = 0;

    printf("publish+deliver+ack iterations: %d, delivered: %d\n", N, hits);
    printf("hot-path allocations: %lu\n", g_allocs);
    arbitro_client_close(c);

    if (hits < N) {
        printf("FAIL — only %d of %d delivered\n", hits, N);
        return 1;
    }
    if (g_allocs != 0) {
        printf("FAIL — hot path allocated %lu times\n", g_allocs);
        return 1;
    }
    printf("PASS — zero allocations across %d round-trips\n", N);
    return 0;
}
