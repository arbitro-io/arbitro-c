#ifndef _WIN32
#define _POSIX_C_SOURCE 200112L
#endif

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include "arbitro/arbitro.h"
#include "test_addr.h"

/* The C twin of the Rust replay bench (arbitro-e2e/benches/throughput.rs,
   BENCH_MODE=replay), of tests/34-replay-backlog.test.ts and of
   replay_backlog_test.go.

   That bench caught a tail of publishes the Rust client accepted and never
   sent: the store was short by exactly one batch and the replay consumer
   waited forever. The shape that exposes it is prefill-then-drain — a live
   consumer hides a missing tail behind a slow drain. */

#define STREAMS    4
#define BATCHES    40
#define PER_BATCH  256
#define PER_STREAM (BATCHES * PER_BATCH)

static int total = 0, pass = 0, fail = 0;
#define T(name) do { total++; printf("[%d] %s ... ", total, name); fflush(stdout); } while(0)
#define OK() do { pass++; printf("PASS\n"); } while(0)
#define FAILF(fmt, ...) do { fail++; printf("FAIL — " fmt "\n", ##__VA_ARGS__); } while(0)

static uint64_t nowms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

/* One bit per sequence, so a duplicate cannot cover for a loss the way a
   plain counter would. */
static uint8_t seen[STREAMS][PER_STREAM];
static int seen_count[STREAMS];
static int active_stream;

static void collect_cb(arbitro_msg_t *m, void *ud) {
    (void)ud;
    if (m->data_len >= 4) {
        uint32_t idx;
        memcpy(&idx, m->data, 4);
        if (idx < PER_STREAM && !seen[active_stream][idx]) {
            seen[active_stream][idx] = 1;
            seen_count[active_stream]++;
        }
    }
    arbitro_msg_ack(m);
}

static arbitro_client_t *dial(void) {
    arbitro_client_t *c = NULL;
    arbitro_opts_t o;
    arbitro_opts_init(&o);
    o.frame_buf_size = 2 * 1024 * 1024;
    if (arbitro_client_connect(arb_test_addr(), arb_test_port(), &o, &c) != ARBITRO_OK)
        return NULL;
    return c;
}

int main(void) {
    char names[STREAMS][64];
    uint32_t sids[STREAMS];
    uint64_t stamp = nowms();
    int s, b, i, rc;

    /* The cleanup at `done:` reads names[s][0] even when we jump there before
       filling them. */
    memset(names, 0, sizeof(names));

    arbitro_client_t *admin = dial();
    if (!admin) {
        printf("SKIP — no broker at %s:%d\n", arb_test_addr(), arb_test_port());
        return 0;
    }

    T("prefill: create streams");
    for (s = 0; s < STREAMS; s++) {
        arbitro_stream_cfg_t cfg;
        char filter[160];
        memset(&cfg, 0, sizeof(cfg));
        snprintf(names[s], sizeof(names[s]), "creplay_%llu_%d",
                 (unsigned long long)stamp, s);
        snprintf(filter, sizeof(filter), "%.60s.>", names[s]);
        cfg.subject_filter = filter;
        cfg.journal = 0;
        rc = arbitro_stream_create(admin, names[s], &cfg, &sids[s]);
        if (rc != ARBITRO_OK) {
            FAILF("create %s: %s", names[s], arbitro_err_str(rc));
            goto done;
        }
    }
    OK();

    T("prefill: every batch is accepted");
    {
        static uint8_t payloads[PER_BATCH][4];
        static arbitro_batch_entry_t entries[PER_BATCH];
        char subject[160];
        for (s = 0; s < STREAMS; s++) {
            snprintf(subject, sizeof(subject), "%.60s.evt", names[s]);
            for (b = 0; b < BATCHES; b++) {
                for (i = 0; i < PER_BATCH; i++) {
                    uint32_t v = (uint32_t)(b * PER_BATCH + i);
                    memcpy(payloads[i], &v, 4);
                    entries[i].subject     = (const uint8_t *)subject;
                    entries[i].subject_len = (uint16_t)strlen(subject);
                    entries[i].msg_id      = NULL;
                    entries[i].msg_id_len  = 0;
                    entries[i].payload     = payloads[i];
                    entries[i].payload_len = 4;
                }
                rc = arbitro_publish_batch(admin, sids[s], entries, PER_BATCH, NULL);
                if (rc != ARBITRO_OK) {
                    FAILF("%s batch %d: %s", names[s], b, arbitro_err_str(rc));
                    goto done;
                }
            }
        }
    }
    OK();

    /* Everything is published and confirmed before a single consumer exists. */
    T("replay: every message comes back, none missing");
    for (s = 0; s < STREAMS; s++) {
        arbitro_consumer_cfg_t ccfg;
        char filter[160];
        uint32_t cid = 0;
        uint64_t deadline;
        arbitro_client_t *reader = dial();
        if (!reader) { FAILF("reader connect"); goto done; }

        memset(&ccfg, 0, sizeof(ccfg));
        snprintf(filter, sizeof(filter), "%.60s.>", names[s]);
        ccfg.name           = names[s];
        ccfg.filter         = filter;
        ccfg.ack_policy     = 1;   /* explicit */
        ccfg.max_inflight   = 4096;
        ccfg.ack_wait_ms    = 30000;
        ccfg.deliver_policy = 0;   /* All — replay from the start */

        rc = arbitro_consumer_create(reader, names[s], &ccfg, &cid);
        if (rc != ARBITRO_OK) {
            FAILF("consumer %s: %s", names[s], arbitro_err_str(rc));
            arbitro_client_close(reader);
            goto done;
        }

        active_stream = s;
        rc = arbitro_subscribe(reader, sids[s], cid, collect_cb, NULL);
        if (rc != ARBITRO_OK) {
            FAILF("subscribe %s: %s", names[s], arbitro_err_str(rc));
            arbitro_client_close(reader);
            goto done;
        }

        deadline = nowms() + 60000;
        while (seen_count[s] < PER_STREAM && nowms() < deadline)
            arbitro_client_poll(reader, 200);

        arbitro_client_close(reader);

        if (seen_count[s] != PER_STREAM) {
            FAILF("%s: drained %d/%d, missing %d",
                  names[s], seen_count[s], PER_STREAM, PER_STREAM - seen_count[s]);
            goto done;
        }
        for (i = 0; i < PER_STREAM; i++) {
            if (!seen[s][i]) {
                FAILF("%s: sequence %d never arrived", names[s], i);
                goto done;
            }
        }
    }
    OK();

done:
    for (s = 0; s < STREAMS; s++)
        if (names[s][0]) arbitro_stream_delete(admin, names[s], 0);
    arbitro_client_close(admin);

    printf("\n%d/%d passed, %d failed\n", pass, total, fail);
    return fail ? 1 : 0;
}
