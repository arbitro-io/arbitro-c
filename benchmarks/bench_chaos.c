#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "arbitro/arbitro.h"

#ifdef _WIN32
#include <windows.h>
static double now_ms(void) {
    LARGE_INTEGER freq, cnt;
    QueryPerformanceFrequency(&freq);
    QueryPerformanceCounter(&cnt);
    return (double)cnt.QuadPart / (double)freq.QuadPart * 1000.0;
}
static void sleep_ms(uint32_t ms) { Sleep(ms); }
#else
#include <sys/time.h>
#include <time.h>
static double now_ms(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (double)tv.tv_sec * 1000.0 + (double)tv.tv_usec / 1000.0;
}
static void sleep_ms(uint32_t ms) {
    struct timespec ts;
    ts.tv_sec = ms / 1000;
    ts.tv_nsec = (long)(ms % 1000) * 1000000L;
    nanosleep(&ts, NULL);
}
#endif

#define MAX_ACKED  1000
#define MAX_RECV   2000
#define RUN_SECS   10
#define RATE_MS    20

static uint64_t acked_seqs[MAX_ACKED];
static int acked_count = 0;
static uint64_t recv_seqs[MAX_RECV];
static int recv_count = 0;
static int recv_dups = 0;
static int publish_errors = 0;
static int reconnects = 0;

static int seq_seen(uint64_t seq) {
    int i;
    for (i = 0; i < recv_count; i++) {
        if (recv_seqs[i] == seq) return 1;
    }
    return 0;
}

static void on_chaos_msg(arbitro_msg_t *msg, void *ud) {
    (void)ud;
    if (seq_seen(msg->seq)) {
        recv_dups++;
    } else if (recv_count < MAX_RECV) {
        recv_seqs[recv_count++] = msg->seq;
    }
    arbitro_msg_ack(msg);
}

static arbitro_client_t *chaos_connect(void) {
    arbitro_client_t *c = NULL;
    arbitro_opts_t opts;
    int rc, attempt;

    arbitro_opts_init(&opts);
    opts.connect_timeout_ms = 2000;
    opts.reconnect = 1;
    opts.reconnect_max = 5;
    opts.reconnect_delay_ms = 200;

    for (attempt = 0; attempt < 10; attempt++) {
        rc = arbitro_client_connect("127.0.0.1", ARBITRO_DEFAULT_PORT, &opts, &c);
        if (rc == ARBITRO_OK) return c;
        sleep_ms(500);
        reconnects++;
    }
    return NULL;
}

int main(void) {
    arbitro_client_t *c;
    uint32_t stream_id, consumer_id;
    arbitro_stream_cfg_t scfg = {0};
    arbitro_consumer_cfg_t ccfg = {0};
    int rc, i;
    double start, elapsed, t;
    uint8_t payload[32];
    int missing = 0;

    memset(payload, 'C', sizeof(payload));

    printf("bench_chaos: correctness under reconnect\n");
    printf("  Requires broker on 127.0.0.1:%d\n", ARBITRO_DEFAULT_PORT);
    printf("  Run duration: %d seconds\n", RUN_SECS);
    printf("  Rate: 1 msg / %d ms (~%d msg/s)\n", RATE_MS, 1000 / RATE_MS);
    printf("  Kill/restart the broker during the run to inject chaos.\n\n");

    c = chaos_connect();
    if (!c) {
        fprintf(stderr, "SKIP: cannot connect to broker\n");
        return 0;
    }

    scfg.subject_filter = "bench.chaos.>";
    scfg.journal = 1;
    rc = arbitro_stream_upsert(c, "bench-chaos", &scfg, &stream_id);
    if (rc != ARBITRO_OK) {
        fprintf(stderr, "stream: %s\n", arbitro_err_str(rc));
        return 1;
    }

    ccfg.name = "bench-chaos-w";
    ccfg.filter = "bench.chaos.>";
    ccfg.ack_policy = ARBITRO_ACK_EXPLICIT;
    ccfg.max_inflight = 200;
    ccfg.ack_wait_ms = 10000;
    ccfg.max_deliver = 5;
    rc = arbitro_consumer_upsert(c, "bench-chaos", &ccfg, &consumer_id);
    if (rc != ARBITRO_OK) {
        fprintf(stderr, "consumer: %s\n", arbitro_err_str(rc));
        return 1;
    }

    rc = arbitro_subscribe(c, stream_id, consumer_id, on_chaos_msg, NULL);
    if (rc != ARBITRO_OK) {
        fprintf(stderr, "subscribe: %s\n", arbitro_err_str(rc));
        return 1;
    }

    start = now_ms();
    t = start;

    while ((now_ms() - start) < (double)(RUN_SECS * 1000)) {
        uint64_t seq = 0;

        rc = arbitro_publish_sync(c, stream_id,
                                  (const uint8_t *)"bench.chaos.msg", 15,
                                  payload, 32, &seq);
        if (rc == ARBITRO_OK && acked_count < MAX_ACKED) {
            acked_seqs[acked_count++] = seq;
        } else if (rc != ARBITRO_OK) {
            publish_errors++;
            arbitro_client_close(c);
            sleep_ms(500);
            c = chaos_connect();
            if (!c) {
                fprintf(stderr, "reconnect failed, stopping\n");
                break;
            }
            reconnects++;
            arbitro_subscribe(c, stream_id, consumer_id, on_chaos_msg, NULL);
        }

        arbitro_client_poll(c, 10);

        t += RATE_MS;
        if (now_ms() < t) {
            sleep_ms((uint32_t)(t - now_ms()));
        } else {
            t = now_ms();
        }
    }
    elapsed = now_ms() - start;

    arbitro_client_poll(c, 3000);

    for (i = 0; i < acked_count; i++) {
        if (!seq_seen(acked_seqs[i])) missing++;
    }

    printf("Results:\n");
    printf("  published (acked):  %d\n", acked_count);
    printf("  publish errors:     %d\n", publish_errors);
    printf("  received (unique):  %d\n", recv_count);
    printf("  duplicates:         %d (redelivery — expected)\n", recv_dups);
    printf("  reconnects:         %d\n", reconnects);
    printf("  elapsed:            %.2f s\n", elapsed / 1000.0);
    printf("\n");

    if (missing == 0) {
        printf("  LOSS CHECK: PASS — all %d acked seqs received\n", acked_count);
    } else {
        printf("  LOSS CHECK: FAIL — %d seqs missing out of %d acked\n",
               missing, acked_count);
        printf("  First missing seqs:");
        for (i = 0; i < acked_count && missing > 0; i++) {
            if (!seq_seen(acked_seqs[i])) {
                printf(" %llu", (unsigned long long)acked_seqs[i]);
                missing--;
                if (missing > 10) { printf(" ..."); break; }
            }
        }
        printf("\n");
    }

    arbitro_client_close(c);
    return (missing > 0) ? 1 : 0;
}
