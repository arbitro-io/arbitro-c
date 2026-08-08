#ifndef _WIN32
#define _POSIX_C_SOURCE 200112L
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "arbitro/arbitro.h"

#ifdef _WIN32
#include <windows.h>
static uint64_t now_ms(void) { return GetTickCount64(); }
#else
#include <time.h>
static uint64_t now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}
#endif

#define PAYLOAD_SIZE 64
#define DEFAULT_ITERS 1000
#define BASIC_BACKLOG 100
#define PAT_BASIC_CAP 1
#define PAT_PREMIUM_CAP 100
#define PAT_DYNAMIC_CAP 1
#define DEFAULT_DYNAMIC_USERS 1000

static int cmp_double(const void *a, const void *b) {
    double da = *(const double *)a;
    double db = *(const double *)b;
    if (da < db) return -1;
    if (da > db) return 1;
    return 0;
}

static double percentile(double *sorted, int n, double p) {
    int idx;
    if (n == 0) return 0.0;
    idx = (int)((double)(n - 1) * p + 0.5);
    if (idx >= n) idx = n - 1;
    return sorted[idx];
}

static void report(const char *label, double *latencies, int n) {
    double sum = 0, avg, p50, p90, p99;
    int i;
    if (n == 0) { printf("  %s | n=0\n", label); return; }
    qsort(latencies, (size_t)n, sizeof(double), cmp_double);
    for (i = 0; i < n; i++) sum += latencies[i];
    avg = sum / (double)n;
    p50 = percentile(latencies, n, 0.50);
    p90 = percentile(latencies, n, 0.90);
    p99 = percentile(latencies, n, 0.99);
    printf("  %-36s | n=%-5d | avg=%7.3f ms | p50=%7.3f ms | p90=%7.3f ms | p99=%7.3f ms\n",
           label, n, avg, p50, p90, p99);
}

static int delivered_count;
static int stop_at;

static void on_deliver_count_ack(arbitro_msg_t *msg, void *ud) {
    (void)ud;
    arbitro_msg_ack(msg);
    delivered_count++;
    if (delivered_count >= stop_at)
        arbitro_client_stop(msg->client);
}

static void on_deliver_no_ack(arbitro_msg_t *msg, void *ud) {
    (void)ud;
    delivered_count++;
    if (delivered_count >= stop_at)
        arbitro_client_stop(msg->client);
}

static int stage0_cap_enforced(arbitro_client_t *c) {
    arbitro_stream_cfg_t scfg = {0};
    arbitro_consumer_cfg_t ccfg = {0};
    uint32_t stream_id, consumer_id;
    uint8_t payload[PAYLOAD_SIZE];
    int rc, total, extras;
    uint64_t t0;

    arbitro_subject_limit_t limits[1];
    limits[0].pattern = (const uint8_t *)"orders.premium.>";
    limits[0].pattern_len = 16;
    limits[0].limit = PAT_PREMIUM_CAP;

    memset(payload, 0, sizeof(payload));

    scfg.subject_filter = ">";
    rc = arbitro_stream_upsert(c, "limits_cap", &scfg, &stream_id);
    if (rc != ARBITRO_OK) {
        fprintf(stderr, "  stage0: stream_upsert: %s\n", arbitro_err_str(rc));
        return -1;
    }

    ccfg.name = "cap_enforced";
    ccfg.filter = ">";
    ccfg.ack_policy = ARBITRO_ACK_EXPLICIT;
    ccfg.max_inflight = 10000;
    ccfg.ack_wait_ms = 30000;
    ccfg.subject_limits = limits;
    ccfg.subject_limit_count = 1;

    rc = arbitro_consumer_create(c, "limits_cap", &ccfg, &consumer_id);
    if (rc != ARBITRO_OK) {
        fprintf(stderr, "  stage0: consumer_create: %s\n", arbitro_err_str(rc));
        return -1;
    }

    total = (int)PAT_PREMIUM_CAP + 10;

    {
        arbitro_batch_entry_t *entries = (arbitro_batch_entry_t *)calloc(
            (size_t)total, sizeof(arbitro_batch_entry_t));
        int i;
        for (i = 0; i < total; i++) {
            entries[i].subject = (const uint8_t *)"orders.premium.singleton";
            entries[i].subject_len = 24;
            entries[i].payload = payload;
            entries[i].payload_len = PAYLOAD_SIZE;
        }
        rc = arbitro_publish_batch(c, stream_id, entries, (uint32_t)total, NULL);
        free(entries);
        if (rc != ARBITRO_OK) {
            fprintf(stderr, "  stage0: publish_batch: %s\n", arbitro_err_str(rc));
            return -1;
        }
    }

    delivered_count = 0;
    stop_at = (int)PAT_PREMIUM_CAP;
    arbitro_subscribe(c, stream_id, consumer_id, on_deliver_no_ack, NULL);

    t0 = now_ms();
    while (delivered_count < (int)PAT_PREMIUM_CAP) {
        if (now_ms() - t0 > 5000) {
            fprintf(stderr, "  stage0: TIMEOUT waiting for cap deliveries (%d/%d)\n",
                    delivered_count, (int)PAT_PREMIUM_CAP);
            arbitro_unsubscribe(c, consumer_id);
            return -1;
        }
        arbitro_client_poll(c, 100);
    }

    extras = 0;
    t0 = now_ms();
    while (now_ms() - t0 < 250) {
        int before = delivered_count;
        arbitro_client_poll(c, 50);
        extras += delivered_count - before;
    }

    arbitro_unsubscribe(c, consumer_id);

    printf("  delivered=%d  extras=%d  -> cap %s\n",
           (int)PAT_PREMIUM_CAP, extras,
           extras == 0 ? "IS enforced" : "FAILED");

    if (extras != 0) {
        fprintf(stderr, "  stage0: cap_enforcement FAILED — got %d extras\n", extras);
        return -1;
    }
    return 0;
}

static int stage1_baseline(arbitro_client_t *c, int iters, double *latencies) {
    arbitro_stream_cfg_t scfg = {0};
    arbitro_consumer_cfg_t ccfg = {0};
    uint32_t stream_id, consumer_id;
    uint8_t payload[PAYLOAD_SIZE];
    int rc, i, lat_count = 0;
    uint64_t t0;
    char subject[64];

    arbitro_subject_limit_t limits[1];
    limits[0].pattern = (const uint8_t *)"orders.premium.>";
    limits[0].pattern_len = 16;
    limits[0].limit = PAT_PREMIUM_CAP;

    memset(payload, 0, sizeof(payload));

    scfg.subject_filter = ">";
    rc = arbitro_stream_upsert(c, "limits_base", &scfg, &stream_id);
    if (rc != ARBITRO_OK) {
        fprintf(stderr, "  stage1: stream: %s\n", arbitro_err_str(rc));
        return -1;
    }

    ccfg.name = "baseline";
    ccfg.filter = ">";
    ccfg.ack_policy = ARBITRO_ACK_EXPLICIT;
    ccfg.max_inflight = 10000;
    ccfg.ack_wait_ms = 30000;
    ccfg.subject_limits = limits;
    ccfg.subject_limit_count = 1;

    rc = arbitro_consumer_create(c, "limits_base", &ccfg, &consumer_id);
    if (rc != ARBITRO_OK) {
        fprintf(stderr, "  stage1: consumer: %s\n", arbitro_err_str(rc));
        return -1;
    }

    delivered_count = 0;
    stop_at = 1;
    arbitro_subscribe(c, stream_id, consumer_id, on_deliver_count_ack, NULL);

    for (i = 0; i < iters; i++) {
        double start_t;
        snprintf(subject, sizeof(subject), "orders.premium.vip_%d", i);
        delivered_count = 0;
        stop_at = 1;

        start_t = (double)now_ms();
        rc = arbitro_publish(c, stream_id, (const uint8_t *)subject,
                             (uint16_t)strlen(subject), payload, PAYLOAD_SIZE);
        if (rc != ARBITRO_OK) continue;

        t0 = now_ms();
        while (delivered_count < 1) {
            if (now_ms() - t0 > 5000) break;
            arbitro_client_poll(c, 100);
        }
        if (delivered_count >= 1)
            latencies[lat_count++] = (double)now_ms() - start_t;
    }

    arbitro_unsubscribe(c, consumer_id);
    return lat_count;
}

static int stage2_isolated(arbitro_client_t *c, int iters, double *latencies) {
    arbitro_stream_cfg_t scfg = {0};
    arbitro_consumer_cfg_t ccfg = {0};
    uint32_t stream_id, consumer_id;
    uint8_t payload[PAYLOAD_SIZE];
    int rc, i, lat_count = 0;
    uint64_t t0;
    char subject[64];

    arbitro_subject_limit_t limits[2];
    limits[0].pattern = (const uint8_t *)"orders.basic.>";
    limits[0].pattern_len = 14;
    limits[0].limit = PAT_BASIC_CAP;
    limits[1].pattern = (const uint8_t *)"orders.premium.>";
    limits[1].pattern_len = 16;
    limits[1].limit = PAT_PREMIUM_CAP;

    memset(payload, 0, sizeof(payload));

    scfg.subject_filter = ">";
    rc = arbitro_stream_upsert(c, "limits_iso", &scfg, &stream_id);
    if (rc != ARBITRO_OK) {
        fprintf(stderr, "  stage2: stream: %s\n", arbitro_err_str(rc));
        return -1;
    }

    ccfg.name = "isolation_tester";
    ccfg.filter = ">";
    ccfg.ack_policy = ARBITRO_ACK_EXPLICIT;
    ccfg.max_inflight = 10000;
    ccfg.ack_wait_ms = 30000;
    ccfg.subject_limits = limits;
    ccfg.subject_limit_count = 2;

    rc = arbitro_consumer_create(c, "limits_iso", &ccfg, &consumer_id);
    if (rc != ARBITRO_OK) {
        fprintf(stderr, "  stage2: consumer: %s\n", arbitro_err_str(rc));
        return -1;
    }

    delivered_count = 0;
    stop_at = BASIC_BACKLOG;
    arbitro_subscribe(c, stream_id, consumer_id, on_deliver_no_ack, NULL);

    {
        arbitro_batch_entry_t *entries = (arbitro_batch_entry_t *)calloc(
            BASIC_BACKLOG, sizeof(arbitro_batch_entry_t));
        char subjects[BASIC_BACKLOG][32];
        for (i = 0; i < BASIC_BACKLOG; i++) {
            snprintf(subjects[i], sizeof(subjects[i]), "orders.basic.user_%d", i);
            entries[i].subject = (const uint8_t *)subjects[i];
            entries[i].subject_len = (uint16_t)strlen(subjects[i]);
            entries[i].payload = payload;
            entries[i].payload_len = PAYLOAD_SIZE;
        }
        rc = arbitro_publish_batch(c, stream_id, entries, BASIC_BACKLOG, NULL);
        free(entries);
        if (rc != ARBITRO_OK) {
            fprintf(stderr, "  stage2: batch: %s\n", arbitro_err_str(rc));
            arbitro_unsubscribe(c, consumer_id);
            return -1;
        }
    }

    t0 = now_ms();
    while (delivered_count < BASIC_BACKLOG) {
        if (now_ms() - t0 > 5000) {
            fprintf(stderr, "  stage2: timeout draining basic backlog (%d/%d)\n",
                    delivered_count, BASIC_BACKLOG);
            arbitro_unsubscribe(c, consumer_id);
            return -1;
        }
        arbitro_client_poll(c, 100);
    }

    arbitro_unsubscribe(c, consumer_id);
    arbitro_subscribe(c, stream_id, consumer_id, on_deliver_count_ack, NULL);

    for (i = 0; i < iters; i++) {
        double start_t;
        snprintf(subject, sizeof(subject), "orders.premium.vip_%d", i);
        delivered_count = 0;
        stop_at = 1;

        start_t = (double)now_ms();
        rc = arbitro_publish(c, stream_id, (const uint8_t *)subject,
                             (uint16_t)strlen(subject), payload, PAYLOAD_SIZE);
        if (rc != ARBITRO_OK) continue;

        t0 = now_ms();
        while (delivered_count < 1) {
            if (now_ms() - t0 > 5000) break;
            arbitro_client_poll(c, 100);
        }
        if (delivered_count >= 1)
            latencies[lat_count++] = (double)now_ms() - start_t;
    }

    arbitro_unsubscribe(c, consumer_id);
    return lat_count;
}

static int stage4_dynamic(arbitro_client_t *c, int n_users) {
    arbitro_stream_cfg_t scfg = {0};
    arbitro_consumer_cfg_t ccfg = {0};
    uint32_t stream_id, consumer_id;
    uint8_t payload[PAYLOAD_SIZE];
    int rc, i;
    uint64_t t0, elapsed;
    double msgs_per_sec, ns_per_msg;

    arbitro_subject_limit_t limits[1];
    limits[0].pattern = (const uint8_t *)"notif.user.>";
    limits[0].pattern_len = 12;
    limits[0].limit = PAT_DYNAMIC_CAP;

    if (n_users > 1000) n_users = 1000;
    memset(payload, 0, sizeof(payload));

    scfg.subject_filter = ">";
    rc = arbitro_stream_upsert(c, "limits_dyn", &scfg, &stream_id);
    if (rc != ARBITRO_OK) {
        fprintf(stderr, "  stage4: stream: %s\n", arbitro_err_str(rc));
        return -1;
    }

    ccfg.name = "dyn_consumer";
    ccfg.filter = ">";
    ccfg.ack_policy = ARBITRO_ACK_EXPLICIT;
    ccfg.max_inflight = 60000;
    ccfg.ack_wait_ms = 30000;
    ccfg.subject_limits = limits;
    ccfg.subject_limit_count = 1;

    rc = arbitro_consumer_create(c, "limits_dyn", &ccfg, &consumer_id);
    if (rc != ARBITRO_OK) {
        fprintf(stderr, "  stage4: consumer: %s\n", arbitro_err_str(rc));
        return -1;
    }

    {
        arbitro_batch_entry_t *entries = (arbitro_batch_entry_t *)calloc(
            (size_t)n_users, sizeof(arbitro_batch_entry_t));
        char (*subjects)[32] = (char (*)[32])calloc((size_t)n_users, 32);
        for (i = 0; i < n_users; i++) {
            snprintf(subjects[i], 32, "notif.user.%d", i);
            entries[i].subject = (const uint8_t *)subjects[i];
            entries[i].subject_len = (uint16_t)strlen(subjects[i]);
            entries[i].payload = payload;
            entries[i].payload_len = PAYLOAD_SIZE;
        }

        delivered_count = 0;
        stop_at = n_users;
        arbitro_subscribe(c, stream_id, consumer_id, on_deliver_count_ack, NULL);

        t0 = now_ms();
        rc = arbitro_publish_batch(c, stream_id, entries, (uint32_t)n_users, NULL);
        free(entries);
        free(subjects);
        if (rc != ARBITRO_OK) {
            fprintf(stderr, "  stage4: batch: %s\n", arbitro_err_str(rc));
            arbitro_unsubscribe(c, consumer_id);
            return -1;
        }
    }

    while (delivered_count < n_users) {
        if (now_ms() - t0 > 30000) {
            fprintf(stderr, "  stage4: timeout (%d/%d)\n", delivered_count, n_users);
            break;
        }
        arbitro_client_poll(c, 100);
    }
    elapsed = now_ms() - t0;

    arbitro_unsubscribe(c, consumer_id);

    msgs_per_sec = (double)delivered_count / ((double)elapsed / 1000.0);
    ns_per_msg = (double)elapsed * 1000000.0 / (double)delivered_count;
    printf("  %d users | delivered=%d | elapsed=%llu ms | %.0f msg/s | %.0f ns/msg\n",
           n_users, delivered_count, (unsigned long long)elapsed,
           msgs_per_sec, ns_per_msg);
    return 0;
}

int main(void) {
    arbitro_client_t *c;
    double *lat_base, *lat_iso;
    int rc, n_base, n_iso, iters;
    double avg_base, avg_iso, ratio;
    const char *env;

    iters = DEFAULT_ITERS;
    env = getenv("BENCH_LIMITS_ITERS");
    if (env) iters = atoi(env);
    if (iters < 1) iters = 1;
    if (iters > 1000) iters = 1000;

    lat_base = (double *)calloc((size_t)iters, sizeof(double));
    lat_iso = (double *)calloc((size_t)iters, sizeof(double));
    if (!lat_base || !lat_iso) { fprintf(stderr, "malloc\n"); return 1; }

    {
        arbitro_opts_t opts;
        arbitro_opts_init(&opts);
        opts.frame_buf_size = 1 * 1024 * 1024;
        rc = arbitro_client_connect("127.0.0.1", ARBITRO_DEFAULT_PORT, &opts, &c);
    }
    if (rc != ARBITRO_OK) {
        fprintf(stderr, "connect: %s\n", arbitro_err_str(rc));
        return 1;
    }

    printf("\n");
    printf("========================================================\n");
    printf("              Subject limits bench (C)\n");
    printf("========================================================\n");
    printf("  iters=%d   payload=%dB\n", iters, PAYLOAD_SIZE);
    printf("  Caps: basic=%d, premium=%d, dynamic=%d\n",
           PAT_BASIC_CAP, PAT_PREMIUM_CAP, PAT_DYNAMIC_CAP);
    printf("  Stage 2 holds %d basic subjects at 1/1.\n", BASIC_BACKLOG);
    printf("\n");

    printf("--------------------------------------------------------\n");
    printf("  Stage 0 — cap enforcement (publish %d to ONE subject, expect %d)\n",
           PAT_PREMIUM_CAP + 10, PAT_PREMIUM_CAP);
    printf("--------------------------------------------------------\n");
    rc = stage0_cap_enforced(c);
    if (rc != 0) {
        fprintf(stderr, "  Stage 0 FAILED — aborting\n");
        arbitro_client_close(c);
        free(lat_base); free(lat_iso);
        return 1;
    }
    printf("\n");

    printf("--------------------------------------------------------\n");
    printf("  Stage 1 — baseline (orders.premium.> capped at %d, never saturated)\n",
           PAT_PREMIUM_CAP);
    printf("--------------------------------------------------------\n");
    n_base = stage1_baseline(c, iters, lat_base);
    if (n_base < 0) {
        fprintf(stderr, "  Stage 1 FAILED\n");
        arbitro_client_close(c);
        free(lat_base); free(lat_iso);
        return 1;
    }
    report("baseline VIP publish -> deliver", lat_base, n_base);
    printf("\n");

    printf("--------------------------------------------------------\n");
    printf("  Stage 2 — isolated (%d basic subjects pinned at 1/1)\n", BASIC_BACKLOG);
    printf("--------------------------------------------------------\n");
    n_iso = stage2_isolated(c, iters, lat_iso);
    if (n_iso < 0) {
        fprintf(stderr, "  Stage 2 FAILED\n");
        arbitro_client_close(c);
        free(lat_base); free(lat_iso);
        return 1;
    }
    report("VIP under basic load", lat_iso, n_iso);
    printf("\n");

    if (n_base > 0 && n_iso > 0) {
        int i;
        double sum_b = 0, sum_i = 0;
        for (i = 0; i < n_base; i++) sum_b += lat_base[i];
        for (i = 0; i < n_iso; i++) sum_i += lat_iso[i];
        avg_base = sum_b / (double)n_base;
        avg_iso = sum_i / (double)n_iso;
        ratio = avg_iso / avg_base;

        printf("--------------------------------------------------------\n");
        printf("  Summary\n");
        printf("--------------------------------------------------------\n");
        printf("  baseline avg: %7.3f ms\n", avg_base);
        printf("  isolated avg: %7.3f ms\n", avg_iso);
        printf("  ratio (vs baseline): %.2fx\n", ratio);
        printf("  (closer to 1.0 = better isolation)\n");
        printf("\n");
    }

    printf("--------------------------------------------------------\n");
    printf("  Stage 4 — dynamic subjects throughput (%d unique users)\n",
           DEFAULT_DYNAMIC_USERS);
    printf("  Pattern: notif.user.<id> with max_subject_inflight(notif.user.>, 1)\n");
    printf("--------------------------------------------------------\n");
    stage4_dynamic(c, DEFAULT_DYNAMIC_USERS);
    printf("\n");

    arbitro_client_close(c);
    free(lat_base);
    free(lat_iso);
    return 0;
}
