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
#else
#include <sys/time.h>
static double now_ms(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (double)tv.tv_sec * 1000.0 + (double)tv.tv_usec / 1000.0;
}
#endif

static int cmp_double(const void *a, const void *b) {
    double da = *(const double *)a;
    double db = *(const double *)b;
    if (da < db) return -1;
    if (da > db) return 1;
    return 0;
}

static double *latencies;
static int lat_idx;
static double pub_time;

static void on_msg(arbitro_msg_t *msg, void *ud) {
    double now = now_ms();
    (void)ud;
    latencies[lat_idx++] = now - pub_time;
    arbitro_msg_ack(msg);
    arbitro_client_stop(msg->client);
}

int main(int argc, char **argv) {
    arbitro_client_t *c;
    uint32_t stream_id, consumer_id;
    arbitro_stream_cfg_t scfg = {0};
    arbitro_consumer_cfg_t ccfg = {0};
    int rc, i;
    int msgs = 1000;
    int payload_size = 64;
    uint8_t *payload;
    double total_ms;

    for (i = 1; i < argc - 1; i++) {
        if (strcmp(argv[i], "--msgs") == 0) msgs = atoi(argv[++i]);
        if (strcmp(argv[i], "--payload") == 0) payload_size = atoi(argv[++i]);
    }

    if (msgs > 1000) msgs = 1000;

    payload = (uint8_t *)malloc((size_t)payload_size);
    latencies = (double *)calloc((size_t)msgs, sizeof(double));
    if (!payload || !latencies) { fprintf(stderr, "malloc\n"); return 1; }
    memset(payload, 'B', (size_t)payload_size);

    rc = arbitro_client_connect("127.0.0.1", ARBITRO_DEFAULT_PORT, NULL, &c);
    if (rc != ARBITRO_OK) {
        fprintf(stderr, "connect: %s\n", arbitro_err_str(rc));
        return 1;
    }

    scfg.subject_filter = "bench-rt.>";
    rc = arbitro_stream_upsert(c, "bench-rt", &scfg, &stream_id);
    if (rc != ARBITRO_OK) { fprintf(stderr, "stream: %s\n", arbitro_err_str(rc)); return 1; }

    ccfg.name = "bench-rt-w";
    ccfg.filter = "bench-rt.>";
    ccfg.ack_policy = ARBITRO_ACK_EXPLICIT;
    ccfg.max_inflight = 100;
    rc = arbitro_consumer_upsert(c, "bench-rt", &ccfg, &consumer_id);
    if (rc != ARBITRO_OK) { fprintf(stderr, "consumer: %s\n", arbitro_err_str(rc)); return 1; }

    rc = arbitro_subscribe(c, stream_id, consumer_id, on_msg, NULL);
    if (rc != ARBITRO_OK) { fprintf(stderr, "sub: %s\n", arbitro_err_str(rc)); return 1; }

    lat_idx = 0;
    total_ms = now_ms();

    for (i = 0; i < msgs; i++) {
        pub_time = now_ms();
        rc = arbitro_publish(c, stream_id,
                             (const uint8_t *)"bench-rt.x", 10,
                             payload, (uint32_t)payload_size);
        if (rc != ARBITRO_OK) break;
        arbitro_client_run(c);
    }

    total_ms = now_ms() - total_ms;

    qsort(latencies, (size_t)lat_idx, sizeof(double), cmp_double);

    printf("bench_roundtrip (publish->deliver->ack):\n");
    printf("  msgs:    %d\n", lat_idx);
    printf("  payload: %d bytes\n", payload_size);
    printf("  total:   %.2f ms\n", total_ms);
    if (lat_idx > 0) {
        printf("  p50:     %.3f ms\n", latencies[lat_idx / 2]);
        printf("  p99:     %.3f ms\n", latencies[(int)((double)lat_idx * 0.99)]);
        printf("  min:     %.3f ms\n", latencies[0]);
        printf("  max:     %.3f ms\n", latencies[lat_idx - 1]);
    }

    arbitro_client_close(c);
    free(payload);
    free(latencies);
    return 0;
}
