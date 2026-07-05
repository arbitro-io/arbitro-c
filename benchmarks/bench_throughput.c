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

static int msgs = 1000;
static int batch_size = 64;
static int payload_size = 64;

static void bench_single_publish(arbitro_client_t *c, uint32_t stream_id,
                                 uint8_t *payload) {
    double start, elapsed, rate, mbps;
    int i, rc;

    start = now_ms();
    for (i = 0; i < msgs; i++) {
        rc = arbitro_publish(c, stream_id,
                             (const uint8_t *)"bench.throughput.single", 22,
                             payload, (uint32_t)payload_size);
        if (rc != ARBITRO_OK) {
            fprintf(stderr, "publish failed at %d: %s\n", i, arbitro_err_str(rc));
            break;
        }
    }
    arbitro_client_flush(c);
    elapsed = now_ms() - start;
    rate = (double)i / (elapsed / 1000.0);
    mbps = rate * (double)payload_size / (1024.0 * 1024.0);

    printf("  publish_single:\n");
    printf("    msgs:       %d\n", i);
    printf("    elapsed:    %.2f ms\n", elapsed);
    printf("    throughput: %.0f msg/s\n", rate);
    printf("    bandwidth:  %.2f MB/s\n", mbps);
}

static void bench_sync_publish(arbitro_client_t *c, uint32_t stream_id,
                               uint8_t *payload) {
    double start, elapsed, rate;
    uint64_t seq;
    int i, rc;

    start = now_ms();
    for (i = 0; i < msgs; i++) {
        rc = arbitro_publish_sync(c, stream_id,
                                  (const uint8_t *)"bench.throughput.sync", 20,
                                  payload, (uint32_t)payload_size, &seq);
        if (rc != ARBITRO_OK) {
            fprintf(stderr, "publish_sync failed at %d: %s\n", i, arbitro_err_str(rc));
            break;
        }
    }
    elapsed = now_ms() - start;
    rate = (double)i / (elapsed / 1000.0);

    printf("  publish_sync (server-confirmed):\n");
    printf("    msgs:       %d\n", i);
    printf("    elapsed:    %.2f ms\n", elapsed);
    printf("    throughput: %.0f msg/s\n", rate);
    printf("    avg_rtt:    %.3f ms\n", elapsed / (double)i);
}

static void bench_batch_publish(arbitro_client_t *c, uint32_t stream_id,
                                uint8_t *payload) {
    double start, elapsed, rate, mbps;
    int i, b, rc, total_sent = 0;
    int batches;
    arbitro_batch_entry_t *entries;

    entries = (arbitro_batch_entry_t *)calloc((size_t)batch_size,
                                             sizeof(arbitro_batch_entry_t));
    if (!entries) { fprintf(stderr, "malloc\n"); return; }

    for (i = 0; i < batch_size; i++) {
        entries[i].subject = (const uint8_t *)"bench.throughput.batch";
        entries[i].subject_len = 21;
        entries[i].payload = payload;
        entries[i].payload_len = (uint32_t)payload_size;
    }

    batches = (msgs + batch_size - 1) / batch_size;
    start = now_ms();
    for (b = 0; b < batches; b++) {
        int size = batch_size;
        uint64_t first_seq;
        if (b == batches - 1 && msgs % batch_size != 0)
            size = msgs % batch_size;
        rc = arbitro_publish_batch(c, stream_id, entries, (size_t)size, &first_seq);
        if (rc != ARBITRO_OK) {
            fprintf(stderr, "publish_batch failed at batch %d: %s\n", b, arbitro_err_str(rc));
            break;
        }
        total_sent += size;
    }
    elapsed = now_ms() - start;
    rate = (double)total_sent / (elapsed / 1000.0);
    mbps = rate * (double)payload_size / (1024.0 * 1024.0);

    printf("  publish_batch (batch_size=%d, server-confirmed):\n", batch_size);
    printf("    msgs:       %d\n", total_sent);
    printf("    elapsed:    %.2f ms\n", elapsed);
    printf("    throughput: %.0f msg/s\n", rate);
    printf("    bandwidth:  %.2f MB/s\n", mbps);

    free(entries);
}

int main(int argc, char **argv) {
    arbitro_client_t *c;
    uint32_t stream_id;
    arbitro_stream_cfg_t scfg = {0};
    int rc, i;
    uint8_t *payload;

    for (i = 1; i < argc - 1; i++) {
        if (strcmp(argv[i], "--msgs") == 0) msgs = atoi(argv[++i]);
        if (strcmp(argv[i], "--batch") == 0) batch_size = atoi(argv[++i]);
        if (strcmp(argv[i], "--payload") == 0) payload_size = atoi(argv[++i]);
    }
    if (msgs > 1000) msgs = 1000;
    if (batch_size > 256) batch_size = 256;

    payload = (uint8_t *)malloc((size_t)payload_size);
    if (!payload) { fprintf(stderr, "malloc\n"); return 1; }
    memset(payload, 'T', (size_t)payload_size);

    rc = arbitro_client_connect("127.0.0.1", ARBITRO_DEFAULT_PORT, NULL, &c);
    if (rc != ARBITRO_OK) {
        fprintf(stderr, "connect: %s\n", arbitro_err_str(rc));
        return 1;
    }

    scfg.subject_filter = "bench.throughput.>";
    rc = arbitro_stream_upsert(c, "bench-throughput", &scfg, &stream_id);
    if (rc != ARBITRO_OK) {
        fprintf(stderr, "stream: %s\n", arbitro_err_str(rc));
        return 1;
    }

    printf("bench_throughput: %d msgs, %d bytes payload\n\n", msgs, payload_size);

    bench_single_publish(c, stream_id, payload);
    printf("\n");
    bench_sync_publish(c, stream_id, payload);
    printf("\n");
    bench_batch_publish(c, stream_id, payload);

    arbitro_client_close(c);
    free(payload);
    return 0;
}
