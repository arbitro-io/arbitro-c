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

int main(int argc, char **argv) {
    arbitro_client_t *c;
    uint32_t stream_id;
    arbitro_stream_cfg_t scfg = {0};
    int rc, i;
    int msgs = 100000;
    int payload_size = 64;
    uint8_t *payload;
    double t0, t1, elapsed, rate, mbps;

    for (i = 1; i < argc - 1; i++) {
        if (strcmp(argv[i], "--msgs") == 0) msgs = atoi(argv[++i]);
        if (strcmp(argv[i], "--payload") == 0) payload_size = atoi(argv[++i]);
    }

    if (msgs > 1000) msgs = 1000;

    payload = (uint8_t *)malloc((size_t)payload_size);
    if (!payload) { fprintf(stderr, "malloc failed\n"); return 1; }
    memset(payload, 'A', (size_t)payload_size);

    rc = arbitro_client_connect("127.0.0.1", ARBITRO_DEFAULT_PORT, NULL, &c);
    if (rc != ARBITRO_OK) {
        fprintf(stderr, "connect: %s\n", arbitro_err_str(rc));
        free(payload);
        return 1;
    }

    scfg.subject_filter = "bench.>";
    rc = arbitro_stream_upsert(c, "bench-pub", &scfg, &stream_id);
    if (rc != ARBITRO_OK) {
        fprintf(stderr, "stream: %s\n", arbitro_err_str(rc));
        arbitro_client_close(c);
        free(payload);
        return 1;
    }

    for (i = 0; i < 1000 && i < msgs; i++) {
        arbitro_publish(c, stream_id,
                        (const uint8_t *)"bench.warmup", 12,
                        payload, (uint32_t)payload_size);
    }
    arbitro_client_flush(c);

    t0 = now_ms();
    for (i = 0; i < msgs; i++) {
        rc = arbitro_publish(c, stream_id,
                             (const uint8_t *)"bench.msg", 9,
                             payload, (uint32_t)payload_size);
        if (rc != ARBITRO_OK) {
            fprintf(stderr, "publish failed at %d: %s\n", i, arbitro_err_str(rc));
            break;
        }
    }
    arbitro_client_flush(c);
    t1 = now_ms();

    elapsed = t1 - t0;
    rate = (double)msgs / (elapsed / 1000.0);
    mbps = rate * (double)(payload_size + 9 + 16 + 8) / (1024.0 * 1024.0);

    printf("bench_publish:\n");
    printf("  msgs:     %d\n", msgs);
    printf("  payload:  %d bytes\n", payload_size);
    printf("  elapsed:  %.2f ms\n", elapsed);
    printf("  rate:     %.0f msgs/sec\n", rate);
    printf("  throughput: %.2f MB/sec\n", mbps);

    arbitro_client_close(c);
    free(payload);
    return 0;
}
