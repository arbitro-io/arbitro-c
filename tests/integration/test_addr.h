#ifndef ARB_TEST_ADDR_H
#define ARB_TEST_ADDR_H

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "arbitro/arbitro.h"

/* Header-only so each test binary can be pointed at its own broker. */

static inline const char *arb_test_addr(void) {
    static char host[128];
    const char *addr = getenv("ARBITRO_ADDR");
    const char *colon;
    size_t n;

    if (!addr || !addr[0]) return "127.0.0.1";

    colon = strchr(addr, ':');
    if (!colon) return addr;

    n = (size_t)(colon - addr);
    if (n >= sizeof(host)) n = sizeof(host) - 1;
    memcpy(host, addr, n);
    host[n] = '\0';
    return host;
}

static inline uint16_t arb_test_port(void) {
    const char *addr = getenv("ARBITRO_ADDR");
    const char *colon;

    if (addr && addr[0]) {
        colon = strchr(addr, ':');
        if (colon) {
            int p = atoi(colon + 1);
            if (p > 0 && p <= 65535) return (uint16_t)p;
        }
    }
    return ARBITRO_DEFAULT_PORT;
}

#endif
