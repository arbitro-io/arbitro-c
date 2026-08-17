#ifndef ARB_TEST_ADDR_H
#define ARB_TEST_ADDR_H

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "arbitro/arbitro.h"

/* Header-only so each test binary can be pointed at its own broker. */

/* Scope a subject or filter under a stream's own name.
 *
 * The broker refuses a bare `>` as a stream filter, and refuses two streams
 * whose slices overlap — a stream owns its slice alone. Tests that claimed
 * `>` were written before those rules and now fail at create, which reads
 * downstream as "no deliveries" rather than as a rejected stream.
 *
 * The returned pointer is valid until the eighth further call, which is
 * enough for one publish or one config and keeps call sites from having to
 * thread a buffer through every helper. */
static inline const char *arb_scoped(const char *stream, const char *tail) {
    static char buf[8][320];
    static int slot = 0;
    slot = (slot + 1) & 7;
    snprintf(buf[slot], sizeof(buf[slot]), "%s.%s", stream, tail);
    return buf[slot];
}

/* The stream's whole slice: `<stream>.>`. */
static inline const char *arb_slice(const char *stream) {
    return arb_scoped(stream, ">");
}

static inline uint16_t arb_slen(const char *s) {
    return (uint16_t)strlen(s);
}

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
