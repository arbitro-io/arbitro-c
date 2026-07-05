#ifndef ARB_TEST_HARNESS_H
#define ARB_TEST_HARNESS_H

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

static int arb__test_pass = 0;
static int arb__test_fail = 0;
static const char *arb__test_current = "";

#define ARB_TEST(name) \
    static void name(void); \
    static void name##_register(void) __attribute__((constructor)); \
    static void name##_register(void) { \
        arb__test_current = #name; \
        name(); \
    } \
    static void name(void)

#define ARB_ASSERT(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "  FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond); \
        arb__test_fail++; \
        return; \
    } \
} while(0)

#define ARB_ASSERT_EQ(a, b) do { \
    if ((a) != (b)) { \
        fprintf(stderr, "  FAIL %s:%d: %s != %s\n", __FILE__, __LINE__, #a, #b); \
        arb__test_fail++; \
        return; \
    } \
} while(0)

#define ARB_ASSERT_MEM(a, b, len) do { \
    if (memcmp((a), (b), (len)) != 0) { \
        fprintf(stderr, "  FAIL %s:%d: memcmp(%s, %s, %d)\n", \
                __FILE__, __LINE__, #a, #b, (int)(len)); \
        arb__test_fail++; \
        return; \
    } \
} while(0)

#define ARB_PASS() do { \
    arb__test_pass++; \
    fprintf(stdout, "  PASS %s\n", arb__test_current); \
} while(0)

#define ARB_RUN_TESTS() do { \
    fprintf(stdout, "\n%d passed, %d failed\n", arb__test_pass, arb__test_fail); \
    return arb__test_fail > 0 ? 1 : 0; \
} while(0)

#endif
