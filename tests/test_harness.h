#ifndef ARB_TEST_HARNESS_H
#define ARB_TEST_HARNESS_H

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

static int arb__test_pass = 0;
static int arb__test_fail = 0;
static const char *arb__test_current = "";

/* Run a function before main. MSVC has no __attribute__((constructor)); a
   function pointer in .CRT$XCU is the documented equivalent — the CRT walks
   that section on startup. */
#if defined(_MSC_VER)
  #pragma section(".CRT$XCU", read)
  #define ARB__AUTORUN(fn) \
      static void fn(void); \
      __declspec(allocate(".CRT$XCU")) void (*fn##_slot)(void) = fn; \
      static void fn(void)
#else
  #define ARB__AUTORUN(fn) \
      static void fn(void) __attribute__((constructor)); \
      static void fn(void)
#endif

#define ARB_TEST(name) \
    static void name(void); \
    ARB__AUTORUN(name##_register) { \
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

/* Zero registered tests is a FAILURE, not a pass. Registration happens through
   a pre-main constructor, and if that mechanism ever stops working — a linker
   that strips the .CRT$XCU slot, a toolchain without either idiom — the suite
   would otherwise print "0 passed, 0 failed" and exit 0. Green CI proving
   nothing is worse than red. */
#define ARB_RUN_TESTS() do { \
    fprintf(stdout, "\n%d passed, %d failed\n", arb__test_pass, arb__test_fail); \
    if (arb__test_pass == 0 && arb__test_fail == 0) { \
        fprintf(stderr, "no tests registered — the pre-main hook did not run\n"); \
        return 1; \
    } \
    return arb__test_fail > 0 ? 1 : 0; \
} while(0)

#endif
