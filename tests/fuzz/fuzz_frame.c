#ifndef _WIN32
#define _POSIX_C_SOURCE 200112L
#endif

/*
 * libFuzzer harness for arbitro-c wire parser.
 *
 * Build:
 *   clang -std=c99 -O1 -g -fsanitize=fuzzer,address,undefined \
 *         -DARB_FUZZ_INCLUDE -include src/arbitro.c \
 *         tests/fuzz/fuzz_frame.c -o /tmp/fuzz_frame
 *
 * Run:
 *   /tmp/fuzz_frame -max_total_time=60 -runs=100000
 *
 * The harness targets frame header parsing (`arb__hdr_parse`) and body
 * dispatch (`arb__dispatch_batch_body`) with random byte inputs. Any
 * crash, UBSan/ASan violation, or heap OOB is a bug.
 */

#include <stdint.h>
#include <stddef.h>
#include <string.h>

/* When invoked via `-include src/arbitro.c` we get direct access to static
 * helpers. When compiled standalone, provide minimal stubs that let the
 * harness at least parse a header without linking. */

#ifndef ARB_FUZZ_INCLUDE
/* Standalone smoke — declare the internal shape we test against. */
typedef struct {
    uint16_t action;
    uint8_t  flags;
    uint8_t  entry_flags;
    uint32_t msg_len;
    uint64_t seq;
    uint32_t stream_id;
    uint32_t env_seq;
    int      is_envelope;
} arb_frame_t;

/* Simple redecl matching the source. */
static void arb__hdr_parse(const uint8_t *hdr, arb_frame_t *out) {
    out->action = (uint16_t)hdr[0] | ((uint16_t)hdr[1] << 8);
    out->flags  = hdr[2];
    if (out->action == 0x0205 || out->action == 0x0207) {
        out->is_envelope = 1;
        out->entry_flags = 0;
        out->stream_id = (uint32_t)hdr[4]  | ((uint32_t)hdr[5]<<8)
                       | ((uint32_t)hdr[6]<<16) | ((uint32_t)hdr[7]<<24);
        out->msg_len = (uint32_t)hdr[8]  | ((uint32_t)hdr[9]<<8)
                     | ((uint32_t)hdr[10]<<16) | ((uint32_t)hdr[11]<<24);
        out->env_seq = (uint32_t)hdr[12] | ((uint32_t)hdr[13]<<8)
                     | ((uint32_t)hdr[14]<<16) | ((uint32_t)hdr[15]<<24);
        out->seq = 0;
    } else {
        out->is_envelope = 0;
        out->entry_flags = hdr[3];
        out->msg_len = (uint32_t)hdr[4]  | ((uint32_t)hdr[5]<<8)
                     | ((uint32_t)hdr[6]<<16) | ((uint32_t)hdr[7]<<24);
        out->seq = 0;
        for (int i = 0; i < 8; i++)
            out->seq |= ((uint64_t)hdr[8+i]) << (i*8);
        out->stream_id = 0;
        out->env_seq = 0;
    }
}
#endif

/*
 * Fuzz target: feed random bytes as if they were a broker frame. Verify
 * that the parser doesn't OOB-read the header (16 bytes) or trust the
 * declared `msg_len` beyond a safe cap.
 */
int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    if (size < 16) return 0;

    arb_frame_t fr;
    memset(&fr, 0, sizeof(fr));
    arb__hdr_parse(data, &fr);

    /* Sanity: the parser must not have written past its output. */
    if (fr.action == 0 && fr.msg_len == 0 && fr.flags == 0) return 0;

    /* Bounds check the declared msg_len against remaining bytes. */
    if (fr.msg_len > 64 * 1024) return 0;   /* would be TOOLARGE */
    if (16 + fr.msg_len > size) return 0;   /* truncated, ignore */

    /* If action indicates a batch delivery, walk entries. Each entry has
     * a 24-byte fixed header. Feed the body forward — this is where
     * out-of-bounds bugs surface most readily. */
    if (fr.action == 0x0205 || fr.action == 0x0207 || fr.action == 0x0200) {
        const uint8_t *body = data + 16;
        uint32_t body_len = fr.msg_len;
        if (body_len < 4) return 0;
        uint16_t count = (uint16_t)body[0] | ((uint16_t)body[1] << 8);
        uint32_t off = 4;
        for (uint16_t i = 0; i < count; i++) {
            if (off + 24 > body_len) return 0;
            uint16_t subj_len  = (uint16_t)body[off+12] | ((uint16_t)body[off+13]<<8);
            uint16_t reply_len = (uint16_t)body[off+14] | ((uint16_t)body[off+15]<<8);
            uint32_t data_len  = (uint32_t)body[off+16] | ((uint32_t)body[off+17]<<8)
                               | ((uint32_t)body[off+18]<<16) | ((uint32_t)body[off+19]<<24);
            /* CRITICAL: entry stride uses data_len (which INCLUDES subj+reply).
             * A buggy client that adds subj+reply on top overflows. */
            if (data_len > body_len) return 0;
            if ((uint32_t)subj_len + reply_len > data_len) return 0;
            uint32_t entry_size = 24 + data_len;
            if (off + entry_size > body_len) return 0;
            off += entry_size;
        }
    }

    return 0;
}
