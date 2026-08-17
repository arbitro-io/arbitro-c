/* One consumer, several filtered subscriptions — same connection and across
   connections. Plus: may two consumers share a filter, or a name?

   Ported from the Rust suite, which is the source of truth for what the
   broker actually does: arbitro-e2e/tests/one_consumer_many_filters.rs.
   Same fixture, same shapes, same expected counts, so a divergence between
   the clients shows up as two suites disagreeing rather than as silence in
   one of them.

   The broker sends ONE wire copy per (connection, consumer) and stamps it
   with whichever subscription won the round; the client hands that copy to
   every sibling whose filter accepts the subject. Per consumer the wire
   carries 6 copies and the handles see 11 deliveries — the gap is what the
   collapse buys.

   The stream owns `<name>.>` rather than a global slice: Rust can afford
   `*.>` because every test there spawns its own broker, but these share one
   and two streams may not claim overlapping subjects. */

#ifndef _WIN32
#define _POSIX_C_SOURCE 200112L
#endif

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include "arbitro/arbitro.h"
#include "test_addr.h"

static int total = 0, pass = 0, fail = 0, skipped = 0;
#define T(name) do { total++; printf("[%d] %s ... ", total, name); fflush(stdout); } while(0)
#define OK() do { pass++; printf("PASS\n"); } while(0)
#define FAIL(fmt, ...) do { fail++; printf("FAIL — " fmt "\n", ##__VA_ARGS__); } while(0)
#define SKIP(reason) do { skipped++; printf("SKIP — %s\n", reason); } while(0)

#define FAN_ORDERS   3
#define FAN_PAYMENTS 2
#define FAN_AUDIT    1
#define FAN_TOTAL    (FAN_ORDERS + FAN_PAYMENTS + FAN_AUDIT)

#define MAX_HITS 64

/* One subscription's view: what it was handed, and under which id it acked.
   Keeping the ids lets a test prove each sibling acked its own pending
   rather than the delivered one's. */
typedef struct {
    int      n;
    char     subj[MAX_HITS][128];
    uint32_t sub_id[MAX_HITS];
} sink_t;

static void sink_reset(sink_t *s) { s->n = 0; }

static void sink_cb(arbitro_msg_t *m, void *ud) {
    sink_t *s = (sink_t *)ud;
    if (s->n < MAX_HITS) {
        size_t n = m->subject_len < sizeof(s->subj[0]) - 1
                 ? m->subject_len : sizeof(s->subj[0]) - 1;
        memcpy(s->subj[s->n], m->subject, n);
        s->subj[s->n][n] = '\0';
        s->sub_id[s->n] = m->sub_id;
        s->n++;
    }
    arbitro_msg_ack(m);
}

static uint64_t nowms(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static void mk_name(char *dst, size_t cap, const char *tag) {
    static uint32_t ctr = 0;
    snprintf(dst, cap, "cfan_%s_%llu_%u", tag, (unsigned long long)nowms(), ++ctr);
}

static arbitro_client_t *dial(void) {
    arbitro_client_t *c = NULL;
    arbitro_opts_t o;
    arbitro_opts_init(&o);
    if (arbitro_client_connect(arb_test_addr(), arb_test_port(), &o, &c) != ARBITRO_OK)
        return NULL;
    return c;
}

/* Reads until every subscription goes quiet. A fixed sleep would either be
   flaky or slow; polling until two consecutive rounds add nothing is both
   fast when the broker is and patient when it is not. */
static void drain_all(arbitro_client_t **cs, int ncs, sink_t **sinks, int nsinks) {
    int quiet = 0, i, before;
    while (quiet < 2) {
        before = 0;
        for (i = 0; i < nsinks; i++) before += sinks[i]->n;
        for (i = 0; i < ncs; i++) arbitro_client_poll(cs[i], 200);
        {
            int after = 0;
            for (i = 0; i < nsinks; i++) after += sinks[i]->n;
            quiet = (after == before) ? quiet + 1 : 0;
        }
    }
}

static int setup_stream(arbitro_client_t *c, const char *name, uint32_t *out_id) {
    arbitro_stream_cfg_t cfg;
    char filter[160];
    memset(&cfg, 0, sizeof(cfg));
    snprintf(filter, sizeof(filter), "%s.>", name);
    cfg.subject_filter = filter;
    return arbitro_stream_create(c, name, &cfg, out_id);
}

/* The shared shape: one consumer, wide open, fanout. Every narrowing is
   subscription-side. */
static void wide_consumer(arbitro_consumer_cfg_t *cfg, const char *name) {
    memset(cfg, 0, sizeof(*cfg));
    cfg->name = name;
    cfg->fanout = 1;
    cfg->ack_policy = ARBITRO_ACK_EXPLICIT;
    cfg->max_inflight = 1000;
    cfg->ack_wait_ms = 30000;
}

static int publish_fixture(arbitro_client_t *c, uint32_t sid, const char *stream) {
    char subj[160];
    int i, rc;
    for (i = 0; i < FAN_ORDERS; i++) {
        snprintf(subj, sizeof(subj), "%s.orders.%d", stream, i);
        rc = arbitro_publish_sync(c, sid, (const uint8_t *)subj,
                                  (uint16_t)strlen(subj), (const uint8_t *)"o", 1, NULL);
        if (rc != ARBITRO_OK) return rc;
    }
    for (i = 0; i < FAN_PAYMENTS; i++) {
        snprintf(subj, sizeof(subj), "%s.payments.%d", stream, i);
        rc = arbitro_publish_sync(c, sid, (const uint8_t *)subj,
                                  (uint16_t)strlen(subj), (const uint8_t *)"p", 1, NULL);
        if (rc != ARBITRO_OK) return rc;
    }
    /* Matches the consumer's wide filter and only the widest subscription.
       Handed to a narrow one, it would be stranded. */
    snprintf(subj, sizeof(subj), "%s.audit.trail", stream);
    return arbitro_publish_sync(c, sid, (const uint8_t *)subj,
                                (uint16_t)strlen(subj), (const uint8_t *)"x", 1, NULL);
}

static int sub_trio(arbitro_client_t *c, uint32_t sid, uint32_t cid,
                    const char *stream, sink_t *o, sink_t *p, sink_t *a) {
    char f[160];
    int rc;
    snprintf(f, sizeof(f), "%s.orders.*", stream);
    rc = arbitro_subscribe_filter(c, sid, cid, (const uint8_t *)f,
                                  (uint16_t)strlen(f), sink_cb, o);
    if (rc != ARBITRO_OK) return rc;
    snprintf(f, sizeof(f), "%s.payments.*", stream);
    rc = arbitro_subscribe_filter(c, sid, cid, (const uint8_t *)f,
                                  (uint16_t)strlen(f), sink_cb, p);
    if (rc != ARBITRO_OK) return rc;
    snprintf(f, sizeof(f), "%s.>", stream);
    return arbitro_subscribe_filter(c, sid, cid, (const uint8_t *)f,
                                    (uint16_t)strlen(f), sink_cb, a);
}

/* The Rust `assert_outcome`, verbatim in intent. Returns 0 on success and
   prints what actually arrived, because a bare count tells you nothing about
   which message went astray. */
static int check_outcome(const char *label, sink_t *o, sink_t *p, sink_t *a) {
    int i, bad = 0;

    for (i = 0; i < o->n; i++)
        if (!strstr(o->subj[i], ".orders.")) {
            printf("\n    [%s] orders sub got a foreign subject: %s", label, o->subj[i]);
            bad = 1;
        }
    for (i = 0; i < p->n; i++)
        if (!strstr(p->subj[i], ".payments.")) {
            printf("\n    [%s] payments sub got a foreign subject: %s", label, p->subj[i]);
            bad = 1;
        }

    if (o->n != FAN_ORDERS) {
        printf("\n    [%s] orders.* saw %d of %d", label, o->n, FAN_ORDERS);
        bad = 1;
    }
    if (p->n != FAN_PAYMENTS) {
        printf("\n    [%s] payments.* saw %d of %d", label, p->n, FAN_PAYMENTS);
        bad = 1;
    }
    if (a->n != FAN_TOTAL) {
        printf("\n    [%s] catch-all saw %d of %d", label, a->n, FAN_TOTAL);
        bad = 1;
    }
    return bad;
}

/* Each subscription must ack under one id, and a different one from its
   siblings. If the client echoed the delivered id instead, two of the three
   would leave a pending open until ack_wait fired and the broker redelivered
   — the counts above would still pass while the acks were wrong. */
static int check_distinct_ids(sink_t *o, sink_t *p, sink_t *a) {
    uint32_t io, ip, ia;
    int i, bad = 0;

    if (o->n == 0 || p->n == 0 || a->n == 0) return 0;
    io = o->sub_id[0]; ip = p->sub_id[0]; ia = a->sub_id[0];

    for (i = 0; i < o->n; i++) if (o->sub_id[i] != io) bad = 1;
    for (i = 0; i < p->n; i++) if (p->sub_id[i] != ip) bad = 1;
    for (i = 0; i < a->n; i++) if (a->sub_id[i] != ia) bad = 1;
    if (bad) { printf("\n    a subscription acked under more than one id"); return 1; }

    if (io == ip || io == ia || ip == ia) {
        printf("\n    siblings share a sub id: %u %u %u", io, ip, ia);
        return 1;
    }
    return 0;
}

static void cleanup(arbitro_client_t *c, const char *stream) {
    if (c) arbitro_stream_delete(c, stream, 0);
}

/* ── 1. one connection, one consumer, three filters ──────────────────── */

static void t_same_connection(void) {
    arbitro_client_t *c;
    uint32_t sid, cid;
    char stream[96], cons[96];
    arbitro_consumer_cfg_t ccfg;
    sink_t o, p, a;
    sink_t *sinks[3];

    T("filtered subs on one consumer, same connection");
    c = dial();
    if (!c) { SKIP("no broker"); return; }

    mk_name(stream, sizeof(stream), "same");
    mk_name(cons, sizeof(cons), "c");
    if (setup_stream(c, stream, &sid) != ARBITRO_OK) {
        FAIL("create stream"); arbitro_client_close(c); return;
    }

    wide_consumer(&ccfg, cons);
    if (arbitro_consumer_create(c, stream, &ccfg, &cid) != ARBITRO_OK) {
        FAIL("create consumer"); cleanup(c, stream); arbitro_client_close(c); return;
    }

    sink_reset(&o); sink_reset(&p); sink_reset(&a);
    if (sub_trio(c, sid, cid, stream, &o, &p, &a) != ARBITRO_OK) {
        FAIL("subscribe trio"); cleanup(c, stream); arbitro_client_close(c); return;
    }
    if (publish_fixture(c, sid, stream) != ARBITRO_OK) {
        FAIL("publish fixture"); cleanup(c, stream); arbitro_client_close(c); return;
    }

    sinks[0] = &o; sinks[1] = &p; sinks[2] = &a;
    drain_all(&c, 1, sinks, 3);

    if (check_outcome("same-conn", &o, &p, &a) || check_distinct_ids(&o, &p, &a))
        FAIL("fanout");
    else
        OK();
    cleanup(c, stream);
    arbitro_client_close(c);
}

/* ── 2. the same consumer, one connection per subscription ───────────── */

static void t_across_connections(void) {
    arbitro_client_t *admin, *c1, *c2, *c3, *cs[3];
    uint32_t sid, cid;
    char stream[96], cons[96], f[160];
    arbitro_consumer_cfg_t ccfg;
    sink_t o, p, a;
    sink_t *sinks[3];
    int bad;

    T("filtered subs on one consumer, across connections");
    admin = dial();
    if (!admin) { SKIP("no broker"); return; }

    mk_name(stream, sizeof(stream), "across");
    mk_name(cons, sizeof(cons), "c");
    if (setup_stream(admin, stream, &sid) != ARBITRO_OK) {
        FAIL("create stream"); arbitro_client_close(admin); return;
    }
    wide_consumer(&ccfg, cons);
    if (arbitro_consumer_create(admin, stream, &ccfg, &cid) != ARBITRO_OK) {
        FAIL("create consumer"); cleanup(admin, stream); arbitro_client_close(admin); return;
    }

    c1 = dial(); c2 = dial(); c3 = dial();
    if (!c1 || !c2 || !c3) {
        FAIL("dial three"); cleanup(admin, stream); arbitro_client_close(admin); return;
    }

    sink_reset(&o); sink_reset(&p); sink_reset(&a);
    snprintf(f, sizeof(f), "%s.orders.*", stream);
    arbitro_subscribe_filter(c1, sid, cid, (const uint8_t *)f, (uint16_t)strlen(f), sink_cb, &o);
    snprintf(f, sizeof(f), "%s.payments.*", stream);
    arbitro_subscribe_filter(c2, sid, cid, (const uint8_t *)f, (uint16_t)strlen(f), sink_cb, &p);
    snprintf(f, sizeof(f), "%s.>", stream);
    arbitro_subscribe_filter(c3, sid, cid, (const uint8_t *)f, (uint16_t)strlen(f), sink_cb, &a);

    if (publish_fixture(admin, sid, stream) != ARBITRO_OK) {
        FAIL("publish fixture"); goto done;
    }

    cs[0] = c1; cs[1] = c2; cs[2] = c3;
    sinks[0] = &o; sinks[1] = &p; sinks[2] = &a;
    drain_all(cs, 3, sinks, 3);

    bad = check_outcome("across-conn", &o, &p, &a);
    if (bad) FAIL("counts"); else OK();
done:
    cleanup(admin, stream);
    arbitro_client_close(c1); arbitro_client_close(c2); arbitro_client_close(c3);
    arbitro_client_close(admin);
}

/* ── 3. four connections, the fourth a second catch-all ──────────────── */

static void t_four_connections(void) {
    arbitro_client_t *admin, *c1, *c2, *c3, *c4, *cs[4];
    uint32_t sid, cid;
    char stream[96], cons[96], f[160];
    arbitro_consumer_cfg_t ccfg;
    sink_t o, p, a, a2;
    sink_t *sinks[4];
    int bad;

    T("filtered subs on four connections");
    admin = dial();
    if (!admin) { SKIP("no broker"); return; }

    mk_name(stream, sizeof(stream), "four");
    mk_name(cons, sizeof(cons), "c");
    if (setup_stream(admin, stream, &sid) != ARBITRO_OK) {
        FAIL("create stream"); arbitro_client_close(admin); return;
    }
    wide_consumer(&ccfg, cons);
    if (arbitro_consumer_create(admin, stream, &ccfg, &cid) != ARBITRO_OK) {
        FAIL("create consumer"); cleanup(admin, stream); arbitro_client_close(admin); return;
    }

    c1 = dial(); c2 = dial(); c3 = dial(); c4 = dial();
    if (!c1 || !c2 || !c3 || !c4) {
        FAIL("dial four"); cleanup(admin, stream); arbitro_client_close(admin); return;
    }

    sink_reset(&o); sink_reset(&p); sink_reset(&a); sink_reset(&a2);
    snprintf(f, sizeof(f), "%s.orders.*", stream);
    arbitro_subscribe_filter(c1, sid, cid, (const uint8_t *)f, (uint16_t)strlen(f), sink_cb, &o);
    snprintf(f, sizeof(f), "%s.payments.*", stream);
    arbitro_subscribe_filter(c2, sid, cid, (const uint8_t *)f, (uint16_t)strlen(f), sink_cb, &p);
    snprintf(f, sizeof(f), "%s.>", stream);
    arbitro_subscribe_filter(c3, sid, cid, (const uint8_t *)f, (uint16_t)strlen(f), sink_cb, &a);
    arbitro_subscribe_filter(c4, sid, cid, (const uint8_t *)f, (uint16_t)strlen(f), sink_cb, &a2);

    if (publish_fixture(admin, sid, stream) != ARBITRO_OK) {
        FAIL("publish fixture"); goto done;
    }

    cs[0] = c1; cs[1] = c2; cs[2] = c3; cs[3] = c4;
    sinks[0] = &o; sinks[1] = &p; sinks[2] = &a; sinks[3] = &a2;
    drain_all(cs, 4, sinks, 4);

    bad = check_outcome("four-conn", &o, &p, &a);
    if (a2.n != FAN_TOTAL) {
        printf("\n    [four-conn] second catch-all saw %d of %d", a2.n, FAN_TOTAL);
        bad = 1;
    }
    if (bad) FAIL("counts"); else OK();
done:
    cleanup(admin, stream);
    arbitro_client_close(c1); arbitro_client_close(c2);
    arbitro_client_close(c3); arbitro_client_close(c4);
    arbitro_client_close(admin);
}

/* ── 4. nine consumers, each with the same three filters ─────────────── */

static void t_nine_consumers(void) {
    arbitro_client_t *c;
    uint32_t sid, cid[9];
    char stream[96], cons[9][96];
    arbitro_consumer_cfg_t ccfg;
    sink_t o[9], p[9], a[9];
    sink_t *sinks[27];
    int i, bad = 0;

    T("nine consumers, each with three filtered subscriptions");
    c = dial();
    if (!c) { SKIP("no broker"); return; }

    mk_name(stream, sizeof(stream), "nine");
    if (setup_stream(c, stream, &sid) != ARBITRO_OK) {
        FAIL("create stream"); arbitro_client_close(c); return;
    }

    for (i = 0; i < 9; i++) {
        snprintf(cons[i], sizeof(cons[i]), "cons_%d", i);
        wide_consumer(&ccfg, cons[i]);
        if (arbitro_consumer_create(c, stream, &ccfg, &cid[i]) != ARBITRO_OK) {
            FAIL("create consumer %d", i); cleanup(c, stream); arbitro_client_close(c); return;
        }
        sink_reset(&o[i]); sink_reset(&p[i]); sink_reset(&a[i]);
        if (sub_trio(c, sid, cid[i], stream, &o[i], &p[i], &a[i]) != ARBITRO_OK) {
            FAIL("subscribe trio %d", i); cleanup(c, stream); arbitro_client_close(c); return;
        }
        sinks[i * 3 + 0] = &o[i];
        sinks[i * 3 + 1] = &p[i];
        sinks[i * 3 + 2] = &a[i];
    }

    if (publish_fixture(c, sid, stream) != ARBITRO_OK) {
        FAIL("publish fixture"); cleanup(c, stream); arbitro_client_close(c); return;
    }

    drain_all(&c, 1, sinks, 27);

    /* Every consumer is independent: nine complete copies of the outcome,
       not nine shares of one. */
    for (i = 0; i < 9; i++) {
        char label[32];
        snprintf(label, sizeof(label), "cons-%d", i);
        if (check_outcome(label, &o[i], &p[i], &a[i])) bad = 1;
    }
    if (bad) FAIL("counts"); else OK();
    cleanup(c, stream);
    arbitro_client_close(c);
}

/* ── 5. two consumers may share one filter ───────────────────────────── */

static void t_two_consumers_share_filter(void) {
    arbitro_client_t *admin, *c1, *c2, *cs[2];
    uint32_t sid, cid1, cid2;
    char stream[96], n1[96], n2[96], f[160];
    arbitro_consumer_cfg_t ccfg;
    sink_t s1, s2;
    sink_t *sinks[2];
    int bad = 0;

    T("two consumers may share one filter");
    admin = dial();
    if (!admin) { SKIP("no broker"); return; }

    mk_name(stream, sizeof(stream), "share");
    mk_name(n1, sizeof(n1), "one");
    mk_name(n2, sizeof(n2), "two");
    if (setup_stream(admin, stream, &sid) != ARBITRO_OK) {
        FAIL("create stream"); arbitro_client_close(admin); return;
    }
    wide_consumer(&ccfg, n1);
    if (arbitro_consumer_create(admin, stream, &ccfg, &cid1) != ARBITRO_OK) {
        FAIL("create consumer 1"); cleanup(admin, stream); arbitro_client_close(admin); return;
    }
    wide_consumer(&ccfg, n2);
    if (arbitro_consumer_create(admin, stream, &ccfg, &cid2) != ARBITRO_OK) {
        FAIL("create consumer 2"); cleanup(admin, stream); arbitro_client_close(admin); return;
    }

    /* One connection each: two consumers on one connection would still work,
       but separating them mirrors how the sibling suites read. */
    c1 = dial(); c2 = dial();
    if (!c1 || !c2) {
        FAIL("dial two"); cleanup(admin, stream); arbitro_client_close(admin); return;
    }

    sink_reset(&s1); sink_reset(&s2);
    snprintf(f, sizeof(f), "%s.orders.*", stream);
    arbitro_subscribe_filter(c1, sid, cid1, (const uint8_t *)f, (uint16_t)strlen(f), sink_cb, &s1);
    arbitro_subscribe_filter(c2, sid, cid2, (const uint8_t *)f, (uint16_t)strlen(f), sink_cb, &s2);

    if (publish_fixture(admin, sid, stream) != ARBITRO_OK) {
        FAIL("publish fixture"); goto done;
    }

    cs[0] = c1; cs[1] = c2;
    sinks[0] = &s1; sinks[1] = &s2;
    drain_all(cs, 2, sinks, 2);

    /* The same filter on two consumers is not a split: each gets its own
       full copy, because a consumer is the unit of delivery. */
    if (s1.n != FAN_ORDERS) { printf("\n    consumer 1 saw %d of %d", s1.n, FAN_ORDERS); bad = 1; }
    if (s2.n != FAN_ORDERS) { printf("\n    consumer 2 saw %d of %d", s2.n, FAN_ORDERS); bad = 1; }
    if (bad) FAIL("counts"); else OK();
done:
    cleanup(admin, stream);
    arbitro_client_close(c1); arbitro_client_close(c2);
    arbitro_client_close(admin);
}

/* ── 6. two consumers with the same name ─────────────────────────────── */

static void t_two_consumers_same_name(void) {
    arbitro_client_t *admin;
    uint32_t sid, cid1, cid2;
    char stream[96], name[96];
    arbitro_consumer_cfg_t ccfg;
    int rc;

    T("two consumers with the same name resolve to one");
    admin = dial();
    if (!admin) { SKIP("no broker"); return; }

    mk_name(stream, sizeof(stream), "samename");
    mk_name(name, sizeof(name), "dup");
    if (setup_stream(admin, stream, &sid) != ARBITRO_OK) {
        FAIL("create stream"); arbitro_client_close(admin); return;
    }

    wide_consumer(&ccfg, name);
    if (arbitro_consumer_create(admin, stream, &ccfg, &cid1) != ARBITRO_OK) {
        FAIL("create consumer 1"); cleanup(admin, stream); arbitro_client_close(admin); return;
    }
    wide_consumer(&ccfg, name);
    rc = arbitro_consumer_create(admin, stream, &ccfg, &cid2);

    /* A name is an identity, not a request for a new consumer: creating the
       same one twice with the same config returns the same id rather than a
       second consumer competing for the same stream. */
    if (rc != ARBITRO_OK) {
        FAIL("second create returned %d (%s)", rc, arbitro_err_str(rc));
    } else if (cid1 != cid2) {
        FAIL("same name gave two ids: %u and %u", cid1, cid2);
    } else {
        OK();
    }
    cleanup(admin, stream);
    arbitro_client_close(admin);
}

int main(void) {
    printf("arbitro-c — fanout siblings (broker at %s:%u)\n\n",
           arb_test_addr(), (unsigned)arb_test_port());

    t_same_connection();
    t_across_connections();
    t_four_connections();
    t_nine_consumers();
    t_two_consumers_share_filter();
    t_two_consumers_same_name();

    printf("\n%d tests: %d pass, %d fail, %d skip\n", total, pass, fail, skipped);
    return fail == 0 ? 0 : 1;
}
