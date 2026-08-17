#ifndef ARBITRO_H
#define ARBITRO_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#define ARBITRO_VERSION_MAJOR 0
#define ARBITRO_VERSION_MINOR 2
#define ARBITRO_VERSION_PATCH 2

#define ARBITRO_DEFAULT_PORT       9898
#define ARBITRO_MAX_SUBJECT_LEN    255
#define ARBITRO_MAX_MSG_ID_LEN     64
#define ARBITRO_DEFAULT_FRAME_BUF  (64 * 1024)
#define ARBITRO_ACK_BATCH_MAX      256
#define ARBITRO_NACK_BATCH_MAX     256
#define ARBITRO_PUBLISH_BATCH_MAX  256

#define ARBITRO_OK             0
#define ARBITRO_ERR_SOCKET    -1
#define ARBITRO_ERR_CONNECT   -2
#define ARBITRO_ERR_HANDSHAKE -3
#define ARBITRO_ERR_CLOSED    -4
#define ARBITRO_ERR_TIMEOUT   -5
#define ARBITRO_ERR_PROTOCOL  -6
#define ARBITRO_ERR_TOOLARGE  -7
#define ARBITRO_ERR_NOMEM     -8
#define ARBITRO_ERR_BROKER    -9
#define ARBITRO_ERR_ARG       -10
#define ARBITRO_ERR_NOTFOUND  -11
#define ARBITRO_ERR_STATE     -12
#define ARBITRO_ERR_POOL      -13
#define ARBITRO_ERR_AUTH      -14
/* Transient: the kernel buffer is full on a non-blocking socket and NOTHING
   of the frame was written. Retry the same call. Every other error is
   terminal for the connection. */
#define ARBITRO_ERR_WOULDBLOCK -15

#define ARBITRO_ACK_NONE      0
#define ARBITRO_ACK_EXPLICIT  1

const char *arbitro_err_str(int code);

typedef struct arbitro_client arbitro_client_t;

typedef struct arbitro_msg {
    arbitro_client_t *client;
    const uint8_t    *subject;
    uint16_t          subject_len;
    const uint8_t    *reply_to;
    uint16_t          reply_len;
    const uint8_t    *data;
    uint32_t          data_len;
    uint64_t          seq;
    uint32_t          consumer_id;
    /* The subscription this copy was delivered for. Several subscriptions
       may share a consumer, and the broker opens one pending per
       subscription, so an ack that carries the wrong id leaves the real one
       outstanding until ack_wait fires. */
    uint32_t          sub_id;
} arbitro_msg_t;

typedef void (*arbitro_msg_cb)(arbitro_msg_t *msg, void *userdata);

typedef struct arbitro_opts {
    uint32_t connect_timeout_ms;
    uint32_t request_timeout_ms;
    uint32_t frame_buf_size;
    int      reconnect;
    uint32_t reconnect_max;      /* 0 = retry forever (decorrelated jitter backoff) */
    uint32_t reconnect_delay_ms;
    uint32_t keepalive_interval_ms; /* 0 disables client-initiated Ping */
    uint32_t keepalive_timeout_ms;  /* 0 disables Pong-staleness watchdog */

    /* SO_SNDBUF request, applied right after connect. There is no user-space
       send queue in this client, so this is what decides how much unsent
       data can pile up before arbitro_publish starts returning
       ARBITRO_ERR_WOULDBLOCK on a non-blocking socket. 0 = leave the OS
       default alone. The kernel may round or cap it (Linux doubles it), so
       treat it as a request, not a guarantee -- never read it back and assert on it. */
    uint32_t send_buf_size;

    /* Bearer token sent once, right after Hello, and again on every reconnect.
       NULL = no Auth frame, which is what a broker with auth disabled expects.
       A wrong token is terminal: reconnect stops instead of retrying forever. */
    const char *auth_token;

    int         tls_enabled;      /* 0/1 — requires build with ARBITRO_TLS */
    const char *tls_server_name;  /* SNI override; NULL falls back to host */
    int         tls_verify;       /* 1 = strict (default), 0 = accept invalid certs */
    const char *tls_ca_file;      /* NULL = use OpenSSL default trust store */
} arbitro_opts_t;

void arbitro_opts_init(arbitro_opts_t *opts);

int  arbitro_client_connect(const char *host, uint16_t port,
                            const arbitro_opts_t *opts,
                            arbitro_client_t **out_client);
void arbitro_client_close(arbitro_client_t *c);
int  arbitro_client_is_connected(const arbitro_client_t *c);
int  arbitro_client_poll(arbitro_client_t *c, int timeout_ms);
int  arbitro_client_run(arbitro_client_t *c);
void arbitro_client_stop(arbitro_client_t *c);
int  arbitro_client_flush(arbitro_client_t *c);
int  arbitro_client_flush_acks(arbitro_client_t *c);
int  arbitro_client_flush_nacks(arbitro_client_t *c);

int  arbitro_publish(arbitro_client_t *c, uint32_t stream_id,
                     const uint8_t *subject, uint16_t subject_len,
                     const uint8_t *payload, uint32_t payload_len);
int  arbitro_publish_sync(arbitro_client_t *c, uint32_t stream_id,
                          const uint8_t *subject, uint16_t subject_len,
                          const uint8_t *payload, uint32_t payload_len,
                          uint64_t *out_seq);
/* name-parity alias for the Rust reference client's publish_wait: waits for the broker OK, not a disk fsync */
int  arbitro_publish_wait(arbitro_client_t *c, uint32_t stream_id,
                          const uint8_t *subject, uint16_t subject_len,
                          const uint8_t *payload, uint32_t payload_len,
                          uint64_t *out_seq);
int  arbitro_publish_with_id(arbitro_client_t *c, uint32_t stream_id,
                             const uint8_t *subject, uint16_t subject_len,
                             const uint8_t *msg_id, uint16_t msg_id_len,
                             const uint8_t *payload, uint32_t payload_len);
int  arbitro_publish_sync_with_id(arbitro_client_t *c, uint32_t stream_id,
                                  const uint8_t *subject, uint16_t subject_len,
                                  const uint8_t *msg_id, uint16_t msg_id_len,
                                  const uint8_t *payload, uint32_t payload_len,
                                  uint64_t *out_seq);

typedef struct {
    const uint8_t *subject;
    uint16_t       subject_len;
    const uint8_t *msg_id;
    uint16_t       msg_id_len;
    const uint8_t *payload;
    uint32_t       payload_len;
} arbitro_batch_entry_t;

int  arbitro_publish_batch(arbitro_client_t *c, uint32_t stream_id,
                           const arbitro_batch_entry_t *entries, size_t count,
                           uint64_t *out_first_seq);
int  arbitro_publish_batch_sync(arbitro_client_t *c, uint32_t stream_id,
                                const arbitro_batch_entry_t *entries, size_t count,
                                uint64_t *out_first_seq);
int  arbitro_publish_batch_async(arbitro_client_t *c, uint32_t stream_id,
                                 const arbitro_batch_entry_t *entries, size_t count);

int  arbitro_subscribe(arbitro_client_t *c, uint32_t stream_id,
                       uint32_t consumer_id, arbitro_msg_cb cb, void *userdata);
int  arbitro_subscribe_filter(arbitro_client_t *c, uint32_t stream_id,
                              uint32_t consumer_id,
                              const uint8_t *filter, uint16_t filter_len,
                              arbitro_msg_cb cb, void *userdata);
int  arbitro_unsubscribe(arbitro_client_t *c, uint32_t consumer_id);

/* One entry of arbitro_subscribe_batch. A NULL/zero-length filter inherits
   the consumer's, exactly as arbitro_subscribe does. */
typedef struct {
    const uint8_t *filter;
    uint16_t       filter_len;
    arbitro_msg_cb cb;
    void          *userdata;
} arbitro_sub_entry_t;

/* Per-entry verdict, filled in request order. code == 0 means accepted;
   anything else is the wire error code that refused it (usually
   ARBITRO_ERRCODE_INVALID_SUBSCRIPTION_FILTER). */
typedef struct {
    uint32_t sub_id;
    uint16_t code;
} arbitro_sub_result_t;

/* Open `count` filtered subscriptions on one consumer in ONE round-trip.

   `count` arbitro_subscribe calls cost `count` round-trips; the broker's work
   per subscription is a filter check and a binding, so the trip is nearly the
   whole cost. Every entry runs the same admission rules as a single subscribe.

   `results` may be NULL; when given it must have `count` slots and is filled
   in request order. Returns ARBITRO_OK when every entry was accepted,
   ARBITRO_ERR_BROKER when some were refused (read `results` to learn which,
   the accepted ones are live), or a negative error for a whole-frame failure,
   in which case nothing was subscribed. */
int  arbitro_subscribe_batch(arbitro_client_t *c, uint32_t stream_id,
                             uint32_t consumer_id,
                             const arbitro_sub_entry_t *entries, uint16_t count,
                             arbitro_sub_result_t *results);

typedef struct {
    const uint8_t *pattern;
    uint16_t       pattern_len;
    uint32_t       limit;
} arbitro_subject_limit_t;

/* Zero-initialise for defaults: group falls back to the stream name, no
   subject filter, broker-default redelivery deadline, unlimited in-flight.
   ack_policy is absent on purpose -- a work queue is always explicit-ack. */
typedef struct {
    const char *group;
    const char *filter;
    uint32_t    ack_wait_ms;
    uint32_t    max_inflight;   /* clamped to the u16 wire field */
    uint8_t     deliver_policy;
    uint64_t    start_seq;      /* required when deliver_policy is ByStartSeq */
    const arbitro_subject_limit_t *subject_limits;
    uint32_t    subject_limit_count;
} arbitro_queue_cfg_t;

/* Joins a durable work queue: every worker calling this with the same group
   shares one round-robin queue, each message going to exactly one of them.
   The group is both the durable consumer name and the queue group, which is
   what keeps concurrent joins idempotent instead of a config clash. A
   different group is an independent durable queue. Pass cfg NULL for all
   defaults. */
int  arbitro_queue_subscribe(arbitro_client_t *c, const char *stream,
                             const arbitro_queue_cfg_t *cfg,
                             arbitro_msg_cb cb, void *userdata);

typedef struct {
    const uint8_t *name;
    uint16_t       name_len;
    uint64_t       fire_time_ms;
    uint64_t       fire_count;
} arbitro_cron_fire_t;

typedef int (*arbitro_cron_cb)(const arbitro_cron_fire_t *fire, void *user);

int  arbitro_cron_create(arbitro_client_t *c, const char *name,
                         const char *cron_expr, const char *tz,
                         arbitro_cron_cb cb, void *user);
int  arbitro_cron_delete(arbitro_client_t *c, const char *name);

int  arbitro_msg_ack(arbitro_msg_t *msg);
int  arbitro_msg_nack(arbitro_msg_t *msg);
int  arbitro_msg_nack_delay(arbitro_msg_t *msg, uint32_t delay_ms);
int  arbitro_msg_reply(arbitro_msg_t *msg, const uint8_t *payload,
                       uint32_t payload_len);

typedef struct {
    const char *subject_filter;
    uint64_t    max_msgs;
    uint64_t    max_bytes;
    uint64_t    max_age_ms;
    uint32_t    replicas;
    int         journal;
    uint64_t    idempotency_window_ms;
} arbitro_stream_cfg_t;

/* group is never sent empty: it falls back to name, then to the stream name.
   The broker rejects an empty group, so leaving it unset is safe but means
   this consumer gets its own queue rather than joining a shared one.

   *** BEHAVIOUR CHANGE — READ THIS IF YOU ARE UPGRADING ***
   A zero-initialised config is now a QUEUE: members of the group share the
   work and each message goes to exactly ONE of them. It used to be a fanout,
   where every member received every message. This aligns the C client with
   the TS, Go and Rust clients, all of which default to queue. Fanout is still
   available -- set `fanout` to non-zero -- it just has to be asked for.

   The old `uint8_t deliver_mode` field (0 = Fanout, 1 = Queue) was REMOVED
   rather than re-interpreted, so any call site that set it stops compiling
   instead of silently inverting its own semantics. Translate it as:
       deliver_mode = 1 (Queue)  ->  drop the line, that is the default now
       deliver_mode = 0 (Fanout) ->  fanout = 1 */
typedef struct {
    const char *name;
    const char *filter;
    const char *group;
    int         ack_policy;
    int         fanout;         /* 0 = shared queue (default), 1 = all members get every message */
    uint32_t    max_inflight;   /* clamped to the u16 wire field */
    uint32_t    ack_wait_ms;
    /* RESERVED — not on the wire, has no effect.
       No client (Rust, Go, TS, C) serialises a delivery cap: the
       CreateConsumer body carries exactly 11 fields and this is not one
       of them. The broker's nearest primitive, `max_nack`, counts only
       nack-driven redeliveries (never ack_wait timeouts) and is itself
       inert — exceeding it redelivers instead of dropping, because the
       broker-native DLQ is not implemented. Kept only so existing
       callers still compile; setting it is a no-op. */
    uint32_t    max_deliver;
    uint8_t     deliver_policy;
    uint64_t    start_seq;      /* required when deliver_policy is ByStartSeq */
    const arbitro_subject_limit_t *subject_limits;
    uint32_t    subject_limit_count;
} arbitro_consumer_cfg_t;

int  arbitro_stream_create(arbitro_client_t *c, const char *name,
                           const arbitro_stream_cfg_t *cfg,
                           uint32_t *out_stream_id);
int  arbitro_stream_delete(arbitro_client_t *c, const char *name, int keep_data);
int  arbitro_stream_purge(arbitro_client_t *c, const char *name,
                          uint64_t *out_purged);
int  arbitro_subject_drain(arbitro_client_t *c, const char *stream,
                           const char *subject_pattern, uint64_t *out_drained);
int  arbitro_message_delete(arbitro_client_t *c, const char *stream, uint64_t seq);

int  arbitro_consumer_create(arbitro_client_t *c, const char *stream,
                             const arbitro_consumer_cfg_t *cfg,
                             uint32_t *out_consumer_id);
int  arbitro_consumer_delete(arbitro_client_t *c, const char *stream,
                             const char *consumer);
int  arbitro_pause_consumer(arbitro_client_t *c, const char *stream,
                            const char *consumer);
int  arbitro_resume_consumer(arbitro_client_t *c, const char *stream,
                             const char *consumer);
int  arbitro_get_pending(arbitro_client_t *c, uint32_t consumer_id,
                         uint64_t *out_pending);

typedef struct {
    uint8_t *buf;
    uint16_t subject_len;
    uint16_t reply_len;
    uint32_t data_len;
    uint64_t seq;
    uint32_t consumer_id;
    uint32_t sub_id;
} arbitro_msg_owned_t;

int  arbitro_msg_copy(const arbitro_msg_t *msg, arbitro_msg_owned_t *out);
void arbitro_msg_owned_free(arbitro_msg_owned_t *m);

typedef struct {
    uint64_t publishes_sent;
    uint64_t batch_entries_sent;
    uint64_t deliveries_recv;
    uint64_t acks_sent;
    uint64_t nacks_sent;
    uint64_t reconnects;
    uint64_t batch_frames_recv;
    uint64_t requests_sent;
    uint64_t replies_recv;
    uint64_t publish_errors;
    uint64_t last_pong_rtt_ns;  /* 0 = no pong observed yet */
    uint32_t active_subs;
    uint32_t pending_requests;
    uint64_t acks_deferred;     /* recorded into the ackrel hot tier */
    uint64_t acks_confirmed;    /* purged after AckStateRep/AckBatchResp */
    uint64_t acks_expired;      /* server reported below_retention */
    /* Cold-tier (ack durability across process restart) is out of scope
       for this client — see README-ackrel.md. */
} arbitro_metrics_t;

void arbitro_client_metrics(const arbitro_client_t *c, arbitro_metrics_t *out);

typedef struct {
    uint32_t active_subs;
    uint32_t pending_slots_in_use;
    uint32_t waiters_in_use;
    uint32_t stream_cache_entries;
    uint32_t wr_buffered_bytes;
    uint32_t ack_pending;
} arbitro_stats_t;

void arbitro_client_stats(const arbitro_client_t *c, arbitro_stats_t *out);

int  arbitro_client_ping(arbitro_client_t *c);
int  arbitro_client_set_nonblock(arbitro_client_t *c, int enable);
int  arbitro_client_fd(const arbitro_client_t *c);
int  arbitro_stream_upsert(arbitro_client_t *c, const char *name,
                           const arbitro_stream_cfg_t *cfg,
                           uint32_t *out_stream_id);
int  arbitro_consumer_upsert(arbitro_client_t *c, const char *stream,
                              const arbitro_consumer_cfg_t *cfg,
                              uint32_t *out_consumer_id);
int  arbitro_resolve_stream_id(arbitro_client_t *c, const char *stream_name,
                               uint32_t *out_stream_id);

typedef struct {
    uint32_t stream_id;
} arbitro_stream_info_t;

typedef struct {
    uint32_t consumer_id;
} arbitro_consumer_info_t;

int  arbitro_stream_info(arbitro_client_t *c, const char *name,
                         arbitro_stream_info_t *out);
int  arbitro_stream_list(arbitro_client_t *c, arbitro_stream_info_t *out,
                         size_t cap, size_t *out_n);
int  arbitro_consumer_info(arbitro_client_t *c, const char *stream,
                           const char *consumer,
                           arbitro_consumer_info_t *out);
int  arbitro_consumer_list(arbitro_client_t *c, const char *stream,
                           arbitro_consumer_info_t *out, size_t cap,
                           size_t *out_n);
int  arbitro_stream_exists(arbitro_client_t *c, const char *name);
int  arbitro_consumer_exists(arbitro_client_t *c, const char *stream, const char *consumer);

typedef struct {
    const uint8_t *key;
    uint16_t       key_len;
    const uint8_t *val;
    uint32_t       val_len;
} arbitro_header_t;

int  arbitro_publish_with_headers(arbitro_client_t *c, uint32_t stream_id,
                                  const uint8_t *subject, uint16_t subject_len,
                                  const arbitro_header_t *headers, size_t header_count,
                                  const uint8_t *payload, uint32_t payload_len);
int  arbitro_publish_with_headers_sync(arbitro_client_t *c, uint32_t stream_id,
                                       const uint8_t *subject, uint16_t subject_len,
                                       const arbitro_header_t *headers, size_t header_count,
                                       const uint8_t *payload, uint32_t payload_len,
                                       uint64_t *out_seq);
int  arbitro_publish_delayed(arbitro_client_t *c, uint32_t stream_id,
                             const uint8_t *subject, uint16_t subject_len,
                             const uint8_t *payload, uint32_t payload_len,
                             uint64_t delay_ms);
int  arbitro_publish_delayed_sync(arbitro_client_t *c, uint32_t stream_id,
                                  const uint8_t *subject, uint16_t subject_len,
                                  const uint8_t *payload, uint32_t payload_len,
                                  uint64_t delay_ms, uint64_t *out_seq);

int  arbitro_request(arbitro_client_t *c, const char *service,
                     const char *method,
                     const uint8_t *payload, uint32_t payload_len,
                     uint32_t timeout_ms,
                     uint8_t *out_buf, uint32_t out_cap, uint32_t *out_len);
int  arbitro_request_with_id(arbitro_client_t *c, const char *service,
                             const char *method,
                             const uint8_t *msg_id, uint16_t msg_id_len,
                             const uint8_t *payload, uint32_t payload_len,
                             uint32_t timeout_ms,
                             uint8_t *out_buf, uint32_t out_cap,
                             uint32_t *out_len);

typedef struct arbitro_service arbitro_service_t;

/**
 * Incoming service request. Read-only view — the framework manages
 * ack/nack/reply based on the handler's return value.
 */
typedef struct arbitro_request {
    const uint8_t *subject;
    uint16_t       subject_len;
    const uint8_t *payload;
    uint32_t       payload_len;
    int            has_reply;    /* non-zero if the requester expects a reply */
    uint64_t       seq;
    uint32_t       consumer_id;
} arbitro_request_t;

/**
 * Handler for incoming service requests.
 *
 * The handler writes any reply payload into `out_buf` (up to `out_cap` bytes)
 * and sets `*out_len` to the number of bytes written.
 *
 * Return value:
 *   ARBITRO_OK (0)      — framework replies (if `has_reply` and `*out_len > 0`)
 *                          and acks the delivery.
 *   negative error code — framework nacks the delivery for redelivery.
 *
 * The framework guarantees exactly one ack or nack per invocation. The handler
 * must not call arbitro_msg_ack/nack/reply — that responsibility now belongs
 * to the framework.
 */
typedef int (*arbitro_svc_handler)(const arbitro_request_t *req,
                                   uint8_t *out_buf, uint32_t out_cap,
                                   uint32_t *out_len, void *userdata);

int  arbitro_service_create(arbitro_client_t *c, const char *name,
                            uint32_t max_inflight,
                            arbitro_service_t **out_svc);
int  arbitro_service_handle(arbitro_service_t *svc, const char *method,
                            arbitro_svc_handler handler, void *userdata);
void arbitro_service_destroy(arbitro_service_t *svc);
const char *arbitro_service_name(const arbitro_service_t *svc);
uint32_t    arbitro_service_stream_id(const arbitro_service_t *svc);

int  arbitro_service_request(arbitro_service_t *svc,
                             const char *target_service, const char *method,
                             const uint8_t *payload, uint32_t len,
                             uint32_t timeout_ms,
                             uint8_t *out, uint32_t cap, uint32_t *out_len);
int  arbitro_service_send(arbitro_service_t *svc,
                          const char *target, const char *method,
                          const uint8_t *payload, uint32_t len);

#ifdef __cplusplus
}
#endif

#endif /* ARBITRO_H */
