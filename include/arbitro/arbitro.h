#ifndef ARBITRO_H
#define ARBITRO_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#define ARBITRO_VERSION_MAJOR 0
#define ARBITRO_VERSION_MINOR 1
#define ARBITRO_VERSION_PATCH 1

#define ARBITRO_DEFAULT_PORT       9898
#define ARBITRO_MAX_SUBJECT_LEN    255
#define ARBITRO_MAX_MSG_ID_LEN     64
#define ARBITRO_DEFAULT_FRAME_BUF  (64 * 1024)
#define ARBITRO_ACK_BATCH_MAX      256

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
    uint32_t          subject_hash;
} arbitro_msg_t;

typedef void (*arbitro_msg_cb)(arbitro_msg_t *msg, void *userdata);

typedef struct arbitro_opts {
    uint32_t connect_timeout_ms;
    uint32_t request_timeout_ms;
    uint32_t frame_buf_size;
    int      reconnect;
    uint32_t reconnect_max;
    uint32_t reconnect_delay_ms;
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

int  arbitro_publish(arbitro_client_t *c, uint32_t stream_id,
                     const uint8_t *subject, uint16_t subject_len,
                     const uint8_t *payload, uint32_t payload_len);
int  arbitro_publish_sync(arbitro_client_t *c, uint32_t stream_id,
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

int  arbitro_subscribe(arbitro_client_t *c, uint32_t stream_id,
                       uint32_t consumer_id, arbitro_msg_cb cb, void *userdata);
int  arbitro_subscribe_filter(arbitro_client_t *c, uint32_t stream_id,
                              uint32_t consumer_id,
                              const uint8_t *filter, uint16_t filter_len,
                              arbitro_msg_cb cb, void *userdata);
int  arbitro_unsubscribe(arbitro_client_t *c, uint32_t consumer_id);

int  arbitro_msg_ack(arbitro_msg_t *msg);
int  arbitro_msg_nack(arbitro_msg_t *msg);
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

typedef struct {
    const uint8_t *pattern;
    uint16_t       pattern_len;
    uint32_t       limit;
} arbitro_subject_limit_t;

typedef struct {
    const char *name;
    const char *filter;
    const char *group;
    int         ack_policy;
    uint32_t    max_inflight;
    uint32_t    ack_wait_ms;
    uint32_t    max_deliver;
    uint8_t     deliver_policy;
    uint8_t     deliver_mode;
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

typedef struct {
    uint8_t *buf;
    uint16_t subject_len;
    uint16_t reply_len;
    uint32_t data_len;
    uint64_t seq;
    uint32_t consumer_id;
    uint32_t subject_hash;
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
    uint32_t active_subs;
    uint32_t pending_requests;
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
int  arbitro_publish_delayed(arbitro_client_t *c, uint32_t stream_id,
                             const uint8_t *subject, uint16_t subject_len,
                             const uint8_t *payload, uint32_t payload_len,
                             uint64_t delay_ms);

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
