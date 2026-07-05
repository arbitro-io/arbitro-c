#include <stdio.h>
#include <string.h>
#include "arbitro/arbitro.h"

static int msg_count = 0;

static void on_message(arbitro_msg_t *msg, void *userdata) {
    (void)userdata;
    printf("recv [%.*s]: %.*s\n",
           (int)msg->subject_len, msg->subject,
           (int)msg->data_len, msg->data);
    arbitro_msg_ack(msg);
    msg_count++;
    if (msg_count >= 10)
        arbitro_client_stop(msg->client);
}

int main(void) {
    arbitro_client_t *c;
    uint32_t stream_id, consumer_id;
    arbitro_stream_cfg_t scfg = {0};
    arbitro_consumer_cfg_t ccfg = {0};
    int rc, i;

    rc = arbitro_client_connect("127.0.0.1", ARBITRO_DEFAULT_PORT, NULL, &c);
    if (rc != ARBITRO_OK) {
        fprintf(stderr, "connect: %s\n", arbitro_err_str(rc));
        return 1;
    }

    scfg.subject_filter = "orders.>";
    rc = arbitro_stream_upsert(c, "orders", &scfg, &stream_id);
    if (rc != ARBITRO_OK) {
        fprintf(stderr, "stream_create: %s\n", arbitro_err_str(rc));
        arbitro_client_close(c);
        return 1;
    }
    printf("stream_id = %u\n", stream_id);

    ccfg.name = "workers";
    ccfg.filter = "orders.>";
    ccfg.ack_policy = ARBITRO_ACK_EXPLICIT;
    ccfg.max_inflight = 100;
    rc = arbitro_consumer_upsert(c, "orders", &ccfg, &consumer_id);
    if (rc != ARBITRO_OK) {
        fprintf(stderr, "consumer_create: %s\n", arbitro_err_str(rc));
        arbitro_client_close(c);
        return 1;
    }
    printf("consumer_id = %u\n", consumer_id);

    rc = arbitro_subscribe(c, stream_id, consumer_id, on_message, NULL);
    if (rc != ARBITRO_OK) {
        fprintf(stderr, "subscribe: %s\n", arbitro_err_str(rc));
        arbitro_client_close(c);
        return 1;
    }

    for (i = 0; i < 10; i++) {
        char payload[32];
        int n = snprintf(payload, sizeof(payload), "message %d", i);
        rc = arbitro_publish(c, stream_id,
                             (const uint8_t *)"orders.new", 10,
                             (const uint8_t *)payload, (uint32_t)n);
        if (rc != ARBITRO_OK) {
            fprintf(stderr, "publish: %s\n", arbitro_err_str(rc));
            break;
        }
    }

    rc = arbitro_client_run(c);
    printf("run exited: %s (received %d msgs)\n", arbitro_err_str(rc), msg_count);

    arbitro_client_close(c);
    return 0;
}
