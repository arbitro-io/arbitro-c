#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "arbitro/arbitro.h"

static void on_add(arbitro_msg_t *msg, void *ud) {
    int32_t a, b, result;
    uint8_t out[16];
    (void)ud;

    if (msg->data_len < 8) {
        arbitro_msg_reply(msg, (const uint8_t *)"err", 3);
        return;
    }

    memcpy(&a, msg->data, 4);
    memcpy(&b, msg->data + 4, 4);
    result = a + b;
    memcpy(out, &result, 4);
    arbitro_msg_reply(msg, out, 4);
}

int main(void) {
    arbitro_client_t *c;
    arbitro_service_t *svc;
    int rc;

    rc = arbitro_client_connect("127.0.0.1", ARBITRO_DEFAULT_PORT, NULL, &c);
    if (rc != ARBITRO_OK) {
        fprintf(stderr, "connect: %s\n", arbitro_err_str(rc));
        return 1;
    }

    rc = arbitro_service_create(c, "calc", 10, &svc);
    if (rc != ARBITRO_OK) {
        fprintf(stderr, "service_create: %s\n", arbitro_err_str(rc));
        arbitro_client_close(c);
        return 1;
    }

    rc = arbitro_service_handle(svc, "add", on_add, NULL);
    if (rc != ARBITRO_OK) {
        fprintf(stderr, "service_handle: %s\n", arbitro_err_str(rc));
        arbitro_service_destroy(svc);
        arbitro_client_close(c);
        return 1;
    }

    printf("calc service running. Send requests to _svc.calc.add\n");
    printf("Payload: two int32_t values (8 bytes). Reply: one int32_t (4 bytes).\n");

    arbitro_client_run(c);

    arbitro_service_destroy(svc);
    arbitro_client_close(c);
    return 0;
}
