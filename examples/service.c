#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "arbitro/arbitro.h"

static int on_add(const arbitro_request_t *req,
                  uint8_t *out_buf, uint32_t out_cap,
                  uint32_t *out_len, void *ud) {
    int32_t a, b, result;
    (void)ud;

    if (req->payload_len < 8 || out_cap < 4) {
        return ARBITRO_ERR_ARG;
    }

    memcpy(&a, req->payload, 4);
    memcpy(&b, req->payload + 4, 4);
    result = a + b;
    memcpy(out_buf, &result, 4);
    *out_len = 4;
    return ARBITRO_OK;
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
