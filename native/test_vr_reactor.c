#define _GNU_SOURCE
#include "vr_reactor_bridge.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>

#define TEST_PORT 19876

int main(void) {
    printf("[test_vr_reactor] Starting native VR reactor unit tests...\n");

    /* 1. Lifecycle & Wakeup Test */
    printf("[test_vr_reactor] 1. Testing reactor lifecycle, wakeup, and shutdown flag...\n");
    vr_reactor_t *r = vr_reactor_create();
    assert(r != NULL);
    assert(vr_reactor_is_shutting_down(r) == 0);

    /* Wakeup without pending user events */
    int res = vr_reactor_wakeup(r);
    assert(res == 0);

    vr_reactor_event_t events[16];
    int n = vr_reactor_wait(r, events, 16, 100);
    /* wake_fd should be consumed internally, returning 0 user events */
    assert(n == 0);

    /* Test metrics */
    vr_reactor_metric_inc_accept(r);
    vr_reactor_metric_inc_open(r);
    vr_reactor_metric_inc_events(r, 5);
    vr_reactor_metric_inc_stale(r);
    vr_reactor_metrics_t metrics;
    vr_reactor_get_metrics(r, &metrics);
    assert(metrics.accepts_total == 1);
    assert(metrics.active_connections == 1);
    assert(metrics.events_total == 5);
    assert(metrics.stale_events == 1);
    vr_reactor_metric_dec_open(r);
    vr_reactor_get_metrics(r, &metrics);
    assert(metrics.active_connections == 0);

    /* 2. TCP Listen Test */
    printf("[test_vr_reactor] 2. Testing vr_reactor_listen_tcp on port %d...\n", TEST_PORT);
    int listen_fd = vr_reactor_listen_tcp("127.0.0.1", TEST_PORT, 64);
    assert(listen_fd >= 0);

    /* Register listen_fd with generation 0 */
    res = vr_reactor_add(r, listen_fd, VR_REACTOR_IN, 0);
    assert(res == 0);

    /* 3. Client Connect & Accept Test */
    printf("[test_vr_reactor] 3. Testing client connection and accept...\n");
    int client_sock = socket(AF_INET, SOCK_STREAM, 0);
    assert(client_sock >= 0);

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(TEST_PORT);
    inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);

    res = connect(client_sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr));
    assert(res == 0);

    /* Wait for listen_fd readability */
    n = vr_reactor_wait(r, events, 16, 500);
    assert(n == 1);
    assert(events[0].fd == listen_fd);
    assert(events[0].generation == 0);
    assert((events[0].flags & VR_REACTOR_IN) != 0);

    /* Accept client */
    int client_fd = vr_reactor_accept_nonblocking(listen_fd);
    assert(client_fd >= 0);

    /* Subsequent accept should return WOULD_BLOCK (-1) */
    int drained = vr_reactor_accept_nonblocking(listen_fd);
    assert(drained == VR_REACTOR_IO_WOULD_BLOCK);

    /* Re-arm listen_fd (due to EPOLLONESHOT) */
    res = vr_reactor_mod(r, listen_fd, VR_REACTOR_IN, 0);
    assert(res == 0);

    /* 4. Client I/O Event & Drain Discard */
    printf("[test_vr_reactor] 4. Testing client I/O and vr_reactor_drain_discard...\n");
    uint32_t expected_gen = 42;
    res = vr_reactor_add(r, client_fd, VR_REACTOR_IN, expected_gen);
    assert(res == 0);

    /* Send payload from client */
    const char *msg = "voidrush_phase2_payload";
    ssize_t sent = send(client_sock, msg, strlen(msg), 0);
    assert(sent == (ssize_t)strlen(msg));

    /* Wait for client_fd readiness */
    n = vr_reactor_wait(r, events, 16, 500);
    assert(n == 1);
    assert(events[0].fd == client_fd);
    assert(events[0].generation == expected_gen);
    assert((events[0].flags & VR_REACTOR_IN) != 0);

    /* Drain discard test */
    int drain_res = vr_reactor_drain_discard(client_fd);
    assert(drain_res == VR_REACTOR_IO_WOULD_BLOCK);

    /* Close client side and check EOF detection on next drain */
    close(client_sock);
    /* Mod client_fd to re-arm */
    res = vr_reactor_mod(r, client_fd, VR_REACTOR_IN, expected_gen);
    assert(res == 0);

    n = vr_reactor_wait(r, events, 16, 500);
    assert(n == 1);
    assert(events[0].fd == client_fd);
    drain_res = vr_reactor_drain_discard(client_fd);
    assert(drain_res == VR_REACTOR_IO_EOF);

    /* 5. Atomic shutdown test */
    printf("[test_vr_reactor] 5. Testing atomic shutdown control...\n");
    vr_reactor_request_shutdown(r);
    assert(vr_reactor_is_shutting_down(r) == 1);

    /* 6. Clean teardown and disposal */
    printf("[test_vr_reactor] 6. Cleaning up sockets and reactor...\n");
    res = vr_reactor_close(client_fd);
    assert(res == 0);

    res = vr_reactor_close(listen_fd);
    assert(res == 0);

    vr_reactor_destroy(r);

    printf("[test_vr_reactor] All native VR reactor unit tests passed successfully!\n");
    return 0;
}
