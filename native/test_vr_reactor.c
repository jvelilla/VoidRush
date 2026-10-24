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

    /* Test metrics (88 bytes) */
    assert(sizeof(vr_reactor_metrics_t) == 88);
    vr_reactor_metric_inc_accept(r);
    vr_reactor_metric_inc_open(r);
    vr_reactor_metric_inc_events(r, 5);
    vr_reactor_metric_inc_stale(r);
    vr_reactor_metric_add_bytes_in(r, 1024);
    vr_reactor_metric_add_bytes_out(r, 2048);
    vr_reactor_metric_inc_idle_timeouts(r);
    vr_reactor_metric_inc_ws_upgrades(r);
    vr_reactor_metric_inc_ws_frames_in(r);
    vr_reactor_metric_inc_ws_frames_out(r);
    vr_reactor_metric_inc_ws_dropped(r);

    vr_reactor_metrics_t metrics;
    vr_reactor_get_metrics(r, &metrics);
    assert(metrics.accepts_total == 1);
    assert(metrics.active_connections == 1);
    assert(metrics.events_total == 5);
    assert(metrics.stale_events == 1);
    assert(metrics.bytes_in_total == 1024);
    assert(metrics.bytes_out_total == 2048);
    assert(metrics.idle_timeouts_total == 1);
    assert(metrics.ws_upgrades_total == 1);
    assert(metrics.ws_frames_in_total == 1);
    assert(metrics.ws_frames_out_total == 1);
    assert(metrics.ws_dropped_backpressure_total == 1);

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

    /* 4b. Phase 3 Non-blocking Read/Write, Clock, and Memmove Test */
    printf("[test_vr_reactor] 4b. Testing Phase 3 clock, memmove, and non-blocking I/O...\n");
    uint64_t t1 = vr_reactor_now_ms();
    assert(t1 > 0);
    usleep(2000); /* sleep 2ms */
    uint64_t t2 = vr_reactor_now_ms();
    assert(t2 >= t1);

    /* Test memmove */
    char test_mem[16] = "abcdefgh";
    vr_reactor_memmove(test_mem, test_mem + 3, 5); /* shift "defgh" to start */
    assert(memcmp(test_mem, "defgh", 5) == 0);

    /* Test read/write on a socketpair */
    int sp[2];
    res = socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sp);
    assert(res == 0);

    const char *echo_in = "Phase3_Echo_Payload";
    size_t echo_len = strlen(echo_in);
    int written = vr_reactor_write(sp[0], echo_in, echo_len);
    assert(written == (int)echo_len);

    char echo_out[64];
    memset(echo_out, 0, sizeof(echo_out));
    int read_bytes = vr_reactor_read(sp[1], echo_out, sizeof(echo_out));
    assert(read_bytes == (int)echo_len);
    assert(strcmp(echo_out, echo_in) == 0);

    /* Subsequent read on empty non-blocking socket should return WOULD_BLOCK */
    int wb = vr_reactor_read(sp[1], echo_out, sizeof(echo_out));
    assert(wb == VR_REACTOR_IO_WOULD_BLOCK);

    /* Closing write end triggers EOF on read end */
    close(sp[0]);
    int eof_res = vr_reactor_read(sp[1], echo_out, sizeof(echo_out));
    assert(eof_res == VR_REACTOR_IO_EOF);
    close(sp[1]);

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

    /* 7. Phase 5 WebSocket Crypto, Framing & Ref-Counted Buffers */
    printf("[test_vr_reactor] 7. Testing WebSocket SHA-1, Base64, framing, and unmasking...\n");

    /* 7a. RFC 6455 §1.3 Accept-Key Test Vector */
    const char *rfc_client_key = "dGhlIHNhbXBsZSBub25jZQ==";
    char accept_out[32];
    memset(accept_out, 0, sizeof(accept_out));
    int accept_len = vr_ws_compute_accept_key(rfc_client_key, strlen(rfc_client_key), accept_out);
    assert(accept_len == 28);
    assert(strcmp(accept_out, "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=") == 0);

    /* Test invalid inputs */
    assert(vr_ws_compute_accept_key(NULL, 24, accept_out) == -1);
    assert(vr_ws_compute_accept_key(rfc_client_key, 23, accept_out) == -1);
    assert(vr_ws_compute_accept_key(rfc_client_key, 25, accept_out) == -1);
    assert(vr_ws_compute_accept_key(rfc_client_key, 24, NULL) == -1);

    /* 7b. Fast 64-bit XOR unmasking test (aligned and unaligned) */
    uint8_t mask_key[4] = {0x12, 0x34, 0x56, 0x78};
    uint8_t test_data[65];
    uint8_t orig_data[65];
    for (size_t i = 0; i < sizeof(test_data); i++) {
        test_data[i] = (uint8_t)(i * 3 + 7);
        orig_data[i] = test_data[i];
    }
    /* Unmask once -> mutates */
    vr_ws_unmask(test_data, sizeof(test_data), mask_key);
    assert(memcmp(test_data, orig_data, sizeof(test_data)) != 0);
    /* Unmask twice with same key -> restores original */
    vr_ws_unmask(test_data, sizeof(test_data), mask_key);
    assert(memcmp(test_data, orig_data, sizeof(test_data)) == 0);

    /* Test edge lengths for unmask: 0, 1, 3, 7, 8, 16 */
    size_t edge_lens[] = {0, 1, 3, 7, 8, 16};
    for (size_t e = 0; e < sizeof(edge_lens) / sizeof(edge_lens[0]); e++) {
        size_t elen = edge_lens[e];
        vr_ws_unmask(test_data, elen, mask_key);
        vr_ws_unmask(test_data, elen, mask_key);
        assert(memcmp(test_data, orig_data, elen) == 0);
    }

    /* 7c. Server unmasked frame header encoding */
    uint8_t hdr[10];
    int hlen;
    /* Small (<126) */
    hlen = vr_ws_encode_frame_header(0x81, 10, hdr);
    assert(hlen == 2);
    assert(hdr[0] == 0x81);
    assert(hdr[1] == 10);

    /* Medium (126..65535) */
    hlen = vr_ws_encode_frame_header(0x82, 500, hdr);
    assert(hlen == 4);
    assert(hdr[0] == 0x82);
    assert(hdr[1] == 126);
    assert(hdr[2] == (500 >> 8));
    assert(hdr[3] == (500 & 0xFF));

    /* Large (>65535) */
    hlen = vr_ws_encode_frame_header(0x81, 70000, hdr);
    assert(hlen == 10);
    assert(hdr[0] == 0x81);
    assert(hdr[1] == 127);
    assert(hdr[9] == (70000 & 0xFF));

    /* 7d. Pre-framed ref-counted message buffer */
    const char *msg_text = "VoidRush WebSockets";
    size_t msg_len = strlen(msg_text);
    vr_ws_msg_t *ws_msg = vr_ws_msg_create(0x1, msg_text, msg_len, 2);
    assert(ws_msg != NULL);
    assert(vr_ws_msg_size(ws_msg) == 2 + msg_len);
    const uint8_t *mdata = vr_ws_msg_data(ws_msg);
    assert(mdata != NULL);
    assert(mdata[0] == 0x81);
    assert(mdata[1] == (uint8_t)msg_len);
    assert(memcmp(mdata + 2, msg_text, msg_len) == 0);

    /* First unref decrements ref_count to 1 */
    vr_ws_msg_unref(ws_msg);
    /* Second unref frees the buffer */
    vr_ws_msg_unref(ws_msg);

    printf("[test_vr_reactor] All native VR reactor unit tests passed successfully!\n");
    return 0;
}
