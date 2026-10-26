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

    /* Test metrics (Expanded to 136 bytes for Phase 7) */
    assert(sizeof(vr_reactor_metrics_t) == 136);
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
    vr_reactor_metric_inc_sse_streams(r);
    vr_reactor_metric_inc_sse_events_out(r);
    vr_reactor_metric_inc_sse_heartbeats(r);
    vr_reactor_metric_inc_sse_dropped(r);
    vr_reactor_metric_inc_restarts(r);
    vr_reactor_metric_inc_isolated_faults(r);

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
    assert(metrics.sse_streams_total == 1);
    assert(metrics.sse_events_out_total == 1);
    assert(metrics.sse_heartbeats_total == 1);
    assert(metrics.sse_dropped_backpressure_total == 1);
    assert(metrics.supervisor_restarts_total == 1);
    assert(metrics.supervisor_isolated_faults_total == 1);

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

    /* 8. Phase 6 Server-Sent Events (SSE) Tests */
    printf("[test_vr_reactor] 8. Testing Phase 6 SSE formatting and ref-counted messages...\n");

    /* 8a. Single-line formatting with id and event */
    char sse_buf[512];
    int slen = vr_sse_format_event("42", 2, "greeting", 8, "hello world", 11, sse_buf, sizeof(sse_buf));
    assert(slen > 0);
    sse_buf[slen] = '\0';
    assert(strcmp(sse_buf, "id: 42\nevent: greeting\ndata: hello world\n\n") == 0);

    /* 8b. Multi-line formatting with '\n' */
    slen = vr_sse_format_event(NULL, 0, NULL, 0, "line1\nline2\nline3", 17, sse_buf, sizeof(sse_buf));
    assert(slen > 0);
    sse_buf[slen] = '\0';
    assert(strcmp(sse_buf, "data: line1\ndata: line2\ndata: line3\n\n") == 0);

    /* 8c. Multi-line formatting with '\r\n' */
    slen = vr_sse_format_event(NULL, 0, NULL, 0, "first\r\nsecond\r\n", 15, sse_buf, sizeof(sse_buf));
    assert(slen > 0);
    sse_buf[slen] = '\0';
    assert(strcmp(sse_buf, "data: first\ndata: second\n\n") == 0);

    /* 8d. CRLF Injection stripping in id: and event: */
    slen = vr_sse_format_event("id\r\n1", 5, "ev\nent", 6, "safe", 4, sse_buf, sizeof(sse_buf));
    assert(slen > 0);
    sse_buf[slen] = '\0';
    assert(strcmp(sse_buf, "id: id1\nevent: event\ndata: safe\n\n") == 0);

    /* 8e. Ref-counted broadcast message */
    vr_ws_msg_t *sse_msg = vr_sse_msg_create("100", 3, "msg", 3, "fanout test", 11, 2);
    assert(sse_msg != NULL);
    assert(vr_ws_msg_size(sse_msg) > 0);
    const uint8_t *sdata = vr_ws_msg_data(sse_msg);
    assert(sdata != NULL);
    assert(memcmp(sdata, "id: 100\nevent: msg\ndata: fanout test\n\n", vr_ws_msg_size(sse_msg)) == 0);

    /* Ref count decrement and free */
    vr_ws_msg_unref(sse_msg);
    vr_ws_msg_unref(sse_msg);

    /* 9. Testing Phase 7 Supervision: Heartbeats, Fault Mask & Shard FD Purge */
    printf("[test_vr_reactor] 9. Testing Phase 7 heartbeats, fault mask, and shard FD purge...\n");
    vr_reactor_t *sup_r = vr_reactor_create();
    assert(sup_r != NULL);

    /* 9a. Heartbeats */
    assert(vr_reactor_get_reactor_beat(sup_r) == 0);
    assert(vr_reactor_get_broker_beat(sup_r) == 0);
    assert(vr_reactor_get_worker_beat(sup_r, 0) == 0);

    vr_reactor_beat_reactor(sup_r, 123456ULL);
    vr_reactor_beat_broker(sup_r, 234567ULL);
    vr_reactor_beat_worker(sup_r, 2, 345678ULL);

    assert(vr_reactor_get_reactor_beat(sup_r) == 123456ULL);
    assert(vr_reactor_get_broker_beat(sup_r) == 234567ULL);
    assert(vr_reactor_get_worker_beat(sup_r, 2) == 345678ULL);
    assert(vr_reactor_get_worker_beat(sup_r, 0) == 0);

    /* 9b. Fault Reporting & Consumption */
    assert(vr_reactor_consume_fault_mask(sup_r) == 0);
    vr_reactor_report_worker_fault(sup_r, 3);
    vr_reactor_report_broker_fault(sup_r);

    uint64_t mask = vr_reactor_consume_fault_mask(sup_r);
    assert((mask & (1ULL << 3)) != 0);
    assert((mask & (1ULL << 63)) != 0);
    /* Should be cleared upon consumption */
    assert(vr_reactor_consume_fault_mask(sup_r) == 0);

    /* 9c. Shard FD Ownership & Purge */
    int s1 = socket(AF_INET, SOCK_STREAM, 0);
    int s2 = socket(AF_INET, SOCK_STREAM, 0);
    int s3 = socket(AF_INET, SOCK_STREAM, 0);
    assert(s1 >= 0 && s2 >= 0 && s3 >= 0);

    vr_reactor_add(sup_r, s1, VR_REACTOR_IN, 1);
    vr_reactor_add(sup_r, s2, VR_REACTOR_IN, 1);
    vr_reactor_add(sup_r, s3, VR_REACTOR_IN, 1);

    vr_reactor_set_fd_owner(sup_r, s1, 1);
    vr_reactor_set_fd_owner(sup_r, s2, 1);
    vr_reactor_set_fd_owner(sup_r, s3, 2);

    assert(vr_reactor_get_fd_owner(sup_r, s1) == 1);
    assert(vr_reactor_get_fd_owner(sup_r, s2) == 1);
    assert(vr_reactor_get_fd_owner(sup_r, s3) == 2);

    vr_reactor_metric_inc_open(sup_r);
    vr_reactor_metric_inc_open(sup_r);
    vr_reactor_metric_inc_open(sup_r);

    int purged = vr_reactor_purge_worker_fds(sup_r, 1);
    assert(purged == 2);
    assert(vr_reactor_get_fd_owner(sup_r, s1) == -1);
    assert(vr_reactor_get_fd_owner(sup_r, s2) == -1);
    assert(vr_reactor_get_fd_owner(sup_r, s3) == 2);

    /* Sockets s1 and s2 were closed by purge. Calling close_fd_for_worker must safely fail without double-close */
    assert(vr_reactor_close_fd_for_worker(sup_r, s1, 1) == -1);
    assert(vr_reactor_close_fd_for_worker(sup_r, s2, 1) == -1);

    /* s3 is owned by worker 2. Calling close_fd_for_worker with wrong worker (1) must fail */
    assert(vr_reactor_close_fd_for_worker(sup_r, s3, 1) == -1);
    assert(vr_reactor_get_fd_owner(sup_r, s3) == 2);
    /* Calling close_fd_for_worker with correct worker (2) succeeds */
    assert(vr_reactor_close_fd_for_worker(sup_r, s3, 2) == 0);
    assert(vr_reactor_get_fd_owner(sup_r, s3) == -1);

    /* Test least-connections worker assignment & routing */
    assert(vr_reactor_get_worker_conns(sup_r, 0) == 0);
    assert(vr_reactor_get_worker_conns(sup_r, 1) == 0);
    assert(vr_reactor_get_worker_conns(sup_r, 2) == 0);

    /* Assignment 1: worker 0 gets fd 100 */
    int w_a = vr_reactor_assign_worker(sup_r, 100, 3);
    assert(w_a == 0);
    assert(vr_reactor_get_worker_conns(sup_r, 0) == 1);
    assert(vr_reactor_route_fd(sup_r, 100, 3) == 0);

    /* Assignment 2: worker 1 gets fd 101 */
    int w_b = vr_reactor_assign_worker(sup_r, 101, 3);
    assert(w_b == 1);
    assert(vr_reactor_get_worker_conns(sup_r, 1) == 1);
    assert(vr_reactor_route_fd(sup_r, 101, 3) == 1);

    /* Assignment 3: worker 2 gets fd 102 */
    int w_c = vr_reactor_assign_worker(sup_r, 102, 3);
    assert(w_c == 2);
    assert(vr_reactor_get_worker_conns(sup_r, 2) == 1);
    assert(vr_reactor_route_fd(sup_r, 102, 3) == 2);

    /* Assignment 4: tie broken by lowest index (worker 0) */
    int w_d = vr_reactor_assign_worker(sup_r, 103, 3);
    assert(w_d == 0);
    assert(vr_reactor_get_worker_conns(sup_r, 0) == 2);

    /* Release connection from worker 0 */
    vr_reactor_release_worker_conn(sup_r, 0);
    assert(vr_reactor_get_worker_conns(sup_r, 0) == 1);

    /* Untracked FD routing: assign_worker and route_fd MUST agree */
    int untracked_fd = VR_MAX_TRACKED_FDS + 7;
    int assigned_untracked = vr_reactor_assign_worker(sup_r, untracked_fd, 4);
    assert(assigned_untracked == untracked_fd % 4);
    assert(vr_reactor_route_fd(sup_r, untracked_fd, 4) == assigned_untracked);

    /* 10. Testing Phase 8: PUB command parsing, msg ref, room sequences */
    printf("[test_vr_reactor] 10. Testing PUB parsing, msg ref, and room sequences...\n");
    int out_room = -1;
    size_t out_offset = 0;

    /* 10a. WebSocket PUB with explicit room */
    const char pub1[] = "PUB 42 hello world";
    assert(vr_ws_parse_pub_command(pub1, strlen(pub1), 0, &out_room, &out_offset) == 1);
    assert(out_room == 42);
    assert(out_offset == 7);
    assert(memcmp(pub1 + out_offset, "hello world", 11) == 0);

    /* 10b. WebSocket PUB with default room (no room integer) */
    const char pub2[] = "PUB hello world";
    assert(vr_ws_parse_pub_command(pub2, strlen(pub2), 0, &out_room, &out_offset) == 1);
    assert(out_room == 0);
    assert(out_offset == 4);
    assert(memcmp(pub2 + out_offset, "hello world", 11) == 0);

    /* 10c. SSE PUB with explicit room */
    const char pub_sse1[] = "PUB_SSE 100 {\"temp\": 23.5}";
    assert(vr_ws_parse_pub_command(pub_sse1, strlen(pub_sse1), 1, &out_room, &out_offset) == 1);
    assert(out_room == 100);
    assert(out_offset == 12);
    assert(memcmp(pub_sse1 + out_offset, "{\"temp\": 23.5}", 14) == 0);

    /* 10d. Non-pub command */
    const char not_pub[] = "HELLO world";
    assert(vr_ws_parse_pub_command(not_pub, strlen(not_pub), 0, &out_room, &out_offset) == 0);

    /* 10e. Message ref counting */
    vr_ws_msg_t *test_ref_msg = vr_ws_msg_create(1, "payload", 7, 1);
    assert(test_ref_msg != NULL);
    vr_ws_msg_ref(test_ref_msg);
    /* Ref count is 2: first unref leaves it allocated, second frees */
    vr_ws_msg_unref(test_ref_msg);
    vr_ws_msg_unref(test_ref_msg);

    /* 10f. Room sequence numbers */
    assert(vr_reactor_next_room_seq(sup_r, 42) == 1ULL);
    assert(vr_reactor_next_room_seq(sup_r, 42) == 2ULL);
    assert(vr_reactor_next_room_seq(sup_r, 43) == 1ULL);
    assert(vr_reactor_next_room_seq(sup_r, 42) == 3ULL);

    /* 11. Testing TLA+ Remediation: Fault-mask routing, atomic close, saturating dec, SSE multiline */
    printf("[test_vr_reactor] 11. Testing TLA+ remediation fixes...\n");

    /* 11a. Assignment skips faulted workers */
    vr_reactor_t *rem_r = vr_reactor_create();
    assert(rem_r != NULL);
    assert(vr_reactor_get_worker_conns(rem_r, 0) == 0);
    assert(vr_reactor_get_worker_conns(rem_r, 1) == 0);

    /* Fault worker 0: assignment must skip worker 0 and choose worker 1 */
    vr_reactor_report_worker_fault(rem_r, 0);
    int assigned_w = vr_reactor_assign_worker(rem_r, 200, 2);
    assert(assigned_w == 1);
    assert(vr_reactor_get_worker_conns(rem_r, 1) == 1);
    assert(vr_reactor_get_worker_conns(rem_r, 0) == 0);

    /* After fault cleared, worker 0 is eligible and chosen because load is 0 < 1 */
    uint64_t consumed = vr_reactor_consume_fault_mask(rem_r);
    assert(consumed == (1ULL << 0));
    assigned_w = vr_reactor_assign_worker(rem_r, 201, 2);
    assert(assigned_w == 0);
    assert(vr_reactor_get_worker_conns(rem_r, 0) == 1);

    /* 11b. Atomic close_fd_for_worker updates both worker_conns and active_connections */
    vr_reactor_metrics_t rem_m;
    vr_reactor_get_metrics(rem_r, &rem_m);
    assert(rem_m.active_connections == 0);

    /* Simulate tracking 1 active conn */
    vr_reactor_metric_inc_open(rem_r);
    vr_reactor_get_metrics(rem_r, &rem_m);
    assert(rem_m.active_connections == 1);

    int test_s = socket(AF_INET, SOCK_STREAM, 0);
    assert(test_s >= 0);
    vr_reactor_set_fd_owner(rem_r, test_s, 0);
    /* Close for worker 0 atomically decrements worker 0 load and active_conns */
    assert(vr_reactor_close_fd_for_worker(rem_r, test_s, 0) == 0);
    assert(vr_reactor_get_worker_conns(rem_r, 0) == 0);
    vr_reactor_get_metrics(rem_r, &rem_m);
    assert(rem_m.active_connections == 0);

    /* Duplicate close fails and does NOT underflow active_connections */
    assert(vr_reactor_close_fd_for_worker(rem_r, test_s, 0) == -1);
    vr_reactor_get_metrics(rem_r, &rem_m);
    assert(rem_m.active_connections == 0);

    /* 11c. Saturating decrement test on active_connections */
    vr_reactor_metric_dec_open(rem_r);
    vr_reactor_get_metrics(rem_r, &rem_m);
    assert(rem_m.active_connections == 0);

    /* 11d. SSE multi-line data with 250 newlines (previously failed due to under-allocation) */
    char sse_data[256];
    for (int i = 0; i < 250; i++) {
        sse_data[i] = '\n';
    }
    sse_data[250] = '\0';
    vr_ws_msg_t *sse_multi_msg = vr_sse_msg_create("id1", 3, "ev1", 3, sse_data, 250, 1);
    assert(sse_multi_msg != NULL);
    assert(sse_multi_msg->total_len > 250 * 6);
    vr_ws_msg_unref(sse_multi_msg);

    vr_reactor_destroy(rem_r);
    vr_reactor_destroy(sup_r);

    printf("[test_vr_reactor] All native VR reactor unit tests passed successfully!\n");
    return 0;
}
