#ifndef VR_REACTOR_BRIDGE_H
#define VR_REACTOR_BRIDGE_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Event bitmask flags */
#define VR_REACTOR_IN     0x01U
#define VR_REACTOR_OUT    0x02U
#define VR_REACTOR_ERR    0x04U
#define VR_REACTOR_HUP    0x08U

/* Non-blocking I/O status codes */
#define VR_REACTOR_IO_OK          1
#define VR_REACTOR_IO_EOF         0
#define VR_REACTOR_IO_WOULD_BLOCK -1
#define VR_REACTOR_IO_ERROR       -2

/* Exactly 12 bytes, matching Eiffel VR_REACTOR_EVENT_BUFFER offsets */
typedef struct {
    int32_t  fd;
    uint32_t generation;
    uint32_t flags;
} vr_reactor_event_t;

#if defined(__STDC_VERSION__) && __STDC_VERSION__ >= 201112L
_Static_assert(sizeof(vr_reactor_event_t) == 12, "vr_reactor_event_t must be 12 bytes");
#endif

typedef struct vr_reactor vr_reactor_t;

#define VR_MAX_WORKERS 63
#define VR_MAX_TRACKED_FDS 65536

/* Metrics Snapshot (Expanded to 136 bytes for Phase 7 supervision counters) */
typedef struct {
    uint64_t active_connections;
    uint64_t accepts_total;
    uint64_t events_total;
    uint64_t stale_events;
    uint64_t bytes_in_total;
    uint64_t bytes_out_total;
    uint64_t idle_timeouts_total;
    /* Phase 5 WebSocket metrics */
    uint64_t ws_upgrades_total;
    uint64_t ws_frames_in_total;
    uint64_t ws_frames_out_total;
    uint64_t ws_dropped_backpressure_total;
    /* Phase 6 SSE metrics */
    uint64_t sse_streams_total;
    uint64_t sse_events_out_total;
    uint64_t sse_heartbeats_total;
    uint64_t sse_dropped_backpressure_total;
    /* Phase 7 Supervision metrics */
    uint64_t supervisor_restarts_total;
    uint64_t supervisor_isolated_faults_total;
} vr_reactor_metrics_t;

#if defined(__STDC_VERSION__) && __STDC_VERSION__ >= 201112L
_Static_assert(sizeof(vr_reactor_metrics_t) == 136, "vr_reactor_metrics_t must be 136 bytes");
#endif

/* Lifecycle */
vr_reactor_t *vr_reactor_create(void);
void          vr_reactor_destroy(vr_reactor_t *r);

/* Atomic Shutdown Control */
void vr_reactor_request_shutdown(vr_reactor_t *r);
int  vr_reactor_is_shutting_down(const vr_reactor_t *r);

/* Interest management (mod/del are thread-safe across SCOOP workers) */
int vr_reactor_add(vr_reactor_t *r, int fd, uint32_t flags, uint32_t generation);
int vr_reactor_mod(vr_reactor_t *r, int fd, uint32_t flags, uint32_t generation);
int vr_reactor_del(vr_reactor_t *r, int fd);

/* Poll & Wakeup */
int vr_reactor_wait(vr_reactor_t *r, vr_reactor_event_t *events_out, int max_events, int timeout_ms);
int vr_reactor_wakeup(vr_reactor_t *r);

/* Socket Helpers */
int vr_reactor_listen_tcp(const char *host, int port, int backlog);
int vr_reactor_accept_nonblocking(int listen_fd);
int vr_reactor_set_nonblocking(int fd);
int vr_reactor_close(int fd);
int vr_reactor_shutdown_wr(int fd);
int vr_reactor_connect_tcp(const char *host, int port);
int vr_reactor_send(int fd, const char *buf, int len);

/* Non-blocking Data I/O (Phase 3)
 * Returns > 0 (bytes transferred), VR_REACTOR_IO_EOF (0),
 * VR_REACTOR_IO_WOULD_BLOCK (-1), or VR_REACTOR_IO_ERROR (-2)
 */
int vr_reactor_read(int fd, void *buf, size_t len);
int vr_reactor_write(int fd, const void *buf, size_t len);

/* Fast Monotonic Millisecond Clock (CLOCK_MONOTONIC_COARSE) */
uint64_t vr_reactor_now_ms(void);
void     vr_reactor_sleep_ms(int ms);

/* Memory Helpers */
void vr_reactor_memmove(void *dest, const void *src, size_t n);

/* Edge-Triggered Drain & Discard */
int vr_reactor_drain_discard(int fd);

/* Atomic Metrics */
void vr_reactor_metric_inc_accept(vr_reactor_t *r);
void vr_reactor_metric_inc_open(vr_reactor_t *r);
void vr_reactor_metric_dec_open(vr_reactor_t *r);
void vr_reactor_metric_inc_events(vr_reactor_t *r, uint64_t count);
void vr_reactor_metric_inc_stale(vr_reactor_t *r);
void vr_reactor_metric_add_bytes_in(vr_reactor_t *r, uint64_t bytes);
void vr_reactor_metric_add_bytes_out(vr_reactor_t *r, uint64_t bytes);
void vr_reactor_metric_inc_idle_timeouts(vr_reactor_t *r);
void vr_reactor_metric_inc_ws_upgrades(vr_reactor_t *r);
void vr_reactor_metric_inc_ws_frames_in(vr_reactor_t *r);
void vr_reactor_metric_inc_ws_frames_out(vr_reactor_t *r);
void vr_reactor_metric_inc_ws_dropped(vr_reactor_t *r);
void vr_reactor_metric_inc_sse_streams(vr_reactor_t *r);
void vr_reactor_metric_inc_sse_events_out(vr_reactor_t *r);
void vr_reactor_metric_inc_sse_heartbeats(vr_reactor_t *r);
void vr_reactor_metric_inc_sse_dropped(vr_reactor_t *r);
void vr_reactor_get_metrics(const vr_reactor_t *r, vr_reactor_metrics_t *out);

/* Socket Options: TCP User Timeout (RFC 5482) */
int vr_reactor_set_tcp_user_timeout(int fd, unsigned int timeout_ms);

/* Phase 5: WebSocket Crypto, Framing & Ref-Counted Broadcast Buffers */
int    vr_ws_compute_accept_key(const char *client_key, size_t key_len, char *out_accept);
void   vr_sha1(const uint8_t *data, size_t len, uint8_t out_digest[20]);
size_t vr_base64_encode(const uint8_t *in, size_t in_len, char *out, size_t out_cap);
void   vr_ws_unmask(uint8_t *payload, size_t len, const uint8_t mask_key[4]);
int    vr_ws_encode_frame_header(uint8_t fin_and_opcode, size_t payload_len, uint8_t out_hdr[10]);

struct vr_ws_msg {
    int32_t ref_count;
    size_t  total_len;
    uint8_t data[];
};
typedef struct vr_ws_msg vr_ws_msg_t;

#define VR_MAX_ROOMS 4096

vr_ws_msg_t   *vr_ws_msg_create(uint8_t opcode, const void *payload, size_t len, int initial_refs);
const uint8_t *vr_ws_msg_data(const vr_ws_msg_t *msg);
size_t         vr_ws_msg_size(const vr_ws_msg_t *msg);
void           vr_ws_msg_ref(vr_ws_msg_t *msg);
void           vr_ws_msg_unref(vr_ws_msg_t *msg);
int            vr_ws_is_valid_utf8(const unsigned char *s, size_t len);
int            vr_ws_parse_pub_command(const char *data, size_t len, int is_sse, int *out_room, size_t *out_payload_offset);
uint64_t       vr_reactor_next_room_seq(vr_reactor_t *r, int room_id);

/* Phase 6: Server-Sent Events (SSE) Codec & Ref-Counted Broadcast */
int vr_sse_format_event(
    const char *id,    size_t id_len,
    const char *event, size_t event_len,
    const char *data,  size_t data_len,
    char       *out_buf, size_t out_cap
);

vr_ws_msg_t *vr_sse_msg_create(
    const char *id,    size_t id_len,
    const char *event, size_t event_len,
    const char *data,  size_t data_len,
    int initial_refs
);

/* Phase 7: Supervision Heartbeats, Fault Reporting, Shard FD Purge & Metrics */
void     vr_reactor_beat_reactor(vr_reactor_t *r, uint64_t now_ms);
void     vr_reactor_beat_broker (vr_reactor_t *r, uint64_t now_ms);
void     vr_reactor_beat_worker (vr_reactor_t *r, int worker_id, uint64_t now_ms);

uint64_t vr_reactor_get_reactor_beat(const vr_reactor_t *r);
uint64_t vr_reactor_get_broker_beat (const vr_reactor_t *r);
uint64_t vr_reactor_get_worker_beat (const vr_reactor_t *r, int worker_id);

void     vr_reactor_report_broker_fault(vr_reactor_t *r);
void     vr_reactor_report_worker_fault(vr_reactor_t *r, int worker_id);
uint64_t vr_reactor_consume_fault_mask (vr_reactor_t *r);

void     vr_reactor_set_fd_owner(vr_reactor_t *r, int fd, int16_t worker_id);
int16_t  vr_reactor_get_fd_owner(const vr_reactor_t *r, int fd);
int      vr_reactor_purge_worker_fds(vr_reactor_t *r, int worker_id);
int      vr_reactor_close_fd_for_worker(vr_reactor_t *r, int fd, int worker_id);

void     vr_reactor_metric_inc_restarts(vr_reactor_t *r);
void     vr_reactor_metric_inc_isolated_faults(vr_reactor_t *r);

/* Performance Remediation: Least-Connections Worker Assignment
 *  - assign_worker: picks the worker with the fewest live connections among
 *    [0, worker_count), atomically increments its load, records fd ownership,
 *    and returns the chosen worker index (lowest index wins ties).
 *  - route_fd: returns the owning worker for `fd`; for untracked FDs
 *    (fd >= VR_MAX_TRACKED_FDS) falls back to fd % worker_count; returns -1
 *    if a tracked FD currently has no owner (stale event).
 *  - release_worker_conn: saturating atomic decrement of a worker's load.
 */
int      vr_reactor_assign_worker(vr_reactor_t *r, int fd, int worker_count);
int      vr_reactor_route_fd(const vr_reactor_t *r, int fd, int worker_count);
void     vr_reactor_release_worker_conn(vr_reactor_t *r, int worker_id);
uint32_t vr_reactor_get_worker_conns(const vr_reactor_t *r, int worker_id);

#ifdef __cplusplus
}
#endif
#endif /* VR_REACTOR_BRIDGE_H */
