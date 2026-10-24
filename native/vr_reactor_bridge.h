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

/* Metrics Snapshot (56 bytes) */
typedef struct {
    uint64_t active_connections;
    uint64_t accepts_total;
    uint64_t events_total;
    uint64_t stale_events;
    uint64_t bytes_in_total;
    uint64_t bytes_out_total;
    uint64_t idle_timeouts_total;
} vr_reactor_metrics_t;

#if defined(__STDC_VERSION__) && __STDC_VERSION__ >= 201112L
_Static_assert(sizeof(vr_reactor_metrics_t) == 56, "vr_reactor_metrics_t must be 56 bytes");
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
void vr_reactor_get_metrics(const vr_reactor_t *r, vr_reactor_metrics_t *out);

#ifdef __cplusplus
}
#endif
#endif /* VR_REACTOR_BRIDGE_H */
