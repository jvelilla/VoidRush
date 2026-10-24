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

/* Lifecycle */
vr_reactor_t *vr_reactor_create(void);
void          vr_reactor_destroy(vr_reactor_t *r);

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

#ifdef __cplusplus
}
#endif
#endif /* VR_REACTOR_BRIDGE_H */
