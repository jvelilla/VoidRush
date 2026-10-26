#define _GNU_SOURCE
#include "vr_reactor_bridge.h"
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <signal.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <sys/resource.h>

#define VR_MAX_INTERNAL_EVENTS 2048

struct vr_reactor {
    int epfd;
    int wake_fd;
    int shutdown_flag;
    uint64_t active_connections;
    uint64_t accepts_total;
    uint64_t events_total;
    uint64_t stale_events;
    uint64_t bytes_in_total;
    uint64_t bytes_out_total;
    uint64_t idle_timeouts_total;
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

    /* Phase 7 Control Block */
    uint64_t reactor_heartbeat_ms;
    uint64_t broker_heartbeat_ms;
    uint64_t worker_heartbeats_ms[VR_MAX_WORKERS];
    uint64_t child_fault_mask; /* Bit 63: Broker; Bits 0..62: Worker ID */
    int16_t  fd_owner[VR_MAX_TRACKED_FDS];
    /* Performance Remediation: live connection count per worker (least-connections) */
    uint32_t worker_conns[VR_MAX_WORKERS];

    /* Phase 8 Room Sequence Numbers for SSE and Pub/Sub */
    uint64_t room_seq[VR_MAX_ROOMS];

    struct epoll_event raw_events[VR_MAX_INTERNAL_EVENTS];
};

static uint32_t to_epoll_flags(uint32_t flags) {
    uint32_t ep = EPOLLET | EPOLLONESHOT | EPOLLRDHUP;
    if (flags & VR_REACTOR_IN)  ep |= EPOLLIN;
    if (flags & VR_REACTOR_OUT) ep |= EPOLLOUT;
    return ep;
}

static uint32_t from_epoll_flags(uint32_t ep) {
    uint32_t flags = 0;
    if (ep & EPOLLIN)                 flags |= VR_REACTOR_IN;
    if (ep & EPOLLOUT)                flags |= VR_REACTOR_OUT;
    if (ep & EPOLLERR)                flags |= VR_REACTOR_ERR;
    if (ep & (EPOLLHUP | EPOLLRDHUP)) flags |= VR_REACTOR_HUP;
    return flags;
}

vr_reactor_t *vr_reactor_create(void) {
    /* Prevent broken client pipes from killing the Eiffel runtime */
    signal(SIGPIPE, SIG_IGN);

    /* Raise RLIMIT_NOFILE to maximum allowed soft/hard limit */
    struct rlimit rl;
    if (getrlimit(RLIMIT_NOFILE, &rl) == 0) {
        rl.rlim_cur = rl.rlim_max;
        setrlimit(RLIMIT_NOFILE, &rl);
    }

    vr_reactor_t *r = (vr_reactor_t *)calloc(1, sizeof(vr_reactor_t));
    if (!r) return NULL;

    for (int i = 0; i < VR_MAX_TRACKED_FDS; i++) {
        r->fd_owner[i] = -1;
    }

    r->epfd = epoll_create1(EPOLL_CLOEXEC);
    if (r->epfd < 0) {
        free(r);
        return NULL;
    }

    r->wake_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (r->wake_fd < 0) {
        close(r->epfd);
        free(r);
        return NULL;
    }

    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.events = EPOLLIN | EPOLLET; /* wake_fd is persistent, no ONESHOT */
    ev.data.u64 = (uint64_t)(uint32_t)r->wake_fd;
    if (epoll_ctl(r->epfd, EPOLL_CTL_ADD, r->wake_fd, &ev) < 0) {
        close(r->wake_fd);
        close(r->epfd);
        free(r);
        return NULL;
    }

    return r;
}

void vr_reactor_destroy(vr_reactor_t *r) {
    if (!r) return;
    if (r->wake_fd >= 0) close(r->wake_fd);
    if (r->epfd >= 0)    close(r->epfd);
    free(r);
}

void vr_reactor_request_shutdown(vr_reactor_t *r) {
    if (r) {
        __atomic_store_n(&r->shutdown_flag, 1, __ATOMIC_RELEASE);
        vr_reactor_wakeup(r);
    }
}

int vr_reactor_is_shutting_down(const vr_reactor_t *r) {
    if (!r) return 1;
    return __atomic_load_n(&r->shutdown_flag, __ATOMIC_ACQUIRE);
}

int vr_reactor_add(vr_reactor_t *r, int fd, uint32_t flags, uint32_t generation) {
    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.events = to_epoll_flags(flags);
    ev.data.u64 = ((uint64_t)generation << 32) | (uint32_t)fd;
    return epoll_ctl(r->epfd, EPOLL_CTL_ADD, fd, &ev);
}

int vr_reactor_mod(vr_reactor_t *r, int fd, uint32_t flags, uint32_t generation) {
    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.events = to_epoll_flags(flags);
    ev.data.u64 = ((uint64_t)generation << 32) | (uint32_t)fd;
    return epoll_ctl(r->epfd, EPOLL_CTL_MOD, fd, &ev);
}

int vr_reactor_del(vr_reactor_t *r, int fd) {
    if (r && fd >= 0 && fd < VR_MAX_TRACKED_FDS) {
        __atomic_store_n(&r->fd_owner[fd], (int16_t)-1, __ATOMIC_RELEASE);
    }
    return epoll_ctl(r->epfd, EPOLL_CTL_DEL, fd, NULL);
}

int vr_reactor_wait(vr_reactor_t *r, vr_reactor_event_t *events_out, int max_events, int timeout_ms) {
    if (max_events > VR_MAX_INTERNAL_EVENTS) {
        max_events = VR_MAX_INTERNAL_EVENTS;
    }

    int n = epoll_wait(r->epfd, r->raw_events, max_events, timeout_ms);
    if (n < 0) {
        if (errno == EINTR) return 0;
        return -1;
    }

    int out_idx = 0;
    for (int i = 0; i < n; i++) {
        uint64_t u64 = r->raw_events[i].data.u64;
        int32_t fd = (int32_t)(u64 & 0xFFFFFFFFULL);

        if (fd == r->wake_fd) {
            uint64_t val;
            while (read(r->wake_fd, &val, sizeof(val)) > 0) {}
            continue;
        }

        events_out[out_idx].fd         = fd;
        events_out[out_idx].generation = (uint32_t)(u64 >> 32);
        events_out[out_idx].flags      = from_epoll_flags(r->raw_events[i].events);
        out_idx++;
    }
    return out_idx;
}

int vr_reactor_wakeup(vr_reactor_t *r) {
    uint64_t val = 1;
    ssize_t res = write(r->wake_fd, &val, sizeof(val));
    return (res == sizeof(val) || errno == EAGAIN) ? 0 : -1;
}

int vr_reactor_set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

int vr_reactor_listen_tcp(const char *host, int port, int backlog) {
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) return -1;

    int on = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    if (!host || host[0] == '\0' || strcmp(host, "0.0.0.0") == 0) {
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
    } else if (inet_pton(AF_INET, host, &addr.sin_addr) != 1) {
        close(fd);
        return -1;
    }

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0 ||
        listen(fd, backlog > 0 ? backlog : SOMAXCONN) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

int vr_reactor_accept_nonblocking(int listen_fd) {
    for (;;) {
        int client = accept4(listen_fd, NULL, NULL, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (client >= 0) {
            int one = 1;
            setsockopt(client, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
            return client;
        }
        if (errno == EINTR || errno == ECONNABORTED || errno == EPROTO) {
            continue; /* Transient signal or aborted handshake */
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return VR_REACTOR_IO_WOULD_BLOCK; /* Queue drained */
        }
        if (errno == EMFILE || errno == ENFILE) {
            /* Drain pending connection via spare file descriptor to prevent epoll busy-loop */
            static __thread int spare_fd = -1;
            if (spare_fd < 0) {
                spare_fd = open("/dev/null", O_RDONLY | O_CLOEXEC);
            }
            if (spare_fd >= 0) {
                close(spare_fd);
                int client = accept4(listen_fd, NULL, NULL, SOCK_NONBLOCK | SOCK_CLOEXEC);
                if (client >= 0) {
                    close(client);
                }
                spare_fd = open("/dev/null", O_RDONLY | O_CLOEXEC);
            }
            return VR_REACTOR_IO_WOULD_BLOCK;
        }
        return VR_REACTOR_IO_ERROR; /* Fatal error */
    }
}

int vr_reactor_close(int fd) {
    return close(fd);
}

int vr_reactor_shutdown_wr(int fd) {
    return shutdown(fd, SHUT_WR);
}

int vr_reactor_connect_tcp(const char *host, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    if (!host || host[0] == '\0') {
        inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    } else {
        inet_pton(AF_INET, host, &addr.sin_addr);
    }

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

int vr_reactor_send(int fd, const char *buf, int len) {
    return (int)send(fd, buf, len, 0);
}

int vr_reactor_read(int fd, void *buf, size_t len) {
    if (len == 0) return 0;
    for (;;) {
        ssize_t n = read(fd, buf, len);
        if (n > 0) return (int)n;
        if (n == 0) return VR_REACTOR_IO_EOF;
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) return VR_REACTOR_IO_WOULD_BLOCK;
        return VR_REACTOR_IO_ERROR;
    }
}

int vr_reactor_write(int fd, const void *buf, size_t len) {
    if (len == 0) return 0;
    for (;;) {
        ssize_t n = write(fd, buf, len);
        if (n > 0) return (int)n;
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) return VR_REACTOR_IO_WOULD_BLOCK;
        return VR_REACTOR_IO_ERROR;
    }
}

uint64_t vr_reactor_now_ms(void) {
    struct timespec ts;
#ifdef CLOCK_MONOTONIC_COARSE
    clock_gettime(CLOCK_MONOTONIC_COARSE, &ts);
#else
    clock_gettime(CLOCK_MONOTONIC, &ts);
#endif
    return (uint64_t)ts.tv_sec * 1000ULL + (uint64_t)ts.tv_nsec / 1000000ULL;
}

void vr_reactor_sleep_ms(int ms) {
    if (ms > 0) {
        usleep((useconds_t)ms * 1000);
    }
}

void vr_reactor_memmove(void *dest, const void *src, size_t n) {
    if (n > 0 && dest != src) {
        memmove(dest, src, n);
    }
}

int vr_reactor_drain_discard(int fd) {
    char buf[4096];
    for (;;) {
        ssize_t n = read(fd, buf, sizeof(buf));
        if (n > 0) {
            continue; /* Keep reading until EAGAIN for EPOLLET */
        }
        if (n == 0) {
            return VR_REACTOR_IO_EOF; /* Peer closed connection cleanly (FIN) */
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return VR_REACTOR_IO_WOULD_BLOCK; /* Drained to kernel boundary */
        }
        return VR_REACTOR_IO_ERROR; /* Socket reset or error */
    }
}

void vr_reactor_metric_inc_accept(vr_reactor_t *r) {
    if (r) {
        __atomic_add_fetch(&r->accepts_total, 1, __ATOMIC_RELAXED);
    }
}

void vr_reactor_metric_inc_open(vr_reactor_t *r) {
    if (r) {
        __atomic_add_fetch(&r->active_connections, 1, __ATOMIC_RELAXED);
    }
}

static inline void vr_reactor_saturating_dec_active(vr_reactor_t *r) {
    if (!r) return;
    uint64_t cur = __atomic_load_n(&r->active_connections, __ATOMIC_RELAXED);
    while (cur > 0) {
        if (__atomic_compare_exchange_n(&r->active_connections, &cur, cur - 1,
                                        1, __ATOMIC_RELAXED, __ATOMIC_RELAXED)) {
            break;
        }
    }
}

void vr_reactor_metric_dec_open(vr_reactor_t *r) {
    vr_reactor_saturating_dec_active(r);
}

void vr_reactor_metric_inc_events(vr_reactor_t *r, uint64_t count) {
    if (r) {
        __atomic_add_fetch(&r->events_total, count, __ATOMIC_RELAXED);
    }
}

void vr_reactor_metric_inc_stale(vr_reactor_t *r) {
    if (r) {
        __atomic_add_fetch(&r->stale_events, 1, __ATOMIC_RELAXED);
    }
}

void vr_reactor_metric_add_bytes_in(vr_reactor_t *r, uint64_t bytes) {
    if (r && bytes > 0) {
        __atomic_add_fetch(&r->bytes_in_total, bytes, __ATOMIC_RELAXED);
    }
}

void vr_reactor_metric_add_bytes_out(vr_reactor_t *r, uint64_t bytes) {
    if (r && bytes > 0) {
        __atomic_add_fetch(&r->bytes_out_total, bytes, __ATOMIC_RELAXED);
    }
}

void vr_reactor_metric_inc_idle_timeouts(vr_reactor_t *r) {
    if (r) {
        __atomic_add_fetch(&r->idle_timeouts_total, 1, __ATOMIC_RELAXED);
    }
}

void vr_reactor_metric_inc_ws_upgrades(vr_reactor_t *r) {
    if (r) {
        __atomic_add_fetch(&r->ws_upgrades_total, 1, __ATOMIC_RELAXED);
    }
}

void vr_reactor_metric_inc_ws_frames_in(vr_reactor_t *r) {
    if (r) {
        __atomic_add_fetch(&r->ws_frames_in_total, 1, __ATOMIC_RELAXED);
    }
}

void vr_reactor_metric_inc_ws_frames_out(vr_reactor_t *r) {
    if (r) {
        __atomic_add_fetch(&r->ws_frames_out_total, 1, __ATOMIC_RELAXED);
    }
}

void vr_reactor_metric_inc_ws_dropped(vr_reactor_t *r) {
    if (r) {
        __atomic_add_fetch(&r->ws_dropped_backpressure_total, 1, __ATOMIC_RELAXED);
    }
}

void vr_reactor_metric_inc_sse_streams(vr_reactor_t *r) {
    if (r) {
        __atomic_add_fetch(&r->sse_streams_total, 1, __ATOMIC_RELAXED);
    }
}

void vr_reactor_metric_inc_sse_events_out(vr_reactor_t *r) {
    if (r) {
        __atomic_add_fetch(&r->sse_events_out_total, 1, __ATOMIC_RELAXED);
    }
}

void vr_reactor_metric_inc_sse_heartbeats(vr_reactor_t *r) {
    if (r) {
        __atomic_add_fetch(&r->sse_heartbeats_total, 1, __ATOMIC_RELAXED);
    }
}

void vr_reactor_metric_inc_sse_dropped(vr_reactor_t *r) {
    if (r) {
        __atomic_add_fetch(&r->sse_dropped_backpressure_total, 1, __ATOMIC_RELAXED);
    }
}

int vr_reactor_set_tcp_user_timeout(int fd, unsigned int timeout_ms) {
#if defined(TCP_USER_TIMEOUT)
    return setsockopt(fd, IPPROTO_TCP, TCP_USER_TIMEOUT, &timeout_ms, sizeof(timeout_ms));
#else
    (void)fd;
    (void)timeout_ms;
    return 0;
#endif
}

void vr_reactor_get_metrics(const vr_reactor_t *r, vr_reactor_metrics_t *out) {
    if (r && out) {
        out->active_connections  = __atomic_load_n(&r->active_connections, __ATOMIC_RELAXED);
        out->accepts_total        = __atomic_load_n(&r->accepts_total, __ATOMIC_RELAXED);
        out->events_total         = __atomic_load_n(&r->events_total, __ATOMIC_RELAXED);
        out->stale_events         = __atomic_load_n(&r->stale_events, __ATOMIC_RELAXED);
        out->bytes_in_total       = __atomic_load_n(&r->bytes_in_total, __ATOMIC_RELAXED);
        out->bytes_out_total      = __atomic_load_n(&r->bytes_out_total, __ATOMIC_RELAXED);
        out->idle_timeouts_total  = __atomic_load_n(&r->idle_timeouts_total, __ATOMIC_RELAXED);
        out->ws_upgrades_total    = __atomic_load_n(&r->ws_upgrades_total, __ATOMIC_RELAXED);
        out->ws_frames_in_total   = __atomic_load_n(&r->ws_frames_in_total, __ATOMIC_RELAXED);
        out->ws_frames_out_total  = __atomic_load_n(&r->ws_frames_out_total, __ATOMIC_RELAXED);
        out->ws_dropped_backpressure_total = __atomic_load_n(&r->ws_dropped_backpressure_total, __ATOMIC_RELAXED);
        out->sse_streams_total             = __atomic_load_n(&r->sse_streams_total, __ATOMIC_RELAXED);
        out->sse_events_out_total          = __atomic_load_n(&r->sse_events_out_total, __ATOMIC_RELAXED);
        out->sse_heartbeats_total          = __atomic_load_n(&r->sse_heartbeats_total, __ATOMIC_RELAXED);
        out->sse_dropped_backpressure_total = __atomic_load_n(&r->sse_dropped_backpressure_total, __ATOMIC_RELAXED);
        /* Phase 7 */
        out->supervisor_restarts_total      = __atomic_load_n(&r->supervisor_restarts_total, __ATOMIC_RELAXED);
        out->supervisor_isolated_faults_total = __atomic_load_n(&r->supervisor_isolated_faults_total, __ATOMIC_RELAXED);
    }
}

/* Phase 7: Atomic Liveness Heartbeats */
void vr_reactor_beat_reactor(vr_reactor_t *r, uint64_t now_ms) {
    if (!r) return;
    __atomic_store_n(&r->reactor_heartbeat_ms, now_ms, __ATOMIC_RELEASE);
}

void vr_reactor_beat_broker(vr_reactor_t *r, uint64_t now_ms) {
    if (!r) return;
    __atomic_store_n(&r->broker_heartbeat_ms, now_ms, __ATOMIC_RELEASE);
}

void vr_reactor_beat_worker(vr_reactor_t *r, int worker_id, uint64_t now_ms) {
    if (!r || worker_id < 0 || worker_id >= VR_MAX_WORKERS) return;
    __atomic_store_n(&r->worker_heartbeats_ms[worker_id], now_ms, __ATOMIC_RELEASE);
}

uint64_t vr_reactor_get_reactor_beat(const vr_reactor_t *r) {
    if (!r) return 0;
    return __atomic_load_n(&r->reactor_heartbeat_ms, __ATOMIC_ACQUIRE);
}

uint64_t vr_reactor_get_broker_beat(const vr_reactor_t *r) {
    if (!r) return 0;
    return __atomic_load_n(&r->broker_heartbeat_ms, __ATOMIC_ACQUIRE);
}

uint64_t vr_reactor_get_worker_beat(const vr_reactor_t *r, int worker_id) {
    if (!r || worker_id < 0 || worker_id >= VR_MAX_WORKERS) return 0;
    return __atomic_load_n(&r->worker_heartbeats_ms[worker_id], __ATOMIC_ACQUIRE);
}

/* Phase 7: Atomic Fault Reporting & Consumption */
void vr_reactor_report_broker_fault(vr_reactor_t *r) {
    if (!r) return;
    uint64_t mask = ((uint64_t)1) << 63;
    __atomic_or_fetch(&r->child_fault_mask, mask, __ATOMIC_SEQ_CST);
}

void vr_reactor_report_worker_fault(vr_reactor_t *r, int worker_id) {
    if (!r || worker_id < 0 || worker_id >= 63) return;
    uint64_t mask = ((uint64_t)1) << worker_id;
    __atomic_or_fetch(&r->child_fault_mask, mask, __ATOMIC_SEQ_CST);
}

uint64_t vr_reactor_consume_fault_mask(vr_reactor_t *r) {
    if (!r) return 0;
    return __atomic_exchange_n(&r->child_fault_mask, 0, __ATOMIC_SEQ_CST);
}

/* Phase 7: Shard FD Ownership & Purge */
void vr_reactor_set_fd_owner(vr_reactor_t *r, int fd, int16_t worker_id) {
    if (!r || fd < 0 || fd >= VR_MAX_TRACKED_FDS) return;
    __atomic_store_n(&r->fd_owner[fd], worker_id, __ATOMIC_RELEASE);
}

int16_t vr_reactor_get_fd_owner(const vr_reactor_t *r, int fd) {
    if (!r || fd < 0 || fd >= VR_MAX_TRACKED_FDS) return -1;
    return __atomic_load_n(&r->fd_owner[fd], __ATOMIC_ACQUIRE);
}

void vr_reactor_release_worker_conn(vr_reactor_t *r, int worker_id) {
    if (!r || worker_id < 0 || worker_id >= VR_MAX_WORKERS) return;
    uint32_t cur = __atomic_load_n(&r->worker_conns[worker_id], __ATOMIC_ACQUIRE);
    while (cur > 0) {
        if (__atomic_compare_exchange_n(&r->worker_conns[worker_id], &cur, cur - 1U,
                                        0, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
            break;
        }
    }
}

int vr_reactor_purge_worker_fds(vr_reactor_t *r, int worker_id) {
    if (!r || worker_id < 0 || worker_id >= VR_MAX_WORKERS) return 0;
    int purged = 0;
    for (int fd = 0; fd < VR_MAX_TRACKED_FDS; fd++) {
        int16_t expected = (int16_t)worker_id;
        if (__atomic_compare_exchange_n(&r->fd_owner[fd], &expected, (int16_t)-1,
                                        0, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
            epoll_ctl(r->epfd, EPOLL_CTL_DEL, fd, NULL);
            close(fd);
            vr_reactor_saturating_dec_active(r);
            purged++;
        }
    }
    /* Replacement worker starts with an empty shard */
    __atomic_store_n(&r->worker_conns[worker_id], 0U, __ATOMIC_RELEASE);
    return purged;
}

int vr_reactor_close_fd_for_worker(vr_reactor_t *r, int fd, int worker_id) {
    if (!r || fd < 0) return -1;
    if (fd < VR_MAX_TRACKED_FDS) {
        int16_t expected = (int16_t)worker_id;
        /* Atomically verify ownership and clear it before closing */
        if (!__atomic_compare_exchange_n(&r->fd_owner[fd], &expected, (int16_t)-1,
                                         0, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
            /* fd was already purged or re-assigned to another worker! Do NOT close! */
            return -1;
        }
    }
    /* Atomically decrement per-worker load and total active connections */
    vr_reactor_release_worker_conn(r, worker_id);
    vr_reactor_saturating_dec_active(r);

    epoll_ctl(r->epfd, EPOLL_CTL_DEL, fd, NULL);
    return close(fd);
}

/* Performance Remediation: Least-Connections Worker Assignment */
int vr_reactor_assign_worker(vr_reactor_t *r, int fd, int worker_count) {
    if (!r || worker_count <= 0) return 0;
    if (worker_count > VR_MAX_WORKERS) worker_count = VR_MAX_WORKERS;

    if (fd >= VR_MAX_TRACKED_FDS) {
        /* Untracked fallback: deterministic modulo routing matching route_fd */
        int w = fd % worker_count;
        __atomic_add_fetch(&r->worker_conns[w], 1U, __ATOMIC_ACQ_REL);
        return w;
    }

    uint64_t faults = __atomic_load_n(&r->child_fault_mask, __ATOMIC_ACQUIRE);
    int best = -1;
    uint32_t best_load = UINT32_MAX;

    for (int i = 0; i < worker_count; i++) {
        if (i < 63 && (faults & (((uint64_t)1) << i))) {
            continue; /* Skip dead or restarting worker in fault mask */
        }
        uint32_t load = __atomic_load_n(&r->worker_conns[i], __ATOMIC_ACQUIRE);
        if (best == -1 || load < best_load) {
            best = i;
            best_load = load;
        }
    }

    /* Fallback if all workers are in fault mask */
    if (best == -1) {
        best = 0;
        best_load = __atomic_load_n(&r->worker_conns[0], __ATOMIC_ACQUIRE);
        for (int i = 1; i < worker_count; i++) {
            uint32_t load = __atomic_load_n(&r->worker_conns[i], __ATOMIC_ACQUIRE);
            if (load < best_load) {
                best = i;
                best_load = load;
            }
        }
    }

    if (fd >= 0) {
        __atomic_store_n(&r->fd_owner[fd], (int16_t)best, __ATOMIC_RELEASE);
    }
    __atomic_add_fetch(&r->worker_conns[best], 1U, __ATOMIC_ACQ_REL);
    return best;
}

int vr_reactor_route_fd(const vr_reactor_t *r, int fd, int worker_count) {
    if (!r || fd < 0 || worker_count <= 0) return -1;
    if (fd >= VR_MAX_TRACKED_FDS) {
        /* Untracked FD range: deterministic modulo fallback (matches assign_worker) */
        return fd % worker_count;
    }
    int16_t owner = __atomic_load_n(&r->fd_owner[fd], __ATOMIC_ACQUIRE);
    if (owner < 0 || owner >= worker_count) return -1;
    return (int)owner;
}

uint32_t vr_reactor_get_worker_conns(const vr_reactor_t *r, int worker_id) {
    if (!r || worker_id < 0 || worker_id >= VR_MAX_WORKERS) return 0;
    return __atomic_load_n(&r->worker_conns[worker_id], __ATOMIC_ACQUIRE);
}

/* Phase 7: Supervision Metrics Counters */
void vr_reactor_metric_inc_restarts(vr_reactor_t *r) {
    if (!r) return;
    __atomic_fetch_add(&r->supervisor_restarts_total, 1, __ATOMIC_RELAXED);
}

void vr_reactor_metric_inc_isolated_faults(vr_reactor_t *r) {
    if (!r) return;
    __atomic_fetch_add(&r->supervisor_isolated_faults_total, 1, __ATOMIC_RELAXED);
}

uint64_t vr_reactor_next_room_seq(vr_reactor_t *r, int room_id) {
    if (!r || room_id < 0) return 0;
    uint32_t rid = (uint32_t)room_id % VR_MAX_ROOMS;
    return __atomic_add_fetch(&r->room_seq[rid], 1, __ATOMIC_RELAXED);
}

