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

#define VR_MAX_INTERNAL_EVENTS 1024

struct vr_reactor {
    int epfd;
    int wake_fd;
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

    vr_reactor_t *r = (vr_reactor_t *)calloc(1, sizeof(vr_reactor_t));
    if (!r) return NULL;

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
        return VR_REACTOR_IO_ERROR; /* Fatal error */
    }
}

int vr_reactor_close(int fd) {
    return close(fd);
}
