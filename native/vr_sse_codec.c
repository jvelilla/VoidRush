#include "vr_reactor_bridge.h"
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

/* Helper to append sanitized token (stripping \r and \n to prevent CRLF injection) */
static size_t append_sanitized(char *dst, size_t dst_cap, size_t offset, const char *prefix, size_t prefix_len, const char *src, size_t src_len) {
    if (offset + prefix_len >= dst_cap) return 0;
    memcpy(dst + offset, prefix, prefix_len);
    offset += prefix_len;

    for (size_t i = 0; i < src_len; i++) {
        char c = src[i];
        if (c != '\r' && c != '\n') {
            if (offset + 1 >= dst_cap) return 0;
            dst[offset++] = c;
        }
    }
    if (offset + 1 >= dst_cap) return 0;
    dst[offset++] = '\n';
    return offset;
}

int vr_sse_format_event(
    const char *id,    size_t id_len,
    const char *event, size_t event_len,
    const char *data,  size_t data_len,
    char       *out_buf, size_t out_cap
) {
    if (!out_buf || out_cap < 3) return -1;
    size_t offset = 0;

    /* 1. Optional id: field */
    if (id && id_len > 0) {
        offset = append_sanitized(out_buf, out_cap, offset, "id: ", 4, id, id_len);
        if (offset == 0) return -1;
    }

    /* 2. Optional event: field */
    if (event && event_len > 0) {
        offset = append_sanitized(out_buf, out_cap, offset, "event: ", 7, event, event_len);
        if (offset == 0) return -1;
    }

    /* 3. Multi-line data: field */
    if (data && data_len > 0) {
        size_t line_start = 0;
        for (size_t i = 0; i < data_len; i++) {
            if (data[i] == '\n') {
                size_t line_len = i - line_start;
                /* Strip trailing \r if present */
                if (line_len > 0 && data[line_start + line_len - 1] == '\r') {
                    line_len--;
                }
                if (offset + 6 + line_len >= out_cap) return -1;
                memcpy(out_buf + offset, "data: ", 6);
                offset += 6;
                if (line_len > 0) {
                    memcpy(out_buf + offset, data + line_start, line_len);
                    offset += line_len;
                }
                out_buf[offset++] = '\n';
                line_start = i + 1;
            }
        }
        if (line_start < data_len) {
            size_t line_len = data_len - line_start;
            if (line_len > 0 && data[line_start + line_len - 1] == '\r') {
                line_len--;
            }
            if (offset + 6 + line_len >= out_cap) return -1;
            memcpy(out_buf + offset, "data: ", 6);
            offset += 6;
            if (line_len > 0) {
                memcpy(out_buf + offset, data + line_start, line_len);
                offset += line_len;
            }
            out_buf[offset++] = '\n';
        }
    } else {
        if (offset + 7 >= out_cap) return -1;
        memcpy(out_buf + offset, "data:\n", 6);
        offset += 6;
    }

    /* 4. Terminal blank line */
    if (offset + 1 >= out_cap) return -1;
    out_buf[offset++] = '\n';
    return (int)offset;
}

vr_ws_msg_t *vr_sse_msg_create(
    const char *id,    size_t id_len,
    const char *event, size_t event_len,
    const char *data,  size_t data_len,
    int initial_refs
) {
    size_t est_cap = data_len * 2 + id_len + event_len + 128;
    char *stack_buf = (char *)malloc(est_cap);
    if (!stack_buf) return NULL;

    int formatted_len = vr_sse_format_event(id, id_len, event, event_len, data, data_len, stack_buf, est_cap);
    if (formatted_len <= 0) {
        free(stack_buf);
        return NULL;
    }

    size_t total = (size_t)formatted_len;
    vr_ws_msg_t *msg = (vr_ws_msg_t *)malloc(sizeof(vr_ws_msg_t) + total);
    if (!msg) {
        free(stack_buf);
        return NULL;
    }
    msg->ref_count = initial_refs;
    msg->total_len = total;
    memcpy(msg->data, stack_buf, total);
    free(stack_buf);
    return msg;
}
