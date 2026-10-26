#include "vr_reactor_bridge.h"
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#define RFC6455_GUID "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
#define RFC6455_GUID_LEN 36

/* --- SHA-1 Implementation (RFC 3174 compliant, zero heap allocation) --- */
typedef struct {
    uint32_t state[5];
    uint32_t count[2];
    uint8_t  buffer[64];
} vr_sha1_ctx_t;

#define SHA1_ROL(value, bits) (((value) << (bits)) | ((value) >> (32 - (bits))))

static void vr_sha1_transform(uint32_t state[5], const uint8_t buffer[64]) {
    uint32_t a = state[0], b = state[1], c = state[2], d = state[3], e = state[4];
    uint32_t w[80];

    for (int i = 0; i < 16; i++) {
        w[i] = ((uint32_t)buffer[i * 4] << 24) |
               ((uint32_t)buffer[i * 4 + 1] << 16) |
               ((uint32_t)buffer[i * 4 + 2] << 8) |
               ((uint32_t)buffer[i * 4 + 3]);
    }
    for (int i = 16; i < 80; i++) {
        w[i] = SHA1_ROL(w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16], 1);
    }

    /* Round 1 (0..19) */
    for (int i = 0; i < 20; i++) {
        uint32_t temp = SHA1_ROL(a, 5) + ((b & c) | ((~b) & d)) + e + w[i] + 0x5A827999U;
        e = d;
        d = c;
        c = SHA1_ROL(b, 30);
        b = a;
        a = temp;
    }
    /* Round 2 (20..39) */
    for (int i = 20; i < 40; i++) {
        uint32_t temp = SHA1_ROL(a, 5) + (b ^ c ^ d) + e + w[i] + 0x6ED9EBA1U;
        e = d;
        d = c;
        c = SHA1_ROL(b, 30);
        b = a;
        a = temp;
    }
    /* Round 3 (40..59) */
    for (int i = 40; i < 60; i++) {
        uint32_t temp = SHA1_ROL(a, 5) + ((b & c) | (b & d) | (c & d)) + e + w[i] + 0x8F1BBCDCU;
        e = d;
        d = c;
        c = SHA1_ROL(b, 30);
        b = a;
        a = temp;
    }
    /* Round 4 (60..79) */
    for (int i = 60; i < 80; i++) {
        uint32_t temp = SHA1_ROL(a, 5) + (b ^ c ^ d) + e + w[i] + 0xCA62C1D6U;
        e = d;
        d = c;
        c = SHA1_ROL(b, 30);
        b = a;
        a = temp;
    }

    state[0] += a;
    state[1] += b;
    state[2] += c;
    state[3] += d;
    state[4] += e;
}

static void vr_sha1_init(vr_sha1_ctx_t *context) {
    context->state[0] = 0x67452301U;
    context->state[1] = 0xEFCDAB89U;
    context->state[2] = 0x98BADCFEU;
    context->state[3] = 0x10325476U;
    context->state[4] = 0xC3D2E1F0U;
    context->count[0] = context->count[1] = 0;
}

static void vr_sha1_update(vr_sha1_ctx_t *context, const uint8_t *data, size_t len) {
    size_t i, j;
    j = (context->count[0] >> 3) & 63;
    if ((context->count[0] += (uint32_t)(len << 3)) < (uint32_t)(len << 3)) {
        context->count[1]++;
    }
    context->count[1] += (uint32_t)(len >> 29);
    if ((j + len) > 63) {
        memcpy(&context->buffer[j], data, (i = 64 - j));
        vr_sha1_transform(context->state, context->buffer);
        for (; i + 63 < len; i += 64) {
            vr_sha1_transform(context->state, &data[i]);
        }
        j = 0;
    } else {
        i = 0;
    }
    memcpy(&context->buffer[j], &data[i], len - i);
}

static void vr_sha1_final(uint8_t digest[20], vr_sha1_ctx_t *context) {
    uint8_t finalcount[8];
    for (int i = 0; i < 8; i++) {
        finalcount[i] = (uint8_t)((context->count[(i >= 4 ? 0 : 1)] >> ((3 - (i & 3)) * 8)) & 255);
    }
    vr_sha1_update(context, (const uint8_t *)"\x80", 1);
    while ((context->count[0] & 504) != 448) {
        vr_sha1_update(context, (const uint8_t *)"\0", 1);
    }
    vr_sha1_update(context, finalcount, 8);
    for (int i = 0; i < 20; i++) {
        digest[i] = (uint8_t)((context->state[i >> 2] >> ((3 - (i & 3)) * 8)) & 255);
    }
}

void vr_sha1(const uint8_t *data, size_t len, uint8_t out_digest[20]) {
    vr_sha1_ctx_t ctx;
    vr_sha1_init(&ctx);
    vr_sha1_update(&ctx, data, len);
    vr_sha1_final(out_digest, &ctx);
}

/* --- Base64 Encoding --- */
static const char b64_table[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

size_t vr_base64_encode(const uint8_t *in, size_t in_len, char *out, size_t out_cap) {
    size_t out_len = 4 * ((in_len + 2) / 3);
    if (out_cap < out_len + 1) return 0;
    size_t i = 0, j = 0;
    while (i + 2 < in_len) {
        uint32_t oct_a = in[i++];
        uint32_t oct_b = in[i++];
        uint32_t oct_c = in[i++];
        uint32_t triple = (oct_a << 16) | (oct_b << 8) | oct_c;
        out[j++] = b64_table[(triple >> 18) & 0x3F];
        out[j++] = b64_table[(triple >> 12) & 0x3F];
        out[j++] = b64_table[(triple >> 6) & 0x3F];
        out[j++] = b64_table[triple & 0x3F];
    }
    if (i < in_len) {
        size_t rem = in_len - i;
        if (rem == 2) {
            uint32_t oct_a = in[i++];
            uint32_t oct_b = in[i++];
            uint32_t triple = (oct_a << 16) | (oct_b << 8);
            out[j++] = b64_table[(triple >> 18) & 0x3F];
            out[j++] = b64_table[(triple >> 12) & 0x3F];
            out[j++] = b64_table[(triple >> 6) & 0x3F];
            out[j++] = '=';
        } else if (rem == 1) {
            uint32_t oct_a = in[i++];
            uint32_t triple = (oct_a << 16);
            out[j++] = b64_table[(triple >> 18) & 0x3F];
            out[j++] = b64_table[(triple >> 12) & 0x3F];
            out[j++] = '=';
            out[j++] = '=';
        }
    }
    out[j] = '\0';
    return j;
}

int vr_ws_compute_accept_key(const char *client_key, size_t key_len, char *out_accept) {
    if (!client_key || key_len != 24 || !out_accept) return -1;
    uint8_t concat[24 + RFC6455_GUID_LEN];
    memcpy(concat, client_key, 24);
    memcpy(concat + 24, RFC6455_GUID, RFC6455_GUID_LEN);
    uint8_t digest[20];
    vr_sha1(concat, sizeof(concat), digest);
    size_t encoded = vr_base64_encode(digest, 20, out_accept, 29);
    return (encoded == 28) ? 28 : -1;
}

/* --- Fast 64-Bit Word XOR Unmasking (Safe for unaligned buffers via memcpy) --- */
void vr_ws_unmask(uint8_t *payload, size_t len, const uint8_t mask_key[4]) {
    if (!payload || len == 0 || !mask_key) return;
    uint32_t mask32;
    memcpy(&mask32, mask_key, 4);
    uint64_t mask64 = ((uint64_t)mask32) | (((uint64_t)mask32) << 32);

    size_t words = len / 8;
    for (size_t w = 0; w < words; w++) {
        uint64_t chunk;
        memcpy(&chunk, payload + w * 8, 8);
        chunk ^= mask64;
        memcpy(payload + w * 8, &chunk, 8);
    }
    size_t i = words * 8;
    for (; i < len; i++) {
        payload[i] ^= mask_key[i & 3];
    }
}

/* --- Frame Header Encoder (Server-to-Client unmasked) --- */
int vr_ws_encode_frame_header(uint8_t fin_and_opcode, size_t payload_len, uint8_t out_hdr[10]) {
    out_hdr[0] = fin_and_opcode;
    if (payload_len < 126) {
        out_hdr[1] = (uint8_t)payload_len; /* Server frame: MASK bit = 0 */
        return 2;
    } else if (payload_len <= 0xFFFF) {
        out_hdr[1] = 126;
        out_hdr[2] = (uint8_t)((payload_len >> 8) & 0xFF);
        out_hdr[3] = (uint8_t)(payload_len & 0xFF);
        return 4;
    } else {
        out_hdr[1] = 127;
        for (int i = 0; i < 8; i++) {
            out_hdr[2 + i] = (uint8_t)((payload_len >> (56 - i * 8)) & 0xFF);
        }
        return 10;
    }
}

/* --- Ref-Counted Pre-Framed Message Buffer --- */

vr_ws_msg_t *vr_ws_msg_create(uint8_t opcode, const void *payload, size_t len, int initial_refs) {
    uint8_t hdr[10];
    int hdr_len = vr_ws_encode_frame_header(0x80 | (opcode & 0x0F), len, hdr);
    size_t total = (size_t)hdr_len + len;
    vr_ws_msg_t *msg = (vr_ws_msg_t *)malloc(sizeof(vr_ws_msg_t) + total);
    if (!msg) return NULL;
    msg->ref_count = initial_refs;
    msg->total_len = total;
    memcpy(msg->data, hdr, hdr_len);
    if (payload && len > 0) {
        memcpy(msg->data + hdr_len, payload, len);
    }
    return msg;
}

const uint8_t *vr_ws_msg_data(const vr_ws_msg_t *msg) {
    return msg ? msg->data : NULL;
}

size_t vr_ws_msg_size(const vr_ws_msg_t *msg) {
    return msg ? msg->total_len : 0;
}

void vr_ws_msg_ref(vr_ws_msg_t *msg) {
    if (msg) {
        __atomic_add_fetch(&msg->ref_count, 1, __ATOMIC_RELAXED);
    }
}

void vr_ws_msg_unref(vr_ws_msg_t *msg) {
    if (msg) {
        if (__atomic_sub_fetch(&msg->ref_count, 1, __ATOMIC_ACQ_REL) == 0) {
            free(msg);
        }
    }
}

int vr_ws_parse_pub_command(const char *data, size_t len, int is_sse, int *out_room, size_t *out_payload_offset) {
    if (!data || !out_room || !out_payload_offset) return 0;
    const char *prefix = is_sse ? "PUB_SSE " : "PUB ";
    size_t prefix_len = is_sse ? 8 : 4;

    if (len < prefix_len) return 0;
    if (memcmp(data, prefix, prefix_len) != 0) return 0;

    size_t p = prefix_len;
    /* Check for optional integer room parameter: digits followed by space */
    if (p < len && data[p] >= '0' && data[p] <= '9') {
        int room = 0;
        while (p < len && data[p] >= '0' && data[p] <= '9') {
            int d = data[p] - '0';
            if (room <= 214748364) {
                room = room * 10 + d;
            }
            p++;
        }
        if (p < len && data[p] == ' ') {
            while (p < len && data[p] == ' ') {
                p++;
            }
            *out_room = room;
            *out_payload_offset = p;
            return 1;
        }
    }

    *out_room = 0; /* Default room */
    *out_payload_offset = prefix_len;
    return 1;
}

int vr_ws_is_valid_utf8(const unsigned char *s, size_t len) {
    if (!s) return 1;
    size_t i = 0;
    while (i < len) {
        if (s[i] <= 0x7F) {
            i++;
        } else if ((s[i] & 0xE0) == 0xC0) {
            if (i + 1 >= len || (s[i + 1] & 0xC0) != 0x80 || (s[i] & 0x1E) == 0) return 0;
            i += 2;
        } else if ((s[i] & 0xF0) == 0xE0) {
            if (i + 2 >= len || (s[i + 1] & 0xC0) != 0x80 || (s[i + 2] & 0xC0) != 0x80) return 0;
            unsigned int cp = ((s[i] & 0x0F) << 12) | ((s[i + 1] & 0x3F) << 6) | (s[i + 2] & 0x3F);
            if (cp < 0x800 || (cp >= 0xD800 && cp <= 0xDFFF)) return 0;
            i += 3;
        } else if ((s[i] & 0xF8) == 0xF0) {
            if (i + 3 >= len || (s[i + 1] & 0xC0) != 0x80 || (s[i + 2] & 0xC0) != 0x80 || (s[i + 3] & 0xC0) != 0x80) return 0;
            unsigned int cp = ((s[i] & 0x07) << 18) | ((s[i + 1] & 0x3F) << 12) | ((s[i + 2] & 0x3F) << 6) | (s[i + 3] & 0x3F);
            if (cp < 0x10000 || cp > 0x10FFFF) return 0;
            i += 4;
        } else {
            return 0;
        }
    }
    return 1;
}
