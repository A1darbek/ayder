// app_routes.c
#include <string.h>
#include "app.h"
#include "aof_batch.h"
#include "fast_json.h"
#include "router.h"
#include "ramforge_rotation_metrics.h"
#include "shared_storage.h"
#include "globals.h"
#include "persistence_zp.h"
#include "storage_thread_safeguard.h"
#include "metrics_shared.h"
#include <inttypes.h>
#include <stdlib.h>
#include "rf_broker.h"
#include "ramforge_ha_integration.h"
#include "http_timing.h"
#include <stdarg.h>

#define JOUT_LIT(o, s) jout_try_mem((o), (s), sizeof(s) - 1)


extern App *g_app;

typedef struct {
    char *p;
    char *end; /* last writable (reserve 1 for NUL) */
} jout_t;



static inline void jout_init_res(jout_t *o, Response *res){
    o->p = res->buffer;
    o->end = res->buffer + RESPONSE_BUFFER_SIZE - 1;
    if (o->p <= o->end) *o->p = '\0';
}
static inline void jout_init_buf(jout_t *o, char *buf, size_t cap){
    o->p = buf;
    o->end = (cap ? (buf + cap - 1) : buf);
    if (cap) *buf = '\0';
}
static inline size_t jout_avail(const jout_t *o){
    return (o->p < o->end) ? (size_t)(o->end - o->p) : 0;
}
static inline int jout_try_mem(jout_t *o, const void *src, size_t n){
    if (n > jout_avail(o)) return 0;
    memcpy(o->p, src, n);
    o->p += n;
    *o->p = '\0';
    return 1;
}
static inline int jout_try_c(jout_t *o, char c){
    if (jout_avail(o) < 1) return 0;
    *o->p++ = c;
    *o->p = '\0';
    return 1;
}
static int jout_try_vprintf(jout_t *o, const char *fmt, va_list ap){
    va_list ap2;
    va_copy(ap2, ap);
    int need = vsnprintf(NULL, 0, fmt, ap2);
    va_end(ap2);
    if (need < 0) return 0;
    if ((size_t)need > jout_avail(o)) return 0;
    vsnprintf(o->p, (size_t)need + 1, fmt, ap);
    o->p += (size_t)need;
    *o->p = '\0';
    return 1;
}
static int jout_try_printf(jout_t *o, const char *fmt, ...){
    va_list ap;
    va_start(ap, fmt);
    int ok = jout_try_vprintf(o, fmt, ap);
    va_end(ap);
    return ok;
}

/* Like your json_escape_into but tells you if it truncated */
static size_t json_escape_into_ex(char *dst, size_t cap, const uint8_t *s, size_t n, int *trunc){
    size_t o = 0;
    if (trunc) *trunc = 0;
    for (size_t i = 0; i < n && o < cap; i++) {
        unsigned char c = s[i];
        if (c == '"' || c == '\\') {
            if (o + 2 > cap) { if (trunc) *trunc = 1; break; }
            dst[o++]='\\'; dst[o++]=(char)c;
        } else if (c <= 0x1F) {
            if (o + 6 > cap) { if (trunc) *trunc = 1; break; }
            dst[o++]='\\'; dst[o++]='u'; dst[o++]='0'; dst[o++]='0';
            const char *hex="0123456789abcdef";
            dst[o++]=hex[(c>>4)&0xF];
            dst[o++]=hex[c&0xF];
        } else {
            dst[o++]=(char)c;
        }
    }
    return o;
}

static inline int jv_to_cstr(const json_value_t *v, char *out, size_t cap){
    if (!v || !out || cap==0) return 0;
    if (v->type == JSON_STRING){
        size_t n = v->as.s.len < cap-1 ? v->as.s.len : cap-1;
        memcpy(out, v->as.s.ptr, n); out[n]=0; return 1;
    }
    /* stringify numbers/booleans for convenience */
    if (v->type == JSON_INT) {
        int n = snprintf(out, cap, "%" PRId64, (int64_t) v->as.i);
        if (n < 0) n = 0;
        if ((size_t) n >= cap) out[cap - 1] = 0;
        return 1;
    }
    if (v->type == JSON_DOUBLE) {
        int n = snprintf(out, cap, "%.17g", v->as.d);
        if (n < 0) n = 0;
        if ((size_t) n >= cap) out[cap - 1] = 0;
        return 1;
    }
    if (v->type == JSON_BOOL) {
        int n = snprintf(out, cap, "%s", v->as.b ? "true" : "false");
        if (n < 0) n = 0;
        if ((size_t) n >= cap) out[cap - 1] = 0;
        return 1;
    }
    return 0;
}

/* Build {"field":value,...} into a scratch buffer (safe); returns 1 if built, 0 if too big */
static int build_projected_object_json(char *out, size_t cap, json_value_t *obj, json_value_t *fields){
    jout_t w; jout_init_buf(&w, out, cap);
    if (!obj || obj->type != JSON_OBJECT) return 0;

    if (!jout_try_c(&w, '{')) return 0;
    int first = 1;

    if (fields && fields->type==JSON_ARRAY && fields->as.array.count>0){
        for (size_t i=0;i<fields->as.array.count;i++){
            json_value_t *fn = &fields->as.array.items[i];
            if (fn->type != JSON_STRING) continue;

            char name[64];
            if (!jv_to_cstr(fn, name, sizeof name)) continue;

            json_value_t *fv = json_get_field(obj, name);
            if (!fv) continue;

            if (!first) { if (!jout_try_c(&w, ',')) return 0; }
            first = 0;

            if (!jout_try_printf(&w, "\"%s\":", name)) return 0;

            switch (fv->type){
                case JSON_STRING:
                    /* IMPORTANT: fast_json strings are raw slices (already escaped in source JSON) */
                    if (!jout_try_c(&w, '"')) return 0;
                    if (!jout_try_mem(&w, fv->as.s.ptr, fv->as.s.len)) return 0;
                    if (!jout_try_c(&w, '"')) return 0;
                    break;
                case JSON_INT:
                    if (!jout_try_printf(&w, "%" PRId64, (int64_t)fv->as.i)) return 0;
                    break;
                case JSON_DOUBLE:
                    if (!jout_try_printf(&w, "%.17g", fv->as.d)) return 0;
                    break;
                case JSON_BOOL:
                    if (!jout_try_mem(&w, fv->as.b ? "true" : "false", fv->as.b ? 4 : 5)) return 0;
                    break;
                case JSON_NULL:
                default:
                    if (!jout_try_mem(&w, "null", 4)) return 0;
                    break;
            }
        }
    }

    if (!jout_try_c(&w, '}')) return 0;
    return 1;
}

typedef struct {
    char *p;
    char *end;
    int truncated;
} respw_t;

static inline respw_t rw_init(Response *res) {
    respw_t w;
    w.p = res->buffer;
    w.end = res->buffer + RESPONSE_BUFFER_SIZE - 1;
    w.truncated = 0;
    if (w.p <= w.end) *w.p = '\0';
    return w;
}

static inline void rw_nul(respw_t *w) {
    if (w->p <= w->end) *w->p = '\0';
    else w->end[0] = '\0';
}

static inline void rw_putc(respw_t *w, char c) {
    if (w->p < w->end) *w->p++ = c;
    else w->truncated = 1;
    rw_nul(w);
}

static inline void rw_mem(respw_t *w, const void *src, size_t n) {
    if (!n) return;
    size_t avail = (w->p < w->end) ? (size_t)(w->end - w->p) : 0;
    if (n > avail) { n = avail; w->truncated = 1; }
    if (n) {
        memcpy(w->p, src, n);
        w->p += n;
    }
    rw_nul(w);
}

static inline void rw_str(respw_t *w, const char *s) {
    if (!s) return;
    rw_mem(w, s, strlen(s));
}

static inline void rw_printf(respw_t *w, const char *fmt, ...) {
    if (w->p >= w->end) { w->truncated = 1; rw_nul(w); return; }
    va_list ap;
    va_start(ap, fmt);
    size_t avail = (size_t)(w->end - w->p);
    int n = vsnprintf(w->p, avail + 1, fmt, ap);
    va_end(ap);

    if (n < 0) { w->truncated = 1; rw_nul(w); return; }
    if ((size_t)n > avail) {
        w->p = w->end;
        w->truncated = 1;
    } else {
        w->p += (size_t)n;
    }
    rw_nul(w);
}

static inline uint64_t rf_monotonic_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000ULL + (uint64_t)ts.tv_nsec / 1000ULL;
}
static inline uint64_t rf_wall_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000000ULL + (uint64_t)ts.tv_nsec / 1000ULL;
}

typedef struct {
    const char *error;      // machine-readable code
    const char *message;    // human-readable explanation
    const char *docs;       // link to docs (optional)
} error_response_t;

static void send_error(Response *res, const error_response_t *err) {
    int n = snprintf(res->buffer, RESPONSE_BUFFER_SIZE,
                     "{\"ok\":false,\"error\":\"%s\",\"message\":\"%s\"%s%s%s}",
                     err->error,
                     err->message,
                     err->docs ? ",\"docs\":\"" : "",
                     err->docs ? err->docs : "",
                     err->docs ? "\"" : ""
    );
    res->buffer[(n>0 && n<(int)RESPONSE_BUFFER_SIZE)?n:(RESPONSE_BUFFER_SIZE-1)] = '\0';
}



static inline uint64_t now_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000ULL + ts.tv_nsec / 1000;
}

static inline uint64_t now_us_monotonic(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000ULL + (uint64_t)ts.tv_nsec / 1000ULL;
}

static int wait_synced_until(uint64_t batch_id, int timeout_ms) {
    if (batch_id == 0 || timeout_ms <= 0) return -1;
    uint64_t start = now_us_monotonic();
    for (;;) {
        if (AOF_sealed_last_synced_id() >= batch_id) return 1;
        if ((now_us_monotonic() - start) / 1000ULL >= (uint64_t)timeout_ms) return 0;
        /* small sleep to avoid busy spin; adjust as you like */
        usleep(50);  /* 50µs */
    }
}

static inline uint64_t wall_us(void){
    struct timespec ts; clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec*1000000ULL + (uint64_t)ts.tv_nsec/1000ULL;
}
static inline uint32_t fnv1a32(const void *p, size_t n){
    const uint8_t *s=(const uint8_t*)p; uint32_t h=2166136261u;
    for (size_t i=0;i<n;i++){ h^=s[i]; h*=16777619u; }
    return h ? h : 1;
}
static inline int kv_int_key(const char *ns, const char *key){
    // namespaced hash; high byte 0xB2 to avoid collisions with other integer maps
    uint32_t h = 2166136261u;
    h = (h ^ (uint8_t)0x7C) * 16777619u;
    h = (h ^ (uint8_t)0x2F) * 16777619u;
    h ^= fnv1a32(ns, strlen(ns)); h *= 16777619u;
    h ^= (uint32_t)'/';           h *= 16777619u;
    h ^= fnv1a32(key, strlen(key)); h *= 16777619u;
    return (int)(h | 0xB2000000u);
}
static int qs_u64(const char *qs, const char *name, uint64_t *out){
    if (!qs) return 0;
    size_t nlen = strlen(name);
    const char *p = qs;
    while (*p) {
        while (*p=='&') p++;
        if (!strncmp(p, name, nlen) && p[nlen]=='=') {
            p += nlen + 1;
            uint64_t v = 0; int any=0;
            while (*p && *p!='&') {
                if (*p>='0' && *p<='9') { v = v*10 + (*p-'0'); any=1; }
                else break;
                p++;
            }
            if (any){ *out=v; return 1; }
            return 0;
        }
        while (*p && *p!='&') p++;
    }
    return 0;
}

static int qs_str(const char *qs, const char *name, const char **out, size_t *out_len){
    if (!qs) return 0;
    size_t nlen = strlen(name);
    const char *p = qs;
    while (*p) {
        while (*p=='&') p++;
        if (!strncmp(p, name, nlen) && p[nlen]=='=') {
            const char *v = p + nlen + 1;
            const char *e = v;
            while (*e && *e!='&') e++;
            *out = v; *out_len = (size_t)(e - v);
            return 1;
        }
        while (*p && *p!='&') p++;
    }
    return 0;
}

static inline int hex_nibble(char c){
    if (c>='0' && c<='9') return c-'0';
    if (c>='a' && c<='f') return 10 + (c-'a');
    if (c>='A' && c<='F') return 10 + (c-'A');
    return -1;
}

/* percent-decode query pieces into dst (cap-limited). '+' -> ' ' */
static size_t url_decode_into(char *dst, size_t cap, const char *src, size_t n){
    size_t o=0;
    for (size_t i=0;i<n && o<cap;i++){
        char c = src[i];
        if (c=='%'){
            if (i+2<n){
                int hi = hex_nibble(src[i+1]);
                int lo = hex_nibble(src[i+2]);
                if (hi>=0 && lo>=0){ dst[o++] = (char)((hi<<4)|lo); i+=2; continue; }
            }
            dst[o++] = '%'; /* malformed: keep '%' literal */
        } else if (c=='+'){
            dst[o++] = ' ';
        } else {
            dst[o++] = c;
        }
    }
    return o;
}


/* JSON-escape into dst; returns bytes written (cap-limited).
   Only used for non-b64 consumer path (texty/legacy). */
static size_t json_escape_into(char *dst, size_t cap, const uint8_t *s, size_t n) {
    size_t o = 0;
    for (size_t i = 0; i < n && o < cap; i++) {
        unsigned char c = s[i];
        if (c == '"' || c == '\\') {
            if (o + 2 > cap) break;
            dst[o++] = '\\';
            dst[o++] = c;
        } else if (c <= 0x1F) {
            if (o + 6 > cap) break;
            dst[o++] = '\\';
            dst[o++] = 'u';
            dst[o++] = '0';
            dst[o++] = '0';
            const char *hex = "0123456789abcdef";
            dst[o++] = hex[(c >> 4) & 0xF];
            dst[o++] = hex[c & 0xF];
        } else {
            dst[o++] = c;
        }
    }
    return o;
}

/* ---------- Base64 (encoder only, url-safe off) ---------- */
static const char B64T[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
static size_t b64_len(size_t n){ return ((n + 2) / 3) * 4; }
static size_t b64_encode(const uint8_t *in, size_t n, char *out){
    size_t i=0,o=0;
    while (i+3 <= n) {
        uint32_t v = (in[i]<<16) | (in[i+1]<<8) | in[i+2];
        out[o++] = B64T[(v>>18)&63];
        out[o++] = B64T[(v>>12)&63];
        out[o++] = B64T[(v>>6)&63];
        out[o++] = B64T[v&63];
        i+=3;
    }
    if (i<n) {
        uint32_t v = in[i]<<16;
        int pad = 0;
        if (i+1<n){ v |= in[i+1]<<8; } else { pad++; }
        if (i+2<n){ v |= in[i+2]; }    else { pad++; }
        out[o++] = B64T[(v>>18)&63];
        out[o++] = B64T[(v>>12)&63];
        out[o++] = (pad>=2) ? '=' : B64T[(v>>6)&63];
        out[o++] = (pad>=1) ? '=' : B64T[v&63];
    }
    return o;
}
static inline int want_b64(const char *qs){
    const char *q=NULL; size_t ql=0; if (!qs) return 0;
    if (!qs_str(qs,"encoding",&q,&ql)) return 0;
    return (ql==3 && strncmp(q,"b64",3)==0);
}

static inline int jv_to_double(const json_value_t *v, double *out){
    if (!v) return 0;
    switch (v->type){
        case JSON_INT:    *out = (double)v->as.i; return 1;
        case JSON_DOUBLE: *out = v->as.d;        return 1;
        case JSON_STRING: {
            /* permissive parse for numerics in strings */
            char buf[64]; size_t n = v->as.s.len < sizeof(buf)-1 ? v->as.s.len : sizeof(buf)-1;
            memcpy(buf, v->as.s.ptr, n); buf[n]=0;
            char *ep=NULL; double d=strtod(buf,&ep);
            if (ep && ep!=buf) { *out=d; return 1; }
            return 0;
        }
        default: return 0;
    }
}


/* evaluate a single condition: {"field","op","value"} */
static int eval_condition(json_value_t *obj, const char *field, const char *op, json_value_t *want){
    if (!obj || obj->type != JSON_OBJECT) return 0;
    json_value_t *have = json_get_field(obj, field);
    if (!have) return 0;

    /* numeric ops if both comparable as doubles */
    double ha=0, wa=0;
    int have_num = jv_to_double(have, &ha);
    int want_num = jv_to_double(want, &wa);

    if (!strcmp(op,"eq") || !strcmp(op,"ne")){
        /* try numeric eq first if both numeric */
        if (have_num && want_num){
            int ok = (ha == wa);
            return (!strcmp(op,"eq")) ? ok : !ok;
        }
        /* else do string compare */
        char hb[256], wb[256];
        int hs = jv_to_cstr(have, hb, sizeof(hb));
        int ws = jv_to_cstr(want, wb, sizeof(wb));
        if (!(hs && ws)) return 0;
        int eq = (strcmp(hb, wb) == 0);
        return (!strcmp(op,"eq")) ? eq : !eq;
    }

    if (!(have_num && want_num)) return 0; /* gt/ge/lt/le need numeric */

    if (!strcmp(op,"gt")) return ha >  wa;
    if (!strcmp(op,"ge")) return ha >= wa;
    if (!strcmp(op,"lt")) return ha <  wa;
    if (!strcmp(op,"le")) return ha <= wa;

    return 0;
}

static int obj_matches_filters(json_value_t *obj, json_value_t *filters){
    if (!filters || filters->type != JSON_ARRAY) return 1; /* no filters == pass */
    for (size_t i=0;i<filters->as.array.count;i++){
        json_value_t *c = &filters->as.array.items[i];
        if (c->type != JSON_OBJECT) return 0;
        json_value_t *f = json_get_field(c, "field");
        json_value_t *o = json_get_field(c, "op");
        json_value_t *v = json_get_field(c, "value");
        if (!f || !o || !v || f->type!=JSON_STRING || o->type!=JSON_STRING) return 0;
        char fbuf[64], obuf[16];
        jv_to_cstr(f, fbuf, sizeof fbuf);
        jv_to_cstr(o, obuf, sizeof obuf);
        if (!eval_condition(obj, fbuf, obuf, v)) return 0;
    }
    return 1;
}
/* build a composite group key like "name=Alice|region=us" */
static size_t build_group_key(json_value_t *obj, json_value_t *group_by, char *out, size_t cap){
    size_t o=0;
    if (!group_by || group_by->type!=JSON_ARRAY) return 0;
    for (size_t i=0;i<group_by->as.array.count;i++){
        json_value_t *fld = &group_by->as.array.items[i];
        if (fld->type != JSON_STRING) continue;
        char name[64]; jv_to_cstr(fld, name, sizeof name);
        json_value_t *v = json_get_field(obj, name);
        if (o && o<cap) out[o++]='|';
        int n = snprintf(out + o, (o < cap ? cap - o : 0), "%s=", name);
        if (n < 0) n = 0;
        o += (size_t)n;
        char vb[256]; vb[0]=0;
        if (!jv_to_cstr(v, vb, sizeof vb)) strncpy(vb,"null",sizeof vb);
        size_t l=strlen(vb);
        if (o+l>=cap) l = (cap>o?cap-o-1:0);
        if (l) {
            memcpy(out + o, vb, l);
            o += l;
        }
        if (o<cap) out[o]=0;
    }
    return o;
}

typedef struct { char key[256]; uint64_t count; } agg_t;

int broker_query_handler(Request *req, Response *res){
    json_value_t *root = json_parse(req->body, req->body_len);
    if (!root || root->type != JSON_OBJECT){
        const char *e = "{\"ok\":false,\"error\":\"invalid_json\"}";
        memcpy(res->buffer,e,strlen(e)); res->buffer[strlen(e)] = 0;
        if (root) json_free(root);
        return 0;
    }

    json_value_t *jsrc   = json_get_field(root, "source");
    json_value_t *jflt   = json_get_field(root, "filter");
    json_value_t *jagg   = json_get_field(root, "aggregate");
    json_value_t *jgb    = jagg ? json_get_field(jagg, "group_by") : NULL;
    json_value_t *jtrans = json_get_field(root, "transform");
    json_value_t *jleft  = jtrans ? json_get_field(jtrans, "left_fields") : NULL;
    json_value_t *jemit  = jtrans ? json_get_field(jtrans, "emit_rows")   : NULL;

    if (!jsrc || jsrc->type != JSON_OBJECT){
        const char *e = "{\"ok\":false,\"error\":\"missing_source\"}";
        memcpy(res->buffer,e,strlen(e)); res->buffer[strlen(e)] = 0;
        json_free(root); return 0;
    }

    json_value_t *jt = json_get_field(jsrc, "topic");
    json_value_t *jg = json_get_field(jsrc, "group");
    json_value_t *jp = json_get_field(jsrc, "partition");
    json_value_t *jo = json_get_field(jsrc, "offset");
    json_value_t *jl = json_get_field(jsrc, "limit");

    if (!jt || jt->type!=JSON_STRING || !jp || (jp->type!=JSON_INT && jp->type!=JSON_DOUBLE)){
        const char *e = "{\"ok\":false,\"error\":\"bad_source_fields\"}";
        memcpy(res->buffer,e,strlen(e)); res->buffer[strlen(e)] = 0;
        json_free(root); return 0;
    }

    char topic[RF_MAX_TOPIC_LEN]; jv_to_cstr(jt, topic, sizeof topic);
    char group[RF_MAX_GROUP_LEN] = {0};
    if (jg && jg->type==JSON_STRING) jv_to_cstr(jg, group, sizeof group);

    int partition = (jp->type==JSON_INT) ? (int)jp->as.i : (int)jp->as.d;

    size_t limit = 200;
    if (jl && jl->type==JSON_INT && jl->as.i>0 && jl->as.i<=2000) limit = (size_t)jl->as.i;

    uint64_t from_exclusive = (uint64_t)-1;
    if (jo && (jo->type==JSON_INT || jo->type==JSON_DOUBLE)){
        uint64_t start = (jo->type==JSON_INT)? (uint64_t)jo->as.i : (uint64_t)jo->as.d;
        from_exclusive = (start==0) ? (uint64_t)-1 : (start-1);
    } else {
        uint64_t committed=0;
        if (rf_offset_load(topic, group, partition, &committed)) from_exclusive = committed;
    }

    size_t cap = (limit > 1024 ? 1024 : limit);
    rf_msg_view_t stack_msgs[256];
    rf_msg_view_t *msgs = (cap<=256) ? stack_msgs : slab_alloc(cap * sizeof(*msgs));
    if (!msgs) {
        static const char OOM_JSON[] = "{\"ok\":false,\"error\":\"oom\"}";
        memcpy(res->buffer, OOM_JSON, sizeof(OOM_JSON)); /* includes '\0' */
        json_free(root);
        return 0;
    }

    uint64_t next_offset=0;
    size_t want = (limit < cap) ? limit : cap;               /* FIX */
    size_t nread = rf_consume(topic, group, partition, from_exclusive, want, msgs, cap, &next_offset);

    agg_t aggs[128]; size_t agg_n=0;

    int emit_rows = (jemit && jemit->type==JSON_INT && jemit->as.i>=0) ? (int)jemit->as.i : 0;
    int rows_out = 0;
    int stop_rows = 0;

    jout_t out; jout_init_res(&out, res);

    /* reserve tail so we can always close JSON */
    const size_t TAIL_RESERVE = 512;
    char *hard_end = out.end;
    if ((size_t)(hard_end - res->buffer) > TAIL_RESERVE) out.end = hard_end - (ptrdiff_t)TAIL_RESERVE;

    (void)JOUT_LIT(&out, "{\"ok\":true,\"rows\":[");
    int first_row = 1;

    for (size_t i=0;i<nread;i++){
        const uint8_t *val = (const uint8_t*)msgs[i].val;
        size_t vl = msgs[i].val_len;
        if (vl==0 || val[0] != '{') continue;

        json_value_t *obj = json_parse((const char*)val, vl);
        if (!obj || obj->type != JSON_OBJECT){ if (obj) json_free(obj); continue; }

        if (!obj_matches_filters(obj, jflt)){ json_free(obj); continue; }

        /* aggregate count */
        if (jgb && jgb->type==JSON_ARRAY && jgb->as.array.count>0) {
            char gkey[256]; gkey[0]=0;
            build_group_key(obj, jgb, gkey, sizeof gkey);
            if (gkey[0]){
                size_t k=0; for (; k<agg_n; k++) if (strcmp(aggs[k].key, gkey)==0) break;
                if (k==agg_n && agg_n < (sizeof aggs/sizeof aggs[0])) {
                    strncpy(aggs[agg_n].key, gkey, sizeof aggs[agg_n].key-1);
                    aggs[agg_n].key[sizeof aggs[agg_n].key-1]=0;
                    aggs[agg_n].count=0;
                    agg_n++;
                }
                if (k < agg_n) aggs[k].count++;
            }
        }

        /* emit rows */
        if (!stop_rows && emit_rows > 0 && rows_out < emit_rows){
            char *mark = out.p;

            if (!first_row){
                if (!jout_try_c(&out, ',')) { out.p=mark; *out.p=0; stop_rows=1; json_free(obj); continue; }
            }

            if (!jleft || jleft->type!=JSON_ARRAY || jleft->as.array.count==0){
                /* emit full object: raw bytes */
                if (!jout_try_mem(&out, val, vl)) {
                    out.p=mark; *out.p=0; stop_rows=1;
                } else {
                    first_row = 0;
                    rows_out++;
                }
            } else {
                char rowbuf[4096];
                if (!build_projected_object_json(rowbuf, sizeof rowbuf, obj, jleft)) {
                    out.p=mark; *out.p=0; stop_rows=1;
                } else {
                    size_t rl = strlen(rowbuf);
                    if (!jout_try_mem(&out, rowbuf, rl)) {
                        out.p=mark; *out.p=0; stop_rows=1;
                    } else {
                        first_row = 0;
                        rows_out++;
                    }
                }
            }
        }

        json_free(obj);
    }

    (void)JOUT_LIT(&out, "],\"aggregates\":[");

    int first_agg = 1;
    for (size_t i=0;i<agg_n;i++){
        char rec[512];
        jout_t w; jout_init_buf(&w, rec, sizeof rec);

        if (!first_agg) { if (!jout_try_c(&w, ',')) break; }
        first_agg = 0;

        char esc[512]; int tr=0;
        size_t e = json_escape_into_ex(esc, sizeof esc-1,
                                       (const uint8_t*)aggs[i].key, strlen(aggs[i].key), &tr);
        esc[e]=0;
        if (tr) break;

        if (!jout_try_printf(&w, "{\"key\":\"%s\",\"count\":%" PRIu64 "}", esc, aggs[i].count)) break;

        size_t rl = strlen(rec);
        if (!jout_try_mem(&out, rec, rl)) break;
    }

    /* restore end and write tail (always fits because of reserve) */
    out.end = hard_end;

    (void)jout_try_printf(&out,
                          "],\"source\":{\"topic\":\"%s\",\"group\":\"%s\",\"partition\":%d,\"next_offset\":%" PRIu64 ",\"consumed\":%zu}}",
                          topic, group, partition, next_offset, nread);

    res->buffer[RESPONSE_BUFFER_SIZE - 1] = '\0';

    if (msgs != stack_msgs) slab_free(msgs);
    json_free(root);
    return 0;
}
typedef struct {
    char        key[512];
    uint64_t    ts_ms;
    uint64_t    offset;
    json_value_t *obj;
    const char  *raw;
    size_t      raw_len;
} join_evt_t;



static inline int jv_to_cstr2(const json_value_t *v, char *out, size_t cap){
    if (!v || !out || cap==0) return 0;
    if (v->type == JSON_STRING) {
        size_t n = v->as.s.len < cap-1 ? v->as.s.len : cap-1;
        memcpy(out, v->as.s.ptr, n); out[n]=0; return 1;
    }
    if (v->type == JSON_INT) {
        int n = snprintf(out, cap, "%" PRId64, (int64_t) v->as.i);
        if (n < 0) n = 0;
        if ((size_t) n >= cap) out[cap - 1] = 0;
        return 1;
    }
    if (v->type == JSON_DOUBLE) {
        int n = snprintf(out, cap, "%.17g", v->as.d);
        if (n < 0) n = 0;
        if ((size_t) n >= cap) out[cap - 1] = 0;
        return 1;
    }
    if (v->type == JSON_BOOL) {
        int n = snprintf(out, cap, "%s", v->as.b ? "true" : "false");
        if (n < 0) n = 0;
        if ((size_t) n >= cap) out[cap - 1] = 0;
        return 1;
    }
    return 0;
}

static inline int extract_key_single(json_value_t *obj, const char *field, char *out, size_t cap){
    if (!obj || obj->type!=JSON_OBJECT) return 0;
    json_value_t *v = json_get_field(obj, field);
    if (!v) return 0;
    return jv_to_cstr2(v, out, cap);
}


static inline int extract_key_composite(json_value_t *obj, json_value_t *fields_arr, char *out, size_t cap){
    if (!obj || obj->type!=JSON_OBJECT || !fields_arr || fields_arr->type!=JSON_ARRAY) return 0;
    char *p = out, *end = out + cap;
    int first = 1;
    for (size_t i=0;i<fields_arr->as.array.count;i++){
        json_value_t *fn = &fields_arr->as.array.items[i];
        if (fn->type!=JSON_STRING) continue;
        char name[64]; jv_to_cstr2(fn, name, sizeof name);
        json_value_t *v = json_get_field(obj, name);
        if (!v) return 0; /* strict: missing field fails key extraction */
        char piece[256]; if (!jv_to_cstr2(v, piece, sizeof piece)) return 0;
        if (!first) { if (p<end) *p++='|'; } else first=0;
        size_t n = strnlen(piece, sizeof piece);
        if (p + n >= end) { /* truncate safely */ n = (size_t)(end - p - 1); }
        memcpy(p, piece, n); p += n;
    }
    if (p>=end) out[cap-1]=0; else *p=0;
    return 1;
}

static inline int build_key(json_value_t *obj, json_value_t *keys_arr, const char *single_key, char *out, size_t cap){
    if (keys_arr && keys_arr->type==JSON_ARRAY) return extract_key_composite(obj, keys_arr, out, cap);
    if (single_key && *single_key)             return extract_key_single(obj, single_key, out, cap);
    return 0;
}

static inline void write_projected_object_or_missing(char **pp, char *end, json_value_t *obj,
                                                     json_value_t *fields, int missing_as_empty){
    char *p = *pp;
    if (!obj){
        if (missing_as_empty){ if (p<end) *p++='{'; if (p<end) *p++='}'; *pp = p; return; }
        /* null */
        if (p < end) *p++ = 'n';
        if (p < end) *p++ = 'u';
        if (p < end) *p++ = 'l';
        if (p < end) *p++ = 'l';
        *pp = p;
        return;
    }
    /* same as before */
    *p++ = '{';
    int first=1;
    if (fields && fields->type==JSON_ARRAY && fields->as.array.count>0) {
        for (size_t i=0;i<fields->as.array.count;i++){
            json_value_t *fn = &fields->as.array.items[i];
            if (fn->type!=JSON_STRING) continue;
            char name[64]; jv_to_cstr2(fn, name, sizeof name);
            json_value_t *fv = json_get_field(obj, name);
            if (!fv) continue;
            if (!first) { if (p<end) *p++=','; } else first=0;
            int n = snprintf(p, (size_t)(end-p), "\"%s\":", name);
            if (n<0) n=0;
            p += (n<(end-p)?n:(int)(end-p));
            if (fv->type == JSON_STRING){
                char esc[512]; size_t e = json_escape_into(esc, sizeof esc-1, (const uint8_t*)fv->as.s.ptr, fv->as.s.len); esc[e]=0;
                n = snprintf(p, (size_t)(end-p), "\"%s\"", esc);
                if (n < 0) n = 0;
                p += (n < (end - p) ? n : (int)(end - p));
            } else if (fv->type == JSON_INT){
                n = snprintf(p, (size_t)(end-p), "%" PRId64, (int64_t)fv->as.i);
                if (n < 0) n = 0;
                p += (n < (end - p) ? n : (int)(end - p));
            } else if (fv->type == JSON_DOUBLE){
                n = snprintf(p, (size_t)(end-p), "%.17g", fv->as.d);
                if (n < 0) n = 0;
                p += (n < (end - p) ? n : (int)(end - p));
            } else if (fv->type == JSON_BOOL){
                n = snprintf(p, (size_t)(end-p), "%s", fv->as.b?"true":"false");
                if (n < 0) n = 0;
                p += (n < (end - p) ? n : (int)(end - p));
            } else {
                if (p < end) *p++ = 'n';
                if (p < end) *p++ = 'u';
                if (p < end) *p++ = 'l';
                if (p < end) *p++ = 'l';
            }
        }
    }
    if (p<end) *p++='}';
    *pp = p;
}

static inline uint32_t djb2_32(const char *s){
    uint32_t h=5381u; for (; *s; ++s) h = ((h<<5)+h) ^ (uint8_t)*s; return h? h:1;
}

int broker_join_handler(Request *req, Response *res){
    json_value_t *root = json_parse(req->body, req->body_len);
    if (!root || root->type!=JSON_OBJECT){
        const char *e = "{\"ok\":false,\"error\":\"invalid_json\"}";
        memcpy(res->buffer,e,strlen(e)); res->buffer[strlen(e)]=0;
        if (root) json_free(root);
        return 0;
    }

    json_value_t *jl = json_get_field(root,"left");
    json_value_t *jr = json_get_field(root,"right");
    json_value_t *jon = json_get_field(root,"on");
    json_value_t *jwin= json_get_field(root,"window");
    json_value_t *jemit=json_get_field(root,"emit");
    json_value_t *jjoin=json_get_field(root,"join");
    json_value_t *jded =json_get_field(root,"dedupe");
    json_value_t *jded_once = json_get_field(root,"dedupe_once");

    if (!jl||!jr||!jon || jl->type!=JSON_OBJECT || jr->type!=JSON_OBJECT || jon->type!=JSON_OBJECT){
        const char *e = "{\"ok\":false,\"error\":\"bad_request_fields\"}";
        memcpy(res->buffer,e,strlen(e)); res->buffer[strlen(e)]=0; json_free(root); return 0;
    }

    // left source
    char lt[RF_MAX_TOPIC_LEN], lg[RF_MAX_GROUP_LEN]={0};
    int lp=0; uint64_t loff=(uint64_t)-1; size_t llim=500;
    json_value_t *ltv=json_get_field(jl,"topic"); json_value_t *lpg=json_get_field(jl,"partition");
    json_value_t *lgg=json_get_field(jl,"group"); json_value_t *lof=json_get_field(jl,"offset");
    json_value_t *lll=json_get_field(jl,"limit");
    if (!ltv||ltv->type!=JSON_STRING||!lpg|| (lpg->type!=JSON_INT && lpg->type!=JSON_DOUBLE)){
        const char *e = "{\"ok\":false,\"error\":\"bad_left_source\"}";
        memcpy(res->buffer,e,strlen(e)); res->buffer[strlen(e)]=0; json_free(root); return 0;
    }
    jv_to_cstr2(ltv, lt, sizeof lt);
    lp = (lpg->type==JSON_INT)? (int)lpg->as.i : (int)lpg->as.d;
    if (lgg && lgg->type==JSON_STRING) jv_to_cstr2(lgg, lg, sizeof lg);
    if (lll && lll->type==JSON_INT && lll->as.i>0 && lll->as.i<=2000) llim=(size_t)lll->as.i;
    if (lof && (lof->type==JSON_INT || lof->type==JSON_DOUBLE)){
        uint64_t s = (lof->type==JSON_INT)? (uint64_t)lof->as.i : (uint64_t)lof->as.d;
        loff = (s==0)? (uint64_t)-1 : (s-1);
    } else {
        uint64_t committed=0; if (rf_offset_load(lt, lg, lp, &committed)) loff=committed;
    }

    // right source
    char rt[RF_MAX_TOPIC_LEN], rg[RF_MAX_GROUP_LEN]={0};
    int rp=0; uint64_t roff=(uint64_t)-1; size_t rlim=500;
    json_value_t *rtv=json_get_field(jr,"topic"); json_value_t *rpg=json_get_field(jr,"partition");
    json_value_t *rgg=json_get_field(jr,"group"); json_value_t *rof=json_get_field(jr,"offset");
    json_value_t *rll=json_get_field(jr,"limit");
    if (!rtv||rtv->type!=JSON_STRING||!rpg|| (rpg->type!=JSON_INT && rpg->type!=JSON_DOUBLE)){
        const char *e = "{\"ok\":false,\"error\":\"bad_right_source\"}";
        memcpy(res->buffer,e,strlen(e)); res->buffer[strlen(e)]=0; json_free(root); return 0;
    }
    jv_to_cstr2(rtv, rt, sizeof rt);
    rp = (rpg->type==JSON_INT)? (int)rpg->as.i : (int)rpg->as.d;
    if (rgg && rgg->type==JSON_STRING) jv_to_cstr2(rgg, rg, sizeof rg);
    if (rll && rll->type==JSON_INT && rll->as.i>0 && rll->as.i<=2000) rlim=(size_t)rll->as.i;
    if (rof && (rof->type==JSON_INT || rof->type==JSON_DOUBLE)){
        uint64_t s = (rof->type==JSON_INT)? (uint64_t)rof->as.i : (uint64_t)rof->as.d;
        roff = (s==0)? (uint64_t)-1 : (s-1);
    } else {
        uint64_t committed=0; if (rf_offset_load(rt, rg, rp, &committed)) roff=committed;
    }

    // join keys (single or composite)
    json_value_t *lkeys = json_get_field(jon,"left_keys");
    json_value_t *rkeys = json_get_field(jon,"right_keys");
    json_value_t *lk1   = json_get_field(jon,"left_key");
    json_value_t *rk1   = json_get_field(jon,"right_key");
    char left_key_single[64]={0}, right_key_single[64]={0};
    if ((!lkeys && (!lk1 || lk1->type!=JSON_STRING)) ||
        (!rkeys && (!rk1 || rk1->type!=JSON_STRING))){
        const char *e = "{\"ok\":false,\"error\":\"missing_join_keys\"}";
        memcpy(res->buffer,e,strlen(e)); res->buffer[strlen(e)]=0; json_free(root); return 0;
    }
    if (lk1 && lk1->type==JSON_STRING) jv_to_cstr2(lk1, left_key_single, sizeof left_key_single);
    if (rk1 && rk1->type==JSON_STRING) jv_to_cstr2(rk1, right_key_single, sizeof right_key_single);

    // window
    uint64_t size_ms = 60000, lateness_ms = 0;
    if (jwin && jwin->type==JSON_OBJECT){
        json_value_t *sz=json_get_field(jwin,"size_ms");
        json_value_t *al=json_get_field(jwin,"allowed_lateness_ms");
        if (sz && sz->type==JSON_INT && sz->as.i>0) size_ms=(uint64_t)sz->as.i;
        if (al && al->type==JSON_INT && al->as.i>=0) lateness_ms=(uint64_t)al->as.i;
    }

    // join type
    int join_type = 0; // 0=inner,1=left,2=right,3=full
    if (jjoin && jjoin->type==JSON_OBJECT){
        json_value_t *jt = json_get_field(jjoin,"type");
        if (jt && jt->type==JSON_STRING){
            char tbuf[16]; jv_to_cstr2(jt,tbuf,sizeof tbuf);
            if (!strcmp(tbuf,"left")) join_type=1;
            else if (!strcmp(tbuf,"right")) join_type=2;
            else if (!strcmp(tbuf,"full")) join_type=3;
            else join_type=0;
        }
    }

    // dedupe_once
    int dedupe_once = 0;
    if (jded_once && jded_once->type==JSON_BOOL) dedupe_once = jded_once->as.b ? 1 : 0;
    if (jded && jded->type==JSON_OBJECT){
        json_value_t *mode = json_get_field(jded,"mode");
        if (mode && mode->type==JSON_STRING){
            char mbuf[16]; jv_to_cstr2(mode,mbuf,sizeof mbuf);
            if (!strcmp(mbuf,"once")) dedupe_once = 1;
        }
    }

    // emit
    json_value_t *lf = jemit? json_get_field(jemit,"left_fields"):NULL;
    json_value_t *rf = jemit? json_get_field(jemit,"right_fields"):NULL;
    int max_rows = 200;
    int missing_as_empty = 0; /* default null */
    if (jemit){
        json_value_t *mr = json_get_field(jemit,"max_rows");
        if (mr && mr->type==JSON_INT && mr->as.i>0 && mr->as.i<=5000) max_rows = (int)mr->as.i;
        json_value_t *ma = json_get_field(jemit,"missing_as");
        if (ma && ma->type==JSON_STRING){
            char abuf[16]; jv_to_cstr2(ma,abuf,sizeof abuf);
            if (!strcmp(abuf,"empty")) missing_as_empty=1;
        }
    }

    // consume both sides
    size_t lcap = (llim>2048?2048:llim), rcap = (rlim>2048?2048:rlim);
    rf_msg_view_t lstack[256], rstack[256];
    rf_msg_view_t *lmsgs = (lcap<=256) ? lstack : slab_alloc(lcap*sizeof(*lmsgs));
    rf_msg_view_t *rmsgs = (rcap<=256) ? rstack : slab_alloc(rcap*sizeof(*rmsgs));
    if (!lmsgs || !rmsgs){
        const char *e = "{\"ok\":false,\"error\":\"oom_msgs\"}";
        memcpy(res->buffer,e,strlen(e)); res->buffer[strlen(e)]=0;
        if (lmsgs && lmsgs!=lstack) slab_free(lmsgs);
        if (rmsgs && rmsgs!=rstack) slab_free(rmsgs);
        json_free(root); return 0;
    }

    uint64_t lnext=0, rnext=0;
    size_t ln = rf_consume(lt, lg, lp, loff, llim, lmsgs, lcap, &lnext);
    size_t rn = rf_consume(rt, rg, rp, roff, rlim, rmsgs, rcap, &rnext);

    // collect JSON objects with computed keys
    join_evt_t *L = slab_alloc(ln * sizeof(join_evt_t));
    join_evt_t *R = slab_alloc(rn * sizeof(join_evt_t));
    size_t Ln=0, Rn=0;
    uint64_t lmax=0, rmax=0;

    for (size_t i=0;i<ln;i++){
        const uint8_t *v = (const uint8_t*)lmsgs[i].val; size_t vl = lmsgs[i].val_len;
        if (vl==0 || v[0]!='{') continue;
        json_value_t *obj = json_parse((const char*)v, vl);
        if (!obj || obj->type!=JSON_OBJECT){ if (obj) json_free(obj); continue; }
        char key[sizeof L[0].key];
        if (!build_key(obj, lkeys, left_key_single, key, sizeof key)) { json_free(obj); continue; }
        strncpy(L[Ln].key, key, sizeof L[Ln].key-1); L[Ln].key[sizeof L[Ln].key-1]=0;
        L[Ln].ts_ms = lmsgs[i].ts_us / 1000ULL;
        L[Ln].offset= lmsgs[i].offset;
        L[Ln].obj   = obj;
        if (L[Ln].ts_ms > lmax) lmax = L[Ln].ts_ms;
        Ln++;
    }
    for (size_t i=0;i<rn;i++){
        const uint8_t *v = (const uint8_t*)rmsgs[i].val; size_t vl = rmsgs[i].val_len;
        if (vl==0 || v[0]!='{') continue;
        json_value_t *obj = json_parse((const char*)v, vl);
        if (!obj || obj->type!=JSON_OBJECT){ if (obj) json_free(obj); continue; }
        char key[sizeof R[0].key];
        if (!build_key(obj, rkeys, right_key_single, key, sizeof key)) { json_free(obj); continue; }
        strncpy(R[Rn].key, key, sizeof R[Rn].key-1); R[Rn].key[sizeof R[Rn].key-1]=0;
        R[Rn].ts_ms = rmsgs[i].ts_us / 1000ULL;
        R[Rn].offset= rmsgs[i].offset;
        R[Rn].obj   = obj;
        if (R[Rn].ts_ms > rmax) rmax = R[Rn].ts_ms;
        Rn++;
    }

    // watermark and staleness cutoff
    uint64_t wm = 0;
    if (lmax && rmax) {
        wm = ((lmax < rmax)? lmax : rmax);
        if (wm > lateness_ms) wm -= lateness_ms; else wm = 0;
    }
    uint64_t min_keep = (wm>size_ms)? (wm - size_ms) : 0;
    size_t Lk=0, Rk=0;
    for (size_t i=0;i<Ln;i++) if (L[i].ts_ms >= min_keep) L[Lk++] = L[i]; else json_free(L[i].obj);
    for (size_t i=0;i<Rn;i++) if (R[i].ts_ms >= min_keep) R[Rk++] = R[i]; else json_free(R[i].obj);
    Ln = Lk; Rn = Rk;

    // index right by key
    enum { BUCKS = 4096 };
    int *head = slab_alloc(BUCKS * sizeof(int));
    int *next = slab_alloc(Rn * sizeof(int));
    if (!head || !next){
        const char *e = "{\"ok\":false,\"error\":\"oom_index\"}";
        memcpy(res->buffer,e,strlen(e)); res->buffer[strlen(e)]=0;
        for (size_t i=0;i<Ln;i++) if (L[i].obj) json_free(L[i].obj);
        for (size_t i=0;i<Rn;i++) if (R[i].obj) json_free(R[i].obj);
        if (head) slab_free(head);
        if (next) slab_free(next);
        if (lmsgs!=lstack) slab_free(lmsgs);
        if (rmsgs!=rstack) slab_free(rmsgs);
        slab_free(L); slab_free(R); json_free(root); return 0;
    }
    for (int i=0;i<BUCKS;i++) head[i]=-1;
    for (size_t i=0;i<Rn;i++){
        uint32_t h = djb2_32(R[i].key) & (BUCKS-1);
        next[i] = head[h];
        head[h] = (int)i;
    }

    // match flags for outers
    uint8_t *mL = slab_alloc(Ln ? Ln : 1);
    uint8_t *mR = slab_alloc(Rn ? Rn : 1);
    if (Ln) memset(mL,0,Ln);
    if (Rn) memset(mR,0,Rn);

    // emit rows
    char *p = res->buffer, *end = res->buffer + RESPONSE_BUFFER_SIZE - 1;
#define APP(...) do { int __n = snprintf(p, (size_t)(end-p), __VA_ARGS__); if (__n<0) __n=0; p += (__n<(end-p)?__n:(int)(end-p)); } while(0)

    APP("{\"ok\":true,\"rows\":[");
    int emitted=0; int firstRow=1;
    int outer_left_cnt=0, outer_right_cnt=0;

    // inner pairs (with optional dedupe_once)
    for (size_t i=0;i<Ln && emitted<max_rows;i++){
        uint32_t h = djb2_32(L[i].key) & (BUCKS-1);
        for (int j=head[h]; j!=-1 && emitted<max_rows; j=next[j]){
            if (strcmp(L[i].key, R[j].key)!=0) continue;
            uint64_t dt = (L[i].ts_ms > R[j].ts_ms) ? (L[i].ts_ms - R[j].ts_ms) : (R[j].ts_ms - L[i].ts_ms);
            if (dt > size_ms) continue; /* outside window */
            if (dedupe_once && !(L[i].offset <= R[j].offset)) continue;

            if (!firstRow) { if (p<end) *p++=','; } else firstRow=0;
            char esc[512]; size_t e = json_escape_into(esc, sizeof esc-1, (const uint8_t*)L[i].key, strlen(L[i].key)); esc[e]=0;
            APP("{\"key\":\"%s\",\"left_ts_ms\":%" PRIu64 ",\"right_ts_ms\":%" PRIu64 ",\"left\":", esc, L[i].ts_ms, R[j].ts_ms);
            write_projected_object_or_missing(&p, end, L[i].obj, lf, 0);
            APP(",\"right\":");
            write_projected_object_or_missing(&p, end, R[j].obj, rf, 0);
            if (p<end) *p++='}';
            emitted++;
            mL[i]=1; mR[j]=1;
        }
    }

    // outers
    if ((join_type==1 || join_type==3) && emitted<max_rows){ /* left/full */
        for (size_t i=0;i<Ln && emitted<max_rows;i++){
            if (mL[i]) continue;
            if (!firstRow) { if (p<end) *p++=','; } else firstRow=0;
            char esc[512]; size_t e = json_escape_into(esc, sizeof esc-1, (const uint8_t*)L[i].key, strlen(L[i].key)); esc[e]=0;
            APP("{\"key\":\"%s\",\"left_ts_ms\":%" PRIu64 ",\"right_ts_ms\":null,\"left\":", esc, L[i].ts_ms);
            write_projected_object_or_missing(&p, end, L[i].obj, lf, 0);
            APP(",\"right\":");
            write_projected_object_or_missing(&p, end, NULL, rf, missing_as_empty);
            if (p<end) *p++='}';
            emitted++; outer_left_cnt++;
        }
    }
    if ((join_type==2 || join_type==3) && emitted<max_rows){ /* right/full */
        for (size_t j=0;j<Rn && emitted<max_rows;j++){
            if (mR[j]) continue;
            if (!firstRow) { if (p<end) *p++=','; } else firstRow=0;
            char esc[512]; size_t e = json_escape_into(esc, sizeof esc-1, (const uint8_t*)R[j].key, strlen(R[j].key)); esc[e]=0;
            APP("{\"key\":\"%s\",\"left_ts_ms\":null,\"right_ts_ms\":%" PRIu64 ",\"left\":", esc, R[j].ts_ms);
            write_projected_object_or_missing(&p, end, NULL, lf, missing_as_empty);
            APP(",\"right\":");
            write_projected_object_or_missing(&p, end, R[j].obj, rf, 0);
            if (p<end) *p++='}';
            emitted++; outer_right_cnt++;
        }
    }

    APP("],\"next_offsets\":{\"left\":%" PRIu64 ",\"right\":%" PRIu64 "},"
                                                                      "\"stats\":{\"left_scanned\":%zu,\"right_scanned\":%zu,\"rows\":%d,"
                                                                      "\"watermark_ms\":%" PRIu64 ",\"outer_left\":%d,\"outer_right\":%d,"
                                                                                                  "\"dedupe_once\":%s}}",
        lnext, rnext, Ln, Rn, emitted, wm, outer_left_cnt, outer_right_cnt,
        dedupe_once?"true":"false");
    *p = 0;

    // cleanup
    for (size_t i=0;i<Ln;i++) if (L[i].obj) json_free(L[i].obj);
    for (size_t i=0;i<Rn;i++) if (R[i].obj) json_free(R[i].obj);
    if (mL) slab_free(mL);
    if (mR) slab_free(mR);
    slab_free(L); slab_free(R);
    slab_free(head); slab_free(next);
    if (lmsgs!=lstack)
        slab_free(lmsgs);
    if (rmsgs!=rstack) slab_free(rmsgs);
    json_free(root);
    return 0;
}
// Health check optimized for monitoring tools
int health_fast(Request *req, Response *res) {
    (void)req;

    // Pre-computed response (only 8 bytes!)
    static const char health_response[] = "{\"ok\":1}";
    static const size_t health_len = sizeof(health_response) - 1;

    memcpy(res->buffer, health_response, health_len);
    res->buffer[health_len] = '\0';
    return 0;
}

// Admin compaction with progress tracking
int compact_handler_fast(Request *req, Response *res) {
    (void)req;

    // Start compaction in background
    Persistence_compact();

    // Immediate response (don't wait for completion)
    static const char compact_response[] = "{\"result\":\"compaction_started\",\"async\":true}";
    static const size_t compact_len = sizeof(compact_response) - 1;

    memcpy(res->buffer, compact_response, compact_len);
    res->buffer[compact_len] = '\0';

    return 0;
}

int prometheus_metrics_handler(Request* req, Response* res) {
    (void)req;
    RAMForge_export_prometheus_metrics_buffer(res->buffer, RESPONSE_BUFFER_SIZE);
    return 0;
}

int ha_metrics_handler(Request* req, Response* res) {
    (void)req;
    RAMForge_HA_export_metrics(res->buffer, RESPONSE_BUFFER_SIZE);
    return 0;
}
static inline uint64_t monotonic_us(void){
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec*1000000ull + ts.tv_nsec/1000ull;
}
int broker_produce_raw(Request *req, Response *res) {

    uint64_t t0 = monotonic_us();
    uint64_t timing_flag = 0;
    int want_timing = qs_u64(req->query_string, "timing", &timing_flag) && timing_flag == 1;
    /* path: /broker/topics/:topic/produce */
    const char *topic=NULL;
    for (int i=0;i<req->param_count;i++)
        if (!topic && strcmp(req->params[i].name,"topic")==0) topic=req->params[i].value;

    if (!topic || !*topic) {
        send_error(res, &(error_response_t){
                .error = "missing_topic",
                .message = "Topic name is required in URL path: /broker/topics/{topic}/produce",
                .docs = "https://ayder.dev/docs/api/produce"
        });
        return -1;
    }

    char topic_buf[RF_MAX_TOPIC_LEN];
    size_t tlen=strlen(topic); if (tlen>=sizeof(topic_buf)) tlen=sizeof(topic_buf)-1;
    memcpy(topic_buf, topic, tlen); topic_buf[tlen]=0;

    uint64_t part_u64=UINT64_MAX; int has_part = qs_u64(req->query_string,"partition",&part_u64);
    int partition = has_part ? (int)part_u64 : -1;

    const char *key_q=NULL,*idk_q=NULL; size_t key_q_len=0,idk_q_len=0;
    qs_str(req->query_string,"key",&key_q,&key_q_len);
    qs_str(req->query_string,"idempotency_key",&idk_q,&idk_q_len);

    char key_buf[512]; size_t key_len=0;
    if (key_q) key_len = url_decode_into(key_buf, sizeof(key_buf), key_q, key_q_len);

    char idk_buf[512]; size_t idk_len=0;
    if (idk_q) idk_len = url_decode_into(idk_buf, sizeof(idk_buf), idk_q, idk_q_len);

    const void *val_ptr = req->body; size_t val_len = req->body_len;

    if (idk_len){
        uint64_t offset=0,batch_id=0; int dup=0;
        int rc = rf_produce_sealed_idemp(topic_buf, partition,
                                         key_len?key_buf:NULL, key_len,
                                         val_ptr, val_len,
                                         idk_buf, idk_len,
                                         &offset, &batch_id, &dup);
        if (rc < 0) {
            send_error(res, &(error_response_t){
                    .error = "produce_failed",
                    .message = "Failed to write message to partition. Check topic exists and partition is valid.",
                    .docs = "https://ayder.dev/docs/api/produce#troubleshooting"
            });
            return -1;
        }
        if (dup){
            int n = snprintf(res->buffer, RESPONSE_BUFFER_SIZE,
                             "{\"ok\":true,\"offset\":%" PRIu64 ",\"partition\":%d,"
                             "\"sealed\":true,\"synced\":null,\"duplicate\":true}",
                             offset, partition);
            res->buffer[(n>0&&n<(int)RESPONSE_BUFFER_SIZE)?n:(RESPONSE_BUFFER_SIZE-1)]=0; return 0;
        }
        int n = snprintf(res->buffer, RESPONSE_BUFFER_SIZE,
                         "{\"ok\":true,\"offset\":%" PRIu64 ",\"partition\":%d,"
                         "\"batch_id\":%" PRIu64 ",\"sealed\":true,\"synced\":true,"
                         "\"duplicate\":false}", offset, partition, batch_id);
        res->buffer[(n>0&&n<(int)RESPONSE_BUFFER_SIZE)?n:(RESPONSE_BUFFER_SIZE-1)]=0; return 0;
    } else {
        uint64_t offset=0,batch_id=0;
        int rc = rf_produce_sealed(topic_buf, partition,
                                   key_len?key_buf:NULL, key_len,
                                   val_ptr, val_len, &offset, &batch_id);
        if (rc < 0) {
            send_error(res, &(error_response_t){
                    .error = "produce_failed",
                    .message = "Failed to write message to partition. Check topic exists and partition is valid.",
                    .docs = "https://ayder.dev/docs/api/produce#troubleshooting"
            });
            return -1;
        }

        int timeout_ms=0; uint64_t tmp;
        if (qs_u64(req->query_string,"timeout_ms",&tmp)) timeout_ms=(int)tmp;

        uint64_t t_after_produce = monotonic_us();
        int sync_state = wait_synced_until(batch_id, timeout_ms);

        uint64_t t_end = monotonic_us();

        uint64_t server_us    = t_end - t0;
        uint64_t produce_us   = t_after_produce - t0;
        uint64_t sync_wait_us = t_end - t_after_produce;


        const char *mode = (batch_id==0) ? "rocket":"sealed";
        int durable = (batch_id!=0);
        const char *synced_json = (sync_state<0) ? "null" : (sync_state ? "true":"false");

        if (want_timing) {
            uint64_t queue_us = rf_http_queue_us;
            uint64_t recv_parse_us = rf_http_recv_parse_us;

            int n = snprintf(res->buffer, RESPONSE_BUFFER_SIZE,
                             "{\"ok\":true,\"offset\":%" PRIu64 ",\"partition\":%d,"
                             "\"batch_id\":%" PRIu64 ",\"sealed\":true,"
                             "\"durable\":%s,\"mode\":\"%s\",\"synced\":%s,"
                             "\"queue_us\":%" PRIu64 ",\"recv_parse_us\":%" PRIu64 ","
                             "\"server_us\":%" PRIu64 ",\"produce_us\":%" PRIu64 ",\"sync_wait_us\":%" PRIu64 "}",
                             offset, partition, batch_id,
                             durable?"true":"false", mode, synced_json,
                             queue_us, recv_parse_us,
                             server_us, produce_us, sync_wait_us);

            res->buffer[(n>0 && n<(int)RESPONSE_BUFFER_SIZE)?n:(RESPONSE_BUFFER_SIZE-1)] = 0;
        } else {
            int n = snprintf(res->buffer, RESPONSE_BUFFER_SIZE,
                             "{\"ok\":true,\"offset\":%" PRIu64 ",\"partition\":%d,"
                             "\"batch_id\":%" PRIu64 ",\"sealed\":true,"
                             "\"durable\":%s,\"mode\":\"%s\",\"synced\":%s}",
                             offset, partition, batch_id,
                             durable?"true":"false", mode, synced_json);
            res->buffer[(n>0 && n<(int)RESPONSE_BUFFER_SIZE)?n:(RESPONSE_BUFFER_SIZE-1)] = 0;
        }
        return 0;
    }
}
int broker_produce_ndjson(Request *req, Response *res) {
    /* path: /broker/topics/:topic/produce-ndjson */
    const char *topic=NULL;
    for (int i=0;i<req->param_count;i++)
        if (!topic && strcmp(req->params[i].name,"topic")==0) topic=req->params[i].value;
    if (!topic || !*topic){ memcpy(res->buffer, "{\"error\":\"Missing topic\"}", 26); res->buffer[26]=0; return -1; }

    char topic_buf[RF_MAX_TOPIC_LEN];
    size_t tlen=strlen(topic); if (tlen>=sizeof(topic_buf)) tlen=sizeof(topic_buf)-1;
    memcpy(topic_buf, topic, tlen); topic_buf[tlen]=0;

    uint64_t part_u64=UINT64_MAX; int has_part = qs_u64(req->query_string,"partition",&part_u64);
    int partition = has_part ? (int)part_u64 : -1;

    /* optional whole-batch idempotency key */
    const char *idk_q=NULL; size_t idk_q_len=0;
    qs_str(req->query_string,"idempotency_key",&idk_q,&idk_q_len);
    char idk_buf[512]; size_t idk_len = idk_q ? url_decode_into(idk_buf,sizeof(idk_buf),idk_q,idk_q_len) : 0;

    /* pass 1: estimate line count (cap 1000 like before) */
    const char *in  = (const char*)req->body;
    const char *end = in + req->body_len;
    size_t lines=1;
    for (const char *p=in;p<end;p++) if (*p=='\n') lines++;
    if (lines > 1000) lines = 1000;

    rf_msg_view_t stack_msgs[128];
    rf_msg_view_t *msgs = (lines <= 128) ? stack_msgs : slab_alloc(lines * sizeof(*msgs));
    if (!msgs){ memcpy(res->buffer, "{\"error\":\"Memory allocation failed\"}", 36); res->buffer[36]=0; return -1; }

    size_t count=0;
    const char *line = in;

    for (const char *p=in; p<=end; p++){
        if (p==end || *p=='\n'){
            size_t len = (size_t)(p - line);
            if (len && line[len-1]=='\r') len--;   /* trim CR only; keep leading spaces exactly */

            if (len){
                /* default: whole line bytes */
                const void  *val_ptr = line;
                size_t       val_len = len;

                /* if it looks like JSON, try to parse; only unwrap JSON STRING values */
                unsigned char first = (unsigned char)*line;
                int looks_json = (first=='{' || first=='[' || first=='"' ||
                                  first=='-' || (first>='0'&&first<='9') ||
                                  first=='t' || first=='f' || first=='n');
                if (looks_json){
                    json_value_t *j = json_parse(line, len);
                    if (j){
                        if (j->type == JSON_STRING){
                            /* unwrap: "alpha" -> alpha */
                            val_ptr = j->as.s.ptr;
                            val_len = (size_t)j->as.s.len;
                        }
                        json_free(j);
                    }
                }

                msgs[count].key     = NULL;  /* no per-line keys in NDJSON generic mode */
                msgs[count].key_len = 0;
                msgs[count].val     = val_ptr;
                msgs[count].val_len = val_len;
                count++;
                if (count==lines) break;
            }
            line = p + (p<end ? 1:0);
        }
    }

    if (count==0){
        const char *e = "{\"error\":\"No valid messages\"}";
        memcpy(res->buffer, e, strlen(e)); res->buffer[strlen(e)]=0;
        if (msgs!=stack_msgs) slab_free(msgs);
        return -1;
    }

    uint64_t first_offset=0, batch_id=0; int duplicate=0; int rc;
    if (idk_len){
        rc = rf_produce_batch_sealed_idemp(topic_buf, partition, msgs, count,
                                           idk_buf, idk_len,
                                           &first_offset, &batch_id, &duplicate);
    } else {
        rc = rf_produce_batch_sealed(topic_buf, partition, msgs, count,
                                     &first_offset, &batch_id);
    }

    if (msgs!=stack_msgs) slab_free(msgs);

    if (rc<0){
        memcpy(res->buffer, "{\"error\":\"Sealed batch produce failed\"}", 39);
        res->buffer[39]=0; return -1;
    }

    if (idk_len && duplicate){
        int n = snprintf(res->buffer, RESPONSE_BUFFER_SIZE,
                         "{\"ok\":true,\"first_offset\":%" PRIu64 ",\"count\":%zu,"
                         "\"partition\":%d,\"sealed\":true,\"synced\":null,\"duplicate\":true}",
                         first_offset, count, partition);
        res->buffer[(n>0&&n<(int)RESPONSE_BUFFER_SIZE)?n:(RESPONSE_BUFFER_SIZE-1)]=0;
        return 0;
    }

    int timeout_ms=0; uint64_t tmp;
    if (qs_u64(req->query_string,"timeout_ms",&tmp)) timeout_ms=(int)tmp;
    int sync_state = wait_synced_until(batch_id, timeout_ms);
    const char *mode = (batch_id==0) ? "rocket":"sealed";
    int durable = (batch_id!=0);
    const char *synced_json = (sync_state<0) ? "null" : (sync_state?"true":"false");

    int n = snprintf(res->buffer, RESPONSE_BUFFER_SIZE,
                     "{\"ok\":true,\"first_offset\":%" PRIu64 ",\"count\":%zu,"
                     "\"partition\":%d,\"batch_id\":%" PRIu64 ",\"sealed\":true,"
                     "\"durable\":%s,\"mode\":\"%s\",\"synced\":%s}",
                     first_offset, count, partition, batch_id,
                     durable?"true":"false", mode, synced_json);
    res->buffer[(n>0&&n<(int)RESPONSE_BUFFER_SIZE)?n:(RESPONSE_BUFFER_SIZE-1)]=0;
    return 0;
}
int broker_consume(Request *req, Response *res) {
    /* path: /broker/consume/:topic/:group/:partition */
    const char* topic_in      = req->params[0].value;
    const char* group_in      = req->params[1].value;
    const char* partition_str = req->params[2].value;

    if (!topic_in || !*topic_in || !group_in || !*group_in || !partition_str) {
        send_error(res, &(error_response_t){
                .error="missing_params",
                .message="Topic, group, and partition are required: /broker/consume/{topic}/{group}/{partition}",
                .docs="https://ayder.dev/docs/api/consume"
        });
        return -1;
    }

    char topic_buf[RF_MAX_TOPIC_LEN], group_buf[RF_MAX_GROUP_LEN];
    strncpy(topic_buf, topic_in, sizeof(topic_buf)-1); topic_buf[sizeof(topic_buf)-1]=0;
    strncpy(group_buf, group_in, sizeof(group_buf)-1); group_buf[sizeof(group_buf)-1]=0;

    int partition=0; const char* scan=partition_str;
    while (*scan>='0' && *scan<='9'){ partition = partition*10 + (*scan - '0'); scan++; }

    uint64_t from_exclusive = (uint64_t)-1;
    size_t   max_msgs = 100;
    int      has_offset = 0;

    const char *off_q=NULL, *lim_q=NULL, *enc_q=NULL;
    size_t off_len=0, lim_len=0, enc_len=0;

    qs_str(req->query_string, "offset",   &off_q, &off_len);
    qs_str(req->query_string, "limit",    &lim_q, &lim_len);
    qs_str(req->query_string, "encoding", &enc_q, &enc_len);

    if (off_q && off_len) {
        char buf[32];
        size_t n = (off_len < sizeof(buf)-1) ? off_len : sizeof(buf)-1;
        memcpy(buf, off_q, n); buf[n]=0;
        long long s = strtoll(buf, NULL, 10);
        from_exclusive = (s <= 0) ? (uint64_t)-1 : (uint64_t)(s - 1);
        has_offset = 1;
    }

    if (lim_q && lim_len) {
        char buf[32];
        size_t n = (lim_len < sizeof(buf)-1) ? lim_len : sizeof(buf)-1;
        memcpy(buf, lim_q, n); buf[n]=0;
        unsigned long L = strtoul(buf, NULL, 10);
        if (L > 0 && L <= 1000) max_msgs = (size_t)L;
    }

    int want_b64 = (enc_q && enc_len==3 && !strncmp(enc_q,"b64",3)) ? 1 : 0;

    uint64_t committed = 0;
    int have_committed = rf_offset_load(topic_buf, group_buf, partition, &committed);
    if (!has_offset) from_exclusive = have_committed ? committed : (uint64_t)-1;

    rf_msg_view_t stack_msgs[100];
    rf_msg_view_t *msgs = (max_msgs<=100) ? stack_msgs : slab_alloc(max_msgs * sizeof(*msgs));
    if (!msgs) {
        send_error(res, &(error_response_t){
                .error="out_of_memory",
                .message="Server temporarily out of memory. Reduce limit or retry.",
                .docs=NULL
        });
        return -1;
    }

    uint64_t next_offset=0;
    size_t consumed = rf_consume(topic_buf, group_buf, partition,
                                 from_exclusive, max_msgs,
                                 msgs, max_msgs, &next_offset);

    respw_t w = rw_init(res);

    rw_str(&w, "{\"messages\":[");
    for (size_t i=0; i<consumed; i++) {
        if (i) rw_putc(&w, ',');
        rw_printf(&w, "{\"offset\":%" PRIu64 ",\"partition\":%d", msgs[i].offset, msgs[i].partition);

        if (want_b64) {
            /* quick worst-case size guard */
            size_t vlen = b64_len(msgs[i].val_len);
            size_t klen = msgs[i].key_len ? b64_len(msgs[i].key_len) : 0;
            size_t overhead = 64 + vlen + (msgs[i].key_len ? (32 + klen) : 0);

            if ((size_t)(w.end - w.p) < overhead) { w.truncated = 1; break; }

            rw_str(&w, ",\"value_b64\":\"");
            w.p += b64_encode((const uint8_t*)msgs[i].val, msgs[i].val_len, w.p);
            rw_putc(&w, '"');

            if (msgs[i].key_len) {
                rw_str(&w, ",\"key_b64\":\"");
                w.p += b64_encode((const uint8_t*)msgs[i].key, msgs[i].key_len, w.p);
                rw_putc(&w, '"');
            }
            rw_putc(&w, '}');
        } else {
            char kbuf[256], vbuf[1024];
            size_t kraw = (msgs[i].key_len < sizeof(kbuf)-1) ? msgs[i].key_len : sizeof(kbuf)-1;
            size_t vraw = (msgs[i].val_len < sizeof(vbuf)-1) ? msgs[i].val_len : sizeof(vbuf)-1;
            size_t ke = json_escape_into(kbuf, sizeof(kbuf)-1, (const uint8_t*)msgs[i].key, kraw);
            size_t ve = json_escape_into(vbuf, sizeof(vbuf)-1, (const uint8_t*)msgs[i].val, vraw);
            kbuf[ke]=0; vbuf[ve]=0;
            rw_printf(&w, ",\"key\":\"%s\",\"value\":\"%s\"}", kbuf, vbuf);
        }
    }

    rw_printf(&w, "],\"count\":%zu,\"next_offset\":%" PRIu64 ",\"committed_offset\":",
              consumed, next_offset);
    if (have_committed) rw_printf(&w, "%" PRIu64, committed);
    else rw_str(&w, "null");
    rw_printf(&w, ",\"truncated\":%s}", w.truncated ? "true":"false");

    if (msgs!=stack_msgs) slab_free(msgs);
    return 0;
}
// POST /broker/commit → commit consumer offset
int broker_commit_fast(Request *req, Response *res) {
    json_value_t* root = json_parse(req->body, req->body_len);
    if (!root || root->type != JSON_OBJECT) {
        send_error(res, &(error_response_t){
                .error = "invalid_json",
                .message = "Request body must be valid JSON object",
                .docs = "https://ayder.dev/docs/api/commit"
        });
        if (root) json_free(root);
        return -1;
    }

    json_value_t* topic_field = json_get_field(root, "topic");
    json_value_t* group_field = json_get_field(root, "group");
    json_value_t* partition_field = json_get_field(root, "partition");
    json_value_t* offset_field = json_get_field(root, "offset");

    if (!topic_field || !group_field || !partition_field || !offset_field ||
        topic_field->type != JSON_STRING ||
        group_field->type != JSON_STRING ||
        partition_field->type != JSON_INT ||
        offset_field->type != JSON_INT) {
        send_error(res, &(error_response_t){
                .error = "missing_fields",
                .message = "Required fields: topic (string), group (string), partition (int), offset (int)",
                .docs = "https://ayder.dev/docs/api/commit#request-format"
        });
        json_free(root);
        return -1;
    }

    // Extract fields
    char topic_buf[RF_MAX_TOPIC_LEN];
    char group_buf[RF_MAX_GROUP_LEN];

    size_t topic_len = topic_field->as.s.len;
    if (topic_len >= sizeof(topic_buf)) topic_len = sizeof(topic_buf) - 1;
    memcpy(topic_buf, topic_field->as.s.ptr, topic_len);
    topic_buf[topic_len] = '\0';

    size_t group_len = group_field->as.s.len;
    if (group_len >= sizeof(group_buf)) group_len = sizeof(group_buf) - 1;
    memcpy(group_buf, group_field->as.s.ptr, group_len);
    group_buf[group_len] = '\0';

    int partition = (int)partition_field->as.i;
    uint64_t offset = (uint64_t)offset_field->as.i;

    // Commit offset using lock-free broker
    int result = rf_commit(topic_buf, group_buf, partition, offset);

    if (result < 0) {
        send_error(res, &(error_response_t){
                .error = "commit_failed",
                .message = "Failed to commit offset. Partition may not exist or storage is full.",
                .docs = "https://ayder.dev/docs/api/commit#troubleshooting"
        });
        json_free(root);
        return -1;
    }

    // Success response
    static const char success[] = "{\"ok\":true}";
    memcpy(res->buffer, success, sizeof(success) - 1);
    res->buffer[sizeof(success) - 1] = '\0';

    json_free(root);
    return 0;
}

// POST /broker/topics → create topic (sub-100μs)
int broker_create_topic_fast(Request *req, Response *res) {
    json_value_t* root = json_parse(req->body, req->body_len);
    if (!root || root->type != JSON_OBJECT) {
        const char* error = "{\"error\":\"Invalid JSON\"}";
        size_t error_len = strlen(error);
        memcpy(res->buffer, error, error_len);
        res->buffer[error_len] = '\0';
        if (root) json_free(root);
        return -1;
    }

    json_value_t* name_field = json_get_field(root, "name");
    json_value_t* partitions_field = json_get_field(root, "partitions");

    if (!name_field || name_field->type != JSON_STRING) {
        const char* error = "{\"error\":\"Missing topic name\"}";
        size_t error_len = strlen(error);
        memcpy(res->buffer, error, error_len);
        res->buffer[error_len] = '\0';
        json_free(root);
        return -1;
    }

    char topic_buf[RF_MAX_TOPIC_LEN];
    size_t topic_len = name_field->as.s.len;
    if (topic_len >= sizeof(topic_buf)) topic_len = sizeof(topic_buf) - 1;
    memcpy(topic_buf, name_field->as.s.ptr, topic_len);
    topic_buf[topic_len] = '\0';

    int partitions = 8; // Default
    if (partitions_field && partitions_field->type == JSON_INT) {
        partitions = (int)partitions_field->as.i;
        if (partitions < 1 || partitions > 256) partitions = 8;
    }

    int result = rf_topic_ensure(topic_buf, partitions);
    if (result < 0) {
        const char* error;
        switch (result) {
            case -2: error = "{\"error\":\"Too many topics\"}"; break;
            case -3: error = "{\"error\":\"Memory allocation failed\"}"; break;
            case -4: error = "{\"error\":\"Partition init failed\"}"; break;
            default: error = "{\"error\":\"Topic creation failed\"}"; break;
        }
        size_t error_len = strlen(error);
        memcpy(res->buffer, error, error_len);
        res->buffer[error_len] = '\0';
        json_free(root);
        return -1;
    }

    int n = snprintf(res->buffer, RESPONSE_BUFFER_SIZE,
                     "{\"ok\":true,\"topic\":\"%s\",\"partitions\":%d}",
                     topic_buf, partitions);
    res->buffer[(n>0 && n<(int)RESPONSE_BUFFER_SIZE)?n:(RESPONSE_BUFFER_SIZE-1)] = '\0';

    json_free(root);
    return 0;
}

int sealed_status_handler(Request *req, Response *res) {
    (void)req;
    uint64_t last = AOF_sealed_last_synced_id();
    int n = snprintf(res->buffer, RESPONSE_BUFFER_SIZE,
                     "{\"ok\":true,\"last_synced_batch_id\":%" PRIu64 "}", last);
    res->buffer[(n>0 && n<(int)RESPONSE_BUFFER_SIZE)?n:(RESPONSE_BUFFER_SIZE-1)] = '\0';
    return 0;
}

int sealed_test_release_handler(Request *req, Response *res) {
    (void)req;
    AOF_sealed_test_release();
    const char ok[] = "{\"ok\":true,\"released\":1}";
    memcpy(res->buffer, ok, sizeof(ok)-1);
    return 0;
}

int ready_handler(Request *req, Response *res) {
    (void)req;
    if (Persistence_is_ready()) {
        const char out[] = "{\"ready\":true}";
        memcpy(res->buffer, out, sizeof(out)-1);
    } else {
        const char out[] = "{\"ready\":false}";
        memcpy(res->buffer, out, sizeof(out)-1);
    }
    return 0;
}
int sealed_gc_handler(Request *req, Response *res) {
    json_value_t* root = json_parse(req->body, req->body_len);
    if (!root || root->type != JSON_OBJECT) { memcpy(res->buffer, "{\"error\":\"Invalid JSON\"}", 24); res->buffer[24]=0; if(root)json_free(root); return -1; }

    json_value_t* max_age_ms = json_get_field(root, "max_age_ms");
    json_value_t* max_bytes  = json_get_field(root, "max_bytes");
    uint64_t age = (max_age_ms && max_age_ms->type == JSON_INT) ? (uint64_t)max_age_ms->as.i : 0;
    uint64_t bytes = (max_bytes  && max_bytes->type  == JSON_INT) ? (uint64_t)max_bytes->as.i  : 0;

    uint64_t after = 0;
    int rc = AOF_sealed_gc(age, bytes, &after);
    if (rc < 0) { memcpy(res->buffer, "{\"error\":\"sealed_gc_failed\"}", 28); res->buffer[28]=0; json_free(root); return -1; }
    int m = snprintf(res->buffer, RESPONSE_BUFFER_SIZE, "{\"ok\":true,\"sealed_bytes_after\":%" PRIu64 "}", after);
    res->buffer[(m>0 && m<(int)RESPONSE_BUFFER_SIZE)?m:(RESPONSE_BUFFER_SIZE-1)] = '\0';
    json_free(root); return 0;
}


int broker_set_retention_fast(Request *req, Response *res) {
    json_value_t* root = json_parse(req->body, req->body_len);
    if (!root || root->type != JSON_OBJECT) {
        const char* e = "{\"error\":\"Invalid JSON\"}";
        memcpy(res->buffer, e, strlen(e)); res->buffer[strlen(e)] = 0;
        if (root) json_free(root);
        return -1;
    }

    json_value_t *topic_f  = json_get_field(root, "topic");       // string or "*", optional
    json_value_t *part_f   = json_get_field(root, "partition");   // int, optional
    json_value_t *ttl_f    = json_get_field(root, "ttl_ms");      // int, optional
    json_value_t *maxb_f   = json_get_field(root, "max_bytes");
    json_value_t *sealed_f = json_get_field(root, "sealed");      // object {max_age_ms,max_bytes}, optional

    uint64_t sealed_after = 0;
    int ttl_applied = 0;
    int size_applied = 0;
    int have_any_retention = 0;

    /* Build a single record we’ll persist if either TTL or size was provided */
    aof_ttl_rec_t rec = (aof_ttl_rec_t) {0};

    /* ----- TTL handling (apply locally) ----- */
    if (ttl_f && ttl_f->type == JSON_INT) {
        have_any_retention = 1;
        uint64_t ttl_ms = (uint64_t) ttl_f->as.i;
        rec.ttl_ms = ttl_ms;

        int have_topic = (topic_f && topic_f->type == JSON_STRING && topic_f->as.s.len > 0);
        int part = -1;
        if (part_f && part_f->type == JSON_INT) part = (int) part_f->as.i;

        if (!have_topic || (topic_f->as.s.len == 1 && topic_f->as.s.ptr[0] == '*')) {
            rec.partition = -1;
            rec.topic[0] = '*';
            rec.topic[1] = 0;
            ttl_applied = (rf_set_ttl_ms_all(ttl_ms) == 0);
        } else {
            size_t n = topic_f->as.s.len;
            if (n >= sizeof(rec.topic)) n = sizeof(rec.topic) - 1;
            memcpy(rec.topic, topic_f->as.s.ptr, n);
            rec.topic[n] = 0;
            rec.partition = (int32_t) part;
            ttl_applied = (rf_set_ttl_ms(rec.topic, part, ttl_ms) == 0);
        }
    }

    /* ----- SIZE handling (apply locally) ----- */
    if (maxb_f && maxb_f->type == JSON_INT) {
        have_any_retention = 1;
        uint64_t maxb = (uint64_t) maxb_f->as.i;
        rec.max_bytes = maxb;

        int have_topic = (topic_f && topic_f->type == JSON_STRING && topic_f->as.s.len > 0);
        int part = -1;
        if (part_f && part_f->type == JSON_INT) part = (int) part_f->as.i;

        if (!have_topic || (topic_f->as.s.len == 1 && topic_f->as.s.ptr[0] == '*')) {
            if (rec.topic[0] == 0) {
                rec.topic[0] = '*';
                rec.topic[1] = 0;
                rec.partition = -1;
            }
            size_applied = (rf_set_max_bytes_all(maxb) == 0);
        } else {
            if (rec.topic[0] == 0) {
                size_t n = topic_f->as.s.len;
                if (n >= sizeof(rec.topic)) n = sizeof(rec.topic) - 1;
                memcpy(rec.topic, topic_f->as.s.ptr, n);
                rec.topic[n] = 0;
                rec.partition = (int32_t) part;
            }
            size_applied = (rf_set_max_bytes(rec.topic, part, maxb) == 0);
        }
    }

    /* Persist the combined retention config if anything was set */
    if (have_any_retention) {
        AOF_append_sealed(AOF_REC_BROKER_TTL, &rec, sizeof(rec));
    }

    if (RAMForge_HA_is_enabled()) {
        RAMForge_HA_replicate_write(AOF_REC_BROKER_TTL, &rec, sizeof(rec));
    }

    /* ----- sealed file GC (one-shot) ----- */
    if (sealed_f && sealed_f->type == JSON_OBJECT) {
        json_value_t *age_f   = json_get_field(sealed_f, "max_age_ms");
        json_value_t *bytes_f = json_get_field(sealed_f, "max_bytes");
        uint64_t age   = (age_f   && age_f->type   == JSON_INT) ? (uint64_t)age_f->as.i   : 0;
        uint64_t bytes = (bytes_f && bytes_f->type == JSON_INT) ? (uint64_t)bytes_f->as.i : 0;
        AOF_sealed_gc(age, bytes, &sealed_after);
    }

    int n = snprintf(res->buffer, RESPONSE_BUFFER_SIZE,
                     "{\"ok\":true,\"ttl_applied\":%s,\"size_applied\":%s,"
                     "\"sealed_bytes_after\":%" PRIu64 "}",
                     ttl_applied ? "true" : "false",
                     size_applied ? "true" : "false",
                     sealed_after);
    res->buffer[(n>0 && n<(int)RESPONSE_BUFFER_SIZE)?n:(RESPONSE_BUFFER_SIZE-1)] = '\0';

    json_free(root);
    return 0;
}


int broker_delete_before_offset(Request *req, Response *res) {
    json_value_t* root = json_parse(req->body, req->body_len);
    if (!root || root->type != JSON_OBJECT) {
        const char* error = "{\"error\":\"Invalid JSON\"}";
        memcpy(res->buffer, error, strlen(error));
        res->buffer[strlen(error)] = '\0';
        if (root) json_free(root);
        return -1;
    }

    json_value_t* topic_field         = json_get_field(root, "topic");
    json_value_t* partition_field     = json_get_field(root, "partition");
    json_value_t* before_offset_field = json_get_field(root, "before_offset");

    if (!topic_field || !partition_field || !before_offset_field ||
        topic_field->type != JSON_STRING ||
        partition_field->type != JSON_INT ||
        before_offset_field->type != JSON_INT) {
        const char* error = "{\"error\":\"Missing required fields\"}";
        memcpy(res->buffer, error, strlen(error));
        res->buffer[strlen(error)] = '\0';
        json_free(root);
        return -1;
    }

    char topic_buf[RF_MAX_TOPIC_LEN];
    size_t topic_len = topic_field->as.s.len;
    if (topic_len >= sizeof(topic_buf)) topic_len = sizeof(topic_buf) - 1;
    memcpy(topic_buf, topic_field->as.s.ptr, topic_len);
    topic_buf[topic_len] = '\0';

    int partition = (int)partition_field->as.i;
    uint64_t before = (uint64_t)before_offset_field->as.i;

    uint64_t deleted = 0, freed = 0;
    int rc = rf_delete_before(topic_buf, partition, before, &deleted, &freed);
    if (rc < 0) {
        const char* error = (rc == -2)
                            ? "{\"error\":\"Topic or partition not found\"}"
                            : "{\"error\":\"Delete-before failed\"}";
        memcpy(res->buffer, error, strlen(error));
        res->buffer[strlen(error)] = '\0';
        json_free(root);
        return -1;
    }

    int n = snprintf(res->buffer, RESPONSE_BUFFER_SIZE,
                     "{\"ok\":true,\"deleted_count\":%" PRIu64 ",\"freed_bytes\":%" PRIu64 "}",
                     deleted, freed);
    res->buffer[(n>0 && n<(int)RESPONSE_BUFFER_SIZE)?n:(RESPONSE_BUFFER_SIZE-1)] = '\0';

    json_free(root);
    return 0;
}

/* ---------- GET /kv/:ns/:key+ ---------- */
int kv_get_handler(Request *req, Response *res){
    const char *ns=NULL, *key=NULL;
    for (int i=0;i<req->param_count;i++){
        if (!ns  && strcmp(req->params[i].name,"ns")==0)  ns  = req->params[i].value;
        if (!key && strcmp(req->params[i].name,"key")==0) key = req->params[i].value;
    }
    if (!ns || !key) {
        send_error(res, &(error_response_t){
                .error="missing_params",
                .message="Namespace and key are required: /kv/{namespace}/{key}",
                .docs="https://ayder.dev/docs/api/kv#get"
        });
        return 0;
    }

    int skey = kv_int_key(ns,key);

    uint8_t buf[MAX_DATA_SIZE];
    if (!shared_storage_get_fast(g_shared_storage, skey, buf, sizeof(buf))) {
        send_error(res, &(error_response_t){ .error="not_found", .message="Key does not exist in this namespace", .docs=NULL });
        return 0;
    }

    kvrec_t *r = (kvrec_t*)buf;
    if (r->flags & KV_FLAG_TOMBSTONE) {
        send_error(res, &(error_response_t){ .error="not_found", .message="Key was deleted (tombstone exists)", .docs=NULL });
        return 0;
    }
    if (r->expires_us && r->expires_us <= rf_wall_us()) {
        send_error(res, &(error_response_t){ .error="not_found", .message="Key expired", .docs=NULL });
        return 0;
    }

    respw_t w = rw_init(res);

    size_t v64 = b64_len(r->value_len);
    /* {"value":"<b64>","cas":184467...} */
    size_t overhead = 32 + v64 + 32;
    if ((size_t)(w.end - w.p) < overhead) {
        send_error(res, &(error_response_t){
                .error="response_too_large",
                .message="Value is too large to return in a single response",
                .docs="https://ayder.dev/docs/api/kv#limits"
        });
        return 0;
    }

    rw_str(&w, "{\"value\":\"");
    w.p += b64_encode((const uint8_t*)r->value, r->value_len, w.p);
    rw_printf(&w, "\",\"cas\":%" PRIu64 "}", r->cas);
    return 0;
}

/* ---------- GET /kv/:ns/:key+/meta ---------- */
int kv_meta_handler(Request *req, Response *res){
    const char *ns=NULL, *key=NULL;
    for (int i=0;i<req->param_count;i++){
        if (!ns && strcmp(req->params[i].name,"ns")==0)  ns  = req->params[i].value;
        if (!key&& strcmp(req->params[i].name,"key")==0) key = req->params[i].value;
    }
    if (!ns || !key) { strcpy(res->buffer, "{\"error\":\"bad_params\"}"); return 0; }

    int skey = kv_int_key(ns,key);
    uint8_t buf[MAX_DATA_SIZE];
    if (!shared_storage_get_fast(g_shared_storage, skey, buf, sizeof(buf))) {
        strcpy(res->buffer, "{\"error\":\"not_found\"}");
        return 0;
    }
    kvrec_t *r = (kvrec_t*)buf;
    if (r->flags & KV_FLAG_TOMBSTONE) { strcpy(res->buffer, "{\"error\":\"not_found\"}"); return 0; }
    uint64_t now = wall_us();
    if (r->expires_us && r->expires_us <= now) { strcpy(res->buffer, "{\"error\":\"not_found\",\"expired\":true}"); return 0; }
    uint64_t ttl_ms = (r->expires_us && r->expires_us > now) ? (r->expires_us - now)/1000ULL : 0;
    int n = snprintf(res->buffer, RESPONSE_BUFFER_SIZE,
                     "{\"cas\":%" PRIu64 ",\"ttl_ms\":%" PRIu64 "}", r->cas, ttl_ms);
    (void)n;
    return 0;
}

/* ---------- POST /kv/:ns/:key+ (CAS/TTL) ---------- */
int kv_put_handler(Request *req, Response *res){
    const char *ns=NULL, *key=NULL;
    for (int i=0;i<req->param_count;i++){
        if (!ns && strcmp(req->params[i].name,"ns")==0)  ns  = req->params[i].value;
        if (!key&& strcmp(req->params[i].name,"key")==0) key = req->params[i].value;
    }
    if (!ns || !key) { strcpy(res->buffer, "{\"ok\":false,\"error\":\"bad_params\"}"); return 0; }

    // query: cas, ttl_ms
    uint64_t want_cas=UINT64_MAX, ttl_ms=0;
    if (req->query_string) {
        uint64_t tmp;
        if (qs_u64(req->query_string,"cas",&tmp))     want_cas = tmp;
        if (qs_u64(req->query_string,"ttl_ms",&tmp))  ttl_ms   = tmp;
    }

    int skey = kv_int_key(ns,key);
    uint8_t curbuf[MAX_DATA_SIZE];
    uint64_t cur_cas = 0;
    int have = shared_storage_get_fast(g_shared_storage, skey, curbuf, sizeof(curbuf));
    if (have) {
        kvrec_t *r = (kvrec_t*)curbuf;
        cur_cas = r->cas;
    }

    // CAS check (only if provided)
    if (want_cas != UINT64_MAX && want_cas != cur_cas) {
        int n = snprintf(res->buffer, RESPONSE_BUFFER_SIZE,
                         "{\"ok\":false,\"error\":\"cas_mismatch\","
                         "\"message\":\"CAS value %llu does not match current %llu. Key was modified by another client.\","
                         "\"expected\":%llu,\"current\":%llu,"
                         "\"docs\":\"https://ayder.dev/docs/api/kv#cas\"}",
                         (unsigned long long)want_cas, (unsigned long long)cur_cas,
                         (unsigned long long)want_cas, (unsigned long long)cur_cas
        );
        res->buffer[(n>0 && n<(int)RESPONSE_BUFFER_SIZE)?n:(RESPONSE_BUFFER_SIZE-1)] = '\0';
        return 0;
    }

    // Build new record (decide CAS now so all workers see the same CAS)
    size_t need = sizeof(kvrec_t) + req->body_len;
    if (need > MAX_DATA_SIZE) {
        int max_payload = (int)(MAX_DATA_SIZE - sizeof(kvrec_t));
        int n = snprintf(res->buffer, RESPONSE_BUFFER_SIZE,
                         "{\"ok\":false,\"error\":\"payload_too_large\","
                         "\"message\":\"Value size %zu bytes exceeds maximum %d bytes\","
                         "\"max_bytes\":%d,\"got_bytes\":%zu,"
                         "\"docs\":\"https://ayder.dev/docs/api/kv#limits\"}",
                         req->body_len, max_payload, max_payload, req->body_len
        );
        res->buffer[(n>0 && n<(int)RESPONSE_BUFFER_SIZE)?n:(RESPONSE_BUFFER_SIZE-1)] = '\0';
        return 0;
    }
    uint8_t *buf = (uint8_t*)alloca(need);
    kvrec_t *nw = (kvrec_t*)buf;
    nw->cas = cur_cas + 1;
    nw->flags = 0;
    nw->value_len = (uint32_t)req->body_len;
    nw->expires_us = ttl_ms ? (wall_us() + ttl_ms*1000ULL) : 0;
    if (req->body_len) memcpy(nw->value, req->body, req->body_len);

    if (shared_storage_set(g_shared_storage, skey, buf, need) != 0) {
        strcpy(res->buffer, "{\"ok\":false,\"error\":\"storage_full\"}");
        return 0;
    }

    /* --- sealed/rocket append --- */
    uint64_t batch_id = 0;
    {
        aof_kv_put_t hdr = {
                .skey       = skey,
                .cas        = nw->cas,
                .expires_us = nw->expires_us,
                .flags      = nw->flags,
                .value_len  = nw->value_len
        };
        size_t payload_sz = sizeof(hdr) + nw->value_len;
        void *payload = alloca(payload_sz);
        memcpy(payload, &hdr, sizeof(hdr));
        if (nw->value_len) memcpy((uint8_t *) payload + sizeof(hdr), nw->value, nw->value_len);

        const char *rocket_env = getenv("RF_ROCKET");
        const int rocket_on = (rocket_env && rocket_env[0] != '\0' && rocket_env[0] != '0');
        if (rocket_on) {
            /* broadcast to all workers, non-durable */
            (void) rf_bus_publish(AOF_REC_KV_PUT, payload, (uint32_t) payload_sz);
            batch_id = 0;
        } else {
            batch_id = AOF_append_sealed(AOF_REC_KV_PUT, payload, payload_sz);
        }

        if (RAMForge_HA_is_enabled()) {
            RAMForge_HA_replicate_write(AOF_REC_KV_PUT, payload, payload_sz);
        }
    }

    /* optional fsync wait */
    int timeout_ms = 0;
    uint64_t tmp;
    if (qs_u64(req->query_string, "timeout_ms", &tmp)) timeout_ms = (int) tmp;
    int sync_state = wait_synced_until(batch_id, timeout_ms);
    const char *mode = (batch_id == 0) ? "rocket" : "sealed";
    int durable = (batch_id != 0);
    const char *synced_json = (sync_state < 0) ? "null" : (sync_state ? "true" : "false");

    int n = snprintf(res->buffer, RESPONSE_BUFFER_SIZE,
                     "{\"ok\":true,\"cas\":%" PRIu64 ","
                     "\"sealed\":true,\"durable\":%s,"
                     "\"mode\":\"%s\",\"synced\":%s,"
                     "\"batch_id\":%" PRIu64 "}",
                     nw->cas, durable ? "true" : "false", mode, synced_json, batch_id);
    (void) n;
    return 0;
}

/* ---------- DELETE /kv/:ns/:key+ (CAS) ---------- */
int kv_delete_handler(Request *req, Response *res){
    const char *ns=NULL, *key=NULL;
    for (int i=0;i<req->param_count;i++){
        if (!ns && strcmp(req->params[i].name,"ns")==0)  ns  = req->params[i].value;
        if (!key&& strcmp(req->params[i].name,"key")==0) key = req->params[i].value;
    }
    if (!ns || !key) { strcpy(res->buffer, "{\"ok\":false,\"error\":\"bad_params\"}"); return 0; }

    uint64_t want_cas=UINT64_MAX;
    if (req->query_string) qs_u64(req->query_string,"cas",&want_cas);

    int skey = kv_int_key(ns,key);
    uint8_t curbuf[MAX_DATA_SIZE];
    if (!shared_storage_get_fast(g_shared_storage, skey, curbuf, sizeof(curbuf))) {
        strcpy(res->buffer, "{\"ok\":true,\"deleted\":false}");
        return 0;
    }
    kvrec_t *r = (kvrec_t*)curbuf;
    if (want_cas != UINT64_MAX && want_cas != r->cas) {
        int n = snprintf(res->buffer, RESPONSE_BUFFER_SIZE,
                         "{\"ok\":false,\"error\":\"cas_mismatch\",\"current\":%" PRIu64 "}", r->cas);
        (void)n; return 0;
    }

    // tombstone record (keeps CAS history; safe in shared table that lacks delete)
    uint8_t buf[sizeof(kvrec_t)];
    kvrec_t *del = (kvrec_t*)buf;
    del->cas = r->cas + 1;
    del->flags = KV_FLAG_TOMBSTONE;
    del->value_len = 0;
    del->expires_us = 0;

    shared_storage_set(g_shared_storage, skey, buf, sizeof(buf));

    /* sealed/rocket append for delete */
    uint64_t batch_id = 0;
    {
        aof_kv_del_t d = {.skey = skey, .cas = del->cas, .flags = del->flags};
        const char *rocket_env = getenv("RF_ROCKET");
        const int rocket_on = (rocket_env && rocket_env[0] != '\0' && rocket_env[0] != '0');
        if (rocket_on) {
            (void) rf_bus_publish(AOF_REC_KV_DEL, &d, (uint32_t) sizeof(d));
            batch_id = 0;
        } else {
            batch_id = AOF_append_sealed(AOF_REC_KV_DEL, &d, sizeof(d));
        }

        if (RAMForge_HA_is_enabled()) {
            RAMForge_HA_replicate_write(AOF_REC_KV_DEL, &d, sizeof(d));
        }


    }
    int timeout_ms = 0;
    uint64_t tmp;
    if (qs_u64(req->query_string, "timeout_ms", &tmp)) timeout_ms = (int) tmp;
    int sync_state = wait_synced_until(batch_id, timeout_ms);
    const char *mode = (batch_id == 0) ? "rocket" : "sealed";
    int durable = (batch_id != 0);
    const char *synced_json = (sync_state < 0) ? "null" : (sync_state ? "true" : "false");

    int n = snprintf(res->buffer, RESPONSE_BUFFER_SIZE,
                     "{\"ok\":true,\"deleted\":true,"
                     "\"sealed\":true,\"durable\":%s,"
                     "\"mode\":\"%s\",\"synced\":%s,"
                     "\"batch_id\":%" PRIu64 "}",
                     durable ? "true" : "false", mode, synced_json, batch_id);
    res->buffer[(n > 0 && n < (int) RESPONSE_BUFFER_SIZE) ? n : (RESPONSE_BUFFER_SIZE - 1)] = 0;
    return 0;
}

// Route Registration
void register_application_routes(App *app, int control_plane) {
    (void)control_plane;
    // Set global app reference for route handlers
    g_app = app;
    // System routes
    app->get(app, "/health", health_fast);
    app->post(app, "/admin/compact", compact_handler_fast);
    app->get(app, "/metrics", prometheus_metrics_handler);
    app->get(app, "/metrics_ha", ha_metrics_handler);
    app->get(app, "/admin/sealed/status", sealed_status_handler);
    app->post(app, "/admin/sealed/release", sealed_test_release_handler);
    // Consumer group management
    app->post(app, "/broker/commit", broker_commit_fast);

    // KV (generic, raw-in)
    app->post  (app, "/kv/:ns/:key+",       kv_put_handler);
    app->get   (app, "/kv/:ns/:key+/meta",  kv_meta_handler);
    app->get   (app, "/kv/:ns/:key+",       kv_get_handler);
    app->delete(app, "/kv/:ns/:key+",       kv_delete_handler);
    // Topic management
    app->post(app, "/broker/topics", broker_create_topic_fast);

    // Operational endpoints
    app->post(app, "/broker/delete-before", broker_delete_before_offset);
    app->get(app, "/ready", ready_handler);
    app->post(app, "/broker/retention", broker_set_retention_fast);
    app->post(app, "/admin/sealed/gc",     sealed_gc_handler);


    app->post(app, "/broker/topics/:topic/produce",          broker_produce_raw);
    app->post(app, "/broker/topics/:topic/produce-ndjson",   broker_produce_ndjson);
    app->get (app, "/broker/consume/:topic/:group/:partition", broker_consume);
    app->post(app, "/broker/query", broker_query_handler);
    app->post(app, "/broker/join", broker_join_handler);

}
