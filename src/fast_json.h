// fast_json.h - Ultra-fast JSON SerDe with zero-copy and SIMD optimizations
#ifndef FAST_JSON_H
#define FAST_JSON_H

#include <stdint.h>
#include <string.h>
#include <limits.h>
#include <stdio.h>

// Check for SIMD support and include appropriate headers
#ifdef __AVX2__
#include <immintrin.h>
    #define SIMD_SUPPORTED 1
#elif defined(__SSE2__)
#include <emmintrin.h>
    #define SIMD_SUPPORTED 1
#else
#define SIMD_SUPPORTED 0
#endif

#include "slab_alloc.h"

// Add missing function declarations for C99 compatibility
#ifndef __has_builtin
#define __has_builtin(x) 0
#endif

#if !__has_builtin(__builtin_ctz) && !defined(__GNUC__)
// Fallback implementation of count trailing zeros
static inline int builtin_ctz_fallback(uint32_t x) {
    if (x == 0) return 32;
    int n = 0;
    if ((x & 0x0000FFFF) == 0) { n += 16; x >>= 16; }
    if ((x & 0x000000FF) == 0) { n += 8;  x >>= 8;  }
    if ((x & 0x0000000F) == 0) { n += 4;  x >>= 4;  }
    if ((x & 0x00000003) == 0) { n += 2;  x >>= 2;  }
    if ((x & 0x00000001) == 0) { n += 1;  }
    return n;
}
#define __builtin_ctz(x) builtin_ctz_fallback(x)
#endif

// ═══════════════════════════════════════════════════════════════════════════════
// SIMD-Optimized String Operations
// ═══════════════════════════════════════════════════════════════════════════════

// Find character using AVX2 (32 bytes at a time) if supported
static inline const char* simd_find_char(const char* str, char target, size_t len) {
    if (len < 32 || !SIMD_SUPPORTED) {
        // Fallback for small strings or no SIMD support
        for (size_t i = 0; i < len; i++) {
            if (str[i] == target) return &str[i];
        }
        return NULL;
    }

#ifdef __AVX2__
    __m256i target_vec = _mm256_set1_epi8(target);
    const char* ptr = str;
    const char* end = str + (len & ~31);  // align to 32-byte boundary

    while (ptr < end) {
        __m256i chunk = _mm256_loadu_si256((const __m256i*)ptr);
        __m256i cmp_result = _mm256_cmpeq_epi8(chunk, target_vec);
        uint32_t mask = (uint32_t)_mm256_movemask_epi8(cmp_result);

        if (mask) {
            return ptr + __builtin_ctz(mask);
        }
        ptr += 32;
    }

    // Handle remaining bytes
    for (const char* rem = ptr; rem < str + len; rem++) {
        if (*rem == target) return rem;
    }
#endif
    return NULL;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Zero-Copy String Views
// ═══════════════════════════════════════════════════════════════════════════════

typedef struct {
    const char* ptr;
    size_t      len;
} string_view_t;

#define STRING_VIEW(str) ((string_view_t){str, strlen(str)})
#define STRING_VIEW_LITERAL(str) ((string_view_t){str, sizeof(str)-1})

static inline int sv_equals(string_view_t a, string_view_t b) {
    return a.len == b.len && memcmp(a.ptr, b.ptr, a.len) == 0;
}

static inline int sv_equals_cstr(string_view_t sv, const char* cstr) {
    size_t clen = strlen(cstr);
    return sv.len == clen && memcmp(sv.ptr, cstr, clen) == 0;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Fast Number Parsing (SWAR - SIMD Within A Register)
// ═══════════════════════════════════════════════════════════════════════════════

// Parse up to 8 digits at once using bit manipulation
static inline uint64_t parse_8digits(const char* str) {
    uint64_t val;
    memcpy(&val, str, 8);

    // Convert ASCII to numeric (subtract '0' from each byte)
    val = (val & 0x0F0F0F0F0F0F0F0FULL) * 2561 >> 8;
    val = (val & 0x00FF00FF00FF00FFULL) * 6553601 >> 16;
    val = (val & 0x0000FFFF0000FFFFULL) * 42949672960001ULL >> 32;

    return val;
}

static inline int fast_parse_int(string_view_t sv, int* result) {
    if (sv.len == 0 || sv.len > 10) return 0;

    const char* ptr = sv.ptr;
    const char* end = ptr + sv.len;
    int sign = 1;

    if (*ptr == '-') {
        sign = -1;
        ptr++;
    } else if (*ptr == '+') {
        ptr++;
    }

    uint64_t acc = 0;

    // Process 8 digits at a time if possible
    while (end - ptr >= 8) {
        // Verify all 8 chars are digits
        uint64_t chunk;
        memcpy(&chunk, ptr, 8);
        uint64_t test = chunk - 0x3030303030303030ULL;
        if (test & 0x8080808080808080ULL) break;  // non-digit found

        acc = acc * 100000000 + parse_8digits(ptr);
        ptr += 8;
    }

    // Handle remaining digits
    while (ptr < end && *ptr >= '0' && *ptr <= '9') {
        acc = acc * 10 + (*ptr++ - '0');
    }

    if (ptr != end) return 0;  // invalid character
    if (acc > (uint64_t)INT_MAX) return 0;  // overflow

    *result = sign * (int)acc;
    return 1;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Lightning-Fast JSON Parser
// ═══════════════════════════════════════════════════════════════════════════════

typedef enum {
    JSON_NULL,
    JSON_BOOL,
    JSON_INT,
    JSON_DOUBLE,
    JSON_STRING,
    JSON_OBJECT,
    JSON_ARRAY
} json_type_t;

typedef struct json_value {
    json_type_t      type;
    union {
        int64_t      i;
        double       d;
        int          b;  // boolean
        string_view_t s;  // string (zero-copy)
        struct {
            struct json_value* items;
            size_t count;
        } array;
        struct {
            struct json_kv* pairs;
            size_t count;
        } object;
    } as;
} json_value_t;

typedef struct json_kv {
    string_view_t key;
    json_value_t  value;
} json_kv_t;

typedef struct {
    const char* input;
    const char* ptr;
    const char* end;
    size_t      error_pos;
} json_parser_t;

// Skip whitespace using SIMD
static inline void skip_whitespace(json_parser_t* p) {
    while (p->ptr < p->end) {
        char c = *p->ptr;
        if (c != ' ' && c != '\t' && c != '\n' && c != '\r') break;
        p->ptr++;
    }
}

// Parse string with zero-copy (returns view into original buffer)
static inline int parse_string(json_parser_t* p, string_view_t* out) {
    if (p->ptr >= p->end || *p->ptr != '"') return 0;
    p->ptr++;  // skip opening quote
    const char *start = p->ptr;
    const char *scan = p->ptr;

    while (scan < p->end) {
        const char *q = simd_find_char(scan, '"', (size_t) (p->end - scan));
        if (!q) {
            p->error_pos = (size_t) (p->ptr - p->input);
            return 0;
        }

        // Count preceding backslashes to determine if this quote is escaped.
        const char *b = q - 1;
        int slashes = 0;
        while (b >= start && *b == '\\') {
            ++slashes;
            --b;
        }
        if ((slashes & 1) == 0) {  // even = unescaped
            out->ptr = start;
            out->len = (size_t) (q - start);
            p->ptr = q + 1;  // skip closing quote
            return 1;
        }
        scan = q + 1; // keep searching
    }
    p->error_pos = (size_t) (p->ptr - p->input);
    return 0;
}

// Forward declaration
static int parse_value(json_parser_t* p, json_value_t* out);

static int parse_array(json_parser_t* p, json_value_t* out) {
    if (p->ptr >= p->end || *p->ptr != '[') return 0;
    p->ptr++;  // skip '['

    skip_whitespace(p);

    // Empty array
    if (p->ptr < p->end && *p->ptr == ']') {
        p->ptr++;
        out->type = JSON_ARRAY;
        out->as.array.items = NULL;
        out->as.array.count = 0;
        return 1;
    }

    // Allocate initial space for array items
    size_t capacity = 8;
    json_value_t* items = slab_alloc(capacity * sizeof(json_value_t));
    size_t count = 0;

    while (1) {
        skip_whitespace(p);

        // Parse value
        json_value_t value;
        if (!parse_value(p, &value)) return 0;

        // Grow array if needed
        if (count >= capacity) {
            capacity *= 2;
            json_value_t* new_items = slab_alloc(capacity * sizeof(json_value_t));
            memcpy(new_items, items, count * sizeof(json_value_t));
            slab_free(items);
            items = new_items;
        }

        items[count] = value;
        count++;

        skip_whitespace(p);
        if (p->ptr >= p->end) {
            p->error_pos = p->ptr - p->input;
            return 0;
        }

        if (*p->ptr == ']') {
            p->ptr++;
            break;
        } else if (*p->ptr == ',') {
            p->ptr++;
            continue;
        } else {
            p->error_pos = p->ptr - p->input;
            return 0;
        }
    }

    out->type = JSON_ARRAY;
    out->as.array.items = items;
    out->as.array.count = count;
    return 1;
}

static int parse_object(json_parser_t* p, json_value_t* out) {
    if (p->ptr >= p->end || *p->ptr != '{') return 0;
    p->ptr++;  // skip '{'

    skip_whitespace(p);

    // Empty object
    if (p->ptr < p->end && *p->ptr == '}') {
        p->ptr++;
        out->type = JSON_OBJECT;
        out->as.object.pairs = NULL;
        out->as.object.count = 0;
        return 1;
    }

    // Allocate initial space for key-value pairs
    size_t capacity = 8;
    json_kv_t* pairs = slab_alloc(capacity * sizeof(json_kv_t));
    size_t count = 0;

    while (1) {
        skip_whitespace(p);

        // Parse key
        string_view_t key;
        if (!parse_string(p, &key)) {
            p->error_pos = p->ptr - p->input;
            return 0;
        }

        skip_whitespace(p);
        if (p->ptr >= p->end || *p->ptr != ':') {
            p->error_pos = p->ptr - p->input;
            return 0;
        }
        p->ptr++;  // skip ':'

        skip_whitespace(p);

        // Parse value
        json_value_t value;
        if (!parse_value(p, &value)) return 0;

        // Grow array if needed
        if (count >= capacity) {
            capacity *= 2;
            json_kv_t* new_pairs = slab_alloc(capacity * sizeof(json_kv_t));
            memcpy(new_pairs, pairs, count * sizeof(json_kv_t));
            slab_free(pairs);
            pairs = new_pairs;
        }

        pairs[count].key = key;
        pairs[count].value = value;
        count++;

        skip_whitespace(p);
        if (p->ptr >= p->end) {
            p->error_pos = p->ptr - p->input;
            return 0;
        }

        if (*p->ptr == '}') {
            p->ptr++;
            break;
        } else if (*p->ptr == ',') {
            p->ptr++;
            continue;
        } else {
            p->error_pos = p->ptr - p->input;
            return 0;
        }
    }

    out->type = JSON_OBJECT;
    out->as.object.pairs = pairs;
    out->as.object.count = count;
    return 1;
}

static int parse_value(json_parser_t* p, json_value_t* out) {
    skip_whitespace(p);
    if (p->ptr >= p->end) return 0;

    char c = *p->ptr;

    if (c == '"') {
        // String
        string_view_t str;
        if (!parse_string(p, &str)) return 0;
        out->type = JSON_STRING;
        out->as.s = str;
        return 1;
    } else if (c == '{') {
        // Object
        return parse_object(p, out);
    } else if (c == '[') {
        // Array
        return parse_array(p, out);
    } else if ((c >= '0' && c <= '9') || c == '-') {
        // Number - find end of number
        const char* start = p->ptr;
        if (c == '-') p->ptr++;

        // Check if it's a floating-point number
        char* dot_pos = strchr(p->ptr, '.');
        while (p->ptr < p->end && (*p->ptr >= '0' && *p->ptr <= '9' || *p->ptr == '.' || *p->ptr == 'e' || *p->ptr == 'E' || *p->ptr == '+' || *p->ptr == '-')) {
            p->ptr++;
        }

        string_view_t num_str = {start, p->ptr - start};
        if (dot_pos) {
            double value;
            if (sscanf(num_str.ptr, "%lf", &value) == 1) {
                out->type = JSON_DOUBLE;
                out->as.d = value;
                return 1;
            }
        } else {
            int value;
            if (fast_parse_int(num_str, &value)) {
                out->type = JSON_INT;
                out->as.i = value;
                return 1;
            }
        }
    } else if (strncmp(p->ptr, "true", 4) == 0) {
        p->ptr += 4;
        out->type = JSON_BOOL;
        out->as.b = 1;
        return 1;
    } else if (strncmp(p->ptr, "false", 5) == 0) {
        p->ptr += 5;
        out->type = JSON_BOOL;
        out->as.b = 0;
        return 1;
    } else if (strncmp(p->ptr, "null", 4) == 0) {
        p->ptr += 4;
        out->type = JSON_NULL;
        return 1;
    }

    p->error_pos = p->ptr - p->input;
    return 0;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Public API - Simple and Fast
// ═══════════════════════════════════════════════════════════════════════════════

static inline json_value_t* json_parse(const char* input, size_t len) {
    json_parser_t parser = {
            .input = input,
            .ptr = input,
            .end = input + len,
            .error_pos = 0
    };

    json_value_t* result = slab_alloc(sizeof(json_value_t));
    if (!parse_value(&parser, result)) {
        slab_free(result);
        return NULL;
    }

    return result;
}

static inline void json_deinit(json_value_t* val) {
    if (!val) return;

    switch (val->type) {
        case JSON_OBJECT:
            for (size_t i = 0; i < val->as.object.count; ++i) {
                json_deinit(&val->as.object.pairs[i].value);   // <- was json_free(...)
            }
            if (val->as.object.pairs) slab_free(val->as.object.pairs);
            break;

        case JSON_ARRAY:
            for (size_t i = 0; i < val->as.array.count; ++i) {
                json_deinit(&val->as.array.items[i]);          // <- was json_free(...)
            }
            if (val->as.array.items) slab_free(val->as.array.items);
            break;

        default: /* JSON_STRING/INT/BOOL/NULL: no owning heap */ break;
    }
}

static inline void json_free(json_value_t* val) {
    if (!val) return;
    json_deinit(val);
    slab_free(val);   // only the root (or things individually slab_alloc’d)
}

// Get object field by key
static inline json_value_t* json_get_field(json_value_t* obj, const char* key) {
    if (obj->type != JSON_OBJECT) return NULL;

    string_view_t key_sv = STRING_VIEW(key);
    for (size_t i = 0; i < obj->as.object.count; i++) {
        if (sv_equals(obj->as.object.pairs[i].key, key_sv)) {
            return &obj->as.object.pairs[i].value;
        }
    }
    return NULL;
}

// ═══════════════════════════════════════════════════════════════════════════════
// Ultra-Fast JSON Serializer with Pre-computed Layouts
// ═══════════════════════════════════════════════════════════════════════════════

// Pre-computed serialization templates for common patterns
typedef struct {
    const char* template;
    size_t      template_len;
    int         field_offsets[8];  // where to insert values
    int         field_count;
} json_template_t;

// Generate templates at compile time for User struct
static const json_template_t USER_TEMPLATE = {
        .template = "{\"id\":,\"name\":\"\"}",
        .template_len = 17,
        .field_offsets = {6, 16},  // positions to insert id and name
        .field_count = 2
};

// Fast integer to string using lookup table
static const char digit_pairs[201] =
        "0001020304050607080910111213141516171819"
        "2021222324252627282930313233343536373839"
        "4041424344454647484950515253545556575859"
        "6061626364656667686970717273747576777879"
        "8081828384858687888990919293949596979899";

static inline char* fast_itoa(char* buf, int value) {
    if (value == 0) {
        *buf++ = '0';
        return buf;
    }

    char* p = buf;
    if (value < 0) {
        *p++ = '-';
        value = -value;
    }

    char tmp[16];
    char* tp = tmp;

    while (value >= 100) {
        int pos = (value % 100) * 2;
        value /= 100;
        *tp++ = digit_pairs[pos + 1];
        *tp++ = digit_pairs[pos];
    }

    if (value >= 10) {
        int pos = value * 2;
        *p++ = digit_pairs[pos];
        *p++ = digit_pairs[pos + 1];
    } else {
        *p++ = '0' + value;
    }

    // Reverse tmp into buf
    while (tp > tmp) {
        *p++ = *--tp;
    }

    return p;
}

// Template-based serialization (blazing fast)
static inline size_t serialize_user_fast(char* out, int id, const char* name) {
    char* p = out;

    // Copy template start: {"id":
    memcpy(p, "{\"id\":", 6);
    p += 6;

    // Insert ID
    p = fast_itoa(p, id);

    // Copy middle: ,"name":"
    memcpy(p, ",\"name\":\"", 9);
    p += 9;

    // Insert name (assume it's safe/escaped)
    size_t name_len = strlen(name);
    memcpy(p, name, name_len);
    p += name_len;

    // Copy end: "}
    *p++ = '"';
    *p++ = '}';

    return p - out;
}

#endif // FAST_JSON_H