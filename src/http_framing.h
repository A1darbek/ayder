#ifndef AYDER_HTTP_FRAMING_H
#define AYDER_HTTP_FRAMING_H

#include <stddef.h>

#include "picohttpparser.h"

typedef enum {
    HTTP_BODY_NONE = 0,
    HTTP_BODY_CONTENT_LENGTH,
    HTTP_BODY_CHUNKED
} http_body_mode_t;

typedef struct {
    http_body_mode_t mode;
    size_t content_length;
} http_framing_t;

typedef enum {
    HTTP_FRAMING_OK = 0,
    HTTP_FRAMING_INVALID = -1,
    HTTP_FRAMING_TOO_LARGE = -2,
    HTTP_FRAMING_AMBIGUOUS = -3,
    HTTP_FRAMING_UNSUPPORTED_TRANSFER_ENCODING = -4
} http_framing_result_t;

/*
 * Resolve request-body framing from the complete header list.
 *
 * Content-Length values are strict unsigned decimal numbers. Repeated
 * Content-Length fields are accepted only when all trimmed values are
 * identical.
 * Transfer-Encoding is supported only as one exact "chunked" field and may
 * never be combined with Content-Length. Obsolete folded fields are rejected.
 */
http_framing_result_t http_resolve_framing(const struct phr_header *headers,
                                           size_t num_headers,
                                           size_t max_body_size,
                                           http_framing_t *framing);

typedef struct {
    struct phr_chunked_decoder decoder;
    size_t decoded_total;
    size_t max_decoded_size;
    int complete;
} http_chunk_decoder_t;

typedef enum {
    HTTP_CHUNK_ERROR = -1,
    HTTP_CHUNK_INCOMPLETE = 0,
    HTTP_CHUNK_COMPLETE = 1
} http_chunk_result_t;

void http_chunk_decoder_init(http_chunk_decoder_t *decoder,
                             size_t max_decoded_size);

/*
 * Decode one newly received byte range in place. On return, *buffer_len is
 * the number of decoded body bytes at the start of buffer. If the terminal
 * chunk was found, *leftover_len reports bytes after the message boundary.
 */
http_chunk_result_t http_chunk_decoder_feed(http_chunk_decoder_t *decoder,
                                            char *buffer,
                                            size_t *buffer_len,
                                            size_t *leftover_len);

#endif
