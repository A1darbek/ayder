#include "http_framing.h"

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define FUZZ_MAX_INPUT (128U * 1024U)
#define HTTP_LIMIT (64U * 1024U)

static void fuzz_chunk_one_shot(const uint8_t *data, size_t size)
{
    http_chunk_decoder_t decoder;
    char *scratch;
    size_t decoded_len = size;
    size_t leftover_len = 0;
    http_chunk_result_t result;

    scratch = malloc(size == 0 ? 1 : size);
    if (scratch == NULL) return;
    if (size != 0) memcpy(scratch, data, size);
    http_chunk_decoder_init(&decoder, HTTP_LIMIT);
    result = http_chunk_decoder_feed(&decoder, scratch, &decoded_len,
                                     &leftover_len);
    if (decoded_len > size || decoder.decoded_total > HTTP_LIMIT ||
        (result == HTTP_CHUNK_COMPLETE &&
         leftover_len > size - decoded_len)) {
        __builtin_trap();
    }
    free(scratch);
}

static void fuzz_chunk_fragmented(const uint8_t *data, size_t size)
{
    http_chunk_decoder_t decoder;
    size_t offset = 0;
    size_t fragment_size = size == 0 ? 1 : 1 + data[0] % 31;

    http_chunk_decoder_init(&decoder, HTTP_LIMIT);
    while (offset < size && !decoder.complete) {
        size_t input_len = size - offset;
        size_t decoded_len;
        size_t leftover_len = 0;
        char scratch[31];
        http_chunk_result_t result;

        if (input_len > fragment_size) input_len = fragment_size;
        memcpy(scratch, data + offset, input_len);
        decoded_len = input_len;
        result = http_chunk_decoder_feed(&decoder, scratch, &decoded_len,
                                         &leftover_len);
        if (decoded_len > input_len || decoder.decoded_total > HTTP_LIMIT ||
            (result == HTTP_CHUNK_COMPLETE &&
             leftover_len > input_len - decoded_len)) {
            __builtin_trap();
        }
        if (result == HTTP_CHUNK_ERROR) break;
        offset += input_len;
    }
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    struct phr_header synthetic;
    http_framing_t framing;

    if (size > FUZZ_MAX_INPUT) return 0;

    synthetic.name = "Content-Length";
    synthetic.name_len = sizeof("Content-Length") - 1;
    synthetic.value = (const char *)data;
    synthetic.value_len = size;
    (void)http_resolve_framing(&synthetic, 1, HTTP_LIMIT, &framing);

    synthetic.name = "Transfer-Encoding";
    synthetic.name_len = sizeof("Transfer-Encoding") - 1;
    (void)http_resolve_framing(&synthetic, 1, HTTP_LIMIT, &framing);

    if (size <= HTTP_LIMIT) {
        const char *method = NULL;
        const char *path = NULL;
        size_t method_len = 0;
        size_t path_len = 0;
        int minor_version = 0;
        struct phr_header headers[64];
        size_t num_headers = sizeof(headers) / sizeof(headers[0]);
        int parsed = phr_parse_request((const char *)data, size,
                                       &method, &method_len, &path, &path_len,
                                       &minor_version, headers, &num_headers, 0);

        if (parsed > 0 && (size_t)parsed <= size) {
            http_framing_result_t result =
                    http_resolve_framing(headers, num_headers,
                                         HTTP_LIMIT - (size_t)parsed,
                                         &framing);
            if (result == HTTP_FRAMING_OK && framing.mode == HTTP_BODY_CHUNKED) {
                fuzz_chunk_one_shot(data + parsed, size - (size_t)parsed);
                fuzz_chunk_fragmented(data + parsed, size - (size_t)parsed);
            }
        }
    }

    /* Also drive the chunk state machine independently of HTTP header syntax. */
    fuzz_chunk_one_shot(data, size);
    fuzz_chunk_fragmented(data, size);
    return 0;
}
