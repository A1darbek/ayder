#include "http_framing.h"

#include <stdint.h>
#include <string.h>

static int ascii_case_equal(const char *value, size_t value_len,
                            const char *literal, size_t literal_len)
{
    size_t i;

    if (value_len != literal_len) return 0;
    for (i = 0; i < value_len; ++i) {
        unsigned char a = (unsigned char)value[i];
        unsigned char b = (unsigned char)literal[i];

        if (a >= 'A' && a <= 'Z') a = (unsigned char)(a + ('a' - 'A'));
        if (b >= 'A' && b <= 'Z') b = (unsigned char)(b + ('a' - 'A'));
        if (a != b) return 0;
    }
    return 1;
}

static void trim_optional_whitespace(const char **value, size_t *value_len)
{
    while (*value_len != 0 && ((*value)[0] == ' ' || (*value)[0] == '\t')) {
        ++*value;
        --*value_len;
    }
    while (*value_len != 0 &&
           ((*value)[*value_len - 1] == ' ' || (*value)[*value_len - 1] == '\t')) {
        --*value_len;
    }
}

static http_framing_result_t parse_content_length(const char *value,
                                                  size_t value_len,
                                                  size_t max_body_size,
                                                  size_t *parsed)
{
    size_t result = 0;
    size_t i;

    trim_optional_whitespace(&value, &value_len);
    if (value_len == 0) return HTTP_FRAMING_INVALID;

    for (i = 0; i < value_len; ++i) {
        unsigned char c = (unsigned char)value[i];
        size_t digit;

        if (c < '0' || c > '9') return HTTP_FRAMING_INVALID;
        digit = (size_t)(c - '0');
        if (result > (SIZE_MAX - digit) / 10) return HTTP_FRAMING_INVALID;
        result = result * 10 + digit;
    }

    if (result > max_body_size) return HTTP_FRAMING_TOO_LARGE;
    *parsed = result;
    return HTTP_FRAMING_OK;
}

http_framing_result_t http_resolve_framing(const struct phr_header *headers,
                                           size_t num_headers,
                                           size_t max_body_size,
                                           http_framing_t *framing)
{
    size_t i;
    size_t content_length = 0;
    size_t content_length_count = 0;
    const char *content_length_text = NULL;
    size_t content_length_text_len = 0;
    size_t transfer_encoding_count = 0;

    if (framing == NULL || (num_headers != 0 && headers == NULL)) {
        return HTTP_FRAMING_INVALID;
    }

    framing->mode = HTTP_BODY_NONE;
    framing->content_length = 0;

    for (i = 0; i < num_headers; ++i) {
        const struct phr_header *header = &headers[i];

        /* picohttpparser represents obsolete line folding with name == NULL. */
        if (header->name == NULL) return HTTP_FRAMING_INVALID;
        if (header->value_len != 0 && header->value == NULL) {
            return HTTP_FRAMING_INVALID;
        }

        if (ascii_case_equal(header->name, header->name_len,
                             "Content-Length", sizeof("Content-Length") - 1)) {
            const char *value = header->value;
            size_t value_len = header->value_len;
            size_t parsed = 0;
            http_framing_result_t result;

            trim_optional_whitespace(&value, &value_len);
            result = parse_content_length(value, value_len,
                                          max_body_size, &parsed);
            if (result != HTTP_FRAMING_OK) return result;
            if (content_length_count != 0 &&
                (parsed != content_length ||
                 value_len != content_length_text_len ||
                 memcmp(value, content_length_text, value_len) != 0)) {
                return HTTP_FRAMING_AMBIGUOUS;
            }
            content_length = parsed;
            content_length_text = value;
            content_length_text_len = value_len;
            ++content_length_count;
        } else if (ascii_case_equal(header->name, header->name_len,
                                    "Transfer-Encoding",
                                    sizeof("Transfer-Encoding") - 1)) {
            const char *value = header->value;
            size_t value_len = header->value_len;

            ++transfer_encoding_count;
            trim_optional_whitespace(&value, &value_len);
            if (!ascii_case_equal(value, value_len,
                                  "chunked", sizeof("chunked") - 1)) {
                return HTTP_FRAMING_UNSUPPORTED_TRANSFER_ENCODING;
            }
        }
    }

    if (transfer_encoding_count != 0 && content_length_count != 0) {
        return HTTP_FRAMING_AMBIGUOUS;
    }
    if (transfer_encoding_count > 1) {
        return HTTP_FRAMING_AMBIGUOUS;
    }

    if (transfer_encoding_count == 1) {
        framing->mode = HTTP_BODY_CHUNKED;
    } else if (content_length_count != 0) {
        framing->mode = HTTP_BODY_CONTENT_LENGTH;
        framing->content_length = content_length;
    }
    return HTTP_FRAMING_OK;
}

void http_chunk_decoder_init(http_chunk_decoder_t *decoder,
                             size_t max_decoded_size)
{
    if (decoder == NULL) return;
    memset(decoder, 0, sizeof(*decoder));
    decoder->decoder.consume_trailer = 1;
    decoder->max_decoded_size = max_decoded_size;
}

http_chunk_result_t http_chunk_decoder_feed(http_chunk_decoder_t *decoder,
                                            char *buffer,
                                            size_t *buffer_len,
                                            size_t *leftover_len)
{
    size_t input_len;
    size_t decoded_len;
    ssize_t result;

    if (leftover_len != NULL) *leftover_len = 0;
    if (decoder == NULL || buffer_len == NULL || leftover_len == NULL ||
        decoder->complete) {
        return HTTP_CHUNK_ERROR;
    }

    input_len = *buffer_len;
    if (input_len == 0) return HTTP_CHUNK_INCOMPLETE;
    if (buffer == NULL) return HTTP_CHUNK_ERROR;

    decoded_len = input_len;
    result = phr_decode_chunked(&decoder->decoder, buffer, &decoded_len);
    if (result < -2 || result == -1 || decoded_len > input_len) {
        *buffer_len = 0;
        return HTTP_CHUNK_ERROR;
    }
    if (decoder->decoded_total > decoder->max_decoded_size ||
        decoded_len > decoder->max_decoded_size - decoder->decoded_total) {
        *buffer_len = 0;
        return HTTP_CHUNK_ERROR;
    }

    decoder->decoded_total += decoded_len;
    *buffer_len = decoded_len;
    if (decoder->decoder.bytes_left_in_chunk >
        decoder->max_decoded_size - decoder->decoded_total) {
        *buffer_len = 0;
        return HTTP_CHUNK_ERROR;
    }

    if (result == -2) return HTTP_CHUNK_INCOMPLETE;
    if ((size_t)result > input_len - decoded_len) {
        *buffer_len = 0;
        return HTTP_CHUNK_ERROR;
    }

    *leftover_len = (size_t)result;
    decoder->complete = 1;
    return HTTP_CHUNK_COMPLETE;
}
