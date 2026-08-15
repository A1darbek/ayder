#include "http_framing.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define ARRAY_LEN(a) (sizeof(a) / sizeof((a)[0]))

static unsigned int failures;
static unsigned int checks;

#define CHECK(condition)                                                        \
    do {                                                                        \
        ++checks;                                                               \
        if (!(condition)) {                                                     \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__,          \
                    #condition);                                                \
            ++failures;                                                         \
        }                                                                       \
    } while (0)

static struct phr_header make_header(const char *name, const char *value)
{
    struct phr_header header;

    header.name = name;
    header.name_len = name == NULL ? 0 : strlen(name);
    header.value = value;
    header.value_len = value == NULL ? 0 : strlen(value);
    return header;
}

static void expect_framing(struct phr_header *headers, size_t count,
                           size_t limit, http_framing_result_t expected_result,
                           http_body_mode_t expected_mode,
                           size_t expected_length)
{
    http_framing_t framing = {HTTP_BODY_CHUNKED, 999};
    http_framing_result_t result =
            http_resolve_framing(headers, count, limit, &framing);

    CHECK(result == expected_result);
    if (result == HTTP_FRAMING_OK) {
        CHECK(framing.mode == expected_mode);
        CHECK(framing.content_length == expected_length);
    }
}

static void test_content_length(void)
{
    struct phr_header valid[] = {
        make_header("Content-Length", "0005")
    };
    struct phr_header duplicate_equal[] = {
        make_header("Content-Length", "5"),
        make_header("content-length", "5")
    };
    struct phr_header duplicate_representation_mismatch[] = {
        make_header("Content-Length", "5"),
        make_header("content-length", "05")
    };
    struct phr_header duplicate_mismatch[] = {
        make_header("Content-Length", "5"),
        make_header("Content-Length", "6")
    };
    const char *invalid_values[] = {
        "", "+5", "-1", "1x", "1 0", "5, 5", "5\v"
    };
    size_t i;

    expect_framing(NULL, 0, 64, HTTP_FRAMING_OK, HTTP_BODY_NONE, 0);
    expect_framing(valid, ARRAY_LEN(valid), 64, HTTP_FRAMING_OK,
                   HTTP_BODY_CONTENT_LENGTH, 5);
    valid[0] = make_header("Content-Length", "\t 5 \t");
    expect_framing(valid, ARRAY_LEN(valid), 64, HTTP_FRAMING_OK,
                   HTTP_BODY_CONTENT_LENGTH, 5);
    valid[0] = make_header("Content-Length", "0");
    expect_framing(valid, ARRAY_LEN(valid), 64, HTTP_FRAMING_OK,
                   HTTP_BODY_CONTENT_LENGTH, 0);
    expect_framing(duplicate_equal, ARRAY_LEN(duplicate_equal), 64,
                   HTTP_FRAMING_OK, HTTP_BODY_CONTENT_LENGTH, 5);
    expect_framing(duplicate_representation_mismatch,
                   ARRAY_LEN(duplicate_representation_mismatch), 64,
                   HTTP_FRAMING_AMBIGUOUS, HTTP_BODY_NONE, 0);
    expect_framing(duplicate_mismatch, ARRAY_LEN(duplicate_mismatch), 64,
                   HTTP_FRAMING_AMBIGUOUS, HTTP_BODY_NONE, 0);

    for (i = 0; i < ARRAY_LEN(invalid_values); ++i) {
        valid[0] = make_header("Content-Length", invalid_values[i]);
        expect_framing(valid, ARRAY_LEN(valid), 64, HTTP_FRAMING_INVALID,
                       HTTP_BODY_NONE, 0);
    }

    valid[0] = make_header("Content-Length", "65");
    expect_framing(valid, ARRAY_LEN(valid), 64, HTTP_FRAMING_TOO_LARGE,
                   HTTP_BODY_NONE, 0);
    valid[0] = make_header("Content-Length", "184467440737095516160");
    CHECK(http_resolve_framing(valid, ARRAY_LEN(valid), (size_t)-1,
                               &(http_framing_t){0}) != HTTP_FRAMING_OK);
}

static void test_transfer_encoding(void)
{
    struct phr_header valid[] = {
        make_header("Transfer-Encoding", "chunked")
    };
    struct phr_header conflict[] = {
        make_header("Transfer-Encoding", "chunked"),
        make_header("Content-Length", "3")
    };
    struct phr_header duplicate[] = {
        make_header("Transfer-Encoding", "chunked"),
        make_header("transfer-encoding", "chunked")
    };
    struct phr_header folded[] = {
        make_header("X-Test", "one"),
        make_header(NULL, "continued")
    };
    const char *invalid_values[] = {
        "gzip", "chunkedx", "gzip, chunked", "chunked, chunked", "chunked;foo"
    };
    size_t i;

    expect_framing(valid, ARRAY_LEN(valid), 64, HTTP_FRAMING_OK,
                   HTTP_BODY_CHUNKED, 0);
    valid[0] = make_header("transfer-encoding", "\tChunked ");
    expect_framing(valid, ARRAY_LEN(valid), 64, HTTP_FRAMING_OK,
                   HTTP_BODY_CHUNKED, 0);

    for (i = 0; i < ARRAY_LEN(invalid_values); ++i) {
        valid[0] = make_header("Transfer-Encoding", invalid_values[i]);
        expect_framing(valid, ARRAY_LEN(valid), 64,
                       HTTP_FRAMING_UNSUPPORTED_TRANSFER_ENCODING,
                       HTTP_BODY_NONE, 0);
    }

    expect_framing(conflict, ARRAY_LEN(conflict), 64,
                   HTTP_FRAMING_AMBIGUOUS, HTTP_BODY_NONE, 0);
    expect_framing(duplicate, ARRAY_LEN(duplicate), 64,
                   HTTP_FRAMING_AMBIGUOUS, HTTP_BODY_NONE, 0);
    expect_framing(folded, ARRAY_LEN(folded), 64,
                   HTTP_FRAMING_INVALID, HTTP_BODY_NONE, 0);
    CHECK(http_resolve_framing(NULL, 1, 64, &(http_framing_t){0}) ==
          HTTP_FRAMING_INVALID);
    CHECK(http_resolve_framing(valid, ARRAY_LEN(valid), 64, NULL) ==
          HTTP_FRAMING_INVALID);
}

static void expect_raw_framing(const char *request,
                               http_framing_result_t expected_result,
                               http_body_mode_t expected_mode,
                               size_t expected_length)
{
    const char *method = NULL;
    const char *path = NULL;
    size_t method_len = 0;
    size_t path_len = 0;
    int minor_version = 0;
    struct phr_header headers[32];
    size_t num_headers = ARRAY_LEN(headers);
    http_framing_t framing = {0};
    int parsed = phr_parse_request(request, strlen(request),
                                   &method, &method_len, &path, &path_len,
                                   &minor_version, headers, &num_headers, 0);

    CHECK(parsed > 0);
    if (parsed <= 0) return;
    CHECK(http_resolve_framing(headers, num_headers, 65536 - (size_t)parsed,
                               &framing) == expected_result);
    if (expected_result == HTTP_FRAMING_OK) {
        CHECK(framing.mode == expected_mode);
        CHECK(framing.content_length == expected_length);
    }
}

static void test_adversarial_raw_headers(void)
{
    expect_raw_framing("POST / HTTP/1.1\r\nHost: test\r\n"
                       "Transfer-Encoding: chunked\r\nContent-Length: 4\r\n\r\n",
                       HTTP_FRAMING_AMBIGUOUS, HTTP_BODY_NONE, 0);
    expect_raw_framing("POST / HTTP/1.1\r\nHost: test\r\n"
                       "Content-Length: 4\r\nTransfer-Encoding: chunked\r\n\r\n",
                       HTTP_FRAMING_AMBIGUOUS, HTTP_BODY_NONE, 0);
    expect_raw_framing("POST / HTTP/1.1\r\nHost: test\r\n"
                       "Content-Length: 4\r\nContent-Length: 5\r\n\r\n",
                       HTTP_FRAMING_AMBIGUOUS, HTTP_BODY_NONE, 0);
    expect_raw_framing("POST / HTTP/1.1\r\nHost: test\r\n"
                       "Content-Length: 4x\r\n\r\n",
                       HTTP_FRAMING_INVALID, HTTP_BODY_NONE, 0);
    expect_raw_framing("POST / HTTP/1.1\r\nHost: test\r\n"
                       "Transfer-Encoding: chunkedx\r\n\r\n",
                       HTTP_FRAMING_UNSUPPORTED_TRANSFER_ENCODING,
                       HTTP_BODY_NONE, 0);
    expect_raw_framing("POST / HTTP/1.1\r\nHost: test\r\n"
                       "Content-Length: 4\r\nContent-Length: 4\r\n\r\n",
                       HTTP_FRAMING_OK, HTTP_BODY_CONTENT_LENGTH, 4);
}

static http_chunk_result_t decode_one_shot(const char *encoded, size_t limit,
                                           char *decoded, size_t decoded_cap,
                                           size_t *decoded_len,
                                           size_t *leftover_len)
{
    http_chunk_decoder_t decoder;
    size_t encoded_len = strlen(encoded);
    char *scratch = malloc(encoded_len == 0 ? 1 : encoded_len);
    size_t output_len = encoded_len;
    http_chunk_result_t result;

    CHECK(scratch != NULL);
    if (!scratch) return HTTP_CHUNK_ERROR;
    memcpy(scratch, encoded, encoded_len);
    http_chunk_decoder_init(&decoder, limit);
    result = http_chunk_decoder_feed(&decoder, scratch, &output_len,
                                     leftover_len);
    CHECK(output_len <= decoded_cap);
    if (output_len <= decoded_cap) memcpy(decoded, scratch, output_len);
    *decoded_len = output_len;
    free(scratch);
    return result;
}

static void test_chunked_one_shot(void)
{
    char decoded[64];
    size_t decoded_len = 0;
    size_t leftover_len = 0;
    http_chunk_result_t result;

    result = decode_one_shot("4\r\nWiki\r\n5\r\npedia\r\n0\r\n\r\n", 64,
                             decoded, sizeof(decoded), &decoded_len,
                             &leftover_len);
    CHECK(result == HTTP_CHUNK_COMPLETE);
    CHECK(decoded_len == 9);
    CHECK(memcmp(decoded, "Wikipedia", 9) == 0);
    CHECK(leftover_len == 0);

    result = decode_one_shot("3;foo=bar\r\nabc\r\n0\r\nX-Test: ok\r\n\r\n", 64,
                             decoded, sizeof(decoded), &decoded_len,
                             &leftover_len);
    CHECK(result == HTTP_CHUNK_COMPLETE);
    CHECK(decoded_len == 3);
    CHECK(memcmp(decoded, "abc", 3) == 0);
    CHECK(leftover_len == 0);

    result = decode_one_shot("1\r\na\r\n0\r\n\r\nGET", 64,
                             decoded, sizeof(decoded), &decoded_len,
                             &leftover_len);
    CHECK(result == HTTP_CHUNK_COMPLETE);
    CHECK(decoded_len == 1);
    CHECK(decoded[0] == 'a');
    CHECK(leftover_len == 3);

    result = decode_one_shot("4\r\ntest\r\n0\r\n\r\n", 3,
                             decoded, sizeof(decoded), &decoded_len,
                             &leftover_len);
    CHECK(result == HTTP_CHUNK_ERROR);
    result = decode_one_shot("41\r\n", 64,
                             decoded, sizeof(decoded), &decoded_len,
                             &leftover_len);
    CHECK(result == HTTP_CHUNK_ERROR);
    result = decode_one_shot("1x\r\na\r\n0\r\n\r\n", 64,
                             decoded, sizeof(decoded), &decoded_len,
                             &leftover_len);
    CHECK(result == HTTP_CHUNK_ERROR);
    result = decode_one_shot("10000000000000000\r\n", 64,
                             decoded, sizeof(decoded), &decoded_len,
                             &leftover_len);
    CHECK(result == HTTP_CHUNK_ERROR);
}

static void test_chunked_fragmented(void)
{
    static const char encoded[] =
            "4\r\nWiki\r\n5;name=value\r\npedia\r\n0\r\nTrailer: yes\r\n\r\n";
    http_chunk_decoder_t decoder;
    char decoded[64];
    size_t decoded_len = 0;
    size_t i;

    http_chunk_decoder_init(&decoder, sizeof(decoded));
    for (i = 0; i < sizeof(encoded) - 1; ++i) {
        char byte = encoded[i];
        size_t byte_len = 1;
        size_t leftover_len = 0;
        http_chunk_result_t result =
                http_chunk_decoder_feed(&decoder, &byte, &byte_len,
                                        &leftover_len);

        CHECK(result == (i == sizeof(encoded) - 2 ?
                         HTTP_CHUNK_COMPLETE : HTTP_CHUNK_INCOMPLETE));
        CHECK(leftover_len == 0);
        CHECK(byte_len <= 1);
        if (byte_len == 1) decoded[decoded_len++] = byte;
    }
    CHECK(decoded_len == 9);
    CHECK(memcmp(decoded, "Wikipedia", 9) == 0);
    CHECK(decoder.decoded_total == decoded_len);

    {
        char extra = 'x';
        size_t extra_len = 1;
        size_t leftover_len = 0;
        CHECK(http_chunk_decoder_feed(&decoder, &extra, &extra_len,
                                      &leftover_len) == HTTP_CHUNK_ERROR);
    }
}

int main(void)
{
    test_content_length();
    test_transfer_encoding();
    test_adversarial_raw_headers();
    test_chunked_one_shot();
    test_chunked_fragmented();

    if (failures != 0) {
        fprintf(stderr, "http_framing_test: %u/%u checks failed\n",
                failures, checks);
        return 1;
    }
    printf("http_framing_test: %u checks passed\n", checks);
    return 0;
}
