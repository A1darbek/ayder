#include <inttypes.h>
#include <stdio.h>
#include <string.h>

#include "fast_json.h"

static unsigned checks;
static unsigned failures;

#define CHECK(condition, label) do {                                         \
    checks++;                                                               \
    if (!(condition)) {                                                     \
        failures++;                                                         \
        fprintf(stderr, "FAIL %s\n", (label));                             \
    }                                                                       \
} while (0)

static void check_integer_with_byte_after_boundary(void) {
    char input[] = {'{', '"', 'n', '"', ':', '2', '}', '.', '\0'};
    json_value_t *root = json_parse(input, 7);
    json_value_t *number;

    CHECK(root != NULL, "bounded object parses");
    if (!root) return;
    number = json_get_field(root, "n");
    CHECK(number != NULL, "bounded integer field exists");
    CHECK(number && number->type == JSON_INT, "byte after boundary cannot change number type");
    CHECK(number && number->as.i == 2, "bounded integer value");
    json_free(root);
}

static void check_number_forms(void) {
    static const char object[] =
        "{\"min\":-9223372036854775808,\"max\":9223372036854775807,"
        "\"fraction\":-12.5,\"exponent\":6.25e2}";
    json_value_t *root = json_parse(object, sizeof(object) - 1);
    json_value_t *min_value;
    json_value_t *max_value;
    json_value_t *fraction;
    json_value_t *exponent;

    CHECK(root != NULL, "valid bounded number forms parse");
    if (!root) return;
    min_value = json_get_field(root, "min");
    max_value = json_get_field(root, "max");
    fraction = json_get_field(root, "fraction");
    exponent = json_get_field(root, "exponent");
    CHECK(min_value && min_value->type == JSON_INT && min_value->as.i == INT64_MIN,
          "INT64_MIN parses");
    CHECK(max_value && max_value->type == JSON_INT && max_value->as.i == INT64_MAX,
          "INT64_MAX parses");
    CHECK(fraction && fraction->type == JSON_DOUBLE && fraction->as.d == -12.5,
          "fraction parses");
    CHECK(exponent && exponent->type == JSON_DOUBLE && exponent->as.d == 625.0,
          "exponent parses");
    json_free(root);
}

static void check_invalid_scalars(void) {
    static const char *const invalid[] = {
        "-", "01", "1.", "1e", "1e+", "9223372036854775808",
        "-9223372036854775809", "tru", "fals", "nul", "true false"
    };

    for (size_t i = 0; i < sizeof(invalid) / sizeof(invalid[0]); i++) {
        json_value_t *value = json_parse(invalid[i], strlen(invalid[i]));
        CHECK(value == NULL, invalid[i]);
        json_free(value);
    }
}

int main(void) {
    slab_init();
    check_integer_with_byte_after_boundary();
    check_number_forms();
    check_invalid_scalars();
    slab_destroy();

    printf("fast_json bounds: %u checks, %u failures\n", checks, failures);
    return failures ? 1 : 0;
}
