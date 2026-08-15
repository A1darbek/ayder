#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);

static unsigned long executions;

static void run_one(const uint8_t *data, size_t size)
{
    (void)LLVMFuzzerTestOneInput(data, size);
    ++executions;
}

static int run_file(const char *path)
{
    FILE *file = fopen(path, "rb");
    uint8_t *input = NULL;
    long file_size;
    size_t size;
    size_t i;

    if (file == NULL) {
        perror(path);
        return 0;
    }
    if (fseek(file, 0, SEEK_END) != 0 || (file_size = ftell(file)) < 0 ||
        fseek(file, 0, SEEK_SET) != 0 || file_size > 128 * 1024) {
        fprintf(stderr, "cannot read fuzz seed: %s\n", path);
        fclose(file);
        return 0;
    }

    size = (size_t)file_size;
    input = malloc(size == 0 ? 1 : size);
    if (input == NULL || fread(input, 1, size, file) != size) {
        fprintf(stderr, "cannot load fuzz seed: %s\n", path);
        free(input);
        fclose(file);
        return 0;
    }
    fclose(file);

    run_one(input, size);
    for (i = 0; i <= size; ++i) run_one(input, i);
    for (i = 0; i < size; ++i) {
        uint8_t saved = input[i];

        input[i] ^= 0xffU;
        run_one(input, size);
        input[i] = (uint8_t)'0';
        run_one(input, size);
        input[i] = saved;
    }
    free(input);
    return 1;
}

int main(int argc, char **argv)
{
    static const uint8_t built_in[] =
            "POST / HTTP/1.1\r\n"
            "Transfer-Encoding: chunked\r\n"
            "Content-Length: 1\r\n\r\n"
            "1\r\na\r\n0\r\n\r\n";
    int i;

    run_one(built_in, sizeof(built_in) - 1);
    for (i = 1; i < argc; ++i) {
        if (!run_file(argv[i])) return 1;
    }
    printf("http_framing_fuzz_smoke: %lu executions passed\n", executions);
    return 0;
}
