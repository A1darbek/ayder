CC=gcc
CPPFLAGS=-I./include -Ideps/picohttpparser -DHTTP_SERVER_FAST -DNDEBUG
CFLAGS=-O3 -g -pipe -flto -march=native -ffunction-sections -Wall -Wextra -Wshadow -Wdouble-promotion
LDFLAGS=-flto
LDLIBS=-luv -lz -luring -lm -lcurl -lssl -lcrypto

SRC=$(wildcard src/*.c) deps/picohttpparser/picohttpparser.c
OBJ=$(SRC:.c=.o)
EXEC=ayder
.DEFAULT_GOAL := $(EXEC)
BUILD_DIR?=build

ANALYSIS_CPPFLAGS=-I./include -I./src -Ideps/picohttpparser -DHTTP_SERVER_FAST
SECURITY_TEST_SRCS=tests/security/http_framing_test.c src/http_framing.c deps/picohttpparser/picohttpparser.c
SECURITY_TEST_BIN=$(BUILD_DIR)/security/http_framing_test
SANITIZE_FLAGS=-fsanitize=address,undefined -fno-omit-frame-pointer
SANITIZE_CFLAGS=-std=gnu11 -O1 -g -Wall -Wextra -Wshadow $(SANITIZE_FLAGS)
SANITIZE_TEST_BIN=$(BUILD_DIR)/sanitize/http_framing_test
SANITIZE_BIN=$(BUILD_DIR)/sanitize/ayder
FUZZ_CC?=clang
FUZZ_SRCS=fuzz/http_framing_fuzz.c src/http_framing.c deps/picohttpparser/picohttpparser.c
FUZZ_BIN=$(BUILD_DIR)/fuzz/http_framing_fuzz
FUZZ_SMOKE_BIN=$(BUILD_DIR)/fuzz/http_framing_fuzz_smoke
HARDENED_BIN=$(BUILD_DIR)/hardened/ayder
HARDENED_CFLAGS=-std=gnu11 -O2 -g -pipe -fstack-protector-strong -fPIE -D_FORTIFY_SOURCE=3 -Wall -Wextra -Wshadow -Wformat=2 -Wformat-security
HARDENED_LDFLAGS=-pie -Wl,-z,relro,-z,now,-z,noexecstack
PORTABLE_BIN=$(BUILD_DIR)/portable/ayder
PORTABLE_CFLAGS=-std=gnu11 -O3 -g -pipe -flto -ffunction-sections -Wall -Wextra -Wshadow -Wdouble-promotion

%.o: %.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $< -o $@

src/http_server.o src/http_framing.o: src/http_framing.h

$(EXEC): $(OBJ)
	$(CC) $(OBJ) -o $@ $(LDFLAGS) $(LDLIBS)

debug: $(EXEC)
	gdb ./$(EXEC)

$(SECURITY_TEST_BIN): $(SECURITY_TEST_SRCS) src/http_framing.h
	mkdir -p $(dir $@)
	$(CC) $(ANALYSIS_CPPFLAGS) -std=c11 -O2 -g -Wall -Wextra -Wpedantic -Werror $(SECURITY_TEST_SRCS) -o $@

security-test: $(SECURITY_TEST_BIN)
	./$(SECURITY_TEST_BIN)

$(SANITIZE_TEST_BIN): $(SECURITY_TEST_SRCS) src/http_framing.h
	mkdir -p $(dir $@)
	$(CC) $(ANALYSIS_CPPFLAGS) $(SANITIZE_CFLAGS) -Wpedantic -Werror $(SECURITY_TEST_SRCS) -o $@

$(SANITIZE_BIN): $(SRC) src/http_framing.h
	mkdir -p $(dir $@)
	$(CC) $(ANALYSIS_CPPFLAGS) $(SANITIZE_CFLAGS) $(SRC) -o $@ $(SANITIZE_FLAGS) $(LDLIBS)

sanitize: $(SANITIZE_BIN) $(SANITIZE_TEST_BIN)
	ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1 ./$(SANITIZE_TEST_BIN)

static-analysis:
	mkdir -p $(BUILD_DIR)/static
	$(CC) $(ANALYSIS_CPPFLAGS) -std=c11 -O0 -g -Wall -Wextra -Wpedantic -Werror -fanalyzer -c src/http_framing.c -o $(BUILD_DIR)/static/http_framing.o
	$(CC) $(ANALYSIS_CPPFLAGS) -std=c11 -O0 -g -Wall -Wextra -Wpedantic -Werror -fanalyzer -c tests/security/http_framing_test.c -o $(BUILD_DIR)/static/http_framing_test.o
	$(CC) $(ANALYSIS_CPPFLAGS) -std=gnu11 -O0 -g -Wall -Wextra -Wshadow -fanalyzer -c src/http_server.c -o $(BUILD_DIR)/static/http_server.o
	$(CC) $(ANALYSIS_CPPFLAGS) -std=gnu11 -O0 -g -Wall -Wextra -Wshadow -Werror -fanalyzer -c src/storage.c -o $(BUILD_DIR)/static/storage.o
	$(CC) $(ANALYSIS_CPPFLAGS) -std=gnu11 -O0 -g -Wall -Wextra -Wshadow -Werror -fanalyzer -c src/aof_batch.c -o $(BUILD_DIR)/static/aof_batch.o
	@if command -v clang-tidy >/dev/null 2>&1; then clang-tidy src/http_framing.c tests/security/http_framing_test.c -- $(ANALYSIS_CPPFLAGS); else printf '%s\n' 'clang-tidy unavailable; GCC -fanalyzer completed'; fi

static: static-analysis

$(FUZZ_BIN): $(FUZZ_SRCS) src/http_framing.h
	@command -v $(FUZZ_CC) >/dev/null 2>&1 || { printf '%s\n' 'clang is required for the coverage-guided fuzz target'; exit 1; }
	mkdir -p $(dir $@)
	$(FUZZ_CC) $(ANALYSIS_CPPFLAGS) -std=c11 -O1 -g -fno-omit-frame-pointer -fsanitize=fuzzer,address,undefined $(FUZZ_SRCS) -o $@

fuzz: $(FUZZ_BIN)

$(FUZZ_SMOKE_BIN): $(FUZZ_SRCS) fuzz/http_framing_standalone.c src/http_framing.h
	mkdir -p $(dir $@)
	$(CC) $(ANALYSIS_CPPFLAGS) -std=c11 -O1 -g -fno-omit-frame-pointer -fsanitize=address,undefined -Wall -Wextra -Wpedantic -Werror $(FUZZ_SRCS) fuzz/http_framing_standalone.c -o $@

fuzz-smoke: $(FUZZ_SMOKE_BIN)
	ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1 ./$(FUZZ_SMOKE_BIN) fuzz/corpus/http_framing/*.req

$(HARDENED_BIN): $(SRC) src/http_framing.h
	mkdir -p $(dir $@)
	$(CC) $(ANALYSIS_CPPFLAGS) $(HARDENED_CFLAGS) $(SRC) -o $@ $(HARDENED_LDFLAGS) $(LDLIBS)

hardened: $(HARDENED_BIN)

$(PORTABLE_BIN): $(SRC) src/http_framing.h
	mkdir -p $(dir $@)
	$(CC) $(ANALYSIS_CPPFLAGS) $(PORTABLE_CFLAGS) $(SRC) -o $@ -flto $(LDLIBS)

portable: $(PORTABLE_BIN)

security-raw-test:
	python3 tests/security/http_raw_regression.py --host "$(or $(HTTP_SECURITY_HOST),127.0.0.1)" --port "$(or $(HTTP_SECURITY_PORT),1163)"

cross-worker-test: $(EXEC)
	bash tests/regression/cross_worker_visibility.sh
	bash tests/regression/cross_worker_apply_failure.sh

raft-safety-test:
	bash tests/regression/run_p0_durability_tests.sh

idempotency-test: $(EXEC)
	bash tests/regression/idempotency_atomic_visibility.sh

cold-backup-test: $(EXEC)
	bash tests/regression/cold_backup_restore_test.sh

clean:
	rm -f $(OBJ) $(EXEC)

.PHONY: debug clean security-test sanitize static static-analysis fuzz fuzz-smoke hardened portable security-raw-test cross-worker-test raft-safety-test idempotency-test cold-backup-test
