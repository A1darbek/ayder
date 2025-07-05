CC=gcc
CFLAGS=-O3 -g -I./include -luv -lz -lhttp_parser -pipe -flto -march=native \
          -fno-plt -fdata-sections -ffunction-sections \
          -DNDEBUG -DHTTP_SERVER_FAST \
          -Wall -Wextra -Wshadow -Wconversion -Wdouble-promotion

SRC=$(wildcard src/*.c)
OBJ=$(SRC:.c=.o)
EXEC=ramforge

$(EXEC): $(OBJ)
	$(CC) $(OBJ) -o $(EXEC) $(CFLAGS)

debug: $(EXEC)
	gdb ./$(EXEC)

clean:
	rm -f $(OBJ) $(EXEC)
TESTS := tests/crc32c_test tests/aof_roundtrip tests/rdb_corrupt tests/aof_multi_fork tests/aof_live_rewrite tests/chaos_latency_test tests/live_rotation tests/test_ramforge_suite

# Test: crc32c_test (needs only its .c and src/crc32c.c)
tests/crc32c_test: tests/crc32c_test.c src/crc32c.c
	$(CC) -Isrc -o $@ $^

# Test: aof_roundtrip (needs aof_batch.c, storage.c, and crc32c.c)
tests/aof_roundtrip: tests/aof_roundtrip.c src/crc32c.c src/aof_batch.c src/storage.c
	$(CC) -Isrc -o $@ $^

tests/rdb_corrupt: tests/rdb_corrupt.c src/crc32c.c
	$(CC) -Isrc -o $@ $^

tests/aof_multi_fork: tests/aof_multi_fork.c src/crc32c.c src/aof_batch.c src/storage.c
	$(CC) -pthread -Isrc -o $@ $^

tests/aof_live_rewrite: tests/aof_live_rewrite.c \
                        src/aof_batch.c src/storage.c src/crc32c.c
	$(CC) -pthread -Isrc -o $@ $^

tests/chaos_latency_test: tests/chaos_latency_test.c \
                         src/zero_pause_rdb.c src/storage.c src/crc32c.c src/slab_alloc.c src/ramforge_rotation_metrics.c
	$(CC) -pthread -Isrc -o $@ $^ -luv

tests/live_rotation: tests/live_rotation.c src/ramforge_rotation_metrics.c src/aof_batch.c src/zero_pause_rdb.c src/crc32c.c src/storage.c src/slab_alloc.c src/metrics_shared.c
	$(CC) -lcurl -lpthread -lrt -o $@ $^ -luv

tests/test_ramforge_suite: tests/test_ramforge_suite.c src/zero_pause_rdb.c src/zero_pause_restore.c src/storage.c src/shared_storage.c src/crc32c.c src/slab_alloc.c src/metrics_shared.c src/ramforge_rotation_metrics.c src/aof_batch.c src/globals.c
	$(CC) -o $@ $^ -lcurl -lpthread -lrt -luv




.PHONY: test
test: $(TESTS)
	@for t in $(TESTS); do $$t || exit 1; done
	@echo "All smoke-unit-chaos-tests passed."

.PHONY: chaos
chaos: tests/chaos/hard_kill.sh tests/chaos/disk_full.sh tests/chaos/power_loss.sh
	@set -e; for t in $^ ; do \
	    echo "=== $$t ==="; \
	    bash $$t ; \
	done
