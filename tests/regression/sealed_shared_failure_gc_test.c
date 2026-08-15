#define _GNU_SOURCE

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

#include "../../src/aof_batch.c"

RAMForgeMetrics *g_metrics_ptr;

int main(void) {
    g_metrics_ptr = mmap(NULL, sizeof(*g_metrics_ptr), PROT_READ | PROT_WRITE,
                         MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (g_metrics_ptr == MAP_FAILED) {
        perror("mmap metrics");
        return 1;
    }
    memset(g_metrics_ptr, 0, sizeof(*g_metrics_ptr));
    atomic_store(&g_opt_sealed.failed, false);
    atomic_store(&g_opt_sealed.shutdown, false);

    pid_t child = fork();
    if (child < 0) {
        perror("fork");
        return 1;
    }
    if (child == 0) {
        sealed_mark_failed();
        _exit(0);
    }
    int status = 0;
    if (waitpid(child, &status, 0) != child || !WIFEXITED(status) ||
        WEXITSTATUS(status) != 0) {
        fprintf(stderr, "failure-publishing child did not exit cleanly\n");
        return 1;
    }

    if (atomic_load(&g_opt_sealed.failed) ||
        atomic_load(&g_metrics.persistence_failures_total) != 1 ||
        AOF_sealed_is_healthy() ||
        AOF_append_sealed(7, "x", 1) != 0 ||
        AOF_sealed_wait(1) != -1) {
        fprintf(stderr,
                "shared fatal state did not stop sibling writer/waiter: local=%d shared=%lu healthy=%d\n",
                atomic_load(&g_opt_sealed.failed),
                (unsigned long)atomic_load(&g_metrics.persistence_failures_total),
                AOF_sealed_is_healthy());
        return 1;
    }

    char lock_path[] = "/tmp/ayder-sealed-lock-XXXXXX";
    int holder = mkstemp(lock_path);
    int contender = holder >= 0 ? open(lock_path, O_RDWR | O_CLOEXEC) : -1;
    if (holder < 0 || contender < 0 || flock(holder, LOCK_EX) != 0) {
        perror("lock fixture");
        return 1;
    }
    errno = 0;
    if (sealed_lock_with_timeout(contender) != -1 || errno != ECANCELED) {
        fprintf(stderr, "shared fatal did not cancel lock wait: errno=%d\n", errno);
        return 1;
    }
    close(contender);
    close(holder);
    unlink(lock_path);

    uint64_t after = 99;
    setenv("RF_ENABLE_EXPERIMENTAL_SEALED_GC", "1", 1);
    setenv("RF_EFFECTIVE_WORKER_COUNT", "2", 1);
    errno = 0;
    if (AOF_sealed_gc(0, 0, &after) != -1 || errno != ENOTSUP || after != 0) {
        fprintf(stderr,
                "multiworker sealed GC was not refused: errno=%d after=%lu\n",
                errno, (unsigned long)after);
        return 1;
    }
    unsetenv("RF_ENABLE_EXPERIMENTAL_SEALED_GC");
    unsetenv("RF_EFFECTIVE_WORKER_COUNT");
    after = 99;
    errno = 0;
    if (AOF_sealed_gc(0, 0, &after) != -1 || errno != ENOTSUP || after != 0) {
        fprintf(stderr, "sealed GC did not fail closed without worker identity\n");
        return 1;
    }

    munmap(g_metrics_ptr, sizeof(*g_metrics_ptr));
    puts("PASS shared fatal state stops sibling durability and multiworker GC is refused");
    return 0;
}
