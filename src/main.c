// main.c – parent process (no threads, no libuv, just forks workers)
#include <stdio.h>
#include <string.h>
#include <signal.h>
#include <stdlib.h>
#include "shared_storage.h"
#include "cluster.h"
#include "metrics_shared.h"
#include "globals.h"
// ────────────────────────────────────────────────────────────────
// global configuration visible inside workers
unsigned g_aof_flush_ms = 10;          // 0  → appendfsync always
int g_aof_mode = 2;                     // 0=never, 1=always, 2=fsync (default)
// ────────────────────────────────────────────────────────────────

// graceful shutdown flag (parent only)
static volatile int shutdown_requested = 0;

/* ──────────  signal handling  ────────── */
static void signal_handler(int sig)
{
    printf("\n🛑 Parent received signal %d, forwarding to cluster …\n", sig);
    shutdown_requested = 1;
}

static void setup_signal_handlers(void)
{
    struct sigaction sa = {0};
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT,  &sa, NULL);      // Ctrl-C
    sigaction(SIGTERM, &sa, NULL);      // kill/terminate
}

static void on_exit_cleanup(void)
{
    metrics_cleanup_shared();        /* shm_unlink("/ramforge_metrics") */
}

/* ──────────  CLI parsing  ────────── */
static void parse_arguments(int argc, char **argv)
{
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--aof") == 0 && i + 1 < argc) {
            if (strcmp(argv[i + 1], "always") == 0) {
                g_aof_flush_ms = 0;
                g_aof_mode = 1;         // always mode
                printf("📝 AOF flush mode: ALWAYS (sync-every-write)\n");
            } else if (strcmp(argv[i + 1], "never") == 0) {
                g_aof_mode = 0;         // disabled
                printf("📝 AOF flush mode: NEVER (disabled)\n");
            } else {
                printf("📝 Unknown --aof option “%s”, using default fsync mode\n",
                       argv[i + 1]);
            }
            i++;                        // skip value
        }
    }
}

/* ──────────  entry point  ────────── */
int main(int argc, char **argv) {
    /* force line-buffered stdout even when redirected */
    setvbuf(stdout, NULL, _IOLBF, 0);

    parse_arguments(argc, argv);
    setup_signal_handlers();

    printf("🚀 RamForge parent – starting cluster with shared storage\n");
    printf("   AOF mode: %s\n",
           g_aof_mode == 0 ? "never" :
           g_aof_mode == 1 ? "always" : "fsync (default)");
    printf("   AOF flush interval: %s\n",
           g_aof_flush_ms == 0 ? "always" : "10 ms (default)");
    printf("   Port: 1109\n");

    // INITIALIZE SHARED STORAGE BEFORE FORKING WORKERS
    printf("📦 Initializing shared storage...\n");
    g_shared_storage = shared_storage_init();
    if (!g_shared_storage) {
        fprintf(stderr, "❌ Failed to initialize shared storage\n");
        return 1;
    }
    printf("✅ Shared storage ready (1M entries, process-safe)\n\n");

    /* forks workers & monitors them */
    init_shared_metrics();

    atexit(on_exit_cleanup);
    int rc = start_cluster_with_args(1109, argc, argv);

    // CLEANUP: Destroy shared storage when parent exits
    printf("🧹 Cleaning up shared storage...\n");
    shared_storage_destroy(g_shared_storage);

    printf("👋 Parent exiting (cluster stopped) – status %d\n", rc);
    return rc;
}