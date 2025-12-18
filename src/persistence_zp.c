// persistence_zp.c – zero-pause snapshot + AOF
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include "persistence_zp.h"
#include "aof_batch.h"
#include <sys/file.h>
#include <fcntl.h>
#include <stdatomic.h>

static Storage *g_st;
static int g_aof_enabled = 0;
static char *g_aof_path = NULL;

static _Atomic int g_persistence_ready = 0;

int Persistence_is_ready(void) { return atomic_load(&g_persistence_ready); }

static void replay_sealed_in_this_worker(const char *aof_path) {
    size_t n = AOF_sealed_replay(aof_path);
    if (n > 0) {
        printf("🧩 PID %d replayed %zu sealed record(s)\n", getpid(), n);
    }
    /* IMPORTANT: do not truncate here. Multiple workers may still be starting.
       You can truncate later from a single elected process (worker 0 / control-plane). */
}

void Persistence_zp_init(
                         const char *aof_path, /* "./append.aof"       */
                         Storage    *st,
                         unsigned    aof_flush_ms)
{
    g_st = st;
    g_aof_enabled = (aof_flush_ms > 0);

    if (g_aof_path) {
        free(g_aof_path);
    }
    g_aof_path = strdup(aof_path);


    /*  Start AOF writer and replay log                        */
    if (g_aof_enabled) {

        /* Enhanced ring capacity for high-throughput */
        size_t ring_capacity = 1 << 17; /* 128K entries */

        AOF_init(aof_path, ring_capacity, aof_flush_ms);

        /* Load and replay AOF log */
        AOF_load(st);

        replay_sealed_in_this_worker(aof_path);

        AOF_live_follow_start();

        atomic_store(&g_persistence_ready, 1);


    } else {
        printf("⚠️  AOF disabled (flush_ms=0)\n");
    }
}

void Persistence_shutdown(void) {

    printf("🛑 Shutting down persistence systems...\n");

    if (g_aof_enabled) {
        printf("⏳ Flushing pending AOF writes...\n");

        /* Wait for pending writes to complete */
        size_t pending = AOF_pending_writes();
        if (pending > 0) {
            printf("   Waiting for %zu pending writes...\n", pending);

            /* Wait with timeout */
            for (int i = 0; i < 50 && AOF_pending_writes() > 0; i++) {
                usleep(100000); /* 100ms */
            }
        }

        /* Force sync critical data */
        AOF_sync();

        /* Shutdown AOF system */
        AOF_live_follow_stop();
        AOF_shutdown();
        printf("✅ AOF shutdown complete\n");
    }

    /* Cleanup */
    if (g_aof_path) {
        free(g_aof_path);
        g_aof_path = NULL;
    }

    printf("✅ Persistence shutdown complete\n");
}
void Persistence_compact(void) {
    printf("🗜️  Starting persistence compaction...\n");

    if (!g_st) {
        printf("❌ No storage instance available for compaction\n");
        return;
    }

    /* 1. Start non-blocking AOF rewrite */
    if (g_aof_enabled) {
        printf("🔄 Starting AOF rewrite...\n");
        AOF_rewrite(g_st); /* non-blocking inside */

        /* Optional: wait for rewrite to complete */
        while (AOF_rewrite_in_progress()) {
            printf("   AOF rewrite in progress...\n");
            sleep(1);
        }

        printf("✅ AOF rewrite completed\n");
    }

    printf("✅ Persistence compaction complete\n");
}

void Persistence_segment_rewrite(void) {
    if (!g_aof_enabled || !g_aof_path) {
        printf("❌ AOF not enabled for segment rewrite\n");
        return;
    }

    printf("🔄 Starting AOF segment rewrite...\n");

    if (AOF_segment_rewrite_in_progress()) {
        printf("⚠️  Segment rewrite already in progress\n");
        return;
    }

    int result = AOF_begin_rewrite(g_aof_path);
    if (result == 0) {
        printf("✅ AOF segment rewrite started\n");
    } else {
        printf("❌ Failed to start AOF segment rewrite\n");
    }
}


