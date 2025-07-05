// cluster.c – FIXED forks, monitors and (optionally) restarts worker processes
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sched.h>
#include <signal.h>
#include <errno.h>
#include <string.h>
#include "shared_storage.h"
#include "cluster.h"
#include "slab_alloc.h"
#include "storage.h"
#include "persistence.h"
#include "app.h"
#include "app_routes.h"
#include "zero_pause_rdb.h"
#include "metrics_shared.h"
#include "globals.h"

/* configuration exported by main.c */
extern unsigned g_aof_flush_ms;
extern int g_aof_mode; // 0=never, 1=always, 2=fsync
extern void RAMForge_rotation_init(const char *rdb_base_path, const char *aof_base_path);
extern void RAMForge_configure_rotation_policy(size_t max_rdb_mb, time_t max_age_hours, int keep_count, int chaos_enabled);

extern RAMForgeMetrics *g_metrics_ptr;   /* declared, not defined */
void init_shared_metrics(void);          /* just the prototype */

/* parent-only state */
static volatile int  cluster_shutdown = 0;
static pid_t        *worker_pids      = NULL;
static int           worker_count     = 0;
static int           single_process_mode = 0;


/* ────────── CLI / ENV helpers ────────── */
 int detect_worker_target(int argc, char **argv)
{
    for (int i = 1; i < argc; i++)
        if (!strcmp(argv[i],"--workers") && i+1<argc)
            return atoi(argv[i+1]);

    const char *env = getenv("RAMFORGE_WORKERS");
    if (env) return atoi(env);

    int cores = sysconf(_SC_NPROCESSORS_ONLN);
    return cores < 1 ? 1 : cores;
}

/* ────────── parent signal handler ────────── */
static void cluster_signal_handler(int sig)
{
    printf("🛑 Cluster manager caught signal %d – shutting down workers …\n", sig);
    cluster_shutdown = 1;
    for (int i=0;i<worker_count;i++)
        if (worker_pids[i] > 0) {
            printf("📤 SIGTERM → worker %d (PID %d)\n", i, worker_pids[i]);
            kill(worker_pids[i], SIGTERM);
        }
}

/* ────────── Single process signal handler ────────── */
static void single_process_signal_handler(int sig)
{
    printf("🛑 Single process mode caught signal %d – shutting down gracefully …\n", sig);
    cluster_shutdown = 1;
}

/* ────────── CPU pin helper (worker) ────────── */
static void setup_cpu_affinity(int wid)
{
    if (single_process_mode) return; // Skip CPU pinning in single process mode

    cpu_set_t set; CPU_ZERO(&set); CPU_SET(wid, &set);
    if (sched_setaffinity(0,sizeof set,&set))
        perror("sched_setaffinity");
    else
        printf("⚙ Worker %d pinned to CPU core %d\n", wid, wid);
}

/* ────────── worker bootstrap ────────── */
static App* init_worker_systems(int wid) {
    slab_init();

    // Keep local storage for worker-specific data and caching
    static Storage storage;
    storage_init(&storage);

    // ADDITION: Attach to shared storage (created by parent process)
    if (!g_shared_storage) {
        g_shared_storage = shared_storage_attach();
        if (!g_shared_storage) {
            fprintf(stderr, "❌ Worker %d failed to attach to shared storage\n", wid);
            return NULL;
        }
        if (single_process_mode || wid == 0) {
            printf("✓ Shared storage attached (1M entries available)\n");
        }
    }
    /* 3. ROTATION / METRICS ENGINE – do this **once** ---------- */
    if (wid == 0) {                       // make sure we call it only once
        /* strip the “.rdb” extension – the API expects just the basename */
        RAMForge_rotation_init("./dump", "./append.aof");
        /* optional: customise policy for prod */
        RAMForge_configure_rotation_policy(
                512 /* MB */,     /* max RDB size  */
                24  /* hours */,  /* max age       */
                10  /* keep N files */,
                0   /* chaos OFF in prod       */
        );

        /* 🆕  zero-pause RDB */
        ZeroPauseRDB_init("./zp_dump.rdb",
                          &storage,          /* LOCAL storage ref */
                          200000,            /* max keys */
                          10);               /* snapshot interval seconds */
    }

    const char *aof  = "./append.aof";
    const char *dump = "./dump.rdb";

    if (single_process_mode) {
        printf("Single process mode using AOF: %s\n", aof);
    } else if (wid == 0) {
        printf("Using shared AOF: %s (all workers)\n", aof);
    }

    // Initialize persistence based on AOF mode
    int aof_enabled = (g_aof_mode > 0) ? 1 : 0;
    if (aof_enabled) {
        // IMPORTANT: Pass LOCAL storage to persistence (for AOF recovery)
        // but your HTTP handlers will use shared storage for live data
        Persistence_init(dump, aof, &storage, 60, g_aof_flush_ms);
        if (single_process_mode || wid == 0) {
            printf("✓ AOF persistence enabled (flush_ms=%u)\n", g_aof_flush_ms);
        }
    } else {
        if (single_process_mode || wid == 0) {
            printf("⚠ AOF persistence disabled\n");
        }
    }

    // Create app with LOCAL storage (for compatibility)
    App *app = app_create(&storage);
    if (!app) {
        fprintf(stderr, "❌ app_create failed (worker %d)\n", wid);
        return NULL;
    }

    register_application_routes(app);

    if (single_process_mode) {
        printf("✓ Routes registered (single process mode)\n");
    } else {
        printf("✓ Routes registered (worker %d)\n", wid);
    }

    return app;
}

static void run_worker(int wid, int port)
{
    if (single_process_mode) {
        printf("🏃 Single process mode starting on port %d\n", port);
    } else {
        printf("🏃 Worker %d starting on port %d\n", wid, port);
    }

    setup_cpu_affinity(wid);

    App *app = init_worker_systems(wid);
    if (!app) exit(1);

    // Set up signal handler - different for single vs multi process
    struct sigaction sa={0};
    if (single_process_mode) {
        sa.sa_handler = single_process_signal_handler;
        sigaction(SIGINT, &sa, NULL);
        sigaction(SIGTERM, &sa, NULL);
    } else {
        sa.sa_handler = SIG_DFL;
        sigaction(SIGTERM, &sa, NULL);
    }

    if (single_process_mode) {
        printf("🚀 Single process ready – starting HTTP server …\n");
    } else {
        printf("🚀 Worker %d ready – starting HTTP server …\n", wid);
    }

    app->start(app, port);                /* blocks until exit */

    /* graceful path (rare) */
    if (app->shutdown) app->shutdown();

    // Clean up slab allocator
    slab_destroy();

    exit(0);
}

/* ────────── parent wait-helper ────────── */
static void wait_for_workers(void)
{
    int left = worker_count;
    while (left > 0) {
        int st; pid_t pid = wait(&st);
        if (pid > 0) {
            int wid=-1; for(int i=0;i<worker_count;i++) if(worker_pids[i]==pid) {wid=i;break;}
            if (WIFEXITED(st))
                printf("✓ Worker %d (PID %d) exited code %d\n", wid,pid,WEXITSTATUS(st));
            else if (WIFSIGNALED(st))
                printf("⚠ Worker %d (PID %d) killed by signal %d\n", wid,pid,WTERMSIG(st));
            left--;
        } else if (errno==EINTR) continue; else break;
    }
    printf("✓ All workers exited\n");
}

/* ────────── PUBLIC API ────────── */

int start_cluster_with_args(int port, int argc, char **argv)
{
    worker_count = detect_worker_target(argc, argv);

    /* FIXED - if caller requests 0 workers, run a single worker in-process */
    if (worker_count == 0) {
        printf("🚀 Single-process mode (no cluster manager)\n");
        single_process_mode = 1;
        run_worker(0, port);          /* run_worker never returns */
        return 0;                     /* not reached, but keeps compiler happy */
    }

    single_process_mode = 0;
    worker_pids = calloc(worker_count, sizeof *worker_pids);
    if (!worker_pids) {
        fprintf(stderr, "❌ Failed to allocate memory for worker PIDs\n");
        return -1;
    }

    printf("🚀 Starting RamForge cluster with %d worker%s on port %d\n",
           worker_count, worker_count==1?"":"s", port);

    struct sigaction sa={0}; sa.sa_handler=cluster_signal_handler;
    sigaction(SIGINT,&sa,NULL); sigaction(SIGTERM,&sa,NULL);

    /* fork initial workers */
    for(int i=0;i<worker_count;i++){
        pid_t pid=fork();
        if(pid<0){
            perror("fork");
            // Clean up already forked workers
            for(int j=0; j<i; j++) {
                if(worker_pids[j] > 0) kill(worker_pids[j], SIGTERM);
            }
            free(worker_pids);
            return -1;
        }
        if(pid==0) {
            // Child process
            free(worker_pids); // Child doesn't need this
            run_worker(i,port);
        }
        worker_pids[i]=pid;
        printf("✓ Worker %d started (PID %d)\n",i,pid);

        // Small delay between forks to avoid thundering herd
        usleep(10000); // 10ms
    }

    printf("\n🌟 All workers live – monitoring … (Ctrl-C to stop)\n\n");

    /* monitor loop */
    while(!cluster_shutdown){
        int st; pid_t dead=waitpid(-1,&st,WNOHANG);

        if(dead>0){
            int wid=-1;
            for(int i=0;i<worker_count;i++) {
                if(worker_pids[i]==dead) {
                    wid=i;
                    worker_pids[i] = 0; // Mark as dead
                    break;
                }
            }

            /* decide whether to restart or stop */
            int fatal = (WIFEXITED(st) && WEXITSTATUS(st)!=0) ||
                        WIFSIGNALED(st);

            if (fatal) {
                printf("‼︎ Worker %d (PID %d) exited abnormally – shutting cluster down\n",
                       wid, dead);
                cluster_shutdown = 1;

                // Send SIGTERM to all remaining workers
                for(int i=0; i<worker_count; i++) {
                    if(worker_pids[i] > 0) {
                        kill(worker_pids[i], SIGTERM);
                    }
                }
            } else {
                printf("✓ Worker %d exited normally – stopping cluster\n", wid);
                cluster_shutdown = 1;

                // Send SIGTERM to all remaining workers
                for(int i=0; i<worker_count; i++) {
                    if(worker_pids[i] > 0) {
                        kill(worker_pids[i], SIGTERM);
                    }
                }
            }
        } else if (dead==0) {
            usleep(100000); /* idle 100 ms */
        } else if(errno!=ECHILD){
            if(errno != EINTR) {
                perror("waitpid");
                break;
            }
        }
    }

    printf("🛑 Cluster shutting down – waiting for workers …\n");
    wait_for_workers();
    free(worker_pids);
    printf("✓ Cluster shutdown complete\n");
    return 0;
}

int start_cluster(int port){
    return start_cluster_with_args(port,0,NULL);
}