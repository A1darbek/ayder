// cluster.c – FIXED forks, monitors and (optionally) restarts worker processes
#define _GNU_SOURCE
#include <uv.h>
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
#include "app.h"
#include "app_routes.h"
#include "metrics_shared.h"
#include "globals.h"
#include "persistence_zp.h"
#include "rf_broker.h"
#include "ramforge_ha_integration.h"

/* configuration exported by main.c */
extern unsigned g_aof_flush_ms;
extern int g_aof_mode; // 0=never, 1=always, 2=fsync
extern void RAMForge_rotation_init(const char *rdb_base_path, const char *aof_base_path);
extern void RAMForge_configure_rotation_policy(size_t max_rdb_mb, time_t max_age_hours, int keep_count, int chaos_enabled);

extern RAMForgeMetrics *g_metrics_ptr;   /* declared, not defined */
void init_shared_metrics(void);          /* just the prototype */

static pid_t manager_pid = -1;  /* parent-only state */
static volatile int  cluster_shutdown = 0;
static pid_t        *worker_pids      = NULL;
static int           worker_count     = 0;
static int           single_process_mode = 0;

static volatile sig_atomic_t shut_once = 0;

/* ────────── CLI / ENV helpers ────────── */
int detect_worker_target(int argc, char **argv) {
    for (int i = 1; i < argc; i++)
        if (!strcmp(argv[i],"--workers") && i+1<argc)
            return atoi(argv[i+1]);

    const char *env = getenv("RAMFORGE_WORKERS");
    if (env) return atoi(env);

    int cores = sysconf(_SC_NPROCESSORS_ONLN);
    return cores < 1 ? 1 : cores;
}

static void ignore_sigpipe(void) {
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = SIG_IGN;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGPIPE, &sa, NULL);
}

/* ────────── parent signal handler ────────── */
static void cluster_signal_handler(int sig) {
    /* Guard: only the cluster manager should run this */
    if (getpid() != manager_pid) return;

    /* Prevent re-entrancy */
    if (shut_once) return;
    shut_once = 1;
    printf("🛑 Cluster manager caught signal %d – shutting down workers …\n", sig);
    cluster_shutdown = 1;
    if (!worker_pids || worker_count <= 0) return;
    for (int i=0;i<worker_count;i++)
        if (worker_pids[i] > 0) {
            printf("📤 SIGTERM → worker %d (PID %d)\n", i, worker_pids[i]);
            kill(worker_pids[i], SIGTERM);
        }
}

/* ────────── Single process signal handler ────────── */
static void single_process_signal_handler(int sig) {
    printf("🛑 Single process mode caught signal %d – shutting down gracefully …\n", sig);
    cluster_shutdown = 1;
}

/* ────────── CPU pin helper (worker) ────────── */
static void setup_cpu_affinity(int wid) {
    if (single_process_mode) return;

    int ncpu = (int)sysconf(_SC_NPROCESSORS_ONLN);
    if (ncpu <= 0) return;

    int ded_wid = -1;
    const char *ded = getenv("RF_HA_DEDICATED_WORKER");
    if (ded && *ded) ded_wid = atoi(ded);

    int cpu = 0;

    if (ncpu == 1) {
        cpu = 0;
    } else if (wid == ded_wid) {
        cpu = 0; // HA/OS core
    } else {
        // pack data workers onto 1..ncpu-1
        int slot = wid;
        if (ded_wid >= 0 && wid > ded_wid) slot--;   // close the “gap” after removing dedicated worker
        cpu = 1 + (slot % (ncpu - 1));
    }

    cpu_set_t set; CPU_ZERO(&set); CPU_SET(cpu, &set);
    if (sched_setaffinity(0, sizeof set, &set))
        perror("sched_setaffinity");
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
    const char *aof  = "./append.aof";

    if (single_process_mode) {
        printf("Single process mode using AOF: %s\n", aof);
    }

    rf_broker_init(/*ring_per_partition*/ 0, /*default_partitions*/ 8);
    // Initialize persistence based on AOF mode
    int aof_enabled = (g_aof_mode > 0) ? 1 : 0;
    if (aof_enabled) {

        /* Enhanced AOF flush settings for io_uring */
        unsigned enhanced_flush_ms = g_aof_flush_ms;

        /* Optimize flush interval for io_uring batching */
        if (enhanced_flush_ms > 0 && enhanced_flush_ms < 5) {
            enhanced_flush_ms = 5; /* Minimum 5ms for effective batching */
        }

        // IMPORTANT: Pass LOCAL storage to persistence (for AOF recovery)
        // but your HTTP handlers will use shared storage for live data
        Persistence_zp_init( aof, &storage, enhanced_flush_ms);

        /* start in-memory cross-worker promoter */
        rf_bus_start_promoter();
        if (single_process_mode || wid == 0) {
            printf("✓ AOF persistence enabled (flush_ms=%u)\n", enhanced_flush_ms);
            printf("   Ring capacity: 128K entries\n");
            printf("   Buffer pool: 2048 aligned buffers\n");
            printf("   Batch size: 256 operations\n");
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

    register_application_routes(app, wid == 0 /*control-plane?*/);


    return app;
}

static void run_worker(int wid, int port) {
    ignore_sigpipe();

    setup_cpu_affinity(wid);
    { char buf[16]; snprintf(buf, sizeof buf, "%d", wid); setenv("RF_WORKER_ID", buf, 1); }

    App *app = init_worker_systems(wid);
    if (!app) exit(1);

    // Set up signal handler - different for single vs multi process
    struct sigaction sa={0};
    if (single_process_mode) {
        sa.sa_handler = single_process_signal_handler;
        sigaction(SIGINT, &sa, NULL);
        sigaction(SIGTERM, &sa, NULL);
    } else {
        sigemptyset(&sa.sa_mask);
        sa.sa_flags = 0;
        sa.sa_handler = SIG_DFL;
        sigaction(SIGTERM, &sa, NULL);
        sigaction(SIGINT, &sa, NULL);
        sigaction(SIGHUP, &sa, NULL);
    }

    /* Initialize HA in ALL workers:
    - worker 0: full HA (raft + net + thread)
    - workers >0: agent-only (read HA state from shmem; forward writes)
     */
    int ha_dedicated = 0;
    const char *ded = getenv("RF_HA_DEDICATED_WORKER");
    if (ded && *ded) ha_dedicated = (atoi(ded) == wid);

    const char *ha_en = getenv("RF_HA_ENABLED");
    if (ha_en && ha_en[0] == '1') {
        if (wid > 0) setenv("RF_HA_AGENT_ONLY", "1", 1);
        printf("🔧 Worker %d initializing HA (%s)...\n", wid, (wid == 0 ? "leader-runtime" : "agent-only"));
        if (RAMForge_HA_init() != 0) {
            fprintf(stderr, "⚠️  Worker %d HA init failed - continuing without HA\n", wid);
        }
    }

    if (!ha_dedicated) {
        app->start(app, port);   // participates in SO_REUSEPORT
    } else {
        fprintf(stderr, "🧠 Worker %d is HA-dedicated: not listening on data port %d\n", wid, port);
        for (;;) pause();        // keep process alive; HA threads keep running
    }

    /* graceful path (rare) */
    if (app->shutdown) app->shutdown();

    /* stop rocket follower; join to flush cleanly */

    // Clean up slab allocator
    rf_broker_shutdown();
    slab_destroy();
    RAMForge_HA_shutdown();

    exit(0);
}

/* ────────── parent wait-helper ────────── */
static void wait_for_workers(void) {
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

int start_cluster_with_args(int port, int argc, char **argv) {
    ignore_sigpipe();
    worker_count = detect_worker_target(argc, argv);

    /* shared promotion bus (64MB heap, 8192 descriptors) */
    size_t bus_heap = 512ULL * 1024ULL * 1024ULL;   // 512MB default
    size_t bus_desc = 1ULL << 20;                   // 1,048,576 default

    const char *e;
    if ((e = getenv("RF_BUS_HEAP_MB")) && *e) bus_heap = (size_t)strtoull(e, NULL, 10) * 1024ULL * 1024ULL;
    if ((e = getenv("RF_BUS_DESC")) && *e)    bus_desc = (size_t)strtoull(e, NULL, 10);

    rf_bus_init_pre_fork(bus_heap, bus_desc);

    /* FIXED - if caller requests 0 workers, run a single worker in-process */
    if (worker_count == 0) {
        printf("Single-process mode (no cluster manager)\n");
        single_process_mode = 1;
        run_worker(0, port);
        /* run_worker never returns */
        return 0;                     /* not reached, but keeps compiler happy */
    }

    single_process_mode = 0;
    worker_pids = calloc(worker_count, sizeof *worker_pids);
    if (!worker_pids) {
        fprintf(stderr, "❌ Failed to allocate memory for worker PIDs\n");
        return -1;
    }

    printf("Starting Ayder with %d worker%s on port %d\n",
           worker_count, worker_count==1?"":"s", port);

    manager_pid = getpid();

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
            ignore_sigpipe();
            // Child process
            free(worker_pids); // Child doesn't need this
            worker_pids  = NULL;
            worker_count = 0;
            run_worker(i,port);
        }
        worker_pids[i]=pid;

        // Small delay between forks to avoid thundering herd
        usleep(10000); // 10ms
    }


    printf("\n All workers live – monitoring … (Ctrl-C to stop)\n\n");
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
