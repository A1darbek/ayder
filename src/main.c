// main.c – parent process (no threads, no libuv, just forks workers)
#include "cluster.h"
#include "globals.h"
#include "log.h"
#include "metrics_shared.h"
#include "shared_storage.h"
#include <ctype.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
// ────────────────────────────────────────────────────────────────
// global configuration visible inside workers
unsigned g_aof_flush_ms = 10; // 0  → appendfsync always
int g_aof_mode          = 2;  // 0=never, 1=always, 2=fsync (default)

// ────────────────────────────────────────────────────────────────

// graceful shutdown flag (parent only)
static volatile int shutdown_requested = 0;

/* ──────────  signal handling  ────────── */
static void signal_handler(int sig) {
  LOGW("Parent received signal %d, forwarding to cluster ...", sig);
  shutdown_requested = 1;
}

static void setup_signal_handlers(void) {
  struct sigaction sa = {0};
  sa.sa_handler       = signal_handler;
  sigemptyset(&sa.sa_mask);
  sigaction(SIGINT, &sa, NULL);  // Ctrl-C
  sigaction(SIGTERM, &sa, NULL); // kill/terminate
}

static void on_exit_cleanup(void) { metrics_cleanup_shared(); /* shm_unlink("/ramforge_metrics") */ }

static int parse_u16(const char *s, int *out_port) {
  if (!s || !*s)
    return -1;
  long v = 0;
  for (const char *p = s; *p; ++p) {
    if (!isdigit((unsigned char)*p))
      return -1;
    v = v * 10 + (*p - '0');
    if (v > 65535)
      return -1;
  }
  if (v < 1 || v > 65535)
    return -1;
  *out_port = (int)v;
  return 0;
}

// Accepts "host:port", ":port", or just "port"; returns 0 on success
static int parse_hostport_extract_port(const char *hp, int *out_port) {
  if (!hp)
    return -1;
  const char *colon = strrchr(hp, ':');
  if (colon && *(colon + 1)) {
    return parse_u16(colon + 1, out_port);
  }
  // No colon -> maybe just a bare port
  return parse_u16(hp, out_port);
}

static int detect_port_from_env(int *out_port, const char **source) {
  const char *v = getenv("RF_HTTP_PORT");
  if (v && parse_u16(v, out_port) == 0) {
    if (source)
      *source = "env:RF_HTTP_PORT";
    return 0;
  }
  v = getenv("PORT"); // PaaS convention (Heroku/Render/etc.)
  if (v && parse_u16(v, out_port) == 0) {
    if (source)
      *source = "env:PORT";
    return 0;
  }
  v = getenv("RF_HTTP_ADDR"); // e.g. "0.0.0.0:1209" or ":1209"
  if (v && parse_hostport_extract_port(v, out_port) == 0) {
    if (source)
      *source = "env:RF_HTTP_ADDR";
    return 0;
  }
  return -1;
}

/* ──────────  CLI parsing  ────────── */
static void parse_arguments(int argc, char **argv, int *http_port, const char **port_src) {
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "--aof") == 0 && i + 1 < argc) {
      if (strcmp(argv[i + 1], "always") == 0) {
        g_aof_flush_ms = 0;
        g_aof_mode     = 1; // always mode
        LOGI("AOF flush mode: ALWAYS (sync-every-write)");
      } else if (strcmp(argv[i + 1], "never") == 0) {
        g_aof_mode = 0; // disabled
        LOGI("AOF flush mode: NEVER (disabled)");
      } else {
        LOGW("Unknown --aof option “%s”, using default fsync mode", argv[i + 1]);
      }
      i++; // skip value
    } else if ((strcmp(argv[i], "--port") == 0 || strcmp(argv[i], "-p") == 0) && i + 1 < argc) {
      int p = 0;
      if (parse_u16(argv[i + 1], &p) == 0) {
        *http_port = p;
        if (port_src)
          *port_src = "cli:--port";
        LOGI("HTTP port set via --port: %d", p);
      } else {
        LOGE("Invalid --port value: %s (must be 1..65535)", argv[i + 1]);
        exit(2);
      }
      i++;
    } else if (strcmp(argv[i], "--http") == 0 && i + 1 < argc) {
      // Accept host:port or just :port / port; for now we only extract the port for the cluster
      int p = 0;
      if (parse_hostport_extract_port(argv[i + 1], &p) == 0) {
        *http_port = p;
        if (port_src)
          *port_src = "cli:--http";
        LOGI("HTTP port set via --http: %d (host part ignored by parent)", p);
      } else {
        LOGE("Invalid --http value: %s (expected host:port or :port)", argv[i + 1]);
        exit(2);
      }
      i++;
    }
  }
}

/* ──────────  entry point  ────────── */
int main(int argc, char **argv) {
  /* force line-buffered stdout even when redirected */
  setvbuf(stdout, NULL, _IOLBF, 0);

  log_init_from_env();

  int http_port        = 1109;

  const char *port_src = "default:1109";

  // 1) env first (so CLI can override)
  detect_port_from_env(&http_port, &port_src);

  // 2) parse CLI (may override env/default)
  parse_arguments(argc, argv, &http_port, &port_src);

  // sanity
  if (http_port < 1 || http_port > 65535) {
    LOGE("Final HTTP port invalid: %d", http_port);
    return 2;
  }
  setup_signal_handlers();

  LOGI("AOF mode: %s", g_aof_mode == 0 ? "never" : g_aof_mode == 1 ? "always" : "fsync (default)");
  LOGI("AOF flush interval: %s", g_aof_flush_ms == 0 ? "always" : "10 ms (default)");
  LOGI("HTTP port: %d (%s)", http_port, port_src);

  // INITIALIZE SHARED STORAGE BEFORE FORKING WORKERS
  g_shared_storage = shared_storage_init();
  if (!g_shared_storage) {
    LOGE("Failed to initialize shared storage");
    return 1;
  }

  /* forks workers & monitors them */
  init_shared_metrics();

  atexit(on_exit_cleanup);
  int rc = start_cluster_with_args(http_port, argc, argv);

  // CLEANUP: Destroy shared storage when parent exits
  LOGI("Cleaning up shared storage...");
  shared_storage_destroy(g_shared_storage);

  LOGI("Parent exiting (cluster stopped) – status %d", rc);
  return rc;
}
