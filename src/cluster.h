#ifndef CLUSTER_H
#define CLUSTER_H
#include <signal.h>

/**
 * Start a multi-process cluster with one worker per CPU core.
 * Each worker will:
 * - Be pinned to a specific CPU core
 * - Have its own AOF file (ramforge.worker.N.aof)
 * - Listen on the same port (using SO_REUSEPORT)
 * - Handle HTTP requests independently
 *
 * The master process will monitor workers and restart them if they crash.
 *
 * @param port - Port number for HTTP server
 * @return 0 on successful shutdown, non-zero on error
 */
// Start the framework in built-in cluster mode on the specified port.
int start_cluster(int port);
int start_cluster_with_args(int port, int argc, char **argv);


#endif
