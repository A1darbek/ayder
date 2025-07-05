
#define _GNU_SOURCE
#include <unistd.h>
#include <sys/wait.h>
#include <stdio.h>
#include <stdlib.h>
#include "../src/storage.h"
#include "../src/aof_batch.h"

/* helper: save + append */
static void put(Storage *st, int id) {
    printf("Writing record %d\n", id);
    storage_save(st, id, &id, sizeof id);
    if (AOF_append(id, &id, sizeof id) < 0) {
        perror("AOF_append"); _exit(1);
    }
    printf("Record %d written successfully\n", id);
}

int main(void)
{
    printf("Starting test...\n");
    unlink("mf.aof");
    Storage st; storage_init(&st);
    AOF_init("mf.aof", 1<<12, 0);          /* --aof always */

    /* parent writes one record */
    put(&st, 1);

    pid_t child = fork();
    if (child == 0) {                      /* writer #2 */
        printf("Child process writing...\n");
        put(&st, 2);
        printf("Child process done\n");
        _exit(0);
    }

    int status;
    waitpid(child, &status, 0);
    printf("Child exited with status %d\n", status);

    /* parent again */
    put(&st, 3);

    printf("Before rewrite - checking file size\n");
    system("ls -la mf.aof");

    /* compact */
    AOF_rewrite(&st);
    AOF_shutdown();
    storage_destroy(&st);

    printf("After rewrite - checking file size\n");
    system("ls -la mf.aof");

    /* replay into fresh storage */
    printf("Starting verification...\n");
    Storage verify; storage_init(&verify);
    AOF_init("mf.aof", 1<<12, 0);
    AOF_load(&verify);
    AOF_shutdown();

    printf("Checking what we loaded:\n");
    int found = 0, tmp;
    for (int k = 1; k <= 3; k++) {
        if (storage_get(&verify, k, &tmp, sizeof tmp)) {
            printf("Found key %d with value %d\n", k, tmp);
            found++;
        } else {
            printf("Key %d NOT FOUND\n", k);
        }
    }

    storage_destroy(&verify);
    printf("Total found: %d/3\n", found);
    printf(found == 3 ? "✓ multi-fork reopen OK\n"
                      : "✗ reopen failed\n");
    return (found == 3) ? 0 : 1;
}