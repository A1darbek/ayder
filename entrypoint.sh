#!/bin/sh
set -e

rm -f append.aof dump.rdb

# Now start RAMForge
exec "$@"
