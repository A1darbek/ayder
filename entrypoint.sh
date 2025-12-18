#!/bin/sh
set -e
export RF_BEARER_TOKENS='dev@55555555555555:11111111111111111:111111111111111111111'
rm -f append.aof ./append.aof.sealed ./append.aof.sealed.lock

# Now start Ayder
exec "$@"
