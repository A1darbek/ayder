#!/bin/sh
set -e

: "${RF_BEARER_TOKENS:=dev@55555555555555:11111111111111111:111111111111111111111}"
export RF_BEARER_TOKENS

DATA_DIR=/data
mkdir -p "$DATA_DIR"

for f in append.aof append.aof.sealed append.aof.sealed.lock; do
  if [ -e "/app/$f" ] && [ ! -L "/app/$f" ]; then
    mv "/app/$f" "$DATA_DIR/$f" || true
  fi

  [ -e "$DATA_DIR/$f" ] || touch "$DATA_DIR/$f"

  ln -sf "$DATA_DIR/$f" "/app/$f"
done

if [ "${RF_RESET_DATA:-0}" = "1" ]; then
  rm -f "$DATA_DIR/append.aof" "$DATA_DIR/append.aof.sealed" "$DATA_DIR/append.aof.sealed.lock"
fi

exec "$@"
