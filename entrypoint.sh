#!/bin/sh
set -e

if [ -z "${RF_BEARER_TOKENS:-}" ]; then
  echo "RF_BEARER_TOKENS is required; provision a unique secret before startup" >&2
  exit 64
fi

DATA_DIR=/data
mkdir -p "$DATA_DIR"

if [ "${RF_RESET_DATA:-0}" = "1" ]; then
  rm -f "$DATA_DIR/append.aof" "$DATA_DIR/append.aof.sealed" "$DATA_DIR/append.aof.sealed.lock"
fi

for f in append.aof append.aof.sealed append.aof.sealed.lock; do
  if [ -e "/app/$f" ] && [ ! -L "/app/$f" ]; then
    mv "/app/$f" "$DATA_DIR/$f" || true
  fi

  [ -e "$DATA_DIR/$f" ] || touch "$DATA_DIR/$f"

  ln -sf "$DATA_DIR/$f" "/app/$f"
done

exec "$@"
