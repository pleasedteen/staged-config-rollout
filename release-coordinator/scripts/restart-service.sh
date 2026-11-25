#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ $# -lt 1 ]]; then
  echo "usage: $0 <service-name>" >&2
  exit 64
fi
NAME="$1"
export RELCOORD_HOME="${RELCOORD_HOME:-$ROOT_DIR/runtime}"
PID_FILE="$RELCOORD_HOME/pids/$NAME.pid"

if [[ -f "$PID_FILE" ]]; then
  PID="$(cat "$PID_FILE")"
  if kill -0 "$PID" 2>/dev/null; then
    echo "stopping $NAME pid=$PID"
    kill -9 "$PID" || true
    for _ in {1..30}; do
      kill -0 "$PID" 2>/dev/null || break
      sleep 0.1
    done
  fi
fi

"$ROOT_DIR/scripts/start-service.sh" "$NAME" \
  >>"$RELCOORD_HOME/logs/$NAME.manual-restart.log" 2>&1 &
echo $! > "$PID_FILE"
echo "restarted $NAME pid=$!; log=$RELCOORD_HOME/logs/$NAME.manual-restart.log"
