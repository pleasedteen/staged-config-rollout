#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ $# -lt 1 ]]; then
  echo "usage: $0 <service-name>" >&2
  exit 64
fi
NAME="$1"
cd "$ROOT_DIR"
export PYTHONPATH="$ROOT_DIR${PYTHONPATH:+:$PYTHONPATH}"
: "${RELCOORD_HOME:=$ROOT_DIR/runtime}"
mkdir -p "$RELCOORD_HOME/pids" "$RELCOORD_HOME/logs"
export RELCOORD_HOME="$(cd "$RELCOORD_HOME" && pwd -P)"
PYTHON_BIN="${PYTHON:-python3}"
PID_FILE="$RELCOORD_HOME/pids/$NAME.pid"

# Validates the pid/file command line and start-time metadata before sending
# any signal, so a stale PID reused by an unrelated process is left alone.
"$PYTHON_BIN" -m relcoord.lifecycle stop-component \
  --home "$RELCOORD_HOME" --component "$NAME" || exit $?

"$ROOT_DIR/scripts/start-service.sh" "$NAME" \
  >>"$RELCOORD_HOME/logs/$NAME.manual-restart.log" 2>&1 &
new_pid=$!
echo "$new_pid" > "$PID_FILE"

for _ in {1..50}; do
  if ! kill -0 "$new_pid" 2>/dev/null; then
    echo "$NAME failed to start; see $RELCOORD_HOME/logs/$NAME.manual-restart.log" >&2
    exit 1
  fi
  safe_pid="$("$PYTHON_BIN" -m relcoord.lifecycle safe-pid \
    --home "$RELCOORD_HOME" --component "$NAME" 2>/dev/null || true)"
  if [[ "$safe_pid" == "$new_pid" ]]; then
    echo "restarted $NAME pid=$new_pid; log=$RELCOORD_HOME/logs/$NAME.manual-restart.log"
    exit 0
  fi
  sleep 0.1
done

echo "$NAME did not register within 5s; pid=$new_pid" >&2
exit 1
