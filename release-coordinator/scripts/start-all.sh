#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
export PYTHONPATH="$ROOT_DIR${PYTHONPATH:+:$PYTHONPATH}"

: "${RELCOORD_HOME:=$ROOT_DIR/runtime}"
mkdir -p "$RELCOORD_HOME/pids" "$RELCOORD_HOME/logs"
export RELCOORD_HOME="$(cd "$RELCOORD_HOME" && pwd -P)"
PYTHON_BIN="${PYTHON:-python3}"
BASE_PORT="${RELCOORD_BASE_PORT:-9301}"

# Serialize concurrent start-all invocations.  The shared lifecycle slot is
# compatible with the short shared slots used by starting children, while
# stop-all's exclusive slot waits for this whole launch transaction and then
# rejects starts once shutdown owns the directory.
exec 8>"$RELCOORD_HOME/start-all.lock"
flock -w 20 8 || { echo "another start-all is still running for $RELCOORD_HOME" >&2; exit 75; }
exec 9>"$RELCOORD_HOME/lifecycle.lock"
if ! flock -n -s 9; then
  echo "stop-all is in progress for $RELCOORD_HOME; refusing repeated start" >&2
  exit 75
fi
"$PYTHON_BIN" -m relcoord.lifecycle gate \
  --home "$RELCOORD_HOME" --component start-all || exit $?

"$PYTHON_BIN" -m relcoord init --home "$RELCOORD_HOME" --base-port "$BASE_PORT"
mkdir -p "$RELCOORD_HOME/pids" "$RELCOORD_HOME/logs"

safe_pid() {
  "$PYTHON_BIN" -m relcoord.lifecycle safe-pid \
    --home "$RELCOORD_HOME" --component "$1" 2>/dev/null || true
}

NAMES=(svc-1 svc-2 svc-3 svc-4 svc-5 svc-6)
for name in "${NAMES[@]}"; do
  pid="$(safe_pid "$name")"
  if [[ -n "$pid" ]]; then
    echo "$name already running pid=$pid"
    continue
  fi
  "$PYTHON_BIN" -m relcoord.service --home "$RELCOORD_HOME" --name "$name" \
    >"$RELCOORD_HOME/logs/$name.log" 2>&1 8>&- 9>&- &
  pid=$!
  echo "$pid" > "$RELCOORD_HOME/pids/$name.pid"
  echo "started $name pid=$pid log=$RELCOORD_HOME/logs/$name.log"
done

coordinator_pid="$(safe_pid coordinator)"
if [[ -n "$coordinator_pid" ]]; then
  echo "coordinator already running pid=$coordinator_pid"
else
  "$PYTHON_BIN" -m relcoord.coordinator --home "$RELCOORD_HOME" \
    >"$RELCOORD_HOME/logs/coordinator.log" 2>&1 8>&- 9>&- &
  coordinator_pid=$!
  echo "$coordinator_pid" > "$RELCOORD_HOME/pids/coordinator.pid"
  echo "started coordinator pid=$coordinator_pid log=$RELCOORD_HOME/logs/coordinator.log"
fi

echo "query: ./relctl status --home $RELCOORD_HOME"
