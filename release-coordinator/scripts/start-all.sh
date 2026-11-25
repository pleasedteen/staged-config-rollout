#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
export PYTHONPATH="$ROOT_DIR${PYTHONPATH:+:$PYTHONPATH}"
export RELCOORD_HOME="${RELCOORD_HOME:-$ROOT_DIR/runtime}"

python3 -m relcoord init --home "$RELCOORD_HOME" --base-port 9301
mkdir -p "$RELCOORD_HOME/pids" "$RELCOORD_HOME/logs"

NAMES=(svc-1 svc-2 svc-3 svc-4 svc-5 svc-6)
for name in "${NAMES[@]}"; do
  if [[ -f "$RELCOORD_HOME/pids/$name.pid" ]] && kill -0 "$(cat "$RELCOORD_HOME/pids/$name.pid")" 2>/dev/null; then
    echo "$name already running pid=$(cat "$RELCOORD_HOME/pids/$name.pid")"
    continue
  fi
  python3 -m relcoord.service --home "$RELCOORD_HOME" --name "$name" \
    >"$RELCOORD_HOME/logs/$name.log" 2>&1 &
  echo $! > "$RELCOORD_HOME/pids/$name.pid"
  echo "started $name pid=$! log=$RELCOORD_HOME/logs/$name.log"
done

if [[ -f "$RELCOORD_HOME/pids/coordinator.pid" ]] && kill -0 "$(cat "$RELCOORD_HOME/pids/coordinator.pid")" 2>/dev/null; then
  echo "coordinator already running pid=$(cat "$RELCOORD_HOME/pids/coordinator.pid")"
else
  python3 -m relcoord.coordinator --home "$RELCOORD_HOME" \
    >"$RELCOORD_HOME/logs/coordinator.log" 2>&1 &
  echo $! > "$RELCOORD_HOME/pids/coordinator.pid"
  echo "started coordinator pid=$! log=$RELCOORD_HOME/logs/coordinator.log"
fi

echo "query: ./relctl status --home $RELCOORD_HOME"
