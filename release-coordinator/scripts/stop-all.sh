#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export RELCOORD_HOME="${RELCOORD_HOME:-$ROOT_DIR/runtime}"

for pid_file in "$RELCOORD_HOME/pids"/*.pid; do
  [[ -e "$pid_file" ]] || continue
  name="$(basename "$pid_file" .pid)"
  pid="$(cat "$pid_file" 2>/dev/null || true)"
  if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null; then
    echo "killing $name pid=$pid"
    kill -9 "$pid" || true
  fi
  rm -f "$pid_file"
done
