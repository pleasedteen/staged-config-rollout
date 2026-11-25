#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
export PYTHONPATH="$ROOT_DIR${PYTHONPATH:+:$PYTHONPATH}"

: "${RELCOORD_HOME:=$ROOT_DIR/runtime}"
mkdir -p "$RELCOORD_HOME/pids"
export RELCOORD_HOME="$(cd "$RELCOORD_HOME" && pwd -P)"

LIFECYCLE_LOCK="$RELCOORD_HOME/lifecycle.lock"
exec 9>"$LIFECYCLE_LOCK"
if ! flock -n -s 9; then
  echo "stop-all is in progress for $RELCOORD_HOME; refusing coordinator takeover" >&2
  exit 75
fi

# Record the pre-exec shell PID under a temporary "starting" component.
# exec preserves the PID; once the singleton is won, Python publishes the
# canonical coordinator.pid.  Start-time metadata lets stop-all reject a PID
# that has since been reused by an unrelated process.
python3 -m relcoord.lifecycle register \
  --home "$RELCOORD_HOME" \
  --component coordinator-starting \
  --pid "$$" >/dev/null

# Release the shared lifecycle lock before replacing this shell with the
# long-lived Python coordinator.
exec 9>&-

exec python3 -m relcoord.coordinator --home "$RELCOORD_HOME" "$@"
