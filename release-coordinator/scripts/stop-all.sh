#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

: "${RELCOORD_HOME:=$ROOT_DIR/runtime}"
mkdir -p "$RELCOORD_HOME"
export RELCOORD_HOME="$(cd "$RELCOORD_HOME" && pwd -P)"
export PYTHONPATH="$ROOT_DIR${PYTHONPATH:+:$PYTHONPATH}"

exec python3 -m relcoord.lifecycle stop-all --home "$RELCOORD_HOME" "$@"
