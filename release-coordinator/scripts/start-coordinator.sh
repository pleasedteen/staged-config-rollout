#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
export PYTHONPATH="$ROOT_DIR${PYTHONPATH:+:$PYTHONPATH}"
export RELCOORD_HOME="${RELCOORD_HOME:-$ROOT_DIR/runtime}"
exec python3 -m relcoord.coordinator --home "$RELCOORD_HOME" "$@"
