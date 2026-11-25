#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ $# -lt 1 ]]; then
  echo "usage: $0 <service-name> [extra service args]" >&2
  exit 64
fi
NAME="$1"; shift || true
cd "$ROOT_DIR"
export PYTHONPATH="$ROOT_DIR${PYTHONPATH:+:$PYTHONPATH}"
: "${RELCOORD_HOME:=$ROOT_DIR/runtime}"
mkdir -p "$RELCOORD_HOME"
export RELCOORD_HOME="$(cd "$RELCOORD_HOME" && pwd -P)"
exec python3 -m relcoord.service --home "$RELCOORD_HOME" --name "$NAME" "$@"
