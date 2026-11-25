#!/usr/bin/env bash
# End-to-end start/stop boundary check for one RELCOORD_HOME.
# Covers: coordinator SIGKILL + foreground takeover, stale/reused PID safety,
# rejection during shutdown, durable pause/rollback continuation, and several
# concurrent start-all/stop-all interleavings.  Usually runs in ~20-35s.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
export PYTHONPATH="$ROOT_DIR${PYTHONPATH:+:$PYTHONPATH}"

START_TS=$SECONDS
HOME_DIR="$(mktemp -d /tmp/rc-lifecycle.XXXXXX)"
export RELCOORD_HOME="$HOME_DIR"
FAKE_PID=""

cleanup() {
  set +e
  if [[ -n "$FAKE_PID" ]] && kill -0 "$FAKE_PID" 2>/dev/null; then
    kill "$FAKE_PID" 2>/dev/null || true
  fi
  RELCOORD_HOME="$HOME_DIR" "$ROOT_DIR/scripts/stop-all.sh" >/dev/null 2>&1 || true
  echo "runtime home retained for inspection: $HOME_DIR"
}
trap cleanup EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

info() {
  printf '\n==> %s\n' "$*"
}

# Find six consecutive free TCP ports for this isolated run.
RELCOORD_BASE_PORT="$(
  python3 - <<'PY'
import socket
for base in range(20000, 60000):
    socks = []
    ok = True
    try:
        for port in range(base, base + 6):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("127.0.0.1", port))
            socks.append(sock)
    except OSError:
        ok = False
    finally:
        for sock in socks:
            sock.close()
    if ok and len(socks) == 6:
        print(base)
        raise SystemExit(0)
raise SystemExit("could not find six free consecutive ports")
PY
)"
export RELCOORD_BASE_PORT
info "using home=$HOME_DIR base-port=$RELCOORD_BASE_PORT"

coord_pid() {
  python3 -m relcoord.lifecycle safe-pid \
    --home "$HOME_DIR" --component coordinator 2>/dev/null || true
}

wait_for_pid_value() {
  local desc="$1" timeout="$2" getter="$3" expected="$4"
  local deadline=$((SECONDS + timeout)) actual=""
  while (( SECONDS < deadline )); do
    actual="$($getter)"
    [[ "$actual" == "$expected" ]] && return 0
    sleep 0.1
  done
  fail "$desc: expected '$expected', last '$actual'"
}

wait_for_empty() {
  local desc="$1" timeout="$2" getter="$3"
  local deadline=$((SECONDS + timeout)) actual="x"
  while (( SECONDS < deadline )); do
    actual="$($getter)"
    [[ -z "$actual" ]] && return 0
    sleep 0.1
  done
  fail "$desc: still has pid '$actual'"
}

db_query() {
  python3 - "$HOME_DIR" "$1" <<'PY'
import sqlite3
import sys
home, query = sys.argv[1], sys.argv[2]
con = sqlite3.connect(f"file:{home}/coordinator.sqlite3?mode=ro", uri=True, timeout=3)
row = con.execute(query).fetchone()
if row is None:
    sys.exit(1)
print(row[0])
PY
}

wait_sql_value() {
  local desc="$1" expected="$2" sql="$3" timeout="${4:-30}"
  local deadline=$((SECONDS + timeout)) actual=""
  while (( SECONDS < deadline )); do
    actual="$(db_query "$sql" 2>/dev/null || true)"
    [[ "$actual" == "$expected" ]] && return 0
    sleep 0.25
  done
  fail "$desc: expected '$expected', last '$actual'"
}

wait_release_status() {
  wait_sql_value "$1" "$2" \
    "SELECT status FROM releases ORDER BY created_at DESC, id DESC LIMIT 1" "${3:-30}"
}

assert_no_managed() {
  local context="$1"
  local left
  left="$(
    python3 - "$HOME_DIR" <<'PY'
import sys
from relcoord.lifecycle import canonical_home, collect_targets
home = canonical_home(sys.argv[1])
for pid, info in sorted(collect_targets(home).items()):
    print(pid, info.get("kind"), info.get("component"))
PY
  )"
  if [[ -n "$left" ]]; then
    echo "$left" >&2
    fail "$context: managed processes remain"
  fi
}

assert_process_gone() {
  local pid="$1" desc="$2" deadline=$((SECONDS + 8))
  while (( SECONDS < deadline )); do
    kill -0 "$pid" 2>/dev/null || return 0
    sleep 0.1
  done
  fail "$desc: pid $pid is still visible"
}

# ---------------------------------------------------------------------------
info "1/5 start-all, SIGKILL coordinator, foreground takeover, safe stop"
"$ROOT_DIR/scripts/start-all.sh"
deadline=$((SECONDS + 15))
while (( SECONDS < deadline )); do
  [[ -n "$(coord_pid)" ]] && break
  sleep 0.1
done
[[ -n "$(coord_pid)" ]] || fail "coordinator did not publish a live pid"
wait_sql_value "all six services observed reachable" 6 \
  "SELECT COUNT(*) FROM services WHERE reachable=1" 20

OLD_COORD="$(coord_pid)"
[[ -n "$OLD_COORD" ]] || fail "no initial coordinator pid"
kill -9 "$OLD_COORD"
wait_for_empty "old killed coordinator disappears" 8 coord_pid

"$ROOT_DIR/scripts/start-coordinator.sh" >"$HOME_DIR/logs/foreground-takeover.log" 2>&1 &
FG_COORD=$!
wait_for_pid_value "foreground takeover becomes discoverable" 12 coord_pid "$FG_COORD"

deadline=$((SECONDS + 10))
while (( SECONDS < deadline )); do
  [[ "$(cat "$HOME_DIR/pids/coordinator.pid" 2>/dev/null || true)" == "$FG_COORD" ]] && break
  sleep 0.1
done
[[ "$(cat "$HOME_DIR/pids/coordinator.pid" 2>/dev/null || true)" == "$FG_COORD" ]] \
  || fail "foreground coordinator did not update pids/coordinator.pid"

# Simulate a stale pid number being reused by an unrelated long-lived process.
sleep 60 &
FAKE_PID=$!
echo "$FAKE_PID" > "$HOME_DIR/pids/coordinator.pid"

"$ROOT_DIR/scripts/stop-all.sh"
assert_process_gone "$FG_COORD" "foreground coordinator after stop-all"
if ! kill -0 "$FAKE_PID" 2>/dev/null; then
  fail "stop-all killed unrelated stale-PID process $FAKE_PID"
fi
kill "$FAKE_PID" 2>/dev/null || true
FAKE_PID=""
assert_no_managed "foreground takeover stop"

# ---------------------------------------------------------------------------
info "2/5 reject takeover and duplicate start while stop owns lifecycle lock"
flock -x "$HOME_DIR/lifecycle.lock" sleep 3 &
HOLDER=$!
sleep 0.3

set +e
"$ROOT_DIR/scripts/start-coordinator.sh" >"$HOME_DIR/logs/lock-fg.log" 2>&1
RC_FG=$?
"$ROOT_DIR/scripts/start-all.sh" >"$HOME_DIR/logs/lock-all.log" 2>&1
RC_ALL=$?
python3 -m relcoord.lifecycle gate --home "$HOME_DIR" --component verify >/dev/null 2>&1
RC_GATE=$?
set -e
wait "$HOLDER" 2>/dev/null || true

[[ "$RC_FG" -ne 0 ]] || fail "start-coordinator was accepted during shutdown lock"
[[ "$RC_ALL" -ne 0 ]] || fail "start-all was accepted during shutdown lock"
[[ "$RC_GATE" -eq 75 ]] || fail "gate returned $RC_GATE, expected 75"
assert_no_managed "locked shutdown rejection"

# ---------------------------------------------------------------------------
info "3/5 explicit restart continues accepted pause to paused boundary"
"$ROOT_DIR/scripts/start-all.sh" >"$HOME_DIR/logs/pause-start-all.log"
wait_sql_value "all services reachable before pause test" 6 \
  "SELECT COUNT(*) FROM services WHERE reachable=1" 20

SVC3_PID="$(python3 -m relcoord.lifecycle safe-pid --home "$HOME_DIR" --component svc-3)"
kill -9 "$SVC3_PID"
deadline=$((SECONDS + 8))
while (( SECONDS < deadline )); do
  p="$(python3 -m relcoord.lifecycle safe-pid --home "$HOME_DIR" --component svc-3 2>/dev/null || true)"
  [[ -z "$p" ]] && break
  sleep 0.1
done

"$ROOT_DIR/relctl" submit "$ROOT_DIR/configs/target-v3.json" \
  --batch svc-1,svc-2,svc-3 \
  --batch svc-4,svc-5 \
  --batch svc-6 >/dev/null
"$ROOT_DIR/relctl" pause --reason lifecycle-verify >/dev/null
wait_sql_value "svc-1 confirmed while svc-3 blocks batch" 1 \
  "SELECT applied_confirmed FROM release_services WHERE service_name='svc-1' AND release_id=(SELECT id FROM releases ORDER BY created_at DESC LIMIT 1)" \
  15

"$ROOT_DIR/scripts/stop-all.sh"
"$ROOT_DIR/scripts/start-all.sh" >"$HOME_DIR/logs/pause-restart.log"
wait_release_status "restarted coordinator enters accepted paused boundary" paused 35
BATCH_INDEX="$(db_query "SELECT batch_index FROM releases ORDER BY created_at DESC LIMIT 1")"
SVC4_STARTED="$(db_query "SELECT started FROM release_services WHERE service_name='svc-4' AND release_id=(SELECT id FROM releases ORDER BY created_at DESC LIMIT 1)")"
[[ "$BATCH_INDEX" == "0" ]] || fail "pause advanced batch index to $BATCH_INDEX"
[[ "$SVC4_STARTED" == "0" ]] || fail "batch 2 started while paused"

"$ROOT_DIR/relctl" resume >/dev/null
wait_release_status "resumed release completes" completed 35

# ---------------------------------------------------------------------------
info "4/5 explicit restart continues accepted in-progress rollback"
SVC5_PID="$(python3 -m relcoord.lifecycle safe-pid --home "$HOME_DIR" --component svc-5)"
kill -9 "$SVC5_PID"
sleep 0.3
"$ROOT_DIR/relctl" rollback --reason lifecycle-rollback >/dev/null
wait_release_status "rollback becomes rolling_back" rolling_back 10

"$ROOT_DIR/scripts/stop-all.sh"
"$ROOT_DIR/scripts/start-all.sh" >"$HOME_DIR/logs/rollback-restart.log"
wait_release_status "restarted coordinator finishes accepted rollback" rolled_back 35

# ---------------------------------------------------------------------------
info "5/5 start-all/stop-all interleavings"
for i in 1 2 3 4; do
  "$ROOT_DIR/scripts/start-all.sh" >"$HOME_DIR/logs/interleave-$i-start.log" 2>&1 &
  START_PID=$!
  if (( i % 2 == 0 )); then
    sleep 0.12
  fi
  "$ROOT_DIR/scripts/stop-all.sh" >"$HOME_DIR/logs/interleave-$i-stop.log" 2>&1
  wait "$START_PID" 2>/dev/null || true
  # A second stop must be idempotent and must not kill unrelated processes.
  "$ROOT_DIR/scripts/stop-all.sh" >>"$HOME_DIR/logs/interleave-$i-stop.log" 2>&1
  assert_no_managed "interleave round $i"
done

ELAPSED=$((SECONDS - START_TS))
if (( ELAPSED >= 80 )); then
  fail "lifecycle command took ${ELAPSED}s, exceeding 80s"
fi
info "PASS: takeover, stale PID, shutdown rejection, pause/rollback continuation, and 4 interleavings in ${ELAPSED}s"
