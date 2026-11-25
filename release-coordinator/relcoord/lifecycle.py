from __future__ import annotations

import argparse
import contextlib
import fcntl
import json
import os
import signal
import sys
import time
from pathlib import Path
from typing import Any, Iterator

EXIT_BUSY = 75
LIFECYCLE_LOCK = "lifecycle.lock"
STATE_FILE = "lifecycle.json"

MANAGED_KINDS = {"coordinator", "service", "coordinator-shell", "service-shell"}


class LifecycleError(Exception):
    """Expected lifecycle/process boundary error."""


class StopInProgress(LifecycleError):
    """An exclusive stop/start operation owns the runtime directory."""


def canonical_home(home: str | Path) -> Path:
    return Path(home).expanduser().resolve()


def lifecycle_lock_path(home: Path) -> Path:
    return home / LIFECYCLE_LOCK


def lifecycle_state_path(home: Path) -> Path:
    return home / STATE_FILE


def pid_file_path(home: Path, component: str) -> Path:
    return home / "pids" / f"{component}.pid"


def meta_file_path(home: Path, component: str) -> Path:
    return home / "pids" / f"{component}.meta.json"


def validate_component(component: str) -> str:
    if not component or component in {".", ".."} or "/" in component or "\0" in component:
        raise LifecycleError(f"invalid component name: {component!r}")
    return component


def same_home(value: str | None, home: Path) -> bool:
    if value is None:
        return False
    try:
        if value == str(home):
            return True
        return Path(value).expanduser().resolve() == home
    except (OSError, RuntimeError):
        return False


def write_state(home: Path, state: str, **extra: Any) -> None:
    home.mkdir(parents=True, exist_ok=True)
    data = {"state": state, "pid": os.getpid(), "ts": time.time(), **extra}
    tmp = lifecycle_state_path(home).with_name(f"{STATE_FILE}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(data, sort_keys=True), encoding="utf-8")
    os.replace(tmp, lifecycle_state_path(home))


def read_state(home: Path) -> dict[str, Any] | None:
    try:
        return json.loads(lifecycle_state_path(home).read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None


@contextlib.contextmanager
def lifecycle_slot(
    home: str | Path,
    *,
    exclusive: bool = False,
    block: bool = False,
    timeout: float = 15.0,
) -> Iterator[Any]:
    """Acquire the runtime-directory start/stop boundary lock.

    Starting processes take a shared slot; stop-all takes an exclusive lock.
    A non-blocking shared attempt therefore fails immediately while stopping.
    """

    home = canonical_home(home)
    home.mkdir(parents=True, exist_ok=True)
    lock_f = open(lifecycle_lock_path(home), "a+", encoding="utf-8")
    lock_type = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
    try:
        if block:
            deadline = time.monotonic() + timeout
            while True:
                try:
                    fcntl.flock(lock_f.fileno(), lock_type | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        raise StopInProgress(
                            f"timed out waiting for lifecycle lock {lifecycle_lock_path(home)}"
                        )
                    time.sleep(0.03)
        else:
            try:
                fcntl.flock(lock_f.fileno(), lock_type | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise StopInProgress(
                    f"start/stop operation in progress for {home}; refusing concurrent start"
                ) from exc
        yield lock_f
    finally:
        with contextlib.suppress(OSError):
            fcntl.flock(lock_f.fileno(), fcntl.LOCK_UN)
        lock_f.close()


def check_start_allowed(home: str | Path) -> None:
    """Fast non-blocking boundary check for Python entry points."""

    with start_slot(home):
        return None


def is_stop_all_process(pid: int | None, home: Path) -> bool:
    if not pid or not is_live(pid, zombie_is_dead=False):
        return False
    argv = read_argv(pid)
    if not argv:
        return False
    is_lifecycle = any(
        arg == "relcoord.lifecycle"
        or arg.endswith("/relcoord/lifecycle.py")
        or (arg == "-m")
        for arg in argv
    ) and any("lifecycle" in arg for arg in argv)
    if not is_lifecycle or "stop-all" not in argv:
        # The shell wrapper is tiny; accept it as an owner as well.
        if not any(arg.endswith("/scripts/stop-all.sh") or arg == "stop-all.sh" for arg in argv):
            return False
    return same_home(option_value(argv, "--home"), home)


def assert_not_stopping_marker(home: Path) -> None:
    """Reject starts for a live stop-all, even while it waits for an EX lock.

    The exclusive flock is the main linearization point.  The marker closes
    the smaller window where stop-all has been invoked but is waiting for an
    already-running start-all's shared lock.
    """

    state = read_state(home)
    if not state or state.get("state") != "stopping":
        return
    owner = state.get("pid")
    try:
        owner_pid = int(owner) if owner is not None else 0
    except (TypeError, ValueError):
        owner_pid = 0
    if is_stop_all_process(owner_pid, home):
        raise StopInProgress(
            f"stop-all pid={owner_pid} is in progress for {home}; refusing new start/takeover"
        )
    # Owner exited or its PID was reused by an unrelated process: stale marker.
    return


@contextlib.contextmanager
def start_slot(home: str | Path) -> Iterator[Any]:
    home = canonical_home(home)
    with lifecycle_slot(home, exclusive=False, block=False) as lock_f:
        assert_not_stopping_marker(home)
        yield lock_f


def proc_starttime(pid: int) -> int | None:
    try:
        text = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8", errors="replace")
        rest = text[text.rfind(")") + 2 :].split()
        # Field 22 is starttime; rest[0] is field 3, so field 22 is index 19.
        return int(rest[19])
    except (FileNotFoundError, ProcessLookupError, IndexError, ValueError, OSError):
        return None


def proc_state(pid: int) -> str | None:
    try:
        text = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8", errors="replace")
        rest = text[text.rfind(")") + 2 :].split()
        return rest[0] if rest else None
    except (FileNotFoundError, ProcessLookupError, IndexError, OSError):
        return None


def is_live(pid: int | None, *, zombie_is_dead: bool = True) -> bool:
    if pid is None or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    if zombie_is_dead and proc_state(pid) == "Z":
        return False
    return True


def read_argv(pid: int) -> list[str] | None:
    try:
        raw = Path(f"/proc/{pid}/cmdline").read_bytes()
    except (FileNotFoundError, ProcessLookupError, PermissionError, OSError):
        return None
    argv = [part.decode("utf-8", errors="replace") for part in raw.split(b"\0") if part]
    return argv or None


def read_environ(pid: int) -> dict[str, str]:
    try:
        raw = Path(f"/proc/{pid}/environ").read_bytes()
    except (FileNotFoundError, ProcessLookupError, PermissionError, OSError):
        return {}
    env: dict[str, str] = {}
    for item in raw.split(b"\0"):
        if b"=" not in item:
            continue
        key, value = item.split(b"=", 1)
        env[key.decode("utf-8", "replace")] = value.decode("utf-8", "replace")
    return env


def option_value(argv: list[str], option: str) -> str | None:
    prefix = option + "="
    for i, arg in enumerate(argv):
        if arg == option and i + 1 < len(argv):
            return argv[i + 1]
        if arg.startswith(prefix):
            return arg[len(prefix) :]
    return None


def parse_managed_argv(argv: list[str]) -> dict[str, Any] | None:
    if not argv:
        return None

    home = option_value(argv, "--home")
    name = option_value(argv, "--name")
    kind: str | None = None

    for i, arg in enumerate(argv):
        module: str | None = None
        if arg == "-m" and i + 1 < len(argv):
            module = argv[i + 1]
        elif arg.startswith("-m") and len(arg) > 2:
            module = arg[2:]

        if module == "relcoord.coordinator":
            kind = "coordinator"
            break
        if module == "relcoord.service":
            kind = "service"
            break
        if module in {"relcoord", "relcoord.cli"}:
            for positional in argv[i + 2 :]:
                if positional == "coordinator":
                    kind = "coordinator"
                    break
                if positional == "service":
                    kind = "service"
                    break
            if kind is not None:
                break

        if arg.endswith("/relcoord/coordinator.py") or arg == "relcoord/coordinator.py":
            kind = "coordinator"
            break
        if arg.endswith("/relcoord/service.py") or arg == "relcoord/service.py":
            kind = "service"
            break

    # Do not parse `bash -c '<whole test script>'` as a real script invocation.
    is_c_wrapper = len(argv) >= 2 and argv[1] == "-c"
    if kind is None and not is_c_wrapper:
        for i, arg in enumerate(argv):
            base = os.path.basename(arg)
            if base == "start-coordinator.sh":
                kind = "coordinator-shell"
                break
            if base == "start-service.sh":
                kind = "service-shell"
                if i + 1 < len(argv) and not argv[i + 1].startswith("-"):
                    name = argv[i + 1]
                break

    if kind is None:
        return None

    component = "coordinator" if kind in {"coordinator", "coordinator-shell"} else name
    return {
        "kind": kind,
        "home": home,
        "name": name,
        "component": component,
        "argv": argv,
    }


def identify_pid(pid: int, expected_home: Path | None = None) -> dict[str, Any] | None:
    argv = read_argv(pid)
    if argv is None:
        return None
    info = parse_managed_argv(argv)
    if info is None:
        return None

    home = info.get("home")
    if home is None and info["kind"].endswith("-shell"):
        home = read_environ(pid).get("RELCOORD_HOME")
        info["home"] = home

    if expected_home is not None and not same_home(home, expected_home):
        return None

    info["pid"] = pid
    return info


def ancestor_pids() -> set[int]:
    ancestors: set[int] = set()
    pid = os.getpid()
    for _ in range(64):
        try:
            text = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8", errors="replace")
            rest = text[text.rfind(")") + 2 :].split()
            ppid = int(rest[1])  # field 4; rest[0] is field 3, ppid is index 1
        except (FileNotFoundError, ProcessLookupError, IndexError, ValueError, OSError):
            break
        if ppid <= 1 or ppid == pid:
            break
        ancestors.add(ppid)
        pid = ppid
    return ancestors


def scan_managed_processes(home: Path) -> list[dict[str, Any]]:
    ancestors = ancestor_pids()
    results: list[dict[str, Any]] = []
    proc_root = Path("/proc")
    try:
        entries = list(proc_root.iterdir())
    except OSError:
        return results

    for entry in entries:
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        if pid == os.getpid() or pid in ancestors:
            continue
        info = identify_pid(pid, home)
        if info is not None and info.get("kind") in MANAGED_KINDS and is_live(pid):
            results.append(info)
    return results


def write_registration_files(
    home: str | Path,
    component: str,
    pid: int | None = None,
    *,
    also_root_coordinator_pid: bool = True,
) -> Path:
    home = canonical_home(home)
    component = validate_component(component)
    pid = os.getpid() if pid is None else pid
    pids_dir = home / "pids"
    pids_dir.mkdir(parents=True, exist_ok=True)

    pid_path = pid_file_path(home, component)
    tmp_pid = pid_path.with_name(f".{component}.{os.getpid()}.pid.tmp")
    tmp_pid.write_text(f"{pid}\n", encoding="utf-8")
    os.replace(tmp_pid, pid_path)

    meta = {
        "component": component,
        "pid": pid,
        "starttime": proc_starttime(pid),
        "home": str(home),
        "ts": time.time(),
    }
    meta_path = meta_file_path(home, component)
    tmp_meta = meta_path.with_name(f".{component}.{os.getpid()}.meta.tmp")
    tmp_meta.write_text(json.dumps(meta, sort_keys=True), encoding="utf-8")
    os.replace(tmp_meta, meta_path)

    if component == "coordinator" and also_root_coordinator_pid:
        root_pid = home / "coordinator.pid"
        tmp_root = root_pid.with_name(f".coordinator.{os.getpid()}.pid.tmp")
        tmp_root.write_text(f"{pid}\n", encoding="utf-8")
        os.replace(tmp_root, root_pid)

    return pid_path


def registration_matches(home: Path, component: str, pid: int) -> bool:
    try:
        meta = json.loads(meta_file_path(home, component).read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return False
    if int(meta.get("pid", -1)) != pid:
        return False
    if not same_home(meta.get("home"), home):
        return False
    expected_start = meta.get("starttime")
    actual_start = proc_starttime(pid)
    if expected_start is None or actual_start is None:
        return False
    return int(expected_start) == actual_start


def meta_targets(home: Path, component: str | None = None) -> list[dict[str, Any]]:
    """Return live processes recorded by start-time metadata.

    This still finds the real component if its plain pid file was later
    overwritten with a stale/reused PID.
    """

    pids_dir = home / "pids"
    if not pids_dir.is_dir():
        return []
    results: list[dict[str, Any]] = []
    for meta_path in sorted(pids_dir.glob("*.meta.json")):
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            comp = validate_component(str(meta.get("component") or meta_path.name[: -len(".meta.json")]))
            pid = int(meta.get("pid", -1))
        except (FileNotFoundError, json.JSONDecodeError, OSError, LifecycleError, TypeError, ValueError):
            continue
        if component is not None and comp != component:
            continue
        if not same_home(meta.get("home"), home):
            continue
        expected_start = meta.get("starttime")
        actual_start = proc_starttime(pid)
        if expected_start is None or actual_start is None or int(expected_start) != actual_start:
            continue
        if not is_live(pid):
            continue
        results.append(
            {
                "pid": pid,
                "component": comp,
                "kind": "registered-meta",
                "name": comp if comp != "coordinator" else None,
                "home": str(home),
            }
        )
    return results


def read_pid_file(path: Path) -> int | None:
    try:
        raw = path.read_text(encoding="utf-8").strip()
        pid = int(raw)
        return pid if pid > 0 else None
    except (FileNotFoundError, ValueError, OSError):
        return None


def pid_file_target(home: Path, path: Path, component: str | None = None) -> dict[str, Any] | None:
    if component is None:
        component = path.stem
    component = validate_component(component)
    pid = read_pid_file(path)
    if pid is None or not is_live(pid):
        return None

    info = identify_pid(pid, home)
    if info is not None and info.get("component") == component:
        return info

    if registration_matches(home, component, pid):
        return {
            "pid": pid,
            "component": component,
            "kind": "registered",
            "name": component if component != "coordinator" else None,
            "home": str(home),
        }

    # The PID is alive but is neither our registered process nor a command
    # line for this runtime directory. It may be an unrelated process that
    # reused the PID; callers must leave it alone.
    return None


def expected_pid_files(home: Path) -> list[tuple[Path, str]]:
    files: list[tuple[Path, str]] = []
    root_coord = home / "coordinator.pid"
    if root_coord.exists():
        files.append((root_coord, "coordinator"))
    pids_dir = home / "pids"
    if pids_dir.is_dir():
        for path in sorted(pids_dir.glob("*.pid")):
            files.append((path, path.stem))
    return files


def collect_targets(home: Path, component: str | None = None) -> dict[int, dict[str, Any]]:
    home = canonical_home(home)
    targets: dict[int, dict[str, Any]] = {}

    for info in meta_targets(home, component):
        targets[info["pid"]] = info

    for info in scan_managed_processes(home):
        if component is None or info.get("component") == component:
            targets[info["pid"]] = info

    for path, file_component in expected_pid_files(home):
        if component is not None and file_component != component:
            continue
        info = pid_file_target(home, path, file_component)
        if info is not None and is_live(info["pid"]):
            targets.setdefault(info["pid"], info)

    return targets


def target_label(info: dict[str, Any]) -> str:
    component = info.get("component")
    if component:
        return str(component)
    if info.get("name"):
        return str(info["name"])
    return f"pid-{info.get('pid')}"


def kill_targets_until_gone(
    home: Path,
    targets: dict[int, dict[str, Any]],
    *,
    component: str | None = None,
    timeout: float = 6.0,
) -> dict[int, dict[str, Any]]:
    deadline = time.monotonic() + timeout
    signaled: set[int] = set()
    quiet_since: float | None = None

    while time.monotonic() < deadline:
        current = collect_targets(home, component)
        live = {pid: info for pid, info in current.items() if is_live(pid)}

        if not live:
            if quiet_since is None:
                quiet_since = time.monotonic()
            elif time.monotonic() - quiet_since >= 0.15:
                return {}
            time.sleep(0.03)
            continue

        quiet_since = None
        for pid, info in live.items():
            if pid not in signaled:
                label = target_label(info)
                print(f"killing {label} pid={pid}", flush=True)
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                except PermissionError as exc:
                    raise LifecycleError(f"permission denied killing {label} pid={pid}: {exc}") from exc
                signaled.add(pid)
        time.sleep(0.05)

    remaining = {
        pid: info
        for pid, info in collect_targets(home, component).items()
        if is_live(pid)
    }
    if remaining:
        labels = ", ".join(f"{target_label(info)}({pid})" for pid, info in remaining.items())
        raise LifecycleError(f"timed out waiting for process(es) to exit: {labels}")
    return {}


def cleanup_pid_files(home: Path, component: str | None = None) -> None:
    if component is not None:
        validate_component(component)
        for path in (pid_file_path(home, component), meta_file_path(home, component)):
            with contextlib.suppress(FileNotFoundError):
                path.unlink()
        if component == "coordinator":
            with contextlib.suppress(FileNotFoundError):
                (home / "coordinator.pid").unlink()
        return

    for path in (home / "pids").glob("*.pid") if (home / "pids").is_dir() else []:
        with contextlib.suppress(FileNotFoundError):
            path.unlink()
    for path in (home / "pids").glob("*.meta.json") if (home / "pids").is_dir() else []:
        with contextlib.suppress(FileNotFoundError):
            path.unlink()
    with contextlib.suppress(FileNotFoundError):
        (home / "coordinator.pid").unlink()


def stop_all(home: str | Path, *, timeout: float = 8.0, lock_timeout: float = 20.0) -> int:
    home = canonical_home(home)
    if not home.exists():
        print(f"runtime home does not exist, nothing to stop: {home}")
        return 0

    # Publish the shutdown before waiting for an in-flight start-all's shared
    # lock.  start_slot checks this marker in addition to taking a shared lock.
    write_state(home, "stopping", phase="waiting_for_exclusive_lock")
    try:
        with lifecycle_slot(home, exclusive=True, block=True, timeout=lock_timeout):
            write_state(home, "stopping", phase="stopping")
            try:
                targets = collect_targets(home)
                kill_targets_until_gone(home, targets, timeout=timeout)
                cleanup_pid_files(home)
                write_state(home, "stopped")
            except Exception:
                write_state(home, "stop_failed")
                raise
    except StopInProgress:
        write_state(home, "stop_failed", reason="lock_timeout")
        raise
    print(f"all managed processes stopped for {home}")
    return 0


def stop_component(home: str | Path, component: str, *, timeout: float = 5.0) -> int:
    home = canonical_home(home)
    component = validate_component(component)
    with lifecycle_slot(home, exclusive=True, block=False):
        targets = collect_targets(home, component)
        kill_targets_until_gone(home, targets, component=component, timeout=timeout)
        cleanup_pid_files(home, component)
    return 0


def safe_pid(home: str | Path, component: str) -> int | None:
    home = canonical_home(home)
    component = validate_component(component)
    targets = collect_targets(home, component)
    live = {pid: info for pid, info in targets.items() if is_live(pid)}
    if not live:
        return None
    # Prefer the PID recorded in the component's own pid file.
    recorded = read_pid_file(pid_file_path(home, component))
    if recorded in live:
        return recorded
    root_recorded = read_pid_file(home / "coordinator.pid")
    if component == "coordinator" and root_recorded in live:
        return root_recorded
    return sorted(live)[0]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Runtime-directory start/stop boundary helpers")
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--home", default=os.environ.get("RELCOORD_HOME"))
    sub = parser.add_subparsers(dest="command", required=True)

    p_gate = sub.add_parser("gate", parents=[common], help="fail if an exclusive stop operation is in progress")
    p_gate.add_argument("--component", default="start")
    p_gate.add_argument("--pid", type=int, default=None)

    p_register = sub.add_parser(
        "register", parents=[common], help="record a validated component PID and start time"
    )
    p_register.add_argument("--component", required=True)
    p_register.add_argument("--pid", type=int, default=os.getpid())

    p_pid = sub.add_parser("safe-pid", parents=[common], help="print a live managed PID for a component")
    p_pid.add_argument("--component", required=True)

    p_stop = sub.add_parser("stop-all", parents=[common], help="safely stop every managed process for this home")
    p_stop.add_argument("home_positional", nargs="?", help=argparse.SUPPRESS)
    p_stop.add_argument("--timeout", type=float, default=8.0)
    p_stop.add_argument("--lock-timeout", type=float, default=20.0)

    p_stop_one = sub.add_parser(
        "stop-component", parents=[common], help="safely stop one named managed component"
    )
    p_stop_one.add_argument("--component", required=True)
    p_stop_one.add_argument("--timeout", type=float, default=5.0)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    home_value = getattr(args, "home_positional", None) or args.home
    if not home_value:
        print("error: --home or RELCOORD_HOME is required", file=sys.stderr)
        return 2
    home = canonical_home(home_value)

    try:
        if args.command == "gate":
            check_start_allowed(home)
            if args.pid is not None:
                write_state(home, "starting", component=args.component, pid=args.pid)
            return 0
        if args.command == "register":
            with start_slot(home):
                write_registration_files(home, args.component, args.pid)
            return 0
        if args.command == "safe-pid":
            pid = safe_pid(home, args.component)
            if pid is None:
                return 1
            print(pid)
            return 0
        if args.command == "stop-all":
            return stop_all(home, timeout=args.timeout, lock_timeout=args.lock_timeout)
        if args.command == "stop-component":
            return stop_component(home, args.component, timeout=args.timeout)
    except StopInProgress as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_BUSY
    except LifecycleError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
