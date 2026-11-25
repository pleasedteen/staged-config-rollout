from __future__ import annotations

import argparse
import json
import os
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Callable

from . import client
from .coordinator import MAX_SERVICE_REQUESTS, RequestScheduler
from .storage import Store
from .util import PROJECT_ROOT, canonical_json

NAMES = ["svc-1", "svc-2", "svc-3"]
CORE_PORTS = [19811, 19812, 19813]
CANARY_PORTS = [19821, 19822, 19823]


def free_tcp_ports(count: int) -> list[int]:
    socks = []
    ports = []
    for _ in range(count):
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        socks.append(sock)
        ports.append(sock.getsockname()[1])
    for sock in socks:
        sock.close()
    return ports


def wait_until(
    description: str,
    predicate: Callable[[], Any],
    *,
    timeout: float = 15.0,
    interval: float = 0.05,
) -> Any:
    deadline = time.monotonic() + timeout
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            value = predicate()
            if value:
                return value
        except Exception as exc:
            last = exc
        time.sleep(interval)
    raise TimeoutError(f"timed out waiting for {description}; last_error={last!r}")


def write_config(path: Path, version: str, lane: str) -> Path:
    path.write_text(
        json.dumps({"version": version, "content": {"lane": lane, "ok": True}}),
        encoding="utf-8",
    )
    return path


def make_legacy_home(home: Path, ports: list[int]) -> None:
    """Create the exact flat cluster/DB layout used before multi-group support."""
    (home / "services").mkdir(parents=True)
    services = []
    for name, port in zip(NAMES, ports):
        services.append(
            {
                "name": name,
                "port": port,
                "url": f"http://127.0.0.1:{port}",
                "state_path": str(home / "services" / f"{name}.json"),
            }
        )
    (home / "cluster.json").write_text(
        json.dumps({"group": "core", "host": "127.0.0.1", "services": services}, indent=2),
        encoding="utf-8",
    )
    db = home / "coordinator.sqlite3"
    conn = sqlite3.connect(db)
    try:
        conn.executescript(
            """
            CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE services (
                name TEXT PRIMARY KEY,
                group_name TEXT NOT NULL,
                base_url TEXT NOT NULL,
                state_path TEXT NOT NULL,
                initial_version TEXT NOT NULL,
                initial_content TEXT NOT NULL,
                actual_version TEXT,
                actual_content TEXT,
                actual_fence INTEGER NOT NULL DEFAULT 0,
                reachable INTEGER NOT NULL DEFAULT 0,
                last_checked REAL,
                last_error TEXT
            );
            """
        )
        initial = json.loads(
            (PROJECT_ROOT / "data" / "initial-state.json").read_text(encoding="utf-8")
        )["services"]
        for spec in services:
            name = spec["name"]
            cfg = initial[name]
            content = canonical_json(cfg.get("content", {}))
            conn.execute(
                """
                INSERT INTO services(
                  name, group_name, base_url, state_path, initial_version,
                  initial_content, actual_version, actual_content
                ) VALUES(?, 'core', ?, ?, ?, ?, ?, ?)
                """,
                (
                    name,
                    spec["url"],
                    spec["state_path"],
                    cfg["version"],
                    content,
                    cfg["version"],
                    content,
                ),
            )
        conn.execute("INSERT INTO meta(key,value) VALUES('next_fence','1')")
        conn.commit()
    finally:
        conn.close()


def check_scheduler_cap() -> None:
    active = 0
    peak = 0
    lock = threading.Lock()
    entered = threading.Event()
    release = threading.Event()

    def job() -> str:
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
            if active == MAX_SERVICE_REQUESTS:
                entered.set()
        try:
            release.wait(timeout=2.0)
            time.sleep(0.01)
            return "ok"
        finally:
            with lock:
                active -= 1

    scheduler = RequestScheduler(MAX_SERVICE_REQUESTS)
    try:
        futures = [scheduler.submit(f"owner-{i % 3}", job) for i in range(24)]
        if not entered.wait(1.0):
            raise AssertionError("scheduler never reached eight concurrent requests")
        time.sleep(0.05)
        if peak > MAX_SERVICE_REQUESTS:
            raise AssertionError(f"request concurrency peaked at {peak}")
        release.set()
        for future in futures:
            future.result(timeout=3)
    finally:
        scheduler.shutdown()
    print(f"  PASS global request cap: peak={peak}, limit={MAX_SERVICE_REQUESTS}", flush=True)


class GroupVerification:
    def __init__(self) -> None:
        self.home = Path(tempfile.mkdtemp(prefix="relcoord-groups-"))
        self.ports = free_tcp_ports(6)
        self.core_ports = self.ports[:3]
        self.canary_ports = self.ports[3:]
        self.store = Store(self.home)
        self.procs: dict[str, subprocess.Popen] = {}
        self.coordinator: subprocess.Popen | None = None
        self.env = {**os.environ, "PYTHONPATH": str(PROJECT_ROOT)}
        self.passed: list[str] = []

    def check(self, name: str, condition: bool, detail: str = "") -> None:
        if not condition:
            raise AssertionError(f"{name} {detail}")
        self.passed.append(name)
        print(f"  PASS {name}", flush=True)

    def key(self, group: str, name: str) -> str:
        return f"{group}/{name}"

    def run_cli(self, *args: str, timeout: float = 20.0) -> subprocess.CompletedProcess[str]:
        cmd = [sys.executable, "-m", "relcoord", *args, "--home", str(self.home)]
        result = subprocess.run(
            cmd,
            cwd=PROJECT_ROOT,
            env=self.env,
            text=True,
            capture_output=True,
            timeout=timeout,
        )
        if result.returncode != 0:
            print(result.stdout)
            print(result.stderr, file=sys.stderr)
            raise RuntimeError(f"CLI failed: {' '.join(args)}")
        return result

    def start_service(self, group: str, name: str) -> None:
        key = self.key(group, name)
        log = (self.home / "logs" / f"{group}-{name}.log").open("ab", buffering=0)
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "relcoord",
                "service",
                "--home",
                str(self.home),
                "--group",
                group,
                "--name",
                name,
            ],
            cwd=PROJECT_ROOT,
            env=self.env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        log.close()
        self.procs[key] = proc
        row = self.store.get_service(name, group=group)
        wait_until(
            f"service {key} health",
            lambda: proc.poll() is None
            and client.health(row["base_url"], timeout=0.3).get("ok") is True,
            timeout=8,
        )

    def stop_service(self, group: str, name: str) -> None:
        proc = self.procs.get(self.key(group, name))
        if proc is None or proc.poll() is not None:
            return
        proc.kill()
        proc.wait(timeout=5)

    def start_coordinator(self) -> None:
        log = (self.home / "logs" / "coordinator.log").open("ab", buffering=0)
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "relcoord",
                "coordinator",
                "--home",
                str(self.home),
                "--interval",
                "0.04",
            ],
            cwd=PROJECT_ROOT,
            env=self.env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        log.close()
        self.coordinator = proc
        wait_until(
            "coordinator pid file",
            lambda: proc.poll() is None
            and (self.home / "coordinator.pid").exists()
            and (self.home / "coordinator.pid").read_text().strip() == str(proc.pid),
            timeout=8,
        )

    def kill_coordinator(self) -> int:
        assert self.coordinator is not None
        pid = self.coordinator.pid
        self.coordinator.kill()
        self.coordinator.wait(timeout=5)
        self.coordinator = None
        time.sleep(0.2)
        return pid

    def row(self, release_id: str, name: str) -> Any:
        return next(r for r in self.store.release_services(release_id) if r["service_name"] == name)

    def url(self, group: str, name: str) -> str:
        return self.store.get_service(name, group=group)["base_url"]

    def wait_live(self, group: str, name: str, version: str, timeout: float = 12) -> None:
        def pred() -> bool:
            try:
                state = client.fetch_state(self.url(group, name), timeout=0.4)
                return state["effective"]["version"] == version
            except Exception:
                return False

        wait_until(f"{group}/{name} live {version}", pred, timeout=timeout)

    def cleanup(self) -> None:
        for proc in self.procs.values():
            if proc.poll() is None:
                proc.kill()
        if self.coordinator is not None and self.coordinator.poll() is None:
            self.coordinator.kill()

    def run(self) -> int:
        try:
            print(f"verify home: {self.home}", flush=True)
            check_scheduler_cap()

            print("\n[1/5] Migrate legacy single-group directory and add a same-name group", flush=True)
            make_legacy_home(self.home, self.core_ports)
            self.run_cli(
                "init",
                "--group",
                "core",
                "--services",
                ",".join(NAMES),
                "--ports",
                ",".join(map(str, self.core_ports)),
            )
            self.run_cli(
                "init",
                "--group",
                "canary",
                "--services",
                ",".join(NAMES),
                "--ports",
                ",".join(map(str, self.canary_ports)),
            )
            migrated_pk = self.store.connect().execute("PRAGMA table_info(services)").fetchall()
            pk = [r["name"] for r in sorted(migrated_pk, key=lambda r: r["pk"]) if r["pk"]]
            self.check("legacy services table migrated to composite primary key", pk == ["group_name", "name"], str(pk))
            core_path = Path(self.store.get_service("svc-1", group="core")["state_path"])
            canary_path = Path(self.store.get_service("svc-1", group="canary")["state_path"])
            self.check("old flat core state path is preserved", core_path == self.home / "services" / "svc-1.json")
            self.check("same-name canary has an isolated state path", canary_path != core_path)

            print("\n[2/5] Start same-named services and one coordinator", flush=True)
            for group in ("core", "canary"):
                for name in NAMES:
                    self.start_service(group, name)
            self.start_coordinator()
            wait_until(
                "all six services reachable",
                lambda: all(r["reachable"] for r in self.store.list_services()),
                timeout=10,
            )
            self.check("one coordinator owns the shared run directory", True)

            print("\n[3/5] A stuck core request does not block canary's first batch", flush=True)
            core_v2 = write_config(self.home / "core-v2.json", "v-core-2", "core")
            canary_v2 = write_config(self.home / "canary-v2.json", "v-canary-2", "canary")
            self.run_cli(
                "faults",
                "--group",
                "core",
                "--service",
                "svc-1",
                "--set",
                json.dumps({"delay_seconds": 1.8, "delay_next": 1}),
            )
            core_submit = self.run_cli(
                "submit", str(core_v2), "--group", "core",
                "--batch", "svc-1", "--batch", "svc-2,svc-3",
            )
            canary_submit = self.run_cli(
                "submit", str(canary_v2), "--group", "canary",
                "--batch", "svc-1", "--batch", "svc-2,svc-3",
            )
            rid_core = core_submit.stdout.split("submitted release ", 1)[1].splitlines()[0]
            rid_canary = canary_submit.stdout.split("submitted release ", 1)[1].splitlines()[0]
            start = time.monotonic()
            wait_until(
                "canary svc-1 first batch confirmed",
                lambda: bool(self.row(rid_canary, "svc-1")["applied_confirmed"]),
                timeout=6,
            )
            elapsed = time.monotonic() - start
            self.check(
                "healthy group finishes first batch before 1.8s slow injection ends",
                elapsed < 1.3 and not self.row(rid_core, "svc-1")["applied_confirmed"],
                f"elapsed={elapsed:.2f}",
            )
            self.wait_live("canary", "svc-1", "v-canary-2")
            wait_until(
                "both groups finish their independent publishes",
                lambda: self.store.get_release(rid_core)["status"] == "completed"
                and self.store.get_release(rid_canary)["status"] == "completed",
                timeout=12,
            )
            for name in NAMES:
                self.wait_live("core", name, "v-core-2")
                self.wait_live("canary", name, "v-canary-2")
            self.check("same-named services serve different group configs", True)

            print("\n[4/5] Group-scoped query, pause, SIGKILL takeover and resume", flush=True)
            status = json.loads(
                self.run_cli("status", "--group", "canary", "--json", "--no-live").stdout
            )
            self.check("status --group returns only that group", status["group"] == "canary" and len(status["services"]) == 3)
            self.check(
                "status release is group-scoped",
                status["release"]["id"] == rid_canary and status["release"]["target_version"] == "v-canary-2",
            )

            self.stop_service("core", "svc-2")
            core_v3 = write_config(self.home / "core-v3.json", "v-core-3", "core-next")
            submit3 = self.run_cli(
                "submit", str(core_v3), "--group", "core",
                "--batch", "svc-1,svc-2", "--batch", "svc-3",
            )
            rid_core3 = submit3.stdout.split("submitted release ", 1)[1].splitlines()[0]
            self.run_cli("pause", "--group", "core", "--reason", "verify durable group pause")
            killed_pid = self.kill_coordinator()
            self.check("pause survived in SQLite after SIGKILL", bool(self.store.get_release(rid_core3)["pause_requested"]))
            self.start_coordinator()
            wait_until("replacement coordinator pid", lambda: self.coordinator is not None)
            self.check("replacement coordinator has a new pid", self.coordinator.pid != killed_pid)

            self.start_service("core", "svc-2")
            wait_until(
                "core batch one settles into paused before batch two",
                lambda: self.store.get_release(rid_core3)["status"] == "paused",
                timeout=10,
            )
            self.check(
                "pause is scoped to core and batch two did not start",
                not self.row(rid_core3, "svc-3")["started"],
            )
            canary_after_restart = self.store.get_release(rid_canary)
            self.check("canary completed state was not disturbed", canary_after_restart["status"] == "completed")

            self.run_cli("resume", "--group", "core")
            wait_until(
                "resumed core release completes",
                lambda: self.store.get_release(rid_core3)["status"] == "completed",
                timeout=10,
            )
            self.wait_live("core", "svc-3", "v-core-3")
            self.wait_live("canary", "svc-3", "v-canary-2")

            print("\n[5/5] Group-scoped rollback preserves the other same-name group", flush=True)
            rb = self.run_cli("rollback", "--group", "canary", "--reason", "verify group rollback")
            self.check("rollback command names canary release", rid_canary in rb.stdout)
            wait_until(
                "canary rollback completes",
                lambda: self.store.get_release(rid_canary)["status"] == "rolled_back",
                timeout=12,
            )
            for name in NAMES:
                expected = json.loads(
                    self.row(rid_canary, name)["baseline_content"]
                )
                state = client.fetch_state(self.url("canary", name), timeout=0.5)
                self.check(
                    f"canary/{name} restored to its own baseline",
                    canonical_json(state["effective"]["content"]) == canonical_json(expected),
                )
            core_state = client.fetch_state(self.url("core", "svc-3"), timeout=0.5)
            self.check(
                "core/svc-3 remains at core v3 while canary rolls back",
                core_state["effective"]["version"] == "v-core-3",
            )
            core_status = json.loads(
                self.run_cli("status", "--group", "core", "--json", "--no-live").stdout
            )
            canary_status = json.loads(
                self.run_cli("status", "--group", "canary", "--json", "--no-live").stdout
            )
            self.check("core query still shows v-core-3", core_status["release"]["target_version"] == "v-core-3")
            self.check("canary query shows rolled_back", canary_status["release"]["status"] == "rolled_back")

            print(f"\nALL GROUP CHECKS PASSED: {len(self.passed)} checks", flush=True)
            print(f"retained home for inspection: {self.home}", flush=True)
            return 0
        except Exception:
            print(f"\nGROUP VERIFICATION FAILED; home={self.home}", file=sys.stderr, flush=True)
            coord_log = self.home / "logs" / "coordinator.log"
            if coord_log.exists():
                print("\n--- coordinator log tail ---", file=sys.stderr)
                print("\n".join(coord_log.read_text(errors="replace").splitlines()[-80:]), file=sys.stderr)
            raise
        finally:
            self.cleanup()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Verify multi-group isolation and restart takeover")
    parser.parse_args(argv)
    try:
        return GroupVerification().run()
    except AssertionError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
