from __future__ import annotations

import argparse
import json
import os
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import traceback
from pathlib import Path
from typing import Any, Callable

from . import client
from .status import build_status, render_status
from .storage import Store, load_initial_state
from .util import PROJECT_ROOT, canonical_json
from .verify import render_verify_report

# Exact pre-attribution-check schema, used to construct an authentic "old
# runtime directory" whose rows must be migrated and re-verified in place.
LEGACY_SCHEMA = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE services (
    name TEXT PRIMARY KEY, group_name TEXT NOT NULL, base_url TEXT NOT NULL,
    state_path TEXT NOT NULL, initial_version TEXT NOT NULL,
    initial_content TEXT NOT NULL, actual_version TEXT, actual_content TEXT,
    actual_fence INTEGER NOT NULL DEFAULT 0, reachable INTEGER NOT NULL DEFAULT 0,
    last_checked REAL, last_error TEXT
);
CREATE TABLE releases (
    id TEXT PRIMARY KEY, group_name TEXT NOT NULL, target_version TEXT NOT NULL,
    target_content TEXT NOT NULL, batches TEXT NOT NULL, status TEXT NOT NULL,
    batch_index INTEGER NOT NULL DEFAULT 0,
    pause_requested INTEGER NOT NULL DEFAULT 0,
    rollback_requested INTEGER NOT NULL DEFAULT 0,
    rollback_status TEXT NOT NULL DEFAULT 'none',
    apply_fence INTEGER NOT NULL, rollback_fence INTEGER,
    superseded INTEGER NOT NULL DEFAULT 0, note TEXT NOT NULL DEFAULT '',
    created_at REAL NOT NULL, updated_at REAL NOT NULL, completed_at REAL
);
CREATE UNIQUE INDEX one_active_release_per_group ON releases(group_name)
WHERE status IN ('running', 'paused', 'rolling_back');
CREATE TABLE release_services (
    release_id TEXT NOT NULL REFERENCES releases(id) ON DELETE CASCADE,
    service_name TEXT NOT NULL, batch_index INTEGER NOT NULL,
    baseline_version TEXT NOT NULL, baseline_content TEXT NOT NULL,
    started INTEGER NOT NULL DEFAULT 0,
    applied_confirmed INTEGER NOT NULL DEFAULT 0,
    apply_command_id TEXT NOT NULL, apply_attempts INTEGER NOT NULL DEFAULT 0,
    rollback_confirmed INTEGER NOT NULL DEFAULT 0,
    rollback_command_id TEXT NOT NULL, rollback_attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (release_id, service_name)
);
CREATE TABLE events (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL NOT NULL,
    release_id TEXT, kind TEXT NOT NULL, message TEXT NOT NULL
);
"""


def free_tcp_ports(count: int) -> list[int]:
    socks = []
    ports = []
    for _ in range(count):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("127.0.0.1", 0))
        socks.append(s)
        ports.append(s.getsockname()[1])
    for s in socks:
        s.close()
    return ports


def tail(path: Path, max_chars: int = 6000) -> str:
    try:
        data = path.read_bytes()[-max_chars:]
        return data.decode("utf-8", errors="replace")
    except FileNotFoundError:
        return f"(no log {path})"


class FaultDemo:
    def __init__(self, *, keep_processes: bool = False):
        self.keep_processes = keep_processes
        self.home = Path(tempfile.mkdtemp(prefix="relcoord-demo-"))
        self.ports = free_tcp_ports(6)
        self.store = Store(self.home)
        self.initial = load_initial_state()["services"]
        fault_path = PROJECT_ROOT / "faults" / "demo-faults.json"
        self.fault_data = json.loads(fault_path.read_text(encoding="utf-8"))
        self.service_procs: dict[str, subprocess.Popen] = {}
        self.coordinator_proc: subprocess.Popen | None = None
        self.legacy_home: Path | None = None
        self.legacy_store: Store | None = None
        self.legacy_procs: dict[str, subprocess.Popen] = {}
        self.legacy_coord_proc: subprocess.Popen | None = None
        self.legacy_rid = "rel-legacy-cross-0001"
        self.passed_checks: list[str] = []
        self.env = {**os.environ, "PYTHONPATH": str(PROJECT_ROOT)}

    def banner(self, text: str) -> None:
        print("\n" + "=" * 88, flush=True)
        print(text, flush=True)
        print("=" * 88, flush=True)

    def step(self, text: str) -> None:
        print(f"\n• {text}", flush=True)

    def info(self, text: str) -> None:
        print(f"  {text}", flush=True)

    def check(self, name: str, condition: bool, detail: str = "") -> None:
        if not condition:
            raise AssertionError(f"{name} {detail}")
        self.passed_checks.append(name)
        print(f"  ✅ {name}", flush=True)

    def run_cli(self, *args: str) -> subprocess.CompletedProcess:
        return self.run_cli_in(self.home, *args)

    def run_cli_in(self, home: str | Path, *args: str) -> subprocess.CompletedProcess:
        cmd = [sys.executable, "-m", "relcoord", *args, "--home", str(home)]
        result = subprocess.run(
            cmd,
            cwd=PROJECT_ROOT,
            env=self.env,
            text=True,
            capture_output=True,
            timeout=20,
        )
        if result.returncode != 0:
            print(result.stdout)
            print(result.stderr, file=sys.stderr)
            raise RuntimeError(f"CLI failed: {' '.join(args)}")
        return result

    def run_verify_json(self, home: str | Path, *extra: str) -> dict[str, Any]:
        result = self.run_cli_in(home, "verify", "--json", *extra)
        return json.loads(result.stdout)

    def start_service(self, name: str) -> None:
        log_path = self.home / "logs" / f"{name}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_f = open(log_path, "ab", buffering=0)
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "relcoord",
                "service",
                "--home",
                str(self.home),
                "--name",
                name,
            ],
            cwd=PROJECT_ROOT,
            env=self.env,
            stdout=log_f,
            stderr=subprocess.STDOUT,
        )
        log_f.close()
        self.service_procs[name] = proc
        self.wait_until(
            f"service {name} health",
            lambda: proc.poll() is None and client.health(self.url(name), timeout=0.4).get("ok") is True,
            timeout=8,
        )

    def stop_service(self, name: str) -> None:
        proc = self.service_procs.get(name)
        if proc is None or proc.poll() is not None:
            return
        print(f"  🛑 SIGKILL service {name} (pid {proc.pid})", flush=True)
        proc.kill()
        proc.wait(timeout=5)

    def url(self, name: str) -> str:
        cluster = self.store.load_cluster()
        return next(s["url"] for s in cluster["services"] if s["name"] == name)

    def start_coordinator(self) -> None:
        log_path = self.home / "logs" / "coordinator.log"
        log_f = open(log_path, "ab", buffering=0)
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "relcoord",
                "coordinator",
                "--home",
                str(self.home),
                "--interval",
                "0.10",
            ],
            cwd=PROJECT_ROOT,
            env=self.env,
            stdout=log_f,
            stderr=subprocess.STDOUT,
        )
        log_f.close()
        self.coordinator_proc = proc

        def pid_file_matches() -> bool:
            pid_file = self.home / "coordinator.pid"
            return proc.poll() is None and pid_file.exists() and pid_file.read_text().strip() == str(proc.pid)

        self.wait_until("coordinator singleton lock/pid file", pid_file_matches, timeout=8)
        print(f"  🟢 coordinator started (pid {proc.pid})", flush=True)

    def kill_coordinator(self) -> int:
        if self.coordinator_proc is None:
            raise RuntimeError("coordinator is not running")
        proc = self.coordinator_proc
        pid = proc.pid
        print(f"  🛑 SIGKILL coordinator (pid {pid})", flush=True)
        proc.kill()
        proc.wait(timeout=5)
        self.coordinator_proc = None
        # Give the kernel a moment to release flock and the port-less loop to die.
        time.sleep(0.15)
        return pid

    def wait_until(
        self,
        description: str,
        predicate: Callable[[], Any],
        timeout: float = 12,
        interval: float = 0.10,
        *,
        watch_proc: subprocess.Popen | None = None,
        log_path: Path | None = None,
    ) -> Any:
        deadline = time.monotonic() + timeout
        last_error: Exception | None = None
        watched = watch_proc if watch_proc is not None else self.coordinator_proc
        watched_log = log_path or (self.home / "logs" / "coordinator.log")
        while time.monotonic() < deadline:
            if watched is not None and watched.poll() is not None:
                raise RuntimeError(
                    f"watched process exited unexpectedly while waiting for {description}:\n"
                    + tail(watched_log)
                )
            try:
                value = predicate()
                if value:
                    return value
            except Exception as e:  # polling predicates may hit not-ready services/DB
                last_error = e
            time.sleep(interval)
        extra = f"; last error: {last_error}" if last_error else ""
        raise TimeoutError(f"timed out waiting for {description}{extra}")

    def latest_release(self) -> Any:
        row = self.store.latest_release("core")
        if row is None:
            raise RuntimeError("no release found")
        return row

    def release_row_service(self, release_id: str, name: str) -> Any:
        rows = self.store.release_services(release_id)
        return next(r for r in rows if r["service_name"] == name)

    def live_state(self, name: str) -> dict[str, Any]:
        return client.fetch_state(self.url(name), timeout=0.8)

    def matches_initial(self, name: str, state: dict[str, Any]) -> bool:
        expected = self.initial[name]
        eff = state.get("effective", {})
        return eff.get("version") == expected["version"] and canonical_json(
            eff.get("content")
        ) == canonical_json(expected["content"])

    def wait_live_version(self, name: str, version: str, timeout: float = 12) -> dict[str, Any]:
        def pred() -> dict[str, Any] | None:
            try:
                state = self.live_state(name)
                if state.get("effective", {}).get("version") == version:
                    return state
            except Exception:
                return None
            return None

        return self.wait_until(f"{name} live version {version}", pred, timeout=timeout)

    def cleanup(self) -> None:
        if self.keep_processes:
            print(f"\n--keep-processes set; leaving processes running. Home: {self.home}")
            if self.legacy_home is not None:
                print(f"Legacy crossed-endpoint home: {self.legacy_home}")
            return
        for name, proc in list(self.service_procs.items()):
            if proc.poll() is None:
                try:
                    proc.kill()
                    proc.wait(timeout=3)
                except Exception:
                    pass
        for name, proc in list(self.legacy_procs.items()):
            if proc.poll() is None:
                try:
                    proc.kill()
                    proc.wait(timeout=3)
                except Exception:
                    pass
        if self.coordinator_proc is not None and self.coordinator_proc.poll() is None:
            try:
                self.coordinator_proc.kill()
                self.coordinator_proc.wait(timeout=3)
            except Exception:
                pass
        if self.legacy_coord_proc is not None and self.legacy_coord_proc.poll() is None:
            try:
                self.legacy_coord_proc.kill()
                self.legacy_coord_proc.wait(timeout=3)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Phase 3 helpers: crossed endpoints + legacy runtime verification
    # ------------------------------------------------------------------

    def legacy_url(self, name: str) -> str:
        assert self.legacy_store is not None
        row = self.legacy_store.get_service(name)
        return str(row["base_url"])

    def start_legacy_service(self, name: str) -> None:
        assert self.legacy_home is not None
        home = self.legacy_home
        log_path = home / "logs" / f"{name}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_f = open(log_path, "ab", buffering=0)
        proc = subprocess.Popen(
            [
                sys.executable, "-m", "relcoord", "service",
                "--home", str(home), "--name", name,
            ],
            cwd=PROJECT_ROOT,
            env=self.env,
            stdout=log_f,
            stderr=subprocess.STDOUT,
        )
        log_f.close()
        self.legacy_procs[name] = proc

        def healthy_named() -> bool:
            if proc.poll() is not None:
                raise RuntimeError(tail(log_path))
            health = client.health(self.legacy_url(name), timeout=0.4)
            return health.get("ok") is True and health.get("name") == name

        self.wait_until(
            f"legacy service {name} health and identity",
            healthy_named,
            timeout=8,
            watch_proc=proc,
            log_path=log_path,
        )

    def start_legacy_coordinator(self) -> int:
        assert self.legacy_home is not None
        home = self.legacy_home
        log_path = home / "logs" / "coordinator.log"
        log_f = open(log_path, "ab", buffering=0)
        proc = subprocess.Popen(
            [
                sys.executable, "-m", "relcoord", "coordinator",
                "--home", str(home), "--interval", "0.10",
            ],
            cwd=PROJECT_ROOT,
            env=self.env,
            stdout=log_f,
            stderr=subprocess.STDOUT,
        )
        log_f.close()
        self.legacy_coord_proc = proc

        def pid_file_matches() -> bool:
            pid_file = home / "coordinator.pid"
            return (
                proc.poll() is None
                and pid_file.exists()
                and pid_file.read_text().strip() == str(proc.pid)
            )

        self.wait_until(
            "legacy coordinator singleton lock/pid file",
            pid_file_matches,
            timeout=8,
            watch_proc=proc,
            log_path=log_path,
        )
        print(f"  🟢 legacy coordinator started (pid {proc.pid})", flush=True)
        return proc.pid

    def kill_legacy_coordinator(self) -> int:
        proc = self.legacy_coord_proc
        if proc is None:
            raise RuntimeError("legacy coordinator is not running")
        pid = proc.pid
        print(f"  🛑 SIGKILL legacy coordinator (pid {pid})", flush=True)
        proc.kill()
        proc.wait(timeout=5)
        self.legacy_coord_proc = None
        time.sleep(0.2)
        return pid

    def legacy_rs(self, name: str) -> Any:
        assert self.legacy_store is not None
        return next(
            r
            for r in self.legacy_store.release_services(self.legacy_rid)
            if r["service_name"] == name
        )

    def show_failure_logs(self) -> None:
        coord_log = self.home / "logs" / "coordinator.log"
        print("\n--- coordinator log tail ---", file=sys.stderr)
        print(tail(coord_log, 5000), file=sys.stderr)
        for name in ("svc-2", "svc-3", "svc-5"):
            print(f"\n--- {name} log tail ---", file=sys.stderr)
            print(tail(self.home / "logs" / f"{name}.log", 2500), file=sys.stderr)
        if self.legacy_home is not None:
            leg_log = self.legacy_home / "logs" / "coordinator.log"
            print("\n--- legacy coordinator log tail ---", file=sys.stderr)
            print(tail(leg_log, 4000), file=sys.stderr)

    # ------------------------------------------------------------------
    # Phase 3
    # ------------------------------------------------------------------

    def build_legacy_crossed_home(self) -> tuple[Path, list[int]]:
        """Create an old-schema runtime whose svc-3 endpoint is crossed to svc-1."""
        from .service import MockService

        leg = self.home / "legacy-cross"
        self.legacy_home = leg
        names = ["svc-1", "svc-2", "svc-3"]
        ports = free_tcp_ports(3)
        self.legacy_ports = ports
        p1, p2, p3 = ports
        url1 = f"http://127.0.0.1:{p1}"

        # Current code lays out cluster.json + seeded per-service state files.
        Store(leg).initialize_home(names=names, ports=ports)

        # The old incident crossed svc-3 onto svc-1's endpoint in cluster.json.
        cluster_path = leg / "cluster.json"
        cluster = json.loads(cluster_path.read_text(encoding="utf-8"))
        for spec in cluster["services"]:
            if spec["name"] == "svc-3":
                spec["url"] = url1
        cluster_path.write_text(json.dumps(cluster, indent=2, ensure_ascii=False))

        v2 = json.loads((PROJECT_ROOT / "configs" / "target-v2.json").read_text())
        rid = self.legacy_rid
        initial = load_initial_state()["services"]

        # Durable evidence: svc-1 really executed the command (and was falsely
        # recorded for svc-3 too); svc-2 durably applied but the coordinator
        # died before recording its confirmation (safe backfill case).
        for name in ("svc-1", "svc-2"):
            mock = MockService(name, leg / "services" / f"{name}.json", initial)
            mock.apply(
                {
                    "command_id": f"apply-{rid}-{name}",
                    "fence": 1,
                    "release_id": rid,
                    "mode": "publish",
                    "version": v2["version"],
                    "content": v2["content"],
                }
            )

        # Replace the current-schema DB with an authentic pre-attribution DB.
        db_path = leg / "coordinator.sqlite3"
        for suffix in ("", "-wal", "-shm"):
            p = Path(str(db_path) + suffix)
            try:
                p.unlink()
            except FileNotFoundError:
                pass

        now = time.time()
        conn = sqlite3.connect(db_path)
        try:
            conn.executescript(LEGACY_SCHEMA)
            conn.execute("INSERT INTO meta(key, value) VALUES('next_fence', '2')")
            for i, name in enumerate(names):
                init_cfg = initial[name]
                if name == "svc-3":
                    base_url = url1  # crossed endpoint
                    actual_version = v2["version"]  # polluted by wrong peer
                    actual_content = canonical_json(v2["content"])
                    reachable = 1
                elif name == "svc-1":
                    base_url = f"http://127.0.0.1:{ports[i]}"
                    actual_version = v2["version"]
                    actual_content = canonical_json(v2["content"])
                    reachable = 1
                else:
                    base_url = f"http://127.0.0.1:{ports[i]}"
                    actual_version = init_cfg["version"]
                    actual_content = canonical_json(init_cfg["content"])
                    reachable = 0
                conn.execute(
                    """
                    INSERT INTO services(
                      name, group_name, base_url, state_path,
                      initial_version, initial_content,
                      actual_version, actual_content, actual_fence,
                      reachable, last_checked, last_error)
                    VALUES(?,?,?,?,?,?,?,?,1,?,?,?)
                    """,
                    (
                        name,
                        "core",
                        base_url,
                        str(leg / "services" / f"{name}.json"),
                        init_cfg["version"],
                        canonical_json(init_cfg["content"]),
                        actual_version,
                        actual_content,
                        reachable,
                        now,
                        None,
                    ),
                )

            conn.execute(
                """
                INSERT INTO releases(
                  id, group_name, target_version, target_content, batches,
                  status, batch_index, pause_requested, rollback_requested,
                  rollback_status, apply_fence, rollback_fence, superseded,
                  note, created_at, updated_at, completed_at)
                VALUES(?,?,?,?,?, 'running', 0, 0,0,'none', 1,NULL,0, ?, ?, ?, NULL)
                """,
                (
                    rid,
                    "core",
                    v2["version"],
                    canonical_json(v2["content"]),
                    json.dumps([names]),
                    "legacy crossed-endpoint incident",
                    now,
                    now,
                ),
            )

            for i, name in enumerate(names):
                init_cfg = initial[name]
                if name == "svc-3":
                    # Polluted/unattributable rollback baseline.
                    baseline_version = "v9.9.9-crossed-peer"
                    baseline_content = canonical_json(
                        {"note": "baseline snapshotted from the wrong endpoint"}
                    )
                    applied_confirmed = 1  # false positive from crossed peer
                else:
                    baseline_version = init_cfg["version"]
                    baseline_content = canonical_json(init_cfg["content"])
                    applied_confirmed = 0 if name == "svc-2" else 1
                conn.execute(
                    """
                    INSERT INTO release_services(
                      release_id, service_name, batch_index,
                      baseline_version, baseline_content, started,
                      applied_confirmed, apply_command_id,
                      rollback_confirmed, rollback_command_id)
                    VALUES(?,?,0,?,?,1,?, ?, 0, ?)
                    """,
                    (
                        rid,
                        name,
                        baseline_version,
                        baseline_content,
                        applied_confirmed,
                        f"apply-{rid}-{name}",
                        f"rollback-{rid}-{name}",
                    ),
                )
            conn.execute(
                "INSERT INTO events(ts, release_id, kind, message) VALUES(?,?,?,?)",
                (now, rid, "submitted", "legacy seeded crossed-endpoint release"),
            )
            conn.commit()
        finally:
            conn.close()

        self.legacy_store = Store(leg)
        return leg, ports

    def phase_crossed_endpoints(self) -> None:
        self.banner(
            "Phase 3: crossed endpoints (串端点) + legacy in-place verification "
            "+ restart takeover (重启接手)"
        )
        leg, ports = self.build_legacy_crossed_home()
        p1, p2, p3 = ports
        rid = self.legacy_rid
        url1 = f"http://127.0.0.1:{p1}"
        url3_real = f"http://127.0.0.1:{p3}"
        self.info(f"legacy runtime home: {leg}")
        self.info("old DB maps svc-3 onto svc-1's endpoint; only svc-1 is started")

        self.step("Start the one physical peer behind the crossed endpoint (svc-1)")
        self.start_legacy_service("svc-1")

        self.step("Verify old runtime in place before endpoint correction")
        report = self.run_verify_json(leg)
        for line in render_verify_report(report).splitlines():
            self.info(line)
        rel_rep = report["release"]
        self.check("verify targets the active legacy release", rel_rep and rel_rep["id"] == rid)
        rows_rep = {r["service_name"]: r for r in rel_rep["rows"]}
        self.check(
            "svc-1 existing confirmation preserved via durable evidence",
            rows_rep["svc-1"]["apply"]["attribution"] == "verified"
            and rows_rep["svc-1"]["apply"]["evidence"] is True
            and self.legacy_rs("svc-1")["applied_confirmed"] == 1,
        )
        self.check(
            "svc-2 missing confirmation safely backfilled from durable evidence",
            rows_rep["svc-2"]["apply"]["backfilled"] is True
            and self.legacy_rs("svc-2")["applied_confirmed"] == 1
            and self.legacy_rs("svc-2")["apply_attribution"] == "verified",
        )
        self.check(
            "svc-3 unattributable confirmation marked pending verification",
            rows_rep["svc-3"]["apply"]["attribution"] == "pending_verification"
            and self.legacy_rs("svc-3")["apply_attribution"] == "pending_verification",
        )
        self.check(
            "svc-3 polluted rollback baseline marked pending verification",
            rows_rep["svc-3"]["baseline"]["attribution"] == "pending_verification"
            and self.legacy_rs("svc-3")["baseline_attribution"]
            == "pending_verification",
        )
        self.check(
            "verify does not advance a release with pending attribution",
            self.legacy_store.get_release(rid)["status"] == "running",
        )
        self.check(
            "live probe names the crossed peer for svc-3",
            report["services"]["svc-3"]["live_identity_ok"] is False
            and report["services"]["svc-3"]["peer_name"] == "svc-1"
            and "svc-1" in (report["services"]["svc-3"]["live_error"] or ""),
            str(report["services"]["svc-3"]),
        )

        self.step("`relctl status` attributes the crossed live peer correctly")
        status = build_status(leg, live=True)
        svc_status = {s["name"]: s for s in status["services"]}
        live3 = svc_status["svc-3"].get("live") or {}
        self.check(
            "status rejects svc-3 live state from an endpoint naming svc-1",
            live3.get("ok") is False
            and live3.get("identity_mismatch") is True
            and live3.get("peer_name") == "svc-1",
            str(live3),
        )
        text = render_status(status)
        print("\n".join(text.splitlines()[:22]), flush=True)
        self.check("status text shows PEER-MISMATCH", "PEER-MISMATCH" in text)
        self.check("status text shows 待核实 marker", "待核实" in text)
        self.check(
            "status text carries pending_verification token",
            "pending_verification" in text,
        )

        self.step("New coordinator on crossed config cannot complete or write wrong peer")
        self.start_legacy_coordinator()
        self.wait_until(
            "coordinator records identity_mismatch for svc-3",
            lambda: "identity_mismatch" in (self.legacy_rs("svc-3")["last_error"] or ""),
            timeout=8,
            watch_proc=self.legacy_coord_proc,
            log_path=leg / "logs" / "coordinator.log",
        )
        # Let several reconcile ticks run to prove it is not a transient.
        time.sleep(0.6)
        rel_after = self.legacy_store.get_release(rid)
        r3 = self.legacy_rs("svc-3")
        self.check(
            "crossed/unstarted svc-3 is never treated as verified complete",
            rel_after["status"] == "running"
            and not (r3["applied_confirmed"] and r3["apply_attribution"] == "verified"),
        )

        peer_before = client.fetch_state(url1, timeout=1.0)
        history_before = len(peer_before["history"])
        try:
            client.apply_config(
                url1,
                command_id=f"apply-{rid}-svc-3",
                fence=1,
                release_id=rid,
                mode="publish",
                version="v2.0.0",
                content=json.loads((PROJECT_ROOT / "configs/target-v2.json").read_text())[
                    "content"
                ],
                service_name="svc-3",
                timeout=2,
            )
            raise AssertionError("misdirected svc-3 apply was accepted by svc-1")
        except client.ServiceRejected as e:
            self.check(
                "wrong peer rejects misdirected write with identity_mismatch",
                e.status == 409 and e.code == "identity_mismatch",
                str(e),
            )
        peer_after = client.fetch_state(url1, timeout=1.0)
        self.check(
            "no svc-3 config/command is written onto svc-1",
            len(peer_after["history"]) == history_before
            and f"apply-{rid}-svc-3" not in peer_after.get("seen_commands", {})
            and peer_after["effective"] == peer_before["effective"],
        )

        self.step("SIGKILL coordinator, correct svc-3 endpoint, verify --sync-endpoints")
        killed_pid = self.kill_legacy_coordinator()
        cluster_path = leg / "cluster.json"
        cluster = json.loads(cluster_path.read_text(encoding="utf-8"))
        for spec in cluster["services"]:
            if spec["name"] == "svc-3":
                spec["url"] = url3_real
        cluster_path.write_text(json.dumps(cluster, indent=2, ensure_ascii=False))

        report2 = self.run_verify_json(leg, "--sync-endpoints")
        self.check(
            "verify syncs corrected svc-3 URL from cluster.json",
            "svc-3" in report2["synced_endpoints"]
            and self.legacy_store.get_service("svc-3")["base_url"] == url3_real,
        )
        self.check(
            "svc-3 stays pending while its real process is still absent",
            self.legacy_rs("svc-3")["apply_attribution"] == "pending_verification"
            and self.legacy_store.get_release(rid)["status"] == "running",
        )

        self.step("Start real svc-3 and restart coordinator to take over the runtime")
        self.start_legacy_service("svc-3")
        new_pid = self.start_legacy_coordinator()
        self.check(
            "replacement legacy coordinator has a new PID",
            new_pid != killed_pid,
        )
        self.wait_until(
            "restarted coordinator finishes the verified release",
            lambda: self.legacy_store.get_release(rid)["status"] == "completed",
            timeout=12,
            watch_proc=self.legacy_coord_proc,
            log_path=leg / "logs" / "coordinator.log",
        )
        self.check(
            "real svc-3 applied target and attribution is verified after takeover",
            self.legacy_rs("svc-3")["apply_attribution"] == "verified"
            and client.fetch_state(url3_real, timeout=1.0)["effective"]["version"]
            == "v2.0.0",
        )

        self.step("Rollback refuses to restore from svc-3's unattributed baseline")
        self.run_cli_in(leg, "rollback", "--reason", "demo unverified baseline block")
        self.wait_until(
            "svc-1 restores its verified baseline while svc-2 is still stopped",
            lambda: (
                self.legacy_rs("svc-1")["rollback_confirmed"]
                and self.legacy_rs("svc-1")["rollback_attribution"] == "verified"
            ),
            timeout=12,
            watch_proc=self.legacy_coord_proc,
            log_path=leg / "logs" / "coordinator.log",
        )
        self.check(
            "stopped svc-2 is not rollback-confirmed before it is started",
            not self.legacy_rs("svc-2")["rollback_confirmed"],
        )
        self.start_legacy_service("svc-2")
        self.wait_until(
            "svc-2 catches up and confirms its own baseline after start",
            lambda: (
                self.legacy_rs("svc-2")["rollback_confirmed"]
                and self.legacy_rs("svc-2")["rollback_attribution"] == "verified"
            ),
            timeout=12,
            watch_proc=self.legacy_coord_proc,
            log_path=leg / "logs" / "coordinator.log",
        )
        time.sleep(0.5)
        rel_rb = self.legacy_store.get_release(rid)
        r3_rb = self.legacy_rs("svc-3")
        st3 = client.fetch_state(url3_real, timeout=1.0)
        self.check(
            "rollback stays incomplete while svc-3 baseline is unverified",
            rel_rb["status"] == "rolling_back",
        )
        self.check(
            "svc-3 records baseline_pending_verification and was not restored",
            r3_rb["baseline_attribution"] == "pending_verification"
            and "baseline_pending_verification" in (r3_rb["last_error"] or "")
            and st3["effective"]["version"] == "v2.0.0",
        )

        self.step("Substantiate true svc-3 baseline and verify; rollback then completes")
        true_baseline = load_initial_state()["services"]["svc-3"]
        with self.legacy_store.connect() as conn:
            conn.execute(
                "UPDATE release_services SET baseline_version=?, baseline_content=? "
                "WHERE release_id=? AND service_name='svc-3'",
                (
                    true_baseline["version"],
                    canonical_json(true_baseline["content"]),
                    rid,
                ),
            )
            conn.commit()
        report3 = self.run_verify_json(leg)
        bl3 = next(
            r for r in report3["release"]["rows"] if r["service_name"] == "svc-3"
        )["baseline"]
        self.check(
            "true svc-3 baseline corroborated by its own state history",
            bl3["attribution"] == "verified" and bl3["corroborated_by"] is not None,
            str(bl3),
        )
        self.wait_until(
            "rollback completes after baseline attribution is verified",
            lambda: self.legacy_store.get_release(rid)["status"] == "rolled_back",
            timeout=12,
            watch_proc=self.legacy_coord_proc,
            log_path=leg / "logs" / "coordinator.log",
        )
        st3_final = client.fetch_state(url3_real, timeout=1.0)
        self.check(
            "svc-3 restored to its own real baseline only after verification",
            st3_final["effective"]["version"] == true_baseline["version"]
            and canonical_json(st3_final["effective"]["content"])
            == canonical_json(true_baseline["content"]),
        )
        self.check("crossed-endpoint phase finished without false completion", True)

    def run(self) -> int:
        started_at = time.monotonic()
        try:
            self.banner("Local release coordinator fault demo")
            print(f"runtime home: {self.home}")
            print("All processes are real local Python HTTP/SQLite processes; faults are finite.")

            self.step("Initialize cluster and bundled differing initial states")
            init = self.run_cli("init", "--ports", ",".join(map(str, self.ports)))
            self.info(init.stdout.strip())
            for name in self.initial:
                self.info(f"{name}: initial {self.initial[name]['version']}")
            self.check("initial versions intentionally differ", len({v['version'] for v in self.initial.values()}) >= 3)

            self.step("Start six independently persistent mock services")
            for name in self.initial:
                self.start_service(name)
            for name in self.initial:
                state = self.live_state(name)
                self.check(f"{name} serves its seeded initial config", self.matches_initial(name, state))

            self.step("Start coordinator process")
            self.start_coordinator()
            self.wait_until(
                "coordinator observes all services reachable",
                lambda: all(r["reachable"] for r in self.store.list_services()),
                timeout=8,
            )
            self.check("SQLite records all services reachable", True)

            # ---------------- Phase 1 ----------------
            self.banner("Phase 1: lost instruction/receipt, duplicate retry, restart, completed-release rollback")
            faults = self.fault_data["service_faults"]["svc-2"]
            self.step(f"Inject finite faults into svc-2: {json.dumps(faults)}")
            fault_cli = self.run_cli(
                "faults", "--service", "svc-2", "--set", json.dumps(faults)
            )
            self.info(fault_cli.stdout.strip().replace("\n", " "))

            v2 = str(PROJECT_ROOT / "configs" / "target-v2.json")
            self.step("Submit v2 with three ordered batches")
            submit = self.run_cli(
                "submit",
                v2,
                "--batch",
                "svc-1,svc-2",
                "--batch",
                "svc-3,svc-4",
                "--batch",
                "svc-5,svc-6",
            )
            rid1 = submit.stdout.split("submitted release ", 1)[1].splitlines()[0].strip()
            self.info(submit.stdout.strip().replace("\n", "\n     "))

            self.step("Coordinator retries stable command IDs through the faults")
            def svc2_duplicate_state() -> dict[str, Any] | None:
                state = self.live_state("svc-2")
                if any(r.get("outcome") == "duplicate_command" for r in state.get("recent_requests", [])):
                    return state
                return None

            svc2_state = self.wait_until(
                "svc-2 records duplicate retry after packet/receipt loss",
                svc2_duplicate_state,
                timeout=12,
            )
            outcomes = [r.get("outcome") for r in svc2_state["recent_requests"]]
            self.info(f"svc-2 recent outcomes: {outcomes[-6:]}")
            self.check("svc-2 saw injected instruction loss", "injected_drop" in outcomes)
            self.check("svc-2 saw receipt loss after durable apply", "receipt_lost_after_apply" in outcomes)
            self.check("svc-2 saw duplicate retry", "duplicate_command" in outcomes)
            self.check(
                "duplicate retry did not append a second effective-config history entry",
                len(svc2_state["history"]) == 2,
                f"(history={len(svc2_state['history'])})",
            )

            self.wait_until(
                "v2 release completes after all batches",
                lambda: self.latest_release()["id"] == rid1
                and self.latest_release()["status"] == "completed",
                timeout=18,
            )
            rel1 = self.store.get_release(rid1)
            v2_cfg = json.loads((PROJECT_ROOT / "configs" / "target-v2.json").read_text())
            for name in self.initial:
                state = self.wait_live_version(name, "v2.0.0")
                self.check(
                    f"{name} live content equals target v2",
                    canonical_json(state["effective"]["content"]) == canonical_json(v2_cfg["content"]),
                )
            self.check("all three v2 batches completed", int(rel1["batch_index"]) == 2)

            self.step("Hard-restart svc-2 independently; last effective config survives")
            self.stop_service("svc-2")
            self.start_service("svc-2")
            restarted = self.wait_live_version("svc-2", "v2.0.0")
            self.check(
                "restarted svc-2 still serves persisted v2 content",
                canonical_json(restarted["effective"]["content"]) == canonical_json(v2_cfg["content"]),
            )

            self.step("Roll back the completed but not superseded v2 release")
            baselines1 = {
                r["service_name"]: (r["baseline_version"], json.loads(r["baseline_content"]))
                for r in self.store.release_services(rid1)
            }
            rb = self.run_cli("rollback", "--reason", "demo completed-release rollback")
            self.info(rb.stdout.strip())
            self.wait_until(
                "v2 rollback completes",
                lambda: self.store.get_release(rid1)["status"] == "rolled_back",
                timeout=15,
            )
            for name, (version, content) in baselines1.items():
                state = self.wait_live_version(name, version)
                self.check(
                    f"{name} restored its own pre-v2 config {version}",
                    canonical_json(state["effective"]["content"]) == canonical_json(content),
                )
            versions = {name: self.live_state(name)["effective"]["version"] for name in self.initial}
            self.info(f"restored versions: {versions}")
            self.check("different baselines are preserved per service", versions["svc-1"] == "v1.0.0" and versions["svc-3"] == "v1.4.2" and versions["svc-5"] == "v0.9.7")

            self.step("Replay a late old v2 instruction after rollback confirmation")
            row1 = self.release_row_service(rid1, "svc-1")
            rel1_row = self.store.get_release(rid1)
            before = self.live_state("svc-1")
            try:
                client.apply_config(
                    self.url("svc-1"),
                    command_id=row1["apply_command_id"],
                    fence=int(rel1_row["apply_fence"]),
                    release_id=rid1,
                    mode="publish",
                    version=rel1_row["target_version"],
                    content=json.loads(rel1_row["target_content"]),
                    timeout=2,
                )
                raise AssertionError("late stale publish was unexpectedly accepted")
            except client.ServiceRejected as e:
                self.check("late old publish is rejected with stale_fence", e.status == 409 and e.code == "stale_fence", str(e))
            after = self.live_state("svc-1")
            self.check(
                "svc-1 remains on restored baseline and history is unchanged",
                after["effective"]["version"] == before["effective"]["version"] == "v1.0.0"
                and len(after["history"]) == len(before["history"]),
            )

            # ---------------- Phase 2 ----------------
            self.banner("Phase 2: accepted pause + SIGKILL takeover +失联 blocking + in-progress rollback")
            pause_cfg = self.fault_data["pause_phase"]
            for name in pause_cfg["stop_before_submit"]:
                self.stop_service(name)

            v3 = str(PROJECT_ROOT / "configs" / "target-v3.json")
            batches_args: list[str] = []
            for batch in pause_cfg["batches"]:
                batches_args.extend(["--batch", ",".join(batch)])
            self.step("Submit v3 while svc-3 is unreachable, then durably accept pause")
            submit3 = self.run_cli("submit", v3, *batches_args)
            rid2 = submit3.stdout.split("submitted release ", 1)[1].splitlines()[0].strip()
            self.run_cli("pause", "--reason", "demo freeze after current batch")
            rel2 = self.store.get_release(rid2)
            self.check("pause decision is durable in SQLite", bool(rel2["pause_requested"]))

            killed_pid = self.kill_coordinator()
            self.start_coordinator()
            self.check("replacement coordinator has a new PID", self.coordinator_proc is not None and self.coordinator_proc.pid != killed_pid)
            self.check(
                "new process inherited accepted pause from SQLite",
                bool(self.store.get_release(rid2)["pause_requested"]),
            )

            self.step("Reachable svc-1/svc-2 finish batch 1;失联 svc-3 blocks its completion")
            self.wait_until(
                "svc-1 and svc-2 confirm v3",
                lambda: all(
                    self.release_row_service(rid2, n)["applied_confirmed"]
                    for n in ("svc-1", "svc-2")
                ),
                timeout=12,
            )
            r3 = self.release_row_service(rid2, "svc-3")
            self.check("svc-3 has not confirmed while stopped", not r3["applied_confirmed"])
            self.check("batch 2 has not started", not self.release_row_service(rid2, "svc-4")["started"])

            self.step("Restart svc-3; it catches up and pause closes batch 1 without opening batch 2")
            self.start_service("svc-3")
            self.wait_until(
                "batch 1 completes and release enters paused",
                lambda: self.store.get_release(rid2)["status"] == "paused",
                timeout=12,
            )
            self.check("paused release remains on batch 1", int(self.store.get_release(rid2)["batch_index"]) == 0)
            self.check("svc-4 still has its pre-v3 config while paused", self.live_state("svc-4")["effective"]["version"] == "v1.4.2")

            self.step("Sample `relctl status` query while paused")
            status_text = render_status(build_status(self.home, live=True))
            print("\n".join(status_text.splitlines()[:48]), flush=True)
            self.check("status exposes every service content query", "actual service configuration:" in status_text)
            self.check("status shows live effective version/content", "version=v3.0.0-canary" in status_text and "speculative_retry" in status_text)
            self.check("status does not render null live config", "version=None" not in status_text)

            self.step("Stop svc-5, then resume; reachable svc-4 proceeds but svc-5 blocks later batches")
            self.stop_service("svc-5")
            self.run_cli("resume")
            self.wait_until(
                "svc-4 confirms v3 in batch 2",
                lambda: self.release_row_service(rid2, "svc-4")["applied_confirmed"]
                and self.live_state("svc-4")["effective"]["version"] == "v3.0.0-canary",
                timeout=12,
            )
            r5 = self.release_row_service(rid2, "svc-5")
            self.check("svc-5 is batch-started but unconfirmed", bool(r5["started"]) and not r5["applied_confirmed"])
            r6 = self.release_row_service(rid2, "svc-6")
            self.check("batch 3 / svc-6 has not started", not r6["started"])
            self.check("svc-6 remains on untouched v0.9.7", self.live_state("svc-6")["effective"]["version"] == "v0.9.7")

            self.step("Accept in-progress rollback and SIGKILL coordinator immediately")
            self.run_cli("rollback", "--reason", "demo in-progress rollback")
            killed_pid2 = self.kill_coordinator()
            self.start_coordinator()
            self.check("second replacement coordinator has a new PID", self.coordinator_proc is not None and self.coordinator_proc.pid != killed_pid2)
            self.check(
                "accepted rollback survived coordinator death",
                bool(self.store.get_release(rid2)["rollback_requested"]),
            )

            reachable_started = ["svc-1", "svc-2", "svc-3", "svc-4"]
            self.wait_until(
                "reachable started services restore baselines",
                lambda: all(
                    self.release_row_service(rid2, n)["rollback_confirmed"]
                    and self.live_state(n)["effective"]["version"] == self.initial[n]["version"]
                    for n in reachable_started
                ),
                timeout=15,
            )
            rel2_now = self.store.get_release(rid2)
            self.check("rollback is not complete while svc-5 is失联", rel2_now["status"] == "rolling_back")
            self.check(
                "svc-6 unstarted batch remains untouched",
                not self.release_row_service(rid2, "svc-6")["started"]
                and self.live_state("svc-6")["effective"]["version"] == "v0.9.7",
            )

            self.step("Restart svc-5; it catches up and only then rollback completes")
            self.start_service("svc-5")
            self.wait_until(
                "in-progress rollback completes after svc-5 confirmation",
                lambda: self.store.get_release(rid2)["status"] == "rolled_back",
                timeout=12,
            )
            for name in self.initial:
                state = self.live_state(name)
                self.check(
                    f"final {name} matches its own pre-v3 baseline",
                    self.matches_initial(name, state),
                )

            self.step("A late v3 command to restored svc-5 is fenced off")
            row5 = self.release_row_service(rid2, "svc-5")
            before5 = self.live_state("svc-5")
            try:
                client.apply_config(
                    self.url("svc-5"),
                    command_id=row5["apply_command_id"],
                    fence=int(self.store.get_release(rid2)["apply_fence"]),
                    release_id=rid2,
                    mode="publish",
                    version="v3.0.0-canary",
                    content=json.loads((PROJECT_ROOT / "configs" / "target-v3.json").read_text())["content"],
                    timeout=2,
                )
                raise AssertionError("late v3 publish was unexpectedly accepted after rollback")
            except client.ServiceRejected as e:
                self.check("restored svc-5 rejects late publish with stale_fence", e.code == "stale_fence")
            after5 = self.live_state("svc-5")
            self.check("svc-5 stays on its baseline after late command", self.matches_initial("svc-5", after5))

            # ---------------- Phase 3 ----------------
            self.phase_crossed_endpoints()

            elapsed = time.monotonic() - started_at
            self.banner(f"DEMO PASSED in {elapsed:.1f}s — {len(self.passed_checks)} checks")
            print(f"persistent demo state/logs retained at: {self.home}")
            print("Key files:")
            print(f"  SQLite state : {self.home / 'coordinator.sqlite3'}")
            print(f"  service JSON : {self.home / 'services'}")
            print(f"  logs         : {self.home / 'logs'}")
            if self.legacy_home is not None:
                print(f"  legacy cross : {self.legacy_home} (old-schema in-place verification)")
            print("\nTry manual commands in README.md, or run: ./run_demo.sh")
            return 0
        except Exception as e:
            self.banner("DEMO FAILED")
            print(f"{type(e).__name__}: {e}", file=sys.stderr)
            traceback.print_exc()
            self.show_failure_logs()
            print(f"\nDemo home retained for inspection: {self.home}", file=sys.stderr)
            return 1
        finally:
            self.cleanup()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run self-checking local fault demo")
    parser.add_argument("--keep-processes", action="store_true", help="leave demo services/coordinator running")
    args = parser.parse_args(argv)
    return FaultDemo(keep_processes=args.keep_processes).run()


if __name__ == "__main__":
    raise SystemExit(main())
