from __future__ import annotations

import argparse
import json
import os
import socket
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


class DemoHarness:
    """Shared process/SQLite orchestration for self-checking demo scenarios."""

    def __init__(self, *, keep_processes: bool = False, service_count: int = 6):
        self.keep_processes = keep_processes
        self.home = Path(tempfile.mkdtemp(prefix="relcoord-demo-"))
        self.ports = free_tcp_ports(service_count)
        self.store = Store(self.home)
        self.initial = load_initial_state()["services"]
        self.service_procs: dict[str, subprocess.Popen] = {}
        self.coordinator_proc: subprocess.Popen | None = None
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
        cmd = [sys.executable, "-m", "relcoord", *args, "--home", str(self.home)]
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
    ) -> Any:
        deadline = time.monotonic() + timeout
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            if self.coordinator_proc is not None and self.coordinator_proc.poll() is not None:
                raise RuntimeError(
                    f"coordinator exited unexpectedly while waiting for {description}:\n"
                    + tail(self.home / "logs" / "coordinator.log")
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
            return
        for name, proc in list(self.service_procs.items()):
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

    def show_failure_logs(self) -> None:
        coord_log = self.home / "logs" / "coordinator.log"
        print("\n--- coordinator log tail ---", file=sys.stderr)
        print(tail(coord_log, 5000), file=sys.stderr)
        for name in ("svc-2", "svc-3", "svc-5"):
            print(f"\n--- {name} log tail ---", file=sys.stderr)
            print(tail(self.home / "logs" / f"{name}.log", 2500), file=sys.stderr)


class FaultDemo(DemoHarness):
    def __init__(self, *, keep_processes: bool = False):
        super().__init__(keep_processes=keep_processes, service_count=6)
        fault_path = PROJECT_ROOT / "faults" / "demo-faults.json"
        self.fault_data = json.loads(fault_path.read_text(encoding="utf-8"))

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

            elapsed = time.monotonic() - started_at
            self.banner(f"DEMO PASSED in {elapsed:.1f}s — {len(self.passed_checks)} checks")
            print(f"persistent demo state/logs retained at: {self.home}")
            print("Key files:")
            print(f"  SQLite state : {self.home / 'coordinator.sqlite3'}")
            print(f"  service JSON : {self.home / 'services'}")
            print(f"  logs         : {self.home / 'logs'}")
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


class HangRollbackDemo(DemoHarness):
    """Focused check: a rollback must not wait behind the publish it undoes.

    Batch 1's publishes are suspended 5s by local fault injection. While their
    requests are still outstanding, we accept rollback and require every
    already-started service to confirm its own baseline within 800ms.
    """

    HANG_SECONDS = 5.0
    RECOVERY_BUDGET_MS = 800.0

    def __init__(self, *, keep_processes: bool = False):
        super().__init__(keep_processes=keep_processes, service_count=6)
        self.suspended = ["svc-1", "svc-2"]

    def _set_hang(self, name: str) -> None:
        self.run_cli(
            "faults",
            "--service",
            name,
            "--set",
            json.dumps({"delay_seconds": self.HANG_SECONDS, "delay_next": 1}),
        )

    def _publish_request_pending(self, name: str, release_id: str) -> bool:
        state = self.live_state(name)
        return any(
            item.get("mode") == "publish"
            and item.get("command_id") == f"apply-{release_id}-{name}"
            for item in state.get("inflight", [])
        )

    def run(self) -> int:
        started_at = time.monotonic()
        try:
            self.banner("Rollback while current-batch publishes are suspended")
            print(f"runtime home: {self.home}")
            self.run_cli("init", "--ports", ",".join(map(str, self.ports)))
            for name in self.initial:
                self.start_service(name)
            self.start_coordinator()
            self.wait_until(
                "coordinator observes all services reachable",
                lambda: all(r["reachable"] for r in self.store.list_services()),
                timeout=8,
            )

            self.step(f"Inject a {self.HANG_SECONDS:.0f}s delay on the first publish to {self.suspended}")
            for name in self.suspended:
                self._set_hang(name)
            for name in self.suspended:
                faults = self.live_state(name)["faults"]
                self.info(f"{name} faults: {json.dumps(faults, sort_keys=True)}")

            v2 = str(PROJECT_ROOT / "configs" / "target-v2.json")
            self.step("Submit v2: batch 1 (svc-1, svc-2), then batch 2 (svc-3, svc-4), batch 3 (svc-5, svc-6)")
            submit = self.run_cli(
                "submit", v2,
                "--batch", "svc-1,svc-2",
                "--batch", "svc-3,svc-4",
                "--batch", "svc-5,svc-6",
            )
            rid = submit.stdout.split("submitted release ", 1)[1].splitlines()[0].strip()
            self.info(f"release {rid}")

            self.step("Wait until both batch-1 publishes are physically on the wire")
            def both_suspended() -> bool:
                return all(self._publish_request_pending(n, rid) for n in self.suspended)

            self.wait_until("svc-1/svc-2 publish requests are pending", both_suspended, timeout=5)
            pending_started = self.store.get_release(rid)["created_at"]
            inflight = {n: self.live_state(n).get("inflight", []) for n in self.suspended}
            for n, items in inflight.items():
                self.info(f"{n} in-flight request: {json.dumps(items, sort_keys=True)}")
            self.check(
                "old publish requests genuinely outstanding at rollback time",
                all(items for items in inflight.values()),
            )
            self.check(
                "no service has confirmed the target yet",
                all(not self.release_row_service(rid, n)["applied_confirmed"] for n in self.suspended),
            )
            self.check(
                "later batches have not started",
                not self.release_row_service(rid, "svc-3")["started"]
                and not self.release_row_service(rid, "svc-6")["started"],
            )

            self.step("Accept rollback while the 5s publish delay is still running")
            accepted = self.run_cli("rollback", "--reason", "reproduce rollback-behind-hang")
            self.info(accepted.stdout.strip())
            rollback_t0 = time.monotonic()

            def started_services_restored() -> bool:
                return all(
                    self.release_row_service(rid, n)["rollback_confirmed"]
                    for n in ("svc-1", "svc-2")
                )

            self.wait_until(
                f"batch-1 services confirm their baselines within {self.RECOVERY_BUDGET_MS:.0f}ms",
                started_services_restored,
                timeout=self.RECOVERY_BUDGET_MS / 1000.0,
                interval=0.02,
            )
            recovery_ms = (time.monotonic() - rollback_t0) * 1000.0
            self.info(f"batch-1 rollback confirmed in {recovery_ms:.0f}ms")
            self.check(
                f"rollback confirms within {self.RECOVERY_BUDGET_MS:.0f}ms",
                recovery_ms <= self.RECOVERY_BUDGET_MS,
                f"(took {recovery_ms:.0f}ms)",
            )
            for name in self.suspended:
                state = self.live_state(name)
                self.check(
                    f"{name} live config is its own baseline {self.initial[name]['version']}",
                    self.matches_initial(name, state),
                )
            still_pending = {n: self.live_state(n).get("inflight", []) for n in self.suspended}
            self.info(f"in-flight after rollback: {json.dumps({k: [i['command_id'] for i in v] for k, v in still_pending.items()})}")
            self.check(
                "old publish requests are still waiting when rollback confirms",
                any(still_pending.values()),
            )

            unstarted = ["svc-3", "svc-4", "svc-5", "svc-6"]
            self.check(
                "unstarted batches were never touched",
                all(
                    not self.release_row_service(rid, n)["started"]
                    and self.live_state(n)["effective"]["version"] == self.initial[n]["version"]
                    for n in unstarted
                ),
            )

            self.step("Once the suspended publishes wake, they must not overwrite restored config")
            deadline = time.monotonic() + self.HANG_SECONDS + 4
            while time.monotonic() < deadline:
                if not any(self.live_state(n).get("inflight") for n in self.suspended):
                    break
                time.sleep(0.05)
            time.sleep(0.4)
            for name in self.suspended:
                state = self.live_state(name)
                outcomes = [r.get("outcome") for r in state.get("recent_requests", [])]
                self.info(f"{name} outcomes: {outcomes[-4:]}")
                self.check(
                    f"late publish on {name} fenced off",
                    "rejected_stale_fence" in outcomes,
                )
                self.check(
                    f"{name} still serves baseline content after late publish",
                    self.matches_initial(name, state),
                )

            self.wait_until(
                "rollback completes",
                lambda: self.store.get_release(rid)["status"] == "rolled_back",
                timeout=8,
            )
            self.check(
                "only started batch 1 was restored; release rolled back",
                self.store.get_release(rid)["status"] == "rolled_back",
            )

            self.step("Restart a restored service; the late publish cannot return either")
            self.stop_service("svc-1")
            self.start_service("svc-1")
            restarted = self.live_state("svc-1")
            self.check("restarted svc-1 still serves baseline", self.matches_initial("svc-1", restarted))
            try:
                client.apply_config(
                    self.url("svc-1"),
                    command_id=f"apply-{rid}-svc-1",
                    fence=int(self.store.get_release(rid)["apply_fence"]),
                    release_id=rid,
                    mode="publish",
                    version="v2.0.0",
                    content=json.loads((PROJECT_ROOT / "configs" / "target-v2.json").read_text())["content"],
                    timeout=2,
                )
                raise AssertionError("late publish accepted after restart")
            except client.ServiceRejected as e:
                self.check(
                    "restarted svc-1 rejects the old publish with stale_fence",
                    e.code == "stale_fence",
                )
            self.check(
                "restarted svc-1 config unchanged",
                self.matches_initial("svc-1", self.live_state("svc-1")),
            )

            elapsed = time.monotonic() - started_at
            self.banner(f"ROLLBACK-WHILE-HUNG PASSED in {elapsed:.1f}s — {len(self.passed_checks)} checks")
            print(f"state/logs retained at: {self.home}")
            return 0
        except Exception as e:
            self.banner("ROLLBACK-WHILE-HUNG FAILED")
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
    parser.add_argument(
        "--scenario",
        choices=("full", "rollback-while-hung"),
        default="full",
        help="full: original end-to-end fault demo; rollback-while-hung: focused reproduction",
    )
    args = parser.parse_args(argv)
    if args.scenario == "rollback-while-hung":
        return HangRollbackDemo(keep_processes=args.keep_processes).run()
    return FaultDemo(keep_processes=args.keep_processes).run()


if __name__ == "__main__":
    raise SystemExit(main())
