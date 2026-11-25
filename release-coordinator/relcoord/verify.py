"""One-command verification of per-round probe traffic and recovery behavior.

Runs real local HTTP services plus a coordinator in a throwaway home and
asserts, end to end:

* while configuration and effective state are unchanged, every background
  probe response for every service stays below 2 KiB — including after a
  service process or the coordinator itself is killed and restarted;
* a cold coordinator (missing content fingerprint) performs exactly one
  full-content observation and then falls back to compact summaries;
* swapping content behind the same version name makes the first successful
  probe pull the complete new observation into SQLite, so offline queries and
  later rollbacks use the refreshed state;
* the existing live query, receipt-loss/idempotent retry and rollback
  confirmation (stale fence) behavior is preserved.

Run it with:  relctl verify   (or: python3 -m relcoord verify)
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
import traceback
from pathlib import Path
from typing import Any, Callable

from . import client
from .storage import Store
from .util import PROJECT_ROOT, canonical_json, content_hash

# Hard requirement: unchanged-state background probe bodies must stay <= 2 KiB
# per service per round. The compact summary the service emits is far smaller.
UNCHANGED_BUDGET_BYTES = 2 * 1024
SUMMARY_BODY_BYTES = 256
LARGE_BLOB = "Z" * 2600
SWAPPED_BLOB = "Q" * 2800
NEXT_BLOB = "N" * 2400
SERVICE_NAMES = ["svc-1", "svc-2", "svc-3", "svc-4", "svc-5", "svc-6"]


def free_tcp_ports(count: int) -> list[int]:
    import socket

    socks, ports = [], []
    for _ in range(count):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("127.0.0.1", 0))
        socks.append(s)
        ports.append(s.getsockname()[1])
    for s in socks:
        s.close()
    return ports


class TrafficVerifier:
    def __init__(self) -> None:
        self.home = Path(tempfile.mkdtemp(prefix="relcoord-verify-"))
        self.ports = free_tcp_ports(len(SERVICE_NAMES))
        self.store = Store(self.home)
        self.service_procs: dict[str, subprocess.Popen] = {}
        self.coordinator_proc: subprocess.Popen | None = None
        self.passed_checks: list[str] = []
        self.env = {**os.environ, "PYTHONPATH": str(PROJECT_ROOT)}
        self._round_cursor = 0

    # ------------------------------------------------------------------ utils
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
            cmd, cwd=PROJECT_ROOT, env=self.env, text=True, capture_output=True, timeout=25
        )
        if result.returncode != 0:
            print(result.stdout)
            print(result.stderr, file=sys.stderr)
            raise RuntimeError(f"CLI failed: {' '.join(args)}")
        return result

    def url(self, name: str) -> str:
        cluster = self.store.load_cluster()
        return next(s["url"] for s in cluster["services"] if s["name"] == name)

    def wait_until(
        self, description: str, predicate: Callable[[], Any], timeout: float = 12, interval: float = 0.08
    ) -> Any:
        deadline = time.monotonic() + timeout
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            if self.coordinator_proc is not None and self.coordinator_proc.poll() is not None:
                raise RuntimeError(f"coordinator exited while waiting for {description}")
            try:
                value = predicate()
                if value:
                    return value
            except Exception as e:  # first rounds may race startup
                last_error = e
            time.sleep(interval)
        raise TimeoutError(f"timed out waiting for {description}; last error: {last_error}")

    # --------------------------------------------------------------- processes
    def start_service(self, name: str) -> None:
        log_path = self.home / "logs" / f"{name}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_f = open(log_path, "ab", buffering=0)
        proc = subprocess.Popen(
            [sys.executable, "-m", "relcoord", "service", "--home", str(self.home), "--name", name],
            cwd=PROJECT_ROOT,
            env=self.env,
            stdout=log_f,
            stderr=subprocess.STDOUT,
        )
        log_f.close()
        self.service_procs[name] = proc
        self.wait_until(
            f"service {name} health",
            lambda: proc.poll() is None and client.health(self.url(name), 0.4).get("ok") is True,
        )

    def stop_service(self, name: str) -> None:
        proc = self.service_procs.get(name)
        if proc is None or proc.poll() is not None:
            return
        print(f"  🛑 SIGKILL service {name} (pid {proc.pid})", flush=True)
        proc.kill()
        proc.wait(timeout=5)

    def start_coordinator(self) -> None:
        log_path = self.home / "logs" / "coordinator.log"
        log_f = open(log_path, "ab", buffering=0)
        proc = subprocess.Popen(
            [
                sys.executable, "-m", "relcoord", "coordinator",
                "--home", str(self.home), "--interval", "0.10",
            ],
            cwd=PROJECT_ROOT,
            env=self.env,
            stdout=log_f,
            stderr=subprocess.STDOUT,
        )
        log_f.close()
        self.coordinator_proc = proc
        pid_file = self.home / "coordinator.pid"
        self.wait_until(
            "coordinator pid file",
            lambda: proc.poll() is None and pid_file.exists()
            and pid_file.read_text().strip() == str(proc.pid),
        )
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
        time.sleep(0.15)
        return pid

    def settle(self, seconds: float = 0.6) -> None:
        """Let several 0.10s probe rounds accumulate."""
        time.sleep(seconds)

    # -------------------------------------------------------------- traffic log
    def traffic_rounds(self) -> list[dict[str, Any]]:
        path = self.store.probe_traffic_path
        if not path.exists():
            return []
        return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]

    def new_rounds(self) -> list[dict[str, Any]]:
        rounds = self.traffic_rounds()
        fresh = rounds[self._round_cursor:]
        self._round_cursor = len(rounds)
        return fresh

    def assert_unchanged_rounds_within_budget(self, rounds: list[dict[str, Any]], scope: str) -> None:
        worst = 0
        for entry in rounds:
            for name, item in entry["services"].items():
                if not item.get("ok"):
                    self.check(
                        f"{scope}: failed {name} probe carries no body",
                        item["response_bytes"] == 0,
                        str(item),
                    )
                    continue
                if item["changed"] is False:
                    worst = max(worst, item["response_bytes"])
                    self.check(
                        f"{scope}: {name} unchanged probe <= {UNCHANGED_BUDGET_BYTES} B "
                        f"(round {entry['round']}, {item['response_bytes']} B)",
                        item["response_bytes"] <= UNCHANGED_BUDGET_BYTES,
                    )
        if worst:
            self.info(f"{scope}: largest unchanged response body was {worst} B")

    def full_fetches(self, rounds: list[dict[str, Any]]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for entry in rounds:
            for name, item in entry["services"].items():
                if item.get("ok") and item.get("changed"):
                    counts[name] = counts.get(name, 0) + 1
        return counts

    # ------------------------------------------------------------------- phases
    def write_fixtures(self) -> tuple[Path, Path, Path]:
        initial = {
            "group": "core",
            "services": {
                name: {
                    "version": "v9.0.0",
                    "content": {"role": "verify", "index": i, "blob": LARGE_BLOB},
                }
                for i, name in enumerate(SERVICE_NAMES)
            },
        }
        initial_path = self.home / "initial-large.json"
        initial_path.write_text(json.dumps(initial), encoding="utf-8")

        same_name_new = {
            "version": "v9.0.0",  # deliberately the SAME name, different content
            "content": {"role": "verify", "blob": SWAPPED_BLOB, "swapped": True},
        }
        same_name_path = self.home / "target-same-name.json"
        same_name_path.write_text(json.dumps(same_name_new), encoding="utf-8")

        next_cfg = {
            "version": "v9.1.0",
            "content": {"role": "verify", "blob": NEXT_BLOB, "swapped": True, "next": True},
        }
        next_path = self.home / "target-v91.json"
        next_path.write_text(json.dumps(next_cfg), encoding="utf-8")
        return initial_path, same_name_path, next_path

    def cleanup(self) -> None:
        for proc in list(self.service_procs.values()):
            if proc.poll() is None:
                proc.kill()
        if self.coordinator_proc is not None and self.coordinator_proc.poll() is None:
            self.coordinator_proc.kill()

    def run(self) -> int:
        started_at = time.monotonic()
        try:
            self.banner("RelCoord probe-traffic and recovery verification")
            print(f"runtime home: {self.home}")
            initial_path, same_name_path, next_path = self.write_fixtures()
            initial_blob_canon = canonical_json({"role": "verify", "index": 0, "blob": LARGE_BLOB})

            self.step("Initialize cluster with >2 KiB effective configs and start all processes")
            self.run_cli(
                "init",
                "--ports", ",".join(map(str, self.ports)),
                "--initial-state", str(initial_path),
            )
            for name in SERVICE_NAMES:
                self.start_service(name)
            self.start_coordinator()
            self.wait_until(
                "all services reachable",
                lambda: all(r["reachable"] for r in self.store.list_services()),
            )
            self.settle()
            rounds = self.new_rounds()
            self.check("probe rounds were recorded", len(rounds) >= 3, f"got {len(rounds)}")
            self.assert_unchanged_rounds_within_budget(rounds, "steady state")
            self.check(
                "steady state uses compact summaries only (no full fetch)",
                self.full_fetches(rounds) == {},
                str(self.full_fetches(rounds)),
            )
            steady_total = max(e["total_response_bytes"] for e in rounds)
            self.info(f"largest steady-round total across {len(SERVICE_NAMES)} services: {steady_total} B")
            self.check(
                f"per-round total also stays under {UNCHANGED_BUDGET_BYTES} B",
                steady_total <= UNCHANGED_BUDGET_BYTES,
            )

            self.step("Live /state query still returns the complete large content")
            live = client.fetch_state(self.url("svc-1"), timeout=1.0)
            self.check(
                "live query exposes full content untouched",
                canonical_json(live["effective"]["content"]) == initial_blob_canon,
            )
            offline = self.run_cli("status", "--no-live")
            self.check("offline SQLite status shows the full content", LARGE_BLOB[:64] in offline.stdout)

            self.step("Cold coordinator (fingerprint missing) backfills once, then resumes summaries")
            self.kill_coordinator()
            with self.store.connect() as conn:
                conn.execute(
                    "UPDATE services SET actual_content_hash=NULL, actual_content=NULL WHERE name='svc-1'"
                )
                conn.commit()
            self.start_coordinator()
            self.settle()
            cold_rounds = self.new_rounds()
            cold_fetches = self.full_fetches(cold_rounds)
            self.check(
                "cold svc-1 performs exactly one full observation",
                cold_fetches == {"svc-1": 1},
                str(cold_fetches),
            )
            cold_entry = next(
                item for e in cold_rounds for n, item in e["services"].items()
                if n == "svc-1" and item.get("changed")
            )
            self.check(
                "the one cold full fetch exceeds the steady budget",
                cold_entry["response_bytes"] > UNCHANGED_BUDGET_BYTES,
                f"{cold_entry['response_bytes']} B",
            )
            self.assert_unchanged_rounds_within_budget(cold_rounds, "cold backfill")
            row = self.store.get_service("svc-1")
            self.check(
                "SQLite full observation was backfilled for offline use",
                row["actual_content"] is not None
                and canonical_json(json.loads(row["actual_content"])) == initial_blob_canon
                and row["actual_content_hash"] == content_hash(json.loads(row["actual_content"])),
            )

            self.step("Kill and restart a service process; probes stay small and state survives")
            self.stop_service("svc-2")
            self.settle(0.4)
            self.start_service("svc-2")
            self.settle(0.6)
            restart_rounds = self.new_rounds()
            self.assert_unchanged_rounds_within_budget(restart_rounds, "service restart")
            self.check(
                "no full fetch after plain service restart",
                self.full_fetches(restart_rounds) == {},
                str(self.full_fetches(restart_rounds)),
            )
            live2 = client.fetch_state(self.url("svc-2"), timeout=1.0)
            self.check(
                "restarted service still serves its persisted large config",
                canonical_json(live2["effective"]["content"])
                == canonical_json({"role": "verify", "index": 1, "blob": LARGE_BLOB}),
            )

            self.step("Kill and restart the coordinator; per-round budget still holds")
            killed_pid = self.kill_coordinator()
            self.start_coordinator()
            self.check("replacement coordinator has a new PID", self.coordinator_proc.pid != killed_pid)
            self.settle()
            take_over_rounds = self.new_rounds()
            self.assert_unchanged_rounds_within_budget(take_over_rounds, "coordinator takeover")
            self.check(
                "takeover needs no full fetch while state is unchanged",
                self.full_fetches(take_over_rounds) == {},
                str(self.full_fetches(take_over_rounds)),
            )

            self.step("Same version name, different content: first successful probe refreshes full observation")
            state_path = next(
                s["state_path"] for s in self.store.load_cluster()["services"] if s["name"] == "svc-3"
            )
            self.stop_service("svc-3")
            svc3_state = json.loads(Path(state_path).read_text(encoding="utf-8"))
            self.check(
                "svc-3 currently reports the shared version name",
                svc3_state["effective"]["version"] == "v9.0.0",
            )
            # Keep the version name, replace the content behind it.
            svc3_state["effective"]["content"] = {
                "role": "verify", "blob": SWAPPED_BLOB, "swapped": True
            }
            Path(state_path).write_text(json.dumps(svc3_state, indent=2), encoding="utf-8")
            self.start_service("svc-3")
            self.settle()
            swap_rounds = self.new_rounds()
            swap_fetches = self.full_fetches(swap_rounds)
            self.check(
                "svc-3 same-name drift triggers exactly one full observation",
                swap_fetches == {"svc-3": 1},
                str(swap_fetches),
            )
            self.assert_unchanged_rounds_within_budget(swap_rounds, "same-name swap")
            row3 = self.store.get_service("svc-3")
            swapped_canon = canonical_json(
                {"role": "verify", "blob": SWAPPED_BLOB, "swapped": True}
            )
            self.check(
                "offline SQLite observation now holds the swapped full content",
                row3["actual_version"] == "v9.0.0"
                and row3["actual_content"] == swapped_canon
                and row3["actual_content_hash"] == content_hash(json.loads(swapped_canon)),
            )
            offline3 = self.run_cli("status", "--no-live")
            self.check("offline status renders swapped content", SWAPPED_BLOB[:64] in offline3.stdout)

            self.step("Receipt-loss retry remains idempotent while publishing v9.1.0")
            self.run_cli(
                "faults", "--service", "svc-2", "--set", json.dumps({"lose_receipt": 1})
            )
            submit = self.run_cli(
                "submit", str(next_path), "--batch", ",".join(SERVICE_NAMES)
            )
            rid = submit.stdout.split("submitted release ", 1)[1].splitlines()[0].strip()
            self.wait_until(
                "v9.1.0 release completes",
                lambda: self.store.get_release(rid)["status"] == "completed",
                timeout=15,
            )
            svc2 = client.fetch_state(self.url("svc-2"), timeout=1.0)
            outcomes = [r.get("outcome") for r in svc2.get("recent_requests", [])]
            self.check("svc-2 saw receipt loss and a duplicate retry", "duplicate_command" in outcomes)
            history_versions = [h["version"] for h in svc2["history"]]
            self.check(
                "duplicate retry appended no extra history entry",
                history_versions.count("v9.1.0") == 1,
                str(history_versions),
            )
            self.settle()
            publish_rounds = self.new_rounds()
            self.assert_unchanged_rounds_within_budget(publish_rounds, "post-publish steady state")
            for name in SERVICE_NAMES:
                state = client.fetch_state(self.url(name), timeout=1.0)
                self.check(
                    f"{name} live content equals v9.1.0 target",
                    canonical_json(state["effective"]["content"])
                    == canonical_json({"role": "verify", "blob": NEXT_BLOB, "swapped": True, "next": True}),
                )

            self.step("Rollback restores per-service baselines and stale writes keep being fenced")
            self.run_cli("rollback", "--reason", "verify recovery")
            self.wait_until(
                "rollback completes",
                lambda: self.store.get_release(rid)["status"] == "rolled_back",
                timeout=15,
            )
            baseline_rows = {
                r["service_name"]: r for r in self.store.release_services(rid)
            }
            for name in SERVICE_NAMES:
                state = client.fetch_state(self.url(name), timeout=1.0)
                expected = json.loads(baseline_rows[name]["baseline_content"])
                self.check(
                    f"{name} restored to its own pre-release content",
                    canonical_json(state["effective"]["content"]) == canonical_json(expected),
                )
                db_row = self.store.get_service(name)
                self.check(
                    f"{name} offline observation matches restored baseline",
                    db_row["actual_content"] == canonical_json(expected),
                )
            self.settle()
            rollback_rounds = self.new_rounds()
            self.assert_unchanged_rounds_within_budget(rollback_rounds, "post-rollback steady state")

            rel_row = self.store.get_release(rid)
            target_row = baseline_rows["svc-1"]
            try:
                client.apply_config(
                    self.url("svc-1"),
                    command_id=target_row["apply_command_id"],
                    fence=int(rel_row["apply_fence"]),
                    release_id=rid,
                    mode="publish",
                    version=rel_row["target_version"],
                    content=json.loads(rel_row["target_content"]),
                    timeout=2,
                )
                raise AssertionError("late stale publish was unexpectedly accepted")
            except client.ServiceRejected as e:
                self.check(
                    "late old publish rejected with 409 stale_fence after rollback",
                    e.status == 409 and e.code == "stale_fence",
                    str(e),
                )

            all_rounds = self.traffic_rounds()
            # Steady-state rounds are everything collected before the publish
            # was submitted; later full observations legitimately accompany
            # the publish/rollback configuration changes themselves.
            publish_started_at = self.store.get_release(rid)["created_at"]
            steady_rounds = [r for r in all_rounds if r["ts"] < publish_started_at]
            total_full = self.full_fetches(steady_rounds)
            self.info(f"full-content observations during steady state: {total_full}")
            self.check(
                "steady-state full fetches happened only for the cold backfill and the same-name drift",
                total_full == {"svc-1": 1, "svc-3": 1},
                str(total_full),
            )
            unchanged_sizes = [
                item["response_bytes"]
                for entry in all_rounds
                for item in entry["services"].values()
                if item.get("ok") and item["changed"] is False
            ]
            self.check(
                f"every unchanged probe body <= {SUMMARY_BODY_BYTES} summary size",
                bool(unchanged_sizes) and max(unchanged_sizes) <= SUMMARY_BODY_BYTES,
                f"max={max(unchanged_sizes) if unchanged_sizes else 'n/a'}",
            )
            # Every changed observation must have exceeded the 2 KiB budget,
            # proving full payloads actually travel on real changes.
            changed_sizes = [
                item["response_bytes"]
                for entry in all_rounds
                for item in entry["services"].values()
                if item.get("ok") and item["changed"] is True
            ]
            self.check(
                "every changed observation carried the full >2 KiB payload",
                bool(changed_sizes) and min(changed_sizes) > UNCHANGED_BUDGET_BYTES,
                f"min={min(changed_sizes) if changed_sizes else 'n/a'}",
            )

            elapsed = time.monotonic() - started_at
            self.banner(
                f"VERIFY PASSED in {elapsed:.1f}s — {len(self.passed_checks)} checks\n"
                f"home (incl. probe_traffic.jsonl): {self.home}"
            )
            return 0
        except Exception as e:
            self.banner("VERIFY FAILED")
            print(f"{type(e).__name__}: {e}", file=sys.stderr)
            traceback.print_exc()
            print(f"\nHome retained for inspection: {self.home}", file=sys.stderr)
            return 1
        finally:
            self.cleanup()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Verify probe byte budgets and recovery behavior")
    parser.parse_args(argv)
    return TrafficVerifier().run()


if __name__ == "__main__":
    raise SystemExit(main())
