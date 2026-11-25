from __future__ import annotations

import argparse
import fcntl
import json
import os
import time
import traceback
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from typing import Any

from . import client
from .storage import Store
from .util import DEFAULT_HOME, canonical_json, now_ts


class Coordinator:
    def __init__(self, home: str | Path, interval: float = 0.15):
        self.home = Path(home)
        self.store = Store(self.home)
        self.interval = interval
        self.cluster = self.store.load_cluster()
        # Probing must never share workers with apply/rollback: a suspended
        # publish would otherwise starve the very rollback that unblocks it.
        workers = max(8, len(self.cluster["services"]) + 2)
        self.probe_pool = ThreadPoolExecutor(max_workers=workers)
        # One worker per service per direction (publish + rollback) so rollback
        # requests are dispatched even while publish requests are still pending.
        self.action_pool = ThreadPoolExecutor(max_workers=max(8, len(self.cluster["services"]) * 2 + 2))
        self.apply_inflight: dict[str, Future] = {}
        self.rollback_inflight: dict[str, Future] = {}
        self.probe_inflight: dict[str, tuple[Future, float]] = {}
        # Which release the in-flight apply/rollback futures belong to. Futures
        # are only reused within the same release; a new release starts clean.
        self._apply_release_id: str | None = None
        self._rollback_release_id: str | None = None
        self._lock_fd: Any | None = None

    def acquire_singleton(self) -> None:
        self.store.init_db()
        self._lock_fd = open(self.store.lock_path, "w", encoding="utf-8")
        try:
            fcntl.flock(self._lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit(
                f"another coordinator already holds {self.store.lock_path}; not starting a second reconciler"
            )
        self._lock_fd.seek(0)
        self._lock_fd.truncate()
        self._lock_fd.write(str(os.getpid()))
        self._lock_fd.flush()
        (self.home / "coordinator.pid").write_text(str(os.getpid()), encoding="utf-8")

    def close(self) -> None:
        for pool in (self.action_pool, self.probe_pool):
            pool.shutdown(wait=False, cancel_futures=True)
        if self._lock_fd is not None:
            try:
                fcntl.flock(self._lock_fd.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
            self._lock_fd.close()

    def probe_services(self) -> None:
        services = self.store.list_services()
        by_name = {row["name"]: row for row in services}
        # Drop probes for services that no longer exist.
        for name in list(self.probe_inflight):
            if name not in by_name:
                self.probe_inflight.pop(name, None)
        # Any apply/rollback that completes after its probe started was
        # confirmed by the response itself; the older probe must not overwrite it.
        barrier = now_ts()

        # Non-blocking probing: each service keeps at most one probe on the wire.
        # A slow/unreachable service can therefore never stall the reconcile
        # tick that dispatches a rollback.
        for row in services:
            if row["name"] not in self.probe_inflight:
                self.probe_inflight[row["name"]] = (
                    self.probe_pool.submit(client.fetch_state, row["base_url"], 0.65),
                    barrier,
                )

        updates: list[tuple[Any, ...]] = []
        for name in list(self.probe_inflight):
            future, started_at = self.probe_inflight[name]
            if not future.done():
                continue
            row = by_name.get(name)
            if row is None:
                continue
            try:
                state = future.result()
                effective = state.get("effective") or {}
                content = effective.get("content")
                updates.append(
                    (
                        1,
                        effective.get("version"),
                        canonical_json(content) if content is not None else None,
                        int(state.get("fence", row["actual_fence"] or 0)),
                        started_at,
                        now_ts(),
                        None,
                        row["name"],
                    )
                )
            except Exception as e:
                updates.append(
                    (0, None, None, None, started_at, now_ts(), str(e)[:300], row["name"])
                )
            del self.probe_inflight[name]

        if not updates:
            return
        with self.store.connect() as conn:
            for reachable, version, content, fence, started_at, ts, error, name in updates:
                if reachable:
                    # A probe that finished after an apply/rollback confirmation
                    # landed must not regress the ledger with its older view.
                    conn.execute(
                        """
                        UPDATE services
                        SET reachable=1, actual_version=?, actual_content=?,
                            actual_fence=?, last_checked=?, last_error=NULL
                        WHERE name=? AND (last_checked IS NULL OR last_checked <= ?)
                        """,
                        (version, content, fence, ts, name, started_at),
                    )
                else:
                    conn.execute(
                        """
                        UPDATE services
                        SET reachable=0, last_checked=?, last_error=?
                        WHERE name=? AND (last_checked IS NULL OR last_checked <= ?)
                        """,
                        (ts, error, name, started_at),
                    )
            conn.commit()

    def _apply_one(
        self,
        svc: Any,
        rs: Any,
        *,
        command_id: str,
        fence: int,
        mode: str,
        version: str,
        content: dict[str, Any],
    ) -> dict[str, Any]:
        try:
            resp = client.apply_config(
                svc["base_url"],
                command_id=command_id,
                fence=fence,
                release_id=rs["release_id"],
                mode=mode,
                version=version,
                content=content,
                timeout=1.15,
            )
            effective = resp.get("effective") or {}
            if effective.get("version") != version or canonical_json(
                effective.get("content")
            ) != canonical_json(content):
                return {
                    "ok": False,
                    "error": "effective_config_mismatch",
                    "detail": f"service returned {effective.get('version')}",
                    "effective": effective,
                }
            return {"ok": True, "response": resp, "effective": effective}
        except client.ServiceRejected as e:
            return {"ok": False, "error": e.code, "detail": e.message, "status": e.status}
        except Exception as e:
            return {"ok": False, "error": "unreachable_or_timeout", "detail": str(e)[:300]}

    def _submit_apply(self, rs: Any, svc: Any, release: Any, target_content: dict[str, Any]) -> None:
        self.apply_inflight[rs["service_name"]] = self.action_pool.submit(
            self._apply_one,
            svc,
            rs,
            command_id=rs["apply_command_id"],
            fence=int(release["apply_fence"]),
            mode="publish",
            version=release["target_version"],
            content=target_content,
        )

    def reconcile_apply(self, release: Any) -> None:
        if self._apply_release_id != release["id"]:
            self.apply_inflight.clear()
            self._apply_release_id = release["id"]
        rows = self.store.release_services(release["id"])
        pending = [r for r in rows if r["started"] and not r["applied_confirmed"]]
        if not pending:
            self.advance_if_complete(release["id"])
            return

        target_content = json.loads(release["target_content"])
        services = {s["name"]: s for s in self.store.list_services()}
        pending_names = {rs["service_name"] for rs in pending}
        # A pending publish from an earlier tick is still on the wire (typically
        # under an injected delay); keep it instead of piling up duplicate sends.
        # This does not block the tick: a rollback accepted meanwhile is
        # reconciled on its own path.
        for name in list(self.apply_inflight):
            if name not in pending_names:
                self.apply_inflight.pop(name, None)
        for rs in pending:
            if rs["service_name"] not in self.apply_inflight:
                self._submit_apply(rs, services[rs["service_name"]], release, target_content)

        results: dict[str, dict[str, Any]] = {}
        for name in list(pending_names):
            future = self.apply_inflight.get(name)
            if future is None or not future.done():
                continue
            try:
                results[name] = future.result()
            except Exception as e:
                results[name] = {
                    "ok": False,
                    "error": "coordinator_task_error",
                    "detail": str(e)[:300],
                }
            del self.apply_inflight[name]

        if results:
            with self.store.connect() as conn:
                for rs in pending:
                    result = results.get(rs["service_name"])
                    if result is None:
                        continue
                    if result["ok"]:
                        eff = result["effective"]
                        conn.execute(
                            """
                            UPDATE release_services
                            SET applied_confirmed=1, apply_attempts=apply_attempts+1, last_error=''
                            WHERE release_id=? AND service_name=?
                            """,
                            (release["id"], rs["service_name"]),
                        )
                        conn.execute(
                            """
                            UPDATE services
                            SET reachable=1, actual_version=?, actual_content=?, last_checked=?, last_error=NULL
                            WHERE name=?
                            """,
                            (
                                eff.get("version"),
                                canonical_json(eff.get("content")),
                                now_ts(),
                                rs["service_name"],
                            ),
                        )
                        self.store.add_event(
                            conn,
                            release["id"],
                            "service_confirmed",
                            f"{rs['service_name']} confirmed {release['target_version']}",
                        )
                    else:
                        detail = f"{result.get('error')}: {result.get('detail', '')}"[:300]
                        conn.execute(
                            """
                            UPDATE release_services
                            SET apply_attempts=apply_attempts+1, last_error=?
                            WHERE release_id=? AND service_name=?
                            """,
                            (detail, release["id"], rs["service_name"]),
                        )
                        conn.execute(
                            "UPDATE services SET reachable=0, last_error=?, last_checked=? WHERE name=?",
                            (detail, now_ts(), rs["service_name"]),
                        )
                conn.commit()

        self.advance_if_complete(release["id"])

    def advance_if_complete(self, release_id: str) -> None:
        with self.store.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                release = conn.execute(
                    "SELECT * FROM releases WHERE id=?", (release_id,)
                ).fetchone()
                if release is None or release["status"] != "running" or release["rollback_requested"]:
                    conn.rollback()
                    return

                pending = conn.execute(
                    """
                    SELECT COUNT(*) AS n FROM release_services
                    WHERE release_id=? AND started=1 AND applied_confirmed=0
                    """,
                    (release_id,),
                ).fetchone()["n"]
                if pending:
                    conn.rollback()
                    return

                batches = json.loads(release["batches"])
                idx = int(release["batch_index"])
                if release["pause_requested"]:
                    conn.execute(
                        "UPDATE releases SET status='paused', updated_at=? WHERE id=?",
                        (now_ts(), release_id),
                    )
                    if idx + 1 < len(batches):
                        message = f"batch {idx + 1}/{len(batches)} confirmed; paused before next batch"
                    else:
                        message = f"final batch {idx + 1}/{len(batches)} confirmed; pause accepted, resume to mark release completed"
                    self.store.add_event(conn, release_id, "paused_at_boundary", message)
                elif idx + 1 < len(batches):
                    next_idx = idx + 1
                    conn.execute(
                        "UPDATE releases SET batch_index=?, updated_at=? WHERE id=?",
                        (next_idx, now_ts(), release_id),
                    )
                    conn.execute(
                        "UPDATE release_services SET started=1 WHERE release_id=? AND batch_index=?",
                        (release_id, next_idx),
                    )
                    names = [
                        r["service_name"]
                        for r in conn.execute(
                            "SELECT service_name FROM release_services WHERE release_id=? AND batch_index=? ORDER BY service_name",
                            (release_id, next_idx),
                        )
                    ]
                    self.store.add_event(
                        conn,
                        release_id,
                        "batch_started",
                        f"batch {next_idx + 1}/{len(batches)} started: {', '.join(names)}",
                    )
                else:
                    conn.execute(
                        "UPDATE releases SET status='completed', completed_at=?, updated_at=? WHERE id=?",
                        (now_ts(), now_ts(), release_id),
                    )
                    self.store.add_event(
                        conn, release_id, "completed", "all batches confirmed; release completed"
                    )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def reconcile_rollback(self, release: Any) -> None:
        if self._rollback_release_id != release["id"]:
            self.rollback_inflight.clear()
            self._rollback_release_id = release["id"]
        rows = self.store.release_services(release["id"])
        pending = [r for r in rows if r["started"] and not r["rollback_confirmed"]]

        if release["rollback_status"] == "requested":
            with self.store.connect() as conn:
                conn.execute(
                    "UPDATE releases SET rollback_status='in_progress', updated_at=? WHERE id=?",
                    (now_ts(), release["id"]),
                )
                self.store.add_event(
                    conn, release["id"], "rollback_started", "reconciling restoration of started batches"
                )
                conn.commit()

        if pending:
            services = {s["name"]: s for s in self.store.list_services()}
            fence = release["rollback_fence"]
            if fence is None:
                raise RuntimeError(f"release {release['id']} has no rollback fence")
            pending_names = {rs["service_name"] for rs in pending}
            for name in list(self.rollback_inflight):
                if name not in pending_names:
                    self.rollback_inflight.pop(name, None)
            for rs in pending:
                if rs["service_name"] in self.rollback_inflight:
                    continue
                baseline_content = json.loads(rs["baseline_content"])
                self.rollback_inflight[rs["service_name"]] = self.action_pool.submit(
                    self._apply_one,
                    services[rs["service_name"]],
                    rs,
                    command_id=rs["rollback_command_id"],
                    fence=int(fence),
                    mode="rollback",
                    version=rs["baseline_version"],
                    content=baseline_content,
                )

            results: dict[str, dict[str, Any]] = {}
            for name in list(pending_names):
                future = self.rollback_inflight.get(name)
                if future is None or not future.done():
                    continue
                try:
                    results[name] = future.result()
                except Exception as e:
                    results[name] = {
                        "ok": False,
                        "error": "coordinator_task_error",
                        "detail": str(e)[:300],
                    }
                del self.rollback_inflight[name]

            if results:
                with self.store.connect() as conn:
                    for rs in pending:
                        result = results.get(rs["service_name"])
                        if result is None:
                            continue
                        if result["ok"]:
                            eff = result["effective"]
                            conn.execute(
                                """
                                UPDATE release_services
                                SET rollback_confirmed=1, rollback_attempts=rollback_attempts+1, last_error=''
                                WHERE release_id=? AND service_name=?
                                """,
                                (release["id"], rs["service_name"]),
                            )
                            conn.execute(
                                """
                                UPDATE services
                                SET reachable=1, actual_version=?, actual_content=?, last_checked=?, last_error=NULL
                                WHERE name=?
                                """,
                                (
                                    eff.get("version"),
                                    canonical_json(eff.get("content")),
                                    now_ts(),
                                    rs["service_name"],
                                ),
                            )
                            self.store.add_event(
                                conn,
                                release["id"],
                                "rollback_service_confirmed",
                                f"{rs['service_name']} restored to {rs['baseline_version']}",
                            )
                        else:
                            detail = f"{result.get('error')}: {result.get('detail', '')}"[:300]
                            conn.execute(
                                """
                                UPDATE release_services
                                SET rollback_attempts=rollback_attempts+1, last_error=?
                                WHERE release_id=? AND service_name=?
                                """,
                                (detail, release["id"], rs["service_name"]),
                            )
                            conn.execute(
                                "UPDATE services SET reachable=0, last_error=?, last_checked=? WHERE name=?",
                                (detail, now_ts(), rs["service_name"]),
                            )
                    conn.commit()

        self.complete_rollback_if_done(release["id"])

    def complete_rollback_if_done(self, release_id: str) -> None:
        with self.store.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                release = conn.execute(
                    "SELECT * FROM releases WHERE id=?", (release_id,)
                ).fetchone()
                if release is None or not release["rollback_requested"]:
                    conn.rollback()
                    return
                unconfirmed = conn.execute(
                    """
                    SELECT COUNT(*) AS n FROM release_services
                    WHERE release_id=? AND started=1 AND rollback_confirmed=0
                    """,
                    (release_id,),
                ).fetchone()["n"]
                if unconfirmed:
                    conn.rollback()
                    return
                conn.execute(
                    """
                    UPDATE releases
                    SET status='rolled_back', rollback_status='complete',
                        completed_at=?, updated_at=?
                    WHERE id=?
                    """,
                    (now_ts(), now_ts(), release_id),
                )
                self.store.add_event(
                    conn,
                    release_id,
                    "rollback_completed",
                    "every started service confirmed its pre-release configuration",
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def tick(self) -> None:
        self.probe_services()
        release = self.store.active_release()
        if release is None:
            return
        if release["rollback_requested"]:
            self.reconcile_rollback(release)
        elif release["status"] == "running":
            self.reconcile_apply(release)
        # status == paused: nothing to send until resume.

    def run_forever(self) -> int:
        self.acquire_singleton()
        print(
            f"coordinator {os.getpid()} watching group={self.cluster.get('group')} "
            f"home={self.home} interval={self.interval:.2f}s",
            flush=True,
        )
        try:
            while True:
                started = time.monotonic()
                try:
                    self.tick()
                except SystemExit:
                    raise
                except Exception:
                    print("coordinator tick failed:", flush=True)
                    traceback.print_exc()
                elapsed = time.monotonic() - started
                sleep_for = max(0.02, self.interval - elapsed)
                time.sleep(sleep_for)
        except KeyboardInterrupt:
            print("coordinator interrupted; releasing lock", flush=True)
        finally:
            self.close()
        return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run persistent release reconciliation loop")
    parser.add_argument("--home", default=str(DEFAULT_HOME))
    parser.add_argument("--interval", type=float, default=float(os.environ.get("RELCOORD_INTERVAL", "0.15")))
    args = parser.parse_args(argv)
    return Coordinator(args.home, interval=args.interval).run_forever()


if __name__ == "__main__":
    raise SystemExit(main())
