from __future__ import annotations

import argparse
import fcntl
import json
import os
import threading
import time
import traceback
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from . import client
from .storage import Store
from .util import DEFAULT_HOME, GROUP, canonical_json, now_ts

MAX_SERVICE_REQUESTS = 8


class RequestScheduler:
    """Fair owner queues over a hard cap of service-request worker threads."""

    def __init__(self, limit: int = MAX_SERVICE_REQUESTS, fair_owners: int = 1):
        self.limit = max(1, int(limit))
        # When several groups share the scheduler, ceil(limit / groups) keeps a
        # stuck group from being able to occupy every slot before a healthy
        # group is scheduled.  It is a cap per owner, not a reduction of the
        # global hard limit.
        self.fair_owners = max(1, int(fair_owners or 1))
        self.pool = ThreadPoolExecutor(
            max_workers=self.limit, thread_name_prefix="relcoord-service-request"
        )
        self._lock = threading.Lock()
        self._queues: dict[str, deque[tuple[Any, ...]]] = {}
        self._owners: deque[str] = deque()
        self._owner_queued: set[str] = set()
        self._active = 0
        self._active_by_owner: dict[str, int] = {}
        self.peak_active = 0
        self.started = 0

    @property
    def owner_limit(self) -> int:
        if self.fair_owners <= 1:
            return self.limit
        return max(1, (self.limit + self.fair_owners - 1) // self.fair_owners)

    def submit(self, owner: str, fn: Any, /, *args: Any, **kwargs: Any) -> Future:
        future: Future = Future()
        job = (fn, args, kwargs, future)
        with self._lock:
            queue = self._queues.setdefault(owner, deque())
            queue.append(job)
            if owner not in self._owner_queued:
                self._owner_queued.add(owner)
                self._owners.append(owner)
            self._pump_locked()
        return future

    def _pump_locked(self) -> None:
        while self._active < self.limit and self._owners:
            dispatched = False
            # Scan the round-robin owner queue once.  Skipping an owner that
            # has consumed its fair share lets another group use free slots.
            for _ in range(len(self._owners)):
                if self._active >= self.limit:
                    break
                owner = self._owners[0]
                queue = self._queues.get(owner)
                if not queue:
                    self._owners.popleft()
                    self._owner_queued.discard(owner)
                    self._queues.pop(owner, None)
                    continue
                if self._active_by_owner.get(owner, 0) >= self.owner_limit:
                    self._owners.rotate(-1)
                    continue

                fn, args, kwargs, future = queue.popleft()
                if not queue:
                    self._owners.popleft()
                    self._owner_queued.discard(owner)
                    self._queues.pop(owner, None)
                else:
                    self._owners.rotate(-1)

                self._active += 1
                self._active_by_owner[owner] = self._active_by_owner.get(owner, 0) + 1
                self.started += 1
                self.peak_active = max(self.peak_active, self._active)
                self.pool.submit(self._run_job, owner, fn, args, kwargs, future)
                dispatched = True
                break
            if not dispatched:
                return

    def _run_job(
        self,
        owner: str,
        fn: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        future: Future,
    ) -> None:
        try:
            result = fn(*args, **kwargs)
            if not future.done():
                future.set_result(result)
        except BaseException as exc:  # surfaced to the caller via future
            if not future.done():
                future.set_exception(exc)
        finally:
            with self._lock:
                self._active -= 1
                owner_active = self._active_by_owner.get(owner, 1) - 1
                if owner_active <= 0:
                    self._active_by_owner.pop(owner, None)
                else:
                    self._active_by_owner[owner] = owner_active
                self._pump_locked()

    def shutdown(self) -> None:
        with self._lock:
            for queue in self._queues.values():
                while queue:
                    _, _, _, future = queue.popleft()
                    if not future.done():
                        future.cancel()
            self._queues.clear()
            self._owners.clear()
            self._owner_queued.clear()
            self._active_by_owner.clear()
        self.pool.shutdown(wait=False, cancel_futures=True)


class Coordinator:
    def __init__(self, home: str | Path, interval: float = 0.15):
        self.home = Path(home)
        self.store = Store(self.home)
        self.interval = interval
        self.cluster = self.store.load_cluster() if self.store.cluster_path.exists() else {"groups": {}}
        # Probes and apply/rollback writes all pass through this one hard cap.
        self.scheduler = RequestScheduler(MAX_SERVICE_REQUESTS)
        self.pool = self.scheduler.pool
        self._lock_fd: Any | None = None
        self._closing = False
        self._threads_lock = threading.Lock()
        self._release_threads: dict[str, threading.Thread] = {}
        self._probe_thread: threading.Thread | None = None

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
        self._closing = True
        self.scheduler.shutdown()
        if self._lock_fd is not None:
            try:
                fcntl.flock(self._lock_fd.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
            self._lock_fd.close()

    def _probe_one(self, row: Any) -> tuple[Any, ...]:
        try:
            state = client.fetch_state(row["base_url"], 0.65)
            effective = state.get("effective") or {}
            content = effective.get("content")
            return (
                1,
                effective.get("version"),
                canonical_json(content) if content is not None else None,
                int(state.get("fence", row["actual_fence"] or 0)),
                now_ts(),
                None,
                row["group_name"],
                row["name"],
            )
        except Exception as e:
            return (
                0,
                None,
                None,
                None,
                now_ts(),
                str(e)[:300],
                row["group_name"],
                row["name"],
            )

    def probe_services(self) -> None:
        services = self.store.list_services()
        future_map = {
            self.scheduler.submit(
                row["group_name"], self._probe_one, row
            ): row
            for row in services
        }
        updates: list[tuple[Any, ...]] = []
        for future in as_completed(future_map):
            row = future_map[future]
            try:
                updates.append(future.result())
            except Exception as e:
                updates.append(
                    (
                        0,
                        None,
                        None,
                        None,
                        now_ts(),
                        f"coordinator_task_error: {e}"[:300],
                        row["group_name"],
                        row["name"],
                    )
                )

        if not updates:
            return
        with self.store.connect() as conn:
            for reachable, version, content, fence, ts, error, group_name, name in updates:
                if reachable:
                    conn.execute(
                        """
                        UPDATE services
                        SET reachable=1, actual_version=?, actual_content=?,
                            actual_fence=?, last_checked=?, last_error=NULL
                        WHERE group_name=? AND name=?
                        """,
                        (version, content, fence, ts, group_name, name),
                    )
                else:
                    conn.execute(
                        """
                        UPDATE services
                        SET reachable=0, last_checked=?, last_error=?
                        WHERE group_name=? AND name=?
                        """,
                        (ts, error, group_name, name),
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

    def _record_apply_result(
        self, release: Any, rs: Any, result: dict[str, Any]
    ) -> None:
        group_name = release["group_name"]
        name = rs["service_name"]
        with self.store.connect() as conn:
            if result["ok"]:
                eff = result["effective"]
                conn.execute(
                    """
                    UPDATE release_services
                    SET applied_confirmed=1, apply_attempts=apply_attempts+1, last_error=''
                    WHERE release_id=? AND service_name=?
                    """,
                    (release["id"], name),
                )
                conn.execute(
                    """
                    UPDATE services
                    SET reachable=1, actual_version=?, actual_content=?,
                        actual_fence=?, last_checked=?, last_error=NULL
                    WHERE group_name=? AND name=?
                    """,
                    (
                        eff.get("version"),
                        canonical_json(eff.get("content")),
                        int(eff.get("fence", release["apply_fence"])),
                        now_ts(),
                        group_name,
                        name,
                    ),
                )
                self.store.add_event(
                    conn,
                    release["id"],
                    "service_confirmed",
                    f"{name} confirmed {release['target_version']}",
                )
            else:
                detail = f"{result.get('error')}: {result.get('detail', '')}"[:300]
                conn.execute(
                    """
                    UPDATE release_services
                    SET apply_attempts=apply_attempts+1, last_error=?
                    WHERE release_id=? AND service_name=?
                    """,
                    (detail, release["id"], name),
                )
                conn.execute(
                    """
                    UPDATE services
                    SET reachable=0, last_error=?, last_checked=?
                    WHERE group_name=? AND name=?
                    """,
                    (detail, now_ts(), group_name, name),
                )
            conn.commit()

    def reconcile_apply(self, release: Any) -> None:
        rows = self.store.release_services(release["id"])
        pending = [r for r in rows if r["started"] and not r["applied_confirmed"]]
        if not pending:
            self.advance_if_complete(release["id"])
            return

        target_content = json.loads(release["target_content"])
        services = {
            (s["group_name"], s["name"]): s for s in self.store.list_services(release["group_name"])
        }
        futures = {}
        for rs in pending:
            svc = services[(release["group_name"], rs["service_name"])]
            future = self.scheduler.submit(
                release["group_name"],
                self._apply_one,
                svc,
                rs,
                command_id=rs["apply_command_id"],
                fence=int(release["apply_fence"]),
                mode="publish",
                version=release["target_version"],
                content=target_content,
            )
            futures[future] = rs

        # Persist completions immediately.  A slow member only delays its own
        # batch/group; it cannot block SQLite observation of fast completions.
        for future in as_completed(futures):
            rs = futures[future]
            try:
                result = future.result()
            except Exception as e:
                result = {
                    "ok": False,
                    "error": "coordinator_task_error",
                    "detail": str(e)[:300],
                }
            self._record_apply_result(release, rs, result)
            self.advance_if_complete(release["id"])

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

    def _record_rollback_result(
        self, release: Any, rs: Any, result: dict[str, Any]
    ) -> None:
        group_name = release["group_name"]
        name = rs["service_name"]
        with self.store.connect() as conn:
            if result["ok"]:
                eff = result["effective"]
                conn.execute(
                    """
                    UPDATE release_services
                    SET rollback_confirmed=1, rollback_attempts=rollback_attempts+1, last_error=''
                    WHERE release_id=? AND service_name=?
                    """,
                    (release["id"], name),
                )
                conn.execute(
                    """
                    UPDATE services
                    SET reachable=1, actual_version=?, actual_content=?,
                        actual_fence=?, last_checked=?, last_error=NULL
                    WHERE group_name=? AND name=?
                    """,
                    (
                        eff.get("version"),
                        canonical_json(eff.get("content")),
                        int(release["rollback_fence"] or 0),
                        now_ts(),
                        group_name,
                        name,
                    ),
                )
                self.store.add_event(
                    conn,
                    release["id"],
                    "rollback_service_confirmed",
                    f"{name} restored to {rs['baseline_version']}",
                )
            else:
                detail = f"{result.get('error')}: {result.get('detail', '')}"[:300]
                conn.execute(
                    """
                    UPDATE release_services
                    SET rollback_attempts=rollback_attempts+1, last_error=?
                    WHERE release_id=? AND service_name=?
                    """,
                    (detail, release["id"], name),
                )
                conn.execute(
                    """
                    UPDATE services
                    SET reachable=0, last_error=?, last_checked=?
                    WHERE group_name=? AND name=?
                    """,
                    (detail, now_ts(), group_name, name),
                )
            conn.commit()

    def reconcile_rollback(self, release: Any) -> None:
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
            services = {
                (s["group_name"], s["name"]): s
                for s in self.store.list_services(release["group_name"])
            }
            fence = release["rollback_fence"]
            if fence is None:
                raise RuntimeError(f"release {release['id']} has no rollback fence")
            futures = {}
            for rs in pending:
                baseline_content = json.loads(rs["baseline_content"])
                future = self.scheduler.submit(
                    release["group_name"],
                    self._apply_one,
                    services[(release["group_name"], rs["service_name"])],
                    rs,
                    command_id=rs["rollback_command_id"],
                    fence=int(fence),
                    mode="rollback",
                    version=rs["baseline_version"],
                    content=baseline_content,
                )
                futures[future] = rs

            for future in as_completed(futures):
                rs = futures[future]
                try:
                    result = future.result()
                except Exception as e:
                    result = {
                        "ok": False,
                        "error": "coordinator_task_error",
                        "detail": str(e)[:300],
                    }
                self._record_rollback_result(release, rs, result)
                self.complete_rollback_if_done(release["id"])

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

    def _probe_worker(self) -> None:
        while not self._closing:
            try:
                self.probe_services()
            except Exception:
                if not self._closing:
                    print("coordinator probe failed:", flush=True)
                    traceback.print_exc()
            deadline = time.monotonic() + max(0.10, self.interval)
            while not self._closing and time.monotonic() < deadline:
                time.sleep(0.02)

    def _release_worker(self, release_id: str, group_name: str) -> None:
        try:
            release = self.store.get_release(release_id)
            if release["rollback_requested"]:
                self.reconcile_rollback(release)
            elif release["status"] == "running":
                self.reconcile_apply(release)
            # paused releases have no unconfirmed started rows; resume flips
            # status back to running and the next tick starts a worker.
        except Exception:
            if not self._closing:
                print(f"coordinator group {group_name!r} tick failed:", flush=True)
                traceback.print_exc()
        finally:
            with self._threads_lock:
                current = self._release_threads.get(group_name)
                if current is threading.current_thread():
                    self._release_threads.pop(group_name, None)

    def _ensure_background_work(self) -> None:
        if not self._closing:
            if self._probe_thread is None or not self._probe_thread.is_alive():
                self._probe_thread = threading.Thread(
                    target=self._probe_worker, name="relcoord-probes", daemon=True
                )
                self._probe_thread.start()

        try:
            self.cluster = self.store.load_cluster()
        except Exception:
            pass

        active_releases = self.store.active_releases()
        # Reserve fair sharing only among groups that currently have release
        # work.  A lone active batch may still use all eight slots.
        self.scheduler.fair_owners = max(
            1, len({row["group_name"] for row in active_releases})
        )

        for release in active_releases:
            group_name = release["group_name"]
            with self._threads_lock:
                current = self._release_threads.get(group_name)
                if current is not None and current.is_alive():
                    continue
                thread = threading.Thread(
                    target=self._release_worker,
                    args=(release["id"], group_name),
                    name=f"relcoord-group-{group_name}",
                    daemon=True,
                )
                self._release_threads[group_name] = thread
                thread.start()

    def tick(self) -> None:
        # One non-blocking scheduling tick.  Each group gets its own supervisor
        # thread; all actual HTTP work still goes through the 8-request cap.
        self._ensure_background_work()

    def run_forever(self) -> int:
        self.acquire_singleton()
        groups = ",".join(g["name"] for g in self.cluster.get("groups", {}).values()) or GROUP
        print(
            f"coordinator {os.getpid()} watching groups={groups} "
            f"home={self.home} interval={self.interval:.2f}s max_requests={self.scheduler.limit}",
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
