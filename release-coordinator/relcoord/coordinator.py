from __future__ import annotations

import argparse
import fcntl
import json
import os
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from . import client
from .storage import Store
from .util import DEFAULT_HOME, canonical_json, content_hash, now_ts


class Coordinator:
    def __init__(self, home: str | Path, interval: float = 0.15):
        self.home = Path(home)
        self.store = Store(self.home)
        self.interval = interval
        self.cluster = self.store.load_cluster()
        self.pool = ThreadPoolExecutor(max_workers=max(8, len(self.cluster["services"]) + 2))
        self._lock_fd: Any | None = None
        self._probe_round = 0

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
        self.pool.shutdown(wait=False, cancel_futures=True)
        if self._lock_fd is not None:
            try:
                fcntl.flock(self._lock_fd.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
            self._lock_fd.close()

    def probe_services(self) -> None:
        """One digest-based background probe round.

        Each service is asked via GET /observe about the version, content hash
        and fence the coordinator already holds in SQLite. While the effective
        configuration is unchanged the response is a compact summary, so a
        stable large configuration is never shipped round after round — even
        when a service or the coordinator itself restarts. A changed
        observation (including a same-named version with different content)
        triggers one full-content response and the complete observation is
        refreshed in SQLite for offline queries and later rollbacks.
        """
        services = self.store.list_services()
        future_map = {
            self.pool.submit(
                client.fetch_observation,
                row["base_url"],
                version=row["actual_version"] or "",
                content_hash=row["actual_content_hash"] or "",
                fence=int(row["actual_fence"] or 0),
                timeout=0.65,
            ): row
            for row in services
        }
        results: list[dict[str, Any]] = []
        for future, row in future_map.items():
            try:
                observation, response_bytes = future.result()
                results.append(
                    {
                        "name": row["name"],
                        "ok": True,
                        "observation": observation,
                        "response_bytes": int(response_bytes),
                    }
                )
            except Exception as e:
                results.append(
                    {
                        "name": row["name"],
                        "ok": False,
                        "error": str(e)[:300],
                        "response_bytes": 0,
                    }
                )

        ts = now_ts()
        per_service: dict[str, dict[str, Any]] = {}
        with self.store.connect() as conn:
            for result in results:
                name = result["name"]
                if not result["ok"]:
                    conn.execute(
                        """
                        UPDATE services
                        SET reachable=0, last_checked=?, last_error=?,
                            last_probe_bytes=0, last_probe_changed=NULL
                        WHERE name=?
                        """,
                        (ts, result["error"], name),
                    )
                    per_service[name] = {
                        "ok": False,
                        "response_bytes": 0,
                        "changed": None,
                        "error": result["error"],
                    }
                    continue

                observation = result["observation"]
                response_bytes = result["response_bytes"]
                effective = observation.get("effective") or {}
                fence = int(observation.get("fence", 0))
                if observation.get("changed"):
                    content = effective.get("content")
                    if content is None:
                        error = "observe_changed_without_content"
                        conn.execute(
                            """
                            UPDATE services
                            SET reachable=0, last_checked=?, last_error=?,
                                last_probe_bytes=?, last_probe_changed=1
                            WHERE name=?
                            """,
                            (ts, error, response_bytes, name),
                        )
                        per_service[name] = {
                            "ok": False,
                            "response_bytes": response_bytes,
                            "changed": True,
                            "error": error,
                        }
                        continue
                    digest = effective.get("content_hash") or content_hash(content)
                    conn.execute(
                        """
                        UPDATE services
                        SET reachable=1, actual_version=?, actual_content=?,
                            actual_content_hash=?, actual_fence=?,
                            last_checked=?, last_error=NULL,
                            last_probe_bytes=?, last_probe_changed=1
                        WHERE name=?
                        """,
                        (
                            effective.get("version"),
                            canonical_json(content),
                            digest,
                            fence,
                            ts,
                            response_bytes,
                            name,
                        ),
                    )
                    per_service[name] = {
                        "ok": True,
                        "response_bytes": response_bytes,
                        "changed": True,
                        "fence": fence,
                        "version": effective.get("version"),
                        "content_hash": digest,
                    }
                else:
                    # Nothing changed: keep the complete observation in SQLite
                    # untouched and only refresh reachability/timestamps.
                    digest = effective.get("content_hash")
                    conn.execute(
                        """
                        UPDATE services
                        SET reachable=1, last_checked=?, last_error=NULL,
                            actual_fence=?, last_probe_bytes=?, last_probe_changed=0
                        WHERE name=?
                        """,
                        (ts, fence, response_bytes, name),
                    )
                    per_service[name] = {
                        "ok": True,
                        "response_bytes": response_bytes,
                        "changed": False,
                        "fence": fence,
                        "version": effective.get("version"),
                        "content_hash": digest,
                    }
            conn.commit()

        self._probe_round += 1
        self.store.append_probe_round(
            {
                "round": self._probe_round,
                "ts": ts,
                "total_response_bytes": sum(item["response_bytes"] for item in results),
                "services": per_service,
            }
        )

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

    def reconcile_apply(self, release: Any) -> None:
        rows = self.store.release_services(release["id"])
        pending = [r for r in rows if r["started"] and not r["applied_confirmed"]]
        if not pending:
            self.advance_if_complete(release["id"])
            return

        target_content = json.loads(release["target_content"])
        services = {s["name"]: s for s in self.store.list_services()}
        futures = {}
        for rs in pending:
            svc = services[rs["service_name"]]
            futures[
                self.pool.submit(
                    self._apply_one,
                    svc,
                    rs,
                    command_id=rs["apply_command_id"],
                    fence=int(release["apply_fence"]),
                    mode="publish",
                    version=release["target_version"],
                    content=target_content,
                )
            ] = rs

        results: dict[str, dict[str, Any]] = {}
        for future in futures:
            rs = futures[future]
            try:
                results[rs["service_name"]] = future.result()
            except Exception as e:
                results[rs["service_name"]] = {
                    "ok": False,
                    "error": "coordinator_task_error",
                    "detail": str(e)[:300],
                }

        with self.store.connect() as conn:
            for rs in pending:
                result = results[rs["service_name"]]
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
                        SET reachable=1, actual_version=?, actual_content=?,
                            actual_content_hash=?, last_checked=?,
                            last_error=NULL, last_probe_changed=NULL
                        WHERE name=?
                        """,
                        (
                            eff.get("version"),
                            canonical_json(eff.get("content")),
                            content_hash(eff.get("content")),
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
            futures = {}
            fence = release["rollback_fence"]
            if fence is None:
                raise RuntimeError(f"release {release['id']} has no rollback fence")
            for rs in pending:
                baseline_content = json.loads(rs["baseline_content"])
                futures[
                    self.pool.submit(
                        self._apply_one,
                        services[rs["service_name"]],
                        rs,
                        command_id=rs["rollback_command_id"],
                        fence=int(fence),
                        mode="rollback",
                        version=rs["baseline_version"],
                        content=baseline_content,
                    )
                ] = rs

            results: dict[str, dict[str, Any]] = {}
            for future in futures:
                rs = futures[future]
                try:
                    results[rs["service_name"]] = future.result()
                except Exception as e:
                    results[rs["service_name"]] = {
                        "ok": False,
                        "error": "coordinator_task_error",
                        "detail": str(e)[:300],
                    }

            with self.store.connect() as conn:
                for rs in pending:
                    result = results[rs["service_name"]]
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
                            SET reachable=1, actual_version=?, actual_content=?,
                                actual_content_hash=?, last_checked=?,
                                last_error=NULL, last_probe_changed=NULL
                            WHERE name=?
                            """,
                            (
                                eff.get("version"),
                                canonical_json(eff.get("content")),
                                content_hash(eff.get("content")),
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
