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
from .util import DEFAULT_HOME, canonical_json, now_ts


class Coordinator:
    def __init__(self, home: str | Path, interval: float = 0.15):
        self.home = Path(home)
        self.store = Store(self.home)
        # Opens/migrates the DB so attribution columns exist even when callers
        # invoke tick()/reconcile helpers directly without run_forever().
        self.store.init_db()
        self.interval = interval
        self.cluster = self.store.load_cluster()
        self.pool = ThreadPoolExecutor(max_workers=max(8, len(self.cluster["services"]) + 2))
        self._lock_fd: Any | None = None
        # Restart takeover: resolve any pending attributions that the named
        # services' own durable state files can prove, without downgrading
        # existing claims. Full `relctl verify` remains authoritative.
        try:
            from .verify import verify_runtime

            verify_runtime(
                self.home, live=False, advance=False, only_pending=True
            )
        except Exception:
            print("startup attribution verification pass failed:", flush=True)
            traceback.print_exc()

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
        services = self.store.list_services()
        future_map = {
            self.pool.submit(client.fetch_state, row["base_url"], 0.65): row
            for row in services
        }
        probes: list[dict[str, Any]] = []
        for future, row in future_map.items():
            try:
                state = future.result()
                peer = client.peer_name(state)
                if peer is not None and peer != row["name"]:
                    # The URL answers, but it is a different machine. Never
                    # import its effective config as this service's state.
                    probes.append(
                        {
                            "name": row["name"],
                            "kind": "identity_mismatch",
                            "peer": peer,
                            "ts": now_ts(),
                            "error": (
                                f"peer_identity_mismatch: endpoint at {row['base_url']} "
                                f"identifies as {peer!r}, expected {row['name']!r}"
                            ),
                        }
                    )
                    continue
                effective = state.get("effective") or {}
                content = effective.get("content")
                probes.append(
                    {
                        "name": row["name"],
                        "kind": "ok",
                        "peer": peer or row["name"],
                        "version": effective.get("version"),
                        "content": canonical_json(content) if content is not None else None,
                        "fence": int(state.get("fence", row["actual_fence"] or 0)),
                        "ts": now_ts(),
                    }
                )
            except Exception as e:
                probes.append(
                    {
                        "name": row["name"],
                        "kind": "unreachable",
                        "ts": now_ts(),
                        "error": str(e)[:300],
                    }
                )

        if not probes:
            return
        with self.store.connect() as conn:
            for p in probes:
                if p["kind"] == "ok":
                    conn.execute(
                        """
                        UPDATE services
                        SET reachable=1, identity_ok=1, peer_name=?,
                            actual_version=?, actual_content=?,
                            actual_fence=?, last_checked=?, last_error=NULL
                        WHERE name=?
                        """,
                        (
                            p["peer"],
                            p["version"],
                            p["content"],
                            p["fence"],
                            p["ts"],
                            p["name"],
                        ),
                    )
                elif p["kind"] == "identity_mismatch":
                    conn.execute(
                        """
                        UPDATE services
                        SET reachable=0, identity_ok=0, peer_name=?,
                            last_checked=?, last_error=?
                        WHERE name=?
                        """,
                        (p["peer"], p["ts"], p["error"], p["name"]),
                    )
                else:
                    conn.execute(
                        """
                        UPDATE services
                        SET reachable=0, last_checked=?, last_error=?
                        WHERE name=?
                        """,
                        (p["ts"], p["error"], p["name"]),
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
                service_name=svc["name"],
                timeout=1.15,
            )
            peer = client.peer_name(resp)
            if peer is not None and peer != svc["name"]:
                # Defence in depth: a pre-identity service may have accepted
                # the write, but we must never attribute that confirmation.
                return {
                    "ok": False,
                    "error": "peer_identity_mismatch",
                    "detail": (
                        f"expected peer {svc['name']!r}, response identifies as {peer!r}"
                    ),
                    "effective": resp.get("effective"),
                }
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
        # A confirmation only counts when its peer attribution is verified.
        # Legacy confirmations parked in pending_verification are retried with
        # the same stable command id, which is idempotent on the true peer.
        pending = [
            r
            for r in rows
            if r["started"]
            and not (r["applied_confirmed"] and r["apply_attribution"] == "verified")
        ]
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
                        SET applied_confirmed=1, apply_attribution='verified',
                            apply_attempts=apply_attempts+1, last_error=''
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
                    WHERE release_id=? AND started=1
                      AND NOT (applied_confirmed=1 AND apply_attribution='verified')
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

    def verify_attribution(self, *, live: bool = False, advance: bool = True,
                           sync_endpoints: bool = False) -> dict[str, Any]:
        """Run the in-place peer-attribution verification pass."""
        from .verify import verify_runtime

        return verify_runtime(
            self.home,
            live=live,
            advance=advance,
            sync_endpoints=sync_endpoints,
        )

    def _corroborate_pending_baseline(self, rs: Any, svc: Any) -> bool:
        """Upgrade a pending baseline when the named peer's own durable
        history/effective config proves it belongs to that service."""
        from .verify import _config_matches, _read_state_file

        state, _ = _read_state_file(svc["state_path"])
        if not isinstance(state, dict) or state.get("name") != rs["service_name"]:
            return False
        try:
            baseline_canonical = canonical_json(json.loads(rs["baseline_content"]))
        except (TypeError, ValueError):
            return False
        source = _config_matches(state, rs["baseline_version"], baseline_canonical)
        if source is None:
            return False
        with self.store.connect() as conn:
            conn.execute(
                "UPDATE release_services SET baseline_attribution='verified' "
                "WHERE release_id=? AND service_name=?",
                (rs["release_id"], rs["service_name"]),
            )
            self.store.add_event(
                conn,
                rs["release_id"],
                "baseline_attribution_verified",
                f"{rs['service_name']}: baseline {rs['baseline_version']} "
                f"corroborated by its own {source}",
            )
            conn.commit()
        return True

    def reconcile_rollback(self, release: Any) -> None:
        rows = self.store.release_services(release["id"])
        pending = [
            r
            for r in rows
            if r["started"]
            and not (
                r["rollback_confirmed"] and r["rollback_attribution"] == "verified"
            )
        ]

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

        services_by_name = {s["name"]: s for s in self.store.list_services()}

        # Re-check pending baselines against the named service's own durable
        # evidence first; this safely picks up an operator-corrected baseline.
        baseline_ok: dict[str, bool] = {}
        for rs in pending:
            if rs["baseline_attribution"] == "verified":
                baseline_ok[rs["service_name"]] = True
                continue
            svc = services_by_name.get(rs["service_name"])
            baseline_ok[rs["service_name"]] = bool(
                svc is not None and self._corroborate_pending_baseline(rs, svc)
            )

        # A baseline whose attribution could not be verified must never be
        # pushed to a service: it may belong to a crossed peer. Park those
        # rows with an explanatory error instead of sending anything.
        blocked_baseline = [r for r in pending if not baseline_ok[r["service_name"]]]
        sendable = [r for r in pending if baseline_ok[r["service_name"]]]
        if blocked_baseline:
            with self.store.connect() as conn:
                for rs in blocked_baseline:
                    detail = (
                        "baseline_pending_verification: refusing to restore "
                        f"{rs['service_name']} from an unattributed baseline; "
                        "run `relctl verify` after correcting the endpoint"
                    )
                    if rs["last_error"] != detail:
                        conn.execute(
                            "UPDATE release_services SET last_error=? "
                            "WHERE release_id=? AND service_name=?",
                            (detail, release["id"], rs["service_name"]),
                        )
                        self.store.add_event(
                            conn,
                            release["id"],
                            "rollback_blocked_pending_verification",
                            detail,
                        )
                conn.commit()

        if sendable:
            futures = {}
            fence = release["rollback_fence"]
            if fence is None:
                raise RuntimeError(f"release {release['id']} has no rollback fence")
            for rs in sendable:
                baseline_content = json.loads(rs["baseline_content"])
                futures[
                    self.pool.submit(
                        self._apply_one,
                        services_by_name[rs["service_name"]],
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
                for rs in sendable:
                    result = results[rs["service_name"]]
                    if result["ok"]:
                        eff = result["effective"]
                        conn.execute(
                            """
                            UPDATE release_services
                            SET rollback_confirmed=1, rollback_attribution='verified',
                                rollback_attempts=rollback_attempts+1, last_error=''
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
                    WHERE release_id=? AND started=1
                      AND NOT (rollback_confirmed=1
                              AND rollback_attribution='verified'
                              AND baseline_attribution='verified')
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
