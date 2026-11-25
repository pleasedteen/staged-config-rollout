from __future__ import annotations

import json
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any, Iterable

from .util import (
    DEFAULT_HOME,
    GROUP,
    PROJECT_ROOT,
    ConflictError,
    NotFoundError,
    canonical_json,
    now_ts,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS services (
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
    peer_name TEXT,
    identity_ok INTEGER NOT NULL DEFAULT 1,
    last_checked REAL,
    last_error TEXT
);

CREATE TABLE IF NOT EXISTS releases (
    id TEXT PRIMARY KEY,
    group_name TEXT NOT NULL,
    target_version TEXT NOT NULL,
    target_content TEXT NOT NULL,
    batches TEXT NOT NULL,
    status TEXT NOT NULL,
    batch_index INTEGER NOT NULL DEFAULT 0,
    pause_requested INTEGER NOT NULL DEFAULT 0,
    rollback_requested INTEGER NOT NULL DEFAULT 0,
    rollback_status TEXT NOT NULL DEFAULT 'none',
    apply_fence INTEGER NOT NULL,
    rollback_fence INTEGER,
    superseded INTEGER NOT NULL DEFAULT 0,
    note TEXT NOT NULL DEFAULT '',
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    completed_at REAL
);

-- The database itself enforces “one live release per group”.
CREATE UNIQUE INDEX IF NOT EXISTS one_active_release_per_group
ON releases(group_name)
WHERE status IN ('running', 'paused', 'rolling_back');

CREATE TABLE IF NOT EXISTS release_services (
    release_id TEXT NOT NULL REFERENCES releases(id) ON DELETE CASCADE,
    service_name TEXT NOT NULL,
    batch_index INTEGER NOT NULL,
    baseline_version TEXT NOT NULL,
    baseline_content TEXT NOT NULL,
    started INTEGER NOT NULL DEFAULT 0,
    applied_confirmed INTEGER NOT NULL DEFAULT 0,
    apply_attribution TEXT NOT NULL DEFAULT 'verified',
    apply_command_id TEXT NOT NULL,
    apply_attempts INTEGER NOT NULL DEFAULT 0,
    rollback_confirmed INTEGER NOT NULL DEFAULT 0,
    rollback_attribution TEXT NOT NULL DEFAULT 'verified',
    baseline_attribution TEXT NOT NULL DEFAULT 'verified',
    rollback_command_id TEXT NOT NULL,
    rollback_attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (release_id, service_name)
);

CREATE INDEX IF NOT EXISTS idx_release_services_batch
ON release_services(release_id, batch_index, started, applied_confirmed);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    release_id TEXT,
    kind TEXT NOT NULL,
    message TEXT NOT NULL
);
"""

ACTIVE_STATUSES = ("running", "paused", "rolling_back")


def load_initial_state(path: str | Path | None = None) -> dict[str, Any]:
    p = Path(path) if path else PROJECT_ROOT / "data" / "initial-state.json"
    with p.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if "services" not in data or not isinstance(data["services"], dict):
        raise ValueError("initial state must contain a services object")
    return data


class Store:
    def __init__(self, home: str | Path = DEFAULT_HOME):
        self.home = Path(home)
        self.db_path = self.home / "coordinator.sqlite3"
        self.cluster_path = self.home / "cluster.json"
        self.services_dir = self.home / "services"
        self.logs_dir = self.home / "logs"
        self.pids_dir = self.home / "pids"
        self.lock_path = self.home / "coordinator.lock"

    def connect(self) -> sqlite3.Connection:
        self.home.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path, timeout=6.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=6000")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def init_db(self) -> None:
        self.home.mkdir(parents=True, exist_ok=True)
        for d in (self.services_dir, self.logs_dir, self.pids_dir):
            d.mkdir(parents=True, exist_ok=True)
        with self.connect() as conn:
            conn.executescript(SCHEMA)
            conn.execute(
                "INSERT OR IGNORE INTO meta(key, value) VALUES('next_fence', '1')"
            )
            self.migrate(conn)

    def migrate(self, conn: sqlite3.Connection) -> None:
        """Add attribution columns to runtimes created before peer-identity checks.

        Pre-existing positive claims (a recorded apply/rollback confirmation, or
        any baseline snapshot) cannot be trusted until ``relctl verify`` proves
        they belong to the named service, so legacy claims start in the
        ``pending_verification`` posture.
        """
        rs_cols = {
            r["name"] for r in conn.execute("PRAGMA table_info(release_services)")
        }
        if "apply_attribution" not in rs_cols:
            conn.execute(
                "ALTER TABLE release_services "
                "ADD COLUMN apply_attribution TEXT NOT NULL DEFAULT 'verified'"
            )
            conn.execute(
                "UPDATE release_services SET apply_attribution='pending_verification' "
                "WHERE applied_confirmed=1"
            )
        if "rollback_attribution" not in rs_cols:
            conn.execute(
                "ALTER TABLE release_services "
                "ADD COLUMN rollback_attribution TEXT NOT NULL DEFAULT 'verified'"
            )
            conn.execute(
                "UPDATE release_services SET rollback_attribution='pending_verification' "
                "WHERE rollback_confirmed=1"
            )
        if "baseline_attribution" not in rs_cols:
            conn.execute(
                "ALTER TABLE release_services "
                "ADD COLUMN baseline_attribution TEXT NOT NULL DEFAULT 'verified'"
            )
            # Every old baseline may have been snapshotted from a crossed peer.
            conn.execute(
                "UPDATE release_services SET baseline_attribution='pending_verification'"
            )

        svc_cols = {r["name"] for r in conn.execute("PRAGMA table_info(services)")}
        if "peer_name" not in svc_cols:
            conn.execute("ALTER TABLE services ADD COLUMN peer_name TEXT")
        if "identity_ok" not in svc_cols:
            conn.execute(
                "ALTER TABLE services ADD COLUMN identity_ok INTEGER NOT NULL DEFAULT 1"
            )

    def sync_endpoints_from_cluster(self) -> list[str]:
        """Copy corrected cluster.json URLs/state paths into the services table."""
        cluster = self.load_cluster()
        changed: list[str] = []
        with self.connect() as conn:
            for spec in cluster["services"]:
                name = spec["name"]
                row = conn.execute(
                    "SELECT base_url, state_path FROM services WHERE name=?", (name,)
                ).fetchone()
                if row is None:
                    continue
                url = spec["url"]
                state_path = spec.get("state_path")
                if row["base_url"] != url or (
                    state_path and row["state_path"] != str(state_path)
                ):
                    conn.execute(
                        "UPDATE services SET base_url=?, "
                        "state_path=COALESCE(?, state_path) WHERE name=?",
                        (url, str(state_path) if state_path else None, name),
                    )
                    changed.append(name)
        return changed

    def initialize_home(
        self,
        *,
        names: Iterable[str] | None = None,
        ports: Iterable[int] | None = None,
        base_port: int = 9301,
        host: str = "127.0.0.1",
        group: str = GROUP,
        initial_state_path: str | Path | None = None,
    ) -> dict[str, Any]:
        self.init_db()
        initial = load_initial_state(initial_state_path)
        init_services = initial["services"]

        if names is None:
            names = list(init_services.keys())
        names = list(names)
        if not names:
            raise ValueError("at least one service is required")

        port_list = list(ports) if ports is not None else [base_port + i for i in range(len(names))]
        if len(port_list) != len(names):
            raise ValueError(f"expected {len(names)} ports, got {len(port_list)}")

        if self.cluster_path.exists():
            with self.cluster_path.open(encoding="utf-8") as f:
                cluster = json.load(f)
        else:
            services = []
            for name, port in zip(names, port_list):
                services.append(
                    {
                        "name": name,
                        "port": int(port),
                        "url": f"http://{host}:{int(port)}",
                        "state_path": str(self.services_dir / f"{name}.json"),
                    }
                )
            cluster = {"group": group, "host": host, "services": services}
            tmp = self.cluster_path.with_suffix(".json.tmp")
            with tmp.open("w", encoding="utf-8") as f:
                json.dump(cluster, f, indent=2, ensure_ascii=False)
            tmp.replace(self.cluster_path)

        with self.connect() as conn:
            for spec in cluster["services"]:
                name = spec["name"]
                init_cfg = init_services.get(
                    name,
                    {"version": "v0.0.0", "content": {"note": "unregistered initial state"}},
                )
                version = str(init_cfg["version"])
                content = init_cfg.get("content", {})
                state_path = Path(spec.get("state_path") or self.services_dir / f"{name}.json")
                spec["state_path"] = str(state_path)
                if not state_path.exists():
                    state_path.parent.mkdir(parents=True, exist_ok=True)
                    state = {
                        "name": name,
                        "effective": {"version": version, "content": content},
                        "fence": 0,
                        "history": [
                            {
                                "at": now_ts(),
                                "command_id": "initial-state",
                                "version": version,
                                "content": content,
                            }
                        ],
                        "seen_commands": {},
                        "recent_requests": [],
                        "faults": {
                            "delay_seconds": 0.0,
                            "delay_remaining": 0,
                            "drop_remaining": 0,
                            "lost_receipt_remaining": 0,
                        },
                    }
                    tmp = state_path.with_suffix(".json.tmp")
                    with tmp.open("w", encoding="utf-8") as f:
                        json.dump(state, f, indent=2, ensure_ascii=False)
                    tmp.replace(state_path)

                conn.execute(
                    """
                    INSERT INTO services(
                      name, group_name, base_url, state_path,
                      initial_version, initial_content,
                      actual_version, actual_content, actual_fence
                    ) VALUES(?,?,?,?,?,?,?,?,0)
                    ON CONFLICT(name) DO UPDATE SET
                      group_name=excluded.group_name,
                      base_url=excluded.base_url,
                      state_path=excluded.state_path,
                      initial_version=excluded.initial_version,
                      initial_content=excluded.initial_content
                    """,
                    (
                        name,
                        cluster.get("group", group),
                        spec["url"],
                        str(state_path),
                        version,
                        canonical_json(content),
                        version,
                        canonical_json(content),
                    ),
                )
        return cluster

    def load_cluster(self) -> dict[str, Any]:
        if not self.cluster_path.exists():
            raise NotFoundError(
                f"cluster file not found: {self.cluster_path}. Run `relcoord init` first."
            )
        with self.cluster_path.open(encoding="utf-8") as f:
            return json.load(f)

    def add_event(
        self,
        conn: sqlite3.Connection,
        release_id: str | None,
        kind: str,
        message: str,
    ) -> None:
        conn.execute(
            "INSERT INTO events(ts, release_id, kind, message) VALUES(?,?,?,?)",
            (now_ts(), release_id, kind, message),
        )

    def _next_fence(self, conn: sqlite3.Connection) -> int:
        row = conn.execute("SELECT value FROM meta WHERE key='next_fence'").fetchone()
        current = int(row["value"]) if row else 1
        conn.execute(
            "INSERT INTO meta(key,value) VALUES('next_fence', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (str(current + 1),),
        )
        return current

    def list_services(self) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return list(conn.execute("SELECT * FROM services ORDER BY name"))

    def get_service(self, name: str) -> sqlite3.Row:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM services WHERE name=?", (name,)).fetchone()
        if row is None:
            raise NotFoundError(f"unknown service: {name}")
        return row

    def active_release(self, group: str = GROUP) -> sqlite3.Row | None:
        with self.connect() as conn:
            return conn.execute(
                "SELECT * FROM releases WHERE group_name=? AND status IN ('running','paused','rolling_back')",
                (group,),
            ).fetchone()

    def latest_release(self, group: str = GROUP) -> sqlite3.Row | None:
        with self.connect() as conn:
            return conn.execute(
                "SELECT * FROM releases WHERE group_name=? ORDER BY created_at DESC, id DESC LIMIT 1",
                (group,),
            ).fetchone()

    def latest_non_superseded_release(self, group: str = GROUP) -> sqlite3.Row | None:
        """Most recent release that a newer publish has not replaced."""
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM releases WHERE group_name=? AND superseded=0 "
                "ORDER BY created_at DESC, id DESC LIMIT 1",
                (group,),
            ).fetchone()
        return row or self.latest_release(group)

    def get_release(self, release_id: str) -> sqlite3.Row:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM releases WHERE id=?", (release_id,)).fetchone()
        if row is None:
            raise NotFoundError(f"unknown release: {release_id}")
        return row

    def release_services(self, release_id: str) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return list(
                conn.execute(
                    "SELECT * FROM release_services WHERE release_id=? ORDER BY batch_index, service_name",
                    (release_id,),
                )
            )

    def recent_events(self, limit: int = 12) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return list(
                conn.execute(
                    "SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,)
                ).fetchall()
            )

    def submit_release(
        self,
        *,
        config_path: str | Path,
        batches: list[list[str]],
        group: str = GROUP,
        note: str = "",
    ) -> str:
        path = Path(config_path)
        with path.open(encoding="utf-8") as f:
            cfg = json.load(f)
        version = cfg.get("version")
        content = cfg.get("content")
        if not isinstance(version, str) or not version:
            raise ValueError("config must contain a non-empty string 'version'")
        if not isinstance(content, dict):
            raise ValueError("config must contain an object 'content'")
        if not batches or any(not b for b in batches):
            raise ValueError("at least one non-empty batch is required")

        cluster = self.load_cluster()
        known = [s["name"] for s in cluster["services"]]
        flat = [name for batch in batches for name in batch]
        if len(flat) != len(set(flat)):
            raise ValueError("a service appears more than once in batches")
        if set(flat) != set(known):
            missing = sorted(set(known) - set(flat))
            extra = sorted(set(flat) - set(known))
            raise ValueError(
                "batches must contain every cluster service exactly once; "
                f"missing={missing}, unknown={extra}"
            )

        release_id = f"rel-{time.strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:6]}"
        target_content = canonical_json(content)

        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                active = conn.execute(
                    "SELECT id FROM releases WHERE group_name=? "
                    "AND status IN ('running','paused','rolling_back')",
                    (group,),
                ).fetchone()
                if active:
                    raise ConflictError(
                        f"group {group!r} already has active release {active['id']}; "
                        "pause/resume/rollback it or wait for completion"
                    )

                # A new publish replaces the rollback point of prior releases.
                conn.execute(
                    "UPDATE releases SET superseded=1 WHERE group_name=? AND superseded=0",
                    (group,),
                )

                fence = self._next_fence(conn)
                ts = now_ts()
                conn.execute(
                    """
                    INSERT INTO releases(
                      id, group_name, target_version, target_content, batches,
                      status, batch_index, apply_fence, note, created_at, updated_at
                    ) VALUES(?,?,?,?,?, 'running', 0, ?, ?, ?, ?)
                    """,
                    (
                        release_id,
                        group,
                        version,
                        target_content,
                        json.dumps(batches, ensure_ascii=False),
                        fence,
                        note,
                        ts,
                        ts,
                    ),
                )

                service_rows = {
                    r["name"]: r
                    for r in conn.execute("SELECT * FROM services WHERE group_name=?", (group,))
                }
                for batch_index, batch in enumerate(batches):
                    for name in batch:
                        svc = service_rows[name]
                        baseline_version = svc["actual_version"] or svc["initial_version"]
                        baseline_content = svc["actual_content"] or svc["initial_content"]
                        conn.execute(
                            """
                            INSERT INTO release_services(
                              release_id, service_name, batch_index,
                              baseline_version, baseline_content, started,
                              apply_command_id, rollback_command_id
                            ) VALUES(?,?,?,?,?,?,?,?)
                            """,
                            (
                                release_id,
                                name,
                                batch_index,
                                baseline_version,
                                baseline_content,
                                1 if batch_index == 0 else 0,
                                f"apply-{release_id}-{name}",
                                f"rollback-{release_id}-{name}",
                            ),
                        )
                self.add_event(
                    conn,
                    release_id,
                    "submitted",
                    f"submit {version} with {len(batches)} batch(es), apply fence {fence}",
                )
                self.add_event(
                    conn,
                    release_id,
                    "batch_started",
                    f"batch 1 started: {', '.join(batches[0])}",
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return release_id

    def pause(self, group: str = GROUP, reason: str = "") -> tuple[str, bool]:
        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    "SELECT * FROM releases WHERE group_name=? "
                    "AND status IN ('running','paused','rolling_back')",
                    (group,),
                ).fetchone()
                if row is None:
                    raise ConflictError("no active release to pause")
                if row["status"] == "rolling_back":
                    raise ConflictError(
                        f"release {row['id']} is rolling back; pause cannot supersede rollback"
                    )
                if row["pause_requested"]:
                    msg = f"pause already accepted for {row['id']}"
                    changed = False
                else:
                    conn.execute(
                        "UPDATE releases SET pause_requested=1, updated_at=? WHERE id=?",
                        (now_ts(), row["id"]),
                    )
                    self.add_event(
                        conn,
                        row["id"],
                        "pause_accepted",
                        "pause accepted; current batch may settle, no later batch will start"
                        + (f": {reason}" if reason else ""),
                    )
                    msg = f"pause accepted for {row['id']}; current batch may finish"
                    changed = True
                conn.commit()
                return row["id"], changed
            except Exception:
                conn.rollback()
                raise

    def resume(self, group: str = GROUP) -> tuple[str, bool]:
        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    "SELECT * FROM releases WHERE group_name=? "
                    "AND status IN ('running','paused')",
                    (group,),
                ).fetchone()
                if row is None:
                    rolling = conn.execute(
                        "SELECT id FROM releases WHERE group_name=? AND status='rolling_back'",
                        (group,),
                    ).fetchone()
                    if rolling:
                        raise ConflictError("release is rolling back and cannot be resumed")
                    raise ConflictError("no paused/running release to resume")
                if not row["pause_requested"] and row["status"] == "running":
                    msg = f"release {row['id']} is already running"
                    changed = False
                else:
                    conn.execute(
                        "UPDATE releases SET pause_requested=0, status='running', updated_at=? WHERE id=?",
                        (now_ts(), row["id"]),
                    )
                    self.add_event(conn, row["id"], "resumed", "resume accepted")
                    msg = f"release {row['id']} resumed"
                    changed = True
                conn.commit()
                return row["id"], changed
            except Exception:
                conn.rollback()
                raise

    def request_rollback(self, group: str = GROUP, reason: str = "") -> tuple[str, bool]:
        with self.connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                active = conn.execute(
                    "SELECT * FROM releases WHERE group_name=? "
                    "AND status IN ('running','paused','rolling_back')",
                    (group,),
                ).fetchone()
                row = active
                if row is None:
                    latest = conn.execute(
                        "SELECT * FROM releases WHERE group_name=? ORDER BY created_at DESC LIMIT 1",
                        (group,),
                    ).fetchone()
                    if latest is None:
                        raise ConflictError("no release exists to roll back")
                    if latest["status"] == "rolled_back":
                        return latest["id"], False
                    if latest["status"] != "completed" or latest["superseded"]:
                        raise ConflictError(
                            f"latest release {latest['id']} is not rollbackable "
                            f"(status={latest['status']}, superseded={latest['superseded']})"
                        )
                    row = latest

                if row["rollback_requested"]:
                    conn.commit()
                    return row["id"], False

                fence = self._next_fence(conn)
                conn.execute(
                    """
                    UPDATE releases
                    SET rollback_requested=1, rollback_status='requested',
                        status='rolling_back', rollback_fence=?, updated_at=?
                    WHERE id=?
                    """,
                    (fence, now_ts(), row["id"]),
                )
                started = conn.execute(
                    "SELECT service_name FROM release_services WHERE release_id=? AND started=1",
                    (row["id"],),
                ).fetchall()
                self.add_event(
                    conn,
                    row["id"],
                    "rollback_accepted",
                    f"rollback accepted with fence {fence}; restoring {len(started)} started service(s)"
                    + (f": {reason}" if reason else ""),
                )
                conn.commit()
                return row["id"], True
            except Exception:
                conn.rollback()
                raise

    def verify_attribution(self, **kwargs: Any) -> dict[str, Any]:
        """Convenience wrapper around :func:`relcoord.verify.verify_runtime`."""
        from .verify import verify_runtime

        return verify_runtime(self.home, **kwargs)
