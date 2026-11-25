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
    group_name TEXT NOT NULL,
    name TEXT NOT NULL,
    base_url TEXT NOT NULL,
    state_path TEXT NOT NULL,
    initial_version TEXT NOT NULL,
    initial_content TEXT NOT NULL,
    actual_version TEXT,
    actual_content TEXT,
    actual_fence INTEGER NOT NULL DEFAULT 0,
    reachable INTEGER NOT NULL DEFAULT 0,
    last_checked REAL,
    last_error TEXT,
    PRIMARY KEY (group_name, name)
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
    apply_command_id TEXT NOT NULL,
    apply_attempts INTEGER NOT NULL DEFAULT 0,
    rollback_confirmed INTEGER NOT NULL DEFAULT 0,
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
            self._migrate_services_identity(conn)
            conn.execute(
                "INSERT OR IGNORE INTO meta(key, value) VALUES('next_fence', '1')"
            )
            conn.commit()

    def _migrate_services_identity(self, conn: sqlite3.Connection) -> None:
        """Upgrade a legacy name-PK services table to (group_name, name)."""
        table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='services'"
        ).fetchone()
        if table is None:
            return
        info = conn.execute("PRAGMA table_info(services)").fetchall()
        columns = {row["name"]: row for row in info}
        pk_cols = [row["name"] for row in sorted(info, key=lambda r: r["pk"]) if row["pk"]]
        needs_group_column = "group_name" not in columns
        needs_pk_migration = pk_cols != ["group_name", "name"]
        if not needs_group_column and not needs_pk_migration:
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_services_group ON services(group_name, name)"
            )
            return

        # A pre-group schema may lack group_name; add it before SQL in SCHEMA
        # or later indexes can reference it safely after this rebuild.

        conn.execute("ALTER TABLE services RENAME TO services_legacy_identity")
        conn.execute(
            """
            CREATE TABLE services (
                group_name TEXT NOT NULL,
                name TEXT NOT NULL,
                base_url TEXT NOT NULL,
                state_path TEXT NOT NULL,
                initial_version TEXT NOT NULL,
                initial_content TEXT NOT NULL,
                actual_version TEXT,
                actual_content TEXT,
                actual_fence INTEGER NOT NULL DEFAULT 0,
                reachable INTEGER NOT NULL DEFAULT 0,
                last_checked REAL,
                last_error TEXT,
                PRIMARY KEY (group_name, name)
            )
            """
        )
        old_info = conn.execute("PRAGMA table_info(services_legacy_identity)").fetchall()
        old_cols = {row["name"] for row in old_info}
        group_expr = "group_name" if "group_name" in old_cols else "'core'"
        actual_fence = "actual_fence" if "actual_fence" in old_cols else "0"
        reachable = "reachable" if "reachable" in old_cols else "0"
        last_checked = "last_checked" if "last_checked" in old_cols else "NULL"
        last_error = "last_error" if "last_error" in old_cols else "NULL"
        conn.execute(
            f"""
            INSERT INTO services(
              group_name, name, base_url, state_path, initial_version, initial_content,
              actual_version, actual_content, actual_fence, reachable, last_checked, last_error
            )
            SELECT
              COALESCE({group_expr}, 'core'), name, base_url, state_path,
              initial_version, initial_content, actual_version, actual_content,
              COALESCE({actual_fence}, 0), COALESCE({reachable}, 0),
              {last_checked}, {last_error}
            FROM services_legacy_identity
            """
        )
        conn.execute("DROP TABLE services_legacy_identity")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_services_group ON services(group_name, name)")

    @staticmethod
    def _safe_group_path(group: str) -> str:
        safe = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in str(group))
        return safe or GROUP

    def _read_cluster_file(self) -> dict[str, Any]:
        if not self.cluster_path.exists():
            return {}
        with self.cluster_path.open(encoding="utf-8") as f:
            raw = json.load(f)
        return raw if isinstance(raw, dict) else {}

    def normalize_cluster(self, raw: dict[str, Any] | None = None) -> dict[str, Any]:
        """Return cluster config as groups while retaining a legacy top-level view."""
        raw = self._read_cluster_file() if raw is None else raw
        default_host = str(raw.get("host", "127.0.0.1"))
        groups: dict[str, dict[str, Any]] = {}

        raw_groups = raw.get("groups")
        if isinstance(raw_groups, dict) and raw_groups:
            items: Iterable[tuple[str, Any]] = raw_groups.items()
        elif isinstance(raw_groups, list) and raw_groups:
            items = ((str(g.get("name", GROUP)), g) for g in raw_groups if isinstance(g, dict))
        else:
            items = ((str(raw.get("group", GROUP)), raw),)

        for group_name, group_cfg in items:
            if isinstance(group_cfg, list):
                cfg: dict[str, Any] = {"services": group_cfg}
            elif isinstance(group_cfg, dict):
                cfg = group_cfg
            else:
                cfg = {"services": []}
            host = str(cfg.get("host") or default_host)
            services: list[dict[str, Any]] = []
            for spec in cfg.get("services", []):
                if not isinstance(spec, dict):
                    continue
                item = dict(spec)
                item["group"] = group_name
                if "name" not in item:
                    continue
                port = item.get("port")
                if port is None and item.get("url"):
                    from urllib.parse import urlparse

                    parsed = urlparse(str(item["url"]))
                    port = parsed.port
                    item.setdefault("port", port)
                    if not item.get("host") and parsed.hostname:
                        item["host"] = parsed.hostname
                if not item.get("url") and port is not None:
                    item["url"] = f"http://{item.get('host', host)}:{int(port)}"
                services.append(item)
            groups[group_name] = {
                "name": group_name,
                "host": str(cfg.get("host") or host),
                "services": services,
            }

        default_group = GROUP if GROUP in groups else next(iter(groups), GROUP)
        default_cfg = groups.setdefault(
            default_group, {"name": default_group, "host": default_host, "services": []}
        )
        cluster = dict(raw)
        cluster["group"] = str(raw.get("group") or default_group)
        cluster["host"] = default_host
        cluster["groups"] = groups
        # Backward-compatible projection for single-group callers and the demo.
        cluster["services"] = [dict(s) for s in default_cfg["services"]]
        return cluster

    def save_cluster(self, cluster: dict[str, Any]) -> None:
        self.home.mkdir(parents=True, exist_ok=True)
        tmp = self.cluster_path.with_suffix(".json.tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(cluster, f, indent=2, ensure_ascii=False)
        tmp.replace(self.cluster_path)

    def list_groups(self) -> list[dict[str, Any]]:
        try:
            return list(self.load_cluster()["groups"].values())
        except NotFoundError:
            with self.connect() as conn:
                names = [
                    r["group_name"]
                    for r in conn.execute(
                        "SELECT DISTINCT group_name FROM services ORDER BY group_name"
                    )
                ]
            return [{"name": name, "services": []} for name in names]

    def get_group_config(self, group: str) -> dict[str, Any]:
        cfg = self.load_cluster()["groups"].get(group)
        if cfg is None:
            raise NotFoundError(f"unknown group: {group}")
        return cfg

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
        raw_existed = self.cluster_path.exists()
        cluster = self.normalize_cluster() if raw_existed else {
            "group": group,
            "host": host,
            "groups": {},
            "services": [],
        }
        groups: dict[str, dict[str, Any]] = cluster["groups"]
        existing = groups.get(group)

        if names is None:
            if existing is not None and existing.get("services"):
                names = [s["name"] for s in existing["services"]]
            else:
                names = list(init_services.keys())
        names = list(names)
        if not names:
            raise ValueError("at least one service is required")
        if len(names) != len(set(names)):
            raise ValueError("service names within a group must be unique")

        explicit_ports = list(ports) if ports is not None else None
        if explicit_ports is not None and len(explicit_ports) != len(names):
            raise ValueError(f"expected {len(names)} ports, got {len(explicit_ports)}")

        used_ports: set[int] = set()
        for cfg in groups.values():
            for spec in cfg.get("services", []):
                if spec.get("port") is not None:
                    used_ports.add(int(spec["port"]))

        old_specs = {s["name"]: s for s in (existing or {}).get("services", [])}
        services: list[dict[str, Any]] = []
        cursor = int(base_port)
        for i, name in enumerate(names):
            old = old_specs.get(name, {})
            if explicit_ports is not None:
                port = int(explicit_ports[i])
            elif old.get("port") is not None:
                port = int(old["port"])
            else:
                port = int(base_port) + i
                while port in used_ports:
                    port += 1
            if port in used_ports and int(old.get("port", -1)) != port:
                raise ValueError(f"port {port} is already allocated to another service/group")
            used_ports.add(port)

            state_path = old.get("state_path")
            if not state_path:
                # The first/legacy group keeps the original flat layout.  Later
                # groups are namespaced so equal service names never collide.
                if not raw_existed or (
                    len(groups) == 0 and group == cluster.get("group", GROUP)
                ):
                    state_path = str(self.services_dir / f"{name}.json")
                else:
                    state_path = str(
                        self.services_dir / self._safe_group_path(group) / f"{name}.json"
                    )
            spec_host = str(old.get("host") or host)
            services.append(
                {
                    "name": name,
                    "group": group,
                    "port": port,
                    "host": spec_host,
                    "url": old.get("url") or f"http://{spec_host}:{port}",
                    "state_path": str(state_path),
                }
            )

        groups[group] = {"name": group, "host": host, "services": services}
        if cluster.get("group") not in groups:
            cluster["group"] = GROUP if GROUP in groups else next(iter(groups), group)
        cluster["host"] = host
        cluster["groups"] = groups
        cluster["services"] = [dict(s) for s in groups[cluster["group"]]["services"]]
        self.save_cluster(cluster)

        with self.connect() as conn:
            for spec in services:
                name = spec["name"]
                init_cfg = init_services.get(
                    name,
                    {"version": "v0.0.0", "content": {"note": "unregistered initial state"}},
                )
                version = str(init_cfg["version"])
                content = init_cfg.get("content", {})
                state_path = Path(spec["state_path"])
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
                      group_name, name, base_url, state_path,
                      initial_version, initial_content,
                      actual_version, actual_content, actual_fence
                    ) VALUES(?,?,?,?,?,?,?,?,0)
                    ON CONFLICT(group_name, name) DO UPDATE SET
                      base_url=excluded.base_url,
                      state_path=excluded.state_path,
                      initial_version=excluded.initial_version,
                      initial_content=excluded.initial_content
                    """,
                    (
                        group,
                        name,
                        spec["url"],
                        str(state_path),
                        version,
                        canonical_json(content),
                        version,
                        canonical_json(content),
                    ),
                )
            conn.commit()
        result = dict(cluster)
        result["group"] = group
        result["services"] = [dict(spec) for spec in services]
        return result

    def load_cluster(self) -> dict[str, Any]:
        cluster = self.normalize_cluster()
        if not self.cluster_path.exists():
            raise NotFoundError(
                f"cluster file not found: {self.cluster_path}. Run `relcoord init` first."
            )
        return cluster

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

    def list_services(self, group: str | None = None) -> list[sqlite3.Row]:
        with self.connect() as conn:
            if group is None:
                return list(
                    conn.execute(
                        "SELECT * FROM services ORDER BY group_name, name"
                    )
                )
            return list(
                conn.execute(
                    "SELECT * FROM services WHERE group_name=? ORDER BY name",
                    (group,),
                )
            )

    def get_service(self, name: str, group: str | None = None) -> sqlite3.Row:
        with self.connect() as conn:
            if group is not None:
                row = conn.execute(
                    "SELECT * FROM services WHERE group_name=? AND name=?",
                    (group, name),
                ).fetchone()
            else:
                rows = conn.execute(
                    "SELECT * FROM services WHERE name=? ORDER BY group_name",
                    (name,),
                ).fetchall()
                if len(rows) > 1:
                    groups = [r["group_name"] for r in rows]
                    raise ConflictError(
                        f"service {name!r} exists in multiple groups {groups}; pass --group"
                    )
                row = rows[0] if rows else None
        if row is None:
            qualifier = f" in group {group!r}" if group is not None else ""
            raise NotFoundError(f"unknown service{qualifier}: {name}")
        return row

    def active_releases(self) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return list(
                conn.execute(
                    "SELECT * FROM releases "
                    "WHERE status IN ('running','paused','rolling_back') "
                    "ORDER BY group_name, created_at"
                )
            )

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

    def recent_events(self, limit: int = 12, group: str | None = None) -> list[sqlite3.Row]:
        with self.connect() as conn:
            if group is None:
                return list(
                    conn.execute(
                        "SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,)
                    ).fetchall()
                )
            return list(
                conn.execute(
                    """
                    SELECT e.* FROM events e
                    LEFT JOIN releases r ON r.id = e.release_id
                    WHERE r.group_name = ?
                    ORDER BY e.id DESC LIMIT ?
                    """,
                    (group, limit),
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

        try:
            cluster = self.load_cluster()
        except NotFoundError:
            cluster = {"groups": {}}
        group_cfg = cluster.get("groups", {}).get(group)
        known = [s["name"] for s in group_cfg["services"]] if group_cfg else None
        flat = [name for batch in batches for name in batch]
        if len(flat) != len(set(flat)):
            raise ValueError("a service appears more than once in batches")
        if known is not None and set(flat) != set(known):
            missing = sorted(set(known) - set(flat))
            extra = sorted(set(flat) - set(known))
            raise ValueError(
                "batches must contain every service in group "
                f"{group!r} exactly once; missing={missing}, unknown={extra}"
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
                if not service_rows:
                    raise NotFoundError(
                        f"unknown group or no services registered for group: {group}"
                    )
                db_known = set(service_rows)
                if set(flat) != db_known:
                    missing = sorted(db_known - set(flat))
                    extra = sorted(set(flat) - db_known)
                    raise ValueError(
                        "batches must contain every service in group "
                        f"{group!r} exactly once; missing={missing}, unknown={extra}"
                    )
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
