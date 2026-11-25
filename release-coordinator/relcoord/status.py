from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from . import client
from .storage import Store
from .util import compact_json, parse_json_maybe


def _live_states(services: list[Any], timeout: float = 0.65) -> dict[tuple[str, str], dict[str, Any]]:
    result: dict[tuple[str, str], dict[str, Any]] = {}
    if not services:
        return result
    with ThreadPoolExecutor(max_workers=min(8, max(1, len(services)))) as pool:
        future_map = {
            pool.submit(client.fetch_state, row["base_url"], timeout): (
                row["group_name"],
                row["name"],
            )
            for row in services
        }
        for future in future_map:
            key = future_map[future]
            try:
                result[key] = {"ok": True, "state": future.result()}
            except Exception as e:
                result[key] = {"ok": False, "error": f"{type(e).__name__}: {e}"}
    return result


def build_status(
    home: str | Path,
    *,
    live: bool = True,
    group: str | None = None,
) -> dict[str, Any]:
    store = Store(home)
    try:
        cluster = store.load_cluster()
    except Exception:
        cluster = {"group": group, "groups": {}}
    group_name = group or cluster.get("group") or "core"
    services = store.list_services(group_name)
    group_cfg = cluster.get("groups", {}).get(group_name)
    if group_cfg is None:
        group_cfg = {"name": group_name, "services": [dict(row) for row in services]}
    active = store.active_release(group_name)
    release = active or store.latest_release(group_name)
    live_map = _live_states(services) if live else {}

    release_dict: dict[str, Any] | None = None
    rows_by_service: dict[str, dict[str, Any]] = {}
    if release is not None:
        release_dict = dict(release)
        release_dict["batches_parsed"] = json.loads(release["batches"])
        release_dict["target_content_parsed"] = json.loads(release["target_content"])
        for row in store.release_services(release["id"]):
            d = dict(row)
            d["baseline_content_parsed"] = json.loads(row["baseline_content"])
            rows_by_service[row["service_name"]] = d

    service_items = []
    for svc in services:
        item = dict(svc)
        item["initial_content_parsed"] = parse_json_maybe(svc["initial_content"])
        item["actual_content_parsed"] = parse_json_maybe(svc["actual_content"])
        item["release_state"] = rows_by_service.get(svc["name"])
        if live:
            item["live"] = live_map.get(
                (svc["group_name"], svc["name"]),
                {"ok": False, "error": "not probed"},
            )
            if item["live"].get("ok"):
                st = item["live"]["state"]
                item["live_actual"] = st.get("effective")
                item["live_fence"] = st.get("fence")
        service_items.append(item)

    events = [dict(e) for e in store.recent_events(10, group_name)]
    status_cluster = dict(cluster)
    status_cluster["group"] = group_name
    status_cluster["services"] = group_cfg["services"]
    return {
        "home": str(Path(home)),
        "group": group_name,
        "cluster": status_cluster,
        "release": release_dict,
        "services": service_items,
        "events": events,
    }


def _status_marks(rs: dict[str, Any] | None, release: dict[str, Any] | None) -> str:
    if rs is None or release is None:
        return ""
    parts = [f"batch={rs['batch_index'] + 1}"]
    if rs["started"]:
        if release["rollback_requested"]:
            parts.append("restored✓" if rs["rollback_confirmed"] else "restore-pending")
        else:
            parts.append("applied✓" if rs["applied_confirmed"] else "apply-pending")
    else:
        parts.append("not-started")
    if rs["last_error"]:
        parts.append(f"error={rs['last_error']}")
    return " [" + "; ".join(parts) + "]"


def render_status(data: dict[str, Any]) -> str:
    lines: list[str] = []
    cluster = data["cluster"]
    release = data["release"]
    lines.append(f"home: {data['home']}")
    lines.append(f"group: {cluster.get('group', 'core')}  services: {len(data['services'])}")

    if release is None:
        lines.append("release: none")
    else:
        batches = release["batches_parsed"]
        total = sum(len(b) for b in batches)
        rows = {
            s["name"]: s["release_state"]
            for s in data["services"]
            if s.get("release_state") is not None
        }
        applied = sum(1 for r in rows.values() if r["applied_confirmed"])
        restored = sum(1 for r in rows.values() if r["rollback_confirmed"])
        started = sum(1 for r in rows.values() if r["started"])

        lines.append("")
        lines.append(f"release: {release['id']}")
        lines.append(
            f"  target={release['target_version']}  status={release['status']}  "
            f"batch={int(release['batch_index']) + 1}/{len(batches)}"
        )
        flags = []
        if release["pause_requested"]:
            flags.append("pause accepted")
        if release["rollback_requested"]:
            flags.append(f"rollback {release['rollback_status']}")
        if release["superseded"]:
            flags.append("superseded by newer release")
        if flags:
            lines.append("  flags: " + ", ".join(flags))
        if release["rollback_requested"]:
            lines.append(f"  restore progress: {restored}/{started} started-service confirmations")
        else:
            lines.append(f"  apply progress: {applied}/{total} confirmed ({started} batch-started)")

        lines.append("  batches:")
        for i, batch in enumerate(batches):
            marks = []
            for name in batch:
                rs = rows[name]
                if release["rollback_requested"] and rs["started"]:
                    mark = "R✓" if rs["rollback_confirmed"] else "R…"
                elif rs["applied_confirmed"]:
                    mark = "✓"
                elif rs["started"]:
                    mark = "…"
                else:
                    mark = "·"
                marks.append(f"{name}{mark}")
            batch_rows = [rows[name] for name in batch]
            if release["rollback_requested"] or release["status"] == "rolled_back":
                if not any(rs["started"] for rs in batch_rows):
                    state = "untouched"
                elif all(rs["rollback_confirmed"] for rs in batch_rows):
                    state = "restored"
                else:
                    state = "rb-pending"
            elif all(rs["applied_confirmed"] for rs in batch_rows):
                state = "done"
            elif any(rs["started"] for rs in batch_rows):
                state = "current"
            else:
                state = "queued"
            lines.append(f"    {i + 1}. [{state:9}] " + " ".join(marks))

    lines.append("")
    lines.append("actual service configuration:")
    for svc in data["services"]:
        live = svc.get("live")
        if live is None:
            live_ok = bool(svc["reachable"])
            source = "db-last-seen"
        else:
            live_ok = bool(live.get("ok"))
            source = "live" if live_ok else "db-last-seen"

        if live_ok and live is not None and live.get("ok"):
            actual = svc.get("live_actual") or {}
            fence = svc.get("live_fence")
            reach = "reachable"
        else:
            actual = {
                "version": svc["actual_version"],
                "content": svc["actual_content_parsed"],
            }
            fence = svc["actual_fence"]
            reach = "UNREACHABLE"
            if live is not None and not live.get("ok"):
                reach += f" ({live.get('error')})"

        version = actual.get("version") if actual else None
        content = actual.get("content") if actual else None
        lines.append(f"  - {svc['name']}: {reach}, source={source}, version={version}, fence={fence}")
        lines.append(f"      content={compact_json(content)}")
        if not live_ok and svc.get("last_error"):
            lines.append(f"      coordinator_error={svc['last_error']}")
        mark = _status_marks(svc.get("release_state"), release)
        if mark:
            lines.append(f"      release:{mark}")

    if data.get("events"):
        lines.append("")
        lines.append("recent events:")
        # Stored newest-first; reverse the newest N for chronological reading.
        for event in reversed(data["events"][:8]):
            lines.append(f"  {event['kind']}: {event['message']}")
    return "\n".join(lines)
