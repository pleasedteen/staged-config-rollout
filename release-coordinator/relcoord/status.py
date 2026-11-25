from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from . import client
from .storage import Store
from .util import compact_json, parse_json_maybe


def _live_states(services: list[Any], timeout: float = 0.65) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=max(4, len(services))) as pool:
        future_map = {
            pool.submit(client.fetch_state, row["base_url"], timeout): row["name"]
            for row in services
        }
        for future in future_map:
            name = future_map[future]
            try:
                state = future.result()
                peer = client.peer_name(state)
                if peer is not None and peer != name:
                    # Never present another peer's config as this service's
                    # live state, even if the crossed endpoint answers 200.
                    result[name] = {
                        "ok": False,
                        "identity_mismatch": True,
                        "peer_name": peer,
                        "error": (
                            f"peer_identity_mismatch: endpoint identifies as "
                            f"{peer!r}, expected {name!r}"
                        ),
                    }
                else:
                    result[name] = {
                        "ok": True,
                        "state": state,
                        "peer_name": peer or name,
                    }
            except Exception as e:
                result[name] = {"ok": False, "error": f"{type(e).__name__}: {e}"}
    return result


def build_status(home: str | Path, *, live: bool = True) -> dict[str, Any]:
    store = Store(home)
    # Ensure attribution columns exist and legacy claims are parked as
    # pending_verification before SELECT * is rendered.
    store.init_db()
    cluster = store.load_cluster()
    services = store.list_services()
    active = store.active_release(cluster.get("group", "core"))
    release = active or store.latest_release(cluster.get("group", "core"))
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
            d.setdefault("apply_attribution", "verified")
            d.setdefault("rollback_attribution", "verified")
            d.setdefault("baseline_attribution", "verified")
            d["apply_resolved"] = bool(d["applied_confirmed"]) and d[
                "apply_attribution"
            ] == "verified"
            d["rollback_resolved"] = bool(d["rollback_confirmed"]) and d[
                "rollback_attribution"
            ] == "verified"
            d["baseline_resolved"] = d["baseline_attribution"] == "verified"
            d["needs_verification"] = (
                (bool(d["applied_confirmed"]) and d["apply_attribution"] != "verified")
                or (
                    bool(d["rollback_confirmed"])
                    and d["rollback_attribution"] != "verified"
                )
                or d["baseline_attribution"] != "verified"
            )
            # Vocabulary aliases for machine consumers.
            d["unverified"] = d["needs_verification"]
            d["pending_verification"] = d["needs_verification"]
            d["attribution_status"] = (
                "pending_verification" if d["needs_verification"] else "verified"
            )
            d["verification_status"] = d["attribution_status"]
            rows_by_service[row["service_name"]] = d

    service_items = []
    for svc in services:
        item = dict(svc)
        item["initial_content_parsed"] = parse_json_maybe(svc["initial_content"])
        item["actual_content_parsed"] = parse_json_maybe(svc["actual_content"])
        item["release_state"] = rows_by_service.get(svc["name"])
        if live:
            item["live"] = live_map.get(svc["name"], {"ok": False, "error": "not probed"})
            if item["live"].get("ok"):
                st = item["live"]["state"]
                item["live_actual"] = st.get("effective")
                item["live_fence"] = st.get("fence")
        live_info = item.get("live") or {}
        if live_info.get("identity_mismatch"):
            item["identity_status"] = "peer_mismatch"
        elif live_info.get("ok"):
            item["identity_status"] = "verified"
        elif not item.get("identity_ok", 1):
            item["identity_status"] = "peer_mismatch"
        elif live:
            item["identity_status"] = "unreachable"
        else:
            item["identity_status"] = "verified"
        item["attribution_ok"] = item["identity_status"] != "peer_mismatch"
        service_items.append(item)

    events = [dict(e) for e in store.recent_events(10)]
    return {
        "home": str(Path(home)),
        "cluster": cluster,
        "release": release_dict,
        "services": service_items,
        "events": events,
    }


PENDING_TOKEN = "pending_verification"


def _status_marks(rs: dict[str, Any] | None, release: dict[str, Any] | None) -> str:
    if rs is None or release is None:
        return ""
    parts = [f"batch={rs['batch_index'] + 1}"]
    apply_attr = rs.get("apply_attribution", "verified")
    rb_attr = rs.get("rollback_attribution", "verified")
    baseline_attr = rs.get("baseline_attribution", "verified")
    if rs["started"]:
        if release["rollback_requested"]:
            if rs.get("rollback_resolved"):
                parts.append("restored✓")
            elif rs["rollback_confirmed"] and rb_attr != "verified":
                parts.append("rollback-confirmation待核实(pending_verification)")
            elif baseline_attr != "verified":
                parts.append("baseline待核实(pending_verification); restore blocked")
            else:
                parts.append("restore-pending")
        else:
            if rs.get("apply_resolved"):
                parts.append("applied✓")
            elif rs["applied_confirmed"] and apply_attr != "verified":
                parts.append("confirmation待核实(pending_verification)")
            else:
                parts.append("apply-pending")
    else:
        parts.append("not-started")
    if baseline_attr != "verified" and not release["rollback_requested"]:
        parts.append("rollback-baseline待核实(pending_verification)")
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
        applied = sum(1 for r in rows.values() if r.get("apply_resolved"))
        restored = sum(1 for r in rows.values() if r.get("rollback_resolved"))
        started = sum(1 for r in rows.values() if r["started"])
        apply_unverified = sum(
            1
            for r in rows.values()
            if r["applied_confirmed"] and r.get("apply_attribution") != "verified"
        )
        rb_unverified = sum(
            1
            for r in rows.values()
            if r["rollback_confirmed"] and r.get("rollback_attribution") != "verified"
        )
        baseline_unverified = sum(
            1 for r in rows.values() if r.get("baseline_attribution") != "verified"
        )

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
            lines.append(f"  restore progress: {restored}/{started} verified restorations")
            if rb_unverified or baseline_unverified:
                lines.append(
                    f"  ⚠ 待核实(pending-verification): {rb_unverified} rollback confirmation(s), "
                    f"{baseline_unverified} baseline(s); these block completion/restore "
                    "— run `relctl verify` after fixing endpoints"
                )
        else:
            lines.append(f"  apply progress: {applied}/{total} verified confirmations ({started} batch-started)")
            if apply_unverified or baseline_unverified:
                lines.append(
                    f"  ⚠ 待核实(pending-verification): {apply_unverified} confirmation(s), "
                    f"{baseline_unverified} rollback baseline(s); unverified confirmations "
                    "do not advance the release — run `relctl verify`"
                )

        def _apply_mark(rs: dict[str, Any]) -> str:
            if rs.get("apply_resolved"):
                return "✓"
            if rs["applied_confirmed"]:
                return "?"  # confirmation exists but attribution is 待核实
            if rs["started"]:
                return "…"
            return "·"

        def _rollback_mark(rs: dict[str, Any]) -> str:
            if rs.get("rollback_resolved"):
                return "R✓"
            if rs["rollback_confirmed"] or rs.get("baseline_attribution") != "verified":
                return "R?"
            return "R…"

        lines.append("  batches:")
        for i, batch in enumerate(batches):
            marks = []
            for name in batch:
                rs = rows[name]
                if release["rollback_requested"] and rs["started"]:
                    mark = _rollback_mark(rs)
                else:
                    mark = _apply_mark(rs)
                marks.append(f"{name}{mark}")
            batch_rows = [rows[name] for name in batch]
            if release["rollback_requested"] or release["status"] == "rolled_back":
                if not any(rs["started"] for rs in batch_rows):
                    state = "untouched"
                elif all(rs.get("rollback_resolved") for rs in batch_rows):
                    state = "restored"
                elif any(
                    rs.get("baseline_attribution") != "verified" for rs in batch_rows
                ):
                    state = "verify?"
                else:
                    state = "rb-pending"
            elif all(rs.get("apply_resolved") for rs in batch_rows):
                state = "done"
            elif any(rs["started"] for rs in batch_rows):
                state = "current"
            else:
                state = "queued"
            lines.append(f"    {i + 1}. [{state:9}] " + " ".join(marks))
        if apply_unverified or rb_unverified or baseline_unverified:
            lines.append("    legend: ? = 待核实(pending verification), not counted as confirmation")

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
            if live is not None and live.get("identity_mismatch"):
                reach = (
                    f"PEER-MISMATCH (endpoint identifies as {live.get('peer_name')!r}; "
                    "showing SQLite last-attributed state, not live peer config)"
                )
            else:
                reach = "UNREACHABLE"
                if live is not None and not live.get("ok"):
                    reach += f" ({live.get('error')})"
                elif live is None and not svc["reachable"] and svc.get("last_error"):
                    reach += f" ({svc['last_error']})"

        version = actual.get("version") if actual else None
        content = actual.get("content") if actual else None
        lines.append(f"  - {svc['name']}: {reach}, source={source}, version={version}, fence={fence}")
        lines.append(f"      content={compact_json(content)}")
        if live is None and svc.get("identity_ok") == 0 and svc.get("peer_name"):
            lines.append(
                f"      last_peer_identity_mismatch: peer_name={svc.get('peer_name')!r}"
            )
        if not live_ok and svc.get("last_error") and not (
            live is not None and not live.get("ok")
        ):
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
