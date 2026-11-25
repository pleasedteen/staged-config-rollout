from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from . import client
from .storage import Store, load_initial_state
from .util import GROUP, canonical_json, now_ts

# Stable attribution tokens (persisted in SQLite / JSON output).
VERIFIED = "verified"
PENDING_VERIFICATION = "pending_verification"

# Chinese label used in human output / greppable acceptance checks.
PENDING_LABEL_ZH = "待核实(pending_verification)"


def _read_state_file(path: str | Path | None) -> tuple[dict[str, Any] | None, str | None]:
    if not path:
        return None, "no state_path recorded"
    p = Path(path)
    if not p.exists():
        return None, f"state file not found: {p}"
    try:
        with p.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        return None, f"state file unreadable: {type(e).__name__}: {e}"
    if not isinstance(data, dict):
        return None, "state file is not a JSON object"
    return data, None


def _command_evidence(state: dict[str, Any] | None, command_id: str) -> dict[str, Any] | None:
    """Return durable evidence that *this named peer* executed command_id."""
    if not isinstance(state, dict) or not command_id:
        return None
    seen = state.get("seen_commands")
    if isinstance(seen, dict) and command_id in seen:
        entry = seen[command_id]
        return {
            "kind": "seen_commands",
            "entry": entry if isinstance(entry, dict) else {"value": entry},
        }
    history = state.get("history")
    if isinstance(history, list):
        for item in history:
            if isinstance(item, dict) and item.get("command_id") == command_id:
                return {"kind": "history", "entry": item}
    return None


def _config_matches(state: dict[str, Any], version: str, content_canonical: str) -> str | None:
    eff = state.get("effective")
    if isinstance(eff, dict) and eff.get("version") == version:
        try:
            if canonical_json(eff.get("content")) == content_canonical:
                return "effective"
        except (TypeError, ValueError):
            pass
    history = state.get("history")
    if isinstance(history, list):
        for item in history:
            if not isinstance(item, dict) or item.get("version") != version:
                continue
            try:
                if canonical_json(item.get("content")) == content_canonical:
                    return "history"
            except (TypeError, ValueError):
                continue
    return None


def _probe_live(services: list[Any]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    if not services:
        return result
    with ThreadPoolExecutor(max_workers=max(4, len(services))) as pool:
        future_map = {
            pool.submit(client.fetch_state, row["base_url"], 0.6): row["name"]
            for row in services
        }
        for future in future_map:
            name = future_map[future]
            try:
                body = future.result()
                peer = client.peer_name(body)
                if peer is not None and peer != name:
                    result[name] = {
                        "ok": False,
                        "identity_mismatch": True,
                        "peer_name": peer,
                        "error": f"endpoint identifies as {peer!r}, expected {name!r}",
                    }
                else:
                    result[name] = {"ok": True, "state": body, "peer_name": peer or name}
            except Exception as e:
                result[name] = {
                    "ok": False,
                    "identity_mismatch": False,
                    "peer_name": None,
                    "error": f"{type(e).__name__}: {e}",
                }
    return result


def verify_runtime(
    home: str | Path,
    *,
    release_id: str | None = None,
    sync_endpoints: bool = False,
    live: bool = True,
    advance: bool = True,
    only_pending: bool = False,
) -> dict[str, Any]:
    """Verify peer attribution for the active/latest non-superseded release.

    If ``release_id`` is given, verify that specific release instead.

    Safe in place against an old runtime directory:

    * schema is migrated and legacy claims start pending;
    * evidence is gathered from each named service's own durable state file
      and its (identity-checked) live endpoint;
    * evidence-backed confirmations/baselines are kept/marked verified and
      missing confirmations are backfilled when the durable command record
      proves they happened;
    * claims that cannot be attributed are marked pending_verification and
      block release advancement / baseline restore.
    """
    home = Path(home)
    store = Store(home)
    store.init_db()

    synced_endpoints = store.sync_endpoints_from_cluster() if sync_endpoints else []

    try:
        cluster = store.load_cluster()
    except Exception:
        cluster = {"group": GROUP, "services": []}
    group = cluster.get("group", GROUP)

    if release_id is not None:
        release = store.get_release(release_id)
        selection = "specified"
    else:
        release = store.active_release(group)
        selection = "active"
        if release is None:
            release = store.latest_non_superseded_release(group)
            selection = "latest_non_superseded" if release is not None else "none"

    services = store.list_services()
    cluster_specs = {s["name"]: s for s in cluster.get("services", [])}
    live_map = _probe_live(services) if live else {}

    # If the SQLite URL is still crossed but cluster.json has a different URL
    # that proves the expected identity, safely adopt the corrected endpoint.
    adopted_urls: dict[str, str] = {}
    if live:
        for svc in services:
            name = svc["name"]
            info = live_map.get(name) or {}
            if info.get("ok"):
                continue
            cluster_url = (cluster_specs.get(name) or {}).get("url")
            if not cluster_url or cluster_url.rstrip("/") == svc["base_url"].rstrip("/"):
                continue
            try:
                body = client.fetch_state(cluster_url, 0.6)
            except Exception:
                continue
            if client.peer_name(body) == name:
                live_map[name] = {"ok": True, "state": body, "peer_name": name}
                adopted_urls[name] = cluster_url

    try:
        initial_services = load_initial_state().get("services", {})
    except Exception:
        initial_services = {}

    service_reports: dict[str, dict[str, Any]] = {}
    # name -> identity-valid durable/live state usable as evidence
    evidence_states: dict[str, dict[str, Any]] = {}
    release_rows = store.release_services(release["id"]) if release is not None else []

    with store.connect() as conn:
        for svc in services:
            name = svc["name"]
            report: dict[str, Any] = {
                "base_url": svc["base_url"],
                "state_path": svc["state_path"],
                "file_identity_ok": False,
                "live_identity_ok": False,
                "peer_name": None,
                "identity": "unknown",
                "evidence_source": None,
                "file_error": None,
                "live_error": None,
            }

            state_path = svc["state_path"]
            file_state, file_error = _read_state_file(state_path)
            if file_state is None:
                alt_path = cluster_specs.get(name, {}).get("state_path")
                if alt_path and str(alt_path) != str(state_path):
                    file_state, file_error = _read_state_file(alt_path)
                    if file_state is not None:
                        state_path = alt_path
            if file_state is not None:
                file_peer = file_state.get("name")
                report["file_identity_ok"] = file_peer == name
                if not report["file_identity_ok"]:
                    report["file_error"] = (
                        f"state file belongs to {file_peer!r}, not {name!r}"
                    )
                    report["peer_name"] = file_peer
            else:
                report["file_error"] = file_error

            live_info = live_map.get(name)
            if live_info is not None:
                if live_info.get("ok"):
                    report["live_identity_ok"] = True
                    report["peer_name"] = live_info.get("peer_name") or name
                else:
                    report["live_error"] = live_info.get("error")
                    if live_info.get("peer_name"):
                        report["peer_name"] = live_info["peer_name"]

            # Persist an identity-checked observation of the service.
            live_state = (
                live_info.get("state")
                if live_info is not None and live_info.get("ok")
                else None
            )
            if live_state is not None:
                eff = live_state.get("effective") or {}
                content = eff.get("content")
                if name in adopted_urls:
                    conn.execute(
                        """
                        UPDATE services
                        SET reachable=1, identity_ok=1, peer_name=?, base_url=?,
                            actual_version=?, actual_content=?, actual_fence=?,
                            last_checked=?, last_error=NULL
                        WHERE name=?
                        """,
                        (
                            name,
                            adopted_urls[name],
                            eff.get("version"),
                            canonical_json(content) if content is not None else None,
                            int(live_state.get("fence", svc["actual_fence"] or 0)),
                            now_ts(),
                            name,
                        ),
                    )
                else:
                    conn.execute(
                        """
                        UPDATE services
                        SET reachable=1, identity_ok=1, peer_name=?,
                            actual_version=?, actual_content=?, actual_fence=?,
                            last_checked=?, last_error=NULL
                        WHERE name=?
                        """,
                        (
                            name,
                            eff.get("version"),
                            canonical_json(content) if content is not None else None,
                            int(live_state.get("fence", svc["actual_fence"] or 0)),
                            now_ts(),
                            name,
                        ),
                    )
            elif live_info is not None and live_info.get("identity_mismatch"):
                conn.execute(
                    """
                    UPDATE services
                    SET reachable=0, identity_ok=0, peer_name=?,
                        last_checked=?, last_error=?
                    WHERE name=?
                    """,
                    (
                        live_info.get("peer_name"),
                        now_ts(),
                        "peer_identity_mismatch: " + str(live_info.get("error")),
                        name,
                    ),
                )
            elif live_info is not None:
                conn.execute(
                    "UPDATE services SET reachable=0, last_checked=?, last_error=? WHERE name=?",
                    (now_ts(), str(live_info.get("error"))[:300], name),
                )

            # Durable file evidence repairs polluted actual_* columns even
            # while the service process is stopped. Identity columns record
            # the last live endpoint observation, so a crossed endpoint is
            # not whitewashed by a correctly named state file.
            if report["file_identity_ok"] and file_state is not None:
                eff = file_state.get("effective") or {}
                content = eff.get("content")
                if live:
                    conn.execute(
                        """
                        UPDATE services
                        SET actual_version=?, actual_content=?, actual_fence=?
                        WHERE name=?
                        """,
                        (
                            eff.get("version"),
                            canonical_json(content) if content is not None else None,
                            int(file_state.get("fence", svc["actual_fence"] or 0)),
                            name,
                        ),
                    )
                else:
                    conn.execute(
                        """
                        UPDATE services
                        SET identity_ok=1, peer_name=?, actual_version=?,
                            actual_content=?, actual_fence=?
                        WHERE name=?
                        """,
                        (
                            name,
                            eff.get("version"),
                            canonical_json(content) if content is not None else None,
                            int(file_state.get("fence", svc["actual_fence"] or 0)),
                            name,
                        ),
                    )

            if report["live_identity_ok"] and live_state is not None:
                evidence_states[name] = live_state
                report["evidence_source"] = "live"
            elif report["file_identity_ok"] and file_state is not None:
                evidence_states[name] = file_state
                report["evidence_source"] = "state-file"

            # Overall endpoint attribution: an explicit wrong peer wins as a
            # visible mismatch even when the on-disk evidence is attributable.
            if live_info is not None and live_info.get("identity_mismatch"):
                report["identity"] = "peer_mismatch"
            elif report["live_identity_ok"]:
                report["identity"] = "verified"
            elif report["file_identity_ok"]:
                report["identity"] = "verified"
            else:
                report["identity"] = "unverified"

            service_reports[name] = report

        release_report: dict[str, Any] | None = None
        if release is not None:
            rid = release["id"]
            rows = release_rows
            row_reports: list[dict[str, Any]] = []
            for rs in rows:
                name = rs["service_name"]
                ev_state = evidence_states.get(name)
                rr: dict[str, Any] = {
                    "service_name": name,
                    "batch_index": rs["batch_index"],
                    "started": bool(rs["started"]),
                    "apply": {
                        "stored_confirmed": bool(rs["applied_confirmed"]),
                        "attribution": rs["apply_attribution"],
                        "evidence": False,
                        "evidence_kind": None,
                        "backfilled": False,
                    },
                    "rollback": {
                        "stored_confirmed": bool(rs["rollback_confirmed"]),
                        "attribution": rs["rollback_attribution"],
                        "evidence": False,
                        "evidence_kind": None,
                        "backfilled": False,
                    },
                    "baseline": {
                        "version": rs["baseline_version"],
                        "attribution": rs["baseline_attribution"],
                        "corroborated_by": None,
                    },
                }

                updates: dict[str, Any] = {}
                baseline_canonical = canonical_json(json.loads(rs["baseline_content"]))

                # A confirmation is only backfilled/preserved from proof that
                # the exact command id was durably processed by this named
                # peer. Merely matching effective config corroborates a
                # baseline but does not substitute for a live rollback/apply
                # confirmation, so an unreachable started service still gates.
                apply_ev = _command_evidence(ev_state, rs["apply_command_id"])
                old_apply_attr = rs["apply_attribution"]
                if apply_ev is not None:
                    rr["apply"]["evidence"] = True
                    rr["apply"]["evidence_kind"] = apply_ev["kind"]
                    updates["apply_attribution"] = VERIFIED
                    if not rs["applied_confirmed"]:
                        updates["applied_confirmed"] = 1
                        rr["apply"]["backfilled"] = True
                elif rs["applied_confirmed"]:
                    if only_pending:
                        updates["apply_attribution"] = rs["apply_attribution"]
                    else:
                        updates["apply_attribution"] = PENDING_VERIFICATION
                else:
                    updates["apply_attribution"] = VERIFIED
                rr["apply"]["attribution"] = updates["apply_attribution"]

                rb_ev = _command_evidence(ev_state, rs["rollback_command_id"])
                old_rb_attr = rs["rollback_attribution"]
                if rb_ev is not None:
                    rr["rollback"]["evidence"] = True
                    rr["rollback"]["evidence_kind"] = rb_ev["kind"]
                    updates["rollback_attribution"] = VERIFIED
                    if not rs["rollback_confirmed"]:
                        updates["rollback_confirmed"] = 1
                        rr["rollback"]["backfilled"] = True
                elif rs["rollback_confirmed"]:
                    if only_pending:
                        updates["rollback_attribution"] = rs["rollback_attribution"]
                    else:
                        updates["rollback_attribution"] = PENDING_VERIFICATION
                else:
                    updates["rollback_attribution"] = VERIFIED
                rr["rollback"]["attribution"] = updates["rollback_attribution"]

                # ---- rollback baseline -------------------------------------
                where_from: str | None = None
                if ev_state is not None:
                    where_from = _config_matches(
                        ev_state, rs["baseline_version"], baseline_canonical
                    )
                if where_from is None:
                    init_cfg = initial_services.get(name)
                    if init_cfg and init_cfg.get("version") == rs["baseline_version"]:
                        if (
                            canonical_json(init_cfg.get("content", {}))
                            == baseline_canonical
                        ):
                            where_from = "bundled-initial-state"
                if where_from is not None:
                    updates["baseline_attribution"] = VERIFIED
                    rr["baseline"]["corroborated_by"] = where_from
                elif only_pending:
                    # Startup healing pass never downgrades an existing claim;
                    # the explicit `relctl verify` pass is authoritative.
                    updates["baseline_attribution"] = rs["baseline_attribution"]
                else:
                    updates["baseline_attribution"] = PENDING_VERIFICATION
                rr["baseline"]["attribution"] = updates["baseline_attribution"]

                # Persist only changed columns and audit meaningful transitions.
                set_clauses = ", ".join(f"{k}=?" for k in updates)
                params = [*updates.values(), rid, name]
                conn.execute(
                    f"UPDATE release_services SET {set_clauses} "
                    "WHERE release_id=? AND service_name=?",
                    params,
                )

                if rr["apply"]["backfilled"]:
                    store.add_event(
                        conn,
                        rid,
                        "confirmation_backfilled",
                        f"{name}: durable {rr['apply']['evidence_kind']} evidence proves "
                        f"{rs['apply_command_id']}; backfilled apply confirmation",
                    )
                elif (
                    old_apply_attr != updates["apply_attribution"]
                    and updates["apply_attribution"] == VERIFIED
                ):
                    store.add_event(
                        conn,
                        rid,
                        "attribution_verified",
                        f"{name}: apply confirmation attribution verified from "
                        f"{rr['apply']['evidence_kind']} evidence",
                    )
                elif updates["apply_attribution"] == PENDING_VERIFICATION and (
                    old_apply_attr != PENDING_VERIFICATION or rs["applied_confirmed"]
                ):
                    if old_apply_attr != PENDING_VERIFICATION:
                        store.add_event(
                            conn,
                            rid,
                            "attribution_pending_verification",
                            f"{name}: apply confirmation cannot be attributed; "
                            "marked 待核实 and excluded from completion",
                        )

                if rr["rollback"]["backfilled"]:
                    store.add_event(
                        conn,
                        rid,
                        "rollback_confirmation_backfilled",
                        f"{name}: durable evidence proves {rs['rollback_command_id']}; "
                        "backfilled rollback confirmation",
                    )
                elif updates["rollback_attribution"] == PENDING_VERIFICATION and (
                    old_rb_attr != PENDING_VERIFICATION
                ):
                    store.add_event(
                        conn,
                        rid,
                        "attribution_pending_verification",
                        f"{name}: rollback confirmation cannot be attributed; marked 待核实",
                    )

                if (
                    updates["baseline_attribution"] == PENDING_VERIFICATION
                    and rs["baseline_attribution"] != PENDING_VERIFICATION
                ):
                    store.add_event(
                        conn,
                        rid,
                        "baseline_pending_verification",
                        f"{name}: rollback baseline {rs['baseline_version']} cannot be "
                        "attributed; marked 待核实 and will not be pushed for restore",
                    )

                row_reports.append(rr)

            release_report = {
                "id": rid,
                "selection": selection,
                "status": release["status"],
                "target_version": release["target_version"],
                "rollback_requested": bool(release["rollback_requested"]),
                "rows": row_reports,
            }

            # If an old bug already recorded a terminal completion built on
            # unattributed claims, revoke it (when no other release is active)
            # so it cannot be advanced from or used as a clean rollback point.
            active_other = conn.execute(
                "SELECT id FROM releases WHERE group_name=? AND id<>? "
                "AND status IN ('running','paused','rolling_back')",
                (group, rid),
            ).fetchone()
            apply_unresolved = conn.execute(
                """
                SELECT COUNT(*) AS n FROM release_services
                WHERE release_id=? AND started=1
                  AND NOT (applied_confirmed=1 AND apply_attribution='verified')
                """,
                (rid,),
            ).fetchone()["n"]
            rb_unresolved = conn.execute(
                """
                SELECT COUNT(*) AS n FROM release_services
                WHERE release_id=? AND started=1
                  AND NOT (rollback_confirmed=1
                          AND rollback_attribution='verified'
                          AND baseline_attribution='verified')
                """,
                (rid,),
            ).fetchone()["n"]
            revoked: str | None = None
            if (
                not only_pending
                and release["status"] == "completed"
                and apply_unresolved
                and active_other is None
            ):
                conn.execute(
                    "UPDATE releases SET status='running', updated_at=? WHERE id=?",
                    (now_ts(), rid),
                )
                store.add_event(
                    conn,
                    rid,
                    "completion_revoked_pending_verification",
                    f"{apply_unresolved} started service(s) lack verified apply "
                    "attribution; false completion revoked until reconciled/verified",
                )
                revoked = "completed->running"
            elif (
                not only_pending
                and release["status"] == "rolled_back"
                and rb_unresolved
                and active_other is None
            ):
                conn.execute(
                    "UPDATE releases SET status='rolling_back', "
                    "rollback_status='in_progress', updated_at=? WHERE id=?",
                    (now_ts(), rid),
                )
                store.add_event(
                    conn,
                    rid,
                    "rollback_completion_revoked_pending_verification",
                    f"{rb_unresolved} started service(s) lack verified rollback/"
                    "baseline attribution; rollback completion revoked pending verification",
                )
                revoked = "rolled_back->rolling_back"
            if revoked:
                release_report["status_revoked"] = revoked
        conn.commit()

    # With evidence-based state repaired, let the state machine make any safe
    # forward progress in place. Pending attributions still block it.
    advanced: dict[str, Any] = {"attempted": False}
    if advance and release is not None:
        from .coordinator import Coordinator

        coord = Coordinator(home)
        try:
            rid = release["id"]
            fresh = store.get_release(rid)
            if fresh["rollback_requested"]:
                coord.complete_rollback_if_done(rid)
            else:
                coord.advance_if_complete(rid)
            after = store.get_release(rid)
            advanced = {
                "attempted": True,
                "id": rid,
                "status_before": release["status"],
                "status_after": after["status"],
                "batch_index_after": int(after["batch_index"]),
            }
        finally:
            coord.close()

    pending_items: list[str] = []
    backfilled = 0
    verified_claims = 0
    if release_report is not None:
        for rr in release_report["rows"]:
            n = rr["service_name"]
            if rr["apply"]["attribution"] != VERIFIED and rr["apply"]["stored_confirmed"]:
                pending_items.append(f"{n}:apply-confirmation")
            if (
                rr["rollback"]["attribution"] != VERIFIED
                and rr["rollback"]["stored_confirmed"]
            ):
                pending_items.append(f"{n}:rollback-confirmation")
            if rr["baseline"]["attribution"] != VERIFIED:
                pending_items.append(f"{n}:baseline")
            if rr["apply"]["backfilled"] or rr["rollback"]["backfilled"]:
                backfilled += 1
            if rr["apply"]["evidence"] or rr["rollback"]["evidence"]:
                verified_claims += 1

    report = {
        "home": str(home),
        "synced_endpoints": synced_endpoints,
        "adopted_endpoints": adopted_urls,
        "release": release_report,
        "services": service_reports,
        "advanced": advanced,
        "pending": pending_items,
        "unverified_items": pending_items,
        "summary": {
            "pending_verification": len(pending_items),
            "pending": len(pending_items),
            "unverified": len(pending_items),
            "pending_items": pending_items,
            "items": pending_items,
            "backfilled_services": backfilled,
            "backfilled": backfilled,
            "evidence_backed_services": verified_claims,
            "all_attributed": not pending_items,
            "verified": not pending_items,
        },
    }
    return report


# Plausible descriptive aliases for callers/tests.
verify_release_attribution = verify_runtime
verify_attribution = verify_runtime


def render_verify_report(report: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append(f"verify: {report['home']}")
    if report.get("synced_endpoints"):
        lines.append(f"  endpoints synced from cluster.json: {', '.join(report['synced_endpoints'])}")

    rel = report.get("release")
    if rel is None:
        lines.append("  release: none to verify")
    else:
        lines.append(
            f"  release: {rel['id']} ({rel['selection']}) "
            f"target={rel['target_version']} status={rel['status']}"
        )
        for rr in rel["rows"]:
            name = rr["service_name"]
            ap = rr["apply"]
            rb = rr["rollback"]
            bl = rr["baseline"]
            if ap["evidence"]:
                apply_part = f"apply✓evidence({ap['evidence_kind']})"
            elif ap["stored_confirmed"] and ap["attribution"] != VERIFIED:
                apply_part = f"apply?{PENDING_LABEL_ZH}"
            elif ap["stored_confirmed"]:
                apply_part = "apply✓"
            else:
                apply_part = "apply-pending"
            if ap["backfilled"]:
                apply_part += "+backfilled"

            if rb["evidence"]:
                rb_part = "rollback✓evidence"
            elif rb["stored_confirmed"] and rb["attribution"] != VERIFIED:
                rb_part = f"rollback?{PENDING_LABEL_ZH}"
            elif rb["stored_confirmed"]:
                rb_part = "rollback✓"
            else:
                rb_part = "rollback-pending"

            if bl["attribution"] == VERIFIED:
                base_part = f"baseline✓({bl['corroborated_by']})"
            else:
                base_part = f"baseline?{PENDING_LABEL_ZH}"

            lines.append(f"    - {name}: {apply_part}; {rb_part}; {base_part}")

    mismatch = [
        n for n, s in report["services"].items() if s["identity"] == "peer_mismatch"
    ]
    if mismatch:
        lines.append(
            "  peer mismatch (endpoint belongs to another service): " + ", ".join(mismatch)
        )

    summary = report["summary"]
    if summary["pending_verification"]:
        lines.append(
            f"  ⚠ {summary['pending_verification']} item(s) marked "
            f"{PENDING_LABEL_ZH}: {', '.join(summary['pending_items'])}"
        )
        lines.append("    they must not advance a release or drive a restore until verified")
    else:
        lines.append("  ✅ all confirmations and baselines attributed")
    if summary["backfilled_services"]:
        lines.append(f"  backfilled safe missing confirmation(s): {summary['backfilled_services']} service row(s)")
    adv = report.get("advanced") or {}
    if adv.get("attempted"):
        lines.append(f"  state machine: {adv['status_before']} -> {adv['status_after']}")
    return "\n".join(lines)
