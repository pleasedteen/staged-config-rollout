from __future__ import annotations

import json
import socket
import sqlite3
import tempfile
import threading
import time
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path

from relcoord import client
from relcoord.coordinator import Coordinator
from relcoord.demo import LEGACY_SCHEMA
from relcoord.service import MockService, ServiceAPIError, make_handler
from relcoord.status import build_status, render_status
from relcoord.storage import Store, load_initial_state
from relcoord.util import PROJECT_ROOT, canonical_json
from relcoord.verify import PENDING_VERIFICATION, verify_runtime

V2_CONFIG = PROJECT_ROOT / "configs" / "target-v2.json"
V2 = json.loads(V2_CONFIG.read_text(encoding="utf-8"))
INITIAL = load_initial_state()["services"]


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def serve(mock: MockService, port: int | None = None) -> tuple[ThreadingHTTPServer, int]:
    class Server(ThreadingHTTPServer):
        allow_reuse_address = True
        daemon_threads = True

    httpd = Server(("127.0.0.1", port or 0), make_handler(mock))
    bound_port = httpd.server_address[1]
    thread = threading.Thread(
        target=httpd.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True
    )
    thread.start()
    return httpd, bound_port


class ServiceIdentityTests(unittest.TestCase):
    def test_mismatched_service_name_rejected_before_any_write(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "svc-a.json"
            svc = MockService(
                "svc-a", path, {"svc-a": {"version": "v1", "content": {"k": 1}}}
            )
            payload = {
                "command_id": "cmd-1",
                "fence": 1,
                "release_id": "rel-x",
                "mode": "publish",
                "version": "v2",
                "content": {"k": 2},
                "service_name": "svc-b",
            }
            with self.assertRaises(ServiceAPIError) as ctx:
                svc.apply(payload)
            self.assertEqual(ctx.exception.status, 409)
            self.assertEqual(ctx.exception.code, "identity_mismatch")
            state = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(state["effective"]["version"], "v1")
            self.assertEqual(len(state["history"]), 1)
            self.assertEqual(state["seen_commands"], {})

    def test_matching_service_name_applies_and_reports_identity(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            svc = MockService(
                "svc-a",
                Path(td) / "svc-a.json",
                {"svc-a": {"version": "v1", "content": {}}},
            )
            resp = svc.apply(
                {
                    "command_id": "cmd-1",
                    "fence": 1,
                    "release_id": "r",
                    "mode": "publish",
                    "version": "v2",
                    "content": {},
                    "service_name": "svc-a",
                }
            )
            self.assertEqual(resp["service"], "svc-a")
            self.assertEqual(resp["service_name"], "svc-a")
            self.assertEqual(client.peer_name(resp), "svc-a")

    def test_peer_name_helper(self) -> None:
        self.assertEqual(client.peer_name({"name": "x"}), "x")
        self.assertEqual(client.peer_name({"service": "y"}), "y")
        self.assertIsNone(client.peer_name({}))
        with self.assertRaises(client.PeerIdentityMismatch):
            client.expect_identity({"name": "other"}, "expected")


    def test_managed_command_id_fallback_identity_rejects_cross(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            svc = MockService(
                "svc-1",
                Path(td) / "svc-1.json",
                {"svc-1": {"version": "v1", "content": {}}},
            )
            # No explicit service_name (old client), canonical managed id for svc-2.
            payload = {
                "command_id": "apply-rel-20260101010101-ab12cd-svc-2",
                "fence": 1,
                "release_id": "rel-20260101010101-ab12cd",
                "mode": "publish",
                "version": "v2",
                "content": {},
            }
            with self.assertRaises(ServiceAPIError) as ctx:
                svc.apply(payload)
            self.assertEqual(ctx.exception.code, "identity_mismatch")
            # Same id suffix for this peer is accepted.
            payload["command_id"] = "apply-rel-20260101010101-ab12cd-svc-1"
            resp = svc.apply(payload)
            self.assertFalse(resp["duplicate"])


class CrossedEndpointCoordinatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self._td = tempfile.TemporaryDirectory()
        self.home = Path(self._td.name)
        self.port1 = free_port()
        self.port2 = free_port()
        self.store = Store(self.home)
        self.store.initialize_home(
            names=["svc-1", "svc-2"], ports=[self.port1, self.port2]
        )
        self.mock1 = MockService(
            "svc-1", self.home / "services" / "svc-1.json", INITIAL
        )
        self.httpd1, _ = serve(self.mock1, self.port1)

    def tearDown(self) -> None:
        try:
            self.httpd1.shutdown()
            self.httpd1.server_close()
        except Exception:
            pass
        self._td.cleanup()

    def _wait_row(self, rid: str, name: str, pred, timeout: float = 8.0):
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            row = next(
                r
                for r in self.store.release_services(rid)
                if r["service_name"] == name
            )
            last = row
            if pred(row):
                return row
            time.sleep(0.1)
        self.fail(f"row {name} never matched; last={dict(last) if last else None}")

    def test_crossed_endpoint_does_not_complete_or_write_wrong_peer(self) -> None:
        crossed_url = f"http://127.0.0.1:{self.port1}"
        with self.store.connect() as conn:
            conn.execute(
                "UPDATE services SET base_url=? WHERE name='svc-2'", (crossed_url,)
            )
            conn.commit()

        rid = self.store.submit_release(
            config_path=V2_CONFIG, batches=[["svc-1", "svc-2"]]
        )
        coord = Coordinator(self.home)
        try:
            deadline = time.monotonic() + 8
            while time.monotonic() < deadline:
                coord.tick()
                r2 = next(
                    r
                    for r in self.store.release_services(rid)
                    if r["service_name"] == "svc-2"
                )
                if "identity_mismatch" in (r2["last_error"] or ""):
                    break
                time.sleep(0.1)

            r1 = self._wait_row(
                rid,
                "svc-1",
                lambda r: r["applied_confirmed"]
                and r["apply_attribution"] == "verified",
            )
            r2 = next(
                r
                for r in self.store.release_services(rid)
                if r["service_name"] == "svc-2"
            )
            self.assertIn("identity_mismatch", r2["last_error"])
            self.assertFalse(
                r2["applied_confirmed"] and r2["apply_attribution"] == "verified"
            )
            self.assertEqual(self.store.get_release(rid)["status"], "running")

            state1 = client.fetch_state(crossed_url, timeout=1)
            self.assertNotIn(f"apply-{rid}-svc-2", state1["seen_commands"])
            self.assertIn(f"apply-{rid}-svc-1", state1["seen_commands"])

            # Live status must flag the mismatch rather than import svc-1's state.
            data = build_status(self.home, live=True)
            svc2 = next(s for s in data["services"] if s["name"] == "svc-2")
            self.assertFalse(svc2["live"]["ok"])
            self.assertTrue(svc2["live"]["identity_mismatch"])
            self.assertEqual(svc2["live"]["peer_name"], "svc-1")
            self.assertIn("PEER-MISMATCH", render_status(data))

            # Correct the endpoint and bring up the real svc-2; takeover completes.
            real_url = f"http://127.0.0.1:{self.port2}"
            with self.store.connect() as conn:
                conn.execute(
                    "UPDATE services SET base_url=? WHERE name='svc-2'", (real_url,)
                )
                conn.commit()
            mock2 = MockService(
                "svc-2", self.home / "services" / "svc-2.json", INITIAL
            )
            httpd2, _ = serve(mock2, self.port2)
            try:
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    coord.tick()
                    if self.store.get_release(rid)["status"] == "completed":
                        break
                    time.sleep(0.1)
                self.assertEqual(
                    self.store.get_release(rid)["status"], "completed"
                )
                st2 = client.fetch_state(real_url, timeout=1)
                self.assertEqual(st2["effective"]["version"], "v2.0.0")
            finally:
                httpd2.shutdown()
                httpd2.server_close()
        finally:
            coord.close()


class LegacyVerificationTests(unittest.TestCase):
    def setUp(self) -> None:
        self._td = tempfile.TemporaryDirectory()
        self.home = Path(self._td.name)
        self.store = Store(self.home)
        self.store.initialize_home(
            names=["svc-1", "svc-2"], ports=[free_port(), free_port()]
        )
        self.rid = "rel-legacy-test-1"

        # svc-1 has durable evidence for its apply command.
        MockService(
            "svc-1", self.home / "services" / "svc-1.json", INITIAL
        ).apply(
            {
                "command_id": f"apply-{self.rid}-svc-1",
                "fence": 1,
                "release_id": self.rid,
                "mode": "publish",
                "version": V2["version"],
                "content": V2["content"],
            }
        )

        # Rebuild the DB with the pre-attribution schema and legacy rows.
        db_path = self.home / "coordinator.sqlite3"
        for suffix in ("", "-wal", "-shm"):
            try:
                Path(str(db_path) + suffix).unlink()
            except FileNotFoundError:
                pass
        now = time.time()
        conn = sqlite3.connect(db_path)
        conn.executescript(LEGACY_SCHEMA)
        conn.execute("INSERT INTO meta(key,value) VALUES('next_fence','2')")
        for name, port in (("svc-1", free_port()), ("svc-2", free_port())):
            cfg = INITIAL[name]
            conn.execute(
                """INSERT INTO services(name,group_name,base_url,state_path,
                   initial_version,initial_content,actual_version,actual_content,
                   actual_fence,reachable,last_checked)
                   VALUES(?,?,?,?,?,?,?,?,0,0,?)""",
                (
                    name,
                    "core",
                    f"http://127.0.0.1:{port}",
                    str(self.home / "services" / f"{name}.json"),
                    cfg["version"],
                    canonical_json(cfg["content"]),
                    cfg["version"],
                    canonical_json(cfg["content"]),
                    now,
                ),
            )
        conn.execute(
            """INSERT INTO releases(id,group_name,target_version,target_content,
               batches,status,batch_index,apply_fence,superseded,created_at,updated_at)
               VALUES(?,?,?,?,?,'running',0,1,0,?,?)""",
            (
                self.rid,
                "core",
                V2["version"],
                canonical_json(V2["content"]),
                json.dumps([["svc-1", "svc-2"]]),
                now,
                now,
            ),
        )
        # svc-1: confirmed + evidence (preserve). svc-2: false confirmation,
        # no evidence, and a polluted baseline.
        for name in ("svc-1", "svc-2"):
            if name == "svc-2":
                baseline_version = "v9.9.9-wrong"
                baseline_content = canonical_json({"nope": True})
            else:
                baseline_version = INITIAL[name]["version"]
                baseline_content = canonical_json(INITIAL[name]["content"])
            conn.execute(
                """INSERT INTO release_services(release_id,service_name,batch_index,
                   baseline_version,baseline_content,started,applied_confirmed,
                   apply_command_id,rollback_command_id)
                   VALUES(?,? ,0,?,?,1,1,?,?)""",
                (
                    self.rid,
                    name,
                    baseline_version,
                    baseline_content,
                    f"apply-{self.rid}-{name}",
                    f"rollback-{self.rid}-{name}",
                ),
            )
        conn.commit()
        conn.close()

    def tearDown(self) -> None:
        self._td.cleanup()

    def test_migration_parks_legacy_claims(self) -> None:
        self.store.init_db()
        rows = {r["service_name"]: r for r in self.store.release_services(self.rid)}
        for name in ("svc-1", "svc-2"):
            self.assertEqual(rows[name]["apply_attribution"], PENDING_VERIFICATION)
            self.assertEqual(
                rows[name]["baseline_attribution"], PENDING_VERIFICATION
            )

    def test_verify_preserves_evidence_and_parks_unattributable(self) -> None:
        report = verify_runtime(self.home, live=False)
        rows = {r["service_name"]: r for r in self.store.release_services(self.rid)}

        self.assertEqual(rows["svc-1"]["apply_attribution"], "verified")
        self.assertEqual(rows["svc-2"]["apply_attribution"], PENDING_VERIFICATION)
        self.assertEqual(rows["svc-2"]["baseline_attribution"], PENDING_VERIFICATION)
        self.assertEqual(rows["svc-1"]["baseline_attribution"], "verified")

        # Pending attribution still blocks completion.
        self.assertEqual(self.store.get_release(self.rid)["status"], "running")
        pending = report["summary"]["pending_items"]
        self.assertIn("svc-2:apply-confirmation", pending)
        self.assertIn("svc-2:baseline", pending)

        text = render_status(build_status(self.home, live=False))
        self.assertIn("待核实", text)

    def test_false_completed_release_is_reopened_when_claim_unverified(self) -> None:
        with self.store.connect() as conn:
            conn.execute(
                "UPDATE releases SET status='completed', completed_at=? WHERE id=?",
                (time.time(), self.rid),
            )
            conn.commit()
        report = verify_runtime(self.home, live=False)
        self.assertEqual(
            report["release"].get("status_revoked"), "completed->running"
        )
        self.assertEqual(self.store.get_release(self.rid)["status"], "running")

    def test_unverified_baseline_blocks_rollback_push(self) -> None:
        verify_runtime(self.home, live=False)
        _, changed = self.store.request_rollback()
        self.assertTrue(changed)
        rel = self.store.get_release(self.rid)
        coord = Coordinator(self.home)
        try:
            coord.reconcile_rollback(rel)
        finally:
            coord.close()
        r2 = next(
            r
            for r in self.store.release_services(self.rid)
            if r["service_name"] == "svc-2"
        )
        self.assertEqual(r2["rollback_attempts"], 0)
        self.assertIn("baseline_pending_verification", r2["last_error"])
        self.assertEqual(self.store.get_release(self.rid)["status"], "rolling_back")

        # Once the baseline is corrected to the named service's own history,
        # the reconciler corroborates and attempts the restore (here no live
        # server, so it fails as unreachable rather than baseline-blocked).
        true_cfg = INITIAL["svc-2"]
        with self.store.connect() as conn:
            conn.execute(
                "UPDATE release_services SET baseline_version=?, baseline_content=? "
                "WHERE release_id=? AND service_name='svc-2'",
                (
                    true_cfg["version"],
                    canonical_json(true_cfg["content"]),
                    self.rid,
                ),
            )
            conn.commit()
        rel = self.store.get_release(self.rid)
        coord2 = Coordinator(self.home)
        try:
            coord2.reconcile_rollback(rel)
        finally:
            coord2.close()
        r2 = next(
            r
            for r in self.store.release_services(self.rid)
            if r["service_name"] == "svc-2"
        )
        self.assertEqual(r2["baseline_attribution"], "verified")
        self.assertGreaterEqual(r2["rollback_attempts"], 1)
        self.assertNotIn("baseline_pending_verification", r2["last_error"])


if __name__ == "__main__":
    unittest.main()
