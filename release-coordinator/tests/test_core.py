from __future__ import annotations

import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path

from relcoord.coordinator import Coordinator
from relcoord.service import MockService, ServiceAPIError, make_handler
from relcoord.storage import Store
from relcoord.util import PROJECT_ROOT, content_hash

V2_CONFIG = PROJECT_ROOT / "configs" / "target-v2.json"
BATCHES = [
    ["svc-1", "svc-2"],
    ["svc-3", "svc-4"],
    ["svc-5", "svc-6"],
]


class StoreStateTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.home = Path(self._tmp.name)
        self.store = Store(self.home)
        self.store.initialize_home()

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_one_active_release_and_durable_decisions(self) -> None:
        rid = self.store.submit_release(config_path=V2_CONFIG, batches=BATCHES)

        with self.assertRaises(Exception):
            self.store.submit_release(config_path=V2_CONFIG, batches=BATCHES)

        _, changed = self.store.pause(reason="unit")
        self.assertTrue(changed)
        self.assertTrue(self.store.get_release(rid)["pause_requested"])

        rb_id, rb_changed = self.store.request_rollback(reason="unit")
        self.assertEqual(rb_id, rid)
        self.assertTrue(rb_changed)
        rel = self.store.get_release(rid)
        self.assertEqual(rel["status"], "rolling_back")
        self.assertTrue(rel["rollback_requested"])
        self.assertGreater(rel["rollback_fence"], rel["apply_fence"])

        # The partial unique index still blocks a second publish.
        with self.assertRaises(Exception):
            self.store.submit_release(config_path=V2_CONFIG, batches=BATCHES)

    def test_rollback_uses_distinct_service_baselines(self) -> None:
        rid = self.store.submit_release(config_path=V2_CONFIG, batches=BATCHES)
        rows = {r["service_name"]: r for r in self.store.release_services(rid)}
        self.assertEqual(rows["svc-1"]["baseline_version"], "v1.0.0")
        self.assertEqual(rows["svc-3"]["baseline_version"], "v1.4.2")
        self.assertEqual(rows["svc-5"]["baseline_version"], "v0.9.7")
        self.assertTrue(rows["svc-1"]["started"])
        self.assertFalse(rows["svc-3"]["started"])

        _, changed = self.store.request_rollback()
        self.assertTrue(changed)

        with self.store.connect() as conn:
            conn.execute(
                "UPDATE release_services SET rollback_confirmed=1 "
                "WHERE release_id=? AND started=1",
                (rid,),
            )
            conn.commit()

        coordinator = Coordinator(self.home)
        try:
            coordinator.complete_rollback_if_done(rid)
        finally:
            coordinator.close()
        self.assertEqual(self.store.get_release(rid)["status"], "rolled_back")

    def test_completed_release_can_roll_back(self) -> None:
        rid = self.store.submit_release(config_path=V2_CONFIG, batches=BATCHES)
        with self.store.connect() as conn:
            conn.execute("UPDATE release_services SET applied_confirmed=1 WHERE release_id=?", (rid,))
            conn.execute(
                "UPDATE releases SET status='completed', completed_at=? WHERE id=?",
                (1.0, rid),
            )
            conn.commit()

        self.assertEqual(self.store.latest_release()["status"], "completed")
        rb_id, changed = self.store.request_rollback()
        self.assertEqual(rb_id, rid)
        self.assertTrue(changed)
        self.assertEqual(self.store.get_release(rid)["status"], "rolling_back")


class MockServiceFenceTests(unittest.TestCase):
    def test_duplicate_command_and_stale_fence(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            service = MockService(
                "svc-x",
                Path(td) / "svc-x.json",
                {"svc-x": {"version": "v1", "content": {"k": 1}}},
            )
            payload = {
                "command_id": "cmd-1",
                "fence": 1,
                "release_id": "rel-x",
                "mode": "publish",
                "version": "v2",
                "content": {"k": 2},
            }
            first = service.apply(payload)
            self.assertFalse(first["duplicate"])
            second = service.apply(dict(payload))
            self.assertTrue(second["duplicate"])
            self.assertEqual(service.public_state()["effective"]["version"], "v2")

            service.apply(
                {
                    "command_id": "rb-1",
                    "fence": 2,
                    "release_id": "rel-x",
                    "mode": "rollback",
                    "version": "v1",
                    "content": {"k": 1},
                }
            )
            with self.assertRaises(ServiceAPIError) as ctx:
                service.apply(dict(payload))
            self.assertEqual(ctx.exception.status, 409)
            self.assertEqual(ctx.exception.code, "stale_fence")
            self.assertEqual(service.public_state()["effective"]["version"], "v1")


class DigestObserveTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.td = Path(self._tmp.name)
        self.content = {"blob": "A" * 3000}
        self.service = MockService(
            "svc-d",
            self.td / "svc-d.json",
            {"svc-d": {"version": "v1", "content": self.content}},
        )

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_unchanged_observation_is_compact(self) -> None:
        digest = content_hash(self.content)
        body = self.service.observe("v1", digest, 0)
        self.assertFalse(body["changed"])
        self.assertNotIn("content", body["effective"])
        self.assertEqual(body["effective"]["content_hash"], digest)
        # Compact summaries must fit the 2 KiB per-service per-round budget.
        import json as _json

        self.assertLess(len(_json.dumps(body).encode("utf-8")), 256)

    def test_same_version_different_content_forces_full_observation(self) -> None:
        digest = content_hash(self.content)
        # Same version name, different content behind it.
        self.service.state["effective"] = {"version": "v1", "content": {"blob": "B" * 3000}}
        body = self.service.observe("v1", digest, 0)
        self.assertTrue(body["changed"])
        self.assertIn("content", body["effective"])
        self.assertEqual(body["effective"]["content"], {"blob": "B" * 3000})
        self.assertEqual(body["effective"]["content_hash"], content_hash({"blob": "B" * 3000}))

    def test_fence_and_version_mismatches_force_full_observation(self) -> None:
        digest = content_hash(self.content)
        self.assertTrue(self.service.observe("v2", digest, 0)["changed"])
        self.assertTrue(self.service.observe("v1", "deadbeef", 0)["changed"])
        self.assertTrue(self.service.observe("v1", digest, 9)["changed"])
        # An empty fingerprint (cold coordinator) always triggers a full fetch.
        cold = self.service.observe("", "", 0)
        self.assertTrue(cold["changed"])
        self.assertIn("content", cold["effective"])


class CoordinatorProbeTrafficTests(unittest.TestCase):
    def setUp(self) -> None:
        from relcoord.demo import free_tcp_ports

        self._tmp = tempfile.TemporaryDirectory()
        self.home = Path(self._tmp.name)
        self.port = free_tcp_ports(1)[0]
        self.store = Store(self.home)
        self.store.initialize_home(names=["svc-1"], ports=[self.port])
        spec = self.store.load_cluster()["services"][0]
        self.state_path = Path(spec["state_path"])
        # Remove the seeded state file so MockService boots from the explicit
        # large initial configuration below instead of the bundled small one.
        self.state_path.unlink()
        self.big = {"blob": "C" * 4000}
        self.service = MockService(
            "svc-1",
            self.state_path,
            {"svc-1": {"version": "v1", "content": self.big}},
        )
        self.httpd = ThreadingHTTPServer(("127.0.0.1", self.port), make_handler(self.service))
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()
        self.coordinator = Coordinator(self.home)

    def tearDown(self) -> None:
        self.coordinator.close()
        self.httpd.shutdown()
        self.httpd.server_close()
        self._tmp.cleanup()

    def test_probe_rounds_and_drift_refresh(self) -> None:
        # Round 1: seeded fingerprints already match -> compact.
        self.coordinator.probe_services()
        self.coordinator.probe_services()
        row = self.store.get_service("svc-1")
        self.assertEqual(row["last_probe_changed"], 0)
        self.assertLessEqual(row["last_probe_bytes"], 256)

        # Same version name, swapped content -> one full observation, then tiny.
        self.service.state["effective"] = {"version": "v1", "content": {"blob": "D" * 4000}}
        self.coordinator.probe_services()
        row = self.store.get_service("svc-1")
        self.assertEqual(row["last_probe_changed"], 1)
        self.assertGreater(row["last_probe_bytes"], 2 * 1024)
        import json as _json

        self.assertEqual(
            row["actual_content"],
            _json.dumps({"blob": "D" * 4000}, sort_keys=True, separators=(",", ":")),
        )
        self.coordinator.probe_services()
        row = self.store.get_service("svc-1")
        self.assertEqual(row["last_probe_changed"], 0)
        self.assertLessEqual(row["last_probe_bytes"], 256)

        rounds = [
            _json.loads(line)
            for line in self.store.probe_traffic_path.read_text(encoding="utf-8").splitlines()
        ]
        self.assertEqual(len(rounds), 4)
        self.assertTrue(all("total_response_bytes" in r for r in rounds))

    def test_cold_coordinator_backfills_full_observation(self) -> None:
        with self.store.connect() as conn:
            conn.execute(
                "UPDATE services SET actual_content_hash=NULL, actual_content=NULL"
            )
            conn.commit()
        self.coordinator.probe_services()
        row = self.store.get_service("svc-1")
        self.assertEqual(row["last_probe_changed"], 1)
        self.assertGreater(row["last_probe_bytes"], 2 * 1024)
        self.assertIsNotNone(row["actual_content"])
        self.assertEqual(row["actual_content_hash"], content_hash(self.big))

        self.coordinator.probe_services()
        row = self.store.get_service("svc-1")
        self.assertEqual(row["last_probe_changed"], 0)
        self.assertLessEqual(row["last_probe_bytes"], 256)

    def test_unreachable_service_records_zero_bytes(self) -> None:
        self.httpd.shutdown()
        self.coordinator.probe_services()
        row = self.store.get_service("svc-1")
        self.assertEqual(row["reachable"], 0)
        self.assertEqual(row["last_probe_bytes"], 0)


if __name__ == "__main__":
    unittest.main()
