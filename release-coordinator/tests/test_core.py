from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from relcoord.coordinator import Coordinator
from relcoord.service import MockService, ServiceAPIError
from relcoord.storage import Store
from relcoord.util import PROJECT_ROOT

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


if __name__ == "__main__":
    unittest.main()
