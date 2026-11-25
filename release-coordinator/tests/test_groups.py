from __future__ import annotations

import sqlite3
import tempfile
import threading
import time
import unittest
from pathlib import Path

from relcoord import coordinator as coordinator_mod
from relcoord.coordinator import MAX_SERVICE_REQUESTS, Coordinator, RequestScheduler
from relcoord.storage import Store
from relcoord.util import PROJECT_ROOT

V2 = PROJECT_ROOT / "configs" / "target-v2.json"
V3 = PROJECT_ROOT / "configs" / "target-v3.json"


class MultiGroupStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.home = Path(self._tmp.name)
        self.store = Store(self.home)
        self.store.initialize_home(
            group="blue", names=["web", "api"], ports=[20001, 20002]
        )
        self.store.initialize_home(
            group="green", names=["web", "api"], ports=[20011, 20012]
        )

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_duplicate_service_names_are_separate_identities(self) -> None:
        rows = self.store.list_services()
        identities = {(r["group_name"], r["name"]) for r in rows}
        self.assertIn(("blue", "web"), identities)
        self.assertIn(("green", "web"), identities)
        with self.assertRaises(Exception):
            self.store.get_service("web")
        self.assertEqual(self.store.get_service("web", group="blue")["group_name"], "blue")

    def test_release_decisions_are_group_scoped(self) -> None:
        blue = self.store.submit_release(
            config_path=V2, group="blue", batches=[["web"], ["api"]]
        )
        green = self.store.submit_release(
            config_path=V3, group="green", batches=[["web"], ["api"]]
        )
        self.assertEqual(len(self.store.active_releases()), 2)

        rid, changed = self.store.pause("blue", "unit")
        self.assertEqual(rid, blue)
        self.assertTrue(changed)
        self.assertTrue(self.store.get_release(blue)["pause_requested"])
        self.assertFalse(self.store.get_release(green)["pause_requested"])

        # Green can still accept its own rollback decision independently.
        rb_id, changed = self.store.request_rollback("green", "unit")
        self.assertEqual(rb_id, green)
        self.assertTrue(changed)
        self.assertEqual(self.store.get_release(green)["status"], "rolling_back")
        self.assertEqual(self.store.get_release(blue)["status"], "running")

        with self.assertRaises(Exception):
            self.store.pause("missing-group")


class LegacyMigrationTests(unittest.TestCase):
    def test_name_primary_key_rebuilds_in_place(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            home = Path(td)
            home.mkdir(exist_ok=True)
            conn = sqlite3.connect(home / "coordinator.sqlite3")
            conn.executescript(
                """
                CREATE TABLE services (
                    name TEXT PRIMARY KEY,
                    group_name TEXT NOT NULL DEFAULT 'core',
                    base_url TEXT NOT NULL,
                    state_path TEXT NOT NULL,
                    initial_version TEXT NOT NULL,
                    initial_content TEXT NOT NULL,
                    actual_version TEXT,
                    actual_content TEXT,
                    actual_fence INTEGER NOT NULL DEFAULT 0,
                    reachable INTEGER NOT NULL DEFAULT 0,
                    last_checked REAL,
                    last_error TEXT
                );
                INSERT INTO services(name, group_name, base_url, state_path,
                                     initial_version, initial_content)
                VALUES('web', 'core', 'http://127.0.0.1:1', '/tmp/web.json',
                       'v1', '{}');
                """
            )
            conn.commit()
            conn.close()

            store = Store(home)
            store.init_db()
            rows = store.connect().execute("PRAGMA table_info(services)").fetchall()
            pk = [r["name"] for r in sorted(rows, key=lambda r: r["pk"]) if r["pk"]]
            self.assertEqual(pk, ["group_name", "name"])
            row = store.get_service("web", group="core")
            self.assertEqual(row["initial_version"], "v1")


class SchedulerTests(unittest.TestCase):
    def test_hard_eight_request_cap(self) -> None:
        active = 0
        peak = 0
        lock = threading.Lock()
        entered = threading.Event()
        release = threading.Event()

        def job() -> None:
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
                if active == MAX_SERVICE_REQUESTS:
                    entered.set()
            release.wait(timeout=2)
            with lock:
                active -= 1

        scheduler = RequestScheduler(8)
        try:
            futures = [scheduler.submit(f"g{i % 2}", job) for i in range(20)]
            self.assertTrue(entered.wait(1))
            time.sleep(0.03)
            self.assertLessEqual(peak, 8)
            release.set()
            for future in futures:
                future.result(timeout=2)
        finally:
            scheduler.shutdown()
        self.assertEqual(peak, 8)


    def test_fair_share_keeps_a_slot_for_healthy_group(self) -> None:
        scheduler = RequestScheduler(8, fair_owners=2)
        slow_entered = threading.Event()
        release_slow = threading.Event()
        fast_done = threading.Event()

        def slow_job() -> None:
            slow_entered.set()
            release_slow.wait(timeout=3)

        def fast_job() -> str:
            fast_done.set()
            return "fast"

        try:
            slow_futures = [scheduler.submit("slow", slow_job) for _ in range(8)]
            self.assertTrue(slow_entered.wait(1))
            deadline = time.monotonic() + 1
            while time.monotonic() < deadline:
                with scheduler._lock:
                    slow_active = scheduler._active_by_owner.get("slow", 0)
                if slow_active == 4:
                    break
                time.sleep(0.01)
            self.assertEqual(slow_active, 4)

            fast_future = scheduler.submit("healthy", fast_job)
            self.assertEqual(fast_future.result(timeout=1), "fast")
            self.assertTrue(fast_done.is_set())
            release_slow.set()
            for future in slow_futures:
                future.result(timeout=2)
        finally:
            release_slow.set()
            scheduler.shutdown()


class GroupHeadOfLineTests(unittest.TestCase):
    def test_stuck_group_does_not_block_healthy_group_first_batch(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            home = Path(td)
            store = Store(home)
            store.initialize_home(group="slow", names=["web"], ports=[21001])
            store.initialize_home(group="fast", names=["web"], ports=[21011])
            slow_rid = store.submit_release(
                config_path=V2, group="slow", batches=[["web"]]
            )
            fast_rid = store.submit_release(
                config_path=V3, group="fast", batches=[["web"]]
            )

            coordinator = Coordinator(home, interval=0.01)
            coordinator.probe_services = lambda: None  # type: ignore[method-assign]
            slow_started = threading.Event()
            release_slow = threading.Event()
            original = coordinator_mod.client.apply_config

            def fake_apply(base_url: str, **kwargs):
                if "21001" in base_url:
                    slow_started.set()
                    release_slow.wait(timeout=3)
                return {
                    "effective": {
                        "version": kwargs["version"],
                        "content": kwargs["content"],
                    },
                    "fence": kwargs["fence"],
                }

            coordinator_mod.client.apply_config = fake_apply
            try:
                coordinator.tick()
                self.assertTrue(slow_started.wait(1))
                deadline = time.monotonic() + 2
                while time.monotonic() < deadline:
                    row = next(
                        r
                        for r in store.release_services(fast_rid)
                        if r["service_name"] == "web"
                    )
                    if row["applied_confirmed"]:
                        break
                    time.sleep(0.01)
                fast_row = next(
                    r for r in store.release_services(fast_rid) if r["service_name"] == "web"
                )
                slow_row = next(
                    r for r in store.release_services(slow_rid) if r["service_name"] == "web"
                )
                self.assertTrue(fast_row["applied_confirmed"])
                self.assertFalse(slow_row["applied_confirmed"])
                release_slow.set()
            finally:
                coordinator_mod.client.apply_config = original
                release_slow.set()
                deadline = time.monotonic() + 2
                while time.monotonic() < deadline:
                    if all(
                        r["applied_confirmed"]
                        for r in store.release_services(slow_rid)
                    ):
                        break
                    time.sleep(0.01)
                coordinator.close()
                for thread in list(coordinator._release_threads.values()):
                    thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
