from __future__ import annotations

import argparse
import json
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .storage import Store, load_initial_state
from .util import DEFAULT_HOME, PROJECT_ROOT, canonical_json, now_ts, pretty_json


class ServiceAPIError(Exception):
    def __init__(self, status: int, code: str, message: str):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message


class LostReceipt(Exception):
    """Raised after durable apply when the injected fault eats the HTTP response."""


class MockService:
    def __init__(self, name: str, state_path: Path, initial: dict[str, Any]):
        self.name = name
        self.state_path = Path(state_path)
        self.lock = threading.RLock()
        if self.state_path.exists():
            with self.state_path.open("r", encoding="utf-8") as f:
                self.state = json.load(f)
        else:
            init_cfg = initial.get(
                name, {"version": "v0.0.0", "content": {"note": "missing initial state"}}
            )
            self.state = {
                "name": name,
                "effective": {
                    "version": init_cfg["version"],
                    "content": init_cfg.get("content", {}),
                },
                "fence": 0,
                "history": [
                    {
                        "at": now_ts(),
                        "command_id": "initial-state",
                        "mode": "initial",
                        "version": init_cfg["version"],
                        "content": init_cfg.get("content", {}),
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
            self._save_locked()

    def _save_locked(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.state_path.with_suffix(self.state_path.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(self.state, f, ensure_ascii=False, indent=2, sort_keys=True)
        tmp.replace(self.state_path)

    def public_state(self) -> dict[str, Any]:
        with self.lock:
            return json.loads(json.dumps(self.state))

    def update_faults(self, patch: dict[str, Any]) -> dict[str, Any]:
        with self.lock:
            faults = self.state.setdefault("faults", {})
            faults.setdefault("delay_seconds", 0.0)
            faults.setdefault("delay_remaining", 0)
            faults.setdefault("drop_remaining", 0)
            faults.setdefault("lost_receipt_remaining", 0)

            if patch.get("reset"):
                faults.update(
                    {
                        "delay_seconds": 0.0,
                        "delay_remaining": 0,
                        "drop_remaining": 0,
                        "lost_receipt_remaining": 0,
                    }
                )

            mapping = {
                "delay_seconds": "delay_seconds",
                "delay_remaining": "delay_remaining",
                "delay_next": "delay_remaining",
                "drop_remaining": "drop_remaining",
                "drop_apply": "drop_remaining",
                "lost_receipt_remaining": "lost_receipt_remaining",
                "lose_receipt": "lost_receipt_remaining",
            }
            for key, target in mapping.items():
                if key in patch:
                    value = patch[key]
                    if target == "delay_seconds":
                        faults[target] = max(0.0, float(value))
                    else:
                        if isinstance(value, bool):
                            value = 1 if value else 0
                        faults[target] = max(0, int(value))
            self._record_locked("faults_updated", None, {"patch": patch})
            self._save_locked()
            return dict(faults)

    def _record_locked(
        self, outcome: str, payload: dict[str, Any] | None, extra: dict[str, Any] | None = None
    ) -> None:
        item = {
            "at": now_ts(),
            "outcome": outcome,
            "command_id": (payload or {}).get("command_id"),
            "fence": (payload or {}).get("fence"),
            "version": (payload or {}).get("version"),
            "mode": (payload or {}).get("mode"),
        }
        if extra:
            item.update(extra)
        reqs = self.state.setdefault("recent_requests", [])
        reqs.append(item)
        del reqs[:-30]

    def _delay_if_configured_locked(self) -> None:
        faults = self.state["faults"]
        if faults.get("delay_remaining", 0) > 0 and faults.get("delay_seconds", 0) > 0:
            seconds = float(faults["delay_seconds"])
            faults["delay_remaining"] = int(faults["delay_remaining"]) - 1
            self._record_locked("delayed", None, {"sleep_seconds": seconds})
            self._save_locked()
            time.sleep(seconds)

    def apply(self, payload: dict[str, Any]) -> dict[str, Any]:
        required = ("command_id", "fence", "release_id", "mode", "version", "content")
        missing = [k for k in required if k not in payload]
        if missing:
            raise ServiceAPIError(400, "invalid_request", f"missing fields: {missing}")

        command_id = str(payload["command_id"])
        request_fence = int(payload["fence"])
        version = str(payload["version"])
        content = payload["content"]
        if not isinstance(content, dict):
            raise ServiceAPIError(400, "invalid_request", "content must be an object")

        with self.lock:
            self._delay_if_configured_locked()
            current_fence = int(self.state.get("fence", 0))

            # A confirmed rollback has a larger fence. A delayed old publish is
            # therefore refused before it can overwrite restored configuration.
            if request_fence < current_fence:
                self._record_locked(
                    "rejected_stale_fence",
                    payload,
                    {"current_fence": current_fence},
                )
                self._save_locked()
                raise ServiceAPIError(
                    409,
                    "stale_fence",
                    f"command fence {request_fence} is older than service fence {current_fence}",
                )

            seen = self.state.setdefault("seen_commands", {})
            if command_id in seen:
                self._record_locked("duplicate_command", payload)
                self._save_locked()
                return {
                    "ok": True,
                    "duplicate": True,
                    "command_id": command_id,
                    "effective": self.state["effective"],
                    "fence": self.state["fence"],
                }

            if request_fence != current_fence and request_fence < current_fence:
                # Defensive duplicate of the comparison above.
                raise ServiceAPIError(409, "stale_fence", "stale fence")

            faults = self.state.setdefault("faults", {})
            if int(faults.get("drop_remaining", 0)) > 0:
                faults["drop_remaining"] = int(faults["drop_remaining"]) - 1
                self._record_locked("injected_drop", payload)
                self._save_locked()
                raise ServiceAPIError(
                    503, "injected_drop", "fault injection dropped this apply instruction"
                )

            if request_fence < current_fence:
                # Could have changed only in theory; lock prevents local changes.
                raise ServiceAPIError(409, "stale_fence", "stale fence")

            effective = {"version": version, "content": content}
            self.state["effective"] = effective
            self.state["fence"] = request_fence
            seen[command_id] = {
                "at": now_ts(),
                "release_id": payload.get("release_id"),
                "mode": payload.get("mode"),
                "version": version,
                "content_hash": canonical_json(content),
            }
            history = self.state.setdefault("history", [])
            history.append(
                {
                    "at": now_ts(),
                    "command_id": command_id,
                    "release_id": payload.get("release_id"),
                    "mode": payload.get("mode"),
                    "version": version,
                    "content": content,
                }
            )
            del history[:-20]
            self._record_locked("applied", payload)

            lose_receipt = int(faults.get("lost_receipt_remaining", 0)) > 0
            if lose_receipt:
                faults["lost_receipt_remaining"] = int(faults["lost_receipt_remaining"]) - 1
                self._record_locked("receipt_lost_after_apply", payload)
            self._save_locked()

            if lose_receipt:
                raise LostReceipt()

            return {
                "ok": True,
                "duplicate": False,
                "command_id": command_id,
                "effective": effective,
                "fence": request_fence,
            }


def make_handler(service: MockService) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt: str, *args: Any) -> None:
            # The process stdout/stderr is redirected to a per-service log.
            print(f"[{service.name}] {self.address_string()} - {fmt % args}", flush=True)

        def _send_json(self, status: int, body: dict[str, Any]) -> None:
            raw = json.dumps(body, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def _read_json(self) -> dict[str, Any]:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length) if length else b"{}"
            try:
                data = json.loads(raw.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                raise ServiceAPIError(400, "invalid_json", str(e)) from None
            if not isinstance(data, dict):
                raise ServiceAPIError(400, "invalid_json", "body must be a JSON object")
            return data

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            try:
                if parsed.path == "/health":
                    self._send_json(200, {"ok": True, "name": service.name, "ts": now_ts()})
                elif parsed.path == "/state":
                    body = service.public_state()
                    body["ok"] = True
                    self._send_json(200, body)
                else:
                    self._send_json(404, {"ok": False, "error": "not_found"})
            except Exception as e:  # pragma: no cover - defensive server boundary
                self._send_json(500, {"ok": False, "error": "internal", "message": str(e)})

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            try:
                payload = self._read_json()
                if parsed.path == "/apply":
                    result = service.apply(payload)
                    self._send_json(200, result)
                elif parsed.path == "/admin/faults":
                    faults = service.update_faults(payload)
                    self._send_json(200, {"ok": True, "faults": faults})
                else:
                    self._send_json(404, {"ok": False, "error": "not_found"})
            except LostReceipt:
                # Durable state is already saved. Close without an HTTP response
                # to emulate a receipt lost by the local network.
                self.close_connection = True
                try:
                    self.connection.shutdown(socket.SHUT_WR)
                except OSError:
                    pass
            except ServiceAPIError as e:
                self._send_json(
                    e.status,
                    {"ok": False, "error": e.code, "message": e.message},
                )
            except Exception as e:  # pragma: no cover
                self._send_json(500, {"ok": False, "error": "internal", "message": str(e)})

    return Handler


def build_service(home: Path, name: str, state_path: str | Path | None = None) -> tuple[MockService, int, str]:
    store = Store(home)
    cluster = store.load_cluster()
    spec = next((s for s in cluster["services"] if s["name"] == name), None)
    if spec is None:
        raise SystemExit(f"unknown service {name!r}; cluster has {[s['name'] for s in cluster['services']]}")
    path = Path(state_path or spec["state_path"])
    initial = load_initial_state()
    service = MockService(name, path, initial["services"])
    return service, int(spec["port"]), cluster.get("host", "127.0.0.1")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run one persistent local mock service")
    parser.add_argument("--home", default=str(DEFAULT_HOME), help="coordinator runtime home")
    parser.add_argument("--name", required=True, help="service name from cluster.json")
    parser.add_argument("--port", type=int, default=None, help="override listen port")
    parser.add_argument("--state", default=None, help="override persistent state JSON path")
    parser.add_argument("--faults-json", default=None, help="initial fault injection JSON")
    args = parser.parse_args(argv)

    home = Path(args.home)
    service, cluster_port, host = build_service(home, args.name, args.state)
    port = args.port or cluster_port
    if args.faults_json:
        service.update_faults(json.loads(args.faults_json))

    class ReusableServer(ThreadingHTTPServer):
        allow_reuse_address = True
        daemon_threads = True

    server = ReusableServer((host, port), make_handler(service))
    print(
        f"service {args.name} listening on http://{host}:{port} "
        f"state={service.state_path} effective={service.state['effective']['version']}",
        flush=True,
    )
    try:
        server.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
