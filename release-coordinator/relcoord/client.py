from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any


class ServiceUnreachable(Exception):
    pass


class ServiceRejected(Exception):
    def __init__(self, status: int, code: str, message: str, body: dict[str, Any]):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.body = body


class PeerIdentityMismatch(Exception):
    """The endpoint answered, but identifies as a different service."""

    def __init__(self, expected: str, peer: str | None, detail: str = ""):
        self.expected = expected
        self.peer = peer
        super().__init__(
            detail
            or f"peer identity mismatch: expected {expected!r}, endpoint is {peer!r}"
        )


def peer_name(body: dict[str, Any] | None) -> str | None:
    if not isinstance(body, dict):
        return None
    name = body.get("name")
    if name is None:
        name = body.get("service")
    return str(name) if name is not None else None


def expect_identity(body: dict[str, Any], expected_name: str) -> str:
    """Validate a peer's self-declared name; raise on mismatch.

    Returns the declared peer name (which equals ``expected_name``). Responses
    without any identity field are accepted for backwards compatibility with
    older peers; callers that require proof should check separately.
    """
    actual = peer_name(body)
    if actual is not None and actual != expected_name:
        raise PeerIdentityMismatch(expected_name, actual)
    return actual or expected_name


def _request(
    method: str,
    url: str,
    payload: dict[str, Any] | None = None,
    timeout: float = 1.0,
) -> dict[str, Any]:
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            return json.loads(raw.decode("utf-8")) if raw else {}
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        try:
            body = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            body = {"error": "bad_response", "message": raw}
        code = str(body.get("error", f"http_{e.code}"))
        message = str(body.get("message", raw or f"HTTP {e.code}"))
        raise ServiceRejected(e.code, code, message, body) from None
    except Exception as e:  # urllib raises several disconnected/timeout types
        raise ServiceUnreachable(f"{type(e).__name__}: {e}") from None


def fetch_state(base_url: str, timeout: float = 0.7) -> dict[str, Any]:
    return _request("GET", f"{base_url.rstrip('/')}/state", timeout=timeout)


def health(base_url: str, timeout: float = 0.7) -> dict[str, Any]:
    return _request("GET", f"{base_url.rstrip('/')}/health", timeout=timeout)


def apply_config(
    base_url: str,
    *,
    command_id: str,
    fence: int,
    release_id: str,
    mode: str,
    version: str,
    content: dict[str, Any],
    service_name: str | None = None,
    timeout: float = 1.1,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "command_id": command_id,
        "fence": fence,
        "release_id": release_id,
        "mode": mode,
        "version": version,
        "content": content,
    }
    # Intended peer identity: a crossed endpoint must reject the write before
    # this service's configuration is overwritten on the wrong machine.
    if service_name is not None:
        payload["service_name"] = service_name
    return _request(
        "POST",
        f"{base_url.rstrip('/')}/apply",
        payload,
        timeout=timeout,
    )


def set_faults(base_url: str, faults: dict[str, Any], timeout: float = 1.0) -> dict[str, Any]:
    return _request("POST", f"{base_url.rstrip('/')}/admin/faults", faults, timeout=timeout)
