from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any
from urllib.parse import urlencode


class ServiceUnreachable(Exception):
    pass


class ServiceRejected(Exception):
    def __init__(self, status: int, code: str, message: str, body: dict[str, Any]):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = body
        self.body = body


def _request_raw(
    method: str,
    url: str,
    payload: dict[str, Any] | None = None,
    timeout: float = 1.0,
) -> tuple[dict[str, Any], int]:
    """Perform a JSON request and return (decoded body, raw response byte length)."""
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            body = json.loads(raw.decode("utf-8")) if raw else {}
            return body, len(raw)
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


def _request(
    method: str,
    url: str,
    payload: dict[str, Any] | None = None,
    timeout: float = 1.0,
) -> dict[str, Any]:
    return _request_raw(method, url, payload, timeout)[0]


def fetch_state(base_url: str, timeout: float = 0.7) -> dict[str, Any]:
    """Full live state query; used by `relctl status`, never by background probes."""
    return _request("GET", f"{base_url.rstrip('/')}/state", timeout=timeout)


def fetch_observation(
    base_url: str,
    *,
    version: str,
    content_hash: str,
    fence: int,
    timeout: float = 0.7,
) -> tuple[dict[str, Any], int]:
    """Digest-based background probe.

    Sends the coordinator's last-known version/content-digest/fence. The service
    replies with a tiny summary while nothing changed, and with the full
    effective content only when the observation is stale. Returns the decoded
    body together with the raw response body length in bytes.
    """
    query = urlencode(
        {"version": version or "", "content_hash": content_hash or "", "fence": int(fence)}
    )
    return _request_raw(
        "GET", f"{base_url.rstrip('/')}/observe?{query}", timeout=timeout
    )


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
    timeout: float = 1.1,
) -> dict[str, Any]:
    return _request(
        "POST",
        f"{base_url.rstrip('/')}/apply",
        {
            "command_id": command_id,
            "fence": fence,
            "release_id": release_id,
            "mode": mode,
            "version": version,
            "content": content,
        },
        timeout=timeout,
    )


def set_faults(base_url: str, faults: dict[str, Any], timeout: float = 1.0) -> dict[str, Any]:
    return _request("POST", f"{base_url.rstrip('/')}/admin/faults", faults, timeout=timeout)
