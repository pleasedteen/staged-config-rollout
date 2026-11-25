from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_HOME = Path(os.environ.get("RELCOORD_HOME", PROJECT_ROOT / "runtime"))
GROUP = "core"


def now_ts() -> float:
    return time.time()


def canonical_json(value: Any) -> str:
    """Stable JSON used for comparisons and SQLite persistence."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def content_hash(content: Any) -> str:
    return hashlib.sha256(canonical_json(content).encode("utf-8")).hexdigest()[:16]


def pretty_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)


def compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def parse_json_maybe(raw: str | None) -> Any:
    if raw is None:
        return None
    return json.loads(raw)


class RelCoordError(Exception):
    """Expected command/state error; CLI exits with code 2."""


class ConflictError(RelCoordError):
    """A release already owns the group."""


class NotFoundError(RelCoordError):
    pass
