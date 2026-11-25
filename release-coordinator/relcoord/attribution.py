"""Compatibility namespace for peer-attribution verification.

The primary implementation lives in :mod:`relcoord.verify`; this module
re-exports the common entry points under the "attribution" vocabulary.
"""

from __future__ import annotations

from .verify import (
    PENDING_VERIFICATION,
    VERIFIED,
    render_verify_report,
    verify_attribution,
    verify_release_attribution,
    verify_runtime,
)

__all__ = [
    "PENDING_VERIFICATION",
    "VERIFIED",
    "verify_runtime",
    "verify_attribution",
    "verify_release_attribution",
    "render_verify_report",
]
