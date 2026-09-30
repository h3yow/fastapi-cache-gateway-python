"""Normalized upstream error types."""
from __future__ import annotations


class UpstreamError(Exception):
    """Base class for normalized upstream failures."""

    kind: str = "unknown"


class UpstreamTimeout(UpstreamError):
    kind = "timeout"


class UpstreamBadResponse(UpstreamError):
    kind = "bad_response"


class UpstreamInternalError(UpstreamError):
    kind = "internal"


class ScopeForbidden(Exception):
    """Raised when a caller reads a key outside its scope."""
