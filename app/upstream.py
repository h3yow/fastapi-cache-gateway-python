"""Replaceable in-process local data source simulating a real upstream."""
from __future__ import annotations

import asyncio
from typing import Any, Protocol

_MISSING = object()

from app.errors import UpstreamBadResponse, UpstreamInternalError, UpstreamTimeout


class Upstream(Protocol):
    """Anything able to fetch one key can act as an upstream."""

    async def fetch(self, key: str) -> Any: ...


def validate_payload(value: Any) -> dict[str, Any]:
    """Upstream contract: payload must be a dict with a 'value' field."""
    if not isinstance(value, dict) or "value" not in value:
        raise UpstreamBadResponse(f"payload missing 'value' field: {value!r}")
    return value


class LocalUpstream:
    """Deterministic, instrumented fake upstream for local runs and tests.

    - call_count records how many real fetches happened.
    - fail_next queues one-shot failures (timeout / error / bad payload).
    - latency simulates network cost so races are observable.
    """

    def __init__(self, latency: float = 0.05) -> None:
        self.latency = latency
        self.call_count = 0
        self._values: dict[str, Any] = {}
        self._failures: dict[str, list[BaseException]] = {}
        self._lock = asyncio.Lock()

    def set_value(self, key: str, value: Any) -> None:
        self._values[key] = {"value": value}

    def set_raw(self, key: str, payload: Any) -> None:
        """Plant a payload that violates the upstream contract."""
        self._values[key] = payload

    def fail_next(self, key: str, exc: BaseException) -> None:
        self._failures.setdefault(key, []).append(exc)

    async def fetch(self, key: str) -> dict[str, Any]:
        async with self._lock:
            self.call_count += 1
            queued = self._failures.get(key)
            exc = queued.pop(0) if queued else None
            # snapshot the response when the request is *received*, so a
            # later set_value does not change what an in-flight call returns
            payload = self._values.get(key, _MISSING)
        await asyncio.sleep(self.latency)
        if exc is not None:
            if isinstance(exc, asyncio.TimeoutError):
                raise UpstreamTimeout(f"timeout fetching {key}") from exc
            raise UpstreamInternalError(f"upstream error for {key}: {exc}") from exc
        if payload is _MISSING:
            raise UpstreamInternalError(f"no such key upstream: {key}")
        return validate_payload(payload)
