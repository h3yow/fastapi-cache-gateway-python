"""Cache gateway: TTL cache + singleflight coalescing + scoped isolation."""
from __future__ import annotations

import asyncio
import time
import logging
from dataclasses import dataclass
from typing import Any

from app.errors import ScopeForbidden, UpstreamError
from app.upstream import Upstream

log = logging.getLogger("gateway")


@dataclass(frozen=True)
class CachePolicy:
    ttl: float = 1.0                       # fresh window in seconds
    max_stale: float = 5.0                 # extra seconds a stale entry may be served
    stale_while_revalidate: bool = True    # serve stale + background refresh


@dataclass
class FetchResult:
    value: Any
    fresh: bool          # True if within TTL (new data)
    stale: bool          # True if served past TTL (degraded)
    from_cache: bool
    scope: str
    key: str


@dataclass
class _Entry:
    value: Any
    stored_at: float
    generation: int


class CacheGateway:
    """Process-local cache in front of a replaceable upstream.

    - Coalescing: concurrent gets for the same (scope, key) share one fetch.
    - Generations: invalidate/put bump a per-key generation; an in-flight
      fetch started before the bump is never written back (no resurrection).
    - Isolation: a key is owned by the first scope that claims it; other
      scopes reading it get ScopeForbidden instead of foreign data.
    """

    def __init__(self, upstream: Upstream, policy: CachePolicy | None = None) -> None:
        self._upstream = upstream
        self._policy = policy or CachePolicy()
        self._entries: dict[tuple[str, str], _Entry] = {}
        self._inflight: dict[tuple[str, str], asyncio.Task] = {}
        self._generations: dict[tuple[str, str], int] = {}
        self._owners: dict[str, str] = {}
        self._mu = asyncio.Lock()

    # ---- scope isolation -------------------------------------------------

    def register(self, scope: str, key: str) -> None:
        """Pre-claim a key for a scope."""
        owner = self._owners.get(key)
        if owner is not None and owner != scope:
            raise ScopeForbidden(f"key {key!r} already owned by scope {owner!r}")
        self._owners[key] = scope

    def _check_scope(self, scope: str, key: str) -> None:
        owner = self._owners.get(key)
        if owner is not None and owner != scope:
            raise ScopeForbidden(
                f"scope {scope!r} may not read key {key!r} owned by {owner!r}"
            )

    # ---- public API ------------------------------------------------------

    async def get(self, scope: str, key: str) -> FetchResult:
        self._check_scope(scope, key)
        ck = (scope, key)
        now = time.monotonic()
        entry = self._entries.get(ck)
        if entry is not None:
            age = now - entry.stored_at
            if age < self._policy.ttl:
                return FetchResult(entry.value, fresh=True, stale=False,
                                   from_cache=True, scope=scope, key=key)
            if age < self._policy.ttl + self._policy.max_stale:
                if self._policy.stale_while_revalidate:
                    self._ensure_background_refresh(ck)
                    return FetchResult(entry.value, fresh=False, stale=True,
                                       from_cache=True, scope=scope, key=key)
                # sync refresh path: fall through to coalesced fetch, but
                # keep the stale entry as fallback if upstream fails.
            else:
                self._entries.pop(ck, None)  # beyond max_stale: dead
                entry = None
        try:
            value = await self._coalesced_fetch(ck)
        except UpstreamError:
            if entry is not None:  # degrade to stale within max_stale
                log.warning("upstream failed for %s; serving stale", ck)
                return FetchResult(entry.value, fresh=False, stale=True,
                                   from_cache=True, scope=scope, key=key)
            raise
        return FetchResult(value, fresh=True, stale=False,
                           from_cache=False, scope=scope, key=key)

    async def put(self, scope: str, key: str, value: Any) -> None:
        self._check_scope(scope, key)
        ck = (scope, key)
        async with self._mu:
            self._generations[ck] = self._generations.get(ck, 0) + 1
            self._owners[key] = scope
            self._entries[ck] = _Entry(value, time.monotonic(),
                                       self._generations[ck])

    async def invalidate(self, scope: str, key: str) -> None:
        self._check_scope(scope, key)
        ck = (scope, key)
        async with self._mu:
            self._generations[ck] = self._generations.get(ck, 0) + 1
            self._entries.pop(ck, None)

    # ---- internals -------------------------------------------------------

    def _ensure_background_refresh(self, ck: tuple[str, str]) -> None:
        if ck not in self._inflight:
            task = asyncio.ensure_future(self._fetch_and_store(ck))
            self._inflight[ck] = task
            task.add_done_callback(lambda _t, k=ck: self._inflight.pop(k, None))

    async def _coalesced_fetch(self, ck: tuple[str, str]) -> Any:
        async with self._mu:
            task = self._inflight.get(ck)
            if task is None:
                task = asyncio.ensure_future(self._fetch_and_store(ck))
                self._inflight[ck] = task
                task.add_done_callback(lambda _t, k=ck: self._inflight.pop(k, None))
        return await asyncio.shield(task)

    async def _fetch_and_store(self, ck: tuple[str, str]) -> Any:
        scope, key = ck
        generation = self._generations.get(ck, 0)
        payload = await self._upstream.fetch(key)  # errors propagate, never cached
        value = payload["value"]
        async with self._mu:
            if self._generations.get(ck, 0) == generation:
                self._owners[key] = scope
                self._entries[ck] = _Entry(value, time.monotonic(), generation)
            else:
                log.info("discarding fetch for %s: invalidated in flight", ck)
        return value
