"""进程内本地缓存。

职责边界：
- 只管存储、新鲜度判定、单飞（single-flight）并发合并与“代数”失效；
- 不感知上游与错误归一化（由 service 层通过 loader 注入）。

关键不变量：
1. 同一 (scope, key, generation) 同一时刻最多只有一个在途回源（_Flight），
   其它并发请求等待同一个事件，拿到完全相同的结果（值或异常）；
2. 条目整体替换、从不原地修改，读路径不会读到半新半旧的值；
3. invalidate / put 推进 generation；回源提交时若 generation 已变，
   结果直接丢弃，旧值不会“复活”。
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from app.logging_utils import log_event
from app.models import CacheEntry, CacheResult, RefreshPolicy, ValueState

Loader = Callable[[], Awaitable[Any]]


@dataclass(slots=True)
class _Flight:
    """一次在途回源：同代并发请求共享它。"""

    generation: int
    done: asyncio.Event
    ok: bool = False
    value: Any = None
    error: BaseException | None = None


class LocalCache:
    def __init__(self, *, default_ttl: float = 5.0, default_stale_ttl: float = 10.0) -> None:
        self._entries: dict[tuple[str, str], CacheEntry] = {}
        # 在途回源按 (scope, key, generation) 索引，旧代数的回源不影响新代数
        self._inflight: dict[tuple[str, str, int], _Flight] = {}
        self._generations: dict[tuple[str, str], int] = {}
        self._lock = asyncio.Lock()
        self._default_ttl = default_ttl
        self._default_stale_ttl = default_stale_ttl
        self._bg_tasks: set[asyncio.Task[None]] = set()

    # ------------------------------------------------------------------ #
    # 公共读：缓存命中 / 过期 / 缺失，三种情况下的统一入口
    # ------------------------------------------------------------------ #
    async def get_or_load(
        self,
        scope: str,
        key: str,
        loader: Loader,
        *,
        ttl: float,
        stale_ttl: float,
        policy: RefreshPolicy,
    ) -> CacheResult:
        ck = (scope, key)
        now = time.monotonic()
        start_leader = False
        join_flight: _Flight | None = None
        flight_gen: int | None = None

        async with self._lock:
            current_gen = self._generations.get(ck, 0)
            raw_entry = self._entries.get(ck)
            # 代数不匹配的条目是“已失效”的：读路径不可见，
            # 但保留在缓存中，仅作为上游失败时的陈旧降级候选
            entry = raw_entry if raw_entry is not None and raw_entry.generation == current_gen else None

            if entry is None:
                # 缺失：必须回源
                start_leader, flight_gen = self._ensure_flight_locked(
                    ck, current_gen
                )
                join_flight = None if start_leader else self._inflight[
                    (ck[0], ck[1], flight_gen)
                ]
            else:
                age = now - entry.fetched_at
                if age < entry.ttl:
                    # 硬 TTL 内：所有人读同一条目
                    return self._result_from_entry(entry, ValueState.FRESH, age)

                if age < entry.ttl + entry.stale_ttl and policy is RefreshPolicy.SWR:
                    # 已硬过期但仍在“可接受陈旧上限”内，且策略允许：
                    # 先返回旧值，并确保有且仅有一个后台刷新
                    flight = self._inflight.get((scope, key, current_gen))
                    if flight is None:
                        self._ensure_flight_locked(ck, current_gen)
                        flight = self._inflight[(scope, key, current_gen)]
                        self._spawn_refresh(scope, key, current_gen, loader, entry)
                    else:
                        pass  # 复用已在进行的后台刷新，等待者本次同样拿旧值
                    state = ValueState.REFRESHING
                    log_event(
                        "cache_stale_served",
                        scope=scope,
                        key=key,
                        age=round(age, 6),
                        reason="swr_return_stale_then_refresh",
                    )
                    return self._result_from_entry(entry, state, age)

                # SYNC 过期，或 SWR 下陈旧也已超出上限：必须同步回源
                flight = self._inflight.get((scope, key, current_gen))
                if flight is None:
                    start_leader, flight_gen = self._ensure_flight_locked(
                        ck, current_gen
                    )
                else:
                    join_flight = flight
                    flight_gen = current_gen
                    log_event(
                        "flight_joined",
                        scope=scope,
                        key=key,
                        generation=flight_gen,
                        reason="concurrent_same_request_coalesced",
                    )

        if start_leader:
            assert flight_gen is not None
            return await self._run_leader(
                scope, key, flight_gen, loader, ttl, stale_ttl
            )
        if join_flight is not None:
            return await self._join_flight(
                scope, key, flight_gen, join_flight, ttl, stale_ttl
            )

        raise RuntimeError("unreachable cache state")  # pragma: no cover

    # ------------------------------------------------------------------ #
    # 公共写 / 失效
    # ------------------------------------------------------------------ #
    def put(
        self,
        scope: str,
        key: str,
        value: Any,
        *,
        ttl: float | None = None,
        stale_ttl: float | None = None,
    ) -> None:
        """外部写入：写入新值并推进代数，在途回源的提交将被丢弃。"""
        ck = (scope, key)
        gen = self._generations.get(ck, 0) + 1
        self._generations[ck] = gen
        self._entries[ck] = CacheEntry(
            value=value,
            fetched_at=time.monotonic(),
            ttl=self._default_ttl if ttl is None else ttl,
            stale_ttl=(
                self._default_stale_ttl if stale_ttl is None else stale_ttl
            ),
            scope=scope,
            key=key,
            generation=gen,
            source="external_put",
        )
        log_event(
            "cache_put", scope=scope, key=key, generation=gen, reason="external_write"
        )

    def invalidate(self, scope: str, key: str) -> bool:
        """失效单条数据：移除条目并推进代数。

        旧条目保留为“陈旧降级候选”：正常读取看不到它（代数不匹配），
        但上游失败且配置允许降级、旧值仍在可接受陈旧上限内时可返回 STALE。
        在途回源不会被取消，但它的提交会因代数不匹配被丢弃，
        因此旧值不可能在失效后被重新写回缓存。
        """
        ck = (scope, key)
        existed_entry = self._entries.get(ck)
        current_gen = self._generations.get(ck, 0)
        existed = existed_entry is not None and existed_entry.generation == current_gen
        gen = current_gen + 1
        self._generations[ck] = gen
        log_event(
            "cache_invalidate",
            scope=scope,
            key=key,
            generation=gen,
            existed=existed,
            reason="generation_advanced",
        )
        return existed

    def peek(self, scope: str, key: str) -> CacheEntry | None:
        """只读查看当前代数条目（仅限本范围的精确 (scope, key)）。

        已失效（旧代数）的条目对该视图不可见。
        """
        entry = self._entries.get((scope, key))
        current_gen = self._generations.get((scope, key), 0)
        if entry is not None and entry.generation == current_gen:
            return entry
        return None

    def stale_candidate(self, scope: str, key: str) -> CacheEntry | None:
        """最近一次保存的条目（可能已失效），仅用于失败降级。"""
        return self._entries.get((scope, key))

    def current_generation(self, scope: str, key: str) -> int:
        return self._generations.get((scope, key), 0)

    # ------------------------------------------------------------------ #
    # 单飞内部机制
    # ------------------------------------------------------------------ #
    def _ensure_flight_locked(
        self, ck: tuple[str, str], generation: int
    ) -> tuple[bool, int]:
        """在持锁状态下确保本代存在一个在途回源。

        返回 (是否由本次调用担任 leader, 代数)。
        """
        fk = (ck[0], ck[1], generation)
        if fk in self._inflight:
            return False, generation
        self._inflight[fk] = _Flight(generation=generation, done=asyncio.Event())
        log_event(
            "flight_started",
            scope=ck[0],
            key=ck[1],
            generation=generation,
            reason="single_real_upstream_call",
        )
        return True, generation

    def _spawn_refresh(
        self,
        scope: str,
        key: str,
        generation: int,
        loader: Loader,
        stale_entry: CacheEntry,
    ) -> None:
        task = asyncio.create_task(
            self._background_refresh(scope, key, generation, loader, stale_entry)
        )
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)

    async def _background_refresh(
        self,
        scope: str,
        key: str,
        generation: int,
        loader: Loader,
        stale_entry: CacheEntry,
    ) -> None:
        """SWR 后台刷新：失败只记录日志，保留旧值；成功后按代数守卫提交。"""
        fk = (scope, key, generation)
        try:
            value = await loader()
        except BaseException as exc:  # 后台任务绝不能把异常抛到事件循环
            async with self._lock:
                flight = self._inflight.pop(fk, None)
            if flight is not None:
                flight.ok = False
                flight.error = exc
                flight.done.set()
            log_event(
                "bg_refresh_failed",
                scope=scope,
                key=key,
                generation=generation,
                error=type(exc).__name__,
                reason="keep_serving_stale_no_failure_cached",
            )
            return

        async with self._lock:
            flight = self._inflight.pop(fk, None)
            current_gen = self._generations.get((scope, key), 0)
            if generation != current_gen:
                # 失效或写入已经发生：丢弃本次回源结果，防止旧值复活
                log_event(
                    "commit_superseded",
                    scope=scope,
                    key=key,
                    flight_generation=generation,
                    current_generation=current_gen,
                    reason="invalidation_or_newer_write_won",
                )
            else:
                self._entries[(scope, key)] = self._make_entry(
                    scope, key, value, generation, stale_entry.ttl,
                    stale_entry.stale_ttl,
                )
                log_event(
                    "bg_refresh_committed",
                    scope=scope,
                    key=key,
                    generation=generation,
                    reason="swr_background_write",
                )
        if flight is not None:
            flight.ok = True
            flight.value = value
            flight.done.set()

    async def _run_leader(
        self,
        scope: str,
        key: str,
        generation: int,
        loader: Loader,
        ttl: float,
        stale_ttl: float,
    ) -> CacheResult:
        """当前请求是本代回源的 leader：真正执行 loader。"""
        fk = (scope, key, generation)
        try:
            value = await loader()
        except BaseException as exc:
            # 失败不写缓存；放行给所有等待者，让它们拿到同一个失败
            async with self._lock:
                flight = self._inflight.pop(fk, None)
            if flight is not None:
                flight.ok = False
                flight.error = exc
                flight.done.set()
            raise

        committed = False
        async with self._lock:
            flight = self._inflight.pop(fk, None)
            current_gen = self._generations.get((scope, key), 0)
            if generation == current_gen:
                self._entries[(scope, key)] = self._make_entry(
                    scope, key, value, generation, ttl, stale_ttl
                )
                committed = True
            else:
                log_event(
                    "commit_superseded",
                    scope=scope,
                    key=key,
                    flight_generation=generation,
                    current_generation=current_gen,
                    reason="invalidation_or_newer_write_won",
                )
        if flight is not None:
            flight.ok = True
            flight.value = value
            flight.done.set()

        log_event(
            "flight_leader_done",
            scope=scope,
            key=key,
            generation=generation,
            committed=committed,
            reason="coalesced_result_shared",
        )
        now = time.monotonic()
        entry = self._entries.get((scope, key)) if committed else None
        if entry is not None:
            return self._result_from_entry(entry, ValueState.FRESH, 0.0)
        return CacheResult(
            value=value,
            state=ValueState.FRESH,
            scope=scope,
            key=key,
            generation=generation,
            fetched_at=now,
            age=0.0,
        )

    async def _join_flight(
        self,
        scope: str,
        key: str,
        generation: int | None,
        flight: _Flight,
        ttl: float,
        stale_ttl: float,
    ) -> CacheResult:
        """等待同代 leader 的结果：值一致、异常也一致。"""
        await flight.done.wait()
        if not flight.ok:
            assert flight.error is not None
            raise flight.error
        entry = self._entries.get((scope, key))
        if entry is not None and entry.generation == flight.generation:
            return self._result_from_entry(
                entry, ValueState.FRESH, time.monotonic() - entry.fetched_at
            )
        return CacheResult(
            value=flight.value,
            state=ValueState.FRESH,
            scope=scope,
            key=key,
            generation=flight.generation,
            fetched_at=time.monotonic(),
            age=0.0,
        )

    # ------------------------------------------------------------------ #
    # 小工具
    # ------------------------------------------------------------------ #
    @staticmethod
    def _make_entry(
        scope: str,
        key: str,
        value: Any,
        generation: int,
        ttl: float,
        stale_ttl: float,
    ) -> CacheEntry:
        return CacheEntry(
            value=value,
            fetched_at=time.monotonic(),
            ttl=ttl,
            stale_ttl=stale_ttl,
            scope=scope,
            key=key,
            generation=generation,
            source="upstream",
        )

    @staticmethod
    def _result_from_entry(
        entry: CacheEntry, state: ValueState, age: float
    ) -> CacheResult:
        return CacheResult(
            value=entry.value,
            state=state,
            scope=entry.scope,
            key=entry.key,
            generation=entry.generation,
            fetched_at=entry.fetched_at,
            age=age,
        )
