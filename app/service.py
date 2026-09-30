"""缓存网关服务层。

职责：
- 把本地数据源（或真实上游）的失败归一化为三类：超时 / 报错 / 非法内容；
- 回源失败后决定：允许且存在未超陈旧上限的旧值时降级返回 STALE，
  否则把归一化后的异常抛给调用方；失败结果绝不写入缓存（该规则在 cache 层）；
- 强制调用范围（scope）隔离：读只能读自己范围，越范围直接拒绝。
"""

from __future__ import annotations

from typing import Any

from app.cache import LocalCache
from app.logging_utils import log_event
from app.models import (
    CacheMissError,
    RefreshPolicy,
    ScopeViolationError,
    UpstreamErrorKind,
    UpstreamFailureError,
    ValueState,
)
from app.upstream import (
    LocalUpstream,
    UpstreamDataSource,
    UpstreamRemoteError,
    UpstreamTimeoutError,
)


class CacheGatewayService:
    def __init__(
        self,
        cache: LocalCache,
        upstream: UpstreamDataSource,
        *,
        ttl: float = 5.0,
        stale_ttl: float = 10.0,
        policy: RefreshPolicy = RefreshPolicy.SWR,
        allow_stale_on_error: bool = True,
    ) -> None:
        self._cache = cache
        self._upstream = upstream
        self.ttl = ttl
        self.stale_ttl = stale_ttl
        self.policy = policy
        self.allow_stale_on_error = allow_stale_on_error
        # 最近一次归一化失败记录（仅观测用，不是缓存数据）
        self.last_failure: dict[str, UpstreamErrorKind] = {}

    # ------------------------------------------------------------------ #
    # 读
    # ------------------------------------------------------------------ #
    async def get(
        self,
        scope: str,
        key: str,
        *,
        policy: RefreshPolicy | None = None,
    ) -> dict[str, Any]:
        """在本调用范围内读取数据。

        返回体带 state 字段：fresh / stale / refreshing，
        调用方可据此分辨拿到的是新数据还是降级旧数据。
        """
        effective_policy = self.policy if policy is None else policy
        log_event(
            "request_get",
            scope=scope,
            key=key,
            input=(scope, key),
            upstream_calls=self.upstream_calls(),
            reason="enter_get",
        )

        async def loader() -> Any:
            return await self._load_from_upstream(scope, key)

        try:
            result = await self._cache.get_or_load(
                scope,
                key,
                loader,
                ttl=self.ttl,
                stale_ttl=self.stale_ttl,
                policy=effective_policy,
            )
        except UpstreamFailureError as failure:
            return self._serve_stale_or_raise(scope, key, failure)

        return self._envelope(scope, key, result.value, result.state)

    async def read_across_scope(
        self, client_scope: str, owner_scope: str, key: str
    ) -> dict[str, Any]:
        """显式的跨范围读取：一律拒绝，绝不返回他人数据。"""
        log_event(
            "scope_cross_denied",
            client_scope=client_scope,
            owner_scope=owner_scope,
            key=key,
            reason="scope_isolation_violation",
        )
        raise ScopeViolationError(
            f"scope {client_scope!r} is not allowed to read key {key!r} "
            f"owned by scope {owner_scope!r}"
        )

    # ------------------------------------------------------------------ #
    # 写 / 失效
    # ------------------------------------------------------------------ #
    async def put(self, scope: str, key: str, value: Any) -> None:
        log_event(
            "request_put",
            scope=scope,
            key=key,
            input=(scope, key, value),
            reason="external_write_advances_generation",
        )
        self._cache.put(scope, key, self._wrap(value))

    async def invalidate(self, scope: str, key: str) -> None:
        log_event(
            "request_invalidate",
            scope=scope,
            key=key,
            input=(scope, key),
            reason="invalidate_requested",
        )
        self._cache.invalidate(scope, key)

    # ------------------------------------------------------------------ #
    # 观测
    # ------------------------------------------------------------------ #
    def upstream_calls(self, key: str | None = None) -> int:
        if isinstance(self._upstream, LocalUpstream):
            if key is None:
                return self._upstream.calls_total
            return self._upstream.calls_for(key)
        return 0

    # ------------------------------------------------------------------ #
    # 内部：回源、归一化、降级
    # ------------------------------------------------------------------ #
    async def _load_from_upstream(self, scope: str, key: str) -> dict[str, Any]:
        """真正回源一次；任何失败都被归一化为 UpstreamFailureError。"""
        try:
            raw = await self._upstream.fetch(key, scope=scope)
        except UpstreamTimeoutError as exc:
            # 归一化后再抛出，使所有被合并的等待者拿到同一类失败
            raise self._record_failure(
                key, UpstreamErrorKind.TIMEOUT, str(exc)
            ) from exc
        except UpstreamRemoteError as exc:
            raise self._record_failure(
                key, UpstreamErrorKind.ERROR, str(exc)
            ) from exc
        except UpstreamFailureError:
            raise
        except Exception as exc:  # 未知异常也按“上游报错”归一化
            raise self._record_failure(
                key, UpstreamErrorKind.ERROR, str(exc)
            ) from exc

        return self._validate(raw, key)

    def _validate(self, raw: Any, key: str) -> dict[str, Any]:
        """校验上游内容合法性；不合法则归一化为 INVALID（失败不入缓存）。"""
        if not isinstance(raw, dict) or "payload" not in raw:
            self._record_failure(
                key,
                UpstreamErrorKind.INVALID,
                f"missing 'payload' in upstream response: {raw!r}",
            )
        if not isinstance(raw, dict) or "payload" not in raw:
            raise UpstreamFailureError(
                UpstreamErrorKind.INVALID,
                f"upstream response missing 'payload': {raw!r}",
                key=key,
            )
        return raw

    def _record_failure(
        self, key: str, kind: UpstreamErrorKind, message: str
    ) -> UpstreamFailureError:
        self.last_failure[key] = kind
        log_event(
            "upstream_failure_classified",
            key=key,
            error_kind=kind.value,
            message=message,
            reason="normalized_timeout_error_invalid",
        )
        failure = UpstreamFailureError(kind, message, key=key)
        return failure

    def _serve_stale_or_raise(
        self,
        scope: str,
        key: str,
        failure: UpstreamFailureError,
    ) -> dict[str, Any]:
        """回源失败后的降级判定。"""
        # 降级候选可能是已过期甚至已失效（旧代数）的条目；
        # 只要仍在可接受陈旧上限内，就允许显式标注为 STALE 返回
        entry = self._cache.stale_candidate(scope, key)
        import time

        age = None if entry is None else time.monotonic() - entry.fetched_at

        if self.allow_stale_on_error and entry is not None and age is not None:
            within_stale_limit = age < entry.ttl + entry.stale_ttl
            if within_stale_limit:
                log_event(
                    "degrade_serve_stale",
                    scope=scope,
                    key=key,
                    error_kind=failure.kind.value,
                    age=round(age, 6),
                    stale_limit=entry.ttl + entry.stale_ttl,
                    reason="upstream_failed_but_stale_within_limit",
                )
                return self._envelope(
                    scope, key, entry.value, ValueState.STALE,
                    error_kind=failure.kind,
                )
            log_event(
                "stale_unavailable",
                scope=scope,
                key=key,
                age=round(age, 6),
                stale_limit=entry.ttl + entry.stale_ttl,
                reason="stale_exceeds_max_stale_ttl",
            )
        else:
            log_event(
                "stale_unavailable",
                scope=scope,
                key=key,
                has_entry=entry is not None,
                reason="no_stale_or_stale_disallowed",
            )
        raise CacheMissError(
            f"cache miss and upstream failed for {scope}:{key} "
            f"({failure.kind.value})"
        ) from failure

    @staticmethod
    def _wrap(value: Any) -> dict[str, Any]:
        return {"key": None, "version": 0, "payload": value}

    def _envelope(
        self,
        scope: str,
        key: str,
        value: dict[str, Any],
        state: ValueState,
        *,
        error_kind: UpstreamErrorKind | None = None,
    ) -> dict[str, Any]:
        return {
            "scope": scope,
            "key": key,
            "version": value.get("version"),
            "data": value.get("payload"),
            "state": state.value,
            "upstream_calls": self.upstream_calls(key),
            "degraded_from_error": error_kind.value if error_kind else None,
        }
