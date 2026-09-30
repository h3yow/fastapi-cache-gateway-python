"""FastAPI 路由装配。

把 CacheGatewayService 暴露为 HTTP 接口：
- GET  /data/{key}            读取（X-Scope 头指定调用范围，默认 default）
- PUT  /data/{key}            显式写入
- POST /data/{key}/invalidate 失效
- GET  /data/{owner_scope}/{key} 经 ?cross=1 显式演示跨范围读取（应被拒绝）
- POST /admin/upstream/value  在进程内替换上游数据
- POST /admin/upstream/fail   注入上游故障（timeout/error/invalid）
- GET  /health                健康检查与上游调用计数
"""

from __future__ import annotations

from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from pydantic import BaseModel, Field

from app.cache import LocalCache
from app.logging_utils import log_event
from app.models import (
    CacheMissError,
    RefreshPolicy,
    ScopeViolationError,
    UpstreamFailureError,
)
from app.service import CacheGatewayService
from app.upstream import (
    FAIL_ERROR,
    FAIL_INVALID,
    FAIL_TIMEOUT,
    LocalUpstream,
)


class PutBody(BaseModel):
    value: Any = Field(..., description="要写入缓存的值")


class UpstreamValueBody(BaseModel):
    key: str
    value: Any
    scope: str = "default"


class UpstreamFailBody(BaseModel):
    key: str
    mode: str = Field(..., description="timeout | error | invalid")
    scope: str = "default"


def create_app(
    service: CacheGatewayService | None = None,
    *,
    policy: RefreshPolicy = RefreshPolicy.SWR,
    ttl: float = 5.0,
    stale_ttl: float = 10.0,
) -> FastAPI:
    if service is None:
        upstream = LocalUpstream()
        cache = LocalCache(default_ttl=ttl, default_stale_ttl=stale_ttl)
        service = CacheGatewayService(
            cache,
            upstream,
            ttl=ttl,
            stale_ttl=stale_ttl,
            policy=policy,
        )

    app = FastAPI(
        title="fastapi-cache-gateway",
        version="0.1.0",
        summary="带本地缓存、并发合并与可控降级的缓存网关",
    )
    app.state.service = service

    def scope_dep(x_scope: str | None = Header(default=None)) -> str:
        return x_scope or "default"

    @app.get("/health")
    async def health() -> dict[str, Any]:
        return {"status": "ok", "upstream_calls": service.upstream_calls()}

    @app.get("/data/{key}")
    async def get_data(
        key: str,
        request: Request,
        scope: str = Depends(scope_dep),
        policy: str | None = Query(default=None, description="sync | swr"),
    ) -> dict[str, Any]:
        effective = RefreshPolicy(policy) if policy else None
        log_event(
            "http_get",
            scope=scope,
            key=key,
            input={"key": key, "scope": scope, "policy": policy},
            reason="http_request",
        )
        try:
            return await service.get(scope, key, policy=effective)
        except ScopeViolationError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except UpstreamFailureError as exc:
            raise HTTPException(
                status_code=502,
                detail={"kind": exc.kind.value, "message": str(exc)},
            ) from exc
        except CacheMissError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    @app.get("/cross/{owner_scope}/{key}")
    async def cross_read(
        owner_scope: str,
        key: str,
        scope: str = Depends(scope_dep),
    ) -> dict[str, Any]:
        """显式跨范围读取：必然 403，用来演示范围隔离。"""
        try:
            return await service.read_across_scope(scope, owner_scope, key)
        except ScopeViolationError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc

    @app.put("/data/{key}")
    async def put_data(
        key: str,
        body: PutBody,
        scope: str = Depends(scope_dep),
    ) -> dict[str, Any]:
        await service.put(scope, key, body.value)
        return {"ok": True, "scope": scope, "key": key}

    @app.post("/data/{key}/invalidate")
    async def invalidate_data(
        key: str, scope: str = Depends(scope_dep)
    ) -> dict[str, Any]:
        await service.invalidate(scope, key)
        return {"ok": True, "scope": scope, "key": key, "invalidated": True}

    # ---- 进程内上游控制 ------------------------------------------------ #
    def _upstream() -> LocalUpstream:
        upstream = getattr(service, "_upstream", None)
        if not isinstance(upstream, LocalUpstream):
            raise HTTPException(status_code=400, detail="upstream is not replaceable")
        return upstream

    @app.post("/admin/upstream/value")
    async def set_upstream_value(body: UpstreamValueBody) -> dict[str, Any]:
        _upstream().set_value(body.key, body.value, scope=body.scope)
        return {"ok": True, "key": body.key}

    @app.post("/admin/upstream/fail")
    async def inject_upstream_failure(body: UpstreamFailBody) -> dict[str, Any]:
        mode_map = {
            "timeout": FAIL_TIMEOUT,
            "error": FAIL_ERROR,
            "invalid": FAIL_INVALID,
        }
        if body.mode not in mode_map:
            raise HTTPException(status_code=400, detail="mode must be timeout|error|invalid")
        _upstream().inject_failure(
            body.key, mode_map[body.mode], scope=body.scope
        )
        return {"ok": True, "key": body.key, "mode": body.mode}

    @app.post("/admin/upstream/clear")
    async def clear_upstream_failure(body: UpstreamValueBody) -> dict[str, Any]:
        _upstream().clear_failure(body.key, scope=body.scope)
        return {"ok": True, "key": body.key}

    return app
