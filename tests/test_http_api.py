"""HTTP 层：X-Scope 隔离、跨范围 403、失效与降级返回体。"""

from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from app.api import create_app
from app.cache import LocalCache
from app.models import RefreshPolicy
from app.service import CacheGatewayService
from app.upstream import FAIL_TIMEOUT, LocalUpstream


@pytest.fixture
async def client():
    upstream = LocalUpstream()
    upstream.set_value("p:1", {"title": "hello"}, scope="tenant-a")
    cache = LocalCache()
    service = CacheGatewayService(
        cache, upstream, ttl=5, stale_ttl=10,
        policy=RefreshPolicy.SYNC, allow_stale_on_error=True,
    )
    app = create_app(service)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://t") as ac:
        yield ac, service, upstream


async def test_scoped_get_and_cache_hit(client):
    ac, service, upstream = client
    r1 = await ac.get("/data/p:1", headers={"X-Scope": "tenant-a"})
    assert r1.status_code == 200
    body = r1.json()
    assert body["state"] == "fresh" and body["data"] == {"title": "hello"}
    r2 = await ac.get("/data/p:1", headers={"X-Scope": "tenant-a"})
    assert r2.json()["state"] == "fresh"
    assert upstream.calls_for("p:1") == 1
    print(
        "[判定] input=GET /data/p:1 X-Scope=tenant-a x2 "
        "upstream_calls=1 reason=同范围第二次命中缓存"
    )


async def test_cross_scope_http_403(client):
    ac, service, upstream = client
    await ac.get("/data/p:1", headers={"X-Scope": "tenant-a"})
    r = await ac.get("/cross/tenant-a/p:1", headers={"X-Scope": "tenant-b"})
    assert r.status_code == 403
    assert "tenant-b" in r.json()["detail"] and "tenant-a" in r.json()["detail"]
    # tenant-b 自己范围没有该数据，上游也是隔离的
    r2 = await ac.get("/data/p:1", headers={"X-Scope": "tenant-b"})
    assert r2.status_code == 503
    print(
        "[判定] input=GET /cross/tenant-a/p:1 X-Scope=tenant-b "
        "reason=跨范围读取403拒绝；本范围读取503而不返回他人数据"
    )


async def test_invalidate_then_refetch(client):
    ac, service, upstream = client
    await ac.get("/data/p:1", headers={"X-Scope": "tenant-a"})
    r = await ac.post("/data/p:1/invalidate", headers={"X-Scope": "tenant-a"})
    assert r.status_code == 200
    upstream.set_value("p:1", {"title": "v2"}, scope="tenant-a")
    r2 = await ac.get("/data/p:1", headers={"X-Scope": "tenant-a"})
    assert r2.json()["data"] == {"title": "v2"}
    print(
        "[判定] input=POST invalidate 后 GET upstream_calls="
        f"{upstream.calls_for('p:1')} reason=失效后强制回源得到新值"
    )


async def test_failure_returns_503_without_stale(client):
    ac, service, upstream = client
    upstream.inject_failure("ghost", FAIL_TIMEOUT, scope="default")
    r = await ac.get("/data/ghost")
    assert r.status_code == 503
    print("[判定] input=GET /data/ghost(无旧值,timeout) reason=无陈旧候选->503")
