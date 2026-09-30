"""不同调用范围的缓存严格隔离，跨范围读取必须被拒绝。"""

from __future__ import annotations

import asyncio

import pytest

from app.models import ScopeViolationError


async def test_same_key_in_different_scopes_is_independently_cached(
    harness_factory,
) -> None:
    h = harness_factory()
    h.upstream.set_value("k", "A-DATA", scope="tenant-a")
    h.upstream.set_value("k", "B-DATA", scope="tenant-b")
    # 两个范围都读取同一个上游 key，但缓存条目彼此独立
    ra = await h.service.get("tenant-a", "k")
    rb = await h.service.get("tenant-b", "k")
    assert ra["scope"] == "tenant-a" and rb["scope"] == "tenant-b"
    assert ra["data"] == "A-DATA" and rb["data"] == "B-DATA"

    # 失效 tenant-a 不影响 tenant-b 的命中
    await h.service.invalidate("tenant-a", "k")
    assert h.cache.peek("tenant-a", "k") is None
    kept = h.cache.peek("tenant-b", "k")
    assert kept is not None
    rb2 = await h.service.get("tenant-b", "k")
    assert rb2["state"] == "fresh" and rb2["data"] == "B-DATA"
    print(
        "[判定] input=('tenant-a','k'),('tenant-b','k') "
        f"upstream_calls={h.upstream.calls_for('k')} "
        "reason=缓存键含scope，失效只作用于本范围"
    )


async def test_cross_scope_read_is_denied(harness_factory) -> None:
    h = harness_factory()
    h.seed("k", "secret", scope="owner")
    await h.service.get("owner", "k")

    with pytest.raises(ScopeViolationError) as exc:
        await h.service.read_across_scope("attacker", "owner", "k")
    assert "attacker" in str(exc.value) and "owner" in str(exc.value)

    # 攻击者用自己的范围读同一个 key，拿到的是自己范围的空缓存 -> 回源报错，
    # 绝不会读到 owner 范围的 secret
    from app.models import CacheMissError

    with pytest.raises(CacheMissError):
        await h.service.get("attacker", "k")
    print(
        "[判定] input=('attacker')->('owner','k') upstream_calls="
        f"{h.upstream.calls_total} reason=跨范围显式拒绝(ScopeViolationError)，"
        "缓存按scope隔离不返回他人数据"
    )


async def test_concurrent_scope_mix_does_not_leak(harness_factory) -> None:
    """并发混合两个范围的相同 key 请求：各自合并、互不串值。"""
    h = harness_factory()
    h.upstream.set_value("same", "PA", scope="scope-a")
    h.upstream.set_value("same", "PB", scope="scope-b")
    ga = h.upstream.install_gate("same", scope="scope-a")
    gb = h.upstream.install_gate("same", scope="scope-b")
    tasks = []
    for i in range(10):
        scope = "scope-a" if i % 2 == 0 else "scope-b"
        tasks.append(asyncio.create_task(h.service.get(scope, "same")))
    await asyncio.sleep(0.05)
    # 两个范围各真实回源一次
    assert h.upstream.calls_for("same") == 2
    ga.set()
    gb.set()
    results = await asyncio.gather(*tasks)
    scopes = {r["scope"] for r in results}
    assert scopes == {"scope-a", "scope-b"}
    for r in results:
        assert r["scope"] in ("scope-a", "scope-b")
        assert r["data"] == ("PA" if r["scope"] == "scope-a" else "PB")
    print(
        "[判定] input=scope-a/scope-b 交替 x10 upstream_calls=2 "
        "reason=单飞按(scope,key,generation)隔离，跨范围不合并也不泄露"
    )
