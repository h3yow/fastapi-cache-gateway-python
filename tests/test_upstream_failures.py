"""上游异常归一化、失败不入缓存、陈旧上限与可分辨的降级标记。"""

from __future__ import annotations

import asyncio

import pytest

from app.models import (
    CacheMissError,
    RefreshPolicy,
    UpstreamErrorKind,
    UpstreamFailureError,
)
from app.upstream import FAIL_ERROR, FAIL_INVALID, FAIL_TIMEOUT


@pytest.mark.parametrize(
    "mode,kind",
    [
        (FAIL_TIMEOUT, UpstreamErrorKind.TIMEOUT),
        (FAIL_ERROR, UpstreamErrorKind.ERROR),
        (FAIL_INVALID, UpstreamErrorKind.INVALID),
    ],
)
async def test_failures_are_classified_and_not_cached(
    harness_factory, mode, kind
) -> None:
    h = harness_factory(allow_stale_on_error=False)
    h.upstream.inject_failure("x", mode, scope="s")
    with pytest.raises(CacheMissError) as exc:
        await h.service.get("s", "x")
    assert exc.value.__cause__ is not None
    cause = exc.value.__cause__
    assert isinstance(cause, UpstreamFailureError)
    assert cause.kind is kind
    assert h.service.last_failure["x"] is kind
    # 失败绝不能作为有效数据留在缓存
    assert h.cache.peek("s", "x") is None
    assert h.cache.stale_candidate("s", "x") is None
    print(
        f"[判定] input=('s','x') mode={mode} upstream_calls="
        f"{h.upstream.calls_for('x')} reason=归一化为{kind.value}且未写缓存"
    )


async def test_concurrent_failure_hits_upstream_once_and_all_get_same_kind(
    harness_factory,
) -> None:
    h = harness_factory(allow_stale_on_error=False)
    h.upstream.inject_failure("boom", FAIL_TIMEOUT, scope="s")
    gate = h.upstream.install_gate("boom", scope="s")
    tasks = [
        asyncio.create_task(h.service.get("s", "boom")) for _ in range(12)
    ]
    await asyncio.sleep(0.05)
    gate.set()
    outcomes = await asyncio.gather(*tasks, return_exceptions=True)
    assert h.upstream.calls_for("boom") == 1
    for o in outcomes:
        assert isinstance(o, CacheMissError)
        assert isinstance(o.__cause__, UpstreamFailureError)
        assert o.__cause__.kind is UpstreamErrorKind.TIMEOUT
    print(
        "[判定] input=('s','boom') x12 timeout upstream_calls=1 "
        "reason=单飞下一次失败被广播为同一类归一化错误"
    )


async def test_stale_served_is_explicitly_marked_and_recovers(harness_factory):
    h = harness_factory(ttl=0.02, stale_ttl=5.0, policy=RefreshPolicy.SYNC)
    h.seed("k", "GOOD", scope="s")
    await h.service.get("s", "k")

    h.upstream.inject_failure("k", FAIL_INVALID, scope="s")
    await asyncio.sleep(0.03)  # 硬过期
    r = await h.service.get("s", "k")
    assert r["state"] == "stale"
    assert r["degraded_from_error"] == "invalid"
    assert r["data"] == "GOOD"

    h.upstream.clear_failure("k", scope="s")
    h.upstream.set_value("k", "BETTER", scope="s")
    r2 = await h.service.get("s", "k")
    assert r2["state"] == "fresh" and r2["data"] == "BETTER"
    print(
        "[判定] input=('s','k') 过期后invalid降级 upstream_calls="
        f"{h.upstream.calls_for('k')} reason=state=stale可分辨；恢复后fresh"
    )


async def test_stale_beyond_max_limit_is_rejected(harness_factory):
    """超出可接受陈旧上限：连旧值也不能返回。"""
    h = harness_factory(
        ttl=0.01, stale_ttl=0.02, policy=RefreshPolicy.SYNC
    )
    h.seed("k", "ANCIENT", scope="s")
    await h.service.get("s", "k")
    h.upstream.inject_failure("k", FAIL_TIMEOUT, scope="s")
    await asyncio.sleep(0.05)  # 超过 ttl + stale_ttl
    with pytest.raises(CacheMissError):
        await h.service.get("s", "k")
    print(
        "[判定] input=('s','k') age>ttl+stale_ttl timeout "
        "reason=陈旧数据超出上限，拒绝降级"
    )
