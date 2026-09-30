"""并发相同请求只能真正回源一次，且所有请求看到完全一致的值。

判定依据（见用例末尾的日志）：
- 闸门保证 N 个请求在回源途中并发挂起；
- upstream.calls_for(key) == 1 说明真实回源只有一次；
- 所有返回值/版本相同，说明复用了同一次结果。
"""

from __future__ import annotations

import asyncio

from app.logging_utils import get_logs
from app.models import RefreshPolicy

N_CONCURRENT = 25


async def test_concurrent_same_request_coalesces_to_single_upstream_call(
    harness_factory,
) -> None:
    h = harness_factory(policy=RefreshPolicy.SYNC)
    h.seed("user:1", {"name": "alice"}, scope="tenant-a")
    gate = h.upstream.install_gate("user:1", scope="tenant-a")

    async def call(i: int) -> dict:
        return await h.service.get("tenant-a", "user:1")

    tasks = [asyncio.create_task(call(i)) for i in range(N_CONCURRENT)]
    await asyncio.sleep(0.05)  # 确保所有请求都已挂到同一个在途回源上

    calls_at_flight = h.upstream.calls_for("user:1")
    assert calls_at_flight == 1, (
        f"回源途中上游调用次数应为 1，实际 {calls_at_flight}；日志={get_logs()}"
    )

    gate.set()
    results = await asyncio.gather(*tasks)

    assert h.upstream.calls_for("user:1") == 1
    assert all(r["state"] == "fresh" for r in results)
    assert all(r["data"] == {"name": "alice"} for r in results)
    versions = {r["version"] for r in results}
    assert versions == {1}
    print(
        "[判定] input=('tenant-a','user:1') x"
        f"{N_CONCURRENT} upstream_calls={h.upstream.calls_for('user:1')} "
        "reason=同一在途回源(_Flight)被并发复用，值与版本完全一致"
    )


async def test_waiters_share_the_same_failure(harness_factory) -> None:
    """并发失败也要一致：所有人拿到同一类归一化异常，且失败不入缓存。"""
    from app.models import CacheMissError

    h = harness_factory(policy=RefreshPolicy.SYNC, allow_stale_on_error=False)
    # 上游没有数据 -> 报错
    gate = h.upstream.install_gate("missing", scope="tenant-a")
    tasks = [
        asyncio.create_task(h.service.get("tenant-a", "missing"))
        for _ in range(8)
    ]
    await asyncio.sleep(0.05)
    assert h.upstream.calls_for("missing") == 1
    gate.set()

    outcomes = await asyncio.gather(*tasks, return_exceptions=True)
    assert all(isinstance(o, CacheMissError) for o in outcomes), outcomes
    # 失败后缓存里没有有效条目
    assert h.cache.peek("tenant-a", "missing") is None
    print(
        "[判定] input=('tenant-a','missing') x8 upstream_calls=1 "
        "reason=失败经单飞广播给全部等待者，且无失败结果写入缓存"
    )


async def test_cached_hit_does_not_call_upstream(harness_factory) -> None:
    h = harness_factory()
    h.seed("cfg", {"v": 7}, scope="s")
    first = await h.service.get("s", "cfg")
    assert first["state"] == "fresh" and h.upstream.calls_for("cfg") == 1

    results = await asyncio.gather(
        *[h.service.get("s", "cfg") for _ in range(20)]
    )
    assert h.upstream.calls_for("cfg") == 1
    assert all(r["data"] == {"v": 7} for r in results)
    print(
        "[判定] input=('s','cfg') 命中TTL x20 upstream_calls=1 "
        "reason=硬TTL内共享同一条目，不再回源"
    )


async def test_different_keys_do_not_coalesce(harness_factory) -> None:
    h = harness_factory()
    h.seed("a", 1, scope="s")
    h.seed("b", 2, scope="s")
    ra, rb = await asyncio.gather(
        h.service.get("s", "a"), h.service.get("s", "b")
    )
    assert ra["data"] == 1 and rb["data"] == 2
    assert h.upstream.calls_total == 2
    print(
        "[判定] input=('s','a'),('s','b') upstream_calls=2 "
        "reason=合并只发生在相同 (scope,key,generation)"
    )
