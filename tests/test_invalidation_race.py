"""失效 / 写入与在途回源交错时，旧值不得“复活”。

核心机制：generation（代数）。失效与写入推进代数，
在途回源提交时发现代数已变则丢弃结果。
"""

from __future__ import annotations

import asyncio

from app.models import RefreshPolicy


async def test_invalidate_during_inflight_fetch_drops_stale_writeback(
    harness_factory,
) -> None:
    """回源还没回来时失效：该回源的旧值不能在失效后写回缓存。"""
    h = harness_factory(policy=RefreshPolicy.SYNC)
    h.seed("k", "OLD", scope="s")
    gate = h.upstream.install_gate("k", scope="s")

    in_flight = asyncio.create_task(h.service.get("s", "k"))
    await asyncio.sleep(0.05)
    assert h.upstream.calls_for("k") == 1

    # 回源挂在途中时执行失效
    await h.service.invalidate("s", "k")
    assert h.cache.peek("s", "k") is None  # 读视图立即看不到旧值

    gate.set()  # 放行那次“旧世界”的回源
    await in_flight

    # 旧回源提交必须被丢弃：缓存中不存在当前代数的有效条目
    assert h.cache.peek("s", "k") is None

    # 下一次读取必须发起新的回源，而不是读到复活的 OLD
    h.upstream.set_value("k", "NEW", scope="s")
    r = await h.service.get("s", "k")
    assert r["data"] == "NEW" and r["state"] == "fresh"
    assert h.upstream.calls_for("k") == 2
    print(
        "[判定] input=('s','k') 回源途中invalidate upstream_calls=2 "
        "reason=旧代数回源提交被generation守卫丢弃，旧值未复活"
    )


async def test_external_put_during_inflight_fetch_supersedes_it(
    harness_factory,
) -> None:
    """回源途中外部写入：以写入为准，回源结果不得覆盖新写入。"""
    h = harness_factory(policy=RefreshPolicy.SYNC)
    h.seed("k", "FROM_UPSTREAM", scope="s")
    gate = h.upstream.install_gate("k", scope="s")

    in_flight = asyncio.create_task(h.service.get("s", "k"))
    await asyncio.sleep(0.05)
    await h.service.put("s", "k", "WRITTEN")  # 写入推进代数
    gate.set()
    await in_flight

    entry = h.cache.peek("s", "k")
    assert entry is not None and entry.value["payload"] == "WRITTEN"
    r = await h.service.get("s", "k")
    assert r["data"] == "WRITTEN"
    assert h.upstream.calls_for("k") == 1  # 没有因为被覆盖而额外回源
    print(
        "[判定] input=('s','k') 回源途中put upstream_calls=1 "
        "reason=put推进generation，回源提交时发现已被取代而丢弃"
    )


async def test_invalidation_then_recovery_does_not_resurrect_old(harness_factory):
    """失效后回源失败可降级旧值；恢复后的下一次读取必须拿到新值。"""
    h = harness_factory(policy=RefreshPolicy.SYNC)
    h.seed("k", "V1", scope="s")
    await h.service.get("s", "k")

    from app.upstream import FAIL_TIMEOUT

    h.upstream.inject_failure("k", FAIL_TIMEOUT, scope="s")
    await h.service.invalidate("s", "k")
    stale = await h.service.get("s", "k")
    assert stale["state"] == "stale"
    assert stale["degraded_from_error"] == "timeout"
    assert stale["data"] == "V1"  # 显式标注的降级旧值，不是“复活”的新数据

    h.upstream.clear_failure("k", scope="s")
    h.upstream.set_value("k", "V2", scope="s")
    fresh = await h.service.get("s", "k")
    assert fresh["state"] == "fresh" and fresh["data"] == "V2"
    print(
        "[判定] input=('s','k') 失效后timeout降级->恢复 upstream_calls=3 "
        "reason=降级值带state=stale标记；恢复后新代数回源得到V2"
    )


async def test_swr_serves_stale_immediately_and_refreshes_once(harness_factory):
    """SWR：硬过期后先返回旧值(refreshing)，后台仅刷新一次。"""
    h = harness_factory(
        ttl=0.02, stale_ttl=5.0, policy=RefreshPolicy.SWR
    )
    h.seed("k", "CACHED", scope="s")
    first = await h.service.get("s", "k")
    assert first["state"] == "fresh"
    calls_after_first = h.upstream.calls_for("k")

    gate = h.upstream.install_gate("k", scope="s")
    await asyncio.sleep(0.03)  # 越过硬TTL

    # 并发 15 个过期读：应立即拿到旧值，后台只有一次刷新
    results = await asyncio.gather(
        *[h.service.get("s", "k") for _ in range(15)]
    )
    assert all(r["state"] == "refreshing" for r in results), [
        r["state"] for r in results
    ]
    assert all(r["data"] == "CACHED" for r in results)
    assert h.upstream.calls_for("k") == calls_after_first + 1

    h.upstream.set_value("k", "REFRESHED", scope="s")
    gate.set()
    # 等后台刷新提交
    for _ in range(50):
        await asyncio.sleep(0.01)
        cur = h.cache.peek("s", "k")
        if cur is not None and cur.value["payload"] == "REFRESHED":
            break
    final = await h.service.get("s", "k")
    assert final["data"] == "REFRESHED" and final["state"] == "fresh"
    print(
        "[判定] input=('s','k') SWR过期并发x15 upstream_calls="
        f"{h.upstream.calls_for('k')} "
        "reason=先返回state=refreshing旧值，后台单飞刷新一次"
    )
