"""Concurrency- and race-focused tests for the cache gateway.

Every test prints its inputs, the upstream call count, and the basis on
which the assertion is made (run with `pytest -s` to see the log).
"""
from __future__ import annotations

import asyncio

import pytest

from app.errors import ScopeForbidden, UpstreamBadResponse, UpstreamTimeout
from app.gateway import CacheGateway, CachePolicy
from app.upstream import LocalUpstream


def report(case: str, **facts) -> None:
    print(f"\n[case] {case}")
    for k, v in facts.items():
        print(f"  {k}: {v}")


def make(ttl=0.05, max_stale=1.0, swr=True, latency=0.02):
    up = LocalUpstream(latency=latency)
    gw = CacheGateway(up, CachePolicy(ttl=ttl, max_stale=max_stale,
                                      stale_while_revalidate=swr))
    return up, gw


async def test_concurrent_same_key_fetches_upstream_once():
    up, gw = make()
    up.set_value("k", "v1")
    n = 20
    results = await asyncio.gather(*(gw.get("s", "k") for _ in range(n)))
    values = {r.value for r in results}
    report("concurrent same-key get",
           input=f"{n} parallel gets of ('s','k')",
           upstream_calls=up.call_count,
           distinct_values=values,
           basis="singleflight: exactly 1 upstream call; all callers see one value")
    assert up.call_count == 1
    assert values == {"v1"}
    assert all(r.fresh and not r.stale for r in results)


async def test_invalidate_during_inflight_fetch_no_resurrection():
    up, gw = make(latency=0.1)
    up.set_value("k", "old")
    task = asyncio.ensure_future(gw.get("s", "k"))
    await asyncio.sleep(0.02)  # fetch is in flight, not yet back
    up.set_value("k", "new")
    await gw.invalidate("s", "k")
    first = await task          # caller that raced invalidation may see old
    second = await gw.get("s", "k")
    report("invalidate racing in-flight fetch",
           input="get(k) starts; upstream changes to 'new'; invalidate(k)",
           in_flight_result=first.value,
           after_invalidate=second.value,
           upstream_calls=up.call_count,
           basis="generation bump: in-flight 'old' must not be written back; "
                 "next get refetches and returns 'new'")
    assert first.value == "old"
    assert second.value == "new"
    assert up.call_count == 2


async def test_cross_scope_read_is_rejected():
    up, gw = make()
    up.set_value("k", "secret-of-a")
    first = await gw.get("a", "k")   # scope 'a' claims the key
    report("cross-scope read",
           input="scope 'a' reads k, then scope 'b' reads and writes k",
           a_result=first.value,
           upstream_calls=up.call_count,
           basis="key ownership: foreign scope must get ScopeForbidden, not data")
    with pytest.raises(ScopeForbidden):
        await gw.get("b", "k")
    with pytest.raises(ScopeForbidden):
        await gw.put("b", "k", "hijack")
    with pytest.raises(ScopeForbidden):
        await gw.invalidate("b", "k")
    assert up.call_count == 1  # rejected reads never hit upstream


async def test_upstream_failure_not_cached_and_degrades_to_stale():
    up, gw = make(ttl=0.05, max_stale=1.0, swr=False)
    up.set_value("k", "v1")
    r1 = await gw.get("s", "k")
    await asyncio.sleep(0.06)  # now stale but within max_stale
    up.fail_next("k", asyncio.TimeoutError())
    r2 = await gw.get("s", "k")  # upstream times out -> degrade to stale
    report("upstream timeout with stale entry present",
           input="cached v1 past ttl; next upstream call times out",
           degraded_value=r2.value, fresh=r2.fresh, stale=r2.stale,
           upstream_calls=up.call_count,
           basis="degradation: caller gets stale=True so it can tell old data")
    assert r2.value == "v1" and r2.stale and not r2.fresh
    # failure must not be cached: next call retries upstream and succeeds
    up.set_value("k", "v2")
    r3 = await gw.get("s", "k")
    report("retry after failure",
           upstream_calls=up.call_count, recovered=r3.value,
           basis="failures are never stored; recovery needs no manual flush")
    assert r3.value == "v2" and r3.fresh
    assert up.call_count == 3


async def test_upstream_error_without_stale_propagates_and_stays_uncached():
    up, gw = make()
    up.set_value("k", "v1")
    up.fail_next("k", RuntimeError("boom"))
    with pytest.raises(Exception) as ei:
        await gw.get("s", "k")
    r = await gw.get("s", "k")  # immediate retry must hit upstream again
    report("upstream error with empty cache",
           input="first fetch fails, second fetch succeeds",
           error=type(ei.value).__name__,
           upstream_calls=up.call_count,
           basis="error result is not cached: retry goes upstream, gets v1")
    assert r.value == "v1"
    assert up.call_count == 2


async def test_bad_payload_normalized_and_not_cached():
    up, gw = make()
    up.set_raw("k", "this-is-not-a-dict")  # violates upstream contract
    with pytest.raises(UpstreamBadResponse):
        await gw.get("s", "k")
    up.set_value("k", "fixed")
    r = await gw.get("s", "k")
    report("invalid payload normalization",
           input="upstream returns a string instead of {'value': ...}",
           upstream_calls=up.call_count,
           basis="contract violation -> UpstreamBadResponse; not cached, retry ok")
    assert r.value == "fixed"
    assert up.call_count == 2


async def test_stale_while_revalidate_serves_stale_then_refreshes():
    up, gw = make(ttl=0.1, max_stale=1.0, swr=True)
    up.set_value("k", "v1")
    await gw.get("s", "k")
    await asyncio.sleep(0.12)  # past ttl, within max_stale
    up.set_value("k", "v2")
    r_stale = await gw.get("s", "k")   # serves stale, triggers bg refresh
    while up.call_count < 2:           # background fetch has started...
        await asyncio.sleep(0.005)
    await asyncio.sleep(0.05)          # ...now wait for it to land (latency 0.02)
    r_new = await gw.get("s", "k")     # still within ttl of the refresh
    report("stale-while-revalidate",
           input="ttl expired; upstream already has v2",
           immediate=(r_stale.value, r_stale.stale),
           after_refresh=(r_new.value, r_new.fresh),
           upstream_calls=up.call_count,
           basis="stale served instantly with stale=True; bg refresh stores v2")
    assert r_stale.value == "v1" and r_stale.stale
    assert r_new.value == "v2" and r_new.fresh
    assert up.call_count == 2


async def test_beyond_max_stale_entry_is_dead():
    up, gw = make(ttl=0.02, max_stale=0.05, swr=True)
    up.set_value("k", "v1")
    await gw.get("s", "k")
    await asyncio.sleep(0.09)  # past ttl + max_stale
    up.set_value("k", "v2")
    r = await gw.get("s", "k")  # must fetch synchronously, no stale serving
    report("entry beyond max_stale",
           input="age > ttl + max_stale",
           result=(r.value, r.stale),
           upstream_calls=up.call_count,
           basis="staleness ceiling: too-old entries are dropped, sync refetch")
    assert r.value == "v2" and not r.stale
    assert up.call_count == 2
