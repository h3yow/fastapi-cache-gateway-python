"""pytest 共享夹具：可控上游 + 网关服务 + 日志复位。"""

from __future__ import annotations

from typing import Any

import pytest

from app.cache import LocalCache
from app.logging_utils import reset_logs
from app.models import RefreshPolicy
from app.service import CacheGatewayService
from app.upstream import LocalUpstream


class Harness:
    """测试装配：直接暴露各组件，方便按场景注入故障与闸门。"""

    def __init__(
        self,
        *,
        ttl: float = 5.0,
        stale_ttl: float = 10.0,
        policy: RefreshPolicy = RefreshPolicy.SYNC,
        allow_stale_on_error: bool = True,
    ) -> None:
        self.upstream = LocalUpstream(name="test-upstream")
        self.cache = LocalCache(default_ttl=ttl, default_stale_ttl=stale_ttl)
        self.service = CacheGatewayService(
            self.cache,
            self.upstream,
            ttl=ttl,
            stale_ttl=stale_ttl,
            policy=policy,
            allow_stale_on_error=allow_stale_on_error,
        )

    def seed(self, key: str, value: Any, *, scope: str = "default") -> None:
        self.upstream.set_value(key, value, scope=scope)


@pytest.fixture(autouse=True)
def _reset_logs() -> None:
    reset_logs()
    yield
    reset_logs()


@pytest.fixture
def harness_factory():
    return Harness
