"""可在进程内替换的本地数据源，用于模拟真实上游。

特性：
- 数据可在进程内随时替换（set_value / reset_data），模拟上游数据变化；
- 可按 key 注入超时 / 报错 / 非法内容三类故障；
- 可注入延迟与“闸门”事件，让测试能确定性地把回源挂在途中，
  从而与失效/写入操作制造交错；
- 精确统计上游真实调用次数（总计与按 key）。
"""

from __future__ import annotations

import asyncio
from typing import Any, Protocol, runtime_checkable

from app.logging_utils import log_event

# 注入故障的三种模式，与 models.UpstreamErrorKind 一一对应
FAIL_TIMEOUT = "timeout"
FAIL_ERROR = "error"
FAIL_INVALID = "invalid"


class UpstreamTimeoutError(TimeoutError):
    """模拟上游超时。"""


class UpstreamRemoteError(RuntimeError):
    """模拟上游显式报错（5xx / 业务错误等）。"""


@runtime_checkable
class UpstreamDataSource(Protocol):
    """上游数据源协议：进程内可替换。"""

    name: str

    async def fetch(self, key: str, *, scope: str = "default") -> Any:
        """按 (scope, key) 获取数据；失败由实现抛出自定义异常，service 层归一化。"""
        ...


class LocalUpstream:
    """一个可控的进程内上游实现。"""

    def __init__(self, name: str = "local-upstream") -> None:
        self.name = name
        # 数据以 (scope, key) 隔离，模拟真实上游的多租户数据源
        self._store: dict[tuple[str, str], Any] = {}
        self._failures: dict[tuple[str, str], str] = {}
        self._delays: dict[tuple[str, str], float] = {}
        self._gates: dict[tuple[str, str], asyncio.Event] = {}
        self._calls_total = 0
        self._calls_by_key: dict[str, int] = {}
        self._versions: dict[tuple[str, str], int] = {}

    @staticmethod
    def _id(key: str, scope: str = "default") -> tuple[str, str]:
        return (scope, key)

    # ---- 数据控制：模拟上游内容变化 -------------------------------------
    def set_value(self, key: str, value: Any, *, scope: str = "default") -> None:
        """替换上游中的数据并推进版本号。"""
        ident = self._id(key, scope)
        self._store[ident] = value
        self._versions[ident] = self._versions.get(ident, 0) + 1

    def reset_data(self) -> None:
        self._store.clear()
        self._versions.clear()

    # ---- 故障/时序注入 ---------------------------------------------------
    def inject_failure(
        self, key: str, mode: str, *, scope: str = "default"
    ) -> None:
        if mode not in (FAIL_TIMEOUT, FAIL_ERROR, FAIL_INVALID):
            raise ValueError(f"unknown failure mode: {mode}")
        self._failures[self._id(key, scope)] = mode

    def clear_failure(self, key: str, *, scope: str = "default") -> None:
        self._failures.pop(self._id(key, scope), None)

    def set_delay(
        self, key: str, seconds: float, *, scope: str = "default"
    ) -> None:
        self._delays[self._id(key, scope)] = seconds

    def install_gate(
        self, key: str, *, scope: str = "default"
    ) -> asyncio.Event:
        """安装一个闸门：该 (scope,key) 的下次 fetch 会阻塞，直到事件被 set。"""
        event = asyncio.Event()
        self._gates[self._id(key, scope)] = event
        return event

    def remove_gate(self, key: str, *, scope: str = "default") -> None:
        self._gates.pop(self._id(key, scope), None)

    # ---- 观测 ------------------------------------------------------------
    @property
    def calls_total(self) -> int:
        return self._calls_total

    def calls_for(self, key: str) -> int:
        return self._calls_by_key.get(key, 0)

    # ---- 上游协议 --------------------------------------------------------
    async def fetch(self, key: str, *, scope: str = "default") -> Any:
        self._calls_total += 1
        self._calls_by_key[key] = self._calls_by_key.get(key, 0) + 1
        ident = self._id(key, scope)
        log_event(
            "upstream_fetch",
            upstream=self.name,
            scope=scope,
            key=key,
            upstream_calls=self._calls_total,
            input=(scope, key),
        )

        gate = self._gates.get(ident)
        if gate is not None:
            log_event("upstream_gate_wait", upstream=self.name, scope=scope, key=key)
            await gate.wait()

        delay = self._delays.get(ident, 0.0)
        if delay:
            await asyncio.sleep(delay)

        mode = self._failures.get(ident)
        if mode == FAIL_TIMEOUT:
            raise UpstreamTimeoutError(
                f"upstream {self.name} timeout for {scope}:{key}"
            )
        if mode == FAIL_ERROR:
            raise UpstreamRemoteError(
                f"upstream {self.name} error for {scope}:{key}"
            )
        if mode == FAIL_INVALID:
            # 非法内容：缺少 payload 字段，service 层负责判定
            return {"scope": scope, "key": key, "broken": True}

        if ident not in self._store:
            raise UpstreamRemoteError(
                f"upstream {self.name}: {scope}:{key} not found"
            )

        return {
            "scope": scope,
            "key": key,
            "version": self._versions.get(ident, 1),
            "payload": self._store[ident],
        }
