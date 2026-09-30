"""领域模型：枚举、异常与数据结构（仅类型定义，无行为逻辑）。"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any


class ValueState(str, enum.Enum):
    """调用方拿到的数据新鲜度，用于区分新数据与降级旧数据。"""

    FRESH = "fresh"          # 本次回源得到的新数据
    STALE = "stale"          # 允许降级时返回的过期旧数据
    REFRESHING = "refreshing"  # 先返回旧值，后台正在刷新（stale-while-revalidate）


class RefreshPolicy(str, enum.Enum):
    """缓存过期后的回源策略。"""

    SYNC = "sync"     # 过期即同步回源，调用方等待新值
    SWR = "swr"       # stale-while-revalidate：先返回旧值再后台刷新


class UpstreamErrorKind(str, enum.Enum):
    """上游异常的归一化分类。"""

    TIMEOUT = "timeout"      # 上游超时
    ERROR = "error"          # 上游显式报错
    INVALID = "invalid"      # 上游返回内容不合法


class GatewayError(Exception):
    """网关层错误基类。"""


class ScopeViolationError(GatewayError):
    """跨调用范围读取：拒绝访问，而不是返回别人的数据。"""


class UpstreamFailureError(GatewayError):
    """上游归一化后的失败：超时 / 报错 / 非法内容。"""

    def __init__(self, kind: UpstreamErrorKind, message: str, *, key: str) -> None:
        self.kind = kind
        self.key = key
        super().__init__(f"[{kind.value}] key={key}: {message}")


class CacheMissError(GatewayError):
    """无缓存且无法回源（回源失败且不允许/无法降级）。"""


@dataclass(slots=True)
class CacheEntry:
    """单条缓存记录。

    generation 是该 (scope, key) 的“代数”：失效与写入会推进代数，
    过期回源提交时若代数已变化则丢弃结果，防止旧值复活。
    """

    value: Any
    fetched_at: float                 # 写入缓存的单调时钟时间
    ttl: float                        # 硬过期时间（秒）
    stale_ttl: float                  # 可接受的陈旧上限（秒，相对硬过期）
    scope: str
    key: str
    generation: int
    last_error_kind: UpstreamErrorKind | None = None  # 失败不入缓存，仅用于观测
    source: str = "upstream"          # 数据来源标识


@dataclass(slots=True)
class CacheResult:
    """读操作返回给 service 层的结果包装。"""

    value: Any
    state: ValueState
    scope: str
    key: str
    generation: int
    fetched_at: float
    age: float
