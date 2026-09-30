"""结构化日志辅助。

用例日志需要打印：输入参数、上游被调用次数、判定依据。
为便于测试断言，同时输出到 stderr 与一个进程内可读取的内存缓冲。
"""

from __future__ import annotations

import sys
import threading
import time
from typing import Any

_buffer: list[str] = []
_lock = threading.Lock()


def log_event(event: str, **fields: Any) -> None:
    """打印一条结构化事件日志。

    例如::

        log_event("cache_hit", scope="tenant-a", key="user:1", reason="entry_fresh")
    """
    parts = [f"{k}={_format(v)}" for k, v in fields.items()]
    line = f"{time.strftime('%H:%M:%S')} [{event}] " + " ".join(parts)
    print(line, file=sys.stderr, flush=True)
    with _lock:
        _buffer.append(line)


def get_logs() -> list[str]:
    """返回缓冲的日志副本（供测试断言判定依据）。"""
    with _lock:
        return list(_buffer)


def reset_logs() -> None:
    """清空内存日志缓冲（测试隔离用）。"""
    with _lock:
        _buffer.clear()


def _format(value: Any) -> str:
    text = repr(value)
    return text if len(text) <= 120 else text[:117] + "..."
