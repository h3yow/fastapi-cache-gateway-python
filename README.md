# FastAPI 缓存网关（本地缓存 + 并发合并 + 可控降级）

在普通 FastAPI 服务前面加一层进程内缓存网关，提供：

1. **缓存读写与并发合并（single-flight）**：同一时刻对同一份数据的并发请求，只有一次真正回源；
2. **明确的过期语义**：同步回源（SYNC）或 stale-while-revalidate（SWR，先旧值后后台刷新），并支持配置“可接受的陈旧上限” `stale_ttl`；
3. **失效与范围隔离**：失效/写入用“代数（generation）”压制在途回源的旧值写回；不同调用范围（scope）缓存严格隔离，越范围读取直接拒绝；
4. **上游异常归一化与降级**：超时 / 报错 / 非法内容三类可区分、可记录；失败绝不入缓存；允许降级时显式标注 `state=stale`。

## 目录结构

```
app/
  models.py         # 枚举、异常、CacheEntry/CacheResult 数据结构
  upstream.py       # 可在进程内替换的本地数据源 LocalUpstream（故障/闸门/延迟注入）
  cache.py          # LocalCache：存储、新鲜度判定、单飞合并、generation 失效
  service.py        # CacheGatewayService：策略、异常归一化、陈旧降级、范围强制
  api.py            # FastAPI 路由装配
  logging_utils.py  # 结构化日志（stderr + 内存缓冲，便于测试断言）
tests/              # 并发/竞态为主的用例（21 个）
main.py             # 入口：uvicorn app
```

## 缓存策略

### 并发合并（single-flight）

- 在途回源以 `(scope, key, generation)` 索引，同一代数的并发请求共享同一个 `_Flight`；
- leader 真正执行一次回源，其余请求 await 同一个 `asyncio.Event`，复用**同一个值或同一个异常**；
- 条目采用“整体替换、永不原地修改”，因此读路径不可能读到半新半旧、也不会有人读旧值有人读新值；
- 不同 key、不同 scope 不合并。

### 过期策略

每个条目有两个时间参数（单调时钟判定）：

| 参数 | 含义 |
| --- | --- |
| `ttl` | 硬过期时间。`age < ttl` 一律命中，不回源 |
| `stale_ttl` | **可接受的陈旧上限**，相对硬过期点。`ttl <= age < ttl + stale_ttl` 视为“可用旧值” |

两种策略（`RefreshPolicy`）：

- **SYNC（默认，行为确定）**：一旦硬过期，所有请求同步等待一次合并回源，成功后返回 `state=fresh`；
- **SWR（stale-while-revalidate）**：硬过期但仍在陈旧上限内时，立即返回旧值 `state=refreshing`，
  同时保证后台只有一次刷新；刷新成功后后续请求得到 `fresh`；后台刷新失败只记日志、保留旧值。
- 无论哪种策略，**超过 `ttl + stale_ttl` 的旧值都不允许再服务**。

### 失效语义（generation，防止旧值复活）

- 每个 `(scope, key)` 维护一个递增 `generation`；
- `invalidate` 与外部 `put` 都会推进代数：
  - 读路径立即看不到旧代数条目（`peek` 返回 `None`），下一次读必须发起新代数的回源；
  - 在途回源**不会被取消**，但提交时若发现自己的代数已落后，结果直接丢弃（日志事件 `commit_superseded`）；
- 因此：
  - “回源还没回来时执行失效”——那次回源的旧值不会在失效后写回缓存；
  - “回源途中外部写入新值”——回源结果不会覆盖更新的写入；
- 失效的旧条目会保留为**陈旧降级候选**：正常读取不可见，仅在上游失败、配置允许降级且未超陈旧上限时，
  以 `state=stale` 显式标注返回，调用方可以清楚分辨这不是新数据。

### 范围隔离（scope）

- 缓存键为 `(scope, key)`；本地数据源 `LocalUpstream` 同样按 scope 存储，双重隔离；
- 正常读取只能读自己的 scope；
- 显式跨范围读取走 `read_across_scope` / `GET /cross/{owner_scope}/{key}`，必然抛出
  `ScopeViolationError`（HTTP 403），**不会返回别人的数据**。

### 上游异常归一化与降级

| 上游表现 | 归一化结果 `UpstreamErrorKind` | 触发方式（LocalUpstream） |
| --- | --- | --- |
| 超时 | `timeout` | `inject_failure(key, "timeout")` |
| 显式报错 / 404 / 未知异常 | `error` | `inject_failure(key, "error")` |
| 返回内容缺少 `payload` 等不合法结构 | `invalid` | `inject_failure(key, "invalid")` |

- 归一化异常为 `UpstreamFailureError(kind, ...)`，经单飞广播给全部并发等待者；
- **失败结果绝不写入缓存**（loader 失败时缓存层只摘除在途标记，不产生条目）；
- 回源失败后的决策（`allow_stale_on_error=True`）：
  - 存在旧值且 `age < ttl + stale_ttl`：返回旧值，`state=stale`、`degraded_from_error=timeout|error|invalid`；
  - 无旧值 / 超出陈旧上限 / 不允许降级：抛出 `CacheMissError`（HTTP 503）。

返回体示例：

```json
{
  "scope": "tenant-a", "key": "user:1", "version": 3,
  "data": {"name": "alice"},
  "state": "fresh|refreshing|stale",
  "upstream_calls": 1,
  "degraded_from_error": null
}
```

## HTTP 接口

> 调用范围由请求头 `X-Scope` 指定，缺省为 `default`。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/data/{key}?policy=sync|swr` | 读取（命中 / 合并回源 / 降级） |
| PUT | `/data/{key}` | 显式写入，body：`{"value": ...}`（推进代数） |
| POST | `/data/{key}/invalidate` | 失效（推进代数） |
| GET | `/cross/{owner_scope}/{key}` | 演示跨范围读取，固定 403 |
| POST | `/admin/upstream/value` | 进程内替换上游数据 `{"key","value"}` |
| POST | `/admin/upstream/fail` | 注入故障 `{"key","mode":"timeout|error|invalid"}` |
| POST | `/admin/upstream/clear` | 清除故障注入 |
| GET | `/health` | 健康检查与上游调用总数 |

## 本地验证

需要 Python 3.14+，使用 [uv](https://github.com/astral-sh/uv)：

```bash
# 安装依赖（含 pytest / httpx）
uv sync

# 运行全部用例（并发 + 竞态 + 隔离 + 失败归一化 + HTTP）
uv run pytest -q

# 只看竞态相关日志（用例会打印输入、上游调用次数、判定依据）
uv run pytest tests/test_cache_coalescing.py tests/test_invalidation_race.py -s

# 启动服务
uv run python main.py   # http://127.0.0.1:8000 ，文档 /docs
```

手工验证示例：

```bash
# 1) 先在进程内上游放一份数据，再读两次（第二次命中缓存，upstream_calls 不增加）
curl -s -XPOST localhost:8000/admin/upstream/value \
  -H 'content-type: application/json' \
  -d '{"key":"user:1","value":{"name":"alice"}}'
curl -s localhost:8000/data/user:1 -H 'X-Scope: tenant-a'
curl -s localhost:8000/data/user:1 -H 'X-Scope: tenant-a'

# 2) 注入超时 + 失效，观察降级返回（state=stale, degraded_from_error=timeout）
curl -s -XPOST localhost:8000/admin/upstream/fail \
  -H 'content-type: application/json' -d '{"key":"user:1","mode":"timeout"}'
curl -s -XPOST localhost:8000/data/user:1/invalidate -H 'X-Scope: tenant-a'
curl -s localhost:8000/data/user:1 -H 'X-Scope: tenant-a'

# 3) 跨范围读取被拒绝（403）
curl -i localhost:8000/cross/tenant-a/user:1 -H 'X-Scope: tenant-b'
```

## 测试如何制造确定性竞态

`LocalUpstream.install_gate(key, scope=...)` 返回一个 `asyncio.Event`：
命中的下次 `fetch` 会阻塞在事件上，测试据此把回源确定性地挂在途中，
再安排失效、写入、多请求并发，最后 `gate.set()` 放行并断言：

- 上游真实调用次数（`upstream.calls_for(key)`）；
- 所有并发返回值/版本一致；
- 失效后缓存读视图为空、旧值未复活；
- 跨范围读取得到 `ScopeViolationError`。

每条用例均通过 `print` 输出“输入 / 上游调用次数 / 判定依据”，可用 `pytest -s` 查看。
