# fastapi-cache-gateway

在 FastAPI 服务内提供一层**本地缓存 + 并发合并（singleflight）**的读网关，
上游由一个可在进程内替换的本地数据源（`LocalUpstream`）模拟，便于在高并发、
缓存失效与上游异常场景下得到稳定、可解释的结果。

## 结构

| 文件 | 职责 |
| --- | --- |
| `app/upstream.py` | `Upstream` 协议与 `LocalUpstream`：可注入延迟、一次性故障、非法载荷，并记录真实回源次数 `call_count` |
| `app/gateway.py` | `CacheGateway`：TTL 缓存、并发合并、代际（generation）失效、范围隔离、陈旧降级 |
| `app/errors.py` | 归一化上游异常：`UpstreamTimeout` / `UpstreamBadResponse` / `UpstreamInternalError`，以及 `ScopeForbidden` |
| `main.py` | FastAPI 接线：`GET/PUT/DELETE /cache/{scope}/{key}`，以及演示用的 `/upstream/*` |
| `tests/test_concurrency.py` | 以并发与竞态为主的测试，日志打印输入、上游调用次数与判定依据 |

## 缓存策略

每个缓存项以 `(scope, key)` 为键，策略由 `CachePolicy` 配置：

- `ttl`：新鲜窗口。窗口内命中直接返回，`fresh=True`。
- `max_stale`：可接受的**陈旧上限**。超过 `ttl` 但未超过 `ttl + max_stale` 的条目允许以降级形式返回；超过上限的条目直接作废，必须同步回源。
- `stale_while_revalidate`：过期行为开关。
  - `True`（默认）：先返回旧值（`stale=True`），同时后台回源刷新；
  - `False`：同步回源，调用方阻塞等待新值。

**并发合并**：同一 `(scope, key)` 的并发 `get` 只触发一次真实回源，其余请求挂到同一个
in-flight 任务上，拿到的是同一个结果对象——不会出现有人读到旧值、有人读到新值的
自相矛盾。后台刷新同样进入 in-flight 表，与前台请求合并。

## 过期与失效语义

- 每个 `(scope, key)` 维护一个单调递增的 **generation**；`invalidate` 与 `put` 都会使其 +1。
- 回源在发起时记录当时的 generation，写回前重新校验：
  若期间发生过失效/写入，本次回源结果**直接丢弃，不写回**。
  因此"失效之后旧值被还没回来的回源重新写进缓存"（复活）不可能发生。
- 与失效并发的那个 `get` 调用方仍可能拿到它自己那次回源的值（请求先于失效发出），
  但该值不会进入缓存；失效后的第一个 `get` 必然重新回源。

## 范围隔离

- key 按首次认领归属某个 scope（也可 `register(scope, key)` 预登记）。
- 其他 scope 对该 key 的 `get` / `put` / `invalidate` 一律抛出 `ScopeForbidden`
  （HTTP 层映射为 403），**不会返回别人的数据，也不会触达上游**。

## 上游异常归一化与降级

- 上游超时 → `UpstreamTimeout`（HTTP 504）；上游报错 → `UpstreamInternalError`（502）；
  返回内容不符合契约（必须是含 `value` 字段的 dict）→ `UpstreamBadResponse`（502）。
- **失败结果永不写入缓存**，无需手动清理即可自然恢复。
- 回源失败时，若存在未超过 `max_stale` 的陈旧条目，则降级返回该条目，
  结果中 `stale=True, fresh=False`——调用方可以明确分辨自己拿到的是旧数据；
  没有可用陈旧条目时异常原样抛出。

`FetchResult` 字段：`value / fresh / stale / from_cache / scope / key`。

## 本地验证

```bash
uv run pytest -q -s     # -s 查看每个用例的输入、上游调用次数与判定依据
uv run python main.py   # 启动服务（随机端口）
```

HTTP 快速体验（另开一个终端）：

```bash
curl -X POST localhost:PORT/upstream/k -H 'content-type: application/json' -d '{"value":"v1"}'
curl localhost:PORT/cache/s/k        # 首次回源，fresh=true
curl localhost:PORT/cache/s/k        # 命中缓存，from_cache=true
curl -X DELETE localhost:PORT/cache/s/k   # 失效
curl localhost:PORT/upstream/stats   # 查看真实回源次数
```
