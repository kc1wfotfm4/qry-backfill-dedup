# qry-backfill-dedup

把回填批次的范围、去重键、幂等标识与处理进度记录成可查询的服务，支持按数据集查询已经覆盖的时间区间并安全重放未完成的回填批次。

## 运行要求

- Go 1.26 或以上
- SQLite（本服务自带存储，不需要外部数据库）

## 构建、测试与启动

```bash
go build ./...
go test ./...
go run .
```

服务默认监听 `127.0.0.1:8080`。可用环境变量覆盖：

| 变量 | 默认值 | 用途 |
|---|---|---|
| `ADDR` | `127.0.0.1:8080` | HTTP 监听地址 |
| `DB_PATH` | `qry-backfill-dedup.db` | SQLite 数据库文件路径 |

## 已公开的入口

### `POST /api/v1/backfills`

登记一个回填批次。请求体是 JSON 对象，五个字符串字段均必填：

| 字段 | 含义 |
|---|---|
| `dataset_id` | 数据集标识 |
| `batch_id` | 批次编号，同时作为响应中的 `id` |
| `idempotency_key` | 幂等去重键 |
| `start_time` | 区间起点（含），RFC 3339 |
| `end_time` | 区间终点（不含），RFC 3339 |

新批次返回 HTTP 201：

```json
{"id":"batch-1","dataset_id":"ds-1","batch_id":"batch-1","idempotency_key":"key-1","start_time":"2026-01-01T00:00:00Z","end_time":"2026-01-01T01:00:00Z","status":"pending","progress":0}
```

相同 `idempotency_key` 且五个字段内容一致的重放返回 HTTP 200 与同一份批次 JSON，不新增记录、不重置 `progress`、不重复合并覆盖；相同键但其他字段不同返回 HTTP 409，`code` 为 `idempotency_conflict`。起点与终点相等表示零长度区间，允许创建；终点早于起点返回 HTTP 400，`code` 为 `inverted_range`。

### `GET /api/v1/backfills/{id}`

按批次编号查询。命中返回 HTTP 200 与创建时同结构的批次 JSON；未命中返回 HTTP 404，`code` 为 `batch_not_found`。

### `GET /api/v1/datasets/{dataset_id}/coverage`

返回数据集已覆盖的合并区间，区间按起点升序；重叠或前一终点等于后一起点的相邻区间会合并。空覆盖返回 `[]`：

```json
{"datasetId":"ds-1","intervals":[{"startTime":"2026-01-01T00:00:00Z","endTime":"2026-01-01T04:00:00Z"}]}
```

批次写入与覆盖合并在同一个 SQLite 事务内完成，`idempotency_key` 上有唯一约束，并发重复提交只会有一个批次落库。

### `GET /healthz`

返回服务与存储状态。正常时 HTTP 200：

```json
{"status":"ok","database":"ok"}
```

存储不可用时 HTTP 503：

```json
{"error":{"code":"storage_unavailable","message":"database is not available"}}
```

## 错误约定

所有错误响应都是单个顶层 `error` 对象，包含 `code` 与 `message` 两个字符串字段；`message` 只描述业务原因，不包含 SQL、堆栈、文件路径或内部包名。

| HTTP | `code` | 触发条件 |
|---|---|---|
| 400 | `missing_field` | 必填字段缺失、不是字符串或为空字符串 |
| 400 | `invalid_time` | `start_time`/`end_time` 不是 RFC 3339 时间 |
| 400 | `inverted_range` | `end_time` 早于 `start_time` |
| 400 | `invalid_request` | 请求体不是合法 JSON 对象 |
| 404 | `batch_not_found` | 批次编号不存在 |
| 409 | `idempotency_conflict` | 幂等键已被不同内容的请求占用 |
| 404 | `route_not_found` | 路径不匹配任何入口 |
| 503 | `storage_unavailable` | 健康检查发现存储不可用 |
