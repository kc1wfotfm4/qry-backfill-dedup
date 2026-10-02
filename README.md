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

### `GET /healthz`

返回服务与存储状态。正常时 HTTP 200：

```json
{"status":"ok","database":"ok"}
```

存储不可用时 HTTP 503：

```json
{"error":{"code":"storage_unavailable","message":"database is not available"}}
```

### `POST /api/v1/backfills`

登记一个回填批次。请求体是 JSON 对象，包含五个字符串字段：`dataset_id`、`batch_id`、`idempotency_key`、`start_time`、`end_time`。时间按 RFC 3339 表示，区间为起点含、终点不含；`end_time` 与 `start_time` 相等时表示零长度范围。

新批次返回 HTTP 201，JSON 回显五个输入字段并附带 `id`（取 `batch_id` 值）、`status`（`pending`）与 `progress`（`0`）。批次写入与覆盖区间合并在同一事务完成，`idempotency_key` 有唯一约束。

使用相同 `idempotency_key` 重放且输入内容一致时返回 HTTP 200 与原批次 JSON，不新增记录、不重置进度、不重复合并覆盖；键相同但内容不同返回 HTTP 409，`code` 为 `idempotency_conflict`。

### `GET /api/v1/backfills/{id}`

按批次编号查询。命中返回 HTTP 200 与创建时相同的批次 JSON；未命中返回 HTTP 404，`code` 为 `batch_not_found`。

### `GET /api/v1/datasets/{dataset_id}/coverage`

返回 HTTP 200，JSON 含 `datasetId` 与 `intervals` 数组；元素含 `startTime`、`endTime`，按时间升序给出合并后的最大区间（重叠或首尾相接的相邻区间会被合并）。无覆盖时 `intervals` 为 `[]`。

## 错误约定

所有错误响应都是单个顶层 `error` 对象，包含 `code` 与 `message` 两个字符串字段；`message` 不包含 SQL、堆栈或文件路径。

| HTTP | code | 含义 |
|---|---|---|
| 400 | `invalid_request` | 请求体不是合法的 JSON 对象 |
| 400 | `missing_field` | 缺少必填字段 |
| 400 | `invalid_time` | 时间不是合法的 RFC 3339 表示 |
| 400 | `inverted_range` | `end_time` 早于 `start_time` |
| 404 | `batch_not_found` | 批次编号不存在 |
| 404 | `route_not_found` | 路径没有匹配的路由 |
| 409 | `idempotency_conflict` | 幂等键已用于不同内容 |
| 503 | `storage_unavailable` | 存储不可用 |
