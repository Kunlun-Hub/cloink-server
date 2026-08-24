# Flow 日志可观测性运行手册

本文覆盖 E0-7 已接入的 Flow 日志接收、入库、查询和清理指标。指标只使用固定枚举标签；不得按账号、设备、用户、IP、端口、Flow ID 或事件 ID 分组。

## 指标与初始目标

Management 指标名称为 `management.flow.*`，以 Prometheus 暴露名为准：

- `management_flow_receive_counter`：`result=success|discarded|error`，`reason=none|invalid|retryable`。
- `management_flow_receive_duration`：接收及校验耗时，单位秒。
- `management_flow_store_counter` / `management_flow_store_duration`：`result=success|error`。
- `management_flow_query_counter` / `management_flow_query_duration` / `management_flow_query_rows`：`view=raw|grouped|details`，结果为 `success|error`。
- `management_flow_cleanup_counter` / `management_flow_cleanup_duration`：`result=success|error`。
- `management_flow_cleanup_rows_counter`：删除行数，固定 `reason=retention_or_limit`。

E0 初始目标是部署基线，不是对所有规模的永久承诺：

- 接收 p95 < 1 秒；持续 `retryable` 接收错误为故障。
- 查询 p95：raw/grouped/details 各自以部署基线为准；连续 15 分钟显著偏离基线即调查。
- cleanup 每轮成功；连续两轮失败或耗时超过清理间隔即告警。
- Flow 数据完整性没有 telemetry 证明时，Dashboard 必须显示 `unknown`，不能推断为 healthy。

## 故障定位

### 1. Dashboard 无数据

1. 先看 `management_flow_receive_counter` 是否有 `success`。
2. 若 `discarded{reason=invalid}` 增长，检查客户端版本、Flow token、时间窗口和事件 schema；不要通过关闭认证或放宽校验修复。
3. 若 `error{reason=retryable}` 增长，检查数据库连接、锁等待和 Management 日志。
4. 若接收成功但查询为空，确认日期范围、账号权限和过滤器；raw、grouped、details 都从认证上下文取得账号。

### 2. 查询变慢或失败

比较 `query_duration` 与 `query_rows`，按 `view` 区分 raw、grouped、details。检查数据库慢查询、连接池、时间范围和页大小。不要把账号或搜索词加入指标标签；需要调查时使用请求 ID 和数据库查询日志。

### 3. Cleanup 失败或数据库增长

检查 `cleanup_counter{result=error}`、清理耗时、数据库空间和索引。确认保留时间与每账号行数保护边界仍有效。修复数据库或锁问题后手动触发/等待下一轮 cleanup，再确认成功指标；不要直接删除全表。

### 4. Dashboard 显示 unknown/incomplete

`unknown` 表示当前没有足够的客户端采集、上传、ACK 或启动代次信号，不能解释为“没有流量”。E0-7 目前已记录 Management 接收、存储、查询和清理路径；客户端 admission、未 ACK 高水位和完整性状态接入真实 telemetry 前，不得将 unknown 改为 healthy。收到 `incomplete` 信号时保留原始/分组数据供调查，并先确认丢弃原因和恢复时间。

## 安全与恢复

- 所有查询必须使用认证账号；禁止通过 URL、日志或指标传入 account ID。
- 指标和普通日志不得包含 JWT、token、secret、IP/端口组合、完整事件 payload 或事件 ID。
- 重启期间客户端 retry/outbox 是否保留取决于客户端本地持久化状态；Management 数据库中已 ACK 的事件不会因查询失败重建。
- 恢复后验证：接收 success 恢复、retryable 错误停止、store success 恢复、查询 p95 回到基线、cleanup 下一轮成功。
- 任何数据删除、保留策略修改或跨账号导出都必须另行经过权限校验和审计流程；本手册不授权这些操作。
