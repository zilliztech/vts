import ChangeLog from '../changelog/connector-cdc-milvus.md';

# Milvus CDC

> Milvus CDC 源连接器

## 描述

Milvus CDC 源连接器通过 Milvus `DumpMessages` 读取 WAL 消息，并输出 changelog row。它主要用于和 Milvus Sink 的 `write_mode = "CDC"` 配合，构建实时的 Milvus 到 Milvus 同步链路。

当前版本需要用户显式提供 pchannel 起始位点，不会自动执行初始全量快照，并会持续运行，直到通过 SeaTunnel 任务生命周期取消或停止任务。

## 关键特性

- [ ] [批处理](../../concept/connector-v2-features.md)
- [x] [流处理](../../concept/connector-v2-features.md)
- [x] [精确一次](../../concept/connector-v2-features.md)
- [ ] [列投影](../../concept/connector-v2-features.md)
- [x] [并行度](../../concept/connector-v2-features.md)

## 使用要求

- Milvus 或 Zilliz Cloud 需要提供 Milvus CDC 使用的 `DumpMessages` API。
- 完整 CDC 同步能力依赖 Milvus 内核支持，要求 Milvus 版本大于等于 2.6.21。Milvus 2.6.14 到 2.6.20 存在已知限制：`DumpMessages` 无法获取 upsert transaction 消息内容，因此无法完整支持包含 upsert transaction 的 CDC 同步。
- `channel_positions` 必须配置物理 pchannel，例如 `by-dev-rootcoord-dml_0`，不能配置带 collection id 后缀的 vchannel。
- 每个物理 pchannel 在 `channel_positions` 中只能出现一次。
- Source collection 的 schema 在任务启动后必须保持不变。当前尚不支持运行期间新增、删除或重命名字段以及修改字段类型等 schema change；后续 DML 与启动时 schema 不一致时，Source 会失败，而不会自动执行 schema evolution。
- 使用 Milvus Sink CDC 模式时，目标 collection schema 必须已经存在，并且目标主键必须配置 `autoID = false`。Source 可以读取源 collection 在 `autoID = true` 时自动生成的主键，但目标端必须接收这些源主键值，CDC upsert 和 delete 才能操作同一条数据。

## 支持的消息类型

| Milvus WAL 消息 | SeaTunnel RowKind | 说明 |
|-----------------|-------------------|------|
| Insert          | INSERT            | Milvus Sink CDC writer 会按 upsert 写入。 |
| Delete          | DELETE            | Milvus Sink CDC writer 会按主键删除。 |

事务控制消息由 source 内部消费。如果一个 Milvus 事务被拆成多条 WAL 消息，source 会缓存事务内的 DML row，直到收到事务 commit 消息后再输出，并把 checkpoint 位点推进到 commit 消息。Sink 可能根据 batch size、RowKind 切换、flush interval 和 checkpoint 生命周期，跨原始 DML 消息边界进行批量写入；Milvus Sink CDC 模式不在目标端提供多操作事务原子性。

## 源选项

| 名称 | 类型 | 是否必填 | 默认值 | 描述 |
|------|------|----------|--------|------|
| url | String | 是 | - | Milvus 或 Zilliz Cloud 访问地址。 |
| token | String | 是 | - | Milvus 认证 token，通常为 `username:password`。 |
| database_collections | Map&lt;String, List&lt;String&gt;&gt; | 是 | - | 源 database 到 collection 列表的映射。不支持 collection 通配。 |
| channel_positions | List&lt;Map&gt; | 是 | - | 每个 pchannel 的起始位点。 |
| message_types | List&lt;String&gt; | 否 | `["insert","delete"]` | 输出的 WAL 消息类型。目前支持 `insert` 和 `delete`。 |
| startup_mode | Enum | 否 | `CDC` | 启动模式。目前只支持 `CDC`。 |
| queue_capacity | Integer | 否 | 16 | 每个 reader 缓存的 WAL 消息数量。建议保持较小值；下游写入慢时增大该值不能解决吞吐问题，还可能因大消息增加内存压力。 |
| client_pem_path | String | 否 | - | TLS 客户端证书 PEM 路径。 |
| client_key_path | String | 否 | - | TLS 客户端私钥路径。 |
| ca_pem_path | String | 否 | - | TLS CA 证书 PEM 路径。 |
| server_name | String | 否 | - | TLS 校验使用的 server name。 |

## Channel Position

每个 `channel_positions` 元素描述一个物理 pchannel。

| 字段 | 类型 | 是否必填 | 描述 |
|------|------|----------|------|
| pchannel | String | 是 | Milvus 物理 pchannel，例如 `by-dev-rootcoord-dml_0`。 |
| start | Object | 是 | `DumpMessages` 起始位点。 |

`start` 对象支持以下字段：

| 字段 | 类型 | 是否必填 | 描述 |
|------|------|----------|------|
| wal_name | String | 是 | WAL 名称，例如 `RocksMQ` 或 `Pulsar`。 |
| resume_message_id | String | 是 | 作为 Milvus `start_message_id` 传入的消息 ID。建议使用 Milvus last confirmed message id，例如 FlushAll 消息返回的 `_lc` / `LastConfirmedMessageID`。 |
| consumed_message_id | String | 否 | 已消费消息 ID。通常由 checkpoint 状态维护；只有手工恢复到某条已消费消息时才需要配置。 |
| timetick | Long | 是 | 和 start message id 一起使用的 Milvus hybrid timestamp。 |

为了兼容旧配置，`message_id` 会被当作 `resume_message_id`，`last_confirmed_message_id` 会被当作旧版 resume 字段。

对于 RocksMQ，message id 必须是 Milvus CDC 期望的 base36 文本 offset，例如 `-1`。不要直接复制 Milvus 日志里的二进制转义 `msgID`。

## Checkpoint、位点和一致性语义

Source 会先完整输出一条 WAL 消息，再推进 split offset，避免 checkpoint 越过尚未完整输出的消息。

对于事务消息，source 在事务 row 输出后把 checkpoint 位点推进到事务 commit 消息。这样可以保证 source 侧恢复语义。

配合 Milvus Sink CDC 模式时，INSERT row 会按 upsert 写入，DELETE row 会按主键删除，因此失败恢复后的重复写可以通过目标端幂等写得到最终状态一致。这里的精确一次是基于 checkpoint 位点原子推进和幂等 CDC 写入的最终状态语义，不是目标端 2PC 或事务级精确一次。Milvus Sink CDC 模式不保证同一操作物理上只提交一次，也不提供目标端多操作事务原子性。

## Metrics

| 指标 | 描述 |
|------|------|
| MilvusCdcSourceReceiveDelayMs | 各 split 中最大的 source 接收延迟，即接收墙上时间和最新接收事件时间的差值。收到新 WAL 消息时更新。 |
| MilvusCdcSourceReceiveToCommitDelayMs | 各 split 中最大的最新接收事件时间和最近完成 checkpoint 的 commit 时间差。 |
| MilvusCdcSourceLastCommitTsAllSplitsMinMs | 所有已分配 split 中最小的已提交事件时间，单位毫秒。 |
| MilvusCdcSourceLastCommitTsAllSplitsMaxMs | 所有已分配 split 中最大的已提交事件时间，单位毫秒。 |

如果没有新的 WAL 消息，这些指标不会由后台定时器推进。需要判断链路是否继续前进时，应重点观察 commit timestamp 指标。

## 任务示例

```bash
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  "Milvus-CDC" {
    url = "http://127.0.0.1:19530"
    token = "root:Milvus"
    database_collections = {
      "default" = ["source_collection"]
    }
    startup_mode = "cdc"
    message_types = ["insert", "delete"]
    channel_positions = [
      {
        pchannel = "by-dev-rootcoord-dml_0"
        start = {
          wal_name = "RocksMQ"
          resume_message_id = "-1"
          timetick = 0
        }
      }
    ]
  }
}

sink {
  Milvus {
    url = "http://127.0.0.1:19530"
    token = "root:Milvus"
    database = "default"
    collection_rename = {
      "source_collection" = "target_collection"
    }
    write_mode = "CDC"
    schema_save_mode = "ERROR_WHEN_SCHEMA_NOT_EXIST"
    data_save_mode = "APPEND_DATA"
  }
}
```

## 变更日志

<ChangeLog />
