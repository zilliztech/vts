import ChangeLog from '../changelog/connector-milvus.md';

# Milvus

> Milvus数据接收器

## 描述

Milvus sink连接器将数据写入Milvus或Zilliz Cloud，它具有以下功能：
- 支持按分区读写数据
- 支持从元数据列写入动态模式数据
- json数据将转换为json字符串进行写入
- 自动重试以绕过 ratelimit 限制 和 grpc 限制
## 主要特性

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)（不适用于 CDC 模式）
- [ ] [column projection](../../concept/connector-v2-features.md)

##数据类型映射

|  Milvus数据类型   | SeaTunnel 数据类型 |
|---------------------|---------------------|
| INT8                | TINYINT             |
| INT16               | SMALLINT            |
| INT32               | INT                 |
| INT64               | BIGINT              |
| FLOAT               | FLOAT               |
| DOUBLE              | DOUBLE              |
| BOOL                | BOOLEAN             |
| JSON                | STRING              |
| ARRAY               | ARRAY               |
| VARCHAR             | STRING              |
| FLOAT_VECTOR        | FLOAT_VECTOR        |
| BINARY_VECTOR       | BINARY_VECTOR       |
| FLOAT16_VECTOR      | FLOAT16_VECTOR      |
| BFLOAT16_VECTOR     | BFLOAT16_VECTOR     |
| SPARSE_FLOAT_VECTOR | SPARSE_FLOAT_VECTOR |
| TIMESTAMPTZ         | TIMESTAMP_TZ        |
| GEOMETRY            | GEOMETRY            |

## Sink 选项

|         名字         | 类型    | 是否必传 |           默认值            | 描述                                               |
|----------------------|---------|----------|------------------------------|-----------------------------------------------------------|
| url                  | String  | 是      | -                            | 连接到Milvus或Zilliz Cloud的URL。             |
| token                | String  | 是      | -                            | 用户：密码                                             |
| database             | String  | 否       | -                            | 将数据写入哪个数据库，默认为源数据库。 |
| schema_save_mode     | enum    | 否       | CREATE_SCHEMA_WHEN_NOT_EXIST | 当表不存在时自动创建表。                   |
| data_save_mode       | enum    | 否       | APPEND_DATA                  | 数据保存模式。CDC 写模式只支持 `APPEND_DATA`。 |
| create_index         | boolean | 否       | true                         | 自动创建 collection 时是否创建索引。       |
| enable_auto_id       | boolean | 否       | false                        | 主键列启用autoId。                         |
| enable_upsert        | boolean | 否       | false                        | 仅 CDC 模式生效：对 INSERT/UPDATE_AFTER 始终使用完整行 upsert，优先于重叠时间上界。AutoID 在 upsert 内先 delete 再携带原主键 insert；APPEND 模式不受影响。                                   |
| enable_dynamic_field | boolean | 否       | true                         | 是否启用带动态字段的创建表。                   |
| batch_size           | int     | 否       | 1000                         | 写入批大小。                                         |
| cdc_batch_flush_interval_ms | long | 否       | 1000                         | CDC 模式下，收到新 row 时，如果距上次成功 flush 已达到该间隔，则 flush 未完成批次。单位为毫秒。 |
| cdc_overlap_upsert_cutoff_ms | long | 否 | - | CDC upsert 的可选时间上界，Unix 毫秒，包含边界，仅在 `enable_upsert = false` 时参与判断。源事件时间不晚于该值的 INSERT/UPDATE_AFTER 使用 upsert，包括 AutoID 目标。 |
| write_mode           | enum    | 否       | APPEND                       | 写入模式。`APPEND` 使用普通 batch 或 bulk writer 写入 insert row；`CDC` 默认对 INSERT 和 UPDATE_AFTER 携带源主键执行 insert（可通过 `enable_upsert` 或下述 upsert 窗口调整），对 DELETE 和 UPDATE_BEFORE 执行 delete。AutoID 目标必须开启 `allow_insert_auto_id = "true"`。 |
| geometry_convert_mode | String | 否       | passthrough                  | Milvus Geometry 字段的 String 值处理方式。`passthrough` 原样发送；`parse` 将 WKT/EWKT/GeoJSON/WKB hex 和支持的坐标格式转换为 WKT。 |
| geometry_string_coord_order | String | 否 | lat_lon                     | `parse` 模式下裸数字坐标字符串的顺序，支持 `lat_lon` 和 `lon_lat`。不影响 WKT、GeoJSON 和 WKB 输入。 |
| partition_key        | String  | 否       |                              | Milvus分区键字段                                |
| partition_num        | int     | 否       |                              | 透传给 Milvus create collection 请求的分区数量。当前主要用于 Milvus 分区键模式。 |
| collection_rename    | Map     | 否       | {}                           | 重命名集合：`{源名称 = "目标名称"}`       |
| field_schema         | List    | 否       | []                           | 按字段配置 schema。详见下文。                |

## 字段 Schema 配置

配置 `field_schema` 后，仅写入其中定义的字段。留空则使用完整的源端 schema。

每个字段对象支持以下属性：

| 属性               | 类型    | 是否必传 | 描述                                                                 |
|--------------------|---------|----------|----------------------------------------------------------------------|
| field_name         | String  | 是*     | Milvus 目标字段名。                                                  |
| source_field_name  | String  | 是*     | 源端字段名。两者都提供时，`field_name` 作为目标名。                    |
| data_type          | Integer | 是      | Milvus 数据类型编码（如 Int64=5, VarChar=21, FloatVector=101, Timestamptz=26）。 |
| is_primary_key     | Boolean | 否       | 标记为主键。                                                         |
| auto_id            | Boolean | 否       | 主键启用自动 ID。                                                    |
| dimension          | Integer | 否       | 向量类型必填。                                                       |
| max_length         | Integer | 否       | VarChar 字段的最大长度。                                             |
| element_type       | Integer | 否       | Array 字段的元素类型。                                               |
| max_capacity       | Integer | 否       | Array 字段的最大容量。默认：4096。                                   |
| is_nullable        | Boolean | 否       | 字段是否可为空。                                                     |
| is_partition_key   | Boolean | 否       | 标记为分区键。                                                       |
| timezone           | String  | 否       | IANA 时区 ID（如 `Asia/Shanghai`、`US/Eastern`）或 UTC 偏移（如 `+08:00`），用于在写入 Milvus Timestamptz 字段时解释不带时区的源端时间戳。未设置时回退到 JVM 默认时区。详见下方使用说明。 |

\* `field_name` 和 `source_field_name` 至少需提供一个。

### `timezone` 属性使用说明

`timezone` 仅在 **源端值本身不携带时区信息** 时需要配置。如果源端值已经带有时区，不要配置——现有的转换逻辑会正确处理。

| 源端类型 | 示例 | 是否带时区 | 是否配置 `timezone` |
|---|---|---|---|
| PostgreSQL `timestamp`（无时区） | `2024-01-02 08:00:00` | 否 | **需要** — 指定数据的实际时区 |
| PostgreSQL `timestamptz` | `2024-01-02 08:00:00+08` | 是 | **不需要** — 已携带偏移量 |
| MySQL `datetime` | `2024-01-02 08:00:00` | 否 | **需要** |
| ES `date`（epoch_millis 或带偏移） | `1704153600000` | 是 | **不需要** — 内部为 UTC |
| ES `date`（不带偏移的自定义格式） | `2024-01-02 08:00:00` | 否 | **需要**，仅当该字段所有值都不带偏移时 |

**注意：** 如果源端字段中混合存在带时区和不带时区的值（如 Elasticsearch `date` 字段使用多种格式），不要配置 `timezone`。现有的 systemDefault 转换机制会正确处理带时区的值；额外配置 `timezone` 会导致这些值被双重转换。

## CDC 写模式

当上游 source 输出 CDC changelog row 时，尤其是使用 `Milvus-CDC` source 时，需要设置 `write_mode = "CDC"`。

CDC 写模式有以下限制：

- `schema_save_mode` 必须为 `ERROR_WHEN_SCHEMA_NOT_EXIST`。启动 CDC 任务前需要先创建目标 collection schema。
- `data_save_mode` 必须为 `APPEND_DATA`。
- 不支持 `bulk_writer_config`。
- 目标 collection 开启 `autoID = true` 时，必须设置 collection property `allow_insert_auto_id = "true"`。Sink 在启动时读取目标端属性，缺省按 false 处理；该属性应配置在 Milvus collection 上，不是 sink 配置项，任务运行期间应保持不变。
- 目标 collection 必须只有一个主键字段。

如果 PostgreSQL CDC source 包含 PostGIS `GEOMETRY` 或 `GEOGRAPHY` 列，需要设置 `geometry_convert_mode = "parse"`。PostgreSQL CDC 会将这些值输出为 WKB/EWKB 十六进制字符串，Milvus sink 会将其转换为 WKT。目标 collection 中对应字段必须预先定义为 Milvus `GEOMETRY`。

默认情况下，CDC writer 接受 keyed changelog row，不要求事件时间元数据。无论目标是否开启 AutoID，INSERT 和 UPDATE_AFTER row 都**携带源主键调用 insert**，DELETE 和 UPDATE_BEFORE row 都按各自携带的主键删除。Sink 独立处理每条事件，不要求或校验 BEFORE/AFTER 是否成对；单独收到 UPDATE_BEFORE 就删除，UPDATE_AFTER 默认使用 insert，也可按下述策略使用完整行 upsert。使用 insert 时，需要替换旧行的源端应先发出对应的 DELETE 或 UPDATE_BEFORE；无论选择哪种写入策略，主键变化时都需要删除旧主键。

连续的相同目标操作、相同分区范围的 row 达到 `batch_size` 时会 flush；每次将 row 加入待写 batch 时还会检查 `cdc_batch_flush_interval_ms`，如果距上次成功 flush 已达到该间隔，则将当前 row 一并 flush。任何目标操作（包括 INSERT/UPSERT）或分区范围切换都会先 flush 上一个待写 batch。INSERT 或 UPDATE_AFTER 的主键已存在于待写 insert/upsert batch 中时，先同步 flush 旧 batch，再缓存新 row，使重复写入分别发送，避免覆盖待写记录。同一 batch 中的重复删除可以合并。从 DELETE 切换到 INSERT 或 UPSERT 时，当前写入也会立即 flush，避免旧行已经删除后新行仍等待后续消息或 checkpoint。从 INSERT 或 UPSERT 切换到 DELETE 时，当前 DELETE 仍按正常的 batch、interval、checkpoint 或 writer close 规则 flush。

启动任务前，在已创建的目标 collection 上开启属性，例如使用 PyMilvus：

```python
client.alter_collection_properties(
    collection_name="target_collection",
    properties={"allow_insert_auto_id": "true"},
)
```

CDC 在每个 sink 实例内使用一个 Collection 级 writer。连续的相同操作、相同分区范围的 row 组成批次；操作或分区范围变化时，先同步 flush 前一批。DELETE 未指定分区表示删除全部分区，明确指定 `_default` 则只删除默认分区。目标使用 Partition Key 时，忽略源端物理分区名，由 Milvus 根据数据中的分区键路由写入。普通命名分区只在写入时按需创建，删除不创建分区。仅在删除返回分区不存在错误、且再次确认目标命名分区确实不存在时，将该删除视为成功的空操作；正常删除不查询分区是否存在。APPEND 模式继续使用按分区划分的 writer。

CDC 要求 `multi_table_sink_replica = 1`（默认值）且实际 sink parallelism 为 1，其他值会被拒绝。并行度根据运行时 Context 校验，从 checkpoint 恢复时同样检查。Collection 级 writer 不保证不同 sink 实例或不同任务之间的顺序。

**恢复限制：** AutoID 和非 AutoID 目标的 CDC 写入都采用至少一次重放语义。Insert 不保证主键唯一性；请求结果不确定时的失败重试，或 checkpoint 恢复重放，都可能写入重复主键。遇到重复 INSERT 先 flush 是为了保留收到的事件，并不消除重试产生的重复。Flush 失败会向上传播并保留待写数据；下述 upsert 策略在其适用范围内覆盖重复主键；其余 insert 写入仍有上述重放限制。此模式不保证端到端精确一次。

`cdc_batch_flush_interval_ms` 是由新消息触发的检查，不是后台定时器。如果最后一条 row 进入 buffer 后不再有新 row，它仍会等待 checkpoint 或 writer close。数据库割接前应等待最后一次 checkpoint 成功，或者正常停止同步任务。事务 row 在 source 侧按源端事务 commit 位点推进 checkpoint，但目标 Milvus 仍按普通 INSERT、UPSERT 和 DELETE 操作写入，不提供目标端事务原子性。

### CDC Upsert 策略

`enable_upsert` 默认 `false`，仅在 `write_mode = "CDC"` 时生效。AutoID 和非 AutoID 目标设置为 `true` 后，对全部 INSERT/UPDATE_AFTER 使用完整行 upsert，包括常态写入。其优先级高于 `cdc_overlap_upsert_cutoff_ms`，不要求或校验事件时间元数据；如果配置了 cutoff，其值仍必须非负。APPEND writer 不受影响。

| CDC 配置 | 截止时间以内（含边界） | 截止时间以后 |
|-------------------|----------------------|--------------|
| `enable_upsert = false`，未配置 cutoff | insert | insert |
| `enable_upsert = false`，配置 cutoff | upsert | insert |
| `enable_upsert = true` | upsert | upsert |

非 AutoID 目标直接调用 Milvus 原生完整行 upsert。AutoID 目标在 upsert 内先删除本批主键，再携带原主键插入完整行，必须开启 `allow_insert_auto_id = "true"`；不调用会生成新主键的原生完整行 AutoID upsert。显式分区下两次请求作用于同一目标分区；partition-key collection 不指定分区，按主键跨分区删除。

Writer 先完成整批数据转换，再删除旧行。执行期间临时关闭 SDK 对单次请求的重试，按默认 SDK 重试策略在本地重试完整的 delete→insert 组合，退出时恢复 SDK 默认策略。每次重试都从 delete 重新开始，包括 insert 已成功但响应丢失的情况。两次请求都成功后才清空待写 upsert 批次，checkpoint 准备才能完成；不可重试的错误或重试耗尽后才向上传播，并保留整批数据。两次请求**不原子**：delete 与 insert 之间，或故障恢复成功前，查询可能暂时缺少这些行。必须保持同主键写入顺序，并避免其他 writer 并发修改目标数据。

所有配置下 DELETE/UPDATE_BEFORE 都保持 delete，不合并或跳过 BEFORE/AFTER；删除后写入仍然是 delete 后执行 insert/upsert。常态 upsert 可减少重放产生的重复，但会增加写入开销，尤其是 Source 已将更新拆成 delete + insert 时。它按主键同步最新状态，不提供端到端精确一次，也不防止旧事件覆盖新状态。

### CDC 全量重叠 Upsert 窗口

当 `enable_upsert = false` 时，全量备份恢复后可以在 sink 配置固定时间上界，按主键覆盖与全量重叠的 CDC 数据：

```hocon
write_mode = "CDC"
schema_save_mode = "ERROR_WHEN_SCHEMA_NOT_EXIST"
data_save_mode = "APPEND_DATA"
enable_upsert = false
# 仅为示例：替换为备份重叠范围的 Unix 毫秒上界。
cdc_overlap_upsert_cutoff_ms = 1792454400000
```

- 上界必须是非负的 Unix **毫秒**时间戳，不是 Milvus hybrid timestamp，也不是 message ID。`enable_upsert = false` 时，不配置上界则保持全部 insert；`enable_upsert = true` 时此选项不参与写入方式选择，APPEND 模式也不受影响。
- `enable_upsert = false` 时，AutoID 和非 AutoID 目标中，INSERT / UPDATE_AFTER 的源事件时间 **小于或等于** 上界使用完整行 upsert，否则使用 insert。DELETE / UPDATE_BEFORE 保持原逻辑，不要求事件时间。
- Writer 读取 row 元数据中的 `EventTime`，要求为非负 `Long`。Milvus-CDC 已从 WAL 消息 timetick 提取物理毫秒并填入该值；它不是业务数据字段、Source 接收时间或 Sink 当前时间。启用窗口判断时，事件时间缺失或非法会在修改待写批次前报错。其他 Source/Transform 只有在提供并保留该元数据中的源事件时间时才应使用此选项。
- `autoID = true` 时仍必须开启 `allow_insert_auto_id = "true"`。窗口内使用上述先 delete 再 insert 的 upsert 实现，窗口外仍使用普通 insert。AutoID 目标在窗口模式下同样要求有效的事件时间元数据。
- 控制面应选择覆盖备份全部数据的时间上界，随任务配置持久化，并在恢复时保持不变。优先使用源端备份水位转换后的毫秒值；使用控制面备份完成时间时需考虑时钟偏差。VTS 不会自动计算或保存新的上界。必须先完成全量恢复/导入，再应用 CDC。
- 每条 row 独立判断，即使另一个 channel 已收到超过上界的事件，后续到达的旧事件仍按自身时间处理。操作切换先 flush 前一批，同主键写入仍拆为有序请求。Flush 失败保留待写数据及其写入方式以供重试。
- 窗口按主键同步最新状态，不保留源端相同主键的多条记录。它不跳过 CDC 记录，也不改变启动/checkpoint 位点。必须保留源事件顺序：服务端 upsert 不比较源时间，旧事件晚到仍可能覆盖新状态。窗口外的 insert 重试/恢复仍可能重复，不提供目标端事务或端到端精确一次。

## 任务示例

### 基础用法

```bash
sink {
  Milvus {
    url = "http://127.0.0.1:19530"
    token = "username:password"
    batch_size = 1000
  }
}
```

### 使用 field_schema 和按字段时区配置

```bash
sink {
  Milvus {
    url = "http://127.0.0.1:19530"
    token = "username:password"
    database = "default"
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    field_schema = [
      {field_name = "id", data_type = 5, is_primary_key = true}
      {field_name = "title", data_type = 21, max_length = 512}
      {field_name = "created_at", data_type = 26, is_nullable = true, timezone = "Asia/Shanghai"}
      {field_name = "embedding", data_type = 101, dimension = 768}
    ]
  }
}
```

### Milvus CDC 到 Milvus

```bash
source {
  "Milvus-CDC" {
    url = "http://127.0.0.1:19530"
    token = "root:Milvus"
    database_collections = {
      "default" = ["source_collection"]
    }
    startup_mode = "cdc"
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
