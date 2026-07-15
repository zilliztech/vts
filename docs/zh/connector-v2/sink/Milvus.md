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
- [x] [exactly-once](../../concept/connector-v2-features.md)
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
| enable_upsert        | boolean | 否       | false                        | 是否启用upsert。                                   |
| enable_dynamic_field | boolean | 否       | true                         | 是否启用带动态字段的创建表。                   |
| batch_size           | int     | 否       | 1000                         | 写入批大小。                                         |
| cdc_batch_flush_interval_ms | long | 否       | 1000                         | CDC 模式下，收到新 row 时，如果距上次成功 flush 已达到该间隔，则 flush 未完成批次。单位为毫秒。 |
| write_mode           | enum    | 否       | APPEND                       | 写入模式。`APPEND` 使用普通 batch 或 bulk writer 写入 insert row；`CDC` 对 INSERT 和 UPDATE_AFTER 执行 upsert，对 DELETE 执行删除，并忽略 UPDATE_BEFORE。 |
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
- 目标 collection 不能启用 autoID。
- 目标 collection 必须只有一个主键字段。

如果 PostgreSQL CDC source 包含 PostGIS `GEOMETRY` 或 `GEOGRAPHY` 列，需要设置 `geometry_convert_mode = "parse"`。PostgreSQL CDC 会将这些值输出为 WKB/EWKB 十六进制字符串，Milvus sink 会将其转换为 WKT。目标 collection 中对应字段必须预先定义为 Milvus `GEOMETRY`。

CDC writer 接受通用的 keyed changelog row，不依赖 `Milvus-CDC` 私有消息元数据。INSERT 和 UPDATE_AFTER row 会按 upsert 写入，DELETE row 会按主键删除，UPDATE_BEFORE row 会被忽略。Milvus 主键必须对应稳定的源 CDC key；源端主键变化必须由 reader 表达为旧主键的 DELETE，随后是新主键的 INSERT。同一个 upsert batch 内的重复主键保留最后一条。连续的相同目标操作达到 `batch_size` 时会 flush；每次将 row 加入待写 batch 时还会检查 `cdc_batch_flush_interval_ms`，如果距上次成功 flush 已达到该间隔，则将当前 row 一并 flush。任何目标操作切换都会先 flush 上一个待写 batch；从 DELETE 切换到 upsert 时，当前 upsert 也会立即 flush，避免旧主键已经删除后新主键仍等待后续消息或 checkpoint。从 upsert 切换到 DELETE 时，当前 DELETE 仍按正常的 batch、interval、checkpoint 或 writer close 规则 flush。

`cdc_batch_flush_interval_ms` 是由新消息触发的检查，不是后台定时器。如果最后一条 row 进入 buffer 后不再有新 row，它仍会等待 checkpoint 或 writer close。数据库割接前应等待最后一次 checkpoint 成功，或者正常停止同步任务。事务 row 在 source 侧按源端事务 commit 位点推进 checkpoint，但目标 Milvus 仍按普通 INSERT 和 DELETE 操作写入，不提供目标端事务原子性。

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
