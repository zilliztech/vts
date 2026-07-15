import ChangeLog from '../changelog/connector-milvus.md';

# Milvus

> Milvus sink connector

## Description

This Milvus sink connector write data to Milvus or Zilliz Cloud, it has the following features:
- support read and write data by partition
- support write dynamic schema data from Metadata Column
- json data will be converted to json string and sink as json as well
- retry automatically to bypass ratelimit and grpc limit
## Key Features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)

## Data Type Mapping

|  Milvus Data Type   | SeaTunnel Data Type |
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

## Sink Options

|         Name         | Type    | Required |           Default            | Description                                               |
|----------------------|---------|----------|------------------------------|-----------------------------------------------------------|
| url                  | String  | Yes      | -                            | The URL to connect to Milvus or Zilliz Cloud.             |
| token                | String  | Yes      | -                            | User:password                                             |
| database             | String  | No       | -                            | Write data to which database, default is source database. |
| schema_save_mode     | enum    | No       | CREATE_SCHEMA_WHEN_NOT_EXIST | Auto create table when table not exist.                   |
| data_save_mode       | enum    | No       | APPEND_DATA                  | Data save mode. In CDC write mode only `APPEND_DATA` is supported. |
| create_index         | boolean | No       | true                         | Create indexes when auto creating the collection.         |
| enable_auto_id       | boolean | No       | false                        | Primary key column enable autoId.                         |
| enable_upsert        | boolean | No       | false                        | Upsert data not insert.                                   |
| enable_dynamic_field | boolean | No       | true                         | Enable create table with dynamic field.                   |
| batch_size           | int     | No       | 1000                         | Write batch size.                                         |
| cdc_batch_flush_interval_ms | long | No       | 1000                         | In CDC mode, flush pending rows when a newly received row observes that this interval has elapsed since the last successful flush. |
| write_mode           | enum    | No       | APPEND                       | Write mode. `APPEND` writes normal insert rows by batch or bulk writer. `CDC` upserts INSERT and UPDATE_AFTER rows, deletes DELETE rows, and ignores UPDATE_BEFORE rows. |
| partition_key        | String  | No       |                              | Milvus partition key field                                |
| partition_num        | int     | No       |                              | Number of partitions passed to Milvus create collection request. Currently used by Milvus partition key mode. |
| collection_rename    | Map     | No       | {}                           | Rename collections: `{source_name = "target_name"}`       |
| field_schema         | List    | No       | []                           | Per-field schema configuration. See below.                |

## Field Schema

When `field_schema` is supplied, only the fields defined in it will be written. If empty, the full source schema is used.

Each field object supports:

| Property           | Type    | Required | Description                                                                 |
|--------------------|---------|----------|-----------------------------------------------------------------------------|
| field_name         | String  | Yes*     | Target field name in Milvus collection.                                     |
| source_field_name  | String  | Yes*     | Source field name. If both are provided, `field_name` is the target name.   |
| data_type          | Integer | Yes      | Milvus data type code (e.g. Int64=5, VarChar=21, FloatVector=101, Timestamptz=26). |
| is_primary_key     | Boolean | No       | Mark as primary key.                                                        |
| auto_id            | Boolean | No       | Enable auto ID for primary key.                                             |
| dimension          | Integer | No       | Required for vector types.                                                  |
| max_length         | Integer | No       | Max length for VarChar fields.                                              |
| element_type       | Integer | No       | Element type for Array fields.                                              |
| max_capacity       | Integer | No       | Max capacity for Array fields. Default: 4096.                               |
| is_nullable        | Boolean | No       | Whether the field is nullable.                                              |
| is_partition_key   | Boolean | No       | Mark as partition key.                                                      |
| timezone           | String  | No       | IANA timezone ID (e.g. `Asia/Shanghai`, `US/Eastern`) or UTC offset (e.g. `+08:00`) for interpreting tz-naive source timestamps when writing to Milvus Timestamptz fields. If not set, falls back to JVM default timezone. See usage guidance below. |

\* At least one of `field_name` or `source_field_name` is required.

### When to use the `timezone` property

The `timezone` property is only needed when the **source value does not carry timezone information**. If the source value already has a timezone, do not configure it — the existing conversion handles it correctly.

| Source type | Example | Has timezone? | Configure `timezone`? |
|---|---|---|---|
| PostgreSQL `timestamp` (without tz) | `2024-01-02 08:00:00` | No | **Yes** — specify the intended timezone |
| PostgreSQL `timestamptz` | `2024-01-02 08:00:00+08` | Yes | **No** — already carries offset |
| MySQL `datetime` | `2024-01-02 08:00:00` | No | **Yes** |
| ES `date` (epoch_millis or with offset) | `1704153600000` | Yes | **No** — internally UTC |
| ES `date` (custom format without offset) | `2024-01-02 08:00:00` | No | **Yes**, only if ALL values in this field lack offset |

**Warning:** If a source field contains a mix of values with and without timezone information (e.g. Elasticsearch `date` with multiple formats), do not configure `timezone`. The existing systemDefault-based conversion handles the timezone-aware values correctly; adding a `timezone` override would cause double-conversion for those values.

## CDC Write Mode

Set `write_mode = "CDC"` when the upstream source emits CDC changelog rows, especially when using the `Milvus-CDC` source.

CDC write mode has the following constraints:

- `schema_save_mode` must be `ERROR_WHEN_SCHEMA_NOT_EXIST`. Create the target collection schema before starting the CDC job.
- `data_save_mode` must be `APPEND_DATA`.
- `bulk_writer_config` is not supported.
- The target collection must not use autoID.
- The target collection must have exactly one primary key field.

The CDC writer accepts generic keyed changelog rows and does not require `Milvus-CDC`-specific message metadata. INSERT and UPDATE_AFTER rows are applied by upsert, DELETE rows are applied by primary key delete, and UPDATE_BEFORE rows are ignored. The Milvus primary key must correspond to a stable source CDC key; a primary-key change must be emitted by the source as DELETE for the old key followed by INSERT for the new key. Duplicate primary keys within one upsert batch keep the last row. Consecutive rows with the same target operation are flushed when they reach `batch_size`. Whenever a row is added to the pending batch, the writer also checks `cdc_batch_flush_interval_ms`; when the interval since the last successful flush has elapsed, it flushes the pending batch including the current row. Any target-operation change flushes the previous pending batch. When switching from DELETE to upsert, the current upsert is also flushed immediately so the new key does not remain pending after the old key has been deleted. When switching from upsert to DELETE, the current DELETE continues to follow the normal batch, interval, checkpoint, or writer-close flush rules.

`cdc_batch_flush_interval_ms` is a write-triggered check, not a background timer. If no new row arrives after the last buffered row, that row remains pending until a checkpoint or writer close. Before a database cutover, wait for a final successful checkpoint or stop the synchronization job gracefully. Transaction rows are checkpointed by the source at the source transaction commit offset, but the target Milvus write is still applied as normal INSERT and DELETE operations; it is not a target-side atomic transaction.

## Task Example

### Basic

```bash
sink {
  Milvus {
    url = "http://127.0.0.1:19530"
    token = "username:password"
    batch_size = 1000
  }
}
```

### With field_schema and per-field timezone

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

### Milvus CDC to Milvus

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

## Changelog

<ChangeLog />
