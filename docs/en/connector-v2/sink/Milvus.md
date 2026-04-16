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
| enable_auto_id       | boolean | No       | false                        | Primary key column enable autoId.                         |
| enable_upsert        | boolean | No       | false                        | Upsert data not insert.                                   |
| enable_dynamic_field | boolean | No       | true                         | Enable create table with dynamic field.                   |
| batch_size           | int     | No       | 1000                         | Write batch size.                                         |
| partition_key        | String  | No       |                              | Milvus partition key field                                |
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

## Changelog

<ChangeLog />