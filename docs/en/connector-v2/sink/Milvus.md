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
- [x] [exactly-once](../../concept/connector-v2-features.md) (excludes CDC mode)
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
| enable_upsert        | boolean | No       | false                        | CDC mode only: use full-row upsert for INSERT/UPDATE_AFTER regardless of the overlap cutoff. AutoID targets implement upsert as delete then insert with the original keys; APPEND mode is unaffected.                                   |
| enable_dynamic_field | boolean | No       | true                         | Enable create table with dynamic field.                   |
| batch_size           | int     | No       | 1000                         | Write batch size.                                         |
| cdc_batch_flush_interval_ms | long | No       | 1000                         | In CDC mode, flush pending rows when a newly received row observes that this interval has elapsed since the last successful flush. |
| cdc_overlap_upsert_cutoff_ms | long | No | - | Optional inclusive CDC upsert cutoff in Unix milliseconds, applied when `enable_upsert = false`. INSERT/UPDATE_AFTER rows at or before this source event time use upsert, including AutoID targets. |
| write_mode           | enum    | No       | APPEND                       | Write mode. `APPEND` writes normal insert rows by batch or bulk writer. `CDC` inserts INSERT and UPDATE_AFTER rows with source primary keys by default (configurable through `enable_upsert` or the optional upsert window), and deletes DELETE and UPDATE_BEFORE rows. AutoID targets require `allow_insert_auto_id = "true"`. |
| geometry_convert_mode | String | No       | passthrough                  | How to handle String values for Milvus Geometry fields. `passthrough` sends the value unchanged; `parse` converts WKT/EWKT/GeoJSON/WKB hex and supported coordinate formats to WKT. |
| geometry_string_coord_order | String | No | lat_lon                     | Coordinate order for bare numeric pair strings in `parse` mode. Supported values are `lat_lon` and `lon_lat`. WKT, GeoJSON and WKB inputs are unaffected. |
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
- A target collection with `autoID = true` must have the collection property `allow_insert_auto_id = "true"`. The sink reads this property from the target at startup; a missing property is treated as false. Configure it on Milvus, not as a sink option, and keep it unchanged while the job runs.
- The target collection must have exactly one primary key field.

For PostgreSQL CDC sources containing PostGIS `GEOMETRY` or `GEOGRAPHY` columns, set `geometry_convert_mode = "parse"`. PostgreSQL CDC emits these values as WKB/EWKB hexadecimal strings, and the Milvus sink converts them to WKT. The target collection must already define the corresponding field as Milvus `GEOMETRY`.

By default, the CDC writer accepts keyed changelog rows without event-time metadata. For both AutoID and non-AutoID targets, INSERT and UPDATE_AFTER rows use **insert with the source primary key**, while DELETE and UPDATE_BEFORE rows delete the supplied key. The sink applies each event independently and does not require or validate BEFORE/AFTER pairs. A standalone UPDATE_BEFORE deletes. UPDATE_AFTER uses insert by default, or full-row upsert under the strategy below. When using insert, replacing existing rows requires a preceding DELETE or UPDATE_BEFORE. Primary-key changes require deleting the old key regardless of the write strategy.

Consecutive rows with the same target operation and partition scope are flushed when they reach `batch_size`. Whenever a row is added to the pending batch, the writer also checks `cdc_batch_flush_interval_ms`; when the interval since the last successful flush has elapsed, it flushes the pending batch including the current row. Any target-operation (including INSERT/UPSERT) or partition-scope change flushes the previous pending batch. An INSERT or UPDATE_AFTER whose primary key already exists in the pending insert/upsert batch synchronously flushes that batch before buffering the new row, so repeated writes are preserved in separate requests instead of overwriting a pending row. Duplicate deletes within the same batch may be combined. When switching from DELETE to INSERT or UPSERT, the current write is also flushed immediately so the new row does not remain pending after the old row has been deleted. When switching from INSERT or UPSERT to DELETE, the current DELETE follows the normal batch, interval, checkpoint, or writer-close flush rules.

Enable the property on the pre-created target before starting the job, for example with PyMilvus:

```python
client.alter_collection_properties(
    collection_name="target_collection",
    properties={"allow_insert_auto_id": "true"},
)
```

CDC uses one collection-level writer per sink instance. It batches consecutive rows with the same operation and partition scope, and synchronously flushes before either changes. An unspecified partition on DELETE means all partitions; an explicit `_default` means only the default partition. For partition-key targets, source physical partition names are ignored and Milvus routes inserts using the partition-key field. Named partitions are created on demand for writes, not deletes. A partition-not-found delete error is treated as a successful no-op only after confirming that the named target partition does not exist; normal deletes do not query partition existence. APPEND mode continues to use partition-level writers.

CDC requires `multi_table_sink_replica = 1` (the default) and effective sink parallelism 1; other values are rejected. Parallelism is checked against the runtime context, including on checkpoint restore. A collection-level writer does not provide ordering across separate sink instances or jobs.

**Recovery limitation:** CDC writes use at-least-once replay semantics for both AutoID and non-AutoID targets. Insert does not enforce primary-key uniqueness; a failed request with an unknown outcome or checkpoint replay can insert duplicate primary keys. Flushing before a repeated insert preserves received events but does not deduplicate retries. Failed flushes propagate errors and retain pending rows. The upsert strategies below replace repeated keys within their scope; all remaining insert writes retain these replay limitations. This mode does not guarantee end-to-end exactly-once semantics.

`cdc_batch_flush_interval_ms` is a write-triggered check, not a background timer. If no new row arrives after the last buffered row, that row remains pending until a checkpoint or writer close. Before a database cutover, wait for a final successful checkpoint or stop the synchronization job gracefully. Transaction rows are checkpointed by the source at the source transaction commit offset, but the target Milvus write is still applied as normal INSERT, UPSERT and DELETE operations; it is not a target-side atomic transaction.

### CDC upsert strategy

`enable_upsert` defaults to `false` and only applies to `write_mode = "CDC"`. For both AutoID and non-AutoID targets, set it to `true` to use full-row upsert for every INSERT/UPDATE_AFTER row, including steady-state writes. This takes precedence over `cdc_overlap_upsert_cutoff_ms` and does not require or validate event-time metadata. Any configured cutoff must still be non-negative. APPEND writers are unaffected.

| CDC configuration | At or before cutoff | After cutoff |
|-------------------------|---------------------|--------------|
| `enable_upsert = false`, no cutoff | insert | insert |
| `enable_upsert = false`, with cutoff | upsert | insert |
| `enable_upsert = true` | upsert | upsert |

For non-AutoID targets, upsert calls the native Milvus full-row upsert API. For AutoID targets, upsert instead deletes the supplied primary keys and then inserts the full rows with their original keys; `allow_insert_auto_id = "true"` is required. Native full-row AutoID upsert is not used because it generates new primary keys. Explicit partitions scope both calls to the same target partition; partition-key collections omit the explicit partition and delete the supplied keys across partitions.

The writer converts the entire batch before deleting any rows. It temporarily disables SDK retries for individual calls and retries the complete delete-then-insert operation locally using the default SDK retry policy, then restores the SDK defaults on exit. Every retry starts from delete, including when insert may have succeeded but its response was lost. Both calls must succeed before the pending upsert batch is cleared and checkpoint preparation can complete. A non-retryable failure or exhausted retries propagates and retains the whole batch. These two calls are **not atomic**: rows may be temporarily absent between delete and insert, or until recovery succeeds. Keep writes for the same key ordered and avoid concurrent writers to the target.

DELETE/UPDATE_BEFORE remain deletes in every configuration. BEFORE/AFTER operations are not merged or skipped; a delete followed by a write becomes delete followed by insert/upsert. Always using upsert can reduce replay duplicates but adds write overhead, particularly when the source already emits delete-plus-insert updates. It synchronizes the latest state per key, does not provide end-to-end exactly-once, and does not prevent older events from overwriting newer state.

### CDC overlap upsert window

With `enable_upsert = false`, after restoring a full backup, configure a fixed cutoff on the sink to replace overlapping CDC rows by primary key:

```hocon
write_mode = "CDC"
schema_save_mode = "ERROR_WHEN_SCHEMA_NOT_EXIST"
data_save_mode = "APPEND_DATA"
enable_upsert = false
# Example only: replace with the backup's overlap upper bound in Unix milliseconds.
cdc_overlap_upsert_cutoff_ms = 1792454400000
```

- The cutoff is a non-negative Unix timestamp in **milliseconds**, not a Milvus hybrid timestamp or a message ID. With `enable_upsert = false`, omit it to retain insert-only behavior. It has no effect on write selection when `enable_upsert = true`, or in APPEND mode.
- For `enable_upsert = false`, on both AutoID and non-AutoID targets, INSERT and UPDATE_AFTER use full-row upsert when the row's source event time is **less than or equal to** the cutoff, otherwise insert. DELETE and UPDATE_BEFORE keep their existing behavior and do not require an event time.
- The writer reads the `EventTime` row metadata as a non-negative `Long`. Milvus-CDC already populates it from the WAL message timetick's physical milliseconds. It is not a payload field, source receive time, or sink wall clock. When the window applies, missing or invalid event time fails the write before changing the pending batch. Other sources/transforms must supply and preserve source event time in this metadata before using the option.
- For `autoID = true`, `allow_insert_auto_id = "true"` is still required. Rows inside the window use the delete-then-insert upsert implementation above; rows outside use plain insert. Window-based selection requires valid event-time metadata for AutoID targets too.
- The control plane must choose an upper bound covering all data included in the backup, persist it with the job configuration, and reuse it unchanged after recovery. Prefer a source-derived backup watermark converted to milliseconds; if using control-plane backup completion time, account for clock skew. VTS does not derive or persist a new cutoff automatically. Complete the full restore/import before applying CDC.
- Evaluate every row independently, including older events received after newer events from another channel. Operation changes flush the previous batch; repeated write keys remain in separate ordered requests. A failed flush retains both its rows and selected write operation for retry.
- This window synchronizes the latest state per primary key, rather than preserving multiple source rows sharing a key. It does not skip CDC records or alter startup/checkpoint positions. Preserve source order: upsert does not compare source timestamps on the server, and late older writes may overwrite newer state. Requests outside the window may still insert duplicates after retry/recovery. Target-side transactions and end-to-end exactly-once are not provided.

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
