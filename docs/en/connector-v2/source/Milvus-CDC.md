import ChangeLog from '../changelog/connector-cdc-milvus.md';

# Milvus CDC

> Milvus CDC source connector

## Description

The Milvus CDC source connector reads Milvus WAL messages through Milvus `DumpMessages` and outputs changelog rows. It is intended for realtime Milvus-to-Milvus synchronization together with the Milvus sink `write_mode = "CDC"`.

This connector currently starts from user-supplied pchannel positions. It does not perform an initial snapshot and runs continuously until the SeaTunnel job is canceled or stopped.

When using the Milvus CDC sink, set `multi_table_sink_replica = 1` and effective sink parallelism 1; the sink rejects other values. Each sink instance uses one collection-level CDC writer to preserve the received order across partition scopes, including deletes covering all partitions. This does not establish global order across multiple sink instances.

## Key Features

- [ ] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md) (Milvus sink CDC mode uses at-least-once writes)
- [ ] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)

## Requirements

- Milvus or Zilliz Cloud must expose the `DumpMessages` API used by Milvus CDC.
- Full CDC synchronization depends on Milvus engine support and requires Milvus 2.6.21 or later. Milvus 2.6.14 through 2.6.20 has a known limitation: `DumpMessages` cannot return the content of upsert transaction messages, so CDC streams containing upsert transactions are not fully supported.
- `channel_positions` must use physical pchannels, for example `by-dev-rootcoord-dml_0`. Do not use virtual channels with collection-id suffixes.
- Each physical pchannel must appear only once in `channel_positions`.
- Source collection schemas must remain unchanged after the job starts. Runtime schema changes such as adding, dropping, or renaming fields and changing field types are not supported yet; the source fails instead of applying schema evolution when subsequent DML no longer matches the startup schema.
- The target collection schema must already exist when using the Milvus sink CDC mode. Use `autoID = false`, or enable the target collection property `allow_insert_auto_id = "true"` when `autoID = true`. Both paths insert with source primary keys and have the replay limitations described below. See [Milvus sink CDC mode](../sink/Milvus.md#cdc-write-mode).

## Supported Message Types

| Milvus WAL message | SeaTunnel RowKind | Description |
|--------------------|-------------------|-------------|
| Insert             | INSERT            | Inserted with the source primary key for both AutoID and non-AutoID targets. |
| Delete             | DELETE            | Deleted by primary key by the Milvus sink CDC writer. |

Transaction control messages are consumed internally. When a Milvus transaction is split into multiple WAL messages, the source buffers DML rows until the transaction commit message is received, then emits the rows with the checkpoint offset advanced to the commit message. The sink may batch rows across original DML message boundaries according to its batch size, operation or partition-scope transitions, repeated insert keys, flush interval, and checkpoint lifecycle; Milvus sink CDC mode does not provide an atomic multi-operation transaction on the target side.

## Source Options

| Name                 | Type                         | Required | Default              | Description |
|----------------------|------------------------------|----------|----------------------|-------------|
| url                  | String                       | Yes      | -                    | Milvus or Zilliz Cloud endpoint. |
| token                | String                       | Yes      | -                    | Milvus authentication token, usually `username:password`. |
| database_collections | Map&lt;String, List&lt;String&gt;&gt; | Yes      | -                    | Source database to collection list mapping. Wildcard collection capture is not supported. |
| channel_positions    | List&lt;Map&gt;               | Yes      | -                    | Per-pchannel start position. |
| message_types        | List&lt;String&gt;            | No       | `["insert","delete"]` | WAL message types to output. Currently supports `insert` and `delete`. |
| startup_mode         | Enum                         | No       | `CDC`                | Startup mode. Currently only `CDC` is supported. |
| queue_capacity       | Integer                      | No       | 16                   | Buffered WAL message count per reader. A small value is recommended; increasing it does not fix a slow downstream sink and may increase memory usage for large messages. |
| client_pem_path      | String                       | No       | -                    | Client certificate PEM path for TLS. |
| client_key_path      | String                       | No       | -                    | Client private key path for TLS. |
| ca_pem_path          | String                       | No       | -                    | Trusted certificate PEM path. On its own, enables one-way TLS with custom trust; with both client_pem_path and client_key_path, enables mutual TLS. Client certificate/key options require all three files. |
| server_name          | String                       | No       | -                    | Server name used by TLS verification. With CA-only TLS, defaults to the URL hostname; override only to match the trusted server certificate. |

## Channel Position

Each `channel_positions` item describes one physical pchannel.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| pchannel | String | Yes | Physical Milvus pchannel, such as `by-dev-rootcoord-dml_0`. |
| start | Object | Yes | Start offset for `DumpMessages`. |

The `start` object supports:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| wal_name | String | Yes | WAL name, such as `RocksMQ` or `Pulsar`. |
| resume_message_id | String | Yes | Message id sent to Milvus as `start_message_id`. Prefer the Milvus last confirmed message id, for example the `_lc` / `LastConfirmedMessageID` returned by FlushAll messages. |
| consumed_message_id | String | No | Last consumed message id. It is normally managed by checkpoint state. Use it only when manually restoring from a known consumed message. |
| timetick | Long | Yes | Milvus hybrid timestamp used with the start message id. |

For compatibility, `message_id` is accepted as `resume_message_id`, and `last_confirmed_message_id` is accepted as the legacy resume field.

For RocksMQ, the message id must be the base36 text offset expected by Milvus CDC, for example `-1`. Do not copy escaped binary `msgID` bytes from Milvus logs.

## Checkpoint, Offset, And Consistency Semantics

The source emits a whole WAL message before advancing the split offset. This prevents a checkpoint from moving past a partially emitted message.

For transactional messages, the source advances the checkpoint offset to the transaction commit message after the transaction rows are emitted. This preserves source-side recovery semantics.

When used with the Milvus sink CDC mode, INSERT rows are inserted with source primary keys and DELETE rows are deleted by primary key, for both AutoID and non-AutoID targets. The sink uses at-least-once replay semantics: a request with an unknown outcome or checkpoint recovery may insert duplicate primary keys. Source-side checkpoint offset advancement does not make target inserts idempotent. This mode does not guarantee exactly-once final state or target-side atomicity for multi-operation transactions.

## Metrics

| Metric | Description |
|--------|-------------|
| MilvusCdcSourceReceiveDelayMs | Max per-split delay between source receive wall time and the latest received event timestamp. Updated when new WAL messages are received. |
| MilvusCdcSourceReceiveToCommitDelayMs | Max per-split gap between latest received event timestamp and latest checkpoint-completed commit timestamp. |
| MilvusCdcSourceLastCommitTsAllSplitsMinMs | Minimum committed event timestamp in milliseconds across all assigned splits. |
| MilvusCdcSourceLastCommitTsAllSplitsMaxMs | Maximum committed event timestamp in milliseconds across all assigned splits. |

If there are no new WAL messages, these metrics are not advanced by a background timer. Use the commit timestamp metrics to detect whether the committed watermark is moving.

## Task Example

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

## Changelog

<ChangeLog />
