import ChangeLog from '../changelog/connector-chromadb.md';

# ChromaDB

> ChromaDB source connector

[ChromaDB](https://www.trychroma.com/) is an open-source AI-native vector database.

This connector reads data from ChromaDB collections via the REST API (v2).

## Key Features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md) (within and across collections)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Options

| Name              | Type   | Required | Default          |
|-------------------|--------|----------|------------------|
| url               | string | Yes      | -                |
| tenant            | string | No       | default_tenant   |
| database          | string | No       | default_database |
| collections       | list   | Yes      | -                |
| auth_type         | string | No       | BEARER           |
| token             | string | No       | ""               |
| username          | string | No       | ""               |
| password          | string | No       | ""               |
| batch_size        | int    | No       | 1000             |
| id_split_size     | int    | No       | 10000000         |
| connect_timeout   | int    | No       | 30               |
| read_timeout      | int    | No       | 600              |
| common-options    |        | No       | -                |

### url [string]

ChromaDB server URL, e.g. `http://localhost:8000`.

### tenant [string]

ChromaDB tenant name.

### database [string]

ChromaDB database name.

### collections [list]

List of collection names to read.

### auth_type [string]

Authentication type:
- `BEARER`: Uses `Authorization: Bearer <token>` header
- `X_CHROMA_TOKEN`: Uses `X-Chroma-Token: <token>` header
- `BASIC`: Uses `Authorization: Basic base64(username:password)` header

### token [string]

Token for `BEARER` or `X_CHROMA_TOKEN` auth.

### username [string]

Username for `BASIC` auth.

### password [string]

Password for `BASIC` auth.

### batch_size [int]

Maximum number of IDs per request for full records. Must be positive and within the server's request limits. Configure this independently from `id_split_size`; the connector does not detect or automatically adjust server quotas.

### id_split_size [int]

Maximum number of IDs per scan request inside a reader range. Must be positive. The default is 10000000 (10M). Reduce it to respect the server's query limit and the available reader/checkpoint memory. This controls page size, not the number of splits or readers.

### connect_timeout [int]

HTTP connect timeout in seconds.

### read_timeout [int]

HTTP read timeout in seconds. Increase for very large collections.

### Common Options

Source plugin common parameters, please refer to [Source Common Options](../source-common-options.md) for details. `parallelism` selects the number of source readers and defaults to `env.parallelism`. All configured collections share these readers; adding a collection does not create another reader pool.

## Data Read Strategy

For each collection, the enumerator calls `count` once to obtain `N` and creates up to `P` nonempty ranges, where `P` is the source parallelism. Range `i` is `[floor(N*i/P), floor(N*(i+1)/P))`. These are offset ranges, not ID value ranges.

Each reader processes its assigned ranges using the same two-phase loop:

1. **ID scan**: Request `include=[]`, `offset=next_offset`, and `limit=min(id_split_size, end_offset-next_offset)`.
2. **Full-record fetch**: Fetch embeddings, documents, uris and metadatas for the returned IDs in chunks of at most `batch_size`. Advance progress after each successfully emitted batch, then scan the next ID page.

Pages stay inside the reader's range; they do not create or distribute new splits. `parallelism=1` uses this same loop with one range `[0, N)`. If the server permits it, setting `id_split_size >= N` fetches all IDs in one request before full-record batches are read.

Full-record fetches use explicit IDs and do not use offset pagination. A larger ID page reduces the number of offset scans compared with paging full records at `batch_size`. The ID scan itself still incurs offset costs; performance depends on the server, page sizes and concurrency.

### Checkpoint and recovery

Checkpoints retain unassigned ranges in the enumerator and each reader's range boundaries, next offset and unconsumed IDs. Recovery keeps these boundaries and resumes pending IDs before scanning another page. Collections whose ranges have already been created are not counted or split again, even if parallelism changes. A restored job with fewer ranges than readers may therefore leave some readers idle.

An incomplete ID page or missing requested records fails the job instead of silently completing the range. This detects some source changes or incorrectly sized requests, but it cannot detect all duplicates or omissions caused by concurrent mutations. Records emitted after the last completed checkpoint may be replayed; exactly-once delivery is not guaranteed.

The range-based checkpoint format is incompatible with the previous collection-only format. Start a fresh migration when upgrading from that version instead of restoring its checkpoint/savepoint.

## Output Column Schema

The ChromaDB source produces a **fixed 5-column schema** for every collection:

| Column Name | Type | Nullable | Description |
|-------------|------|----------|-------------|
| `id` | STRING | No | ChromaDB record ID (primary key) |
| `embedding` | FLOAT_VECTOR | No | Vector embedding, dimension from collection metadata |
| `document` | STRING | Yes | Document text |
| `uri` | STRING | Yes | Resource URI |
| `Metadata` | STRING (JSON) | No | All user-defined metadata serialized as JSON. The `Metadata` column is tagged with `metadata=true`, allowing sink connectors to expand fields accordingly |

:::caution Warning
Empty collections (no records inserted, dimension unknown) will cause the job to fail with an error. ChromaDB only determines the vector dimension after the first embedding is inserted, so a target schema cannot be created for empty collections. Make sure all specified collections contain data.
:::

:::tip Tips
**Read-only**: The connector reads collection metadata, record counts and records. It never modifies or deletes source data.

**Memory usage**: Each active reader retains up to `id_split_size` IDs, bounded by its remaining range. Unconsumed IDs are also included in checkpoints. Account for the combined memory of readers running in the same VTS process when increasing parallelism or page size.

**Query pressure**: Increasing parallelism allows concurrent reads within one collection as well as across collections. Select a value that the source service and sink can sustain, and respect the source's request-rate limits.
:::

:::caution Warning
**Data consistency**: This connector uses `count` and offset pagination without a shared snapshot. Writes or deletes during enumeration, reading or recovery may shift offsets and cause missing or duplicate records; `count` does not fix the dataset. Even a single request for all IDs does not freeze subsequent record values or prevent those records from being deleted. For complete migration, keep the source unchanged until the job finishes, including recovery. If the source service provides an isolated fork or snapshot, you can prepare one and configure the connector to read it; the connector does not create one automatically.
:::

## Examples

### Example 1: Read Specific Collections

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  ChromaDB {
    url = "http://localhost:8000"
    collections = ["my_collection"]
  }
}
```

### Example 2: Bearer Token Auth

```hocon
source {
  ChromaDB {
    url = "https://my-chroma-server:8000"
    token = "my-secret-token"
    auth_type = "BEARER"
    collections = ["secure_collection"]
  }
}
```

### Example 3: Basic Auth

```hocon
source {
  ChromaDB {
    url = "https://my-chroma-server:8000"
    auth_type = "BASIC"
    username = "admin"
    password = "my-password"
    collections = ["secure_collection"]
  }
}
```

### Example 4: Tuning Read Parameters

For large collections, adjust batch and timeout parameters:

```hocon
source {
  ChromaDB {
    url = "http://localhost:8000"
    collections = ["large_collection"]
    batch_size = 2000
    id_split_size = 5000000
    read_timeout = 1200
  }
}
```

### Example 5: Parallel Reads with Small Request Limits

For a service whose configured limits allow at most 300 records per request, set both page sizes explicitly. VTS does not identify the deployment type or impose this value:

```hocon
env {
  parallelism = 4
  job.mode = "BATCH"
}

source {
  ChromaDB {
    url = "https://my-chroma-server"
    collections = ["large_collection"]
    id_split_size = 300
    batch_size = 300
  }
}
```

## Changelog

<ChangeLog />
