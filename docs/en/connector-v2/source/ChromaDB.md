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
- [x] [parallelism](../../concept/connector-v2-features.md) (across multiple collections)
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

Batch size for fetching full records by ID list. Limited by ChromaDB's underlying SQLite `SQLITE_MAX_VARIABLE_NUMBER` (currently 32766); do not exceed this value.

### id_split_size [int]

Number of IDs to fetch per scan pass. Default 10000000 (10M), large enough for most collections. Reduce if memory is constrained.

### connect_timeout [int]

HTTP connect timeout in seconds.

### read_timeout [int]

HTTP read timeout in seconds. Increase for very large collections.

### Common Options

Source plugin common parameters, please refer to [Source Common Options](../source-common-options.md) for details.

## Data Read Strategy

The connector uses a two-phase read approach:

1. **Phase 1 (ID Scan)**: Fetch all record IDs using offset-based pagination with `include=[]`, fetching `id_split_size` IDs per pass
2. **Phase 2 (Batch Fetch)**: For each ID batch, fetch full record data (embeddings, documents, uris, metadatas) in chunks of `batch_size`

This avoids the performance degradation of deep offset pagination.

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
**Read-only**: The connector only performs query operations on ChromaDB (GET collection info + POST get records). It never modifies or deletes source data.

**Memory usage**: `id_split_size` defaults to 10M, loading that many ID strings into VTS process memory at once. For very large collections with limited VTS memory, reduce this parameter.

**Query pressure on ChromaDB**: ChromaDB is a single-node SQLite-based system. Reading within a single collection is sequential. When `parallelism > 1` with multiple collections, concurrent reads will put pressure on ChromaDB. Keep `parallelism = 1` when migrating large volumes, or ensure ChromaDB has sufficient resources for concurrent requests.
:::

:::caution Warning
**Data consistency**: ChromaDB does not provide snapshot or cursor mechanisms. The connector scans IDs via `offset+limit` pagination, then fetches full data by ID list. When `id_split_size` is large enough (default 10M) to scan all IDs in one pass, the ID list acts as a point-in-time snapshot — subsequent data fetches by ID are unaffected by concurrent writes. Only when the collection exceeds `id_split_size` and requires multiple scan passes can writes or deletes between passes cause duplicates or missed records. For very large collections, pause writes during migration or increase `id_split_size` (mind the memory cost).
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

## Changelog

<ChangeLog />
