# Qdrant

> Qdrant connector

## Description

Allows reading data from Qdrant vector database.

## Key Features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)

## Source Options

| Name           | Type    | Required | Default | Description                                                                        |
|----------------|---------|----------|---------|------------------------------------------------------------------------------------|
| host           | String  | Yes      | -       | The host address of Qdrant server.                                                 |
| port           | Integer | Yes      | -       | The port number of Qdrant server.                                                  |
| use_tls        | Boolean | No       | false   | Whether to use TLS for connection.                                                 |
| api_key        | String  | Yes      | -       | Your Qdrant API key.                                                               |
| collection_name| String  | Yes      | -       | The name of the Qdrant collection to read from.                                    |
| batch_size     | Integer | No       | 100     | The number of vectors to fetch per request when reading.                           |

## Task Example

This example shows reading from Qdrant and writing to Milvus, based on `qdrant.conf`.

```bash
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Qdrant {
    host="127.0.0.1"
    port=6334
    use_tls=false
    api_key="YOUR_QDRANT_API_KEY" // Replace with your actual API key
    collection_name="midjourney"
    batch_size=100
  }
}

sink {
  Milvus { // Example uses Milvus as sink
      url="https://your-milvus-url.vectordb.zillizcloud.com:19532" // Replace with your Milvus/Zilliz URL
      token="YOUR_MILVUS_TOKEN" // Replace with your Milvus/Zilliz token
      database="default"
      batch_size=10
    }
}
```

## Reading multi-vectors

Qdrant multi-dense fields are represented as SeaTunnel `ARRAY<ARRAY<FLOAT>>`,
with `Float[][]` row values and the subvector dimension in the column's `scale`.
The source accepts modern `multi_dense` and legacy flattened `data` + `vectors_count`
encodings. Subvector order and variable list lengths are preserved. Legacy values
must be divisible into the declared number of subvectors to recover their boundaries.

Named fields retain their names. An unnamed vector is exposed as `vector` with
`qdrant.unnamed_vector = true` in column options, so a Qdrant sink can restore the
original request format. No target-specific vector schema is embedded in the type.

## Writing multi-vectors to Qdrant

The Qdrant sink accepts `ARRAY<ARRAY<FLOAT>>` (`Float[][]`) as a multi-dense vector.
Create the target collection first: the connector does not create collections.
The sink converts each matrix using the SDK and sends it to Qdrant. Field type,
dimension, comparator and value compatibility are validated by the server, and
write failures are propagated through the existing upsert error path. The Qdrant
source preserves unnamed/named mode in column metadata, which the sink uses to
choose the request format without reading target configuration.

- For named vectors, source and target field names must match (for example, `colbert`).
  Multiple named multi-vector fields can be written together with existing dense fields.
- An unnamed Qdrant source vector is written as an unnamed vector. Its SeaTunnel
  field is initially called `vector`, but anonymity is recorded separately and
  survives field renaming when column options are preserved. A named source vector
  called `vector` remains named. Fields without the source anonymity marker use
  the existing named-vector write path.
- If multiple vector fields are supplied, all are retained in the vector map
  (with the empty Qdrant vector name for an unnamed field); the server handles
  incompatible target schemas.
- A null matrix is omitted. Other matrices are encoded without connector-level
  dimension, length or finite-value checks. Null elements inside a matrix cannot
  be represented by the SDK's repeated float fields. The Milvus-specific 4096
  limit does not apply to this sink.
- Numeric `INT`/`BIGINT` point IDs and UUID string IDs are supported. Scalar payload
  handling follows the existing sink behavior; this feature does not add dynamic
  metadata expansion or sparse-vector writing.
- Writes use the Java SDK's `multi_dense` encoding; the target Qdrant server must
  support this encoding.

Use the following sink with a source emitting the matching fields:

```hocon
sink {
  Qdrant {
    host = "127.0.0.1"
    port = 6334
    use_tls = false
    api_key = "YOUR_QDRANT_API_KEY"
    collection_name = "target_documents"
  }
}
```

## Security

Qdrant connections can be secured using TLS (when `use_tls` is set to true) and API key authentication. Ensure your API keys are kept confidential and managed securely.
