# Milvus

> Milvus connector

## Description

Write data from milvus to Milvus or Zilliz Cloud

## Key Features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)

## Source Options

| Name        | Type   | Required | Default | Description                                                                        |
|-------------|--------|----------|---------|------------------------------------------------------------------------------------|
| url         | String | Yes      | -       | The URL to connect to Milvus or Zilliz Cloud.                                      |
| token       | String | Yes      | -       | User:password                                                                      |
| database    | String | Yes      | default | Read data from which database.                                                     |
| collections | List   | No       | -       | If set, will read collections, otherwise will read all collections under database. |
|server_pem_path| String | No       | No      |    Path to the PEM file for server certificate                                                                                |
| server_name            | String | No       | No      |  Server name for TLS verification                                                                                  |
|       client_key_path                 | String | No       | No      |      Path to the KEY file for client certificate                                                                                                              |
|       ca_pem_path                 | String | No       |         |    Path to the PEM file for CA certificate                                                                                                                |

## Sink Options

|         Name         |  Type   | Required |           Default            |                        Description                        |
|----------------------|---------|----------|------------------------------|-----------------------------------------------------------|
| url                  | String  | Yes      | -                            | The URL to connect to Milvus or Zilliz Cloud.             |
| token                | String  | Yes      | -                            | User:password                                             |
| database             | String  | No       | -                            | Write data to which database, default is source database. |
| schema_save_mode     | enum    | No       | CREATE_SCHEMA_WHEN_NOT_EXIST | Auto create table when table not exist.                   |
| create_index         | boolean | No       | true                         | Create indexes when auto creating the collection.         |
| enable_auto_id       | boolean | No       | false                        | Primary key column enable autoId.                         |
| enable_upsert        | boolean | No       | false                        | CDC mode only: use full-row upsert for INSERT/UPDATE_AFTER. AutoID targets require `allow_insert_auto_id = "true"` and use non-atomic delete then insert with the original keys; APPEND mode is unaffected. See [CDC upsert strategy](../en/connector-v2/sink/Milvus.md#cdc-upsert-strategy).                                   |
| enable_dynamic_field | boolean | No       | true                         | Enable create table with dynamic field.                   |
| batch_size           | int     | No       | 1000                         | Write batch size.                                         |
| partition_num        | int     | No       | -                            | Number of partitions passed to Milvus create collection request. Currently used by Milvus partition key mode. |
| functionList         | List    | No       | []                           | List of Milvus functions to add to the collection. Each function contains: name, description, functionType, inputFieldNames, outputFieldNames, and params. Will be merged with functions from source metadata. |
| field_schema         | List    | No       | []                           | **Unified field schema configuration.** Supports both dynamic field extraction and field property configuration. See detailed examples below. |
|server_pem_path| String | No       | No      |    Path to the PEM file for server certificate                                                                                |
| server_name            | String | No       | No      |  Server name for TLS verification                                                                                  |
|       client_key_path                 | String | No       | No      |      Path to the KEY file for client certificate                                                                                                              |
|       ca_pem_path                 | String | No       |         |    Path to the PEM file for CA certificate                                                                                                                |

## Task Example

```bash
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Milvus {
  url="https://in01-***.aws-us-west-2.vectordb.zillizcloud.com:19530"
  token="***"
  database="default"
  collections=["medium_articles"]
  batch_size=100
  }
}

sink {
  Milvus {
  url="https://in01-***.aws-us-west-2.vectordb.zillizcloud.com:19542"
  token="***"
  database="default"
  batch_size=10
  functionList=[
    {
      name="text_embedding_function"
      description="Embedding function for text fields"
      functionType="BM25"
      inputFieldNames=["text_field"]
      outputFieldNames=["text_embedding"]
      params={
        "model"="bm25"
      }
    }
  ]
  # Recommended: Use unified field_schema for all field configurations
  field_schema=[
    # Example 1: Extract field from dynamic schema with renamed target and full text search
    {
      source_field_name="metadata"
      field_name="meta"  # Rename to 'meta'
      data_type=21  # VarChar
      max_length=1000
      is_nullable=true
      enable_analyzer=true
      analyzer_params={type="standard"}
    }
    # Example 2: Extract array field as partition key (keeps same name)
    {
      source_field_name="tags"
      data_type=22  # Array
      element_type=21  # Array of VarChar
      max_capacity=100
      is_nullable=false
      is_partition_key=true
    }
    # Example 3: Extract and set as primary key with auto ID
    {
      source_field_name="id"
      data_type=5  # Int64
      is_primary_key=true
      auto_id=true
    }
    # Example 4: Configure properties for existing table fields
    {
      field_name="text_field"
      is_nullable=true
      enable_analyzer=true
      analyzer_params={type="english"}
      enable_match=true
    }
    {
      field_name="category"
      is_partition_key=true
    }
    {
      field_name="description"
      is_nullable=false
      default_value="N/A"
    }
  ]
  }
}
```
## Multi-vector migration from Qdrant

Qdrant multi-dense fields (`multivector_config.comparator = max_sim`) are automatically
represented as SeaTunnel `ARRAY<ARRAY<FLOAT>>` fields, with the subvector dimension
stored in the column schema and `Float[][]` row values. The Milvus sink maps this
generic type to StructArray; the Qdrant source emits no Milvus-specific metadata or
JSON wrappers. The source accepts both the modern `multi_dense`
encoding and the legacy flattened `data` + `vectors_count` encoding. Named and unnamed
vectors are supported; an unnamed field is called `vector` in the target.

For a source named `colbert`, the Milvus sink writes each point as follows:

```json
{"colbert": [{"vector": [0.1, 0.2]}, {"vector": [0.3, 0.4]}]}
```

Each point remains one entity. Subvector order, dimension and variable list length
are preserved. Payload follows the existing dynamic-field mapping; it is not
implicitly wrapped in a new `payload` JSON field.

The Milvus sink creates a StructArray with one `vector` FloatVector child and a capacity of 4096. This capacity is a target constraint, not a limit imposed by the Qdrant source or the generic type. For a pre-created StructArray with one child, the sink uses that child's name; otherwise it uses the default child name `vector`. The connector converts the matrix without prechecking its capacity, dimension or numeric values; the SDK/server handles incompatible data. Before loading the collection for search, create a compatible index on each vector field. For Cosine + MaxSim, use AUTOINDEX with MAX_SIM_COSINE on `colbert[vector]`.

### Search after migration

Keep the original embedding model and query matrix. With a compatible PyMilvus SDK:

```python
from pymilvus.client.embedding_list import EmbeddingList

query = EmbeddingList()
query.add_batch(query_vectors)  # one query: [number_of_tokens, dimension]
results = client.search(
    collection_name="documents",
    anns_field="colbert[vector]",  # tokens[vector] if renamed
    data=[query],
    limit=10,
)
```

This returns whole entities ranked by MaxSim. Compare retrieval quality and score thresholds
before switching traffic; migration does not translate application query code or Qdrant
prefetch/reranking pipelines.

A reproducible local integration check is provided in
[`qdrant_multivector_e2e.py`](../../tools/qdrant_multivector_e2e.py). It creates uniquely named
collections, invokes a freshly built VTS distribution, checks exact vector contents and
MaxSim results, and removes only its own test collections.

### Configuring a renamed StructArray

Automatic mapping uses the source name and dimension. When using `field_schema`,
a StructArray definition can rename the source field and override dimension/capacity:

```hocon
# Include the primary key and any other fields required by the target schema.
field_schema = [
  { field_name = "id", data_type = 5, is_primary_key = true, auto_id = false },
  { source_field_name = "colbert", field_name = "tokens", data_type = 22,
    element_type = 201, dimension = 128, max_capacity = 4096 }
]
```

The nested FloatVector child remains `vector`. These are schema settings, not
client-side compatibility checks. Explicit non-StructArray definitions keep their
configured target type rather than adding a duplicate StructArray.

## USING TLS
check this doc about how to use tls in milvus, just specify the related config in the source or sink configs:
https://milvus.io/docs/tls.md
