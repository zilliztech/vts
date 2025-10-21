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
| enable_auto_id       | boolean | No       | false                        | Primary key column enable autoId.                         |
| enable_upsert        | boolean | No       | false                        | Upsert data not insert.                                   |
| enable_dynamic_field | boolean | No       | true                         | Enable create table with dynamic field.                   |
| batch_size           | int     | No       | 1000                         | Write batch size.                                         |
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
## USING TLS
check this doc about how to use tls in milvus, just specify the related config in the source or sink configs:
https://milvus.io/docs/tls.md
