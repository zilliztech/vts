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
  }
}
```
## USING TLS
check this doc about how to use tls in milvus, just specify the related config in the source or sink configs:
https://milvus.io/docs/tls.md
