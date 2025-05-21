# Elasticsearch

> Elasticsearch connector

## Description

Allows reading data from and writing data to Elasticsearch.

## Key Features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [column projection](../../concept/connector-v2-features.md)

## Source Options

| Name           | Type    | Required | Default | Description                                                                        |
|----------------|---------|----------|---------|------------------------------------------------------------------------------------|
| hosts          | List    | Yes      | -       | List of Elasticsearch host URLs.                                                   |
| username       | String  | No       | -       | Username for authentication.                                                       |
| password       | String  | No       | -       | Password for authentication.                                                       |
| tls_verify_hostname | Boolean | No    | false   | Whether to verify hostname in TLS connection.                                      |
| tls_verify_certificate | Boolean | No | false   | Whether to verify certificate in TLS connection.                                   |
| index          | String  | Yes*     | -       | The name of the Elasticsearch index to read from. Required if index_list is not set.|
## Task Example

This example shows reading from Elasticsearch and writing to Milvus.

```bash
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Elasticsearch {
    hosts = ["https://your-elasticsearch-host:9200"]
    username = "your-username"
    password = "your-password"
    tls_verify_hostname = false
    tls_verify_certificate = false
    index = "source_index"
  }
}

sink {
  Milvus {
    url = "https://your-milvus-url.vectordb.zillizcloud.com:19532"
    token = "YOUR_MILVUS_TOKEN"
    database = "default"
    batch_size = 10
  }
}
```

## Security

Elasticsearch connections can be secured using:
- Basic authentication (username/password)
- TLS/SSL encryption with certificate verification
- API key authentication

Ensure your credentials are kept confidential and managed securely.