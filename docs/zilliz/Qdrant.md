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

## Security

Qdrant connections can be secured using TLS (when `use_tls` is set to true) and API key authentication. Ensure your API keys are kept confidential and managed securely.
