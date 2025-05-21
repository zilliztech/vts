# Pinecone

> Pinecone connector

## Description

Allows reading data from and writing data to Pinecone vector database.

## Key Features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)

## Source Options

| Name        | Type    | Required | Default | Description                                                                        |
|-------------|---------|----------|---------|------------------------------------------------------------------------------------|
| api_key     | String  | Yes      | -       | Your Pinecone API key.                                                             |
| index       | String  | Yes      | -       | The name of the Pinecone index to read from.                                       |
| merge_namespace | Boolean | No    | false   | Whether to merge data from all namespaces within the index.                      |
| batch_size  | Integer | No       | 100     | The number of vectors to fetch per request when reading.                           |                                   |

## Task Example

This example shows reading from Pinecone and writing to Milvus, based on `pinecone.conf`.

```bash
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Pinecone {
    api_key="YOUR_PINECONE_API_KEY" // Replace with your actual API key
    index="book" // Replace with your Pinecone index name
    merge_namespace=true
    batch_size=100
  }
}

sink {
  Milvus { // Example uses Milvus as sink
      url="https://your-milvus-url.vectordb.zillizcloud.com:19541" // Replace with your Milvus/Zilliz URL
      token="YOUR_MILVUS_TOKEN" // Replace with your Milvus/Zilliz token
      database="default"
      batch_size=10
    }
}
```

## Security

Pinecone connections are secured using HTTPS. Authentication is handled via API keys. Ensure your API keys are kept confidential and managed securely.
