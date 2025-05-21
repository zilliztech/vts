# Tencent VectorDB

> Tencent VectorDB connector

## Description

Allows reading data from Tencent VectorDB.

## Key Features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)

## Source Options

| Name        | Type    | Required | Default | Description                                                                        |
|-------------|---------|----------|---------|------------------------------------------------------------------------------------|
| url         | String  | Yes      | -       | The URL of your Tencent VectorDB instance.                                         |
| api_key     | String  | Yes      | -       | Your Tencent VectorDB API key.                                                     |
| user_name   | String  | Yes      | -       | Your Tencent VectorDB username.                                                    |
| database    | String  | Yes      | -       | The name of the database to read from.                                             |
| collection  | String  | Yes      | -       | The name of the collection to read from.                                           |
| batch_size  | Integer | No       | 100     | The number of vectors to fetch per request when reading.                           |

## Task Example

This example shows reading from Tencent VectorDB and writing to Milvus, based on `tencent.conf`.

```bash
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Tencent {
    url="https://your-tencent-vectordb-url" // Replace with your Tencent VectorDB URL
    api_key="YOUR_TENCENT_API_KEY" // Replace with your actual API key
    user_name="YOUR_USERNAME" // Replace with your username
    database="default"
    collection="midjourney"
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

Tencent VectorDB connections are secured using API key authentication. Ensure your API keys and credentials are kept confidential and managed securely.
