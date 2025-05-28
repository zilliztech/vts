# VTS (Vector Transport Service)

[![Discord](https://img.shields.io/discord/1160323594396635310?label=Discord&logo=discord&style=social)](https://discord.com/invite/mKc3R95yE5)
[![Twitter Follow](https://img.shields.io/twitter/follow/zilliz_universe?style=social)](https://x.com/zilliz_universe)
[![Twitter Follow](https://img.shields.io/twitter/follow/milvusio?style=social)](https://x.com/milvusio)

## Overview

**VTS** (Vector Transport Service) is an open-source tool for moving [vectors](https://zilliz.com/glossary/vector-embeddings) and [unstructured data](https://zilliz.com/learn/introduction-to-unstructured-data). It is developed by [Zilliz](https://zilliz.com/) based on **Apache Seatunnel**.

![VTS Diagram](docs/zilliz/images/vts.png)

## Why do you need a vector and unstructured data moving tool?
1. **Meeting the Growing Data Migration Needs:** VTS evolves from our Milvus Migration Service, which has successfully helped over 100 organizations migrate data between [Milvus](https://github.com/milvus-io/milvus) clusters. User demands have grown to include migrations from different [vector databases](https://zilliz.com/learn/what-is-vector-database), traditional search engines like [Elasticsearch](https://zilliz.com/comparison/milvus-vs-elastic) and Solr, [relational databases](https://zilliz.com/blog/relational-databases-vs-vector-databases), data warehouses, document databases, and even S3 and data lakes to Milvus.
2. **Supporting Real-time Data Streaming and Offline Import:** As vector database capabilities expand, users require both real-time data streaming and offline batch import options.
3. **Simplifying Unstructured Data Transformation:** Unlike traditional ETL, transforming unstructured data requires AI and model capabilities. VTS, in conjunction with the [Zilliz Cloud Pipelines](https://zilliz.com/zilliz-cloud-pipelines), enables vector embedding, tagging, and complex transformations, significantly reducing data cleaning costs and operational complexity.
4. **Ensuring End-to-End Data Quality:** Data integration and synchronization processes are prone to data loss and inconsistencies. VTS addresses these critical data quality concerns with robust monitoring and alerting mechanisms.

## Core Capabilities of VTS
Built on top of Apache Seatunnel, Vector-Transport-Service offers:
1. Rich, extensible connectors
2. Unified stream and batch processing for real-time synchronization and offline batch imports
3. Distributed snapshot support for data consistency
4. High performance, low latency, and scalability
5. Real-time monitoring and visual management

Additionally, Vector-Transport-Service introduces vector-specific capabilities such as multiple data source support, schema matching, and basic data validation. 

## Roadmap

Future developments include:
- Incremental synchronization
- Combined one-time migration and change data capture
- Advanced data transformation capabilities
- Enhanced monitoring and alerting

![roadmap.png](docs/zilliz/images/roadmap.png)

## Getting Started

### Prerequisites
- Docker installed
- Access to source and target databases
- Required credentials and permissions
- Milvus Version >= 2.3.6

### Quick Start

1. **Pull the VTS Image**
```bash
docker pull zilliz/vector-transport-service:latest
docker run -it zilliz/vector-transport-service:latest /bin/bash
```

2. **Configure Your Migration**
Create a configuration file (e.g., `migration.conf`):
```yaml
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  # Source configuration (e.g., Milvus, Elasticsearch, etc.)
  Milvus {
    url = "https://your-source-url:19530"
    token = "your-token"
    database = "default"
    collections = ["your-collection"]
    batch_size = 100
  }
}

sink {
  # Target configuration
  Milvus {
    url = "https://your-target-url:19530"
    token = "your-token"
    database = "default"
    batch_size = 10
  }
}
```

3. **Run the Migration**

Cluster Mode (Recommended):
```bash
# Start the cluster
mkdir -p ./logs
./bin/seatunnel-cluster.sh -d

# Submit the job
./bin/seatunnel.sh --config ./migration.conf
```

Local Mode:
```bash
./bin/seatunnel.sh --config ./migration.conf -m local
```

### Configuration Tips
- Adjust `parallelism` based on your data volume
- Configure appropriate `batch_size` for optimal performance
- Set up proper authentication and security measures
- Monitor system resources during migration

## Supported Connectors

VTS supports various connectors for data migration:

- [Milvus](docs/zilliz/Milvus.md) ([example config](seatunnel-examples/seatunnel-engine-examples/src/main/resources/examples/milvus_to_milvus.conf))
- [Elasticsearch](docs/zilliz/Elasticsearch.md) ([example config](seatunnel-examples/seatunnel-engine-examples/src/main/resources/examples/es_to_milvus.conf))
- [Pinecone](docs/zilliz/Pinecone.md) ([example config](seatunnel-examples/seatunnel-engine-examples/src/main/resources/examples/pinecone.conf))
- [Qdrant](docs/zilliz/Qdrant.md) ([example config](seatunnel-examples/seatunnel-engine-examples/src/main/resources/examples/qdrant.conf))
- [Postgres Vector](docs/zilliz/Postgres%20Vector.md) ([example config](seatunnel-examples/seatunnel-engine-examples/src/main/resources/examples/pg_to_milvus.conf))
- [Tencent VectorDB](docs/zilliz/Tencent%20VectorDB.md) ([example config](seatunnel-examples/seatunnel-engine-examples/src/main/resources/examples/tencent.conf))

## Advanced Features

For more advanced features, refer to our [Tutorial.md](./docs/zilliz/Tutorial.md) and the [Apache SeaTunnel Documentation](https://seatunnel.apache.org/docs/2.3.10/about):

- Transformers (TablePathMapper, FieldMapper, Embedding)
- Cluster mode deployment
- RESTful API for job management
- Docker deployment
- Advanced configuration options

## Development

For development setup and contribution guidelines, see [Development.md](./Development.md).

## Support

Need help? Contact our support team:
- Email: support@zilliz.com
- Discord: [Join our community](https://discord.com/invite/mKc3R95yE5)

## About Apache Seatunnel

SeaTunnel is a next-generation, high-performance, distributed data integration tool. It's:
- Capable of synchronizing vast amounts of data daily
- Trusted by numerous companies for efficiency and stability
- Released under [Apache 2 License](https://github.com/apache/seatunnel/blob/dev/LICENSE)
- A top-level project of the Apache Software Foundation (ASF)

For more information, visit the [Apache Seatunnel website](https://seatunnel.apache.org/).
