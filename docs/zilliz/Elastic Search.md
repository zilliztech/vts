# Tutorial: Migrating Data from Elasticsearch to Milvus Using VTS
This guide explains how to use Vector Transport Service (VTS) to migrate data from Elasticsearch to Milvus vector database.

**Prerequisites**

- Elasticsearch instance
- Milvus instance
- JAVA 8

### Step 1: Download and Extract VTS

- Navigate to the VTS releases page: https://github.com/zilliztech/vts/releases
- Download the latest binary package
- Extract the package using:
```shell
tar -xzvf "vector-transport-service-bin-xxx.tar.gz"
```
### Step 2: Configure the Migration
Create a configuration file named es_to_milvus.conf with the following structure:
```yaml
# Environment Configuration
env {
    # Number of parallel tasks
    parallelism = 1
    # Job execution mode
    job.mode = "BATCH"
}

# Source Configuration (Elasticsearch)
source {
    Elasticsearch {
          # SSL/TLS Settings
          tls_verify_hostname = false
          tls_verify_certificate = false
        
          # Connection Settings
          hosts = ["https://your-elasticsearch-host:9200"]
          username = "your-username"  # Leave empty if no authentication
          password = "your-password"  # Leave empty if no authentication
        
          # Data Source
          index = "my_index"      
    }
}

# Destination Configuration (Milvus)
sink {
    Milvus {
          # Connection Settings
          url = "https://your-milvus-instance.vectordb.zillizcloud.com:19531"
          token = "your-milvus-token"
        
          # Target Database
          database = "default"
        
          # Performance Tuning
          batch_size = 10  # Adjust based on your needs    
    }
}
```
Configuration Notes:

Replace placeholder values (marked with your-*) with your actual credentials
Adjust batch_size based on your data volume and system resources
The parallelism value can be increased for better performance on larger datasets

### Step 3: Run the Migration
Execute the migration using the SeaTunnel shell script:
```shell
./bin/seatunnel.sh --config ./es_to_milvus.conf -m local
```

#### Monitoring and Troubleshooting

Check the console output for progress and any error messages

### Next Steps

Verify data integrity in Milvus after migration. If Need help with specific configuration options or running into issues? Feel free to ask for clarification!