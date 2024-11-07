
## Development
**1. Build the vts project**
```shell
./mvnw install -Dmaven.test.skip
```
**2. Setup the configuration file**

go to ./seatunnel-example/seatunnel-examples/src/main/resources/examples, update the conf file
- milvus_to_milvus.conf
- pg_to_milvus.conf
- es_to_milvus.conf

here is an example of milvus_to_milvus.conf
```yaml
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Milvus {
    url="https://in01-***.aws-us-west-2.vectordb.zillizcloud.com:19530"
    token="***"
    database="default"
    collection="medium_articles"
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
**3. Run examples**

The example file is located at
_./seatunnel-example/seatunnel-examples/src/main/java/com/zilliz/seatunnel/examples/engine/SeatunnelEngineExample.java_

update the configuration file path in _SeatunnelEngineExample.java_, and run the example.
```shell
String configurePath = args.length > 0 ? args[0] : "/examples/****.conf";
```
**4. Check the data in milvus**

go to milvus console, check the data in the collection
