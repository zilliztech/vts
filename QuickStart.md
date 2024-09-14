## QuickStart Guide
This guide will help you get started with how to use vts to transport vector data into milvus, currently, we support the following 3 source connectors:
- milvus
- postgres vector
- elastic search

**1. Build the vts project**
```shell
./mvnw install -Dmaven.test.skip
```
**2. Setup the configuration file**

go to ./seatunnel-example/seatunnel-examples/src/main/resources/, update the conf file
   - milvus_to_milvus.conf
   - pg_to_milvus.conf
   - es_to_milvus.conf

**3. Run examples**

The example file is located at
 _./seatunnel-example/seatunnel-examples/src/main/java/com/zilliz/seatunnel/examples/.engine/SeatunnelEngineExample.java_

update the configuration file path in _SeatunnelEngineExample.java_, and run the example.
```shell
String configurePath = args.length > 0 ? args[0] : "/examples/****.conf";
```
**4. Check the data in milvus**

go to milvus console, check the data in the collection
   