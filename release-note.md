# 2.3.10 Release Note

## Feature

- [improve] update localfile connector config (#8765)
- [Improve][Zeta] Check closing log file handle every 1 minute (#9003)
- Revert " [improve] update localfile connector config" (#9018)
- [improve] update Web3j connector config option  (#9005)
- [Improve] influxdb options (#8966)
- [improve] http connector options (#8969)
- [improve] iotdb options (#8965)
- [improve] email connector options (#8983)
- [Feature][Jdbc] Support sink ddl for sqlserver #8114 (#8936)
- [Improve] iceberg options (#8967)
- [Improve] Update label-scope-conf.yml for core label (#8979)
- [Improve][Core] Support parse quote as key (#8975)
- [Improve][deploy] helm chart config improve (#8954)
- [Feature][Connector-V2] Support between predicate pushdown in paimon (#8962)
- [Improve][Connector-V2] Ensure that the FTP connector behaves reliably during directory operation (#8959)
- [Improve] easysearch options (#8951)
- [Feature][elasticsearch-connector] support elasticsearch sql source (#8895)
- [Improve][Connector-V2] RocketMQ Source add message tag config (#8825)
- [improve] hudi options (#8952)
- [Improve][Shade] Unit the module name of Shade. (#8960)
- [Feature][Connector-V2] Suppor Time type in paimon connector (#8880)
- [improve] fake source options (#8950)
- [Improve] hbase options (#8923)
- [Improve][CDC] Filter ddl for snapshot phase (#8911)
- [Improve][Oracle-CDC] Support ReadOnlyLogWriterFlushStrategy (#8912)
- [improve] google sheets options (#8922)
- [Improve][Connector-V2] Random pick the starrocks fe address which can be connected (#8898)
- [Improve][E2E][Connector V2][CDC Oracle] OracleContainer support ARM architecture images (#8928)
- [Improve] filestore options (#8921)
- [improve] improve TaskLocation/TaskLocationGroup info (#8862)
- [Improve][CDC] Extract duplicate code (#8906)
- [Feature][Paimon] Customize the hadoop user  (#8888)
- [Improve][connector-file-base] Improved multiple table file source allocation algorithm for subtasks (#8878)
- [Improve][connector-hive] Improved hive file allocation algorithm for subtasks (#8876)
- [Improve] sink mongodb schema is not required (#8887)
- [Feature][Connector-v2] Support multi starrocks source (#8789)
- [Feature][Kafka] Support native format read/write kafka record (#8724)
- [Improve][Redis] Optimized Redis connection params (#8841)
- [Improve][CDC] Filter heartbeat event (#8569)
- [Improve][Jdbc] Support upsert for opengauss (#8627)
- [Improve][Zeta] Split classloader in job master (#8622)
- [Feature][Connector-V2] Add  parameter for read/write file (#8769)
- [Improve] doris options (#8745)
- [Feature][Transform-v2] Add support for Zhipu AI in Embedding and LLM module (#8790)
- [Improve][Jdbc] Remove useless utils. (#8793)
- [improve] update clickhouse connector config option (#8755)
- [Feature][Config] Support custom config keys for encrypt/decrypt (#8739)
- [Improve] rabbit mq options (#8740)
- [Improve] re-struct Zeta Engine config options (#8741)
- [Improve][Zeta] Disable restful api v1 by default (#8766)
- [improve] update kafka source default schema from content<ROW<content STRING>> to content<STRING> (#8642)
- [Feature][Transforms-V2] Handling LLM non-standard format responses (#8551)
- [improve] console sink options (#8743)
- [Improve][Connector-V2] Improve orc read error message (#8751)
- [Improve][connector][activemq] Remove duplicate dependencies (#8753)
- [Improve][Jdbc] Improve catalog connection cache (#8626)
- [improve] datahub sink options (#8744)
- [improve] dingtalk sink options (#8742)
- [improve] Slack connector options (#8738)
- [Improve][Connector-V2] Add optional flag for rocketmq connector to skip parse errors instead of failing (#8737)
- [Improve][Connector-v2][Paimon]PaimonCatalog close error message update (#8640)
- [Feature][Jdbc] Support sink ddl for dameng (#8380)
- [Improve] restruct connector common options (#8634)
- [improve] add StarRocks options (#8639)
- [improve] update Redis connector config option (#8631)
- [improve] add assert options (#8620)
- [Improve][e2e] Remove duplicate dependencies (#8628)
- [improve] update S3File connector config option  (#8615)
- [Feature][Connector-V2] Support maxcompute source with multi-table (#8582)
- [improve] add Elasticsearch options (#8623)
- [improve] update amazondynamodb connector (#8601)
- [improve] amazon sqs connector update (#8602)
- [improve] update activemq connector config option (#8580)
- [Feature][transform-v2] jsonpath support map array type (#8577)
- [improve] cassandra connector options (#8608)
- [improve] kafka connector options (#8616)
- [improve] update Druid connector config option (#8594)
- [Refactor][core] Unify transformFactory creation logic (#8574)
- [improve] add connector options class exist check (#8600)
- [Feature][Core] Add slot allocation strategy (#8233)
- [Improve] Update snapshot version to 2.3.10 (#8578)
- [Improve][Jdbc] Remove oracle 'v' query (#8571)
- [feature][core] Unified engine initialization connector logic (#8536)
- [Feature][Iceberg] Support read multi-table (#8524)
- [Feature] [Postgre CDC]support array type (#8560)
- [Feature][Connector-V2] Support create emtpy file when no data (#8543)
- [config][enhance]support use properties when encrypt/decrypt config (#8527)
- [Improve][Connector-v2] add starrocks comment test (#8545)
- [Feature][Transform-V2] llm add deepseek (#8544)
- [Feature][Connector-V2] Support single file mode in file sink (#8518)
- [Improve][Connector-V2] MaxComputeSink support create partition in savemode (#8474)

## Bug Fix

- [Fix][Connector-V2] Fix load state check in MilvusSourceReader to consider partition-level status (#8937)
- [Fix][File]use common-csv to read csv file (#8919)
- [Fix][CI] Ignore dead link check for change log commit url (#9001)
- [Fix][Connector-V2] Fix StarRocksCatalogTest#testCatalog() NPE (#8987)
- [Improve] Update nodejs version to fix ci (#8991)
- [Fix][Command] Run seatunnel on windows with fileAppender enabled, no log file been created (#8938)
- [Fix][Connector-V2] Fix maxcompute read with partition spec (#8896)
- [Fix] [Mongo-cdc] Fallback to timestamp startup mode when resume token has expired (#8754)
- [Fix][Connector-V2] Fix text file read separator issue (#8970)
- [hotfix][redis] fix npe cause by null host parameter (#8881)
- [Fix][CI] Update known-dependencies.txt to fix ci (#8947)
- [Fix] [Clickhouse] Parallelism makes data duplicate (#8916)
- [Fix][Connector-V2] Fixed incorrectly setting s3 key in some cases (#8885)
- [Fix]update reload4j version to 1.7.36 (#8883)
- [Fix][chore] Fix --role parameter not working in seatunnel-cluster.cmd (#8877)
- [Fix][Connector-V2] Fix MaxCompute cannot get project and tableName when use schema (#8865)
- [Fix][Connector-File] Fix conflicting  requirement (#8823)
- [Fix][Connector-V2] Fix parse SqlServer JDBC Url error (#8784)
- [Fix][Plugin] Optimize the plugin discovery mechanism (#8603)
- [Fix] Fix error log name for SourceSplitEnumerator implements class (#8817)
- [Fix][connector-http] fix when post have param (#8434)
- [Bugfix] Fix ClassCastException of ExceptionUtil (#8776)
- [Fix][Connector-V2] Fix possible data loss in scenarios of request_tablet_size is less than the number of BUCKETS (#8768)
- [Fix][Connector-V2]Fix Descriptions for CUSTOM_SQL in Connector (#8778)
- [Fix][E2e] Optimized pom file name tag (#8770)
- [Fix][Connector-v2] Add DateMilliConvertor to Convert DateMilliVector into Default Timezone (#8736)
- [Fix][Transform] Fix FieldMapper transform lost field constraint information (#8697)
- [Bugfix][Canal] Fix canal serialization to json (#8695)
- [Fix][Connector-V2] Fix jdbc sink statement buffer wrong time to clear (#8653)
- [Fix][Connector-V2] Fix file reading cannot read empty strings (#8646)
- [Fix][Connector-v2][DorisIT]Fix the problem that DorisIT cannot run local (#8630)
- [Fix][transform-v2]LLM transform provider switch case miss break (#8643)
- [Fix][transform-v2]SQL transform support max/min function (#8625)
- [Fix][MySQL-CDC]fix recovery task failure caused by binlog deletion (#8587)
- [Fix][mysql-cdc] Fix GTIDs on startup to correctly recover from checkpoint (#8528)
- [Fix][Connector-V2] User selects csv string pattern (#8572)
- [Fix][doris-e2e] Fix flaky Doris e2e tests (#8596)
- [Fix][Connector-Mongodb] close MongodbClient when close MongodbReader (#8592)
- [Bugfix][example] Fixing spark test cases (#8589)
- [Fix][Connector-V2] fix starRocks automatically creates tables with comment (#8568)
- [Fix][Connector-V2] Fix CSV String type write type (#8499)
- [Hotfix][Connector-V2][SFTP] Add quote to sftp file names with wildcard characters (#8501)
- [Fix][connector-elasticsearch] support elasticsearch nest type && spark with Array<map> (#8492)
- [Fix] [Connector-V2] Postgres support for multiple primary keys (#8526)
- [Fix][File] Fix Multi-file with binary format synchronization failed (#8546)
- [Fix] Update zh table-merge.md (#8535)
- [Fix] Update table-merge.md (#8532)
- [Fix][Connector-V2] Fixed adding table comments (#8514)
- [Fix] [Kafka Source] kafka source use topic as table name instead of fullName (#8401)
- [Fix][Hive] Writing parquet files supports the optional timestamp int96 (#8509)

## Docs

- [Improve][Doc] Added automatically generate connector commit history documents (#8948)
- [Fix][Doc] s3 file doc mismatch with code (#8926)
- [Doc][Improve] translate FtpFile related chinese document (#8944)
- [Fix][Doc] Fixed the  parameter error in the JDBC doc (#8943)
- [Doc] Fix example error (#8905)
- [Doc][FTP] Fix the dead link of ftp doc. (#8860)
- [Doc][Improve] support chinese [docs/zh/connector-v2/source/CosFile.md] (#8826)
- [Doc][Improve] support chinese [docs/zh/connector-v2/source/Clickhouse.md] (#8824)
- [Doc][Improve] support chinese [docs/zh/connector-v2/sink/ObsFile.md] (#8827)
- [Doc][Improve] support chinese [docs/zh/connector-v2/sink/OceanBase.md] (#8830)
- [Doc][Improve] support chinese [docs/zh/connector-v2/sink/Oracle.md] (#8831)
- [Doc][Improve] support chinese [docs/zh/connector-v2/sink/OssFile.md] (#8832)
- [Doc][Improve] support chinese [docs/zh/connector-v2/sink/OssJindoFile.md] (#8833)
- [Doc][Improve] support chinese [docs/zh/connector-v2/sink/RocketMQ.md] (#8834)
- [Doc][Improve] support chinese [docs/zh/connector-v2/sink/S3-Redshift.md] (#8835)
- [Doc][Improve] support chinese [docs/zh/connector-v2/source/DB2.md]  (#8842)
- [Doc][Improve] support chinese [docs/zh/connector-v2/source/Easysearch.md] (#8843)
- [Doc][Improve] support chinese [docs/zh/connector-v2/sink/SftpFile.md] (#8844)
- [Doc][Improve] support chinese [docs/zh/connector-v2/sink/Snowflake.md] (#8845)
- [Doc][Improve] support chinese [docs/zh/connector-v2/source/FakeSource.md] (#8847)
- [Doc][Improve] support chinese [docs/zh/connector-v2/source/FtpFile.md] (#8848)
- [Doc][Improve] support chinese [docs/zh/connector-v2/sink/S3File.md] (#8849)
- [Doc][Improve] support chinese [docs/zh/connector-v2/sink/SelectDB-Cloud.md] (#8850)
- [Doc][Improve] support chinese [docs/zh/connector-v2/sink/SqlServer.md] (#8851)
- [Doc][Improve] support chinese [docs/zh/connector-v2/sink/Vertica.md] (#8852)
- [Doc][Improve] support chinese [docs/zh/connector-v2/sink/Mysql.md] (#8818)
- [Doc][Improve] support chinese [docs/zh/connector-v2/sink/Tablestore.md] (#8731)
- [Doc][Improve] support chinese [docs/zh/connector-v2/sink/Kudu.md] (#8725)
- [Fix][Doc] fix s3File doc (#8798)
- [Doc][Improve] support chinese [docs/zh/connector-v2/sink/TDengine.md] (#8732)
- [Doc][Improve] support chinese [docs/zh/connector-v2/sink/Socket.md] (#8729)
- [Doc][Improve] support chinese [docs/zh/connector-v2/sink/Milvus.md] (#8727)
- [Doc][Improve] support chinese [docs/zh/connector-v2/sink/IoTDB.md] (#8722)
- [Docs][S3File] "Orc Data Type" should be corrected to "Parquet Data Type" in the documentation (#8705)
- [Doc][Improve] support chinese [docs/zh/connector-v2/source/Cassandra.md] (#8704)
- [Doc][Improve] support chinese [docs/zh/connector-v2/sink/CosFile.md]  (#8700)
- [Doc][Improve] support chinese [docs/zh/connector-v2/sink/Slack.md] (#8701)
- [Doc][Improve] support chinese [docs/zh/connector-v2/source/AmazonSqs.md] (#8703)
- [Doc][Improve] support chinese [docs/zh/connector-v2/source/GoogleSheets.md] (#8706)
- [Doc] Add Milvus source zh doc (#8709)
- [Doc][Improve] support chinese [docs/zh/connector-v2/source/Maxcompute.md] (#8708)
- [Doc][Improve] support chinese [docs/zh/connector-v2/source/OpenMldb.md] (#8710)
- [Doc][Improve] support chinese [docs/zh/connector-v2/source/Phoenix.md] (#8711)
- [Improve][Doc] Update old links in doc (#8690)
- [Doc][Improve] support chinese [docs/zh/connector-v2/source/AmazonDynamoDB.md] (#8694)
- [Doc][Improve] support chinese [docs/zh/connector-v2/sink/Sentry.md] #8497 (#8672)
- [Improve][dist] Reduce Docker image size (#8641)
- [hotfix][doc] typo HdfsFile doc (#8613)
- [Doc][Improve] translate Redis/Paimon related chinese document (#8584)
- [Doc][Mysql-cdc]Update doc to support mysql 8.0 (#8579)
- [Doc][Fix]fix deadlink (#8555)
- [Doc][Improve] translate postgresql related chinese document  (#8552)
- [Doc][Improve] translate neo4j starrocks related chinese document (#8549)
- [config][doc]add sensitive columns and enhance the doc  (#8523)
- [Doc][Translate]  translated and corrected the error in the original document (#8505)
- [Fix][Doc] Fix dead link (#8525)
- [Improve][Doc] Update the transform contribute guide (#8487)
- [Docs][Iceberg] translate connector-v2/sink/Iceberg.md to Chinese #8497 (#8520)
- [Fix][Doc] Fix dead link in kubernetes docs (#8510)
- [Improve][Doc] Add remote_user parameter in HdfsFile.md (#8503)
