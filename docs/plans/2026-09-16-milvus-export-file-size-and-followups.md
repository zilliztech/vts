# Milvus 数据导出：文件切分问题记录与遗留项

日期：2026-09-16
相关分支：`feat/file-sink-vector-parquet`（vts）、`feat-vts-milvus-export`（zilliz-cloud）

## 背景

导出功能（zilliz-cloud `seatunnel_export` workflow）的 generic 格式走 S3File sink 直写
session stage 桶。讨论中发现 file sink 的文件边界只由 checkpoint 触发，没有按大小切分
（rolling）的能力。

## 问题：file sink 不能按文件大小切分

- `BaseSinkConfig` 没有 `max_file_size` 类配置项，文件边界 = checkpoint 边界；
- server 端 checkpoint 默认间隔 5 分钟（`ServerConfigOptions.CHECKPOINT_INTERVAL`），
  即文件按"时间片"切分，大小不可控；
- 产出文件数 ≈ collection 数 × parallelism × (时长 / checkpoint间隔)。

### 为什么大小值得控制

Parquet 文件的经验最优区间是 128MB~1GB（主流 256~512MB）：太小则查询引擎的
footer/请求固定开销和 task 碎片化放大；太大则并行粒度和失败重传成本变差。
bulkwriter 路线的 chunk_size 默认 512MB 正落在该区间，file sink 路线目前只能
按时间近似控制。

### 现有缓解（不改代码）

- `checkpoint.interval` 调大（小数据量导出基本一个文件）；
- `single_file_mode=true`（每 subtask 一个文件）；
- 注意 s3a 的 commit rename 是 copy+delete，checkpoint 越频繁桶内重复拷贝越多，
  大文件导出应把 interval 往大调。

### 建议的正式解法（未实施）

`AbstractWriteStrategy.write()` 里累计当前事务文件字节数，超过阈值
（新配置项 `max_file_size`）就关闭当前 writer 开新文件；所有文件在同一事务的
`finishAndCloseFile` 里一起进入 `needMoveFiles`，commit 协议不动。纯 writer 层增强，
约几十行，可回馈上游。

## 其他记录在案的遗留项

1. **bulkwriter 单 writer ~10MB/s 上限**（逐行 JsonObject 解析 + SDK 行式校验）：
   无 partition key 的 collection 只有单 split 单通道，全 job 即 10MB/s。
   正式解法是 bulkwriter SDK fork 加批量/列式 append；短期靠导出 API 的体积闸门
   （entity count × 行宽预估，超阈值引导到 Spark 导出）。
2. **bulkwriter 路径的 uuid 目录层**：SDK 强制 `resolve(getUUID())`。
   可选优化：SDK fork 加 `withSessionId`，staging 与客户侧路径一起变干净；
   或拷贝时 flatten+renumber。当前 generic 路线不存在此问题。
3. **BYOC 不支持**（第一期主动拒绝）：三个缺口——getExportStageBucket 需恢复
   useWorkloadIdentity；拷贝需在客户 VPC 内起工具 pod（参考 SessionStageCleanService
   的确定性临时 pod 模式）；客户侧 IAM 需补 volume 写权限。
4. **manifest 完成标记**（暂缓）：`job.retry.times=0` 保证 job 内无恢复重建，
   若未来放开重试，需要在 `MilvusBulkWriter.close()` 加 per-writer manifest 上传。
5. **回导兼容性版本下限**：generic 格式的 list<f32> 向量编码需要 Milvus import
   支持 list-like 向量的版本（importutilv2 field_reader），E2E 时确认版本下限并写文档。

## E2E 验证清单（测试环境）

- 全类型 collection（float/binary/int8/fp16/bf16/sparse 向量 + 标量 + array + json
  + dynamic field）导出为 generic parquet；
- Spark/duckdb 直读校验向量列；
- 回导 Milvus 校验各类型；
- 分段计时（source 读 / staging 写 / copy）与 pod 磁盘水位；
- 中途 kill job 验证 attempt 换路径重试与 staging 清理。
