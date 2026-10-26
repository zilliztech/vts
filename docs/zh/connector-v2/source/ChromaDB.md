import ChangeLog from '../changelog/connector-chromadb.md';

# ChromaDB

> ChromaDB source 连接器

[ChromaDB](https://www.trychroma.com/) 是一个开源的 AI 原生向量数据库。

该连接器通过 ChromaDB REST API (v2) 读取 collection 中的数据。

## Key Features

- [x] [批处理](../../concept/connector-v2-features.md)
- [ ] [流处理](../../concept/connector-v2-features.md)
- [ ] [精准一次](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)
- [x] [并行度](../../concept/connector-v2-features.md)（单 collection 内及多个 collection 间并行）
- [ ] [支持用户自定义的分片](../../concept/connector-v2-features.md)

## 配置参数选项

| 参数名称            | 类型     | 是否必须 | 默认值              |
|-------------------|--------|------|------------------|
| url               | string | 是    | -                |
| tenant            | string | 否    | default_tenant   |
| database          | string | 否    | default_database |
| collections       | list   | 是    | -                |
| auth_type         | string | 否    | BEARER           |
| token             | string | 否    | ""               |
| username          | string | 否    | ""               |
| password          | string | 否    | ""               |
| batch_size        | int    | 否    | 1000             |
| id_split_size     | int    | 否    | 10000000         |
| connect_timeout   | int    | 否    | 30               |
| read_timeout      | int    | 否    | 600              |
| common-options    |        | 否    | -                |

### url [string]

ChromaDB 服务地址，例如 `http://localhost:8000`。

### tenant [string]

ChromaDB 租户名。

### database [string]

ChromaDB 数据库名。

### collections [list]

要读取的 collection 名称列表。

### auth_type [string]

认证方式：
- `BEARER`：使用 `Authorization: Bearer <token>` 请求头
- `X_CHROMA_TOKEN`：使用 `X-Chroma-Token: <token>` 请求头
- `BASIC`：使用 `Authorization: Basic base64(username:password)` 请求头

### token [string]

`BEARER` 或 `X_CHROMA_TOKEN` 认证方式的 token。

### username [string]

`BASIC` 认证的用户名。

### password [string]

`BASIC` 认证的密码。

### batch_size [int]

按 ID 列表拉取完整记录时，每次请求的最大 ID 数量。必须为正数，且不超过服务端的请求限制。它与 `id_split_size` 独立配置；Connector 不识别或自动调整服务端配额。

### id_split_size [int]

Reader 在自己的范围内，每次扫描 ID 的最大数量，必须为正数。默认 10000000（1000 万），应根据服务端查询限制及 reader/checkpoint 可用内存调小。该参数控制页大小，不决定 split 或 reader 数量。

### connect_timeout [int]

HTTP 连接超时，单位秒。

### read_timeout [int]

HTTP 读取超时，单位秒。大 collection 可适当调大。

### 通用选项

Source 插件常用参数，具体请参考 [Source 常用选项](../source-common-options.md)。`parallelism` 控制 source reader 数量，默认使用 `env.parallelism`。所有配置的 collection 共享这些 reader，增加 collection 不会新建另一组 reader。

## 数据读取策略

Enumerator 对每个 collection 调用一次 `count` 得到 `N`，按 source 并行度 `P` 创建至多 `P` 个非空范围。第 `i` 个范围为 `[floor(N*i/P), floor(N*(i+1)/P))`。这是 offset 范围，不是 ID 值范围。

每个 reader 在分配到的范围内执行相同的两阶段循环：

1. **ID 扫描**：请求 `include=[]`、`offset=next_offset`、`limit=min(id_split_size, end_offset-next_offset)`。
2. **完整记录读取**：按返回的 ID 分批读取 embeddings、documents、uris、metadatas，每批至多 `batch_size` 条。每批成功输出后推进进度，当前 ID 页消费完后再扫描下一页。

翻页始终在 reader 的范围内完成，不会为每一页创建、分配新的 split。`parallelism=1` 使用同一套循环，范围自然为 `[0, N)`。服务端允许时，设置 `id_split_size >= N` 即可先用一次请求获取全部 ID，再分批读取完整记录。

完整记录通过明确的 ID 读取，不使用 offset 翻页。较大的 ID 页可以减少相对于按 `batch_size` 翻页读取完整记录时的 offset 查询次数。但 ID 扫描本身仍有 offset 成本，实际收益取决于服务端、页大小和并行度。

### Checkpoint 与恢复

Checkpoint 在 enumerator 中保存未分配的范围，在 reader 中保存范围边界、下一个 offset 和未消费的 ID。恢复时保留原范围，优先消费剩余 ID，再扫描下一页。已生成范围的 collection 不会重新 count 或切分，即使恢复时修改了并行度。原范围数少于恢复后的 reader 数时，部分 reader 因此可能空闲。

如果 ID 页不足预期条数，或按 ID 未取回请求的记录，任务会报错，不会把范围静默标记为完成。这能发现部分源数据变化或请求大小配置问题，但无法发现并发修改造成的所有重复或遗漏。最后一次成功 checkpoint 之后输出的记录可能在恢复时重放，不保证 exactly-once。

新的范围 checkpoint 格式与原先仅保存 collection 的格式不兼容。从旧版本升级时需要重新启动迁移，不能恢复旧 checkpoint/savepoint。

## 输出列结构

ChromaDB Source 对每个 collection 产出**固定 5 列**：

| 列名 | 类型 | 可空 | 说明 |
|------|------|------|------|
| `id` | STRING | 否 | ChromaDB 记录 ID（主键） |
| `embedding` | FLOAT_VECTOR | 否 | 向量，维度从 collection 元信息获取 |
| `document` | STRING | 是 | 文档文本 |
| `uri` | STRING | 是 | 资源 URI |
| `Metadata` | STRING (JSON) | 否 | 所有用户自定义 metadata，序列化为 JSON 字符串。`Metadata` 列标记 `metadata=true`，sink 端可据此进行字段展开 |

:::caution 注意
空 collection（没有插入过数据，dimension 未知）会导致任务直接报错失败。ChromaDB 只有在插入第一条 embedding 后才确定向量维度，无法为空 collection 创建目标端 schema。请确保指定的 collection 中有数据。
:::

:::tip 提示
**只读操作**：Connector 读取 collection 元信息、记录数及记录，不修改或删除源数据。

**内存使用**：每个活跃 reader 最多保留 `id_split_size` 个 ID，并受所在范围的剩余记录数限制。Checkpoint 也会保存尚未消费的 ID。调大并行度或页大小时，需要计算同一 VTS 进程内所有 reader 的总内存需求。

**查询压力**：调大并行度会增加单个 collection 内及多个 collection 间的并发读取。应根据源服务和 sink 的承载能力设置，并遵守源服务的请求速率限制。
:::

:::caution 注意
**数据一致性**：Connector 通过 `count` 和 offset 分页读取，不使用共享快照。在切分、读取或恢复期间写入或删除数据，可能改变 offset 对应的位置，造成遗漏或重复；`count` 不能固定数据集。即使一次请求获取所有 ID，也不能固定后续记录的值或防止记录被删除。若要求完整迁移，应在任务结束前保持源数据不变，恢复期间也一样。如果源服务支持隔离的 fork 或快照，可以先自行准备，再配置 Connector 读取；Connector 不会自动创建。
:::

## 使用案例

### 案例一：读取指定 collection

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  ChromaDB {
    url = "http://localhost:8000"
    collections = ["my_collection"]
  }
}
```

### 案例二：Bearer Token 认证

```hocon
source {
  ChromaDB {
    url = "https://my-chroma-server:8000"
    token = "my-secret-token"
    auth_type = "BEARER"
    collections = ["secure_collection"]
  }
}
```

### 案例三：Basic 认证

```hocon
source {
  ChromaDB {
    url = "https://my-chroma-server:8000"
    auth_type = "BASIC"
    username = "admin"
    password = "my-password"
    collections = ["secure_collection"]
  }
}
```

### 案例四：调整读取参数

对于大 collection，可调整 batch 和 timeout 参数：

```hocon
source {
  ChromaDB {
    url = "http://localhost:8000"
    collections = ["large_collection"]
    batch_size = 2000
    id_split_size = 5000000
    read_timeout = 1200
  }
}
```

### 案例五：请求上限较小时并行读取

如果服务端配置允许每次请求最多 300 条记录，可显式设置两个页大小。VTS 不识别部署类型，也不内置该限制值：

```hocon
env {
  parallelism = 4
  job.mode = "BATCH"
}

source {
  ChromaDB {
    url = "https://my-chroma-server"
    collections = ["large_collection"]
    id_split_size = 300
    batch_size = 300
  }
}
```

## 变更日志

<ChangeLog />
