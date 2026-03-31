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
- [x] [并行度](../../concept/connector-v2-features.md)（多 collection 间并行）
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

按 ID 列表批量拉取完整数据的每批大小。受 ChromaDB 底层 SQLite `SQLITE_MAX_VARIABLE_NUMBER` 限制（当前版本为 32766），不应超过此值。

### id_split_size [int]

每次 ID 扫描的数量。默认 10000000（1000 万），足以一次扫完大多数 collection。内存不足时可调小。

### connect_timeout [int]

HTTP 连接超时，单位秒。

### read_timeout [int]

HTTP 读取超时，单位秒。大 collection 可适当调大。

### 通用选项

Source 插件常用参数，具体请参考 [Source 常用选项](../source-common-options.md)

## 数据读取策略

Connector 采用两阶段读取：

1. **阶段 1（ID 扫描）**：通过 `offset+limit` 分页获取所有记录 ID（`include=[]`），每次获取 `id_split_size` 条
2. **阶段 2（批量拉取）**：按 ID 列表批量获取完整记录（embeddings、documents、uris、metadatas），每批 `batch_size` 条

这种方式避免了 `offset` 深翻页的性能退化。

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
**只读操作**：Connector 对 ChromaDB 仅执行查询操作（GET collection 信息 + POST get 读取记录），不会修改或删除源端数据。

**内存使用**：`id_split_size` 默认 1000 万，会将对应数量的 ID 字符串一次性加载到 VTS 进程内存中。如果 collection 非常大且 VTS 内存有限，建议调小此参数。

**对 ChromaDB 的查询压力**：ChromaDB 为单节点 SQLite 架构，单个 collection 内的读取是顺序执行的。当 `parallelism > 1` 且指定了多个 collection 时，多个 collection 会并行读取，对 ChromaDB 产生并发查询压力。建议迁移大量数据时保持 `parallelism = 1`，或确保 ChromaDB 有足够资源应对并发请求。
:::

:::caution 注意
**数据一致性**：ChromaDB 不提供快照或游标机制。Connector 通过 `offset+limit` 分页扫描 ID，再按 ID 列表拉取完整数据。当 `id_split_size` 足够大（默认 1000 万）能一次扫完所有 ID 时，ID 列表相当于一个时间点快照，后续按 ID 取数据不受并发写入影响。只有当 collection 记录数超过 `id_split_size` 需要多轮扫描时，两轮之间的写入或删除才可能导致数据重复或遗漏。对于超大 collection，建议迁移期间暂停写入，或调大 `id_split_size`（需注意内存开销）。
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

## 变更日志

<ChangeLog />
