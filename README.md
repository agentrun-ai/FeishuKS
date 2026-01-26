# 飞书知识库同步工具

这是一个基于阿里云函数计算的飞书知识库自动同步工具，可以将飞书知识库中的文档自动同步到阿里云OSS存储，支持增量更新和智能跳过未修改文档。

## 系统架构

```mermaid
flowchart TD
    A[定时触发器] --> B[函数计算启动]
    B --> C[权限测试]
    C --> D{权限检查通过?}
    D -->|否| E[返回错误信息]
    D -->|是| F[获取飞书Token]
    F --> G[加载同步记录]
    G --> H[查找知识空间ID]
    H --> I[获取文档节点列表]
    I --> J[遍历文档节点]
    J --> K{需要同步?}
    K -->|否| L[跳过文档]
    K -->|是| M[获取文档内容]
    M --> N[保存到本地]
    N --> O[上传到OSS]
    O --> P[更新同步记录]
    P --> Q{还有文档?}
    Q -->|是| J
    Q -->|否| R[检查删除的文档]
    R --> S[从OSS删除已删除的文档]
    S --> T[保存同步记录到OSS]
    T --> U[返回同步报告]
    
    L --> Q
    
    V[飞书知识库] -.->|API调用| F
    V -.->|API调用| I
    V -.->|API调用| M
    
    W[阿里云OSS] -.->|上传文件| O
    W -.->|删除文件| S
    W -.->|保存记录| T
```

## 前置条件

### 1. 飞书应用配置

#### 创建企业自建应用
1. 登录[飞书开发者后台](https://open.feishu.cn/app)
2. 创建企业自建应用
3. 获取 `App ID` 和 `App Secret`

#### 配置应用权限
在应用的**权限管理**页面，开通以下API权限：

| 权限名称 | 权限标识 | 权限类型 | 说明 |
|---------|---------|---------|------|
| 查看云文档内容 | `docs:document.content:read` | 应用身份 | 必需，用于获取文档内容 |
| 查看知识空间列表 | `wiki:space:retrieve` | 应用身份 | 必需，用于获取知识库列表 |
| 查看、编辑和管理知识库 | `wiki:wiki` | 应用身份 | 必需，用于访问知识库节点 |

#### 授权应用访问知识库

**将应用添加为知识库管理员（成员）**

- 在飞书客户端中创建一个群聊，并将应用添加至群聊中。
- 知识库管理员前往「知识库设置」-> 「成员设置」->「添加管理员」中。
<img width="1920" height="878" alt="image" src="https://github.com/user-attachments/assets/6726568a-5b42-4da1-b61c-ba24c11053f8" />
- 搜索包含机器人的群聊，添加该群为管理员。
<img width="1135" height="838" alt="image" src="https://github.com/user-attachments/assets/63307c5c-c92f-4e90-8309-adfdd7f1f1ce" />

> 详情参考：https://open.feishu.cn/document/server-docs/docs/wiki-v2/wiki-qa#b5da330b

### 2. 阿里云资源准备

- **函数计算服务**：用于运行同步程序
- **OSS存储桶**：用于存储同步的文档文件
- **RAM角色**：函数计算需要访问OSS的权限

## 部署步骤

### 1. 上传代码文件

### 2. 配置环境变量

在函数计算控制台配置以下环境变量：

```json
{
  "FEISHU_APP_ID": "cli_xxxxxxxxxxxxxxxxx",
  "FEISHU_APP_SECRET": "your_app_secret_here",
  "WIKI_SPACE_NAME": "规章制度",
  "OSS_ENDPOINT": "https://oss-cn-beijing.aliyuncs.com",
  "OSS_BUCKET_NAME": "your-knowledge-base-bucket",
  "OSS_PREFIX": "wiki/",
  "MAX_RETRIES": "3",
  "RETRY_DELAY_BASE": "1.0"
}
```

**环境变量说明：**

| 变量名 | 必需 | 说明 | 示例值 |
|-------|------|------|--------|
| `FEISHU_APP_ID` | ✅ | 飞书应用ID | `cli_xxxxxxxxxxxxxxxxx` |
| `FEISHU_APP_SECRET` | ✅ | 飞书应用密钥 | `your_app_secret_here` |
| `WIKI_SPACE_NAME` | ✅* | 知识库名称 | `规章制度` |
| `WIKI_SPACE_ID` | ✅* | 知识库ID | `7599113664066505941` |
| `OSS_ENDPOINT` | ✅ | OSS访问地址 | `https://oss-cn-beijing.aliyuncs.com` |
| `OSS_BUCKET_NAME` | ✅ | OSS存储桶名称 | `your-bucket-name` |
| `OSS_PREFIX` | ❌ | OSS路径前缀 | `wiki/` |
| `MAX_RETRIES` | ❌ | 最大重试次数 | `3` |
| `RETRY_DELAY_BASE` | ❌ | 重试基础延迟(秒) | `1.0` |

> *注：`WIKI_SPACE_NAME` 和 `WIKI_SPACE_ID` 至少提供一个

### 3. 配置触发器

#### 定时触发器（推荐）
```yaml
triggers:
  - name: wiki-sync-timer
    type: timer
    config: 
      cronExpression: "0 0 2 * * *"  # 每天凌晨2点执行
      enable: true
```

#### HTTP触发器（用于手动触发）
```yaml
triggers:
  - name: wiki-sync-http
    type: http
    config:
      authType: anonymous
      methods:
        - GET
        - POST
```

### 4. 设置服务角色

为函数计算服务配置RAM角色，确保具备以下权限：

```json
{
  "Version": "1",
  "Statement": [
    {
      "Action": [
        "oss:PutObject",
        "oss:GetObject",
        "oss:DeleteObject",
        "oss:ListObjects"
      ],
      "Resource": [
        "acs:oss:*:*:your-bucket-name/*"
      ],
      "Effect": "Allow"
    }
  ]
}
```

## 使用方法

### 手动触发
```bash
# 通过HTTP触发器
curl -X POST https://your-function-endpoint.fc.aliyuncs.com/wiki-sync

# 通过控制台测试
# 在函数计算控制台点击"测试函数"
```

### 查看执行结果
成功执行后返回同步报告：
```json
{
  "code": 0,
  "message": "同步完成",
  "space_id": "7599113664066505941",
  "space_name": "规章制度",
  "total_nodes": 15,
  "doc_nodes": 9,
  "successful": 5,
  "failed": 0,
  "skipped": 4,
  "deleted": 0,
  "sync_records_count": 9,
  "api_calls_saved": 4
}
```

### 文件存储结构
同步后的文件在OSS中的存储结构：
```
wiki/
├── 规章制度/
│   ├── 员工管理制度.md
│   ├── 薪酬福利制度.md
│   ├── 人力资源制度.md
│   └── 绩效评估制度.md
├── sync_records.json         # 同步记录文件
└── other-space/
    └── ...
```

## 监控和日志

### 关键日志信息
```log
# 权限测试
✓ 获取access_token成功
✓ 获取知识空间列表成功，共 3 个空间

# 同步优化
文档编辑时间已变化: 1640995200 -> 1640995260 (员工管理制度)
文档无变化，跳过同步: 薪酬福利制度

# 执行统计
同步完成 - 成功: 5, 失败: 0, 跳过: 4, 删除: 0
```

### 错误排查
常见错误及解决方案：

| 错误信息 | 原因 | 解决方案 |
|---------|------|---------|
| `获取access_token失败` | App ID或Secret错误 | 检查环境变量配置 |
| `API权限测试失败` | API权限未开通 | 在开发者后台开通必需权限 |
| `未找到知识空间` | 空间名称错误或无权限 | 检查空间名称，确认应用已获得授权 |
| `获取文档内容失败` | 文档权限不足 | 将应用添加为知识库管理员或文档协作者 |
| `限流错误` | API调用过于频繁 | 程序会自动重试，无需手动处理 |

## 注意事项

1. **权限配置**：确保飞书应用具备足够的API权限，且已被添加为知识库管理员
2. **限流处理**：程序内置了限流重试机制，但建议避免过于频繁的手动触发
3. **文档格式**：目前仅支持新版文档(docx)，其他类型文档将被跳过
4. **中文支持**：OSS完全支持中文文件名，无需担心编码问题
5. **成本优化**：通过编辑时间比较大幅减少API调用，降低使用成本

## 故障排除

### 权限问题
如果遇到权限相关错误，请按以下步骤检查：
1. 确认应用已开通必需的API权限
2. 确认应用已被添加为知识库管理员或协作者
3. 查看函数执行日志中的权限测试结果

### 限流问题
程序内置了限流处理机制：
- 自动检测限流错误并重试
- 使用指数退避算法避免连续触发限流
- 合理控制并发数

### 同步异常
如果发现同步结果异常：
1. 检查`sync_records.json`文件内容
2. 查看函数执行日志
3. 手动触发单次同步进行测试

---

通过这个工具，您可以实现飞书知识库到阿里云OSS的自动化同步，为后续的知识处理和AI应用提供支持。


# OSS 触发器函数

这是一个基于阿里云函数计算的OSS事件处理函数，用于监听OSS存储桶中的文件变更事件，并自动同步到AnalyticDB PostgreSQL的RAG知识库中。

## 系统架构

```mermaid
flowchart TD
    A[OSS文件变更] --> B[OSS触发器]
    B --> C[函数计算启动]
    C --> D[解析OSS事件]
    D --> E{事件类型判断}
    
    E -->|ObjectCreated| F[文件创建处理]
    E -->|ObjectModified| G[文件更新处理]
    E -->|ObjectRemoved| H[文件删除处理]
    
    F --> I[提取文件信息]
    G --> J[删除旧文档]
    H --> M[删除ADB文档]
    
    I --> K[上传到ADB知识库]
    J --> L[上传新文档到ADB知识库]
    
    K --> N[返回处理结果]
    L --> N
    M --> N
    
    O[AnalyticDB PostgreSQL] -.->|RAG Service API| K
    O -.->|RAG Service API| L  
    O -.->|RAG Service API| M
```

## 功能特点

- **实时响应**：OSS文件变更后立即触发处理
- **智能过滤**：支持文件类型和路径前缀过滤
- **完整的CRUD操作**：
  - 新建文件 → 创建知识库文档
  - 更新文件 → 删除旧文档 + 创建新文档
  - 删除文件 → 删除知识库文档
- **元数据保留**：自动提取文件路径信息作为元数据
- **错误处理**：完善的异常处理和重试机制

## 前置条件

### 1. AnalyticDB PostgreSQL 实例

确保已创建并配置了支持RAG Service的AnalyticDB PostgreSQL实例：

1. 实例版本需支持向量引擎
2. 已创建文档集合(Collection)
3. 已配置命名空间和密码

### 2. OSS存储桶

- 已创建OSS存储桶
- 存储桶与函数计算在同一区域
- 配置了适当的访问权限

### 3. 函数计算权限

函数计算服务角色需要具备以下权限：

```json
{
  "Version": "1",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "gpdb:UploadDocumentAsync",
        "gpdb:DeleteDocument",
        "gpdb:DescribeDBInstances"
      ],
      "Resource": "acs:gpdb:*:*:dbinstance/gp-*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "oss:GetObject"
      ],
      "Resource": "acs:oss:*:*:your-bucket-name/*"
    }
  ]
}
```

## 部署配置

### 1. 环境变量配置

```json
{
  "GPDB_INSTANCE_ID": "gp-xxxxxxxxx",
  "GPDB_REGION_ID": "cn-hangzhou",
  "GPDB_COLLECTION": "document",
  "GPDB_NAMESPACE": "public", 
  "GPDB_NAMESPACE_PASSWORD": "testpassword",
  "GPDB_ENDPOINT": "https://gpdb.cn-hangzhou.aliyuncs.com",
  "OSS_TRIGGER_BUCKET": "your-bucket-name",
  "OSS_PREFIX_FILTER": "wiki/"
}
```

**环境变量说明：**

| 变量名 | 必需 | 说明 | 示例值 |
|-------|------|------|--------|
| `GPDB_INSTANCE_ID` | ✅ | AnalyticDB实例ID | `gp-xxxxxxxxx` |
| `GPDB_REGION_ID` | ✅ | 实例所在区域 | `cn-hangzhou` |
| `GPDB_COLLECTION` | ✅ | 文档集合名称 | `document` |
| `GPDB_NAMESPACE` | ❌ | 命名空间 | `public` |
| `GPDB_NAMESPACE_PASSWORD` | ✅ | 命名空间密码 | `testpassword` |
| `GPDB_ENDPOINT` | ❌ | ADB服务端点 | `https://gpdb.cn-hangzhou.aliyuncs.com` |
| `OSS_TRIGGER_BUCKET` | ❌ | 触发的OSS桶名 | `your-bucket-name` |
| `OSS_PREFIX_FILTER` | ❌ | 文件路径前缀过滤 | `wiki/` |

### 2. 创建OSS触发器

在函数计算控制台为函数配置OSS触发器：

```yaml
triggers:
  - name: oss-trigger
    type: oss
    config:
      bucketName: your-bucket-name
      events:
        - oss:ObjectCreated:*
        - oss:ObjectRemoved:*
      prefix: wiki/
      suffix: .md
```

**触发器配置说明：**

- **bucketName**: 监听的OSS存储桶名称
- **events**: 监听的事件类型
  - `oss:ObjectCreated:*`: 所有文件创建事件
  - `oss:ObjectRemoved:*`: 所有文件删除事件
- **prefix**: 只处理指定前缀的文件
- **suffix**: 只处理指定后缀的文件

### 3. 支持的文件类型

函数会自动过滤并处理以下文件类型：

- `.md` - Markdown文档
- `.txt` - 纯文本文档  
- `.pdf` - PDF文档
- `.docx` - Word文档
- `.doc` - 旧版Word文档
- `.html` - HTML文档
- `.json` - JSON文档
- `.csv` - CSV表格文档

## 处理逻辑

### 文件创建事件 (ObjectCreated)

```mermaid
flowchart TD
    A[收到创建事件] --> B[提取文件信息]
    B --> C{文件类型检查}
    C -->|支持| D[生成文件URL]
    C -->|不支持| E[跳过处理]
    D --> F[提取路径元数据]
    F --> G[调用UploadDocumentAsync API]
    G --> H[返回JobId]
```

处理步骤：
1. 从OSS事件中提取文件信息（桶名、文件路径、文件名）
2. 检查文件是否符合处理条件（路径前缀、文件类型）
3. 生成文件的访问URL
4. 从文件路径中提取元数据信息
5. 调用AnalyticDB的异步文档上传API
6. 返回上传任务ID

### 文件更新事件 (ObjectModified)

```mermaid
flowchart TD
    A[收到更新事件] --> B[提取文件信息]
    B --> C{文件类型检查}
    C -->|支持| D[调用DeleteDocument API]
    C -->|不支持| E[跳过处理]
    D --> F[调用UploadDocumentAsync API]
    F --> G[返回处理结果]
```

处理步骤：
1. 先删除AnalyticDB中的旧文档
2. 再上传新文档内容
3. 确保文档内容完全更新

### 文件删除事件 (ObjectRemoved)

```mermaid
flowchart TD
    A[收到删除事件] --> B[提取文件信息]
    B --> C{文件类型检查}
    C -->|支持| D[调用DeleteDocument API]
    C -->|不支持| E[跳过处理]
    D --> F[返回删除结果]
```

处理步骤：
1. 从AnalyticDB中删除对应的文档
2. 确保知识库内容与OSS保持同步

## 使用示例

### 测试文件上传
```bash
# 上传一个Markdown文件到OSS触发处理
aws s3 cp test-document.md s3://your-bucket-name/wiki/规章制度/测试文档.md
```

### 查看处理结果
函数执行后会返回处理结果：

```json
{
  "statusCode": 200,
  "body": {
    "success": true,
    "message": "OSS事件处理完成",
    "event_name": "ObjectCreated:PutObject",
    "file_info": {
      "bucket_name": "your-bucket-name",
      "object_key": "wiki/规章制度/测试文档.md",
      "file_name": "测试文档.md",
      "file_url": "https://your-bucket-name.oss-cn-hangzhou.aliyuncs.com/wiki/规章制度/测试文档.md"
    },
    "result": {
      "action": "upload",
      "status": "success",
      "job_id": "231460f8-75dc-405e-a669-0c5204887e91"
    }
  }
}
```

## 监控和日志

### 关键日志信息

```log
# 事件处理开始
开始处理OSS触发事件
处理事件: ObjectCreated:PutObject, 文件: wiki/规章制度/员工管理制度.md

# 文件处理
处理文件创建事件: wiki/规章制度/员工管理制度.md
文档上传任务已提交 - JobId: 231460f8-75dc-405e-a669-0c5204887e91

# 处理完成
事件处理完成: {"action": "upload", "status": "success", "job_id": "231460f8-75dc-405e-a669-0c5204887e91"}
```

### 错误排查

常见错误及解决方案：

| 错误信息 | 原因 | 解决方案 |
|---------|------|---------|
| `解析OSS事件失败` | 事件格式异常 | 检查OSS触发器配置 |
| `API调用失败` | ADB实例或权限问题 | 验证实例ID和访问权限 |
| `文件不在处理范围内` | 路径前缀不匹配 | 检查`OSS_PREFIX_FILTER`配置 |
| `不支持的文件类型` | 文件扩展名不在支持列表中 | 检查文件类型或修改过滤逻辑 |

## 性能优化

### 1. 并发控制
- 函数默认并发度为100，可根据ADB实例性能调整
- 建议设置合理的预留并发避免冷启动

### 2. 文档处理参数
可在代码中调整以下参数优化处理效果：

```python
params = {
    'ChunkSize': 500,  # 文档分块大小
    'ChunkOverlap': 50,  # 分块重叠大小
    'TextSplitterName': 'ChineseRecursiveTextSplitter',  # 中文分词器
    'DocumentLoaderName': 'UnstructuredFileLoader'  # 文档加载器
}
```

### 3. 成本控制
- 合理设置文件过滤规则，避免处理无关文件
- 使用异步上传API，提高处理效率
- 监控AnalyticDB的API调用量

## 扩展功能

### 支持更多事件类型
可扩展支持以下OSS事件：
- `oss:ObjectCreated:Copy` - 文件复制
- `oss:ObjectCreated:CompleteMultipartUpload` - 分片上传完成

### 批量处理
对于大量文件的批量处理，可以：
1. 使用OSS Inventory功能获取文件清单
2. 通过函数计算的异步调用处理批量任务
3. 实现进度跟踪和断点续传

## 注意事项

1. **文件访问权限**：确保AnalyticDB可以访问OSS中的文件URL
2. **网络连通性**：函数计算与AnalyticDB之间网络要畅通
3. **文档大小限制**：单个文档建议不超过100MB
4. **API限流**：注意AnalyticDB API的调用频率限制
5. **异步处理**：文档上传是异步的，可通过GetUploadDocumentJob查询状态

---

通过OSS触发器函数，您可以实现文件变更的实时响应，自动将文档同步到AnalyticDB PostgreSQL的RAG知识库中，为AI应用提供实时的知识更新支持。
