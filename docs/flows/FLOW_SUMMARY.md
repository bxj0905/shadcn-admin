# Flow 流程总结文档

## 一、整体架构概述

### 1.1 三层架构设计

```
┌─────────────────────────────────────────┐
│           主 Flow 层                      │
│  dataset-etl-flow (主流程)               │
│  rustfs-list-files (兼容入口)            │
└─────────────────────────────────────────┘
                  ↓
┌─────────────────────────────────────────┐
│         功能 Flow 层 (Feature Flows)     │
│  - data-collection-flow (数据收集) ✅    │
│  - data-conversion-flow (数据转换) ⏳    │
│  - data-cleaning-flow (数据清洗) ⏳      │
│  - data-validation-flow (数据验证) 🔵    │
│  - data-encryption-flow (数据加密) ⏳    │
│  - data-aggregation-flow (数据聚合) ⏳   │
└─────────────────────────────────────────┘
                  ↓
┌─────────────────────────────────────────┐
│           任务层 (Tasks)                  │
│  - collect_raw_files ✅                 │
│  - convert_to_parquet ⏳                 │
│  - clean_data ⏳                         │
│  - upload_processed_files ⏳             │
│  - encrypt_sensitive_fields ⏳           │
│  - aggregate_data ⏳                     │
└─────────────────────────────────────────┘
                  ↓
┌─────────────────────────────────────────┐
│          工具模块 (Utils)                 │
│  - s3_utils.py (S3 操作)                 │
│  - file_utils.py (文件处理)              │
│  - config_utils.py (配置加载)            │
└─────────────────────────────────────────┘
```

### 1.2 状态说明
- ✅ **已实现**: 代码完整，可以直接使用
- 🔵 **部分实现**: 代码框架存在，核心功能需要完善
- ⏳ **待完善**: 仅有占位代码，需要从原始文件迁移实现

## 二、执行流程详解

### 2.1 完整 ETL 流程

```
输入: prefix (例如: "team-1/dataset-2/sourcedata/")
  │
  ├─ 步骤 1: 数据收集 (data-collection-flow) ✅
  │   └─ collect_raw_files
  │       ├─ 从 S3 列出文件 (list_objects)
  │       ├─ 过滤文件 (.csv/.xlsx/.xls)
  │       └─ 生成 uploaded_files 列表
  │           └─ 输出: [{relative_path, target_key, bucket, uploaded}]
  │
  ├─ 步骤 2: 数据转换 (data-conversion-flow) ⏳
  │   └─ convert_to_parquet
  │       ├─ 读取 CSV/Excel 文件
  │       ├─ 转换为 DataFrame
  │       ├─ 写入 Parquet 格式
  │       └─ 输出到 raw_extracted 区
  │           └─ 输出: parquet_files 列表
  │
  ├─ 步骤 3: 数据清洗 (data-cleaning-flow) ⏳
  │   └─ clean_data
  │       ├─ 加载配置 (column_types.yaml)
  │       ├─ 规范数据类型 (DECIMAL/INTEGER/TEXT/BOOLEAN)
  │       ├─ 处理缺失值
  │       └─ 输出: cleaned_files 列表
  │
  ├─ 步骤 3.5: 数据验证 (data-validation-flow) 🔵
  │   └─ data_validation_flow
  │       ├─ 构建权威表 (从 611/601 表)
  │       ├─ 通过 code/name 匹配校准
  │       ├─ 检测问题 (code截断/一对多/缺失code)
  │       └─ 循环验证直到无问题
  │           └─ 输出: validated_files 列表
  │
  ├─ 步骤 4: 数据加密 (data-encryption-flow) ⏳
  │   └─ encrypt_sensitive_fields
  │       ├─ 加载配置 (sensitive_fields.yaml)
  │       ├─ SM2 加密敏感字段
  │       └─ 输出: secure_files 列表
  │
  └─ 步骤 5: 数据聚合 (data-aggregation-flow) ⏳
      └─ aggregate_data
          ├─ 数据汇总统计
          ├─ upload_processed_files (上传到 S3)
          └─ 输出: 最终处理结果

输出: 处理结果字典
```

### 2.2 数据流转过程

```
prefix (字符串)
    ↓
uploaded_files (列表)
    ↓ [包含 relative_path, target_key, bucket, uploaded]
parquet_files (列表)
    ↓ [Parquet 格式文件路径]
cleaned_files (列表)
    ↓ [清洗后的文件路径]
validated_files (列表)
    ↓ [验证后的文件路径]
secure_files (列表)
    ↓ [加密后的文件路径]
最终结果 (字典)
    ↓ [包含统计信息和文件信息]
```

## 三、Flow 注册流程

### 3.1 注册方式

#### 方式 1: 前端界面注册（推荐单个注册）
1. 访问 `data-processing` 页面
2. 点击 "新建 Flow"
3. 填写 Flow 信息：
   - **名称**: Flow 名称
   - **标签**: 添加 `dataset-etl` 和类型标签
   - **Labels**: JSON 格式的 labels
   - **代码**: 粘贴 Flow 代码

#### 方式 2: 批量注册 API（推荐批量注册）
- 端点: `POST /api/prefect/flows/batch-register`
- 从 `register_flows.py` 的 `FLOW_REGISTRY` 读取配置
- 自动读取代码文件并注册

#### 方式 3: 注册脚本（参考指南）
- 运行 `python docs/flows/register_flows.py`
- 打印注册指南，供手动参考

### 3.2 Labels 规范

#### 主 Flow
```json
{
  "flow_type": "main",
  "entrypoint": "docs.flows.main:dataset_etl_flow",
  "description": "数据集 ETL 处理主流程"
}
```

#### 功能 Flow
```json
{
  "flow_type": "feature",
  "parent_flow": "dataset-etl-flow",
  "entrypoint": "docs.flows.feature_flows.data_collection_flow:data_collection_flow",
  "description": "数据收集功能 Flow"
}
```

#### 子 Flow
```json
{
  "flow_type": "subflow",
  "parent_flow": "dataset-etl-flow",
  "entrypoint": "docs.flows.main:list_files_flow",
  "description": "兼容历史接口的入口 Flow"
}
```

### 3.3 前端显示效果

```
📁 dataset-etl-flow (主 Flow)
  ├─ 📄 rustfs-list-files (子 Flow)
  ├─ 📄 data-collection-flow (功能 Flow) ✅
  ├─ 📄 data-conversion-flow (功能 Flow) ⏳
  ├─ 📄 data-cleaning-flow (功能 Flow) ⏳
  ├─ 📄 dataset-validation-flow (功能 Flow) 🔵
  ├─ 📄 data-encryption-flow (功能 Flow) ⏳
  └─ 📄 data-aggregation-flow (功能 Flow) ⏳
```

## 四、配置文件系统

### 4.1 配置文件位置

配置文件位于 `docs/flows/config/` 目录：

- `column_types.yaml` - 字段类型映射配置
- `sensitive_fields.yaml` - 敏感字段列表配置
- `encrypt.yaml` - 加密相关配置
- 其他配置文件...

### 4.2 配置加载优先级

```
优先级 1: S3 配置
  └─ dataflow/config/column_types.yaml
  └─ dataflow/config/sensitive_fields.yaml
     ↓ (如果不存在)
优先级 2: 默认配置
  └─ config_utils._get_default_column_types()
  └─ config_utils._get_default_sensitive_fields()
```

### 4.3 配置文件使用场景

| 配置文件 | 使用场景 | 加载函数 |
|---------|---------|---------|
| column_types.yaml | 数据清洗时规范字段类型 | `load_sql_column_types()` |
| sensitive_fields.yaml | 数据加密时识别敏感字段 | `load_sensitive_fields()` |
| encrypt.yaml | 数据加密时的加密配置 | (待实现) |

## 五、目录结构说明

```
docs/flows/
├── __init__.py                 # 模块入口，导出主 Flow
├── main.py                     # 主 Flow 定义
├── register_flows.py           # Flow 注册脚本和配置
├── README.md                   # 项目说明文档
├── HOW_TO_REGISTER.md          # 注册指南
├── REGISTRATION_GUIDE.md       # 注册详细说明
├── FLOW_DIAGRAM.md             # 流程图文档（本文件）
├── FLOW_SUMMARY.md             # 流程总结文档
│
├── config/                     # 配置文件目录
│   ├── column_types.yaml       # 字段类型配置
│   ├── sensitive_fields.yaml   # 敏感字段配置
│   ├── encrypt.yaml            # 加密配置
│   └── ...                     # 其他配置
│
├── tasks/                      # 任务层（最小执行单元）
│   ├── __init__.py
│   ├── collect_raw_files.py           ✅ 已实现
│   ├── convert_to_parquet.py          ⏳ 待完善
│   ├── clean_data.py                  ⏳ 待完善
│   ├── upload_processed_files.py      ⏳ 待完善
│   ├── encrypt_sensitive_fields.py    ⏳ 待完善
│   └── aggregate_data.py              ⏳ 待完善
│
├── feature_flows/              # 功能 Flow 层
│   ├── __init__.py
│   ├── data_collection_flow.py        ✅ 已实现
│   ├── data_conversion_flow.py        ⏳ 待完善
│   ├── data_cleaning_flow.py          ⏳ 待完善
│   ├── data_validation_flow.py        🔵 部分实现
│   ├── data_encryption_flow.py        ⏳ 待完善
│   └── data_aggregation_flow.py       ⏳ 待完善
│
└── utils/                      # 工具模块
    ├── __init__.py
    ├── s3_utils.py             # S3 操作工具
    ├── file_utils.py           # 文件处理工具
    └── config_utils.py         # 配置加载工具
```

## 六、待完善任务清单

### 6.1 任务迁移清单

需要从原始 `dataset_etl_flow.py` 文件迁移以下任务：

1. **tasks/convert_to_parquet.py**
   - 起始位置: 第 510 行
   - 功能: 将 CSV/Excel 转换为 Parquet

2. **tasks/clean_data.py**
   - 起始位置: 第 728 行
   - 功能: 数据清洗和规范化

3. **tasks/upload_processed_files.py**
   - 起始位置: 第 1089 行
   - 功能: 上传处理后的文件到 S3

4. **tasks/encrypt_sensitive_fields.py**
   - 起始位置: 第 2182 行
   - 功能: 加密敏感字段

5. **tasks/aggregate_data.py**
   - 起始位置: 第 2235 行
   - 功能: 数据聚合统计

6. **feature_flows/data_validation_flow.py**
   - 起始位置: 第 1170 行
   - 功能: 完整的验证和校准逻辑

### 6.2 迁移步骤

1. 从原始文件读取对应的函数代码
2. 更新导入语句，使用新的工具函数模块
3. 确保函数签名和返回值保持一致
4. 更新功能 Flow 调用对应的任务
5. 更新主 Flow 调用对应的功能 Flow
6. 测试每个模块的功能是否正常

## 七、关键概念说明

### 7.1 Flow 层级关系

- **主 Flow (Main Flow)**: 
  - `flow_type: 'main'`
  - 整个 ETL 流程的入口
  - 协调所有功能 Flow 的执行

- **功能 Flow (Feature Flow)**: 
  - `flow_type: 'feature'`
  - `parent_flow: '主Flow名称'`
  - 将相关任务组合成完整功能
  - 可独立部署和运行

- **子 Flow (Sub Flow)**: 
  - `flow_type: 'subflow'`
  - `parent_flow: '主Flow名称'`
  - 通常用于兼容历史接口
  - 内部调用主 Flow 或功能 Flow

### 7.2 任务 (Task) 特点

- 最小的执行单元
- 单一职责原则
- 使用 `@task` 装饰器
- 可被多个功能 Flow 复用

### 7.3 工具模块 (Utils) 特点

- 可复用的工具函数
- 不包含 Prefect 装饰器
- 被任务和功能 Flow 调用
- 独立于业务逻辑

## 八、使用示例

### 8.1 导入主 Flow

```python
from docs.flows import dataset_etl_flow

# 执行完整 ETL 流程
result = dataset_etl_flow(prefix="team-1/dataset-2/sourcedata/")
```

### 8.2 导入功能 Flow

```python
from docs.flows.feature_flows import data_collection_flow

# 只执行数据收集
files = data_collection_flow(prefix="team-1/dataset-2/sourcedata/")
```

### 8.3 导入单个任务

```python
from docs.flows.tasks import collect_raw_files

# 只执行收集原始文件任务
files = collect_raw_files(prefix="team-1/dataset-2/sourcedata/")
```

## 九、注意事项

1. **Entrypoint 格式**: 必须使用完整的 Python 模块路径
   - ✅ 正确: `docs.flows.main:dataset_etl_flow`
   - ❌ 错误: `main:dataset_etl_flow` 或 `dataset_etl_flow`

2. **Parent Flow 引用**: `parent_flow` 必须使用 Flow 的 `name`，而不是 `id`

3. **文件路径**: 批量注册时，`filePath` 应该是相对于项目根目录的路径

4. **代码读取**: 批量注册 API 会尝试从服务器文件系统读取代码，确保文件存在

5. **Labels 设置**: Labels 必须在创建 Flow 时设置，后续无法通过代码修改

6. **配置加载**: 配置文件优先从 S3 读取，如果不存在则使用代码内嵌的默认配置

