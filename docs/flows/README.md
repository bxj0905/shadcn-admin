# 数据集 ETL 处理流程 - Flow 模块化结构

## 目录结构

```
flows/
├── __init__.py                    # 模块入口
├── main.py                        # 主 Flow
├── utils/                         # 工具函数
│   ├── __init__.py
│   ├── s3_utils.py               # S3 相关工具
│   ├── file_utils.py             # 文件处理工具
│   └── config_utils.py           # 配置加载工具
├── tasks/                         # 单个任务
│   ├── __init__.py
│   ├── collect_raw_files.py      # 收集原始文件
│   ├── convert_to_parquet.py    # 转换为 Parquet（待完善）
│   ├── clean_data.py             # 数据清洗（待完善）
│   ├── upload_processed_files.py # 上传处理后的文件（待完善）
│   ├── encrypt_sensitive_fields.py # 加密敏感字段（待完善）
│   └── aggregate_data.py         # 数据聚合（待完善）
└── feature_flows/                # 功能 Flow
    ├── __init__.py
    ├── data_collection_flow.py   # 数据收集功能 Flow
    ├── data_conversion_flow.py   # 数据转换功能 Flow（待完善）
    ├── data_cleaning_flow.py     # 数据清洗功能 Flow（待完善）
    ├── data_validation_flow.py    # 数据验证功能 Flow（待完善）
    ├── data_encryption_flow.py   # 数据加密功能 Flow（待完善）
    └── data_aggregation_flow.py  # 数据聚合功能 Flow（待完善）
```

## 设计思路

### 1. 最小单任务 Flow（tasks/）
每个文件包含一个独立的 Prefect `@task`，负责单一职责：
- `collect_raw_files`: 从 S3 收集原始文件
- `convert_to_parquet`: 将 CSV/Excel 转换为 Parquet
- `clean_data`: 数据清洗和规范化
- `upload_processed_files`: 上传处理后的文件
- `encrypt_sensitive_fields`: 加密敏感字段
- `aggregate_data`: 数据聚合

### 2. 功能 Flow（feature_flows/）
将同功能的单个任务串联成功能 Flow：
- `data_collection_flow`: 数据收集功能
- `data_conversion_flow`: 数据转换功能
- `data_cleaning_flow`: 数据清洗功能
- `data_validation_flow`: 数据验证和校准功能
- `data_encryption_flow`: 数据加密功能
- `data_aggregation_flow`: 数据聚合功能

### 3. 主 Flow（main.py）
创建大的主 Flow 来调用所有功能 Flow：
- `dataset_etl_flow`: 主 ETL 流程
- `list_files_flow`: 兼容历史接口的入口 Flow

## 使用方式

### 导入主 Flow
```python
from docs.flows import dataset_etl_flow, list_files_flow

# 使用主 Flow
result = dataset_etl_flow(prefix="team-1/dataset-2/sourcedata/")
```

### 导入单个功能 Flow
```python
from docs.flows.feature_flows import data_collection_flow

# 使用功能 Flow
files = data_collection_flow(prefix="team-1/dataset-2/sourcedata/")
```

### 导入单个任务
```python
from docs.flows.tasks import collect_raw_files

# 使用单个任务
files = collect_raw_files(prefix="team-1/dataset-2/sourcedata/")
```

## Flow 注册

### 注册方式

现在支持通过 **labels** 动态标识 Flow 的层级关系，不再需要硬编码。

**详细注册指南**: 请查看 [REGISTRATION_GUIDE.md](./REGISTRATION_GUIDE.md) 和 [HOW_TO_REGISTER.md](./HOW_TO_REGISTER.md)

### 快速注册

1. **单个注册**: 通过前端 `data-processing` 页面，点击 "新建 Flow"
2. **批量注册**: 使用 `POST /api/prefect/flows/batch-register` API

### Labels 规范

- **主 Flow**: `flow_type: 'main'`
- **功能 Flow**: `flow_type: 'feature'`, `parent_flow: '主Flow名称'`
- **子 Flow**: `flow_type: 'subflow'`, `parent_flow: '主Flow名称'`

前端会自动根据 labels 识别并显示 Flow 的层级关系。

## 待完善的任务

以下任务需要从原始文件 `dataset_etl_flow.py` 中迁移：

1. **tasks/convert_to_parquet.py** - 从第 510 行开始
2. **tasks/clean_data.py** - 从第 728 行开始
3. **tasks/upload_processed_files.py** - 从第 1089 行开始
4. **tasks/encrypt_sensitive_fields.py** - 从第 2182 行开始
5. **tasks/aggregate_data.py** - 从第 2235 行开始
6. **feature_flows/data_validation_flow.py** - 从第 1170 行开始（完整的验证逻辑）

## 迁移步骤

1. 从原始文件读取对应的函数代码
2. 更新导入语句，使用新的工具函数模块
3. 确保函数签名和返回值保持一致
4. 更新功能 Flow 调用对应的任务
5. 更新主 Flow 调用对应的功能 Flow

## 注意事项

- 所有工具函数已迁移到 `utils/` 模块
- 确保导入路径正确
- 保持函数签名和返回值与原始版本一致
- 测试每个模块的功能是否正常

