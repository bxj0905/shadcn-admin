# Flow 注册指南

## 概述

现在支持通过 labels 动态标识 Flow 的层级关系，不再需要硬编码。系统会根据 Flow 的 labels 自动识别主 Flow、功能 Flow 和子 Flow 的关系。

## Flow Labels 规范

每个 Flow 在注册时应该包含以下 labels：

### 主 Flow (flow_type: 'main')
```json
{
  "flow_type": "main",
  "entrypoint": "docs.flows.main:dataset_etl_flow",
  "description": "数据集 ETL 处理主流程"
}
```

### 功能 Flow (flow_type: 'feature')
```json
{
  "flow_type": "feature",
  "parent_flow": "dataset-etl-flow",
  "entrypoint": "docs.flows.feature_flows.data_collection_flow:data_collection_flow",
  "description": "数据收集功能 Flow"
}
```

### 子 Flow (flow_type: 'subflow')
```json
{
  "flow_type": "subflow",
  "parent_flow": "dataset-etl-flow",
  "entrypoint": "docs.flows.main:list_files_flow",
  "description": "兼容历史接口的入口 Flow"
}
```

## 注册方法

### 方法 1: 通过前端界面单个注册

1. 访问 `data-processing` 页面
2. 点击 "新建 Flow" 按钮
3. 填写 Flow 信息：
   - **名称**: Flow 名称（如 `data-collection-flow`）
   - **标签**: 添加 `dataset-etl` 和对应的类型标签（`main`、`feature` 或 `subflow`）
   - **Labels**: 在代码中设置 labels（见下方示例）
   - **代码**: 粘贴 Flow 代码

**示例代码（在 Flow 定义中设置 labels）**:
```python
from prefect import flow

@flow(
    name="data-collection-flow",
    # Prefect 3.x 中，labels 需要在创建 Flow 时通过 API 设置
    # 而不是在装饰器中设置
)
def data_collection_flow(prefix: str = ""):
    """数据收集功能 Flow"""
    # ... 实现代码
```

**注意**: 在 Prefect 3.x 中，labels 是通过 API 在创建 Flow 时设置的，而不是在代码中。所以需要在创建 Flow 时通过前端界面或 API 设置 labels。

### 方法 2: 通过后端 API 批量注册

使用 `POST /api/prefect/flows/batch-register` 端点：

```typescript
import { batchRegisterPrefectFlows } from '@/services/prefect'

const flows = [
  {
    name: 'dataset-etl-flow',
    entrypoint: 'docs.flows.main:dataset_etl_flow',
    filePath: 'docs/flows/main.py',
    flowType: 'main',
    description: '数据集 ETL 处理主流程',
    tags: ['dataset-etl', 'main'],
  },
  {
    name: 'data-collection-flow',
    entrypoint: 'docs.flows.feature_flows.data_collection_flow:data_collection_flow',
    filePath: 'docs/flows/feature_flows/data_collection_flow.py',
    flowType: 'feature',
    parentFlow: 'dataset-etl-flow',
    description: '数据收集功能 Flow',
    tags: ['dataset-etl', 'feature'],
  },
  // ... 更多 Flow
]

const results = await batchRegisterPrefectFlows(flows)
```

### 方法 3: 使用注册脚本（需要实现）

运行 `docs/flows/register_flows.py` 脚本（需要先实现从文件系统读取代码的功能）：

```bash
python docs/flows/register_flows.py
```

## 前端显示逻辑

前端会根据以下规则自动组织 Flow 的显示：

1. **识别主 Flow**: `flow_type === 'main'` 的 Flow 作为主 Flow
2. **识别子 Flow**: `flow_type === 'feature'` 或 `flow_type === 'subflow'` 的 Flow 作为子 Flow
3. **建立关系**: 通过 `parent_flow` label 找到父 Flow 的名称，然后通过名称找到父 Flow 的 ID
4. **排序显示**: 主 Flow 在前，其子 Flow 紧跟其后（缩进显示）

## 完整的 Flow 注册配置

参考 `docs/flows/register_flows.py` 中的 `FLOW_REGISTRY` 配置，包含所有需要注册的 Flow。

## 注意事项

1. **Entrypoint 格式**: 必须使用完整的模块路径，如 `docs.flows.main:dataset_etl_flow`
2. **文件路径**: 在批量注册时，需要确保文件路径相对于项目根目录
3. **代码读取**: 批量注册 API 会尝试从文件路径读取代码，如果文件不存在会报错
4. **Labels 设置**: Labels 必须在创建 Flow 时设置，后续无法通过代码修改

## 迁移现有 Flow

如果已有 Flow 没有设置正确的 labels，可以通过以下方式更新：

1. 通过前端界面编辑 Flow，更新 labels
2. 或者通过 Prefect API 直接更新 Flow 的 labels

