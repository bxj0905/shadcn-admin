# 如何注册 Flow

## 快速开始

### 1. 单个 Flow 注册（通过前端界面）

访问 `data-processing` 页面，点击 "新建 Flow"，填写以下信息：

#### 主 Flow 示例
- **名称**: `dataset-etl-flow`
- **标签**: `dataset-etl`, `main`
- **Labels** (JSON 格式):
```json
{
  "flow_type": "main",
  "entrypoint": "docs.flows.main:dataset_etl_flow",
  "description": "数据集 ETL 处理主流程"
}
```
- **代码**: 粘贴 `docs/flows/main.py` 的内容

#### 功能 Flow 示例
- **名称**: `data-collection-flow`
- **标签**: `dataset-etl`, `feature`
- **Labels** (JSON 格式):
```json
{
  "flow_type": "feature",
  "parent_flow": "dataset-etl-flow",
  "entrypoint": "docs.flows.feature_flows.data_collection_flow:data_collection_flow",
  "description": "数据收集功能 Flow"
}
```
- **代码**: 粘贴 `docs/flows/feature_flows/data_collection_flow.py` 的内容

#### 子 Flow 示例
- **名称**: `rustfs-list-files`
- **标签**: `dataset-etl`, `subflow`
- **Labels** (JSON 格式):
```json
{
  "flow_type": "subflow",
  "parent_flow": "dataset-etl-flow",
  "entrypoint": "docs.flows.main:list_files_flow",
  "description": "兼容历史接口的入口 Flow"
}
```
- **代码**: 粘贴 `docs/flows/main.py` 的内容（包含 `list_files_flow` 函数）

### 2. 批量注册（通过 API）

使用前端代码或 Postman 调用批量注册 API：

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
console.log('注册结果:', results)
```

## Labels 字段说明

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `flow_type` | `'main' \| 'feature' \| 'subflow'` | 是 | Flow 类型 |
| `parent_flow` | `string` | 否 | 父 Flow 的名称（仅 feature 和 subflow 需要） |
| `entrypoint` | `string` | 是 | Flow 的入口点，格式：`模块路径:函数名` |
| `description` | `string` | 否 | Flow 的描述信息 |

## 前端显示效果

注册后，在 `data-processing` 页面会看到：

```
📁 dataset-etl-flow (主 Flow)
  └─ 📄 rustfs-list-files (子 Flow)
  └─ 📄 data-collection-flow (功能 Flow)
  └─ 📄 data-conversion-flow (功能 Flow)
  └─ 📄 data-cleaning-flow (功能 Flow)
  └─ 📄 dataset-validation-flow (功能 Flow)
  └─ 📄 data-encryption-flow (功能 Flow)
  └─ 📄 data-aggregation-flow (功能 Flow)
```

子 Flow 会自动缩进显示在父 Flow 下方。

## 注意事项

1. **Entrypoint 格式**: 必须使用完整的 Python 模块路径
   - ✅ 正确: `docs.flows.main:dataset_etl_flow`
   - ❌ 错误: `main:dataset_etl_flow` 或 `dataset_etl_flow`

2. **Parent Flow 引用**: `parent_flow` 必须使用 Flow 的 `name`，而不是 `id`

3. **文件路径**: 批量注册时，`filePath` 应该是相对于项目根目录的路径

4. **代码读取**: 批量注册 API 会尝试从服务器文件系统读取代码，确保文件存在

## 更新现有 Flow

如果已有 Flow 需要更新 labels：

1. 通过前端界面编辑 Flow
2. 或者通过 Prefect API 直接更新 Flow 的 labels

## 故障排查

### Flow 没有显示为子 Flow

检查：
1. Flow 的 labels 中是否包含 `flow_type: 'feature'` 或 `flow_type: 'subflow'`
2. Flow 的 labels 中是否包含 `parent_flow`，且值正确
3. 父 Flow 是否存在且名称匹配

### 批量注册失败

检查：
1. 文件路径是否正确
2. 文件是否存在
3. 代码格式是否正确
4. Entrypoint 格式是否正确

