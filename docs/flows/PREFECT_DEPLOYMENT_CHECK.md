# Prefect Deployment 目录结构检查

## 当前目录结构

```
S3 bucket (dataflow)
└── my-flow/                    (flow 名称作为前缀)
    └── flow/
        ├── main.py             (flow 代码)
        ├── __init__.py         (必须存在，使 flow 成为包)
        ├── tasks/
        │   ├── __init__.py
        │   └── ...
        ├── feature_flows/
        │   ├── __init__.py
        │   └── ...
        ├── utils/
        │   ├── __init__.py
        │   └── ...
        └── config/             (配置文件目录)
            ├── sensitive_fields.yaml
            └── column_types.yaml
```

## Prefect 部署配置

### 1. Deployment 配置分析

根据 `backend/src/prefect/prefect.service.ts` 的 `createDeploymentForFlow` 方法：

**如果 codeKey 是 `"my-flow/flow/main.py"`**：
- `dir = "my-flow/flow"` （提取目录部分）
- `deploymentBody.path = "my-flow/flow"` （Prefect 会从这个路径下载代码）
- `sanitizedModule = "main"` （提取文件名，不含扩展名）
- `entrypoint = "main:dataset_etl_flow"` （指定入口点）

### 2. Prefect S3 Storage 行为

Prefect 使用 S3 Storage 时：
1. **下载代码**：从 S3 的 `path` 目录下载所有文件（包括子目录）
2. **执行代码**：在临时目录中执行，使用 `entrypoint` 来定位 flow 函数

### 3. 潜在问题检查

#### ✅ 问题 1: Python 包结构

代码使用相对导入：
- `main.py`: `from .feature_flows.data_collection_flow import ...`
- `feature_flows/data_collection_flow.py`: `from ..tasks.collect_raw_files import ...`

**要求**：
- 目录必须是有效的 Python 包
- 需要 `__init__.py` 文件存在于：
  - `flow/__init__.py` ✅
  - `flow/tasks/__init__.py` ✅
  - `flow/feature_flows/__init__.py` ✅
  - `flow/utils/__init__.py` ✅

#### ⚠️ 问题 2: Entrypoint 执行方式

`entrypoint = "main:dataset_etl_flow"` 意味着：
- Prefect 需要能够导入 `main` 模块
- 执行时的工作目录应该包含 `main.py` 文件

**可能的行为**：
- Prefect 会下载到 `/tmp/xxx/` 下的某个目录
- 然后执行 Python 代码，导入 `main` 模块
- 如果目录结构正确，相对导入应该可以工作

#### ✅ 问题 3: 配置文件路径

配置文件路径构建：
- 使用 `_get_flow_base_path()` 获取 flow 基础路径（如 `"my-flow/"`）
- 配置文件路径：`"my-flow/flow/config/sensitive_fields.yaml"`

**要求**：
- 配置文件必须在上传时包含在文件列表中
- 路径必须与实际 S3 路径匹配

## 需要确认的事项

### 1. `__init__.py` 文件是否已上传？

确保以下文件在上传列表中：
- `flow/__init__.py`
- `flow/tasks/__init__.py`
- `flow/feature_flows/__init__.py`
- `flow/utils/__init__.py`
- `flow/config/` 目录中的配置文件

### 2. 前端上传逻辑

检查 `src/features/prefect/components/prefect-flow-new-dialog.tsx`：
- 文件树结构是否包含所有必要的 `__init__.py` 文件？
- 配置文件是否被包含在上传列表中？

### 3. Prefect 执行环境

Prefect 执行 flow 时：
- 下载的代码会被放在临时目录
- Python 路径应该包含下载的目录
- 相对导入应该能够工作（如果包结构正确）

## 建议的验证步骤

1. **检查上传的文件列表**：
   ```typescript
   // 确保包含所有必要的文件
   const requiredFiles = [
     'flow/__init__.py',
     'flow/main.py',
     'flow/tasks/__init__.py',
     'flow/feature_flows/__init__.py',
     'flow/utils/__init__.py',
     'flow/config/sensitive_fields.yaml',
     'flow/config/column_types.yaml',
     // ... 其他文件
   ]
   ```

2. **验证 S3 中的实际路径**：
   - 登录 S3/MinIO 控制台
   - 检查 `dataflow` bucket
   - 确认文件路径结构是否正确

3. **测试 Deployment**：
   - 创建 deployment 后，检查 deployment 配置
   - 确认 `path` 和 `entrypoint` 是否正确
   - 运行 flow，检查是否有导入错误

4. **检查日志**：
   - 如果 flow 运行失败，查看 Prefect 日志
   - 确认是否有 `ModuleNotFoundError` 或其他导入错误

## 可能的改进建议

如果发现导入问题，可以考虑：

1. **使用绝对导入**（不推荐，需要修改代码）：
   ```python
   # 改为绝对导入
   from flow.feature_flows.data_collection_flow import data_collection_flow
   ```

2. **调整 entrypoint**（可能需要修改后端逻辑）：
   - 如果 Prefect 执行时的工作目录是 `flow/` 的父目录
   - 可能需要使用 `flow.main:dataset_etl_flow` 作为 entrypoint

3. **使用 Python 路径设置**：
   - 在 deployment 配置中添加 Python 路径设置
   - 确保导入路径正确

## 结论

**理论上，这个目录结构应该可以被 Prefect 识别**，前提是：
1. ✅ 所有必要的 `__init__.py` 文件都已上传
2. ✅ 配置文件路径正确
3. ✅ Prefect 下载代码时保持了目录结构
4. ✅ Entrypoint 配置正确

**建议进行实际测试**，创建 deployment 并运行 flow，查看是否有任何错误。

