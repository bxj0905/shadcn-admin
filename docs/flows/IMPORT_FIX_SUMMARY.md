# 相对导入问题修复总结

## 问题描述

当 Prefect 使用 entrypoint `main:dataset_etl_flow` 执行 flow 时，出现错误：

```
ImportError: attempted relative import with no known parent package
```

**原因**：Python 将 `main.py` 作为顶层模块导入，此时相对导入（`.`）无法工作，因为顶层模块没有"父包"。

## 解决方案

### 1. 修改 `main.py`

在文件开头添加代码，将当前目录添加到 `sys.path`，然后使用 try-except 处理导入：

- 优先尝试相对导入（如果包结构正确）
- 如果失败，使用绝对导入（基于当前目录）

### 2. 修改其他文件的导入

所有使用相对导入的文件都添加了 try-except 处理：

- `feature_flows/data_collection_flow.py`
- `feature_flows/data_conversion_flow.py`
- `feature_flows/data_cleaning_flow.py`
- `tasks/convert_to_parquet.py`
- `tasks/clean_data.py`

### 3. 导入策略

```python
try:
    from ..tasks.collect_raw_files import collect_raw_files
except ImportError:
    from tasks.collect_raw_files import collect_raw_files
```

这样可以在两种情况下都能工作：
- 作为包的一部分（相对导入）
- 作为顶层模块（绝对导入，基于 sys.path）

## 注意事项

1. **确保当前目录在 sys.path 中**：`main.py` 会在开头添加当前目录
2. **目录结构**：代码假设 `flow/` 目录在 Python 路径中，子目录（`tasks/`, `feature_flows/`, `utils/`）可以直接导入
3. **兼容性**：这种方案同时支持相对导入和绝对导入，保持向后兼容

## 测试建议

1. 重新上传代码到 S3
2. 重新创建 deployment
3. 运行 flow，检查是否还有导入错误

