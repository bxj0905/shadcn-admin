"""
主 Flow - 数据集 ETL 处理流程
"""
from __future__ import annotations

from typing import Union, Optional
import logging
import sys
from pathlib import Path

# 修复相对导入问题：当作为顶层模块导入时，需要确保可以导入同级模块
# Prefect 执行时，代码可能被下载到临时目录，entrypoint 为 main:xxx 时
# Python 会将 main 作为顶层模块导入，此时相对导入无法工作
# 解决方法：将当前目录添加到 sys.path，然后使用绝对导入
if __package__ is None or __package__ == '':
    # 获取当前文件所在目录（flow/ 目录）
    current_file = Path(__file__).resolve()
    current_dir = current_file.parent
    
    # 将当前目录添加到 Python 路径（这样可以直接导入同级目录的模块）
    if str(current_dir) not in sys.path:
        sys.path.insert(0, str(current_dir))

from prefect import flow, get_run_logger

# 使用绝对导入（基于当前目录，已在上面添加到 sys.path）
# 这样可以避免相对导入在顶层模块中的问题
try:
    # 优先尝试相对导入（如果包结构正确）
    from .feature_flows.data_collection_flow import data_collection_flow
    from .feature_flows.data_conversion_flow import data_conversion_flow
    from .feature_flows.data_cleaning_flow import data_cleaning_flow
    from .feature_flows.data_validation_flow import data_validation_flow
    from .feature_flows.data_encryption_flow import data_encryption_flow
except ImportError:
    # 如果相对导入失败，使用绝对导入（当前目录已在 sys.path 中）
    from feature_flows.data_collection_flow import data_collection_flow
    from feature_flows.data_conversion_flow import data_conversion_flow
    from feature_flows.data_cleaning_flow import data_cleaning_flow
    from feature_flows.data_validation_flow import data_validation_flow
    from feature_flows.data_encryption_flow import data_encryption_flow


@flow(name="dataset-etl-flow")
def dataset_etl_flow(prefix: str = ""):
    """
    数据集 ETL 处理流程主函数
    
    参数:
        prefix: S3 对象前缀，例如 "team-1/dataset-2/sourcedata/"
    
    处理步骤:
        1. 收集原始文件
        2. 转换为 Parquet
        3. 数据清洗
        4. 加密敏感字段
        5. 数据汇总
    """
    logger = get_run_logger()
    
    if not prefix:
        logger.error("prefix is required")
        raise ValueError("prefix parameter is required")

    logger.info(f"Starting dataset ETL pipeline with prefix: {prefix}")

    # 步骤 1: 收集原始文件（功能 Flow）
    uploaded_files = data_collection_flow(prefix)

    # 步骤 2: 转换为 Parquet（功能 Flow）
    parquet_files = data_conversion_flow(prefix, uploaded_files)

    # 步骤 3: 数据清洗（功能 Flow）
    cleaned_files = data_cleaning_flow(prefix, parquet_files)

    # 步骤 3.5: 数值校准（功能 Flow）
    validation_result = data_validation_flow(prefix, cleaned_files)

    if isinstance(validation_result, list):
        calibrated_files = validation_result
    else:
        logger.warning(
            "Data validation flow did not return calibrated file list; "
            "skipping encryption and aggregation."
        )
        return {
            "uploaded_count": len(uploaded_files),
            "parquet_count": len(parquet_files),
            "cleaned_count": len(cleaned_files),
            "validation_result": validation_result,
            "status": "validation_pending",
        }

    # 步骤 4: 加密敏感字段（功能 Flow）
    secure_files = data_encryption_flow(prefix, calibrated_files)

    # TODO: 步骤 3.5-5 将在后续完善
    # 步骤 5: 数据汇总

    logger.info("Dataset ETL pipeline completed successfully")
    
    return {
        "uploaded_count": len(uploaded_files),
        "parquet_count": len(parquet_files),
        "cleaned_count": len(cleaned_files),
        "calibrated_count": len(calibrated_files),
        "secure_count": len(secure_files),
        "status": "in_progress",
    }


@flow(name="rustfs-list-files")
def list_files_flow(prefix: str = ""):
    """
    保持与历史兼容的 Flow 入口函数。
    - Prefect Deployment 仍然使用 entrypoint `...:list_files_flow`
    - 内部调用新的 dataset_etl_flow 实现完整 ETL 处理
    """
    return dataset_etl_flow(prefix=prefix)

