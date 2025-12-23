"""
主 Flow - 数据集 ETL 处理流程
"""
from __future__ import annotations

from typing import Union, Optional
import logging

from prefect import flow, get_run_logger

from .feature_flows.data_collection_flow import data_collection_flow
from .feature_flows.data_validation_flow import data_validation_flow


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

    # TODO: 步骤 2-5 将在后续完善
    # 步骤 2: 转换为 Parquet
    # 步骤 3: 数据清洗
    # 步骤 3.5: 数值校准（子 Flow）
    # 步骤 4: 加密敏感字段
    # 步骤 5: 数据汇总

    logger.info("Dataset ETL pipeline completed successfully")
    
    return {
        "uploaded_count": len(uploaded_files),
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

