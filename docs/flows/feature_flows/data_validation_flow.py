"""
功能 Flow：数据验证和校准
"""
from typing import Union, Optional
from prefect import flow, get_run_logger


@flow(name="dataset-validation-flow")
def data_validation_flow(prefix: str, cleaned_files: Optional[list[dict]] = None) -> Union[list[dict], dict]:
    """
    步骤 3.5：数值校准（循环校验版本）
    - 从 单位基本情况_611.parquet 和 调查单位基本情况_601.parquet 构建权威表
    - 通过 code 和 name 匹配校准其他表的数据
    - 检测并报告问题（code截断、一对多、缺失code等）
    - 如果有问题，暂停等待用户修复；修复后重新校验，直到没有问题为止
    
    注意：此 Flow 的实现需要从原始文件中迁移过来
    """
    logger = get_run_logger()
    
    # 兼容两种用法：
    # 1) 作为子 flow：主流程传入 cleaned_files 列表
    # 2) 作为独立 flow 部署并直接运行：只传 prefix，不传 cleaned_files
    cleaned_files = cleaned_files or []
    if not cleaned_files:
        logger.warning("No cleaned files to validate, skipping")
        return cleaned_files
    
    logger.info(f"Starting data validation and calibration for {len(cleaned_files)} files")
    
    # TODO: 实现验证逻辑（从原始文件迁移）
    # 这里需要从 dataset_etl_flow.py 的 validate_and_calibrate_data 函数迁移代码
    
    return cleaned_files

