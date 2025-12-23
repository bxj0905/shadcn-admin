"""
功能 Flow：数据清洗
"""
from prefect import flow, get_run_logger


@flow(name="data-cleaning-flow")
def data_cleaning_flow(prefix: str, parquet_files: list[dict]) -> list[dict]:
    """
    数据清洗功能 Flow
    负责数据清洗和规范化
    
    TODO: 需要从 dataset_etl_flow.py 的 clean_data 函数迁移代码
    """
    logger = get_run_logger()
    logger.info(f"Starting data cleaning flow with prefix: {prefix}")
    
    # TODO: 调用 clean_data 任务
    # from ..tasks.clean_data import clean_data
    # cleaned_files = clean_data(prefix, parquet_files)
    
    logger.warning("Data cleaning flow not yet implemented")
    return []

