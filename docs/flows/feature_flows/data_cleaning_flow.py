"""
功能 Flow：数据清洗
"""
from prefect import flow, get_run_logger

try:
    from ..tasks.clean_data import clean_data
except ImportError:
    from tasks.clean_data import clean_data


@flow(name="data-cleaning-flow")
def data_cleaning_flow(prefix: str, parquet_files: list[dict]) -> list[dict]:
    """
    数据清洗功能 Flow
    负责数据清洗和规范化
    """
    logger = get_run_logger()
    logger.info(f"Starting data cleaning flow with prefix: {prefix}")
    
    # 调用 clean_data 任务
    cleaned_files = clean_data(prefix, parquet_files)
    
    logger.info(f"Data cleaning completed: {len(cleaned_files)} files cleaned")
    return cleaned_files

