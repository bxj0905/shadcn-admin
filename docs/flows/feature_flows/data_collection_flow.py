"""
功能 Flow：数据收集
"""
from prefect import flow, get_run_logger
from ..tasks.collect_raw_files import collect_raw_files


@flow(name="data-collection-flow")
def data_collection_flow(prefix: str = "") -> list[dict]:
    """
    数据收集功能 Flow
    负责从 S3 收集原始文件
    """
    logger = get_run_logger()
    logger.info(f"Starting data collection flow with prefix: {prefix}")
    
    # 调用单个任务
    uploaded_files = collect_raw_files(prefix)
    
    logger.info(f"Data collection completed: {len(uploaded_files)} files collected")
    return uploaded_files

