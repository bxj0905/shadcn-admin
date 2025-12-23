"""
功能 Flow：数据转换
"""
from prefect import flow, get_run_logger


@flow(name="data-conversion-flow")
def data_conversion_flow(prefix: str, uploaded_files: list[dict]) -> list[dict]:
    """
    数据转换功能 Flow
    负责将 CSV/Excel 文件转换为 Parquet 格式
    
    TODO: 需要从 dataset_etl_flow.py 的 convert_to_parquet 函数迁移代码
    """
    logger = get_run_logger()
    logger.info(f"Starting data conversion flow with prefix: {prefix}")
    
    # TODO: 调用 convert_to_parquet 任务
    # from ..tasks.convert_to_parquet import convert_to_parquet
    # parquet_files = convert_to_parquet(prefix, uploaded_files)
    
    logger.warning("Data conversion flow not yet implemented")
    return []

