"""
功能 Flow：数据转换
"""
from prefect import flow, get_run_logger

try:
    from ..tasks.convert_to_parquet import convert_to_parquet
except ImportError:
    from tasks.convert_to_parquet import convert_to_parquet


@flow(name="data-conversion-flow")
def data_conversion_flow(prefix: str, uploaded_files: list[dict]) -> list[dict]:
    """
    数据转换功能 Flow
    负责将 CSV/Excel 文件转换为 Parquet 格式
    """
    logger = get_run_logger()
    logger.info(f"Starting data conversion flow with prefix: {prefix}")
    
    # 调用 convert_to_parquet 任务
    parquet_files = convert_to_parquet(prefix, uploaded_files)
    
    logger.info(f"Data conversion completed: {len(parquet_files)} files converted")
    return parquet_files

