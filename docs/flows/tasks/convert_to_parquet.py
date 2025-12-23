"""
任务：转换为 Parquet 格式
"""
from prefect import task, get_run_logger

# TODO: 需要从 dataset_etl_flow.py 第 510 行开始迁移 convert_to_parquet 函数
# 并更新导入语句使用新的工具函数模块

@task
def convert_to_parquet(prefix: str, uploaded_files: list[dict]) -> list[dict]:
    """
    步骤 2：转换为 Parquet 格式
    - 将 csv/xlsx/xls 读取为 DataFrame 后写入 Parquet
    - 输出到 raw_extracted 区
    
    TODO: 需要从原始文件迁移完整实现
    """
    logger = get_run_logger()
    logger.warning("convert_to_parquet task not yet implemented")
    return []

