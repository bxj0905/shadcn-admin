"""
任务：上传处理后的文件
"""
from typing import Any, Dict, List

from prefect import task, get_run_logger


@task
def upload_processed_files(prefix: str, processed_files: List[Dict[str, Any]]) -> int:
    """上传处理后的文件到 S3（占位实现）。

    当前仅作为占位任务，尚未实现真实上传逻辑。
    这样可以保证 Flow 能正常加载和运行，后续可根据需要补充实现。

    参数:
        prefix: S3 前缀
        processed_files: 处理后的文件列表

    返回:
        成功上传的文件数量（当前恒为 0）
    """
    logger = get_run_logger()
    logger.warning(
        f"upload_processed_files task not yet implemented: prefix={prefix!r}, files={len(processed_files)}"
    )
    return 0

