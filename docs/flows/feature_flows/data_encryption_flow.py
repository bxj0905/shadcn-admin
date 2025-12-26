"""
功能 Flow：数据加密
"""
from prefect import flow, get_run_logger

try:
    from ..tasks.encrypt_sensitive_fields import encrypt_sensitive_fields
except ImportError:  # pragma: no cover - fallback when running as top-level module
    from tasks.encrypt_sensitive_fields import encrypt_sensitive_fields


@flow(name="data-encryption-flow")
def data_encryption_flow(prefix: str, cleaned_files: list[dict]) -> list[dict]:
    """
    数据加密功能 Flow
    负责加密敏感字段
    """
    logger = get_run_logger()
    logger.info(f"Starting data encryption flow with prefix: {prefix}")

    secure_files = encrypt_sensitive_fields(prefix, cleaned_files)

    logger.info(f"Data encryption completed: {len(secure_files)} files encrypted")
    return secure_files

