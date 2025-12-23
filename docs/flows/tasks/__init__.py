"""
单个任务模块
"""
from .collect_raw_files import collect_raw_files
from .convert_to_parquet import convert_to_parquet
from .clean_data import clean_data
from .upload_processed_files import upload_processed_files
from .encrypt_sensitive_fields import encrypt_sensitive_fields
from .aggregate_data import aggregate_data

__all__ = [
    "collect_raw_files",
    "convert_to_parquet",
    "clean_data",
    "upload_processed_files",
    "encrypt_sensitive_fields",
    "aggregate_data",
]

