"""
任务：数据清洗
"""
import os
import tempfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from prefect import task, get_run_logger
from prefect_aws.s3 import S3Bucket

try:
    from ..utils.s3_utils import download_without_etag
    from ..utils.config_utils import load_sql_column_types, load_sensitive_fields
except ImportError:
    from utils.s3_utils import download_without_etag
    from utils.config_utils import load_sql_column_types, load_sensitive_fields


@task
def clean_data(prefix: str, parquet_files: list[dict]) -> list[dict]:
    """
    步骤 3：数据清洗（已优化）
    - 规范数据类型（根据配置文件和关键词匹配）
    - 处理缺失值（删除全空列）
    - 删除敏感字段
    - 特殊过滤（如 611 表的核查情况）
    """
    logger = get_run_logger()

    if not parquet_files:
        logger.warning("No parquet files to clean, skipping")
        return []

    logger.info(f"Cleaning {len(parquet_files)} parquet files")

    # 使用 Prefect S3Bucket Block 获取客户端（用于兼容旧流程，如果文件不在本地）
    s3_block = S3Bucket.load("rustfs-s3storage")
    bucket_name = s3_block.bucket_name

    # 兼容不同版本的 S3Bucket：优先从 Block 字段取配置，缺失时再从环境变量取
    access_key = getattr(s3_block, "aws_access_key_id", None) or os.environ.get(
        "RUSTFS_ACCESS_KEY", os.environ.get("MINIO_ROOT_USER")
    )
    secret_key = getattr(s3_block, "aws_secret_access_key", None) or os.environ.get(
        "RUSTFS_SECRET_KEY", os.environ.get("MINIO_ROOT_PASSWORD")
    )
    endpoint_url = getattr(s3_block, "endpoint_url", None) or os.environ.get("RUSTFS_ENDPOINT")

    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

    # 金额/指标类字段（DECIMAL(18,2)）
    amount_keywords = [
        "amount", "amt", "fee", "price", "total", "money",
        "金额", "单价", "总价", "费用", "成本", "收入", "利润",
    ]

    # 人数/计数字段（INT64）- 这些字段必须是整数，不能是小数
    people_keywords = [
        "count", "cnt", "num", "qty", "quantity",
        "人数", "人次", "人员", "员工", "职工",
    ]
    
    # 手机号码/ID类字段（INT64 或字符串，但如果是数值则用整数）
    id_keywords = [
        "phone", "mobile", "tel", "id", "code",
        "手机", "电话", "号码", "编号", "代码",
    ]

    cleaned_files = []

    for item in parquet_files:
        parquet_key = item.get("parquet_key") or item.get("target_key")
        if not parquet_key:
            continue

        # 优先使用本地路径（如果存在），否则从 S3 下载（兼容旧流程）
        local_parquet_path = item.get("local_path")
        if not local_parquet_path or not os.path.exists(local_parquet_path):
            # 如果没有本地路径，从 S3 下载（兼容旧流程）
            local_parquet_path = os.path.join(tempfile.mkdtemp(prefix="dataset_etl_clean_"), Path(parquet_key).name)
            logger.info(f"Downloading {parquet_key} for cleaning")
            download_without_etag(s3_client, bucket_name, parquet_key, local_parquet_path)
        else:
            logger.info(f"Using local file for cleaning: {local_parquet_path}")

        try:
            # 读取 Parquet
            table = pq.read_table(local_parquet_path)
            schema = table.schema

            # Schema 清洗：统一列类型
            # 先转换为 pandas DataFrame 以便更好地检查数据类型
            df = table.to_pandas()
            new_columns = []
            
            # 使用更大的精度避免精度不足问题
            decimal_type_small = pa.decimal128(18, 2)
            decimal_type_large = pa.decimal128(38, 2)  # 更大的精度
            int_type = pa.int64()
            
            # 加载 SQL 中定义的字段类型映射
            sql_column_types = load_sql_column_types()
            logger.info(f"Loaded {len(sql_column_types)} column type mappings from SQL")

            for i, field in enumerate(schema):
                col_name = field.name
                col = table.column(i)
                lower_name = col_name.lower()
                df_col = df[col_name]

                target_type = None
                should_cast = False
                
                # 优先检查 SQL 中是否定义了该字段的类型
                if col_name in sql_column_types:
                    sql_type = sql_column_types[col_name]
                    if sql_type == "DECIMAL":
                        # 检查数据是否主要是数值
                        non_null_values = df_col.dropna()
                        if len(non_null_values) > 0:
                            numeric_values = pd.to_numeric(non_null_values, errors='coerce')
                            numeric_count = numeric_values.notna().sum()
                            numeric_ratio = numeric_count / len(non_null_values)
                            
                            if numeric_ratio > 0.8:  # 80% 以上是数值才转换
                                valid_numeric = numeric_values.dropna()
                                if len(valid_numeric) > 0:
                                    max_abs_value = valid_numeric.abs().max()
                                    if max_abs_value > 10**16:
                                        target_type = decimal_type_large
                                        logger.info(f"Column {col_name} matches SQL definition: DECIMAL(38,2) (large values: {max_abs_value:.2e})")
                                    else:
                                        target_type = decimal_type_small
                                        logger.info(f"Column {col_name} matches SQL definition: DECIMAL(18,2)")
                                    should_cast = True
                    elif sql_type == "INTEGER":
                        # 检查数据是否主要是整数
                        non_null_values = df_col.dropna()
                        if len(non_null_values) > 0:
                            string_values = non_null_values.astype(str)
                            string_values = string_values[~string_values.str.lower().isin(['nan', 'none', 'null', ''])]
                            
                            if len(string_values) > 0:
                                has_non_digit = string_values.str.contains(r'[^0-9.\-+\s]', regex=True, na=False).any()
                                has_special_chars = string_values.str.contains(r'[-+\s]', regex=True, na=False).any()
                                
                                if not (has_non_digit or has_special_chars):
                                    numeric_values = pd.to_numeric(non_null_values, errors='coerce')
                                    numeric_count = numeric_values.notna().sum()
                                    numeric_ratio = numeric_count / len(non_null_values)
                                    
                                    if numeric_ratio > 0.8:
                                        valid_numeric = numeric_values.dropna()
                                        if len(valid_numeric) > 0:
                                            is_integer = (valid_numeric % 1 == 0).all()
                                            if is_integer:
                                                target_type = int_type
                                                should_cast = True
                                                logger.info(f"Column {col_name} matches SQL definition: INTEGER")
                    # TEXT 和 BOOLEAN 类型保持原样，不进行转换
                
                # 如果 SQL 中没有定义，使用原有的关键词匹配逻辑
                if not should_cast:
                    # 检查是否应该转换为金额类型
                    if any(k in lower_name for k in amount_keywords):
                        # 先检查列是否包含数值数据
                        # 排除明显不是数值的列（如包含"代码"、"名称"、"单位"、"目录"、"指标"等）
                        exclude_keywords = ["代码", "名称", "单位", "目录", "指标", "code", "name", "unit"]
                        if any(exclude_kw in lower_name for exclude_kw in exclude_keywords):
                            # 如果列名包含这些关键词，很可能是分类字段，不转换为数值
                            logger.info(f"Column {col_name} contains exclusion keywords, skipping decimal cast")
                            should_cast = False
                        else:
                            # 检查数据是否主要是数值
                            non_null_values = df_col.dropna()
                            if len(non_null_values) > 0:
                                # 尝试转换为数值，检查成功率
                                numeric_values = pd.to_numeric(non_null_values, errors='coerce')
                                numeric_count = numeric_values.notna().sum()
                                numeric_ratio = numeric_count / len(non_null_values)
                                
                                if numeric_ratio > 0.8:  # 80% 以上是数值才转换
                                    # 检查数据范围，决定使用哪种精度
                                    valid_numeric = numeric_values.dropna()
                                    if len(valid_numeric) > 0:
                                        # 金额/费用类字段即使全部是整数，也应该使用 DECIMAL
                                        # 因为费用通常需要保留小数精度，且可能在未来出现小数
                                        max_abs_value = valid_numeric.abs().max()
                                        # 如果最大值超过 18 位精度能表示的范围，使用大精度
                                        if max_abs_value > 10**16:  # decimal128(18,2) 最大约 10^16
                                            target_type = decimal_type_large
                                            logger.info(f"Column {col_name} has large values (max: {max_abs_value:.2e}), using decimal128(38,2)")
                                        else:
                                            target_type = decimal_type_small
                                        
                                        # 检查数据是否全部是整数（仅用于日志）
                                        is_integer = (valid_numeric % 1 == 0).all()
                                        if is_integer:
                                            logger.info(f"Column {col_name} contains only integer values, but using DECIMAL for amount field")
                                        
                                        should_cast = True
                                else:
                                    logger.info(f"Column {col_name} has low numeric ratio ({numeric_ratio:.2%}), skipping decimal cast")
                    
                    # 检查是否应该转换为整数类型（人数、计数等）
                    elif any(k in lower_name for k in people_keywords):
                        # 检查数据是否主要是整数
                        non_null_values = df_col.dropna()
                        if len(non_null_values) > 0:
                            # 先检查字符串是否包含非数字字符（如横线、空格等）
                            # 如果包含，说明不是纯数字，不应该转换为整数
                            string_values = non_null_values.astype(str)
                            has_non_digit = string_values.str.contains(r'[^0-9.\-+\s]', regex=True, na=False).any()
                            has_special_chars = string_values.str.contains(r'[-+\s]', regex=True, na=False).any()
                            
                            if has_non_digit or has_special_chars:
                                logger.info(f"Column {col_name} contains non-digit characters, skipping INT64 cast")
                                should_cast = False
                            else:
                                numeric_values = pd.to_numeric(non_null_values, errors='coerce')
                                numeric_count = numeric_values.notna().sum()
                                numeric_ratio = numeric_count / len(non_null_values)
                                
                                if numeric_ratio > 0.8:  # 80% 以上是数值才转换
                                    # 检查是否真的是整数（不能有小数）
                                    valid_numeric = numeric_values.dropna()
                                    if len(valid_numeric) > 0:
                                        is_integer = (valid_numeric % 1 == 0).all()
                                        if is_integer:
                                            target_type = int_type
                                            should_cast = True
                                            logger.info(f"Column {col_name} identified as integer count field, using INT64")
                                        else:
                                            logger.warning(f"Column {col_name} contains non-integer values, skipping INT64 cast")
                    
                    # 检查是否应该转换为整数类型（手机号码、ID等）
                    elif any(k in lower_name for k in id_keywords):
                        # 检查数据是否主要是整数
                        non_null_values = df_col.dropna()
                        if len(non_null_values) > 0:
                            # 先检查字符串是否包含非数字字符（如横线、空格等）
                            # 如果包含，说明不是纯数字，不应该转换为整数
                            string_values = non_null_values.astype(str)
                            # 排除 'nan' 字符串
                            string_values = string_values[~string_values.str.lower().isin(['nan', 'none', 'null', ''])]
                            
                            if len(string_values) > 0:
                                has_non_digit = string_values.str.contains(r'[^0-9.\-+\s]', regex=True, na=False).any()
                                has_special_chars = string_values.str.contains(r'[-+\s]', regex=True, na=False).any()
                                
                                if has_non_digit or has_special_chars:
                                    logger.info(f"Column {col_name} contains non-digit characters (e.g., hyphens), keeping as string")
                                    should_cast = False
                                else:
                                    numeric_values = pd.to_numeric(non_null_values, errors='coerce')
                                    numeric_count = numeric_values.notna().sum()
                                    numeric_ratio = numeric_count / len(non_null_values)
                                    
                                    if numeric_ratio > 0.8:  # 80% 以上是数值才转换
                                        # 检查是否真的是整数（不能有小数）
                                        valid_numeric = numeric_values.dropna()
                                        if len(valid_numeric) > 0:
                                            is_integer = (valid_numeric % 1 == 0).all()
                                            if is_integer:
                                                target_type = int_type
                                                should_cast = True
                                                logger.info(f"Column {col_name} identified as integer ID field, using INT64")
                                            else:
                                                logger.warning(f"Column {col_name} contains non-integer values, skipping INT64 cast")

                # 执行类型转换
                if should_cast and target_type is not None:
                    try:
                        # 先尝试转换，如果失败且是精度问题，自动升级到更大精度
                        casted = col.cast(target_type, safe=True)
                        logger.info(f"Casting column {col_name} to {target_type}")
                        new_columns.append(casted)
                    except Exception as exc:
                        error_msg = str(exc)
                        # 如果是精度不足错误，且当前使用的是小精度，尝试使用大精度
                        if "Precision is not great enough" in error_msg and target_type == decimal_type_small:
                            try:
                                logger.warning(f"Column {col_name} precision insufficient for decimal128(18,2), upgrading to decimal128(38,2)")
                                casted = col.cast(decimal_type_large, safe=True)
                                logger.info(f"Casting column {col_name} to decimal128(38,2)")
                                new_columns.append(casted)
                            except Exception as exc2:
                                logger.warning(f"Failed to cast column {col_name} even with decimal128(38,2): {exc2}, keeping original type")
                                new_columns.append(col)
                        else:
                            logger.warning(f"Failed to cast column {col_name} to {target_type}: {exc}, keeping original type")
                            new_columns.append(col)
                else:
                    new_columns.append(col)

            new_table = pa.Table.from_arrays(new_columns, names=schema.names)

            # 值清洗：删除整列都是缺失值的列
            df = new_table.to_pandas()
            before_cols = list(df.columns)
            df = df.dropna(axis=1, how="all")
            after_cols = list(df.columns)

            removed_cols = [c for c in before_cols if c not in after_cols]
            if removed_cols:
                logger.info(f"Removed all-null columns: {removed_cols}")

            # 值清洗：删除敏感数据列
            sensitive_fields = load_sensitive_fields()
            sensitive_cols_to_remove = []
            
            # 1. 删除 YAML 配置中明确列出的敏感字段
            for col_name in df.columns:
                if col_name in sensitive_fields:
                    sensitive_cols_to_remove.append(col_name)
            
            # 2. 删除字段名包含"手机"、"电话"、"传真"的列
            sensitive_keywords = ["手机", "电话", "传真"]
            for col_name in df.columns:
                if col_name not in sensitive_cols_to_remove:  # 避免重复添加
                    for keyword in sensitive_keywords:
                        if keyword in col_name:
                            sensitive_cols_to_remove.append(col_name)
                            break  # 找到一个关键词就足够了
            
            if sensitive_cols_to_remove:
                df = df.drop(columns=sensitive_cols_to_remove)
                logger.info(f"Removed sensitive columns: {sensitive_cols_to_remove}")

            # 值清洗：删除单位基本情况_611.parquet 文件中核查情况不等于1的数据
            # 尝试从多个来源获取文件名
            parquet_filename = (
                item.get("normalized_name", "").replace(".parquet", "") or
                (Path(parquet_key).stem if parquet_key else "") or
                Path(local_parquet_path).stem
            )
            if parquet_filename == "单位基本情况_611" and "核查情况" in df.columns:
                before_rows = len(df)
                # 尽量转成数值，无法解析的记为 NaN
                norm = pd.to_numeric(df["核查情况"], errors="coerce")
                mask = norm == 1
                if mask.any():
                    df = df[mask]
                    after_rows = len(df)
                    removed_rows = before_rows - after_rows
                    logger.info(f"Filtered {parquet_filename} by 核查情况=1: {before_rows} -> {after_rows} rows (removed {removed_rows} rows)")
                else:
                    # 如果没有任何一行等于 1，则不做过滤，避免把基准意外清空
                    logger.warning(f"{parquet_filename}: 核查情况列存在但没有等于 1 的记录，不执行过滤，保持 {before_rows} 行不变")

            # 写回本地文件（不上传，等待所有步骤完成）
            df.to_parquet(local_parquet_path, index=False)
            logger.info(f"Cleaned parquet file (local): {local_parquet_path}")

            # 更新本地路径信息
            item["local_path"] = local_parquet_path
            cleaned_files.append(item)

        except Exception as exc:
            logger.error(f"Failed to clean {parquet_key}: {exc}")
            # 即使清洗失败，也保留文件信息，供后续步骤处理
            cleaned_files.append(item)

    logger.info(f"Cleaned {len(cleaned_files)} files")
    return cleaned_files

