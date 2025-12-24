"""
任务：转换为 Parquet 格式
"""
import os
import tempfile
from pathlib import Path

import pandas as pd
import boto3
from prefect import task, get_run_logger
from prefect_aws.s3 import S3Bucket

try:
    from ..utils.s3_utils import download_without_etag
    from ..utils.file_utils import preprocess_csv_to_utf8, build_new_name_from_parent
except ImportError:
    from utils.s3_utils import download_without_etag
    from utils.file_utils import preprocess_csv_to_utf8, build_new_name_from_parent


@task
def convert_to_parquet(prefix: str, uploaded_files: list[dict]) -> list[dict]:
    """
    步骤 2：转换为 Parquet 格式
    - 将 csv/xlsx/xls 读取为 DataFrame 后写入 Parquet
    - 输出到 raw_extracted 区
    """
    logger = get_run_logger()

    if not prefix or not uploaded_files:
        logger.warning("prefix or uploaded_files is empty, nothing to do")
        return []

    # 统一前缀格式
    normalized_raw_prefix = prefix.lstrip("/")
    if not normalized_raw_prefix.endswith("/"):
        normalized_raw_prefix = f"{normalized_raw_prefix}/"

    # 计算 raw_extracted 前缀：将 ".../raw/" 或 ".../sourcedata/" 替换为 ".../raw_extracted/"
    if "/raw/" in normalized_raw_prefix:
        extracted_prefix = normalized_raw_prefix.replace("/raw/", "/raw_extracted/", 1)
    elif "/sourcedata/" in normalized_raw_prefix:
        extracted_prefix = normalized_raw_prefix.replace("/sourcedata/", "/raw_extracted/", 1)
    else:
        # 兜底：在末尾附加 raw_extracted/
        extracted_prefix = f"{normalized_raw_prefix}raw_extracted/"

    logger.info(f"Converting files to parquet, extracted_prefix: {extracted_prefix}")

    # 使用 Prefect S3Bucket Block 获取客户端
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

    tmp_dir = tempfile.mkdtemp(prefix="dataset_etl_parquet_")
    logger.info(f"Using temp directory: {tmp_dir}")

    parquet_files: list[dict] = []

    for item in uploaded_files:
        if not item.get("uploaded"):
            continue

        raw_key = item["target_key"]
        rel_path = item.get("relative_path") or raw_key[len(normalized_raw_prefix):]

        # 取相对路径的父目录名作为"批次名"
        parent_name = Path(rel_path).parent.name or Path(rel_path).stem
        suffix = (Path(rel_path).suffix or "").lower()
        
        logger.info(f"Processing {raw_key}, suffix={suffix}, parent={parent_name}")

        # 使用统一规则生成规范文件名
        normalized_name_without_ext = build_new_name_from_parent(parent_name, "")
        parquet_filename = f"{normalized_name_without_ext}.parquet"
        parquet_key = f"{extracted_prefix}{parquet_filename}"

        local_raw_path = os.path.join(tmp_dir, Path(raw_key).name)
        local_parquet_path = os.path.join(tmp_dir, parquet_filename)

        try:
            # 下载文件（使用 download_fileobj 避免 ETag 校验问题）
            logger.info(f"Downloading s3://{bucket_name}/{raw_key} -> {local_raw_path}")
            download_without_etag(s3_client, bucket_name, raw_key, local_raw_path)

            # 按文件类型读取
            if suffix in (".csv", ".txt"):
                # 先预处理为 UTF-8，尽量消除编码/坏行导致的异常
                clean_path = None
                used_encoding = None
                try:
                    clean_path, used_encoding = preprocess_csv_to_utf8(local_raw_path)
                    logger.info(f"Preprocessed CSV with encoding {used_encoding}, path: {clean_path}")

                    # 检查文件大小，如果超过 50MB 给出警告
                    file_size_mb = os.path.getsize(clean_path) / (1024 * 1024)
                    if file_size_mb > 50:
                        logger.warning(f"CSV file size ({file_size_mb:.2f}MB) exceeds 50MB, this may cause memory issues")
                    
                    try:
                        # 使用分块读取确保完整读取所有数据，避免数据丢失
                        # 对于所有文件都使用分块读取，确保数据完整性
                        logger.info(f"Reading CSV file ({file_size_mb:.2f}MB) in chunks to ensure data integrity")
                        chunks = []
                        chunk_size = 10000  # 每次读取 10000 行
                        chunk_count = 0
                        total_rows = 0
                        
                        for chunk in pd.read_csv(
                            clean_path,
                            encoding="utf-8",
                            on_bad_lines="skip",
                            chunksize=chunk_size,
                        ):
                            chunks.append(chunk)
                            chunk_count += 1
                            total_rows += len(chunk)
                            if chunk_count % 10 == 0:
                                logger.info(f"Read {chunk_count} chunks, total rows so far: {total_rows}")
                        
                        logger.info(f"Finished reading {chunk_count} chunks, total rows: {total_rows}, concatenating...")
                        df = pd.concat(chunks, ignore_index=True)
                        logger.info(f"Successfully read all data, final shape: {df.shape}")
                        
                        # 验证数据完整性
                        if len(df) != total_rows:
                            logger.warning(f"Data integrity check: expected {total_rows} rows after concat, got {len(df)} rows")
                    except TypeError:
                        # 兼容老版 pandas
                        logger.info(f"Reading CSV file ({file_size_mb:.2f}MB) in chunks to ensure data integrity")
                        chunks = []
                        chunk_size = 10000
                        chunk_count = 0
                        total_rows = 0
                        
                        for chunk in pd.read_csv(
                            clean_path,
                            encoding="utf-8",
                            error_bad_lines=False,
                            warn_bad_lines=True,
                            chunksize=chunk_size,
                        ):
                            chunks.append(chunk)
                            chunk_count += 1
                            total_rows += len(chunk)
                            if chunk_count % 10 == 0:
                                logger.info(f"Read {chunk_count} chunks, total rows so far: {total_rows}")
                        
                        logger.info(f"Finished reading {chunk_count} chunks, total rows: {total_rows}, concatenating...")
                        df = pd.concat(chunks, ignore_index=True)
                        logger.info(f"Successfully read all data, final shape: {df.shape}")
                        
                        # 验证数据完整性
                        if len(df) != total_rows:
                            logger.warning(f"Data integrity check: expected {total_rows} rows after concat, got {len(df)} rows")

                    logger.info(f"Successfully read CSV after preprocess, final shape: {df.shape}")
                except Exception as exc:
                    logger.error(f"Failed to preprocess/read CSV {local_raw_path}: {exc}, skipping")
                    continue
                finally:
                    if clean_path and os.path.exists(clean_path):
                        try:
                            os.remove(clean_path)
                        except OSError:
                            pass

                # 规范混合类型/对象列
                df = df.convert_dtypes()
                for col_name in df.columns:
                    if df[col_name].dtype == "object":
                        try:
                            series = df[col_name].astype(str).str.strip().replace({"": None})
                            df[col_name] = series.astype("string")
                        except Exception as exc:
                            logger.warning(f"Failed to normalize column {col_name}: {exc}")

            elif suffix in (".xlsx", ".xls"):
                logger.info(f"Reading Excel file {local_raw_path}")
                df = pd.read_excel(local_raw_path)

                # 规范混合类型/对象列
                df = df.convert_dtypes()
                for col_name in df.columns:
                    if df[col_name].dtype == "object":
                        try:
                            series = df[col_name].astype(str).str.strip().replace({"": None})
                            df[col_name] = series.astype("string")
                        except Exception as exc:
                            logger.warning(f"Failed to normalize column {col_name}: {exc}")
            else:
                logger.warning(f"Unsupported suffix {suffix} for {raw_key}, skipping")
                continue

            # 写出为 Parquet（不上传，保留在本地，等待所有步骤完成）
            df.to_parquet(local_parquet_path, index=False)
            logger.info(f"Converted to parquet (local): {local_parquet_path}")

            parquet_files.append(
                {
                    "raw_key": raw_key,
                    "parquet_key": parquet_key,
                    "bucket": bucket_name,
                    "normalized_name": parquet_filename,
                    "local_path": local_parquet_path,  # 保存本地路径，供后续步骤使用
                }
            )

        except Exception as exc:
            logger.error(f"Failed to process {raw_key} -> {parquet_key}: {exc}")
        finally:
            # 只清理原始文件，保留 parquet 文件供后续步骤使用
            try:
                if os.path.exists(local_raw_path):
                    os.remove(local_raw_path)
            except OSError:
                pass

    logger.info(f"Converted {len(parquet_files)} files to parquet")
    return parquet_files

