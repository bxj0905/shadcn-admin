"""
数据集 ETL 处理流程 - Prefect Flow 版本
参考 wu_jing_pu_etl.py 的处理逻辑，使用 Prefect 实现完整的数据处理流程
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Union, Optional
import os
import tempfile

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
import yaml
import json

from prefect import flow, task, get_run_logger
from prefect_aws.s3 import S3Bucket


def download_without_etag(
    s3_client, bucket: str, key: str, dest_path: str, chunk_size: int = 8 * 1024 * 1024, max_retries: int = 5
) -> None:
    """
    使用 get_object 流式下载，绕过 boto3 Transfer 的 ETag 校验。
    
    对于大文件（如 100MB+），可能出现的问题：
    1. **ETag 不匹配**：Multipart Upload 的 ETag 格式不同，MinIO/S3 兼容性问题
    2. **网络中断**：下载过程中连接中断（IncompleteRead、Connection broken）
    3. **超时**：大文件下载时间过长导致超时
    
    解决方案：
    - 使用 get_object 直接读取流，绕过 Transfer Manager 的校验机制
    - 对网络错误进行重试（最多 5 次，指数退避）
    - 重试前清理不完整的文件
    """
    import time
    from botocore.exceptions import ClientError, IncompleteReadError
    from urllib3.exceptions import IncompleteRead as Urllib3IncompleteRead
    
    last_error = None
    for attempt in range(max_retries):
        try:
            # 重试前清理可能存在的部分下载文件
            if attempt > 0 and os.path.exists(dest_path):
                try:
                    os.remove(dest_path)
                except OSError:
                    pass
            
            # 使用 get_object 直接获取对象流，不经过 Transfer Manager
            response = s3_client.get_object(Bucket=bucket, Key=key)
            body = response["Body"]
            
            # 流式写入，避免内存溢出
            bytes_written = 0
            with open(dest_path, "wb") as f:
                while True:
                    chunk = body.read(chunk_size)
                    if not chunk:
                        break
                    f.write(chunk)
                    bytes_written += len(chunk)
            
            # 验证文件大小（如果可能）
            content_length = response.get("ContentLength")
            if content_length and bytes_written != content_length:
                raise RuntimeError(
                    f"Download incomplete: expected {content_length} bytes, got {bytes_written} bytes"
                )
            
            # 下载成功，返回
            return
            
        except (IncompleteReadError, Urllib3IncompleteRead, ConnectionError, OSError) as e:
            # 网络连接问题：IncompleteRead、Connection broken 等
            last_error = e
            if attempt < max_retries - 1:
                wait_time = min(2 ** attempt, 30)  # 指数退避，最多等待 30 秒
                time.sleep(wait_time)
                continue
            raise RuntimeError(f"Network error downloading {key} after {max_retries} attempts: {e}") from e
            
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            error_msg = str(e)
            
            # ETag/Checksum 相关错误，重试
            if "ETag" in error_msg or "Checksum" in error_msg:
                last_error = e
                if attempt < max_retries - 1:
                    wait_time = min(2 ** attempt, 30)
                    time.sleep(wait_time)
                    continue
            
            # 其他 ClientError（如 404、403）直接抛出
            raise
            
        except Exception as e:
            error_msg = str(e)
            
            # ETag 相关错误，重试
            if "ETag" in error_msg or "IncompleteRead" in error_msg or "Connection broken" in error_msg:
                last_error = e
                if attempt < max_retries - 1:
                    wait_time = min(2 ** attempt, 30)
                    time.sleep(wait_time)
                    continue
            
            # 其他错误直接抛出
            raise
    
    # 所有重试都失败
    if last_error:
        raise RuntimeError(f"Failed to download {key} after {max_retries} attempts: {last_error}") from last_error


def detect_file_encoding(file_path: str) -> tuple[str | None, float]:
    """
    自动检测文件编码（使用 chardet）
    返回: (encoding, confidence)
    """
    try:
        import chardet
        with open(file_path, "rb") as f:
            raw_data = f.read()
            result = chardet.detect(raw_data)
            if result and result.get("encoding"):
                return result["encoding"], result.get("confidence", 0.0)
    except ImportError:
        pass
    except Exception:
        pass
    
    # 如果 chardet 不可用或检测失败，返回 None
    return None, 0.0


def preprocess_csv_to_utf8(src_path: str) -> tuple[str, str]:
    """
    预处理 CSV：
    - 去除 \x00 等无效字节
    - 自动检测编码（优先使用 chardet）
    - 如果检测失败，按常见编码顺序尝试（优先不使用 errors='replace'，避免数据丢失）
    - 统一写回 UTF-8 新文件，返回 (clean_path, used_encoding)
    """
    raw_bytes = Path(src_path).read_bytes().replace(b"\x00", b"")

    # 优先使用 chardet 自动检测
    detected_encoding, confidence = detect_file_encoding(src_path)
    
    # 编码候选列表（按常见程度排序）
    candidates = []
    if detected_encoding and confidence > 0.7:
        # 如果检测到高置信度的编码，优先使用
        candidates.append(detected_encoding)
    
    # 添加常见中文编码（避免重复）
    common_encodings = ["utf-8", "utf-8-sig", "gbk", "gb2312", "gb18030", "big5", "latin1"]
    for enc in common_encodings:
        if enc not in candidates:
            candidates.append(enc)

    text = None
    used = None

    # 优先尝试不使用 errors='replace'，避免数据丢失
    for enc in candidates:
        try:
            text = raw_bytes.decode(enc)
            used = enc
            break
        except (UnicodeDecodeError, LookupError):
            continue

    # 如果所有编码都失败，使用 errors='replace' 作为最后手段
    if text is None:
        # 优先尝试 utf-8，如果失败再用 latin1
        try:
            text = raw_bytes.decode("utf-8", errors="replace")
            used = "utf-8 (with errors='replace')"
        except Exception:
            text = raw_bytes.decode("latin1", errors="replace")
            used = "latin1 (with errors='replace')"

    fd, clean_path = tempfile.mkstemp(prefix="clean_csv_", suffix=".csv")
    with os.fdopen(fd, "w", encoding="utf-8", newline="") as f:
        f.write(text)

    return clean_path, used


def _get_default_column_types() -> dict[str, str]:
    """
    返回默认的字段类型映射（内嵌配置）
    如果无法读取配置文件，使用此默认配置
    """
    column_types = {}
    
    # DECIMAL(18,2) 类型字段 - 金额/费用类字段
    decimal_fields = [
        "营业成本", "本年折旧", "资产总计", "负债合计", "营业收入",
        "税金及附加", "应交增值税", "应付职工薪酬", "营业利润",
        "资产减值损失", "信用减值损失", "投资收益", "公允价值变动收益",
        "资产处置收益", "其他收益", "净敞口套期收益", "税收合计",
        "研发费用", "增加值", "工业总产值", "经营性单位收入",
        "非经营性单位支出 （费用）"
    ]
    for field_name in decimal_fields:
        column_types[field_name] = "DECIMAL"
    
    # INTEGER 类型字段 - 人数字段
    integer_fields = [
        "期末从业人数", "其中女性从业人数", "研发人员数",
        "专利申请数", "专利授权数"
    ]
    for field_name in integer_fields:
        column_types[field_name] = "INTEGER"
    
    # TEXT 类型字段 - 文本字段
    text_fields = [
        "统一社会信用代码", "单位详细名称", "数据来源",
        "经营地 - 省（自治区、直辖市）", "经营地 - 市（地、州、盟）",
        "经营地 - 县（区、市、旗）", "经营地 - 乡（镇、街道办事处）",
        "是否与单位所在地详细地址一致", "注册地 - 省（自治区、直辖市）",
        "注册地 - 市（地、州、盟）", "注册地 - 县（区、市、旗）",
        "注册地 - 乡（镇、街道办事处）", "机构类型（代码）",
        "机构类型（文字）", "登记注册类别（代码）", "登记注册类别（文字）",
        "行业代码", "专业分类", "产业", "行业门类", "行业大类",
        "行业中类", "行业小类", "主要业务活动", "法人单位详细名称",
        "法人单位详细地址", "法人单位区划代码", "从业人员期末人数",
        "从业人员期末人数其中女性", "单位规模（代码）", "单位规模（文字）",
        "产业分类", "产业领域", "产业大类", "产业中类", "产业小类"
    ]
    for field_name in text_fields:
        column_types[field_name] = "TEXT"
    
    # BOOLEAN 类型字段
    boolean_fields = ["是否一套表单位"]
    for field_name in boolean_fields:
        column_types[field_name] = "BOOLEAN"
    
    return column_types


def _get_default_sensitive_fields() -> list[str]:
    """
    返回默认的敏感字段列表（内嵌配置）
    如果无法读取配置文件，使用此默认配置
    """
    return [
        "单位负责人",
        "传真号码",
        "移动电话",
        "联系电话",
        "1;联系电话",
        "固定电话",
        "固定电话分机号",
        "传真号码分机号",
        "法定代表人（单位负责人）",
        "统计负责人",
        "填表人",
        "报出日期",
        "街（路）、门牌号",
        "街（路）、门牌号注册地区",
        "单位负责人签名",
        "流水号",
    ]


def load_sensitive_fields() -> list[str]:
    """
    从 sensitive_fields.yaml 配置文件加载敏感字段列表
    优先级：
    1. 从 Prefect Blocks 的 dataflow 配置中读取 config/sensitive_fields.yaml
    2. 使用内嵌的默认配置
    
    返回: 敏感字段名列表
    """
    config = None
    
    # 优先级 1: 从 Prefect Blocks 的 dataflow 配置中读取
    try:
        # 尝试从 dataflow bucket 读取配置文件
        s3_block = S3Bucket.load("rustfs-s3storage")
        bucket_name = s3_block.bucket_name
        
        dataflow_bucket = os.environ.get("RUSTFS_DATAFLOW_BUCKET", "dataflow")
        config_key = "config/sensitive_fields.yaml"
        
        # 获取 S3 客户端
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
        
        # 尝试从 dataflow bucket 读取
        try:
            response = s3_client.get_object(Bucket=dataflow_bucket, Key=config_key)
            content = response['Body'].read().decode('utf-8')
            config = yaml.safe_load(content)
        except Exception:
            # 如果 dataflow bucket 中不存在，尝试从当前 bucket 读取
            try:
                response = s3_client.get_object(Bucket=bucket_name, Key=config_key)
                content = response['Body'].read().decode('utf-8')
                config = yaml.safe_load(content)
            except Exception:
                pass
    except Exception:
        # S3 读取失败，使用默认配置
        pass
    
    # 如果找到了配置文件，使用它
    if config and 'columns' in config:
        return config['columns']
    
    # 优先级 2: 如果配置文件不存在，使用内嵌的默认配置
    return _get_default_sensitive_fields()


def load_sql_column_types() -> dict[str, str]:
    """
    从 column_types.yaml 配置文件加载字段名到数据类型的映射
    优先级：
    1. 从 Prefect Blocks 的 dataflow 配置中读取 config/column_types.yaml
    2. 使用内嵌的默认配置
    
    返回: {字段名: 类型} 的字典，类型为 'DECIMAL', 'INTEGER', 'TEXT', 'BOOLEAN'
    """
    column_types = {}
    config = None
    
    # 优先级 1: 从 Prefect Blocks 的 dataflow 配置中读取
    try:
        # 尝试从 dataflow bucket 读取配置文件
        s3_block = S3Bucket.load("rustfs-s3storage")
        bucket_name = s3_block.bucket_name
        
        # 尝试从 dataflow bucket 读取（如果 bucket 名称不同，需要调整）
        # 或者使用环境变量指定的 dataflow bucket
        dataflow_bucket = os.environ.get("RUSTFS_DATAFLOW_BUCKET", "dataflow")
        config_key = "config/column_types.yaml"
        
        # 获取 S3 客户端
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
        
        # 尝试从 dataflow bucket 读取
        try:
            response = s3_client.get_object(Bucket=dataflow_bucket, Key=config_key)
            content = response['Body'].read().decode('utf-8')
            config = yaml.safe_load(content)
        except Exception:
            # 如果 dataflow bucket 中不存在，尝试从当前 bucket 读取
            try:
                response = s3_client.get_object(Bucket=bucket_name, Key=config_key)
                content = response['Body'].read().decode('utf-8')
                config = yaml.safe_load(content)
            except Exception:
                pass
    except Exception:
        # S3 读取失败，使用默认配置
        pass
    
    # 如果找到了配置文件，使用它
    if config:
        # 加载 DECIMAL 字段
        if 'decimal_fields' in config:
            for field_name in config['decimal_fields']:
                column_types[field_name] = "DECIMAL"
        
        # 加载 INTEGER 字段
        if 'integer_fields' in config:
            for field_name in config['integer_fields']:
                column_types[field_name] = "INTEGER"
        
        # 加载 TEXT 字段
        if 'text_fields' in config:
            for field_name in config['text_fields']:
                column_types[field_name] = "TEXT"
        
        # 加载 BOOLEAN 字段
        if 'boolean_fields' in config:
            for field_name in config['boolean_fields']:
                column_types[field_name] = "BOOLEAN"
        
        return column_types
    
    # 优先级 2: 如果配置文件不存在，使用内嵌的默认配置
    return _get_default_column_types()
    


def build_new_name_from_parent(parent_name: str, suffix: str) -> str:
    """根据父目录名生成规范文件名（用于 parquet 等后续阶段）。

    规则：取父目录名中第一个 '_' 之后、最后一个 '_' 之前的部分；
    如果找不到两个下划线，则退化为整个目录名；最后附加原扩展名 suffix。
    """
    first = parent_name.find("_")
    last = parent_name.rfind("_")
    if first != -1 and last != -1 and last > first:
        core = parent_name[first + 1 : last]
    else:
        core = parent_name

    core = core.strip(" _")
    return f"{core}{suffix}"


@task
def collect_raw_files(prefix: str = "") -> list[dict]:
    """
    步骤 1：从 S3 收集原始文件
    - **完全参考你提供的 list_files_flow 实现方式**
      - 使用 `S3Bucket.load("rustfs-s3storage")`
      - 调用 `s3_block.list_objects(folder=prefix)`
    - 在此基础上，转换为后续步骤需要的 `uploaded_files` 结构
    """
    logger = get_run_logger()

    # 和你提供的 list_s3_files 一样：直接使用传入的 prefix
    logger.info(f"Collecting files with prefix (no normalize): {prefix!r}")

    s3_block = S3Bucket.load("rustfs-s3storage")
    bucket_name = s3_block.bucket_name
    logger.info(f"Using bucket from S3Bucket block: {bucket_name}")

    # 直接用 list_objects(folder=prefix)，不做任何前缀处理
    objects = s3_block.list_objects(folder=prefix)
    objects_list = list(objects)
    logger.info(f"list_objects(folder={prefix!r}) returned {len(objects_list)} objects: {objects_list}")

    uploaded_files: List[Dict[str, str]] = []
    matched_count = 0

    for obj in objects_list:
        # list_objects 在当前环境下返回的是 dict，形如：
        # {"Key": "...", "LastModified": ..., "ETag": "...", "Size": ..., ...}
        # 这里优先处理 dict，然后再兼容字符串或带 key 属性的对象
        if isinstance(obj, dict) and "Key" in obj:
            key = str(obj["Key"])
        elif hasattr(obj, "key"):
            key = str(obj.key)
        else:
            key = str(obj)

        lower_key = key.lower()

        # 目录对象（以 / 结尾）直接跳过
        if key.endswith("/"):
            continue

        # 只保留 csv / xlsx / xls
        if not (
            lower_key.endswith(".csv")
            or lower_key.endswith(".xlsx")
            or lower_key.endswith(".xls")
        ):
            continue

        matched_count += 1

        # 相对路径：按你前端传入的 prefix 来截断，不再自己改动
        if prefix and key.startswith(prefix):
            rel_path = key[len(prefix) :]
        else:
            rel_path = key

        uploaded_files.append(
            {
                "relative_path": rel_path,
                "target_key": key,
                "bucket": bucket_name,
                "uploaded": True,
            }
        )

    logger.info(
        f"Finally collected {matched_count} csv/xls(x) files from list_objects, total uploaded_files: {len(uploaded_files)}"
    )
    return uploaded_files


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


@task
def clean_data(prefix: str, parquet_files: list[dict]) -> list[dict]:
    """
    步骤 3：数据清洗（已优化）
    - **注意**：清洗逻辑已在 convert_to_parquet 阶段完成（本地清洗后上传）
    - 此任务现在仅作为流程占位符，直接透传数据
    - 如需额外的清洗步骤（如去重、验证），可在此处添加
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

            # TODO: 在这里可以添加更多清洗步骤：
            # - 去重
            # - 数据验证
            # - 格式规范化
            # - 异常值处理
            # - 等等...

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


@task
def upload_processed_files(prefix: str, processed_files: list[dict]) -> int:
    """
    统一上传步骤：将所有处理后的文件上传到 S3
    - 从本地文件路径读取
    - 上传到对应的 S3 key
    """
    logger = get_run_logger()

    if not processed_files:
        logger.warning("No processed files to upload, skipping")
        return 0

    logger.info(f"Uploading {len(processed_files)} processed files to S3")

    # 使用 Prefect S3Bucket Block 获取客户端
    s3_block = S3Bucket.load("rustfs-s3storage")
    bucket_name = s3_block.bucket_name

    # 兼容不同版本的 S3Bucket
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

    uploaded_count = 0

    for item in processed_files:
        local_path = item.get("local_path")
        s3_key = item.get("parquet_key") or item.get("secure_key") or item.get("target_key")
        
        if not s3_key:
            logger.warning(f"No S3 key found for item {item}, skipping upload")
            continue
        
        if not local_path:
            logger.warning(f"No local_path found for {s3_key}, skipping upload")
            continue
            
        if not os.path.exists(local_path):
            logger.warning(f"Local file not found: {local_path} (for {s3_key}), skipping upload")
            # 尝试查找文件（可能在临时目录中）
            if "parquet" in local_path:
                # 尝试从文件名查找
                filename = os.path.basename(local_path)
                import glob
                possible_paths = glob.glob(f"/tmp/**/{filename}", recursive=True)
                if possible_paths:
                    local_path = possible_paths[0]
                    logger.info(f"Found file at alternative path: {local_path}")
                else:
                    continue
            else:
                continue

        try:
            logger.info(f"Uploading {local_path} -> s3://{bucket_name}/{s3_key}")
            s3_client.upload_file(local_path, bucket_name, s3_key)
            uploaded_count += 1
            
            # 上传成功后清理本地文件
            try:
                os.remove(local_path)
            except OSError:
                pass
        except Exception as exc:
            logger.error(f"Failed to upload {local_path} -> {s3_key}: {exc}")

    logger.info(f"Successfully uploaded {uploaded_count}/{len(processed_files)} files")
    return uploaded_count


@flow(name="dataset-validation-flow")
def validate_and_calibrate_data(prefix: str, cleaned_files: Optional[list[dict]] = None) -> Union[list[dict], dict]:
    """
    步骤 3.5：数值校准（循环校验版本）
    - 从 单位基本情况_611.parquet 和 调查单位基本情况_601.parquet 构建权威表
    - 通过 code 和 name 匹配校准其他表的数据
    - 检测并报告问题（code截断、一对多、缺失code等）
    - 如果有问题，暂停等待用户修复；修复后重新校验，直到没有问题为止
    """
    import json
    from collections import defaultdict
    
    logger = get_run_logger()
    
    # 兼容两种用法：
    # 1) 作为子 flow：主流程传入 cleaned_files 列表
    # 2) 作为独立 flow 部署并直接运行：只传 prefix，不传 cleaned_files
    cleaned_files = cleaned_files or []
    if not cleaned_files:
        logger.warning("No cleaned files to validate, skipping")
        return cleaned_files
    
    logger.info(f"Starting data validation and calibration for {len(cleaned_files)} files")
    
    # 循环校验：直到没有问题为止
    max_iterations = 10  # 防止无限循环
    iteration = 0
    
    while iteration < max_iterations:
        iteration += 1
        logger.info(f"=== Validation iteration {iteration} ===")
        
        # 0. 记录已修复的行，避免重复检测（在应用修复结果后记录）
        fixed_rows = set()  # 格式: (normalized_file_name, row_index)
        
        # 1. 找到权威表文件（611 和 601）- 每次循环都需要重新查找，因为文件路径可能变化
        authority_files = {}
        other_files = []
        
        for item in cleaned_files:
            normalized_name = item.get("normalized_name", "").replace(".parquet", "")
            if normalized_name == "单位基本情况_611":
                authority_files["611"] = item
            elif normalized_name == "调查单位基本情况_601":
                authority_files["601"] = item
            else:
                other_files.append(item)
        
        if not authority_files:
            logger.warning("No authority files (611 or 601) found, skipping calibration")
            return cleaned_files
        
        logger.info(f"Found authority files: {list(authority_files.keys())}")
        
        # 2. 构建权威表（每次循环都重新构建，因为修复可能改变了文件）
        authority_table = {}  # code -> name
        authority_table_reverse = {}  # name -> code
        code_lengths = defaultdict(list)  # 用于检测截断
        issues = {
            "truncated_codes": [],  # 被截断的 code
            "one_to_many_code": [],  # 一个 code 对应多个 name
            "one_to_many_name": [],  # 一个 name 对应多个 code
            "missing_codes": [],  # 缺失的 code
        }
        
        for file_type, item in authority_files.items():
            local_path = item.get("local_path")
            if not local_path or not os.path.exists(local_path):
                logger.warning(f"Authority file {file_type} local_path not found: {local_path}")
                continue
            
            try:
                df = pd.read_parquet(local_path)
                logger.info(f"Loaded authority file {file_type}: {len(df)} rows, columns: {list(df.columns)}")
                
                # 查找统一社会信用代码和单位详细名称列
                # 优先使用标准列名：'统一社会信用代码' 和 '单位详细名称'
                code_col = None
                name_col = None

                # 1) 优先精确匹配标准列名
                if "统一社会信用代码" in df.columns:
                    code_col = "统一社会信用代码"
                if "单位详细名称" in df.columns:
                    name_col = "单位详细名称"

                # 2) 如果没有标准列名，再用模糊匹配作为降级方案
                if code_col is None:
                    for col in df.columns:
                        if "统一社会信用代码" in col or "社会信用代码" in col:
                            code_col = col
                            break

                if name_col is None:
                    for col in df.columns:
                        if "单位详细名称" in col or "详细名称" in col:
                            name_col = col
                            break
                
                if not code_col:
                    logger.warning(f"Authority file {file_type}: No code column found")
                    continue
                if not name_col:
                    logger.warning(f"Authority file {file_type}: No name column found")
                    continue
                
                logger.info(f"Authority file {file_type}: Using code column '{code_col}', name column '{name_col}'")
                
                # 提取 code 和 name，去除空值
                for idx, row in df.iterrows():
                    code = str(row[code_col]).strip() if pd.notna(row[code_col]) else ""
                    name = str(row[name_col]).strip() if pd.notna(row[name_col]) else ""
                    
                    if not code or code.lower() in ['nan', 'none', 'null', '']:
                        continue
                    
                    # 检测和处理科学计数法格式（如 9.5E+17, 9.5M9234245 等）
                    code_original = code
                    code_clean = code.replace('-', '').replace(' ', '').upper()
                    
                    # 检测科学计数法格式（统一社会信用代码可能包含字母，如 93511903309457252D，这是正常的）
                    is_scientific = False
                    scientific_pattern = None
                    
                    import re
                    
                    # 检测标准科学计数法：数字.数字E+数字 或 数字.数字E数字
                    # 注意：统一社会信用代码可能以字母结尾（如 D），所以不能简单地用 isdigit() 判断
                    # 匹配格式如：9.35134E+17, 9.5E+17, 9.5E17 等
                    sci_match = re.match(r'^(\d+)\.(\d+)E\+?(\d+)$', code_clean, re.IGNORECASE)
                    if sci_match:
                        is_scientific = True
                        scientific_pattern = "standard"
                        logger.warning(f"Found scientific notation code in {file_type} row {idx}: {code_original}")
                    else:
                        # 检测可能的截断格式：如 9.5M9234245 (可能是 Excel 的截断格式)
                        # 注意：如果 code 包含小数点和 E/M/e/m/+，且不是正常的字母结尾格式，可能是科学计数法
                        # 正常格式应该是：18位数字+字母（可选），如 93511903309457252D
                        # 异常格式：包含小数点和科学计数法符号，如 9.5E+17, 9.5M9234245
                        if '.' in code_clean:
                            # 检查是否包含科学计数法符号（E, e, M, m, +）
                            # 但排除正常的情况：如果小数点后面直接跟数字和字母，可能是正常的（虽然不太可能）
                            if re.search(r'[EeMm]\+?', code_clean):
                                is_scientific = True
                                scientific_pattern = "truncated"
                                logger.warning(f"Found truncated/scientific notation code in {file_type} row {idx}: {code_original}")
                    
                    # 如果是科学计数法，标记为截断问题
                    if is_scientific:
                        issues["truncated_codes"].append({
                            "file": file_type,
                            "code": code_original,
                            "length": "scientific_notation",
                            "name": name,
                            "row_index": idx,
                            "pattern": scientific_pattern,
                            "note": "统一社会信用代码被转换为科学计数法格式（如 9.35134E+17），无法自动恢复原始值。需要用户手动修复 611/601 文件中的该 code，将其恢复为正确的 18 位统一社会信用代码格式（如 935119033094572520）"
                        })
                        continue  # 跳过这个 code，不加入权威表
                    
                    # 处理统一社会信用代码（可能包含数字和字母，如 93511903309457252D）
                    # 统一社会信用代码格式：18位，可能包含数字和字母（通常最后一位可能是字母）
                    # 使用正则表达式检查：主要是数字，可能以字母结尾
                    code_alphanumeric = re.sub(r'[^0-9A-Za-z]', '', code_clean)  # 只保留数字和字母
                    
                    if code_alphanumeric and len(code_alphanumeric) > 0:
                        # 检查长度（统一社会信用代码应该是18位）
                        if len(code_alphanumeric) != 18:
                            issues["truncated_codes"].append({
                                "file": file_type,
                                "code": code_original,
                                "length": len(code_alphanumeric),
                                "name": name,
                                "row_index": idx,
                                "note": f"统一社会信用代码长度应为18位，实际为{len(code_alphanumeric)}位"
                            })
                        else:
                            # 长度正确，记录到 code_lengths（用于统计）
                            code_lengths[code_alphanumeric].append(len(code_alphanumeric))
                    else:
                        # 完全不包含有效字符
                        if len(code_clean) > 0:
                            issues["truncated_codes"].append({
                                "file": file_type,
                                "code": code_original,
                                "length": len(code_clean),
                                "name": name,
                                "row_index": idx,
                                "note": "统一社会信用代码格式无效，不包含有效的数字或字母"
                            })
                        continue  # 跳过无效的 code
                    
                    if not name or name.lower() in ['nan', 'none', 'null', '']:
                        continue
                    
                    # 检查一对多关系
                    if code in authority_table:
                        if authority_table[code] != name:
                            issues["one_to_many_code"].append({
                                "code": code,
                                "existing_name": authority_table[code],
                                "new_name": name,
                                "file": file_type
                            })
                    else:
                        authority_table[code] = name
                    
                    if name in authority_table_reverse:
                        if authority_table_reverse[name] != code:
                            issues["one_to_many_name"].append({
                                "name": name,
                                "existing_code": authority_table_reverse[name],
                                "new_code": code,
                                "file": file_type
                            })
                    else:
                        authority_table_reverse[name] = code
                    
            except Exception as exc:
                logger.error(f"Failed to process authority file {file_type}: {exc}")
                continue
        
        logger.info(f"Authority table built: {len(authority_table)} code-name pairs")
        
        # 小工具：清理当前 prefix 下的校验中间文件，避免前端误判
        def _cleanup_validation_files(prefix_for_cleanup: str):
            """
            尽量清理当前前缀下的校验中间文件：
            - validation_report_pending.json
            - validation_resolutions.json
            """
            try:
                s3_block = S3Bucket.load("rustfs-s3storage")
                bucket_name = s3_block.bucket_name
                access_key = getattr(s3_block, "aws_access_key_id", None) or os.environ.get(
                    "RUSTFS_ACCESS_KEY", os.environ.get("MINIO_ROOT_USER")
                )
                secret_key = getattr(s3_block, "aws_secret_access_key", None) or os.environ.get(
                    "RUSTFS_SECRET_KEY", os.environ.get("MINIO_ROOT_PASSWORD")
                )
                endpoint_url = getattr(s3_block, "endpoint_url", None) or os.environ.get("RUSTFS_ENDPOINT")

                s3_client_local = boto3.client(
                    "s3",
                    endpoint_url=endpoint_url,
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_key,
                )

                # 构造一组可能的前缀，尽量覆盖不同写入方式
                candidates = set()
                base = prefix_for_cleanup.lstrip("/")
                if not base.endswith("/"):
                    base = f"{base}/"
                candidates.add(base)

                # 上一级目录（去掉最后一段）
                tmp = base.rstrip("/")
                if "/" in tmp:
                    parent = tmp.rsplit("/", 1)[0] + "/"
                    candidates.add(parent)

                # 如果路径中包含 sourcedata，额外尝试以 sourcedata/ 为界的前缀
                if "sourcedata/" in base:
                    before, after = base.split("sourcedata/", 1)
                    sourcedata_prefix = before + "sourcedata/"
                    candidates.add(sourcedata_prefix)

                keys_to_delete = []
                for cand in candidates:
                    logger.info(f"Attempting to cleanup validation temp files under prefix candidate: {cand}")
                    resp = s3_client_local.list_objects_v2(
                        Bucket=bucket_name,
                        Prefix=cand,
                    )
                    for obj in resp.get("Contents", []):
                        key = obj.get("Key", "")
                        if key.endswith("validation_report_pending.json") or key.endswith("validation_resolutions.json"):
                            if key not in keys_to_delete:
                                keys_to_delete.append(key)

                if not keys_to_delete:
                    logger.info("No validation temp files found to cleanup")
                    return

                delete_req = {"Objects": [{"Key": k} for k in keys_to_delete]}
                s3_client_local.delete_objects(Bucket=bucket_name, Delete=delete_req)
                logger.info(f"Deleted validation temp files: {keys_to_delete}")
            except Exception as cleanup_err:
                logger.warning(f"Failed to cleanup validation temp files: {cleanup_err}")

        # 3. 检查是否已有修复结果（用户已处理）
        # 重要：修复结果应该在应用修复后立即删除，避免重复应用
        resolutions = None
        had_resolutions = False  # 标记本次运行是否加载过用户的修复结果
        resolutions_s3_key = None
        should_delete_resolutions = False  # 标记是否应该删除修复结果文件
        try:
            s3_block = S3Bucket.load("rustfs-s3storage")
            bucket_name = s3_block.bucket_name
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
            
            # 检查修复结果文件
            normalized_prefix = prefix.lstrip("/")
            if not normalized_prefix.endswith("/"):
                normalized_prefix = f"{normalized_prefix}/"
            resolutions_s3_key = f"{normalized_prefix}validation_resolutions.json"
            
            try:
                response = s3_client.get_object(Bucket=bucket_name, Key=resolutions_s3_key)
            resolutions_content = response['Body'].read().decode('utf-8')
            resolutions = json.loads(resolutions_content)
            had_resolutions = True
            logger.info(f"Found validation resolutions, applying fixes...")
            
            # 应用修复结果到权威表
            if resolutions.get("resolutions"):
                res = resolutions["resolutions"]
                
                # 应用截断代码的修复
                if res.get("truncated_codes"):
                    for fix in res["truncated_codes"]:
                        old_code = fix.get("code", "").strip()
                        new_code = fix.get("fixed_code", "").strip()
                        if old_code and new_code and old_code in authority_table:
                            # 更新权威表
                            name = authority_table[old_code]
                            authority_table[new_code] = name
                            authority_table_reverse[name] = new_code
                            # 移除旧的映射
                            if old_code in authority_table:
                                del authority_table[old_code]
                            logger.info(f"Applied fix: {old_code} -> {new_code}")
                
                # 应用一对多 code 的修复
                if res.get("one_to_many_code"):
                    for fix in res["one_to_many_code"]:
                        code = fix.get("code", "").strip()
                        selected_name = fix.get("selected_name", "").strip()
                        if code and selected_name:
                            authority_table[code] = selected_name
                            authority_table_reverse[selected_name] = code
                            logger.info(f"Applied fix: code {code} -> name {selected_name}")
                
                # 应用一对多 name 的修复
                if res.get("one_to_many_name"):
                    for fix in res["one_to_many_name"]:
                        name = fix.get("name", "").strip()
                        selected_code = fix.get("selected_code", "").strip()
                        if name and selected_code:
                            authority_table[selected_code] = name
                            authority_table_reverse[name] = selected_code
                            logger.info(f"Applied fix: name {name} -> code {selected_code}")
                
                # 应用缺失 code 的修复
                if res.get("missing_codes"):
                    for fix in res["missing_codes"]:
                        name = fix.get("name", "").strip()
                        code = fix.get("code", "").strip()
                        file_name = fix.get("file", "").strip()
                        row_index = fix.get("row_index")
                        
                        if name and code:
                            # 添加到权威表
                            authority_table[code] = name
                            authority_table_reverse[name] = code
                            logger.info(f"Applied fix: missing code {code} for name {name}")
                            
                            # 直接回填到原始数据文件
                            # 查找对应的文件并更新（在 cleaned_files 和 other_files 中查找）
                            target_file = None
                            
                            # 标准化 file_name（去掉 .parquet 后缀，统一格式）
                            normalized_file_name = file_name.replace(".parquet", "").strip()
                            
                            # 先在其他文件中查找（这些是需要校验的文件）
                            for item in other_files:
                                item_normalized = item.get("normalized_name", "").replace(".parquet", "").strip()
                                # 严格匹配：完全匹配或包含关系
                                if item_normalized == normalized_file_name or normalized_file_name == item_normalized:
                                    target_file = item
                                    logger.debug(f"Found target file in other_files: {item.get('normalized_name')} matches {file_name}")
                                    break
                            
                            # 如果没找到，在 cleaned_files 中查找（包括权威表文件）
                            if not target_file:
                                for item in cleaned_files:
                                    item_normalized = item.get("normalized_name", "").replace(".parquet", "").strip()
                                    if item_normalized == normalized_file_name or normalized_file_name == item_normalized:
                                        target_file = item
                                        logger.debug(f"Found target file in cleaned_files: {item.get('normalized_name')} matches {file_name}")
                                        break
                            
                            if target_file:
                                local_path = target_file.get("local_path")
                                if local_path and os.path.exists(local_path):
                                    try:
                                        df = pd.read_parquet(local_path)
                                        
                                        # 查找 code 和 name 列
                                        code_col = None
                                        name_col = None
                                        for col in df.columns:
                                            if "统一社会信用代码" in col or "社会信用代码" in col:
                                                code_col = col
                                            if "单位详细名称" in col or "详细名称" in col:
                                                name_col = col
                                        
                                        if code_col and name_col and row_index is not None:
                                            # 根据 row_index 和 name 匹配行
                                            # row_index 是 DataFrame 的原始索引（从0开始），前端显示时会+1
                                            # 所以这里直接使用 row_index，不需要转换
                                            df_row_index = row_index
                                            
                                            # 确保索引在有效范围内
                                            if 0 <= df_row_index < len(df):
                                                # 验证 name 是否匹配
                                                if name_col in df.columns:
                                                    actual_name = str(df.iloc[df_row_index][name_col]).strip() if pd.notna(df.iloc[df_row_index][name_col]) else ""
                                                    if actual_name == name:
                                                        # 更新 code
                                                        old_code = str(df.iloc[df_row_index][code_col]).strip() if pd.notna(df.iloc[df_row_index][code_col]) else ""
                                                        df.iloc[df_row_index, df.columns.get_loc(code_col)] = code
                                                        df.to_parquet(local_path, index=False)
                                                        logger.info(f"Applied fix to file {file_name} at row {row_index} (index {df_row_index}): {name} -> {code} (old: {old_code})")
                                                        
                                                        # 立即验证修复是否成功
                                                        try:
                                                            df_verify = pd.read_parquet(local_path)
                                                            verify_code = str(df_verify.iloc[df_row_index][code_col]).strip() if pd.notna(df_verify.iloc[df_row_index][code_col]) else ""
                                                            if verify_code == code:
                                                                logger.info(f"✓ Verified: fix successfully applied to {file_name} row {row_index}")
                                                            else:
                                                                logger.error(f"✗ Warning: fix verification failed for {file_name} row {row_index}, expected '{code}', got '{verify_code}'")
                                                        except Exception as verify_exc:
                                                            logger.warning(f"Failed to verify fix for {file_name} row {row_index}: {verify_exc}")
                                                    else:
                                                        logger.warning(f"Name mismatch at row {row_index} in {file_name}: expected '{name}', found '{actual_name}'")
                                                else:
                                                    # 如果没有 name 列，直接根据 row_index 更新
                                                    df.iloc[df_row_index, df.columns.get_loc(code_col)] = code
                                                    df.to_parquet(local_path, index=False)
                                                    logger.info(f"Applied fix to file {file_name} at row {row_index}: code = {code}")
                                            else:
                                                logger.warning(f"Row index {df_row_index} out of range for file {file_name} (length: {len(df)})")
                                    except Exception as file_fix_exc:
                                        logger.error(f"Failed to apply fix to file {file_name}: {file_fix_exc}")
                                else:
                                    logger.warning(f"File {file_name} not found locally: {local_path}")
                            else:
                                logger.warning(f"File {file_name} not found in cleaned_files list")
                
                logger.info("Validation resolutions applied successfully, continuing with calibration...")
                
                # 记录已修复的行，避免在重新校验时重复检测
                applied_count = 0
                if res.get("missing_codes"):
                    logger.info(f"Recording {len(res.get('missing_codes', []))} fixed missing codes to skip during re-validation")
                    for fix in res["missing_codes"]:
                        file_name = fix.get("file", "").strip()
                        row_index = fix.get("row_index")
                        code = fix.get("code", "").strip()
                        if file_name and row_index is not None and code:
                            # 标准化文件名（去掉 .parquet 后缀）
                            normalized_file_name = file_name.replace(".parquet", "")
                            fixed_rows.add((normalized_file_name, row_index))
                            applied_count += 1
                            logger.debug(f"Marked row as fixed: {normalized_file_name} row {row_index} code={code}")
                
                logger.info(f"Total {applied_count} rows marked as fixed (out of {len(res.get('missing_codes', []))}), will skip them during validation")
                
                # 无论本次修复中是否包含 missing_codes，只要成功加载并应用了 resolutions，
                # 都应该删除 resolutions 文件，避免后续重复应用同一批修复。
                should_delete_resolutions = True
                
                # 重要：应用修复后，文件已经直接修改到本地文件系统
                # cleaned_files 列表中的 local_path 指向的文件已经被修改
                # 后续步骤（encrypt_sensitive_fields、upload_processed_files）会使用这些 local_path
                # 所以修复后的文件会被正确上传到 S3
                logger.info("Fixes applied to local files. Files will be uploaded to S3 in subsequent steps.")
            except s3_client.exceptions.NoSuchKey:
                logger.info("No validation resolutions found, proceeding with normal validation...")
            except Exception as res_exc:
                logger.warning(f"Failed to read resolutions: {res_exc}, proceeding with normal validation...")
            
            # 如果修复结果已成功应用，删除修复结果文件，避免重复应用，
            # 并使用统一的清理函数删除当前前缀下所有相关中间文件
            if should_delete_resolutions and resolutions_s3_key:
                try:
                    s3_client.delete_object(Bucket=bucket_name, Key=resolutions_s3_key)
                    logger.info(f"Deleted validation resolutions file: {resolutions_s3_key} (to prevent re-application)")
                except Exception as delete_exc:
                    logger.warning(f"Failed to delete resolutions file {resolutions_s3_key}: {delete_exc}")

                # 额外：在成功应用修复后，尽量清理当前前缀下所有校验中间文件
                _cleanup_validation_files(prefix)
        except Exception as s3_exc:
            logger.warning(f"Failed to check for resolutions: {s3_exc}, proceeding with normal validation...")
        
        # 4. 检查问题并生成报告
        has_critical_issues = False
        
        if issues["truncated_codes"]:
            logger.error(f"Found {len(issues['truncated_codes'])} truncated codes")
            has_critical_issues = True
        
        if issues["one_to_many_code"]:
            logger.error(f"Found {len(issues['one_to_many_code'])} one-to-many code mappings")
            has_critical_issues = True
        
        if issues["one_to_many_name"]:
            logger.error(f"Found {len(issues['one_to_many_name'])} one-to-many name mappings")
            has_critical_issues = True
        
        if issues["missing_codes"]:
            logger.error(f"Found {len(issues['missing_codes'])} rows with missing codes")
            has_critical_issues = True
        
        logger.info(f"Critical issues check: has_critical_issues={has_critical_issues}")
        logger.info(f"Issues summary: truncated={len(issues['truncated_codes'])}, one_to_many_code={len(issues['one_to_many_code'])}, one_to_many_name={len(issues['one_to_many_name'])}, missing_codes={len(issues['missing_codes'])}")

        # 如果没有关键问题，清理可能存在的旧的待处理校验报告和修复结果，避免前端误判
        if not has_critical_issues:
            _cleanup_validation_files(prefix)
        
        # 5. 校准其他文件
        calibrated_files = []
    
    # fixed_rows 已经在步骤 3 中定义和填充（如果应用了修复结果）
    # 如果没有修复结果，fixed_rows 是空集合，不影响正常流程
    
    for item in other_files:
        local_path = item.get("local_path")
        if not local_path or not os.path.exists(local_path):
            calibrated_files.append(item)
            continue
        
        try:
            df = pd.read_parquet(local_path)
            original_rows = len(df)
            
            # 查找 code 和 name 列
            code_col = None
            name_col = None
            
            for col in df.columns:
                if "统一社会信用代码" in col or "社会信用代码" in col:
                    code_col = col
                if "单位详细名称" in col or "详细名称" in col:
                    name_col = col
            
            if code_col and name_col:
                # 先通过 code 匹配 name
                if code_col in df.columns:
                    def match_name_by_code(code_val):
                        if pd.isna(code_val):
                            return None
                        code_str = str(code_val).strip()
                        if code_str in authority_table:
                            return authority_table[code_str]
                        return None
                    
                    # 创建临时列存储匹配的 name
                    df['_matched_name_by_code'] = df[code_col].apply(match_name_by_code)
                    
                    # 如果匹配到 name，更新 name 列
                    if name_col in df.columns:
                        mask = df['_matched_name_by_code'].notna()
                        if mask.any():
                            df.loc[mask, name_col] = df.loc[mask, '_matched_name_by_code']
                            logger.info(f"Matched {mask.sum()} names by code in {item.get('normalized_name', 'unknown')}")
                    
                    df = df.drop(columns=['_matched_name_by_code'])
                
                # 再通过 name 匹配 code
                if name_col in df.columns:
                    def match_code_by_name(name_val):
                        if pd.isna(name_val):
                            return None
                        name_str = str(name_val).strip()
                        if name_str in authority_table_reverse:
                            return authority_table_reverse[name_str]
                        return None
                    
                    # 创建临时列存储匹配的 code
                    df['_matched_code_by_name'] = df[name_col].apply(match_code_by_name)
                    
                    # 如果匹配到 code，更新 code 列
                    if code_col in df.columns:
                        mask = df['_matched_code_by_name'].notna()
                        if mask.any():
                            df.loc[mask, code_col] = df.loc[mask, '_matched_code_by_name']
                            logger.info(f"Matched {mask.sum()} codes by name in {item.get('normalized_name', 'unknown')}")
                    
                    df = df.drop(columns=['_matched_code_by_name'])
                    
                    # 检查缺失的 code
                    # 注意：只在通过 name 匹配 code 之后，如果仍然没有 code 且 name 存在的情况下才标记为缺失
                    # 如果原始数据中就有 code，不应该标记为缺失
                    if code_col in df.columns:
                        # 更严格的缺失判断：排除可能被误判的情况
                        def is_code_missing(code_val):
                            """判断 code 是否真正缺失"""
                            if pd.isna(code_val):
                                return True
                            code_str = str(code_val).strip()
                            # 排除空字符串和常见的空值表示
                            if code_str in ['', 'nan', 'none', 'null', 'None', 'NULL', 'NaN', 'N/A', 'n/a']:
                                return True
                            # 排除只有空格的情况
                            if not code_str or code_str.isspace():
                                return True
                            return False
                        
                        # 只在通过 name 匹配 code 之后检查
                        # 如果 code 列在匹配后仍然为空，且 name 也不为空，才标记为缺失
                        after_match_missing_mask = df[code_col].apply(is_code_missing)
                        
                        # 只标记那些：code 缺失 且 name 存在 的情况
                        # 如果 name 也为空，可能是整行数据都不完整，不算作需要处理的缺失 code
                        if name_col in df.columns:
                            def is_name_valid(name_val):
                                """判断 name 是否有效"""
                                if pd.isna(name_val):
                                    return False
                                name_str = str(name_val).strip()
                                return name_str not in ['', 'nan', 'none', 'null', 'None', 'NULL', 'NaN', 'N/A', 'n/a'] and not name_str.isspace()
                            
                            name_valid = df[name_col].apply(is_name_valid)
                            missing_with_name = after_match_missing_mask & name_valid
                            
                            if missing_with_name.any():
                                missing_indices = df[missing_with_name].index.tolist()
                                missing_data = df.loc[missing_with_name, [name_col]].to_dict('records')
                                file_name = item.get("normalized_name", "unknown")
                                
                                # 过滤掉已经修复的行
                                new_missing_codes = []
                                normalized_file_name = file_name.replace(".parquet", "")
                                for idx, row in enumerate(missing_data):
                                    row_index = int(missing_indices[idx]) if idx < len(missing_indices) else idx
                                    # 检查这一行是否已经被修复
                                    if (normalized_file_name, row_index) not in fixed_rows:
                                        new_missing_codes.append({
                                            "file": file_name,
                                            "name": str(row.get(name_col, "")).strip() if row.get(name_col) else "",
                                            # row_index 是 DataFrame 的索引（从 0 开始）
                                            # 为了更符合用户习惯，我们保存原始索引，前端可以显示为 row_index + 1
                                            "row_index": row_index
                                        })
                                    else:
                                        logger.debug(f"Skipping already fixed row: {file_name} row {row_index}")
                                
                                if new_missing_codes:
                                    issues["missing_codes"].extend(new_missing_codes)
                                    logger.info(f"Found {len(new_missing_codes)} new rows with missing codes but valid names in {file_name} (skipped {missing_with_name.sum() - len(new_missing_codes)} already fixed)")
                                else:
                                    logger.info(f"All missing codes in {file_name} have been fixed, no new issues found")
                        else:
                            # 如果没有 name 列，不检查缺失 code（因为无法通过 name 匹配）
                            if after_match_missing_mask.any():
                                logger.debug(f"Found {after_match_missing_mask.sum()} rows with missing codes in {item.get('normalized_name', 'unknown')}, but no name column to match")
            
            # 保存校准后的数据
            df.to_parquet(local_path, index=False)
            logger.info(f"Calibrated {item.get('normalized_name', 'unknown')}: {original_rows} rows")
            
            calibrated_files.append(item)
            
        except Exception as exc:
            logger.error(f"Failed to calibrate {item.get('normalized_name', 'unknown')}: {exc}")
            calibrated_files.append(item)
    
    # 5. 将权威表文件也加入结果
    for item in authority_files.values():
        calibrated_files.append(item)
    
    # 6. 在遍历完所有文件后，再次检查是否有严重问题（特别是 missing_codes）
    # 因为 missing_codes 是在遍历其他文件时添加的，需要在遍历完成后再次检查
    if issues["missing_codes"]:
        logger.error(f"Found {len(issues['missing_codes'])} rows with missing codes after calibration")
        has_critical_issues = True
    
    # 再次记录最终的问题统计
    logger.info(f"Final critical issues check: has_critical_issues={has_critical_issues}")
    logger.info(f"Final issues summary: truncated={len(issues['truncated_codes'])}, one_to_many_code={len(issues['one_to_many_code'])}, one_to_many_name={len(issues['one_to_many_name'])}, missing_codes={len(issues['missing_codes'])}")
    
    # 如果本次运行已经加载过用户修复结果（第二轮运行），不再生成新的交互报告，
    # 无论 has_critical_issues 是否为 True，都认为“本轮交互结束”，只记录日志。
    if had_resolutions:
        logger.info(
            "User resolutions have been applied in this run; "
            "skip generating new validation_report_pending.json even if there are remaining issues."
        )
        has_critical_issues = False

        # 8. 即使没有严重问题，也生成包含缺失 code 的报告（如果存在）
        if issues["missing_codes"]:
            report = {
                "timestamp": pd.Timestamp.now().isoformat(),
                "issues": {
                    "missing_codes": issues["missing_codes"],
                },
                "authority_table_size": len(authority_table),
                "summary": {
                    "missing_codes_count": len(issues["missing_codes"]),
                }
            }
            
            report_path = os.path.join(tempfile.mkdtemp(prefix="dataset_etl_validation_"), "missing_codes_report.json")
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2)
            
            logger.warning(f"Found {len(issues['missing_codes'])} rows with missing codes. Report saved to: {report_path}")
            
            # 尝试上传到 S3
            try:
                s3_block = S3Bucket.load("rustfs-s3storage")
                bucket_name = s3_block.bucket_name
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
                
                normalized_prefix = prefix.lstrip("/")
                if not normalized_prefix.endswith("/"):
                    normalized_prefix = f"{normalized_prefix}/"
                report_s3_key = f"{normalized_prefix}missing_codes_report_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.json"
                
                s3_client.upload_file(report_path, bucket_name, report_s3_key)
                logger.info(f"Missing codes report uploaded to s3://{bucket_name}/{report_s3_key}")
            except Exception:
                pass  # 静默失败，不影响主流程
        
        # 9. 判断是否继续循环
        # 如果没有严重问题，清理 JSON 文件并返回结果（结束循环）
        if not has_critical_issues:
            logger.info("No critical issues found, validation passed. Cleaning up and returning calibrated files.")
            _cleanup_validation_files(prefix)
            logger.info("Data validation and calibration completed successfully")
            return calibrated_files
        
        # 如果有严重问题，检查是否有 resolutions（用户已修复）
        # 如果没有 resolutions，生成报告并暂停（结束循环，等待用户修复）
        # 如果有 resolutions，说明已经应用了修复，继续循环重新校验
        if not had_resolutions:
            # 第一次发现问题，生成报告并暂停
            logger.info("Critical issues found but no resolutions yet. Generating report and pausing.")
            # 这里会生成报告、暂停并 return，所以不会继续循环
            # 代码会继续执行到下面的 pause 逻辑
            pass
        else:
            # 已经应用了修复，但还有问题，继续循环重新校验
            logger.info("Resolutions applied but issues still exist. Continuing to next iteration...")
            # 清理已应用的 resolutions 文件，避免重复应用
            if should_delete_resolutions and resolutions_s3_key:
                try:
                    s3_client.delete_object(Bucket=bucket_name, Key=resolutions_s3_key)
                    logger.info(f"Deleted validation resolutions file: {resolutions_s3_key}")
                except Exception as delete_exc:
                    logger.warning(f"Failed to delete resolutions file: {delete_exc}")
            # 继续下一次循环
            continue
        
    # 7. 如果有严重问题（且本次不是带修复结果的重跑），生成报告并保存到 S3，等待用户在前端界面处理
    # 严重问题包括：code截断、一对多关系、缺失 code
    report_s3_key = None
    if has_critical_issues:
        # 生成报告文件
        # 获取当前验证子 Flow 的 flow run id（优先使用 runtime 提供的 id）
        try:
            from prefect.runtime import flow_run as runtime_flow_run
            current_flow_run_id = getattr(runtime_flow_run, "id", None)
        except Exception:
            current_flow_run_id = None

        if not current_flow_run_id:
            current_flow_run_id = os.environ.get("PREFECT_FLOW_RUN_ID", "unknown")

        report = {
            "timestamp": pd.Timestamp.now().isoformat(),
            "prefix": prefix,
            # 这里记录的是验证子 Flow 的 flow_run_id，前端需要用它来恢复暂停的子 Flow
            "flow_run_id": current_flow_run_id,
            "status": "pending_user_action",  # 标记为等待用户处理
            "issues": issues,
            "authority_table_size": len(authority_table),
            "summary": {
                "truncated_codes_count": len(issues["truncated_codes"]),
                "one_to_many_code_count": len(issues["one_to_many_code"]),
                "one_to_many_name_count": len(issues["one_to_many_name"]),
                "missing_codes_count": len(issues["missing_codes"]),
            },
            "instructions": {
                "action_required": "请在前端界面修复以下问题后，重新运行流程",
                "truncated_codes": "需要将科学计数法格式（如 9.35134E+17）恢复为正确的 18 位统一社会信用代码",
                "one_to_many": "需要解决一对多关系，确保每个 code 对应唯一的 name，每个 name 对应唯一的 code",
                "missing_codes": "需要为缺失统一社会信用代码的单位补充正确的 18 位统一社会信用代码"
            }
        }
        
        # 保存报告到临时文件
        report_path = os.path.join(tempfile.mkdtemp(prefix="dataset_etl_validation_"), "validation_report.json")
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        logger.error(f"Critical validation issues found. Report saved to: {report_path}")
        logger.error(f"Summary: {report['summary']}")
        logger.error("流程已暂停，等待用户在前端界面处理问题")
        
        # 上传报告到 S3（前端可以从这里读取）
        try:
            s3_block = S3Bucket.load("rustfs-s3storage")
            bucket_name = s3_block.bucket_name
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
            
            # 生成 S3 key（使用固定的路径，方便前端查找）
            normalized_prefix = prefix.lstrip("/")
            if not normalized_prefix.endswith("/"):
                normalized_prefix = f"{normalized_prefix}/"
            # 使用固定的文件名，方便前端查找最新的报告
            report_s3_key = f"{normalized_prefix}validation_report_pending.json"
            
            s3_client.upload_file(report_path, bucket_name, report_s3_key)
            logger.info(f"Validation report uploaded to s3://{bucket_name}/{report_s3_key}")
            logger.info("前端界面可以从该 S3 路径读取报告，展示给用户处理")
        except Exception as upload_exc:
            logger.warning(f"Failed to upload report to S3: {upload_exc}, keeping local file only")
            report_s3_key = None
        
        # 使用 Prefect 3 的机制来暂停流程，等待用户在前端界面处理
        # 根据 Prefect 3 API 文档，使用客户端 API 直接设置状态为 PAUSED
        logger.info("Attempting to pause flow run for user intervention...")
        logger.info(f"报告已上传到 S3: s3://{bucket_name}/{report_s3_key}" if report_s3_key else f"报告保存在: {report_path}")
        
        # 获取当前 flow run ID
        current_flow_run_id = os.environ.get("PREFECT_FLOW_RUN_ID")
        try:
            from prefect.runtime import flow_run as runtime_flow_run
            if hasattr(runtime_flow_run, 'id'):
                current_flow_run_id = runtime_flow_run.id
                logger.info(f"Got flow run ID from runtime: {current_flow_run_id}")
        except Exception as runtime_err:
            logger.debug(f"Could not get flow run ID from runtime: {runtime_err}")
        
        if not current_flow_run_id:
            logger.warning("Could not get flow run ID, will try alternative methods")
        
        paused = False
        pause_method = None
        
        # 方法1: 使用 Prefect 3 推荐的 pause_flow_run() 函数（官方推荐方式）
        # 参考: https://docs.prefect.io/v3/advanced/interactive#pause-or-suspend-a-flow-until-it-receives-input
        try:
            from prefect.flow_runs import pause_flow_run
            
            logger.info("Calling pause_flow_run() to pause flow run (Prefect 3 recommended method)...")
            # pause_flow_run() 会暂停当前 flow run，这是 Prefect 3 推荐的方式
            # 不需要传递参数，函数会自动暂停当前执行的 flow run
            pause_flow_run()
            logger.info("Flow run successfully paused using prefect.flow_runs.pause_flow_run()")
            paused = True
            pause_method = "pause_flow_run"
        except ImportError as import_err:
            logger.warning(f"prefect.flow_runs.pause_flow_run not available: {import_err}")
        except Exception as pause_err:
            logger.warning(f"pause_flow_run() failed: {pause_err}")
        
        # 方法2: 如果 pause_flow_run 失败，尝试使用 suspend_flow_run
        if not paused:
            try:
                from prefect.flow_runs import suspend_flow_run
                logger.info("Calling suspend_flow_run() to suspend flow run...")
                suspend_flow_run()
                logger.info("Flow run suspended using prefect.flow_runs.suspend_flow_run()")
                paused = True
                pause_method = "suspend_flow_run"
            except ImportError:
                logger.warning("prefect.flow_runs.suspend_flow_run also not available")
            except Exception as suspend_err:
                logger.warning(f"suspend_flow_run() failed: {suspend_err}")
        
        # 方法3: 如果上述方法都失败，使用 Prefect 客户端 API 直接设置状态为 PAUSED（备用方案）
        if not paused and current_flow_run_id:
            try:
                from prefect import get_client
                from prefect.states import Paused
                import asyncio
                
                async def set_paused_state():
                    async with get_client() as client:
                        paused_state = Paused(
                            message="数据校准发现严重问题，需要用户在前端界面处理。请查看 S3 报告并修复问题后继续。"
                        )
                        result = await client.set_flow_run_state(
                            flow_run_id=current_flow_run_id,
                            state=paused_state,
                            force=True
                        )
                        logger.info(f"Flow run state set to PAUSED via client API")
                        return result
                
                try:
                    loop = asyncio.get_event_loop()
                except RuntimeError:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                
                result = loop.run_until_complete(set_paused_state())
                logger.info(f"Flow run successfully paused via Prefect client API (flow_run_id: {current_flow_run_id})")
                paused = True
                pause_method = "client API"
            except Exception as client_err:
                logger.warning(f"Failed to set state via client API: {client_err}")
        
        # 方法4: 尝试使用 Prefect 2.x 的方式（向后兼容）
        if not paused:
            try:
                from prefect.runtime import flow_run as runtime_flow_run
                from prefect.states import Suspended
                
                suspended_state = Suspended(
                    message="数据校准发现严重问题，需要用户在前端界面处理。请查看 S3 报告并修复问题后继续。"
                )
                runtime_flow_run.set_state(suspended_state)
                logger.info("Flow run state set to SUSPENDED using prefect.runtime.flow_run.set_state (Prefect 2.x)")
                paused = True
                pause_method = "runtime.set_state"
            except ImportError:
                logger.warning("prefect.runtime.flow_run.set_state not available")
            except Exception as set_state_err:
                logger.warning(f"set_state failed: {set_state_err}")
        
        # 记录暂停结果
        if paused:
            logger.info(f"Flow run paused successfully using method: {pause_method}")
            # 如果暂停成功，返回一个包含暂停信息的字典，而不是抛出异常
            # 这样可以避免 task 状态变为 Failed，同时保持 flow run 的暂停状态
            # 注意：Prefect 3 中，如果 flow run 已经被暂停，task 可以正常返回
            # 但是我们需要确保前端能够检测到暂停状态
            logger.info("Flow run paused, task will return normally to allow frontend to detect paused state")
            # 返回一个包含暂停信息的字典，这样 flow 可以继续但处于暂停状态
            return {
                "_paused": True,
                "_flow_run_id": current_flow_run_id,
                "_report_path": report_path,
                "_report_s3_key": report_s3_key,
                "_issues_summary": {
                    "truncated_codes": len(issues['truncated_codes']),
                    "one_to_many_code": len(issues['one_to_many_code']),
                    "one_to_many_name": len(issues['one_to_many_name']),
                    "missing_codes": len(issues['missing_codes']),
                },
                "_calibrated_files": calibrated_files  # 返回已校准的文件列表
            }
        else:
            logger.error("All pause/suspend methods failed, will raise exception to stop execution")
            # 如果所有暂停方法都失败，抛出异常
            error_msg = (
                f"数据校准发现严重问题，但暂停操作失败，流程将停止：\n"
                f"- 截断的 code: {len(issues['truncated_codes'])} 个\n"
                f"- 一对多 code: {len(issues['one_to_many_code'])} 个\n"
                f"- 一对多 name: {len(issues['one_to_many_name'])} 个\n"
                f"- 缺失的 code: {len(issues['missing_codes'])} 个\n"
                f"详细报告已保存到: {report_path}\n"
                + (f"报告已上传到 S3: s3://{bucket_name}/{report_s3_key}\n" if report_s3_key else "")
                + f"请在前端界面查看报告并修复问题后，重新运行流程。"
            )
            raise ValueError(error_msg)
    
    # 如果循环结束（达到最大迭代次数），返回当前结果
    logger.warning(f"Reached maximum iterations ({max_iterations}), returning current calibrated files")
    return calibrated_files


@task
def encrypt_sensitive_fields(prefix: str, cleaned_files: list[dict]) -> list[dict]:
    """
    步骤 4：加密敏感字段
    - 对 cleaned 数据中的敏感字段做脱敏/加密
    - 写入 secure 区
    """
    logger = get_run_logger()

    if not cleaned_files:
        logger.warning("No cleaned files to encrypt, skipping")
        return []

    logger.info(f"Encrypting {len(cleaned_files)} files")

    # 统一前缀格式
    normalized_prefix = prefix.lstrip("/")
    if not normalized_prefix.endswith("/"):
        normalized_prefix = f"{normalized_prefix}/"

    # 计算 secure 前缀
    if "/raw_extracted/" in normalized_prefix:
        secure_prefix = normalized_prefix.replace("/raw_extracted/", "/secure/", 1)
    else:
        secure_prefix = f"{normalized_prefix}secure/"

    # 使用 Prefect S3Bucket Block
    s3_block = S3Bucket.load("rustfs-s3storage")
    bucket_name = s3_block.bucket_name

    secure_files = []

    # TODO: 实现具体的加密逻辑
    # 1. 加载加密配置
    # 2. 对指定字段进行处理
    # 3. 输出到 secure 区
    # 目前先透传 cleaned_files，仅在 metadata 中标记 secure_key，后续可扩展为真实加密写入
    for item in cleaned_files:
        secure_key = item.get("parquet_key", "").replace("/raw_extracted/", "/secure/")
        # 保持本地路径，不上传
        secure_files.append(
            {
                "secure_key": secure_key,
                "parquet_key": secure_key,  # 更新为 secure 路径
                "local_path": item.get("local_path"),  # 保留本地路径
                **{k: v for k, v in item.items() if k not in ["parquet_key", "secure_key"]},
            }
        )

    logger.info(f"Encrypted {len(secure_files)} files (placeholder, local files preserved)")
    return secure_files


@task
def aggregate_data(prefix: str, secure_files: list[dict]) -> dict:
    """
    步骤 5：数据汇总
    - 读取 secure 区数据
    - 执行汇总 SQL
    - 输出汇总结果
    """
    logger = get_run_logger()

    if not secure_files:
        logger.warning("No secure files to aggregate, skipping")
        return {}

    logger.info(f"Aggregating {len(secure_files)} files")

    # TODO: 实现具体的汇总逻辑
    # 1. 连接 DuckDB / Postgres
    # 2. 将 secure_files 注册成表
    # 3. 执行聚合 SQL
    # 4. 输出结果表

    logger.info("Aggregation completed (placeholder)")
    return {}


def dataset_etl_flow(prefix: str = ""):
    """
    数据集 ETL 处理流程主函数
    
    参数:
        prefix: S3 对象前缀，例如 "team-1/dataset-2/sourcedata/"
    
    处理步骤:
        1. 收集原始文件
        2. 转换为 Parquet
        3. 数据清洗
        4. 加密敏感字段
        5. 数据汇总
    """
    # 注意：本函数现在是普通 Python 函数，不再是 Prefect Flow
    # 日志统一复用外层 Flow（list_files_flow）的 logger，避免直接依赖 get_run_logger
    import logging
    logger = logging.getLogger("dataset_etl_flow")
    logger.setLevel(logging.INFO)
    
    if not prefix:
        logger.error("prefix is required")
        raise ValueError("prefix parameter is required")

    logger.info(f"Starting dataset ETL pipeline with prefix: {prefix}")

    # 步骤 1: 收集原始文件
    uploaded_files = collect_raw_files(prefix)

    # 步骤 2: 转换为 Parquet
    parquet_files = convert_to_parquet(prefix, uploaded_files)

    # 步骤 3: 数据清洗
    cleaned_files = clean_data(prefix, parquet_files)

    # 步骤 3.5: 数值校准（子 Flow）
    calibrated_state = validate_and_calibrate_data(prefix, cleaned_files)
    # 如果是 Prefect 子 flow 返回的 State，取结果
    if hasattr(calibrated_state, "result"):
        calibrated_result = calibrated_state.result()
    else:
        calibrated_result = calibrated_state
    
    # 如果子 Flow 返回暂停信息，不要再暂停主 Flow，直接返回等待状态
    if isinstance(calibrated_result, dict) and calibrated_result.get("_paused"):
        issues_summary = calibrated_result.get("_issues_summary", {})
        logger.info(
            f"Validation pending, waiting user action. "
            f"Report: {calibrated_result.get('_report_s3_key', calibrated_result.get('_report_path', 'unknown'))}. "
            f"Issues: truncated={issues_summary.get('truncated_codes', 0)}, "
            f"one_to_many_code={issues_summary.get('one_to_many_code', 0)}, "
            f"one_to_many_name={issues_summary.get('one_to_many_name', 0)}, "
            f"missing_codes={issues_summary.get('missing_codes', 0)}"
        )
        # 主 Flow 正常返回，状态会是 Completed；前端根据 report 存在与否决定是否进入验证界面
        return {
            "status": "validation_pending",
            "reason": "validation_issues",
            "report_path": calibrated_result.get("_report_path"),
            "report_s3_key": calibrated_result.get("_report_s3_key"),
            "issues_summary": issues_summary,
            "validation_flow_run_id": calibrated_result.get("_flow_run_id"),
        }
    
    # 如果返回的是列表，说明校验已通过
    if isinstance(calibrated_result, list):
        calibrated_files = calibrated_result
    else:
        logger.error(f"Unexpected return type from validate_and_calibrate_data: {type(calibrated_result)}, value: {calibrated_result}")
        raise ValueError(f"validate_and_calibrate_data returned unexpected type: {type(calibrated_result)}")

    # 步骤 4: 加密敏感字段
    secure_files = encrypt_sensitive_fields(prefix, calibrated_files)

    # 步骤 5: 数据汇总
    aggregated_result = aggregate_data(prefix, secure_files)

    # 步骤 6: 统一上传所有处理后的文件
    uploaded_count = upload_processed_files(prefix, secure_files)

    logger.info("Dataset ETL pipeline completed successfully")
    
    return {
        "uploaded_count": len(uploaded_files),
        "parquet_count": len(parquet_files),
        "cleaned_count": len(cleaned_files),
        "secure_count": len(secure_files),
        "uploaded_processed_count": uploaded_count,
        "aggregated": aggregated_result,
    }


@flow(name="rustfs-list-files")
def list_files_flow(prefix: str = ""):
    """
    保持与历史兼容的 Flow 入口函数。
    - Prefect Deployment 仍然使用 entrypoint `...:list_files_flow`
    - 内部调用新的 dataset_etl_flow 实现完整 ETL 处理
    """
    return dataset_etl_flow(prefix=prefix)


if __name__ == "__main__":
    # 本地测试
    result = dataset_etl_flow(prefix="team-1/dataset-2/sourcedata/")
    print(f"Flow result: {result}")


