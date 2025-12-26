"""
任务：加密敏感字段
"""
import os
from pathlib import Path

import pandas as pd
from prefect import task, get_run_logger

try:
    from ..utils.config_utils import load_encryption_fields
    from ..utils.crypto_utils import aes_siv_encrypt_to_b64
except ImportError:  # pragma: no cover - fallback when running as top-level module
    from utils.config_utils import load_encryption_fields
    from utils.crypto_utils import aes_siv_encrypt_to_b64


@task
def encrypt_sensitive_fields(prefix: str, cleaned_files: list[dict]) -> list[dict]:
    """
    加密敏感字段
    
    - 对 cleaned 数据中的敏感字段做确定性加密（AES-SIV + Base64 URL-safe）
    - 本地就地修改 Parquet 文件内容
    - 仅更新元数据中的路径为 secure 区，实际上传由后续任务处理
    """
    logger = get_run_logger()

    if not cleaned_files:
        logger.warning("No cleaned files to encrypt, skipping")
        return []

    logger.info(f"Encrypting {len(cleaned_files)} files with AES-SIV")

    # 统一前缀格式，仅用于构造 secure 前缀（与历史实现保持一致）
    normalized_prefix = prefix.lstrip("/")
    if not normalized_prefix.endswith("/"):
        normalized_prefix = f"{normalized_prefix}/"

    if "/raw_extracted/" in normalized_prefix:
        secure_prefix = normalized_prefix.replace("/raw_extracted/", "/secure/", 1)
    else:
        secure_prefix = f"{normalized_prefix}secure/"

    encrypt_fields = load_encryption_fields()
    logger.info(f"Loaded {len(encrypt_fields)} encryption columns from config")

    secure_files: list[dict] = []

    for item in cleaned_files:
        parquet_key = item.get("parquet_key") or item.get("target_key")
        local_path = item.get("local_path")

        if not local_path or not os.path.exists(local_path):
            logger.warning(
                f"Local parquet file not found for encryption: {local_path!r}, key={parquet_key!r}; passing through."
            )
            secure_files.append(item)
            continue

        try:
            df = pd.read_parquet(local_path)
        except Exception as exc:  # noqa: BLE001
            logger.error(f"Failed to read parquet for encryption: {local_path}: {exc}; passing through")
            secure_files.append(item)
            continue

        # 选出需要加密的列：encrypt.yaml 中列出的且当前 DataFrame 中存在的列
        columns_to_encrypt = [c for c in df.columns if c in encrypt_fields]

        if not columns_to_encrypt:
            logger.info(f"No sensitive columns found in file {Path(local_path).name}, skipping encryption for this file")
        else:
            logger.info(
                f"Encrypting {len(columns_to_encrypt)} columns in {Path(local_path).name}: {columns_to_encrypt}"
            )

            for col in columns_to_encrypt:
                series = df[col]

                def _encrypt_cell(v: object) -> object:
                    # 保留空值/缺失值为原样，仅对有意义的值加密
                    if v is None:
                        return None
                    try:
                        import pandas as _pd  # 局部导入以避免顶层依赖

                        if _pd.isna(v):  # type: ignore[attr-defined]
                            return v
                    except Exception:  # noqa: BLE001
                        pass

                    text = str(v).strip()
                    if not text:
                        return v

                    try:
                        return aes_siv_encrypt_to_b64(text)
                    except Exception as exc:  # noqa: BLE001
                        logger.warning(f"Failed to encrypt value in column {col}: {exc}; keeping original")
                        return v

                df[col] = series.map(_encrypt_cell)

            # 写回本地文件（就地加密）
            try:
                df.to_parquet(local_path, index=False)
                logger.info(f"Encrypted parquet file (local): {local_path}")
            except Exception as exc:  # noqa: BLE001
                logger.error(f"Failed to write encrypted parquet file {local_path}: {exc}")

        # 构造 secure 区路径（仅更新元数据，不在此处上传）
        secure_key = None
        if parquet_key:
            if "/raw_extracted/" in parquet_key:
                secure_key = parquet_key.replace("/raw_extracted/", "/secure/", 1)
            else:
                # 兜底：在前缀后面附加 secure/，并保留文件名
                filename = Path(parquet_key).name
                secure_key = f"{secure_prefix}{filename}"

        secure_item = {
            **item,
            **({"secure_key": secure_key} if secure_key else {}),
        }

        # 为后续步骤兼容，将 parquet_key 也更新为 secure 路径（如果已生成）
        if secure_key:
            secure_item["parquet_key"] = secure_key

        secure_files.append(secure_item)

    logger.info(f"Encrypted {len(secure_files)} files (metadata updated for secure zone)")
    return secure_files

