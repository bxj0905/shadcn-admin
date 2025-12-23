"""
任务：收集原始文件
"""
from typing import Dict, List
from prefect import task, get_run_logger
from prefect_aws.s3 import S3Bucket


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

