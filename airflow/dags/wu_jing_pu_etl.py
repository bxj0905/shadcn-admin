from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, List
import os
import tempfile

import boto3
from botocore.exceptions import BotoCoreError, ClientError
import pandas as pd

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.utils.trigger_rule import TriggerRule


DAG_ID = "wu_jing_pu_etl"


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


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule=None,           # 由前端手动触发
    catchup=False,
    default_args={
        "retries": 1,
    },
    params={
        # 默认容器内目录，前端/后端触发时用 dag_run.conf 覆盖
        "local_dir": "/opt/airflow/local_data",
        "team": "default_team",
        "dataset": "wjp_5th",
        # 可选：业务日期
        "stat_date": "2025-01-01",
        # 前端/后端在触发 DAG 时传入的 MinIO 原始文件前缀，例如 "8/3/raw/"
        "raw_prefix": "",
    },
    tags=["wjp", "etl"],
) as dag:

    @task
    def get_runtime_config(**context) -> dict:
        """
        从 dag_run.conf + params 里统一读取运行时配置。
        方便后面所有 task 共享 team/dataset/local_dir 等。
        """
        dag_run = context.get("dag_run")
        conf = (dag_run.conf or {}) if dag_run else {}

        params = context["params"] or {}

        runtime = {
            "local_dir": conf.get("local_dir", params.get("local_dir")),
            "team": conf.get("team", params.get("team")),
            "dataset": conf.get("dataset", params.get("dataset")),
            "stat_date": conf.get("stat_date", params.get("stat_date")),
            # 前端/后端在触发 DAG 时传入的 MinIO 原始文件前缀，例如 "8/3/raw/"
            "raw_prefix": conf.get("raw_prefix", params.get("raw_prefix")),
        }

        # TODO: 可以在这里做一些校验，比如必填字段检查
        return runtime

    @task
    def collect_and_upload_raw_files(runtime: dict) -> list[dict]:
        """步骤 1（已改造）：
        - 不再从本地目录读取并上传，而是基于 MinIO 中已有的原始文件进行处理
        - 根据运行时传入的 ``raw_prefix``（例如 ``"8/3/raw/"``）列出所有 csv/xlsx/xls 对象
        - 返回对象清单，供后续步骤使用
        """

        raw_prefix = runtime.get("raw_prefix") or ""
        if not raw_prefix:
            print("[collect_and_upload_raw_files] raw_prefix is empty, nothing to do")
            return []

        # 统一前缀格式：去掉开头的斜杠，并保证以 "/" 结尾
        normalized_prefix = raw_prefix.lstrip("/")
        if not normalized_prefix.endswith("/"):
            normalized_prefix = f"{normalized_prefix}/"

        print("[collect_and_upload_raw_files] runtime:", runtime)
        print("[collect_and_upload_raw_files] raw_prefix (normalized):", normalized_prefix)

        # MinIO/S3 客户端配置（与后端保持一致）
        endpoint_url = os.environ.get("MINIO_ENDPOINT", "http://host.docker.internal:9000")
        access_key = os.environ.get(
            "MINIO_ACCESS_KEY", os.environ.get("MINIO_ROOT_USER", "admin")
        )
        secret_key = os.environ.get(
            "MINIO_SECRET_KEY", os.environ.get("MINIO_ROOT_PASSWORD", "Minio123!")
        )
        bucket_name = os.environ.get("MINIO_BUCKET", "dataset")

        s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )

        uploaded_files: List[Dict[str, str]] = []
        matched_count = 0

        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket_name, Prefix=normalized_prefix):
            contents = page.get("Contents") or []
            for obj in contents:
                key = obj.get("Key") or ""
                lower_key = key.lower()
                if not (
                    lower_key.endswith(".csv")
                    or lower_key.endswith(".xlsx")
                    or lower_key.endswith(".xls")
                ):
                    continue

                matched_count += 1
                rel_path = key[len(normalized_prefix) :]

                uploaded_files.append(
                    {
                        "relative_path": rel_path,
                        "target_key": key,
                        "bucket": bucket_name,
                        # 保持字段名不变，表示该对象可用于后续步骤
                        "uploaded": True,
                    }
                )

        print(
            "[collect_and_upload_raw_files] finished. matched:",
            matched_count,
            "collected:",
            len(uploaded_files),
        )

        return uploaded_files

    @task
    def convert_to_parquet(runtime: dict, uploaded_files: list[dict]) -> list[dict]:
        """步骤 2：
        - 基于第 1 步在 MinIO raw 区的对象做 Parquet 转换
        - 将 csv/xlsx/xls 读取为 DataFrame 后写入 Parquet，并输出到 raw_extracted 区

        规则：
        - raw key:        <team>/<dataset>/raw/.../<父目录>/文件名
        - raw_extracted:  <team>/<dataset>/raw_extracted/<规范名>.parquet
        - 规范名 = 父目录名第一个 '_' 之后、最后一个 '_' 之前的部分
        """

        raw_prefix = runtime.get("raw_prefix") or ""

        if not raw_prefix:
            print("[convert_to_parquet] raw_prefix is empty, nothing to do")
            return []

        # 与步骤 1 保持一致的 MinIO 客户端配置
        endpoint_url = os.environ.get("MINIO_ENDPOINT", "http://host.docker.internal:9000")
        access_key = os.environ.get(
            "MINIO_ACCESS_KEY", os.environ.get("MINIO_ROOT_USER", "admin")
        )
        secret_key = os.environ.get(
            "MINIO_SECRET_KEY", os.environ.get("MINIO_ROOT_PASSWORD", "Minio123!")
        )
        bucket_name = os.environ.get("MINIO_BUCKET", "dataset")

        s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )

        parquet_files: list[dict] = []

        # 计算 raw_extracted 前缀：将 ".../raw/" 替换为 ".../raw_extracted/"
        normalized_raw_prefix = raw_prefix.lstrip("/")
        if not normalized_raw_prefix.endswith("/"):
            normalized_raw_prefix = f"{normalized_raw_prefix}/"
        if "/raw/" in normalized_raw_prefix:
            extracted_prefix = normalized_raw_prefix.replace("/raw/", "/raw_extracted/", 1)
        else:
            # 兜底：在末尾附加 raw_extracted/
            extracted_prefix = f"{normalized_raw_prefix}raw_extracted/"

        # 使用容器本地临时目录存放中间文件
        tmp_dir = tempfile.mkdtemp(prefix="wjp_raw_to_parquet_")
        print("[convert_to_parquet] tmp_dir=", tmp_dir)

        for item in uploaded_files or []:
            if not item.get("uploaded"):
                continue

            raw_key = item["target_key"]
            rel_path = item.get("relative_path") or raw_key[len(normalized_raw_prefix) :]

            # 取相对路径的父目录名作为“批次名”
            parent_name = Path(rel_path).parent.name or Path(rel_path).stem
            # 原始后缀，用于决定读取方式
            suffix = (Path(rel_path).suffix or Path(raw_key).suffix or "").lower()

            # 使用统一规则生成规范文件名（不带扩展名），parquet 后缀固定为 .parquet
            normalized_name_without_ext = build_new_name_from_parent(parent_name, "")
            parquet_filename = f"{normalized_name_without_ext}.parquet"
            parquet_key = f"{extracted_prefix}{parquet_filename}"

            local_raw_path = os.path.join(tmp_dir, Path(raw_key).name)
            local_parquet_path = os.path.join(tmp_dir, parquet_filename)

            try:
                print(f"[convert_to_parquet] downloading s3://{bucket_name}/{raw_key} -> {local_raw_path}")
                s3_client.download_file(bucket_name, raw_key, local_raw_path)

                # 按文件类型读取
                if suffix in (".csv", ".txt"):
                    df = pd.read_csv(local_raw_path)
                elif suffix in (".xlsx", ".xls"):
                    df = pd.read_excel(local_raw_path)
                else:
                    print(f"[convert_to_parquet] unsupported suffix {suffix} for {raw_key}, skip")
                    continue

                # 写出为 Parquet（需要容器里安装 pyarrow 或 fastparquet）
                df.to_parquet(local_parquet_path, index=False)

                print(
                    f"[convert_to_parquet] uploading parquet {local_parquet_path} -> s3://{bucket_name}/{parquet_key}"
                )
                s3_client.upload_file(local_parquet_path, bucket_name, parquet_key)

                parquet_files.append(
                    {
                        "raw_key": raw_key,
                        "parquet_key": parquet_key,
                        "bucket": bucket_name,
                        "normalized_name": parquet_filename,
                    }
                )
            except Exception as exc:  # 捕获所有异常，避免单个文件失败中断整个批次
                print(
                    f"[convert_to_parquet] failed for {bucket_name}/{raw_key} -> {bucket_name}/{parquet_key}: {exc}"
                )
            finally:
                # 尝试删除本地临时文件，忽略失败
                for path in (local_raw_path, local_parquet_path):
                    try:
                        if os.path.exists(path):
                            os.remove(path)
                    except OSError:
                        pass

        return parquet_files

    @task_group(group_id="clean_data")
    def clean_data_group(runtime: dict, parquet_files: list[dict]) -> list[dict]:
        """
        步骤 3：数据清洗 TaskGroup
        可以在里面拆多步：
        - 结构清洗
        - 值清洗
        - 去重
        - 质量校验
        """

        @task
        def clean_schema(runtime: dict, files: list[dict]) -> list[dict]:
            # TODO: 统一列名/类型等，输出到 clean/schema 阶段
            return files

        @task
        def clean_values(runtime: dict, files: list[dict]) -> list[dict]:
            # TODO: 处理空值、异常值、编码标准化等
            return files

        @task
        def deduplicate(runtime: dict, files: list[dict]) -> list[dict]:
            # TODO: 去重
            return files

        @task
        def validate(runtime: dict, files: list[dict]) -> list[dict]:
            # TODO: 做一些质量校验，必要时可以写入校验结果表/文件
            return files

        step1 = clean_schema(runtime, parquet_files)
        step2 = clean_values(runtime, step1)
        step3 = deduplicate(runtime, step2)
        cleaned = validate(runtime, step3)

        return cleaned

    @task
    def mask_sensitive_fields(runtime: dict, cleaned_files: list[dict]) -> list[dict]:
        """
        步骤 4：
        - 对 cleaned 数据中的敏感字段做脱敏/加密
        - 写入 secure 区
        """
        team = runtime["team"]
        dataset = runtime["dataset"]
        stat_date = runtime["stat_date"]

        # TODO:
        # 1. 遍历 cleaned_files
        # 2. 加载加密/脱敏配置，读取密钥（从 env / Connection / Secrets）
        # 3. 对指定字段进行处理
        # 4. 输出到 .../secure/<stat_date>/...
        secure_files: list[dict] = []

        return secure_files

    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def build_aggregated_tables(runtime: dict, secure_files: list[dict]) -> None:
        """
        步骤 5：
        - 选定 SQL 执行引擎（DuckDB / Postgres 等）
        - 读取 secure 区数据为表/视图
        - 执行一系列 SQL 生成汇总表
        - 汇总结果写到 MinIO 或数据库
        """
        stat_date = runtime["stat_date"]

        # TODO:
        # 1. 连接 DuckDB / Postgres
        # 2. 将 secure_files 注册成表（外部表或临时表）
        # 3. 执行聚合 SQL
        # 4. 输出结果表
        return

    # 依赖关系编排
    runtime_cfg = get_runtime_config()
    uploaded = collect_and_upload_raw_files(runtime_cfg)
    parquet_files = convert_to_parquet(runtime_cfg, uploaded)
    cleaned_files = clean_data_group(runtime_cfg, parquet_files)
    secure_files = mask_sensitive_fields(runtime_cfg, cleaned_files)
    build_aggregated_tables(runtime_cfg, secure_files)