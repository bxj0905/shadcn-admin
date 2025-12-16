from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, List
import os
import tempfile
import json

import boto3
from botocore.exceptions import BotoCoreError, ClientError
import pandas as pd
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

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
        endpoint_url = os.environ.get("RUSTFS_ENDPOINT", "http://host.docker.internal:9000")
        access_key = os.environ.get(
            "RUSTFS_ACCESS_KEY", os.environ.get("MINIO_ROOT_USER", "admin")
        )
        secret_key = os.environ.get(
            "RUSTFS_SECRET_KEY", os.environ.get("MINIO_ROOT_PASSWORD", "Minio123!")
        )
        bucket_name = os.environ.get("RUSTFS_BUCKET", "dataset")

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
        endpoint_url = os.environ.get("RUSTFS_ENDPOINT", "http://host.docker.internal:9000")
        access_key = os.environ.get(
            "RUSTFS_ACCESS_KEY", os.environ.get("MINIO_ROOT_USER", "admin")
        )
        secret_key = os.environ.get(
            "RUSTFS_SECRET_KEY", os.environ.get("MINIO_ROOT_PASSWORD", "Minio123!")
        )
        bucket_name = os.environ.get("RUSTFS_BUCKET", "dataset")

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
            print(f"[convert_to_parquet] processing {raw_key}, suffix={suffix}, parent={parent_name}")

            # 使用统一规则生成规范文件名（不带扩展名），parquet 后缀固定为 .parquet
            normalized_name_without_ext = build_new_name_from_parent(parent_name, "")
            parquet_filename = f"{normalized_name_without_ext}.parquet"
            parquet_key = f"{extracted_prefix}{parquet_filename}"

            local_raw_path = os.path.join(tmp_dir, Path(raw_key).name)
            local_parquet_path = os.path.join(tmp_dir, parquet_filename)
            try:
                print(f"[convert_to_parquet] downloading s3://{bucket_name}/{raw_key} -> {local_raw_path}")
                s3_client.download_file(bucket_name, raw_key, local_raw_path)

                # 按文件类型读取（仅做结构转换，不做行级清洗）
                if suffix in (".csv", ".txt"):
                    # CSV 读取增加编码兜底：优先 utf-8，失败后尝试 gbk、latin1
                    encodings_to_try = ["utf-8", "gbk", "latin1"]
                    last_exc = None
                    for enc in encodings_to_try:
                        try:
                            print(f"[convert_to_parquet] reading CSV {local_raw_path} with encoding {enc}")
                            df = pd.read_csv(local_raw_path, encoding=enc)
                            last_exc = None
                            break
                        except Exception as exc:  # 捕获编码相关错误并尝试下一种
                            last_exc = exc
                            print(
                                f"[convert_to_parquet] failed to read CSV {local_raw_path} with encoding {enc}: {exc}"
                            )
                    if last_exc is not None:
                        # 三种编码都失败，记录并跳过该文件
                        print(
                            f"[convert_to_parquet] give up reading CSV {local_raw_path} due to encoding errors, skip this file"
                        )
                        continue

                    # 与 Excel 路径保持一致：规范混合类型/对象列，避免写 Parquet 时出现
                    # "Expected bytes, got a 'float' object" 等错误。
                    df = df.convert_dtypes()
                    for col_name in df.columns:
                        if df[col_name].dtype == "object":
                            try:
                                orig_dtype = df[col_name].dtype
                                series = df[col_name].astype(str).str.strip().replace({"": None})
                                df[col_name] = series.astype("string")
                                print(
                                    f"[convert_to_parquet] normalize CSV object column {col_name} from {orig_dtype} to pandas string in {parquet_filename}"
                                )
                            except Exception as exc:
                                print(
                                    f"[convert_to_parquet] failed to normalize CSV object column {col_name} in {parquet_filename}: {exc}"
                                )

                elif suffix in (".xlsx", ".xls"):
                    print(f"[convert_to_parquet] reading Excel file {local_raw_path}")
                    df = pd.read_excel(local_raw_path)

                    # 通用规则：
                    # 1) 先让 pandas 推断合理 dtype（减少不必要的 object 列）
                    # 2) 对残留的 object 列统一做：去空白、空字符串转为 NaN，再 cast 到 pandas string
                    #    这样可以更稳健地处理“数字/字符串混合 + 空白”的列，避免写 Parquet 报类型错误。
                    df = df.convert_dtypes()
                    for col_name in df.columns:
                        if df[col_name].dtype == "object":
                            try:
                                orig_dtype = df[col_name].dtype
                                series = df[col_name].astype(str).str.strip().replace({"": None})
                                df[col_name] = series.astype("string")
                                print(
                                    f"[convert_to_parquet] normalize object column {col_name} from {orig_dtype} to pandas string in {parquet_filename}"
                                )
                            except Exception as exc:
                                print(
                                    f"[convert_to_parquet] failed to normalize object column {col_name} in {parquet_filename}: {exc}"
                                )
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

    @task
    def profile_parquet_with_duckdb(runtime: dict, parquet_files: list[dict]) -> list[dict]:
        """步骤 2.5：
        - 使用 DuckDB 对第 2 步输出的 Parquet 文件做数据 Profiling
        - Profiling 结果写入本地 DuckDB 数据库文件的内部表

        约定：
        - DuckDB 直接通过 S3 配置从 MinIO 读取源 Parquet
        - Profiling 结果以 Parquet 形式写回 MinIO：
          s3://<bucket>/<team>/<dataset>/profile/<normalized_name>_profile.parquet
        """

        if not parquet_files:
            print("[profile_parquet_with_duckdb] parquet_files is empty, nothing to do")
            return []

        # MinIO/S3 配置，供 DuckDB 直接从 MinIO 读取/写入 Parquet
        endpoint_url = os.environ.get("RUSTFS_ENDPOINT", "http://host.docker.internal:9000")
        access_key = os.environ.get(
            "RUSTFS_ACCESS_KEY", os.environ.get("MINIO_ROOT_USER", "admin")
        )
        secret_key = os.environ.get(
            "RUSTFS_SECRET_KEY", os.environ.get("MINIO_ROOT_PASSWORD", "Minio123!")
        )
        bucket_name = os.environ.get("RUSTFS_BUCKET", "dataset")
        team = runtime["team"]
        dataset = runtime["dataset"]

        profiled_files: list[dict] = []

        for item in parquet_files or []:
            parquet_key = item.get("parquet_key")
            if not parquet_key:
                continue

            normalized_name = item.get("normalized_name") or Path(parquet_key).name

            try:
                # 使用内存 DuckDB 实例直接从 MinIO 读取 Parquet 并计算 Profiling 指标
                con = duckdb.connect(database=":memory:")

                use_ssl = endpoint_url.startswith("https://")
                s3_endpoint = endpoint_url.replace("http://", "").replace("https://", "")

                con.execute(
                    "SET s3_endpoint=?",
                    [s3_endpoint],
                )
                con.execute(
                    "SET s3_access_key_id=?",
                    [access_key],
                )
                con.execute(
                    "SET s3_secret_access_key=?",
                    [secret_key],
                )
                con.execute(
                    "SET s3_use_ssl=?",
                    [use_ssl],
                )
                con.execute("SET s3_url_style='path'")

                s3_path = f"s3://{bucket_name}/{parquet_key}"
                print("[profile_parquet_with_duckdb] reading from", s3_path)

                con.execute(
                    "CREATE TABLE t AS SELECT * FROM read_parquet(?)",
                    [s3_path],
                )

                cols_df = con.execute("PRAGMA table_info('t')").fetchdf()

                profile_rows: list[Dict[str, object]] = []

                for _, col in cols_df.iterrows():
                    col_name = col["name"]
                    data_type = col["type"]

                    # 通用指标：行数、空值数、去重数
                    stats = con.execute(
                        f'''
                        SELECT
                            COUNT(*) AS row_count,
                            SUM(CASE WHEN "{col_name}" IS NULL THEN 1 ELSE 0 END) AS null_count,
                            COUNT(DISTINCT "{col_name}") AS distinct_count
                        FROM t
                        '''
                    ).fetchone()

                    row_count = stats[0]
                    null_count = stats[1]
                    distinct_count = stats[2]

                    # 数值型列的 min/max/avg/stddev
                    data_type_str = str(data_type).upper()
                    is_numeric = any(
                        kw in data_type_str for kw in ["INT", "DOUBLE", "DECIMAL", "NUMERIC", "FLOAT", "REAL"]
                    )

                    min_value = max_value = avg_value = stddev_value = None
                    if is_numeric and row_count and row_count > 0:
                        num_stats = con.execute(
                            f'''
                            SELECT
                                MIN("{col_name}") AS min_value,
                                MAX("{col_name}") AS max_value,
                                AVG("{col_name}") AS avg_value,
                                STDDEV("{col_name}") AS stddev_value
                            FROM t
                            '''
                        ).fetchone()
                        min_value, max_value, avg_value, stddev_value = num_stats

                    # Top N 频率分布
                    top_df = con.execute(
                        f'''
                        SELECT "{col_name}" AS value, COUNT(*) AS freq
                        FROM t
                        GROUP BY "{col_name}"
                        ORDER BY freq DESC
                        LIMIT 10
                        '''
                    ).fetchdf()
                    top_values = top_df.to_dict(orient="records")

                    profile_rows.append(
                        {
                            "dataset": dataset,
                            "file_name": normalized_name,
                            "column_name": col_name,
                            "data_type": data_type,
                            "row_count": row_count,
                            "null_count": null_count,
                            "distinct_count": distinct_count,
                            "min_value": min_value,
                            "max_value": max_value,
                            "avg_value": avg_value,
                            "stddev_value": stddev_value,
                            "top_values": json.dumps(top_values, ensure_ascii=False),
                            "profiled_at": datetime.utcnow(),
                        }
                    )

                profile_df = pd.DataFrame(profile_rows)

                # 通过 DuckDB 直接将 profiling 结果写回 MinIO（Parquet）
                profile_key = f"{team}/{dataset}/profile/{normalized_name}_profile.parquet"
                profile_s3_path = f"s3://{bucket_name}/{profile_key}"

                con.register("tmp_profile", profile_df)
                print("[profile_parquet_with_duckdb] writing profile to", profile_s3_path)
                con.execute(
                    "COPY tmp_profile TO ? (FORMAT 'parquet')",
                    [profile_s3_path],
                )
                con.unregister("tmp_profile")

                profiled_files.append(
                    {
                        "bucket": bucket_name,
                        "profile_key": profile_key,
                        "source_parquet": parquet_key,
                        "normalized_name": normalized_name,
                    }
                )

            except Exception as exc:
                print(
                    f"[profile_parquet_with_duckdb] failed for {bucket_name}/{parquet_key}: {exc}"
                )
            finally:
                try:
                    con.close()
                except Exception:
                    pass

        return profiled_files

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
            # 统一列类型等：
            # - 涉及“金额”的字段统一转为 DECIMAL(18,2)
            # - 涉及“人数/计数”的字段统一转为整型（INT64）

            if not files:
                print("[clean_schema] no files to clean schema, skip")
                return files

            # MinIO/S3 配置，与其他任务保持一致
            endpoint_url = os.environ.get("RUSTFS_ENDPOINT", "http://host.docker.internal:9000")
            access_key = os.environ.get(
                "RUSTFS_ACCESS_KEY", os.environ.get("MINIO_ROOT_USER", "admin")
            )
            secret_key = os.environ.get(
                "RUSTFS_SECRET_KEY", os.environ.get("MINIO_ROOT_PASSWORD", "Minio123!")
            )
            bucket_name = os.environ.get("RUSTFS_BUCKET", "dataset")

            s3_client = boto3.client(
                "s3",
                endpoint_url=endpoint_url,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
            )

            tmp_dir = tempfile.mkdtemp(prefix="wjp_clean_schema_")
            print("[clean_schema] tmp_dir=", tmp_dir)

            # 列名匹配规则：
            # - 优先使用 report_tables.sql / report_populate.sql 中出现的核心字段名
            # - 英文关键词作为兜底匹配

            # 金额/指标类字段（DECIMAL(18,2)）
            amount_keywords = [
                # 英文兜底
                "amount",
                "amt",
                "fee",
                "price",
                "total",
                "money",
                # 报表字段（RPT01/RPT02/RPT03/RPT04 等）
                "营业成本",
                "本年折旧",
                "资产总计",
                "负债合计",
                "营业收入",
                "税金及附加",
                "应交增值税",
                "应付职工薪酬",
                "营业利润",
                "资产减值损失",
                "信用减值损失",
                "投资收益",
                "公允价值变动收益",
                "资产处置收益",
                "其他收益",
                "净敞口套期收益",
                "税收合计",
                "研发费用",
                "增加值",
                "工业总产值",
                "经营性单位收入",
                "非经营性单位支出",
                "研究开发费用合计",
                # 费用/金额类通用关键词
                "金额",
                "单价",
                "总价",
                "费用",
                "成本",
                "收入",
                "利润",
            ]

            # 人数/计数字段（INT64）
            people_keywords = [
                # 英文兜底
                "count",
                "cnt",
                "num",
                "qty",
                # 报表字段
                "期末从业人数",
                "其中女性从业人数",
                "从业人员期末人数",
                "从业人员期末人数其中女性",
                "研发人员数",
                "研究开发人员合计",
                "专利申请数",
                "当年专利申请数",
                "期末有效发明专利数",
                "专利授权数",
                # 通用中文关键词
                "人数",
                "人次",
            ]

            for item in files or []:
                parquet_key = item.get("parquet_key") or item.get("target_key")
                if not parquet_key:
                    continue

                local_parquet_path = os.path.join(
                    tmp_dir,
                    Path(parquet_key).name,
                )

                try:
                    print(
                        f"[clean_schema] downloading s3://{bucket_name}/{parquet_key} -> {local_parquet_path}"
                    )
                    s3_client.download_file(bucket_name, parquet_key, local_parquet_path)

                    table = pq.read_table(local_parquet_path)
                    schema = table.schema

                    new_columns = []
                    decimal_type = pa.decimal128(18, 2)
                    int_type = pa.int64()

                    for i, field in enumerate(schema):
                        col_name = field.name
                        col = table.column(i)
                        lower_name = col_name.lower()

                        target_type = None
                        if any(k in lower_name for k in amount_keywords):
                            target_type = decimal_type
                        elif any(k in lower_name for k in people_keywords):
                            target_type = int_type

                        if target_type is not None:
                            try:
                                casted = col.cast(target_type, safe=False)
                                print(
                                    f"[clean_schema] cast column {col_name} to {target_type} in {parquet_key}"
                                )
                                new_columns.append(casted)
                            except Exception as exc:
                                print(
                                    f"[clean_schema] failed to cast column {col_name} in {parquet_key}: {exc}"
                                )
                                new_columns.append(col)
                        else:
                            new_columns.append(col)

                    new_table = pa.Table.from_arrays(new_columns, names=schema.names)

                    pq.write_table(new_table, local_parquet_path)
                    print(
                        f"[clean_schema] uploading schema-normalized parquet {local_parquet_path} -> s3://{bucket_name}/{parquet_key}"
                    )
                    s3_client.upload_file(local_parquet_path, bucket_name, parquet_key)

                except Exception as exc:
                    print(
                        f"[clean_schema] failed for {bucket_name}/{parquet_key}: {exc}"
                    )
                finally:
                    try:
                        if os.path.exists(local_parquet_path):
                            os.remove(local_parquet_path)
                    except OSError:
                        pass

            return files

        @task
        def clean_values(runtime: dict, files: list[dict]) -> list[dict]:
            # 处理缺失值等：这里先实现一个基础规则
            # - 对每个 Parquet 文件，删除“整列都是缺失值”的列

            if not files:
                print("[clean_values] no files to clean, skip")
                return files

            # MinIO/S3 配置，与其他任务保持一致
            endpoint_url = os.environ.get("RUSTFS_ENDPOINT", "http://host.docker.internal:9000")
            access_key = os.environ.get(
                "RUSTFS_ACCESS_KEY", os.environ.get("RUSTFS_ROOT_USER", "admin")
            )
            secret_key = os.environ.get(
                "RUSTFS_SECRET_KEY", os.environ.get("RUSTFS_ROOT_PASSWORD", "Minio123!")
            )
            bucket_name = os.environ.get("RUSTFS_BUCKET", "dataset")

            s3_client = boto3.client(
                "s3",
                endpoint_url=endpoint_url,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
            )

            tmp_dir = tempfile.mkdtemp(prefix="wjp_clean_values_")
            print("[clean_values] tmp_dir=", tmp_dir)

            for item in files or []:
                parquet_key = item.get("parquet_key") or item.get("target_key")

                if not parquet_key:
                    continue

                local_parquet_path = os.path.join(
                    tmp_dir,
                    Path(parquet_key).name,
                )

                try:
                    print(
                        f"[clean_values] downloading s3://{bucket_name}/{parquet_key} -> {local_parquet_path}"
                    )
                    s3_client.download_file(bucket_name, parquet_key, local_parquet_path)

                    # 读取 Parquet
                    df = pd.read_parquet(local_parquet_path)

                    # 特例：单位基本情况_611 -> 只保留 核查情况 == 1 的记录，且要避免误删整表
                    # 这里假设 核查情况 列为数字或空
                    stem = Path(parquet_key).stem
                    if stem == "单位基本情况_611" and "核查情况" in df.columns:
                        before_rows = len(df)
                        # 尽量转成数值，无法解析的记为 NaN
                        norm = pd.to_numeric(df["核查情况"], errors="coerce")
                        mask = norm == 1
                        if mask.any():
                            df = df[mask]
                            after_rows = len(df)
                            print(
                                f"[clean_values] filtered 单位基本情况_611 by 核查情况=1: {before_rows} -> {after_rows} rows"
                            )
                        else:
                            # 如果没有任何一行等于 1，则不做过滤，避免把基准意外清空
                            print(
                                f"[clean_values] 核查情况 列存在但没有等于 1 的记录，不执行过滤，保持 {before_rows} 行不变"
                            )

                    before_cols = list(df.columns)
                    df = df.dropna(axis=1, how="all")
                    after_cols = list(df.columns)

                    removed_cols = [c for c in before_cols if c not in after_cols]
                    if removed_cols:
                        print(
                            f"[clean_values] removed all-null columns from {parquet_key}: {removed_cols}"
                        )
                    else:
                        print(f"[clean_values] no all-null columns in {parquet_key}")

                    # 覆盖写回同一路径
                    df.to_parquet(local_parquet_path, index=False)
                    print(
                        f"[clean_values] uploading cleaned parquet {local_parquet_path} -> s3://{bucket_name}/{parquet_key}"
                    )
                    s3_client.upload_file(local_parquet_path, bucket_name, parquet_key)

                except Exception as exc:
                    print(
                        f"[clean_values] failed for {bucket_name}/{parquet_key}: {exc}"
                    )
                finally:
                    try:
                        if os.path.exists(local_parquet_path):
                            os.remove(local_parquet_path)
                    except OSError:
                        pass

            return files

        @task
        def deduplicate(runtime: dict, files: list[dict]) -> list[dict]:
            # TODO: 去重逻辑可以后续补充，目前先原样透传
            return files

        @task
        def validate(runtime: dict, files: list[dict]) -> list[dict]:
            # 基于 611 / 601 并列基准做主数据校验：
            # - 若基准自身 code 被截断/损坏，则直接认为本次导入无效，任务失败，提示用户修复原始文件
            # - 其他表中若 code 损坏，则优先通过名称在基准中唯一匹配自动修复，否则输出到校验结果，由前端人工选择

            if not files:
                print("[validate] no files to validate, skip")
                return files

            # MinIO/S3 配置，与其他任务保持一致
            endpoint_url = os.environ.get("RUSTFS_ENDPOINT", "http://host.docker.internal:9000")
            access_key = os.environ.get(
                "RUSTFS_ACCESS_KEY", os.environ.get("RUSTFS_ROOT_USER", "admin")
            )
            secret_key = os.environ.get(
                "RUSTFS_SECRET_KEY", os.environ.get("RUSTFS_ROOT_PASSWORD", "Minio123!")
            )
            bucket_name = os.environ.get("RUSTFS_BUCKET", "dataset")

            s3_client = boto3.client(
                "s3",
                endpoint_url=endpoint_url,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
            )

            # team / dataset 优先来自 runtime；若未提供，则先用默认值，后面再尝试从 parquet_key 前缀推断
            team = runtime.get("team") or "default_team"
            dataset = runtime.get("dataset") or "wjp_5th"

            tmp_dir = tempfile.mkdtemp(prefix="wjp_validate_")
            print("[validate] tmp_dir=", tmp_dir)

            code_col = "统一社会信用代码"
            name_col = "单位详细名称"

            # 1. 找到 611 和 601 两张基准表
            base_611_key = None
            base_601_key = None

            for item in files or []:
                parquet_key = item.get("parquet_key") or item.get("target_key")
                if not parquet_key:
                    continue

                # 若 runtime 中未提供 team/dataset（仍为默认值），则尝试从 parquet_key 前缀自动解析一次
                # 例如: "8/10/raw_extracted/xxx.parquet" -> team="8", dataset="10"
                if team == "default_team" and dataset == "wjp_5th":
                    parts = parquet_key.split("/")
                    if len(parts) >= 3:
                        cand_team, cand_dataset = parts[0], parts[1]
                        if cand_team and cand_dataset:
                            team = cand_team
                            dataset = cand_dataset

                stem = Path(parquet_key).stem
                if stem == "单位基本情况_611":
                    base_611_key = parquet_key
                elif stem == "调查单位基本情况_601":
                    base_601_key = parquet_key

            if not base_611_key and not base_601_key:
                print("[validate] no baseline tables (611/601) found, skip master-data validation")
                return files

            def _load_baseline(parquet_key: str | None, label: str) -> pd.DataFrame:
                if not parquet_key:
                    return pd.DataFrame(columns=[code_col, name_col])

                local_path = os.path.join(tmp_dir, Path(parquet_key).name)
                try:
                    print(
                        f"[validate] downloading baseline {label} s3://{bucket_name}/{parquet_key} -> {local_path}"
                    )
                    s3_client.download_file(bucket_name, parquet_key, local_path)
                    df = pd.read_parquet(local_path)
                except Exception as exc:
                    print(f"[validate] failed to load baseline {label} {parquet_key}: {exc}")
                    return pd.DataFrame(columns=[code_col, name_col])
                finally:
                    try:
                        if os.path.exists(local_path):
                            os.remove(local_path)
                    except OSError:
                        pass

                missing_cols = [c for c in (code_col, name_col) if c not in df.columns]
                if missing_cols:
                    print(
                        f"[validate] baseline {label} {parquet_key} missing columns {missing_cols}, ignore as baseline"
                    )
                    return pd.DataFrame(columns=[code_col, name_col])

                df = df[[code_col, name_col]].copy()
                df[code_col] = df[code_col].astype(str).str.strip()
                df[name_col] = df[name_col].astype(str).str.strip()

                # 将常见占位字符串视为缺失，避免像 "None" 这样的值进入基准集合
                placeholder_values = {"", "None", "none", "NA", "NaN"}
                df[code_col] = df[code_col].where(~df[code_col].isin(placeholder_values), pd.NA)
                df[name_col] = df[name_col].where(~df[name_col].isin(placeholder_values), pd.NA)

                df = df.dropna(subset=[code_col, name_col])

                return df.drop_duplicates()

            base_611_df = _load_baseline(base_611_key, "611")
            base_601_df = _load_baseline(base_601_key, "601")

            # 2. 检查基准表本身是否存在疑似被截断/损坏的统一社会信用代码
            def _is_corrupted_code_series(series: pd.Series) -> pd.Series:
                s = series.astype(str).str.strip()
                # 这里只识别明显被 Excel 处理成科学计数法/小数形式的情况：
                #  - 形如 9.15106E+17 / 9.15E-3（包含 "E+数字" 或 "E-数字"）
                #  - 或者包含小数点（9.15106）
                # 普通业务编码中出现的字母 "E"（例如 91510600791840793E、9151060079E1840793）不视为损坏
                sci_mask = s.str.contains(r"[Ee][+-]\d+", na=False)
                dot_mask = s.str.contains(r"\.", na=False)
                return sci_mask | dot_mask

            baseline_corrupted = False
            for label, df_base in (("611", base_611_df), ("601", base_601_df)):
                if df_base.empty:
                    continue
                bad_mask = _is_corrupted_code_series(df_base[code_col])
                if bad_mask.any():
                    baseline_corrupted = True
                    bad_rows = df_base.loc[bad_mask, [code_col, name_col]]
                    print(
                        f"[validate] baseline {label} has corrupted codes, count={len(bad_rows)}; failing validation so user can fix source files"
                    )

            if baseline_corrupted:
                # 基准一旦损坏，本次导入数据整体视为不可信，直接失败，让前端提示用户修改 611/601 源文件
                raise ValueError(
                    "[validate] baseline 611/601 has corrupted 统一社会信用代码, please fix source Excel/CSV and re-import."
                )

            # 3. 合并两张表作为并列基准
            baseline_df = pd.concat([base_611_df, base_601_df], ignore_index=True).drop_duplicates()

            if baseline_df.empty:
                print("[validate] combined baseline is empty, skip master-data validation")
                return files

            # 预计算方便查询的集合/映射
            baseline_df["_in_baseline"] = True
            baseline_codes = set(baseline_df[code_col].dropna().unique())
            baseline_names = set(baseline_df[name_col].dropna().unique())

            # code -> set(names)
            code_to_names: Dict[str, set] = {}
            # name -> set(codes)
            name_to_codes: Dict[str, set] = {}
            for row in baseline_df.itertuples(index=False):
                c = getattr(row, code_col)
                n = getattr(row, name_col)
                if pd.isna(c) or pd.isna(n):
                    continue
                code_to_names.setdefault(c, set()).add(n)
                name_to_codes.setdefault(n, set()).add(c)

            validation_rows: List[Dict[str, object]] = []

            # 暂存所有“基准中不存在的新单位”，后面再分析 code/name 一对多
            new_units_records: List[Dict[str, object]] = []

            for item in files or []:
                parquet_key = item.get("parquet_key") or item.get("target_key")
                if not parquet_key:
                    continue

                stem = Path(parquet_key).stem
                # 基准表自身只做互相对比，不在这一轮“其他表强制同步”中修改
                is_baseline_table = stem in ("单位基本情况_611", "调查单位基本情况_601")

                local_path = os.path.join(tmp_dir, Path(parquet_key).name)

                try:
                    print(
                        f"[validate] downloading s3://{bucket_name}/{parquet_key} -> {local_path}"
                    )
                    s3_client.download_file(bucket_name, parquet_key, local_path)

                    df = pd.read_parquet(local_path)

                    # 有的业务表可能只带 code 或只带 name，这里也尝试做基于权威表的补齐
                    has_code_orig = code_col in df.columns
                    has_name_orig = name_col in df.columns

                    if not has_code_orig and not has_name_orig:
                        print(
                            f"[validate] {parquet_key} has neither {code_col} nor {name_col}, skip master-data validation for this file"
                        )
                        continue

                    if not has_code_orig:
                        # 创建占位列，后续可以通过名称反推 code
                        df[code_col] = ""

                    if not has_name_orig:
                        # 创建占位列，后续可以通过 code 反推名称
                        df[name_col] = ""

                    df[code_col] = df[code_col].astype(str).str.strip()
                    df[name_col] = df[name_col].astype(str).str.strip()

                    # 额外：识别非基准表中的疑似损坏 code，并尝试用名称在基准中唯一匹配自动修复
                    if not is_baseline_table and has_code_orig:
                        code_series = df[code_col]
                        corrupted_mask = _is_corrupted_code_series(code_series)

                        if corrupted_mask.any():
                            corrupt_idxs = df.index[corrupted_mask]
                            for idx in corrupt_idxs:
                                code_src = df.at[idx, code_col]
                                name_val = df.at[idx, name_col]

                                # 在基准中按名称精确匹配，尝试唯一反推 code
                                candidates = (
                                    baseline_df[baseline_df[name_col] == name_val][code_col]
                                    .dropna()
                                    .unique()
                                )

                                if len(candidates) == 1:
                                    code_base = candidates[0]
                                    df.at[idx, code_col] = code_base
                                    validation_rows.append(
                                        {
                                            "dataset": dataset,
                                            "source_file": Path(parquet_key).name,
                                            "issue_type": "AUTO_SYNC_CODE_FROM_BASE_NAME",
                                            code_col: code_base,
                                            "统一社会信用代码_source": code_src,
                                            name_col: name_val,
                                        }
                                    )
                                else:
                                    # 无法唯一确定，交给前端人工判断
                                    validation_rows.append(
                                        {
                                            "dataset": dataset,
                                            "source_file": Path(parquet_key).name,
                                            "issue_type": "CODE_CORRUPTED_NEED_MANUAL",
                                            code_col: code_src,
                                            name_col: name_val,
                                        }
                                    )

                    # 第一轮：以 code 为基准的判断已经通过前面的 corrupted & AUTO_SYNC_CODE_FROM_BASE_NAME 处理过
                    # 接下来追加一轮：以 name 为基准，修复“code 不在基准但 name 在基准”的情况

                    name_in_baseline_mask = df[name_col].isin(baseline_names) if has_name_orig else pd.Series(
                        [False] * len(df), index=df.index
                    )
                    code_in_baseline_mask = df[code_col].isin(baseline_codes) if has_code_orig else pd.Series(
                        [False] * len(df), index=df.index
                    )

                    name_only_mask = (~code_in_baseline_mask) & name_in_baseline_mask

                    if name_only_mask.any() and not is_baseline_table:
                        for idx in df.index[name_only_mask]:
                            name_val = df.at[idx, name_col]
                            code_src = df.at[idx, code_col]
                            codes = list(name_to_codes.get(name_val, set()))

                            if len(codes) == 1:
                                # 以 name 反推唯一 code，自动修复
                                code_base = codes[0]
                                df.at[idx, code_col] = code_base
                                validation_rows.append(
                                    {
                                        "dataset": dataset,
                                        "source_file": Path(parquet_key).name,
                                        "issue_type": "AUTO_SYNC_CODE_FROM_BASE_NAME",
                                        code_col: code_base,
                                        "统一社会信用代码_source": code_src,
                                        name_col: name_val,
                                    }
                                )
                            elif len(codes) > 1:
                                # 同名多 code，需人工判断
                                validation_rows.append(
                                    {
                                        "dataset": dataset,
                                        "source_file": Path(parquet_key).name,
                                        "issue_type": "BASE_NAME_MULTI_CODES",
                                        code_col: code_src,
                                        name_col: name_val,
                                        "统一社会信用代码_base候选": ",".join(codes),
                                    }
                                )

                    # 经过 code / name 两轮自动修正后，重新计算基于基准的匹配关系
                    df_pairs = df[[code_col, name_col]].copy()

                    merged = df_pairs.merge(
                        baseline_df[[code_col, name_col, "_in_baseline"]],
                        on=[code_col, name_col],
                        how="left",
                    )
                    in_baseline_mask = merged["_in_baseline"].fillna(False)

                    # 这里重新计算一遍，使用当前 df 状态
                    code_in_baseline_mask = df[code_col].isin(baseline_codes)
                    name_in_baseline_mask = df[name_col].isin(baseline_names)

                    # 1) code 在基准中，但 (code,name) 组合不在基准中：
                    #    - 若该 code 在基准中只有单一名称，则强制将当前行名称同步为该名称
                    #    - 若该 code 在基准中对应多个名称，则视为冲突，输出到校验结果
                    conflict_mask = code_in_baseline_mask & (~in_baseline_mask)

                    if conflict_mask.any():
                        idxs = df.index[conflict_mask]
                        for idx in idxs:
                            c = df.at[idx, code_col]
                            n_src = df.at[idx, name_col]
                            base_names = list(code_to_names.get(c, set()))

                            if not base_names:
                                continue

                            if len(base_names) == 1:
                                # 强制同步为唯一的基准名称
                                n_base = base_names[0]
                                if not is_baseline_table:
                                    df.at[idx, name_col] = n_base
                                validation_rows.append(
                                    {
                                        "dataset": dataset,
                                        "source_file": Path(parquet_key).name,
                                        "issue_type": "AUTO_SYNC_NAME_TO_BASE",
                                        code_col: c,
                                        "单位详细名称_source": n_src,
                                        "单位详细名称_base": n_base,
                                    }
                                )
                            else:
                                # 基准中同一 code 对应多个名称，本身存在冲突，需要人工判断
                                validation_rows.append(
                                    {
                                        "dataset": dataset,
                                        "source_file": Path(parquet_key).name,
                                        "issue_type": "BASE_CODE_MULTI_NAMES",
                                        code_col: c,
                                        "单位详细名称_source": n_src,
                                        "单位详细名称_base候选": ",".join(base_names),
                                    }
                                )

                    # 2) code 和 name 都不在基准中的单位：视为真正“新单位”
                    #    - 先立即记一条 NEW_UNIT_NOT_IN_BASELINE，方便前端看到所有“新单位”列表
                    #    - 同时暂存下来，后面再做 code/name 一对多分析
                    not_in_baseline_mask = (~code_in_baseline_mask) & (~name_in_baseline_mask)

                    if not_in_baseline_mask.any():
                        sub = df.loc[not_in_baseline_mask, [code_col, name_col]].copy()
                        sub["source_file"] = Path(parquet_key).name

                        for row in sub.itertuples(index=False):
                            rec = row._asdict()
                            new_units_records.append(rec)

                            # 立即输出一条“未在 611/601 出现的新单位”记录
                            validation_rows.append(
                                {
                                    "dataset": dataset,
                                    "source_file": rec.get("source_file", Path(parquet_key).name),
                                    "issue_type": "NEW_UNIT_NOT_IN_BASELINE",
                                    code_col: rec.get(code_col, ""),
                                    name_col: rec.get(name_col, ""),
                                }
                            )

                    # 覆盖写回被“强制同步”修改过的非基准表
                    if not is_baseline_table:
                        df.to_parquet(local_path, index=False)
                        print(
                            f"[validate] uploading name-synced parquet {local_path} -> s3://{bucket_name}/{parquet_key}"
                        )
                        s3_client.upload_file(local_path, bucket_name, parquet_key)

                except Exception as exc:
                    print(f"[validate] failed for {bucket_name}/{parquet_key} in validate: {exc}")
                finally:
                    try:
                        if os.path.exists(local_path):
                            os.remove(local_path)
                    except OSError:
                        pass

            # 3) 对所有“基准中不存在的新单位”做 code/name 一对多分析，输出需要人工判断的记录
            if new_units_records:
                new_units_df = pd.DataFrame(new_units_records)

                # 统一列名，确保存在
                if code_col not in new_units_df.columns:
                    new_units_df[code_col] = ""
                if name_col not in new_units_df.columns:
                    new_units_df[name_col] = ""

                # 为了一对多分析，先做简单归一化：去空白，"" -> NaN
                new_units_df[code_col] = (
                    new_units_df[code_col].astype(str).str.strip().replace({"": None})
                )
                new_units_df[name_col] = (
                    new_units_df[name_col].astype(str).str.strip().replace({"": None})
                )

                # code 一对多：同一 code 对应多个不同名称（忽略 code 为空/NaN 的记录）
                valid_code_mask = new_units_df[code_col].notna()
                code_name_counts = (
                    new_units_df.loc[valid_code_mask, [code_col, name_col]]
                    .drop_duplicates()
                    .groupby(code_col)[name_col]
                    .nunique()
                )
                multi_name_codes = set(code_name_counts[code_name_counts > 1].index)

                # name 一对多：同一名称对应多个不同 code（忽略 name 为空/NaN 的记录）
                valid_name_mask = new_units_df[name_col].notna()
                name_code_counts = (
                    new_units_df.loc[valid_name_mask, [code_col, name_col]]
                    .drop_duplicates()
                    .groupby(name_col)[code_col]
                    .nunique()
                )
                multi_code_names = set(name_code_counts[name_code_counts > 1].index)

                if multi_name_codes or multi_code_names:
                    for row in new_units_df.itertuples(index=False):
                        c = getattr(row, code_col)
                        n = getattr(row, name_col)
                        src = getattr(row, "source_file", "")

                        issue_types: List[str] = []
                        if c in multi_name_codes:
                            issue_types.append("NEW_CODE_ONE_TO_MANY_NAMES")
                        if n in multi_code_names:
                            issue_types.append("NEW_NAME_ONE_TO_MANY_CODES")

                        if issue_types:
                            validation_rows.append(
                                {
                                    "dataset": dataset,
                                    "source_file": src,
                                    "issue_type": ";".join(issue_types),
                                    code_col: c,
                                    name_col: n,
                                }
                            )

            # 4) 将校验结果写回 MinIO，并按“新单位一对多冲突”判断是否需要人工确认
            if validation_rows:
                validation_df = pd.DataFrame(validation_rows)

                # Parquet 版本：全量结果，供下游批处理或审计使用
                validation_parquet_local_path = os.path.join(tmp_dir, "unit_master_conflicts.parquet")
                validation_df.to_parquet(validation_parquet_local_path, index=False)

                validation_parquet_key = f"{team}/{dataset}/validation/unit_master_conflicts.parquet"
                print(
                    f"[validate] uploading validation parquet {validation_parquet_local_path} -> s3://{bucket_name}/{validation_parquet_key}"
                )
                try:
                    s3_client.upload_file(validation_parquet_local_path, bucket_name, validation_parquet_key)
                except (BotoCoreError, ClientError) as exc:
                    print(f"[validate] failed to upload validation parquet result: {exc}")

                # CSV 版本：全量结果，供 Node/前端需要时查看明细
                validation_csv_local_path = os.path.join(tmp_dir, "unit_master_conflicts.csv")
                validation_df.to_csv(validation_csv_local_path, index=False)

                validation_csv_key = f"{team}/{dataset}/validation/unit_master_conflicts.csv"
                print(
                    f"[validate] uploading validation csv {validation_csv_local_path} -> s3://{bucket_name}/{validation_csv_key}"
                )
                try:
                    s3_client.upload_file(validation_csv_local_path, bucket_name, validation_csv_key)
                except (BotoCoreError, ClientError) as exc:
                    print(f"[validate] failed to upload validation csv result: {exc}")

                # 额外输出仅包含“新单位内部一对多冲突”的子集，供前端直接展示
                # 注意：issue_type 可能为 "A;B" 这种复合形式，因此不能只用 isin 精确匹配
                # 这里只保留 NEW_* 两类：表示不在权威表中的新单位之间的 1 对多关系。
                conflict_issue_types_subset = [
                    "NEW_CODE_ONE_TO_MANY_NAMES",
                    "NEW_NAME_ONE_TO_MANY_CODES",
                ]

                issue_series = validation_df["issue_type"].astype(str)
                conflict_only_mask = pd.Series(False, index=validation_df.index)
                for itype in conflict_issue_types_subset:
                    conflict_only_mask |= issue_series.str.contains(itype, na=False)

                conflict_only_df = validation_df.loc[conflict_only_mask].copy()

                conflict_parquet_local_path = os.path.join(
                    tmp_dir,
                    "unit_master_conflicts_one_to_many.parquet",
                )
                conflict_parquet_key = f"{team}/{dataset}/validation/unit_master_conflicts_one_to_many.parquet"
                conflict_csv_local_path = os.path.join(
                    tmp_dir,
                    "unit_master_conflicts_one_to_many.csv",
                )
                conflict_csv_key = f"{team}/{dataset}/validation/unit_master_conflicts_one_to_many.csv"

                try:
                    # 即使当前没有一对多冲突，也写出一个空表，方便后端稳定读取
                    conflict_only_df.to_parquet(conflict_parquet_local_path, index=False)
                    print(
                        f"[validate] uploading conflict-only parquet {conflict_parquet_local_path} -> s3://{bucket_name}/{conflict_parquet_key}"
                    )
                    s3_client.upload_file(conflict_parquet_local_path, bucket_name, conflict_parquet_key)

                    conflict_only_df.to_csv(conflict_csv_local_path, index=False)
                    print(
                        f"[validate] uploading conflict-only csv {conflict_csv_local_path} -> s3://{bucket_name}/{conflict_csv_key}"
                    )
                    s3_client.upload_file(conflict_csv_local_path, bucket_name, conflict_csv_key)
                except (BotoCoreError, ClientError) as exc:
                    print(f"[validate] failed to upload conflict-only validation result: {exc}")

                try:
                    for p in [
                        validation_parquet_local_path,
                        validation_csv_local_path,
                        conflict_parquet_local_path,
                        conflict_csv_local_path,
                    ]:
                        if p and os.path.exists(p):
                            os.remove(p)
                except OSError:
                    pass

                # 若存在“新单位内部一对多”冲突，则认为需要人工确认
                conflict_issue_types = {
                    "NEW_CODE_ONE_TO_MANY_NAMES",
                    "NEW_NAME_ONE_TO_MANY_CODES",
                }

                conflict_mask = validation_df["issue_type"].isin(conflict_issue_types)
                conflict_count = int(conflict_mask.sum())
                if conflict_count > 0:
                    raise ValueError(
                        f"[validate] found {conflict_count} master-data conflicts requiring manual confirmation (one-to-many code/name). "
                        f"Please resolve them in the frontend using {validation_parquet_key} and rerun the DAG."
                    )
            else:
                print("[validate] no validation issues found")

            return files

        step1 = clean_schema(runtime, parquet_files)
        step2 = clean_values(runtime, step1)
        step3 = deduplicate(runtime, step2)
        cleaned = validate(runtime, step3)

        # ... (rest of the code remains the same)
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
    profiled_files = profile_parquet_with_duckdb(runtime_cfg, parquet_files)
    cleaned_files = clean_data_group(runtime_cfg, parquet_files)
    secure_files = mask_sensitive_fields(runtime_cfg, cleaned_files)
    build_aggregated_tables(runtime_cfg, secure_files)