"""功能 Flow：数据验证和校准（循环版本）。

从 611 / 601 权威表构建统一社会信用代码与单位名称的映射，
对其他表进行 code / name 校准，并在发现严重问题时生成报告、暂停等待用户修复，
用户在前端修复后再次运行本 Flow，应用修复结果并重新校验，直到没有关键问题为止。
"""

from __future__ import annotations

import json
import os
import tempfile
from collections import defaultdict
from typing import Optional, Union

import boto3
import pandas as pd
from prefect import flow, get_run_logger
from prefect_aws.s3 import S3Bucket


@flow(name="dataset-validation-flow")
def data_validation_flow(prefix: str, cleaned_files: Optional[list[dict]] = None) -> Union[list[dict], dict]:
    """步骤 3.5：数值校准（循环校验版本）。

    - 从 `单位基本情况_611.parquet` 和 `调查单位基本情况_601.parquet` 构建权威表
    - 通过 code 和 name 匹配校准其他表的数据
    - 检测并报告问题（code 截断、一对多、缺失 code 等）
    - 如果有问题，暂停等待用户修复；修复后重新校验，直到没有问题为止
    """

    logger = get_run_logger()

    # 兼容两种用法：
    # 1) 作为子 flow：主流程传入 cleaned_files 列表
    # 2) 作为独立 flow 部署并直接运行：只传 prefix，不传 cleaned_files
    cleaned_files = cleaned_files or []
    if not cleaned_files:
        logger.warning("No cleaned files to validate, skipping")
        return cleaned_files

    logger.info(f"Starting data validation and calibration for {len(cleaned_files)} files")

    # 循环校验：直到没有问题为止（或达到最大迭代次数）
    max_iterations = 10  # 防止无限循环
    iteration = 0

    while iteration < max_iterations:
        iteration += 1
        logger.info(f"=== Validation iteration {iteration} ===")

        # 记录已修复的行，避免重复检测（在应用修复结果后记录）
        fixed_rows: set[tuple[str, int]] = set()  # (normalized_file_name, row_index)

        # 1. 找到权威表文件（611 和 601）
        authority_files: dict[str, dict] = {}
        other_files: list[dict] = []

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

        # 2. 构建权威表
        authority_table: dict[str, str] = {}  # code -> name
        authority_table_reverse: dict[str, str] = {}  # name -> code
        code_lengths: defaultdict[str, list[int]] = defaultdict(list)
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
                logger.info(
                    f"Loaded authority file {file_type}: {len(df)} rows, columns: {list(df.columns)}"
                )

                # 查找统一社会信用代码和单位详细名称列
                code_col = None
                name_col = None

                # 1) 优先精确匹配标准列名
                if "统一社会信用代码" in df.columns:
                    code_col = "统一社会信用代码"
                if "单位详细名称" in df.columns:
                    name_col = "单位详细名称"

                # 2) 如果没有标准列名，再用模糊匹配
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

                logger.info(
                    f"Authority file {file_type}: Using code column '{code_col}', name column '{name_col}'"
                )

                import re

                # 提取 code 和 name，去除空值
                for idx, row in df.iterrows():
                    code = str(row[code_col]).strip() if pd.notna(row[code_col]) else ""
                    name = str(row[name_col]).strip() if pd.notna(row[name_col]) else ""

                    if not code or code.lower() in ["nan", "none", "null", ""]:
                        continue

                    # 检测和处理科学计数法格式（如 9.5E+17, 9.5M9234245 等）
                    code_original = code
                    code_clean = code.replace("-", "").replace(" ", "").upper()

                    is_scientific = False
                    scientific_pattern = None

                    # 标准科学计数法
                    sci_match = re.match(r"^(\d+)\.(\d+)E\+?(\d+)$", code_clean, re.IGNORECASE)
                    if sci_match:
                        is_scientific = True
                        scientific_pattern = "standard"
                        logger.warning(
                            f"Found scientific notation code in {file_type} row {idx}: {code_original}"
                        )
                    else:
                        # 截断科学计数法格式
                        if "." in code_clean and re.search(r"[EeMm]\+?", code_clean):
                            is_scientific = True
                            scientific_pattern = "truncated"
                            logger.warning(
                                f"Found truncated/scientific notation code in {file_type} row {idx}: {code_original}"
                            )

                    if is_scientific:
                        issues["truncated_codes"].append(
                            {
                                "file": file_type,
                                "code": code_original,
                                "length": "scientific_notation",
                                "name": name,
                                "row_index": idx,
                                "pattern": scientific_pattern,
                                "note": "统一社会信用代码被转换为科学计数法格式（如 9.35134E+17），无法自动恢复原始值。需要用户手动修复 611/601 文件中的该 code，将其恢复为正确的 18 位统一社会信用代码格式。",
                            }
                        )
                        continue

                    # 处理统一社会信用代码（18 位，数字+可选字母）
                    code_alphanumeric = re.sub(r"[^0-9A-Za-z]", "", code_clean)

                    if code_alphanumeric:
                        if len(code_alphanumeric) != 18:
                            issues["truncated_codes"].append(
                                {
                                    "file": file_type,
                                    "code": code_original,
                                    "length": len(code_alphanumeric),
                                    "name": name,
                                    "row_index": idx,
                                    "note": f"统一社会信用代码长度应为18位，实际为{len(code_alphanumeric)}位",
                                }
                            )
                        else:
                            code_lengths[code_alphanumeric].append(len(code_alphanumeric))
                    else:
                        if len(code_clean) > 0:
                            issues["truncated_codes"].append(
                                {
                                    "file": file_type,
                                    "code": code_original,
                                    "length": len(code_clean),
                                    "name": name,
                                    "row_index": idx,
                                    "note": "统一社会信用代码格式无效，不包含有效的数字或字母",
                                }
                            )
                        continue

                    if not name or name.lower() in ["nan", "none", "null", ""]:
                        continue

                    # 一对多关系检查
                    if code in authority_table:
                        if authority_table[code] != name:
                            issues["one_to_many_code"].append(
                                {
                                    "code": code,
                                    "existing_name": authority_table[code],
                                    "new_name": name,
                                    "file": file_type,
                                }
                            )
                    else:
                        authority_table[code] = name

                    if name in authority_table_reverse:
                        if authority_table_reverse[name] != code:
                            issues["one_to_many_name"].append(
                                {
                                    "name": name,
                                    "existing_code": authority_table_reverse[name],
                                    "new_code": code,
                                    "file": file_type,
                                }
                            )
                    else:
                        authority_table_reverse[name] = code

            except Exception as exc:  # noqa: BLE001
                logger.error(f"Failed to process authority file {file_type}: {exc}")
                continue

        logger.info(f"Authority table built: {len(authority_table)} code-name pairs")

        # 小工具：清理当前 prefix 下的校验中间文件
        def _cleanup_validation_files(prefix_for_cleanup: str) -> None:
            """尽量清理当前前缀下的校验中间文件。"""

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

                candidates: set[str] = set()
                base = prefix_for_cleanup.lstrip("/")
                if not base.endswith("/"):
                    base = f"{base}/"
                candidates.add(base)

                tmp = base.rstrip("/")
                if "/" in tmp:
                    parent = tmp.rsplit("/", 1)[0] + "/"
                    candidates.add(parent)

                if "sourcedata/" in base:
                    before, _after = base.split("sourcedata/", 1)
                    sourcedata_prefix = before + "sourcedata/"
                    candidates.add(sourcedata_prefix)

                keys_to_delete: list[str] = []
                for cand in candidates:
                    logger.info(
                        f"Attempting to cleanup validation temp files under prefix candidate: {cand}"
                    )
                    resp = s3_client_local.list_objects_v2(
                        Bucket=bucket_name,
                        Prefix=cand,
                    )
                    for obj in resp.get("Contents", []):
                        key = obj.get("Key", "")
                        if key.endswith("validation_report_pending.json") or key.endswith(
                            "validation_resolutions.json"
                        ):
                            if key not in keys_to_delete:
                                keys_to_delete.append(key)

                if not keys_to_delete:
                    logger.info("No validation temp files found to cleanup")
                    return

                delete_req = {"Objects": [{"Key": k} for k in keys_to_delete]}
                s3_client_local.delete_objects(Bucket=bucket_name, Delete=delete_req)
                logger.info(f"Deleted validation temp files: {keys_to_delete}")
            except Exception as cleanup_err:  # noqa: BLE001
                logger.warning(f"Failed to cleanup validation temp files: {cleanup_err}")

        # 3. 检查是否已有修复结果（用户已处理）
        resolutions = None
        had_resolutions = False
        resolutions_s3_key = None
        should_delete_resolutions = False

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
            resolutions_s3_key = f"{normalized_prefix}validation_resolutions.json"

            try:
                response = s3_client.get_object(Bucket=bucket_name, Key=resolutions_s3_key)
                resolutions_content = response["Body"].read().decode("utf-8")
                resolutions = json.loads(resolutions_content)
                had_resolutions = True
                logger.info("Found validation resolutions, applying fixes...")
            except s3_client.exceptions.NoSuchKey:
                logger.info("No validation resolutions found, proceeding with normal validation...")

            # 应用修复结果到权威表
            if resolutions and resolutions.get("resolutions"):
                res = resolutions["resolutions"]

                # 截断代码修复
                if res.get("truncated_codes"):
                    for fix in res["truncated_codes"]:
                        old_code = fix.get("code", "").strip()
                        new_code = fix.get("fixed_code", "").strip()
                        if old_code and new_code and old_code in authority_table:
                            name = authority_table[old_code]
                            authority_table[new_code] = name
                            authority_table_reverse[name] = new_code
                            if old_code in authority_table:
                                del authority_table[old_code]
                            logger.info(f"Applied fix: {old_code} -> {new_code}")

                # 一对多 code 修复
                if res.get("one_to_many_code"):
                    for fix in res["one_to_many_code"]:
                        code = fix.get("code", "").strip()
                        selected_name = fix.get("selected_name", "").strip()
                        if code and selected_name:
                            authority_table[code] = selected_name
                            authority_table_reverse[selected_name] = code
                            logger.info(f"Applied fix: code {code} -> name {selected_name}")

                # 一对多 name 修复
                if res.get("one_to_many_name"):
                    for fix in res["one_to_many_name"]:
                        name = fix.get("name", "").strip()
                        selected_code = fix.get("selected_code", "").strip()
                        if name and selected_code:
                            authority_table[selected_code] = name
                            authority_table_reverse[name] = selected_code
                            logger.info(f"Applied fix: name {name} -> code {selected_code}")

                # 缺失 code 的修复
                if res.get("missing_codes"):
                    for fix in res["missing_codes"]:
                        name = fix.get("name", "").strip()
                        code = fix.get("code", "").strip()
                        file_name = fix.get("file", "").strip()
                        row_index = fix.get("row_index")

                        if name and code:
                            authority_table[code] = name
                            authority_table_reverse[name] = code
                            logger.info(f"Applied fix: missing code {code} for name {name}")

                            # 回填到原始数据文件
                            target_file = None
                            normalized_file_name = file_name.replace(".parquet", "").strip()

                            for other in other_files:
                                item_normalized = (
                                    other.get("normalized_name", "").replace(".parquet", "").strip()
                                )
                                if item_normalized == normalized_file_name:
                                    target_file = other
                                    logger.debug(
                                        f"Found target file in other_files: {other.get('normalized_name')} matches {file_name}"
                                    )
                                    break

                            if not target_file:
                                for other in cleaned_files:
                                    item_normalized = (
                                        other.get("normalized_name", "").replace(".parquet", "").strip()
                                    )
                                    if item_normalized == normalized_file_name:
                                        target_file = other
                                        logger.debug(
                                            f"Found target file in cleaned_files: {other.get('normalized_name')} matches {file_name}"
                                        )
                                        break

                            if target_file:
                                local_path = target_file.get("local_path")
                                if local_path and os.path.exists(local_path):
                                    try:
                                        df_target = pd.read_parquet(local_path)

                                        code_col = None
                                        name_col = None
                                        for col in df_target.columns:
                                            if "统一社会信用代码" in col or "社会信用代码" in col:
                                                code_col = col
                                            if "单位详细名称" in col or "详细名称" in col:
                                                name_col = col

                                        if code_col and name_col and row_index is not None:
                                            df_row_index = row_index
                                            if 0 <= df_row_index < len(df_target):
                                                actual_name = (
                                                    str(df_target.iloc[df_row_index][name_col]).strip()
                                                    if pd.notna(df_target.iloc[df_row_index][name_col])
                                                    else ""
                                                )
                                                if actual_name == name:
                                                    old_code = (
                                                        str(df_target.iloc[df_row_index][code_col]).strip()
                                                        if pd.notna(df_target.iloc[df_row_index][code_col])
                                                        else ""
                                                    )
                                                    df_target.iloc[
                                                        df_row_index, df_target.columns.get_loc(code_col)
                                                    ] = code
                                                    df_target.to_parquet(local_path, index=False)
                                                    logger.info(
                                                        f"Applied fix to file {file_name} at row {row_index}: {name} -> {code} (old: {old_code})"
                                                    )
                                                else:
                                                    logger.warning(
                                                        f"Name mismatch at row {row_index} in {file_name}: expected '{name}', found '{actual_name}'"
                                                    )
                                    except Exception as file_fix_exc:  # noqa: BLE001
                                        logger.error(f"Failed to apply fix to file {file_name}: {file_fix_exc}")
                                else:
                                    logger.warning(
                                        f"File {file_name} not found locally: {target_file.get('local_path')}"
                                    )
                            else:
                                logger.warning(f"File {file_name} not found in cleaned_files list")

                logger.info("Validation resolutions applied successfully, continuing with calibration...")

                # 记录已修复的行，避免重复检测
                applied_count = 0
                if res.get("missing_codes"):
                    logger.info(
                        f"Recording {len(res.get('missing_codes', []))} fixed missing codes to skip during re-validation"
                    )
                    for fix in res["missing_codes"]:
                        file_name = fix.get("file", "").strip()
                        row_index = fix.get("row_index")
                        code = fix.get("code", "").strip()
                        if file_name and row_index is not None and code:
                            normalized_file_name = file_name.replace(".parquet", "")
                            fixed_rows.add((normalized_file_name, row_index))
                            applied_count += 1

                logger.info(
                    f"Total {applied_count} rows marked as fixed (out of {len(res.get('missing_codes', []))}), will skip them during validation"
                )

                should_delete_resolutions = True
                logger.info(
                    "Fixes applied to local files. Files will be uploaded to S3 in subsequent steps."
                )

            # 如果修复结果已成功应用，删除修复结果文件并清理中间文件
            if should_delete_resolutions and resolutions_s3_key:
                try:
                    s3_client.delete_object(Bucket=bucket_name, Key=resolutions_s3_key)
                    logger.info(
                        f"Deleted validation resolutions file: {resolutions_s3_key} (to prevent re-application)"
                    )
                except Exception as delete_exc:  # noqa: BLE001
                    logger.warning(
                        f"Failed to delete resolutions file {resolutions_s3_key}: {delete_exc}"
                    )

                _cleanup_validation_files(prefix)
        except Exception as s3_exc:  # noqa: BLE001
            logger.warning(f"Failed to check for resolutions: {s3_exc}, proceeding with normal validation...")

        # 4. 检查问题并生成统计
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

        logger.info(
            "Issues summary: "
            f"truncated={len(issues['truncated_codes'])}, "
            f"one_to_many_code={len(issues['one_to_many_code'])}, "
            f"one_to_many_name={len(issues['one_to_many_name'])}, "
            f"missing_codes={len(issues['missing_codes'])}"
        )

        if not has_critical_issues:
            _cleanup_validation_files(prefix)

        # 5. 校准其他文件
        calibrated_files: list[dict] = []

        for item in other_files:
            local_path = item.get("local_path")
            if not local_path or not os.path.exists(local_path):
                calibrated_files.append(item)
                continue

            try:
                df = pd.read_parquet(local_path)
                original_rows = len(df)

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
                        def match_name_by_code(code_val: object) -> Optional[str]:
                            if pd.isna(code_val):
                                return None
                            code_str = str(code_val).strip()
                            return authority_table.get(code_str)

                        df["_matched_name_by_code"] = df[code_col].apply(match_name_by_code)

                        if name_col in df.columns:
                            mask = df["_matched_name_by_code"].notna()
                            if mask.any():
                                df.loc[mask, name_col] = df.loc[mask, "_matched_name_by_code"]
                                logger.info(
                                    f"Matched {mask.sum()} names by code in {item.get('normalized_name', 'unknown')}"
                                )

                        df = df.drop(columns=["_matched_name_by_code"])

                    # 再通过 name 匹配 code
                    if name_col in df.columns:
                        def match_code_by_name(name_val: object) -> Optional[str]:
                            if pd.isna(name_val):
                                return None
                            name_str = str(name_val).strip()
                            return authority_table_reverse.get(name_str)

                        df["_matched_code_by_name"] = df[name_col].apply(match_code_by_name)

                        if code_col in df.columns:
                            mask = df["_matched_code_by_name"].notna()
                            if mask.any():
                                df.loc[mask, code_col] = df.loc[mask, "_matched_code_by_name"]
                                logger.info(
                                    f"Matched {mask.sum()} codes by name in {item.get('normalized_name', 'unknown')}"
                                )

                        df = df.drop(columns=["_matched_code_by_name"])

                        # 检查缺失 code（在通过 name 匹配之后）
                        if code_col in df.columns:
                            def is_code_missing(code_val: object) -> bool:
                                if pd.isna(code_val):
                                    return True
                                code_str = str(code_val).strip()
                                if code_str in [
                                    "",
                                    "nan",
                                    "none",
                                    "null",
                                    "None",
                                    "NULL",
                                    "NaN",
                                    "N/A",
                                    "n/a",
                                ]:
                                    return True
                                if not code_str or code_str.isspace():
                                    return True
                                return False

                            after_match_missing_mask = df[code_col].apply(is_code_missing)

                            if name_col in df.columns:
                                def is_name_valid(name_val: object) -> bool:
                                    if pd.isna(name_val):
                                        return False
                                    name_str = str(name_val).strip()
                                    return name_str not in [
                                        "",
                                        "nan",
                                        "none",
                                        "null",
                                        "None",
                                        "NULL",
                                        "NaN",
                                        "N/A",
                                        "n/a",
                                    ] and not name_str.isspace()

                                name_valid = df[name_col].apply(is_name_valid)
                                missing_with_name = after_match_missing_mask & name_valid

                                if missing_with_name.any():
                                    missing_indices = df[missing_with_name].index.tolist()
                                    missing_data = df.loc[missing_with_name, [name_col]].to_dict("records")
                                    file_name = item.get("normalized_name", "unknown")

                                    new_missing_codes = []
                                    normalized_file_name = file_name.replace(".parquet", "")
                                    for idx, row in enumerate(missing_data):
                                        row_index = int(missing_indices[idx]) if idx < len(missing_indices) else idx
                                        if (normalized_file_name, row_index) not in fixed_rows:
                                            new_missing_codes.append(
                                                {
                                                    "file": file_name,
                                                    "name": str(row.get(name_col, "")).strip()
                                                    if row.get(name_col)
                                                    else "",
                                                    "row_index": row_index,
                                                }
                                            )

                                    if new_missing_codes:
                                        issues["missing_codes"].extend(new_missing_codes)
                                        logger.info(
                                            f"Found {len(new_missing_codes)} new rows with missing codes but valid names in {file_name}"
                                        )

                # 保存校准后的数据
                df.to_parquet(local_path, index=False)
                logger.info(f"Calibrated {item.get('normalized_name', 'unknown')}: {original_rows} rows")
                calibrated_files.append(item)

            except Exception as exc:  # noqa: BLE001
                logger.error(f"Failed to calibrate {item.get('normalized_name', 'unknown')}: {exc}")
                calibrated_files.append(item)

        # 将权威表文件也加入结果
        for item in authority_files.values():
            calibrated_files.append(item)

        # 最终严重问题检查（尤其是 missing_codes）
        if issues["missing_codes"]:
            logger.error(
                f"Found {len(issues['missing_codes'])} rows with missing codes after calibration"
            )
            has_critical_issues = True

        logger.info(
            "Final issues summary: "
            f"truncated={len(issues['truncated_codes'])}, "
            f"one_to_many_code={len(issues['one_to_many_code'])}, "
            f"one_to_many_name={len(issues['one_to_many_name'])}, "
            f"missing_codes={len(issues['missing_codes'])}"
        )

        # 如果本次运行已经加载过用户修复结果（第二轮运行），不再生成新的交互报告
        if had_resolutions:
            logger.info(
                "User resolutions have been applied in this run; "
                "skip generating new validation_report_pending.json even if there are remaining issues."
            )
            has_critical_issues = False

            # 即使没有严重问题，也生成包含缺失 code 的报告（如果存在）
            if issues["missing_codes"]:
                report = {
                    "timestamp": pd.Timestamp.now().isoformat(),
                    "issues": {"missing_codes": issues["missing_codes"]},
                    "authority_table_size": len(authority_table),
                    "summary": {"missing_codes_count": len(issues["missing_codes"])},
                }

                report_path = os.path.join(
                    tempfile.mkdtemp(prefix="dataset_etl_validation_"),
                    "missing_codes_report.json",
                )
                with open(report_path, "w", encoding="utf-8") as f:
                    json.dump(report, f, ensure_ascii=False, indent=2)

                try:
                    s3_block = S3Bucket.load("rustfs-s3storage")
                    bucket_name = s3_block.bucket_name
                    access_key = getattr(s3_block, "aws_access_key_id", None) or os.environ.get(
                        "RUSTFS_ACCESS_KEY", os.environ.get("MINIO_ROOT_USER")
                    )
                    secret_key = getattr(s3_block, "aws_secret_access_key", None) or os.environ.get(
                        "RUSTFS_SECRET_KEY", os.environ.get("MINIO_ROOT_PASSWORD")
                    )
                    endpoint_url = getattr(s3_block, "endpoint_url", None) or os.environ.get(
                        "RUSTFS_ENDPOINT"
                    )

                    s3_client = boto3.client(
                        "s3",
                        endpoint_url=endpoint_url,
                        aws_access_key_id=access_key,
                        aws_secret_access_key=secret_key,
                    )

                    normalized_prefix = prefix.lstrip("/")
                    if not normalized_prefix.endswith("/"):
                        normalized_prefix = f"{normalized_prefix}/"
                    report_s3_key = (
                        f"{normalized_prefix}missing_codes_report_"
                        f"{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.json"
                    )

                    s3_client.upload_file(report_path, bucket_name, report_s3_key)
                    logger.info(
                        f"Missing codes report uploaded to s3://{bucket_name}/{report_s3_key}"
                    )
                except Exception:  # noqa: BLE001
                    pass

            if not has_critical_issues:
                logger.info(
                    "No critical issues after applying resolutions, cleaning up and returning calibrated files."
                )
                _cleanup_validation_files(prefix)
                return calibrated_files

        # 如果有严重问题且本轮没有 resolutions，则生成交互报告并暂停
        if has_critical_issues and not had_resolutions:
            try:
                from prefect.runtime import flow_run as runtime_flow_run

                current_flow_run_id = getattr(runtime_flow_run, "id", None)
            except Exception:  # noqa: BLE001
                current_flow_run_id = None

            if not current_flow_run_id:
                current_flow_run_id = os.environ.get("PREFECT_FLOW_RUN_ID", "unknown")

            report = {
                "timestamp": pd.Timestamp.now().isoformat(),
                "prefix": prefix,
                "flow_run_id": current_flow_run_id,
                "status": "pending_user_action",
                "issues": issues,
                "authority_table_size": len(authority_table),
                "summary": {
                    "truncated_codes_count": len(issues["truncated_codes"]),
                    "one_to_many_code_count": len(issues["one_to_many_code"]),
                    "one_to_many_name_count": len(issues["one_to_many_name"]),
                    "missing_codes_count": len(issues["missing_codes"]),
                },
            }

            report_path = os.path.join(
                tempfile.mkdtemp(prefix="dataset_etl_validation_"),
                "validation_report.json",
            )
            with open(report_path, "w", encoding="utf-8") as f:
                json.dump(report, f, ensure_ascii=False, indent=2)

            logger.error(
                f"Critical validation issues found. Report saved to: {report_path}, summary: {report['summary']}"
            )

            report_s3_key = None
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
                report_s3_key = f"{normalized_prefix}validation_report_pending.json"

                s3_client.upload_file(report_path, bucket_name, report_s3_key)
                logger.info(
                    f"Validation report uploaded to s3://{bucket_name}/{report_s3_key}, waiting for user action."
                )
            except Exception as upload_exc:  # noqa: BLE001
                logger.warning(f"Failed to upload validation report to S3: {upload_exc}")

            paused_successfully = False
            # 尝试通过 Prefect 3 API 暂停当前 Flow Run
            try:
                from prefect.flow_runs import pause_flow_run

                logger.info("Calling pause_flow_run() to pause flow run...")
                pause_flow_run()
                logger.info("Flow run successfully paused using pause_flow_run().")
                paused_successfully = True
            except Exception as pause_err:  # noqa: BLE001
                logger.warning(f"pause_flow_run() failed: {pause_err}")
                try:
                    from prefect.flow_runs import suspend_flow_run

                    logger.info("Calling suspend_flow_run() as fallback...")
                    suspend_flow_run()
                    logger.info("Flow run suspended using suspend_flow_run().")
                    paused_successfully = True
                except Exception as suspend_err:  # noqa: BLE001
                    logger.warning(f"suspend_flow_run() failed: {suspend_err}")

            if paused_successfully:
                continue

            # 返回一个带状态信息的字典，便于上层 Flow 或前端解析
            return {
                "_paused": True,
                "_report_path": report_path,
                "_report_s3_key": report_s3_key,
                "_issues_summary": {
                    "truncated_codes": len(issues["truncated_codes"]),
                    "one_to_many_code": len(issues["one_to_many_code"]),
                    "one_to_many_name": len(issues["one_to_many_name"]),
                    "missing_codes": len(issues["missing_codes"]),
                },
            }

        # 有严重问题但已经处理过 resolutions 的情况在循环开头已处理，这里继续下一轮

    # 兜底：如果达到最大迭代次数仍未退出，返回当前的 calibrated_files 或原始 cleaned_files
    logger.warning(
        f"Reached max validation iterations ({max_iterations}) without resolving all issues. Returning latest files."
    )
    return cleaned_files

