"""
配置加载工具函数
"""
import os
import boto3
import yaml
from prefect_aws.s3 import S3Bucket


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


def _get_flow_base_path() -> str:
    """
    获取当前 flow 的基础路径（用于构建配置文件路径）
    优先级：
    1. 从环境变量 PREFECT_FLOW_DIR_PREFIX 获取（如果设置）
    2. 从 Prefect runtime 获取 flow 名称，构建路径
    3. 默认使用 'flow'
    
    返回: flow 的基础路径前缀（例如 'my-flow/' 或 'flow/'）
    """
    # 优先级 1: 从环境变量获取
    env_prefix = os.environ.get("PREFECT_FLOW_DIR_PREFIX")
    if env_prefix:
        return env_prefix.rstrip('/') + '/'
    
    # 优先级 2: 从 Prefect runtime 获取 flow 名称
    try:
        from prefect.runtime import flow_run
        if hasattr(flow_run, 'flow_name') and flow_run.flow_name:
            # 使用 flow 名称作为路径前缀
            flow_name = flow_run.flow_name
            return f"{flow_name}/"
    except Exception:
        pass
    
    # 优先级 3: 默认值
    return "flow/"


def load_sensitive_fields() -> list[str]:
    """
    从 sensitive_fields.yaml 配置文件加载敏感字段列表
    优先级：
    1. 从 S3 中 flow 代码同目录的 config/sensitive_fields.yaml 读取
       新目录结构路径格式：{flow_name}/config/sensitive_fields.yaml
    2. 使用内嵌的默认配置
    
    返回: 敏感字段名列表
    """
    config = None
    
    # 优先级 1: 从 S3 读取配置文件（与 flow 代码同目录）
    try:
        # 获取 flow 的基础路径（例如 my-flow/），新的目录结构中配置文件位于 {flow_name}/config 下
        flow_base_path = _get_flow_base_path()
        # 配置文件路径：与 flow 代码同目录结构的新布局
        config_key = f"{flow_base_path}config/sensitive_fields.yaml"
        
        s3_block = S3Bucket.load("rustfs-s3storage")
        bucket_name = s3_block.bucket_name
        
        # 使用 dataflow bucket（与 flow 代码存储在同一 bucket）
        dataflow_bucket = os.environ.get("RUSTFS_DATAFLOW_BUCKET", "dataflow")
        
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
        
        # 从 dataflow bucket 读取配置文件
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


def load_encryption_fields() -> list[str]:
    """从 encrypt.yaml 配置文件加载需要加密的字段列表。

    配置示例::

        sensitive_fields:
          - 统一社会信用代码
          - 单位详细名称
          - 法人单位详细名称
          - 法人单位详细地址

    优先级：
    1. 从 S3 中 flow 代码同目录的 config/encrypt.yaml 读取
       路径格式：{flow_name}/config/encrypt.yaml
    2. 使用内嵌的默认配置（同上示例）
    """
    config = None

    try:
        # 获取 flow 的基础路径（例如 my-flow/），新的目录结构中配置文件位于 {flow_name}/config 下
        flow_base_path = _get_flow_base_path()
        config_key = f"{flow_base_path}config/encrypt.yaml"

        s3_block = S3Bucket.load("rustfs-s3storage")
        bucket_name = s3_block.bucket_name

        # 使用 dataflow bucket（与 flow 代码存储在同一 bucket）
        dataflow_bucket = os.environ.get("RUSTFS_DATAFLOW_BUCKET", "dataflow")

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

        # 从 dataflow bucket 读取配置文件
        try:
            response = s3_client.get_object(Bucket=dataflow_bucket, Key=config_key)
            content = response["Body"].read().decode("utf-8")
            config = yaml.safe_load(content)
        except Exception:
            # 如果 dataflow bucket 中不存在，尝试从当前 bucket 读取
            try:
                response = s3_client.get_object(Bucket=bucket_name, Key=config_key)
                content = response["Body"].read().decode("utf-8")
                config = yaml.safe_load(content)
            except Exception:
                pass
    except Exception:
        # S3 读取失败，使用默认配置
        pass

    if config and isinstance(config, dict):
        fields = config.get("sensitive_fields")
        if isinstance(fields, list):
            # 只保留字符串条目
            return [str(name) for name in fields]

    # 默认配置（与文档示例保持一致）
    return [
        "统一社会信用代码",
        "单位详细名称",
        "法人单位详细名称",
        "法人单位详细地址",
    ]


def load_sql_column_types() -> dict[str, str]:
    """
    从 column_types.yaml 配置文件加载字段名到数据类型的映射
    优先级：
    1. 从 S3 中 flow 代码同目录的 config/column_types.yaml 读取
       新目录结构路径格式：{flow_name}/config/column_types.yaml
    2. 使用内嵌的默认配置
    
    返回: {字段名: 类型} 的字典，类型为 'DECIMAL', 'INTEGER', 'TEXT', 'BOOLEAN'
    """
    column_types = {}
    config = None
    
    # 优先级 1: 从 S3 读取配置文件（与 flow 代码同目录）
    try:
        # 获取 flow 的基础路径（例如 my-flow/），新的目录结构中配置文件位于 {flow_name}/config 下
        flow_base_path = _get_flow_base_path()
        # 配置文件路径：与 flow 代码同目录结构的新布局
        config_key = f"{flow_base_path}config/column_types.yaml"
        
        s3_block = S3Bucket.load("rustfs-s3storage")
        bucket_name = s3_block.bucket_name
        
        # 使用 dataflow bucket（与 flow 代码存储在同一 bucket）
        dataflow_bucket = os.environ.get("RUSTFS_DATAFLOW_BUCKET", "dataflow")
        
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
        
        # 从 dataflow bucket 读取配置文件
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

