"""
Flow 注册脚本
用于批量注册所有 Flow 到 Prefect

使用方法：
1. 确保 Prefect API 已配置
2. 运行: python docs/flows/register_flows.py
"""
import os
import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Flow 注册配置
FLOW_REGISTRY = {
    # 主 Flow
    "main": {
        "name": "dataset-etl-flow",
        "entrypoint": "docs.flows.main:dataset_etl_flow",
        "file_path": "docs/flows/main.py",
        "flow_type": "main",
        "description": "数据集 ETL 处理主流程",
    },
    # 兼容入口 Flow（作为主 Flow 的子 Flow）
    "compatibility": {
        "name": "rustfs-list-files",
        "entrypoint": "docs.flows.main:list_files_flow",
        "file_path": "docs/flows/main.py",
        "flow_type": "subflow",
        "parent_flow": "dataset-etl-flow",
        "description": "兼容历史接口的入口 Flow",
    },
    # 功能 Flow
    "feature_flows": {
        "data-collection-flow": {
            "name": "data-collection-flow",
            "entrypoint": "docs.flows.feature_flows.data_collection_flow:data_collection_flow",
            "file_path": "docs/flows/feature_flows/data_collection_flow.py",
            "flow_type": "feature",
            "parent_flow": "dataset-etl-flow",
            "description": "数据收集功能 Flow",
        },
        "data-conversion-flow": {
            "name": "data-conversion-flow",
            "entrypoint": "docs.flows.feature_flows.data_conversion_flow:data_conversion_flow",
            "file_path": "docs/flows/feature_flows/data_conversion_flow.py",
            "flow_type": "feature",
            "parent_flow": "dataset-etl-flow",
            "description": "数据转换功能 Flow",
        },
        "data-cleaning-flow": {
            "name": "data-cleaning-flow",
            "entrypoint": "docs.flows.feature_flows.data_cleaning_flow:data_cleaning_flow",
            "file_path": "docs/flows/feature_flows/data_cleaning_flow.py",
            "flow_type": "feature",
            "parent_flow": "dataset-etl-flow",
            "description": "数据清洗功能 Flow",
        },
        "data-validation-flow": {
            "name": "dataset-validation-flow",
            "entrypoint": "docs.flows.feature_flows.data_validation_flow:data_validation_flow",
            "file_path": "docs/flows/feature_flows/data_validation_flow.py",
            "flow_type": "feature",
            "parent_flow": "dataset-etl-flow",
            "description": "数据验证和校准功能 Flow",
        },
        "data-encryption-flow": {
            "name": "data-encryption-flow",
            "entrypoint": "docs.flows.feature_flows.data_encryption_flow:data_encryption_flow",
            "file_path": "docs/flows/feature_flows/data_encryption_flow.py",
            "flow_type": "feature",
            "parent_flow": "dataset-etl-flow",
            "description": "数据加密功能 Flow",
        },
        "data-aggregation-flow": {
            "name": "data-aggregation-flow",
            "entrypoint": "docs.flows.feature_flows.data_aggregation_flow:data_aggregation_flow",
            "file_path": "docs/flows/feature_flows/data_aggregation_flow.py",
            "flow_type": "feature",
            "parent_flow": "dataset-etl-flow",
            "description": "数据聚合功能 Flow",
        },
    },
}


def read_flow_code(file_path: str) -> str:
    """读取 Flow 代码文件"""
    full_path = project_root / file_path
    if not full_path.exists():
        raise FileNotFoundError(f"Flow file not found: {file_path}")
    return full_path.read_text(encoding="utf-8")


def register_flow_via_api(flow_config: dict) -> dict:
    """
    通过 Prefect API 注册 Flow
    
    注意：这需要 Prefect API 的访问权限
    实际使用时，应该通过后端 API 来注册
    """
    import requests
    from requests.auth import HTTPBasicAuth

    base_url = os.getenv("PREFECT_API_BASE", "http://localhost:4200/api")
    username = os.getenv("PREFECT_API_USERNAME", "")
    password = os.getenv("PREFECT_API_PASSWORD", "")

    if not username or not password:
        raise ValueError("PREFECT_API_USERNAME and PREFECT_API_PASSWORD must be set")

    # 读取代码
    code = read_flow_code(flow_config["file_path"])

    # 准备 labels
    labels = {
        "flow_type": flow_config["flow_type"],
        "entrypoint": flow_config["entrypoint"],
        "description": flow_config.get("description", ""),
    }
    if "parent_flow" in flow_config:
        labels["parent_flow"] = flow_config["parent_flow"]

    # 创建 Flow
    response = requests.post(
        f"{base_url}/flows/",
        json={
            "name": flow_config["name"],
            "tags": ["dataset-etl"],
            "labels": labels,
            "code": code,
        },
        auth=HTTPBasicAuth(username, password),
    )

    if response.status_code != 200:
        raise Exception(f"Failed to create flow: {response.status_code} {response.text}")

    return response.json()


def print_registration_guide():
    """打印注册指南"""
    print("=" * 80)
    print("Flow 注册指南")
    print("=" * 80)
    print()
    print("由于需要通过后端 API 注册 Flow，请使用以下方法：")
    print()
    print("方法 1: 通过前端界面注册")
    print("  1. 访问 data-processing 页面")
    print("  2. 点击 '新建 Flow' 按钮")
    print("  3. 填写以下信息：")
    print()
    
    for category, flows in FLOW_REGISTRY.items():
        if category == "main":
            flow = flows
            print(f"  主 Flow: {flow['name']}")
            print(f"    - 名称: {flow['name']}")
            print(f"    - 标签: ['dataset-etl', 'main']")
            print(f"    - Labels: {{")
            print(f"        'flow_type': 'main',")
            print(f"        'entrypoint': '{flow['entrypoint']}',")
            print(f"        'description': '{flow['description']}'")
            print(f"      }}")
            print()
        elif category == "compatibility":
            flow = flows
            print(f"  兼容 Flow: {flow['name']}")
            print(f"    - 名称: {flow['name']}")
            print(f"    - 标签: ['dataset-etl', 'subflow']")
            print(f"    - Labels: {{")
            print(f"        'flow_type': 'subflow',")
            print(f"        'parent_flow': '{flow['parent_flow']}',")
            print(f"        'entrypoint': '{flow['entrypoint']}',")
            print(f"        'description': '{flow['description']}'")
            print(f"      }}")
            print()
        elif category == "feature_flows":
            print(f"  功能 Flow ({len(flows)} 个):")
            for flow_name, flow in flows.items():
                print(f"    - {flow['name']}")
                print(f"      标签: ['dataset-etl', 'feature']")
                print(f"      Labels: {{")
                print(f"        'flow_type': 'feature',")
                print(f"        'parent_flow': '{flow['parent_flow']}',")
                print(f"        'entrypoint': '{flow['entrypoint']}',")
                print(f"        'description': '{flow['description']}'")
                print(f"      }}")
            print()
    
    print("方法 2: 通过后端 API 批量注册")
    print("  使用 POST /api/prefect/flows/batch-register 端点（需要实现）")
    print()
    print("=" * 80)


if __name__ == "__main__":
    print_registration_guide()

