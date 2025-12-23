"""
文件处理工具函数
"""
import os
import tempfile
from pathlib import Path


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

