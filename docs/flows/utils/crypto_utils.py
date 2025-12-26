"""加密相关工具函数：AES-SIV + Base64 URL-safe.

特性：
- 使用 AES-SIV（确定性 AEAD），同一密钥 + 明文 (+ AAD) → 固定密文
- 使用 Base64 URL-safe 编码输出，并去掉尾部填充 "="
- 仅依赖 Python 标准库 + PyCryptodome

注意：
- 需要通过环境变量 DATAFLOW_AES_SIV_KEY 提供密钥
  - 支持 Hex 编码（长度 64/96/128，对应 32/48/64 字节）
  - 或 Base64 URL-safe 编码（可去掉尾部 "=")
"""
from __future__ import annotations

import base64
import os
from typing import Optional, Union

from Crypto.Cipher import AES


AES_SIV_ENV_KEY = "DATAFLOW_AES_SIV_KEY"


def _b64url_encode(data: bytes) -> str:
    """将二进制数据编码为 Base64 URL-safe 字符串，并去掉尾部 '='。"""
    encoded = base64.urlsafe_b64encode(data).decode("ascii")
    return encoded.rstrip("=")


def _b64url_decode(token: str) -> bytes:
    """从 Base64 URL-safe 字符串解码二进制数据（自动补齐 '='）。"""
    token = token.strip()
    # 计算需要补齐的 '=' 数量
    padding = (4 - (len(token) % 4)) % 4
    token_padded = token + ("=" * padding)
    return base64.urlsafe_b64decode(token_padded.encode("ascii"))


def _looks_like_hex(s: str) -> bool:
    if not s:
        return False
    try:
        int(s, 16)
        return True
    except ValueError:
        return False


def load_aes_siv_key(env_var: str = AES_SIV_ENV_KEY) -> bytes:
    """从环境变量加载 AES-SIV 密钥。

    支持两种形式：
    - Hex 字符串，长度为 64/96/128（对应 32/48/64 字节）
    - Base64 URL-safe 字符串（允许去掉尾部 '='）
    """
    key_str = os.environ.get(env_var, "").strip()
    if not key_str:
        raise RuntimeError(
            f"Environment variable {env_var} is not set; "
            "AES-SIV encryption cannot be used without a key."
        )

    # 优先尝试 Hex
    key: bytes
    if len(key_str) in (64, 96, 128) and _looks_like_hex(key_str):
        key = bytes.fromhex(key_str)
    else:
        # 否则按 Base64 URL-safe 解码
        try:
            key = _b64url_decode(key_str)
        except Exception as exc:  # noqa: BLE001
            raise ValueError(
                "Failed to decode AES-SIV key; it must be either hex (64/96/128 chars) "
                "or Base64 URL-safe encoded."
            ) from exc

    if len(key) not in (32, 48, 64):
        raise ValueError(
            "AES-SIV key must be 32, 48 or 64 bytes after decoding; "
            f"got {len(key)} bytes."
        )

    return key


def aes_siv_encrypt_to_b64(
    value: Union[str, bytes, None],
    *,
    key: Optional[bytes] = None,
    aad: Optional[bytes] = None,
) -> Optional[str]:
    """使用 AES-SIV 加密并返回 Base64 URL-safe 字符串（无尾部 '='）。

    - None → None
    - 空字符串 b"" → 空字符串 ""
    - 其他值先按 UTF-8（若为 str）或原始 bytes 加密
    - 输出为 tag||ciphertext 的拼接后做 Base64 URL-safe
    """
    if value is None:
        return None

    if isinstance(value, bytes):
        plaintext = value
    else:
        plaintext = value.encode("utf-8")

    if len(plaintext) == 0:
        return ""

    k = key or load_aes_siv_key()
    cipher = AES.new(k, AES.MODE_SIV)

    if aad is not None:
        ciphertext, tag = cipher.encrypt_and_digest(plaintext, aad)
    else:
        ciphertext, tag = cipher.encrypt_and_digest(plaintext)

    combined = tag + ciphertext
    return _b64url_encode(combined)


def aes_siv_decrypt_from_b64(
    token: Optional[Union[str, bytes]],
    *,
    key: Optional[bytes] = None,
    aad: Optional[bytes] = None,
) -> Optional[str]:
    """从 Base64 URL-safe 字符串解密并返回 UTF-8 文本。

    - None → None
    - 空字符串 "" → 空字符串 ""
    - 其他值视为 Base64 URL-safe 编码的 tag||ciphertext
    """
    if token is None:
        return None

    if isinstance(token, bytes):
        token_str = token.decode("ascii")
    else:
        token_str = token

    token_str = token_str.strip()
    if token_str == "":
        return ""

    data = _b64url_decode(token_str)
    if len(data) <= AES.block_size:
        raise ValueError("Invalid AES-SIV payload: too short to contain tag and ciphertext")

    tag = data[: AES.block_size]
    ciphertext = data[AES.block_size :]

    k = key or load_aes_siv_key()
    cipher = AES.new(k, AES.MODE_SIV)

    if aad is not None:
        plaintext = cipher.decrypt_and_verify(ciphertext, tag, aad)
    else:
        plaintext = cipher.decrypt_and_verify(ciphertext, tag)

    return plaintext.decode("utf-8")
