"""
S3 相关工具函数
"""
import os
import time
from botocore.exceptions import ClientError, IncompleteReadError
from urllib3.exceptions import IncompleteRead as Urllib3IncompleteRead


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

