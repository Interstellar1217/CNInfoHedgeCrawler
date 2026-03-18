#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
工具函数模块
需求：提供通用的辅助功能，如文件处理、日志配置、延时控制等
实现思路：将常用功能封装成独立函数，便于复用
"""

import os
import time
import json
import hashlib
from functools import wraps
from pathlib import Path
from typing import Optional, Dict, Any
from urllib.parse import urlparse, parse_qs

from curl_cffi import requests
from loguru import logger
from config import config


def setup_logger(log_file: str = "crawler.log") -> None:
    """
    配置日志记录器

    需求：统一管理日志输出，同时输出到文件和终端
    实现思路：使用loguru库，配置文件和控制台两种输出方式

    Args:
        log_file: 日志文件名
    """
    # 确保日志目录存在
    log_dir = Path(config.LOGS_DIR)
    log_dir.mkdir(exist_ok=True)

    # 移除默认的处理器
    logger.remove()

    # 添加文件处理器（记录所有级别）
    logger.add(
        log_dir / log_file,
        rotation="500 MB",  # 每500MB轮转一次
        retention="30 days",  # 保留30天
        compression="zip",  # 压缩旧日志
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level="DEBUG",
        encoding="utf-8",
        backtrace=True,
        diagnose=True,
    )

    # 添加控制台处理器（只记录INFO及以上级别）
    logger.add(
        sink=lambda msg: print(msg, end=""),
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>",
        level="INFO",
        colorize=True,
    )

    logger.info("日志系统初始化完成")


def ensure_directories() -> None:
    """
    确保必要的目录存在

    需求：程序启动前创建所有需要的目录
    实现思路：检查并创建配置中定义的各个目录
    """
    directories = [
        config.DATA_DIR,
        config.LOGS_DIR,
    ]

    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        logger.debug(f"确保目录存在: {directory}")


def random_delay() -> None:
    """
    随机延时

    需求：在请求之间添加随机延时，避免对服务器造成压力
    实现思路：调用配置中的随机延时函数并等待
    """
    delay = config.get_random_delay()
    logger.debug(f"随机延时 {delay:.2f} 秒")
    time.sleep(delay)


def extract_announcement_id_from_url(url: str) -> Optional[str]:
    """
    从公告URL中提取公告ID

    需求：获取公告详情页中的唯一标识符
    实现思路：解析URL参数中的announcementId

    Args:
        url: 公告详情页URL

    Returns:
        公告ID，如果提取失败返回None
    """
    try:
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        return params.get('announcementId', [None])[0]
    except Exception as e:
        logger.error(f"从URL提取公告ID失败 {url}: {e}")
        return None


def generate_filename(title: str, announcement_id: str, file_type: str = "pdf") -> str:
    """
    生成安全的文件名

    需求：根据公告标题和ID生成唯一且安全的文件名
    实现思路：清理特殊字符，使用公告ID保证唯一性

    Args:
        title: 公告标题
        announcement_id: 公告ID
        file_type: 文件类型（pdf, html等）

    Returns:
        安全的文件名
    """
    # 清理标题中的非法字符
    invalid_chars = '<>:"/\\|?*'
    clean_title = ''.join(c for c in title if c not in invalid_chars)
    clean_title = clean_title.strip().replace(' ', '_')

    # 限制标题长度
    if len(clean_title) > 50:
        clean_title = clean_title[:50]

    return f"{clean_title}_{announcement_id}.{file_type}"


def save_metadata(metadata: Dict[str, Any], file_path: str) -> None:
    """
    保存元数据到JSON文件

    需求：保存公告的元数据信息，便于后续查询和分析
    实现思路：追加模式写入JSON文件，每条记录一行

    Args:
        metadata: 元数据字典
        file_path: 保存路径
    """
    try:
        # 确保目录存在
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)

        # 以追加模式写入
        with open(file_path, 'a', encoding='utf-8') as f:
            json_line = json.dumps(metadata, ensure_ascii=False)
            f.write(json_line + '\n')

        logger.debug(f"元数据已保存: {metadata.get('announcementId', 'unknown')}")
    except Exception as e:
        logger.error(f"保存元数据失败: {e}")


def load_metadata(file_path: str) -> list:
    """
    加载元数据

    需求：从JSON文件加载已保存的元数据
    实现思路：逐行读取JSON文件，解析每条记录

    Args:
        file_path: 元数据文件路径

    Returns:
        元数据列表
    """
    metadata_list = []

    if not Path(file_path).exists():
        logger.warning(f"元数据文件不存在: {file_path}")
        return metadata_list

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        metadata = json.loads(line)
                        metadata_list.append(metadata)
                    except json.JSONDecodeError as e:
                        logger.error(f"解析JSON行失败: {e}")
                        continue

        logger.info(f"已加载 {len(metadata_list)} 条元数据记录")
    except Exception as e:
        logger.error(f"加载元数据失败: {e}")

    return metadata_list


def calculate_file_hash(file_path: Path, algorithm: str = "md5") -> Optional[str]:
    """
    计算文件哈希值

    需求：用于验证文件完整性，避免重复下载
    实现思路：分块读取大文件，避免内存占用过大

    Args:
        file_path: 文件路径
        algorithm: 哈希算法 (md5, sha1, sha256)

    Returns:
        哈希值字符串，失败返回None
    """
    if not file_path.exists():
        return None

    hash_func = hashlib.new(algorithm)

    try:
        with open(file_path, 'rb') as f:
            # 分块读取，每块8192字节
            for chunk in iter(lambda: f.read(8192), b''):
                hash_func.update(chunk)

        return hash_func.hexdigest()
    except Exception as e:
        logger.error(f"计算文件哈希失败 {file_path}: {e}")
        return None


def retry_on_failure(max_retries: int = None, delay: float = None):
    """
    请求重试装饰器

    需求：在网络请求失败时自动重试
    实现思路：装饰器模式，捕获异常后等待并重试

    Args:
        max_retries: 最大重试次数
        delay: 重试间隔（秒）
    """
    max_retries = max_retries or config.MAX_RETRIES
    delay = delay or config.RETRY_DELAY

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    logger.warning(f"第 {attempt} 次尝试失败: {e}")

                    if attempt < max_retries:
                        logger.info(f"{delay} 秒后进行第 {attempt + 1} 次重试...")
                        time.sleep(delay)
                    else:
                        logger.error(f"已达到最大重试次数 {max_retries}，放弃请求")

            raise last_exception

        return wrapper

    return decorator


if __name__ == "__main__":
    # 测试工具函数
    setup_logger()
    ensure_directories()

    # 测试文件名生成
    test_title = "某某公司:关于开展套期保值业务的公告"
    test_id = "123456789"
    filename = generate_filename(test_title, test_id)
    print(f"生成的文件名: {filename}")