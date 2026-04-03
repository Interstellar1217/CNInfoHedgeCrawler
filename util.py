#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""工具函数模块"""

import time
import hashlib
from functools import wraps
from pathlib import Path
from typing import Optional, Dict, Any

from loguru import logger
from config import config


def setup_logger(log_file: str = "crawler.log") -> None:
    """配置日志记录器 - 同时输出到文件和控制台"""
    log_dir = Path(config.LOGS_DIR)
    log_dir.mkdir(exist_ok=True)

    logger.remove()

    logger.add(
        log_dir / log_file,
        rotation="500 MB",
        retention="30 days",
        compression="zip",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level="DEBUG",
        encoding="utf-8",
        backtrace=True,
        diagnose=True,
    )

    logger.add(
        sink=lambda msg: print(msg, end=""),
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>",
        level="INFO",
        colorize=True,
    )

    logger.info("日志系统初始化完成")


def ensure_directories() -> None:
    """创建必要的目录"""
    for directory in [config.DATA_DIR, config.LOGS_DIR]:
        Path(directory).mkdir(parents=True, exist_ok=True)
        logger.debug(f"确保目录存在：{directory}")


def random_delay() -> None:
    """随机延时"""
    delay = config.get_random_delay()
    logger.debug(f"随机延时 {delay:.2f} 秒")
    time.sleep(delay)


def generate_filename(title: str, announcement_id: str, file_type: str = "pdf") -> str:
    """生成安全的文件名 - 清理特殊字符，限制长度"""
    invalid_chars = '<>:"/\\|?*'
    clean_title = ''.join(c for c in title if c not in invalid_chars)
    clean_title = clean_title.strip().replace(' ', '_')
    if len(clean_title) > 50:
        clean_title = clean_title[:50]
    return f"{clean_title}_{announcement_id}.{file_type}"


def retry_on_failure(max_retries: int = None, delay: float = None):
    """请求重试装饰器"""
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
                    logger.warning(f"第 {attempt} 次尝试失败：{e}")
                    if attempt < max_retries:
                        logger.info(f"{delay} 秒后进行第 {attempt + 1} 次重试...")
                        time.sleep(delay)
                    else:
                        logger.error(f"已达到最大重试次数 {max_retries}，放弃请求")
            raise last_exception
        return wrapper
    return decorator
