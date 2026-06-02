#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import asyncio
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Callable, TypeVar, Any

from loguru import logger
from config import config


def ensure_directories() -> None:
    directories = [
        config.get_data_dir(),
        config.get_logs_dir(),
    ]
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)


def random_delay() -> None:
    delay = config.get_random_delay()
    time.sleep(delay)


def generate_filename(title: str, announcement_id: str, file_type: str = "pdf") -> str:
    invalid_chars = '<>:"/\\|？*'
    clean_title = ''.join(c for c in title if c not in invalid_chars)
    clean_title = clean_title.strip().replace(' ', '_')
    if len(clean_title) > 50:
        clean_title = clean_title[:50]
    return f"{clean_title}_{announcement_id}.{file_type}"


F = TypeVar('F', bound=Callable[..., Any])


def retry_on_failure(max_retries: int = None, delay: float = None):
    max_retries = max_retries or config.MAX_RETRIES
    retry_delay = delay or config.RETRY_DELAY

    def decorator(func: F) -> F:
        if asyncio.iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                last_exception = None
                for attempt in range(1, max_retries + 1):
                    try:
                        return await func(*args, **kwargs)
                    except Exception as e:
                        last_exception = e
                        logger.warning(f"第 {attempt} 次尝试失败：{e}")
                        if attempt < max_retries:
                            await asyncio.sleep(retry_delay)
                        else:
                            logger.error(f"已达到最大重试次数 {max_retries}，放弃请求")
                raise last_exception
            return async_wrapper
        else:
            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                last_exception = None
                for attempt in range(1, max_retries + 1):
                    try:
                        return func(*args, **kwargs)
                    except Exception as e:
                        last_exception = e
                        logger.warning(f"第 {attempt} 次尝试失败：{e}")
                        if attempt < max_retries:
                            time.sleep(retry_delay)
                        else:
                            logger.error(f"已达到最大重试次数 {max_retries}，放弃请求")
                raise last_exception
            return sync_wrapper

    return decorator


def is_date_string(s: str) -> bool:
    if len(s) != 10 or s.count("-") != 2:
        return False
    try:
        datetime.strptime(s, "%Y-%m-%d")
        return True
    except ValueError:
        return False
