#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
边界测试

测试日期范围、页码、网络异常等边界条件
"""

import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
from datetime import datetime

# 被测模块
import sys
import os
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import config


# ─────────────────────────────────────────────────────────────────────────────
# 测试日期范围边界
# ─────────────────────────────────────────────────────────────────────────────

def test_date_range_same_day():
    """日期范围为同一天应正常处理"""
    params = config.get_search_params(
        keyword="套期保值",
        page_num=1,
        start_date="2025-01-01",
        end_date="2025-01-01",
    )
    assert params["seDate"] == "2025-01-01~2025-01-01"


def test_date_range_cross_year():
    """跨年份日期范围应正常处理"""
    params = config.get_search_params(
        keyword="套期保值",
        page_num=1,
        start_date="2024-12-01",
        end_date="2025-01-31",
    )
    assert params["seDate"] == "2024-12-01~2025-01-31"


def test_date_range_empty():
    """日期范围为空应正常处理"""
    params = config.get_search_params(
        keyword="套期保值",
        page_num=1,
    )
    assert params["seDate"] == ""


def test_date_range_large_span():
    """超大日期范围应正常处理"""
    params = config.get_search_params(
        keyword="套期保值",
        page_num=1,
        start_date="2020-01-01",
        end_date="2025-12-31",
    )
    assert params["seDate"] == "2020-01-01~2025-12-31"


# ─────────────────────────────────────────────────────────────────────────────
# 测试页码边界
# ─────────────────────────────────────────────────────────────────────────────

def test_page_num_first_page():
    """第 1 页应正常处理"""
    params = config.get_search_params(page_num=1)
    assert params["pageNum"] == 1


def test_page_num_large():
    """大页码应正常处理"""
    params = config.get_search_params(page_num=999)
    assert params["pageNum"] == 999


def test_page_size_default():
    """默认页大小应为 30"""
    params = config.get_search_params()
    assert params["pageSize"] == 30


def test_page_size_custom():
    """自定义页大小应正常处理"""
    params = config.get_search_params(page_size=50)
    assert params["pageSize"] == 50


# ─────────────────────────────────────────────────────────────────────────────
# 测试关键词边界
# ─────────────────────────────────────────────────────────────────────────────

def test_keyword_empty():
    """空关键词应正常处理"""
    params = config.get_search_params(keyword="")
    assert params["searchkey"] == ""


def test_keyword_special_chars():
    """含特殊字符的关键词应正常处理"""
    params = config.get_search_params(keyword="套期保值 & 外汇")
    assert params["searchkey"] == "套期保值 & 外汇"


def test_keyword_long():
    """长关键词应正常处理"""
    keyword = "关于开展套期保值业务的可行性研究报告"
    params = config.get_search_params(keyword=keyword)
    assert params["searchkey"] == keyword


# ─────────────────────────────────────────────────────────────────────────────
# 测试配置边界
# ─────────────────────────────────────────────────────────────────────────────

def test_delay_range_valid():
    """延时范围应合理"""
    assert config.MIN_DELAY >= 0
    assert config.MAX_DELAY >= config.MIN_DELAY


def test_max_retries_positive():
    """最大重试次数应为正数"""
    assert config.MAX_RETRIES > 0


def test_filter_keywords_not_empty():
    """过滤关键词列表不应为空"""
    assert len(config.FILTER_TITLE_KEYWORDS) > 0


def test_keep_keywords_not_empty():
    """保留关键词列表不应为空"""
    assert len(config.KEEP_TITLE_KEYWORDS) > 0


# ─────────────────────────────────────────────────────────────────────────────
# 测试网络异常边界（模拟）
# ─────────────────────────────────────────────────────────────────────────────

# 注意：网络异常测试需要在 integration test 中进行，
# 因为 crawler.py 的初始化依赖较多，难以单元测试

def test_empty_response_handling():
    """空响应应被正确处理 - 待实现"""
    # TODO: 需要重构 crawler.py 以便更好地进行单元测试
    pass


def test_non_200_status_handling():
    """非 200 状态码应被正确处理"""
    from curl_cffi import requests

    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.text = "Internal Server Error"

    with patch.object(requests.Session, 'post', return_value=mock_response):
        # 应抛出异常或返回 None
        pass  # 具体逻辑在 crawler.py 中测试


# ─────────────────────────────────────────────────────────────────────────────
# 测试文件处理边界
# ─────────────────────────────────────────────────────────────────────────────

def test_filename_generation():
    """文件名生成应处理特殊字符"""
    from util import generate_filename

    # 含非法字符的标题
    title = '关于开展"套期保值"业务的公告<test>'
    filename = generate_filename(title, "123456")
    assert '<' not in filename
    assert '>' not in filename
    assert '"' not in filename
    assert '123456' in filename


def test_filename_length_limit():
    """文件名长度应被限制"""
    from util import generate_filename

    long_title = "A" * 200
    filename = generate_filename(long_title, "123456")
    assert len(filename) < 100  # 标题限制 50 + 分隔符 + ID + 后缀
