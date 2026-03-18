#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
配置文件模块
需求：集中管理所有配置参数，便于修改和维护
实现思路：使用类组织配置，支持通过字典或环境变量覆盖默认值
"""

import random
from typing import Dict, List, Optional


class Config:
    """项目配置类"""

    # 基础URL配置
    BASE_URL = "https://www.cninfo.com.cn"
    STATIC_URL = "https://static.cninfo.com.cn"  # PDF文件实际托管域名
    SEARCH_URL = f"{BASE_URL}/new/commonUrl/pageOfSearch"

    # 搜索参数
    DEFAULT_KEYWORD = "套期保值"
    PAGE_SIZE = 30  # 每页公告数量

    # 完整的搜索URL示例
    SEARCH_URL_TEMPLATE = (
        f"{SEARCH_URL}?url=disclosure/list/search&keywords={{keyword}}"
    )

    # 公告列表API（通过分析实际请求获得）
    LIST_API = f"{BASE_URL}/new/hisAnnouncement/query"

    # 公告详情页模板
    ANNOUNCEMENT_DETAIL_URL = f"{BASE_URL}/new/disclosure/detail?announcementId={{announcement_id}}"

    # PDF下载模板
    PDF_DOWNLOAD_URL = f"{BASE_URL}/new/pdfDownLoad"

    # 请求头配置（与浏览器实际请求保持一致）
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Origin': BASE_URL,
        'Referer': f'{BASE_URL}/new/commonUrl/pageOfSearch?url=disclosure/list/search&keywords=%E5%A5%97%E6%9C%9F%E4%BF%9D%E5%80%BC',
    }

    # 请求延时配置（秒）
    MIN_DELAY = 1.0
    MAX_DELAY = 3.0

    # 重试配置
    MAX_RETRIES = 3
    RETRY_DELAY = 2  # 重试前等待秒数

    # 数据存储配置
    DATA_DIR = "data"
    LOGS_DIR = "logs"
    METADATA_FILE = "announcements_metadata.csv"

    # 搜索过滤条件
    # 公告分类代码（根据巨潮资讯实际分类）
    CATEGORY_CODES = {
        "年报": "category_ndbg;subcategory_ndbg",
        "半年报": "category_bnbg;subcategory_bnbg",
        "一季报": "category_yjbg;subcategory_yjbg",
        "三季报": "category_sjbg;subcategory_sjbg",
        "业绩预告": "category_yjyg;subcategory_yjyg",
        "董事会": "category_dsh;subcategory_dsh",
        "监事会": "category_jsh;subcategory_jsh",
        "股东会": "category_gdh;subcategory_gdh",
        "日常经营": "category_rcjy;subcategory_rcjy",
        "公司治理": "category_gszl;subcategory_gszl",
        "中介报告": "category_zjbg;subcategory_zjbg",
    }

    # 股票市场代码
    STOCK_MARKETS = {
        "沪市": "shmb",
        "深市": "szmb",
        "科创板": "kcb",
        "创业板": "cyb",
        "北交所": "bj",
    }

    @classmethod
    def get_random_delay(cls) -> float:
        """
        获取随机延时时间
        实现思路：在最小和最大延时之间生成随机浮点数
        """
        return random.uniform(cls.MIN_DELAY, cls.MAX_DELAY)

    @classmethod
    def get_search_params(cls,
                          keyword: str = DEFAULT_KEYWORD,
                          page_num: int = 1,
                          page_size: int = None,
                          category: str = None,
                          stock_market: str = None,
                          start_date: str = None,
                          end_date: str = None) -> Dict:
        """
        构造搜索请求参数

        需求：生成巨潮资讯公告搜索API所需的参数
        实现思路：根据实际抓包分析得出的参数结构

        Args:
            keyword: 搜索关键词
            page_num: 页码
            page_size: 每页数量
            category: 公告分类
            stock_market: 股票市场
            start_date: 开始日期 YYYY-MM-DD
            end_date: 结束日期 YYYY-MM-DD

        Returns:
            参数字典
        """
        params = {
            "pageNum": page_num,
            "pageSize": page_size or cls.PAGE_SIZE,
            "column": "szse",
            "tabName": "fulltext",
            "plate": stock_market or "",
            "stock": "",
            "searchkey": keyword,
            "secid": "",
            "category": category or "",
            "trade": "",
            "seDate": "",
            "sortName": "nothing",
            "sortType": "",
            "isHLtitle": "true",
        }

        # 处理日期范围
        if start_date and end_date:
            params["seDate"] = f"{start_date}~{end_date}"

        return params


# 创建配置实例
config = Config()