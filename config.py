#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""配置模块"""

import random
from typing import Dict


class Config:
    """配置类"""

    BASE_URL = "https://www.cninfo.com.cn"
    STATIC_URL = "https://static.cninfo.com.cn"
    SEARCH_URL = f"{BASE_URL}/new/commonUrl/pageOfSearch"

    DEFAULT_KEYWORD = "套期保值"
    PAGE_SIZE = 30

    SEARCH_URL_TEMPLATE = (
        f"{SEARCH_URL}?url=disclosure/list/search&keywords={{keyword}}"
    )

    LIST_API = f"{BASE_URL}/new/hisAnnouncement/query"
    ANNOUNCEMENT_DETAIL_URL = f"{BASE_URL}/new/disclosure/detail?announcementId={{announcement_id}}"
    PDF_DOWNLOAD_URL = f"{BASE_URL}/new/pdfDownLoad"

    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Origin': BASE_URL,
        'Referer': f'{BASE_URL}/new/commonUrl/pageOfSearch?url=disclosure/list/search&keywords=%E5%A5%97%E6%9C%9F%E4%BF%9D%E5%80%BC',
    }

    MIN_DELAY = 1.0
    MAX_DELAY = 3.0
    MAX_RETRIES = 3
    RETRY_DELAY = 2

    DATA_DIR = "data"
    LOGS_DIR = "logs"
    METADATA_FILE = "announcements_metadata.csv"

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

    STOCK_MARKETS = {
        "沪市": "shmb",
        "深市": "szmb",
        "科创板": "kcb",
        "创业板": "cyb",
        "北交所": "bj",
    }

    WECOM_WEBHOOK_URL = ""

    @classmethod
    def get_random_delay(cls) -> float:
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

        if start_date and end_date:
            params["seDate"] = f"{start_date}~{end_date}"

        return params


config = Config()
