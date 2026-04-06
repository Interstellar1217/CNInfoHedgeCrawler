#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Astrbot 插件 - 巨潮资讯套期保值公告查询

使用方法：
- /套保查询 [关键词] [日期范围] - 搜索套期保值公告
- /套保 2025-01-01 2025-12-31 - 按日期范围查询
"""

import json
import time
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime

from curl_cffi import requests
from curl_cffi.requests import Session

# 引入 Astrbot 基类（需要根据实际安装情况调整）
try:
    from astrbot.api import logger
    from astrbot.api.event import AstrMessageEvent, MessageChain
    from astrbot.api.plugin import BasePlugin
    from astrbot.api.platform import MessageType
except ImportError:
    # 本地测试时用模拟类
    class BasePlugin:
        pass
    class logger:
        @staticmethod
        def info(msg): print(f"[INFO] {msg}")
        @staticmethod
        def error(msg): print(f"[ERROR] {msg}")
        @staticmethod
        def warning(msg): print(f"[WARN] {msg}")


# 导入项目配置
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import config as project_config


class CNInfoHedgePlugin(BasePlugin):
    """
    Astrbot 插件 - 巨潮资讯套期保值公告查询
    """

    def __init__(self, config: dict):
        super().__init__()
        self.config = config
        self.session = Session(impersonate="chrome136")
        self.session.headers.update(project_config.HEADERS)
        logger.info("巨潮资讯套期保值公告插件已加载")

    def register(self, context):
        """注册插件命令"""
        # 注册命令处理器
        context.register_command(
            cmd="套保查询",
            func=self.search_handler,
            description="搜索套期保值公告，用法：/套保查询 [关键词] [开始日期] [结束日期]",
            priority=5
        )
        context.register_command(
            cmd="套保",
            func=self.search_handler,
            description="快捷查询套期保值公告，用法：/套保 2025-01-01 2025-12-31",
            priority=5
        )

    async def search_handler(self, event: AstrMessageEvent, message: str):
        """
        处理套保查询命令

        用法:
        - /套保查询 - 使用默认参数查询
        - /套保查询 外汇套保 - 指定关键词
        - /套保 2025-01-01 2025-12-31 - 按日期范围查询
        - /套保查询 套期保值 2025-01-01 2025-12-31 - 完整参数
        """
        args = message.strip().split() if message.strip() else []

        keyword = project_config.DEFAULT_KEYWORD
        start_date = None
        end_date = None
        max_pages = 1

        # 解析参数
        for arg in args:
            if self._is_date(arg):
                if start_date is None:
                    start_date = arg
                elif end_date is None:
                    end_date = arg
            elif arg.isdigit():
                max_pages = int(arg)
            else:
                keyword = arg

        logger.info(f"收到查询请求：keyword={keyword}, start_date={start_date}, end_date={end_date}")

        # 执行查询
        await event.set_result(MessageChain().message_plain(f"正在查询套期保值公告，关键词：{keyword}..."))

        try:
            result = self.search_announcements(
                keyword=keyword,
                start_date=start_date,
                end_date=end_date,
                max_pages=max_pages
            )

            if result.get("success"):
                total = result.get("total", 0)
                announcements = result.get("announcements", [])[:5]  # 只显示前 5 条

                reply = f"找到 {total} 条套期保值公告：\n\n"
                for i, ann in enumerate(announcements, 1):
                    date_str = ""
                    if ann.get("publishTime"):
                        try:
                            ts = int(ann["publishTime"]) / 1000
                            date_str = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
                        except:
                            date_str = str(ann["publishTime"])[:10]

                    reply += f"{i}. {ann.get('secName', '')}（{ann.get('secCode', '')}）\n"
                    reply += f"   标题：{ann.get('title', '')}\n"
                    reply += f"   日期：{date_str}\n"
                    reply += f"   链接：{ann.get('pdfUrl', '')}\n\n"

                if total > 5:
                    reply += f"... 还有 {total - 5} 条，请缩小日期范围\n"

                await event.set_result(MessageChain().message_plain(reply))
            else:
                error_msg = result.get("error", "查询失败")
                await event.set_result(MessageChain().message_plain(f"查询失败：{error_msg}"))

        except Exception as e:
            logger.error(f"查询出错：{e}")
            await event.set_result(MessageChain().message_plain(f"查询出错：{str(e)}"))

    def _is_date(self, s: str) -> bool:
        """检查字符串是否为日期格式 YYYY-MM-DD"""
        if len(s) != 10 or s.count('-') != 2:
            return False
        try:
            datetime.strptime(s, "%Y-%m-%d")
            return True
        except ValueError:
            return False

    def search_announcements(
        self,
        keyword: str = "套期保值",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        max_pages: int = 1
    ) -> Dict[str, Any]:
        """搜索套期保值公告（与 Dify 插件共用逻辑）"""
        try:
            announcements = []

            for page_num in range(1, max_pages + 1):
                logger.info(f"正在爬取第 {page_num} 页，关键词：{keyword}")

                data = self._fetch_announcement_list(
                    keyword=keyword,
                    page_num=page_num,
                    start_date=start_date,
                    end_date=end_date
                )

                if not data:
                    logger.warning(f"第 {page_num} 页没有数据")
                    break

                page_announcements = self._parse_announcements(data)
                announcements.extend(page_announcements)

                time.sleep(project_config.get_random_delay())

            for ann in announcements:
                ann["pdfUrl"] = self._generate_pdf_url(
                    ann["announcementId"],
                    ann.get("adjunctUrl")
                )

            logger.info(f"搜索完成，共获取 {len(announcements)} 条公告")

            return {
                "success": True,
                "total": len(announcements),
                "announcements": announcements
            }

        except Exception as e:
            logger.error(f"搜索失败：{e}")
            return {
                "success": False,
                "total": 0,
                "announcements": [],
                "error": str(e)
            }

    def _fetch_announcement_list(
        self,
        keyword: str,
        page_num: int,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Optional[Dict]:
        """获取公告列表页数据"""
        params = project_config.get_search_params(
            keyword=keyword,
            page_num=page_num,
            page_size=project_config.PAGE_SIZE,
            start_date=start_date,
            end_date=end_date
        )

        try:
            response = self.session.post(
                project_config.LIST_API,
                data=params,
                timeout=30
            )

            if response.status_code != 200:
                logger.error(f"请求失败，状态码：{response.status_code}")
                return None

            if not response.text:
                logger.error(f"第 {page_num} 页响应体为空")
                return None

            data = response.json()

            if not data or 'announcements' not in data:
                logger.warning(f"第 {page_num} 页返回数据格式异常")
                return None

            return data

        except Exception as e:
            logger.error(f"请求失败：{e}")
            return None

    def _parse_announcements(self, data: Dict) -> List[Dict]:
        """解析公告列表数据"""
        announcements = []

        for item in data.get('announcements', []):
            try:
                raw_title = item.get('announcementTitle', '') or ''
                clean_title = raw_title.replace('<em>', '').replace('</em>', '')

                sec_code_raw = item.get('secCode', '')
                sec_code = str(sec_code_raw).zfill(6) if sec_code_raw else ''

                announcement = {
                    "announcementId": str(item.get('announcementId', '')),
                    "secCode": sec_code,
                    "secName": item.get('secName', ''),
                    "title": clean_title,
                    "publishTime": str(item.get('announcementTime', '')),
                    "adjunctUrl": item.get('adjunctUrl', ''),
                }

                if announcement['announcementId']:
                    announcements.append(announcement)

            except Exception as e:
                logger.error(f"解析公告数据失败：{e}")
                continue

        return announcements

    def _generate_pdf_url(self, announcement_id: str, adjunct_url: str = None) -> Optional[str]:
        """生成 PDF 下载链接"""
        if adjunct_url:
            if adjunct_url.startswith('http'):
                return adjunct_url
            path = adjunct_url if adjunct_url.startswith('/') else f"/{adjunct_url}"
            return f"{project_config.STATIC_URL}{path}"
        return f"{project_config.PDF_DOWNLOAD_URL}?announcementId={announcement_id}&flag=pdf"


# 导出插件类
__all__ = ["CNInfoHedgePlugin"]
