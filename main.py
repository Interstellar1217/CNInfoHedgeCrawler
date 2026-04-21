#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""AstrBot 插件 - 巨潮资讯套期保值公告查询。"""

import asyncio
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from curl_cffi.requests import Session

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.star import Context, Star, register

from config import config


@register("cninfo_hedge", "interstellar", "巨潮资讯套保公告查询", "1.1.0")
class CNInfoHedgePlugin(Star):
    """AstrBot 插件 - 巨潮资讯套期保值公告查询。"""

    def __init__(self, context: Context, config_dict: dict = None):
        super().__init__(context)
        self.config = config_dict or {}
        self.session: Optional[Session] = None
        logger.info("巨潮资讯套期保值公告插件已加载")

    async def initialize(self) -> None:
        """初始化：创建 Session 并配置数据目录。"""
        self.session = Session(impersonate="chrome136")
        self.session.headers.update(config.HEADERS)
        try:
            from astrbot.api.star import StarTools
            data_dir = StarTools.get_data_dir("cninfo_hedge")
            logs_dir = data_dir / "logs"
            logs_dir.mkdir(parents=True, exist_ok=True)
            config.set_data_dir(data_dir)
            config.set_logs_dir(logs_dir)
            logger.info(f"插件数据目录：{data_dir}")
        except Exception as e:
            logger.warning(f"获取插件数据目录失败：{e}，使用默认目录")

    async def destroy(self) -> None:
        """销毁：关闭 Session，释放资源。"""
        if self.session is not None:
            self.session.close()
            logger.info("Session 已关闭")

    def _is_date(self, s: str) -> bool:
        """判断字符串是否为日期格式 (YYYY-MM-DD)。"""
        if len(s) != 10 or s.count("-") != 2:
            return False
        try:
            datetime.strptime(s, "%Y-%m-%d")
            return True
        except ValueError:
            return False

    async def _run_in_executor(self, func, *args, **kwargs):
        """在线程池中运行同步函数，避免阻塞事件循环。"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, lambda: func(*args, **kwargs))

    def search_announcements_sync(
        self,
        keyword: str = "套期保值",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        max_pages: int = 1,
    ) -> Dict[str, Any]:
        """同步搜索函数（在线程池中运行）。"""
        try:
            announcements: List[Dict[str, Any]] = []
            for page_num in range(1, max_pages + 1):
                data = self._fetch_announcement_list(
                    keyword=keyword,
                    page_num=page_num,
                    start_date=start_date,
                    end_date=end_date,
                )
                if not data:
                    break
                announcements.extend(self._parse_announcements(data))
                time.sleep(config.get_random_delay())
            for ann in announcements:
                ann["pdfUrl"] = self._generate_pdf_url(ann["announcementId"], ann.get("adjunctUrl"))
            return {"success": True, "total": len(announcements), "announcements": announcements}
        except Exception as e:
            logger.error(f"搜索失败：{e}")
            return {"success": False, "total": 0, "announcements": [], "error": str(e)}

    def _fetch_announcement_list(
        self,
        keyword: str,
        page_num: int,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """获取公告列表。"""
        params = config.get_search_params(
            keyword=keyword,
            page_num=page_num,
            page_size=config.PAGE_SIZE,
            start_date=start_date,
            end_date=end_date,
        )
        try:
            response = self.session.post(config.LIST_API, data=params, timeout=30)
            if response.status_code != 200 or not response.text:
                return None
            data = response.json()
            if not data or "announcements" not in data:
                return None
            return data
        except Exception as e:
            logger.error(f"请求失败：{e}")
            return None

    def _parse_announcements(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """解析公告列表数据。"""
        announcements: List[Dict[str, Any]] = []
        for item in data.get("announcements", []):
            try:
                raw_title = item.get("announcementTitle", "") or ""
                clean_title = raw_title.replace("<em>", "").replace("</em>", "")
                sec_code_raw = item.get("secCode", "")
                sec_code = str(sec_code_raw).zfill(6) if sec_code_raw else ""
                announcement = {
                    "announcementId": str(item.get("announcementId", "")),
                    "secCode": sec_code,
                    "secName": item.get("secName", ""),
                    "title": clean_title,
                    "publishTime": str(item.get("announcementTime", "")),
                    "adjunctUrl": item.get("adjunctUrl", ""),
                }
                if announcement["announcementId"]:
                    announcements.append(announcement)
            except Exception as e:
                logger.error(f"解析公告数据失败：{e}")
                continue
        return announcements

    def _generate_pdf_url(self, announcement_id: str, adjunct_url: str = None) -> str:
        """生成 PDF 下载链接。"""
        if adjunct_url:
            if adjunct_url.startswith("http"):
                return adjunct_url
            path = adjunct_url if adjunct_url.startswith("/") else f"/{adjunct_url}"
            return f"{config.STATIC_URL}{path}"
        return f"{config.PDF_DOWNLOAD_URL}?announcementId={announcement_id}&flag=pdf"

    @filter.command("套保查询")
    @filter.command("套保")
    async def search_handler(self, event: AstrMessageEvent, message: str = ""):
        """处理套期保值公告查询命令。"""
        args = message.strip().split() if message and message.strip() else []
        keyword = config.DEFAULT_KEYWORD
        start_date = None
        end_date = None
        max_pages = 1

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

        await event.set_result(MessageChain().message_plain(f"正在查询套期保值公告，关键词：{keyword}..."))

        try:
            result = await self._run_in_executor(
                self.search_announcements_sync,
                keyword=keyword,
                start_date=start_date,
                end_date=end_date,
                max_pages=max_pages
            )
            if result.get("success"):
                total = result.get("total", 0)
                announcements = result.get("announcements", [])[:5]
                reply = f"找到 {total} 条套期保值公告：\n\n"
                for i, ann in enumerate(announcements, 1):
                    date_str = ""
                    if ann.get("publishTime"):
                        try:
                            ts = int(ann["publishTime"]) / 1000
                            date_str = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
                        except (ValueError, TypeError) as e:
                            logger.warning(f"时间戳解析失败：{e}")
                            date_str = str(ann["publishTime"])[:10]
                    reply += f"{i}. {ann.get('secName', '')}({ann.get('secCode', '')})\n"
                    reply += f"   标题：{ann.get('title', '')}\n"
                    reply += f"   日期：{date_str}\n"
                    reply += f"   链接：{ann.get('pdfUrl', '')}\n\n"
                if total > 5:
                    reply += f"... 还有 {total - 5} 条，请缩小日期范围\n"
                await event.set_result(MessageChain().message_plain(reply))
                return
            await event.set_result(MessageChain().message_plain(f"查询失败：{result.get('error', '未知错误')}"))
        except Exception as e:
            logger.error(f"查询出错：{e}")
            await event.set_result(MessageChain().message_plain(f"查询出错：{str(e)}"))


__all__ = ["CNInfoHedgePlugin"]
