#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Astrbot 插件 - 巨潮资讯套期保值公告爬虫

使用方法:
  /套期保值公告 [关键词] [日期范围]

示例:
  /套期保值公告
  /套期保值公告 外汇套保
  /套期保值公告 2025-01-01 2025-12-31
"""

import requests
from typing import Optional


class CNInfoHedgePlugin:
    """Astrbot 插件主类"""

    def __init__(self, api_base_url: str = "http://localhost:8000"):
        self.api_base_url = api_base_url.rstrip('/')

    async def search_announcements(
        self,
        keyword: str = "套期保值",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        page: int = 1,
    ) -> dict:
        """搜索公告"""
        url = f"{self.api_base_url}/search"
        params = {"keyword": keyword, "page": page}

        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"error": str(e)}

    async def crawl_announcements(
        self,
        keyword: str = "套期保值",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        max_pages: int = 5,
    ) -> dict:
        """爬取公告"""
        url = f"{self.api_base_url}/crawl"
        payload = {
            "keyword": keyword,
            "start_date": start_date,
            "end_date": end_date,
            "max_pages": max_pages,
        }

        try:
            response = requests.post(url, json=payload, timeout=300)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"error": str(e)}

    async def extract_announcement(self, announcement_id: str) -> dict:
        """提取公告 PDF 内容"""
        url = f"{self.api_base_url}/announcements/{announcement_id}/extract"

        try:
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"error": str(e)}

    async def notify_announcement(self, announcement_id: str) -> dict:
        """推送公告到企业微信"""
        url = f"{self.api_base_url}/announcements/{announcement_id}/notify"

        try:
            response = requests.post(url, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"error": str(e)}


# Astrbot 插件入口
async def main(context, config):
    """Astrbot 插件主函数"""
    plugin = CNInfoHedgePlugin(api_base_url=config.get("api_base_url", "http://localhost:8000"))

    # 解析用户消息
    message = context.get_message_text().strip()
    parts = message.split()

    command = parts[0] if parts else ""

    if command == "套期保值公告":
        keyword = "套期保值"
        start_date = None
        end_date = None

        # 解析参数
        if len(parts) >= 2:
            keyword = parts[1]

        if len(parts) == 3:
            # 简单处理：假设第二个参数是日期
            start_date = parts[2]
        elif len(parts) >= 4:
            start_date = parts[2]
            end_date = parts[3]

        # 搜索结果
        result = await plugin.search_announcements(keyword=keyword, start_date=start_date, end_date=end_date)

        if "error" in result:
            await context.send(f"搜索失败：{result['error']}")
            return

        announcements = result.get("announcements", [])
        if not announcements:
            await context.send("未找到相关公告")
            return

        # 构建回复
        reply = f"找到 {len(announcements)} 条套期保值公告：\n\n"
        for i, ann in enumerate(announcements[:5], 1):  # 只显示最新 5 条
            reply += f"{i}. {ann['secName']}({ann['secCode']})\n"
            reply += f"   {ann['title']}\n"
            reply += f"   ID: {ann['announcementId']}\n\n"

        if len(announcements) > 5:
            reply += f"... 还有 {len(announcements) - 5} 条\n"

        reply += "\n回复【公告 ID】查看详情或推送"

        await context.send(reply)

    elif command.isdigit():
        # 用户输入了公告 ID，查看详情并推送
        announcement_id = command
        result = await plugin.extract_announcement(announcement_id)

        if "error" in result:
            await context.send(f"获取详情失败：{result['error']}")
            return

        # 构建卡片消息
        info = result
        reply = f"📋 套期保值公告\n\n"
        reply += f"**公司**: {info.get('sec_name', '')} ({info.get('sec_code', '')})\n"
        reply += f"**标题**: {info.get('title', '')}\n"
        reply += f"**公告日期**: {info.get('publish_date', '')}\n"

        if info.get('varieties'):
            reply += f"**套保品种**: {info['varieties']}\n"
        if info.get('quota'):
            reply += f"**套保额度**: {info['quota']}\n"
        if info.get('period'):
            reply += f"**有效期**: {info['period']}\n"
        if info.get('purpose'):
            reply += f"**套保目的**: {info['purpose']}\n"

        await context.send(reply)

        # 自动推送
        notify_result = await plugin.notify_announcement(announcement_id)
        if notify_result.get("status") == "success":
            await context.send("✓ 已推送到企业微信")
        else:
            await context.send("✗ 推送失败")

    else:
        help_msg = """
💡 套期保值公告爬虫

使用方法:
  /套期保值公告          - 搜索最新公告
  /套期保值公告 外汇      - 按关键词搜索
  /套期保值公告 2025-01-01 2025-12-31 - 按日期范围搜索
  [公告 ID]              - 查看详情并推送
""".strip()
        await context.send(help_msg)
