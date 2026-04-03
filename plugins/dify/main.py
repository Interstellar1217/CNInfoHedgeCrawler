#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dify 插件工具 - 巨潮资讯套期保值公告爬虫
"""

import requests
from typing import Any


def search_announcements(
    api_base_url: str,
    keyword: str = "套期保值",
    start_date: str = None,
    end_date: str = None,
    page: int = 1,
) -> dict:
    """
    搜索套期保值公告

    Args:
        api_base_url: API 服务地址
        keyword: 搜索关键词
        start_date: 开始日期 YYYY-MM-DD
        end_date: 结束日期 YYYY-MM-DD
        page: 页码

    Returns:
        公告列表
    """
    url = f"{api_base_url.rstrip('/')}/search"
    params = {"keyword": keyword, "page": page}

    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        # 格式化输出
        announcements = data.get("announcements", [])
        if not announcements:
            return {"result": "未找到相关公告"}

        output = f"找到 {len(announcements)} 条套期保值公告：\n\n"
        for i, ann in enumerate(announcements, 1):
            output += f"{i}. 【{ann['secName']}({ann['secCode']})】\n"
            output += f"   标题：{ann['title']}\n"
            output += f"   公告 ID: {ann['announcementId']}\n\n"

        return {"result": output}

    except requests.RequestException as e:
        return {"error": str(e)}


def extract_announcement(api_base_url: str, announcement_id: str) -> dict:
    """
    提取公告 PDF 内容

    Args:
        api_base_url: API 服务地址
        announcement_id: 公告 ID

    Returns:
        提取的结构化信息
    """
    url = f"{api_base_url.rstrip('/')}/announcements/{announcement_id}/extract"

    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        data = response.json()

        # 格式化输出
        output = f"📋 套期保值公告提取结果\n\n"
        output += f"**公司**: {data.get('sec_name', '')} ({data.get('sec_code', '')})\n"
        output += f"**标题**: {data.get('title', '')}\n"
        output += f"**公告日期**: {data.get('publish_date', '')}\n"

        if data.get('varieties'):
            output += f"**套保品种**: {data['varieties']}\n"
        if data.get('quota'):
            output += f"**套保额度**: {data['quota']}\n"
        if data.get('period'):
            output += f"**有效期**: {data['period']}\n"
        if data.get('purpose'):
            output += f"**套保目的**: {data['purpose']}\n"
        if data.get('authority'):
            output += f"**授权机构**: {data['authority']}\n"

        # 判断是否为管理制度类文件
        if data.get('is_policy'):
            output += "\n⚠️ 注意：此为管理制度类文件"

        return {"result": output}

    except requests.RequestException as e:
        return {"error": str(e)}


# Dify 工具入口函数
def main(arguments: dict, credentials: dict) -> dict:
    """
    Dify 工具主函数

    Args:
        arguments: 工具参数
        credentials: 认证信息

    Returns:
        工具执行结果
    """
    api_base_url = credentials.get("api_base_url", "http://localhost:8000")
    tool_name = arguments.get("__tool_name__", "")

    if tool_name == "search_announcements":
        return search_announcements(
            api_base_url=api_base_url,
            keyword=arguments.get("keyword", "套期保值"),
            start_date=arguments.get("start_date"),
            end_date=arguments.get("end_date"),
            page=arguments.get("page", 1),
        )
    elif tool_name == "extract_announcement":
        return extract_announcement(
            api_base_url=api_base_url,
            announcement_id=arguments.get("announcement_id"),
        )
    else:
        return {"error": f"Unknown tool: {tool_name}"}
