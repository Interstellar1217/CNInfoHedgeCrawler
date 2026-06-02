#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dify 插件 - 巨潮资讯套期保值公告爬虫工具

使用方法：
1. 将此插件部署到 Dify
2. 在 Dify Agent 中添加工具 "cninfo_hedge"
3. AI 可以自动调用此工具搜索套期保值公告
"""

import asyncio
import json
import time
from pathlib import Path
from typing import Dict, List, Optional, Any
from concurrent.futures import ThreadPoolExecutor

from curl_cffi import requests
from curl_cffi.requests import Session

from loguru import logger

# 导入项目配置（使用绝对路径）
PLUGIN_ROOT = Path(__file__).parent.parent.parent
CONFIG_PATH = PLUGIN_ROOT / "config.py"

# 动态导入 config 模块
import importlib.util
spec = importlib.util.spec_from_file_location("config", CONFIG_PATH)
config_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(config_module)
project_config = config_module.config


def _assert_no_running_loop(message: str) -> None:
    """断言当前没有运行中的事件循环，避免在异步环境中误用同步代码。"""
    try:
        asyncio.get_running_loop()
        raise RuntimeError(message)
    except RuntimeError as e:
        if "no running event loop" not in str(e):
            raise


class CNInfoHedgeTool:
    """
    Dify Tool - 巨潮资讯套期保值公告搜索工具
    """

    def __init__(self):
        self.session: Optional[Session] = None
        self._executor: Optional[ThreadPoolExecutor] = None

    def initialize(self) -> None:
        """初始化 Session 和线程池"""
        self.session = Session(impersonate="chrome136")
        self.session.headers.update(project_config.HEADERS)
        self._executor = ThreadPoolExecutor(max_workers=2)

    def shutdown(self) -> None:
        """关闭 Session 和线程池，释放资源"""
        if self.session is not None:
            self.session.close()
            self.session = None
        if self._executor is not None:
            self._executor.shutdown(wait=False)
            self._executor = None

    def _run_in_executor(self, func, *args, **kwargs):
        """
        在线程池中运行同步函数。

        注意：此方法使用同步等待（future.result()），仅适用于同步环境。
        """
        _assert_no_running_loop(
            "CNInfoHedgeTool 是同步工具，不能在异步环境中直接调用。"
            "请使用 asyncio.to_thread() 或在单独进程中运行。"
        )

        if self._executor is None:
            self.initialize()
        future = self._executor.submit(func, *args, **kwargs)
        return future.result()

    def search_announcements(
        self,
        keyword: str = "套期保值",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        max_pages: int = 1
    ) -> Dict[str, Any]:
        """
        搜索套期保值公告

        Args:
            keyword: 搜索关键词
            start_date: 开始日期 YYYY-MM-DD
            end_date: 结束日期 YYYY-MM-DD
            max_pages: 最大爬取页数（1-10，防止过度请求）

        Returns:
            包含公告列表的字典
        """
        # 参数边界校验
        try:
            max_pages = int(max_pages)
        except (ValueError, TypeError):
            return {
                "success": False,
                "total": 0,
                "announcements": [],
                "error": "max_pages 必须是整数"
            }

        # 限制 max_pages 范围，防止过度请求
        if max_pages < 1:
            max_pages = 1
        elif max_pages > 10:
            logger.warning(f"max_pages={max_pages} 过大，限制为 10")
            max_pages = 10

        try:
            announcements = []

            for page_num in range(1, max_pages + 1):
                logger.info(f"正在爬取第 {page_num} 页，关键词：{keyword}")

                # 获取公告列表（在线程池中运行）
                data = self._fetch_announcement_list(
                    keyword=keyword,
                    page_num=page_num,
                    start_date=start_date,
                    end_date=end_date
                )

                if not data:
                    logger.warning(f"第 {page_num} 页没有数据")
                    break

                # 解析公告
                page_announcements = self._parse_announcements(data)
                announcements.extend(page_announcements)

                # 随机延时
                time.sleep(project_config.get_random_delay())

            # 构建 PDF 链接
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
        finally:
            # 使用后关闭 Session
            self.shutdown()

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

        except requests.RequestException as e:
            logger.error(f"请求失败：{e}")
            return None
        except Exception as e:
            logger.error(f"未知错误：{e}")
            return None

    def _parse_announcements(self, data: Dict) -> List[Dict]:
        """解析公告列表数据"""
        announcements = []

        for item in data.get('announcements', []):
            try:
                # 清理标题中的 <em> 高亮标签
                raw_title = item.get('announcementTitle', '') or ''
                clean_title = raw_title.replace('<em>', '').replace('</em>', '')

                # 股票代码补前导 0
                sec_code_raw = item.get('secCode', '')
                sec_code = str(sec_code_raw).zfill(6) if sec_code_raw else ''

                announcement = {
                    "announcementId": str(item.get('announcementId', '')),
                    "secCode": sec_code,
                    "secName": item.get('secName', ''),
                    "title": clean_title,
                    "publishTime": str(item.get('announcementTime', '')),
                    "adjunctUrl": item.get('adjunctUrl', ''),
                    "adjunctType": item.get('adjunctType', ''),
                    "adjunctSize": item.get('adjunctSize', 0),
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


# 全局工具实例（用于状态保持）
_tool_instance: Optional[CNInfoHedgeTool] = None


def _get_tool() -> CNInfoHedgeTool:
    """获取或创建工具实例"""
    global _tool_instance
    if _tool_instance is None:
        _tool_instance = CNInfoHedgeTool()
        _tool_instance.initialize()
    return _tool_instance


# Dify 入口函数
def invoke(tool_name: str, credentials: Dict, tool_parameters: Dict) -> Dict:
    """
    Dify 调用入口

    Args:
        tool_name: 工具名称
        credentials: 凭证信息（如需要 API Key 等）
        tool_parameters: 工具参数

    Returns:
        工具执行结果
    """
    logger.info(f"Dify 调用工具：{tool_name}, 参数：{tool_parameters}")

    tool = _get_tool()

    if tool_name == "search_announcements":
        return tool.search_announcements(
            keyword=tool_parameters.get("keyword", "套期保值"),
            start_date=tool_parameters.get("start_date"),
            end_date=tool_parameters.get("end_date"),
            max_pages=tool_parameters.get("max_pages", 1)
        )
    else:
        return {
            "success": False,
            "error": f"未知工具：{tool_name}"
        }


# 本地测试
if __name__ == "__main__":
    # 测试搜索
    result = invoke(
        tool_name="search_announcements",
        credentials={},
        tool_parameters={
            "keyword": "套期保值",
            "start_date": "2025-01-01",
            "end_date": "2025-12-31",
            "max_pages": 1
        }
    )

    print("\n" + "=" * 60)
    print("测试结果:")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    print("=" * 60)

    # 清理资源
    if _tool_instance is not None:
        _tool_instance.shutdown()
