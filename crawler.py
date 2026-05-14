#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
巨潮资讯套期保值公告爬虫主模块

需求：从巨潮资讯网搜索并下载包含"套期保值"关键词的公告
实现思路：
1. 使用 requests 模拟 API 请求获取公告列表
2. 解析列表数据，提取公告元信息（标题、日期、ID 等）
3. 根据公告 ID 构造下载链接，下载 PDF 文件
4. 保存公告元数据到 CSV 文件
5. 支持断点续爬和去重
"""

import asyncio
import json
import time
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Generator, Tuple
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from curl_cffi import requests
from curl_cffi.requests import Session
from tqdm import tqdm
from loguru import logger
from bs4 import BeautifulSoup

from config import config
from util import (
    ensure_directories,
    random_delay,
    random_delay_async,
    generate_filename,
    save_metadata,
    retry_on_failure,
)
from extractors.extractor import extract_hedge_info
from notifiers.notifier import send_to_wecom
from db.repository import init_db, get_connection, announcement_exists, insert_announcement


class CNInfoHedgeCrawler:
    """
    巨潮资讯套期保值公告爬虫

    注意：本模块设计为独立运行脚本，不支持在异步环境中直接调用。
    如需在异步环境中使用，请通过 subprocess 或单独线程运行。
    """

    def __init__(self, keyword: str = None):
        """
        初始化爬虫

        Args:
            keyword: 搜索关键词，默认为"套期保值"
        """
        self.keyword = keyword or config.DEFAULT_KEYWORD
        self.session: Optional[Session] = None
        self._executor: Optional[ThreadPoolExecutor] = None
        self._db_conn: Optional[sqlite3.Connection] = None

        # 已下载的公告 ID 集合（用于去重）
        self.downloaded_ids = set()

        # 元数据存储路径
        self.metadata_file = config.get_data_dir() / config.METADATA_FILE

        # 初始化
        self._setup()

        logger.info(f"爬虫初始化完成，搜索关键词：{self.keyword}")

    def _setup(self) -> None:
        """初始化设置"""
        # 创建必要目录
        ensure_directories()

        # 初始化数据库
        init_db()

        # 加载已下载的公告 ID（从数据库）
        self._load_downloaded_ids()

    def initialize(self) -> None:
        """初始化 Session 和线程池"""
        self.session = Session(impersonate="chrome136")
        self.session.headers.update(config.HEADERS)
        self._executor = ThreadPoolExecutor(max_workers=4)

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

        注意：此方法使用同步等待（future.result()），仅适用于独立脚本模式。
        在独立脚本模式下运行（如 python crawler.py），事件循环尚未启动，因此同步等待是安全的。
        切勿在异步环境中直接调用此方法。
        """
        # 运行时检测：如果在异步环境中被调用，抛出明确的错误
        try:
            asyncio.get_running_loop()
            raise RuntimeError(
                "CNInfoHedgeCrawler 是同步爬虫，不能在异步环境中直接调用。"
                "请使用 asyncio.to_thread() 或在单独进程中运行。"
            )
        except RuntimeError as e:
            # 如果没有运行中的事件循环，这是预期的行为
            if "no running event loop" not in str(e):
                raise

        if self.session is None:
            self.initialize()
        future = self._executor.submit(func, *args, **kwargs)
        return future.result()

    def _load_downloaded_ids(self) -> None:
        """
        加载已下载的公告 ID

        需求：避免重复下载相同的公告
        实现思路：从数据库和 CSV 中读取已成功下载的公告 ID（兼容旧数据）
        """
        # 先从数据库加载
        try:
            conn = get_connection()
            try:
                rows = conn.execute("SELECT announcement_id FROM announcement").fetchall()
                for row in rows:
                    self.downloaded_ids.add(row[0])
                if rows:
                    logger.info(f"从数据库加载 {len(rows)} 条下载记录")
            finally:
                conn.close()
        except Exception as e:
            logger.warning(f"从数据库加载失败：{e}")

        # 再从 CSV 加载（兼容旧数据）
        if not self.metadata_file.exists():
            return

        try:
            df = pd.read_csv(self.metadata_file)
            if 'announcementId' in df.columns:
                csv_count = 0
                for aid in df['announcementId'].astype(str):
                    if aid not in self.downloaded_ids:
                        self.downloaded_ids.add(aid)
                        csv_count += 1
                if csv_count > 0:
                    logger.info(f"从 CSV 加载 {csv_count} 条旧记录")
            else:
                logger.warning("元数据文件中缺少 announcementId 列")
        except Exception as e:
            logger.error(f"加载 CSV 记录失败：{e}")

    @retry_on_failure()
    def fetch_announcement_list(self, page_num: int = 1, start_date: str = None, end_date: str = None) -> Optional[Dict]:
        """
        获取公告列表页数据

        需求：根据页码获取公告列表
        实现思路：调用巨潮资讯的 API 接口

        Args:
            page_num: 页码
            start_date: 开始日期 YYYY-MM-DD（可选）
            end_date: 结束日期 YYYY-MM-DD（可选）

        Returns:
            API 返回的 JSON 数据，失败返回 None
        """
        # 构造请求参数
        params = config.get_search_params(
            keyword=self.keyword,
            page_num=page_num,
            page_size=config.PAGE_SIZE,
            start_date=start_date,
            end_date=end_date,
        )

        try:
            logger.debug(f"请求第 {page_num} 页公告列表")

            # 在线程池中运行同步网络请求
            response = self._run_in_executor(
                self.session.post,
                config.LIST_API,
                data=params,
                timeout=30,
            )

            if response.status_code != 200:
                logger.error(f"请求失败，状态码：{response.status_code}")
                return None

            if not response.text:
                logger.error(f"第 {page_num} 页响应体为空，可能被反爬拦截")
                raise requests.RequestException("Empty response body")

            data = response.json()

            # 检查返回数据是否有效
            if not data or 'announcements' not in data:
                logger.warning(f"第 {page_num} 页返回数据格式异常")
                return None

            logger.debug(f"成功获取第 {page_num} 页数据，共 {len(data.get('announcements', []))} 条公告")
            return data

        except requests.RequestException as e:
            logger.error(f"请求第 {page_num} 页时发生网络错误：{e}")
            raise  # 让重试装饰器处理
        except json.JSONDecodeError as e:
            logger.error(f"解析第 {page_num} 页 JSON 数据失败：{e}")
            return None

    def parse_announcements(self, data: Dict) -> List[Dict]:
        """
        解析公告列表数据

        需求：从 API 返回的 JSON 中提取有用的公告信息
        实现思路：提取标题、ID、日期、分类等关键字段

        Args:
            data: API 返回的 JSON 数据

        Returns:
            公告信息列表
        """
        announcements = []

        for item in data.get('announcements', []):
            try:
                # 清理标题中的 <em> 高亮标签
                raw_title = item.get('announcementTitle', '') or ''
                clean_title = raw_title.replace('<em>', '').replace('</em>', '')

                # 股票代码处理：API 返回可能是数字类型，需要补前导 0 至 6 位
                sec_code_raw = item.get('secCode', '')
                sec_code = str(sec_code_raw).zfill(6) if sec_code_raw else ''

                announcement = {
                    'announcementId': str(item.get('announcementId', '')),
                    'secCode': sec_code,
                    'secName': item.get('secName', ''),
                    'orgId': item.get('orgId', ''),
                    'title': clean_title,
                    'publishTime': item.get('announcementTime', ''),  # 毫秒时间戳
                    'adjunctType': item.get('adjunctType', ''),
                    'adjunctSize': item.get('adjunctSize', 0),
                    'adjunctUrl': item.get('adjunctUrl', ''),
                }

                # 过滤掉没有公告 ID 的无效记录
                if announcement['announcementId']:
                    announcements.append(announcement)

            except Exception as e:
                logger.error(f"解析公告数据失败：{e}, 原始数据：{item}")
                continue

        return announcements

    def generate_pdf_url(self, announcement_id: str, adjunct_url: str = None) -> Optional[str]:
        """
        生成 PDF 下载链接

        需求：根据公告 ID 构造可下载的 PDF 链接
        实现思路：
          - adjunctUrl 形如 /finalpage/2026-03-18/1225015373.PDF
          - PDF 实际托管在 static.cninfo.com.cn，不是 www.cninfo.com.cn
          - adjunctUrl 为空时才走 pdfDownLoad 兜底接口

        Args:
            announcement_id: 公告 ID
            adjunct_url: 附件相对路径（来自列表 API 的 adjunctUrl 字段）

        Returns:
            PDF 下载 URL
        """
        if adjunct_url:
            if adjunct_url.startswith('http'):
                return adjunct_url
            # adjunctUrl 可能有前导斜杠也可能没有，统一处理
            path = adjunct_url if adjunct_url.startswith('/') else f"/{adjunct_url}"
            return f"{config.STATIC_URL}{path}"
        # 兜底：走主站下载接口
        return f"{config.PDF_DOWNLOAD_URL}?announcementId={announcement_id}&flag=pdf"

    @retry_on_failure()
    def download_pdf(self, announcement: Dict, save_path: Path) -> bool:
        """
        下载 PDF 文件

        需求：下载公告 PDF 并保存到本地
        实现思路：流式下载大文件，显示进度

        Args:
            announcement: 公告信息
            save_path: 保存路径

        Returns:
            下载成功返回 True，否则 False
        """
        announcement_id = announcement['announcementId']

        # 生成下载 URL
        pdf_url = self.generate_pdf_url(announcement_id, announcement.get('adjunctUrl'))

        # 从 adjunctUrl 中提取日期，格式：finalpage/2026-03-18/xxx.PDF
        adjunct_url = announcement.get('adjunctUrl', '')
        date_str = adjunct_url.split('/')[1] if '/' in adjunct_url else ''

        # 构造详情页 URL 作为 Referer，static 域名会校验来源
        referer = (
            f"{config.BASE_URL}/new/disclosure/detail"
            f"?stockCode={announcement.get('secCode', '')}"
            f"&announcementId={announcement_id}"
            f"&orgId={announcement.get('orgId', '')}"
            f"&announcementTime={date_str}"
        )

        try:
            # 在线程池中发起下载请求
            response = self._run_in_executor(
                self.session.get,
                pdf_url,
                stream=True,
                timeout=60,
                headers={'Referer': referer, 'Origin': config.BASE_URL},
            )

            if response.status_code != 200:
                logger.error(f"下载失败 {announcement_id}, 状态码：{response.status_code}")
                return False

            # 检查内容类型
            content_type = response.headers.get('Content-Type', '')
            if 'application/pdf' not in content_type and 'application/octet-stream' not in content_type:
                logger.warning(f"非 PDF 内容 {announcement_id}: {content_type}")
                # 非 PDF 内容，不保存，返回 False
                return False

            # 获取文件大小
            total_size = int(response.headers.get('Content-Length', 0))

            # 流式下载
            with open(save_path, 'wb') as f:
                if total_size > 0:
                    # 有文件大小信息时显示进度条
                    with tqdm(
                            total=total_size,
                            unit='B',
                            unit_scale=True,
                            desc=f"下载 {announcement_id[:8]}",
                            leave=False
                    ) as pbar:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                                pbar.update(len(chunk))
                else:
                    # 无文件大小信息时直接写入
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)

            logger.debug(f"文件已保存：{save_path}")
            return True

        except requests.RequestException as e:
            logger.error(f"下载 PDF 时发生网络错误 {announcement_id}: {e}")
            raise
        except IOError as e:
            logger.error(f"保存文件失败 {announcement_id}: {e}")
            return False

    def save_metadata_to_csv(self, announcements: List[Dict]) -> None:
        """
        保存元数据到数据库（兼容 CSV 导出）

        需求：将公告信息保存为结构化数据
        实现思路：使用 SQLite 存储，同时导出 CSV 用于兼容

        Args:
            announcements: 公告信息列表
        """
        if not announcements:
            return

        conn = get_connection()
        try:
            for ann in announcements:
                # 从 PDF 提取信息
                pdf_path = config.get_data_dir() / generate_filename(ann['title'], ann['announcementId'], 'pdf')
                if pdf_path.exists():
                    try:
                        info = extract_hedge_info(pdf_path, ann)
                        ann['pdf_path'] = str(pdf_path)
                        ann['varieties'] = info.get('varieties', '')
                        ann['quota'] = info.get('quota', '')
                        ann['period'] = info.get('period', '')
                        ann['purpose'] = info.get('purpose', '')
                        ann['authority'] = info.get('authority', '')
                        ann['is_policy'] = info.get('is_policy', False)
                        ann['is_irrelevant'] = info.get('is_irrelevant', False)
                        ann['filter_reason'] = info.get('filter_reason', '')
                    except Exception as e:
                        logger.warning(f"PDF 提取失败：{e}")

                # 插入数据库
                insert_announcement(conn, ann)

            conn.commit()
            logger.info(f"元数据已保存到数据库，新增 {len(announcements)} 条记录")
        except Exception as e:
            logger.error(f"保存元数据失败：{e}")
            conn.rollback()
        finally:
            conn.close()

        # 导出 CSV（兼容旧功能）
        self._export_csv(announcements)

    def _export_csv(self, announcements: List[Dict]) -> None:
        """导出 CSV 用于兼容"""
        if not announcements:
            return

        df_new = pd.DataFrame(announcements)
        if self.metadata_file.exists():
            df_existing = pd.read_csv(self.metadata_file)
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            df_combined = df_combined.drop_duplicates(subset=['announcementId'], keep='last')
            df_combined.to_csv(self.metadata_file, index=False, encoding='utf-8-sig')
        else:
            df_new.to_csv(self.metadata_file, index=False, encoding='utf-8-sig')
        logger.debug(f"CSV 已导出：{self.metadata_file}")

    def crawl_page(self, page_num: int, start_date: str = None, end_date: str = None) -> Tuple[List[Dict], int]:
        """
        爬取单页公告

        需求：获取并处理单页的公告数据
        实现思路：调用 API -> 解析 -> 过滤已下载 -> 下载 PDF -> 保存元数据

        Args:
            page_num: 页码
            start_date: 开始日期（可选）
            end_date: 结束日期（可选）

        Returns:
            (本页成功下载的公告列表, 本页公告总数)
        """
        # 获取列表数据
        data = self.fetch_announcement_list(page_num, start_date=start_date, end_date=end_date)
        if not data:
            return [], 0


        # 解析公告
        announcements = self.parse_announcements(data)
        total_in_page = len(announcements)
        if not announcements:
            logger.info(f"第 {page_num} 页没有公告数据")
            return [], total_in_page

        # 过滤已下载的公告
        new_announcements = [
            a for a in announcements
            if a['announcementId'] not in self.downloaded_ids
        ]

        if not new_announcements:
            logger.info(f"第 {page_num} 页所有公告都已下载过")
            return [], total_in_page

        logger.info(f"第 {page_num} 页发现 {len(new_announcements)} 条新公告")

        # 下载新公告
        downloaded = []
        for announcement in new_announcements:
            announcement_id = announcement['announcementId']

            # 生成保存路径
            filename = generate_filename(
                announcement['title'],
                announcement_id,
                'pdf'
            )
            save_path = config.get_data_dir() / filename

            # 如果文件已存在，跳过
            if save_path.exists():
                logger.debug(f"文件已存在：{filename}")
                self.downloaded_ids.add(announcement_id)
                downloaded.append(announcement)
                continue

            # 下载文件
            if self.download_pdf(announcement, save_path):
                self.downloaded_ids.add(announcement_id)
                downloaded.append(announcement)
                logger.success(f"下载成功：{announcement['title']}")

                # 提取 PDF 关键信息并推送企业微信
                try:
                    info = extract_hedge_info(save_path, announcement)

                    # 检查是否为无关公告
                    if info.get("is_irrelevant"):
                        reason = info.get("filter_reason", "")
                        logger.info(f"跳过推送 - 无关公告：{announcement['title']}（原因：{reason}）")
                    else:
                        send_to_wecom(info)
                except Exception as e:
                    logger.warning(f"提取/推送失败，不影响下载流程：{e}")
            else:
                logger.error(f"下载失败：{announcement['title']}")

            # 随机延时，避免请求过快
            random_delay()

        # 保存本页的元数据
        if downloaded:
            self.save_metadata_to_csv(downloaded)

        return downloaded, total_in_page

    def crawl_all(self, max_pages: int = None, start_page: int = 1,
                  start_date: str = None, end_date: str = None) -> Dict:
        """
        爬取所有页面的公告

        需求：自动翻页获取所有套期保值公告
        实现思路：循环获取每一页，直到没有数据或达到最大页数

        Args:
            max_pages: 最大爬取页数，None 表示爬取全部
            start_page: 起始页码
            start_date: 开始日期 YYYY-MM-DD（可选）
            end_date: 结束日期 YYYY-MM-DD（可选）

        Returns:
            统计信息
        """
        stats = {
            'total_pages': 0,
            'total_announcements': 0,  # 累计爬取的公告总数（包括已下载）
            'downloaded': 0,
            'start_time': datetime.now().isoformat(),
        }

        page = start_page
        consecutive_empty = 0  # 连续空页计数

        logger.info(f"开始爬取，关键词：{self.keyword}")

        # 初始化 Session
        if self.session is None:
            self.initialize()

        try:
            while True:
                # 检查最大页数限制
                if max_pages and page > start_page + max_pages - 1:
                    logger.info(f"达到最大页数限制 {max_pages}，停止爬取")
                    break

                logger.info(f"正在处理第 {page} 页...")

                try:
                    # 爬取当前页
                    downloaded, total_in_page = self.crawl_page(page, start_date=start_date, end_date=end_date)
                except Exception as e:
                    logger.error(f"第 {page} 页爬取失败：{e}")
                    consecutive_empty += 1
                    logger.warning(f"连续空页计数：{consecutive_empty}/3")
                    if consecutive_empty >= 3:
                        logger.info("连续 3 页爬取失败，停止爬取")
                        break
                    page += 1
                    random_delay()
                    continue

                stats['total_pages'] += 1
                # 使用 crawl_page 返回的本页公告总数，避免重复请求
                stats['total_announcements'] += total_in_page

                if downloaded:
                    stats['downloaded'] += len(downloaded)
                    consecutive_empty = 0
                else:
                    consecutive_empty += 1
                    logger.debug(f"第 {page} 页无新数据，连续空页：{consecutive_empty}")

                # 如果连续 3 页没有数据，认为已经爬取完毕
                if consecutive_empty >= 3:
                    logger.info("连续 3 页无数据，爬取结束")
                    break

                page += 1

                # 页面间延时
                logger.debug(f"准备爬取下一页，延时 {config.MIN_DELAY}-{config.MAX_DELAY} 秒")
                random_delay()
        finally:
            # 爬取结束后关闭 Session
            self.shutdown()

        # 记录结束时间
        stats['end_time'] = datetime.now().isoformat()
        stats['duration'] = str(
            datetime.fromisoformat(stats['end_time']) -
            datetime.fromisoformat(stats['start_time'])
        )

        logger.success(f"爬取完成！共处理 {stats['total_pages']} 页，下载 {stats['downloaded']} 条新公告")
        return stats

    def search_by_date(self, start_date: str, end_date: str, max_pages: int = None) -> Dict:
        """
        按日期范围搜索公告

        需求：在特定日期范围内搜索套期保值公告
        实现思路：将日期参数透传给 crawl_all

        Args:
            start_date: 开始日期 YYYY-MM-DD
            end_date: 结束日期 YYYY-MM-DD
            max_pages: 最大页数

        Returns:
            统计信息
        """
        logger.info(f"按日期范围搜索：{start_date} 至 {end_date}")
        return self.crawl_all(max_pages=max_pages, start_date=start_date, end_date=end_date)


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description='巨潮资讯套期保值公告爬虫')
    parser.add_argument('--keyword', type=str, default='套期保值',
                        help='搜索关键词 (默认：套期保值)')
    parser.add_argument('--max-pages', type=int, default=None,
                        help='最大爬取页数 (默认：全部)')
    parser.add_argument('--start-page', type=int, default=1,
                        help='起始页码 (默认：1)')
    parser.add_argument('--start-date', type=str,
                        help='开始日期 YYYY-MM-DD')
    parser.add_argument('--end-date', type=str,
                        help='结束日期 YYYY-MM-DD')

    args = parser.parse_args()

    # 创建爬虫实例
    crawler = CNInfoHedgeCrawler(keyword=args.keyword)

    try:
        if args.start_date and args.end_date:
            # 按日期范围搜索
            stats = crawler.search_by_date(
                start_date=args.start_date,
                end_date=args.end_date,
                max_pages=args.max_pages
            )
        else:
            # 普通搜索
            stats = crawler.crawl_all(
                max_pages=args.max_pages,
                start_page=args.start_page,
            )

        # 输出统计信息
        print("\n" + "=" * 50)
        print("爬取完成！统计信息：")
        print(f"关键词：{args.keyword}")
        print(f"处理页数：{stats['total_pages']}")
        print(f"下载公告：{stats['downloaded']}")
        print(f"总公告数：{stats['total_announcements']}")
        print(f"开始时间：{stats['start_time']}")
        print(f"结束时间：{stats['end_time']}")
        print(f"总耗时：{stats['duration']}")
        print("=" * 50)

    except KeyboardInterrupt:
        logger.warning("用户中断程序")
    except Exception as e:
        logger.error(f"程序运行出错：{e}")
        raise
    finally:
        # 确保资源释放
        crawler.shutdown()


if __name__ == "__main__":
    main()
