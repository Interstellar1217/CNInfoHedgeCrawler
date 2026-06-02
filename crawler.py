#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
巨潮资讯套期保值公告爬虫主模块
"""

import asyncio
import json
import sqlite3
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from curl_cffi import requests
from curl_cffi.requests import Session
from tqdm import tqdm
from loguru import logger

from config import config
from util import (
    ensure_directories,
    random_delay,
    generate_filename,
    retry_on_failure,
)
from extractors.extractor import extract_hedge_info
from notifiers.notifier import send_to_wecom
from db.repository import init_db, get_connection, insert_announcement


def _assert_no_running_loop(message: str) -> None:
    """断言当前没有运行中的事件循环，避免在异步环境中误用同步代码。"""
    try:
        asyncio.get_running_loop()
        raise RuntimeError(message)
    except RuntimeError as e:
        if "no running event loop" not in str(e):
            raise


class CNInfoHedgeCrawler:
    """
    巨潮资讯套期保值公告爬虫

    注意：本模块设计为独立运行脚本，不支持在异步环境中直接调用。
    如需在异步环境中使用，请通过 subprocess 或单独线程运行。
    """

    def __init__(self, keyword: str = None):
        self.keyword = keyword or config.DEFAULT_KEYWORD
        self.session: Optional[Session] = None
        self._executor: Optional[ThreadPoolExecutor] = None
        self._db_conn: Optional[sqlite3.Connection] = None
        self.downloaded_ids = set()
        self.metadata_file = config.get_data_dir() / config.METADATA_FILE
        self._setup()
        logger.info(f"爬虫初始化完成，搜索关键词：{self.keyword}")

    def _setup(self) -> None:
        ensure_directories()
        init_db()
        self._load_downloaded_ids()

    def initialize(self) -> None:
        self.session = Session(impersonate="chrome136")
        self.session.headers.update(config.HEADERS)
        self._executor = ThreadPoolExecutor(max_workers=4)

    def shutdown(self) -> None:
        if self.session is not None:
            self.session.close()
            self.session = None
        if self._executor is not None:
            self._executor.shutdown(wait=False)
            self._executor = None

    def _run_in_executor(self, func, *args, **kwargs):
        _assert_no_running_loop(
            "CNInfoHedgeCrawler 是同步爬虫，不能在异步环境中直接调用。"
            "请使用 asyncio.to_thread() 或在单独进程中运行。"
        )

        if self.session is None:
            self.initialize()
        future = self._executor.submit(func, *args, **kwargs)
        return future.result()

    def _load_downloaded_ids(self) -> None:
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
        announcements = []

        for item in data.get('announcements', []):
            try:
                raw_title = item.get('announcementTitle', '') or ''
                clean_title = raw_title.replace('<em>', '').replace('</em>', '')

                sec_code_raw = item.get('secCode', '')
                sec_code = str(sec_code_raw).zfill(6) if sec_code_raw else ''

                announcement = {
                    'announcementId': str(item.get('announcementId', '')),
                    'secCode': sec_code,
                    'secName': item.get('secName', ''),
                    'orgId': item.get('orgId', ''),
                    'title': clean_title,
                    'publishTime': item.get('announcementTime', ''),
                    'adjunctType': item.get('adjunctType', ''),
                    'adjunctSize': item.get('adjunctSize', 0),
                    'adjunctUrl': item.get('adjunctUrl', ''),
                }

                if announcement['announcementId']:
                    announcements.append(announcement)

            except Exception as e:
                logger.error(f"解析公告数据失败：{e}, 原始数据：{item}")
                continue

        return announcements

    def generate_pdf_url(self, announcement_id: str, adjunct_url: str = None) -> Optional[str]:
        if adjunct_url:
            if adjunct_url.startswith('http'):
                return adjunct_url
            path = adjunct_url if adjunct_url.startswith('/') else f"/{adjunct_url}"
            return f"{config.STATIC_URL}{path}"
        return f"{config.PDF_DOWNLOAD_URL}?announcementId={announcement_id}&flag=pdf"

    @retry_on_failure()
    def download_pdf(self, announcement: Dict, save_path: Path) -> bool:
        announcement_id = announcement['announcementId']
        pdf_url = self.generate_pdf_url(announcement_id, announcement.get('adjunctUrl'))
        adjunct_url = announcement.get('adjunctUrl', '')
        date_str = adjunct_url.split('/')[1] if '/' in adjunct_url else ''
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

            content_type = response.headers.get('Content-Type', '')
            if 'application/pdf' not in content_type and 'application/octet-stream' not in content_type:
                logger.warning(f"非 PDF 内容 {announcement_id}: {content_type}")
                return False

            total_size = int(response.headers.get('Content-Length', 0))

            with open(save_path, 'wb') as f:
                if total_size > 0:
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

    def save_metadata_to_db(self, announcements: List[Dict]) -> None:
        if not announcements:
            return

        conn = get_connection()
        try:
            for ann in announcements:
                insert_announcement(conn, ann)
            conn.commit()
            logger.info(f"元数据已保存到数据库，新增 {len(announcements)} 条记录")
        except Exception as e:
            logger.error(f"保存元数据失败：{e}")
            conn.rollback()
        finally:
            conn.close()

        self._export_csv(announcements)

    def _export_csv(self, announcements: List[Dict]) -> None:
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

    def _enrich_with_extraction(self, announcement: Dict, save_path: Path) -> Dict:
        enriched = dict(announcement)
        enriched['pdf_path'] = str(save_path)
        if save_path.exists():
            try:
                info = extract_hedge_info(save_path, announcement)
                enriched['varieties'] = info.get('varieties', '')
                enriched['quota'] = info.get('quota', '')
                enriched['period'] = info.get('period', '')
                enriched['purpose'] = info.get('purpose', '')
                enriched['authority'] = info.get('authority', '')
                enriched['is_policy'] = info.get('is_policy', False)
                enriched['is_irrelevant'] = info.get('is_irrelevant', False)
                enriched['filter_reason'] = info.get('filter_reason', '')
            except Exception as e:
                logger.warning(f"PDF 提取失败：{e}")
        return enriched

    def crawl_page(self, page_num: int, start_date: str = None, end_date: str = None) -> Tuple[List[Dict], int]:
        data = self.fetch_announcement_list(page_num, start_date=start_date, end_date=end_date)
        if not data:
            return [], 0

        announcements = self.parse_announcements(data)
        total_in_page = len(announcements)
        if not announcements:
            logger.info(f"第 {page_num} 页没有公告数据")
            return [], total_in_page

        new_announcements = [
            a for a in announcements
            if a['announcementId'] not in self.downloaded_ids
        ]

        if not new_announcements:
            logger.info(f"第 {page_num} 页所有公告都已下载过")
            return [], total_in_page

        logger.info(f"第 {page_num} 页发现 {len(new_announcements)} 条新公告")

        downloaded = []
        for announcement in new_announcements:
            announcement_id = announcement['announcementId']

            filename = generate_filename(announcement['title'], announcement_id, 'pdf')
            save_path = config.get_data_dir() / filename

            if save_path.exists():
                logger.debug(f"文件已存在：{filename}")
                self.downloaded_ids.add(announcement_id)
                enriched = self._enrich_with_extraction(announcement, save_path)
                downloaded.append(enriched)
                continue

            if self.download_pdf(announcement, save_path):
                self.downloaded_ids.add(announcement_id)
                enriched = self._enrich_with_extraction(announcement, save_path)
                downloaded.append(enriched)
                logger.success(f"下载成功：{announcement['title']}")

                if enriched.get("is_irrelevant"):
                    reason = enriched.get("filter_reason", "")
                    logger.info(f"跳过推送 - 无关公告：{announcement['title']}（原因：{reason}）")
                else:
                    try:
                        send_to_wecom(enriched)
                    except Exception as e:
                        logger.warning(f"推送失败，不影响下载流程：{e}")
            else:
                logger.error(f"下载失败：{announcement['title']}")

            random_delay()

        if downloaded:
            self.save_metadata_to_db(downloaded)

        return downloaded, total_in_page

    def crawl_all(self, max_pages: int = None, start_page: int = 1,
                  start_date: str = None, end_date: str = None) -> Dict:
        stats = {
            'total_pages': 0,
            'total_announcements': 0,
            'downloaded': 0,
            'start_time': datetime.now().isoformat(),
        }

        page = start_page
        consecutive_empty = 0

        logger.info(f"开始爬取，关键词：{self.keyword}")

        if self.session is None:
            self.initialize()

        try:
            while True:
                if max_pages and page > start_page + max_pages - 1:
                    logger.info(f"达到最大页数限制 {max_pages}，停止爬取")
                    break

                logger.info(f"正在处理第 {page} 页...")

                try:
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
                stats['total_announcements'] += total_in_page

                if downloaded:
                    stats['downloaded'] += len(downloaded)
                    consecutive_empty = 0
                else:
                    consecutive_empty += 1
                    logger.debug(f"第 {page} 页无新数据，连续空页：{consecutive_empty}")

                if consecutive_empty >= 3:
                    logger.info("连续 3 页无数据，爬取结束")
                    break

                page += 1

                logger.debug(f"准备爬取下一页，延时 {config.MIN_DELAY}-{config.MAX_DELAY} 秒")
                random_delay()
        finally:
            self.shutdown()

        stats['end_time'] = datetime.now().isoformat()
        stats['duration'] = str(
            datetime.fromisoformat(stats['end_time']) -
            datetime.fromisoformat(stats['start_time'])
        )

        logger.success(f"爬取完成！共处理 {stats['total_pages']} 页，下载 {stats['downloaded']} 条新公告")
        return stats

    def search_by_date(self, start_date: str, end_date: str, max_pages: int = None) -> Dict:
        logger.info(f"按日期范围搜索：{start_date} 至 {end_date}")
        return self.crawl_all(max_pages=max_pages, start_date=start_date, end_date=end_date)

    def search_announcements_sync(
        self,
        keyword: str = None,
        start_date: str = None,
        end_date: str = None,
        max_pages: int = 1,
    ) -> Dict:
        """轻量搜索：只查公告列表并生成 PDF 链接，不下载、不存储。"""
        kw = keyword or self.keyword
        try:
            announcements: List[Dict] = []
            for page_num in range(1, max_pages + 1):
                params = config.get_search_params(
                    keyword=kw,
                    page_num=page_num,
                    page_size=config.PAGE_SIZE,
                    start_date=start_date,
                    end_date=end_date,
                )
                try:
                    response = self.session.post(config.LIST_API, data=params, timeout=30)
                    if response.status_code != 200 or not response.text:
                        break
                    data = response.json()
                    if not data or "announcements" not in data:
                        break
                    page_anns = self.parse_announcements(data)
                    announcements.extend(page_anns)
                except Exception as e:
                    logger.error(f"搜索第 {page_num} 页失败：{e}")
                    break
                time.sleep(config.get_random_delay())
            for ann in announcements:
                ann["pdfUrl"] = self.generate_pdf_url(ann["announcementId"], ann.get("adjunctUrl"))
            return {"success": True, "total": len(announcements), "announcements": announcements}
        except Exception as e:
            logger.error(f"搜索失败：{e}")
            return {"success": False, "total": 0, "announcements": [], "error": str(e)}


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
