#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""爬虫服务 - 公告搜索、下载、元数据管理"""

import json
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

import pandas as pd
from curl_cffi import requests
from curl_cffi.requests import Session
from tqdm import tqdm
from loguru import logger

from config import config
from util import ensure_directories, generate_filename, random_delay, retry_on_failure


class CrawlerService:
    """爬虫服务"""

    def __init__(self, keyword: str = None):
        self.keyword = keyword or config.DEFAULT_KEYWORD
        self.session = Session(impersonate="chrome136")
        self.session.headers.update(config.HEADERS)
        self.downloaded_ids = set()
        self.metadata_file = Path(config.DATA_DIR) / config.METADATA_FILE
        self._init()

    def _init(self):
        ensure_directories()
        self._load_downloaded_ids()
        logger.info(f"爬虫服务初始化完成，关键词：{self.keyword}")

    def _load_downloaded_ids(self):
        if not self.metadata_file.exists():
            return
        try:
            df = pd.read_csv(self.metadata_file)
            if 'announcementId' in df.columns:
                self.downloaded_ids = set(df['announcementId'].astype(str))
                logger.info(f"已加载 {len(self.downloaded_ids)} 条记录")
        except Exception as e:
            logger.error(f"加载记录失败：{e}")

    @retry_on_failure()
    def fetch_list(self, page_num: int = 1, start_date: str = None, end_date: str = None) -> Optional[Dict]:
        """获取公告列表"""
        params = config.get_search_params(
            keyword=self.keyword,
            page_num=page_num,
            page_size=config.PAGE_SIZE,
            start_date=start_date,
            end_date=end_date,
        )

        response = self.session.post(config.LIST_API, data=params, timeout=30)

        if response.status_code != 200:
            logger.error(f"请求失败，状态码：{response.status_code}")
            return None

        if not response.text:
            raise requests.RequestsError("Empty response body")

        data = response.json()
        return data if data and 'announcements' in data else None

    def parse_announcements(self, data: Dict) -> List[Dict]:
        """解析公告列表"""
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
                    'adjunctUrl': item.get('adjunctUrl', ''),
                }

                if announcement['announcementId']:
                    announcements.append(announcement)
            except Exception as e:
                logger.error(f"解析公告失败：{e}")
                continue

        return announcements

    def generate_pdf_url(self, announcement_id: str, adjunct_url: str = None) -> str:
        """生成 PDF 下载链接"""
        if adjunct_url:
            if adjunct_url.startswith('http'):
                return adjunct_url
            path = adjunct_url if adjunct_url.startswith('/') else f"/{adjunct_url}"
            return f"{config.STATIC_URL}{path}"
        return f"{config.PDF_DOWNLOAD_URL}?announcementId={announcement_id}&flag=pdf"

    @retry_on_failure()
    def download_pdf(self, announcement: Dict, save_path: Path) -> bool:
        """下载 PDF"""
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

        response = self.session.get(
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
            save_path = save_path.with_suffix('.html')

        total_size = int(response.headers.get('Content-Length', 0))

        with open(save_path, 'wb') as f:
            if total_size > 0:
                with tqdm(total=total_size, unit='B', unit_scale=True, desc=f"下载 {announcement_id[:8]}", leave=False) as pbar:
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

    def save_metadata(self, announcements: List[Dict]):
        """保存元数据到 CSV"""
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

        logger.info(f"元数据已保存，新增 {len(announcements)} 条")

    def crawl_page(self, page_num: int, start_date: str = None, end_date: str = None) -> List[Dict]:
        """爬取单页"""
        data = self.fetch_list(page_num, start_date=start_date, end_date=end_date)
        if not data:
            return []

        announcements = self.parse_announcements(data)
        if not announcements:
            logger.info(f"第 {page_num} 页无数据")
            return []

        new_announcements = [a for a in announcements if a['announcementId'] not in self.downloaded_ids]

        if not new_announcements:
            logger.info(f"第 {page_num} 页已全部下载过")
            return []

        logger.info(f"第 {page_num} 页发现 {len(new_announcements)} 条新公告")

        downloaded = []
        for announcement in new_announcements:
            announcement_id = announcement['announcementId']
            filename = generate_filename(announcement['title'], announcement_id, 'pdf')
            save_path = Path(config.DATA_DIR) / filename

            if save_path.exists():
                self.downloaded_ids.add(announcement_id)
                downloaded.append(announcement)
                continue

            if self.download_pdf(announcement, save_path):
                self.downloaded_ids.add(announcement_id)
                downloaded.append(announcement)
                logger.success(f"下载成功：{announcement['title']}")
            else:
                logger.error(f"下载失败：{announcement['title']}")

            random_delay()

        if downloaded:
            self.save_metadata(downloaded)

        return downloaded

    def crawl_all(self, max_pages: int = None, start_page: int = 1, start_date: str = None, end_date: str = None) -> Dict:
        """爬取所有页面"""
        stats = {
            'total_pages': 0,
            'total_announcements': 0,
            'downloaded': 0,
            'start_time': datetime.now().isoformat(),
        }

        page = start_page
        consecutive_empty = 0

        logger.info(f"开始爬取，关键词：{self.keyword}")

        while True:
            if max_pages and page > start_page + max_pages - 1:
                logger.info(f"达到最大页数限制 {max_pages}")
                break

            logger.info(f"处理第 {page} 页...")

            try:
                downloaded = self.crawl_page(page, start_date=start_date, end_date=end_date)
            except Exception as e:
                logger.error(f"第 {page} 页失败：{e}")
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    logger.info("连续 3 页失败，停止")
                    break
                page += 1
                random_delay()
                continue

            stats['total_pages'] += 1
            stats['total_announcements'] += len(downloaded)
            stats['downloaded'] += len(downloaded) if downloaded else 0

            consecutive_empty = len(downloaded) if downloaded else consecutive_empty + 1

            if consecutive_empty >= 3:
                logger.info("连续 3 页无数据，结束")
                break

            page += 1
            random_delay()

        stats['end_time'] = datetime.now().isoformat()
        stats['duration'] = str(datetime.fromisoformat(stats['end_time']) - datetime.fromisoformat(stats['start_time']))

        logger.success(f"完成：{stats['total_pages']} 页，下载 {stats['downloaded']} 条")
        return stats

    def get_metadata_df(self) -> Optional[pd.DataFrame]:
        """获取元数据 DataFrame"""
        if not self.metadata_file.exists():
            return None
        try:
            return pd.read_csv(self.metadata_file, dtype={'announcementId': str, 'secCode': str})
        except Exception as e:
            logger.error(f"加载元数据失败：{e}")
            return None

    def lookup_announcement(self, announcement_id: str) -> Optional[Dict]:
        """根据 ID 查找公告"""
        df = self.get_metadata_df()
        if df is None:
            return None

        rows = df[df["announcementId"] == str(announcement_id)]
        if rows.empty:
            return None

        row = rows.iloc[0]
        return {
            "announcementId": str(row.get("announcementId", "")),
            "secCode": str(row.get("secCode", "")),
            "secName": str(row.get("secName", "")),
            "orgId": str(row.get("orgId", "")),
            "title": str(row.get("title", "")),
            "publishTime": str(row.get("publishTime", "")),
            "adjunctUrl": str(row.get("adjunctUrl", "")),
        }

    def find_pdf_path(self, announcement_id: str) -> Optional[Path]:
        """查找本地 PDF 文件路径"""
        data_dir = Path(config.DATA_DIR)
        matches = list(data_dir.glob(f"*_{announcement_id}.pdf")) + list(data_dir.glob(f"*_{announcement_id}.PDF"))
        return matches[0] if matches else None
