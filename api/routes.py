#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""FastAPI 路由"""

from pathlib import Path
from typing import Optional, List

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from core.crawler import CrawlerService
from core.extractor import extract_from_pdf
from core.notifier import send_to_wecom, build_markdown
from config import config

router = APIRouter()

_crawler_service: Optional[CrawlerService] = None


def get_crawler() -> CrawlerService:
    global _crawler_service
    if _crawler_service is None:
        _crawler_service = CrawlerService()
    return _crawler_service


class CrawlRequest(BaseModel):
    keyword: str = "套期保值"
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    max_pages: Optional[int] = None


class CrawlResponse(BaseModel):
    total_pages: int
    total_announcements: int
    downloaded: int
    duration: str


class Announcement(BaseModel):
    announcementId: str
    secCode: str
    secName: str
    title: str
    publishTime: str


class SearchResult(BaseModel):
    announcements: List[Announcement]
    has_more: bool


@router.get("/")
async def root():
    return {
        "name": "CNInfo Hedge Crawler API",
        "version": "1.0.0",
    }


@router.get("/search", response_model=SearchResult)
async def search(
    keyword: str = Query(default="套期保值"),
    page: int = Query(default=1, ge=1),
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    """搜索公告"""
    service = get_crawler()
    original_keyword = service.keyword
    service.keyword = keyword

    try:
        data = service.fetch_list(page_num=page, start_date=start_date, end_date=end_date)
        if not data:
            return SearchResult(announcements=[], has_more=False)

        announcements = service.parse_announcements(data)
        has_more = len(announcements) >= config.PAGE_SIZE

        return SearchResult(
            announcements=[
                Announcement(
                    announcementId=a['announcementId'],
                    secCode=a['secCode'],
                    secName=a['secName'],
                    title=a['title'],
                    publishTime=a['publishTime'],
                )
                for a in announcements
            ],
            has_more=has_more,
        )
    finally:
        service.keyword = original_keyword


@router.post("/crawl", response_model=CrawlResponse)
async def crawl(request: CrawlRequest):
    """爬取公告"""
    service = get_crawler()
    if request.keyword:
        service.keyword = request.keyword

    stats = service.crawl_all(
        max_pages=request.max_pages,
        start_date=request.start_date,
        end_date=request.end_date,
    )

    return CrawlResponse(
        total_pages=stats['total_pages'],
        total_announcements=stats['total_announcements'],
        downloaded=stats['downloaded'],
        duration=stats['duration'],
    )


@router.get("/announcements")
async def list_announcements():
    """获取已下载公告列表"""
    service = get_crawler()
    df = service.get_metadata_df()
    if df is None:
        return []
    return df.to_dict(orient='records')


@router.get("/announcements/{announcement_id}/extract")
async def extract(announcement_id: str):
    """提取 PDF 内容"""
    service = get_crawler()
    announcement = service.lookup_announcement(announcement_id)
    if not announcement:
        raise HTTPException(status_code=404, detail="公告不存在")

    pdf_path = service.find_pdf_path(announcement_id)
    if not pdf_path:
        raise HTTPException(status_code=404, detail="PDF 文件不存在")

    info = extract_from_pdf(pdf_path, announcement)
    return info


@router.post("/announcements/{announcement_id}/notify")
async def notify(announcement_id: str, webhook_url: Optional[str] = None):
    """推送到企业微信"""
    service = get_crawler()
    announcement = service.lookup_announcement(announcement_id)
    if not announcement:
        raise HTTPException(status_code=404, detail="公告不存在")

    pdf_path = service.find_pdf_path(announcement_id)
    if not pdf_path:
        raise HTTPException(status_code=404, detail="PDF 文件不存在")

    info = extract_from_pdf(pdf_path, announcement)
    info["org_id"] = announcement.get("orgId", "")

    success = send_to_wecom(info, webhook_url)
    if success:
        return {"status": "success"}
    raise HTTPException(status_code=500, detail="推送失败")


@router.get("/announcements/{announcement_id}")
async def get_announcement(announcement_id: str):
    """获取公告详情（含提取内容）"""
    service = get_crawler()
    announcement = service.lookup_announcement(announcement_id)
    if not announcement:
        raise HTTPException(status_code=404, detail="公告不存在")

    pdf_path = service.find_pdf_path(announcement_id)
    if pdf_path:
        info = extract_from_pdf(pdf_path, announcement)
        return {**announcement, **info}

    return announcement
