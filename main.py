#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""FastAPI 应用入口"""

from fastapi import FastAPI

from api.routes import router

app = FastAPI(
    title="CNInfo Hedge Crawler API",
    description="巨潮资讯套期保值公告爬虫 API",
    version="1.0.0",
)

app.include_router(router)


@app.on_event("startup")
async def startup_event():
    """初始化爬虫服务"""
    from api.routes import get_crawler
    get_crawler()
