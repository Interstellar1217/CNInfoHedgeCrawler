# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

CNInfoHedgeCrawler - 从巨潮资讯网爬取"套期保值"公告，提取 PDF 内容并推送至企业微信。提供 CLI、RESTful API 和插件（Astrbot/Dify）三种使用方式。

## Quick Commands

```bash
# 安装依赖
pip install -r requirements.txt

# 启动 API 服务
uvicorn main:app --reload --host 0.0.0.0 --port 8000

# CLI 爬取
python crawler.py
python crawler.py --start-date 2025-01-01 --end-date 2025-12-31 --max-pages 10

# CLI 推送
python -m notifiers.notifier --id 1225015373
python -m notifiers.notifier --id 1225015373 --send
```

## Architecture

```
CNInfoHedgeCrawler/
├── config.py           # 配置：URL、Webhook、延时参数
├── main.py             # FastAPI 入口
├── crawler.py          # CLI 入口（向后兼容）
├── util.py             # 工具：日志、重试、文件名
├── core/
│   ├── crawler.py      # 爬虫服务：搜索、下载、元数据
│   ├── extractor.py    # PDF 提取：正则匹配字段
│   └── notifier.py     # 企业微信推送
├── api/
│   └── routes.py       # API 路由
├── plugins/
│   ├── astrbot/        # Astrbot 插件
│   └── dify/           # Dify 插件
├── extractors/         # 兼容层 → core.extractor
├── notifiers/          # 兼容层 → core.notifier
├── data/               # PDF 和 CSV
└── logs/
```

## API Endpoints

| 方法 | 端点 | 功能 |
|------|------|------|
| GET | `/` | API 首页 |
| GET | `/search?keyword=套期保值&page=1` | 搜索公告 |
| POST | `/crawl` | 爬取公告（body: keyword, start_date, end_date, max_pages） |
| GET | `/announcements` | 获取已下载列表 |
| GET | `/announcements/{id}/extract` | 提取 PDF 内容 |
| GET | `/announcements/{id}` | 获取公告详情（含提取内容） |
| POST | `/announcements/{id}/notify` | 推送到企业微信 |

## Core Services

### CrawlerService (`core/crawler.py`)

```python
from core.crawler import CrawlerService

service = CrawlerService(keyword="套期保值")
service.fetch_list(page_num=1)              # 获取公告列表
service.parse_announcements(data)           # 解析列表
service.download_pdf(announcement, path)    # 下载 PDF
service.crawl_all(max_pages=10)             # 爬取所有
service.lookup_announcement(id)             # 查找元数据
service.find_pdf_path(id)                   # 查找 PDF 路径
```

### ExtractorService (`core/extractor.py`)

```python
from core.extractor import extract_from_pdf

info = extract_from_pdf(pdf_path, announcement)
# 返回：sec_code, sec_name, varieties, quota, period, purpose, authority, is_policy
```

### NotifierService (`core/notifier.py`)

```python
from core.notifier import send_to_wecom, build_markdown

send_to_wecom(info)           # 推送到企业微信
build_markdown(info)          # 构建 Markdown 消息
```

## Configuration

在 `config.py` 中配置：

```python
WECOM_WEBHOOK_URL = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxx"
```

其他配置：
- `MIN_DELAY` / `MAX_DELAY`: 请求延时范围（秒）
- `MAX_RETRIES` / `RETRY_DELAY`: 重试配置
- `PAGE_SIZE`: 每页公告数量

## Technical Notes

- **反爬**: 使用 `curl_cffi` 模拟 Chrome136 TLS 指纹
- **PDF URL**: 优先 `static.cninfo.com.cn`，兜底 `pdfDownLoad` 接口
- **提取策略**: 去除所有空白字符后应用宽松正则
- **去重**: 基于 `announcementId`，自动跳过已下载
- **政策文件**: 标题含"管理制度"的文件跳过推送

## Supported Varieties

外汇、美元、欧元、港元、铜、铝、锌、镍、黄金、白银、原油、天然气、橡胶、大豆、玉米、PTA、甲醇、碳酸锂、氢氧化锂等。
