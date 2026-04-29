# CODEBUDDY.md

This file provides guidance to CodeBuddy when working with code in this repository.

本项目是巨潮资讯网套期保值公告爬虫，支持 PDF 下载、信息提取和企业微信推送。

---

## 快速命令

```bash
# 安装依赖
pip install -r requirements.txt

# 爬取公告（默认关键词：套期保值）
python crawler.py

# 按日期范围爬取
python crawler.py --start-date 2025-01-01 --end-date 2025-12-31

# 限制页数
python crawler.py --start-date 2025-01-01 --end-date 2025-12-31 --max-pages 10

# 自定义关键词
python crawler.py --keyword 套期保值 --start-date 2025-01-01 --end-date 2025-12-31

# 预览公告（按 ID）
python -m notifiers.notifier --id 1225015373

# 预览公告（按文件路径）
python -m notifiers.notifier "data/xxx.pdf"

# 批量预览 CSV 所有记录
python -m notifiers.notifier --batch

# 推送到企业微信（加 --send）
python -m notifiers.notifier --id 1225015373 --send
python -m notifiers.notifier --batch --send

# 临时指定 Webhook
python -m notifiers.notifier --id 1225015373 --send --webhook "https://..."
```

---

## 项目架构

### 核心数据流

```
巨潮 API → 公告列表 JSON → 去重(CSV) → 下载PDF → 提取字段 → 推送企业微信
```

### 模块职责

| 模块 | 职责 |
|------|------|
| `config.py` | 配置中心：URL、请求头、延时、重试、分类/市场代码、Webhook |
| `crawler.py` | 爬虫核心：`CNInfoHedgeCrawler` 类，同步实现 |
| `util.py` | 工具函数：日志、延时、文件名生成、重试装饰器 |
| `extractors/extractor.py` | PDF 文本提取与正则字段解析 |
| `notifiers/notifier.py` | 企业微信推送 + CLI 工具 |
| `dify_plugin/` | Dify AI 平台插件 |
| `main.py` | AstrBot 插件入口 |

### 同步/异步架构设计

爬虫核心 (`crawler.py`) 是**同步代码**，使用 `curl_cffi` 的同步 Session。为了在异步环境（AstrBot）中使用，采用 **ThreadPoolExecutor** 模式：

```python
# 在线程池中运行同步函数，避免阻塞事件循环
async def _run_in_executor(self, func, *args, **kwargs):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: func(*args, **kwargs))
```

`CNInfoHedgeCrawler` 和 `CNInfoHedgeTool` 包含运行时检测，如果在异步环境中直接调用（而非通过线程池），会抛出 `RuntimeError`。

### 关键设计

| 特性 | 实现 |
|------|------|
| TLS 指纹 | `Session(impersonate="chrome136")` |
| PDF URL | STATIC_URL + adjunctUrl（兜底 pdfDownLoad 接口） |
| 去重 | downloaded_ids 集合（从 CSV 初始化） |
| 文本标准化 | `_normalize()` 去除所有空白后再正则匹配 |
| 制度过滤 | 标题匹配"管理制度"跳过推送 |
| 重试机制 | `@retry_on_failure()` 装饰器 |

---

## 项目结构

```
CNInfoHedgeCrawler/
├── config.py                  # 配置中心（URL、请求头、延时、重试、分类/市场代码、Webhook）
├── crawler.py                 # 爬虫主逻辑（CNInfoHedgeCrawler 类）
├── util.py                    # 工具函数（日志、延时、文件名生成、重试装饰器）
├── main.py                    # AstrBot 插件入口
├── extractors/
│   ├── __init__.py
│   └── extractor.py           # PDF 文本提取与正则字段解析
├── notifiers/
│   ├── __init__.py
│   └── notifier.py            # 企业微信推送 + CLI 工具
├── dify_plugin/
│   └── cninfo_hedge/
│       ├── main.py            # Dify 插件入口
│       └── __init__.py
├── data/
│   ├── announcements_metadata.csv
│   └── *.pdf
├── requirements.txt
└── README.md
```

---

## 核心模块

### config.py
- **URL 配置**: BASE_URL, STATIC_URL, LIST_API, PDF_DOWNLOAD_URL
- **请求头**: User-Agent, Accept, Referer（与浏览器一致）
- **延时配置**: MIN_DELAY=1.0s, MAX_DELAY=3.0s
- **重试配置**: MAX_RETRIES=3, RETRY_DELAY=2s
- **分类代码**: 年报/半年报/季报/董事会/监事会等
- **市场代码**: 沪市/深市/科创板/创业板/北交所
- **Webhook**: WECOM_WEBHOOK_URL

### crawler.py (CNInfoHedgeCrawler)
- `fetch_announcement_list()` - 获取公告列表（@retry_on_failure）
- `parse_announcements()` - 解析 JSON，提取标题/ID/日期/股票代码
- `generate_pdf_url()` - 构造 PDF 下载链接（优先 STATIC_URL + adjunctUrl）
- `download_pdf()` - 流式下载，非 PDF 内容返回 False
- `save_metadata()` - 追加写入 CSV，去重
- `crawl_page()` - 爬取单页，返回 (下载列表, 本页总数)
- `crawl_all()` - 自动翻页，使用 crawl_page 返回值统计，避免重复请求

### util.py
- `ensure_directories()` - 创建数据目录
- `random_delay()` - 随机延时
- `generate_filename()` - 生成 `{title}_{announcementId}.pdf`
- `save_metadata()` - 保存元数据到 CSV
- `retry_on_failure()` - 重试装饰器

### extractors/extractor.py
- `_normalize()` - 去除所有空白字符，消除 PDF 排版噪声
- 正则提取字段：
  - 品种：`_RE_VARIETY` - 外汇/美元/铜/黄金/大豆等 30+ 品种
  - 额度：`_RE_QUOTA` - 去重，过滤零值
  - 有效期：`_RE_PERIOD` - 绝对区间 或 N 个月/N 年
  - 目的：`_RE_PURPOSE` - 规避/锁定/降低风险
  - 授权机构：`_RE_AUTHORITY` - 董事会/股东大会
- `extract_hedge_info()` - 返回字典包含 org_id 字段
- `is_policy` - 标题含"管理制度"自动跳过推送

### notifiers/notifier.py
- `build_markdown()` - 构建 Markdown 消息（包含 org_id 链接）
- `send_to_wecom()` - 同步推送 Markdown 卡片
- `send_to_wecom_async()` - 异步推送（使用 asyncio.to_thread()）
- CLI 模式：`--id`, `--batch`, `--send`, `--webhook`

---

## 数据流

1. 爬取：LIST_API → JSON → 去重（downloaded_ids）→ 下载 PDF → 追加 CSV
2. 提取：PDF → extract_hedge_info() → 结构化字段（包含 org_id）
3. 推送：is_policy=False → Markdown 卡片 → WeCom Webhook

---

## PDF 字段提取逻辑

`extractors/extractor.py` 使用正则从 PDF 提取结构化字段：

1. **文本标准化**：`_normalize()` 去除所有空白字符，消除 PDF 排版噪声
2. **正则提取**（均作用于标准化后的紧凑文本）：
   - 品种：`_RE_VARIETY` - 外汇/美元/铜/黄金/大豆等 30+ 品种
   - 额度：`_RE_QUOTA` - 去重，过滤零值
   - 有效期：`_RE_PERIOD` - 绝对区间 或 N 个月/N 年
   - 目的：`_RE_PURPOSE` - 规避/锁定/降低风险
   - 授权机构：`_RE_AUTHORITY` - 董事会/股东大会

---

## 企业微信推送

`notifiers/notifier.py` 支持两种模式：
- **同步** `send_to_wecom()` - CLI、Dify 插件使用
- **异步** `send_to_wecom_async()` - AstrBot 插件使用（`asyncio.to_thread()`）

推送格式为 Markdown 卡片，包含：公司、标题、日期、品种、额度、有效期、目的、授权机构、原文链接（需要 org_id）。

---

## 配置说明

### config.py 关键参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `DEFAULT_KEYWORD` | "套期保值" | 默认搜索关键词 |
| `PAGE_SIZE` | 30 | 每页公告数量 |
| `MIN_DELAY` / `MAX_DELAY` | 1.0 / 3.0 | 请求随机延时范围（秒） |
| `MAX_RETRIES` | 3 | 失败重试次数 |
| `DATA_DIR` | "data" | 数据存储目录 |
| `WECOM_WEBHOOK_URL` | "" | 企业微信 Webhook URL |

### 支持的套保品种

外汇、美元、欧元、港元、日元、英镑、铜、铝、锌、镍、铅、锡、黄金、白银、原油、天然气、橡胶、大豆、玉米、小麦、棉花、铁矿石、螺纹钢、热轧卷板、PTA、甲醇、乙二醇、聚乙烯、聚丙烯、碳酸锂、氢氧化锂

---

## 插件

### Dify 插件（dify_plugin/cninfo_hedge/）
- **用途**: 为 AI Agent 提供工具
- **工具**: `search_announcements` - 搜索公告返回 JSON
- **部署**: 复制到 Dify API 插件目录或打包上传到 Dify 云

### AstrBot 插件（main.py）
- **用途**: QQ/微信聊天命令查询
- **命令**: `/套保查询 [关键词] [日期]`, `/套保 [日期]`
- **返回**: 聊天消息格式（前 5 条摘要）
- **部署**: 安装 astrbot 依赖后，将项目作为插件加载

---

## 依赖

| 包 | 用途 |
|----|------|
| curl_cffi | TLS 指纹模拟（Chrome136） |
| pdfplumber | PDF 文本提取 |
| pandas | CSV 读写 |
| loguru | 日志 |
| tqdm | 下载进度条 |
| beautifulsoup4 | HTML 解析 |
| astrbot | AstrBot 插件 SDK（可选） |

---

## 调试技巧

### 预览单条公告
```bash
python -m notifiers.notifier --id 1225015373
```

### 测试 PDF 提取效果
```bash
python -m notifiers.notifier "data/xxx.pdf"
```

### 查看提取的字段
使用 `extractors/extractor.py` 的 `extract_hedge_info()` 函数：
```python
from extractors.extractor import extract_hedge_info
from pathlib import Path
info = extract_hedge_info(Path("data/xxx.pdf"), {"secName": "某公司", "secCode": "600123", "announcementId": "1225015373"})
print(info)
```

---

## 开发注意事项

1. **同步/异步边界**：爬虫核心是同步的，在异步环境中必须通过线程池调用。

2. **正则规则修改**：所有正则都作用于 `_normalize()` 处理后的紧凑文本（去除所有空白），修改时无需考虑 PDF 排版问题。

3. **企业微信推送测试**：使用 `--webhook` 参数临时指定测试 Webhook URL。

4. **断点续爬**：已下载记录保存在 `data/announcements_metadata.csv`，删除此文件将重新爬取所有公告。

5. **PDF URL 构造**：优先使用 `STATIC_URL + adjunctUrl`，兜底使用 `PDF_DOWNLOAD_URL` 接口。

6. **download_pdf 逻辑**：非 PDF 内容时返回 False，调用方需要处理返回值。

7. **org_id 字段**：extract_hedge_info 返回的字典中包含 org_id，用于构建企业微信推送的原文链接。

---

## 最近更新

- 修复 `download_pdf` 逻辑：非 PDF 内容时返回 False
- 添加 `org_id` 字段到 `extract_hedge_info` 返回字典
- 优化 `crawl_all` 统计逻辑：使用 `crawl_page` 返回值避免重复请求
- 修正 `util.py` 文档字符串
- 优化 `extractor.py` 导入位置（`datetime` 移到顶部）
- 删除冗余目录：`core/`, `astrbot_plugin/`, `data/t2i_templates/`, `data/temp/`
