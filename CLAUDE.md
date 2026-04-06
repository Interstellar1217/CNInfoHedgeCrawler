# CLAUDE.md

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

## 项目结构

```
CNInfoHedgeCrawler/
├── config.py                  # 配置中心（URL、请求头、延时、重试、分类/市场代码、Webhook）
├── crawler.py                 # 爬虫主逻辑（CNInfoHedgeCrawler 类）
├── util.py                    # 工具函数（日志、延时、文件名生成、重试装饰器）
├── extractors/
│   └── extractor.py           # PDF 文本提取与正则字段解析
├── notifiers/
│   └── notifier.py            # 企业微信推送 + CLI 工具
├── dify_plugin/
│   └── cninfo_hedge/
│       ├── main.py            # Dify 插件入口
│       └── __init__.py
├── astrbot_plugin/
│   ├── main.py                # Astrbot 聊天机器人插件
│       └── __init__.py
├── data/
│   ├── announcements_metadata.csv
│   └── *.pdf
├── logs/
│   └── crawler.log
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
- `download_pdf()` - 流式下载，显示进度条
- `save_metadata_to_csv()` - 追加写入 CSV，去重
- `crawl_all()` - 自动翻页，连续 3 页无数据则停止

### util.py
- `setup_logger()` - loguru，文件 + 控制台双输出
- `random_delay()` - 随机延时
- `generate_filename()` - 生成 `{title}_{announcementId}.pdf`
- `retry_on_failure()` - 重试装饰器

### extractors/extractor.py
- `_normalize()` - 去除所有空白字符，消除 PDF 排版噪声
- 正则提取字段：
  - 品种：`_RE_VARIETY` - 外汇/美元/铜/黄金/大豆等 30+ 品种
  - 额度：`_RE_QUOTA` - 去重，过滤零值
  - 有效期：`_RE_PERIOD` - 绝对区间 或 N 个月/N 年
  - 目的：`_RE_PURPOSE` - 规避/锁定/降低风险
  - 授权机构：`_RE_AUTHORITY` - 董事会/股东大会
- `is_policy` - 标题含"管理制度"自动跳过推送

### notifiers/notifier.py
- `send_to_wecom()` - Markdown 卡片推送
- `preview_markdown()` - 本地预览
- CLI 模式：`--id`, `--batch`, `--send`, `--webhook`

---

## 数据流

1. 爬取：LIST_API → JSON → 去重（downloaded_ids）→ 下载 PDF → 追加 CSV
2. 提取：PDF → extract_hedge_info() → 结构化字段
3. 推送：is_policy=False → Markdown 卡片 → WeCom Webhook

---

## 关键设计

| 特性 | 实现 |
|------|------|
| TLS 指纹 | `Session(impersonate="chrome136")` |
| PDF URL | STATIC_URL + adjunctUrl（兜底 pdfDownLoad 接口） |
| 去重 | downloaded_ids 集合（从 CSV 初始化） |
| 文本标准化 | `_normalize()` 去除所有空白后再正则匹配 |
| 制度过滤 | 标题匹配"管理制度"跳过推送 |
| 重试机制 | `@retry_on_failure()` 装饰器 |

---

## 支持的套保品种

外汇、美元、欧元、港元、日元、英镑、铜、铝、锌、镍、铅、锡、黄金、白银、原油、天然气、橡胶、大豆、玉米、小麦、棉花、铁矿石、螺纹钢、热轧卷板、PTA、甲醇、乙二醇、聚乙烯、聚丙烯、碳酸锂、氢氧化锂

---

## 插件

### Dify 插件（dify_plugin/）
- **用途**: 为 AI Agent 提供工具
- **工具**: `search_announcements` - 搜索公告返回 JSON
- **入口**: `invoke(tool_name, credentials, tool_parameters)`

### Astrbot 插件（astrbot_plugin/）
- **用途**: QQ/微信聊天命令查询
- **命令**: `/套保查询 [关键词] [日期]`, `/套保 [日期]`
- **返回**: 聊天消息格式（前 5 条摘要）

---

## 依赖

| 包 | 用途 |
|----|------|
| curl_cffi | TLS 指纹模拟 |
| pdfplumber | PDF 文本提取 |
| pandas | CSV 读写 |
| loguru | 日志 |
| tqdm | 下载进度 |
| beautifulsoup4 | HTML 解析 |
