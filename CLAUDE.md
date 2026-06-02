# CLAUDE.md

本项目是巨潮资讯网公告爬虫，支持 PDF 下载、信息提取和企业微信推送。同时提供 AstrBot 和 Dify 两种插件集成。

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

# 运行测试
pytest tests/ -v
python smoke_test.py
```

---

## 项目架构

### 核心数据流

```
巨潮 API → 公告列表 JSON → 去重(SQLite) → 下载PDF → 提取字段 → 推送企业微信
```

### 模块职责

| 模块 | 职责 |
|------|------|
| `config.py` | 配置中心：URL、请求头、延时、重试、分类/市场代码、Webhook |
| `crawler.py` | 爬虫核心：`CNInfoHedgeCrawler` 类，同步实现 |
| `util.py` | 工具函数：日志、延时、文件名生成、重试装饰器 |
| `extractors/extractor.py` | PDF 文本提取与正则字段解析 |
| `notifiers/notifier.py` | 企业微信推送 + CLI 工具 |
| `db/repository.py` | SQLite CRUD 操作 |
| `main.py` | AstrBot 插件入口 |
| `dify_plugin/` | Dify AI 平台插件 |

### 同步/异步架构设计

爬虫核心 (`crawler.py`) 是**同步代码**，使用 `curl_cffi` 的同步 Session。为了在异步环境（AstrBot）中使用，通过 `asyncio.to_thread()` 在独立线程中运行：

```python
result = await asyncio.to_thread(crawler.search_announcements_sync, ...)
```

`CNInfoHedgeCrawler` 内部包含运行时检测，如果在异步环境中直接调用（而非通过线程池），会抛出 `RuntimeError`。

### 代码复用原则

`main.py`（AstrBot 插件）不重复实现网络请求逻辑，直接复用 `crawler.py` 的 `CNInfoHedgeCrawler` 类。插件层只负责：
- 消息解析（命令参数提取）
- 异步线程调度（`asyncio.to_thread()`）
- 结果格式化（Markdown 回复）

爬虫逻辑（列表查询、解析、URL 生成）统一在 `crawler.py` 中维护。

### 关键设计

| 特性 | 实现 |
|------|------|
| TLS 指纹 | `Session(impersonate="chrome136")` |
| PDF URL | STATIC_URL + adjunctUrl（兜底 pdfDownLoad 接口） |
| 去重 | SQLite 数据库记录已下载 ID |
| 文本标准化 | `_normalize()` 去除所有空白后再正则匹配 |
| 制度过滤 | 标题匹配"管理制度"跳过推送 |
| 重试机制 | `@retry_on_failure()` 装饰器 |
| 存储 | SQLite 为主，CSV 兜底导出 |

---

## 项目结构

```
CNInfoHedgeCrawler/
├── config.py                  # 配置中心
├── crawler.py                 # 爬虫主逻辑
├── util.py                    # 工具函数
├── main.py                    # AstrBot 插件入口
├── metadata.yaml              # AstrBot 插件元数据
├── _conf_schema.json          # AstrBot 插件配置 schema
├── extractors/
│   ├── __init__.py
│   └── extractor.py           # PDF 文本提取与正则字段解析
├── notifiers/
│   ├── __init__.py
│   └── notifier.py            # 企业微信推送 + CLI 工具
├── db/
│   ├── __init__.py
│   └── repository.py          # SQLite CRUD 操作
├── dify_plugin/               # Dify AI 平台插件
│   ├── README.md
│   ├── requirements.txt
│   └── cninfo_hedge/
│       ├── __init__.py
│       ├── main.py
│       ├── provider.yaml
│       ├── tools.yaml
│       ├── icon.svg
│       └── tools/
│           └── search_announcements.yaml
├── tests/
│   ├── __init__.py
│   ├── test_extractor.py      # 提取器单元测试
│   └── test_boundaries.py     # 边界条件测试
├── smoke_test.py              # 冒烟测试
├── data/                      # 数据存储（gitignored: *.pdf, *.csv）
│   └── announcements.db       # SQLite 数据库
├── logs/                      # 运行日志（gitignored）
├── requirements.txt           # Python 依赖
├── LICENSE                    # MIT 许可证
├── README.md                  # 项目说明
├── CLAUDE.md                  # 本文件（AI 辅助开发指南）
└── .gitignore
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
- `generate_pdf_url()` - 构造 PDF 下载链接
- `download_pdf()` - 流式下载，非 PDF 内容返回 False
- `_enrich_with_extraction()` - 对公告补充 PDF 提取结果
- `save_metadata_to_db()` - 保存元数据到 SQLite + 导出 CSV 兜底
- `crawl_page()` - 爬取单页
- `crawl_all()` - 自动翻页
- `search_announcements_sync()` - 轻量搜索（只查列表不下载），供 AstrBot/Dify 插件复用

### extractors/extractor.py
- `_normalize()` - 去除所有空白字符，消除 PDF 排版噪声
- 正则提取字段（作用于标准化后的紧凑文本）：
  - 品种：`_RE_VARIETY` - 外汇/美元/铜/黄金/大豆等 30+ 品种
  - 额度：`_RE_QUOTA` - 去重，过滤零值，支持千分位逗号
  - 有效期：`_RE_PERIOD` - 绝对区间 或 N 个月/N 年
  - 目的：`_RE_PURPOSE` - 规避/锁定/降低风险
  - 授权机构：`_RE_AUTHORITY` - 董事会/股东大会
- `extract_hedge_info()` - 返回字典包含 org_id 字段
- `is_policy` - 标题含"管理制度"自动跳过推送

### notifiers/notifier.py
- `build_markdown()` - 构建 Markdown 消息
- `send_to_wecom()` - 同步推送 Markdown 卡片
- `send_to_wecom_async()` - 异步推送（使用 asyncio.to_thread()）
- CLI 模式：`--id`, `--batch`, `--send`, `--webhook`

### db/repository.py
- `init_db()` - 初始化数据库表
- `save_announcement()` - 保存单条公告
- `save_announcements_batch()` - 批量保存
- `announcement_exists()` - 检查是否已存在
- `get_announcements()` - 查询公告列表

---

## 数据流

1. **爬取**: LIST_API → JSON → 去重（SQLite）→ 下载 PDF → 提取字段 → 存入 SQLite + 导出 CSV
2. **提取**: PDF → extract_hedge_info() → 结构化字段
3. **推送**: is_policy=False → Markdown 卡片 → WeCom Webhook

---

## 插件生态

### AstrBot 插件（根目录）
- `main.py` - 插件入口，提供 `/套保查询` 命令
- `metadata.yaml` - 插件元数据
- `_conf_schema.json` - 配置 schema
- AstrBot 要求插件文件必须在根目录

### Dify 插件（dify_plugin/）
- `dify_plugin/cninfo_hedge/main.py` - Dify 工具入口
- 为 AI Agent 提供公告搜索能力
- 复用主项目的爬虫核心代码

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

### 支持的套保品种
外汇、美元、欧元、港元、港币、日元、英镑、人民币、铜、铝、锌、镍、铅、锡、黄金、白银、原油、天然气、橡胶、大豆、玉米、小麦、棉花、铁矿石、螺纹钢、热轧卷板、PTA、甲醇、乙二醇、聚乙烯、聚丙烯、碳酸锂、氢氧化锂、锂

---

## 已知约束

- AstrBot 插件文件（main.py, metadata.yaml, _conf_schema.json）必须位于项目根目录
- dify_plugin/ 作为独立子目录存放，不干扰 AstrBot 识别
- 爬虫使用同步 curl_cffi，异步环境通过 ThreadPoolExecutor 适配
- PDF 提取使用 pdfplumber，正则匹配经过文本标准化处理
