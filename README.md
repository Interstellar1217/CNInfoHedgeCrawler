# CNInfoHedgeCrawler

<div align="center">

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](http://makeapullrequest.com)

**巨潮资讯网公告自动爬取工具**

支持任意关键词模糊查询，从 [巨潮资讯网](https://www.cninfo.com.cn) 自动爬取公告，提取关键信息并推送到企业微信

[功能特性](#-功能特性) • [快速开始](#-快速开始) • [插件生态](#-插件生态) • [配置说明](#-配置说明)

</div>

---

## 项目简介

CNInfoHedgeCrawler 是一个专业的 A 股上市公司公告爬取工具，支持：

- **智能搜索**: 支持任意关键词模糊查询，可按日期范围、公告分类精准检索
- **批量下载**: 自动翻页，断点续爬，PDF 批量下载
- **信息提取**: 从 PDF 自动提取关键字段（品种、额度、有效期、授权机构等）
- **实时推送**: 通过企业微信机器人推送结构化信息卡片
- **AI 集成**: 提供 Dify 和 AstrBot 插件，轻松接入 AI Agent 和聊天机器人

**典型应用场景**：套期保值、业绩预告、ESG 报告、股东大会、收购重组等各类公告爬取。

## 功能特性

| 特性 | 说明 |
|------|------|
| **反爬规避** | 使用 `curl_cffi` 模拟 Chrome136 TLS 指纹，绕过 JA3 检测 |
| **断点续爬** | 自动记录已下载公告 ID，避免重复劳动 |
| **双重存储** | 元数据保存至 SQLite（主）+ CSV（兜底），方便查询和分析 |
| **精准提取** | 正则匹配 + 文本标准化，从 PDF 中提取 5+ 个关键字段 |
| **异步推送** | 下载完成后自动推送企业微信，支持批量操作 |
| **插件扩展** | 提供 Dify/AstrBot 插件，快速接入 AI 工作流 |

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置企业微信（可选）

编辑 `config.py`，填入企业微信机器人 Webhook URL：

```python
WECOM_WEBHOOK_URL = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=你的 key"
```

### 3. 运行爬虫

```bash
# 默认爬取"套期保值"公告
python crawler.py

# 自定义关键词（如：业绩预告、ESG、收购等）
python crawler.py --keyword 业绩

# 按日期范围爬取
python crawler.py --keyword 套期保值 --start-date 2025-01-01 --end-date 2025-12-31

# 限制页数 + 自定义关键词
python crawler.py --keyword 股东大会 --max-pages 10
```

### 4. 推送工具

```bash
# 预览单条公告（按 ID）
python -m notifiers.notifier --id 1225015373

# 批量推送 CSV 中所有记录
python -m notifiers.notifier --batch --send
```

完整命令参考：[CLAUDE.md](CLAUDE.md)

## 推送效果示例

```markdown
公司：福立旺（688678）
标题：关于开展外汇套期保值业务的公告
日期：2025-03-17

套保品种：外汇、美元
套保额度：5,000.00 万美元或等值外币
有效期：12 个月
套保目的：规避汇率波动风险
授权机构：董事会
```

## 插件生态

### AstrBot 插件

**用途**: 在 QQ/微信/Telegram 中通过命令查询公告

插件文件位于项目根目录（AstrBot 要求），配置插件时指向本项目根目录即可。

**使用命令**:

```
/套保查询                          # 默认查询
/套保查询 外汇套保                  # 指定关键词
/套保 2025-01-01 2025-12-31        # 按日期范围
```

### Dify 插件

**用途**: 为 AI Agent 提供公告搜索能力，支持任意关键词

插件位于 `dify_plugin/cninfo_hedge/`，部署方式：

```bash
# 自托管 Dify
cp -r dify_plugin/cninfo_hedge /path/to/dify/api/core/plugins/
docker restart dify-api

# Dify 云服务：打包上传
cd dify_plugin && zip -r cninfo_hedge.zip cninfo_hedge/
```

**工具参数**:
| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| keyword | string | 套期保值 | 搜索关键词（支持任意词） |
| start_date | string | - | 开始日期 YYYY-MM-DD |
| end_date | string | - | 结束日期 YYYY-MM-DD |
| max_pages | number | 1 | 最大爬取页数 |

## 项目结构

```
CNInfoHedgeCrawler/
├── config.py                  # 配置中心
├── crawler.py                 # 爬虫核心逻辑
├── util.py                    # 工具函数
├── main.py                    # AstrBot 插件入口
├── metadata.yaml              # AstrBot 插件元数据
├── _conf_schema.json          # AstrBot 插件配置 schema
├── extractors/
│   └── extractor.py           # PDF 文本提取与字段解析
├── notifiers/
│   └── notifier.py            # 企业微信推送 + CLI 工具
├── db/
│   └── repository.py          # SQLite CRUD 操作
├── dify_plugin/               # Dify AI 平台插件
├── tests/                     # 单元测试
├── smoke_test.py              # 冒烟测试
├── data/                      # 数据存储（不入库）
├── logs/                      # 运行日志（不入库）
├── requirements.txt           # Python 依赖
├── LICENSE                    # MIT 许可证
├── README.md                  # 本文件
└── CLAUDE.md                  # AI 辅助开发指南
```

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

## 常见问题

### Q: 请求被反爬拦截怎么办？
A: 本项目已使用 `curl_cffi` 模拟浏览器 TLS 指纹，正常情况下不会被拦截。如遇问题，检查 `User-Agent` 是否需要更新。

### Q: PDF 提取字段不准确？
A: 各公司公告格式差异较大，可先用预览命令查看提取效果，再调整 `extractors/extractor.py` 中的正则规则。

### Q: 如何查询已下载的公告？
A: 数据存储在 `data/announcements.db`（SQLite），同时 `data/announcements_metadata.csv` 作为兜底备份。

### Q: 插件无法加载？
A: 确保已安装依赖 `pip install -r requirements.txt`。AstrBot 插件指向项目根目录即可；Dify 插件见上方部署说明。
