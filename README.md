# CNInfoHedgeCrawler

<div align="center">

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](http://makeapullrequest.com)

**巨潮资讯网公告自动爬取工具**

支持任意关键词模糊查询，从 [巨潮资讯网](https://www.cninfo.com.cn) 自动爬取公告，提取关键信息并推送到企业微信

[功能特性](#-功能特性) • [快速开始](#-快速开始) • [插件生态](#-插件生态) • [配置说明](#-配置说明) • [开发计划](#-开发计划)

</div>

---

## 📖 项目简介

CNInfoHedgeCrawler 是一个专业的 A 股上市公司公告爬取工具，支持：

- 🔍 **智能搜索**: 支持任意关键词模糊查询，可按日期范围、公告分类精准检索
- 📥 **批量下载**: 自动翻页，断点续爬，PDF 批量下载
- 📊 **信息提取**: 从 PDF 自动提取关键字段（品种、额度、有效期、授权机构等）
- 📱 **实时推送**: 通过企业微信机器人推送结构化信息卡片
- 🤖 **AI 集成**: 提供 Dify 和 Astrbot 插件，轻松接入 AI Agent 和聊天机器人

**典型应用场景**：套期保值、业绩预告、ESG 报告、股东大会、收购重组等各类公告爬取。

## ✨ 功能特性

| 特性 | 说明 |
|------|------|
| 🛡️ **反爬规避** | 使用 `curl_cffi` 模拟 Chrome136 TLS 指纹，绕过 JA3 检测 |
| 🔄 **断点续爬** | 自动记录已下载公告 ID，避免重复劳动 |
| 📁 **结构化存储** | 元数据保存至 CSV，方便后续分析和查询 |
| 🎯 **精准提取** | 正则匹配 + 文本标准化，从 PDF 中提取 5+ 个关键字段 |
| 🚀 **异步推送** | 下载完成后自动推送企业微信，支持批量操作 |
| 🔌 **插件扩展** | 提供 Dify/Astrbot 插件，快速接入 AI 工作流 |

## 🚀 快速开始

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

## 📊 推送效果示例

```markdown
📋 套期保值公告
─────────────────────────────────
公司：福立旺（688678）
标题：关于开展外汇套期保值业务的公告
日期：2025-03-17

套保品种：外汇、美元
套保额度：5,000.00 万美元或等值外币
有效期：12 个月
套保目的：规避汇率波动风险
授权机构：董事会

[查看原文](https://www.cninfo.com.cn/...)
```

## 🔌 插件生态

### Dify 插件

**用途**: 为 AI Agent 提供公告搜索能力，支持任意关键词

**部署方式**:

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

### Astrbot 插件

**用途**: 在 QQ/微信/Telegram 中通过命令查询公告

**部署方式**:

```bash
# 复制插件到 Astrbot 目录
cp -r astrbot_plugin /path/to/astrbot/plugins/astrbot_plugin_cninfo_hedge

# 安装依赖并重启
cd /path/to/astrbot/plugins/astrbot_plugin_cninfo_hedge
pip install -r requirements.txt
```

**使用命令**:
```
/套保查询                          # 默认查询
/套保查询 外汇套保                  # 指定关键词
/套保 2025-01-01 2025-12-31        # 按日期范围
```

详细文档：[dify_plugin/README.md](dify_plugin/README.md) • [astrbot_plugin/README.md](astrbot_plugin/README.md)

## 📁 项目结构

```
CNInfoHedgeCrawler/
├── config.py                  # 配置中心（URL、请求头、Webhook 等）
├── crawler.py                 # 爬虫核心逻辑
├── util.py                    # 工具函数（日志、重试、延时）
├── extractors/
│   └── extractor.py           # PDF 文本提取与字段解析
├── notifiers/
│   └── notifier.py            # 企业微信推送 + CLI 工具
├── dify_plugin/               # Dify AI 平台插件
├── astrbot_plugin/            # Astrbot 聊天机器人插件
├── data/                      # 数据存储
│   ├── announcements_metadata.csv
│   └── *.pdf
├── logs/                      # 运行日志
├── LICENSE                    # MIT 许可证
├── README.md                  # 本文件
└── requirements.txt           # Python 依赖
```

## ⚙️ 配置说明

### config.py 关键参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `DEFAULT_KEYWORD` | "套期保值" | 默认搜索关键词 |
| `PAGE_SIZE` | 30 | 每页公告数量 |
| `MIN_DELAY` / `MAX_DELAY` | 1.0 / 3.0 | 请求随机延时范围（秒） |
| `MAX_RETRIES` | 3 | 失败重试次数 |
| `DATA_DIR` | "data" | 数据存储目录 |

### 支持的套保品种

> 外汇、美元、欧元、港元、日元、英镑、铜、铝、锌、镍、铅、锡、黄金、白银、原油、天然气、橡胶、大豆、玉米、小麦、棉花、铁矿石、螺纹钢、热轧卷板、PTA、甲醇、乙二醇、聚乙烯、聚丙烯、碳酸锂、氢氧化锂

## 🔧 高级用法

### 自定义公告分类

在 `config.py` 的 `CATEGORY_CODES` 中添加新的分类：

```python
CATEGORY_CODES = {
    "业绩预告": "category_yjyg;subcategory_yjyg",
    "ESG 报告": "category_esg;subcategory_esg",  # 自定义分类
}
```

### 跳过特定类型公告

编辑 `extractors/extractor.py` 中的 `is_policy` 判断逻辑：

```python
# 标题包含以下关键词的公告将跳过推送
skip_keywords = ['管理制度', '内部控制', '风险提示']
```

## 📝 输出文件说明

### announcements_metadata.csv

| 字段 | 说明 |
|------|------|
| announcementId | 公告唯一 ID |
| secCode | 股票代码（6 位，自动补零） |
| secName | 公司简称 |
| title | 公告标题 |
| publishTime | 发布时间（毫秒时间戳） |
| adjunctUrl | PDF 附件路径 |

### 提取字段（推送卡片）

| 字段 | 提取逻辑 |
|------|----------|
| 套保品种 | 正则匹配 30+ 种商品/货币关键词 |
| 套保额度 | 提取"不超过/上限/额度" + 数字 + 单位 |
| 有效期 | 匹配日期区间 或 N 个月/N 年 |
| 套保目的 | 规避/锁定/降低/对冲 + 风险/成本 |
| 授权机构 | 董事会/股东大会/股东会 |

## 🛠️ 常见问题

### Q: 请求被反爬拦截怎么办？
A: 本项目已使用 `curl_cffi` 模拟浏览器 TLS 指纹，正常情况下不会被拦截。如遇问题，检查 `User-Agent` 是否需要更新。

### Q: PDF 提取字段不准确？
A: 各公司公告格式差异较大，可先用预览命令查看提取效果，再调整 `extractors/extractor.py` 中的正则规则。

### Q: 如何查询已下载的公告？
A: 直接查看 `data/announcements_metadata.csv` 文件，或使用推送工具的 `--id` 命令按 ID 查询。

### Q: 插件无法加载？
A: 确保已安装依赖 `pip install -r requirements.txt`，并检查插件目录结构是否正确。

## 📅 开发计划

- [ ] 支持更多公告类型（ESG、社会责任报告等）
- [ ] 增加 PDF 表格提取能力
- [ ] 提供 REST API 接口
- [ ] 支持更多推送渠道（钉钉、飞书）
- [ ] 添加数据可视化面板

欢迎提交 Issue 和 PR！

## 📄 许可证

本项目采用 [MIT 许可证](LICENSE)

```
Copyright (c) 2025 CNInfoHedgeCrawler Contributors

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
```

## 🙏 致谢

- 数据来源：[巨潮资讯网](https://www.cninfo.com.cn)
- TLS 指纹：[curl_cffi](https://github.com/yifeikong/curl_cffi)
- PDF 解析：[pdfplumber](https://github.com/jsvine/pdfplumber)
- 日志库：[loguru](https://github.com/Delgan/loguru)

---

<div align="center">

**如果这个项目对你有帮助，欢迎 ⭐ Star 支持一下！**

[问题反馈](https://github.com/Interstellar1217/CNInfoHedgeCrawler/issues) • [功能建议](https://github.com/Interstellar1217/CNInfoHedgeCrawler/discussions)

</div>
