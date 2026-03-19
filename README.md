# CNInfoHedgeCrawler

从[巨潮资讯网](https://www.cninfo.com.cn)自动爬取"套期保值"相关公告 PDF，提取关键信息，并通过企业微信机器人推送信息卡片。

## 功能

- 按关键词 + 日期范围搜索公告，自动翻页
- 使用 `curl_cffi` 模拟浏览器 TLS/JA3 指纹，规避反爬
- 下载 PDF 到本地，元数据保存至 CSV
- 断点续爬，避免重复下载
- 从 PDF 提取套保品种、额度、有效期、目的、授权机构
- 通过企业微信机器人推送 Markdown 信息卡片
- CLI 工具支持单条预览、按 ID 查找、批量推送

## 项目结构

```
CNInfoHedgeCrawler/
├── config.py                  # 全局配置（URL、请求头、Webhook 等）
├── crawler.py                 # 爬虫主逻辑（搜索、下载、元数据保存）
├── util.py                    # 工具函数（日志、重试、文件名生成等）
├── extractors/
│   └── extractor.py           # PDF 文本提取与正则字段解析
├── notifiers/
│   └── notifier.py            # 企业微信推送 + CLI 工具
├── data/
│   ├── announcements_metadata.csv   # 已下载公告元数据
│   └── *.pdf                        # 下载的公告 PDF
├── logs/
│   └── crawler.log            # 运行日志
└── requirements.txt
```

## 安装

Python 3.10+ 推荐。

```bash
pip install -r requirements.txt
```

## 配置

在 `config.py` 中填入企业微信机器人 Webhook URL：

```python
WECOM_WEBHOOK_URL = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=你的key"
```

在企业微信群中添加机器人的方式：群设置 → 添加机器人 → 复制 Webhook 地址。

## 使用

### 爬取公告

```bash
# 爬取全部（从第1页开始）
python crawler.py

# 按日期范围爬取
python crawler.py --start-date 2025-01-01 --end-date 2025-12-31

# 限制页数
python crawler.py --start-date 2025-01-01 --end-date 2025-12-31 --max-pages 10

# 自定义关键词
python crawler.py --keyword 套期保值 --start-date 2025-01-01 --end-date 2025-12-31
```

爬取过程中会自动提取 PDF 内容并推送企业微信（需配置 Webhook）。

### 推送工具（独立使用）

```bash
# 按公告 ID 预览（自动从 CSV 查元数据）
python -m notifiers.notifier --id 1225015373

# 按文件路径预览
python -m notifiers.notifier "data/关于开展外汇套期保值业务的公告_1225015373.pdf"

# 确认无误后推送
python -m notifiers.notifier --id 1225015373 --send

# 批量预览 CSV 中所有记录
python -m notifiers.notifier --batch

# 批量推送
python -m notifiers.notifier --batch --send

# 临时指定 Webhook（不修改 config.py）
python -m notifiers.notifier --id 1225015373 --send --webhook "https://qyapi.weixin.qq.com/..."
```

### 推送卡片示例

```
📋 套期保值公告
公司：福立旺（688678）
标题：关于开展外汇套期保值业务的公告
公告日期：2026-03-17
套保品种：外汇、美元
套保额度：5,000.00万美元或等值外币
有效期：12个月
套保目的：防范汇率风险
授权机构：董事会
[查看原文](https://www.cninfo.com.cn/...)
```

## 字段提取说明

提取逻辑在 `extractors/extractor.py`，采用"文本标准化 + 宽松正则"策略：

1. PDF 文本提取后去除所有空白字符，消除排版噪声
2. 正则按"触发词 → 任意短距离字符 → 目标值"匹配，不依赖固定格式
3. 额度按数值去重，同一金额多次出现只保留第一条
4. 标题含"管理制度"的文件自动跳过推送

各公司公告格式不同，如遇字段提取不准，先用预览命令确认，再调整 `extractor.py` 中对应的正则规则。

目前支持的套保品种关键词：外汇、美元、欧元、港元、日元、英镑、铜、铝、锌、镍、铅、锡、黄金、白银、原油、天然气、橡胶、大豆、玉米、小麦、棉花、铁矿石、螺纹钢、热轧卷板、PTA、甲醇、乙二醇、聚乙烯、聚丙烯、碳酸锂、氢氧化锂。

## 依赖

| 包 | 用途 |
|---|---|
| curl_cffi | 模拟浏览器 TLS 指纹，防反爬 |
| pdfplumber | PDF 文本提取 |
| pandas | 元数据 CSV 读写 |
| loguru | 日志 |
| tqdm | 下载进度条 |
| beautifulsoup4 | HTML 解析 |
