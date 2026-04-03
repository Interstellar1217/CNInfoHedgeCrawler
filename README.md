# CNInfoHedgeCrawler

从 [巨潮资讯网](https://www.cninfo.com.cn) 自动爬取"套期保值"相关公告 PDF，提取关键信息，并通过企业微信机器人推送。

## 功能

- 按关键词 + 日期范围搜索公告
- 使用 `curl_cffi` 模拟浏览器 TLS/JA3 指纹
- 断点续爬，避免重复下载
- 从 PDF 提取套保品种、额度、有效期、目的、授权机构
- 企业微信机器人推送 Markdown 卡片
- RESTful API + Astrbot/Dify 插件支持

## 项目结构

```
CNInfoHedgeCrawler/
├── config.py           # 配置（URL、Webhook 等）
├── main.py             # FastAPI 应用入口
├── crawler.py          # CLI 入口（向后兼容）
├── util.py             # 工具（日志、重试、文件名）
├── core/
│   ├── crawler.py      # 爬虫服务
│   ├── extractor.py    # PDF 提取
│   └── notifier.py     # 企业微信推送
├── api/
│   └── routes.py       # API 路由
├── extractors/         # 兼容层
├── notifiers/          # 兼容层
├── plugins/
│   ├── astrbot/        # Astrbot 插件
│   └── dify/           # Dify 插件
├── data/               # 下载的 PDF 和元数据
└── logs/               # 日志
```

## 安装

```bash
pip install -r requirements.txt
```

## 配置

在 `config.py` 中设置企业微信 Webhook：

```python
WECOM_WEBHOOK_URL = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=你的 key"
```

## 使用

### CLI

```bash
# 爬取公告
python crawler.py

# 按日期范围爬取
python crawler.py --start-date 2025-01-01 --end-date 2025-12-31

# 限制页数
python crawler.py --start-date 2025-01-01 --end-date 2025-12-31 --max-pages 10

# 预览推送
python -m notifiers.notifier --id 1225015373

# 推送
python -m notifiers.notifier --id 1225015373 --send
```

### API

```bash
# 启动服务
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

API 端点：

| 方法 | 端点 | 功能 |
|------|------|------|
| GET | `/search?keyword=套期保值` | 搜索公告 |
| POST | `/crawl` | 爬取公告 |
| GET | `/announcements` | 获取已下载列表 |
| GET | `/announcements/{id}/extract` | 提取 PDF 内容 |
| POST | `/announcements/{id}/notify` | 推送到企业微信 |

### 插件

**Astrbot**: 复制 `plugins/astrbot/` 到 Astrbot 插件目录

**Dify**: 在 Dify 中导入 `plugins/dify/manifest.json`

## 字段提取

支持的套保品种关键词：外汇、美元、欧元、港元、铜、铝、锌、黄金、白银、原油、大豆、玉米、PTA、碳酸锂等。

提取逻辑在 `core/extractor.py`，采用"文本标准化 + 宽松正则"策略。

## License

MIT
