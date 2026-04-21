# Astrbot 插件 - 巨潮资讯套期保值公告查询

<div align="center">

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Astrbot Plugin](https://img.shields.io/badge/Astrbot-Plugin-orange)](https://github.com/Soulter/Astrbot)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

**在 QQ/微信/Telegram 聊天机器人中一键查询 A 股套期保值公告**

</div>

---

## 📖 插件简介

此插件将 [CNInfoHedgeCrawler](../README.md) 的核心功能集成到 Astrbot 聊天机器人框架，让你能在 IM 聊天工具中直接：

- 💬 **命令查询**: 通过简单的聊天命令搜索套期保值公告
- 📱 **多平台支持**: 兼容 QQ、微信、Telegram 等 Astrbot 支持的_platform
- 📊 **结构化回复**: 返回公司名、股票代码、公告标题、日期、PDF 链接

## ✨ 功能特性

| 特性 | 说明 |
|------|------|
| 🎯 **简洁命令** | `/套保` 或 `/套保查询` 即可开始 |
| 🔍 **灵活搜索** | 支持关键词、日期范围、页数限制 |
| 📱 **多平台** | QQ/微信/Telegram 等，取决于 Astrbot 配置 |
| 🚀 **轻量设计** | 自动限制返回条数，避免消息过长 |

## 🚀 快速开始

### 前置要求

- 已部署的 [Astrbot](https://github.com/Soulter/Astrbot) 机器人
- Python 3.10+

### 安装方式

#### 方式 1：通过 Astrbot 插件商店（推荐）

1. 在 Astrbot 控制台进入 **插件管理**
2. 搜索 "巨潮资讯" 或 "cninfo"
3. 点击安装并启用

#### 方式 2：手动安装

```bash
# 1. 复制插件到 Astrbot 插件目录
cp -r astrbot_plugin /path/to/astrbot/plugins/astrbot_plugin_cninfo_hedge

# 2. 安装依赖
cd /path/to/astrbot/plugins/astrbot_plugin_cninfo_hedge
pip install -r requirements.txt

# 3. 重启 Astrbot
# 根据部署方式重启服务
```

### 配置（可选）

在 Astrbot 插件配置文件中可修改默认参数：

```json
{
  "plugins": {
    "astrbot_plugin_cninfo_hedge": {
      "keyword_default": "套期保值",
      "max_pages_default": 1
    }
  }
}
```

## 💬 使用命令

| 命令 | 说明 | 示例 |
|------|------|------|
| `/套保查询` | 搜索公告 | `/套保查询 外汇套保` |
| `/套保` | 快捷查询 | `/套保 2025-01-01 2025-12-31` |

### 命令参数详解

```
# 使用默认参数查询（关键词：套期保值）
/套保查询

# 指定关键词
/套保查询 外汇套保

# 指定日期范围（快捷命令）
/套保 2025-01-01 2025-12-31

# 完整参数：关键词 + 日期范围 + 页数
/套保查询 套期保值 2025-01-01 2025-12-31 5
```

> 💡 **参数解析规则**：
> - 日期格式：YYYY-MM-DD
> - 纯数字视为页数限制
> - 其他文本视为关键词

## 📱 回复示例

### 成功查询

```
找到 15 条套期保值公告：

1. 福立旺（688678）
   标题：关于开展外汇套期保值业务的公告
   日期：2025-03-17
   链接：https://static.cninfo.com.cn/...

2. 某某公司（000001）
   标题：关于开展商品期货套期保值业务的公告
   日期：2025-02-28
   链接：https://static.cninfo.com.cn/...

... 还有 10 条，请缩小日期范围
```

### 无结果

```
未找到符合条件的套期保值公告，请尝试其他关键词或日期范围
```

### 错误处理

```
查询失败：网络连接超时，请稍后重试
```

## 📁 项目结构

```
astrbot_plugin/
├── __init__.py          # 插件入口（导出 CNInfoHedgePlugin）
├── main.py              # 插件主逻辑（命令注册 + 爬虫实现）
├── plugin.json          # 插件元数据（名称、版本、描述）
├── requirements.txt     # Python 依赖
└── README.md            # 本文件
```

### plugin.json 说明

实际 `plugin.json` 配置：

```json
{
  "name": "cninfo_hedge",
  "version": "1.1.0",
  "description": "巨潮资讯公告查询插件，支持任意关键词搜索和日期范围过滤",
  "author": "Stanley Wang",
  "repo": "https://github.com/Interstellar1217/CNInfoHedgeCrawler",
  "category": "实用工具",
  "keyword": ["套期保值", "公告查询", "巨潮资讯", "业绩预告", "金融"]
}
```

## 🔧 开发说明

### 命令注册

插件在 `main.py` 的 `register()` 方法中注册命令：

```python
context.register_command(
    cmd="套保查询",
    func=self.search_handler,
    description="搜索套期保值公告",
    priority=5
)
```

### 添加新命令

1. 在 `register()` 中添加新命令注册
2. 实现新的 `xxx_handler()` 异步方法
3. 使用 `event.set_result()` 返回消息

### 消息格式

插件使用 Astrbot 的 `MessageChain` 构建回复：

```python
from astrbot.api.event import AstrMessageEvent, MessageChain
from astrbot.api import logger

await event.set_result(MessageChain().message_plain("回复内容"))
```

### 日志输出

```python
from astrbot.api import logger

logger.info("查询请求参数：xxx")
logger.error("查询出错：xxx")
```

## 📝 依赖说明

| 包 | 版本 | 用途 |
|----|------|------|
| curl_cffi | >=0.7.0 | TLS 指纹模拟，防反爬 |

> 💡 插件复用主项目的 `config.py` 配置，确保在同一 Python 环境中安装依赖。

## ❓ 常见问题

### Q: 插件安装后不显示命令？
A: 检查 Astrbot 日志确认插件是否成功加载，重启 Astrbot 后重试。

### Q: 查询结果为空？
A: 检查日期范围是否正确，或尝试扩大 `max_pages` 值。

### Q: 如何在其他平台（如 Discord）使用？
A: Astrbot 支持多平台适配，只要配置对应平台 adapter 即可。参考 [Astrbot 文档](https://astrbot.app)。

### Q: 自定义返回消息格式？
A: 编辑 `main.py` 中 `search_handler()` 的 `reply` 字符串构建逻辑。

## 🔗 相关链接

- [Astrbot 官方文档](https://astrbot.app)
- [Astrbot GitHub](https://github.com/Soulter/Astrbot)
- [主项目 CNInfoHedgeCrawler](../README.md)
- [Dify 插件版本](../dify_plugin/README.md)

## 📄 许可证

MIT License - 详见 [主项目 LICENSE](../LICENSE)

---

<div align="center">

**让金融数据查询融入日常聊天，从此信息触手可及！**

[返回主项目](../README.md) • [问题反馈](https://github.com/Interstellar1217/CNInfoHedgeCrawler/issues)

</div>
