# Dify 插件 - 巨潮资讯套期保值公告

<div align="center">

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Dify Plugin](https://img.shields.io/badge/Dify-Plugin-blue)](https://dify.ai)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

**为 AI Agent 添加一键查询 A 股套期保值公告的能力**

</div>

---

## 📖 插件简介

此插件将 [CNInfoHedgeCrawler](../README.md) 的核心功能封装为 Dify 工具，让你的 AI Agent 能够：

- 🔍 **智能搜索**: 按关键词、日期范围搜索上市公司套期保值公告
- 📊 **结构化返回**: 返回公告 ID、股票代码、公司名、标题、日期、PDF 链接
- 🤖 **AI 调用**: AI 可自动理解用户意图并调用工具获取最新公告信息

## ✨ 功能特性

| 特性 | 说明 |
|------|------|
| 🎯 **精准搜索** | 支持关键词 + 日期范围 + 页数限制 |
| 📦 **开箱即用** | 复用主项目配置，无需额外设置 |
| 🔒 **稳定可靠** | 使用 TLS 指纹模拟，规避反爬检测 |
| 📝 **详细日志** | 内置 loguru 日志，方便调试 |

## 🚀 快速开始

### 前置要求

- Python 3.10+
- 已部署的 Dify 实例（自托管或云服务）

### 安装方式

#### 方式 1：自托管 Dify（推荐）

```bash
# 1. 进入 Dify API 插件目录
cd /path/to/dify/api/core/plugins/

# 2. 复制插件
cp -r /path/to/CNInfoHedgeCrawler/dify_plugin/cninfo_hedge .

# 3. 重启 Dify 服务
docker restart dify-api

# 4. 在 Dify 控制台 → 工具 → 内置工具 中启用
```

#### 方式 2：Dify 云服务

1. 登录 [Dify 云平台](https://cloud.dify.ai)
2. 进入 **工具** → **创建自定义工具**
3. 上传插件包或填写 OpenAPI Schema
4. 配置凭证（如需要）
5. 在 Agent 配置中添加此工具

#### 方式 3：本地测试

```bash
cd dify_plugin
pip install -r requirements.txt
python cninfo_hedge/main.py
```

测试成功会输出 JSON 格式的公告列表。

## 🛠️ 工具参数

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| keyword | string | 否 | 套期保值 | 搜索关键词 |
| start_date | string | 否 | - | 开始日期 (YYYY-MM-DD) |
| end_date | string | 否 | - | 结束日期 (YYYY-MM-DD) |
| max_pages | number | 否 | 1 | 最大爬取页数 |

## 📤 返回数据格式

```json
{
  "success": true,
  "total": 15,
  "announcements": [
    {
      "announcementId": "1225015373",
      "secCode": "688678",
      "secName": "福立旺",
      "title": "关于开展外汇套期保值业务的公告",
      "publishTime": "1741824000000",
      "adjunctUrl": "/finalpage/2025-03-17/1225015373.PDF",
      "pdfUrl": "https://static.cninfo.com.cn/finalpage/2025-03-17/1225015373.PDF"
    }
  ],
  "error": null
}
```

## 💡 在 Dify Agent 中使用

### 场景 1：查询特定公司公告

**用户提问**：
> 帮我找一下 2025 年福立旺公司的套期保值公告

**AI 自动调用工具**：
```json
{
  "tool": "cninfo_hedge",
  "parameters": {
    "keyword": "套期保值",
    "start_date": "2025-01-01",
    "end_date": "2025-12-31",
    "max_pages": 3
  }
}
```

**AI 回复**：
> 找到以下公告：
> 1. 福立旺（688678）- 关于开展外汇套期保值业务的公告 - 2025-03-17
>    PDF 链接：https://static.cninfo.com.cn/...

### 场景 2：汇总多家公司公告

**用户提问**：
> 最近有哪些上市公司发布了外汇套期保值公告？

**AI 自动调用工具**：
```json
{
  "tool": "cninfo_hedge",
  "parameters": {
    "keyword": "外汇套期保值",
    "max_pages": 5
  }
}
```

## 📁 项目结构

```
dify_plugin/cninfo_hedge/
├── __init__.py                 # 插件入口
├── main.py                     # 工具实现（CNInfoHedgeTool 类）
├── provider.yaml               # 提供商配置
├── tools.yaml                  # 工具列表定义
├── tools/
│   ├── search_announcements.yaml  # 工具定义和输入输出 Schema
│   └── ...
├── icon.svg                    # 插件图标
└── requirements.txt            # Python 依赖
```

## 🔧 开发说明

### 添加工具参数

编辑 `tools/search_announcements.yaml`：

```yaml
parameters:
  - name: new_parameter
    type: string
    required: false
    default: "default_value"
    description: "参数说明"
```

然后在 `main.py` 的 `invoke()` 函数中处理新参数。

### 调试日志

插件内置 loguru 日志，运行时会输出到 `dify_plugin.log`：

```bash
tail -f dify_plugin.log
```

### 本地测试

```python
from cninfo_hedge.main import invoke

result = invoke(
    tool_name="search_announcements",
    credentials={},
    tool_parameters={
        "keyword": "套期保值",
        "start_date": "2025-01-01",
        "end_date": "2025-12-31",
        "max_pages": 1
    }
)

print(result)
```

## 📝 依赖说明

| 包 | 版本 | 用途 |
|----|------|------|
| curl_cffi | >=0.7.0 | TLS 指纹模拟，防反爬 |
| loguru | >=0.7.2 | 日志记录 |

> 💡 插件复用主项目的 `config.py` 配置，确保在同一 Python 环境中安装依赖。

## ❓ 常见问题

### Q: 工具调用后返回空结果？
A: 检查日期范围是否正确，或尝试扩大 `max_pages` 值。

### Q: 如何更新插件配置？
A: 修改主项目 `config.py` 后重启 Dify 服务。

### Q: 支持哪些 Dify 版本？
A: Dify 0.6.0+，建议使用最新版本。

## 📄 许可证

MIT License - 详见 [主项目 LICENSE](../LICENSE)

---

<div align="center">

**让 AI 获取实时金融公告数据，从此决策更智能！**

[返回主项目](../README.md) • [问题反馈](https://github.com/你的用户名/CNInfoHedgeCrawler/issues)

</div>
