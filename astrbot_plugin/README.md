# Astrbot 插件 - 巨潮资讯套期保值公告查询

从 [巨潮资讯网](https://www.cninfo.com.cn) 查询套期保值相关公告的 Astrbot 聊天机器人插件。

## 功能

- 聊天命令查询套期保值公告
- 支持关键词搜索
- 支持日期范围过滤
- 返回结构化信息：公司名、股票代码、公告标题、日期、PDF 链接

## 安装

### 方式 1：通过 Astrbot 插件商店（推荐）

1. 在 Astrbot 控制台进入插件管理
2. 搜索 "巨潮资讯" 或 "cninfo"
3. 点击安装

### 方式 2：手动安装

1. 将 `astrbot_plugin` 目录重命名为 `astrbot_plugin_cninfo_hedge`
2. 复制到 Astrbot 插件目录：
   ```bash
   cp -r astrbot_plugin_cninfo_hedge /path/to/astrbot/plugins/
   ```
3. 安装依赖：
   ```bash
   pip install -r requirements.txt
   ```
4. 重启 Astrbot

## 使用命令

| 命令 | 说明 | 示例 |
|------|------|------|
| `/套保查询` | 搜索公告 | `/套保查询 外汇套保` |
| `/套保` | 快捷查询 | `/套保 2025-01-01 2025-12-31` |

### 命令用法详解

```
# 使用默认参数查询（关键词：套期保值，最近 1 页）
/套保查询

# 指定关键词
/套保查询 外汇套保

# 指定日期范围
/套保 2025-01-01 2025-12-31

# 完整参数：关键词 + 日期范围 + 页数
/套保查询 套期保值 2025-01-01 2025-12-31 5
```

## 回复示例

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

## 配置

在 Astrbot 插件配置中可修改：

```json
{
  "keyword_default": "套期保值",
  "max_pages_default": 1
}
```

## 依赖

- `curl_cffi`: 模拟浏览器 TLS 指纹，防反爬

## 项目结构

```
astrbot_plugin/
├── __init__.py      # 插件入口
├── main.py          # 插件主逻辑
├── plugin.json      # 插件元数据
└── README.md        # 说明文档
```

## 开发说明

本插件复用了主项目的 `config.py` 配置和爬虫逻辑，确保与原版行为一致。

## License

MIT
