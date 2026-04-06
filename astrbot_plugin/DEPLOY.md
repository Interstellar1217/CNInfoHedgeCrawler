# Astrbot 插件部署指南

<div align="center">

[![Astrbot Version](https://img.shields.io/badge/Astrbot-1.0+-orange.svg)](https://github.com/Soulter/Astrbot)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

</div>

## 部署到 Astrbot 的两种方式

---

## 方式一：通过插件商店安装（推荐）

### 前提条件

需要将插件发布到 Astrbot 插件商店，让用户可以直接安装。

### 发布步骤

**1. 准备插件包**

```bash
# 确保插件目录结构正确
astrbot_plugin/
├── __init__.py      # 导出 CNInfoHedgePlugin
├── main.py          # 插件主逻辑
├── plugin.json      # 插件元数据
├── requirements.txt # 依赖
└── README.md        # 说明文档
```

**2. 提交到 Astrbot 插件仓库**

- Fork [Astrbot 插件仓库](https://github.com/Soulter/Astrbot-Plugins)
- 将 `astrbot_plugin` 目录复制到 `plugins/` 下
- 修改 `plugin.json` 中的仓库地址为你的 GitHub 地址
- 提交 Pull Request

**3. 等待审核合并**

审核通过后，用户即可在 Astrbot 控制台搜索安装。

---

## 方式二：手动安装（即时使用）

### 步骤

**1. 复制插件到 Astrbot 目录**

```bash
# 找到 Astrbot 安装目录
# 复制插件并重命名
cp -r astrbot_plugin /path/to/astrbot/plugins/astrbot_plugin_cninfo_hedge
```

**2. 安装依赖**

```bash
cd /path/to/astrbot/plugins/astrbot_plugin_cninfo_hedge
pip install -r requirements.txt
```

**3. 重启 Astrbot**

```bash
# 根据部署方式重启
# Docker 部署
docker restart astrbot

# 直接运行
pkill -f astrbot
python -m astrbot
```

**4. 启用插件**

- 登录 Astrbot 控制台
- 进入 **插件管理**
- 找到 "巨潮资讯套期保值公告查询" 并启用

---

## 使用命令

安装完成后，在聊天工具中发送：

```
/套保查询                          # 使用默认参数查询
/套保查询 外汇套保                  # 指定关键词
/套保 2025-01-01 2025-12-31        # 按日期范围查询
/套保查询 套期保值 2025-01-01 5     # 完整参数
```

---

## 配置（可选）

在 Astrbot 控制台的插件配置中可修改：

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

---

## 本地测试

在部署前，可以先本地测试插件能否正常加载：

```bash
cd astrbot_plugin
python -c "from main import CNInfoHedgePlugin; print('插件加载成功')"
```

---

## 支持的平台

Astrbot 支持以下平台，配置对应 adapter 即可：

- QQ (go-cqhttp)
- 微信 (WeChat)
- Telegram
- Discord
- 钉钉
- 企业微信

详细配置参考 [Astrbot 官方文档](https://astrbot.app)

---

## 常见问题

### Q: 插件安装后命令不响应？
A: 检查 Astrbot 日志确认插件是否成功加载。

### Q: 查询结果为空？
A: 检查日期范围是否正确，或尝试扩大 `max_pages` 值。

### Q: 如何在多个平台使用？
A: Astrbot 支持多平台适配，在控制台配置对应 platform adapter 即可。

---

## 相关文档

- [Astrbot 官方文档](https://astrbot.app)
- [Astrbot 插件开发](https://astrbot.app/dev/plugin.html)
- [主项目 README](../README.md)
