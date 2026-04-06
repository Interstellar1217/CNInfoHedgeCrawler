# Dify 插件部署指南

<div align="center">

[![Dify Version](https://img.shields.io/badge/Dify-0.6.0+-blue.svg)](https://dify.ai)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

</div>

## 部署到 Dify 的两种方式

---

## 方式一：部署为 Dify 原生插件（推荐）

### 1. 自托管 Dify

如果你使用 Docker 自托管 Dify，可以将插件直接部署到 Dify 服务中。

#### 步骤

**1. 复制插件到 Dify 目录**

```bash
# 找到你的 Dify 安装目录
# 通常是 /path/to/dify/docker 或 /opt/dify

# 复制插件
cp -r dify_plugin/cninfo_hedge /path/to/dify/api/core/plugins/
```

**2. 重启 Dify API 服务**

```bash
cd /path/to/dify/docker
docker compose restart api
```

**3. 在 Dify 控制台启用插件**

- 登录 Dify
- 进入 **工具** → **内置工具**
- 找到 "巨潮资讯套期保值公告" 并启用

**4. 在 Agent 中添加工具**

- 创建或编辑一个 Agent
- 在工具列表中添加 "cninfo_hedge"
- 保存后即可使用

---

### 2. Dify 云服务

如果你使用 Dify 云服务 (cloud.dify.ai)，需要通过 API 方式部署。

#### 步骤

**1. 打包插件**

```bash
cd dify_plugin
zip -r cninfo_hedge.zip cninfo_hedge/
```

**2. 上传到 Dify**

- 登录 [Dify 云平台](https://cloud.dify.ai)
- 进入 **工具** → **创建自定义工具**
- 选择 **上传插件包**
- 上传 `cninfo_hedge.zip`

**3. 配置凭证（如需要）**

本插件不需要额外凭证，直接使用即可。

**4. 在 Agent 中使用**

- 创建 Agent 时选择添加此工具
- AI 即可自动调用查询公告

---

## 方式二：封装为 API 服务（备选）

如果你想让插件独立运行，可以封装成 HTTP API 服务。

### 1. 创建 FastAPI 服务

```python
# api_server.py
from fastapi import FastAPI
from cninfo_hedge.main import invoke

app = FastAPI()

@app.post("/api/search_announcements")
async def search(keyword: str = "套期保值", 
                 start_date: str = None,
                 end_date: str = None,
                 max_pages: int = 1):
    result = invoke(
        tool_name="search_announcements",
        credentials={},
        tool_parameters={
            "keyword": keyword,
            "start_date": start_date,
            "end_date": end_date,
            "max_pages": max_pages
        }
    )
    return result

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
```

### 2. 安装依赖并运行

```bash
pip install fastapi uvicorn curl_cffi loguru
python api_server.py
```

### 3. 在 Dify 中配置 HTTP 工具

- 进入 **工具** → **创建自定义工具** → **HTTP 请求**
- 填写 API 地址：`http://your-server:8000/api/search_announcements`
- 配置输入输出 Schema
- 保存后即可使用

---

## 本地测试

在部署前，可以先本地测试插件功能：

```bash
cd dify_plugin
pip install -r cninfo_hedge/requirements.txt
python cninfo_hedge/main.py
```

测试成功会输出 JSON 格式的公告列表。

---

## 常见问题

### Q: 插件加载后不显示？
A: 检查 Dify API 日志：`docker logs dify-api --tail 100`

### Q: 调用工具返回空结果？
A: 检查日期范围是否正确，或尝试扩大 `max_pages` 值。

### Q: 如何更新插件？
A: 修改代码后重启 Dify API 服务即可。

---

## 相关文档

- [Dify 插件开发文档](https://docs.dify.ai/plugins)
- [主项目 README](../README.md)
