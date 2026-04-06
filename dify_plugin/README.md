# Dify 插件 - 巨潮资讯套期保值公告

从 [巨潮资讯网](https://www.cninfo.com.cn) 搜索并获取套期保值相关公告的 Dify 工具。

## 功能

- 按关键词搜索公告（默认"套期保值"）
- 支持日期范围过滤
- 可限制爬取页数
- 返回结构化数据：公告 ID、股票代码、公司名、标题、日期、PDF 链接

## 安装部署

### Dify 插件类型说明

Dify 支持两种插件形式：

1. **Dify 原生插件（推荐）**：使用 YAML 定义工具，Python 实现逻辑
2. **Dify API 工具**：通过 OpenAPI/Swagger 定义

本插件采用原生插件形式。

### 方式 1：部署到 Dify（自托管）

如果你使用 Docker 自托管 Dify：

1. 将 `cninfo_hedge` 目录复制到 Dify 插件目录：
   ```bash
   cp -r cninfo_hedge /path/to/dify/api/core/plugins/
   ```

2. 重启 Dify 服务

3. 在 Dify 控制台 → 工具 → 内置工具中启用

4. 在 Agent 配置中添加此工具

### 方式 2：使用 Dify 云服务

1. 登录 [Dify 云平台](https://cloud.dify.ai)
2. 进入工具页面，点击"创建自定义工具"
3. 上传插件包或填写 OpenAPI Schema
4. 配置凭证（如需要）
5. 在 Agent 中添加工具

### 方式 3：本地测试

```bash
cd dify_plugin
pip install -r requirements.txt
python cninfo_hedge/main.py
```

测试成功会输出 JSON 格式的公告列表。

## 工具参数

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| keyword | string | 否 | 套期保值 | 搜索关键词 |
| start_date | string | 否 | - | 开始日期 YYYY-MM-DD |
| end_date | string | 否 | - | 结束日期 YYYY-MM-DD |
| max_pages | number | 否 | 1 | 最大爬取页数 |

## 返回数据

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
      "publishTime": "1710633600000",
      "pdfUrl": "https://static.cninfo.com.cn/..."
    }
  ]
}
```

## 在 Dify 中使用示例

**用户提问**：帮我找一下 2025 年福立旺公司的套期保值公告

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

**AI 回复**：找到以下公告：
1. 福立旺（688678）- 关于开展外汇套期保值业务的公告 - 2025-03-17
2. ...

## 项目结构

```
dify_plugin/cninfo_hedge/
├── __init__.py                 # 插件入口
├── main.py                     # 工具实现
├── provider.yaml               # 提供商配置
├── tools.yaml                  # 工具列表
├── tools/
│   └── search_announcements.yaml  # 工具定义和 Schema
├── icon.svg                    # 插件图标
└── requirements.txt            # 依赖
```

## 依赖

- `curl_cffi`: 模拟浏览器 TLS 指纹，防反爬
- `loguru`: 日志记录

## License

MIT
