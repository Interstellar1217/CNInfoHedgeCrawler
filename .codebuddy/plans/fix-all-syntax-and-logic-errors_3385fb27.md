---
name: fix-all-syntax-and-logic-errors
overview: 修复所有文件中的语法错误（f-string引号、导入语句、拼写错误）和逻辑错误（download_pdf返回值、缺少org_id字段）
todos:
  - id: fix-syntax-errors-main
    content: 修复 main.py 中的语法错误：f-string 引号、字典逗号、函数调用逗号
    status: completed
  - id: fix-syntax-errors-crawler
    content: 修复 crawler.py 中的语法错误：拼写错误（shutdown、impersonate）、优化 crawl_page 返回值
    status: completed
    dependencies:
      - fix-syntax-errors-main
  - id: fix-syntax-errors-config
    content: 修复 config.py 中的拼写错误（STATICP_URL、HEADERS、LOGS_DIR、METADATA_FILE、WECOM_WEBHOOK_URL）
    status: completed
    dependencies:
      - fix-syntax-errors-main
  - id: fix-syntax-errors-util
    content: 修复 util.py 中的拼写错误（parse_qs 等）、修正文档字符串
    status: completed
    dependencies:
      - fix-syntax-errors-main
  - id: fix-syntax-errors-extractor
    content: 修复 extractors/extractor.py 中的拼写错误、添加 org_id 字段、优化导入
    status: completed
    dependencies:
      - fix-syntax-errors-main
  - id: fix-syntax-errors-notifier
    content: 修复 notifiers/notifier.py 中的拼写错误、函数调用逗号
    status: completed
    dependencies:
      - fix-syntax-errors-main
  - id: fix-logic-errors
    content: 修复逻辑错误：download_pdf 返回值、org_id 字段传递
    status: completed
    dependencies:
      - fix-syntax-errors-crawler
      - fix-syntax-errors-extractor
      - fix-syntax-errors-notifier
  - id: optimize-performance
    content: 优化 crawl_all 统计逻辑：改进 crawl_page 返回值避免重复请求
    status: completed
    dependencies:
      - fix-logic-errors
  - id: verify-all-fixes
    content: 验证所有修复：语法检查、模块导入测试、功能测试
    status: completed
    dependencies:
      - optimize-performance
---

## 产品概述

对 CNInfoHedgeCrawler 项目进行全面的代码质量修复，确保程序能够正常运行并达到生产级别代码质量标准。

## 核心功能

1. **语法错误修复** - 修复所有文件中的语法错误，包括 f-string 引号问题、导入语句引号问题、字典/函数调用中缺少逗号、拼写错误等
2. **逻辑错误修复** - 修复 download_pdf 返回值逻辑、添加缺失的 org_id 字段传递
3. **性能优化** - 优化 crawl_all 统计逻辑，避免为统计而已下载公告数量重新发起网络请求
4. **代码质量提升** - 修正文档字符串、优化导入位置

## 技术栈选择

- **编程语言**: Python 3.10+
- **现有依赖**: curl_cffi, pdfplumber, pandas, loguru, tqdm, bs4
- **代码质量工具**: 使用 Python 内置语法检查，确保代码符合 PEP 8 标准

## 实现方案

### 1. 语法错误修复策略

- **引号修正**: 所有 f-string 和字符串必须使用英文双引号，项目统一使用双引号风格
- **逗号补全**: 字典、列表、函数调用参数列表中缺少的逗号必须补全
- **拼写修正**: 
- `shutdown` → `shutdown`
- `impersonate` → `impersonate`
- `is not None` → `is not None`
- `HEADERS` → `HEADERS`
- `LOGS_DIR` → `LOGS_DIR`
- `METADATA_FILE` → `METADATA_FILE`
- `WECOM_WEBHOOK_URL` → `WECOM_WEBHOOK_URL`
- `parse_qs` → `parse_qs`
- `quotas` → `quotas`

### 2. 逻辑错误修复

- **download_pdf 返回值修复**: 当内容类型不是 PDF 时，即使保存为 .html 文件，也应返回 False，避免调用方误认为下载成功
- **org_id 字段传递**: 在 `extract_hedge_info` 返回字典中添加 `org_id` 字段，从 announcement 参数中获取

### 3. 性能优化

- **统计逻辑改进**: 修改 `crawl_page` 方法，使其返回本页公告总数（包括已下载的），避免在 `crawl_all` 中重复请求网络

### 4. 代码质量提升

- **文档修正**: 更新 util.py 文档字符串，使其与实际导入一致
- **导入优化**: 将 `extractors/extractor.py` 中 `_format_date` 函数内的局部导入 `from datetime import datetime` 移到文件顶部

## 实施细节

### 修复顺序

1. 先修复所有语法错误（使程序能运行）
2. 修复逻辑错误（使功能正常）
3. 优化性能
4. 提升代码质量

### 文件修改清单

1. **main.py** - 修复 f-string 引号、字典逗号、函数调用逗号
2. **crawler.py** - 修复拼写错误、优化 crawl_page 返回值
3. **config.py** - 修复拼写错误（STATICP_URL → STATIC_URL、HEADERS、LOGS_DIR、METADATA_FILE、WECOM_WEBHOOK_URL）
4. **util.py** - 修复拼写错误（parse_qs、HEADERS 等）、修正文档字符串
5. **extractors/extractor.py** - 修复拼写错误、添加 org_id 字段、优化导入
6. **notifiers/notifier.py** - 修复拼写错误、函数调用逗号

## 架构设计

本修复计划不影响现有架构，仅对代码进行错误修复和质量提升。所有修改都保持向后兼容性。

## 目录结构

无需变更目录结构，仅修改现有文件。

## 关键代码结构

无需新增接口或类型定义，仅修正现有代码。

## Agent Extensions

### SubAgent

- **code-explorer**: 
- Purpose: 在修复代码前进行最终验证，确保所有语法错误已被识别
- Expected outcome: 生成完整的文件修改清单，确保无遗漏