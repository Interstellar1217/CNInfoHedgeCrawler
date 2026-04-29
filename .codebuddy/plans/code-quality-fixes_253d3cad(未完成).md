---
name: code-quality-fixes
overview: 修复代码中的 bug、性能问题和代码质量问题，包括：download_pdf 返回值问题、缺少 org_id 字段、冗余网络请求、文档不一致、函数内导入等
todos:
  - id: fix-main-py-syntax
    content: 修复 main.py 语法错误：字典逗号、函数调用逗号
    status: in_progress
  - id: fix-crawler-py-syntax
    content: 修复 crawler.py 语法错误：引号、shutdown拼写、drop_duplicates参数
    status: pending
    dependencies:
      - fix-main-py-syntax
  - id: fix-config-py-syntax
    content: 修复 config.py 语法错误：is not None 拼写错误
    status: pending
    dependencies:
      - fix-main-py-syntax
  - id: fix-util-py-syntax
    content: 修复 util.py 语法错误：文件操作引号、retry_on_failure装饰器
    status: pending
    dependencies:
      - fix-main-py-syntax
  - id: fix-extractor-py-syntax
    content: 修复 extractors/extractor.py 语法错误：正则表达式引号、变量名拼写
    status: pending
    dependencies:
      - fix-main-py-syntax
  - id: fix-notifier-py-syntax
    content: 修复 notifiers/notifier.py 语法错误：函数调用逗号、字典键名
    status: pending
    dependencies:
      - fix-main-py-syntax
  - id: fix-download-pdf-logic
    content: 修复 crawler.py download_pdf：非PDF内容时返回False
    status: pending
    dependencies:
      - fix-crawler-py-syntax
  - id: add-org-id-field
    content: 在 extract_hedge_info 返回字典中添加 org_id 字段
    status: pending
    dependencies:
      - fix-extractor-py-syntax
  - id: optimize-crawl-all
    content: 优化 crawl_all 统计逻辑：通过改进 crawl_page 返回值避免重复请求
    status: pending
    dependencies:
      - fix-crawler-py-syntax
  - id: fix-documentation
    content: 修正 util.py 文档字符串和 extractor.py 导入位置
    status: pending
    dependencies:
      - fix-util-py-syntax
  - id: verify-all-fixes
    content: 验证所有修复：语法检查、模块导入测试
    status: pending
    dependencies:
      - fix-download-pdf-logic
      - add-org-id-field
      - optimize-crawl-all
      - fix-documentation
---

## 需求概述

修复 CNInfoHedgeCrawler 项目中的代码问题，确保程序能够正常运行。

## 问题分类

### 严重语法错误（导致程序无法运行）

1. main.py 字典定义缺少逗号（第96、99行）
2. 多处使用单引号代替双引号（Python 语法错误）
3. config.py 中 `is not None` 拼写错误（写成 `is not None`）
4. crawler.py 中 `shutdown()` 方法名拼写错误（写成 `shutdown`）
5. notifier.py 函数调用参数缺少逗号

### 逻辑错误

1. crawler.py download_pdf：非PDF内容时返回True导致调用方错误
2. extractor.py extract_hedge_info 返回字典缺少 org_id 字段
3. notifier.py _build_markdown 期望 org_id 但获取不到

### 性能问题

1. crawler.py crawl_all 中为统计而重复发起网络请求

### 代码质量

1. util.py 文档字符串与实际导入不符
2. extractor.py _format_date 内部导入应移到顶部

## 技术栈

- Python 3.10+ 语法修正
- 保持现有依赖（curl_cffi, pdfplumber, pandas, loguru）

## 实现方法

### 1. 语法错误修复策略

- **引号修正**：Python 中虽然单双引号都合法，但项目统一使用双引号，需修正混入的单引号
- **逗号补全**：字典/参数列表中缺少的逗号
- **拼写修正**：`is not None` → `is not None`，`shutdown` → `shutdown`

### 2. 逻辑错误修复

- **download_pdf 返回值**：当内容类型不是 PDF 时，保存为 .html 后应返回 False
- **org_id 字段传递**：在 extract_hedge_info 返回字典中添加 org_id 字段

### 3. 性能优化

- **统计逻辑改进**：修改 crawl_page 方法返回本页公告总数，避免在 crawl_all 中重复请求

### 4. 代码质量提升

- **文档修正**：更新 util.py 文档字符串
- **导入优化**：将 _format_date 中的局部导入移到文件顶部

## 实施细节

### 修复顺序

1. 先修复所有语法错误（使程序能运行）
2. 修复逻辑错误（使功能正常）
3. 优化性能
4. 提升代码质量

### 测试验证

- 每个文件修复后进行语法检查
- 确保模块可正常导入