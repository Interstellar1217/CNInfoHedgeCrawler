# CNInfoHedgeCrawler - 测试检查清单

## ✅ 自动化测试

### 运行所有测试
```bash
# 运行单元测试
pytest tests/ -v

# 运行冒烟测试
python smoke_test.py
```

---

## 📋 手动测试清单

### 1. 基础爬取功能

```bash
# 测试默认关键词（套期保值）
python crawler.py --max-pages 3

# 测试自定义关键词
python crawler.py --keyword 业绩 --max-pages 3

# 测试日期范围
python crawler.py --start-date 2025-01-01 --end-date 2025-01-31 --max-pages 3
```

**检查点**：
- [ ] 能正常连接到巨潮资讯网
- [ ] 能获取公告列表
- [ ] PDF 能正常下载
- [ ] 数据库能正常写入
- [ ] CSV 能正常导出

---

### 2. 过滤功能测试

**准备**：确保 data 目录有一些 PDF 文件

```bash
# 预览单个 PDF 的提取结果
python -m notifiers.notifier "data/xxx.pdf"
```

**过滤规则验证**：

| 标题关键词 | 预期结果 | 实际结果 |
|------------|----------|----------|
| 关于开展外汇套期保值业务的公告 | ✅ 保留 | [ ] |
| 套期保值管理制度 | ❌ 过滤（标题过滤） | [ ] |
| 关于开展套期保值业务的可行性报告 | ❌ 过滤（内容无关） | [ ] |
| 独立董事关于套期保值的独立意见 | ❌ 过滤（标题过滤） | [ ] |
| 套期保值业务管理制度（修订版） | ✅ 保留（含"修订"） | [ ] |

**检查点**：
- [ ] 含"管理制度"的文件被过滤
- [ ] 含"独立董事意见"的文件被过滤
- [ ] 含"法律意见"的文件被过滤
- [ ] 含"修订"的文件被保留
- [ ] 未提取到品种/额度的文件被过滤

---

### 3. 推送功能测试

```bash
# 预览推送内容（不实际推送）
python -m notifiers.notifier --id 1225015373

# 实际推送（需配置 Webhook）
python -m notifiers.notifier --id 1225015373 --send

# 批量推送
python -m notifiers.notifier --batch --send
```

**检查点**：
- [ ] Markdown 格式正确
- [ ] 字段提取完整
- [ ] 原文链接正确
- [ ] 被过滤的公告不推送

---

### 4. 数据库测试

```bash
# 查看数据库统计
python -c "
from db.repository import get_connection, get_stats
conn = get_connection()
stats = get_stats(conn)
print(stats)
conn.close()
"
```

**检查点**：
- [ ] 数据库表结构正确
- [ ] 公告记录能正常插入
- [ ] 去重逻辑生效
- [ ] 推送状态能正确更新

---

### 5. 边界条件测试

**日期范围**：
- [ ] 同一天日期范围
- [ ] 跨年日期范围
- [ ] 空日期范围

**页码**：
- [ ] 第 1 页
- [ ] 空页（无数据）
- [ ] 连续空页（应停止爬取）

**网络异常**：
- [ ] 超时处理
- [ ] 空响应处理
- [ ] 重试机制生效

---

### 6. 插件测试

#### Dify 插件
```bash
cd dify_plugin
python cninfo_hedge/main.py
```

**检查点**：
- [ ] 工具能正常调用
- [ ] 返回 JSON 格式正确

#### AstrBot 插件
```bash
# 在 AstrBot 中加载插件后测试命令
/套保查询
/套保查询 外汇 2025-01-01
```

**检查点**：
- [ ] 命令能正常响应
- [ ] 返回格式正确

---

## 🔧 调试技巧

### 查看被过滤的公告
```bash
python -c "
from db.repository import get_connection, get_filtered_announcements
conn = get_connection()
filtered = get_filtered_announcements(conn, limit=20)
for row in filtered:
    print(f\"{row['title'][:50]}... ({row['filter_reason']})\")
conn.close()
"
```

### 临时绕过过滤器
```bash
# 修改 crawler.py 中的过滤检查，临时测试所有公告
# 或使用 --bypass-filter 参数（如果已实现）
```

### 查看数据库内容
```bash
# 使用 SQLite 客户端
sqlite3 data/announcements.db

# 查询被过滤的公告
SELECT title, filter_reason FROM announcement WHERE is_irrelevant = 1 LIMIT 10;
```

---

## 📝 测试报告模板

```markdown
测试日期：2025-XX-XX
测试人员：XXX
测试版本：X.X.X

### 通过率
- 单元测试：X/Y (Z%)
- 冒烟测试：X/Y (Z%)
- 手动测试：X/Y (Z%)

### 已知问题
1. [问题描述]
2. [问题描述]

### 备注
[其他说明]
```

---

## ✅ 发布前检查

- [ ] 所有单元测试通过
- [ ] 冒烟测试通过
- [ ] 过滤功能验证通过
- [ ] 推送功能验证通过
- [ ] 数据库迁移逻辑验证通过（如有）
- [ ] 文档已更新
