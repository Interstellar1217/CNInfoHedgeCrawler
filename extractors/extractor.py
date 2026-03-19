#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PDF 数据提取模块

从套期保值公告 PDF 中提取对期货公司展业有用的结构化信息。

设计原则：
  1. 文本标准化优先：提取后先去除所有空白字符，让正则只面对紧凑文本
  2. 正则宽松匹配：触发词 → 任意短距离字符 → 目标值，不依赖固定格式
  3. 去重 + 过滤零值：同一字段多次出现时去重，过滤明显噪声
  4. 字段缺失时返回空字符串，不影响推送流程
"""

import re
from pathlib import Path

import pdfplumber
from loguru import logger


# ── 文本标准化 ────────────────────────────────────────────────────────────────

def _normalize(text: str) -> str:
    """
    去除所有空白字符（空格、全角空格、制表符、换行），
    让正则只面对紧凑的中文文本，彻底消除 PDF 排版噪声。
    保留原始文本用于调试，标准化文本用于匹配。
    """
    return re.sub(r'[\s\u3000]+', '', text)


# ── 正则规则（均作用于标准化后的紧凑文本）────────────────────────────────────

# 套保品种关键词列表（可按需扩展）
_VARIETY_KEYWORDS = (
    '外汇|美元|欧元|港元|港币|日元|英镑|人民币|'
    '铜|铝|锌|镍|铅|锡|黄金|白银|原油|天然气|橡胶|'
    '大豆|玉米|小麦|棉花|铁矿石|螺纹钢|热轧卷板|'
    'PTA|甲醇|乙二醇|聚乙烯|聚丙烯|碳酸锂|氢氧化锂|锂'
)

_RE_VARIETY = re.compile(
    r'(?:套期保值|套保|对冲).{0,30}?(' + _VARIETY_KEYWORDS + r')',
    re.IGNORECASE,
)

# 额度：触发词 → 0~15字 → 数字（含千分位逗号）→ 单位
# 单位：万/亿（可选）+ 货币词
_CURRENCY = r'(?:亿|万)?(?:美元|欧元|港元|港币|日元|英镑|人民币|元|USD|EUR|HKD|JPY|GBP|CNY)'
_RE_QUOTA = re.compile(
    r'(?:不超过|上限|额度|合约价值|保证金|权利金).{0,15}?'
    r'((?<!\d)\d[\d,.]*' + _CURRENCY + r'(?:或等值[^（(，。；]{0,8})?)',
    re.IGNORECASE,
)

# 有效期：触发词 → 绝对区间 或 相对 N个月/N年
_RE_PERIOD = re.compile(
    r'(?:有效期|授权期限|期限|额度有效期).{0,20}?'
    r'('
    r'\d{4}年\d{1,2}月\d{1,2}日(?:至|到|—|-)\d{4}年\d{1,2}月\d{1,2}日'   # 绝对区间
    r'|\d{4}[-/]\d{1,2}[-/]\d{1,2}(?:至|到|—|-)\d{4}[-/]\d{1,2}[-/]\d{1,2}'
    r'|(?<!\d)\d{1,3}(?!\d)个月'                                            # N个月
    r'|(?<!\d)\d{1,2}(?!\d)年'                                              # N年
    r')',
    re.IGNORECASE,
)

# 套保目的（宽松匹配，取"规避/锁定/降低/对冲…风险/成本"短语）
_RE_PURPOSE = re.compile(
    r'(?:目的|为了|旨在|以).{0,5}?'
    r'(规避.{0,20}?风险|锁定.{0,20}?成本|降低.{0,20}?风险|对冲.{0,20}?风险|防范.{0,20}?风险)',
    re.IGNORECASE,
)

# 证券简称（第一页页眉行）
_RE_SEC_SHORT = re.compile(r'证券简称[：:]\s*([^\s　]+)')

# 公司全称（"XX股份有限公司" / "XX有限公司"）
_RE_SEC_FULL = re.compile(r'([\u4e00-\u9fff（()）\w]{4,30}(?:股份有限公司|有限公司|集团股份有限公司))')

# 授权机构
_RE_AUTHORITY = re.compile(r'(董事会|股东大会|股东会)(?:授权|批准|同意|审议通过)')


# ── 辅助函数 ──────────────────────────────────────────────────────────────────

def _first_match(pattern: re.Pattern, text: str) -> str:
    m = pattern.search(text)
    return m.group(1).strip() if m else ""


def _all_matches(pattern: re.Pattern, text: str) -> list[str]:
    """去重：相同数值的额度只保留第一次出现，过滤数字部分为零的噪声"""
    seen_vals: dict[str, bool] = {}   # 按规范化数值去重（如 "5000" 只保留一条）
    result: list[str] = []
    for m in pattern.finditer(text):
        val = m.group(1).strip()
        digits = re.sub(r'[^\d]', '', val[:10])
        if digits and int(digits) == 0:
            continue
        # 用数字部分作为去重 key，避免同一金额因后缀文字不同而重复
        key = digits if digits else val
        if key not in seen_vals:
            seen_vals[key] = True
            result.append(val)
    return result


# ── 主提取函数 ────────────────────────────────────────────────────────────────

def extract_text_from_pdf(pdf_path: Path) -> str:
    """提取 PDF 全文原始文本"""
    try:
        parts = []
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                t = page.extract_text()
                if t:
                    parts.append(t)
        return "\n".join(parts)
    except Exception as e:
        logger.warning(f"PDF文本提取失败 [{pdf_path.name}]: {e}")
        return ""


def extract_hedge_info(pdf_path: Path, announcement: dict) -> dict:
    """
    从 PDF 提取套期保值关键信息。

    Args:
        pdf_path: PDF 文件路径
        announcement: 爬虫元数据（announcementId / secCode / secName / title / publishTime）

    Returns:
        结构化字典，字段缺失时为空字符串；
        is_policy=True 表示这是管理制度类文件，建议跳过推送。
    """
    raw_text = extract_text_from_pdf(pdf_path)
    text = _normalize(raw_text)  # 去除所有空白，正则只面对紧凑文本

    # 公司名：优先用爬虫传入的 secName，CLI 模式下从 PDF 原始文本提取
    sec_name = announcement.get("secName", "")
    sec_code = announcement.get("secCode", "")
    if not sec_name:
        # 先尝试页眉的"证券简称：XX"
        m = _RE_SEC_SHORT.search(raw_text)
        if m:
            sec_name = m.group(1).strip()
        else:
            # 再尝试第一个出现的公司全称
            m = _RE_SEC_FULL.search(raw_text)
            if m:
                sec_name = m.group(1).strip()
    if not sec_code:
        m = re.search(r'证券代码[：:]\s*(\d+)', raw_text)
        if m:
            sec_code = m.group(1).strip()

    # 公告类型判断：标题含"管理制度"才视为制度文件，跳过推送
    title = announcement.get("title", "") or pdf_path.stem
    is_policy = bool(re.search(r'管理制度|内部控制制度|风险管理制度', title))

    varieties = _all_matches(_RE_VARIETY, text)
    quotas = _all_matches(_RE_QUOTA, text)

    result = {
        "announcement_id": announcement.get("announcementId", ""),
        "sec_code": sec_code,
        "sec_name": sec_name,
        "title": announcement.get("title", ""),
        "publish_date": _format_date(announcement.get("publishTime", "")),
        "varieties": "、".join(varieties) if varieties else "",
        "quota": "；".join(quotas) if quotas else "",
        "period": _first_match(_RE_PERIOD, text),
        "purpose": _first_match(_RE_PURPOSE, text),
        "authority": _first_match(_RE_AUTHORITY, text),
        "is_policy": is_policy,
        "pdf_path": str(pdf_path),
    }

    logger.debug(
        f"提取完成 [{announcement.get('secCode', pdf_path.stem)}] "
        f"品种={result['varieties']} 额度={result['quota']} "
        f"有效期={result['period']} 管理制度={is_policy}"
    )
    return result


def _format_date(publish_time) -> str:
    """毫秒时间戳或字符串 → YYYY-MM-DD"""
    if not publish_time:
        return ""
    try:
        from datetime import datetime, timezone
        return datetime.fromtimestamp(int(publish_time) / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return str(publish_time)[:10]
