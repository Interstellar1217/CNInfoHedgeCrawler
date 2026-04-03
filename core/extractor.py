#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""PDF 提取服务"""

import re
from pathlib import Path

import pdfplumber
from loguru import logger


def _normalize(text: str) -> str:
    """去除所有空白字符"""
    return re.sub(r'[\s\u3000]+', '', text)


_VARIETY_KEYWORDS = (
    '外汇 | 美元 | 欧元 | 港元 | 港币 | 日元 | 英镑 | 人民币|'
    '铜 | 铝 | 锌 | 镍 | 铅 | 锡 | 黄金 | 白银 | 原油 | 天然气 | 橡胶|'
    '大豆 | 玉米 | 小麦 | 棉花 | 铁矿石 | 螺纹钢 | 热轧卷板|'
    'PTA|甲醇 | 乙二醇 | 聚乙烯 | 聚丙烯 | 碳酸锂 | 氢氧化锂 | 锂'
)

_RE_VARIETY = re.compile(
    r'(?:套期保值 | 套保 | 对冲).{0,30}?(' + _VARIETY_KEYWORDS + r')',
    re.IGNORECASE,
)

_CURRENCY = r'(?:亿 | 万)?(?:美元 | 欧元 | 港元 | 港币 | 日元 | 英镑 | 人民币 | 元|USD|EUR|HKD|JPY|GBP|CNY)'
_RE_QUOTA = re.compile(
    r'(?:不超过 | 上限 | 额度 | 合约价值 | 保证金 | 权利金).{0,15}?'
    r'((?<!\d)\d[\d,.]*' + _CURRENCY + r'(?:或等值 [^（ (，。；]{0,8})?)',
    re.IGNORECASE,
)

_RE_PERIOD = re.compile(
    r'(?:有效期 | 授权期限 | 期限 | 额度有效期).{0,20}?'
    r'('
    r'\d{4}年\d{1,2}月\d{1,2}日(?:至 | 到|—|-)\d{4}年\d{1,2}月\d{1,2}日'
    r'|\d{4}[-/]\d{1,2}[-/]\d{1,2}(?:至 | 到|—|-)\d{4}[-/]\d{1,2}[-/]\d{1,2}'
    r'|(?<!\d)\d{1,3}(?!\d)个月'
    r'|(?<!\d)\d{1,2}(?!\d)年'
    r')',
    re.IGNORECASE,
)

_RE_PURPOSE = re.compile(
    r'(?:目的 | 为了 | 旨在 | 以).{0,5}?'
    r'(规避.{0,20}?风险 | 锁定.{0,20}?成本 | 降低.{0,20}?风险 | 对冲.{0,20}?风险 | 防范.{0,20}?风险)',
    re.IGNORECASE,
)

_RE_AUTHORITY = re.compile(r'(董事会 | 股东大会 | 股东会)(?:授权 | 批准 | 同意 | 审议通过)')


def _first_match(pattern: re.Pattern, text: str) -> str:
    m = pattern.search(text)
    return m.group(1).strip() if m else ""


def _all_matches(pattern: re.Pattern, text: str) -> list[str]:
    seen_vals = {}
    result = []
    for m in pattern.finditer(text):
        val = m.group(1).strip()
        digits = re.sub(r'[^\d]', '', val[:10])
        if digits and int(digits) == 0:
            continue
        key = digits if digits else val
        if key not in seen_vals:
            seen_vals[key] = True
            result.append(val)
    return result


def extract_from_pdf(pdf_path: Path, announcement: dict) -> dict:
    """从 PDF 提取套期保值信息"""
    raw_text = ""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                t = page.extract_text()
                if t:
                    raw_text += t
    except Exception as e:
        logger.warning(f"PDF 文本提取失败 [{pdf_path.name}]: {e}")

    text = _normalize(raw_text)

    sec_name = announcement.get("secName", "")
    sec_code = announcement.get("secCode", "")

    if not sec_name:
        m = re.search(r'证券简称 [：:]\s*([^\s  ]+)', raw_text)
        if m:
            sec_name = m.group(1).strip()
        else:
            m = re.search(r'([\u4e00-\u9fff（()）\w]{4,30}(?:股份有限公司 | 有限公司 | 集团股份有限公司))', raw_text)
            if m:
                sec_name = m.group(1).strip()

    if not sec_code:
        m = re.search(r'证券代码 [：:]\s*(\d+)', raw_text)
        if m:
            sec_code = m.group(1).strip()

    title = announcement.get("title", "") or pdf_path.stem
    is_policy = bool(re.search(r'管理制度 | 内部控制制度 | 风险管理制度', title))

    from datetime import datetime, timezone
    publish_time = announcement.get("publishTime", "")
    publish_date = ""
    if publish_time:
        try:
            publish_date = datetime.fromtimestamp(int(publish_time) / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            publish_date = str(publish_time)[:10]

    return {
        "announcement_id": announcement.get("announcementId", ""),
        "sec_code": sec_code,
        "sec_name": sec_name,
        "title": title,
        "publish_date": publish_date,
        "varieties": "、".join(_all_matches(_RE_VARIETY, text)) if _all_matches(_RE_VARIETY, text) else "",
        "quota": "；".join(_all_matches(_RE_QUOTA, text)) if _all_matches(_RE_QUOTA, text) else "",
        "period": _first_match(_RE_PERIOD, text),
        "purpose": _first_match(_RE_PURPOSE, text),
        "authority": _first_match(_RE_AUTHORITY, text),
        "is_policy": is_policy,
        "pdf_path": str(pdf_path),
    }
