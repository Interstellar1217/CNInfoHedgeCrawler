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
  5. 支持环境变量自定义正则规则（HEDGE_CRAWLER_*）

金融领域正则设计参考：
  - 金额匹配采用"触发词 + 数字 + 单位"三段式结构，支持千分位逗号和小数
  - 有效期匹配同时支持绝对日期区间和相对期限（N个月/年/天）
  - 品种匹配采用"业务动词 + 品种关键词"的上下文锚定模式
  - 所有正则均作用于标准化后的紧凑文本，消除 PDF 排版噪声干扰
"""

import os
import re
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

import pdfplumber
from loguru import logger

from config import config


def _normalize(text: str) -> str:
    """去除所有空白字符（空格、全角空格、制表符、换行）"""
    return re.sub(r'[\s\u3000]+', '', text)


def _load_regex_env(key: str, default: str) -> str:
    """从环境变量加载正则模式，未设置时返回默认值。"""
    try:
        value = os.environ.get(f"HEDGE_CRAWLER_{key.upper()}", None)
        if value:
            return value
    except Exception:
        pass
    return default


_VARIETY_KEYWORDS_DEFAULT = (
    r'外汇|美元|欧元|港元|港币|日元|英镑|人民币|'
    r'铜|铝|锌|镍|铅|锡|黄金|白银|原油|天然气|橡胶|'
    r'大豆|玉米|小麦|棉花|铁矿石|螺纹钢|热轧卷板|'
    r'PTA|甲醇|乙二醇|聚乙烯|聚丙烯|碳酸锂|氢氧化锂|锂'
)
_VARIETY_KEYWORDS = _load_regex_env("variety_keywords", _VARIETY_KEYWORDS_DEFAULT)

_RE_VARIETY = re.compile(
    r'(?:套期保值|套保|对冲|期货|远期|外汇衍生|金融衍生)'
    r'.{0,50}?'
    r'(' + _VARIETY_KEYWORDS + r')',
    re.IGNORECASE,
)

_CURRENCY_DEFAULT = (
    r'(?:亿|万)?'
    r'(?:美元|欧元|港元|港币|日元|英镑|人民币|元|'
    r'USD|EUR|HKD|JPY|GBP|CNY)'
)
_CURRENCY = _load_regex_env("quota_currency_suffix", _CURRENCY_DEFAULT)

_QUOTA_TRIGGERS_DEFAULT = (
    r'(?:不超过|上限|额度|合约价值|保证金|权利金|'
    r'拟使用|计划投入|资金规模|交易金额|投入资金|'
    r'授权额度|申请额度|最高额度)'
)
_QUOTA_TRIGGERS = _load_regex_env("quota_trigger_words", _QUOTA_TRIGGERS_DEFAULT)

_RE_QUOTA = re.compile(
    _QUOTA_TRIGGERS + r'.{0,20}?'
    r'('
    r'(?<!\d)(?:\d{1,3}(?:,\d{3})*|\d+)(?:\.\d+)?'
    r'(?:' + _CURRENCY + r')'
    r'(?:或等值[^（(，。；]{0,12})?'
    r')',
    re.IGNORECASE,
)

_PERIOD_TRIGGERS_DEFAULT = (
    r'(?:有效期|授权期限|期限|额度有效期|'
    r'业务期限|有效期自|授权有效期|有效期限|'
    r'开展期限|交易期限)'
)
_PERIOD_TRIGGERS = _load_regex_env("period_trigger_words", _PERIOD_TRIGGERS_DEFAULT)

_RE_PERIOD = re.compile(
    _PERIOD_TRIGGERS + r'.{0,30}?'
    r'('
    r'(?:自|从)?'
    r'\d{4}年\d{1,2}月\d{1,2}日'
    r'(?:至|到|—|–|-|~|起)'
    r'\d{4}年\d{1,2}月\d{1,2}日'
    r'|'
    r'(?:自|从)?'
    r'\d{4}[-/]\d{1,2}[-/]\d{1,2}'
    r'(?:至|到|—|–|-|~|起)'
    r'\d{4}[-/]\d{1,2}[-/]\d{1,2}'
    r'|'
    r'(?<!\d)\d{1,3}(?!\d)个月'
    r'|'
    r'(?<!\d)\d{1,2}(?!\d)年'
    r'|'
    r'(?<!\d)\d{1,3}(?!\d)天'
    r')',
    re.IGNORECASE,
)

_PURPOSE_TRIGGERS_DEFAULT = (
    r'(?:目的|为了|旨在|以|特开展|拟开展|开展本次|'
    r'本次套期保值|本次拟开展|通过此次)'
)
_PURPOSE_TRIGGERS = _load_regex_env("purpose_trigger_words", _PURPOSE_TRIGGERS_DEFAULT)

_PURPOSE_ACTIONS_DEFAULT = (
    r'(?:'
    r'规避.{0,30}?风险'
    r'|锁定.{0,30}?(?:成本|利润|收益)'
    r'|降低.{0,30}?风险'
    r'|对冲.{0,30}?风险'
    r'|防范.{0,30}?风险'
    r'|管理.{0,30}?风险'
    r'|减少.{0,30}?影响'
    r'|平滑.{0,30}?波动'
    r'|控制.{0,30}?风险'
    r'|转移.{0,30}?风险'
    r')'
)
_PURPOSE_ACTIONS = _load_regex_env("purpose_actions", _PURPOSE_ACTIONS_DEFAULT)

_RE_PURPOSE = re.compile(
    r'(' + _PURPOSE_TRIGGERS + r'.{0,10}?' + _PURPOSE_ACTIONS + r')',
    re.IGNORECASE,
)

_RE_SEC_SHORT = re.compile(r'证券简称[：:]\s*([^\s\n]+)')

_RE_SEC_FULL = re.compile(
    r'([\u4e00-\u9fff（）\w]{4,50}'
    r'(?:股份有限公司|有限公司|集团股份有限公司))'
)

_AUTHORITY_NAMES_DEFAULT = (
    r'(?:董事会|股东大会|股东会|总经理办公会|董事长|'
    r'公司董事会|公司股东大会|公司股东会)'
)
_AUTHORITY_NAMES = _load_regex_env("authority_names", _AUTHORITY_NAMES_DEFAULT)

_RE_AUTHORITY = re.compile(
    r'(' + _AUTHORITY_NAMES + r'(?:授权|批准|同意|审议通过|审议批准|决议通过|'
    r'审议并批准|审议同意|审批通过)' + r')'
)


def _first_match(pattern: re.Pattern, text: str) -> str:
    """返回第一个捕获组匹配项，去除首尾空白。"""
    m = pattern.search(text)
    return m.group(1).strip() if m else ""


def _all_matches(pattern: re.Pattern, text: str) -> List[str]:
    """
    去重匹配：相同数值只保留第一次出现，过滤零值噪声。

    使用数字部分作为去重键，避免同一金额因前后文差异被重复提取。
    """
    seen_vals: dict[str, bool] = {}
    result: List[str] = []
    for m in pattern.finditer(text):
        val = m.group(1).strip()
        digits = re.sub(r'[^\d]', '', val[:15])
        if digits and int(digits) == 0:
            continue
        key = digits if digits else val
        if key not in seen_vals:
            seen_vals[key] = True
            result.append(val)
    return result


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
        logger.warning(f"PDF 文本提取失败 [{pdf_path.name}]: {e}")
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
    text = _normalize(raw_text)

    sec_name = announcement.get("secName", "")
    sec_code = announcement.get("secCode", "")
    if not sec_name:
        m = _RE_SEC_SHORT.search(raw_text)
        if m:
            sec_name = m.group(1).strip()
        else:
            m = _RE_SEC_FULL.search(raw_text)
            if m:
                sec_name = m.group(1).strip()
    if not sec_code:
        m = re.search(r'证券代码[：:]\s*(\d+)', raw_text)
        if m:
            sec_code = m.group(1).strip()

    title = announcement.get("title", "") or pdf_path.stem
    is_policy = bool(re.search(r'管理制度|内部控制制度|风险管理制度', title))

    varieties = _all_matches(_RE_VARIETY, text)
    quotas = _all_matches(_RE_QUOTA, text)

    is_irrelevant, filter_reason = is_irrelevant_announcement(title, {
        "varieties": ",".join(varieties) if varieties else "",
        "quota": "；".join(quotas) if quotas else "",
    })

    result = {
        "announcement_id": announcement.get("announcementId", ""),
        "org_id": announcement.get("orgId", ""),
        "sec_code": sec_code,
        "sec_name": sec_name,
        "title": announcement.get("title", ""),
        "publish_date": _format_date(announcement.get("publishTime", "")),
        "varieties": ",".join(varieties) if varieties else "",
        "quota": "；".join(quotas) if quotas else "",
        "period": _first_match(_RE_PERIOD, text),
        "purpose": _first_match(_RE_PURPOSE, text),
        "authority": _first_match(_RE_AUTHORITY, text),
        "is_policy": is_policy,
        "is_irrelevant": is_irrelevant,
        "filter_reason": filter_reason,
        "pdf_path": str(pdf_path),
    }

    logger.debug(
        f"提取完成 [{announcement.get('secCode', pdf_path.stem)}] "
        f"品种={result['varieties']} 额度={result['quota']} "
        f"有效期={result['period']} 管理制度={is_policy} 无关={is_irrelevant}({filter_reason})"
    )
    return result


def is_irrelevant_announcement(title: str, extracted_info: dict) -> Tuple[bool, str]:
    """
    判断公告是否无关。

    Args:
        title: 公告标题
        extracted_info: extract_hedge_info() 返回的字典

    Returns:
        (是否无关，过滤原因)
    """
    if any(kw in title for kw in config.FILTER_TITLE_KEYWORDS):
        if not any(keep in title for keep in config.KEEP_TITLE_KEYWORDS):
            return True, "标题过滤"

    varieties = extracted_info.get("varieties", "")
    quota = extracted_info.get("quota", "")
    if not varieties and not quota:
        return True, "内容无关"

    return False, ""


def _format_date(publish_time) -> str:
    """
    毫秒时间戳或字符串 → YYYY-MM-DD

    注意：使用本地时区而非 UTC，避免日期出现偏差
    """
    if not publish_time:
        return ""
    try:
        return datetime.fromtimestamp(int(publish_time) / 1000).strftime("%Y-%m-%d")
    except (ValueError, TypeError, OSError):
        return str(publish_time)[:10]
