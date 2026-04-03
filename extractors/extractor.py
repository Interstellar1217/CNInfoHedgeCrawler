#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""提取器模块 - 向后兼容"""

from pathlib import Path
from core.extractor import extract_from_pdf


def extract_hedge_info(pdf_path: Path, announcement: dict) -> dict:
    """从 PDF 提取套期保值信息（向后兼容）"""
    return extract_from_pdf(pdf_path, announcement)
