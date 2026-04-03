#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""企业微信通知服务"""

from loguru import logger
from config import config


def build_markdown(info: dict) -> str:
    """构建 Markdown 消息"""
    sec = f"{info['sec_name']}({info['sec_code']})" if info.get('sec_code') else info.get('sec_name', '')

    lines = [
        "## 📋 套期保值公告",
        f"**公司：** {sec}",
        f"**标题：** {info.get('title', '')}",
        f"**公告日期：** {info.get('publish_date', '')}",
    ]

    if info.get("varieties"):
        lines.append(f"**套保品种：** {info['varieties']}")
    if info.get("quota"):
        lines.append(f"**套保额度：** {info['quota']}")
    if info.get("period"):
        lines.append(f"**有效期：** {info['period']}")
    if info.get("purpose"):
        lines.append(f"**套保目的：** {info['purpose']}")
    if info.get("authority"):
        lines.append(f"**授权机构：** {info['authority']}")

    ann_id = info.get("announcement_id", "")
    org_id = info.get("org_id", "")
    sec_code = info.get("sec_code", "")

    if ann_id:
        detail_url = (
            f"https://www.cninfo.com.cn/new/disclosure/detail"
            f"?stockCode={sec_code}&announcementId={ann_id}&orgId={org_id}"
        )
        lines.append(f"[查看原文]({detail_url})")

    return "\n".join(lines)


def send_to_wecom(info: dict, webhook_url: str = None) -> bool:
    """推送到企业微信"""
    if info.get("is_policy"):
        logger.info(f"管理制度类文件，跳过推送：{info.get('title', '')}")
        return False

    url = webhook_url or config.WECOM_WEBHOOK_URL
    if not url:
        logger.warning("未配置企业微信 Webhook URL")
        return False

    import requests
    payload = {"msgtype": "markdown", "markdown": {"content": build_markdown(info)}}

    try:
        resp = requests.post(url, json=payload, timeout=10)
        result = resp.json()
        if result.get("errcode") == 0:
            logger.info(f"推送成功：{info.get('sec_name')}")
            return True
        logger.error(f"推送失败：{result}")
        return False
    except Exception as e:
        logger.error(f"推送异常：{e}")
        return False
