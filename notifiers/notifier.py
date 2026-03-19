#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
企业微信机器人推送模块

将套期保值公告的结构化信息以 Markdown 卡片形式推送到企业微信群。
企业微信机器人 Webhook 文档：
https://developer.work.weixin.qq.com/document/path/91770
"""

from pathlib import Path

from curl_cffi import requests
from loguru import logger

from config import config


def _build_markdown(info: dict) -> str:
    sec = f"{info['sec_name']}（{info['sec_code']}）" if info.get('sec_code') else info.get('sec_name', '')
    date = info.get("publish_date", "")

    lines = [
        "## 📋 套期保值公告",
        f"**公司：** {sec}",
        f"**标题：** {info.get('title', '')}",
        f"**公告日期：** {date}",
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
    """推送单条公告到企业微信"""
    if info.get("is_policy"):
        logger.info(f"管理制度类文件，跳过推送: {info.get('title', '')}")
        return False

    url = webhook_url or config.WECOM_WEBHOOK_URL
    if not url:
        logger.warning("未配置企业微信 Webhook URL，请在 config.py 中设置 WECOM_WEBHOOK_URL")
        return False

    payload = {"msgtype": "markdown", "markdown": {"content": _build_markdown(info)}}

    try:
        resp = requests.post(url, json=payload, timeout=10)
        result = resp.json()
        if result.get("errcode") == 0:
            logger.info(f"推送成功: {info.get('sec_name')} - {info.get('title', '')[:30]}")
            return True
        logger.error(f"推送失败: {result}")
        return False
    except Exception as e:
        logger.error(f"推送异常: {e}")
        return False


def preview_markdown(info: dict) -> None:
    print("\n" + "=" * 60)
    print("【推送预览】")
    print("=" * 60)
    print(_build_markdown(info))
    print("=" * 60 + "\n")


def load_metadata(csv_path: Path = None) -> "pd.DataFrame | None":
    """加载 announcements_metadata.csv，返回 DataFrame，失败返回 None"""
    try:
        import pandas as pd
        p = csv_path or Path(config.DATA_DIR) / config.METADATA_FILE
        if not p.exists():
            logger.warning(f"元数据文件不存在: {p}")
            return None
        return pd.read_csv(p, encoding="utf-8-sig", dtype={"announcementId": str, "secCode": str})
    except Exception as e:
        logger.error(f"加载元数据失败: {e}")
        return None


def lookup_announcement(ann_id: str, df: "pd.DataFrame") -> dict:
    """从 DataFrame 中按 announcementId 查找元数据，返回 announcement dict"""
    rows = df[df["announcementId"] == str(ann_id)]
    if rows.empty:
        return {}
    row = rows.iloc[0]
    return {
        "announcementId": str(row.get("announcementId", "")),
        "secCode":        str(row.get("secCode", "")),
        "secName":        str(row.get("secName", "")),
        "orgId":          str(row.get("orgId", "")),
        "title":          str(row.get("title", "")),
        "publishTime":    str(row.get("publishTime", "")),
        "adjunctUrl":     str(row.get("adjunctUrl", "")),
    }


def pdf_path_from_metadata(row: dict, data_dir: Path = None) -> Path | None:
    """
    根据元数据推断本地 PDF 路径。
    文件名格式：{title}_{announcementId}.pdf（与 crawler.py generate_filename 一致）
    """
    data_dir = data_dir or Path(config.DATA_DIR)
    ann_id = row.get("announcementId", "")
    # 直接按 announcementId 后缀匹配，兼容各种标题
    matches = list(data_dir.glob(f"*_{ann_id}.pdf")) + list(data_dir.glob(f"*_{ann_id}.PDF"))
    return matches[0] if matches else None


if __name__ == "__main__":
    """
    CLI 用法：

      # 按文件路径（自动从 CSV 补全元数据）
      python -m notifiers.notifier data/关于开展外汇套期保值业务的公告_1225015373.pdf

      # 按公告 ID（自动从 CSV 查找文件路径和元数据）
      python -m notifiers.notifier --id 1225015373

      # 批量推送 CSV 中所有记录（仅预览，不推送）
      python -m notifiers.notifier --batch

      # 加 --send 实际推送
      python -m notifiers.notifier --id 1225015373 --send

      # 临时指定 Webhook
      python -m notifiers.notifier --id 1225015373 --send --webhook https://qyapi.weixin.qq.com/...
    """
    import argparse
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))

    from extractors.extractor import extract_hedge_info

    parser = argparse.ArgumentParser(description="套期保值公告推送工具")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("pdf", nargs="?", help="PDF 文件路径")
    group.add_argument("--id", dest="ann_id", help="公告 ID（从 CSV 自动查找）")
    group.add_argument("--batch", action="store_true", help="批量处理 CSV 中所有记录")
    parser.add_argument("--send",    action="store_true", help="实际推送到企业微信（默认仅预览）")
    parser.add_argument("--webhook", default=None,        help="临时指定 Webhook URL")
    parser.add_argument("--csv",     default=None,        help="指定元数据 CSV 路径")
    args = parser.parse_args()

    df = load_metadata(Path(args.csv) if args.csv else None)

    def _process(pdf_path: Path, announcement: dict):
        if not pdf_path.exists():
            print(f"[跳过] 文件不存在: {pdf_path}")
            return
        # 补全 org_id 供详情页链接使用
        info = extract_hedge_info(pdf_path, announcement)
        info["org_id"] = announcement.get("orgId", "")
        preview_markdown(info)
        if args.send:
            ok = send_to_wecom(info, webhook_url=args.webhook)
            print("推送成功 ✓" if ok else "推送失败 ✗")
        else:
            print("（仅预览，未推送。加 --send 参数可推送）")

    # ── 模式1：指定文件路径 ──────────────────────────────────────────────────
    if args.pdf:
        pdf_path = Path(args.pdf)
        stem = pdf_path.stem
        ann_id = stem.rsplit("_", 1)[-1] if "_" in stem else ""
        # 优先从 CSV 查元数据
        announcement = {}
        if df is not None and ann_id:
            announcement = lookup_announcement(ann_id, df)
        if not announcement:
            announcement = {
                "announcementId": ann_id,
                "secCode": "", "secName": "", "orgId": "",
                "title": stem.rsplit("_", 1)[0] if "_" in stem else stem,
                "publishTime": "",
            }
        _process(pdf_path, announcement)

    # ── 模式2：指定公告 ID ───────────────────────────────────────────────────
    elif args.ann_id:
        if df is None:
            print("无法加载元数据 CSV，请检查 data/announcements_metadata.csv")
            sys.exit(1)
        announcement = lookup_announcement(args.ann_id, df)
        if not announcement:
            print(f"CSV 中未找到公告 ID: {args.ann_id}")
            sys.exit(1)
        pdf_path = pdf_path_from_metadata(announcement)
        if pdf_path is None:
            print(f"本地未找到对应 PDF，announcementId={args.ann_id}")
            sys.exit(1)
        _process(pdf_path, announcement)

    # ── 模式3：批量处理 ──────────────────────────────────────────────────────
    elif args.batch:
        if df is None:
            print("无法加载元数据 CSV")
            sys.exit(1)
        total, ok_count, skip_count = len(df), 0, 0
        print(f"CSV 共 {total} 条记录，开始处理...\n")
        for _, row in df.iterrows():
            announcement = row.to_dict()
            announcement["announcementId"] = str(announcement.get("announcementId", ""))
            pdf_path = pdf_path_from_metadata(announcement)
            if pdf_path is None:
                skip_count += 1
                continue
            info = extract_hedge_info(pdf_path, announcement)
            info["org_id"] = announcement.get("orgId", "")
            if info.get("is_policy"):
                skip_count += 1
                continue
            preview_markdown(info)
            if args.send:
                if send_to_wecom(info, webhook_url=args.webhook):
                    ok_count += 1
                import time; time.sleep(0.5)  # 避免触发频率限制
        print(f"\n批量完成：推送 {ok_count} 条，跳过 {skip_count} 条")

    else:
        parser.print_help()
