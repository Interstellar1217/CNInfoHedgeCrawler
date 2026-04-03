#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""通知模块 CLI - 向后兼容"""

import sys
from pathlib import Path

from core.crawler import CrawlerService
from core.extractor import extract_from_pdf
from core.notifier import send_to_wecom


def preview_markdown(info: dict) -> None:
    from core.notifier import build_markdown
    print("\n" + "=" * 60)
    print("【推送预览】")
    print("=" * 60)
    print(build_markdown(info))
    print("=" * 60 + "\n")


def main():
    """
    CLI:
      python -m notifiers.notifier --id 1225015373
      python -m notifiers.notifier --id 1225015373 --send
      python -m notifiers.notifier --batch
    """
    import argparse

    parser = argparse.ArgumentParser(description="套期保值公告推送工具")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("pdf", nargs="?", help="PDF 文件路径")
    group.add_argument("--id", dest="ann_id", help="公告 ID")
    group.add_argument("--batch", action="store_true", help="批量处理 CSV")
    parser.add_argument("--send", action="store_true", help="实际推送")
    parser.add_argument("--webhook", default=None, help="临时 Webhook URL")

    args = parser.parse_args()

    service = CrawlerService()
    df = service.get_metadata_df()

    def _process(pdf_path: Path, announcement: dict):
        if not pdf_path.exists():
            print(f"[跳过] 文件不存在：{pdf_path}")
            return
        info = extract_from_pdf(pdf_path, announcement)
        info["org_id"] = announcement.get("orgId", "")
        preview_markdown(info)
        if args.send:
            ok = send_to_wecom(info, webhook_url=args.webhook)
            print("推送成功 ✓" if ok else "推送失败 ✗")
        else:
            print("（仅预览，未推送。加 --send 参数可推送）")

    if args.pdf:
        pdf_path = Path(args.pdf)
        stem = pdf_path.stem
        ann_id = stem.rsplit("_", 1)[-1] if "_" in stem else ""
        announcement = {}
        if df is not None and ann_id:
            announcement = service.lookup_announcement(ann_id)
        if not announcement:
            announcement = {
                "announcementId": ann_id,
                "secCode": "", "secName": "", "orgId": "",
                "title": stem.rsplit("_", 1)[0] if "_" in stem else stem,
                "publishTime": "",
            }
        _process(pdf_path, announcement)

    elif args.ann_id:
        if df is None:
            print("无法加载元数据 CSV")
            sys.exit(1)
        announcement = service.lookup_announcement(args.ann_id)
        if not announcement:
            print(f"CSV 中未找到公告 ID: {args.ann_id}")
            sys.exit(1)
        pdf_path = service.find_pdf_path(args.ann_id)
        if pdf_path is None:
            print(f"本地未找到 PDF: {args.ann_id}")
            sys.exit(1)
        _process(pdf_path, announcement)

    elif args.batch:
        if df is None:
            print("无法加载元数据 CSV")
            sys.exit(1)
        total, ok_count, skip_count = len(df), 0, 0
        print(f"CSV 共 {total} 条记录，开始处理...\n")
        for _, row in df.iterrows():
            announcement = row.to_dict()
            announcement["announcementId"] = str(announcement.get("announcementId", ""))
            pdf_path = service.find_pdf_path(announcement["announcementId"])
            if pdf_path is None:
                skip_count += 1
                continue
            info = extract_from_pdf(pdf_path, announcement)
            info["org_id"] = announcement.get("orgId", "")
            if info.get("is_policy"):
                skip_count += 1
                continue
            preview_markdown(info)
            if args.send:
                if send_to_wecom(info, webhook_url=args.webhook):
                    ok_count += 1
                import time
                time.sleep(0.5)
        print(f"\n批量完成：推送 {ok_count} 条，跳过 {skip_count} 条")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
