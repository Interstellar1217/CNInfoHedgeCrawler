#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""CLI 入口 - 向后兼容"""

import argparse
from datetime import datetime

from loguru import logger
from util import setup_logger
from core.crawler import CrawlerService
from core.extractor import extract_from_pdf
from core.notifier import send_to_wecom


def main():
    parser = argparse.ArgumentParser(description='巨潮资讯套期保值公告爬虫')
    parser.add_argument('--keyword', type=str, default='套期保值', help='搜索关键词')
    parser.add_argument('--max-pages', type=int, default=None, help='最大爬取页数')
    parser.add_argument('--start-page', type=int, default=1, help='起始页码')
    parser.add_argument('--start-date', type=str, help='开始日期 YYYY-MM-DD')
    parser.add_argument('--end-date', type=str, help='结束日期 YYYY-MM-DD')

    args = parser.parse_args()

    setup_logger()

    crawler = CrawlerService(keyword=args.keyword)

    try:
        if args.start_date and args.end_date:
            stats = crawler.crawl_all(
                max_pages=args.max_pages,
                start_page=args.start_page,
                start_date=args.start_date,
                end_date=args.end_date,
            )
        else:
            stats = crawler.crawl_all(
                max_pages=args.max_pages,
                start_page=args.start_page,
            )

        print("\n" + "=" * 50)
        print("爬取完成！统计信息：")
        print(f"关键词：{args.keyword}")
        print(f"处理页数：{stats['total_pages']}")
        print(f"下载公告：{stats['downloaded']}")
        print(f"总公告数：{stats['total_announcements']}")
        print(f"开始时间：{stats['start_time']}")
        print(f"结束时间：{stats['end_time']}")
        print(f"总耗时：{stats['duration']}")
        print("=" * 50)

    except KeyboardInterrupt:
        logger.warning("用户中断程序")
    except Exception as e:
        logger.error(f"程序运行出错：{e}")
        raise


if __name__ == "__main__":
    main()
