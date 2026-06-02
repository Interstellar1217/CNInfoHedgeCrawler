#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
冒烟测试脚本

用法：
    python smoke_test.py
"""

import os
import sys
import time
import subprocess
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from curl_cffi import requests
from curl_cffi.requests import Session as CurlSession
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from config import config


def print_section(title: str):
    """打印分节标题"""
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


def test_cninfo_connectivity():
    """测试 1: 巨潮连通性"""
    print_section("测试 1: 巨潮连通性")

    print("\n[1.1] curl 测试...")
    for i in range(2):
        t0 = time.time()
        try:
            result = subprocess.run(
                ["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}",
                 "-A", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                 "--connect-timeout", "10", "--max-time", "15",
                 "https://www.cninfo.com.cn"],
                capture_output=True, text=True, timeout=20
            )
            elapsed = time.time() - t0
            print(f"  [{i+1}] 状态码：{result.stdout.strip()}, 耗时：{elapsed:.2f}s")
        except Exception as e:
            elapsed = time.time() - t0
            print(f"  [{i+1}] 失败：{e}, 耗时：{elapsed:.2f}s")

    print("\n[1.2] curl_cffi Session 测试（模拟爬虫实际行为）...")
    for i in range(2):
        t0 = time.time()
        try:
            session = CurlSession(impersonate="chrome136")
            r = session.get("https://www.cninfo.com.cn", timeout=15)
            elapsed = time.time() - t0
            print(f"  [{i+1}] 状态码：{r.status_code}, 耗时：{elapsed:.2f}s")
            session.close()
        except Exception as e:
            elapsed = time.time() - t0
            print(f"  [{i+1}] 失败：{e}, 耗时：{elapsed:.2f}s")

    print("\n[1.3] 巨潮查询接口（爬虫实际用的）...")
    query_url = config.LIST_API
    query_data = config.get_search_params(keyword="套期保值", page_num=1)

    for i in range(2):
        t0 = time.time()
        try:
            session = CurlSession(impersonate="chrome136")
            r = session.post(query_url, data=query_data, timeout=15)
            elapsed = time.time() - t0
            print(f"  [{i+1}] 状态码：{r.status_code}, 耗时：{elapsed:.2f}s")
            if r.status_code == 200:
                j = r.json()
                count = len(j.get('announcements', []))
                print(f"      公告数：{count}")
            session.close()
        except Exception as e:
            elapsed = time.time() - t0
            print(f"  [{i+1}] 失败：{e}, 耗时：{elapsed:.2f}s")


def test_pdf_download():
    """测试 2: PDF 下载"""
    print_section("测试 2: PDF 下载")

    # 使用一个已知的公告 ID 测试
    test_announcements = [
        ("1225015373", "福立旺", "688678"),
    ]

    for ann_id, sec_name, sec_code in test_announcements:
        print(f"\n测试公告：{sec_name}({sec_code}) ID={ann_id}")

        pdf_url = f"{config.STATIC_URL}/finalpage/2025-03-17/{ann_id}.PDF"
        print(f"PDF URL: {pdf_url}")

        t0 = time.time()
        try:
            session = CurlSession(impersonate="chrome136")
            r = session.get(pdf_url, timeout=30)
            elapsed = time.time() - t0

            print(f"  状态码：{r.status_code}, 耗时：{elapsed:.2f}s")
            print(f"  Content-Type: {r.headers.get('Content-Type', 'unknown')}")

            if r.status_code == 200:
                content_type = r.headers.get('Content-Type', '')
                if 'application/pdf' in content_type or 'application/octet-stream' in content_type:
                    print(f"  [OK] PDF 下载成功，大小：{len(r.content)} bytes")
                else:
                    print(f"  [WARN] 内容类型不符：{content_type}")
            else:
                print(f"  [FAIL] 下载失败：HTTP {r.status_code}")

            session.close()
        except Exception as e:
            elapsed = time.time() - t0
            print(f"  [FAIL] 下载异常：{e}, 耗时：{elapsed:.2f}s")

        time.sleep(1)


def test_pdf_extraction():
    """测试 3: PDF 文本提取"""
    print_section("测试 3: PDF 文本提取")

    data_dir = config.get_data_dir()
    pdf_files = list(data_dir.glob("*.pdf")) + list(data_dir.glob("*.PDF"))

    if not pdf_files:
        print("  未找到 PDF 文件，跳过提取测试")
        print("  提示：先运行 `python crawler.py --max-pages 1` 下载样本")
        return

    print(f"  找到 {len(pdf_files)} 个 PDF 文件，测试第一个...\n")

    pdf_path = pdf_files[0]
    print(f"  测试文件：{pdf_path.name}")

    try:
        from extractors.extractor import extract_hedge_info

        announcement = {
            "announcementId": pdf_path.stem.split("_")[-1] if "_" in pdf_path.stem else "unknown",
            "secCode": "",
            "secName": "",
            "title": pdf_path.stem,
            "publishTime": "",
        }

        t0 = time.time()
        info = extract_hedge_info(pdf_path, announcement)
        elapsed = time.time() - t0

        print(f"  提取耗时：{elapsed:.2f}s")
        print(f"  品种：{info.get('varieties', '无')}")
        print(f"  额度：{info.get('quota', '无')}")
        print(f"  有效期：{info.get('period', '无')}")
        print(f"  目的：{info.get('purpose', '无')}")
        print(f"  授权机构：{info.get('authority', '无')}")
        print(f"  是否制度：{info.get('is_policy', False)}")
        print(f"  是否无关：{info.get('is_irrelevant', False)} ({info.get('filter_reason', '')})")

    except Exception as e:
        print(f"  [FAIL] 提取失败：{e}")


def test_wecom_push():
    """测试 4: 企业微信推送"""
    print_section("测试 4: 企业微信推送")

    webhook = config.WECOM_WEBHOOK_URL
    if not webhook:
        print("  [SKIP] 未配置 WECOM_WEBHOOK_URL")
        print("  提示：在 config.py 中设置 WECOM_WEBHOOK_URL")
        return

    print(f"  Webhook: {webhook[:60]}...")

    headers = {"Content-Type": "application/json"}
    data = {
        "msgtype": "text",
        "text": {
            "content": f"[爬虫冒烟测试] 时间：{time.strftime('%Y-%m-%d %H:%M:%S')} 来自：{Path(__file__).parent.name}"
        }
    }

    for i in range(2):
        t0 = time.time()
        try:
            r = requests.post(webhook, headers=headers, json=data, timeout=10)
            elapsed = time.time() - t0
            print(f"  [{i+1}] 状态码：{r.status_code}, 耗时：{elapsed:.2f}s, 响应：{r.text[:80]}")
        except Exception as e:
            elapsed = time.time() - t0
            print(f"  [{i+1}] 失败：{e}, 耗时：{elapsed:.2f}s")


def test_database():
    """测试 5: 数据库初始化"""
    print_section("测试 5: 数据库初始化")

    try:
        from db.repository import init_db, get_stats, get_connection

        init_db()
        print("  [OK] 数据库表结构初始化成功")

        conn = get_connection()
        stats = get_stats(conn)
        conn.close()

        print(f"  数据库统计:")
        print(f"    总数：{stats.get('total', 0)}")
        print(f"    已推送：{stats.get('pushed', 0)}")
        print(f"    被过滤：{stats.get('filtered', 0)}")
        print(f"    待推送：{stats.get('pending', 0)}")

    except Exception as e:
        print(f"  [FAIL] 数据库测试失败：{e}")


def main():
    print("=" * 60)
    print("CNInfoHedgeCrawler 冒烟测试")
    print("=" * 60)
    print(f"时间：{time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"关键词：{config.DEFAULT_KEYWORD}")
    print(f"数据目录：{config.get_data_dir()}")

    test_cninfo_connectivity()
    test_pdf_download()
    test_pdf_extraction()
    test_wecom_push()
    test_database()

    print_section("全部测试完成")
    print("提示：如果某项测试失败，请检查网络、配置或依赖")


if __name__ == "__main__":
    main()
