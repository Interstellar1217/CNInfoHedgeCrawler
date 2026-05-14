#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
数据库仓库模块

提供 SQLite 数据库的 CRUD 操作，支持：
- 公告元数据存储
- 去重判断
- 推送状态管理
- 断点续爬
"""

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any

from loguru import logger
from config import config


def get_db_path() -> Path:
    """获取数据库文件路径"""
    return config.get_data_dir() / "announcements.db"


def get_connection() -> sqlite3.Connection:
    """获取数据库连接"""
    conn = sqlite3.connect(str(get_db_path()))
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """初始化数据库表结构"""
    conn = get_connection()
    try:
        conn.executescript("""
        -- 公告表
        CREATE TABLE IF NOT EXISTS announcement (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            announcement_id TEXT UNIQUE NOT NULL,
            sec_code        TEXT,
            sec_name        TEXT,
            org_id          TEXT,
            title           TEXT,
            publish_time    TEXT,
            adjunct_url     TEXT,
            pdf_path        TEXT,
            varieties       TEXT,
            quota           TEXT,
            period          TEXT,
            purpose         TEXT,
            authority       TEXT,
            is_policy       INTEGER DEFAULT 0,
            is_irrelevant   INTEGER DEFAULT 0,
            filter_reason   TEXT,
            pushed          INTEGER DEFAULT 0,
            push_error      TEXT,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- 创建索引加速查询
        CREATE INDEX IF NOT EXISTS idx_announcement_id ON announcement(announcement_id);
        CREATE INDEX IF NOT EXISTS idx_sec_code ON announcement(sec_code);
        CREATE INDEX IF NOT EXISTS idx_pushed ON announcement(pushed);
        CREATE INDEX IF NOT EXISTS idx_created_at ON announcement(created_at);
        """)
        conn.commit()
        logger.info("数据库表结构初始化完成")
    except Exception as e:
        logger.error(f"数据库表结构初始化失败：{e}")
        raise
    finally:
        conn.close()


def announcement_exists(conn: sqlite3.Connection, announcement_id: str) -> bool:
    """判断公告是否已存在"""
    row = conn.execute(
        "SELECT 1 FROM announcement WHERE announcement_id = ?",
        (announcement_id,)
    ).fetchone()
    return row is not None


def insert_announcement(conn: sqlite3.Connection, record: Dict[str, Any]) -> None:
    """
    插入公告记录

    Args:
        conn: 数据库连接
        record: 公告元数据字典，包含：
            - announcementId, secCode, secName, orgId, title, publishTime, adjunctUrl
            - pdf_path, varieties, quota, period, purpose, authority
            - is_policy, is_irrelevant, filter_reason
    """
    conn.execute("""
        INSERT INTO announcement (
            announcement_id, sec_code, sec_name, org_id, title, publish_time, adjunct_url,
            pdf_path, varieties, quota, period, purpose, authority,
            is_policy, is_irrelevant, filter_reason
        ) VALUES (
            :announcement_id, :sec_code, :sec_name, :org_id, :title, :publish_time, :adjunct_url,
            :pdf_path, :varieties, :quota, :period, :purpose, :authority,
            :is_policy, :is_irrelevant, :filter_reason
        )
        ON CONFLICT(announcement_id) DO UPDATE SET
            pdf_path        = excluded.pdf_path,
            varieties       = excluded.varieties,
            quota           = excluded.quota,
            period          = excluded.period,
            purpose         = excluded.purpose,
            authority       = excluded.authority,
            is_policy       = excluded.is_policy,
            is_irrelevant   = excluded.is_irrelevant,
            filter_reason   = excluded.filter_reason,
            updated_at      = CURRENT_TIMESTAMP
    """, {
        "announcement_id": str(record.get("announcementId", "")),
        "sec_code": record.get("secCode", ""),
        "sec_name": record.get("secName", ""),
        "org_id": record.get("orgId", ""),
        "title": record.get("title", ""),
        "publish_time": record.get("publishTime", ""),
        "adjunct_url": record.get("adjunctUrl", ""),
        "pdf_path": record.get("pdf_path", ""),
        "varieties": record.get("varieties", ""),
        "quota": record.get("quota", ""),
        "period": record.get("period", ""),
        "purpose": record.get("purpose", ""),
        "authority": record.get("authority", ""),
        "is_policy": 1 if record.get("is_policy") else 0,
        "is_irrelevant": 1 if record.get("is_irrelevant") else 0,
        "filter_reason": record.get("filter_reason", ""),
    })


def update_push_status(conn: sqlite3.Connection, announcement_id: str, success: bool, error: str = None) -> None:
    """
    更新推送状态

    Args:
        conn: 数据库连接
        announcement_id: 公告 ID
        success: 是否推送成功
        error: 错误信息（失败时）
    """
    if success:
        conn.execute("""
            UPDATE announcement
            SET pushed = 1, push_error = NULL, updated_at = CURRENT_TIMESTAMP
            WHERE announcement_id = ?
        """, (announcement_id,))
    else:
        conn.execute("""
            UPDATE announcement
            SET pushed = 0, push_error = ?, updated_at = CURRENT_TIMESTAMP
            WHERE announcement_id = ?
        """, (error, announcement_id))


def get_unpushed_announcements(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    """获取未推送且非无关的公告列表"""
    return conn.execute("""
        SELECT * FROM announcement
        WHERE pushed = 0 AND is_irrelevant = 0
        ORDER BY created_at DESC
    """).fetchall()


def get_filtered_announcements(conn: sqlite3.Connection, limit: int = 50) -> List[sqlite3.Row]:
    """获取被过滤的公告列表（用于调试）"""
    return conn.execute("""
        SELECT announcement_id, sec_name, title, filter_reason, created_at
        FROM announcement
        WHERE is_irrelevant = 1
        ORDER BY created_at DESC
        LIMIT ?
    """, (limit,)).fetchall()


def get_stats(conn: sqlite3.Connection) -> Dict[str, Any]:
    """获取统计信息"""
    stats = {}

    # 总数
    row = conn.execute("SELECT COUNT(*) as cnt FROM announcement").fetchone()
    stats["total"] = row["cnt"]

    # 已推送
    row = conn.execute("SELECT COUNT(*) as cnt FROM announcement WHERE pushed = 1").fetchone()
    stats["pushed"] = row["cnt"]

    # 被过滤
    row = conn.execute("SELECT COUNT(*) as cnt FROM announcement WHERE is_irrelevant = 1").fetchone()
    stats["filtered"] = row["cnt"]

    # 待推送
    row = conn.execute("SELECT COUNT(*) as cnt FROM announcement WHERE pushed = 0 AND is_irrelevant = 0").fetchone()
    stats["pending"] = row["cnt"]

    return stats
