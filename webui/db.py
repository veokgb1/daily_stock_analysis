# -*- coding: utf-8 -*-
"""
webui/db.py  v2
===============
在 v1 基础上：
  - strategy_groups 新增 tags 列（JSON 数组，如 ["科技","长线"]）
  - 采用"追加列 + 软迁移"策略：ALTER TABLE ADD COLUMN，
    OperationalError（列已存在）静默忽略，老数据零影响
  - save_strategy_group / get_strategy_group / list_strategy_groups
    均更新以支持 tags 参数
"""

import json
import shutil
import sqlite3
import logging
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Dict, Any

logger = logging.getLogger(__name__)

_ROOT_DIR = Path(__file__).parent.parent
_LEGACY_DB_PATH = _ROOT_DIR / "stock_history.db"
_DEFAULT_DB_PATH = _ROOT_DIR / "data" / "history_data.db"
_DB_MAINTENANCE_MARKER = Path(__file__).parent.parent / "data" / ".db_maintenance.json"
_SNAPSHOT_FACTOR_COLUMNS = [
    ("trend_prediction", "TEXT"),
    ("current_price", "REAL"),
    ("change_pct", "REAL"),
    ("ma_alignment", "TEXT"),
    ("buy_point", "REAL"),
    ("stop_loss", "REAL"),
    ("target_price", "REAL"),
    ("position_advice", "TEXT"),
    ("bias_rate", "REAL"),
    ("volume_ratio", "REAL"),
    ("turnover_rate", "REAL"),
    ("chip_profit_ratio", "REAL"),
    ("time_sensitivity", "TEXT"),
    ("factors_json", "TEXT"),
]


def _resolve_db_path() -> Path:
    configured = (
        os.getenv("HISTORY_DATABASE_PATH")
        or os.getenv("DATABASE_PATH")
        or ""
    ).strip()
    if configured:
        return Path(configured)

    if _DEFAULT_DB_PATH.exists():
        return _DEFAULT_DB_PATH
    if _LEGACY_DB_PATH.exists():
        _DEFAULT_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        try:
            shutil.copy2(_LEGACY_DB_PATH, _DEFAULT_DB_PATH)
            logger.info("历史数据库已迁移到: %s", _DEFAULT_DB_PATH)
            return _DEFAULT_DB_PATH
        except Exception as exc:
            logger.warning("历史数据库迁移失败，继续使用旧路径: %s", exc)
            return _LEGACY_DB_PATH
    return _DEFAULT_DB_PATH


_DB_PATH = _resolve_db_path()


# ─────────────────────────────────────────────────────────────────────────────
# 连接上下文
# ─────────────────────────────────────────────────────────────────────────────

@contextmanager
def _get_conn():
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(_DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA auto_vacuum=INCREMENTAL")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# 初始化（幂等）
# ─────────────────────────────────────────────────────────────────────────────

def init_db() -> None:
    """建表 + 索引 + 软迁移新列。可多次安全调用。"""
    with _get_conn() as conn:
        # ── 建表（若不存在）──────────────────────────────────────────────────
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS strategy_groups (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT    NOT NULL UNIQUE,
            codes       TEXT    NOT NULL,
            description TEXT    DEFAULT '',
            tags        TEXT    DEFAULT '[]',
            created_at  TEXT    NOT NULL,
            updated_at  TEXT    NOT NULL
        );

        CREATE TABLE IF NOT EXISTS analysis_snapshots (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id           TEXT    NOT NULL,
            code             TEXT    NOT NULL,
            name             TEXT    NOT NULL,
            report_md        TEXT    NOT NULL,
            sentiment_score  INTEGER DEFAULT 50,
            operation_advice TEXT    DEFAULT '',
            run_mode         TEXT    DEFAULT '',
            created_at       TEXT    NOT NULL,
            is_visible       INTEGER DEFAULT 1
        );

        CREATE INDEX IF NOT EXISTS idx_snap_code_ts
            ON analysis_snapshots (code, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_snap_run_id
            ON analysis_snapshots (run_id);
        CREATE INDEX IF NOT EXISTS idx_snap_ts
            ON analysis_snapshots (created_at DESC);

        CREATE TABLE IF NOT EXISTS watchlist (
            id                     INTEGER PRIMARY KEY AUTOINCREMENT,
            code                   TEXT    NOT NULL UNIQUE,
            name                   TEXT    NOT NULL,
            added_from_snapshot_id INTEGER,
            added_from_run_id      TEXT,
            added_at               TEXT    NOT NULL,
            entry_ref_price        REAL,
            buy_point              REAL,
            stop_loss              REAL,
            target_price           REAL,
            initial_score          REAL,
            initial_advice         TEXT,
            initial_trend          TEXT,
            last_price             REAL,
            last_price_updated     TEXT,
            alert_status           TEXT    DEFAULT 'normal',
            user_tags              TEXT    DEFAULT '[]',
            user_notes             TEXT    DEFAULT '',
            is_active              INTEGER DEFAULT 1
        );

        CREATE INDEX IF NOT EXISTS idx_watchlist_active
            ON watchlist (is_active, added_at DESC);

        CREATE TABLE IF NOT EXISTS quick_pool (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            code       TEXT    NOT NULL UNIQUE,
            name       TEXT    NOT NULL,
            added_at   TEXT    NOT NULL
        );

        CREATE TABLE IF NOT EXISTS run_artifacts (
            run_id             TEXT PRIMARY KEY,
            created_at         TEXT NOT NULL,
            run_mode           TEXT DEFAULT '',
            market_report_md   TEXT DEFAULT '',
            stock_report_md    TEXT DEFAULT '',
            full_report_md     TEXT DEFAULT '',
            business_log       TEXT DEFAULT '',
            debug_log          TEXT DEFAULT '',
            schema_json        TEXT DEFAULT '{}'
        );

        CREATE INDEX IF NOT EXISTS idx_quick_pool_added
            ON quick_pool (added_at DESC);
        """)

        # ── 软迁移：给已存在的旧表补 tags 列 ───────────────────────────────
        # SQLite 对"已存在的列"执行 ALTER TABLE ADD COLUMN 会抛
        # OperationalError: duplicate column name: tags
        # 静默忽略即可，老数据自动得到 DEFAULT '[]'
        try:
            conn.execute(
                "ALTER TABLE strategy_groups ADD COLUMN tags TEXT DEFAULT '[]'"
            )
            logger.info("strategy_groups.tags 列迁移完成")
        except Exception:
            pass  # 列已存在，正常忽略

        for col_name, col_type in _SNAPSHOT_FACTOR_COLUMNS:
            try:
                conn.execute(
                    f"ALTER TABLE analysis_snapshots ADD COLUMN {col_name} {col_type}"
                )
                logger.info("analysis_snapshots.%s 列迁移完成", col_name)
            except Exception:
                pass

        try:
            conn.execute(
                "ALTER TABLE analysis_snapshots ADD COLUMN is_visible INTEGER DEFAULT 1"
            )
            logger.info("analysis_snapshots.is_visible 列迁移完成")
        except Exception:
            pass

        try:
            conn.execute("PRAGMA optimize")
        except Exception:
            pass

    _run_db_maintenance_if_due()
    logger.info(f"数据库初始化完成：{_DB_PATH}")


# ─────────────────────────────────────────────────────────────────────────────
# 工具
# ─────────────────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()

def _load_json_list(raw: str) -> List[str]:
    """安全解析 JSON 列表字段，异常返回空列表。"""
    try:
        v = json.loads(raw or "[]")
        return v if isinstance(v, list) else []
    except Exception:
        return []

def _factor_json(raw: Optional[Dict[str, Any]]) -> str:
    return json.dumps(raw or {}, ensure_ascii=False)


def _run_db_maintenance_if_due() -> None:
    """每天最多执行一次轻量数据库维护，控制 SQLite 文件膨胀。"""
    today = datetime.now().date().isoformat()
    last_run = None
    if _DB_MAINTENANCE_MARKER.exists():
        try:
            payload = json.loads(_DB_MAINTENANCE_MARKER.read_text(encoding="utf-8"))
            last_run = payload.get("last_vacuum_date")
        except Exception:
            last_run = None

    if last_run == today:
        return

    _DB_MAINTENANCE_MARKER.parent.mkdir(parents=True, exist_ok=True)
    try:
        conn = sqlite3.connect(str(_DB_PATH), check_same_thread=False)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA incremental_vacuum")
            conn.execute("VACUUM")
        finally:
            conn.close()
        _DB_MAINTENANCE_MARKER.write_text(
            json.dumps({"last_vacuum_date": today}, ensure_ascii=False),
            encoding="utf-8",
        )
        logger.info("SQLite 维护完成：VACUUM")
    except Exception as exc:
        logger.warning("SQLite 维护失败：%s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# strategy_groups CRUD
# ─────────────────────────────────────────────────────────────────────────────

def save_strategy_group(
    name: str,
    codes: List[str],
    description: str = "",
    tags: Optional[List[str]] = None,
) -> int:
    """
    保存策略组（upsert）。
    tags: 如 ["科技", "长线", "热门"]，默认空列表。
    """
    now        = _now_iso()
    codes_json = json.dumps(codes,          ensure_ascii=False)
    tags_json  = json.dumps(tags or [],     ensure_ascii=False)
    with _get_conn() as conn:
        cur = conn.execute(
            """
            INSERT INTO strategy_groups
                (name, codes, description, tags, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                codes       = excluded.codes,
                description = excluded.description,
                tags        = excluded.tags,
                updated_at  = excluded.updated_at
            """,
            (name, codes_json, description, tags_json, now, now),
        )
        return cur.lastrowid


def list_strategy_groups() -> List[Dict[str, Any]]:
    """返回所有策略组（含 tags，按更新时间倒序）。"""
    with _get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM strategy_groups ORDER BY updated_at DESC"
        ).fetchall()
    result = []
    for row in rows:
        d = dict(row)
        d["codes"] = _load_json_list(d.get("codes", "[]"))
        d["tags"]  = _load_json_list(d.get("tags",  "[]"))
        result.append(d)
    return result


def get_strategy_group(name: str) -> Optional[Dict[str, Any]]:
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM strategy_groups WHERE name = ?", (name,)
        ).fetchone()
    if row is None:
        return None
    d = dict(row)
    d["codes"] = _load_json_list(d.get("codes", "[]"))
    d["tags"]  = _load_json_list(d.get("tags",  "[]"))
    return d


def delete_strategy_group(name: str) -> None:
    with _get_conn() as conn:
        conn.execute("DELETE FROM strategy_groups WHERE name = ?", (name,))


# ─────────────────────────────────────────────────────────────────────────────
# analysis_snapshots CRUD（与 v1 完全兼容）
# ─────────────────────────────────────────────────────────────────────────────

def save_snapshot(
    run_id: str,
    code: str,
    name: str,
    report_md: str,
    sentiment_score: int = 50,
    operation_advice: str = "",
    run_mode: str = "",
    factors: Optional[Dict] = None,
) -> int:
    factors = factors or {}
    with _get_conn() as conn:
        cur = conn.execute(
            """
            INSERT INTO analysis_snapshots
                (run_id, code, name, report_md, sentiment_score,
                 operation_advice, run_mode, created_at,
                 is_visible,
                 trend_prediction, current_price, change_pct, ma_alignment,
                 buy_point, stop_loss, target_price, position_advice,
                 bias_rate, volume_ratio, turnover_rate, chip_profit_ratio,
                 time_sensitivity, factors_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (run_id, code, name, report_md, sentiment_score,
             operation_advice, run_mode, _now_iso(), 1,
             factors.get("trend_prediction"),
             factors.get("current_price"),
             factors.get("change_pct"),
             factors.get("ma_alignment"),
             factors.get("buy_point"),
             factors.get("stop_loss"),
             factors.get("target_price"),
             factors.get("position_advice"),
             factors.get("bias_rate"),
             factors.get("volume_ratio"),
             factors.get("turnover_rate"),
             factors.get("chip_profit_ratio"),
             factors.get("time_sensitivity"),
             _factor_json(factors)),
        )
        return cur.lastrowid

def add_to_watchlist(
    code: str,
    name: str,
    snapshot_id: Optional[int],
    run_id: Optional[str],
    factors: Optional[Dict[str, Any]],
) -> int:
    now = _now_iso()
    factors = factors or {}
    with _get_conn() as conn:
        cur = conn.execute(
            """
            INSERT INTO watchlist (
                code, name, added_from_snapshot_id, added_from_run_id, added_at,
                entry_ref_price, buy_point, stop_loss, target_price,
                initial_score, initial_advice, initial_trend,
                last_price, last_price_updated, alert_status,
                user_tags, user_notes, is_active
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'normal', '[]', '', 1)
            ON CONFLICT(code) DO UPDATE SET
                name                   = excluded.name,
                added_from_snapshot_id = COALESCE(excluded.added_from_snapshot_id, watchlist.added_from_snapshot_id),
                added_from_run_id      = COALESCE(excluded.added_from_run_id, watchlist.added_from_run_id),
                entry_ref_price        = COALESCE(excluded.entry_ref_price, watchlist.entry_ref_price),
                buy_point              = COALESCE(excluded.buy_point, watchlist.buy_point),
                stop_loss              = COALESCE(excluded.stop_loss, watchlist.stop_loss),
                target_price           = COALESCE(excluded.target_price, watchlist.target_price),
                initial_score          = COALESCE(excluded.initial_score, watchlist.initial_score),
                initial_advice         = COALESCE(excluded.initial_advice, watchlist.initial_advice),
                initial_trend          = COALESCE(excluded.initial_trend, watchlist.initial_trend),
                last_price             = COALESCE(excluded.last_price, watchlist.last_price),
                last_price_updated     = COALESCE(excluded.last_price_updated, watchlist.last_price_updated),
                is_active              = 1
            """,
            (
                code,
                name,
                snapshot_id,
                run_id,
                now,
                factors.get("current_price"),
                factors.get("buy_point"),
                factors.get("stop_loss"),
                factors.get("target_price"),
                factors.get("sentiment_score"),
                factors.get("operation_advice"),
                factors.get("trend_prediction"),
                factors.get("current_price"),
                now if factors.get("current_price") is not None else None,
            ),
        )
        return cur.lastrowid

def list_watchlist() -> List[Dict[str, Any]]:
    with _get_conn() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM watchlist
            WHERE is_active = 1
            ORDER BY added_at DESC
            """
        ).fetchall()
    result = []
    for row in rows:
        item = dict(row)
        item["user_tags"] = _load_json_list(item.get("user_tags", "[]"))
        result.append(item)
    return result

def remove_from_watchlist(code: str) -> None:
    with _get_conn() as conn:
        conn.execute(
            """
            UPDATE watchlist
            SET is_active = 0
            WHERE code = ?
            """,
            (code,),
        )

def update_watchlist_market_snapshot(
    code: str,
    last_price: Optional[float],
    alert_status: str,
    updated_at: Optional[str] = None,
) -> None:
    with _get_conn() as conn:
        conn.execute(
            """
            UPDATE watchlist
            SET last_price = ?,
                last_price_updated = ?,
                alert_status = ?
            WHERE code = ?
            """,
            (last_price, updated_at or _now_iso(), alert_status, code),
        )


def update_watchlist_alert_status(code: str, alert_status: str) -> None:
    with _get_conn() as conn:
        conn.execute(
            """
            UPDATE watchlist
            SET alert_status = ?
            WHERE code = ?
            """,
            (alert_status, code),
        )


def list_recent_runs(limit: Optional[int] = 30) -> List[Dict[str, Any]]:
    sql = """
        SELECT
            run_id,
            GROUP_CONCAT(name || '(' || code || ')', ' · ') AS stocks,
            COUNT(*)        AS stock_count,
            MAX(created_at) AS run_time,
            run_mode
        FROM analysis_snapshots
        WHERE COALESCE(is_visible, 1) = 1
        GROUP BY run_id
        ORDER BY MAX(created_at) DESC
    """
    params: List[Any] = []
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
    with _get_conn() as conn:
        rows = conn.execute(sql, tuple(params)).fetchall()
    return [dict(r) for r in rows]


def get_snapshots_with_filters(
    limit: int = 50,
    trend: Optional[str] = None,
    advice: Optional[str] = None,
    code: Optional[str] = None,
) -> List[Dict[str, Any]]:
    sql = """
        SELECT *
        FROM analysis_snapshots
        WHERE COALESCE(is_visible, 1) = 1
    """
    params: List[Any] = []

    if trend:
        sql += " AND COALESCE(trend_prediction, '') LIKE ?"
        params.append(f"%{trend}%")
    if advice:
        sql += " AND COALESCE(operation_advice, '') LIKE ?"
        params.append(f"%{advice}%")
    if code:
        keyword = code.strip()
        sql += " AND (code LIKE ? OR name LIKE ?)"
        params.extend([f"%{keyword}%", f"%{keyword}%"])

    sql += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)

    with _get_conn() as conn:
        rows = conn.execute(sql, tuple(params)).fetchall()
    return [dict(r) for r in rows]


def get_run_snapshots(run_id: str) -> List[Dict[str, Any]]:
    with _get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM analysis_snapshots WHERE run_id = ? AND COALESCE(is_visible, 1) = 1 ORDER BY code",
            (run_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def get_code_history(code: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    sql = """
        SELECT *
        FROM analysis_snapshots
        WHERE code = ?
          AND COALESCE(is_visible, 1) = 1
        ORDER BY created_at DESC
    """
    params: List[Any] = [code]
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
    with _get_conn() as conn:
        rows = conn.execute(sql, tuple(params)).fetchall()
    return [dict(r) for r in rows]


def save_run_artifacts(
    run_id: str,
    *,
    run_mode: str = "",
    market_report_md: str = "",
    stock_report_md: str = "",
    full_report_md: str = "",
    business_log: str = "",
    debug_log: str = "",
    schema_json: str = "{}",
    created_at: Optional[str] = None,
) -> None:
    now = created_at or _now_iso()
    with _get_conn() as conn:
        conn.execute(
            """
            INSERT INTO run_artifacts (
                run_id, created_at, run_mode, market_report_md, stock_report_md,
                full_report_md, business_log, debug_log, schema_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id) DO UPDATE SET
                created_at       = excluded.created_at,
                run_mode         = excluded.run_mode,
                market_report_md = excluded.market_report_md,
                stock_report_md  = excluded.stock_report_md,
                full_report_md   = excluded.full_report_md,
                business_log     = excluded.business_log,
                debug_log        = excluded.debug_log,
                schema_json      = excluded.schema_json
            """,
            (
                run_id,
                now,
                run_mode,
                market_report_md,
                stock_report_md,
                full_report_md,
                business_log,
                debug_log,
                schema_json,
            ),
        )


def get_run_artifacts(run_id: str) -> Optional[Dict[str, Any]]:
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM run_artifacts WHERE run_id = ?",
            (run_id,),
        ).fetchone()
    return dict(row) if row else None


def get_snapshot_detail(snapshot_id: int) -> Optional[Dict[str, Any]]:
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM analysis_snapshots WHERE id = ? AND COALESCE(is_visible, 1) = 1",
            (snapshot_id,),
        ).fetchone()
    return dict(row) if row else None


def delete_snapshot(snapshot_id: int) -> None:
    with _get_conn() as conn:
        conn.execute(
            "UPDATE analysis_snapshots SET is_visible = 0 WHERE id = ?",
            (snapshot_id,),
        )


def delete_run_permanently(run_id: str) -> None:
    with _get_conn() as conn:
        conn.execute("DELETE FROM analysis_snapshots WHERE run_id = ?", (run_id,))
        conn.execute("DELETE FROM run_artifacts WHERE run_id = ?", (run_id,))


def clear_all_data() -> None:
    with _get_conn() as conn:
        conn.execute("DELETE FROM analysis_snapshots")
        conn.execute("DELETE FROM watchlist")
        conn.execute("DELETE FROM quick_pool")


def add_to_quick_pool(code: str, name: str) -> int:
    now = _now_iso()
    with _get_conn() as conn:
        cur = conn.execute(
            """
            INSERT INTO quick_pool (code, name, added_at)
            VALUES (?, ?, ?)
            ON CONFLICT(code) DO UPDATE SET
                name = excluded.name,
                added_at = excluded.added_at
            """,
            (code, name, now),
        )
        return cur.lastrowid


def list_quick_pool() -> List[Dict[str, Any]]:
    with _get_conn() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM quick_pool
            ORDER BY added_at DESC
            """
        ).fetchall()
    return [dict(r) for r in rows]


def clear_quick_pool() -> None:
    with _get_conn() as conn:
        conn.execute("DELETE FROM quick_pool")


def list_tracked_codes() -> List[Dict[str, Any]]:
    with _get_conn() as conn:
        rows = conn.execute(
            """
            SELECT
                code,
                MAX(name)            AS name,
                COUNT(*)             AS total_runs,
                MAX(created_at)      AS last_run,
                MAX(sentiment_score) AS last_score,
                MAX(CASE WHEN created_at = (
                    SELECT MAX(s2.created_at) FROM analysis_snapshots s2
                    WHERE s2.code = analysis_snapshots.code
                ) THEN operation_advice END) AS last_advice
            FROM analysis_snapshots
            WHERE COALESCE(is_visible, 1) = 1
            GROUP BY code
            ORDER BY MAX(created_at) DESC
            """
        ).fetchall()
    return [dict(r) for r in rows]
