# -*- coding: utf-8 -*-
"""
pages/3_📈_标的跟踪池.py
=========================
V9.0 标的跟踪池（盯盘雷达）
"""

import os
import sys
from datetime import datetime
from typing import Dict, Optional, Tuple

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from src.config import setup_env

setup_env()

import streamlit as st

from src.notification import NotificationService
from src.services.stock_service import StockService
from webui.db import (
    init_db,
    list_watchlist,
    remove_from_watchlist,
    update_watchlist_alert_status,
    update_watchlist_market_snapshot,
)

try:
    from streamlit_autorefresh import st_autorefresh
except Exception:
    st_autorefresh = None

init_db()

st.set_page_config(
    page_title="标的跟踪池 · A股智能分析站",
    page_icon="📈",
    layout="wide",
)

_STATUS_LABELS: Dict[str, str] = {
    "normal": "⚪ 监控中",
    "target_hit": "🔴 达成目标 (待推送)",
    "target_notified": "🔴 达成目标 (已通知)",
    "stop_hit": "🟢 跌破止损 (待推送)",
    "stop_notified": "🟢 跌破止损 (已通知)",
}


def _fmt_price(value: Optional[float]) -> str:
    if value is None:
        return "—"
    try:
        return f"{float(value):.2f}"
    except Exception:
        return "—"


def _fmt_dt(value: Optional[str]) -> str:
    if not value:
        return "—"
    try:
        return datetime.fromisoformat(value).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(value)[:16]


def _safe_float(value: Optional[float]) -> Optional[float]:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _status_label(status: Optional[str], last_price: Optional[float], target_price: Optional[float], stop_loss: Optional[float]) -> str:
    if status in _STATUS_LABELS:
        return _STATUS_LABELS[status]
    if last_price is None:
        return _STATUS_LABELS["normal"]
    if target_price is not None and last_price >= target_price:
        return _STATUS_LABELS["target_notified"]
    if stop_loss is not None and last_price <= stop_loss:
        return _STATUS_LABELS["stop_notified"]
    return _STATUS_LABELS["normal"]


def _build_alert_message(item: Dict, event_type: str, last_price: float) -> str:
    code = item.get("code", "")
    name = item.get("name", code)
    if event_type == "target_hit":
        target_price = _safe_float(item.get("target_price"))
        return (
            f"## 🚨 止盈触发\n\n"
            f"**{name} ({code})**\n\n"
            f"- 实时现价：`{last_price:.2f}` 元\n"
            f"- 目标价：`{target_price:.2f}` 元\n\n"
            f"现价已突破目标价，请注意盯盘。"
        )
    stop_loss = _safe_float(item.get("stop_loss"))
    return (
        f"## ⚠️ 止损警告\n\n"
        f"**{name} ({code})**\n\n"
        f"- 实时现价：`{last_price:.2f}` 元\n"
        f"- 止损价：`{stop_loss:.2f}` 元\n\n"
        f"现价已跌破止损价，请严格执行纪律。"
    )


def _resolve_transition(item: Dict, last_price: Optional[float]) -> Tuple[str, Optional[str]]:
    target_price = _safe_float(item.get("target_price"))
    stop_loss = _safe_float(item.get("stop_loss"))
    current_status = item.get("alert_status") or "normal"

    if last_price is None:
        return current_status, None

    if target_price is not None and last_price >= target_price:
        if current_status in {"target_hit", "target_notified"}:
            return current_status, None
        if current_status in {"stop_hit", "stop_notified"}:
            return current_status, None
        return "target_hit", "target_hit"

    if stop_loss is not None and last_price <= stop_loss:
        if current_status in {"stop_hit", "stop_notified"}:
            return current_status, None
        if current_status in {"target_hit", "target_notified"}:
            return current_status, None
        return "stop_hit", "stop_hit"

    return current_status if current_status in _STATUS_LABELS else "normal", None


def _refresh_watchlist_quotes(items):
    svc = StockService()
    notifier = NotificationService()
    refreshed = 0
    failed = 0
    notified = 0

    for item in items:
        code = item.get("code")
        try:
            quote = svc.get_realtime_quote(code)
            last_price = _safe_float(quote.get("current_price") if quote else None)
            next_status, notify_event = _resolve_transition(item, last_price)

            if last_price is None:
                update_watchlist_market_snapshot(code, None, next_status)
                refreshed += 1
                continue

            if notify_event:
                update_watchlist_market_snapshot(code, last_price, next_status)
                if notifier.is_available():
                    content = _build_alert_message(item, notify_event, last_price)
                    sent_ok = notifier.send(content, email_stock_codes=[code])
                    if sent_ok:
                        next_status = "target_notified" if notify_event == "target_hit" else "stop_notified"
                        update_watchlist_alert_status(code, next_status)
                        notified += 1
                refreshed += 1
                continue

            update_watchlist_market_snapshot(code, last_price, next_status)
            refreshed += 1
        except Exception:
            failed += 1

    return refreshed, failed, notified, notifier.is_available()


st.markdown("# 📈 标的跟踪池")
st.caption("已加入跟踪池的标的会在这里进行批量巡航、价格对比与止盈止损报警。")
st.divider()

watch_items = list_watchlist()

ctrl1, ctrl2, ctrl3 = st.columns([2.3, 2.2, 4.5])
with ctrl1:
    refresh_clicked = st.button("🔄 一键全量巡航", use_container_width=True, type="primary")
with ctrl2:
    auto_cruise = st.checkbox("开启盘中自动巡航（60秒/次）", value=False)
with ctrl3:
    if auto_cruise and st_autorefresh is None:
        st.info("当前环境未安装 `streamlit_autorefresh`，已保留稳妥的手动全量巡航按钮。")
    else:
        st.caption("巡航将批量刷新现价，并在首次触发目标/止损时走系统原生推送通道。")

auto_tick = 0
if auto_cruise and st_autorefresh is not None:
    auto_tick = st_autorefresh(interval=60_000, key="watchlist_auto_cruise")

if (refresh_clicked or (auto_cruise and auto_tick)) and watch_items:
    with st.spinner("正在批量巡航跟踪池，刷新现价并检查报警…"):
        refreshed, failed, notified, notify_enabled = _refresh_watchlist_quotes(watch_items)
    msg = f"✅ 已巡航 {refreshed} 只标的"
    if failed:
        msg += f"，失败 {failed} 只"
    if notified:
        msg += f"，触发并推送 {notified} 条报警"
    elif refresh_clicked and not notify_enabled:
        msg += "，当前未检测到已配置的通知渠道"
    st.success(msg)
    st.rerun()

watch_items = list_watchlist()

if not watch_items:
    st.info("📭 跟踪池为空。请先在「🏠 快速分析」页将股票加入跟踪池。")
    st.stop()

header_cols = st.columns([1.15, 1.35, 1.05, 1.0, 1.0, 1.0, 1.0, 1.2, 1.15, 0.9])
headers = [
    "代码 / 名称",
    "入池时间 / 评级",
    "入池参考价",
    "理想买点",
    "🎯目标价",
    "🛑止损价",
    "实时现价",
    "状态报警",
    "更新时间",
    "操作",
]
for col, label in zip(header_cols, headers):
    with col:
        st.markdown(f"**{label}**")

st.divider()

for item in watch_items:
    row_cols = st.columns([1.15, 1.35, 1.05, 1.0, 1.0, 1.0, 1.0, 1.2, 1.15, 0.9])
    with row_cols[0]:
        st.markdown(f"**{item.get('code', '—')}**  \n{item.get('name', '—')}")
    with row_cols[1]:
        st.markdown(f"{_fmt_dt(item.get('added_at'))}  \n{item.get('initial_advice') or '—'}")
    with row_cols[2]:
        st.write(_fmt_price(_safe_float(item.get("entry_ref_price"))))
    with row_cols[3]:
        st.write(_fmt_price(_safe_float(item.get("buy_point"))))
    with row_cols[4]:
        st.write(_fmt_price(_safe_float(item.get("target_price"))))
    with row_cols[5]:
        st.write(_fmt_price(_safe_float(item.get("stop_loss"))))
    with row_cols[6]:
        st.write(_fmt_price(_safe_float(item.get("last_price"))))
    with row_cols[7]:
        st.write(
            _status_label(
                item.get("alert_status"),
                _safe_float(item.get("last_price")),
                _safe_float(item.get("target_price")),
                _safe_float(item.get("stop_loss")),
            )
        )
    with row_cols[8]:
        st.write(_fmt_dt(item.get("last_price_updated")))
    with row_cols[9]:
        if st.button("移出", key=f"rm_watch_{item['code']}", use_container_width=True):
            remove_from_watchlist(item["code"])
            st.success(f"✅ 已将 {item['name']}（{item['code']}）移出跟踪池")
            st.rerun()

st.divider()
st.caption("说明：若目标价或止损价为空（None），巡航时会自动跳过对应比较；同一只股票命中同一类条件后会进入已锁定状态，不会重复推送。")
