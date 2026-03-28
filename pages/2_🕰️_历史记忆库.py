# -*- coding: utf-8 -*-
import json
import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from src.config import setup_env

setup_env()

import streamlit as st

from src.streamlit_guard import enforce_sidebar_password_gate
from webui.db import (
    add_to_quick_pool,
    clear_all_data,
    delete_snapshot,
    get_code_history,
    get_run_artifacts,
    get_run_snapshots,
    get_snapshots_with_filters,
    init_db,
    list_recent_runs,
)

init_db()

if "history_delete_pending_id" not in st.session_state:
    st.session_state.history_delete_pending_id = None
if "confirm_clear_all_data" not in st.session_state:
    st.session_state.confirm_clear_all_data = False

st.set_page_config(
    page_title="历史记忆库 · DUKA Stock Analysis Engine V5-Pro",
    page_icon="🕰️",
    layout="wide",
)
enforce_sidebar_password_gate()


def _fmt_dt(value: Optional[str]) -> str:
    if not value:
        return "—"
    try:
        return datetime.fromisoformat(value).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(value)[:16]


def _fmt_price(value: Optional[float]) -> str:
    try:
        return f"{float(value):.2f}" if value not in (None, "") else "—"
    except Exception:
        return "—"


def _fmt_pct(value: Optional[float]) -> str:
    try:
        return f"{float(value):.1f}%" if value not in (None, "") else "—"
    except Exception:
        return "—"


def _fmt_score(value: Optional[float]) -> str:
    try:
        return f"{float(value):.0f}" if value not in (None, "") else "—"
    except Exception:
        return "—"


def _safe_json_loads(raw: Optional[str]) -> Dict[str, Any]:
    try:
        data = json.loads(raw or "{}")
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _resolve_precise_code(keyword: str, snapshots: List[Dict[str, Any]]) -> Optional[str]:
    q = (keyword or "").strip()
    if not q:
        return None
    exact = [s["code"] for s in snapshots if s.get("code") == q]
    if exact:
        return exact[0]
    by_name = [s["code"] for s in snapshots if s.get("name") == q]
    unique = list(dict.fromkeys(by_name))
    return unique[0] if len(unique) == 1 else None


def _render_tags(snap: Dict[str, Any]) -> None:
    advice = snap.get("operation_advice") or "未评级"
    trend = snap.get("trend_prediction") or "趋势待补"
    ma = snap.get("ma_alignment") or "均线待补"
    bias = _fmt_pct(snap.get("bias_rate"))
    st.caption(f"{advice} | {trend} | {ma} | 乖离率 {bias}")


def _render_factor_table(snap: Dict[str, Any]) -> None:
    rows = [{
        "买点": _fmt_price(snap.get("buy_point")),
        "目标价": _fmt_price(snap.get("target_price")),
        "止损位": _fmt_price(snap.get("stop_loss")),
        "评分": _fmt_score(snap.get("sentiment_score")),
    }]
    st.dataframe(rows, use_container_width=True, hide_index=True)


def _render_snapshot_card(snap: Dict[str, Any], prefix: str) -> None:
    is_market = snap.get("code") == "__market__"
    title = "📊 大盘报告" if is_market else f"{snap.get('name') or '—'}（{snap.get('code') or '—'}）"
    cols = st.columns([4, 1.2, 1.6, 1.1] if is_market else [3.5, 1.1, 1.5, 0.9, 1.1])

    with cols[0]:
        st.markdown(f"**{title}**")
    with cols[1]:
        st.caption(f"评分 {_fmt_score(snap.get('sentiment_score'))}")
    with cols[2]:
        st.caption(_fmt_dt(snap.get("created_at")))

    if not is_market:
        with cols[3]:
            if st.button("📌", key=f"push_{prefix}_{snap['id']}", help="送入首页分析池", type="secondary"):
                add_to_quick_pool(snap["code"], snap["name"])
                st.toast(f"{snap['name']} 已送入首页分析池")
        delete_col = cols[4]
    else:
        delete_col = cols[3]

    with delete_col:
        if st.button("🙈 隐藏报告", key=f"del_{prefix}_{snap['id']}", type="secondary"):
            st.session_state.history_delete_pending_id = snap["id"]
            st.rerun()

    if not is_market:
        _render_tags(snap)
        _render_factor_table(snap)

    if st.session_state.history_delete_pending_id == snap["id"]:
        st.warning("确认后该报告将从历史列表中隐藏，但不会物理删除数据库记录。")
        action_cols = st.columns([1.2, 4.8])
        with action_cols[0]:
            if st.button("确认隐藏", key=f"confirm_del_{prefix}_{snap['id']}", type="secondary"):
                delete_snapshot(snap["id"])
                st.session_state.history_delete_pending_id = None
                st.rerun()
        with action_cols[1]:
            if st.button("取消", key=f"cancel_del_{prefix}_{snap['id']}", type="secondary"):
                st.session_state.history_delete_pending_id = None
                st.rerun()

    with st.expander("查看完整报告", expanded=False):
        st.markdown(snap.get("report_md") or "_无报告内容_")


def _build_batch_summary_table(batch_snaps: List[Dict[str, Any]]) -> None:
    stock_rows = [snap for snap in batch_snaps if snap.get("code") != "__market__"]
    if not stock_rows:
        st.caption("该批次暂无个股汇总数据。")
        return

    summary_rows = [{
        "代码": snap.get("code") or "—",
        "名称": snap.get("name") or "—",
        "评分": _fmt_score(snap.get("sentiment_score")),
        "评级": snap.get("operation_advice") or "—",
        "趋势": snap.get("trend_prediction") or "—",
        "买点": _fmt_price(snap.get("buy_point")),
        "目标价": _fmt_price(snap.get("target_price")),
        "止损位": _fmt_price(snap.get("stop_loss")),
    } for snap in stock_rows]
    st.dataframe(summary_rows, use_container_width=True, hide_index=True)


def _build_batch_download_text(run_id: str, batch_snaps: List[Dict[str, Any]]) -> str:
    lines = [f"# Match5 历史批次报告", f"run_id: {run_id}", ""]
    market_reports = [snap for snap in batch_snaps if snap.get("code") == "__market__"]
    stock_reports = [snap for snap in batch_snaps if snap.get("code") != "__market__"]

    if market_reports:
        lines.extend(["## 大盘报告", ""])
        for snap in market_reports:
            lines.extend([snap.get("report_md") or "_无报告内容_", ""])

    if stock_reports:
        lines.extend(["## 个股报告", ""])
        for snap in stock_reports:
            lines.extend([
                f"### {snap.get('name') or '—'}（{snap.get('code') or '—'}）",
                snap.get("report_md") or "_无报告内容_",
                "",
            ])

    return "\n".join(lines).strip() + "\n"


def _build_batch_debug_log(run_id: str, batch_snaps: List[Dict[str, Any]]) -> str:
    chunks = [f"run_id={run_id}", f"snapshot_count={len(batch_snaps)}", ""]
    for snap in batch_snaps:
        chunks.extend([
            f"[snapshot:{snap.get('id')}] {snap.get('name')} ({snap.get('code')})",
            f"created_at={snap.get('created_at')}",
            f"sentiment_score={snap.get('sentiment_score')}",
            f"operation_advice={snap.get('operation_advice')}",
            f"trend_prediction={snap.get('trend_prediction')}",
            f"ma_alignment={snap.get('ma_alignment')}",
            f"factors_json={snap.get('factors_json') or '{}'}",
            "",
            snap.get("report_md") or "",
            "",
            "-" * 80,
        ])
    return "\n".join(chunks)


def _build_batch_schema_json(run_id: str, batch_snaps: List[Dict[str, Any]]) -> str:
    payload = {
        "run_id": run_id,
        "created_at": max((snap.get("created_at") or "" for snap in batch_snaps), default=""),
        "snapshots": [{
            "id": snap.get("id"),
            "run_id": snap.get("run_id"),
            "code": snap.get("code"),
            "name": snap.get("name"),
            "created_at": snap.get("created_at"),
            "sentiment_score": snap.get("sentiment_score"),
            "operation_advice": snap.get("operation_advice"),
            "trend_prediction": snap.get("trend_prediction"),
            "ma_alignment": snap.get("ma_alignment"),
            "buy_point": snap.get("buy_point"),
            "stop_loss": snap.get("stop_loss"),
            "target_price": snap.get("target_price"),
            "factors": _safe_json_loads(snap.get("factors_json")),
        } for snap in batch_snaps],
    }
    return json.dumps(payload, ensure_ascii=False, indent=2)


def _render_batch_reports(run_id: str, batch_snaps: List[Dict[str, Any]]) -> None:
    artifacts = get_run_artifacts(run_id) or {}
    market_reports = [snap for snap in batch_snaps if snap.get("code") == "__market__"]
    stock_reports = [snap for snap in batch_snaps if snap.get("code") != "__market__"]
    market_report_md = (artifacts.get("market_report_md") or "").strip()
    stock_report_md = (artifacts.get("stock_report_md") or "").strip()
    full_report_md = (artifacts.get("full_report_md") or "").strip()
    business_log = (artifacts.get("business_log") or "").strip()
    debug_log = (artifacts.get("debug_log") or "").strip()
    schema_json = (artifacts.get("schema_json") or "").strip()

    st.divider()
    st.markdown("### 报告区")

    if market_report_md or market_reports:
        with st.expander("📊 大盘报告", expanded=False):
            if market_report_md:
                st.markdown(market_report_md)
            else:
                for idx, snap in enumerate(market_reports, start=1):
                    if len(market_reports) > 1:
                        st.caption(f"版本 {idx} | {_fmt_dt(snap.get('created_at'))}")
                    st.markdown(snap.get("report_md") or "_无报告内容_")
                    if idx < len(market_reports):
                        st.divider()

    if stock_report_md or stock_reports:
        with st.expander("🧾 个股报告", expanded=False):
            if stock_report_md:
                st.markdown(stock_report_md)
            else:
                for idx, snap in enumerate(stock_reports, start=1):
                    st.markdown(f"**{snap.get('name') or '—'}（{snap.get('code') or '—'}）**")
                    st.markdown(snap.get("report_md") or "_无报告内容_")
                    if idx < len(stock_reports):
                        st.divider()

    st.download_button(
        "⬇️ 下载全量报告",
        data=(full_report_md or _build_batch_download_text(run_id, batch_snaps)),
        file_name=f"match5_batch_{run_id}.md",
        mime="text/markdown",
        use_container_width=True,
    )

    with st.expander("🔒 机要区 (调试日志与 JSON)", expanded=False):
        debug_col, json_col = st.columns(2)
        with debug_col:
            st.download_button(
                "下载 API 原始 Log",
                data=(business_log or debug_log or _build_batch_debug_log(run_id, batch_snaps)),
                file_name=f"match5_batch_{run_id}_debug.log",
                mime="text/plain",
                use_container_width=True,
            )
        with json_col:
            st.download_button(
                "下载 Schema JSON",
                data=(schema_json or _build_batch_schema_json(run_id, batch_snaps)),
                file_name=f"match5_batch_{run_id}_schema.json",
                mime="application/json",
                use_container_width=True,
            )


def _render_batch_expanders(filtered_snaps: List[Dict[str, Any]], recent_limit: int) -> None:
    match_ids = {snap["id"] for snap in filtered_snaps}
    run_rows = list_recent_runs(limit=max(recent_limit, 60))
    ordered_run_ids: List[str] = []
    for row in run_rows:
        run_id = row.get("run_id")
        if not run_id:
            continue
        run_snaps = get_run_snapshots(run_id)
        if any(snap.get("id") in match_ids for snap in run_snaps):
            ordered_run_ids.append(run_id)

    seen = set()
    ordered_run_ids = [rid for rid in ordered_run_ids if not (rid in seen or seen.add(rid))]
    if not ordered_run_ids:
        st.info("当前过滤条件下没有命中的分析批次。")
        return

    with st.expander("🕒 按次分组的分析批次", expanded=False):
        for run_id in ordered_run_ids:
            batch_snaps = get_run_snapshots(run_id)
            batch_snaps = sorted(
                batch_snaps,
                key=lambda snap: (snap.get("code") == "__market__", snap.get("code") or ""),
            )
            visible_stock_count = len([snap for snap in batch_snaps if snap.get("code") != "__market__"])
            batch_time = _fmt_dt(max((snap.get("created_at") for snap in batch_snaps if snap.get("created_at")), default=""))
            label = f"🕒 分析批次：{batch_time} | 包含 {visible_stock_count} 只标的"
            with st.expander(label, expanded=False):
                st.caption(f"run_id: {run_id}")
                _build_batch_summary_table(batch_snaps)
                st.divider()
                for idx, snap in enumerate(batch_snaps, start=1):
                    _render_snapshot_card(snap, prefix=f"batch_{run_id}_{idx}")
                    if idx < len(batch_snaps):
                        st.divider()
                _render_batch_reports(run_id, batch_snaps)


with st.sidebar:
    st.markdown("## 过滤器")
    advice_filter = st.selectbox("评级过滤", ["全部", "买入", "观望", "卖出"], index=0)
    trend_filter = st.selectbox("趋势过滤", ["全部", "看多", "震荡", "看空"], index=0)
    keyword = st.text_input("股票搜索", placeholder="输入股票代码或名称")
    limit = st.slider("最近记录条数", min_value=20, max_value=200, value=60, step=10)

    with st.expander("⚠️ 危险操作区", expanded=False):
        if st.button("🗑️ 清空所有历史与跟踪数据", use_container_width=True):
            st.session_state.confirm_clear_all_data = True
        if st.session_state.confirm_clear_all_data:
            st.warning("此操作将清空历史快照与跟踪池数据，且不可恢复。")
            if st.button("🚨 确认执行清空（不可逆）", use_container_width=True, type="primary"):
                clear_all_data()
                st.session_state.confirm_clear_all_data = False
                st.success("数据已全部清空")
                st.rerun()


st.markdown("# 🕰️ 历史记忆库")
st.caption("支持按分析批次分组回看，也支持精确锁定单只股票进入时光轴复盘。")
st.divider()

trend_value = None if trend_filter == "全部" else trend_filter
advice_value = None if advice_filter == "全部" else advice_filter

snapshots = get_snapshots_with_filters(
    limit=limit,
    trend=trend_value,
    advice=advice_value,
    code=keyword or None,
)

if not snapshots:
    st.info("当前过滤条件下没有命中的历史快照。")
    st.stop()

precise_code = _resolve_precise_code(keyword, snapshots)

if precise_code:
    history = get_code_history(precise_code)
    stock_name = history[0].get("name") if history else precise_code
    st.markdown(f"## ⏱️ {stock_name}（{precise_code}）时光轴复盘")
    st.caption("纵向对比历史分析中的评分、评级和均线状态变化。")

    if not history:
        st.info("暂无该股票历史记录。")
        st.stop()

    version_options = {
        f"{_fmt_dt(s.get('created_at'))} | 评分 {_fmt_score(s.get('sentiment_score'))} | {s.get('operation_advice') or '—'}": s
        for s in history
    }
    selected_version = st.selectbox("🎞️ 切换历史报告版本", list(version_options.keys()), index=0)
    selected_snapshot = version_options[selected_version]

    with st.expander("🗂️ 查看当前选中版本完整报告", expanded=True):
        st.markdown(selected_snapshot.get("report_md") or "_无报告内容_")

    st.divider()

    for idx, snap in enumerate(history, start=1):
        _render_snapshot_card(snap, prefix=f"timeline_{idx}")
        if idx < len(history):
            st.divider()
else:
    st.caption("未精确锁定单只股票时，历史结果会按分析批次折叠显示。")
    _render_batch_expanders(snapshots, recent_limit=limit)
