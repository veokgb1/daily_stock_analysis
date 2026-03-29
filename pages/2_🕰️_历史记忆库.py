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

st.set_page_config(
    page_title="历史记忆库 · DUKA Stock Analysis Engine V5-Pro",
    page_icon="🕰️",
    layout="wide",
)
enforce_sidebar_password_gate()

st.session_state.setdefault("selected_run_id", None)
st.session_state.setdefault("report_view_mode", "全量模式")
st.session_state.setdefault("report_preset", "快速决策组合")
st.session_state.setdefault("hide_pending_snap_id", None)
st.session_state.setdefault("confirm_clear_all_data", False)

PRESET_OPTIONS = ["快速决策组合", "风险审查组合", "数据复盘组合"]


def _fmt_dt(value: Optional[str]) -> str:
    if not value:
        return "—"
    try:
        return datetime.fromisoformat(value).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(value)[:16]


def _fmt_price(value: Any) -> str:
    try:
        return f"{float(value):.2f}" if value not in (None, "") else "—"
    except Exception:
        return "—"


def _fmt_pct(value: Any) -> str:
    try:
        return f"{float(value):.1f}%" if value not in (None, "") else "—"
    except Exception:
        return "—"


def _fmt_score(value: Any) -> str:
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


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _resolve_precise_code(keyword: str, snapshots: List[Dict[str, Any]]) -> Optional[str]:
    q = _normalize_text(keyword)
    if not q:
        return None
    exact_code = [snap["code"] for snap in snapshots if _normalize_text(snap.get("code")) == q]
    if exact_code:
        return exact_code[0]
    exact_name = [snap["code"] for snap in snapshots if _normalize_text(snap.get("name")) == q]
    unique = list(dict.fromkeys(exact_name))
    return unique[0] if len(unique) == 1 else None


def _infer_run_time(batch_snaps: List[Dict[str, Any]]) -> str:
    timestamps = [snap.get("created_at") for snap in batch_snaps if snap.get("created_at")]
    return _fmt_dt(max(timestamps) if timestamps else None)


def _infer_run_date_slug(batch_snaps: List[Dict[str, Any]]) -> str:
    timestamps = [snap.get("created_at") for snap in batch_snaps if snap.get("created_at")]
    if not timestamps:
        return datetime.now().strftime("%Y%m%d_%H%M%S")
    try:
        return datetime.fromisoformat(max(timestamps)).strftime("%Y%m%d_%H%M%S")
    except Exception:
        return datetime.now().strftime("%Y%m%d_%H%M%S")


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


def _build_stock_report_text(stock_snaps: List[Dict[str, Any]]) -> str:
    lines: List[str] = []
    for snap in stock_snaps:
        lines.extend([
            f"## {snap.get('name') or '—'}（{snap.get('code') or '—'}）",
            "",
            snap.get("report_md") or "_无报告内容_",
            "",
        ])
    return "\n".join(lines).strip() + ("\n" if lines else "")


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
        "snapshots": [
            {
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
            }
            for snap in batch_snaps
        ],
    }
    return json.dumps(payload, ensure_ascii=False, indent=2)


def _get_batch_artifacts_payload(run_id: str, batch_snaps: List[Dict[str, Any]]) -> Dict[str, str]:
    artifacts = get_run_artifacts(run_id) or {}
    stock_snaps = [snap for snap in batch_snaps if snap.get("code") != "__market__"]
    market_report_fallback = "\n\n".join(
        snap.get("report_md") or "" for snap in batch_snaps if snap.get("code") == "__market__"
    ).strip()
    stock_report_fallback = _build_stock_report_text(stock_snaps)
    full_report_fallback = _build_batch_download_text(run_id, batch_snaps)
    return {
        "market_report_md": _normalize_text(artifacts.get("market_report_md")) or market_report_fallback,
        "stock_report_md": _normalize_text(artifacts.get("stock_report_md")) or stock_report_fallback,
        "full_report_md": _normalize_text(artifacts.get("full_report_md")) or full_report_fallback,
        "business_log": _normalize_text(artifacts.get("business_log")),
        "debug_log": _normalize_text(artifacts.get("debug_log")),
        "schema_json": _normalize_text(artifacts.get("schema_json")) or _build_batch_schema_json(run_id, batch_snaps),
    }


def _format_advice_label(advice: str) -> str:
    text = _normalize_text(advice) or "未评级"
    if any(word in text for word in ("买", "多", "增持", "看涨")):
        return f"**🟢 {text}**"
    if any(word in text for word in ("卖", "空", "减持", "离场")):
        return f"**🔴 {text}**"
    return f"**🟡 {text}**"


def _format_trend_label(trend: str) -> str:
    text = _normalize_text(trend) or "趋势待补"
    if any(word in text for word in ("多", "上行", "强势")):
        return f"**🟢 {text}**"
    if any(word in text for word in ("空", "下行", "弱势")):
        return f"**🔴 {text}**"
    return f"**🟡 {text}**"


def _extract_risk_alerts(snap: Dict[str, Any], factors: Dict[str, Any]) -> List[str]:
    alerts: List[str] = []
    for key in ("risk_alerts", "alerts", "warnings"):
        value = factors.get(key)
        if isinstance(value, list):
            alerts.extend(_normalize_text(item) for item in value if _normalize_text(item))
    for key in ("time_sensitivity", "ma_alignment", "position_advice"):
        value = _normalize_text(snap.get(key) or factors.get(key))
        if value:
            alerts.append(value)

    deduped: List[str] = []
    seen = set()
    for item in alerts:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped[:2]


def _render_data_block(snap: Dict[str, Any], factors: Dict[str, Any]) -> None:
    data_row = {
        "收盘价": _fmt_price(snap.get("current_price") or factors.get("current_price")),
        "MA5": _fmt_price(factors.get("ma5")),
        "MA10": _fmt_price(factors.get("ma10")),
        "MA20": _fmt_price(factors.get("ma20")),
        "乖离率(MA5)": _fmt_pct(snap.get("bias_rate") or factors.get("bias_rate")),
        "量比": _fmt_pct(snap.get("volume_ratio") or factors.get("volume_ratio")),
        "换手率": _fmt_pct(snap.get("turnover_rate") or factors.get("turnover_rate")),
        "支撑位": _fmt_price(factors.get("support") or factors.get("support_level")),
        "压力位": _fmt_price(factors.get("resistance") or factors.get("resistance_level")),
    }
    st.caption("📊 数据维度")
    st.dataframe([data_row], use_container_width=True, hide_index=True)


def _render_judge_block(snap: Dict[str, Any], factors: Dict[str, Any]) -> None:
    st.caption("🎯 判断维度")
    judge_cols = st.columns(3)
    with judge_cols[0]:
        st.markdown(_format_advice_label(_normalize_text(snap.get("operation_advice"))))
        st.markdown(_format_trend_label(_normalize_text(snap.get("trend_prediction"))))
    with judge_cols[1]:
        st.write(f"均线排列：{_normalize_text(snap.get('ma_alignment')) or '—'}")
        st.write(f"多头排列：{_normalize_text(factors.get('bullish_alignment')) or '—'}")
    with judge_cols[2]:
        st.write(f"趋势强度分：{_fmt_score(factors.get('trend_strength_score'))}")
        st.write(f"情绪评分：{_fmt_score(snap.get('sentiment_score'))}")
        st.write(f"舆情情绪：{_normalize_text(factors.get('news_sentiment')) or '—'}")
    risk_alerts = _extract_risk_alerts(snap, factors)
    if risk_alerts:
        for alert in risk_alerts:
            st.warning(alert)
    else:
        st.caption("暂无风险警报。")


def _render_action_block(snap: Dict[str, Any], factors: Dict[str, Any]) -> None:
    st.caption("📋 操作维度")
    action_cols = st.columns(4)
    action_data = [
        ("理想买入点", _fmt_price(snap.get("buy_point") or factors.get("buy_point"))),
        ("次优买入点", _fmt_price(factors.get("secondary_buy_point") or factors.get("backup_buy_point"))),
        ("止损位", _fmt_price(snap.get("stop_loss") or factors.get("stop_loss"))),
        ("目标位", _fmt_price(snap.get("target_price") or factors.get("target_price"))),
    ]
    for col, (label, value) in zip(action_cols, action_data):
        with col:
            st.metric(label, value)
    st.write(f"仓位建议：{_normalize_text(snap.get('position_advice') or factors.get('position_advice')) or '—'}")
    decision = (
        _normalize_text(factors.get("one_line_decision"))
        or _normalize_text(snap.get("position_advice"))
        or _normalize_text(snap.get("operation_advice"))
        or "暂无一句话决策"
    )
    st.info(f"一句话决策：{decision}")


def _render_quick_decision_preset(snap: Dict[str, Any], factors: Dict[str, Any]) -> None:
    st.caption("快速决策组合")
    st.markdown(_format_advice_label(_normalize_text(snap.get("operation_advice"))))
    st.markdown(_format_trend_label(_normalize_text(snap.get("trend_prediction"))))
    quick_cols = st.columns(3)
    quick_metrics = [
        ("买入点", _fmt_price(snap.get("buy_point") or factors.get("buy_point"))),
        ("止损位", _fmt_price(snap.get("stop_loss") or factors.get("stop_loss"))),
        ("目标位", _fmt_price(snap.get("target_price") or factors.get("target_price"))),
    ]
    for col, (label, value) in zip(quick_cols, quick_metrics):
        with col:
            st.metric(label, value)
    decision = (
        _normalize_text(factors.get("one_line_decision"))
        or _normalize_text(snap.get("position_advice"))
        or "暂无一句话决策"
    )
    st.info(f"一句话决策：{decision}")


def _render_risk_review_preset(snap: Dict[str, Any], factors: Dict[str, Any]) -> None:
    st.caption("风险审查组合")
    _render_judge_block(snap, factors)
    risk_cols = st.columns(2)
    with risk_cols[0]:
        st.metric("止损位", _fmt_price(snap.get("stop_loss") or factors.get("stop_loss")))
    with risk_cols[1]:
        st.metric("仓位建议", _normalize_text(snap.get("position_advice") or factors.get("position_advice")) or "—")


def _render_data_review_preset(snap: Dict[str, Any], factors: Dict[str, Any]) -> None:
    st.caption("数据复盘组合")
    _render_data_block(snap, factors)
    st.write(f"均线排列：{_normalize_text(snap.get('ma_alignment')) or '—'}")
    st.write(f"趋势强度分：{_fmt_score(factors.get('trend_strength_score'))}")


def _render_card_actions(snap: Dict[str, Any], prefix: str) -> None:
    action_cols = st.columns([1.2, 1.2, 3.6])
    with action_cols[0]:
        if snap.get("code") != "__market__":
            if st.button("📌 跟踪", key=f"track_{prefix}_{snap['id']}", use_container_width=True):
                add_to_quick_pool(snap["code"], snap["name"])
                st.toast(f"{snap['name']} 已加入跟踪池")
    with action_cols[1]:
        if st.button("👁️ 隐藏当前", key=f"hide_{prefix}_{snap['id']}", use_container_width=True):
            st.session_state["hide_pending_snap_id"] = snap["id"]
            st.rerun()
    with action_cols[2]:
        with st.expander("查看原始 Markdown 报告", expanded=False):
            st.markdown(snap.get("report_md") or "_无报告内容_")

    if st.session_state.get("hide_pending_snap_id") == snap["id"]:
        st.warning("确认后，该条历史报告将从列表中隐藏，但不会物理删除数据库记录。")
        confirm_cols = st.columns([1.4, 1.4, 3.2])
        with confirm_cols[0]:
            if st.button("确认隐藏", key=f"confirm_hide_{prefix}_{snap['id']}", use_container_width=True):
                delete_snapshot(snap["id"])
                st.session_state["hide_pending_snap_id"] = None
                st.rerun()
        with confirm_cols[1]:
            if st.button("取消", key=f"cancel_hide_{prefix}_{snap['id']}", use_container_width=True):
                st.session_state["hide_pending_snap_id"] = None
                st.rerun()


def _render_stock_card_v2(
    snap: Dict[str, Any],
    mode: str,
    preset: str,
    index: int,
    prefix: str,
) -> None:
    factors = _safe_json_loads(snap.get("factors_json"))
    title = f"{index}. {snap.get('name') or '—'}（{snap.get('code') or '—'}）"
    st.subheader(title)
    st.caption(f"{_fmt_dt(snap.get('created_at'))} · 评分 {_fmt_score(snap.get('sentiment_score'))}")

    if mode == "全量模式":
        _render_data_block(snap, factors)
        _render_judge_block(snap, factors)
        _render_action_block(snap, factors)
    elif mode == "进阶模式":
        if preset == "快速决策组合":
            _render_quick_decision_preset(snap, factors)
        elif preset == "风险审查组合":
            _render_risk_review_preset(snap, factors)
        else:
            _render_data_review_preset(snap, factors)
    else:
        with st.expander("📊 数据透视", expanded=False):
            _render_data_block(snap, factors)
        with st.expander("🎯 判断结论", expanded=True):
            _render_judge_block(snap, factors)
        with st.expander("📋 操作计划", expanded=True):
            _render_action_block(snap, factors)

    _render_card_actions(snap, prefix)
    st.divider()


def _snapshot_matches_filters(
    snap: Dict[str, Any],
    advice_filter: Optional[str],
    trend_filter: Optional[str],
    keyword: str,
) -> bool:
    if snap.get("code") == "__market__":
        return False

    advice_text = _normalize_text(snap.get("operation_advice"))
    trend_text = _normalize_text(snap.get("trend_prediction"))
    code_text = _normalize_text(snap.get("code"))
    name_text = _normalize_text(snap.get("name"))
    keyword_text = _normalize_text(keyword).lower()

    if advice_filter and advice_filter not in advice_text:
        return False
    if trend_filter and trend_filter not in trend_text:
        return False
    if keyword_text and keyword_text not in code_text.lower() and keyword_text not in name_text.lower():
        return False
    return True


def _render_timeline_mode(precise_code: str) -> Optional[str]:
    history = get_code_history(precise_code)
    stock_name = history[0].get("name") if history else precise_code
    st.subheader(f"⏱️ {stock_name}（{precise_code}）时光轴复盘")
    st.caption("按版本切换与时序卡片回看，保留个股原始报告与隐藏操作。")

    if not history:
        st.info("暂无该股票历史记录。")
        return None

    version_options = {
        f"{_fmt_dt(snap.get('created_at'))} │ 评分 {_fmt_score(snap.get('sentiment_score'))} │ {_normalize_text(snap.get('operation_advice')) or '—'}": snap
        for snap in history
    }
    selected_label = st.selectbox("选择历史版本", list(version_options.keys()), index=0)
    selected_snapshot = version_options[selected_label]

    with st.expander("📄 当前版本报告详情", expanded=True):
        st.markdown(selected_snapshot.get("report_md") or "_无报告内容_")

    st.divider()
    for index, snap in enumerate(history, start=1):
        _render_stock_card_v2(
            snap=snap,
            mode="折叠模式",
            preset=st.session_state["report_preset"],
            index=index,
            prefix=f"timeline_{precise_code}",
        )

    return selected_snapshot.get("run_id")


def _build_batch_summary_rows(batch_snaps: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for index, snap in enumerate(
        [item for item in batch_snaps if item.get("code") != "__market__"],
        start=1,
    ):
        rows.append(
            {
                "序号": str(index),
                "名称": _normalize_text(snap.get("name")) or "—",
                "代码": _normalize_text(snap.get("code")) or "—",
                "评级": _normalize_text(snap.get("operation_advice")) or "—",
                "趋势": _normalize_text(snap.get("trend_prediction")) or "—",
                "评分": _fmt_score(snap.get("sentiment_score")),
                "时间": _fmt_dt(snap.get("created_at")),
            }
        )
    return rows


def _render_batch_navigation(run_rows: List[Dict[str, Any]]) -> None:
    st.subheader("📋 分析批次")
    if not run_rows:
        st.info("暂无可浏览的分析批次。")
        return

    valid_run_ids = {row.get("run_id") for row in run_rows if row.get("run_id")}
    if st.session_state.get("selected_run_id") not in valid_run_ids:
        st.session_state["selected_run_id"] = None

    for index, row in enumerate(run_rows, start=1):
        run_id = row.get("run_id")
        if not run_id:
            continue
        batch_snaps = get_run_snapshots(run_id)
        stock_names = [snap.get("name") for snap in batch_snaps if snap.get("code") != "__market__" and snap.get("name")]
        count = len(stock_names)
        preview = "、".join(stock_names[:3]) + ("..." if count > 3 else "")
        dt_str = _infer_run_time(batch_snaps)
        label = f"[{index:02d}]  {dt_str}  │  {count}只  │  {preview or '暂无标的'}"
        is_selected = st.session_state.get("selected_run_id") == run_id
        if st.button(
            label,
            key=f"run_select_{run_id}",
            type="primary" if is_selected else "secondary",
            use_container_width=True,
        ):
            st.session_state["selected_run_id"] = run_id
            st.rerun()


def _render_batch_mode(
    run_rows: List[Dict[str, Any]],
    advice_value: Optional[str],
    trend_value: Optional[str],
    keyword: str,
) -> Optional[Dict[str, str]]:
    selected_run_id = st.session_state.get("selected_run_id")
    if not selected_run_id:
        st.info("👈 请从上方选择一个分析批次开始浏览")
        return None

    batch_snaps = get_run_snapshots(selected_run_id)
    if not batch_snaps:
        st.warning("当前批次暂无可展示内容。")
        return None

    artifacts = _get_batch_artifacts_payload(selected_run_id, batch_snaps)
    run_date_slug = _infer_run_date_slug(batch_snaps)
    market_report_md = artifacts.get("market_report_md", "")
    stock_report_md = artifacts.get("stock_report_md", "")
    full_report_md = artifacts.get("full_report_md", "")
    summary_rows = _build_batch_summary_rows(batch_snaps)

    if summary_rows:
        st.caption("本批次汇总总表")
        st.dataframe(summary_rows, use_container_width=True, hide_index=True)
    else:
        st.caption("当前批次暂无可展示的个股汇总表")

    tab_market, tab_full, tab_stock = st.tabs(["📊 大盘报告", "📄 全量报告", "🔬 个股分析"])

    with tab_market:
        if market_report_md:
            st.markdown(market_report_md)
            st.download_button(
                "⬇️ 下载大盘报告",
                data=market_report_md,
                file_name=f"market_review_{run_date_slug}.md",
                mime="text/markdown",
                use_container_width=True,
            )
        else:
            st.info("当前批次暂无大盘报告。")

    with tab_full:
        if full_report_md:
            st.markdown(full_report_md)
            st.download_button(
                "⬇️ 下载全量报告",
                data=full_report_md,
                file_name=f"full_report_{run_date_slug}.md",
                mime="text/markdown",
                use_container_width=True,
            )
        else:
            st.info("当前批次暂无全量报告。")

    filtered_stock_snaps = [
        snap for snap in batch_snaps
        if _snapshot_matches_filters(snap, advice_value, trend_value, keyword)
    ]

    with tab_stock:
        mode = st.radio(
            "浏览模式",
            ["全量模式", "进阶模式", "折叠模式"],
            horizontal=True,
            key="report_view_mode",
        )
        preset = st.session_state["report_preset"]
        if mode == "进阶模式":
            preset = st.selectbox(
                "选择预设组合",
                PRESET_OPTIONS,
                index=PRESET_OPTIONS.index(st.session_state["report_preset"]),
                key="report_preset",
            )
        if not filtered_stock_snaps:
            st.info("当前过滤条件下，没有命中的个股卡片。")
        else:
            for index, snap in enumerate(filtered_stock_snaps, start=1):
                _render_stock_card_v2(
                    snap=snap,
                    mode=mode,
                    preset=preset,
                    index=index,
                    prefix=f"batch_{selected_run_id}",
                )
        if stock_report_md:
            st.download_button(
                "⬇️ 下载个股报告",
                data=stock_report_md,
                file_name=f"stock_report_{run_date_slug}.md",
                mime="text/markdown",
                use_container_width=True,
            )
        else:
            st.caption("暂无个股报告")

    st.markdown("### ⬇️ 下载矩阵")
    dl_cols = st.columns(3)
    with dl_cols[0]:
        if market_report_md:
            st.download_button(
                "⬇️ 大盘报告 .md",
                data=market_report_md,
                file_name=f"market_review_{run_date_slug}.md",
                mime="text/markdown",
                use_container_width=True,
            )
        else:
            st.caption("暂无大盘报告")
    with dl_cols[1]:
        if stock_report_md:
            st.download_button(
                "⬇️ 个股报告 .md",
                data=stock_report_md,
                file_name=f"stock_report_{run_date_slug}.md",
                mime="text/markdown",
                use_container_width=True,
            )
        else:
            st.caption("暂无个股报告")
    with dl_cols[2]:
        if full_report_md:
            st.download_button(
                "⬇️ 全量报告 .md",
                data=full_report_md,
                file_name=f"full_report_{run_date_slug}.md",
                mime="text/markdown",
                use_container_width=True,
            )
        else:
            st.caption("暂无全量报告")

    return {
        **artifacts,
        "run_id": selected_run_id,
        "run_date_slug": run_date_slug,
    }


with st.sidebar:
    st.divider()
    st.subheader("🔍 过滤条件")
    advice_filter = st.selectbox("评级过滤", ["全部", "买入", "观望", "卖出"], index=0)
    trend_filter = st.selectbox("趋势过滤", ["全部", "看多", "震荡", "看空"], index=0)
    keyword = st.text_input("股票搜索", placeholder="输入股票代码或名称")
    limit = st.slider("最近记录条数", min_value=20, max_value=200, value=60, step=10)

    with st.expander("⚠️ 危险区", expanded=False):
        if st.button("🗑️ 清空所有历史与跟踪数据", use_container_width=True):
            st.session_state["confirm_clear_all_data"] = True
        if st.session_state.get("confirm_clear_all_data"):
            st.warning("此操作将清空历史快照与跟踪池数据，且不可恢复。")
            if st.button("🚨 确认执行清空", use_container_width=True, type="primary"):
                clear_all_data()
                st.session_state["confirm_clear_all_data"] = False
                st.success("数据已全部清空")
                st.rerun()


st.title("🕰️ 历史记忆库")
st.caption("按分析批次分组回看 · 支持精确锁定个股时光轴")

run_rows = list_recent_runs(limit=limit)
all_snapshots = get_snapshots_with_filters(limit=max(limit * 3, 120))

precise_code = _resolve_precise_code(keyword, all_snapshots)

_render_batch_navigation(run_rows)
st.divider()

selected_artifacts_for_secret: Dict[str, str] = {
    "business_log": "",
    "debug_log": "",
    "schema_json": "",
    "run_id": "",
    "run_date_slug": datetime.now().strftime("%Y%m%d_%H%M%S"),
}

advice_value = None if advice_filter == "全部" else advice_filter
trend_value = None if trend_filter == "全部" else trend_filter

if precise_code:
    timeline_run_id = _render_timeline_mode(precise_code)
    if timeline_run_id:
        timeline_snaps = get_run_snapshots(timeline_run_id)
        selected_artifacts_for_secret.update(_get_batch_artifacts_payload(timeline_run_id, timeline_snaps))
        selected_artifacts_for_secret["run_id"] = timeline_run_id
        selected_artifacts_for_secret["run_date_slug"] = _infer_run_date_slug(timeline_snaps)
else:
    batch_artifacts = _render_batch_mode(run_rows, advice_value, trend_value, keyword)
    if batch_artifacts:
        selected_artifacts_for_secret.update(batch_artifacts)


st.divider()
st.caption("🔒 机要区 · 底层运行日志")
secret_cols = st.columns(2)

with secret_cols[0]:
    with st.expander("📋 运行日志（Business Log）", expanded=False):
        business_log = selected_artifacts_for_secret.get("business_log") or "暂无运行日志"
        st.text_area(
            label="",
            value=business_log,
            height=300,
            disabled=True,
            key="history_business_log",
        )
        if selected_artifacts_for_secret.get("business_log"):
            st.download_button(
                "⬇️ 下载运行日志",
                data=selected_artifacts_for_secret["business_log"],
                file_name=f"business_log_{selected_artifacts_for_secret['run_date_slug']}.log",
                mime="text/plain",
                use_container_width=True,
            )
        else:
            st.caption("暂无可下载的运行日志")

with secret_cols[1]:
    with st.expander("🔧 通信日志（Debug Log）", expanded=False):
        debug_log = selected_artifacts_for_secret.get("debug_log") or "暂无通信日志"
        st.text_area(
            label="",
            value=debug_log,
            height=300,
            disabled=True,
            key="history_debug_log",
        )
        if selected_artifacts_for_secret.get("debug_log"):
            st.download_button(
                "⬇️ 下载通信日志",
                data=selected_artifacts_for_secret["debug_log"],
                file_name=f"debug_log_{selected_artifacts_for_secret['run_date_slug']}.log",
                mime="text/plain",
                use_container_width=True,
            )
        else:
            st.caption("暂无可下载的通信日志")

schema_col1, schema_col2 = st.columns(2)
with schema_col1:
    if selected_artifacts_for_secret.get("schema_json"):
        st.download_button(
            "⬇️ 下载 Schema JSON",
            data=selected_artifacts_for_secret["schema_json"],
            file_name=f"schema_{selected_artifacts_for_secret['run_date_slug']}.json",
            mime="application/json",
            use_container_width=True,
        )
    else:
        st.caption("暂无 Schema JSON")
with schema_col2:
    combined_log = selected_artifacts_for_secret.get("business_log") or selected_artifacts_for_secret.get("debug_log")
    if combined_log:
        st.download_button(
            "⬇️ 下载 API 原始日志",
            data=combined_log,
            file_name=f"api_raw_{selected_artifacts_for_secret['run_date_slug']}.log",
            mime="text/plain",
            use_container_width=True,
        )
    else:
        st.caption("暂无 API 原始日志")
