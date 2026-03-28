# -*- coding: utf-8 -*-
"""
pages/2_🕰️_历史记忆库.py  v3.1
================================
修正：
- 统一标题字号（span 0.92rem，完全绕开 Streamlit 的 h1-h3 层级）
- 色值全面压暗：标题 #D1D1D1，辅助文字 #BDBDBD，禁用 #E0E0E0+
- _render_dense_fields 替换为单张 HTML 表格，消除"大标签 + 小值"失衡
- 因子行垂直基线对齐
"""

import os
import sys
from datetime import datetime
from typing import Dict, List, Optional

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
    get_snapshots_with_filters,
    init_db,
)

init_db()

if "history_delete_pending_id" not in st.session_state:
    st.session_state.history_delete_pending_id = None

st.set_page_config(
    page_title="历史记忆库 · DUKA Stock Analysis Engine V5-Pro",
    page_icon="🕰️",
    layout="wide",
)
enforce_sidebar_password_gate()

# ─────────────────────── 全局 CSS 注入 ──────────────────────────────────────
# 目的：
#   1. 把 Streamlit 默认的 h1-h3 字号强制压低（防止 st.markdown("## ...") 过大）
#   2. 统一 caption / small 文字颜色为 #BDBDBD
#   3. 卡片分割线更低调
st.markdown("""
<style>
/* 压制 Streamlit 默认标题层级，统一走我们自己的 span 控制 */
section[data-testid="stMain"] h1 { font-size: 1.25rem !important; color: #D1D1D1 !important; }
section[data-testid="stMain"] h2 { font-size: 1.05rem !important; color: #D1D1D1 !important; }
section[data-testid="stMain"] h3 { font-size: 0.95rem !important; color: #D1D1D1 !important; }

/* caption / small */
.stCaption, .stCaption p, small { color: #BDBDBD !important; font-size: 11px !important; }

/* 分割线低调化 */
hr { border-color: #2A3444 !important; margin: 8px 0 !important; }

/* 消除 st.divider 的默认上下 margin 过大 */
[data-testid="stDivider"] { margin: 4px 0 !important; }

/* expander 标题字号压缩 */
[data-testid="stExpander"] summary { font-size: 12px !important; color: #BDBDBD !important; }

/* 次级按钮：奶灰色图标样式，透明背景，避免喧宾夺主 */
section[data-testid="stMain"] .stButton > button[kind="secondary"] {
    color: #D1D1D1 !important;
    border: 1px solid rgba(209, 209, 209, 0.28) !important;
    background: transparent !important;
    min-height: 28px !important;
    padding: 0 !important;
    border-radius: 8px !important;
    box-shadow: none !important;
    font-size: 12px !important;
}
section[data-testid="stMain"] .stButton > button[kind="secondary"]:hover {
    background: rgba(209, 209, 209, 0.08) !important;
    border-color: rgba(209, 209, 209, 0.42) !important;
    color: #E0E0E0 !important;
}
</style>
""", unsafe_allow_html=True)


# ─────────────────────── 格式化工具 ─────────────────────────────────────────

def _fmt_dt(value: Optional[str]) -> str:
    if not value:
        return "—"
    try:
        return datetime.fromisoformat(value).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(value)[:16]

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

def _fmt_price(value: Optional[float]) -> str:
    try:
        return f"{float(value):.2f}" if value not in (None, "") else "—"
    except Exception:
        return "—"


# ─────────────────────── Badge 徽章 ─────────────────────────────────────────

_BADGE_PALETTE = {
    "bull":    ("#5B1D24", "#FFB8C0"),
    "bear":    ("#163B2C", "#91F2BA"),
    "warn":    ("#4D3A12", "#FFD27A"),
    "neutral": ("#1E2A36", "#A8BFCF"),   # fg 从 #CFE3F6 压暗到 #A8BFCF
}

def _badge(text: str, tone: str = "neutral") -> str:
    bg, fg = _BADGE_PALETTE.get(tone, _BADGE_PALETTE["neutral"])
    return (
        f"<span style='display:inline-block;padding:2px 8px;border-radius:999px;"
        f"background:{bg};color:{fg};font-size:11px;font-weight:500;"
        f"margin-right:5px;margin-bottom:3px;vertical-align:middle'>"
        f"{text}</span>"
    )

def _advice_tone(advice: Optional[str]) -> str:
    t = str(advice or "")
    if any(k in t for k in ("买", "多", "看涨", "增持")):
        return "bull"
    if any(k in t for k in ("卖", "空", "减持", "离场")):
        return "bear"
    return "warn"

def _trend_tone(trend: Optional[str]) -> str:
    t = str(trend or "")
    if any(k in t for k in ("多", "上行", "强势")):
        return "bull"
    if any(k in t for k in ("空", "下行", "弱势")):
        return "bear"
    return "neutral"

def _render_tags(snap: Dict) -> None:
    advice = snap.get("operation_advice") or "—"
    trend  = snap.get("trend_prediction") or "趋势未提取"
    ma     = snap.get("ma_alignment")     or "均线状态未提取"
    bias   = _fmt_pct(snap.get("bias_rate"))
    html = "".join([
        _badge(advice,           _advice_tone(advice)),
        _badge(trend,            _trend_tone(trend)),
        _badge(ma,               "neutral"),
        _badge(f"乖离率 {bias}", "warn"),
    ])
    st.markdown(html, unsafe_allow_html=True)


# ─────────────────────── 核心：统一字号的标题 span ───────────────────────────

# 所有卡片标题（含大盘、个股）全部走这一个函数，绝对一致的 0.92rem
def _card_title_html(display_name: str) -> str:
    return (
        f"<span style='"
        f"color:#D1D1D1;"          # 标题用奶灰 #D1D1D1，不超过此值
        f"font-size:0.92rem;"      # 统一字号
        f"font-weight:600;"
        f"letter-spacing:0.01em;"
        f"line-height:1.4;"
        f"display:block;"
        f"margin-bottom:2px;"
        f"'>{display_name}</span>"
    )

def _meta_html(text: str) -> str:
    return (
        f"<span style='"
        f"color:#D1D1D1;"          # 标题行元信息统一奶灰色
        f"font-size:11px;"
        f"'>{text}</span>"
    )


# ─────────────────────── 因子表（HTML 表格，消除列宽失衡）───────────────────

def _render_factor_table(snap: Dict) -> None:
    """
    单张 HTML 两行表格：
      行1：灰色小字标签（11px）
      行2：奶灰数据值（12px，font-weight:500）
    所有列同一基线，长文本（均线状态）自然换行，不截断。
    """
    fields = [
        ("评级",     snap.get("operation_advice") or "—"),
        ("趋势",     snap.get("trend_prediction") or "—"),
        ("均线状态",  snap.get("ma_alignment")    or "—"),
        ("乖离率",   _fmt_pct(snap.get("bias_rate"))),
        ("理想买点",  _fmt_price(snap.get("buy_point"))),
        ("目标价",   _fmt_price(snap.get("target_price"))),
        ("止损位",   _fmt_price(snap.get("stop_loss"))),
    ]

    th = (
        "padding:4px 12px 2px 0;"
        "color:#8A9BAE;"           # 标签灰
        "font-size:11px;"
        "font-weight:400;"
        "text-align:left;"
        "border:none;"
        "white-space:nowrap;"
        "vertical-align:bottom;"
    )
    td = (
        "padding:1px 12px 6px 0;"
        "color:#C8C8C8;"           # 数据值：#C8C8C8（低于 #D1D1D1，更内敛）
        "font-size:12px;"
        "font-weight:500;"
        "text-align:left;"
        "border:none;"
        "word-break:break-word;"   # 均线长文本换行不截断
        "vertical-align:top;"
    )

    ths = "".join(f"<th style='{th}'>{k}</th>" for k, _ in fields)
    tds = "".join(f"<td style='{td}'>{v}</td>" for _, v in fields)

    html = (
        f"<table style='width:100%;border-collapse:collapse;margin:6px 0 2px 0'>"
        f"<thead><tr>{ths}</tr></thead>"
        f"<tbody><tr>{tds}</tr></tbody>"
        f"</table>"
    )
    st.markdown(html, unsafe_allow_html=True)


# ─────────────────────── 精确代码匹配 ───────────────────────────────────────

def _resolve_precise_code(keyword: str, snapshots: List[Dict]) -> Optional[str]:
    q = (keyword or "").strip()
    if not q:
        return None
    exact = [s["code"] for s in snapshots if s.get("code") == q]
    if exact:
        return exact[0]
    by_name = [s["code"] for s in snapshots if s.get("name") == q]
    unique = list(dict.fromkeys(by_name))
    return unique[0] if len(unique) == 1 else None


# ═══════════════════════════════════════════════════════════════════════════
# Sidebar
# ═══════════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.markdown("## 🎛️ 过滤器")
    advice_filter = st.selectbox("评级过滤", ["全部", "买入", "观望", "卖出"], index=0)
    trend_filter  = st.selectbox("趋势过滤", ["全部", "看多", "震荡", "看空"], index=0)
    keyword       = st.text_input("股票搜索", placeholder="输入股票代码或名称")
    limit         = st.slider("最近记录条数", min_value=20, max_value=200, value=60, step=10)

    with st.expander("⚠️ 危险操作区", expanded=False):
        if "confirm_clear_all_data" not in st.session_state:
            st.session_state.confirm_clear_all_data = False
        if st.button("🗑️ 清空所有历史与跟踪数据", use_container_width=True):
            st.session_state.confirm_clear_all_data = True
        if st.session_state.confirm_clear_all_data:
            st.warning("此操作将清空历史快照与跟踪池数据，且不可恢复。")
            if st.button("🚨 确认执行清空（不可逆）", use_container_width=True, type="primary"):
                clear_all_data()
                st.session_state.confirm_clear_all_data = False
                st.success("数据已全部清空")
                st.rerun()


# ═══════════════════════════════════════════════════════════════════════════
# 主体
# ═══════════════════════════════════════════════════════════════════════════

st.markdown("# 🕰️ 历史记忆库")
st.caption("支持多维标签过滤，精准搜索单只股票时切换为时光轴复盘。")
st.divider()

trend_value  = None if trend_filter == "全部" else trend_filter
advice_value = None if advice_filter == "全部" else advice_filter

snapshots = get_snapshots_with_filters(
    limit=limit,
    trend=trend_value,
    advice=advice_value,
    code=keyword or None,
)

if not snapshots:
    st.info("📭 当前过滤条件下没有命中的历史快照。")
    st.stop()

precise_code = _resolve_precise_code(keyword, snapshots)

# ── 时光轴复盘模式 ──────────────────────────────────────────────────────────

if precise_code:
    history    = get_code_history(precise_code)
    stock_name = history[0].get("name") if history else precise_code

    st.markdown(
        _card_title_html(f"⏳ {stock_name}（{precise_code}）时光轴复盘"),
        unsafe_allow_html=True,
    )
    st.caption("纵向对比历次分析中的评分、评级和均线状态变化。")

    if not history:
        st.info("暂无该股票历史记录。")
        st.stop()

    version_options = {
        f"{_fmt_dt(s.get('created_at'))} · 评分 {_fmt_score(s.get('sentiment_score'))} · {s.get('operation_advice') or '—'}": s
        for s in history
    }
    selected_version  = st.selectbox("📅 切换历史报告版本", list(version_options.keys()), index=0)
    selected_snapshot = version_options[selected_version]

    with st.expander("🕰️ 查看当前选中版本完整报告", expanded=True):
        st.markdown(selected_snapshot.get("report_md") or "_无报告内容_")

    st.divider()

    for idx, snap in enumerate(history, start=1):
        c1, c2, c_ops = st.columns([4.6, 1.3, 0.55])
        with c1:
            st.markdown(
                _meta_html(f"第 {idx} 次 · {_fmt_dt(snap.get('created_at'))} · 评分 {_fmt_score(snap.get('sentiment_score'))}"),
                unsafe_allow_html=True,
            )
        with c2:
            st.markdown(
                _meta_html(f"均线：{snap.get('ma_alignment') or '—'}"),
                unsafe_allow_html=True,
            )
        with c_ops:
            if st.button("📥", key=f"push_tl_{snap['id']}", help="送入首页分析池", use_container_width=False, type="secondary"):
                add_to_quick_pool(snap["code"], snap["name"])
                st.toast(f"{snap['name']} 已送入首页分析池")
            if st.button(
                "🙈 隐藏报告",
                key=f"del_tl_{snap['id']}",
                help="仅隐藏这份历史报告，不会从数据库物理删除。",
                use_container_width=False,
                type="secondary",
            ):
                st.session_state.history_delete_pending_id = snap["id"]
                st.rerun()

        _render_tags(snap)
        _render_factor_table(snap)
        if st.session_state.history_delete_pending_id == snap["id"]:
            st.warning("确认后，这份历史报告将从列表中隐藏，但仍保留在数据库中。")
            _dc1, _dc2 = st.columns([1.2, 5.8])
            with _dc1:
                if st.button(
                    "🚨 确认隐藏",
                    key=f"confirm_del_tl_{snap['id']}",
                    use_container_width=True,
                    type="secondary",
                ):
                    delete_snapshot(snap["id"])
                    st.session_state.history_delete_pending_id = None
                    st.rerun()
            with _dc2:
                if st.button(
                    "取消",
                    key=f"cancel_del_tl_{snap['id']}",
                    use_container_width=False,
                    type="secondary",
                ):
                    st.session_state.history_delete_pending_id = None
                    st.rerun()
        with st.expander("查看当日完整报告", expanded=False):
            st.markdown(snap.get("report_md") or "_无报告内容_")

        if idx < len(history):
            st.divider()

# ── 列表卡片模式（默认视图）──────────────────────────────────────────────────

else:
    st.markdown(
        _meta_html("未精确锁定单只股票时，展示符合条件的最新快照卡片。"),
        unsafe_allow_html=True,
    )
    st.markdown("")  # 小间距

    for snap in snapshots:
        is_market = snap.get("code") == "__market__"
        raw_name  = snap.get("name") or snap.get("code") or "—"
        raw_code  = snap.get("code") or "—"

        # 大盘和个股完全一致的标题处理：统一 display_name 格式
        if is_market:
            display_name = f"📊 大盘复盘"
        else:
            display_name = f"{raw_name}（{raw_code}）"

        with st.container():
            if is_market:
                h_name, h_score, h_date, h_del = st.columns([4.4, 0.75, 1.55, 1.05])
            else:
                h_name, h_score, h_date, h_push, h_del = st.columns([3.95, 0.75, 1.45, 0.62, 1.05])

            with h_name:
                st.markdown(_card_title_html(display_name), unsafe_allow_html=True)

            with h_score:
                st.markdown(_meta_html(f"评分 {_fmt_score(snap.get('sentiment_score'))}"), unsafe_allow_html=True)

            with h_date:
                st.markdown(_meta_html(_fmt_dt(snap.get('created_at'))), unsafe_allow_html=True)

            if not is_market:
                with h_push:
                    if st.button("📥", key=f"push_card_{snap['id']}", help="送入首页分析池", use_container_width=False, type="secondary"):
                        add_to_quick_pool(snap["code"], snap["name"])
                        st.toast(f"{snap['name']} 已送入首页分析池")

            with h_del:
                if st.button(
                    "🙈 隐藏报告",
                    key=f"del_card_{snap['id']}",
                    help="仅隐藏这份历史报告，不会从数据库物理删除。",
                    use_container_width=False,
                    type="secondary",
                ):
                    st.session_state.history_delete_pending_id = snap["id"]
                    st.rerun()

            # 标签徽章
            _render_tags(snap)

            # 因子表（大盘无意义因子，跳过）
            if not is_market:
                _render_factor_table(snap)

            if st.session_state.history_delete_pending_id == snap["id"]:
                st.warning("确认后，这份历史报告将从列表中隐藏，但仍保留在数据库中。")
                _dc1, _dc2 = st.columns([1.2, 5.8])
                with _dc1:
                    if st.button(
                        "🚨 确认隐藏",
                        key=f"confirm_del_card_{snap['id']}",
                        use_container_width=True,
                        type="secondary",
                    ):
                        delete_snapshot(snap["id"])
                        st.session_state.history_delete_pending_id = None
                        st.rerun()
                with _dc2:
                    if st.button(
                        "取消",
                        key=f"cancel_del_card_{snap['id']}",
                        use_container_width=False,
                        type="secondary",
                    ):
                        st.session_state.history_delete_pending_id = None
                        st.rerun()

            with st.expander("查看完整报告", expanded=False):
                st.markdown(snap.get("report_md") or "_无报告内容_")

        st.divider()
