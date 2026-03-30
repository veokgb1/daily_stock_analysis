# -*- coding: utf-8 -*-
"""
历史记忆库 · DUKA Stock Analysis Engine V5-Pro
根治重构版 —— 原始 Markdown 直出，彻底消灭 factors_json 空壳渲染
"""
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
    delete_run_permanently,
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
    page_title="历史记忆库 · DUKA V5-Pro",
    page_icon="🕰️",
    layout="wide",
)
enforce_sidebar_password_gate()

st.session_state["_active_streamlit_page"] = "history_memory"
st.session_state.setdefault("selected_run_id", None)
st.session_state.setdefault("confirm_delete_run_id", None)
st.session_state.setdefault("confirm_clear_all_data", False)

# ─────────────────────────────────────────────────────────────────────────────
# 全局 CSS：侧边栏瘦身 + Markdown 渲染增强 + 通用样式
# ─────────────────────────────────────────────────────────────────────────────
st.markdown(
    """
<style>
/* ══ 侧边栏极限瘦身 ══ */
[data-testid="stSidebar"] {
    min-width: 250px !important;
    max-width: 300px !important;
}
[data-testid="stSidebar"] p,
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] .stCaption {
    font-size: 0.82em !important;
}

/* ══ 正文 Markdown 渲染（完全对齐快速分析页面排版）══ */
.stMarkdown p, .stMarkdown li {
    color: #E2E8F0 !important;
    line-height: 1.85 !important;
    font-size: 0.95em !important;
}
.stMarkdown h1 {
    color: #93C5FD !important;
    font-size: 1.4em !important;
    margin-top: 1.5em !important;
    letter-spacing: 0.03em !important;
}
.stMarkdown h2 {
    color: #93C5FD !important;
    font-size: 1.15em !important;
    margin-top: 1.3em !important;
    border-bottom: 1px solid #1e3a5f !important;
    padding-bottom: 4px !important;
}
.stMarkdown h3 {
    color: #7DD3FC !important;
    font-size: 1.05em !important;
    margin-top: 1.1em !important;
}
.stMarkdown strong { color: #F1F5F9 !important; }
.stMarkdown em    { color: #CBD5E1 !important; }
.stMarkdown blockquote {
    border-left: 3px solid #3B82F6 !important;
    padding: 10px 1.2em !important;
    color: #CBD5E1 !important;
    background: rgba(59,130,246,0.07) !important;
    border-radius: 0 6px 6px 0 !important;
    margin: 0.8em 0 !important;
}
.stMarkdown table { border-collapse: collapse !important; width: 100% !important; margin: 0.8em 0 !important; font-size: 0.9em !important; }
.stMarkdown th {
    background: #1e3a5f !important;
    color: #93C5FD !important;
    padding: 7px 14px !important;
    font-weight: 600 !important;
    text-align: center !important;
    border: 1px solid #2d4a7a !important;
}
.stMarkdown td {
    padding: 6px 14px !important;
    border: 1px solid #1f2937 !important;
    color: #E2E8F0 !important;
    text-align: center !important;
}
.stMarkdown tr:nth-child(even) td { background: rgba(30,58,115,0.12) !important; }
.stMarkdown tr:hover td { background: rgba(59,130,246,0.06) !important; }
.stMarkdown code {
    background: rgba(30,42,71,0.75) !important;
    color: #93C5FD !important;
    padding: 1px 6px !important;
    border-radius: 4px !important;
    font-size: 0.88em !important;
    font-family: 'JetBrains Mono', 'Consolas', monospace !important;
}
.stMarkdown pre {
    background: rgba(10,20,45,0.85) !important;
    border: 1px solid #1e3a5f !important;
    border-radius: 8px !important;
    padding: 14px !important;
}
.stMarkdown hr { border-color: #1e3a5f !important; margin: 1em 0 !important; }

/* ══ 报告容器（大盘 / 全量）══ */
.report-container {
    background: linear-gradient(160deg, #0a1929 0%, #0d1117 100%);
    border: 1px solid #1e3a5f;
    border-radius: 12px;
    padding: 28px 36px;
    margin: 6px 0 16px 0;
}

/* ══ 个股报告容器 ══ */
.stock-report-container {
    background: rgba(8, 15, 28, 0.7);
    border: 1px solid #1e3358;
    border-radius: 10px;
    padding: 20px 28px;
    margin: 6px 0 14px 0;
}

/* ══ 评级徽章 ══ */
.badge {
    display: inline-block; padding: 2px 9px; border-radius: 4px;
    font-size: 0.78em; font-weight: 700; letter-spacing: 0.05em;
}
.badge-buy   { background: rgba(239,68,68,0.12); color: #F87171; border: 1px solid rgba(239,68,68,0.5); }
.badge-sell  { background: rgba(34,197,94,0.12);  color: #4ADE80; border: 1px solid rgba(34,197,94,0.5); }
.badge-watch { background: rgba(234,179,8,0.12);  color: #FACC15; border: 1px solid rgba(234,179,8,0.5); }

/* ══ 分隔线 ══ */
hr { border-color: #1f2937 !important; margin: 14px 0 !important; }

/* ══ 下载按钮 ══ */
.stDownloadButton > button {
    background: rgba(10, 20, 50, 0.9) !important;
    color: #93C5FD !important;
    border: 1px solid rgba(59,130,246,0.45) !important;
    border-radius: 8px !important;
    font-weight: 600 !important;
    font-size: 0.82em !important;
    transition: background .2s, border-color .2s !important;
}
.stDownloadButton > button:hover {
    background: rgba(59,130,246,0.18) !important;
    border-color: #3B82F6 !important;
}

/* ══ 侧边栏批次导航按钮 ══ */
[data-testid="stSidebar"] .stButton > button {
    font-size: 0.79em !important;
    padding: 4px 8px !important;
}
</style>
""",
    unsafe_allow_html=True,
)


# ─────────────────────────────────────────────────────────────────────────────
# 一、工具函数（极简，仅保留必需）
# ─────────────────────────────────────────────────────────────────────────────

def _fmt_dt(value: Optional[str]) -> str:
    if not value:
        return "—"
    try:
        return datetime.fromisoformat(value).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(value)[:16]


def _fmt_score(value: Any) -> str:
    try:
        return f"{float(value):.0f}" if value not in (None, "") else "—"
    except Exception:
        return "—"


def _norm(value: Any) -> str:
    return str(value or "").strip()


def _advice_badge_html(advice: str) -> str:
    t = _norm(advice) or "未评级"
    if any(w in t for w in ("买", "增持", "看涨", "做多")):
        return f'<span class="badge badge-buy">▲ {t}</span>'
    if any(w in t for w in ("卖", "减持", "离场", "做空")):
        return f'<span class="badge badge-sell">▼ {t}</span>'
    return f'<span class="badge badge-watch">◆ {t}</span>'


def _advice_emoji(advice: str) -> str:
    t = _norm(advice)
    if any(w in t for w in ("买", "增持", "看涨")):
        return "🔴"
    if any(w in t for w in ("卖", "减持", "离场")):
        return "🟢"
    return "🟡"


def _dl_bytes(text: str) -> bytes:
    """强制 UTF-8 编码，杜绝乱码。"""
    return (text or "").encode("utf-8")


def _slug_from_snaps(batch_snaps: List[Dict[str, Any]]) -> str:
    ts = [s.get("created_at") for s in batch_snaps if s.get("created_at")]
    if not ts:
        return datetime.now().strftime("%Y%m%d_%H%M%S")
    try:
        return datetime.fromisoformat(max(ts)).strftime("%Y%m%d_%H%M%S")
    except Exception:
        return datetime.now().strftime("%Y%m%d_%H%M%S")


def _run_time_from_snaps(batch_snaps: List[Dict[str, Any]]) -> str:
    ts = [s.get("created_at") for s in batch_snaps if s.get("created_at")]
    return _fmt_dt(max(ts) if ts else None)


# ─────────────────────────────────────────────────────────────────────────────
# 二、批次数据获取：最优先使用 run_artifacts 原始 Markdown
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_artifacts(run_id: str, batch_snaps: List[Dict[str, Any]]) -> Dict[str, str]:
    """
    优先级策略：
      1. run_artifacts 表中存储的原始 Markdown（生成时直接写入，最完整）
      2. 从 analysis_snapshots.report_md 拼接重建（备份方案）

    返回两套内容：
      *_md  → 原始 Markdown，用于 st.markdown() 渲染（零损失）
      *_txt → 纯文本，用于 download_button（UTF-8）
    """
    arts = get_run_artifacts(run_id) or {}
    stock_snaps = [s for s in batch_snaps if s.get("code") != "__market__"]

    # ── 大盘报告 ──────────────────────────────────────────────────────────────
    market_md = (arts.get("market_report_md") or "").strip()
    if not market_md:
        # 备份：从 __market__ 快照拼接
        parts = [
            (s.get("report_md") or "").strip()
            for s in batch_snaps
            if s.get("code") == "__market__"
        ]
        market_md = "\n\n".join(p for p in parts if p)

    # ── 个股报告 ──────────────────────────────────────────────────────────────
    stock_md = (arts.get("stock_report_md") or "").strip()
    if not stock_md:
        # 备份：拼接所有个股 report_md
        lines: List[str] = []
        for snap in stock_snaps:
            name = _norm(snap.get("name")) or "—"
            code = _norm(snap.get("code")) or "—"
            rmd  = (_norm(snap.get("report_md")))
            lines.append(f"## {name}（{code}）")
            lines.append(rmd if rmd else "_该股票暂无完整报告内容_")
            lines.append("")
        stock_md = "\n".join(lines).strip()

    # ── 全量报告 ──────────────────────────────────────────────────────────────
    full_md = (arts.get("full_report_md") or "").strip()
    if not full_md:
        sections: List[str] = []
        if market_md:
            sections.append("# 大盘报告\n\n" + market_md)
        if stock_md:
            sections.append("# 个股报告\n\n" + stock_md)
        full_md = "\n\n---\n\n".join(sections)

    # ── 日志 ─────────────────────────────────────────────────────────────────
    business_log = (arts.get("business_log") or "").strip()
    debug_log    = (arts.get("debug_log")    or "").strip()

    return {
        "market_md":   market_md,
        "stock_md":    stock_md,
        "full_md":     full_md,
        "business_log": business_log,
        "debug_log":   debug_log,
    }


# ─────────────────────────────────────────────────────────────────────────────
# 三、批次条目构建（供侧边栏和历史库使用）
# ─────────────────────────────────────────────────────────────────────────────

def _build_run_entries(run_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    for idx, row in enumerate(run_rows, start=1):
        run_id = row.get("run_id")
        if not run_id:
            continue
        batch_snaps = get_run_snapshots(run_id)
        stock_snaps = [s for s in batch_snaps if s.get("code") != "__market__"]
        names   = [s.get("name") for s in stock_snaps if s.get("name")]
        count   = len(names)
        preview = "、".join(names[:3]) + ("…" if count > 3 else "")
        dt_str  = _run_time_from_snaps(batch_snaps)
        entries.append({
            "index":   idx,
            "run_id":  run_id,
            "dt":      dt_str,
            "count":   count,
            "preview": preview or "暂无标的",
            "snaps":   batch_snaps,
        })
    return entries


def _select_run(run_id: str) -> None:
    if run_id != st.session_state.get("selected_run_id"):
        st.session_state["selected_run_id"] = run_id
        st.session_state["confirm_delete_run_id"] = None
        st.rerun()


def _ensure_selected(entries: List[Dict[str, Any]]) -> None:
    valid = {e["run_id"] for e in entries}
    if st.session_state.get("selected_run_id") not in valid:
        st.session_state["selected_run_id"] = entries[0]["run_id"] if entries else None


# ─────────────────────────────────────────────────────────────────────────────
# 四、侧边栏（瘦身：仅导航 + 过滤 + 危险区）
# ─────────────────────────────────────────────────────────────────────────────

run_rows    = list_recent_runs(limit=None)
run_entries = _build_run_entries(run_rows)
_ensure_selected(run_entries)

with st.sidebar:
    st.markdown(
        "<p style='color:#475569;font-size:0.7em;text-transform:uppercase;"
        "letter-spacing:0.1em;margin-bottom:6px;'>🕰️ 最近分析批次</p>",
        unsafe_allow_html=True,
    )
    if not run_entries:
        st.caption("暂无历史记录。运行一次快速分析后，记录会出现在这里。")
    else:
        for entry in run_entries[:8]:  # 侧边栏最多展示最近 8 条
            run_id    = entry["run_id"]
            is_active = run_id == st.session_state.get("selected_run_id")
            label     = f"{'▶ ' if is_active else ''}[{entry['index']:02d}] {entry['dt']}"
            if st.button(
                label,
                key=f"sb_run_{run_id}",
                use_container_width=True,
                type="primary" if is_active else "secondary",
            ):
                _select_run(run_id)
            if is_active:
                st.caption(f"{entry['count']}只 · {entry['preview']}")

    st.divider()

    # 过滤条件（紧凑布局）
    st.markdown(
        "<p style='color:#475569;font-size:0.7em;text-transform:uppercase;"
        "letter-spacing:0.1em;margin-bottom:4px;'>过滤</p>",
        unsafe_allow_html=True,
    )
    advice_filter = st.selectbox("评级", ["全部", "买入", "观望", "卖出"], index=0, label_visibility="collapsed")
    keyword       = st.text_input("搜索股票", placeholder="代码 / 名称", label_visibility="collapsed")

    st.divider()

    with st.expander("⚠️ 危险区", expanded=False):
        if st.button("🗑️ 清空所有历史数据", use_container_width=True):
            st.session_state["confirm_clear_all_data"] = True
        if st.session_state.get("confirm_clear_all_data"):
            st.warning("此操作不可恢复！")
            if st.button("🚨 确认清空", use_container_width=True, type="primary"):
                clear_all_data()
                st.session_state.update({
                    "confirm_clear_all_data": False,
                    "confirm_delete_run_id":  None,
                    "selected_run_id":        None,
                })
                st.success("已全部清空")
                st.rerun()
            if st.button("取消", use_container_width=True):
                st.session_state["confirm_clear_all_data"] = False
                st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# 五、主页面标题
# ─────────────────────────────────────────────────────────────────────────────

st.markdown(
    "<h2 style='margin-bottom:2px;color:#F1F5F9;font-weight:700;'>🕰️ 历史记忆库</h2>"
    "<p style='color:#475569;font-size:0.82em;margin-bottom:14px;'>"
    "完整原始报告直出 · 排版 100% 对齐《快速分析》生成时的原貌</p>",
    unsafe_allow_html=True,
)


# ─────────────────────────────────────────────────────────────────────────────
# 六、完整历史记忆库（Expander 收纳，含彻底删除）
# ─────────────────────────────────────────────────────────────────────────────

with st.expander("📂 完整历史记忆库 —— 切换批次 / 彻底删除", expanded=False):
    if not run_entries:
        st.caption("暂无历史批次。")
    else:
        st.caption(f"共 {len(run_entries)} 个历史批次，点击「查看」切换主区域报告，「🗑️」永久删除。")
        for entry in run_entries:
            run_id    = entry["run_id"]
            is_active = run_id == st.session_state.get("selected_run_id")
            c1, c2, c3 = st.columns([6, 1, 1.4])
            with c1:
                st.markdown(
                    f"**[{entry['index']:02d}]** {entry['dt']} &nbsp;·&nbsp; "
                    f"`{entry['count']}只` {entry['preview']}",
                    unsafe_allow_html=True,
                )
            with c2:
                if st.button(
                    "查看",
                    key=f"lib_view_{run_id}",
                    use_container_width=True,
                    type="primary" if is_active else "secondary",
                ):
                    _select_run(run_id)
            with c3:
                if st.button("🗑️ 删除", key=f"lib_del_{run_id}", use_container_width=True):
                    st.session_state["confirm_delete_run_id"] = run_id
                    st.rerun()

            if st.session_state.get("confirm_delete_run_id") == run_id:
                st.warning("⚠️ 将物理删除该批次全部快照与报告，不可恢复。")
                ca, cb, _ = st.columns([1.5, 1.2, 5])
                with ca:
                    if st.button("确认删除", key=f"lib_cfm_{run_id}", type="primary", use_container_width=True):
                        delete_run_permanently(run_id)
                        st.session_state["confirm_delete_run_id"] = None
                        if st.session_state.get("selected_run_id") == run_id:
                            st.session_state["selected_run_id"] = (
                                run_entries[1]["run_id"]
                                if len(run_entries) > 1
                                else None
                            )
                        st.rerun()
                with cb:
                    if st.button("取消", key=f"lib_cnl_{run_id}", use_container_width=True):
                        st.session_state["confirm_delete_run_id"] = None
                        st.rerun()
            st.divider()


# ─────────────────────────────────────────────────────────────────────────────
# 七、报告主区域
# ─────────────────────────────────────────────────────────────────────────────

selected_run_id = st.session_state.get("selected_run_id")

if not selected_run_id:
    st.info("👈 请从左侧导航栏或上方历史库中选择一个分析批次。")
    st.stop()

# 加载当前批次数据
batch_snaps = get_run_snapshots(selected_run_id)
if not batch_snaps:
    st.warning("当前批次暂无可展示内容，可能已被删除或数据为空。")
    st.stop()

arts      = _fetch_artifacts(selected_run_id, batch_snaps)
date_slug = _slug_from_snaps(batch_snaps)

stock_snaps = [
    s for s in batch_snaps
    if s.get("code") != "__market__"
    and (not _norm(keyword) or
         _norm(keyword).lower() in _norm(s.get("code")).lower() or
         _norm(keyword).lower() in _norm(s.get("name")).lower())
    and (advice_filter == "全部" or advice_filter in _norm(s.get("operation_advice", "")))
]

# 当前批次摘要条
run_dt    = _run_time_from_snaps(batch_snaps)
all_names = [s.get("name") for s in batch_snaps if s.get("code") != "__market__" and s.get("name")]
st.markdown(
    f"<div style='background:rgba(14,26,52,0.7);border:1px solid #1e3a5f;"
    f"border-radius:8px;padding:10px 18px;margin-bottom:12px;display:flex;"
    f"align-items:center;gap:16px;flex-wrap:wrap;'>"
    f"<span style='color:#93C5FD;font-weight:700;font-size:0.88em;'>📋 当前批次</span>"
    f"<span style='color:#64748B;font-size:0.82em;'>{run_dt}</span>"
    f"<span style='color:#475569;font-size:0.78em;'>共 {len(all_names)} 只 · "
    f"{'、'.join(all_names[:5])}{'…' if len(all_names) > 5 else ''}</span>"
    f"</div>",
    unsafe_allow_html=True,
)

# ── 三 Tab 报告区 ──────────────────────────────────────────────────────────────
tab_market, tab_full, tab_stocks = st.tabs(["📊 大盘报告", "📄 全量完整报告", "🔬 个股分析"])

# ═══ Tab 1：大盘报告 ═══════════════════════════════════════════════════════════
with tab_market:
    raw = arts["market_md"]
    if raw:
        st.markdown(
            '<div class="report-container">',
            unsafe_allow_html=True,
        )
        st.markdown(raw)
        st.markdown("</div>", unsafe_allow_html=True)
    else:
        st.info("当前批次暂无大盘报告。（仅运行了「仅个股分析」模式时正常出现此提示）")

# ═══ Tab 2：全量完整报告 ════════════════════════════════════════════════════════
with tab_full:
    raw = arts["full_md"]
    if raw:
        st.markdown(raw)
    else:
        st.info("当前批次暂无全量报告。")

# ═══ Tab 3：个股分析（原始 Markdown 直出，零二次解析）═══════════════════════════
with tab_stocks:
    if not stock_snaps:
        if _norm(keyword) or advice_filter != "全部":
            st.info("当前过滤条件下没有命中的个股，请尝试调整左侧过滤条件。")
        else:
            st.info("当前批次暂无个股分析数据。")
    else:
        # 简明汇总表
        rows = []
        for i, s in enumerate(stock_snaps, 1):
            rows.append({
                "#":    i,
                "名称": _norm(s.get("name")) or "—",
                "代码": _norm(s.get("code")) or "—",
                "评级": f"{_advice_emoji(s.get('operation_advice',''))} "
                        f"{_norm(s.get('operation_advice')) or '—'}",
                "评分": _fmt_score(s.get("sentiment_score")),
                "时间": _fmt_dt(s.get("created_at")),
            })
        st.markdown(
            "<p style='color:#475569;font-size:0.75em;margin-bottom:4px;"
            "text-transform:uppercase;letter-spacing:0.08em;'>本批次个股汇总</p>",
            unsafe_allow_html=True,
        )
        st.dataframe(rows, use_container_width=True, hide_index=True)
        st.divider()

        # 逐一渲染每只股票的原始 Markdown 报告
        for i, snap in enumerate(stock_snaps, start=1):
            name     = _norm(snap.get("name")) or "—"
            code     = _norm(snap.get("code")) or "—"
            advice   = _norm(snap.get("operation_advice")) or "未评级"
            score    = _fmt_score(snap.get("sentiment_score"))
            dt_str   = _fmt_dt(snap.get("created_at"))
            badge_h  = _advice_badge_html(advice)
            report_md = _norm(snap.get("report_md"))

            # 个股折叠卡：标题行含评级徽章，展开后直出原文
            with st.expander(
                f"[{i:02d}] {name}（{code}）— {advice}  评分 {score}",
                expanded=(len(stock_snaps) <= 3),   # ≤3只时默认展开
            ):
                st.markdown(
                    f"<div style='display:flex;align-items:center;gap:10px;"
                    f"padding:6px 0 10px 0;border-bottom:1px solid #1e3a5f;"
                    f"margin-bottom:12px;flex-wrap:wrap;'>"
                    f"<span style='color:#F1F5F9;font-weight:700;font-size:1.05em;'>{name}</span>"
                    f"<span style='color:#64748B;font-size:0.85em;'>（{code}）</span>"
                    f"{badge_h}"
                    f"<span style='color:#475569;font-size:0.75em;margin-left:auto;'>"
                    f"{dt_str} &nbsp;·&nbsp; 评分 {score}</span>"
                    f"</div>",
                    unsafe_allow_html=True,
                )

                if report_md:
                    # ★ 原始 Markdown 直接渲染，100% 对齐快速分析原生输出
                    st.markdown(
                        '<div class="stock-report-container">',
                        unsafe_allow_html=True,
                    )
                    st.markdown(report_md)
                    st.markdown("</div>", unsafe_allow_html=True)
                else:
                    st.caption("暂无该股票的完整报告原文（快照可能仅保存了结构化字段）。")

                # 单股下载按钮
                snap_slug = (snap.get("created_at") or "")[:16].replace(":", "").replace(" ", "_").replace("-", "")
                if report_md:
                    st.download_button(
                        f"⬇️ 下载 {name} 报告 .txt",
                        data=_dl_bytes(report_md),
                        file_name=f"stock_{code}_{snap_slug}.txt",
                        mime="text/plain; charset=utf-8",
                        key=f"dl_snap_{snap.get('id', i)}",
                    )

                # 快速操作
                act1, act2, _ = st.columns([1, 1, 4])
                with act1:
                    if snap.get("code") and snap.get("code") != "__market__":
                        if st.button("📌 加入跟踪池", key=f"track_{snap.get('id', i)}", use_container_width=True):
                            add_to_quick_pool(snap["code"], snap.get("name", ""))
                            st.toast(f"✅ {name} 已加入跟踪池")
                with act2:
                    if st.button("👁️ 隐藏此条", key=f"hide_{snap.get('id', i)}", use_container_width=True):
                        delete_snapshot(snap.get("id"))
                        st.toast(f"已隐藏 {name}")
                        st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# 八、下载全家桶（5 个按钮，强制 UTF-8 纯文本，杜绝乱码）
# ─────────────────────────────────────────────────────────────────────────────

st.divider()
st.markdown(
    "<p style='color:#475569;font-size:0.75em;text-transform:uppercase;"
    "letter-spacing:0.1em;margin-bottom:10px;'>⬇ 下载全家桶 —— 全部为纯文本 .txt，强制 UTF-8</p>",
    unsafe_allow_html=True,
)

dc1, dc2, dc3, dc4, dc5 = st.columns(5)

with dc1:
    txt = arts["market_md"]
    st.download_button(
        "📊 大盘报告",
        data=_dl_bytes(txt),
        file_name=f"market_review_{date_slug}.txt",
        mime="text/plain; charset=utf-8",
        use_container_width=True,
        disabled=not txt,
        key="dl_market",
    )
    if not txt:
        st.caption("暂无")

with dc2:
    txt = arts["stock_md"]
    st.download_button(
        "🔬 个股报告",
        data=_dl_bytes(txt),
        file_name=f"stock_report_{date_slug}.txt",
        mime="text/plain; charset=utf-8",
        use_container_width=True,
        disabled=not txt,
        key="dl_stock",
    )
    if not txt:
        st.caption("暂无")

with dc3:
    txt = arts["full_md"]
    st.download_button(
        "📄 全量报告",
        data=_dl_bytes(txt),
        file_name=f"full_report_{date_slug}.txt",
        mime="text/plain; charset=utf-8",
        use_container_width=True,
        disabled=not txt,
        key="dl_full",
    )
    if not txt:
        st.caption("暂无")

with dc4:
    txt = arts["business_log"]
    st.download_button(
        "📋 运行日志",
        data=_dl_bytes(txt),
        file_name=f"business_log_{date_slug}.txt",
        mime="text/plain; charset=utf-8",
        use_container_width=True,
        disabled=not txt,
        key="dl_biz",
    )
    if not txt:
        st.caption("暂无")

with dc5:
    txt = arts["debug_log"]
    st.download_button(
        "🔧 通信日志",
        data=_dl_bytes(txt),
        file_name=f"debug_log_{date_slug}.txt",
        mime="text/plain; charset=utf-8",
        use_container_width=True,
        disabled=not txt,
        key="dl_dbg",
    )
    if not txt:
        st.caption("暂无")


# ─────────────────────────────────────────────────────────────────────────────
# 九、机要区（底部日志预览）
# ─────────────────────────────────────────────────────────────────────────────

st.divider()
st.markdown(
    "<p style='color:#475569;font-size:0.72em;text-transform:uppercase;"
    "letter-spacing:0.1em;'>🔒 机要区 · 底层运行日志预览</p>",
    unsafe_allow_html=True,
)

lc1, lc2 = st.columns(2)

with lc1:
    with st.expander("📋 运行日志（Business Log）", expanded=False):
        biz = arts["business_log"]
        st.text_area(
            label="",
            value=biz if biz else "暂无运行日志",
            height=260,
            disabled=True,
            key="log_biz_area",
        )

with lc2:
    with st.expander("🔧 通信日志（Debug Log）", expanded=False):
        dbg = arts["debug_log"]
        st.text_area(
            label="",
            value=dbg if dbg else "暂无通信日志",
            height=260,
            disabled=True,
            key="log_dbg_area",
        )
