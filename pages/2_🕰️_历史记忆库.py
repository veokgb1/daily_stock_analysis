# -*- coding: utf-8 -*-
"""
历史记忆库 v4 · 管理全入侧边栏 · 红多绿空配色本土化 · 日志链路修复
"""
import os
import re
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
    get_run_artifacts,
    get_run_snapshots,
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
# CSS：侧边栏宽度锁定 + Markdown 增强 + 本土化配色（红多绿空黄中性）
# ─────────────────────────────────────────────────────────────────────────────
st.markdown(
    """
<style>
/* ══ 侧边栏宽度锁死 ══ */
[data-testid="stSidebar"] {
    min-width: 260px !important;
    max-width: 310px !important;
}
[data-testid="stSidebar"] section[data-testid="stSidebarContent"] {
    padding: 1rem 0.75rem !important;
}
[data-testid="stSidebar"] .stButton > button {
    font-size: 0.78em !important;
    padding: 4px 6px !important;
    line-height: 1.3 !important;
}
[data-testid="stSidebar"] p,
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] small {
    font-size: 0.80em !important;
}

/* ══ 全局报告 Markdown 渲染 ══ */
.stMarkdown p, .stMarkdown li {
    color: #E2E8F0 !important;
    line-height: 1.88 !important;
    font-size: 0.95em !important;
}
.stMarkdown h1 {
    color: #93C5FD !important; font-size: 1.4em !important;
    margin-top: 1.5em !important; letter-spacing: 0.03em !important;
}
.stMarkdown h2 {
    color: #93C5FD !important; font-size: 1.15em !important;
    margin-top: 1.3em !important;
    border-bottom: 1px solid #1e3a5f !important;
    padding-bottom: 4px !important;
}
.stMarkdown h3 { color: #7DD3FC !important; font-size: 1.05em !important; margin-top: 1.1em !important; }
.stMarkdown strong { color: #F1F5F9 !important; }
.stMarkdown em    { color: #CBD5E1 !important; }
.stMarkdown blockquote {
    border-left: 3px solid #3B82F6 !important; padding: 10px 1.2em !important;
    color: #CBD5E1 !important; background: rgba(59,130,246,0.07) !important;
    border-radius: 0 6px 6px 0 !important; margin: 0.8em 0 !important;
}
.stMarkdown table { border-collapse: collapse !important; width: 100% !important;
    margin: 0.8em 0 !important; font-size: 0.9em !important; }
.stMarkdown th {
    background: #1e3a5f !important; color: #93C5FD !important;
    padding: 7px 14px !important; font-weight: 600 !important;
    text-align: center !important; border: 1px solid #2d4a7a !important;
}
.stMarkdown td {
    padding: 6px 14px !important; border: 1px solid #1f2937 !important;
    color: #E2E8F0 !important; text-align: center !important;
}
.stMarkdown tr:nth-child(even) td { background: rgba(30,58,115,0.12) !important; }
.stMarkdown tr:hover td { background: rgba(59,130,246,0.06) !important; }
.stMarkdown code {
    background: rgba(30,42,71,0.75) !important; color: #93C5FD !important;
    padding: 1px 6px !important; border-radius: 4px !important;
    font-size: 0.88em !important; font-family: 'JetBrains Mono','Consolas',monospace !important;
}
.stMarkdown pre {
    background: rgba(10,20,45,0.9) !important; border: 1px solid #1e3a5f !important;
    border-radius: 8px !important; padding: 14px !important;
}
.stMarkdown hr { border-color: #1e3a5f !important; margin: 1em 0 !important; }

/* ══ 报告外框容器 ══ */
.rpt-wrap {
    background: linear-gradient(160deg, #0a1929 0%, #0d1117 100%);
    border: 1px solid #1e3a5f; border-radius: 12px;
    padding: 26px 34px; margin: 6px 0 14px 0;
}
.stock-rpt-wrap {
    background: rgba(6,12,24,0.75); border: 1px solid #1e3358;
    border-radius: 10px; padding: 18px 26px; margin: 4px 0 12px 0;
}

/* ══ A 股本土化评级徽章 ══
   严格遵循：看多/买入 = 红，看空/卖出 = 绿，观望 = 黄
   （与国际惯例相反，这是 A 股散户的认知习惯）
*/
.badge {
    display: inline-block; padding: 2px 10px; border-radius: 4px;
    font-size: 0.78em; font-weight: 700; letter-spacing: 0.05em;
    vertical-align: middle;
}
/* 买入 / 看多 → 红色 */
.badge-bull {
    background: rgba(239,68,68,0.14);
    color: #fca5a5;
    border: 1px solid rgba(239,68,68,0.55);
}
/* 卖出 / 看空 → 绿色 */
.badge-bear {
    background: rgba(34,197,94,0.12);
    color: #86efac;
    border: 1px solid rgba(34,197,94,0.5);
}
/* 观望 / 中性 → 黄色 */
.badge-neutral {
    background: rgba(234,179,8,0.12);
    color: #fde047;
    border: 1px solid rgba(234,179,8,0.5);
}

/* ══ 批次信息条 ══ */
.batch-bar {
    background: rgba(14,26,52,0.75); border: 1px solid #1e3a5f;
    border-radius: 8px; padding: 9px 16px; margin-bottom: 10px;
    display: flex; align-items: center; gap: 14px; flex-wrap: wrap;
}
.batch-bar .b-label { color: #93C5FD; font-weight: 700; font-size: 0.87em; }
.batch-bar .b-time  { color: #64748B; font-size: 0.81em; }
.batch-bar .b-stocks{ color: #475569; font-size: 0.76em; }

/* ══ 下载按钮 ══ */
.stDownloadButton > button {
    background: rgba(10,20,50,0.9) !important; color: #93C5FD !important;
    border: 1px solid rgba(59,130,246,0.45) !important;
    border-radius: 8px !important; font-weight: 600 !important; font-size: 0.81em !important;
}
.stDownloadButton > button:hover {
    background: rgba(59,130,246,0.18) !important; border-color: #3B82F6 !important;
}

/* ══ 日志文本区 ══ */
textarea[disabled] { color: #4B5563 !important; font-size: 0.79em !important; }

/* ══ 分隔线 ══ */
hr { border-color: #1f2937 !important; margin: 12px 0 !important; }
</style>
""",
    unsafe_allow_html=True,
)


# ─────────────────────────────────────────────────────────────────────────────
# 工具函数
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


def _dl_bytes(text: str) -> bytes:
    return (text or "").encode("utf-8")


def _slug(batch_snaps: List[Dict[str, Any]]) -> str:
    ts = [s.get("created_at") for s in batch_snaps if s.get("created_at")]
    if not ts:
        return datetime.now().strftime("%Y%m%d_%H%M%S")
    try:
        return datetime.fromisoformat(max(ts)).strftime("%Y%m%d_%H%M%S")
    except Exception:
        return datetime.now().strftime("%Y%m%d_%H%M%S")


def _run_dt(batch_snaps: List[Dict[str, Any]]) -> str:
    ts = [s.get("created_at") for s in batch_snaps if s.get("created_at")]
    return _fmt_dt(max(ts) if ts else None)


# ── A 股本土化配色徽章 ────────────────────────────────────────────────────────
# 规范：买入/看多 = 红色   卖出/看空 = 绿色   观望 = 黄色
_BULL_WORDS  = ("买", "增持", "看涨", "做多", "强烈推荐")
_BEAR_WORDS  = ("卖", "减持", "离场", "做空", "回避", "看跌", "看空")
_BULL_EMOJI  = "🔴"
_BEAR_EMOJI  = "🟢"
_NEUT_EMOJI  = "🟡"


def _badge(advice: str) -> str:
    t = _norm(advice) or "未评级"
    if any(w in t for w in _BULL_WORDS):
        return f'<span class="badge badge-bull">▲ {t}</span>'
    if any(w in t for w in _BEAR_WORDS):
        return f'<span class="badge badge-bear">▼ {t}</span>'
    return f'<span class="badge badge-neutral">◆ {t}</span>'


def _emoji(advice: str) -> str:
    t = _norm(advice)
    if any(w in t for w in _BULL_WORDS):
        return _BULL_EMOJI
    if any(w in t for w in _BEAR_WORDS):
        return _BEAR_EMOJI
    return _NEUT_EMOJI


# A 股本土化动态颜色（评分 + 评级）
# 评分：≥80 红（强多）/ 40-79 黄（中性）/ <40 绿（弱空）
# 评级：买入看多 → 红   卖出看空 → 绿   观望 → 黄
def _score_color(raw: Any) -> str:
    try:
        s = float(raw)
        if s >= 80:
            return "#fca5a5"    # 红 · 强多
        elif s >= 40:
            return "#fde047"    # 黄 · 中性
        else:
            return "#86efac"    # 绿 · 弱/空
    except Exception:
        return "#94a3b8"        # 灰 · 无数据


def _advice_color(advice: str) -> str:
    t = _norm(advice)
    if any(w in t for w in _BULL_WORDS):
        return "#fca5a5"    # 红
    if any(w in t for w in _BEAR_WORDS):
        return "#86efac"    # 绿
    return "#fde047"        # 黄


def _stock_info_strip(
    idx: int, total: int,
    name: str, code: str,
    adv: str, score_raw: Any, dt_str: str,
) -> str:
    """渲染个股头部：序号[i/N] + 名称 + 代码 + 动态颜色评级 + 动态颜色评分 + 时间"""
    score_str = _fmt_score(score_raw)
    sc        = _score_color(score_raw)
    ac        = _advice_color(adv)
    adv_txt   = adv or "未评级"
    # 评级前缀箭头
    if any(w in adv_txt for w in _BULL_WORDS):
        arr = "▲"
    elif any(w in adv_txt for w in _BEAR_WORDS):
        arr = "▼"
    else:
        arr = "◆"

    return (
        f"<div style='display:flex;align-items:center;gap:10px;"
        f"padding:8px 14px;border-radius:8px;"
        f"background:rgba(10,18,40,0.7);border:1px solid #1e3a5f;"
        f"margin-bottom:12px;flex-wrap:wrap;'>"
        # 序号
        f"<span style='color:#475569;font-size:0.74em;font-family:monospace;"
        f"font-weight:700;min-width:40px;'>[{idx}/{total}]</span>"
        # 名称
        f"<span style='color:#F1F5F9;font-weight:700;font-size:1.02em;'>{name}</span>"
        # 代码
        f"<span style='color:#64748B;font-size:0.82em;'>（{code}）</span>"
        # 动态颜色评级
        f"<span style='color:{ac};font-weight:700;font-size:0.86em;"
        f"background:{ac}18;padding:2px 9px;border-radius:4px;"
        f"border:1px solid {ac}55;'>{arr} {adv_txt}</span>"
        # 动态颜色评分
        f"<span style='color:{sc};font-weight:700;font-size:0.84em;"
        f"font-family:monospace;'>情绪 {score_str}</span>"
        # 时间（右对齐）
        f"<span style='color:#475569;font-size:0.72em;margin-left:auto;'>{dt_str}</span>"
        f"</div>"
    )


def _inject_seq_into_full_md(full_md: str, stock_snaps: List[Dict[str, Any]]) -> str:
    """
    向全量报告 Markdown 中的个股标题注入 [i/N] 序号。

    匹配规则：任何以 ## 开头、后跟股票名称或代码的标题行，
    按 stock_snaps 出现顺序逐个替换（精确匹配代码），
    无法精确匹配的标题保持原样，不破坏大盘报告等其他段落。
    """
    if not full_md or not stock_snaps:
        return full_md
    total = len(stock_snaps)
    result = full_md
    for idx, snap in enumerate(stock_snaps, start=1):
        code = _norm(snap.get("code")) or ""
        name = _norm(snap.get("name")) or ""
        if not code:
            continue
        # 匹配形如 "## 名称（代码）" 或 "## 名称 (代码)" 的标题，允许全/半角括号
        pattern = (
            r"(^|\n)(##\s+"
            + re.escape(name)
            + r"\s*[（(]"
            + re.escape(code)
            + r"[）)])"
        )
        replacement = rf"\1## [{idx}/{total}] {name}（{code}）"
        result = re.sub(pattern, replacement, result)
    return result


# ─────────────────────────────────────────────────────────────────────────────
# 批次条目构建（带快照缓存，避免重复查询）
# ─────────────────────────────────────────────────────────────────────────────

def _build_entries(run_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    for idx, row in enumerate(run_rows, start=1):
        run_id = row.get("run_id")
        if not run_id:
            continue
        snaps       = get_run_snapshots(run_id)
        stock_snaps = [s for s in snaps if s.get("code") != "__market__"]
        names       = [s.get("name") for s in stock_snaps if s.get("name")]
        count       = len(names)
        preview     = "、".join(names[:3]) + ("…" if count > 3 else "")
        dt_str      = _run_dt(snaps)
        entries.append({
            "idx":     idx,
            "run_id":  run_id,
            "dt":      dt_str,
            "count":   count,
            "preview": preview or "暂无标的",
            "snaps":   snaps,
        })
    return entries


def _select(run_id: str) -> None:
    if run_id != st.session_state.get("selected_run_id"):
        st.session_state["selected_run_id"] = run_id
        st.session_state["confirm_delete_run_id"] = None
        st.rerun()


def _ensure_selected(entries: List[Dict[str, Any]]) -> None:
    valid = {e["run_id"] for e in entries}
    if st.session_state.get("selected_run_id") not in valid:
        st.session_state["selected_run_id"] = entries[0]["run_id"] if entries else None


# ─────────────────────────────────────────────────────────────────────────────
# run_artifacts 提取（优先原文 Markdown，备份拼接快照）
# ─────────────────────────────────────────────────────────────────────────────

def _fetch(run_id: str, batch_snaps: List[Dict[str, Any]]) -> Dict[str, str]:
    """
    日志链路说明：
    - 优先读取 run_artifacts 表（save_run_artifacts 写入时同时保存报告+日志）
    - 若 run_artifacts 行不存在（旧版批次），仅能从 analysis_snapshots.report_md 拼接报告
    - 日志字段（business_log / debug_log）无备用来源
    """
    raw_arts: Dict[str, Any] = get_run_artifacts(run_id) or {}
    has_artifacts_row = bool(raw_arts)

    stock_snaps = [s for s in batch_snaps if s.get("code") != "__market__"]

    # ── 大盘 Markdown ─────────────────────────────────────────────────────────
    market_md = _norm(raw_arts.get("market_report_md"))
    if not market_md:
        market_md = "\n\n".join(
            _norm(s.get("report_md"))
            for s in batch_snaps if s.get("code") == "__market__"
        ).strip()

    # ── 个股 Markdown ─────────────────────────────────────────────────────────
    stock_md = _norm(raw_arts.get("stock_report_md"))
    if not stock_md:
        parts: List[str] = []
        for s in stock_snaps:
            parts.append(f"## {_norm(s.get('name')) or '—'}（{_norm(s.get('code')) or '—'}）")
            parts.append(_norm(s.get("report_md")) or "_暂无报告内容_")
            parts.append("")
        stock_md = "\n".join(parts).strip()

    # ── 全量 Markdown ─────────────────────────────────────────────────────────
    full_md = _norm(raw_arts.get("full_report_md"))
    if not full_md:
        segs: List[str] = []
        if market_md:
            segs.append(f"# 大盘报告\n\n{market_md}")
        if stock_md:
            segs.append(f"# 个股报告\n\n{stock_md}")
        full_md = "\n\n---\n\n".join(segs)

    # ── 日志（仅来源于 run_artifacts，无备用）─────────────────────────────────
    business_log = _norm(raw_arts.get("business_log"))
    debug_log    = _norm(raw_arts.get("debug_log"))

    return {
        "market_md":     market_md,
        "stock_md":      stock_md,
        "full_md":       full_md,
        "business_log":  business_log,
        "debug_log":     debug_log,
        "has_arts_row":  has_artifacts_row,   # 诊断用
    }


# ─────────────────────────────────────────────────────────────────────────────
# 侧边栏：历史管理全入口（查看 + 删除 + 过滤 + 危险区）
# ─────────────────────────────────────────────────────────────────────────────

run_rows    = list_recent_runs(limit=None)
run_entries = _build_entries(run_rows)
_ensure_selected(run_entries)

with st.sidebar:
    st.markdown(
        "<div style='color:#475569;font-size:0.68em;text-transform:uppercase;"
        "letter-spacing:0.1em;margin-bottom:8px;font-weight:600;'>🕰️ 最近 3 条批次</div>",
        unsafe_allow_html=True,
    )

    # ── 最近 3 条（直接展示，含删除按钮）─────────────────────────────────────
    def _render_entry_row(entry: Dict[str, Any], key_prefix: str) -> None:
        """渲染一行批次：查看按钮 + 删除按钮，下方可展开确认框"""
        run_id       = entry["run_id"]
        is_active    = run_id == st.session_state.get("selected_run_id")
        is_confirming = st.session_state.get("confirm_delete_run_id") == run_id
        label        = f"{'▶ ' if is_active else ''}[{entry['idx']:02d}] {entry['dt']}"

        col_btn, col_del = st.columns([5, 1])
        with col_btn:
            if st.button(
                label,
                key=f"{key_prefix}_sel_{run_id}",
                use_container_width=True,
                type="primary" if is_active else "secondary",
            ):
                _select(run_id)
        with col_del:
            if st.button("🗑️", key=f"{key_prefix}_del_{run_id}",
                         use_container_width=True, help="永久删除此批次"):
                st.session_state["confirm_delete_run_id"] = run_id
                st.rerun()

        if is_active and not is_confirming:
            st.caption(f"  {entry['count']} 只 · {entry['preview']}")

        if is_confirming:
            st.warning(f"删除 {entry['count']} 只批次，不可恢复！")
            ca, cb = st.columns(2)
            with ca:
                if st.button("确认删除", key=f"{key_prefix}_cfm_{run_id}",
                             type="primary", use_container_width=True):
                    delete_run_permanently(run_id)
                    st.session_state["confirm_delete_run_id"] = None
                    if st.session_state.get("selected_run_id") == run_id:
                        remaining = [e for e in run_entries if e["run_id"] != run_id]
                        st.session_state["selected_run_id"] = (
                            remaining[0]["run_id"] if remaining else None
                        )
                    st.rerun()
            with cb:
                if st.button("取消", key=f"{key_prefix}_cnl_{run_id}",
                             use_container_width=True):
                    st.session_state["confirm_delete_run_id"] = None
                    st.rerun()

    if not run_entries:
        st.caption("暂无历史记录，运行一次分析后会出现在这里。")
    else:
        for entry in run_entries[:3]:
            _render_entry_row(entry, "top")

    # ── 📂 所有历史记录（折叠，含剩余批次 + 删除）────────────────────────────
    if len(run_entries) > 3:
        with st.expander(f"📂 所有历史记录（共 {len(run_entries)} 条）", expanded=False):
            for entry in run_entries[3:]:
                _render_entry_row(entry, "lib")
    elif run_entries:
        st.markdown(
            "<div style='color:#334155;font-size:0.73em;margin-top:4px;'>"
            f"共 {len(run_entries)} 条历史记录</div>",
            unsafe_allow_html=True,
        )

    st.divider()

    # ── 过滤条件 ──────────────────────────────────────────────────────────────
    st.markdown(
        "<div style='color:#475569;font-size:0.68em;text-transform:uppercase;"
        "letter-spacing:0.1em;margin-bottom:6px;font-weight:600;'>🔍 过滤</div>",
        unsafe_allow_html=True,
    )
    advice_filter = st.selectbox(
        "评级过滤", ["全部", "买入", "观望", "卖出"],
        index=0, label_visibility="collapsed",
    )
    keyword = st.text_input(
        "股票搜索", placeholder="代码 / 名称",
        label_visibility="collapsed",
    )

    st.divider()

    # ── 危险区 ────────────────────────────────────────────────────────────────
    with st.expander("⚠️ 危险区", expanded=False):
        if st.button("🗑️ 清空全部历史数据", use_container_width=True):
            st.session_state["confirm_clear_all_data"] = True
        if st.session_state.get("confirm_clear_all_data"):
            st.error("此操作清空全库，不可恢复！")
            if st.button("🚨 确认清空", use_container_width=True, type="primary"):
                clear_all_data()
                st.session_state.update({
                    "confirm_clear_all_data": False,
                    "confirm_delete_run_id":  None,
                    "selected_run_id":        None,
                })
                st.success("已清空")
                st.rerun()
            if st.button("取消", key="danger_cancel", use_container_width=True):
                st.session_state["confirm_clear_all_data"] = False
                st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# 主页面：纯净报告区（不含任何历史管理控件）
# ─────────────────────────────────────────────────────────────────────────────

st.markdown(
    "<h2 style='margin-bottom:2px;color:#F1F5F9;font-weight:700;'>🕰️ 历史记忆库</h2>"
    "<p style='color:#475569;font-size:0.80em;margin-bottom:12px;'>"
    "原始 Markdown 直出 · 100% 对齐《快速分析》原貌 · "
    "<span style='color:#fca5a5;'>🔴买多</span> "
    "<span style='color:#86efac;'>🟢卖空</span> "
    "<span style='color:#fde047;'>🟡观望</span></p>",
    unsafe_allow_html=True,
)

selected_run_id = st.session_state.get("selected_run_id")
if not selected_run_id:
    st.info("👈 请从左侧导航栏选择一个分析批次开始浏览。")
    st.stop()

# 加载当前批次快照
batch_snaps = get_run_snapshots(selected_run_id)
if not batch_snaps:
    st.warning("当前批次暂无数据，可能已被删除。请从左侧重新选择。")
    st.stop()

# 一次性加载全部 artifacts（含日志）
arts      = _fetch(selected_run_id, batch_snaps)
date_slug = _slug(batch_snaps)

# 过滤个股
kw = _norm(keyword).lower()
stock_snaps = [
    s for s in batch_snaps
    if s.get("code") != "__market__"
    and (not kw or kw in _norm(s.get("code")).lower() or kw in _norm(s.get("name")).lower())
    and (advice_filter == "全部" or advice_filter in _norm(s.get("operation_advice", "")))
]

# ── 批次摘要信息条 ────────────────────────────────────────────────────────────
run_dt_str = _run_dt(batch_snaps)
all_names  = [s.get("name") for s in batch_snaps if s.get("code") != "__market__" and s.get("name")]
st.markdown(
    f"<div class='batch-bar'>"
    f"<span class='b-label'>📋 当前批次</span>"
    f"<span class='b-time'>{run_dt_str}</span>"
    f"<span class='b-stocks'>共 {len(all_names)} 只 · "
    f"{'、'.join(all_names[:6])}{'…' if len(all_names) > 6 else ''}</span>"
    f"</div>",
    unsafe_allow_html=True,
)

# ── 三 Tab 报告区 ─────────────────────────────────────────────────────────────
tab_market, tab_full, tab_stocks = st.tabs(["📊 大盘报告", "📄 全量完整报告", "🔬 个股分析"])

# ═══ Tab 1：大盘报告 ══════════════════════════════════════════════════════════
with tab_market:
    md = arts["market_md"]
    if md:
        st.markdown('<div class="rpt-wrap">', unsafe_allow_html=True)
        st.markdown(md)
        st.markdown("</div>", unsafe_allow_html=True)
    else:
        st.info(
            "当前批次暂无大盘报告。\n\n"
            "- 若运行模式为「仅个股分析」，属正常现象。\n"
            "- 若运行过「全量分析」或「仅大盘复盘」，请检查快速分析页面的 `save_run_artifacts` 调用。"
        )

# ═══ Tab 2：全量报告 ══════════════════════════════════════════════════════════
with tab_full:
    md = arts["full_md"]
    if md:
        # 动态注入个股序号（正则替换标题行，不破坏大盘等其他段落）
        md = _inject_seq_into_full_md(md, stock_snaps)
        st.markdown(md)
    else:
        st.info("当前批次暂无全量报告。")

# ═══ Tab 3：个股分析（原始 Markdown 直出）════════════════════════════════════
with tab_stocks:
    if not stock_snaps:
        if kw or advice_filter != "全部":
            st.info("当前过滤条件下无命中个股，请调整左侧过滤条件。")
        else:
            st.info("当前批次暂无个股分析数据。")
    else:
        # 汇总简表（含本土化配色评级列）
        tbl = []
        for i, s in enumerate(stock_snaps, 1):
            adv = _norm(s.get("operation_advice"))
            tbl.append({
                "#":   i,
                "名称": _norm(s.get("name")) or "—",
                "代码": _norm(s.get("code")) or "—",
                "评级": f"{_emoji(adv)} {adv or '—'}",
                "评分": _fmt_score(s.get("sentiment_score")),
                "时间": _fmt_dt(s.get("created_at")),
            })
        st.markdown(
            "<p style='color:#475569;font-size:0.73em;margin-bottom:4px;"
            "text-transform:uppercase;letter-spacing:0.08em;'>本批次个股汇总</p>",
            unsafe_allow_html=True,
        )
        st.dataframe(tbl, use_container_width=True, hide_index=True)
        st.divider()

        # 逐个渲染原始报告（expander 折叠，≤3只时自动展开）
        total       = len(stock_snaps)
        auto_expand = total <= 3
        for i, snap in enumerate(stock_snaps, start=1):
            name      = _norm(snap.get("name")) or "—"
            code      = _norm(snap.get("code")) or "—"
            adv       = _norm(snap.get("operation_advice"))
            score_raw = snap.get("sentiment_score")
            dt_str    = _fmt_dt(snap.get("created_at"))
            rpt_md    = _norm(snap.get("report_md"))

            # expander 标题：[序号/总数] 名称（代码）—— 评级
            with st.expander(
                f"[{i}/{total}] {name}（{code}）—— {adv or '未评级'}",
                expanded=auto_expand,
            ):
                # ── 层 1：批次进度标题（醒目序号，独立 UI，绝不与报告正文混用）──
                st.markdown(
                    f"### 📌 批次进度：**[{i}/{total}]**　｜　"
                    f"股票：**{name}（{code}）**"
                )

                # ── 层 2：动态颜色信息条（红/绿/黄 评级 + 评分 + 时间）──────────
                st.markdown(
                    _stock_info_strip(i, total, name, code, adv, score_raw, dt_str),
                    unsafe_allow_html=True,
                )

                # ── 层 3：分割线，彻底隔离元数据区与报告正文区 ─────────────────
                st.divider()

                # ── 层 4：原生报告正文（干净直出，不再拼接任何序号字符串）────────
                if rpt_md:
                    st.markdown('<div class="stock-rpt-wrap">', unsafe_allow_html=True)
                    st.markdown(rpt_md)
                    st.markdown("</div>", unsafe_allow_html=True)
                else:
                    st.caption(
                        "⚠️ 该快照暂无完整报告原文。"
                        "（可能此快照仅保存了结构化字段而未保存 Markdown 原文）"
                    )

                # 操作行：下载 + 跟踪
                snap_slug = (snap.get("created_at") or "")[:16] \
                    .replace(":", "").replace(" ", "_").replace("-", "")
                dl_col, tr_col, hd_col, _ = st.columns([2, 1.2, 1.2, 3])
                with dl_col:
                    if rpt_md:
                        st.download_button(
                            f"⬇️ 下载 {name} .txt",
                            data=_dl_bytes(rpt_md),
                            file_name=f"stock_{code}_{snap_slug}.txt",
                            mime="text/plain; charset=utf-8",
                            key=f"dl_snap_{snap.get('id', i)}",
                        )
                with tr_col:
                    if snap.get("code") not in ("", None, "__market__"):
                        if st.button("📌 跟踪", key=f"track_{snap.get('id', i)}",
                                     use_container_width=True):
                            add_to_quick_pool(snap["code"], snap.get("name", ""))
                            st.toast(f"✅ {name} 已加入跟踪池")
                with hd_col:
                    if st.button("👁️ 隐藏", key=f"hide_{snap.get('id', i)}",
                                 use_container_width=True):
                        delete_snapshot(snap.get("id"))
                        st.toast(f"已隐藏 {name}")
                        st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# 下载全家桶（5 个按钮，全部 UTF-8 纯文本）
# ─────────────────────────────────────────────────────────────────────────────

st.divider()
st.markdown(
    "<p style='color:#475569;font-size:0.73em;text-transform:uppercase;"
    "letter-spacing:0.09em;margin-bottom:10px;'>⬇ 下载全家桶 —— 全部为纯文本 .txt · 强制 UTF-8</p>",
    unsafe_allow_html=True,
)

d1, d2, d3, d4, d5 = st.columns(5)

with d1:
    t = arts["market_md"]
    st.download_button(
        "📊 大盘报告",
        data=_dl_bytes(t), file_name=f"market_{date_slug}.txt",
        mime="text/plain; charset=utf-8",
        use_container_width=True, disabled=not t, key="dl_market",
    )
    if not t:
        st.caption("暂无")

with d2:
    t = arts["stock_md"]
    st.download_button(
        "🔬 个股报告",
        data=_dl_bytes(t), file_name=f"stocks_{date_slug}.txt",
        mime="text/plain; charset=utf-8",
        use_container_width=True, disabled=not t, key="dl_stocks",
    )
    if not t:
        st.caption("暂无")

with d3:
    t = arts["full_md"]
    st.download_button(
        "📄 全量报告",
        data=_dl_bytes(t), file_name=f"full_{date_slug}.txt",
        mime="text/plain; charset=utf-8",
        use_container_width=True, disabled=not t, key="dl_full",
    )
    if not t:
        st.caption("暂无")

with d4:
    t = arts["business_log"]
    st.download_button(
        "📋 运行日志",
        data=_dl_bytes(t), file_name=f"bizlog_{date_slug}.txt",
        mime="text/plain; charset=utf-8",
        use_container_width=True, disabled=not t, key="dl_biz",
    )
    if not t:
        st.caption("暂无")

with d5:
    t = arts["debug_log"]
    st.download_button(
        "🔧 通信日志",
        data=_dl_bytes(t), file_name=f"dbglog_{date_slug}.txt",
        mime="text/plain; charset=utf-8",
        use_container_width=True, disabled=not t, key="dl_dbg",
    )
    if not t:
        st.caption("暂无")


# ─────────────────────────────────────────────────────────────────────────────
# 机要区：日志预览 + 断层诊断
# ─────────────────────────────────────────────────────────────────────────────

st.divider()
st.markdown(
    "<p style='color:#475569;font-size:0.70em;text-transform:uppercase;"
    "letter-spacing:0.1em;'>🔒 机要区 · 底层运行日志</p>",
    unsafe_allow_html=True,
)

biz = arts["business_log"]
dbg = arts["debug_log"]
has_arts = arts["has_arts_row"]

# 日志断层诊断提示
if not has_arts:
    st.warning(
        "⚠️ **日志断层诊断**：当前批次在 `run_artifacts` 表中**没有记录行**。\n\n"
        "这说明本次分析完成时，`save_run_artifacts()` 未被调用，或调用时 `run_id` 不匹配。\n"
        "报告内容来自 `analysis_snapshots.report_md`（个股快照），"
        "但日志（business_log / debug_log）无法从快照表中恢复。\n\n"
        "**修复建议**：检查《快速分析》页面的 `save_run_artifacts` 调用链路，"
        "确保传入正确的 `business_log` 和 `debug_log` 参数。"
    )
elif has_arts and not biz and not dbg:
    st.info(
        "ℹ️ **日志断层诊断**：`run_artifacts` 行存在，但 `business_log` 和 `debug_log` 均为空字符串。\n\n"
        "说明 `save_run_artifacts()` 被调用时，两个日志参数传入了空值。\n"
        "请检查快速分析页面中日志收集与传参的时机是否正确。"
    )

lc1, lc2 = st.columns(2)

with lc1:
    with st.expander("📋 运行日志（Business Log）", expanded=False):
        st.text_area(
            label="",
            value=biz if biz else "── 暂无运行日志 ──",
            height=280, disabled=True, key="log_biz",
        )
        if biz:
            st.caption(f"共 {len(biz):,} 字符 · {biz.count(chr(10))+1} 行")

with lc2:
    with st.expander("🔧 通信日志（Debug Log）", expanded=False):
        st.text_area(
            label="",
            value=dbg if dbg else "── 暂无通信日志 ──",
            height=280, disabled=True, key="log_dbg",
        )
        if dbg:
            st.caption(f"共 {len(dbg):,} 字符 · {dbg.count(chr(10))+1} 行")
