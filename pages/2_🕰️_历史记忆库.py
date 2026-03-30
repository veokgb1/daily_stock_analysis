# -*- coding: utf-8 -*-
"""
历史记忆库 · DUKA Stock Analysis Engine V5-Pro
彻底重构版 —— 精简 UI + 报告 100% 对齐《快速分析》原生格式
"""
import json
import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from src.config import setup_env

setup_env()

import streamlit as st

from src.formatters import markdown_to_plain_text
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
    page_title="历史记忆库 · DUKA Stock Analysis Engine V5-Pro",
    page_icon="🕰️",
    layout="wide",
)
enforce_sidebar_password_gate()

_HISTORY_PAGE_ID = "history_memory"
st.session_state["_active_streamlit_page"] = _HISTORY_PAGE_ID

# ── Session state 初始化 ──────────────────────────────────────────────────────
st.session_state.setdefault("selected_run_id", None)
st.session_state.setdefault("report_view_mode", "全量模式")
st.session_state.setdefault("report_preset", "快速决策组合")
st.session_state.setdefault("hide_pending_snap_id", None)
st.session_state.setdefault("confirm_clear_all_data", False)
st.session_state.setdefault("confirm_delete_run_id", None)

PRESET_OPTIONS: List[str] = ["快速决策组合", "风险审查组合", "数据复盘组合"]

# ── 全局 CSS 注入 ──────────────────────────────────────────────────────────────
st.markdown(
    """
<style>
/* ═══ 全局正文 ═══ */
.stMarkdown p, .stMarkdown li {
    color: #E2E8F0 !important;
    line-height: 1.75 !important;
}
.stMarkdown h1 { color: #93C5FD !important; font-size: 1.3em !important; margin-top: 1.4em !important; }
.stMarkdown h2 { color: #93C5FD !important; font-size: 1.1em !important; margin-top: 1.2em !important;
    border-bottom: 1px solid #1e3a5f !important; padding-bottom: 4px !important; }
.stMarkdown h3 { color: #7DD3FC !important; font-size: 1.0em !important; margin-top: 1.0em !important; }
.stMarkdown blockquote {
    border-left: 3px solid #3B82F6 !important;
    padding: 10px 1.2em !important;
    color: #CBD5E1 !important;
    background: rgba(59,130,246,0.06) !important;
    border-radius: 0 6px 6px 0 !important;
    margin: 1em 0 !important;
}
.stMarkdown table { border-collapse: collapse !important; width: 100% !important; font-size: 0.9em !important; margin: 1em 0 !important; }
.stMarkdown th { background: #1e3a5f !important; color: #93C5FD !important; padding: 8px 14px !important;
    text-align: center !important; font-weight: 600 !important; }
.stMarkdown td { padding: 6px 14px !important; border-bottom: 1px solid #1f2937 !important;
    color: #E2E8F0 !important; text-align: center !important; }
.stMarkdown tr:hover td { background: rgba(255,255,255,0.03) !important; }

/* ═══ 大盘报告容器 ═══ */
.market-prose-wrap {
    background: linear-gradient(160deg, #0d1f35 0%, #0f172a 100%);
    border: 1px solid #1e3a5f;
    border-radius: 12px;
    padding: 28px 36px;
    margin: 8px 0 16px 0;
}
.market-prose-wrap .stMarkdown p { color: #E2E8F0 !important; line-height: 1.95 !important; font-size: 0.97em !important; }
.market-prose-wrap .stMarkdown h1 { color: #93C5FD !important; font-size: 1.3em !important; letter-spacing: 0.04em !important; }
.market-prose-wrap .stMarkdown h2 { color: #93C5FD !important; border-bottom: 1px solid #1e3a5f !important; }
.market-prose-wrap .stMarkdown h3 { color: #7DD3FC !important; }

/* ═══ 精简 Metric 卡片 ═══ */
.run-metric-card {
    background: linear-gradient(160deg, #0d1f35 0%, #0f172a 100%);
    border: 1.5px solid #1e3a5f;
    border-radius: 12px;
    padding: 14px 18px;
    transition: border-color .2s;
}
.run-metric-card.active {
    border-color: #3B82F6;
    background: linear-gradient(160deg, #0f2744 0%, #0a1e38 100%);
}
.rmc-index { font-size: 0.68em; color: #475569; letter-spacing: 0.1em; text-transform: uppercase; margin-bottom: 5px; }
.rmc-time  { font-size: 0.88em; font-weight: 700; color: #F1F5F9; margin-bottom: 4px; }
.rmc-badge {
    display: inline-block; background: rgba(59,130,246,0.15);
    border: 1px solid rgba(59,130,246,0.4); color: #93C5FD;
    border-radius: 999px; padding: 1px 10px; font-size: 0.72em; font-weight: 700;
    margin-bottom: 5px;
}
.rmc-badge.active { background: rgba(59,130,246,0.3); border-color: #3B82F6; }
.rmc-preview { font-size: 0.75em; color: #64748B; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }

/* ═══ 个股卡片头部 ═══ */
.scard-header {
    display: flex; align-items: center; gap: 10px;
    padding: 6px 0 10px 0; border-bottom: 1px solid #1f2937;
    margin-bottom: 10px; flex-wrap: wrap;
}
.scard-idx   { color: #475569; font-size: 0.75em; font-weight: 700; min-width: 22px; font-family: monospace; }
.scard-name  { color: #F1F5F9; font-size: 1.02em; font-weight: 700; letter-spacing: 0.02em; }
.scard-code  { color: #64748B; font-size: 0.82em; }
.scard-meta  { color: #475569; font-size: 0.74em; margin-left: auto; white-space: nowrap; }

/* ═══ 评级徽章（A 股红涨绿跌规范）═══ */
.badge { display: inline-block; padding: 2px 9px; border-radius: 4px;
    font-size: 0.78em; font-weight: 700; letter-spacing: 0.05em; }
.badge-buy  { background: rgba(239,68,68,0.12); color: #F87171; border: 1px solid rgba(239,68,68,0.5); }
.badge-sell { background: rgba(34,197,94,0.12);  color: #4ADE80; border: 1px solid rgba(34,197,94,0.5); }
.badge-watch{ background: rgba(234,179,8,0.12);  color: #FACC15; border: 1px solid rgba(234,179,8,0.5); }

/* ═══ 数据维度网格 ═══ */
.data-grid {
    display: grid; grid-template-columns: repeat(9, 1fr);
    background: #080f1c; border: 1px solid #1e3358;
    border-radius: 8px; overflow: hidden; margin: 8px 0 12px 0;
}
.data-cell { padding: 8px 4px; text-align: center; border-right: 1px solid #1a2640; }
.data-cell:last-child { border-right: none; }
.dim-label { font-size: 0.7em; color: #475569; margin-bottom: 3px; white-space: nowrap; }
.dim-val   { font-size: 0.85em; color: #A0AEC0; font-family: 'SF Mono', 'Consolas', monospace; }
.dim-val-hi{ font-size: 0.88em; color: #CBD5E1; font-family: 'SF Mono', 'Consolas', monospace; font-weight: 600; }

/* ═══ 判断维度区 ═══ */
.judge-row { display: flex; gap: 16px; flex-wrap: wrap; align-items: flex-start; margin: 6px 0 10px 0; }
.judge-cell { min-width: 90px; }
.judge-cell .dim-label { margin-bottom: 5px; }

/* ═══ 操作维度：价格卡片 ═══ */
.action-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; margin: 8px 0 10px 0; }
.action-card { border-radius: 8px; padding: 10px 8px; text-align: center; background: #080f1c; }
.ac-buy    { border: 1px solid rgba(239,68,68,0.6); }
.ac-alt    { border: 1px solid #1f2937; }
.ac-stop   { border: 1px solid rgba(34,197,94,0.5); }
.ac-target { border: 1px solid rgba(245,158,11,0.5); }
.ac-label  { font-size: 0.7em; color: #64748B; margin-bottom: 4px; }
.ac-val-buy    { font-size: 1.08em; font-weight: 700; color: #FCA5A5; font-family: monospace; }
.ac-val-alt    { font-size: 0.95em; font-weight: 600; color: #94A3B8; font-family: monospace; }
.ac-val-stop   { font-size: 1.08em; font-weight: 700; color: #86EFAC; font-family: monospace; }
.ac-val-target { font-size: 1.08em; font-weight: 700; color: #FCD34D; font-family: monospace; }
.decision-box {
    background: #0a1e38; border-left: 3px solid #3B82F6;
    border-radius: 0 8px 8px 0; padding: 10px 16px; margin-top: 8px;
    color: #CBD5E1; font-size: 0.9em; line-height: 1.5;
}

/* ═══ 原始报告容器 ═══ */
.raw-report-wrap {
    background: #080f1c; border: 1px solid #1e3358;
    border-radius: 10px; padding: 20px 24px; margin-top: 8px;
}

/* ═══ 批次导航 radio 极简样式 ═══ */
div[data-testid="stRadio"] > label:first-child { display: none; }
div[data-testid="stRadio"] [data-baseweb="radio"] { padding: 4px 0 !important; }
div[data-testid="stRadio"] [data-baseweb="radio"] label {
    font-size: 0.84em !important; color: #64748B !important;
    font-family: 'SF Mono', 'Consolas', monospace !important; letter-spacing: 0.01em;
}
div[data-testid="stRadio"] [data-baseweb="radio"] label:hover { color: #94A3B8 !important; }
div[data-testid="stRadio"] [aria-checked="true"] ~ label,
div[data-testid="stRadio"] [data-baseweb="radio"]:has(input:checked) label { color: #93C5FD !important; }

/* ═══ Metric 组件 ═══ */
[data-testid="metric-container"] { background: #080f1c; border-radius: 8px; padding: 8px 10px; }
[data-testid="metric-container"] label { font-size: 0.75em !important; color: #64748B !important; }
[data-testid="metric-container"] [data-testid="stMetricValue"] { font-size: 1.05em !important; }

/* ═══ 机要区日志 ═══ */
textarea[disabled] { color: #4B5563 !important; font-size: 0.8em !important; }

/* ═══ 分隔线弱化 ═══ */
hr { border-color: #1f2937 !important; margin: 18px 0 !important; }

/* ═══ 下载按钮 ═══ */
.stDownloadButton > button {
    background: rgba(30,42,71,0.8) !important;
    color: #93C5FD !important;
    border: 1px solid rgba(59,130,246,0.4) !important;
    border-radius: 8px !important;
    font-weight: 600 !important;
    font-size: 0.82em !important;
}
.stDownloadButton > button:hover {
    background: rgba(59,130,246,0.2) !important;
    border-color: #3B82F6 !important;
}
</style>
""",
    unsafe_allow_html=True,
)


# ─────────────────────────────────────────────────────────────────────────────
# 一、格式化工具函数
# ─────────────────────────────────────────────────────────────────────────────

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


def _artifact_text(value: Any) -> str:
    if value is None:
        return ""
    return value if isinstance(value, str) else str(value)


def _plain_text(value: Any) -> str:
    """转为纯文本（用于下载），保留原始结构。"""
    return markdown_to_plain_text(_normalize_text(value))


def _report_filename(prefix: str, slug: str) -> str:
    return f"{prefix}_{slug}.txt"


def _dl_bytes(text: str) -> bytes:
    """下载用 UTF-8 bytes，坚决杜绝乱码。"""
    return (text or "").encode("utf-8")


# ─────────────────────────────────────────────────────────────────────────────
# 二、A 股色彩规范徽章
# ─────────────────────────────────────────────────────────────────────────────

def _advice_badge(advice: str) -> str:
    text = _normalize_text(advice) or "未评级"
    if any(w in text for w in ("买", "增持", "看涨", "做多")):
        return f'<span class="badge badge-buy">▲ {text}</span>'
    if any(w in text for w in ("卖", "减持", "离场", "做空")):
        return f'<span class="badge badge-sell">▼ {text}</span>'
    return f'<span class="badge badge-watch">◆ {text}</span>'


def _trend_badge(trend: str) -> str:
    text = _normalize_text(trend) or "趋势待定"
    if any(w in text for w in ("多", "上行", "强势", "看多", "涨")):
        return f'<span class="badge badge-buy">▲ {text}</span>'
    if any(w in text for w in ("空", "下行", "弱势", "看空", "跌")):
        return f'<span class="badge badge-sell">▼ {text}</span>'
    return f'<span class="badge badge-watch">◆ {text}</span>'


def _advice_emoji(advice: str) -> str:
    text = _normalize_text(advice)
    if any(w in text for w in ("买", "增持", "看涨")):
        return "🔴"
    if any(w in text for w in ("卖", "减持", "离场")):
        return "🟢"
    return "🟡"


# ─────────────────────────────────────────────────────────────────────────────
# 三、业务工具函数
# ─────────────────────────────────────────────────────────────────────────────

def _resolve_precise_code(keyword: str, snapshots: List[Dict[str, Any]]) -> Optional[str]:
    q = _normalize_text(keyword)
    if not q:
        return None
    exact_code = [s["code"] for s in snapshots if _normalize_text(s.get("code")) == q]
    if exact_code:
        return exact_code[0]
    exact_name = [s["code"] for s in snapshots if _normalize_text(s.get("name")) == q]
    unique = list(dict.fromkeys(exact_name))
    return unique[0] if len(unique) == 1 else None


def _infer_run_time(batch_snaps: List[Dict[str, Any]]) -> str:
    timestamps = [s.get("created_at") for s in batch_snaps if s.get("created_at")]
    return _fmt_dt(max(timestamps) if timestamps else None)


def _infer_run_date_slug(batch_snaps: List[Dict[str, Any]]) -> str:
    timestamps = [s.get("created_at") for s in batch_snaps if s.get("created_at")]
    if not timestamps:
        return datetime.now().strftime("%Y%m%d_%H%M%S")
    try:
        return datetime.fromisoformat(max(timestamps)).strftime("%Y%m%d_%H%M%S")
    except Exception:
        return datetime.now().strftime("%Y%m%d_%H%M%S")


def _extract_risk_alerts(snap: Dict[str, Any], factors: Dict[str, Any]) -> List[str]:
    alerts: List[str] = []
    for key in ("risk_alerts", "alerts", "warnings"):
        value = factors.get(key)
        if isinstance(value, list):
            alerts.extend(_normalize_text(i) for i in value if _normalize_text(i))
    for key in ("time_sensitivity", "ma_alignment", "position_advice"):
        value = _normalize_text(snap.get(key) or factors.get(key))
        if value:
            alerts.append(value)
    seen: set = set()
    deduped: List[str] = []
    for item in alerts:
        if item not in seen:
            seen.add(item)
            deduped.append(item)
    return deduped[:2]


# ─────────────────────────────────────────────────────────────────────────────
# 四、下载内容构建（保持原始 Markdown，下载才转纯文本）
# ─────────────────────────────────────────────────────────────────────────────

def _build_stock_report_md(stock_snaps: List[Dict[str, Any]]) -> str:
    """拼接所有个股 report_md 为完整 Markdown（原文，不做转换）"""
    lines: List[str] = []
    for snap in stock_snaps:
        lines.extend([
            f"## {snap.get('name') or '—'}（{snap.get('code') or '—'}）",
            "",
            snap.get("report_md") or "_无报告内容_",
            "",
        ])
    return "\n".join(lines).strip() + ("\n" if lines else "")


def _build_batch_report_md(run_id: str, batch_snaps: List[Dict[str, Any]]) -> str:
    """拼接整批次完整 Markdown（原文）"""
    lines = [f"# DUKA 历史批次报告", f"run_id: {run_id}", ""]
    market = [s for s in batch_snaps if s.get("code") == "__market__"]
    stocks = [s for s in batch_snaps if s.get("code") != "__market__"]
    if market:
        lines.extend(["## 大盘报告", ""])
        for snap in market:
            lines.extend([snap.get("report_md") or "_无报告内容_", ""])
    if stocks:
        lines.extend(["## 个股报告", ""])
        for snap in stocks:
            lines.extend([
                f"### {snap.get('name') or '—'}（{snap.get('code') or '—'}）",
                snap.get("report_md") or "_无报告内容_",
                "",
            ])
    return "\n".join(lines).strip() + "\n"


def _build_batch_schema_json(run_id: str, batch_snaps: List[Dict[str, Any]]) -> str:
    payload = {
        "run_id": run_id,
        "created_at": max((s.get("created_at") or "" for s in batch_snaps), default=""),
        "snapshots": [
            {
                "id": s.get("id"),
                "run_id": s.get("run_id"),
                "code": s.get("code"),
                "name": s.get("name"),
                "created_at": s.get("created_at"),
                "sentiment_score": s.get("sentiment_score"),
                "operation_advice": s.get("operation_advice"),
                "trend_prediction": s.get("trend_prediction"),
                "ma_alignment": s.get("ma_alignment"),
                "buy_point": s.get("buy_point"),
                "stop_loss": s.get("stop_loss"),
                "target_price": s.get("target_price"),
                "factors": _safe_json_loads(s.get("factors_json")),
            }
            for s in batch_snaps
        ],
    }
    return json.dumps(payload, ensure_ascii=False, indent=2)


def _get_batch_artifacts(run_id: str, batch_snaps: List[Dict[str, Any]]) -> Dict[str, str]:
    """
    返回两套内容：
      *_md  → 原始 Markdown，用于 st.markdown() 展示（视觉对齐原生报告）
      *_txt → 纯文本，用于 download_button（utf-8，无乱码）
    """
    artifacts = get_run_artifacts(run_id) or {}
    stock_snaps = [s for s in batch_snaps if s.get("code") != "__market__"]

    # ── 优先使用 run_artifacts 存储的原始 Markdown ────────────────────────────
    # 大盘
    market_md = _artifact_text(artifacts.get("market_report_md"))
    if not market_md:
        market_md = "\n\n".join(
            _artifact_text(s.get("report_md"))
            for s in batch_snaps
            if s.get("code") == "__market__"
        ).strip()

    # 个股
    stock_md = _artifact_text(artifacts.get("stock_report_md"))
    if not stock_md:
        stock_md = _build_stock_report_md(stock_snaps)

    # 全量
    full_md = _artifact_text(artifacts.get("full_report_md"))
    if not full_md:
        full_md = _build_batch_report_md(run_id, batch_snaps)

    return {
        # 展示用（原始 Markdown）
        "market_md": market_md,
        "stock_md":  stock_md,
        "full_md":   full_md,
        # 下载用（纯文本，保证 UTF-8）
        "market_txt": _plain_text(market_md) if market_md else "",
        "stock_txt":  _plain_text(stock_md)  if stock_md  else "",
        "full_txt":   _plain_text(full_md)   if full_md   else "",
        # 日志
        "business_log": _artifact_text(artifacts.get("business_log")),
        "debug_log":    _artifact_text(artifacts.get("debug_log")),
        "schema_json":  _normalize_text(artifacts.get("schema_json"))
                        or _build_batch_schema_json(run_id, batch_snaps),
    }


# ─────────────────────────────────────────────────────────────────────────────
# 五、个股卡片三维渲染函数
# ─────────────────────────────────────────────────────────────────────────────

def _render_data_block(snap: Dict[str, Any], factors: Dict[str, Any]) -> None:
    items: List[Tuple[str, str, bool]] = [
        ("收盘价",     _fmt_price(snap.get("current_price") or factors.get("current_price")), True),
        ("MA5",       _fmt_price(factors.get("ma5")),                                         False),
        ("MA10",      _fmt_price(factors.get("ma10")),                                        False),
        ("MA20",      _fmt_price(factors.get("ma20")),                                        False),
        ("乖离率MA5", _fmt_pct(snap.get("bias_rate") or factors.get("bias_rate")),            False),
        ("量比",      _fmt_pct(snap.get("volume_ratio") or factors.get("volume_ratio")),      False),
        ("换手率",    _fmt_pct(snap.get("turnover_rate") or factors.get("turnover_rate")),    False),
        ("支撑位",    _fmt_price(factors.get("support") or factors.get("support_level")),     True),
        ("压力位",    _fmt_price(factors.get("resistance") or factors.get("resistance_level")), True),
    ]
    cells = "".join(
        f'<div class="data-cell">'
        f'<div class="dim-label">{lbl}</div>'
        f'<div class="{"dim-val-hi" if hi else "dim-val"}">{val}</div>'
        f'</div>'
        for lbl, val, hi in items
    )
    st.markdown(f'<div class="data-grid">{cells}</div>', unsafe_allow_html=True)


def _render_judge_block(snap: Dict[str, Any], factors: Dict[str, Any]) -> None:
    advice_h = _advice_badge(_normalize_text(snap.get("operation_advice")))
    trend_h  = _trend_badge(_normalize_text(snap.get("trend_prediction")))
    ma_align = _normalize_text(snap.get("ma_alignment")) or "—"
    bullish  = _normalize_text(factors.get("bullish_alignment")) or "—"
    t_score  = _fmt_score(factors.get("trend_strength_score"))
    s_score  = _fmt_score(snap.get("sentiment_score"))
    news     = _normalize_text(factors.get("news_sentiment")) or "—"
    st.markdown(
        f"""
<div class="judge-row">
  <div class="judge-cell">
    <div class="dim-label">操作评级</div>{advice_h}
  </div>
  <div class="judge-cell">
    <div class="dim-label">趋势判断</div>{trend_h}
  </div>
  <div class="judge-cell">
    <div class="dim-label">均线排列</div>
    <div class="dim-val">{ma_align}</div>
  </div>
  <div class="judge-cell">
    <div class="dim-label">多头排列</div>
    <div class="dim-val">{bullish}</div>
  </div>
  <div class="judge-cell">
    <div class="dim-label">趋势强度</div>
    <div class="dim-val-hi">{t_score}</div>
  </div>
  <div class="judge-cell">
    <div class="dim-label">情绪评分</div>
    <div class="dim-val-hi">{s_score}</div>
  </div>
  <div class="judge-cell" style="min-width:140px;">
    <div class="dim-label">舆情情绪</div>
    <div class="dim-val">{news}</div>
  </div>
</div>
""",
        unsafe_allow_html=True,
    )
    for alert in _extract_risk_alerts(snap, factors):
        st.warning(alert, icon="⚠️")


def _render_action_block(snap: Dict[str, Any], factors: Dict[str, Any]) -> None:
    buy_pt  = _fmt_price(snap.get("buy_point") or factors.get("buy_point"))
    sec_buy = _fmt_price(factors.get("secondary_buy_point") or factors.get("backup_buy_point"))
    stop    = _fmt_price(snap.get("stop_loss") or factors.get("stop_loss"))
    target  = _fmt_price(snap.get("target_price") or factors.get("target_price"))
    pos     = _normalize_text(snap.get("position_advice") or factors.get("position_advice")) or "—"
    decision = (
        _normalize_text(factors.get("one_line_decision"))
        or _normalize_text(snap.get("position_advice"))
        or _normalize_text(snap.get("operation_advice"))
        or "暂无一句话决策"
    )
    st.markdown(
        f"""
<div class="action-grid">
  <div class="action-card ac-buy">
    <div class="ac-label">🎯 理想买入点</div>
    <div class="ac-val-buy">{buy_pt}</div>
  </div>
  <div class="action-card ac-alt">
    <div class="ac-label">🔵 次优买入点</div>
    <div class="ac-val-alt">{sec_buy}</div>
  </div>
  <div class="action-card ac-stop">
    <div class="ac-label">🛑 止损位</div>
    <div class="ac-val-stop">{stop}</div>
  </div>
  <div class="action-card ac-target">
    <div class="ac-label">🎊 目标位</div>
    <div class="ac-val-target">{target}</div>
  </div>
</div>
<div style="margin:6px 0 2px 0;">
  <span class="dim-label">仓位建议：</span>
  <span class="dim-val">{pos}</span>
</div>
<div class="decision-box">💡 {decision}</div>
""",
        unsafe_allow_html=True,
    )


def _render_quick_decision_preset(snap: Dict[str, Any], factors: Dict[str, Any]) -> None:
    advice_h = _advice_badge(_normalize_text(snap.get("operation_advice")))
    trend_h  = _trend_badge(_normalize_text(snap.get("trend_prediction")))
    buy_pt   = _fmt_price(snap.get("buy_point") or factors.get("buy_point"))
    stop     = _fmt_price(snap.get("stop_loss") or factors.get("stop_loss"))
    target   = _fmt_price(snap.get("target_price") or factors.get("target_price"))
    decision = (
        _normalize_text(factors.get("one_line_decision"))
        or _normalize_text(snap.get("position_advice"))
        or "暂无一句话决策"
    )
    st.markdown(
        f"""
<div style="display:flex; gap:10px; margin:6px 0 10px 0; flex-wrap:wrap; align-items:center;">
  {advice_h}&nbsp;&nbsp;{trend_h}
</div>
<div class="action-grid" style="grid-template-columns: repeat(3, 1fr);">
  <div class="action-card ac-buy">
    <div class="ac-label">🎯 买入点</div>
    <div class="ac-val-buy">{buy_pt}</div>
  </div>
  <div class="action-card ac-stop">
    <div class="ac-label">🛑 止损位</div>
    <div class="ac-val-stop">{stop}</div>
  </div>
  <div class="action-card ac-target">
    <div class="ac-label">🎊 目标位</div>
    <div class="ac-val-target">{target}</div>
  </div>
</div>
<div class="decision-box">💡 {decision}</div>
""",
        unsafe_allow_html=True,
    )


def _render_risk_review_preset(snap: Dict[str, Any], factors: Dict[str, Any]) -> None:
    _render_judge_block(snap, factors)
    stop = _fmt_price(snap.get("stop_loss") or factors.get("stop_loss"))
    pos  = _normalize_text(snap.get("position_advice") or factors.get("position_advice")) or "—"
    st.markdown(
        f"""
<div style="display:grid; grid-template-columns: 1fr 1fr; gap:10px; margin-top:8px;">
  <div class="action-card ac-stop">
    <div class="ac-label">🛑 止损位</div>
    <div class="ac-val-stop">{stop}</div>
  </div>
  <div class="action-card ac-alt">
    <div class="ac-label">💰 仓位建议</div>
    <div class="ac-val-alt">{pos}</div>
  </div>
</div>
""",
        unsafe_allow_html=True,
    )


def _render_data_review_preset(snap: Dict[str, Any], factors: Dict[str, Any]) -> None:
    _render_data_block(snap, factors)
    ma_align = _normalize_text(snap.get("ma_alignment")) or "—"
    t_score  = _fmt_score(factors.get("trend_strength_score"))
    st.markdown(
        f'<span class="dim-label">均线排列：</span>'
        f'<span class="dim-val">{ma_align}</span>'
        f'&emsp;<span class="dim-label">趋势强度分：</span>'
        f'<span class="dim-val-hi">{t_score}</span>',
        unsafe_allow_html=True,
    )


def _render_stock_raw_report(snap: Dict[str, Any], prefix: str) -> None:
    """渲染个股原始 Markdown 报告 —— 完全对齐《快速分析》原生格式。"""
    report_md = _normalize_text(snap.get("report_md"))
    if not report_md:
        st.caption("暂无原始报告内容。")
        return
    st.markdown(report_md)


def _render_card_actions(snap: Dict[str, Any], prefix: str) -> None:
    action_cols = st.columns([1, 1, 2, 2])
    with action_cols[0]:
        if snap.get("code") != "__market__":
            if st.button("📌 跟踪", key=f"track_{prefix}_{snap['id']}", use_container_width=True):
                add_to_quick_pool(snap["code"], snap["name"])
                st.toast(f"✅ {snap['name']} 已加入跟踪池")
    with action_cols[1]:
        if st.button("👁️ 隐藏", key=f"hide_{prefix}_{snap['id']}", use_container_width=True):
            st.session_state["hide_pending_snap_id"] = snap["id"]
            st.rerun()
    with action_cols[2]:
        report_txt = _plain_text(snap.get("report_md"))
        code_str   = snap.get("code", "unknown")
        snap_slug  = (snap.get("created_at") or "")[:16].replace(":", "").replace(" ", "_")
        if report_txt:
            st.download_button(
                "⬇️ 个股报告 .txt",
                data=_dl_bytes(report_txt),
                file_name=f"stock_{code_str}_{snap_slug}.txt",
                mime="text/plain; charset=utf-8",
                use_container_width=True,
                key=f"dl_snap_{prefix}_{snap['id']}",
            )
    with action_cols[3]:
        st.caption("")  # 占位

    if st.session_state.get("hide_pending_snap_id") == snap["id"]:
        st.warning("确认后将从列表隐藏，数据库记录不会物理删除。")
        c1, c2, _ = st.columns([1, 1, 4])
        with c1:
            if st.button("确认隐藏", key=f"confirm_hide_{prefix}_{snap['id']}", use_container_width=True):
                delete_snapshot(snap["id"])
                st.session_state["hide_pending_snap_id"] = None
                st.rerun()
        with c2:
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
    factors  = _safe_json_loads(snap.get("factors_json"))
    name     = snap.get("name") or "—"
    code     = snap.get("code") or "—"
    dt_str   = _fmt_dt(snap.get("created_at"))
    score    = _fmt_score(snap.get("sentiment_score"))
    advice_h = _advice_badge(_normalize_text(snap.get("operation_advice")))
    trend_h  = _trend_badge(_normalize_text(snap.get("trend_prediction")))

    # 卡片头部
    st.markdown(
        f"""
<div class="scard-header">
  <span class="scard-idx">{index:02d}</span>
  <span class="scard-name">{name}</span>
  <span class="scard-code">（{code}）</span>
  {advice_h}&nbsp;{trend_h}
  <span class="scard-meta">{dt_str} &nbsp;·&nbsp; 评分 {score}</span>
</div>
""",
        unsafe_allow_html=True,
    )

    # 内容区：结构化维度 + 原始报告 双 Tab
    tab_struct, tab_raw = st.tabs(["📊 结构化分析", "📄 原始报告全文"])

    with tab_struct:
        if mode == "全量模式":
            st.markdown('<p class="dim-label" style="margin:6px 0 2px 0;">📊 数据维度</p>', unsafe_allow_html=True)
            _render_data_block(snap, factors)
            st.markdown('<p class="dim-label" style="margin:8px 0 2px 0;">🎯 判断维度</p>', unsafe_allow_html=True)
            _render_judge_block(snap, factors)
            st.markdown('<p class="dim-label" style="margin:8px 0 2px 0;">📋 操作维度</p>', unsafe_allow_html=True)
            _render_action_block(snap, factors)
        elif mode == "进阶模式":
            if preset == "快速决策组合":
                _render_quick_decision_preset(snap, factors)
            elif preset == "风险审查组合":
                _render_risk_review_preset(snap, factors)
            else:
                _render_data_review_preset(snap, factors)
        else:  # 折叠模式
            with st.expander("📊 数据透视", expanded=False):
                _render_data_block(snap, factors)
            with st.expander("🎯 判断结论", expanded=True):
                _render_judge_block(snap, factors)
            with st.expander("📋 操作计划", expanded=True):
                _render_action_block(snap, factors)

    with tab_raw:
        # 100% 对齐《快速分析》原生排版 —— 直接渲染原始 Markdown
        _render_stock_raw_report(snap, prefix)

    _render_card_actions(snap, prefix)
    st.divider()


# ─────────────────────────────────────────────────────────────────────────────
# 六、过滤工具
# ─────────────────────────────────────────────────────────────────────────────

def _snapshot_matches_filters(
    snap: Dict[str, Any],
    advice_filter: Optional[str],
    trend_filter: Optional[str],
    keyword: str,
) -> bool:
    if snap.get("code") == "__market__":
        return False
    advice_text = _normalize_text(snap.get("operation_advice"))
    trend_text  = _normalize_text(snap.get("trend_prediction"))
    code_text   = _normalize_text(snap.get("code")).lower()
    name_text   = _normalize_text(snap.get("name")).lower()
    kw          = _normalize_text(keyword).lower()
    if advice_filter and advice_filter not in advice_text:
        return False
    if trend_filter and trend_filter not in trend_text:
        return False
    if kw and kw not in code_text and kw not in name_text:
        return False
    return True


# ─────────────────────────────────────────────────────────────────────────────
# 七、时光轴模式
# ─────────────────────────────────────────────────────────────────────────────

def _render_timeline_mode(precise_code: str) -> Optional[str]:
    history    = get_code_history(precise_code)
    stock_name = history[0].get("name") if history else precise_code

    st.markdown(
        f"<p style='color:#93C5FD; font-size:1.0em; font-weight:600; margin-bottom:2px;'>"
        f"⏱️ {stock_name}（{precise_code}）时光轴复盘</p>"
        f"<p class='dim-label'>按版本切换与时序卡片回看 · 保留个股原始报告</p>",
        unsafe_allow_html=True,
    )
    if not history:
        st.info("暂无该股票历史记录。")
        return None

    version_options = {
        (
            f"{_fmt_dt(s.get('created_at'))}  │  "
            f"评分 {_fmt_score(s.get('sentiment_score'))}  │  "
            f"{_normalize_text(s.get('operation_advice')) or '—'}"
        ): s
        for s in history
    }
    selected_label = st.selectbox("选择历史版本", list(version_options.keys()), index=0)
    selected_snap  = version_options[selected_label]

    with st.expander("📄 当前版本原始报告全文", expanded=True):
        report_md = _normalize_text(selected_snap.get("report_md"))
        if report_md:
            st.markdown(report_md)
        else:
            st.caption("暂无原始报告内容")

    st.divider()
    for i, snap in enumerate(history, start=1):
        _render_stock_card_v2(
            snap=snap,
            mode="折叠模式",
            preset=st.session_state["report_preset"],
            index=i,
            prefix=f"timeline_{precise_code}",
        )
    return selected_snap.get("run_id")


# ─────────────────────────────────────────────────────────────────────────────
# 八、批次导航条（st.radio 极简选择器）
# ─────────────────────────────────────────────────────────────────────────────

def _render_batch_navigation(run_rows: List[Dict[str, Any]]) -> None:
    st.markdown(
        "<p style='color:#475569; font-size:0.75em; text-transform:uppercase; "
        "letter-spacing:0.1em; margin-bottom:6px;'>分析批次</p>",
        unsafe_allow_html=True,
    )
    if not run_rows:
        st.caption("暂无可浏览的分析批次。")
        return
    batch_info: List[Tuple[str, str]] = []
    for idx, row in enumerate(run_rows, start=1):
        run_id = row.get("run_id")
        if not run_id:
            continue
        batch_snaps = get_run_snapshots(run_id)
        stock_names = [s.get("name") for s in batch_snaps if s.get("code") != "__market__" and s.get("name")]
        count   = len(stock_names)
        preview = "、".join(stock_names[:3]) + ("..." if count > 3 else "")
        dt_str  = _infer_run_time(batch_snaps)
        label   = f"[{idx:02d}]  {dt_str}  │  {count}只  │  {preview or '暂无标的'}"
        batch_info.append((run_id, label))
    if not batch_info:
        st.caption("暂无有效批次。")
        return
    run_ids = [ri for ri, _ in batch_info]
    labels  = [lbl for _, lbl in batch_info]
    if st.session_state.get("selected_run_id") not in set(run_ids):
        st.session_state["selected_run_id"] = run_ids[0]
    cur_idx = next((i for i, ri in enumerate(run_ids) if ri == st.session_state["selected_run_id"]), 0)
    selected_label = st.radio(
        label="batch_nav",
        options=labels,
        index=cur_idx,
        key="batch_nav_radio",
        label_visibility="collapsed",
    )
    new_run_id = run_ids[labels.index(selected_label)]
    if new_run_id != st.session_state.get("selected_run_id"):
        st.session_state["selected_run_id"] = new_run_id
        st.rerun()


def _build_run_entries(run_rows: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    entries: List[Dict[str, str]] = []
    for idx, row in enumerate(run_rows, start=1):
        run_id = row.get("run_id")
        if not run_id:
            continue
        batch_snaps = get_run_snapshots(run_id)
        stock_names = [s.get("name") for s in batch_snaps if s.get("code") != "__market__" and s.get("name")]
        count   = len(stock_names)
        preview = "、".join(stock_names[:3]) + ("..." if count > 3 else "")
        dt_str  = _infer_run_time(batch_snaps)
        entries.append({
            "index":      f"{idx:02d}",
            "run_id":     run_id,
            "label":      f"{dt_str}  │  {count}只  │  {preview or '暂无标的'}",
            "caption":    f"[{idx:02d}] {dt_str}",
            "count_text": f"{count}只",
            "preview":    preview or "暂无标的",
        })
    return entries


def _ensure_selected_run(entries: List[Dict[str, str]]) -> None:
    run_ids = {entry["run_id"] for entry in entries}
    current = st.session_state.get("selected_run_id")
    if current not in run_ids:
        st.session_state["selected_run_id"] = entries[0]["run_id"] if entries else None


def _select_run(run_id: str) -> None:
    if run_id != st.session_state.get("selected_run_id"):
        st.session_state["selected_run_id"] = run_id
        st.session_state["confirm_delete_run_id"] = None
        st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# 九、侧边栏快速入口（最近 3 条精简按钮）
# ─────────────────────────────────────────────────────────────────────────────

def _render_recent_run_hub(entries: List[Dict[str, str]]) -> None:
    st.markdown(
        "<p style='color:#475569; font-size:0.75em; text-transform:uppercase; "
        "letter-spacing:0.1em; margin-bottom:6px;'>最近 3 条批次</p>",
        unsafe_allow_html=True,
    )
    if not entries:
        st.caption("暂无可浏览批次。")
        return
    for entry in entries[:3]:
        is_active    = entry["run_id"] == st.session_state.get("selected_run_id")
        button_label = f"{'▶ ' if is_active else ''}{entry['caption']} · {entry['count_text']}"
        if st.button(
            button_label,
            key=f"recent_run_{entry['run_id']}",
            use_container_width=True,
            type="primary" if is_active else "secondary",
        ):
            _select_run(entry["run_id"])
        st.caption(entry["preview"])


# ─────────────────────────────────────────────────────────────────────────────
# 十、完整历史库（收纳在 Expander 内）
# ─────────────────────────────────────────────────────────────────────────────

def _render_full_history_library(entries: List[Dict[str, str]]) -> None:
    with st.expander("📂 进入完整历史库", expanded=False):
        if not entries:
            st.caption("暂无历史批次。")
            return
        st.caption("全量批次收纳于此，可切换查看并执行物理删除。")
        for entry in entries:
            run_id    = entry["run_id"]
            is_active = run_id == st.session_state.get("selected_run_id")
            meta_cols = st.columns([6, 1.2, 1.6])
            with meta_cols[0]:
                st.markdown(
                    f"**[{entry['index']}] {entry['caption']}**  \n"
                    f"`{entry['count_text']}` · {entry['preview']}",
                )
            with meta_cols[1]:
                if st.button(
                    "查看",
                    key=f"view_run_{run_id}",
                    use_container_width=True,
                    type="primary" if is_active else "secondary",
                ):
                    _select_run(run_id)
            with meta_cols[2]:
                if st.button("🗑️ 彻底删除", key=f"delete_run_{run_id}", use_container_width=True):
                    st.session_state["confirm_delete_run_id"] = run_id
                    st.rerun()

            if st.session_state.get("confirm_delete_run_id") == run_id:
                st.warning("将物理删除该批次的快照与 artifacts，且不可恢复。")
                confirm_cols = st.columns([1.5, 1.2, 5])
                with confirm_cols[0]:
                    if st.button(
                        "确认删除",
                        key=f"confirm_delete_run_{run_id}",
                        type="primary",
                        use_container_width=True,
                    ):
                        delete_run_permanently(run_id)
                        st.session_state["confirm_delete_run_id"] = None
                        if st.session_state.get("selected_run_id") == run_id:
                            st.session_state["selected_run_id"] = None
                        st.rerun()
                with confirm_cols[1]:
                    if st.button("取消", key=f"cancel_delete_run_{run_id}", use_container_width=True):
                        st.session_state["confirm_delete_run_id"] = None
                        st.rerun()
            st.divider()


# ─────────────────────────────────────────────────────────────────────────────
# 十一、批次报告阅读区（三 Tab + 三模式 + 下载全家桶）
# ─────────────────────────────────────────────────────────────────────────────

def _render_batch_mode(
    run_rows: List[Dict[str, Any]],
    advice_value: Optional[str],
    trend_value: Optional[str],
    keyword: str,
) -> Optional[Dict[str, str]]:
    selected_run_id = st.session_state.get("selected_run_id")
    if not selected_run_id:
        st.info("👈 请从左侧或上方选择一个分析批次开始浏览")
        return None

    batch_snaps = get_run_snapshots(selected_run_id)
    if not batch_snaps:
        st.warning("当前批次暂无可展示内容。")
        return None

    arts         = _get_batch_artifacts(selected_run_id, batch_snaps)
    run_date_slug = _infer_run_date_slug(batch_snaps)

    # ── 批次汇总简表 ──────────────────────────────────────────────────────────
    summary_rows = []
    for i, snap in enumerate([s for s in batch_snaps if s.get("code") != "__market__"], start=1):
        summary_rows.append({
            "#":   str(i),
            "名称": _normalize_text(snap.get("name")) or "—",
            "代码": _normalize_text(snap.get("code")) or "—",
            "评级": f"{_advice_emoji(snap.get('operation_advice',''))} "
                    f"{_normalize_text(snap.get('operation_advice')) or '—'}",
            "趋势": _normalize_text(snap.get("trend_prediction")) or "—",
            "评分": _fmt_score(snap.get("sentiment_score")),
        })
    if summary_rows:
        st.markdown(
            "<p class='dim-label' style='margin-bottom:4px;'>本批次个股汇总</p>",
            unsafe_allow_html=True,
        )
        st.dataframe(summary_rows, use_container_width=True, hide_index=True)

    # ── 三 Tab 报告区 ──────────────────────────────────────────────────────────
    tab_market, tab_full, tab_stock = st.tabs(["📊 大盘报告", "📄 全量报告", "🔬 个股分析"])

    # Tab 1：大盘报告 —— 原生 Markdown 渲染，100% 对齐《快速分析》排版
    with tab_market:
        raw_market = arts.get("market_md", "")
        if raw_market:
            # 使用样式容器包裹，视觉对齐内参散文风格
            st.markdown(
                '<div class="market-prose-wrap">',
                unsafe_allow_html=True,
            )
            st.markdown(raw_market)
            st.markdown("</div>", unsafe_allow_html=True)

            # 下载按钮（纯文本 UTF-8）
            dl_market = arts.get("market_txt", "") or _plain_text(raw_market)
            if dl_market:
                st.download_button(
                    "⬇️ 下载大盘报告 .txt",
                    data=_dl_bytes(dl_market),
                    file_name=_report_filename("market_review", run_date_slug),
                    mime="text/plain; charset=utf-8",
                    use_container_width=False,
                    key=f"dl_market_{selected_run_id}",
                )
        else:
            st.info("当前批次暂无大盘报告。")

    # Tab 2：全量报告 —— 原生 Markdown 渲染
    with tab_full:
        raw_full = arts.get("full_md", "")
        if raw_full:
            st.markdown(raw_full)
            dl_full = arts.get("full_txt", "") or _plain_text(raw_full)
            if dl_full:
                st.download_button(
                    "⬇️ 下载全量报告 .txt",
                    data=_dl_bytes(dl_full),
                    file_name=_report_filename("full_report", run_date_slug),
                    mime="text/plain; charset=utf-8",
                    use_container_width=False,
                    key=f"dl_full_{selected_run_id}",
                )
        else:
            st.info("当前批次暂无全量报告。")

    # Tab 3：个股分析（驾驶舱仪表盘 + 原始报告双 Tab）
    filtered_stock_snaps = [
        s for s in batch_snaps
        if _snapshot_matches_filters(s, advice_value, trend_value, keyword)
    ]

    with tab_stock:
        # 浏览模式切换
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

        st.divider()

        if not filtered_stock_snaps:
            st.info("当前过滤条件下，没有命中的个股。")
        else:
            for i, snap in enumerate(filtered_stock_snaps, start=1):
                _render_stock_card_v2(
                    snap=snap,
                    mode=mode,
                    preset=preset,
                    index=i,
                    prefix=f"batch_{selected_run_id}",
                )

    # ── 下载全家桶（Tab 区下方，四列整齐）────────────────────────────────────
    st.markdown(
        "<p class='dim-label' style='margin:20px 0 8px 0; text-transform:uppercase; "
        "letter-spacing:0.08em;'>⬇ 下载全家桶</p>",
        unsafe_allow_html=True,
    )
    dl1, dl2, dl3, dl4 = st.columns(4)

    with dl1:
        dl_market = arts.get("market_txt", "")
        if dl_market:
            st.download_button(
                "📊 大盘报告",
                data=_dl_bytes(dl_market),
                file_name=_report_filename("market_review", run_date_slug),
                mime="text/plain; charset=utf-8",
                use_container_width=True,
                key=f"dl2_market_{selected_run_id}",
            )
        else:
            st.caption("暂无大盘报告")

    with dl2:
        dl_stock = arts.get("stock_txt", "")
        if dl_stock:
            st.download_button(
                "🔬 个股报告",
                data=_dl_bytes(dl_stock),
                file_name=_report_filename("stock_report", run_date_slug),
                mime="text/plain; charset=utf-8",
                use_container_width=True,
                key=f"dl2_stock_{selected_run_id}",
            )
        else:
            st.caption("暂无个股报告")

    with dl3:
        dl_full = arts.get("full_txt", "")
        if dl_full:
            st.download_button(
                "📄 全量报告",
                data=_dl_bytes(dl_full),
                file_name=_report_filename("full_report", run_date_slug),
                mime="text/plain; charset=utf-8",
                use_container_width=True,
                key=f"dl2_full_{selected_run_id}",
            )
        else:
            st.caption("暂无全量报告")

    with dl4:
        biz_log = arts.get("business_log", "")
        dbg_log = arts.get("debug_log", "")
        combined_logs = (
            f"===== Business Log =====\n{biz_log}\n\n===== Debug Log =====\n{dbg_log}"
            if biz_log or dbg_log else ""
        )
        if combined_logs:
            st.download_button(
                "🔒 日志打包",
                data=_dl_bytes(combined_logs),
                file_name=_report_filename("logs_bundle", run_date_slug),
                mime="text/plain; charset=utf-8",
                use_container_width=True,
                key=f"dl2_logs_{selected_run_id}",
            )
        else:
            st.caption("暂无日志")

    return {**arts, "run_id": selected_run_id, "run_date_slug": run_date_slug}


# ─────────────────────────────────────────────────────────────────────────────
# 十二、侧边栏（快速入口 + 过滤条件 + 危险区）
# ─────────────────────────────────────────────────────────────────────────────

run_rows    = list_recent_runs(limit=None)
run_entries = _build_run_entries(run_rows)
_ensure_selected_run(run_entries)

with st.sidebar:
    st.divider()
    _render_recent_run_hub(run_entries)
    st.divider()
    st.markdown(
        "<p style='color:#475569; font-size:0.75em; text-transform:uppercase; "
        "letter-spacing:0.1em; margin-bottom:4px;'>过滤条件</p>",
        unsafe_allow_html=True,
    )
    advice_filter = st.selectbox("评级过滤", ["全部", "买入", "观望", "卖出"], index=0)
    trend_filter  = st.selectbox("趋势过滤", ["全部", "看多", "震荡", "看空"], index=0)
    keyword       = st.text_input("股票搜索", placeholder="代码 / 名称精确搜索")
    limit         = st.slider("最近记录条数", min_value=20, max_value=200, value=60, step=10)

    with st.expander("⚠️ 危险区", expanded=False):
        if st.button("🗑️ 清空所有历史与跟踪数据", use_container_width=True):
            st.session_state["confirm_clear_all_data"] = True
        if st.session_state.get("confirm_clear_all_data"):
            st.warning("此操作不可恢复，将清空历史快照与跟踪池数据。")
            if st.button("🚨 确认执行清空", use_container_width=True, type="primary"):
                clear_all_data()
                st.session_state["confirm_clear_all_data"] = False
                st.session_state["confirm_delete_run_id"] = None
                st.session_state["selected_run_id"] = None
                st.success("数据已全部清空")
                st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# 十三、主页面入口
# ─────────────────────────────────────────────────────────────────────────────

st.markdown(
    "<h2 style='margin-bottom:2px; color:#F1F5F9; font-weight:700;'>🕰️ 历史记忆库</h2>"
    "<p class='dim-label' style='margin-bottom:16px;'>"
    "按分析批次分组回看 · 支持精确锁定个股时光轴 · 报告完全对齐原生格式</p>",
    unsafe_allow_html=True,
)

all_snapshots = get_snapshots_with_filters(limit=max(limit * 3, 120))
precise_code  = _resolve_precise_code(keyword, all_snapshots)
advice_value  = None if advice_filter == "全部" else advice_filter
trend_value   = None if trend_filter == "全部" else trend_filter

# ── ZONE A：最近 3 条精简 Metric 卡片（横排，优雅）────────────────────────────
if run_entries:
    top3 = run_entries[:3]
    cols = st.columns(len(top3))
    for col, entry in zip(cols, top3):
        is_active  = entry["run_id"] == st.session_state.get("selected_run_id")
        card_class = "run-metric-card active" if is_active else "run-metric-card"
        badge_cls  = "rmc-badge active" if is_active else "rmc-badge"
        col.markdown(
            f"""
<div class="{card_class}">
  <div class="rmc-index">批次 {entry['index']}</div>
  <div class="rmc-time">{entry['caption']}</div>
  <div>
    <span class="{badge_cls}">{entry['count_text']}</span>
  </div>
  <div class="rmc-preview">{entry['preview']}</div>
</div>
""",
            unsafe_allow_html=True,
        )
        col.button(
            "▶ 查看此批次" if is_active else "查看",
            key=f"top_run_{entry['run_id']}",
            use_container_width=True,
            type="primary" if is_active else "secondary",
            on_click=_select_run,
            args=(entry["run_id"],),
        )
else:
    st.caption("暂无可浏览的分析批次。")

# ── ZONE B：完整历史库（收纳在 Expander 中）──────────────────────────────────
_render_full_history_library(run_entries)
st.divider()

# ── 机要区占位（保证最底部日志区正常渲染）────────────────────────────────────
_secret: Dict[str, str] = {
    "business_log": "",
    "debug_log": "",
    "run_id": "",
    "run_date_slug": datetime.now().strftime("%Y%m%d_%H%M%S"),
}

# ── ZONE C：报告详情区（时光轴模式 or 批次模式）──────────────────────────────
if precise_code:
    timeline_run_id = _render_timeline_mode(precise_code)
    if timeline_run_id:
        timeline_snaps = get_run_snapshots(timeline_run_id)
        tl_arts = _get_batch_artifacts(timeline_run_id, timeline_snaps)
        _secret.update(tl_arts)
        _secret["run_id"]        = timeline_run_id
        _secret["run_date_slug"] = _infer_run_date_slug(timeline_snaps)
else:
    batch_result = _render_batch_mode(run_rows, advice_value, trend_value, keyword)
    if batch_result:
        _secret.update(batch_result)


# ─────────────────────────────────────────────────────────────────────────────
# ZONE E：机要区（页面最底部，永远渲染，两片式极简布局）
# ─────────────────────────────────────────────────────────────────────────────

st.divider()
st.markdown(
    "<p class='dim-label' style='text-transform:uppercase; letter-spacing:0.1em;'>"
    "🔒 机要区 &nbsp;·&nbsp; 底层运行日志</p>",
    unsafe_allow_html=True,
)

_biz_log: str = _artifact_text(_secret.get("business_log"))
_dbg_log: str = _artifact_text(_secret.get("debug_log"))
_slug:    str = _secret.get("run_date_slug") or datetime.now().strftime("%Y%m%d_%H%M%S")
_biz_log_display = _biz_log if _biz_log else "暂无运行日志"
_dbg_log_display = _dbg_log if _dbg_log else "暂无通信日志"

sec_col1, sec_col2 = st.columns(2)

with sec_col1:
    with st.expander("📋 运行日志（Business Log）", expanded=False):
        st.text_area(
            label="",
            value=_biz_log_display,
            height=300,
            disabled=True,
            key="history_business_log",
        )
        st.download_button(
            label="⬇️ 下载运行日志 .txt",
            data=_dl_bytes(_biz_log),
            file_name=_report_filename("business_log", _slug),
            mime="text/plain; charset=utf-8",
            use_container_width=True,
            disabled=not _biz_log,
        )

with sec_col2:
    with st.expander("🔧 通信日志（Debug Log）", expanded=False):
        st.text_area(
            label="",
            value=_dbg_log_display,
            height=300,
            disabled=True,
            key="history_debug_log",
        )
        st.download_button(
            label="⬇️ 下载通信日志 .txt",
            data=_dl_bytes(_dbg_log),
            file_name=_report_filename("debug_log", _slug),
            mime="text/plain; charset=utf-8",
            use_container_width=True,
            disabled=not _dbg_log,
        )
