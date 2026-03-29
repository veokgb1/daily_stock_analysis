# -*- coding: utf-8 -*-
"""
历史记忆库 · DUKA Stock Analysis Engine V5-Pro
高科技驾驶舱风格重构版 — 遵循 A 股红涨绿跌色彩规范
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

# ── Session state 初始化 ──────────────────────────────────────────────────────
st.session_state.setdefault("selected_run_id", None)
st.session_state.setdefault("report_view_mode", "全量模式")
st.session_state.setdefault("report_preset", "快速决策组合")
st.session_state.setdefault("hide_pending_snap_id", None)
st.session_state.setdefault("confirm_clear_all_data", False)

PRESET_OPTIONS: List[str] = ["快速决策组合", "风险审查组合", "数据复盘组合"]

# ── 全局 CSS 注入（高科技驾驶舱暗黑风格，A 股色彩规范）────────────────────────
st.markdown(
    """
<style>
/* ═══ 全局奶白护眼正文色 ═══ */
.stMarkdown p, .stMarkdown li {
    color: #E2E8F0 !important;
    line-height: 1.75 !important;
}

/* ═══ 大盘报告：高级内参散文容器 ═══ */
.market-prose {
    background: linear-gradient(160deg, #0d1f35 0%, #0f172a 100%);
    border: 1px solid #1e3a5f;
    border-radius: 12px;
    padding: 32px 40px;
    line-height: 1.95;
    color: #E2E8F0;
    font-size: 0.97em;
}
.market-prose h1 { color: #93C5FD; font-size: 1.3em; margin-top: 1.6em; letter-spacing: 0.04em; }
.market-prose h2 { color: #93C5FD; font-size: 1.1em; margin-top: 1.4em; border-bottom: 1px solid #1e3a5f; padding-bottom: 4px; }
.market-prose h3 { color: #7DD3FC; font-size: 1.0em; margin-top: 1.2em; }
.market-prose blockquote {
    border-left: 3px solid #3B82F6;
    padding-left: 1.2em;
    color: #CBD5E1;
    font-style: normal;
    background: rgba(59,130,246,0.06);
    border-radius: 0 6px 6px 0;
    margin: 1em 0;
    padding: 10px 1.2em;
}
.market-prose table { border-collapse: collapse; width: 100%; font-size: 0.9em; margin: 1em 0; }
.market-prose th { background: #1e3a5f; color: #93C5FD; padding: 8px 14px; text-align: center; font-weight: 600; }
.market-prose td { padding: 6px 14px; border-bottom: 1px solid #1f2937; color: #E2E8F0; text-align: center; }
.market-prose tr:hover td { background: rgba(255,255,255,0.03); }

/* ═══ 个股卡片头部 ═══ */
.scard-header {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 6px 0 10px 0;
    border-bottom: 1px solid #1f2937;
    margin-bottom: 10px;
    flex-wrap: wrap;
}
.scard-idx   { color: #475569; font-size: 0.75em; font-weight: 700; min-width: 22px; font-family: monospace; }
.scard-name  { color: #F1F5F9; font-size: 1.02em; font-weight: 700; letter-spacing: 0.02em; }
.scard-code  { color: #64748B; font-size: 0.82em; }
.scard-meta  { color: #475569; font-size: 0.74em; margin-left: auto; white-space: nowrap; }

/* ═══ 评级徽章（A 股红涨绿跌规范）═══ */
.badge {
    display: inline-block; padding: 2px 9px; border-radius: 4px;
    font-size: 0.78em; font-weight: 700; letter-spacing: 0.05em;
}
/* 买入 → 红色系（A 股上涨色）*/
.badge-buy  { background: rgba(239,68,68,0.12); color: #F87171; border: 1px solid rgba(239,68,68,0.5); }
/* 卖出 → 绿色系（A 股下跌色）*/
.badge-sell { background: rgba(34,197,94,0.12);  color: #4ADE80; border: 1px solid rgba(34,197,94,0.5); }
/* 观望 → 黄色系 */
.badge-watch{ background: rgba(234,179,8,0.12);  color: #FACC15; border: 1px solid rgba(234,179,8,0.5); }

/* ═══ 数据维度网格 ═══ */
.data-grid {
    display: grid;
    grid-template-columns: repeat(9, 1fr);
    background: #080f1c;
    border: 1px solid #1e3358;
    border-radius: 8px;
    overflow: hidden;
    margin: 8px 0 12px 0;
}
.data-cell {
    padding: 8px 4px;
    text-align: center;
    border-right: 1px solid #1a2640;
}
.data-cell:last-child { border-right: none; }
.dim-label { font-size: 0.7em; color: #475569; margin-bottom: 3px; white-space: nowrap; }
.dim-val   { font-size: 0.85em; color: #A0AEC0; font-family: 'SF Mono', 'Consolas', monospace; }
.dim-val-hi{ font-size: 0.88em; color: #CBD5E1; font-family: 'SF Mono', 'Consolas', monospace; font-weight: 600; }

/* ═══ 判断维度区 ═══ */
.judge-row {
    display: flex; gap: 16px; flex-wrap: wrap;
    align-items: flex-start; margin: 6px 0 10px 0;
}
.judge-cell { min-width: 90px; }
.judge-cell .dim-label { margin-bottom: 5px; }

/* ═══ 操作维度：价格卡片 ═══ */
.action-grid {
    display: grid; grid-template-columns: repeat(4, 1fr);
    gap: 10px; margin: 8px 0 10px 0;
}
.action-card {
    border-radius: 8px; padding: 10px 8px; text-align: center;
    background: #080f1c;
}
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

/* ═══ 批次导航：radio 极简样式 ═══ */
div[data-testid="stRadio"] > label:first-child { display: none; }
div[data-testid="stRadio"] [data-baseweb="radio"] {
    padding: 4px 0 !important;
}
div[data-testid="stRadio"] [data-baseweb="radio"] label {
    font-size: 0.84em !important;
    color: #64748B !important;
    font-family: 'SF Mono', 'Consolas', monospace !important;
    letter-spacing: 0.01em;
}
div[data-testid="stRadio"] [data-baseweb="radio"] label:hover {
    color: #94A3B8 !important;
}
div[data-testid="stRadio"] [aria-checked="true"] ~ label,
div[data-testid="stRadio"] [data-baseweb="radio"]:has(input:checked) label {
    color: #93C5FD !important;
}

/* ═══ Metric 组件缩小 ═══ */
[data-testid="metric-container"] { background: #080f1c; border-radius: 8px; padding: 8px 10px; }
[data-testid="metric-container"] label { font-size: 0.75em !important; color: #64748B !important; }
[data-testid="metric-container"] [data-testid="stMetricValue"] { font-size: 1.05em !important; }

/* ═══ 机要区日志文字 ═══ */
textarea[disabled] { color: #4B5563 !important; font-size: 0.8em !important; }

/* ═══ 分隔线弱化 ═══ */
hr { border-color: #1f2937 !important; margin: 18px 0 !important; }
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


# ─────────────────────────────────────────────────────────────────────────────
# 二、A 股色彩规范徽章（红涨绿跌，与国际惯例相反）
# ─────────────────────────────────────────────────────────────────────────────

def _advice_badge(advice: str) -> str:
    """
    评级徽章 HTML（严格遵循 A 股色彩规范）
      买入 / 增持 / 看涨 → 🔴 红色系
      卖出 / 减持 / 离场 → 🟢 绿色系
      观望 / 中性      → 🟡 黄色系
    """
    text = _normalize_text(advice) or "未评级"
    if any(w in text for w in ("买", "增持", "看涨", "做多")):
        return f'<span class="badge badge-buy">▲ {text}</span>'
    if any(w in text for w in ("卖", "减持", "离场", "做空")):
        return f'<span class="badge badge-sell">▼ {text}</span>'
    return f'<span class="badge badge-watch">◆ {text}</span>'


def _trend_badge(trend: str) -> str:
    """
    趋势徽章 HTML（A 股色彩规范：看多=红，看空=绿）
    """
    text = _normalize_text(trend) or "趋势待定"
    if any(w in text for w in ("多", "上行", "强势", "看多", "涨")):
        return f'<span class="badge badge-buy">▲ {text}</span>'
    if any(w in text for w in ("空", "下行", "弱势", "看空", "跌")):
        return f'<span class="badge badge-sell">▼ {text}</span>'
    return f'<span class="badge badge-watch">◆ {text}</span>'


def _advice_emoji(advice: str) -> str:
    """纯 Emoji 简版，用于列表摘要（无 HTML）"""
    text = _normalize_text(advice)
    if any(w in text for w in ("买", "增持", "看涨")):
        return "🔴"
    if any(w in text for w in ("卖", "减持", "离场")):
        return "🟢"
    return "🟡"


# ─────────────────────────────────────────────────────────────────────────────
# 三、业务数据工具函数
# ─────────────────────────────────────────────────────────────────────────────

def _resolve_precise_code(
    keyword: str, snapshots: List[Dict[str, Any]]
) -> Optional[str]:
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


def _extract_risk_alerts(
    snap: Dict[str, Any], factors: Dict[str, Any]
) -> List[str]:
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
# 四、下载 / 数据构建工具函数
# ─────────────────────────────────────────────────────────────────────────────

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


def _build_batch_download_text(run_id: str, batch_snaps: List[Dict[str, Any]]) -> str:
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


def _get_batch_artifacts_payload(
    run_id: str, batch_snaps: List[Dict[str, Any]]
) -> Dict[str, str]:
    artifacts = get_run_artifacts(run_id) or {}
    stock_snaps = [s for s in batch_snaps if s.get("code") != "__market__"]
    market_fb = "\n\n".join(
        s.get("report_md") or "" for s in batch_snaps if s.get("code") == "__market__"
    ).strip()
    return {
        "market_report_md": _normalize_text(artifacts.get("market_report_md")) or market_fb,
        "stock_report_md": _normalize_text(artifacts.get("stock_report_md"))
            or _build_stock_report_text(stock_snaps),
        "full_report_md": _normalize_text(artifacts.get("full_report_md"))
            or _build_batch_download_text(run_id, batch_snaps),
        "business_log": _normalize_text(artifacts.get("business_log")),
        "debug_log": _normalize_text(artifacts.get("debug_log")),
        "schema_json": _normalize_text(artifacts.get("schema_json"))
            or _build_batch_schema_json(run_id, batch_snaps),
    }


# ─────────────────────────────────────────────────────────────────────────────
# 五、个股卡片三维渲染函数
# ─────────────────────────────────────────────────────────────────────────────

def _render_data_block(snap: Dict[str, Any], factors: Dict[str, Any]) -> None:
    """数据维度：价格 / 均线 / 乖离率 / 量比 / 支撑压力 — HTML 网格，小字暗色"""
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
    st.markdown(
        f'<div class="data-grid">{cells}</div>',
        unsafe_allow_html=True,
    )


def _render_judge_block(snap: Dict[str, Any], factors: Dict[str, Any]) -> None:
    """判断维度：评级徽章 / 趋势 / 均线 / 评分 / 舆情"""
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
    """操作维度：四格价格卡 + 仓位 + 一句话决策"""
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


# ── 进阶预设渲染（复用三维函数）────────────────────────────────────────────────

def _render_quick_decision_preset(snap: Dict[str, Any], factors: Dict[str, Any]) -> None:
    """快速决策：评级 + 三价 + 一句话决策"""
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
    """风险审查：全量判断维度 + 止损位单格"""
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
    """数据复盘：全量数据维度 + 均线 + 趋势强度"""
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


# ── 操作按钮行（跟踪 / 隐藏 / 原始报告）──────────────────────────────────────

def _render_card_actions(snap: Dict[str, Any], prefix: str) -> None:
    action_cols = st.columns([1, 1, 4])
    with action_cols[0]:
        if snap.get("code") != "__market__":
            if st.button(
                "📌 跟踪",
                key=f"track_{prefix}_{snap['id']}",
                use_container_width=True,
            ):
                add_to_quick_pool(snap["code"], snap["name"])
                st.toast(f"✅ {snap['name']} 已加入跟踪池")
    with action_cols[1]:
        if st.button(
            "👁️ 隐藏当前",
            key=f"hide_{prefix}_{snap['id']}",
            use_container_width=True,
        ):
            st.session_state["hide_pending_snap_id"] = snap["id"]
            st.rerun()
    with action_cols[2]:
        with st.expander("查看原始 Markdown 报告", expanded=False):
            st.markdown(snap.get("report_md") or "_无报告内容_")

    if st.session_state.get("hide_pending_snap_id") == snap["id"]:
        st.warning("确认后将从列表隐藏，数据库记录不会物理删除。")
        c1, c2, _ = st.columns([1, 1, 4])
        with c1:
            if st.button(
                "确认隐藏",
                key=f"confirm_hide_{prefix}_{snap['id']}",
                use_container_width=True,
            ):
                delete_snapshot(snap["id"])
                st.session_state["hide_pending_snap_id"] = None
                st.rerun()
        with c2:
            if st.button(
                "取消",
                key=f"cancel_hide_{prefix}_{snap['id']}",
                use_container_width=True,
            ):
                st.session_state["hide_pending_snap_id"] = None
                st.rerun()


# ── 核心个股卡片渲染（三维 × 三模式）────────────────────────────────────────

def _render_stock_card_v2(
    snap: Dict[str, Any],
    mode: str,
    preset: str,
    index: int,
    prefix: str,
) -> None:
    factors = _safe_json_loads(snap.get("factors_json"))
    name    = snap.get("name") or "—"
    code    = snap.get("code") or "—"
    dt_str  = _fmt_dt(snap.get("created_at"))
    score   = _fmt_score(snap.get("sentiment_score"))
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

    # 内容区：按浏览模式分支
    if mode == "全量模式":
        st.markdown(
            '<p class="dim-label" style="margin:6px 0 2px 0;">📊 数据维度</p>',
            unsafe_allow_html=True,
        )
        _render_data_block(snap, factors)
        st.markdown(
            '<p class="dim-label" style="margin:8px 0 2px 0;">🎯 判断维度</p>',
            unsafe_allow_html=True,
        )
        _render_judge_block(snap, factors)
        st.markdown(
            '<p class="dim-label" style="margin:8px 0 2px 0;">📋 操作维度</p>',
            unsafe_allow_html=True,
        )
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
    advice_text  = _normalize_text(snap.get("operation_advice"))
    trend_text   = _normalize_text(snap.get("trend_prediction"))
    code_text    = _normalize_text(snap.get("code")).lower()
    name_text    = _normalize_text(snap.get("name")).lower()
    kw           = _normalize_text(keyword).lower()
    if advice_filter and advice_filter not in advice_text:
        return False
    if trend_filter and trend_filter not in trend_text:
        return False
    if kw and kw not in code_text and kw not in name_text:
        return False
    return True


# ─────────────────────────────────────────────────────────────────────────────
# 七、时光轴模式（精确锁定单只股票）
# ─────────────────────────────────────────────────────────────────────────────

def _render_timeline_mode(precise_code: str) -> Optional[str]:
    history = get_code_history(precise_code)
    stock_name = history[0].get("name") if history else precise_code

    st.markdown(
        f"<p style='color:#93C5FD; font-size:1.0em; font-weight:600; margin-bottom:2px;'>"
        f"⏱️ {stock_name}（{precise_code}）时光轴复盘</p>"
        f"<p class='dim-label'>按版本切换与时序卡片回看 · 保留个股原始报告与隐藏操作</p>",
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
    selected_label = st.selectbox(
        "选择历史版本", list(version_options.keys()), index=0
    )
    selected_snap = version_options[selected_label]

    with st.expander("📄 当前版本报告详情", expanded=True):
        st.markdown(selected_snap.get("report_md") or "_无报告内容_")

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

    # 构建批次信息（run_id + 显示标签）
    batch_info: List[Tuple[str, str]] = []
    for idx, row in enumerate(run_rows, start=1):
        run_id = row.get("run_id")
        if not run_id:
            continue
        batch_snaps = get_run_snapshots(run_id)
        stock_names = [
            s.get("name")
            for s in batch_snaps
            if s.get("code") != "__market__" and s.get("name")
        ]
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

    # 首次加载时自动选中最新批次
    if st.session_state.get("selected_run_id") not in set(run_ids):
        st.session_state["selected_run_id"] = run_ids[0]

    cur_idx = next(
        (i for i, ri in enumerate(run_ids) if ri == st.session_state["selected_run_id"]),
        0,
    )

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


# ─────────────────────────────────────────────────────────────────────────────
# 九、批次报告阅读区（三 Tab + 三模式 + 下载矩阵）
# ─────────────────────────────────────────────────────────────────────────────

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

    artifacts     = _get_batch_artifacts_payload(selected_run_id, batch_snaps)
    run_date_slug = _infer_run_date_slug(batch_snaps)
    market_md     = artifacts.get("market_report_md", "")
    stock_md      = artifacts.get("stock_report_md", "")
    full_md       = artifacts.get("full_report_md", "")

    # 批次汇总简表
    summary_rows = []
    for i, snap in enumerate(
        [s for s in batch_snaps if s.get("code") != "__market__"], start=1
    ):
        summary_rows.append({
            "#": str(i),
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

    # ── 三 Tab 报告区 ──────────────────────────────────────────────────────
    tab_market, tab_full, tab_stock = st.tabs(
        ["📊 大盘报告", "📄 全量报告", "🔬 个股分析"]
    )

    # Tab 1：大盘报告（高级内参散文风格）
    with tab_market:
        if market_md:
            st.markdown(
                f'<div class="market-prose">\n\n{market_md}\n\n</div>',
                unsafe_allow_html=True,
            )
        else:
            st.info("当前批次暂无大盘报告。")

    # Tab 2：全量报告
    with tab_full:
        if full_md:
            st.markdown(full_md)
        else:
            st.info("当前批次暂无全量报告。")

    # Tab 3：个股分析（驾驶舱仪表盘）
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

    # ── 下载矩阵（Tab 区下方，三列整齐）──────────────────────────────────
    st.markdown(
        "<p class='dim-label' style='margin:18px 0 6px 0; text-transform:uppercase; "
        "letter-spacing:0.08em;'>⬇ 下载矩阵</p>",
        unsafe_allow_html=True,
    )
    dl1, dl2, dl3 = st.columns(3)
    with dl1:
        if market_md:
            st.download_button(
                "⬇️ 大盘报告 .md",
                data=market_md,
                file_name=f"market_review_{run_date_slug}.md",
                mime="text/markdown",
                use_container_width=True,
            )
        else:
            st.caption("暂无大盘报告")
    with dl2:
        if stock_md:
            st.download_button(
                "⬇️ 个股报告 .md",
                data=stock_md,
                file_name=f"stock_report_{run_date_slug}.md",
                mime="text/markdown",
                use_container_width=True,
            )
        else:
            st.caption("暂无个股报告")
    with dl3:
        if full_md:
            st.download_button(
                "⬇️ 全量报告 .md",
                data=full_md,
                file_name=f"full_report_{run_date_slug}.md",
                mime="text/markdown",
                use_container_width=True,
            )
        else:
            st.caption("暂无全量报告")

    return {**artifacts, "run_id": selected_run_id, "run_date_slug": run_date_slug}


# ─────────────────────────────────────────────────────────────────────────────
# 十、侧边栏（过滤条件 + 危险区）
# ─────────────────────────────────────────────────────────────────────────────

with st.sidebar:
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
            if st.button(
                "🚨 确认执行清空",
                use_container_width=True,
                type="primary",
            ):
                clear_all_data()
                st.session_state["confirm_clear_all_data"] = False
                st.success("数据已全部清空")
                st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# 十一、主页面入口
# ─────────────────────────────────────────────────────────────────────────────

st.markdown(
    "<h2 style='margin-bottom:2px; color:#F1F5F9; font-weight:700;'>🕰️ 历史记忆库</h2>"
    "<p class='dim-label' style='margin-bottom:16px;'>"
    "按分析批次分组回看 · 支持精确锁定个股时光轴</p>",
    unsafe_allow_html=True,
)

run_rows      = list_recent_runs(limit=limit)
all_snapshots = get_snapshots_with_filters(limit=max(limit * 3, 120))
precise_code  = _resolve_precise_code(keyword, all_snapshots)
advice_value  = None if advice_filter == "全部" else advice_filter
trend_value   = None if trend_filter == "全部" else trend_filter

# ZONE B：批次导航条
_render_batch_navigation(run_rows)
st.divider()

# 机要区占位（保证页面底部永远有内容）
_secret: Dict[str, str] = {
    "business_log": "",
    "debug_log": "",
    "run_id": "",
    "run_date_slug": datetime.now().strftime("%Y%m%d_%H%M%S"),
}

# ZONE C：报告详情区（时光轴模式 or 批次模式）
if precise_code:
    timeline_run_id = _render_timeline_mode(precise_code)
    if timeline_run_id:
        timeline_snaps = get_run_snapshots(timeline_run_id)
        _secret.update(_get_batch_artifacts_payload(timeline_run_id, timeline_snaps))
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

# ── 统一提取：确保 text_area 与 download_button 100% 使用同一份变量 ──────────
# 从 _secret 中安全读取，空字符串统一视为"无数据"
_biz_log: str = (_secret.get("business_log") or "").strip()
_dbg_log: str = (_secret.get("debug_log") or "").strip()
_slug:    str = _secret.get("run_date_slug") or datetime.now().strftime("%Y%m%d_%H%M%S")

sec_col1, sec_col2 = st.columns(2)

# 左片：运行日志（Business Log）
with sec_col1:
    with st.expander("📋 运行日志（Business Log）", expanded=False):
        st.text_area(
            label="",
            value=_biz_log if _biz_log else "暂无运行日志",
            height=300,
            disabled=True,
            key="history_business_log",
        )
        st.download_button(
            label="⬇️ 下载运行日志",
            data=_biz_log if _biz_log else "（暂无运行日志）",
            file_name=f"business_log_{_slug}.log",
            mime="text/plain",
            use_container_width=True,
            disabled=not _biz_log,
        )

# 右片：通信日志（Debug Log）
with sec_col2:
    with st.expander("🔧 通信日志（Debug Log）", expanded=False):
        st.text_area(
            label="",
            value=_dbg_log if _dbg_log else "暂无通信日志",
            height=300,
            disabled=True,
            key="history_debug_log",
        )
        st.download_button(
            label="⬇️ 下载通信日志",
            data=_dbg_log if _dbg_log else "（暂无通信日志）",
            file_name=f"debug_log_{_slug}.log",
            mime="text/plain",
            use_container_width=True,
            disabled=not _dbg_log,
        )
