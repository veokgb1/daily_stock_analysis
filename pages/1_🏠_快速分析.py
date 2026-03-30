# Fix stock data alignment-*- coding: utf-8 -*-
"""
pages/1_🏠_快速分析.py  V5-Pro · 纯血 Gemini 底盘 · 双风格布局切换
================================================================

布局风格（侧边栏切换）：
  🅰 编辑室 · 左右分栏：候选池（左）+ 已选预览（右）+ 黄色确认小票
  🅱 手术室 · 深色 5 列网格 + 底部状态条 + 边框确认清单

架构红线（不可倒退）：
  · 提取引擎 → `extract_engine.py`（纯 Gemini，OpenAI 兼容格式）
  · `_run_stock_analysis` 包含 `StockAnalysisPipeline + save_snapshot` 真实逻辑
  · `_run_market_review` 包含 `run_market_review` 真实逻辑
  · Live Timer 实时计时器贯穿整个执行过程
  · 浅色模式背景 `#F5F3EE`（护眼奶白）
"""

import json
import os, sys, re, time, uuid, logging, queue, threading
from collections import deque
from contextlib import nullcontext
from io import BytesIO
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import List, Set

# 路径
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from src.config import setup_env
setup_env()

import streamlit as st
import streamlit.components.v1 as components
from streamlit_searchbox import st_searchbox

from src.config import get_config
from src.streamlit_guard import enforce_sidebar_password_gate
from src.formatters import markdown_to_plain_text
from src.core.pipeline import StockAnalysisPipeline
from src.logging_config import setup_logging
from src.data.stock_mapping import STOCK_NAME_MAP
from stock_mapping import STOCK_NAME_MAP as LOCAL_NAME_CODE_MAP
from data_provider.base import canonical_stock_code
from webui.db import (
    add_to_watchlist, init_db, save_snapshot, save_strategy_group,
    list_strategy_groups, get_strategy_group, delete_strategy_group,
    list_watchlist, list_quick_pool, clear_quick_pool, save_run_artifacts,
)
from webui.factor_extractor import extract_factors, enrich_from_md
from webui.fuzzy_matcher import suggest_stock_candidates
from extract_engine import (
    extract_from_text, extract_from_images,
    extract_from_voice, transcribe_audio,
    ensure_proxy,
)

setup_logging(log_prefix="webui_v8", debug=False)
logger = logging.getLogger(__name__)
init_db()

# ── 股票目录单例（进程级缓存，所有 Streamlit session 共享）─────────────────
from webui.stock_catalog import CatalogStore  # noqa: E402
import webui.stock_catalog as stock_catalog_module  # noqa: E402

@st.cache_resource(show_spinner=False)
def _get_catalog() -> CatalogStore:
    cat = CatalogStore()
    cat.bootstrap()          # 同步加载本地种子，后台启动三市场热更新线程
    return cat

_catalog: CatalogStore = _get_catalog()


def _catalog_is_search_ready(market: str = "A") -> bool:
    method = getattr(_catalog, "is_search_ready", None)
    if callable(method):
        try:
            return bool(method(market))
        except Exception:
            pass
    fallback = getattr(stock_catalog_module, "is_search_ready", None)
    if callable(fallback):
        try:
            return bool(fallback(market))
        except Exception:
            return False
    return False


def _schedule_catalog_daily_refresh() -> None:
    today = datetime.now().date().isoformat()
    if st.session_state.get("last_update_check") == today:
        return
    st.session_state["last_update_check"] = today
    _catalog.schedule_daily_a_refresh(today=today)


def _flush_catalog_update_notices() -> None:
    for message in _catalog.consume_notices():
        st.toast(message)

_MAX_WARN   = 30
_BATCH_SIZE = 15
_PRESET_TAGS = ["热门", "科技", "消费", "金融", "长线", "短线", "观察", "重仓"]
_MODES       = ["仅个股分析", "仅大盘复盘", "全量分析（个股 + 大盘）"]
_LOCAL_NAME_CODE_ITEMS = sorted(
    [
        (str(name).strip(), str(code).strip())
        for name, code in LOCAL_NAME_CODE_MAP.items()
        if str(name).strip() and re.fullmatch(r"\d{6}", str(code).strip())
    ],
    key=lambda item: len(item[0]),
    reverse=True,
)

# =============================================================================
# 主题令牌
# =============================================================================
_THEMES = {
    "浅色·编辑室": {
        "--bg":            "#F5F3EE",
        "--surface":       "#EFECE5",   # V8.2：护眼暖灰，消除纯白刺眼
        "--surface2":      "#E8E5DD",   # V8.2：更深一档暖灰
        "--border":        "rgba(214,211,200,1)",
        "--text":          "#1a1a2e",
        "--text-muted":    "#64748b",
        "--sidebar-bg":    "#F0EDE6",
        "--receipt-bg":    "linear-gradient(135deg,#fefce8 0%,#fef9c3 100%)",
        "--receipt-border":"#eab308",
        "--receipt-text":  "#713f12",
        "--success-bg":    "#f0fdf4",
        "--success-fg":    "#15803d",
        "--warn-bg":       "#fffbeb",
        "--warn-fg":       "#92400e",
        "--preview-bg":    "#E8E5DD",   # V8.2：与 surface2 一致
        "--preview-border":"#d4d0c8",
        "--th-bg":         "#E0DDD5",
        "--td-alt":        "#EDEAE2",
        "--code-bg":       "#E8E5DD",
        "--blockquote-l":  "#3b82f6",
        "--blockquote-bg": "#eff6ff",
    },
    "深色·手术室": {
        "--bg":            "#0d1117",
        "--surface":       "rgba(22,27,34,0.95)",
        "--surface2":      "rgba(33,38,45,0.8)",
        "--border":        "rgba(48,54,61,1)",
        "--text":          "#e6edf3",
        "--text-muted":    "#8b949e",
        "--sidebar-bg":    "#0d1117",
        "--receipt-bg":    "linear-gradient(135deg,rgba(21,128,61,0.2) 0%,rgba(20,83,45,0.3) 100%)",
        "--receipt-border":"rgba(74,222,128,0.5)",
        "--receipt-text":  "#86efac",
        "--success-bg":    "rgba(20,83,45,0.3)",
        "--success-fg":    "#86efac",
        "--warn-bg":       "rgba(120,53,15,0.3)",
        "--warn-fg":       "#fca5a5",
        "--preview-bg":    "rgba(22,27,34,0.98)",
        "--preview-border":"rgba(48,54,61,1)",
        "--th-bg":         "rgba(33,38,45,0.9)",
        "--td-alt":        "rgba(22,27,34,0.7)",
        "--code-bg":       "rgba(33,38,45,0.9)",
        "--blockquote-l":  "#58a6ff",
        "--blockquote-bg": "rgba(30,58,138,0.2)",
    },
}
_ACCENTS = {
    "科技蓝": {"--accent":"#3b82f6","--accent-dim":"rgba(59,130,246,0.12)","--accent-text":"#fff","--badge-bg":"#1e3a5f","--badge-text":"#bfdbfe"},
    "护眼绿": {"--accent":"#10b981","--accent-dim":"rgba(16,185,129,0.12)","--accent-text":"#fff","--badge-bg":"#064e3b","--badge-text":"#d1fae5"},
    "极光紫": {"--accent":"#8b5cf6","--accent-dim":"rgba(139,92,246,0.12)","--accent-text":"#fff","--badge-bg":"#4c1d95","--badge-text":"#ede9fe"},
}
_FONT_SIZES = {
    "小": {"base":"13px","report":"0.82rem","heading":"1.0rem","metric":"0.78rem"},
    "中": {"base":"14px","report":"0.88rem","heading":"1.08rem","metric":"0.84rem"},
    "大": {"base":"16px","report":"0.96rem","heading":"1.18rem","metric":"0.92rem"},
}

# =============================================================================
# 页面配置
# =============================================================================
st.set_page_config(
    page_title="快速分析 · DUKA Stock Analysis Engine V5-Pro",
    page_icon="🏠", layout="wide",
    initial_sidebar_state="expanded",
)
enforce_sidebar_password_gate()

_FAST_PAGE_ID = "quick_analysis"
_fast_page_entered = st.session_state.get("_active_streamlit_page") != _FAST_PAGE_ID
st.session_state["_active_streamlit_page"] = _FAST_PAGE_ID

# =============================================================================
# Session State
# =============================================================================
_SS = {
    # 外观
    "theme":       "深色·手术室",
    "accent":      "科技蓝",
    "font_size":   "中",
    # 布局风格（只保留两种）
    "layout_style":"🅱 手术室",
    # 代码池
    "pool_codes":   [],
    "checked_codes":set(),
    "pool_sources": {},   # {code: "text"|"image"|"voice"}
    "pool_names":   {},   # {code: 股票名称}
    "fuzzy_candidates": [],
    # 运行
    "run_mode":    "仅个股分析",
    "is_running":  False,
    "run_requested": False,
    "pause_flag":  False,
    "stop_flag":   False,
    "pending_codes": [],
    "pending_mode": "",
    "last_error":  "",
    "run_ts":      "",
    "run_id":      "",
    "elapsed_sec": 0.0,
    # 结果
    "analysis_results":  [],
    "per_stock_reports": {},
    "snapshot_ids":     {},
    "snapshot_factors": {},
    "watchlist_feedback": "",
    "analysis_report":   "",
    "market_report":     "",
    "report_panel_hidden": False,
    "voice_transcript":  "",
    # ── Input Wizard ──────────────────────────────────────────────────────────
    "iw_market":       "A",    # 当前市场选择：A / HK / US
    "iw_pending":      None,   # 待加入池的 (code, name)，None = 无待处理
    "iw_should_focus": False,  # True → JS 在下次渲染后回焦输入框
    "iw_input_gen":    0,      # 每次清空输入框时递增，用于 key 轮转
    "iw_searchbox_result_count": 0,
    "iw_searchbox_single_exact": False,
    "last_update_check": "",
}
# setdefault 保证：只在 key 不存在时写入初始值，跨页面导航绝不覆盖已有状态
for _k, _v in _SS.items():
    st.session_state.setdefault(
        _k, _v.copy() if isinstance(_v, (set, list, dict)) else _v
    )

_FAST_WIDGET_PREFIXES = (
    "iw_q_",
    "iw_search_",
    "cb_",
    "cart_keep_",
    "fuzzy_use_",
    "fuzzy_pick_",
    "tag_",
)
_FAST_WIDGET_KEYS = {
    "iw_market_radio",
    "iw_searchbox_locked",
    "iw_searchbox_loading",
    "sb_run_mode",
    "main_run_mode",
    "sg_name",
    "sg_desc",
}

_schedule_catalog_daily_refresh()
_flush_catalog_update_notices()

# =============================================================================
# 主题注入
# =============================================================================
def _inject_theme():
    tk  = {**_THEMES.get(st.session_state.theme, _THEMES["深色·手术室"]),
           **_ACCENTS.get(st.session_state.accent, _ACCENTS["科技蓝"])}
    fs  = _FONT_SIZES.get(st.session_state.font_size, _FONT_SIZES["中"])
    tok = "\n".join(f"  {k}: {v};" for k,v in tk.items())
    st.markdown(f"""<style>
/* 使用系统原生无衬线字体栈，避免引入外部字体导致输入法异常 */
:root {{ {tok} }}
html,body,[class*="css"]{{font-family:'Microsoft YaHei','PingFang SC','Hiragino Sans GB','WenQuanYi Micro Hei',sans-serif;font-size:{fs['base']};-webkit-font-smoothing:antialiased;color:var(--text);background-color:var(--bg);}}
/* Markdown */
.stMarkdown p,.stMarkdown li{{font-size:{fs['report']};line-height:1.78;color:var(--text);}}
.stMarkdown h1{{font-size:calc({fs['heading']} + .22rem);font-weight:900;color:var(--text);margin:1.2rem 0 .5rem;}}
.stMarkdown h2{{font-size:calc({fs['heading']} + .1rem);font-weight:700;color:var(--text);margin:1rem 0 .4rem;border-bottom:2px solid var(--border);padding-bottom:.25rem;}}
.stMarkdown h3{{font-size:{fs['heading']};font-weight:600;color:var(--text);margin:.8rem 0 .3rem;}}
.stMarkdown table{{font-size:{fs['report']};border-collapse:collapse;width:100%;margin:.4rem 0;}}
.stMarkdown th{{background:var(--th-bg);padding:6px 12px;font-weight:600;text-align:left;border:1px solid var(--border);color:var(--text);}}
.stMarkdown td{{padding:5px 12px;border:1px solid var(--border);color:var(--text);}}
.stMarkdown tr:nth-child(even) td{{background:var(--td-alt);}}
.stMarkdown code{{font-family:'JetBrains Mono',monospace;font-size:.85em;background:var(--code-bg);padding:1px 5px;border-radius:4px;color:var(--text);}}
.stMarkdown blockquote{{border-left:3px solid var(--blockquote-l);padding:.4rem .8rem;background:var(--blockquote-bg);margin:.5rem 0;border-radius:0 6px 6px 0;color:var(--text);}}
/* 步骤卡 */
.step-card{{border:1px solid var(--border);border-radius:10px;padding:.5rem .9rem;margin-bottom:.45rem;background:var(--surface);box-shadow:0 1px 3px rgba(0,0,0,.05);}}
.step-card.compact{{padding:.38rem .78rem;margin-bottom:.35rem;}}
.step-head{{display:flex;align-items:center;gap:8px;min-height:24px;}}
.step-badge{{display:inline-flex;align-items:center;justify-content:center;background:var(--badge-bg);color:var(--badge-text);border-radius:999px;padding:0 8px;font-size:.58rem;font-family:'JetBrains Mono',monospace;font-weight:700;letter-spacing:.06em;line-height:1.55;height:20px;min-width:48px;}}
.step-title{{font-size:calc({fs['heading']} - .08rem);font-weight:700;color:var(--text);margin:0;line-height:1.2;}}
.step-sub{{font-size:.76rem;color:var(--text-muted);margin:.22rem 0 0;line-height:1.35;}}
.result-card{{background:var(--surface2);border:1px solid var(--border);border-radius:12px;padding:.72rem .82rem;min-height:124px;box-shadow:0 4px 14px rgba(0,0,0,.08);}}
.result-name{{font-size:.9rem;font-weight:800;color:var(--text);line-height:1.3;margin-bottom:.38rem;}}
.result-advice{{font-size:.92rem;font-weight:800;line-height:1.22;margin:.04rem 0 .26rem;}}
.result-advice.bull{{color:#ef4444;}}
.result-advice.bear{{color:#22c55e;}}
.result-advice.neutral{{color:#f59e0b;}}
.result-score{{font-size:.8rem;color:var(--text-muted);font-family:'JetBrains Mono',monospace;line-height:1.45;}}
.result-score .bull{{color:#f87171;font-weight:700;}}
.result-score .bear{{color:#4ade80;font-weight:700;}}
.result-score .neutral{{color:#fbbf24;font-weight:700;}}
/* 按钮 */
.stButton>button[kind="primary"]{{background:var(--accent)!important;color:var(--accent-text)!important;border:none!important;border-radius:8px!important;font-weight:600!important;}}
.stButton>button[kind="secondary"]{{border:1.5px solid var(--accent)!important;color:var(--accent)!important;background:var(--accent-dim)!important;border-radius:8px!important;font-weight:600!important;}}
.stDownloadButton>button{{background:var(--badge-bg)!important;color:var(--badge-text)!important;border-radius:8px!important;font-weight:600!important;border:none!important;}}
/* checkbox */
.stCheckbox label{{font-family:'JetBrains Mono',monospace;font-size:.84rem!important;font-weight:600;color:var(--text)!important;}}
/* metric */
[data-testid="metric-container"]{{background:var(--surface2);border:1px solid var(--border);border-radius:10px;padding:.6rem .85rem .45rem;}}
[data-testid="metric-container"] label{{font-size:{fs['metric']}!important;color:var(--text-muted)!important;}}
[data-testid="metric-container"] [data-testid="stMetricValue"]{{font-size:calc({fs['metric']} + .16rem)!important;font-weight:700;color:var(--text)!important;}}
/* 侧边栏 */
[data-testid="stSidebar"]{{background:var(--sidebar-bg)!important;border-right:1px solid var(--border);}}
[data-testid="stSidebar"] p,[data-testid="stSidebar"] label{{color:var(--text)!important;font-size:.83rem;}}
/* 输入框 */
.stTextInput>div>div,.stTextArea>div>div{{border:1px solid var(--border)!important;background:var(--surface)!important;border-radius:7px!important;}}
.stTextInput input,.stTextArea textarea{{color:var(--text)!important;background:transparent!important;}}
/* tabs */
.stTabs [data-baseweb="tab"]{{color:var(--text-muted);font-size:.85rem;}}
.stTabs [aria-selected="true"]{{color:var(--accent)!important;border-bottom:2px solid var(--accent)!important;}}
/* 计时器 */
.live-timer{{display:inline-flex;align-items:center;gap:6px;background:var(--warn-bg);color:var(--warn-fg);border:1px solid var(--warn-fg);border-radius:999px;padding:3px 14px;font-size:.82rem;font-family:'JetBrains Mono',monospace;font-weight:600;opacity:.88;}}
.elapsed-pill{{display:inline-flex;align-items:center;gap:6px;background:var(--success-bg);color:var(--success-fg);border:1px solid var(--success-fg);border-radius:999px;padding:3px 14px;font-size:.82rem;font-family:'JetBrains Mono',monospace;font-weight:600;}}
/* status */
[data-testid="stStatusWidget"]{{border-color:var(--accent)!important;}}
/* 确认小票 */
.receipt-box{{background:var(--receipt-bg);border:2px solid var(--receipt-border);border-radius:10px;padding:.75rem 1.1rem;margin:.5rem 0 .8rem;font-family:'JetBrains Mono',monospace;font-size:.84rem;color:var(--receipt-text);font-weight:600;}}
.receipt-label{{font-size:.7rem;opacity:.75;margin-bottom:.25rem;font-weight:500;letter-spacing:.08em;text-transform:uppercase;}}
/* 编辑室：预览面板 */
.preview-panel{{background:var(--preview-bg);border:1px solid var(--preview-border);border-radius:10px;padding:.8rem 1rem;min-height:200px;max-height:420px;overflow-y:auto;font-size:.83rem;line-height:1.7;}}
.preview-item{{display:flex;justify-content:space-between;align-items:center;padding:3px 0;border-bottom:1px solid var(--border);}}
.preview-item:last-child{{border-bottom:none;}}
/* 手术室：状态条 */
.status-bar{{background:linear-gradient(135deg,rgba(16,185,129,0.14) 0%,rgba(20,83,45,0.26) 100%);border:1px solid rgba(74,222,128,0.35);border-radius:12px;padding:.7rem 1rem;margin:.65rem 0;display:flex;align-items:center;gap:12px;font-size:.83rem;box-shadow:inset 0 1px 0 rgba(255,255,255,0.03),0 8px 24px rgba(0,0,0,.12);}}
/* 手术室：代码卡格 */
.op-card{{background:var(--surface);border:1.5px solid var(--accent);border-radius:8px;padding:5px 10px;font-family:'JetBrains Mono',monospace;font-size:.82rem;font-weight:700;color:var(--accent);}}
.un-card{{background:var(--surface2);border:1px solid var(--border);border-radius:8px;padding:5px 10px;font-family:'JetBrains Mono',monospace;font-size:.82rem;font-weight:500;color:var(--text-muted);}}
.wizard-shell{{display:flex;flex-direction:column;gap:12px;margin:.25rem 0 1rem;}}
.wizard-step{{display:flex;gap:12px;align-items:flex-start;background:var(--surface);border:1px solid var(--border);border-radius:14px;padding:.9rem 1rem;}}
.wizard-step.is-active{{background:var(--blockquote-bg);border-left:4px solid var(--accent);}}
.wizard-step.is-done{{border-left:4px solid #10b981;}}
.wizard-step.is-idle{{opacity:.92;}}
.wizard-icon{{width:34px;height:34px;border-radius:999px;display:flex;align-items:center;justify-content:center;background:var(--accent-dim);color:var(--accent);font-size:1rem;flex:0 0 34px;}}
.wizard-body{{display:flex;flex-direction:column;gap:3px;}}
.wizard-title{{font-size:.98rem;font-weight:800;color:var(--text);}}
.wizard-sub{{font-size:.8rem;color:var(--text-muted);line-height:1.6;}}
.paste-hint{{font-size:.78rem;color:var(--text-muted);margin:.3rem 0 0;}}
.src-badge{{display:inline-flex;align-items:center;justify-content:center;min-width:72px;padding:2px 8px;border-radius:999px;font-size:.72rem;font-family:'JetBrains Mono',monospace;font-weight:700;letter-spacing:.01em;margin-right:8px;}}
.src-badge.text{{background:rgba(96,125,139,0.22);color:#dbeafe;border:1px solid rgba(148,163,184,0.38);}}
.src-badge.image{{background:rgba(74,222,128,0.18);color:#dcfce7;border:1px solid rgba(74,222,128,0.38);}}
.src-badge.voice{{background:rgba(251,191,36,0.20);color:#fef3c7;border:1px solid rgba(251,191,36,0.42);}}
.src-badge.manual{{background:rgba(148,163,184,0.16);color:#cbd5e1;border:1px solid rgba(148,163,184,0.35);}}
.src-badge.fuzzy{{background:rgba(236,72,153,0.16);color:#f9a8d4;border:1px solid rgba(236,72,153,0.35);}}
.source-group-header{{display:flex;align-items:center;gap:10px;padding:.5rem .8rem;border-radius:12px;margin:.35rem 0 .65rem;font-size:.8rem;font-weight:700;border:1px solid var(--border);}}
.source-group-header .group-badge{{display:inline-flex;align-items:center;justify-content:center;min-width:38px;height:24px;border-radius:999px;font-family:'JetBrains Mono',monospace;font-size:.72rem;font-weight:800;}}
.source-group-header.text{{background:rgba(96,125,139,0.15);}}
.source-group-header.text .group-badge{{background:rgba(96,125,139,0.32);color:#dbeafe;}}
.source-group-header.image{{background:rgba(74,222,128,0.12);}}
.source-group-header.image .group-badge{{background:rgba(74,222,128,0.24);color:#dcfce7;}}
.source-group-header.voice{{background:rgba(251,191,36,0.12);}}
.source-group-header.voice .group-badge{{background:rgba(251,191,36,0.24);color:#fef3c7;}}
.source-group-header.manual{{background:rgba(148,163,184,0.12);}}
.source-group-header.manual .group-badge{{background:rgba(148,163,184,0.22);color:#e2e8f0;}}
.source-group-header.fuzzy{{background:rgba(236,72,153,0.10);}}
.source-group-header.fuzzy .group-badge{{background:rgba(236,72,153,0.18);color:#fbcfe8;}}
.cart-chip{{border:1px solid var(--border);border-radius:12px;padding:.25rem .75rem .45rem;background:var(--surface2);min-height:58px;box-shadow:0 4px 14px rgba(0,0,0,.06);}}
.cart-chip.text{{background:rgba(96,125,139,0.12);}}
.cart-chip.image{{background:rgba(74,222,128,0.10);}}
.cart-chip.voice{{background:rgba(251,191,36,0.10);}}
.cart-chip.manual,.cart-chip.fuzzy{{background:var(--surface2);}}
/* 鍝嶅簲寮?*/
@media(max-width:768px){{.step-card{{padding:.42rem .7rem;}}.step-card.compact{{padding:.34rem .62rem;}}.step-title{{font-size:.94rem;}}.step-badge{{min-width:44px;height:18px;font-size:.54rem;}}}}
</style>""", unsafe_allow_html=True)

_inject_theme()

# 强制声明简体中文环境，避免浏览器因外部字体请求触发繁体输入法
st.markdown("<script>document.documentElement.lang='zh-CN';</script>",
            unsafe_allow_html=True)

# =============================================================================
# API Key 检查
# =============================================================================
@st.cache_resource(show_spinner=False)
def _check_key():
    try:
        cfg = get_config()
        return bool(
            getattr(cfg,"gemini_api_key",None) or getattr(cfg,"openai_api_key",None)
            or os.getenv("GEMINI_API_KEY") or os.getenv("OPENAI_API_KEY")
        ), ""
    except Exception as e:
        return False, str(e)

_ok, _err = _check_key()
if not _ok:
    st.error(
        "⚠️ **未检测到有效 API Key**\n\n"
        "请在 `.env` 中配置 `GEMINI_API_KEY` 或 `OPENAI_API_KEY`，然后重启服务。"
        + (f"\n\n> `{_err}`" if _err else "")
    )
    st.stop()

# =============================================================================
# 代码池工具
# =============================================================================
def _norm(c: str) -> str:
    return canonical_stock_code(c.strip())


def _copy_state_value(value):
    return value.copy() if isinstance(value, (set, list, dict)) else value


def _clear_fast_analysis_widget_state(*, keep_keys: set | None = None) -> None:
    keep = keep_keys or set()
    for key in list(st.session_state.keys()):
        if key in keep:
            continue
        if key in _FAST_WIDGET_KEYS or any(key.startswith(prefix) for prefix in _FAST_WIDGET_PREFIXES):
            st.session_state.pop(key, None)


def _normalize_fast_analysis_pool_state() -> None:
    raw_pool = st.session_state.get("pool_codes") or []
    pool = []
    seen = set()
    for raw_code in raw_pool:
        code = str(raw_code or "").strip()
        if not code:
            continue
        try:
            code = _norm(code)
        except Exception:
            continue
        if code in seen:
            continue
        seen.add(code)
        pool.append(code)

    raw_checked = st.session_state.get("checked_codes") or set()
    checked = set()
    for raw_code in raw_checked:
        code = str(raw_code or "").strip()
        if not code:
            continue
        try:
            code = _norm(code)
        except Exception:
            continue
        if code in seen:
            checked.add(code)

    raw_sources = dict(st.session_state.get("pool_sources") or {})
    raw_names = dict(st.session_state.get("pool_names") or {})
    pool_sources = {}
    pool_names = {}
    for code in pool:
        pool_sources[code] = str(raw_sources.get(code) or "manual")
        pool_names[code] = (
            str(raw_names.get(code) or "").strip()
            or STOCK_NAME_MAP.get(code, "")
            or _catalog.lookup(code, market=st.session_state.get("iw_market", "A"))
            or code
        )

    st.session_state.pool_codes = pool
    st.session_state.checked_codes = checked
    st.session_state.pool_sources = pool_sources
    st.session_state.pool_names = pool_names


def _sync_fast_analysis_widget_state_from_session() -> None:
    pool = st.session_state.get("pool_codes") or []
    checked = set(st.session_state.get("checked_codes") or set())
    keep_keys = set()
    for code in pool:
        is_checked = code in checked
        cb_key = f"cb_{code}"
        cart_key = f"cart_keep_{code}"
        st.session_state[cb_key] = is_checked
        st.session_state[cart_key] = is_checked
        keep_keys.add(cb_key)
        keep_keys.add(cart_key)

    market = st.session_state.get("iw_market", "A")
    st.session_state["iw_market_radio"] = {
        "A": "🇨🇳 A股",
        "HK": "🇭🇰 港股",
        "US": "🇺🇸 美股",
    }.get(market, "🇨🇳 A股")
    st.session_state["sb_run_mode"] = st.session_state.get("run_mode", _MODES[0])
    st.session_state["main_run_mode"] = st.session_state.get("run_mode", _MODES[0])
    keep_keys.update({"iw_market_radio", "sb_run_mode", "main_run_mode"})
    _clear_fast_analysis_widget_state(keep_keys=keep_keys)


def _clear_pool_session_state() -> None:
    st.session_state.pool_codes = []
    st.session_state.checked_codes = set()
    st.session_state.pool_sources = {}
    st.session_state.pool_names = {}
    st.session_state.fuzzy_candidates = []
    st.session_state["iw_pending"] = None
    st.session_state["iw_commit_requested"] = False
    st.session_state["iw_should_focus"] = False
    st.session_state["iw_input_gen"] = st.session_state.get("iw_input_gen", 0) + 1
    _clear_fast_analysis_widget_state()


def _clear_fast_analysis_state() -> None:
    for key, value in _SS.items():
        st.session_state[key] = _copy_state_value(value)
    st.session_state["iw_commit_requested"] = False
    _clear_fast_analysis_widget_state()


_normalize_fast_analysis_pool_state()
if _fast_page_entered:
    _sync_fast_analysis_widget_state_from_session()

def _append_items(items: list) -> int:
    """将 StockItem 列表增量追加到代码池，返回实际新增数。"""
    pool     = st.session_state.pool_codes
    checked  = st.session_state.checked_codes
    sources  = st.session_state.pool_sources
    names    = st.session_state.pool_names
    pool_set = set(pool)
    added    = 0
    for it in items:
        code = str(it.get("code","")).strip()
        if not re.match(r"^\d{6}$", code) or not it.get("valid", True):
            continue
        code = _norm(code)
        name = STOCK_NAME_MAP.get(code) or str(it.get("name", "")).strip()
        if not name or name == code:
            continue
        if code not in pool_set:
            pool.append(code); pool_set.add(code); added += 1
        checked.add(code)
        sources[code] = it.get("source","text")
        names[code] = name
    st.session_state.pool_codes    = pool
    st.session_state.checked_codes = checked
    st.session_state.pool_sources  = sources
    st.session_state.pool_names    = names
    return added

def _remove_from_pool(code: str):
    pool = [c for c in st.session_state.pool_codes if c != code]
    st.session_state.pool_codes = pool
    st.session_state.checked_codes.discard(code)
    st.session_state.pool_sources.pop(code, None)
    st.session_state.pool_names.pop(code, None)


def _ingest_quick_pool_cache() -> int:
    cached = list_quick_pool()
    if not cached:
        return 0
    items = [
        {
            "code": item.get("code"),
            "name": item.get("name"),
            "valid": True,
            "source": "manual",
        }
        for item in cached
    ]
    added = _append_items(items)
    clear_quick_pool()
    return added

def _queue_fuzzy_candidates(raw_terms: list, source: str) -> int:
    pending = list(st.session_state.fuzzy_candidates)
    existing = {(item.get("raw",""), item.get("source","")) for item in pending}
    queued = 0
    for raw in raw_terms:
        token = str(raw or "").strip()
        if len(token) < 2:
            continue
        key = (token, source)
        if key in existing:
            continue
        options = suggest_stock_candidates(token, limit=5)
        if not options:
            continue
        pending.append({"raw": token, "source": source, "options": options})
        existing.add(key)
        queued += 1
    st.session_state.fuzzy_candidates = pending
    return queued

def _reset_fuzzy_state():
    st.session_state.fuzzy_candidates = []
    for key in list(st.session_state.keys()):
        if key.startswith("fuzzy_use_") or key.startswith("fuzzy_pick_"):
            del st.session_state[key]

def _extract_exact_code_items(raw_text: str, source: str) -> list:
    text = raw_text or ""
    codes = []
    for hit in re.findall(r"(?<!\d)(\d{6})(?!\d)", text):
        code = _norm(hit)
        if code not in codes:
            codes.append(code)
    return [
        {
            "code": code,
            "name": STOCK_NAME_MAP.get(code, ""),
            "valid": True,
            "source": source,
        }
        for code in codes
    ]

def _extract_local_text_items(raw_text: str, source: str) -> list:
    text = raw_text or ""
    merged = {}
    for item in _extract_exact_code_items(text, source):
        merged[item["code"]] = item

    stripped = re.sub(r"(?<!\d)\d{6}(?!\d)", "", text)
    pure_cn = re.sub(r"[^\u4e00-\u9fff]", "", stripped)
    if not pure_cn:
        return list(merged.values())

    remaining = pure_cn
    for name, code in _LOCAL_NAME_CODE_ITEMS:
        if len(name) < 2 or name not in remaining:
            continue
        merged.setdefault(
            code,
            {
                "code": code,
                "name": STOCK_NAME_MAP.get(code, name),
                "valid": True,
                "source": source,
            },
        )
        remaining = remaining.replace(name, " ")
    return list(merged.values())

def _extract_fuzzy_terms_from_text(raw_text: str) -> list:
    text = (raw_text or "").strip()
    if not text:
        return []
    hits = re.findall(r"[\u4e00-\u9fff]{2,8}", text)
    stopwords = {"帮我看","还有那个","顺便再看","这几只","图片识别","截图识别","股票代码","股票名称"}
    terms = []
    for hit in hits:
        if hit in stopwords:
            continue
        if hit not in terms:
            terms.append(hit)
    return terms

def _ingest_items(items: list, source: str, fallback_text: str = ""):
    good_items = _extract_local_text_items(fallback_text, source) if fallback_text else []
    good_map = {it["code"]: it for it in good_items}
    fuzzy_terms = []
    for it in items or []:
        code = str(it.get("code", "")).strip()
        name = str(it.get("name", "")).strip()
        valid = bool(it.get("valid", True))
        if re.match(r"^\d{6}$", code):
            cloned = dict(it)
            cloned["source"] = source
            if not cloned.get("name") and code in STOCK_NAME_MAP:
                cloned["name"] = STOCK_NAME_MAP[code]
            if code in good_map:
                if name and not good_map[code].get("name"):
                    good_map[code]["name"] = name
                continue
            good_items.append(cloned)
            good_map[code] = cloned
        else:
            fuzzy_terms.append(name or code)
    if fallback_text:
        for token in _extract_fuzzy_terms_from_text(fallback_text):
            if token not in fuzzy_terms:
                fuzzy_terms.append(token)
    added = _append_items(good_items) if good_items else 0
    queued = _queue_fuzzy_candidates(fuzzy_terms, source) if fuzzy_terms else 0
    return added, queued, len(good_items)

def _dedupe_items_against_pool(items: list) -> tuple[list, int]:
    existing_codes = {
        _norm(code)
        for code in (st.session_state.get("pool_codes") or [])
        if re.match(r"^\d{6}$", str(code or "").strip())
    }
    fresh_items = []
    staged_codes = set()
    skipped = 0
    for item in items or []:
        code = str(item.get("code", "")).strip()
        if re.match(r"^\d{6}$", code):
            code = _norm(code)
            if code in existing_codes or code in staged_codes:
                skipped += 1
                continue
            cloned = dict(item)
            cloned["code"] = code
            fresh_items.append(cloned)
            staged_codes.add(code)
        else:
            fresh_items.append(dict(item))
    return fresh_items, skipped

def _append_codes(codes: list, source: str = "manual") -> int:
    items = [{"name":c,"code":c,"valid":True,"source":source} for c in codes]
    return _append_items(items)


# =============================================================================
# Input Wizard：金融级实时输入精灵
# =============================================================================

# =============================================================================
# Input Wizard v2.0：金融级实时输入精灵
# 新增：@st.fragment 局部刷新 · Enter 键绑定 · n/40 计数器 · 一键清空
# =============================================================================

_IW_MAX_POOL  = 40   # 代码池容量上限
_IW_CHIP_COLS = 4    # 候选芯片每行数量


def _wizard_commit(code: str, name: str) -> None:
    """
    将股票加入代码池。
    ─────────────────────────────────────────────────────────────────────────
    安全原则：本函数必须在 _wizard_body() 的最顶部调用（任何 widget 渲染之前），
    因此可以安全地写 session_state 中的 widget key（iw_input_gen 轮转），
    不会触发 StreamlitAPIException。

    「输入框清空」机制：
      · 不直接写 iw_q（那会在 widget 已实例化后报错）
      · 改为递增 iw_input_gen，text_input 的 key 变为新值，Streamlit 视为
        全新 widget，自然以空字符串初始化 —— 实现清空，无任何副作用。
    """
    pool     = st.session_state.pool_codes
    checked  = st.session_state.checked_codes
    sources  = st.session_state.pool_sources
    names    = st.session_state.pool_names
    pool_set = set(pool)

    if code not in pool_set:
        pool.append(code)
    checked.add(code)
    sources[code] = "manual"
    names[code] = name or _catalog.lookup(code, market=st.session_state.get("iw_market", "A")) or code

    st.session_state.pool_codes    = pool
    st.session_state.checked_codes = checked
    st.session_state.pool_sources  = sources
    st.session_state.pool_names    = names

    # 轮转 key → 下次渲染时 text_input 是"新 widget"→ 自动空白，无异常
    st.session_state["iw_input_gen"] = st.session_state.get("iw_input_gen", 0) + 1
    st.session_state["iw_should_focus"] = True   # 通知 JS 回焦


def _is_manual_force_add_candidate(query: str, market: str) -> bool:
    q = (query or "").strip()
    if market == "A":
        return bool(re.fullmatch(r"\d{6}", q))
    if market == "HK":
        return bool(re.fullmatch(r"\d{5}", q))
    if market == "US":
        return bool(re.fullmatch(r"[A-Za-z]{1,5}", q))
    return False


def _searchbox_search(searchterm: str, market: str = "A") -> list:
    q = (searchterm or "").strip()
    st.session_state["iw_searchbox_result_count"] = 0
    st.session_state["iw_searchbox_single_exact"] = False
    if not _catalog_is_search_ready(market):
        return []
    if not q:
        return []

    query = q.upper() if market == "US" else q
    candidates = _catalog.search(query, market=market, limit=8)
    options = []
    seen_codes = set()

    for item in candidates:
        code = item["code"]
        name = item["name"]
        seen_codes.add(code)
        options.append(
            (
                f"{code} · {name}",
                {
                    "code": code,
                    "name": name,
                    "market": market,
                    "match_type": item.get("match_type", "catalog"),
                },
            )
        )

    exact_matches = [
        item for item in candidates
        if item.get("code") == query
        or str(item.get("name") or "").strip().upper() == query.upper()
    ]

    if _is_manual_force_add_candidate(query, market) and query not in seen_codes:
        fallback_name = _catalog.lookup(query, market=market) or query
        options.append(
            (
                f"直接添加 {query}（目录未收录）",
                {
                    "code": query,
                    "name": fallback_name,
                    "market": market,
                    "match_type": "force_add",
                },
            )
        )

    st.session_state["iw_searchbox_result_count"] = len(options)
    st.session_state["iw_searchbox_single_exact"] = len(exact_matches) == 1 and len(options) == 1

    return options


def _on_searchbox_submit(selected: dict | None) -> None:
    if not selected:
        return

    code = str(selected.get("code") or "").strip()
    if not code:
        return

    market = str(selected.get("market") or st.session_state.get("iw_market", "A")).strip() or "A"
    name = str(selected.get("name") or "").strip() or _catalog.lookup(code, market=market) or code
    st.session_state["iw_pending"] = (code, name)
    st.session_state["iw_commit_requested"] = True


_IW_CSS = """<style>
/* 候选芯片：等宽紧凑，JetBrains Mono */
[data-testid="stBaseButton-secondary"],
[data-testid="baseButton-secondary"] {
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 0.74rem !important;
    padding: 1px 8px !important;
    min-height: 26px !important;
    border-radius: 6px !important;
    line-height: 1.3 !important;
}
/* 市场 radio：去掉标签、缩小间距 */
[data-testid="stRadio"] > label { display: none !important; }
[data-testid="stRadio"] > div   { gap: 4px !important; flex-wrap: nowrap !important; }
</style>"""


def _iw_focus_js(should_focus: bool) -> str:
    """
    返回注入 components.html() 的 JS 字符串。

    功能：
      1. 定位 Input Wizard 文本框（通过 placeholder 特征字符串）
      2. 每次渲染后重新绑定 Enter 键 → 点击第一个候选芯片
      3. 当 should_focus=True 时，主动 focus() 并移光标到末尾
         （避免抢夺与 Wizard 无关区域的焦点）
    """
    focus_stmt = (
        "inp.focus();"
        "inp.setSelectionRange(inp.value.length, inp.value.length);"
    ) if should_focus else ""

    return f"""<script>
(function(){{
  'use strict';
  var doc = window.parent.document;

  /* ── 定位 Input Wizard 输入框 ─────────────────────────────────── */
  /* 策略：先找含 iw_q_ 前缀的 data-testid，再降级到 placeholder 特征 */
  function findInput(){{
    // 优先：Streamlit 给 text_input 的 label 元素上标注了 for 属性，
    // 其值等于 widget key；直接通过 key 前缀定位更稳定。
    var all = doc.querySelectorAll(
      '[data-testid="stTextInput"] input,' +
      '[data-baseweb="input"] input'
    );
    for(var i=0;i<all.length;i++){{
      var ph = all[i].placeholder || '';
      var id = all[i].id || '';
      // 匹配 key 前缀 iw_q_ 或 placeholder 含「代码」「Ticker」
      if(id.indexOf('iw_q_')>=0 ||
         ph.indexOf('\\u4ee3\\u7801')>=0 ||
         ph.indexOf('Ticker')>=0 ||
         ph.indexOf('\\u2713')>=0)
        return all[i];
    }}
    return null;
  }}

  /* ── 点击第一个候选芯片（或兜底"强制添加"按钮）──────────────── */
  function clickFirstChip(){{
    var sel =
      '[data-testid="stBaseButton-secondary"],' +
      '[data-testid="baseButton-secondary"]';
    var btns = doc.querySelectorAll(sel);
    var re = /\\d{{5,6}}\\s*[\\xB7\\u30FB\\u22C5\\uff65]/;
    for(var i=0;i<btns.length;i++){{
      if(re.test(btns[i].innerText||'')){{
        btns[i].click(); return true;
      }}
    }}
    // 降级：点击"强制添加"按钮
    for(var j=0;j<btns.length;j++){{
      if((btns[j].innerText||'').indexOf('\\u5f3a\\u5236')>=0){{  /* 强制 */
        btns[j].click(); return true;
      }}
    }}
    return false;
  }}

  /* ── 主逻辑（100ms 等 Streamlit DOM 渲染稳定）──────────────────── */
  setTimeout(function(){{
    var inp = findInput();
    if(!inp) return;

    inp.addEventListener('keydown', function(e){{
      if(e.key==='Enter'){{
        e.preventDefault();
        e.stopImmediatePropagation();
        clickFirstChip();
      }}
    }});

    // 条件回焦
    {focus_stmt}
  }}, 100);
}})();
</script>"""


def _iw_searchbox_js(should_focus: bool, auto_submit_single: bool) -> str:
    focus_stmt = (
        "inp.focus();"
        "if (typeof inp.setSelectionRange === 'function') {"
        "  var len = (inp.value || '').length;"
        "  inp.setSelectionRange(len, len);"
        "}"
    ) if should_focus else ""
    single_submit = "true" if auto_submit_single else "false"

    return f"""<script>
(function(){{
  'use strict';
  var doc = window.parent.document;
  var autoSubmitSingle = {single_submit};

  function visible(el){{
    if(!el) return false;
    var style = window.parent.getComputedStyle(el);
    return style.display !== 'none' && style.visibility !== 'hidden' && el.offsetParent !== null;
  }}

  function findInput(){{
    var selectors = [
      'input[id*="iw_search_"]',
      'input[role="combobox"]',
      'input[aria-autocomplete="list"]',
      '[data-baseweb="select"] input',
      '[data-baseweb="input"] input'
    ];
    for(var s=0; s<selectors.length; s++){{
      var all = doc.querySelectorAll(selectors[s]);
      for(var i=0; i<all.length; i++){{
        var el = all[i];
        var ph = (el.getAttribute('placeholder') || '');
        var id = (el.id || '');
        if(
          id.indexOf('iw_search_') >= 0 ||
          ph.indexOf('\\u4ee3\\u7801') >= 0 ||
          ph.indexOf('Ticker') >= 0 ||
          ph.indexOf('\\u62fc\\u97f3') >= 0
        ){{
          return el;
        }}
      }}
    }}
    return null;
  }}

  function listOptions(){{
    var selectors = [
      '[role="option"]',
      '[id*="react-select"][id*="-option-"]',
      '[class*="option"]'
    ];
    for(var s=0; s<selectors.length; s++){{
      var nodes = Array.prototype.slice.call(doc.querySelectorAll(selectors[s]))
        .filter(visible)
        .filter(function(el){{
          return !!(el.innerText || '').trim();
        }});
      if(nodes.length) return nodes;
    }}
    return [];
  }}

  function clickSingleVisibleOption(){{
    var options = listOptions();
    if(options.length !== 1) return false;
    options[0].dispatchEvent(new MouseEvent('mousedown', {{ bubbles: true }}));
    options[0].click();
    return true;
  }}

  function bindEnter(inp){{
    if(!inp || inp.dataset.iwSearchboxBound === '1') return;
    inp.dataset.iwSearchboxBound = '1';
    inp.addEventListener('keydown', function(e){{
      if(e.key !== 'Enter') return;
      if(!autoSubmitSingle) return;
      window.parent.setTimeout(function(){{
        clickSingleVisibleOption();
      }}, 0);
    }}, true);
  }}

  window.parent.setTimeout(function(){{
    var inp = findInput();
    if(!inp) return;
    bindEnter(inp);
    {focus_stmt}
  }}, 120);
}})();
</script>"""


def _wizard_body() -> None:
    """
    Input Wizard UI 主体逻辑。
    被 _render_input_wizard（fragment 包装版）调用。
    """
    st.markdown(_IW_CSS, unsafe_allow_html=True)

    # ── Session state 初始化 ──────────────────────────────────────────────
    for _k, _v in [
        ("iw_q", ""),
        ("iw_should_focus", False),
        ("iw_pending", None),
        ("iw_commit_requested", False),
    ]:
        if _k not in st.session_state:
            st.session_state[_k] = _v

    # ── 处理上一次的 pending（必须在任何 widget 渲染前执行）──────────────
    pending = st.session_state.get("iw_pending")
    if pending:
        _wizard_commit(pending[0], pending[1])
        st.session_state["iw_pending"] = None
        st.toast(f"✅ {pending[1]}（{pending[0]}）已入池")

    # ── 市场选择器 + 目录状态 ─────────────────────────────────────────────
    # index= 从 iw_market session state 反向映射，确保跨页导航后选项与逻辑值对齐
    _MW_OPTS     = ["🇨🇳 A股", "🇭🇰 港股", "🇺🇸 美股"]
    _MW_CODE_IDX = {"A": 0, "HK": 1, "US": 2}
    mkt_col, stat_col = st.columns([3.5, 5])
    with mkt_col:
        mkt_display = st.radio(
            "市场",
            _MW_OPTS,
            horizontal=True,
            index=_MW_CODE_IDX.get(st.session_state.get("iw_market", "A"), 0),
            key="iw_market_radio",
            label_visibility="collapsed",
        )
    market = {"🇨🇳 A股": "A", "🇭🇰 港股": "HK", "🇺🇸 美股": "US"}.get(mkt_display, "A")
    st.session_state["iw_market"] = market
    with stat_col:
        st.markdown(
            f"<span style='font-size:11px;color:#8A9BAE;line-height:2.8'>"
            f"{_catalog.status_badge(market)}</span>",
            unsafe_allow_html=True,
        )

    # ── 输入行：[文本框·大] [计数徽章·小] [🗑️清空·极小] ────────────────
    pool_n = len(st.session_state.pool_codes)
    locked = pool_n >= _IW_MAX_POOL

    placeholder_map = {
        "A":  "代码 · 名称 · 拼音首字母  (⏎ 确认第一条)",
        "HK": "代码 / 名称  如：00700 · 腾讯控股",
        "US": "Ticker / 名称  如：AAPL · Apple Inc",
    }
    inp_col, cnt_col, clr_col = st.columns([7.0, 1.5, 0.8])

    with inp_col:
        # key 含 iw_input_gen：每次 _wizard_commit 递增该值，
        # Streamlit 将其视为全新 widget（默认空字符串），不写已有 widget 的 state。
        _iw_key = f"iw_q_{st.session_state.get('iw_input_gen', 0)}"
        q: str = st.text_input(
            "iw_input_label",
            placeholder=(
                "⛔ 代码池已满 40 只，请先运行或删除后继续"
                if locked else
                placeholder_map.get(market, "")
            ),
            key=_iw_key,
            label_visibility="collapsed",
            disabled=locked,
        )
        q = q.strip()

    with cnt_col:
        if pool_n >= _IW_MAX_POOL:
            badge_color, badge_icon = "#ef4444", "⛔"
            badge_weight = "700"
        elif pool_n >= 30:
            badge_color, badge_icon = "#f59e0b", "⚠️"
            badge_weight = "600"
        else:
            badge_color, badge_icon = "#6b7280", "📊"
            badge_weight = "400"
        st.markdown(
            f"<div style='text-align:center;padding-top:7px;"
            f"font-size:11px;font-family:JetBrains Mono,monospace;"
            f"color:{badge_color};font-weight:{badge_weight}'>"
            f"{badge_icon} {pool_n}/{_IW_MAX_POOL}</div>",
            unsafe_allow_html=True,
        )

    with clr_col:
        if st.button(
            "🗑️",
            key="iw_btn_clear_pool",
            help="一键清空整个代码池（不可撤销）",
            use_container_width=True,
            type="secondary",
        ):
            _clear_pool_session_state()
            st.rerun()

    # ── 候选芯片区 ────────────────────────────────────────────────────────
    if q and not locked:
        candidates = _catalog.search(q, market=market, limit=8)

        if candidates:
            for row_start in range(0, len(candidates), _IW_CHIP_COLS):
                row_cands = candidates[row_start: row_start + _IW_CHIP_COLS]
                chip_cols = st.columns(len(row_cands))
                for j, c in enumerate(row_cands):
                    code = c["code"]
                    name = c["name"]
                    short = name[:8] + "…" if len(name) > 8 else name
                    # 第一个芯片标注 ⏎ 提示 Enter 会选它
                    prefix = "⏎ " if (row_start == 0 and j == 0) else ""
                    label  = f"{prefix}{code} · {short}"
                    with chip_cols[j]:
                        if st.button(
                            label,
                            key=f"iw_chip_{code}_{market}",
                            use_container_width=True,
                            type="secondary",
                            help=f"{name}（{code}）\n点击或按 Enter 加入代码池",
                        ):
                            # 只写 pending，绝不写已实例化的 widget key
                            # _wizard_commit 在下次 rerun 最顶部执行，彼时 widget 尚未渲染
                            st.session_state["iw_pending"] = (code, name)
                            st.rerun()
        else:
            # ── 兜底：格式合法但 catalog 无记录（如新上市股票）──────────
            is_valid_a  = bool(re.match(r"^\d{6}$", q))
            is_valid_hk = bool(re.match(r"^\d{5}$", q)) and market == "HK"
            is_valid_us = bool(re.match(r"^[A-Za-z]{1,5}$", q)) and market == "US"
            if is_valid_a or is_valid_hk or is_valid_us:
                fa_col, _ = st.columns([3, 5])
                with fa_col:
                    if st.button(
                        f"⚡ 强制添加 {q}（目录未收录）",
                        key="iw_force_add",
                        type="secondary",
                        use_container_width=True,
                        help="代码格式合法，可能是新上市或目录尚未热更新",
                    ):
                        fb_name = STOCK_NAME_MAP.get(q, q)
                        st.session_state["iw_pending"] = (q, fb_name)
                        st.rerun()
            elif q:
                st.caption(
                    f"💡 「{q}」暂无匹配 — "
                    f"目录：{_catalog.status_badge(market)}"
                )

    # ── JS 注入：Enter 键绑定 + 条件回焦（每次渲染都执行）────────────────
    _should_focus = bool(st.session_state.get("iw_should_focus", False))
    if _should_focus:
        st.session_state["iw_should_focus"] = False  # 单次触发，立即重置
    components.html(_iw_focus_js(_should_focus), height=0, scrolling=False)


# ── fragment 包装（Streamlit ≥ 1.37 自动局部刷新，旧版回退普通函数）─────────
# 原理：
#   · 用户打字 → 只有 wizard 区域 rerun（约 50ms），页面其余不动
#   · 芯片点击 / 清空按钮 → 调用 st.rerun() → 触发全页 rerun（代码池同步更新）
#   · JS 在每次渲染后自动重新绑定 Enter 键 + 在全页 rerun 后回焦输入框
def _wizard_body_searchbox() -> None:
    """Searchbox-based Input Wizard with pooled state updates."""
    st.markdown(_IW_CSS, unsafe_allow_html=True)

    for _k, _v in [
        ("iw_q", ""),
        ("iw_should_focus", False),
        ("iw_pending", None),
        ("iw_commit_requested", False),
    ]:
        if _k not in st.session_state:
            st.session_state[_k] = _v

    pending = st.session_state.get("iw_pending")
    if pending:
        _wizard_commit(pending[0], pending[1])
        st.session_state["iw_pending"] = None
        st.toast(f"✅ {pending[1]}（{pending[0]}）已入池")

    # index= 从 iw_market session state 反向映射，确保跨页导航后选项与逻辑值对齐
    _MW_OPTS     = ["🇨🇳 A股", "🇭🇰 港股", "🇺🇸 美股"]
    _MW_CODE_IDX = {"A": 0, "HK": 1, "US": 2}
    mkt_col, stat_col = st.columns([3.5, 5])
    with mkt_col:
        mkt_display = st.radio(
            "市场",
            _MW_OPTS,
            horizontal=True,
            index=_MW_CODE_IDX.get(st.session_state.get("iw_market", "A"), 0),
            key="iw_market_radio",
            label_visibility="collapsed",
        )
    market = {"🇨🇳 A股": "A", "🇭🇰 港股": "HK", "🇺🇸 美股": "US"}.get(mkt_display, "A")
    st.session_state["iw_market"] = market
    with stat_col:
        st.markdown(
            f"<span style='font-size:11px;color:#8A9BAE;line-height:2.8'>"
            f"{_catalog.status_badge(market)}</span>",
            unsafe_allow_html=True,
        )

    pool_n = len(st.session_state.pool_codes)
    locked = pool_n >= _IW_MAX_POOL
    placeholder_map = {
        "A": "代码 / 名称 / 拼音首字母",
        "HK": "代码 / 名称 例如 00700 / 腾讯控股",
        "US": "Ticker / 名称 例如 AAPL / Apple Inc",
    }
    inp_col, cnt_col, clr_col = st.columns([7.0, 1.5, 0.8])

    with inp_col:
        if locked:
            st.text_input(
                "iw_input_label",
                value="",
                placeholder="🚫 代码池已满 40 只，请先运行或删除后继续",
                key="iw_searchbox_locked",
                label_visibility="collapsed",
                disabled=True,
            )
        elif not _catalog_is_search_ready(market):
            with st.spinner("正在载入本地股票雷达快照..."):
                st.text_input(
                    "iw_input_loading",
                    value="",
                    placeholder="正在载入本地股票雷达快照...",
                    key="iw_searchbox_loading",
                    label_visibility="collapsed",
                    disabled=True,
                )
            if _catalog.error(market):
                st.warning(_catalog.error(market))
        else:
            _iw_key = f"iw_search_{st.session_state.get('iw_input_gen', 0)}"
            st_searchbox(
                _searchbox_search,
                market=market,
                key=_iw_key,
                label=None,
                placeholder=placeholder_map.get(market, ""),
                clear_on_submit=True,
                debounce=120,
                rerun_scope="fragment",
                submit_function=_on_searchbox_submit,
            )
            st.caption("输入代码、名称或拼音首字母，选中后会自动加入代码池。")

    with cnt_col:
        if pool_n >= _IW_MAX_POOL:
            badge_color, badge_icon = "#ef4444", "🚫"
            badge_weight = "700"
        elif pool_n >= 30:
            badge_color, badge_icon = "#f59e0b", "⚠️"
            badge_weight = "600"
        else:
            badge_color, badge_icon = "#6b7280", "📳"
            badge_weight = "400"
        st.markdown(
            f"<div style='text-align:center;padding-top:7px;"
            f"font-size:11px;font-family:JetBrains Mono,monospace;"
            f"color:{badge_color};font-weight:{badge_weight}'>"
            f"{badge_icon} {pool_n}/{_IW_MAX_POOL}</div>",
            unsafe_allow_html=True,
        )

    with clr_col:
        if st.button(
            "🗏",
            key="iw_btn_clear_pool",
            help="一键清空整个代码池（不可撤销）",
            use_container_width=True,
            type="secondary",
        ):
            _clear_pool_session_state()
            st.rerun()

    if st.session_state.get("iw_commit_requested"):
        st.session_state["iw_commit_requested"] = False
        st.rerun()

    _should_focus = bool(st.session_state.get("iw_should_focus", False))
    if _should_focus:
        st.session_state["iw_should_focus"] = False
    components.html(
        _iw_searchbox_js(
            should_focus=_should_focus,
            auto_submit_single=bool(st.session_state.get("iw_searchbox_single_exact", False)),
        ),
        height=0,
        scrolling=False,
    )


_render_input_wizard = getattr(st, "fragment", lambda fn: fn)(_wizard_body_searchbox)

def _source_label(source: str) -> str:
    return {
        "text": "[A]",
        "image": "[B]",
        "voice": "[C]",
        "manual": "[M]",
        "fuzzy": "[F]",
    }.get(source, "[M]")

def _source_badge_html(source: str) -> str:
    css = source if source in {"text", "image", "voice", "manual", "fuzzy"} else "manual"
    return f'<span class="src-badge {css}">{_source_label(source)}</span>'

def _source_group_header_html(source: str, count: int) -> str:
    title = {
        "text": "文字导入组",
        "image": "图片导入组",
        "voice": "语音导入组",
        "manual": "手动添加组",
        "fuzzy": "模糊确认组",
    }.get(source, "手动添加组")
    css = source if source in {"text", "image", "voice", "manual", "fuzzy"} else "manual"
    return (
        f'<div class="source-group-header {css}">'
        f'<span class="group-badge">{_source_label(source)}</span>'
        f'<span>{title}</span>'
        f'<span style="color:var(--text-muted)">共 {count} 只</span>'
        f'</div>'
    )

def _resolved_name(code: str) -> str:
    return st.session_state.pool_names.get(code, "") or STOCK_NAME_MAP.get(code, "")

def _display_name(code: str) -> str:
    name = _resolved_name(code)
    return f"{code} {name}".strip()

def _source_icon(code: str) -> str:
    s = st.session_state.pool_sources.get(code, "manual")
    return {"text":"📝","image":"🖼️","voice":"🎤","manual":"⌨️","fuzzy":"⚠️"}.get(s,"⌨️")

def _normalize_uploaded_images(files) -> list:
    normalized = []
    for idx, file_obj in enumerate(files or [], 1):
        if hasattr(file_obj, "seek"):
            file_obj.seek(0)
        raw = file_obj.read() if hasattr(file_obj, "read") else bytes(file_obj)
        if not raw:
            continue
        out = BytesIO(raw)
        out.name = getattr(file_obj, "name", f"upload-{idx}.png")
        try:
            from PIL import Image
            src = BytesIO(raw)
            with Image.open(src) as img:
                fmt = "PNG" if (img.mode in ("RGBA", "LA", "P")) else "JPEG"
                converted = img.convert("RGBA" if fmt == "PNG" else "RGB")
                encoded = BytesIO()
                converted.save(encoded, format=fmt)
                encoded.seek(0)
                base = os.path.splitext(out.name)[0]
                encoded.name = f"{base}.{fmt.lower()}"
                normalized.append(encoded)
                continue
        except Exception:
            pass
        out.seek(0)
        normalized.append(out)
    return normalized

def _get_final() -> List[str]:
    checked = st.session_state.checked_codes
    return [c for c in st.session_state.pool_codes if c in checked]

# =============================================================================
# 评分
# =============================================================================
def _dc(s):
    return "inverse" if s>=65 else ("normal" if s<=40 else "off")
def _sl(s):
    if s>=75: return "强烈看多"
    if s>=65: return "偏多"
    if s>=50: return "观望"
    if s>=40: return "偏空"
    return "看空"

def _tone(s):
    if s >= 65:
        return "bull"
    if s <= 40:
        return "bear"
    return "neutral"

def _render_result_metric(container, r):
    score = getattr(r, "sentiment_score", 50)
    advice = getattr(r, "operation_advice", "-")
    tone = _tone(score)
    tone_label = _sl(score)
    trend = getattr(r, "trend_prediction", "") or "待补充"
    container.markdown(
        f"""
<div class="result-card">
  <div class="result-name">{r.name}（{r.code}）</div>
  <div class="result-advice {tone}">{advice}</div>
  <div class="result-score">
    <span class="{tone}">情绪评分 {score} · {tone_label}</span><br/>
    趋势判断：{trend}
  </div>
</div>
""",
        unsafe_allow_html=True,
    )

def _read_physical_log(log_prefix: str, debug: bool = False, lines: int = 1000):
    log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "logs"))
    if not os.path.isdir(log_dir):
        return None, b"", ""
    try:
        entries = [
            os.path.abspath(os.path.join(log_dir, name))
            for name in os.listdir(log_dir)
            if name.lower().endswith(".log")
        ]
    except Exception:
        return None, b"", ""
    if debug:
        candidates = [p for p in entries if "debug" in os.path.basename(p).lower()]
    else:
        candidates = [p for p in entries if "debug" not in os.path.basename(p).lower()]
    candidates = [p for p in candidates if os.path.isfile(p)]
    path = max(candidates, key=os.path.getmtime) if candidates else None
    if not path:
        return None, b"", ""
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            tail_lines = deque(fh, maxlen=lines)
    except Exception:
        return path, b"", ""
    tail = "".join(tail_lines)
    return path, tail.encode("utf-8"), tail


def _plain_text_report(value: str) -> str:
    return markdown_to_plain_text(value or "").strip()


def _report_filename(prefix: str, slug: str) -> str:
    return f"{prefix}_{slug}.txt"


def _render_text_preview(title: str, content: str, key: str, height: int = 260) -> None:
    with st.expander(title, expanded=False):
        st.text_area("", value=content or "No report content", height=height, disabled=True, key=key)


def _persist_run_artifacts(run_id: str, run_mode: str) -> None:
    """
    持久化本次分析批次的完整产物到 run_artifacts 表。
    修复要点：
      1. 报告保存原始 Markdown（不再经 _plain_text_report 转换）
      2. schema_json 安全序列化（防止 Pydantic/非序列化对象导致静默崩溃）
      3. 日志文件分两路读取，debug 路无独立文件时降级为主日志
      4. 每步独立 try-except，任何单步失败不影响其余步骤
    """
    if not run_id:
        return

    # ── 步骤 1：收集原始 Markdown 报告 ────────────────────────────────────────
    market_report_md = ""
    stock_report_md  = ""
    full_report_md   = ""
    try:
        market_report_md = (st.session_state.get("market_report", "") or "").strip()
        full_report_md   = (st.session_state.get("analysis_report", "") or "").strip()
        per_reports      = st.session_state.get("per_stock_reports", {}) or {}
        pool_names       = st.session_state.get("pool_names", {}) or {}
        stock_parts = []
        for code, report_md in per_reports.items():
            if not report_md:
                continue
            display_name = pool_names.get(code, code)
            stock_parts.append(f"## {display_name}（{code}）\n\n{report_md}")
        stock_report_md = "\n\n---\n\n".join(stock_parts)
        # 全量兜底：若 analysis_report 为空，则拼接大盘+个股
        if not full_report_md and (market_report_md or stock_report_md):
            segs = []
            if market_report_md:
                segs.append(f"# 大盘报告\n\n{market_report_md}")
            if stock_report_md:
                segs.append(f"# 个股报告\n\n{stock_report_md}")
            full_report_md = "\n\n---\n\n".join(segs)
    except Exception as exc:
        logger.warning("_persist_run_artifacts 报告收集失败：%s", exc)

    # ── 步骤 2：读取物理日志文件 ──────────────────────────────────────────────
    business_log_tail = ""
    debug_log_tail    = ""
    try:
        _, _, business_log_tail = _read_physical_log("webui_v8", debug=False)
        _, _, debug_log_tail    = _read_physical_log("webui_v8", debug=True)
        # 无独立 debug 文件时，从主日志提取 WARNING/ERROR 条目作为 debug 视图
        if not debug_log_tail and business_log_tail:
            debug_lines = [
                ln for ln in business_log_tail.splitlines()
                if any(tag in ln for tag in ("WARNING", "ERROR", "CRITICAL", "DEBUG"))
            ]
            debug_log_tail = "\n".join(debug_lines)
        # debug 仍为空（非 Debug 模式 + 无高级别日志）→ 写入默认说明，禁止空值入库
        if not debug_log_tail:
            debug_log_tail = "非 Debug 模式运行，无底层通信日志记录。"
    except Exception as exc:
        logger.warning("_persist_run_artifacts 日志读取失败：%s", exc)

    # ── 步骤 3：安全序列化 schema_json ────────────────────────────────────────
    schema_json = "{}"
    try:
        schema_payload: dict = {
            "run_id":       run_id,
            "run_mode":     run_mode,
            "run_ts":       st.session_state.get("run_ts", ""),
            "snapshot_ids": st.session_state.get("snapshot_ids", {}),
        }
        # default=str 兜底：任何非序列化对象（Pydantic / datetime 等）一律转 str
        schema_json = json.dumps(schema_payload, ensure_ascii=False, indent=2,
                                 default=str)
    except Exception as exc:
        logger.warning("_persist_run_artifacts schema 序列化失败：%s", exc)
        schema_json = json.dumps({"run_id": run_id, "run_mode": run_mode},
                                 ensure_ascii=False, default=str)

    # ── 步骤 4：写入数据库 ────────────────────────────────────────────────────
    try:
        save_run_artifacts(
            run_id,
            run_mode=run_mode,
            market_report_md=market_report_md,
            stock_report_md=stock_report_md,
            full_report_md=full_report_md,
            business_log=business_log_tail or "",
            debug_log=debug_log_tail or "",
            schema_json=schema_json,
        )
        logger.info(
            "run_artifacts 已写入：run_id=%s | market=%d chars | stock=%d chars | bizlog=%d chars",
            run_id, len(market_report_md), len(stock_report_md),
            len(business_log_tail or ""),
        )
    except Exception as exc:
        logger.warning("save_run_artifacts 写入失败：%s", exc, exc_info=True)
def _get_rt():
    from src.enums import ReportType
    cfg=get_config()
    return {"brief":ReportType.BRIEF,"full":ReportType.FULL}.get(
        getattr(cfg,"report_type","simple").lower(), ReportType.SIMPLE)
def _ts(): return datetime.now().strftime("[%H:%M:%S]")

def _wizard_summary():
    pool = st.session_state.pool_codes
    checked = st.session_state.checked_codes
    final_count = len([c for c in pool if c in checked])
    run_count = len(st.session_state.analysis_results)
    report_ready = bool(st.session_state.analysis_report or st.session_state.market_report)
    st.markdown(f"""
<div class="wizard-shell">
  <div class="wizard-step is-active">
    <div class="wizard-icon">1</div>
    <div class="wizard-body">
      <div class="wizard-title">Step 1 · 输入区</div>
      <div class="wizard-sub">文本 / 图片 / 语音三合一入口，图片支持 Ctrl+V 粘贴桥接，单次软上限 {_MAX_WARN} 只。</div>
    </div>
  </div>
  <div class="wizard-step {'is-done' if final_count else 'is-idle'}">
    <div class="wizard-icon">2</div>
    <div class="wizard-body">
      <div class="wizard-title">Step 2 · 代码池</div>
      <div class="wizard-sub">统一进入待选池，当前已入池 {len(pool)} 只，已勾选 {final_count} 只。</div>
    </div>
  </div>
  <div class="wizard-step {'is-done' if st.session_state.elapsed_sec > 0 else 'is-idle'}">
    <div class="wizard-icon">3</div>
    <div class="wizard-body">
      <div class="wizard-title">Step 3 · 分析进度</div>
      <div class="wizard-sub">本轮先保留现有稳定引擎；Phase 2 再接静默分批与双进度条，当前已完成 {run_count} 只。</div>
    </div>
  </div>
  <div class="wizard-step {'is-done' if report_ready else 'is-idle'}">
    <div class="wizard-icon">4</div>
    <div class="wizard-body">
      <div class="wizard-title">Step 4 · 报告</div>
      <div class="wizard-sub">Phase 1 先保留当前报告渲染出口；Phase 4 再切到切片阅读与过滤标签。</div>
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

def _render_clipboard_bridge(max_files: int = 30):
    components.html(f"""
<div id="duka-paste-zone" style="
  border:1.5px dashed #94a3b8;
  border-radius:14px;
  padding:18px 16px;
  background:rgba(59,130,246,0.06);
  color:#1f2937;
  font-family:'Microsoft YaHei','PingFang SC',sans-serif;
">
  <div style="font-size:15px;font-weight:700;margin-bottom:6px;">📋 Ctrl+V / ⌘+V 直接粘贴截图到这里</div>
  <div style="font-size:12px;line-height:1.6;color:#475569;">
    粘贴成功后，图片会被自动写入下方上传框。若浏览器阻止注入，请改用拖拽或点击上传。
  </div>
  <div id="duka-paste-status" style="margin-top:8px;font-size:12px;color:#0f766e;">等待剪贴板图片…</div>
</div>
<script>
const box = document.getElementById("duka-paste-zone");
const status = document.getElementById("duka-paste-status");
function findUploader() {{
  const parentDoc = window.parent.document;
  return parentDoc.querySelector('input[type="file"][multiple]');
}}
function updateStatus(msg, color) {{
  status.innerText = msg;
  if (color) status.style.color = color;
}}
async function handlePaste(event) {{
  const items = event.clipboardData && event.clipboardData.items ? Array.from(event.clipboardData.items) : [];
  const imgItem = items.find(item => item.type && item.type.startsWith("image/"));
  if (!imgItem) {{
    updateStatus("未检测到图片，继续支持拖拽或点击上传。", "#64748b");
    return;
  }}
  const input = findUploader();
  if (!input) {{
    updateStatus("未找到上传框，请稍后重试。", "#b91c1c");
    return;
  }}
  const blob = imgItem.getAsFile();
  if (!blob) {{
    updateStatus("剪贴板图片读取失败，请改用上传。", "#b91c1c");
    return;
  }}
  const dt = new DataTransfer();
  const current = input.files ? Array.from(input.files) : [];
  current.slice(0, {max_files - 1}).forEach(file => dt.items.add(file));
  const ext = (blob.type || "image/png").split("/").pop().replace("jpeg", "jpg");
  const pastedFile = new File([blob], `clipboard-${{Date.now()}}.${{ext}}`, {{ type: blob.type || "image/png" }});
  dt.items.add(pastedFile);
  input.files = dt.files;
  input.dispatchEvent(new Event("change", {{ bubbles: true }}));
  updateStatus(`已写入上传框：${{Math.min(dt.files.length, {max_files})}} / {max_files} 张`, "#15803d");
}}
window.addEventListener("paste", handlePaste);
box.addEventListener("click", () => updateStatus("焦点已就绪，现在直接按 Ctrl+V / ⌘+V。", "#2563eb"));
</script>
""", height=150)


# =============================================================================
# 核心分析函数（真实逻辑 + Live Timer）
# =============================================================================
def _run_stock_analysis(codes, sc, run_id, run_mode, timer_ph):
    t0=time.time()
    ts=datetime.now().strftime("%Y-%m-%d %H:%M")
    st.session_state.run_ts           = ts
    st.session_state.analysis_results = []
    st.session_state.per_stock_reports= {}
    st.session_state.analysis_report  = ""
    st.session_state.last_error       = ""

    live_timer_ph = timer_ph if timer_ph is not None else st.empty()
    stock_timeout_sec = 90

    def _tick():
        e=time.time()-t0
        live_timer_ph.markdown(
            f'<span class="elapsed-pill">⏰ 已耗时 {e:.1f}s</span>',
            unsafe_allow_html=True
        )

    def _mk_payload(code, name, result=None, full_md="", score=None,
                    advice="-", fetch_ok=False, fetch_error="",
                    error="", elapsed=0.0):
        return {
            "code": code,
            "name": name,
            "result": result,
            "full_md": full_md,
            "score": score,
            "advice": advice,
            "fetch_ok": fetch_ok,
            "fetch_error": fetch_error,
            "error": error,
            "elapsed": elapsed,
        }

    try:
        if not codes:
            st.session_state.last_error="未检测到可分析的股票代码。"
            return
        cfg=get_config()
        sc.write(f"{_ts()} 初始化分析引擎…"); _tick()
        rt=_get_rt()
        max_workers=min(5, len(codes))
        pipeline=StockAnalysisPipeline(config=cfg,max_workers=1,
            query_id=run_id,query_source="webui",save_context_snapshot=False)
        task_queue = queue.Queue()
        result_queue = queue.Queue()
        stop_event = threading.Event()
        pause_event = threading.Event()
        for idx, code in enumerate(codes, 1):
            task_queue.put((idx, code))
        sc.write(f"{_ts()} 任务队列：**{', '.join(codes)}**（共 {len(codes)} 只）"); _tick()

        def _process_single_stock(code):
            ts_s=time.time()
            worker=StockAnalysisPipeline(
                config=cfg,max_workers=1,
                query_id=run_id,query_source="webui",
                save_context_snapshot=False
            )
            try:
                name=worker.fetcher_manager.get_stock_name(code) or code
            except Exception:
                name=code

            ok=False
            err=""
            try:
                ok,err=worker.fetch_and_save_stock_data(code)
            except Exception as exc:
                err=str(exc)
                logger.exception(f"{code} fetch_and_save_stock_data")

            try:
                result=worker.analyze_stock(
                    code=code,report_type=rt,query_id=uuid.uuid4().hex
                )
            except Exception as exc:
                logger.exception(f"{code} analyze_stock")
                return {
                    "code": code,
                    "name": name,
                    "result": None,
                    "full_md": "",
                    "score": None,
                    "advice": "-",
                    "fetch_ok": ok,
                    "fetch_error": err,
                    "error": str(exc),
                    "elapsed": time.time()-ts_s,
                }

            if not result:
                return {
                    "code": code,
                    "name": name,
                    "result": None,
                    "full_md": "",
                    "score": None,
                    "advice": "-",
                    "fetch_ok": ok,
                    "fetch_error": err,
                    "error": "",
                    "elapsed": time.time()-ts_s,
                }

            try:
                full_md=worker.notifier.generate_single_stock_report(result)
            except Exception:
                full_md=_fb_report(result)

            return {
                "code": code,
                "name": getattr(result, "name", name),
                "result": result,
                "full_md": full_md,
                "score": getattr(result,"sentiment_score",50),
                "advice": getattr(result,"operation_advice","-"),
                "fetch_ok": ok,
                "fetch_error": err,
                "error": "",
                "elapsed": time.time()-ts_s,
            }

        def _run_single_with_timeout(code):
            started_at = time.time()
            holder = {}
            finished = threading.Event()

            def _target():
                try:
                    holder["payload"] = _process_single_stock(code)
                except Exception as exc:
                    logger.exception(f"{code} background_worker")
                    holder["payload"] = _mk_payload(
                        code=code,
                        name=code,
                        error=str(exc),
                        elapsed=time.time()-started_at,
                    )
                finally:
                    finished.set()

            thread = threading.Thread(
                target=_target,
                name=f"stock-task-{code}",
                daemon=True,
            )
            thread.start()

            while True:
                if finished.wait(timeout=0.2):
                    return holder.get("payload", _mk_payload(
                        code=code, name=code, error="任务未返回结果",
                        elapsed=time.time()-started_at,
                    ))
                if stop_event.is_set():
                    return _mk_payload(
                        code=code,
                        name=code,
                        error="用户手动停止，当前任务已中断回收",
                        elapsed=time.time()-started_at,
                    )
                if time.time()-started_at >= stock_timeout_sec:
                    return _mk_payload(
                        code=code,
                        name=code,
                        error=f"单股分析超时（>{stock_timeout_sec}s）",
                        elapsed=time.time()-started_at,
                    )

        def _worker_loop():
            while True:
                if stop_event.is_set():
                    return
                while pause_event.is_set() and not stop_event.is_set():
                    time.sleep(0.2)
                if stop_event.is_set():
                    return
                try:
                    idx, code = task_queue.get(timeout=0.2)
                except queue.Empty:
                    return
                try:
                    if stop_event.is_set():
                        payload = _mk_payload(
                            code=code,
                            name=code,
                            error="用户手动停止，未开始执行",
                            elapsed=0.0,
                        )
                    else:
                        payload = _run_single_with_timeout(code)
                    result_queue.put((idx, payload))
                finally:
                    task_queue.task_done()

        completed_payloads = {}
        last_pause_state = False
        stop_announced = False
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            worker_futures = {
                executor.submit(_worker_loop): worker_id
                for worker_id in range(max_workers)
            }
            while True:
                if st.session_state.stop_flag and not stop_event.is_set():
                    stop_event.set()
                if st.session_state.pause_flag:
                    pause_event.set()
                else:
                    pause_event.clear()

                if pause_event.is_set() != last_pause_state:
                    last_pause_state = pause_event.is_set()
                    sc.write(f"{_ts()} {'⏸ 已暂停新任务派发' if last_pause_state else '▶️ 已恢复任务派发'}")

                if stop_event.is_set() and not stop_announced:
                    stop_announced = True
                    sc.write(f"{_ts()} ⏹ 已收到彻底停止指令，未开始任务将被跳过，已完成部分会继续汇总")

                _tick()
                while True:
                    try:
                        idx, payload = result_queue.get_nowait()
                    except queue.Empty:
                        break

                    code = payload["code"]
                    pfx=f"[{idx}/{len(codes)}]"
                    completed_payloads[idx]=payload
                    if payload["fetch_error"] and not payload["fetch_ok"]:
                        sc.write(f"{_ts()} ⚠️ {pfx} {code} 行情异常：{payload['fetch_error']}（继续）")

                    if payload["error"]:
                        sc.write(f"{_ts()} ❌ {pfx} {code} 分析失败：{payload['error']}")
                    elif payload["result"]:
                        sc.write(
                            f"{_ts()} ✅ {pfx} **{payload['name']}**（{code}）分析完成，"
                            f"评分 {payload['score']}，耗时 {payload['elapsed']:.1f}s"
                        )
                    else:
                        sc.write(f"{_ts()} ⚠️ {pfx} {code} 未返回结果，耗时 {payload['elapsed']:.1f}s")

                if all(f.done() for f in worker_futures) and result_queue.empty():
                    break
                time.sleep(0.1)

        results=[]
        for idx in sorted(completed_payloads):
            payload = completed_payloads[idx]
            result = payload["result"]
            if not result:
                continue
            code = payload["code"]
            results.append(result)
            report_md = payload["full_md"]
            st.session_state.per_stock_reports[code]=report_md
            try:
                factors = _build_snapshot_factors(result, report_md)
                snapshot_id = save_snapshot(
                    run_id=run_id,
                    code=code,
                    name=payload["name"],
                    report_md=report_md,
                    sentiment_score=payload["score"],
                    operation_advice=payload["advice"],
                    run_mode=run_mode,
                    factors=factors,
                )
                st.session_state.snapshot_ids[code] = snapshot_id
                st.session_state.snapshot_factors[code] = factors
            except Exception as exc:
                logger.warning(f"save_snapshot:{exc}")

        st.session_state.analysis_results=results
        if not results:
            st.session_state.last_error="所有股票分析均失败，或已在超时/停止保护下被中断。"; return
        sc.write(f"{_ts()} 🧾 正在生成聚合仪表盘…"); _tick()
        try:
            agg=pipeline.notifier.generate_aggregate_report(results,rt)
        except:
            agg="\n\n---\n\n".join(
                st.session_state.per_stock_reports.get(r.code,f"## {r.name}({r.code})")
                for r in results)
        st.session_state.analysis_report=agg
        if stop_event.is_set():
            st.session_state.last_error="任务已按要求停止，以下展示已完成部分。"
        el=time.time()-t0; st.session_state.elapsed_sec=el
        sc.write(f"{_ts()} 🏁 全部完成，共 **{len(results)}** 只，总耗时 **{el:.1f}s**")
    except Exception as exc:
        logger.exception(f"分析顶层异常:{exc}"); st.session_state.last_error=f"分析引擎异常：{exc}"


def _fb_report(r):
    s=getattr(r,"sentiment_score",50)
    lines=[f"## {r.name}（{r.code}）","","| 字段 | 内容 |","|---|---|",
           f"| 操作建议 | **{r.operation_advice}** |",
           f"| 情绪评分 | {s} - {_sl(s)} |",
           f"| 趋势预测 | {r.trend_prediction} |",""]
    sm=getattr(r,"analysis_summary","") or ""
    if sm: lines+=["**AI 分析摘要**","",sm,""]
    db=getattr(r,"dashboard",None)
    if isinstance(db,dict): lines+=["**完整决策仪表盘**",""]+_d2md(db)
    return "\n".join(lines)

def _d2md(d,lv=3):
    lines=[]; ph="#"*min(lv,6)
    for k,v in d.items():
        if isinstance(v,dict): lines.append(f"{ph} {k}"); lines+=_d2md(v,lv+1)
        elif isinstance(v,list):
            lines.append(f"**{k}**")
            for it in v:
                lines.append(f"- {it}" if not isinstance(it,dict)
                              else "- "+"；".join(f"{a}:{b}" for a,b in it.items()))
        else: lines.append(f"**{k}**：{v}")
    return lines

def _build_snapshot_factors(result, report_md: str) -> dict:
    raw_factors = extract_factors(result)
    final_factors = enrich_from_md(raw_factors, report_md)
    final_factors.setdefault("sentiment_score", getattr(result, "sentiment_score", None))
    final_factors.setdefault("operation_advice", getattr(result, "operation_advice", None))
    final_factors.setdefault("trend_prediction", getattr(result, "trend_prediction", None))
    return {k: v for k, v in final_factors.items() if v not in (None, "")}


def _execute_market_review_task(run_id):
    t0=time.time()
    from src.analyzer import GeminiAnalyzer
    from src.core.market_review import run_market_review
    from src.notification import NotificationService
    from src.search_service import SearchService

    cfg=get_config()
    notifier=NotificationService()
    ss=None
    if cfg.has_search_capability_enabled():
        ss=SearchService(
            bocha_keys=cfg.bocha_api_keys,
            tavily_keys=cfg.tavily_api_keys,
            brave_keys=cfg.brave_api_keys,
            serpapi_keys=cfg.serpapi_keys,
            minimax_keys=cfg.minimax_api_keys,
            searxng_base_urls=cfg.searxng_base_urls,
            searxng_public_instances_enabled=cfg.searxng_public_instances_enabled,
            news_max_age_days=cfg.news_max_age_days,
            news_strategy_profile=getattr(cfg,"news_strategy_profile","short")
        )
    az=None
    if getattr(cfg,"gemini_api_key",None) or getattr(cfg,"openai_api_key",None):
        az=GeminiAnalyzer(api_key=cfg.gemini_api_key)
        if not az.is_available():
            az=None
    try:
        res=run_market_review(
            notifier=notifier,
            analyzer=az,
            search_service=ss,
            send_notification=False,
            merge_notification=False
        )
        return {
            "report": res,
            "elapsed": time.time()-t0,
            "error": "" if res else "大盘复盘未返回报告，请检查配置或终端日志。",
            "run_id": run_id,
        }
    except Exception as exc:
        logger.exception(f"大盘复盘任务异常:{exc}")
        return {
            "report": "",
            "elapsed": time.time()-t0,
            "error": f"大盘复盘出错：{exc}",
            "run_id": run_id,
        }


def _run_market_review(sc, run_id, timer_ph):
    t0=time.time()
    st.session_state.market_report=""
    if not st.session_state.run_ts:
        st.session_state.run_ts=datetime.now().strftime("%Y-%m-%d %H:%M")
    def _tick():
        e=time.time()-t0
        timer_ph.markdown(f'<span class="live-timer">⏱ 已耗时 {e:.1f}s</span>',
                          unsafe_allow_html=True)
    def _apply_market_result(payload):
        st.session_state.elapsed_sec=payload["elapsed"]
        if payload["report"]:
            st.session_state.market_report=payload["report"]
            try:
                save_snapshot(run_id=run_id,code="__market__",name="大盘复盘",
                              report_md=payload["report"],run_mode="仅大盘复盘")
            except Exception as exc:
                logger.warning(f"大盘 save_snapshot:{exc}")
            sc.write(f"{_ts()} ✅ 大盘复盘完成，耗时 {payload['elapsed']:.1f}s")
        else:
            st.session_state.last_error=payload["error"] or "大盘复盘未返回报告，请检查配置或终端日志。"
    try:
        sc.write(f"{_ts()} 初始化大盘复盘模块…"); _tick()
        payload=_execute_market_review_task(run_id)
        _apply_market_result(payload)
    except Exception as exc:
        logger.exception(f"大盘复盘异常:{exc}"); st.session_state.last_error=f"大盘复盘出错：{exc}"


def _run_full(codes,sc,run_id,timer_ph):
    t0=time.time()
    sc.write(f"{_ts()} 🌍 大盘复盘已并行启动，将与个股分析重叠执行")
    with ThreadPoolExecutor(max_workers=1) as market_executor:
        market_future = market_executor.submit(_execute_market_review_task, run_id)
        _run_stock_analysis(codes,sc,run_id,"全量分析（个股 + 大盘）",timer_ph)
        if st.session_state.stop_flag:
            sc.write(f"{_ts()} ⏹ 已停止个股分析，等待并汇总大盘复盘当前结果…")
        elif not market_future.done():
            sc.write(f"{_ts()} ⏳ 个股已完成，大盘复盘正在收尾…")
        while not market_future.done():
            if timer_ph is not None:
                timer_ph.markdown(
                    f'<span class="elapsed-pill">⏰ 已耗时 {time.time()-t0:.1f}s</span>',
                    unsafe_allow_html=True
                )
            time.sleep(0.1)
        payload=market_future.result()
    if payload["report"]:
        st.session_state.market_report=payload["report"]
        try:
            save_snapshot(run_id=run_id,code="__market__",name="大盘复盘",
                          report_md=payload["report"],run_mode="仅大盘复盘")
        except Exception as exc:
            logger.warning(f"大盘 save_snapshot:{exc}")
        sc.write(f"{_ts()} ✅ 大盘复盘完成，耗时 {payload['elapsed']:.1f}s")
    elif payload["error"] and not st.session_state.last_error:
        st.session_state.last_error=payload["error"]

def _chunk_codes(codes: list, batch_size: int = _BATCH_SIZE) -> list:
    uniq = list(dict.fromkeys(codes))
    return [uniq[i:i+batch_size] for i in range(0, len(uniq), batch_size)] or [[]]

def _build_aggregate_report(results, per_rpts, run_id: str):
    if not results:
        return ""
    try:
        pipeline = StockAnalysisPipeline(
            config=get_config(),
            max_workers=1,
            query_id=run_id,
            query_source="webui",
            save_context_snapshot=False,
        )
        return pipeline.notifier.generate_aggregate_report(results, _get_rt())
    except Exception:
        return "\n\n---\n\n".join(
            per_rpts.get(r.code, f"## {r.name}（{r.code}）")
            for r in results
        )

def _render_progress_state(progress_ph, progress_bar_ph, total_batches: int,
                           completed_batches: int, current_batch: int,
                           stage: str):
    if total_batches <= 0:
        progress_ph.empty()
        progress_bar_ph.empty()
        return
    percent = completed_batches / total_batches
    if stage == "running":
        msg = f"分析总进度：第 {current_batch}/{total_batches} 批进行中 ⏳，已完成 {completed_batches} 批"
    elif stage == "done":
        msg = f"分析总进度：{total_batches}/{total_batches} 批已完成 ✅"
    else:
        msg = f"分析总进度：已完成 {completed_batches}/{total_batches} 批 🟢"
    progress_ph.markdown(
        f'<div class="status-bar"><span style="color:var(--text)">{msg}</span></div>',
        unsafe_allow_html=True,
    )
    progress_bar_ph.progress(percent)

def _render_stock_report_block(target, results, per_rpts, report, ts_lbl,
                               hint: str = "", section_title: str = ""):
    box = target.container()
    with box:
        if not results:
            return
        title = section_title or f"🧮 「{ts_lbl}」个股决策仪表盘"
        box.markdown(f"## {title}")
        if hint:
            box.caption(hint)
        for rs in range(0, len(results), 4):
            row = results[rs:rs+4]
            cols = box.columns(len(row))
            for col, r in zip(cols, row):
                with col:
                    _render_result_metric(col, r)
        box.write("")
        valid = [(r, per_rpts[r.code]) for r in results if r.code in per_rpts]
        if len(valid) >= 2:
            tabs = box.tabs([f"{r.name}（{r.code}）" for r, _ in valid] + ["📋 完整聚合报告"])
            for tab, (r, md) in zip(tabs[:-1], valid):
                with tab:
                    st.markdown(md)
            with tabs[-1]:
                st.markdown(report)
        elif valid:
            box.markdown(valid[0][1])
        else:
            box.markdown(report)

def _run_stock_batches(codes, sc, run_id, run_mode, timer_ph,
                       progress_ph, progress_bar_ph, live_report_ph):
    batches = _chunk_codes(codes, _BATCH_SIZE)
    total_batches = len(batches)
    all_results = []
    all_per_reports = {}
    batch_errors = []
    ts_label = datetime.now().strftime("%Y-%m-%d %H:%M")
    st.session_state.run_ts = ts_label
    st.session_state.analysis_results = []
    st.session_state.per_stock_reports = {}
    st.session_state.analysis_report = ""
    st.session_state.last_error = ""

    for idx, batch_codes in enumerate(batches, 1):
        _render_progress_state(progress_ph, progress_bar_ph, total_batches, idx-1, idx, "running")
        sc.write(f"{_ts()} 📦 启动第 {idx}/{total_batches} 批（{len(batch_codes)} 只）")
        if idx == 2:
            _ctx = st.spinner("正在静默分析第二批...")
        elif idx > 2:
            _ctx = st.spinner(f"正在静默分析第 {idx} 批...")
        else:
            _ctx = nullcontext()
        with _ctx:
            _run_stock_analysis(batch_codes, sc, run_id, run_mode, timer_ph)

        batch_results = list(st.session_state.analysis_results)
        batch_per_reports = dict(st.session_state.per_stock_reports)
        batch_error = st.session_state.last_error.strip()
        if batch_error:
            batch_errors.append(f"第 {idx} 批：{batch_error}")

        for result in batch_results:
            if not any(getattr(r, "code", "") == getattr(result, "code", "") for r in all_results):
                all_results.append(result)
        all_per_reports.update(batch_per_reports)

        st.session_state.analysis_results = list(all_results)
        st.session_state.per_stock_reports = dict(all_per_reports)
        st.session_state.analysis_report = _build_aggregate_report(all_results, all_per_reports, run_id)
        st.session_state.last_error = "\n".join(batch_errors).strip()

        if batch_results:
            hint = (
                f"第 {idx} 批已完成，正在静默分析第 {idx+1} 批…"
                if idx < total_batches else
                f"第 {idx} 批已完成，本轮共 {len(all_results)} 只股票分析完成。"
            )
            batch_report = _build_aggregate_report(batch_results, batch_per_reports, f"{run_id}_batch_{idx}")
            _render_stock_report_block(
                live_report_ph,
                batch_results,
                batch_per_reports,
                batch_report,
                st.session_state.run_ts,
                hint=hint,
                section_title=f"第 {idx}/{total_batches} 批分析结果",
            )

        _render_progress_state(progress_ph, progress_bar_ph, total_batches, idx, idx, "done" if idx == total_batches else "ready")

        if st.session_state.stop_flag:
            sc.write(f"{_ts()} ⏹ 已在批次边界停止，后续批次不再启动")
            break

def _run_full_batched(codes, sc, run_id, timer_ph,
                      progress_ph, progress_bar_ph, live_report_ph):
    t0 = time.time()
    sc.write(f"{_ts()} 🌍 大盘复盘已并行启动，将与个股批次分析重叠执行")
    with ThreadPoolExecutor(max_workers=1) as market_executor:
        market_future = market_executor.submit(_execute_market_review_task, run_id)
        _run_stock_batches(
            codes,
            sc,
            run_id,
            "全量分析（个股 + 大盘）",
            timer_ph,
            progress_ph,
            progress_bar_ph,
            live_report_ph,
        )
        if st.session_state.stop_flag:
            sc.write(f"{_ts()} ⏹ 个股批次已停止，等待并汇总大盘复盘当前结果…")
        elif not market_future.done():
            sc.write(f"{_ts()} ⏳ 个股批次已完成，大盘复盘正在收尾…")
        while not market_future.done():
            if timer_ph is not None:
                timer_ph.markdown(
                    f'<span class="elapsed-pill">⏰ 已耗时 {time.time()-t0:.1f}s</span>',
                    unsafe_allow_html=True
                )
            time.sleep(0.1)
        payload = market_future.result()
    if payload["report"]:
        st.session_state.market_report = payload["report"]
        try:
            save_snapshot(
                run_id=run_id,
                code="__market__",
                name="大盘复盘",
                report_md=payload["report"],
                run_mode="仅大盘复盘",
            )
        except Exception as exc:
            logger.warning(f"大盘 save_snapshot:{exc}")
        sc.write(f"{_ts()} ✅ 大盘复盘完成，耗时 {payload['elapsed']:.1f}s")
    elif payload["error"]:
        if st.session_state.last_error:
            st.session_state.last_error += "\n" + payload["error"]
        else:
            st.session_state.last_error = payload["error"]


# =============================================================================
# 渚ц竟鏍?
# =============================================================================
with st.sidebar:
    st.markdown("## 🎨 外观与偏好")

    st.markdown("##### 🌓 主题")
    _tn=st.radio("主题",list(_THEMES.keys()),
        index=list(_THEMES.keys()).index(st.session_state.theme),
        horizontal=False,key="sb_theme",label_visibility="collapsed")
    if _tn!=st.session_state.theme: st.session_state.theme=_tn; st.rerun()

    st.markdown("##### 🎨 强调色")
    _an=st.radio("颜色",list(_ACCENTS.keys()),
        index=list(_ACCENTS.keys()).index(st.session_state.accent),
        horizontal=True,key="sb_accent",label_visibility="collapsed")
    if _an!=st.session_state.accent: st.session_state.accent=_an; st.rerun()

    st.markdown("##### 🔠 字体")
    _fn=st.radio("字号",list(_FONT_SIZES.keys()),
        index=list(_FONT_SIZES.keys()).index(st.session_state.font_size),
        horizontal=True,key="sb_font",label_visibility="collapsed")
    if _fn!=st.session_state.font_size: st.session_state.font_size=_fn; st.rerun()

    st.divider()
    st.markdown("##### 🧩 布局风格")
    _ls_opts=["🅰 编辑室","🅱 手术室"]
    _ls_new=st.radio("布局",_ls_opts,
        index=_ls_opts.index(st.session_state.layout_style),
        horizontal=False,key="sb_layout",label_visibility="collapsed")
    if _ls_new!=st.session_state.layout_style:
        st.session_state.layout_style=_ls_new; st.rerun()

    st.divider()
    st.markdown("##### ⚙️ 运行模式")
    _mn=st.radio("模式",_MODES,index=_MODES.index(st.session_state.run_mode),
        key="sb_run_mode",label_visibility="collapsed")
    if _mn!=st.session_state.run_mode: st.session_state.run_mode=_mn

    st.divider()
    st.markdown("##### 🗂️ 策略库")
    _gs=list_strategy_groups()
    if _gs:
        _gn=[g["name"] for g in _gs]
        _sel=st.selectbox("策略组",_gn,label_visibility="collapsed",key="sb_sel_group")
        _cl,_cd=st.columns(2)
        with _cl:
            if st.button("📥 载入",use_container_width=True,key="sb_load"):
                g=get_strategy_group(_sel)
                if g:
                    added=_append_codes(g["codes"],"manual")
                    st.success(f"载入 {len(g['codes'])} 只（新增 {added}）")
                    st.rerun()
        with _cd:
            if st.button("🗑️ 删除",use_container_width=True,key="sb_del"):
                delete_strategy_group(_sel); st.success(f"已删除：{_sel}"); st.rerun()
        _sg=next((g for g in _gs if g["name"]==_sel),None)
        if _sg:
            if _sg.get("tags"): st.caption("标签："+"  ".join(f"`{t}`" for t in _sg["tags"]))
            pv=", ".join(_sg["codes"][:5])
            if len(_sg["codes"])>5: pv+=f"… 共 {len(_sg['codes'])} 只"
            st.caption(f"持仓：{pv}")
    else:
        st.caption("暂无策略组，分析后可一键保存。")

    st.divider()
    st.caption("仅个人专属 · 不构成投资建议")


# =============================================================================
# 主内容区
# =============================================================================
st.markdown("""
<div style="padding:.5rem 0 .3rem">
  <span style="font-size:1.45rem;font-weight:900;letter-spacing:-.02em;color:var(--text)">🏠 快速分析</span>
  <span style="color:var(--text-muted);font-size:.83rem;margin-left:10px">
    V9.0 Phase 1 · 向导式步骤骨架 · 图片粘贴桥接 · 稳定底层引擎保持不动
  </span>
</div>
""", unsafe_allow_html=True)
with st.expander("ℹ️ V9.0 向导流程指引", expanded=False):
    _wizard_summary()
st.divider()

# -----------------------------------------------------------------------------
# STEP 1：多模态输入
# -----------------------------------------------------------------------------
st.markdown("""
<div class="step-card compact">
  <div class="step-head">
    <div class="step-badge">STEP 01</div>
    <div class="step-title">🧾 Step 1 · 输入区</div>
  </div>
</div>
""", unsafe_allow_html=True)

tab_text, tab_img, tab_voice = st.tabs(["📝 文字粘贴","🖼️ 截图识别","🎤 语音录入"])

# -- Tab 1 --------------------------------------------------------------------
with tab_text:
    raw_text=st.text_area("粘贴文本",height=130,
        placeholder="随意粘贴，例如：\n「帮我看茅台、浪潮信息和宁德时代」\n"
                    "「关注 600519、000001 平安银行、300750」\nGemini 会自动识别名称并转换为代码",
        label_visibility="collapsed",key="s1_raw_text")
    _c1,_c2=st.columns([3,2])
    with _c1:
        if st.button("🤖 智能提取并加入代码池",use_container_width=True,
                     type="secondary",key="btn_ext_text"):
            if raw_text.strip():
                _reset_fuzzy_state()
                with st.spinner("正在呼叫 LLM 进行语义提取与黑话破译..."):
                    items=extract_from_text(raw_text)
                if items:
                    fresh_items, skipped_dup = _dedupe_items_against_pool(items)
                    added, queued, parsed = _ingest_items(fresh_items, "text")
                    if len(st.session_state.pool_codes)>_MAX_WARN:
                        st.warning(f"⚠️ 代码池已达 **{len(st.session_state.pool_codes)}** 只，**建议分批运行**。")
                    st.success(
                        f"✅ 有效入池 {parsed} 只"
                        + (f"，新增 {added} 只" if added else "")
                        + (f"，忽略重复 {skipped_dup} 只" if skipped_dup else "")
                        + (f"，另有 {queued} 条进入模糊确认区" if queued else "")
                    )
                    st.rerun()
                else:
                    st.warning("未识别到高置信度的股票实体，或大模型解析异常。")
            else:
                st.info("请先粘贴文本。")
    with _c2:
        st.caption("支持汉字名称、简称、拼音缩写、6位代码混合输入")

# -- Tab 2 --------------------------------------------------------------------
with tab_img:
    _render_clipboard_bridge(max_files=_MAX_WARN)
    st.caption("三合一入口：粘贴截图 / 拖拽图片 / 点击上传。粘贴桥接会尝试将剪贴板图片自动写入下方上传框。")
    uploaded_files=st.file_uploader(
        f"📸 拖拽或点击上传截图（最多 {_MAX_WARN} 张，PNG/JPG/WebP）",
        type=["png","jpg","jpeg","webp","bmp"],
        accept_multiple_files=True,
        key="s1_uploader",
    )
    if uploaded_files and len(uploaded_files) > _MAX_WARN:
        st.warning(f"⚠️ 当前共载入 {len(uploaded_files)} 张图，Phase 1 仅保留前 {_MAX_WARN} 张进入识别。")
        uploaded_files = uploaded_files[:_MAX_WARN]
    if uploaded_files:
        prev_cols=st.columns(min(len(uploaded_files),4))
        for i,f in enumerate(uploaded_files[:4]):
            with prev_cols[i]:
                st.image(f,caption=f.name[:18],use_column_width=True)
        if len(uploaded_files)>4: st.caption(f"… 另有 {len(uploaded_files)-4} 张未预览")
        if st.button("🖼️ AI 批量解析截图并智能映射代码",
                     use_container_width=True,type="secondary",key="btn_ext_img"):
            _reset_fuzzy_state()
            normalized_files = _normalize_uploaded_images(uploaded_files)
            with st.spinner(f"解析 {len(normalized_files)} 张图片并交给 Gemini 映射…"):
                items=extract_from_images(normalized_files)
            if items:
                added, queued, parsed = _ingest_items(items, "image")
                if len(st.session_state.pool_codes)>_MAX_WARN:
                    st.warning(f"⚠️ 代码池已达 **{len(st.session_state.pool_codes)}** 只，**建议分批运行**。")
                st.success(
                    f"✅ {len(normalized_files)} 张图有效入池 {parsed} 只"
                    + (f"，新增 {added} 只" if added else "")
                    + (f"，另有 {queued} 条进入模糊确认区" if queued else "")
                )
                st.rerun()
            else:
                st.warning("⚠️ 未识别到有效股票代码，请确保图片清晰。")

# -- Tab 3 --------------------------------------------------------------------
with tab_voice:
    st.markdown("""
<div style="background:var(--success-bg);border:1px solid var(--receipt-border);
            border-radius:10px;padding:.75rem 1rem;margin-bottom:.7rem;
            font-size:.83rem;color:var(--success-fg);">
    <b>使用步骤：</b>
    ① 点击麦克风 &ensp;→&ensp; ② 说出股票名称，如「帮我看茅台和浪潮信息」
    &ensp;→&ensp; ③ 点击停止 &ensp;→&ensp; ④ 点击【转写并提取】
    <br><small>⚠️ 转写失败时系统会如实报告，绝不捏造代码</small>
</div>
""", unsafe_allow_html=True)
    audio_data=None
    try:
        audio_data=st.audio_input("点击录音",key="s1_audio",label_visibility="collapsed")
    except AttributeError:
        st.info("ℹ️ 语音录入需要 Streamlit >= 1.36，请执行 `pip install -U streamlit` 升级。")

    if audio_data is not None:
        # 立即读取字节并缓存，防止被 Streamlit rerun 时回收
        _audio_bytes = audio_data.read()
        st.audio(_audio_bytes)
        if st.button("🎙️ 转写并智能提取股票代码",
                     use_container_width=True,type="secondary",key="btn_voice"):
            _reset_fuzzy_state()
            with st.spinner("Gemini 正在转写语音…"):
                transcript=transcribe_audio(_audio_bytes)
            if transcript:
                st.session_state.voice_transcript=transcript
                st.info(f"🗣️ 转写结果：**{transcript}**")
                with st.spinner("Gemini 正在映射股票名称到代码…"):
                    items=extract_from_voice(_audio_bytes)
                if items:
                    added, queued, parsed = _ingest_items(items, "voice", fallback_text=transcript)
                    st.success(
                        f"✅ 从语音有效入池 {parsed} 只"
                        + (f"，新增 {added} 只" if added else "")
                        + (f"，另有 {queued} 条进入模糊确认区" if queued else "")
                    )
                    st.rerun()
                else:
                    local_added = _append_items(_extract_local_text_items(transcript, "voice"))
                    queued = _queue_fuzzy_candidates(_extract_fuzzy_terms_from_text(transcript), "voice")
                    if local_added:
                        st.success(f"✅ 本地兜底已从转写结果中截获 {local_added} 只股票并加入代码池。")
                        st.rerun()
                    elif queued:
                        st.warning(f"⚠️ 语音未精确映射，已将 {queued} 条送入模糊确认区。")
                        st.rerun()
                    else:
                        st.warning("⚠️ 未识别到股票，请在代码池手动输入。")
                        st.caption(f"转写原文：{transcript}")
            else:
                st.error("❌ 转写失败，请检查 API Key 或网络。转写结果为空时，系统不会自动编造代码。")
    if st.session_state.voice_transcript:
        st.caption(f"📝 上次转写：{st.session_state.voice_transcript}")

st.divider()


# =============================================================================
# STEP 1.5：模糊确认区
# =============================================================================
if st.session_state.fuzzy_candidates:
    st.markdown("""
<div class="step-card">
  <div class="step-badge">STEP 01.5</div>
  <div class="step-title">⚠️ 模糊确认区</div>
  <div class="step-sub">识别到未能精确映射的词条。请从本地字典 / 拼音候选中勾选正确股票，系统不会直接丢弃。</div>
</div>
""", unsafe_allow_html=True)
    for idx, entry in enumerate(st.session_state.fuzzy_candidates):
        options = entry.get("options", [])
        if not options:
            continue
        labels = [f"{opt['code']} {opt['name']} · {opt['reason']}" for opt in options]
        st.markdown(f"**识别词：** `{entry['raw']}`  ·  来源：{_source_label(entry['source'])}")
        _fc1, _fc2 = st.columns([1, 6])
        with _fc1:
            st.checkbox("加入", value=True, key=f"fuzzy_use_{idx}")
        with _fc2:
            st.selectbox(
                "候选",
                options=list(range(len(options))),
                format_func=lambda i, labels=labels: labels[i],
                key=f"fuzzy_pick_{idx}",
                label_visibility="collapsed",
            )
    _f1, _f2 = st.columns(2)
    with _f1:
        if st.button("✅ 确认选中项加入代码池", use_container_width=True, key="btn_fuzzy_confirm"):
            confirmed = []
            for idx, entry in enumerate(st.session_state.fuzzy_candidates):
                if not st.session_state.get(f"fuzzy_use_{idx}", True):
                    continue
                options = entry.get("options", [])
                pick_idx = int(st.session_state.get(f"fuzzy_pick_{idx}", 0))
                if not options:
                    continue
                pick = options[min(max(pick_idx, 0), len(options)-1)]
                confirmed.append({
                    "code": pick["code"],
                    "name": pick["name"],
                    "valid": True,
                    "source": "fuzzy",
                })
            added = _append_items(confirmed) if confirmed else 0
            st.session_state.fuzzy_candidates = []
            st.success(f"✅ 已从模糊确认区加入 {added} 只股票")
            st.rerun()
    with _f2:
        if st.button("❌ 忽略全部", use_container_width=True, key="btn_fuzzy_clear"):
            st.session_state.fuzzy_candidates = []
            st.rerun()
    st.divider()


# =============================================================================
# STEP 2：分拣工作台（双风格切换）
# =============================================================================
_quick_pool_added = _ingest_quick_pool_cache()
if _quick_pool_added:
    _normalize_fast_analysis_pool_state()
    _sync_fast_analysis_widget_state_from_session()
    st.toast(f"已从历史记忆库自动导入 {_quick_pool_added} 只股票到代码池")

pool    = st.session_state.pool_codes
checked = st.session_state.checked_codes

_style = st.session_state.layout_style  # "🅰 编辑室" 或 "🅱 手术室"

st.markdown(f"""
<div class="step-card compact">
  <div class="step-head">
    <div class="step-badge">STEP 02</div>
    <div class="step-title">🛒 Step 2 · 代码池</div>
  </div>
</div>
""", unsafe_allow_html=True)

# ── Input Wizard：金融级实时输入精灵（三市场 + 拼音首字母补全）──────────────
_render_input_wizard()

# 批量操作（两种风格共用）
if pool:
    _b1,_b2,_b3,_b4=st.columns(4)
    with _b1:
        if st.button("☑️ 全选",use_container_width=True,key="btn_all"):
            st.session_state.checked_codes=set(pool); st.rerun()
    with _b2:
        if st.button("🔁 反选",use_container_width=True,key="btn_inv"):
            st.session_state.checked_codes=set(pool)-checked; st.rerun()
    with _b3:
        if st.button("✖️ 清空选中",use_container_width=True,key="btn_unchk"):
            st.session_state.checked_codes=set(); st.rerun()
    with _b4:
        if st.button("🗑️ 清空代码池",use_container_width=True,key="btn_clrpool"):
            _clear_pool_session_state()
            st.rerun()

# 重读（批量操作后同步刷新）
checked=st.session_state.checked_codes

# 鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲
# 编辑室风格：左侧候选池 | 右侧已选预览
# 鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲
if _style == "🅰 编辑室":
    if not pool:
        st.markdown('<div style="color:var(--text-muted);font-size:.84rem;padding:.35rem 0">'
                    "代码池为空，请先在 Step 01 提取代码。</div>",
                    unsafe_allow_html=True)
    else:
        _left, _right = st.columns([3, 2], gap="medium")

        src_groups = {"text":[],"image":[],"voice":[],"manual":[],"fuzzy":[]}
        for c in pool:
            src_groups.get(st.session_state.pool_sources.get(c,"manual"),[]).append(c)

        # 左：候选池货架（按来源分组）
        with _left:
            st.caption("**🧾 原料池（点击勾选）**")
            for src in ["text","image","voice","manual","fuzzy"]:
                grp = src_groups[src]
                if not grp:
                    continue
                st.markdown(_source_group_header_html(src, len(grp)), unsafe_allow_html=True)
                for row_s in range(0, len(grp), 4):
                    row_codes = grp[row_s:row_s+4]
                    cols = st.columns(4)
                    for idx, col in enumerate(cols):
                        with col:
                            if idx >= len(row_codes):
                                st.markdown("&nbsp;", unsafe_allow_html=True)
                                continue
                            code = row_codes[idx]
                            is_on = code in checked
                            nv = st.checkbox(_display_name(code), value=is_on, key=f"cb_{code}")
                            if nv != is_on:
                                if nv:
                                    st.session_state.checked_codes.add(code)
                                else:
                                    st.session_state.checked_codes.discard(code)

        # 右：已选预览面板（实时同步）
        with _right:
            checked = st.session_state.checked_codes
            final_codes_preview = [c for c in pool if c in checked]
            st.caption(f"**🧮 今日发版单（已选 {len(final_codes_preview)} 只）**")
            if final_codes_preview:
                # 渲染预览列表
                preview_lines = []
                for c in final_codes_preview:
                    name = _resolved_name(c)
                    preview_lines.append(
                        f'<div class="preview-item">'
                        f'<span style="color:var(--text);font-family:\'JetBrains Mono\',monospace;font-size:.82rem">'
                        f'<b>{c}</b> {name}</span>'
                        f'</div>'
                    )
                st.markdown(
                    f'<div class="preview-panel">{"".join(preview_lines)}</div>',
                    unsafe_allow_html=True,
                )
                if len(final_codes_preview) > _MAX_WARN:
                    st.warning(f"⚠️ 已选 **{len(final_codes_preview)}** 只，超过推荐 {_MAX_WARN} 只，建议分批运行。")
            else:
                st.markdown(
                    '<div class="preview-panel" style="color:var(--text-muted);'
                    'display:flex;align-items:center;justify-content:center;">'
                    "☝️ 在左侧勾选股票后，这里会实时显示</div>",
                    unsafe_allow_html=True,
                )

# 鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲
# 手术室风格：深色系 5 列网格 + 底部状态条
# 鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲
elif _style == "🅱 手术室":
    if not pool:
        st.markdown('<div style="color:var(--text-muted);font-size:.84rem;padding:.35rem 0">'
                    "代码池为空，请先在 Step 01 提取代码。</div>",
                    unsafe_allow_html=True)
    else:
        src_groups = {"text":[],"image":[],"voice":[],"manual":[],"fuzzy":[]}
        for c in pool:
            src_groups.get(st.session_state.pool_sources.get(c, "manual"),[]).append(c)

        for src in ["text","image","voice","manual","fuzzy"]:
            grp = src_groups[src]
            if not grp:
                continue
            st.markdown(_source_group_header_html(src, len(grp)), unsafe_allow_html=True)
            for row_s in range(0, len(grp), 5):
                row_codes = grp[row_s:row_s+5]
                cols = st.columns(5)
                for idx, col in enumerate(cols):
                    with col:
                        if idx >= len(row_codes):
                            st.markdown("&nbsp;", unsafe_allow_html=True)
                            continue
                        code = row_codes[idx]
                        is_on = code in checked
                        nv = st.checkbox(_display_name(code), value=is_on, key=f"cb_{code}")
                        if nv != is_on:
                            if nv:
                                st.session_state.checked_codes.add(code)
                            else:
                                st.session_state.checked_codes.discard(code)

        # 重读勾选状态
        checked = st.session_state.checked_codes
        final_codes_preview = [c for c in pool if c in checked]

        # 底部状态条
        _warn_txt = (f"&nbsp;⚠️ 超过 {_MAX_WARN} 只，建议分批"
                     if len(final_codes_preview)>_MAX_WARN else "")
        st.markdown(
            f'<div class="status-bar">'
            f'<span style="color:var(--text)">✅ 已选 <b>{len(final_codes_preview)}</b> / {len(pool)}</span>'
            f'<span style="color:var(--text-muted)">·</span>'
            f'<span style="color:var(--text-muted);font-family:\'JetBrains Mono\',monospace;font-size:.8rem">'
            f'{", ".join(f"{c} {_resolved_name(c)}" for c in final_codes_preview[:8])}'
            f'{"  …" if len(final_codes_preview)>8 else ""}</span>'
            f'<span style="color:var(--warn-fg)">{_warn_txt}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )

# 重读 final_codes（两种风格统一出口）
checked     = st.session_state.checked_codes
final_codes = [c for c in st.session_state.pool_codes if c in checked]
cart_run_clicked = False

st.markdown("""
<div class="step-card compact" style="margin-top:.25rem">
  <div class="step-head">
    <div class="step-badge">CART</div>
    <div class="step-title">🧺 二号区 · 今日发版单 / 最终购物车</div>
  </div>
</div>
""", unsafe_allow_html=True)

if pool:
    if final_codes:
        for row_s in range(0, len(final_codes), 4):
            row_codes = final_codes[row_s:row_s+4]
            cols = st.columns(4)
            for idx, col in enumerate(cols):
                with col:
                    if idx >= len(row_codes):
                        st.markdown("&nbsp;", unsafe_allow_html=True)
                        continue
                    code = row_codes[idx]
                    is_on = code in st.session_state.checked_codes
                    keep = st.checkbox(_display_name(code), value=is_on, key=f"cart_keep_{code}")
                    if keep != is_on:
                        if keep:
                            st.session_state.checked_codes.add(code)
                        else:
                            st.session_state.checked_codes.discard(code)
    else:
        st.caption("☝️ 先从候选池勾选至少一只股票，这里才会出现最终购物车。")

    _qa1, _qa2, _qa3 = st.columns(3)
    with _qa1:
        if st.button("☑ 全选", use_container_width=True, key="cart_select_all"):
            st.session_state.checked_codes = set(pool); st.rerun()
    with _qa2:
        if st.button("🧹 清空选定", use_container_width=True, key="cart_clear_checked"):
            st.session_state.checked_codes = set(); st.rerun()
    with _qa3:
        cart_run_clicked = st.button(
            "🚀 最后确认（开始分析）",
            use_container_width=True,
            type="primary",
            disabled=len(final_codes) == 0 and st.session_state.run_mode != "仅大盘复盘",
            key="cart_run_now",
        )

# 确认小票（两种风格共用，渲染在代码池下方）
if final_codes:
    cnt = len(final_codes)
    warn_txt = (
        f'<span style="color:var(--warn-fg);font-size:.76rem;margin-left:8px">'
        f'⚠️ 已选 {cnt} 只，超过推荐 {_MAX_WARN} 只，建议分批</span>'
        if cnt > _MAX_WARN else ""
    )
    ticket_codes = ", ".join(
        f"{c} {_resolved_name(c)}"
        for c in final_codes
    )
    st.markdown(f"""
<div class="receipt-box">
  <div class="receipt-label">✅ 最终将分析（确认小票）</div>
  <span style="font-size:.88rem">{cnt} 只：{ticket_codes}</span>
  {warn_txt}
</div>""", unsafe_allow_html=True)
elif pool:
    st.caption("☝️ 请勾选至少一只股票（仅大盘复盘模式可留空）。")

st.divider()


# =============================================================================
# STEP 3：收银台（运行控制 + 策略保存）
# =============================================================================
st.markdown("""
<div class="step-card">
  <div class="step-badge">STEP 03</div>
  <div class="step-title">⚙️ Step 3 · 分析进度</div>
  <div class="step-sub">Phase 1 先保留现有执行控制与稳定并发底座，后续再接静默分批与双进度条</div>
</div>
""", unsafe_allow_html=True)

run_mode=st.radio("运行模式",_MODES,
    index=_MODES.index(st.session_state.run_mode),
    horizontal=True,key="main_run_mode",label_visibility="collapsed")
if run_mode!=st.session_state.run_mode: st.session_state.run_mode=run_mode

_need  = run_mode in ("仅个股分析","全量分析（个股 + 大盘）")
_has   = len(final_codes)>0

_cr,_cc=st.columns([5,1])
with _cc:
    if st.button("🗑️ 全清", use_container_width=True, key="btn_clear_all"):
        _clear_fast_analysis_state()
        st.rerun()
with _cr:
    run_clicked=cart_run_clicked or st.button(
        label="⏳ 分析进行中…" if st.session_state.is_running else "🚀 确认并运行分析",
        type="primary",use_container_width=True,
        disabled=st.session_state.is_running or (_need and not _has),
        key="btn_run")

if _need and not _has and not st.session_state.is_running:
    st.warning("⚠️ 请在上方代码池中勾选至少一只股票，再运行分析。")
elif _need and len(final_codes) > _BATCH_SIZE:
    st.info(f"ℹ️ 当前已选 {len(final_codes)} 只，系统会在后台自动按每批 {_BATCH_SIZE} 只静默分批执行。")

_batch_progress_ph = st.empty()
_batch_progress_bar_ph = st.empty()

# 策略组保存
if final_codes:
    # 智能默认名：取第一只股票名称
    _fn0  = st.session_state.pool_names.get(final_codes[0], final_codes[0])
    _dsg  = f"[{datetime.now().strftime('%m-%d')}] {_fn0}等{len(final_codes)}只"
    with st.expander("🔖 保存为策略组",expanded=False):
        _sc1,_sc2=st.columns([5,3])
        with _sc1:
            sg_name=st.text_input("名称",value=_dsg,
                label_visibility="collapsed",key="sg_name")
            sg_desc=st.text_input("备注",placeholder="如：本周关注",
                label_visibility="collapsed",key="sg_desc")
        with _sc2:
            st.caption("**选择标签**")
            sel_tags=[]
            for _trow in [_PRESET_TAGS[i:i+4] for i in range(0,len(_PRESET_TAGS),4)]:
                _tcols=st.columns(len(_trow))
                for _tc,_tag in zip(_tcols,_trow):
                    with _tc:
                        if st.checkbox(_tag,key=f"tag_{_tag}"): sel_tags.append(_tag)
        if st.button("✅ 保存策略组",use_container_width=True,key="btn_save_sg"):
            if sg_name.strip():
                save_strategy_group(name=sg_name.strip(),codes=final_codes,
                    description=sg_desc.strip(),tags=sel_tags)
                st.success(f"✅ 「{sg_name.strip()}」已保存（{len(final_codes)} 只）")
                st.rerun()
            else: st.error("请输入策略组名称")

st.divider()

# =============================================================================
# STEP 4：执行 + 实时进度 + 结果渲染
# =============================================================================
if "report_panel_hidden" not in st.session_state:
    st.session_state.report_panel_hidden = False

_report_data_ready = bool(st.session_state.analysis_results) or bool(st.session_state.market_report)
_step4_title_col, _step4_btn_col = st.columns([4, 1])
with _step4_title_col:
    st.markdown("""
<div class="step-card">
  <div class="step-badge">STEP 04</div>
  <div class="step-title">📊 Step 4 · 报告</div>
  <div class="step-sub">Phase 1 保留现有报告出口；后续再切到摘要优先、标签过滤与切片阅读</div>
</div>
""", unsafe_allow_html=True)
with _step4_btn_col:
    button_label = "📂 展开报告" if st.session_state.report_panel_hidden else "👁️ 隐藏报告"
    if st.button(button_label, key="toggle_report_btn", use_container_width=True, disabled=not _report_data_ready):
        st.session_state.report_panel_hidden = not st.session_state.report_panel_hidden
        st.rerun()

_live_report_ph = st.container()

_ctrl_cols = st.columns(2)
with _ctrl_cols[0]:
    if st.button("⏹ 彻底停止", use_container_width=True,
                 disabled=not st.session_state.is_running, key="btn_hard_stop"):
        st.session_state.stop_flag = True
        st.session_state.pause_flag = False
        st.rerun()
with _ctrl_cols[1]:
    _pause_label = "▶️ 继续" if st.session_state.pause_flag else "⏸ 临时暂停"
    if st.button(_pause_label, use_container_width=True,
                 disabled=not st.session_state.is_running, key="btn_pause_resume"):
        st.session_state.pause_flag = not st.session_state.pause_flag
        st.rerun()

if st.session_state.is_running:
    _ctl_msg = "⏸ 当前处于暂停态，未开始任务将等待继续。" if st.session_state.pause_flag else "▶️ 当前处于运行态。"
    if st.session_state.stop_flag:
        _ctl_msg = "⏹ 已下达停止指令，系统将汇总已完成部分。"
    st.caption(_ctl_msg)

# 触发运行
if run_clicked and not st.session_state.is_running:
    st.session_state.is_running = True
    st.session_state.run_requested = True
    st.session_state.pause_flag = False
    st.session_state.stop_flag = False
    st.session_state.report_panel_hidden = False
    st.session_state.snapshot_ids = {}
    st.session_state.snapshot_factors = {}
    st.session_state.watchlist_feedback = ""
    st.session_state.run_id = uuid.uuid4().hex
    st.session_state.pending_mode = run_mode
    st.session_state.pending_codes = list(dict.fromkeys(final_codes))
    st.rerun()

if st.session_state.is_running and st.session_state.run_requested:
    _t0=time.time()
    _run_id=st.session_state.run_id
    _mode=st.session_state.pending_mode
    _to_run=list(st.session_state.pending_codes)
    _timer_ph=st.empty()
    with st.status("🚀 正在执行分析任务...",expanded=True) as _sc:
        try:
            if _mode=="仅个股分析":
                _run_stock_batches(
                    _to_run, _sc, _run_id, _mode, _timer_ph,
                    _batch_progress_ph, _batch_progress_bar_ph, _live_report_ph
                )
            elif _mode=="仅大盘复盘":
                _run_market_review(_sc,_run_id,_timer_ph)
            else:
                _run_full_batched(
                    _to_run, _sc, _run_id, _timer_ph,
                    _batch_progress_ph, _batch_progress_bar_ph, _live_report_ph
                )
            _el=time.time()-_t0; st.session_state.elapsed_sec=_el
            _timer_ph.markdown(
                f'<span class="elapsed-pill">⏱️ 本次分析总耗时 {_el:.1f}s</span>',
                unsafe_allow_html=True)
            _render_progress_state(
                _batch_progress_ph,
                _batch_progress_bar_ph,
                max(len(_chunk_codes(_to_run, _BATCH_SIZE)), 1) if _mode != "仅大盘复盘" else 0,
                len(_chunk_codes(_to_run, _BATCH_SIZE)) if _mode != "仅大盘复盘" else 0,
                len(_chunk_codes(_to_run, _BATCH_SIZE)) if _mode != "仅大盘复盘" else 0,
                "done" if _mode != "仅大盘复盘" else "ready",
            )
            if st.session_state.stop_flag:
                _sc.update(label=f"⏹ 任务已停止并完成汇总，总耗时 {_el:.1f}s",
                           state="error",expanded=True)
            elif st.session_state.last_error:
                _sc.update(label=f"❌ 任务结束（部分失败），总耗时 {_el:.1f}s",
                           state="error",expanded=True)
            else:
                _sc.update(label=f"✅ 分析全部完成，总耗时 {_el:.1f}s",
                           state="complete",expanded=False)
            _persist_run_artifacts(_run_id, _mode)
        except Exception as _exc:
            logger.exception(f"椤跺眰寮傚父:{_exc}")
            st.session_state.last_error=str(_exc)
            _sc.update(label=f"❌ 运行异常：{_exc}",state="error",expanded=True)
        finally:
            st.session_state.is_running = False
            st.session_state.run_requested = False
            st.session_state.pause_flag = False
            st.session_state.stop_flag = False
            st.session_state.pending_codes = []
            st.session_state.pending_mode = ""
    st.rerun()  # 必须强制重载页面，打断顶部计时器与按钮锁态

if st.session_state.last_error:
    st.error(f"❌ {st.session_state.last_error}")

_has_any=bool(st.session_state.analysis_results) or bool(st.session_state.market_report)
if _has_any and st.session_state.is_running:
    st.session_state.is_running = False
    st.session_state.run_requested = False
    st.rerun()
if not _has_any and not st.session_state.last_error:
    st.info("📝 完成上方步骤后，点击【确认并运行分析】，报告将会在此处同屏渲染。")
    st.stop()

_report_hidden = bool(st.session_state.get("report_panel_hidden", False))
if _has_any and _report_hidden:
    st.caption("👁️ 报告已折叠隐藏 (数据已缓存)")

if st.session_state.elapsed_sec>0:
    st.markdown(
        f'<span class="elapsed-pill">⏱️ 本次分析总耗时 {st.session_state.elapsed_sec:.1f} 秒</span>',
        unsafe_allow_html=True)
    st.write("")

if not _report_hidden:
    # 个股结果
    if st.session_state.analysis_results:
        results =st.session_state.analysis_results
        per_rpts=st.session_state.per_stock_reports
        report  =st.session_state.analysis_report
        ts_lbl  =st.session_state.run_ts
        tracked_codes = {item["code"] for item in list_watchlist()}

        st.markdown(f"## 🧮 「{ts_lbl}」个股决策仪表盘")
        if st.session_state.watchlist_feedback:
            st.success(st.session_state.watchlist_feedback)

        for rs in range(0,len(results),4):
            row=results[rs:rs+4]; cols=st.columns(len(row))
            for col,r in zip(cols,row):
                with col:
                    _render_result_metric(col, r)
                    if r.code in tracked_codes:
                        st.button(
                            "✅ 已在跟踪池",
                            key=f"btn_watch_done_{r.code}",
                            use_container_width=True,
                            disabled=True,
                        )
                    else:
                        if st.button("⭐ 加入跟踪池", key=f"btn_watch_{r.code}", use_container_width=True):
                            try:
                                snapshot_id = st.session_state.snapshot_ids.get(r.code)
                                factors = st.session_state.snapshot_factors.get(r.code, {})
                                add_to_watchlist(
                                    code=r.code,
                                    name=r.name,
                                    snapshot_id=snapshot_id,
                                    run_id=st.session_state.run_id or None,
                                    factors=factors,
                                )
                                st.session_state.watchlist_feedback = f"✅ 已将 {r.name}（{r.code}）加入跟踪池"
                            except Exception as exc:
                                st.session_state.watchlist_feedback = f"❌ 加入跟踪池失败：{exc}"
                            st.rerun()
        st.write("")

        _valid=[(r,per_rpts[r.code]) for r in results if r.code in per_rpts]
        if len(_valid)>=2:
            _tabs=st.tabs([f"{r.name}（{r.code}）" for r,_ in _valid]+["📋 完整聚合报告"])
            for tab,(r,md) in zip(_tabs[:-1],_valid):
                with tab: st.markdown(md)
            with _tabs[-1]: st.markdown(report)
        elif _valid: st.markdown(_valid[0][1])
        else: st.markdown(report)

        st.write("")
        _d1,_d2=st.columns(2)
        _tfn=ts_lbl.replace(" ","_").replace(":","")
        _report_txt = _plain_text_report(report)
        with _d1:
            st.download_button("📋 下载完整 TXT 报告",
                data=_report_txt.encode("utf-8"),
                file_name=_report_filename("full_report", _tfn),
                mime="text/plain",use_container_width=True,key="dl_agg")
        with _d2:
            _all_per="\n\n==========\n\n".join(
                f"{r.name}（{r.code}）\n\n"+_plain_text_report(per_rpts.get(r.code,""))
                for r in results)
            st.download_button("🗂️ 下载逐股 TXT 报告",
                data=_all_per.encode("utf-8"),
                file_name=_report_filename("stock_report", _tfn),
                mime="text/plain",use_container_width=True,key="dl_per")
        _render_text_preview("完整报告 TXT 预览", _report_txt, key="report_preview_agg", height=260)
        _render_text_preview("逐股报告 TXT 预览", _all_per, key="report_preview_per", height=260)
        st.divider()

    # 大盘复盘
    if st.session_state.market_report:
        _tl=st.session_state.run_ts
        st.markdown(f"## 🌍 「{_tl}」大盘复盘报告")
        st.markdown(st.session_state.market_report)
        _market_txt = _plain_text_report(st.session_state.market_report)
        st.download_button(
            "📥 下载大盘复盘 TXT",
            data=_market_txt.encode("utf-8"),
            file_name=_report_filename("market_review", datetime.now().strftime('%Y%m%d_%H%M%S')),
            mime="text/plain",
            key="dl_market_business_unique",
        )
        _render_text_preview("大盘复盘 TXT 预览", _market_txt, key="report_preview_market", height=240)
else:
    st.write("")

st.divider()

_biz_log_path, _biz_log_raw, _biz_log_tail = _read_physical_log("stock_analysis", debug=False)
_debug_log_path, _debug_log_raw, _debug_log_tail = _read_physical_log("stock_analysis", debug=True)
_debug_log_warn = (
    _debug_log_path is None
    or _biz_log_path is None
    or _biz_log_path == _debug_log_path
    or (_biz_log_path and _debug_log_path and os.path.getsize(_biz_log_path) == os.path.getsize(_debug_log_path))
)

with st.expander("🛠️ 开发者调试与全局报告区", expanded=False):
    _dbg2, _dbg3 = st.columns(2)
    with _dbg2:
        st.markdown("**【📥 下载业务分析日志 (INFO)】**")
        if _biz_log_path:
            st.caption(f"物理文件：`{os.path.basename(_biz_log_path)}`")
            st.download_button(
                "📥 下载业务分析日志 (INFO)",
                data=_biz_log_raw,
                file_name="webui_info_latest_1000.txt",
                mime="text/plain",
                use_container_width=True,
                key="dl_run_log",
            )
            st.text_area(
                "最后 1000 行",
                value=_biz_log_tail or "日志文件为空。",
                height=260,
                key="tail_run_log",
            )
        else:
            st.info("未找到当天业务分析日志。")
    with _dbg3:
        st.markdown("**【🐞 下载底层握手日志 (DEBUG)】**")
        if _debug_log_path:
            st.caption(f"物理文件：`{os.path.basename(_debug_log_path)}`")
            st.download_button(
                "🐞 下载底层握手日志 (DEBUG)",
                data=_debug_log_raw,
                file_name="webui_debug_latest_1000.txt",
                mime="text/plain",
                use_container_width=True,
                key="dl_engine_log",
            )
            st.text_area(
                "最后 1000 行",
                value=_debug_log_tail or "日志文件为空。",
                height=260,
                key="tail_engine_log",
            )
        else:
            st.info("未找到当天底层握手日志。")
    if _debug_log_warn:
        st.warning("⚠️ 警告：本地 logs 目录下的调试日志未物理对齐，请检查 FileHandler 配置。")
