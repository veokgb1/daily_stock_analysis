# -*- coding: utf-8 -*-
"""
app.py —— Streamlit 多页应用入口
================================
职责：
  1. 环境初始化（必须在所有项目模块之前）
  2. SQLite 数据库建表（幂等，启动时执行一次）
  3. 全局页面配置
  4. 渲染首页导航入口（Streamlit 会自动发现 pages/ 目录下的页面）

多页路由：
  Streamlit ≥ 1.28 支持 pages/ 目录自动发现。
  文件名格式：`序号_emoji_页面名.py`，Streamlit 解析为导航菜单项。

  pages/
    1_🏠_快速分析.py       ← 核心分析页
    2_🕰️_历史记忆库.py    ← 历史报告时间线
    3_📈_标的跟踪池.py     ← 单标的跨期趋势
"""

import os
import sys

# ── 将项目根目录加入 sys.path，确保 src/ 等模块可被 pages/ 正确导入 ──────────
_ROOT = os.path.dirname(os.path.abspath(__file__))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

# ── 环境初始化（必须先于所有项目模块）──────────────────────────────────────────
from src.config import detect_runtime_mode, setup_env
setup_env()

# ── 数据库初始化（幂等）──────────────────────────────────────────────────────────
from webui.db import init_db
init_db()

# ── Streamlit ────────────────────────────────────────────────────────────────
import streamlit as st
from src.streamlit_guard import enforce_sidebar_password_gate

st.set_page_config(
    page_title="DUKA Stock Analysis Engine V5-Pro",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

def main() -> None:
    enforce_sidebar_password_gate()
    runtime_mode = detect_runtime_mode()
    runtime_label = "Cloud" if runtime_mode == "cloud" else "Local"

    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Noto+Serif+SC:wght@400;700;900&family=JetBrains+Mono:wght@400;600&display=swap');

        html, body, [class*="css"] { font-family: 'Noto Serif SC', serif; }

        .hero-wrap {
            background: linear-gradient(135deg, #0f172a 0%, #1e293b 60%, #0f2744 100%);
            border-radius: 16px;
            padding: 3.5rem 3rem 3rem;
            margin-bottom: 2rem;
            position: relative;
            overflow: hidden;
        }
        .hero-wrap::before {
            content: '';
            position: absolute; inset: 0;
            background: radial-gradient(ellipse at 70% 50%, rgba(59,130,246,0.15) 0%, transparent 65%);
            pointer-events: none;
        }
        .hero-title {
            font-size: 2.6rem; font-weight: 900; color: #f1f5f9;
            line-height: 1.15; margin: 0 0 0.6rem;
            letter-spacing: -0.02em;
        }
        .hero-sub {
            font-size: 1rem; color: #94a3b8; margin: 0 0 2rem;
            font-family: 'JetBrains Mono', monospace;
        }
        .nav-card {
            background: rgba(255,255,255,0.05);
            border: 1px solid rgba(255,255,255,0.1);
            border-radius: 12px; padding: 1.4rem 1.6rem;
            transition: background 0.2s, border-color 0.2s;
            height: 100%;
        }
        .nav-card:hover {
            background: rgba(59,130,246,0.12);
            border-color: rgba(59,130,246,0.4);
        }
        .nav-icon  { font-size: 1.8rem; margin-bottom: 0.5rem; }
        .nav-title { font-size: 1.05rem; font-weight: 700; color: #e2e8f0; margin-bottom: 0.3rem; }
        .nav-desc  { font-size: 0.84rem; color: #94a3b8; line-height: 1.5; }
        .badge {
            display: inline-block; background: #1d4ed8; color: #fff;
            border-radius: 4px; padding: 1px 8px; font-size: 0.72rem;
            font-family: 'JetBrains Mono', monospace; margin-left: 6px;
            vertical-align: middle;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        f"""
        <div class="hero-wrap">
            <div class="hero-title">📈 DUKA Stock Analysis Engine V5</div>
            <div class="hero-sub">V5-Pro · Personal Quant Research · AI-Powered · {runtime_label} Mode</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    c1, c2, c3 = st.columns(3, gap="large")

    with c1:
        st.markdown(
            """
            <div class="nav-card">
                <div class="nav-icon">🏠</div>
                <div class="nav-title">快速分析 <span class="badge">CORE</span></div>
                <div class="nav-desc">
                    多模态输入（文字/截图）→ 代码池勾选 →
                    GitHub 风格实时进度 → 完整报告同屏渲染
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with c2:
        st.markdown(
            """
            <div class="nav-card">
                <div class="nav-icon">🕰️</div>
                <div class="nav-title">历史记忆库</div>
                <div class="nav-desc">
                    按时间线回溯每次分析的完整报告快照，
                    支持同批次展开与跨期内容对比
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with c3:
        st.markdown(
            """
            <div class="nav-card">
                <div class="nav-icon">📈</div>
                <div class="nav-title">标的跟踪池</div>
                <div class="nav-desc">
                    查看单只股票的历史评分趋势、操作建议变化，
                    点击任意时间点查看当日完整报告
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.write("")
    st.info("👈 使用左侧导航栏切换页面，或直接点击上方卡片对应的菜单项开始使用。")


main()
