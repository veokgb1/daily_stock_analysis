# -*- coding: utf-8 -*-
"""Shared Streamlit UI guards for SCC deployment."""

from __future__ import annotations

import hmac

import streamlit as st

from src.config import get_streamlit_secret


def render_sidebar_branding() -> None:
    """Render the shared V5 sidebar branding."""
    with st.sidebar:
        st.markdown("## 🚀 DUKA Stock Analysis Engine V5")
        st.caption("Current Version: V5.0.1 (Cloud Ready)")
        st.divider()


def enforce_sidebar_password_gate() -> None:
    """
    Require a sidebar password only when APP_PASSWORD is configured in Streamlit secrets.
    Local/dev mode without APP_PASSWORD bypasses the guard automatically.
    """
    render_sidebar_branding()

    expected_password = get_streamlit_secret("APP_PASSWORD")
    if not expected_password:
        return

    auth_key = "_app_password_authenticated"
    error_key = "_app_password_error"
    input_key = "_app_password_input"

    if st.session_state.get(auth_key):
        return

    with st.sidebar:
        st.markdown("### Login Shield")
        submitted = st.text_input(
            "访问密码",
            type="password",
            key=input_key,
            help="已配置 APP_PASSWORD，输入正确密码后才显示应用内容。",
        )
        if submitted:
            if hmac.compare_digest(submitted, expected_password):
                st.session_state[auth_key] = True
                st.session_state[error_key] = ""
                st.session_state[input_key] = ""
                st.rerun()
            else:
                st.session_state[error_key] = "密码不正确，请重试。"

        if st.session_state.get(error_key):
            st.error(st.session_state[error_key])

    st.warning("请先在侧边栏输入访问密码。")
    st.stop()
