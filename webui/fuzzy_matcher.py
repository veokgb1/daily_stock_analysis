# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Dict, List

# pypinyin 是可选依赖；缺失时降级为空字符串，搜索功能仍可用（拼音维度失效）
try:
    from pypinyin import Style, lazy_pinyin as _lazy_pinyin  # type: ignore
    def _pinyin_initials(text: str) -> str:
        return "".join(_lazy_pinyin(text or "", style=Style.FIRST_LETTER)).upper()
    def _pinyin_full(text: str) -> str:
        return "".join(_lazy_pinyin(text or "")).lower()
except ImportError:
    def _pinyin_initials(text: str) -> str:  # type: ignore[misc]
        return ""
    def _pinyin_full(text: str) -> str:  # type: ignore[misc]
        return ""

from src.data.stock_mapping import STOCK_NAME_MAP


_GENERIC_SUFFIXES = ("科技", "信息", "股份", "集团", "智能", "电子", "时代", "材料", "能源")


def _initials(text: str) -> str:
    return _pinyin_initials(text)


def _pinyin(text: str) -> str:
    return _pinyin_full(text)


def _strip_suffix(text: str) -> str:
    token = (text or "").strip()
    for suffix in _GENERIC_SUFFIXES:
        if token.endswith(suffix) and len(token) > len(suffix):
            return token[:-len(suffix)]
    return token


def suggest_stock_candidates(raw: str, limit: int = 5) -> List[Dict[str, str]]:
    token = (raw or "").strip()
    if not token:
        return []
    if not any("\u4e00" <= ch <= "\u9fff" for ch in token) and not token.isdigit():
        return []

    token_upper = token.upper()
    token_initials = _initials(token)
    token_pinyin = _pinyin(token)
    token_core = _strip_suffix(token)
    token_core_initials = _initials(token_core)
    token_core_pinyin = _pinyin(token_core)
    scored = []

    for code, name in STOCK_NAME_MAP.items():
        score = 0.0
        reason = ""
        name_initials = _initials(name)
        name_pinyin = _pinyin(name)
        name_core = _strip_suffix(name)
        name_core_initials = _initials(name_core)
        name_core_pinyin = _pinyin(name_core)

        if token == name or token_upper == code.upper():
            score = 100.0
            reason = "精确匹配"
        elif token and token in name:
            score = 92.0 - (len(name) - len(token)) * 1.5
            reason = "名称片段匹配"
        elif token_pinyin and token_pinyin in name_pinyin:
            score = 90.0
            reason = f"拼音整词匹配：{token_pinyin}"
        elif token_core_pinyin and token_core_pinyin in name_core_pinyin:
            score = 86.0
            reason = f"拼音核心词匹配：{token_core_pinyin}"
        elif token_initials and token_initials == name_initials:
            score = 88.0
            reason = f"拼音首字母完全匹配：{token_initials}"
        elif token_core_initials and token_core_initials == name_core_initials:
            score = 84.0
            reason = f"拼音核心首字母匹配：{token_core_initials}"
        elif token_initials and name_initials.startswith(token_initials):
            score = 82.0
            reason = f"拼音前缀匹配：{token_initials}"

        if score >= 82.0:
            scored.append({
                "code": code,
                "name": name,
                "reason": reason,
                "_score": score,
            })

    scored.sort(key=lambda item: (-item["_score"], item["code"]))
    return [
        {
            "code": item["code"],
            "name": item["name"],
            "reason": item["reason"],
        }
        for item in scored[:limit]
    ]
