# -*- coding: utf-8 -*-
"""
webui/factor_extractor.py
=========================

从 AnalysisResult / Markdown 中提取可冻结的观察池因子。
仅做容错提取，不依赖 UI。
"""

from __future__ import annotations

import re
from typing import Any, Dict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from src.analyzer import AnalysisResult


def _dig(data: Dict[str, Any], *keys: str) -> Any:
    cur: Any = data
    for key in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def _first_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    text = text.replace(",", "")
    m = re.search(r"-?\d+(?:\.\d+)?", text)
    if not m:
        return None
    try:
        return float(m.group())
    except ValueError:
        return None


def _stringify_position_advice(value: Any) -> Optional[str]:
    if isinstance(value, dict):
        no_position = str(value.get("no_position", "")).strip()
        has_position = str(value.get("has_position", "")).strip()
        parts = []
        if no_position:
            parts.append(f"空仓：{no_position}")
        if has_position:
            parts.append(f"持仓：{has_position}")
        return " | ".join(parts) if parts else None
    text = str(value or "").strip()
    return text or None


def extract_factors(result: "AnalysisResult") -> Dict[str, Any]:
    dashboard = result.dashboard if isinstance(getattr(result, "dashboard", None), dict) else {}
    core = dashboard.get("core_conclusion", {}) if isinstance(dashboard, dict) else {}
    data_perspective = dashboard.get("data_perspective", {}) if isinstance(dashboard, dict) else {}
    trend = data_perspective.get("trend_status", {}) if isinstance(data_perspective, dict) else {}
    price = data_perspective.get("price_position", {}) if isinstance(data_perspective, dict) else {}
    volume = data_perspective.get("volume_analysis", {}) if isinstance(data_perspective, dict) else {}
    chip = data_perspective.get("chip_structure", {}) if isinstance(data_perspective, dict) else {}
    battle = dashboard.get("battle_plan", {}) if isinstance(dashboard, dict) else {}
    sniper = battle.get("sniper_points", {}) if isinstance(battle, dict) else {}

    factors: Dict[str, Any] = {
        "trend_prediction": getattr(result, "trend_prediction", None),
        "current_price": _first_float(getattr(result, "current_price", None) or price.get("current_price")),
        "change_pct": _first_float(getattr(result, "change_pct", None)),
        "ma_alignment": trend.get("ma_alignment"),
        "buy_point": _first_float(sniper.get("ideal_buy") or sniper.get("secondary_buy")),
        "stop_loss": _first_float(sniper.get("stop_loss")),
        "target_price": _first_float(sniper.get("take_profit")),
        "position_advice": _stringify_position_advice(core.get("position_advice")),
        "bias_rate": _first_float(price.get("bias_ma5")),
        "volume_ratio": _first_float(volume.get("volume_ratio")),
        "turnover_rate": _first_float(volume.get("turnover_rate")),
        "chip_profit_ratio": _first_float(chip.get("profit_ratio")),
        "time_sensitivity": core.get("time_sensitivity"),
        "sentiment_score": getattr(result, "sentiment_score", None),
        "operation_advice": getattr(result, "operation_advice", None),
    }
    return {k: v for k, v in factors.items() if v not in (None, "")}


def enrich_from_md(factors: Dict[str, Any], report_md: str) -> Dict[str, Any]:
    text = report_md or ""
    enriched = dict(factors or {})

    patterns = {
        "stop_loss": [
            r"止损(?:位|价)?[:：]\s*([^\n|]+)",
            r"🛑\s*止损(?:位|价)?[:：]?\s*([^\n|]+)",
        ],
        "target_price": [
            r"(?:目标位|目标价|止盈位|止盈价)[:：]\s*([^\n|]+)",
        ],
        "buy_point": [
            r"(?:理想买入点|买入点|ideal_buy)[:：]\s*([^\n|]+)",
        ],
        "bias_rate": [
            r"乖离率[:：]?\s*([+-]?\d+(?:\.\d+)?)\s*%",
        ],
        "volume_ratio": [
            r"量比[:：]?\s*([+-]?\d+(?:\.\d+)?)",
        ],
        "turnover_rate": [
            r"换手率[:：]?\s*([+-]?\d+(?:\.\d+)?)\s*%",
        ],
        "chip_profit_ratio": [
            r"(?:获利比例|获利盘比例)[:：]?\s*([+-]?\d+(?:\.\d+)?)\s*%",
        ],
        "ma_alignment": [
            r"均线排列[:：]\s*([^\n|]+)",
        ],
        "time_sensitivity": [
            r"时效性[:：]\s*([^\n|]+)",
        ],
    }

    for key, regex_list in patterns.items():
        if enriched.get(key) not in (None, ""):
            continue
        for pattern in regex_list:
            m = re.search(pattern, text, flags=re.IGNORECASE)
            if not m:
                continue
            raw = m.group(1).strip()
            if key in {"ma_alignment", "time_sensitivity"}:
                enriched[key] = raw
            else:
                value = _first_float(raw)
                if value is not None:
                    enriched[key] = value
            if enriched.get(key) not in (None, ""):
                break

    return enriched
