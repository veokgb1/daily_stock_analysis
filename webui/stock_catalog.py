# -*- coding: utf-8 -*-
"""
In-memory stock catalog used by the Streamlit searchbox.

Key behavior:
- Local mode: snapshot-first bootstrap, then background refresh/load.
- Cloud mode: only load `data/stock_catalog.pkl`; never build pinyin initials
  from scratch and never trigger remote catalog refresh.
"""

from __future__ import annotations

import logging
import pickle
import threading
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Deque, Dict, List, Optional, Tuple

from src.config import detect_runtime_mode

logger = logging.getLogger(__name__)

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
_SNAPSHOT_PATH = _DATA_DIR / "stock_snapshot.pkl"
_LEGACY_SNAPSHOT_PATH = _DATA_DIR / "stock_catalog.pkl"


def _initials(text: str) -> str:
    try:
        from pypinyin import Style, lazy_pinyin  # type: ignore

        return "".join(lazy_pinyin(text or "", style=Style.FIRST_LETTER)).upper()
    except Exception:
        return ""


class CatalogStore:
    MARKETS: Tuple[str, ...] = ("A", "HK", "US")
    MARKET_LABELS: Dict[str, str] = {
        "A": "A股",
        "HK": "港股",
        "US": "美股",
    }

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._codes: Dict[str, Dict[str, str]] = {m: {} for m in self.MARKETS}
        self._initials_idx: Dict[str, Dict[str, List[str]]] = {m: {} for m in self.MARKETS}
        self._status: Dict[str, str] = {m: "idle" for m in self.MARKETS}
        self._errors: Dict[str, str] = {m: "" for m in self.MARKETS}
        self._snapshot_path = _SNAPSHOT_PATH
        self._legacy_snapshot_path = _LEGACY_SNAPSHOT_PATH
        self._last_refresh_date: Optional[str] = None
        self._refresh_inflight = False
        self._notices: Deque[str] = deque()
        self._runtime_mode = detect_runtime_mode()
        self._cloud_mode = self._runtime_mode == "cloud"

    def _build_initials_index(self, market: str) -> None:
        idx: Dict[str, List[str]] = {}
        for code, name in self._codes[market].items():
            ini = _initials(name)
            if ini:
                idx.setdefault(ini, []).append(code)
        self._initials_idx[market] = idx

    def _normalize_code(self, market: str, code: str) -> str:
        text = str(code or "").strip()
        if market == "A" and text.isdigit():
            return text.zfill(6)
        if market == "HK" and text.isdigit():
            return text.zfill(5)
        return text.upper() if market == "US" else text

    def _ensure_snapshot_dir(self) -> None:
        self._snapshot_path.parent.mkdir(parents=True, exist_ok=True)

    def _normalize_snapshot_payload(
        self,
        payload: object,
    ) -> Tuple[Dict[str, Dict[str, str]], Dict[str, Dict[str, List[str]]]]:
        normalized: Dict[str, Dict[str, str]] = {m: {} for m in self.MARKETS}
        initials_idx: Dict[str, Dict[str, List[str]]] = {m: {} for m in self.MARKETS}
        if not isinstance(payload, dict):
            return normalized, initials_idx

        if payload and all(isinstance(k, str) and isinstance(v, str) for k, v in payload.items()):
            normalized["A"] = {
                self._normalize_code("A", code): str(name).strip()
                for code, name in payload.items()
                if str(code).strip() and str(name).strip()
            }
            return normalized, initials_idx

        markets_payload = payload.get("markets", payload)
        if not isinstance(markets_payload, dict):
            return normalized, initials_idx

        raw_initials = payload.get("initials_idx", {})

        for market, stocks in markets_payload.items():
            if market not in self.MARKETS or not isinstance(stocks, dict):
                continue
            parsed: Dict[str, str] = {}
            for code, name in stocks.items():
                code_text = self._normalize_code(market, str(code))
                name_text = str(name or "").strip()
                if code_text and name_text:
                    parsed[code_text] = name_text
            normalized[market] = parsed

            market_initials = raw_initials.get(market, {}) if isinstance(raw_initials, dict) else {}
            if isinstance(market_initials, dict):
                initials_idx[market] = {
                    str(initials).upper(): [
                        self._normalize_code(market, code)
                        for code in codes
                        if str(code).strip()
                    ]
                    for initials, codes in market_initials.items()
                    if isinstance(codes, list)
                }
        return normalized, initials_idx

    def _load_snapshot(self) -> bool:
        candidate_paths = (
            (self._legacy_snapshot_path,)
            if self._cloud_mode
            else (self._snapshot_path, self._legacy_snapshot_path)
        )
        for path in candidate_paths:
            if not path.exists():
                continue
            try:
                with path.open("rb") as fh:
                    payload = pickle.load(fh)
                markets, initials_idx = self._normalize_snapshot_payload(payload)
            except Exception as exc:
                logger.warning("[catalog] snapshot load failed for %s: %s", path.name, exc)
                continue

            loaded_markets: List[str] = []
            with self._lock:
                for market, stocks in markets.items():
                    if not stocks:
                        continue
                    self._codes[market] = stocks
                    if initials_idx.get(market):
                        self._initials_idx[market] = initials_idx[market]
                    elif self._cloud_mode:
                        self._initials_idx[market] = {}
                    else:
                        self._build_initials_index(market)
                    self._status[market] = "seeded"
                    self._errors[market] = ""
                    loaded_markets.append(f"{market}={len(stocks)}")

            if loaded_markets:
                print(f"[catalog] snapshot loaded ({self._runtime_mode}): {path.name} -> {', '.join(loaded_markets)}")
                logger.info("[catalog] snapshot loaded from %s: %s", path.name, ", ".join(loaded_markets))
                if not self._cloud_mode and path == self._legacy_snapshot_path:
                    try:
                        self._persist_snapshot()
                        path.unlink(missing_ok=True)
                    except Exception as exc:
                        logger.warning("[catalog] legacy snapshot migration failed for %s: %s", path.name, exc)
                self._push_notice("键盘精灵已就绪，支持盲打")
                return True

        with self._lock:
            for market in self.MARKETS:
                self._errors[market] = (
                    "cloud 缺少 data/stock_catalog.pkl，搜索框暂不可用"
                    if self._cloud_mode
                    else "未找到可用股票快照文件"
                )
                self._status[market] = "error"
        logger.info("[catalog] no usable snapshot found; waiting for first sync")
        return False

    def _snapshot_payload(self) -> Dict[str, object]:
        with self._lock:
            markets = {
                market: dict(stocks)
                for market, stocks in self._codes.items()
                if stocks
            }
            initials_idx = {
                market: dict(index)
                for market, index in self._initials_idx.items()
                if index
            }
        return {
            "version": 1,
            "updated_at": datetime.now().isoformat(timespec="seconds"),
            "markets": markets,
            "initials_idx": initials_idx,
        }

    def _persist_snapshot(self) -> None:
        self._ensure_snapshot_dir()
        payload = self._snapshot_payload()
        for path in (self._snapshot_path, self._legacy_snapshot_path):
            tmp_path = path.with_suffix(f"{path.suffix}.tmp")
            with tmp_path.open("wb") as fh:
                pickle.dump(payload, fh, protocol=pickle.HIGHEST_PROTOCOL)
            tmp_path.replace(path)

    def _push_notice(self, message: str) -> None:
        with self._lock:
            self._notices.append(message)
            while len(self._notices) > 8:
                self._notices.popleft()

    def _fetch_a_shares(self) -> Dict[str, str]:
        import akshare as ak  # type: ignore

        stocks: Dict[str, str] = {}
        df = ak.stock_info_a_code_name()
        code_col = next((c for c in ["code", "股票代码", "代码"] if c in df.columns), None)
        name_col = next((c for c in ["name", "股票简称", "名称"] if c in df.columns), None)
        if code_col and name_col:
            for _, row in df.iterrows():
                code = self._normalize_code("A", str(row[code_col]))
                name = str(row[name_col]).strip()
                if code and name:
                    stocks[code] = name
        return stocks

    def _load_a_shares(self) -> None:
        with self._lock:
            self._status["A"] = "loading"

        try:
            stocks = self._fetch_a_shares()
        except Exception as exc:
            logger.warning("[catalog] A-share refresh failed: %s", exc)
            with self._lock:
                self._status["A"] = "ready" if self._codes["A"] else "error"
                self._errors["A"] = str(exc)
                self._refresh_inflight = False
            return

        if not stocks:
            with self._lock:
                self._status["A"] = "ready" if self._codes["A"] else "error"
                self._errors["A"] = "A-share refresh returned empty data"
                self._refresh_inflight = False
            return

        changed = False
        old_count = 0
        with self._lock:
            current_codes = self._codes["A"]
            old_count = len(current_codes)
            if len(current_codes) != len(stocks) or set(current_codes) != set(stocks):
                self._codes["A"] = stocks
                self._build_initials_index("A")
                changed = True
            self._status["A"] = "ready"
            self._errors["A"] = ""
            self._refresh_inflight = False

        if changed:
            self._persist_snapshot()
            self._push_notice("股票字典已自动更新至最新版本")
            logger.info("[catalog] A-share snapshot updated: %s -> %s", old_count, len(stocks))
        else:
            logger.info("[catalog] A-share snapshot already up to date: %s", len(stocks))

    def _load_hk_shares(self) -> None:
        with self._lock:
            self._status["HK"] = "loading"
        stocks: Dict[str, str] = {}
        try:
            import akshare as ak  # type: ignore

            df = ak.stock_hk_spot_em()
            code_col = next((c for c in ["代码", "code", "股票代码"] if c in df.columns), None)
            name_col = next((c for c in ["名称", "name", "股票简称"] if c in df.columns), None)
            if code_col and name_col:
                for _, row in df.iterrows():
                    code = self._normalize_code("HK", str(row[code_col]))
                    name = str(row[name_col]).strip()
                    if code and name:
                        stocks[code] = name
            logger.info("[catalog] HK snapshot loaded: %s", len(stocks))
        except Exception as exc:
            logger.warning("[catalog] HK snapshot load failed: %s", exc)
            with self._lock:
                self._errors["HK"] = str(exc)

        with self._lock:
            if stocks:
                self._codes["HK"] = stocks
                self._build_initials_index("HK")
                self._errors["HK"] = ""
            self._status["HK"] = "ready" if stocks else "error"

    def _load_us_shares(self) -> None:
        with self._lock:
            self._status["US"] = "loading"
        stocks: Dict[str, str] = {}
        try:
            import akshare as ak  # type: ignore

            df = ak.stock_us_spot_em()
            code_col = next((c for c in ["代码", "code", "名称代码"] if c in df.columns), None)
            name_col = next((c for c in ["名称", "name", "英文名称"] if c in df.columns), None)
            if code_col and name_col:
                for _, row in df.iterrows():
                    code = self._normalize_code("US", str(row[code_col]))
                    name = str(row[name_col]).strip()
                    if code and name:
                        stocks[code] = name
            logger.info("[catalog] US snapshot loaded: %s", len(stocks))
        except Exception as exc:
            logger.warning("[catalog] US snapshot load failed: %s", exc)
            with self._lock:
                self._errors["US"] = str(exc)

        with self._lock:
            if stocks:
                self._codes["US"] = stocks
                self._build_initials_index("US")
                self._errors["US"] = ""
            self._status["US"] = "ready" if stocks else "error"

    def bootstrap(self) -> None:
        self._ensure_snapshot_dir()
        print(f"[catalog] bootstrap mode={self._runtime_mode}")
        self._load_snapshot()

        if self._cloud_mode:
            return

        for loader in (self._load_hk_shares, self._load_us_shares):
            thread = threading.Thread(target=loader, daemon=True, name=f"catalog_{loader.__name__}")
            thread.start()

    def status(self, market: str) -> str:
        with self._lock:
            return self._status.get(market, "idle")

    def size(self, market: str) -> int:
        with self._lock:
            return len(self._codes.get(market, {}))

    def error(self, market: str) -> str:
        with self._lock:
            return self._errors.get(market, "")

    def is_search_ready(self, market: str) -> bool:
        with self._lock:
            return bool(self._codes.get(market)) and self._status.get(market) in {"seeded", "ready"}

    def status_badge(self, market: str) -> str:
        status = self.status(market)
        count = self.size(market)
        if status in {"seeded", "ready"}:
            suffix = "热更新完成" if status == "ready" else "快照就绪"
            return f"🟢 {count:,}只 · {suffix}"
        if status == "loading":
            return f"🟡 {count:,}只 · 正在载入本地股票雷达快照..."
        if status == "error":
            detail = self.error(market) or "加载失败"
            return f"🔴 {detail}" if count == 0 else f"🟠 {count:,}只 · 部分可用"
        return "⚪ 未加载"

    def schedule_daily_a_refresh(self, today: Optional[str] = None) -> bool:
        if self._cloud_mode:
            return False
        refresh_date = today or datetime.now().date().isoformat()
        with self._lock:
            if self._refresh_inflight or self._last_refresh_date == refresh_date:
                return False
            self._last_refresh_date = refresh_date
            self._refresh_inflight = True
        thread = threading.Thread(
            target=self._load_a_shares,
            daemon=True,
            name="catalog_refresh_a_daily",
        )
        thread.start()
        return True

    def consume_notices(self) -> List[str]:
        with self._lock:
            notices = list(self._notices)
            self._notices.clear()
        return notices

    def search(self, query: str, market: str = "A", limit: int = 8) -> List[Dict]:
        q = (query or "").strip()
        if not q:
            return []

        q_upper = q.upper()
        is_digit = q.isdigit()
        is_cn = any("\u4e00" <= ch <= "\u9fff" for ch in q)
        is_alpha = q_upper.isalpha()

        with self._lock:
            codes = self._codes.get(market, {})
            initials_idx = self._initials_idx.get(market, {})

        results: List[Dict[str, str]] = []
        seen = set()

        if q in codes:
            results.append({"code": q, "name": codes[q], "match_type": "code_exact"})
            seen.add(q)

        if is_digit and len(results) < limit:
            for code, name in codes.items():
                if len(results) >= limit:
                    break
                if code.startswith(q) and code not in seen:
                    results.append({"code": code, "name": name, "match_type": "code_prefix"})
                    seen.add(code)

        if is_cn and len(results) < limit:
            for code, name in codes.items():
                if len(results) >= limit:
                    break
                if q in name and code not in seen:
                    results.append({"code": code, "name": name, "match_type": "name"})
                    seen.add(code)

        if is_alpha and len(results) < limit:
            for initials, code_list in initials_idx.items():
                if len(results) >= limit:
                    break
                if initials.startswith(q_upper):
                    for code in code_list:
                        if len(results) >= limit:
                            break
                        if code not in seen and code in codes:
                            results.append({"code": code, "name": codes[code], "match_type": "initials"})
                            seen.add(code)

        return results[:limit]

    def lookup(self, code: str, market: str = "A") -> Optional[str]:
        with self._lock:
            return self._codes.get(market, {}).get(code)
