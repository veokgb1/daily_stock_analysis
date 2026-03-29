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


def _full_pinyin(text: str) -> str:
    try:
        from pypinyin import lazy_pinyin  # type: ignore

        return "".join(lazy_pinyin(text or "")).lower()
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
        self._pinyin_map: Dict[str, Dict[str, str]] = {m: {} for m in self.MARKETS}
        self._status: Dict[str, str] = {m: "idle" for m in self.MARKETS}
        self._errors: Dict[str, str] = {m: "" for m in self.MARKETS}
        self._snapshot_path = _SNAPSHOT_PATH
        self._legacy_snapshot_path = _LEGACY_SNAPSHOT_PATH
        self._last_refresh_date: Optional[str] = None
        self._refresh_inflight = False
        self._notices: Deque[str] = deque()
        self._runtime_mode = detect_runtime_mode()
        self._cloud_mode = self._runtime_mode == "cloud"

    def _build_search_indexes(self, market: str) -> None:
        idx: Dict[str, List[str]] = {}
        pinyin_map: Dict[str, str] = {}
        for code, name in self._codes[market].items():
            ini = _initials(name)
            full = _full_pinyin(name)
            if ini:
                idx.setdefault(ini, []).append(code)
            if full:
                pinyin_map[code] = full
        self._initials_idx[market] = idx
        self._pinyin_map[market] = pinyin_map

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
    ) -> Tuple[Dict[str, Dict[str, str]], Dict[str, Dict[str, List[str]]], Dict[str, Dict[str, str]]]:
        normalized: Dict[str, Dict[str, str]] = {m: {} for m in self.MARKETS}
        initials_idx: Dict[str, Dict[str, List[str]]] = {m: {} for m in self.MARKETS}
        pinyin_map: Dict[str, Dict[str, str]] = {m: {} for m in self.MARKETS}
        if not isinstance(payload, dict):
            return normalized, initials_idx, pinyin_map

        if payload and all(isinstance(k, str) and isinstance(v, str) for k, v in payload.items()):
            normalized["A"] = {
                self._normalize_code("A", code): str(name).strip()
                for code, name in payload.items()
                if str(code).strip() and str(name).strip()
            }
            return normalized, initials_idx, pinyin_map

        markets_payload = payload.get("markets", payload)
        if not isinstance(markets_payload, dict):
            return normalized, initials_idx, pinyin_map

        raw_initials = payload.get("initials_idx", {})
        raw_pinyin = payload.get("pinyin_map", {})

        for market, stocks in markets_payload.items():
            if market not in self.MARKETS or not isinstance(stocks, dict):
                continue
            parsed: Dict[str, str] = {}
            parsed_pinyin: Dict[str, str] = {}
            for code, name in stocks.items():
                code_text = self._normalize_code(market, str(code))
                if isinstance(name, dict):
                    name_text = str(name.get("name") or "").strip()
                    pinyin_text = str(
                        name.get("pinyin")
                        or name.get("full_pinyin")
                        or ""
                    ).strip().lower()
                else:
                    name_text = str(name or "").strip()
                    pinyin_text = ""
                if code_text and name_text:
                    parsed[code_text] = name_text
                    if pinyin_text:
                        parsed_pinyin[code_text] = pinyin_text
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
            market_pinyin = raw_pinyin.get(market, {}) if isinstance(raw_pinyin, dict) else {}
            if isinstance(market_pinyin, dict):
                parsed_pinyin.update(
                    {
                        self._normalize_code(market, str(code)): str(value).strip().lower()
                        for code, value in market_pinyin.items()
                        if str(code).strip() and str(value).strip()
                    }
                )
            pinyin_map[market] = parsed_pinyin
        return normalized, initials_idx, pinyin_map

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
                markets, initials_idx, pinyin_map = self._normalize_snapshot_payload(payload)
            except Exception as exc:
                logger.warning("[catalog] snapshot load failed for %s: %s", path.name, exc)
                continue

            loaded_markets: List[str] = []
            with self._lock:
                for market, stocks in markets.items():
                    if not stocks:
                        continue
                    self._codes[market] = stocks
                    self._initials_idx[market] = initials_idx.get(market, {})
                    self._pinyin_map[market] = pinyin_map.get(market, {})
                    if not self._initials_idx[market] or not self._pinyin_map[market]:
                        self._build_search_indexes(market)
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
            pinyin_map = {
                market: dict(index)
                for market, index in self._pinyin_map.items()
                if index
            }
        return {
            "version": 1,
            "updated_at": datetime.now().isoformat(timespec="seconds"),
            "markets": markets,
            "initials_idx": initials_idx,
            "pinyin_map": pinyin_map,
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
                self._build_search_indexes("A")
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
                self._build_search_indexes("HK")
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
                self._build_search_indexes("US")
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

        query_code = self._normalize_code(market, q)
        q_upper = q.upper()
        q_lower = q.lower()
        is_digit = q.isdigit()
        is_cn = any("\u4e00" <= ch <= "\u9fff" for ch in q)
        has_alpha = any(ch.isalpha() for ch in q)

        with self._lock:
            codes = self._codes.get(market, {})
            initials_idx = self._initials_idx.get(market, {})
            pinyin_map = self._pinyin_map.get(market, {})

        results: List[Dict[str, str]] = []
        seen = set()

        def _append(code: str, match_type: str) -> None:
            if code in seen or code not in codes or len(results) >= limit:
                return
            results.append({"code": code, "name": codes[code], "match_type": match_type})
            seen.add(code)

        if query_code in codes:
            _append(query_code, "code_exact")

        if (is_digit or market in {"HK", "US"}) and len(results) < limit:
            for code, name in codes.items():
                if len(results) >= limit:
                    break
                if code.upper().startswith(query_code.upper()):
                    _append(code, "code_prefix")

        if len(results) < limit:
            for code, name in codes.items():
                if len(results) >= limit:
                    break
                folded_name = str(name or "").lower()
                if q_lower == folded_name:
                    _append(code, "name_exact")
            for code, name in codes.items():
                if len(results) >= limit:
                    break
                folded_name = str(name or "").lower()
                if q_lower in folded_name:
                    _append(code, "name")

        if has_alpha and len(results) < limit:
            for code, pinyin in pinyin_map.items():
                if len(results) >= limit:
                    break
                if pinyin == q_lower:
                    _append(code, "pinyin_exact")
            for initials, code_list in initials_idx.items():
                if len(results) >= limit:
                    break
                if initials == q_upper:
                    for code in code_list:
                        if len(results) >= limit:
                            break
                        _append(code, "initials_exact")
            for code, pinyin in pinyin_map.items():
                if len(results) >= limit:
                    break
                if pinyin.startswith(q_lower) or q_lower in pinyin:
                    _append(code, "pinyin")
            for initials, code_list in initials_idx.items():
                if len(results) >= limit:
                    break
                if initials.startswith(q_upper):
                    for code in code_list:
                        if len(results) >= limit:
                            break
                        _append(code, "initials")

        return results[:limit]

    def lookup(self, code: str, market: str = "A") -> Optional[str]:
        with self._lock:
            return self._codes.get(market, {}).get(code)


_compat_catalog: Optional[CatalogStore] = None
_compat_lock = threading.Lock()


def _get_compat_catalog() -> CatalogStore:
    global _compat_catalog
    if _compat_catalog is not None:
        return _compat_catalog
    with _compat_lock:
        if _compat_catalog is None:
            store = CatalogStore()
            store.bootstrap()
            _compat_catalog = store
    return _compat_catalog


def is_search_ready(market: str = "A") -> bool:
    """Module-level compatibility shim for callers that resolved the module."""
    try:
        return _get_compat_catalog().is_search_ready(market)
    except Exception:
        return False


def error(market: str = "A") -> str:
    try:
        return _get_compat_catalog().error(market)
    except Exception:
        return ""


def status(market: str = "A") -> str:
    try:
        return _get_compat_catalog().status(market)
    except Exception:
        return "error"


def size(market: str = "A") -> int:
    try:
        return _get_compat_catalog().size(market)
    except Exception:
        return 0


def status_badge(market: str = "A") -> str:
    try:
        return _get_compat_catalog().status_badge(market)
    except Exception:
        return "⚪ 未加载"


def schedule_daily_a_refresh(today: Optional[str] = None) -> bool:
    try:
        return _get_compat_catalog().schedule_daily_a_refresh(today=today)
    except Exception:
        return False


def consume_notices() -> List[str]:
    try:
        return _get_compat_catalog().consume_notices()
    except Exception:
        return []


def search(query: str, market: str = "A", limit: int = 8) -> List[Dict]:
    try:
        return _get_compat_catalog().search(query, market=market, limit=limit)
    except Exception:
        return []


def lookup(code: str, market: str = "A") -> Optional[str]:
    try:
        return _get_compat_catalog().lookup(code, market=market)
    except Exception:
        return None


def is_loading(market: str = "A") -> bool:
    try:
        return _get_compat_catalog().status(market) == "loading"
    except Exception:
        return False


def get_progress(market: str = "A") -> float:
    try:
        return 1.0 if _get_compat_catalog().is_search_ready(market) else 0.0
    except Exception:
        return 0.0
