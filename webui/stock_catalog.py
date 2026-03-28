# -*- coding: utf-8 -*-
"""
webui/stock_catalog.py  v2.0
==============================
纯内存股票目录 ── Input Wizard 的搜索后端

设计原则
  · 启动快：优先读取 data/stock_snapshot.pkl（兼容 data/stock_catalog.pkl）
  · 常驻快：目录实例挂在 Streamlit @st.cache_resource，进程不关就不重复读盘
  · 更新轻：A 股快照每日最多后台检查一次，发现新代码后静默覆盖快照
  · 零阻塞：前台始终使用当前内存中的旧数据，后台联网不阻断搜索
  · 兜底优先：catalog 查不到 ≠ 代码无效，调用方自行决定是否拦截
"""

from __future__ import annotations

import logging
import pickle
import threading
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Deque, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
_SNAPSHOT_PATH = _DATA_DIR / "stock_snapshot.pkl"
_LEGACY_SNAPSHOT_PATH = _DATA_DIR / "stock_catalog.pkl"

# ─────────────────────────────────────────────────────────────────────────────
# 拼音首字母（离线计算，不依赖网络）
# ─────────────────────────────────────────────────────────────────────────────

def _initials(text: str) -> str:
    """提取汉字拼音首字母，如 '贵州茅台' → 'GZMT'。"""
    try:
        from pypinyin import Style, lazy_pinyin  # type: ignore
        return "".join(lazy_pinyin(text or "", style=Style.FIRST_LETTER)).upper()
    except Exception:
        return ""


# ─────────────────────────────────────────────────────────────────────────────
# CatalogStore：线程安全的内存目录
# ─────────────────────────────────────────────────────────────────────────────

class CatalogStore:
    """
    全市场股票代码目录（纯内存）。

    内部数据结构
    ─────────────
    _codes[market]       : Dict[code, name]       主字典，O(1) 精确查找
    _initials_idx[market]: Dict[initials, [codes]] 拼音首字母倒排索引
    _status[market]      : 'seeded' | 'loading' | 'ready' | 'error'

    搜索优先级（search 方法）
    ─────────────────────────
      P1  精确代码匹配
      P2  代码前缀（数字输入未满位时，如 "6005" → "600519…"）
      P3  名称片段（中文输入，LIKE 语义）
      P4  拼音首字母前缀（大写字母，如 "GZMT" → 贵州茅台）
    """

    MARKETS: Tuple[str, ...] = ("A", "HK", "US")

    # 各市场别名，用于 UI 显示
    MARKET_LABELS: Dict[str, str] = {
        "A":  "🇨🇳 A股",
        "HK": "🇭🇰 港股",
        "US": "🇺🇸 美股",
    }

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._codes:        Dict[str, Dict[str, str]]        = {m: {} for m in self.MARKETS}
        self._initials_idx: Dict[str, Dict[str, List[str]]]  = {m: {} for m in self.MARKETS}
        self._status:       Dict[str, str]                   = {m: "idle" for m in self.MARKETS}
        self._snapshot_path = _SNAPSHOT_PATH
        self._legacy_snapshot_path = _LEGACY_SNAPSHOT_PATH
        self._last_refresh_date: Optional[str] = None
        self._refresh_inflight = False
        self._notices: Deque[str] = deque()

    # ── 私有：构建拼音首字母倒排索引 ─────────────────────────────────────────

    def _build_initials_index(self, market: str) -> None:
        """调用前必须已持锁。"""
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

    def _normalize_snapshot_payload(self, payload: object) -> Dict[str, Dict[str, str]]:
        normalized: Dict[str, Dict[str, str]] = {m: {} for m in self.MARKETS}
        if not isinstance(payload, dict):
            return normalized

        # 兼容旧格式：{"600519": "贵州茅台", ...}
        if payload and all(isinstance(k, str) and isinstance(v, str) for k, v in payload.items()):
            normalized["A"] = {
                self._normalize_code("A", code): str(name).strip()
                for code, name in payload.items()
                if str(code).strip() and str(name).strip()
            }
            return normalized

        markets_payload = payload.get("markets", payload)
        if not isinstance(markets_payload, dict):
            return normalized

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
        return normalized

    def _load_snapshot(self) -> bool:
        for path in (self._snapshot_path, self._legacy_snapshot_path):
            if not path.exists():
                continue
            try:
                with path.open("rb") as fh:
                    payload = pickle.load(fh)
                markets = self._normalize_snapshot_payload(payload)
            except Exception as e:
                logger.warning("[catalog] 快照读取失败 %s：%s", path.name, e)
                continue

            loaded_markets: List[str] = []
            with self._lock:
                for market, stocks in markets.items():
                    if not stocks:
                        continue
                    self._codes[market] = stocks
                    self._build_initials_index(market)
                    self._status[market] = "seeded"
                    loaded_markets.append(f"{market}={len(stocks)}")
            if loaded_markets:
                logger.info("[catalog] 快照加载完成 %s：%s", path.name, ", ".join(loaded_markets))
                if path == self._legacy_snapshot_path:
                    try:
                        self._persist_snapshot()
                        path.unlink(missing_ok=True)
                    except Exception as e:
                        logger.warning("[catalog] 旧快照迁移失败 %s：%s", path.name, e)
                return True
        logger.info("[catalog] 未发现可用目录快照，等待后台首次同步")
        return False

    def _snapshot_payload(self) -> Dict[str, object]:
        with self._lock:
            markets = {
                market: dict(stocks)
                for market, stocks in self._codes.items()
                if stocks
            }
        return {
            "version": 1,
            "updated_at": datetime.now().isoformat(timespec="seconds"),
            "markets": markets,
        }

    def _persist_snapshot(self) -> None:
        self._ensure_snapshot_dir()
        payload = self._snapshot_payload()
        path = self._snapshot_path
        tmp_path = path.with_suffix(f"{path.suffix}.tmp")
        with tmp_path.open("wb") as fh:
            pickle.dump(payload, fh, protocol=pickle.HIGHEST_PROTOCOL)
        tmp_path.replace(path)

    def _push_notice(self, message: str) -> None:
        with self._lock:
            self._notices.append(message)
            while len(self._notices) > 8:
                self._notices.popleft()

    # ── 私有：各市场加载函数（后台线程执行）──────────────────────────────────

    def _fetch_a_shares(self) -> Dict[str, str]:
        stocks: Dict[str, str] = {}
        import akshare as ak  # type: ignore

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
        """
        A 股后台静默刷新：
          1. 联网获取最新 A 股代码列表
          2. 仅当代码数量或集合发生变化时，覆盖内存并写回快照
          3. 全程不阻断前台搜索
        """
        with self._lock:
            self._status["A"] = "loading"

        try:
            stocks = self._fetch_a_shares()
        except Exception as e:
            logger.warning("[catalog] A股后台更新失败，继续使用内存快照：%s", e)
            with self._lock:
                self._status["A"] = "ready" if self._codes["A"] else "error"
                self._refresh_inflight = False
            return

        if not stocks:
            with self._lock:
                self._status["A"] = "ready" if self._codes["A"] else "error"
                self._refresh_inflight = False
            return

        changed = False
        with self._lock:
            current_codes = self._codes["A"]
            current_key_set = set(current_codes)
            fresh_key_set = set(stocks)
            if len(current_codes) != len(stocks) or current_key_set != fresh_key_set:
                self._codes["A"] = stocks
                self._build_initials_index("A")
                changed = True
            self._status["A"] = "ready"
            self._refresh_inflight = False

        if changed:
            self._persist_snapshot()
            self._push_notice("股票字典已自动更新至最新版本")
            logger.info("[catalog] A股快照已更新：%d → %d", len(current_codes), len(stocks))
        else:
            logger.info("[catalog] A股快照已是最新：%d 只", len(stocks))

    def _load_hk_shares(self) -> None:
        """
        港股加载：akshare.stock_hk_spot_em
        代码格式：5 位数字（如 00700 → 腾讯控股）
        """
        with self._lock:
            self._status["HK"] = "loading"
        stocks: Dict[str, str] = {}
        try:
            import akshare as ak  # type: ignore
            df = ak.stock_hk_spot_em()
            code_col = next(
                (c for c in ["代码", "code", "股票代码"] if c in df.columns), None
            )
            name_col = next(
                (c for c in ["名称", "name", "股票简称"] if c in df.columns), None
            )
            if code_col and name_col:
                for _, row in df.iterrows():
                    code = self._normalize_code("HK", str(row[code_col]))
                    name = str(row[name_col]).strip()
                    if code and name:
                        stocks[code] = name
            logger.info("[catalog] 港股加载完成：%d 只", len(stocks))
        except Exception as e:
            logger.warning("[catalog] 港股加载失败：%s", e)

        with self._lock:
            if stocks:
                self._codes["HK"] = stocks
                self._build_initials_index("HK")
            self._status["HK"] = "ready" if stocks else "error"

    def _load_us_shares(self) -> None:
        """
        美股加载：akshare.stock_us_spot_em
        代码格式：字母 ticker（如 AAPL）
        """
        with self._lock:
            self._status["US"] = "loading"
        stocks: Dict[str, str] = {}
        try:
            import akshare as ak  # type: ignore
            df = ak.stock_us_spot_em()
            code_col = next(
                (c for c in ["代码", "code", "名称代码"] if c in df.columns), None
            )
            name_col = next(
                (c for c in ["名称", "name", "英文名称"] if c in df.columns), None
            )
            if code_col and name_col:
                for _, row in df.iterrows():
                    code = self._normalize_code("US", str(row[code_col]))
                    name = str(row[name_col]).strip()
                    if code and name:
                        stocks[code] = name
            logger.info("[catalog] 美股加载完成：%d 只", len(stocks))
        except Exception as e:
            logger.warning("[catalog] 美股加载失败：%s", e)

        with self._lock:
            if stocks:
                self._codes["US"] = stocks
                self._build_initials_index("US")
            self._status["US"] = "ready" if stocks else "error"

    # ── 公共：启动（由 @st.cache_resource 包装后调用一次）───────────────────

    def bootstrap(self) -> None:
        """
        两阶段启动，保证"快照秒开 + 后台静默补全"：

        阶段 1（同步）：
          优先读取 data/stock_snapshot.pkl；若不存在，再兼容读取 data/stock_catalog.pkl。

        阶段 2（异步）：
          港股 / 美股仍在后台加载；A 股是否联网检查由页面按“每日一次”策略触发。
        """
        self._ensure_snapshot_dir()
        self._load_snapshot()

        # 阶段 2：后台补齐港股 / 美股；A股改为每日一次静默检查
        for loader in (self._load_hk_shares, self._load_us_shares):
            t = threading.Thread(target=loader, daemon=True, name=f"catalog_{loader.__name__}")
            t.start()

    # ── 公共：状态查询 ────────────────────────────────────────────────────────

    def status(self, market: str) -> str:
        """返回市场加载状态：idle / seeded / loading / ready / error"""
        with self._lock:
            return self._status.get(market, "idle")

    def size(self, market: str) -> int:
        """返回市场当前内存中的股票数量。"""
        with self._lock:
            return len(self._codes.get(market, {}))

    def status_badge(self, market: str) -> str:
        """
        返回适合 UI 展示的简洁状态徽章字符串，如 "🟢 5183只·已就绪"
        """
        s = self.status(market)
        n = self.size(market)
        if s in ("seeded", "ready"):
            return f"🟢 {n:,}只·{'热更新完成' if s == 'ready' else '快照就绪'}"
        if s == "loading":
            return f"🟡 {n:,}只·加载中…"
        if s == "error":
            return f"🔴 加载失败" if n == 0 else f"🟠 {n:,}只·部分"
        return "⚪ 未加载"

    def schedule_daily_a_refresh(self, today: Optional[str] = None) -> bool:
        """
        每个进程每天最多启动一次 A 股后台更新线程。
        返回 True 表示本次成功触发线程，False 表示已检查过或已有线程在跑。
        """
        refresh_date = today or datetime.now().date().isoformat()
        with self._lock:
            if self._refresh_inflight or self._last_refresh_date == refresh_date:
                return False
            self._last_refresh_date = refresh_date
            self._refresh_inflight = True
        t = threading.Thread(
            target=self._load_a_shares,
            daemon=True,
            name="catalog_refresh_a_daily",
        )
        t.start()
        return True

    def consume_notices(self) -> List[str]:
        with self._lock:
            notices = list(self._notices)
            self._notices.clear()
        return notices

    # ── 公共：核心搜索 ────────────────────────────────────────────────────────

    def search(
        self,
        query: str,
        market: str = "A",
        limit: int = 8,
    ) -> List[Dict]:
        """
        纯内存多维搜索，典型耗时 < 1ms（5000+ 条数据）。

        返回格式：
          [{"code": "600519", "name": "贵州茅台", "match_type": "code_exact"}]

        match_type 取值：
          code_exact   精确代码
          code_prefix  代码前缀
          name         名称片段
          initials     拼音首字母前缀

        兜底说明：
          若 query 是合法格式但 catalog 无记录，返回空列表。
          调用方不应因此拦截用户输入，而应允许其直接进入分析流程。
        """
        q = (query or "").strip()
        if not q:
            return []

        q_upper = q.upper()
        is_digit = q.isdigit()
        is_cn    = any("\u4e00" <= ch <= "\u9fff" for ch in q)
        is_alpha = q_upper.isalpha()

        # 线程安全：复制引用后立即释放锁，搜索在局部副本上进行
        with self._lock:
            codes       = self._codes.get(market, {})
            initials_idx = self._initials_idx.get(market, {})

        results: List[Dict] = []
        seen: set = set()

        # ── P1：精确代码匹配 ─────────────────────────────────────────────────
        if q in codes:
            results.append({"code": q, "name": codes[q], "match_type": "code_exact"})
            seen.add(q)

        # ── P2：代码前缀（数字输入，未满位或已精确但还有余量）──────────────
        if is_digit and len(results) < limit:
            for code, name in codes.items():
                if len(results) >= limit:
                    break
                if code.startswith(q) and code not in seen:
                    results.append({"code": code, "name": name, "match_type": "code_prefix"})
                    seen.add(code)

        # ── P3：名称片段（中文输入）──────────────────────────────────────────
        if is_cn and len(results) < limit:
            for code, name in codes.items():
                if len(results) >= limit:
                    break
                if q in name and code not in seen:
                    results.append({"code": code, "name": name, "match_type": "name"})
                    seen.add(code)

        # ── P4：拼音首字母前缀（纯大写字母输入）────────────────────────────
        if is_alpha and len(results) < limit:
            for ini, code_list in initials_idx.items():
                if len(results) >= limit:
                    break
                if ini.startswith(q_upper):
                    for code in code_list:
                        if len(results) >= limit:
                            break
                        if code not in seen and code in codes:
                            results.append({
                                "code": code,
                                "name": codes[code],
                                "match_type": "initials",
                            })
                            seen.add(code)

        return results[:limit]

    def lookup(self, code: str, market: str = "A") -> Optional[str]:
        """精确代码 → 名称，不存在返回 None。"""
        with self._lock:
            return self._codes.get(market, {}).get(code)
