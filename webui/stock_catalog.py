# -*- coding: utf-8 -*-
"""
webui/stock_catalog.py  v1.0
==============================
纯内存股票目录 ── Input Wizard 的搜索后端

设计原则
  · 零 SQLite 写入，数据库保持绝对干净
  · 启动立即可用：同步加载本地 STOCK_NAME_MAP 种子（< 50ms）
  · 后台并发：三条守护线程分别拉取 A / HK / US 全量数据
  · 零延迟搜索：所有匹配在内存字典 + 预计算拼音首字母索引中进行
  · 兜底优先：catalog 查不到 ≠ 代码无效，调用方自行决定是否拦截

市场扩展
  · market 字符串统一为 "A" / "HK" / "US"
  · 新增市场只需实现 _load_xx() 并在 bootstrap() 里启动线程即可

线程安全
  · 读写均持 RLock；搜索时 copy 局部引用后立即释放锁
  · Streamlit @st.cache_resource 友好（同一进程共享单例）
"""

from __future__ import annotations

import logging
import threading
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

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

    # ── 私有：构建拼音首字母倒排索引 ─────────────────────────────────────────

    def _build_initials_index(self, market: str) -> None:
        """调用前必须已持锁。"""
        idx: Dict[str, List[str]] = {}
        for code, name in self._codes[market].items():
            ini = _initials(name)
            if ini:
                idx.setdefault(ini, []).append(code)
        self._initials_idx[market] = idx

    # ── 私有：各市场加载函数（后台线程执行）──────────────────────────────────

    def _load_a_shares(self) -> None:
        """
        A 股加载策略：
          1. 尝试 akshare.stock_info_a_code_name（全量 ~5500 只，免费无 key）
          2. 失败 → 已在 bootstrap() 里用本地种子预填，此处静默退出
        """
        with self._lock:
            self._status["A"] = "loading"
        stocks: Dict[str, str] = {}
        try:
            import akshare as ak  # type: ignore
            df = ak.stock_info_a_code_name()
            # 兼容多版本列名
            code_col = next(
                (c for c in ["code", "股票代码", "代码"] if c in df.columns), None
            )
            name_col = next(
                (c for c in ["name", "股票简称", "名称"] if c in df.columns), None
            )
            if code_col and name_col:
                for _, row in df.iterrows():
                    code = str(row[code_col]).strip().zfill(6)
                    name = str(row[name_col]).strip()
                    if code and name:
                        stocks[code] = name
            if stocks:
                logger.info("[catalog] A股 akshare 热更新完成：%d 只", len(stocks))
        except Exception as e:
            logger.warning("[catalog] A股 akshare 加载失败，继续使用本地种子：%s", e)

        if stocks:  # 只在获取到数据时才覆盖（保留种子）
            with self._lock:
                self._codes["A"] = stocks
                self._build_initials_index("A")
                self._status["A"] = "ready"
        else:
            with self._lock:
                # 本地种子已在 bootstrap 写入，仅标记状态
                self._status["A"] = "ready" if self._codes["A"] else "error"

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
                    code = str(row[code_col]).strip()
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
                    code = str(row[code_col]).strip()
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
        两阶段启动，保证"立即可用 + 后台更新"：

        阶段 1（同步，< 50ms）：
          从项目内置 STOCK_NAME_MAP 写入 A 股内存字典，
          并立即构建拼音索引。Input Wizard 在页面完成首次渲染前就已就绪。

        阶段 2（异步，后台守护线程）：
          并发启动 3 条线程，分别调用 akshare 更新 A / HK / US 全量数据。
          失败时静默，不阻断主线程，不影响页面加载速度。
        """
        # 阶段 1：立即同步种子
        try:
            from src.data.stock_mapping import STOCK_NAME_MAP  # type: ignore
            with self._lock:
                self._codes["A"] = dict(STOCK_NAME_MAP)
                self._build_initials_index("A")
                self._status["A"] = "seeded"
            logger.info(
                "[catalog] 本地种子立即加载：%d 只 A股，Input Wizard 已就绪",
                len(self._codes["A"]),
            )
        except Exception as e:
            logger.warning("[catalog] 本地种子加载失败：%s", e)

        # 阶段 2：后台并发热更新
        for loader in (self._load_a_shares, self._load_hk_shares, self._load_us_shares):
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
            return f"🟢 {n:,}只·{'热更新完成' if s == 'ready' else '种子就绪'}"
        if s == "loading":
            return f"🟡 {n:,}只·加载中…"
        if s == "error":
            return f"🔴 加载失败" if n == 0 else f"🟠 {n:,}只·部分"
        return "⚪ 未加载"

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
