# -*- coding: utf-8 -*-
"""
webui/extract_engine.py — 纯 Gemini 提取引擎 Match5
==================================================

V8.1 修复点：
  [漏单修复] 彻底废除 "name|code" 逐行切分格式。
             _MAPPING_PROMPT 强制要求 LLM 返回 JSON 数组，
             Python 端用 json.loads() 解析，杜绝 | 误识别导致的漏单。
             三级容错：直接解析 → 正则抠 JSON 块 → 正则提6位代码。

  [谐音修复] 在文本/语音映射 Prompt 中增加严肃的谐音纠错规则，
             语音渠道使用独立的 _VOICE_MAPPING_PROMPT，
             要求 LLM 利用 A 股知识库智能推断并纠正同音错字。

  [图片修复] all_texts 改用换行符拼接（不再用 | ），
             不会干扰后续 JSON 解析。

设计红线（不变）：
  · 绝对禁止 pytesseract / easyocr
  · 网络调用统一用 openai 兼容客户端，base_url 指向 Gemini 兼容端点
  · 函数入口按运行环境决定是否注入代理
  · 标准数据契约：List[{"name":str, "code":str, "valid":bool, "source":str}]
  · 语音转写防幻觉：转写失败时返回空串，绝不捏造代码
"""

import base64
import json
import logging
import os
import re
import sys
from io import BytesIO
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

_GEMINI_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/openai/"
_GEMINI_MODEL    = "gemini-2.5-flash"  # Match5：写死新模型，禁止回退

StockItem = Dict  # {"name": str, "code": str, "valid": bool, "source": str}


# =============================================================================
# 代理注入（方案 A 保底）
# =============================================================================

def ensure_proxy() -> None:
    """仅在本地命中指定网关时注入代理；云端默认直连。"""
    if os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy"):
        return
    gateway_ip = os.getenv("GATEWAY_IP", "").strip()
    if gateway_ip != "10.10.10.252":
        return
    host = os.getenv("PROXY_HOST", gateway_ip).strip()
    port = os.getenv("PROXY_PORT", "").strip()
    if host and port:
        proxy_url = f"http://{host}:{port}"
        for k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
            os.environ[k] = proxy_url
        logger.info(f"[ExtractEngine] 本地代理已注入: {proxy_url}")


# =============================================================================
# Gemini 兼容客户端工厂（方案 B 主路）
# =============================================================================

def _get_client():
    """返回以 OpenAI 兼容格式访问 Gemini 的客户端，底层 httpx 遵守 HTTPS_PROXY。"""
    ensure_proxy()
    _root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if _root not in sys.path:
        sys.path.insert(0, _root)
    from src.config import setup_env
    setup_env()
    from src.config import get_config
    cfg     = get_config()
    api_key = getattr(cfg, "gemini_api_key", None) or os.getenv("GEMINI_API_KEY", "")
    if not api_key:
        logger.warning("[ExtractEngine] 未找到 GEMINI_API_KEY")
        return None
    try:
        import openai
        return openai.OpenAI(api_key=api_key, base_url=_GEMINI_BASE_URL)
    except Exception as exc:
        logger.error(f"[ExtractEngine] 客户端创建失败: {exc}")
        return None


# =============================================================================
# 辅助函数
# =============================================================================

def _regex_extract(text: str) -> List[str]:
    """正则兜底：从任意文本提取6位代码，去重保序。"""
    seen, result = set(), []
    for raw in re.findall(r"\b(\d{6})\b", text):
        if raw not in seen and raw != "000000":
            seen.add(raw); result.append(raw)
    return result


def _make_item(name: str, code: str, source: str, valid: bool = True) -> StockItem:
    return {"name": name, "code": code, "valid": valid, "source": source}


def _items_from_parsed(parsed: Optional[list], source: str) -> List[StockItem]:
    """将 LLM 解析后的 JSON 列表标准化为 StockItem 列表。"""
    items: List[StockItem] = []
    if not isinstance(parsed, list):
        return items

    seen_codes: set = set()
    for obj in parsed:
        if not isinstance(obj, dict):
            continue
        name = str(obj.get("name", "")).strip()
        code_raw = str(obj.get("code", "")).strip()
        code = re.sub(r"[^\d]", "", code_raw).zfill(6)
        if (re.match(r"^\d{6}$", code)
                and code != "000000"
                and code not in seen_codes):
            seen_codes.add(code)
            items.append(_make_item(name or code, code, source))
    return items


def _parse_llm_json(raw_text: str) -> Optional[list]:
    """
    从 LLM 响应中解析 JSON 数组，三级容错：
      1. 去除 Markdown 代码块后直接 json.loads()
      2. 正则从文本中抠出首个 [...] 块再解析
      3. 失败返回 None
    """
    text = raw_text.strip()
    # 去掉 ```json ... ``` 或 ``` ... ``` 包裹
    text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*```\s*$", "", text)
    text = text.strip()

    def _coerce_list(obj) -> Optional[list]:
        if isinstance(obj, list):
            return obj
        if isinstance(obj, dict):
            # 兼容模型返回 {"stocks":[...]} / {"items":[...]} 等包裹结构
            for key in ("stocks", "items", "data", "result", "results"):
                value = obj.get(key)
                if isinstance(value, list):
                    return value
        return None

    # 尝试 1：直接解析
    try:
        result = _coerce_list(json.loads(text))
        if result is not None:
            return result
    except (json.JSONDecodeError, ValueError):
        pass

    # 尝试 2：扫描并解析文本中的所有 JSON 数组片段，优先返回元素最多的数组
    candidates: List[str] = []
    depth = 0
    start = -1
    in_string = False
    escaped = False
    for idx, ch in enumerate(text):
        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
            continue
        if ch == "[":
            if depth == 0:
                start = idx
            depth += 1
            continue
        if ch == "]" and depth > 0:
            depth -= 1
            if depth == 0 and start >= 0:
                candidates.append(text[start:idx + 1])
                start = -1

    best: Optional[list] = None
    for chunk in candidates:
        try:
            parsed = _coerce_list(json.loads(chunk))
            if isinstance(parsed, list):
                if best is None or len(parsed) > len(best):
                    best = parsed
        except (json.JSONDecodeError, ValueError):
            continue

    if best is not None:
        return best

    return None


# =============================================================================
# Prompt 定义（V8.1：强制 JSON + 谐音纠错）
# =============================================================================

# 通用文字 / 图片 OCR 结果映射 Prompt
_MAPPING_PROMPT = (
    "你是一位专业的 A 股代码识别专家，拥有完整的 A 股上市公司知识库。\n\n"
    "【重要背景】\n"
    "输入内容可能来自语音识别转写或手写，存在严重的同音/谐音错字，例如：\n"
    "  · \"托维信息\" → 实为 \"拓维信息\"（300002）\n"
    "  · \"平按银行\" → 实为 \"平安银行\"（000001）\n"
    "  · \"宁的时代\" → 实为 \"宁德时代\"（300750）\n"
    "  · \"龙基绿能\" → 实为 \"隆基绿能\"（601012）\n"
    "你必须运用 A 股知识库，智能推断并纠正所有谐音错误，输出正确的公司名称和代码。\n\n"
    "【任务】\n"
    "从以下内容中识别所有提到的股票（包括中文全称、简称、拼音缩写、6位数字代码等），\n"
    "将每只股票转换为标准 JSON 对象，组合为一个 JSON 数组返回。\n\n"
    "【输出格式——严格遵守，不得偏离】\n"
    "直接输出一个 JSON 数组，不要任何前缀、后缀、解释说明或 Markdown 代码块标记。\n"
    "若未识别到任何股票，输出空数组：[]\n\n"
    "正确输出示例：\n"
    '[{"name": "贵州茅台", "code": "600519"}, {"name": "宁德时代", "code": "300750"}]\n\n'
    "【字段规则】\n"
    "1. name：官方中文简称\n"
    "2. code：6位纯数字代码（不足6位请补零）\n"
    "3. 若只有代码无名称，name 填写代码本身\n"
    "4. 同一只股票多次出现只输出一次\n\n"
    "【待识别内容】\n"
    "{content}"
)

# 文本/图片兜底专用的极简严格 JSON Prompt
_STRICT_JSON_RETRY_PROMPT = (
    "请从下面内容中提取所有 A 股股票，并严格返回 JSON 数组。"
    "不要解释，不要 Markdown，不要任何前后缀。\n"
    "数组元素格式只能是："
    '[{"name":"官方中文简称","code":"6位数字代码"}]。\n'
    "如果某条只出现代码没有名称，则 name 填同一个代码。"
    "如果没有识别到股票，返回 []。\n"
    "待识别内容：\n"
    "{content}"
)

# 语音专用 Prompt（更严格的谐音纠错要求）
_VOICE_MAPPING_PROMPT = (
    "你是一位专业的 A 股代码识别专家，拥有完整的 A 股上市公司知识库。\n\n"
    "【极其重要的背景】\n"
    "以下内容来自语音自动识别（ASR），语音识别引擎会产生大量同音/谐音错字，\n"
    "这是技术固有缺陷。你必须非常认真地进行谐音纠错！\n\n"
    "常见谐音错误类型：\n"
    "  · 声母混淆：\"拓\"→\"托\"，\"隆\"→\"龙\"，\"招\"→\"朝\"，\"宁\"→\"凝\"\n"
    "  · 韵母混淆：\"比亚\"→\"必呀\"，\"伊利\"→\"一力\"\n"
    "  · 整词替换：\"平安\"→\"平按\"，\"茅台\"→\"猫台\"，\"海天\"→\"还天\"\n"
    "  · 奇怪组合：任何不像正规公司名的词，优先考虑是谐音替换\n\n"
    "你的工作流程：\n"
    "  1. 扫描输入，找出所有疑似股票名称的词语（包括明显谐音的）\n"
    "  2. 对每个词语，用 A 股知识库进行谐音推断，还原正确公司名\n"
    "  3. 查找正确公司名对应的6位代码\n"
    "  4. 输出 JSON 数组\n\n"
    "【输出格式——严格遵守】\n"
    "直接输出 JSON 数组，不要任何前缀、后缀或解释。\n"
    "若未识别到任何股票，输出空数组：[]\n\n"
    "示例：输入\"帮我看托维信息和平按银行\"\n"
    '输出：[{"name": "拓维信息", "code": "300002"}, {"name": "平安银行", "code": "000001"}]\n\n'
    "【待识别语音转写内容】\n"
    "{content}"
)


# =============================================================================
# 核心映射函数（V8.1：强制 JSON + 三级容错）
# =============================================================================

def llm_map_to_items(content: str, source: str = "text",
                     is_voice: bool = False) -> List[StockItem]:
    """
    调用 Gemini（OpenAI 兼容格式）将任意文本映射为标准 StockItem 列表。

    V8.1：强制 JSON 输出，彻底废除 | 切分；
    is_voice=True 使用含严格谐音纠错规则的专用 Prompt；
    三级 JSON 容错解析，确保20+股票零漏单。
    """
    client = _get_client()
    items: List[StockItem] = []

    prompt = _VOICE_MAPPING_PROMPT if is_voice else _MAPPING_PROMPT

    def _extract_text_from_message_content(message_content) -> str:
        if isinstance(message_content, str):
            return message_content
        if isinstance(message_content, list):
            parts: List[str] = []
            for part in message_content:
                if isinstance(part, dict):
                    text = part.get("text")
                    if isinstance(text, str):
                        parts.append(text)
            return "\n".join(p for p in parts if p).strip()
        return str(message_content or "").strip()

    def _call_and_parse(prompt_text: str, label: str) -> List[StockItem]:
        resp = client.chat.completions.create(
            model=_GEMINI_MODEL,
            messages=[{
                "role": "user",
                "content": prompt_text,
            }],
            max_tokens=1600,
            temperature=0,
        )
        raw_text = _extract_text_from_message_content(
            getattr(resp.choices[0].message, "content", "")
        )
        logger.debug(f"[ExtractEngine] {label} 原始响应: {raw_text[:300]}")
        return _items_from_parsed(_parse_llm_json(raw_text), source)

    if client:
        try:
            items = _call_and_parse(
                prompt.format(content=content[:12000]),
                "LLM主路",
            )
            if items:
                logger.info(f"[ExtractEngine] JSON解析成功，共{len(items)}只")
                return items

            # 第二次用极简强约束 Prompt 重试，避免模型输出解释性文本导致 JSON 解析失败。
            items = _call_and_parse(
                _STRICT_JSON_RETRY_PROMPT.format(content=content[:12000]),
                "LLM重试",
            )
            if items:
                logger.info(f"[ExtractEngine] JSON重试成功，共{len(items)}只")
                return items
            logger.warning("[ExtractEngine] LLM 两次均未返回可解析 JSON")

        except Exception as exc:
            logger.warning(f"[ExtractEngine] LLM调用异常: {exc}")

    # 对含中文/名称的输入，禁止静默退化成“纯代码结果”，否则会制造漏单和丢名假象。
    has_named_signal = bool(re.search(r"[\u4e00-\u9fffA-Za-z]", content))
    if has_named_signal:
        logger.warning("[ExtractEngine] 检测到名称类输入，但 LLM 映射失败；跳过正则兜底以避免错误结果")
        return []

    # 正则兜底（仅限纯代码输入）
    logger.info("[ExtractEngine] 降级到正则兜底提取")
    for code in _regex_extract(content):
        items.append(_make_item(code, code, source))
    return items


# =============================================================================
# 文字提取
# =============================================================================

def extract_from_text(text: str) -> List[StockItem]:
    """从任意粘贴文字中提取股票列表（含谐音纠错）。"""
    if not text or not text.strip():
        return []
    return llm_map_to_items(text.strip(), source="text", is_voice=False)


# =============================================================================
# 图片提取（多图，base64 + Gemini Vision → JSON 映射）
# =============================================================================

def _detect_mime(data: bytes) -> str:
    if data[:4] == b"\x89PNG": return "image/png"
    if data[:2] == b"\xff\xd8": return "image/jpeg"
    if data[:4] in (b"RIFF", b"WEBP"): return "image/webp"
    return "image/jpeg"


def _vision_ocr_single(img_bytes: bytes, client) -> str:
    """Gemini Vision OCR：提取图片中的原始股票文字（不直接要代码，先要文字）。"""
    b64  = base64.b64encode(img_bytes).decode("utf-8")
    mime = _detect_mime(img_bytes)
    try:
        resp = client.chat.completions.create(
            model=_GEMINI_MODEL,
            messages=[{
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:{mime};base64,{b64}"},
                    },
                    {
                        "type": "text",
                        "text": (
                            "请逐行完整转写图片中所有与股票相关的文字信息，"
                            "包括股票名称、6位股票代码、自选股列表、分组标题下的所有可见条目。"
                            "绝对不要只挑前几条，必须从上到下、从左到右全部读完。"
                            "请尽量保持每个股票一行，保留名称与代码的对应关系。"
                            "多个条目之间使用换行分隔，不要改成逗号长串。"
                            "若图中没有股票相关信息，输出空字符串。"
                            "不要做任何分析、解释或代码识别，只输出原始文字。"
                        ),
                    },
                ],
            }],
            max_tokens=1600,
        )
        return (resp.choices[0].message.content or "").strip()
    except Exception as exc:
        logger.error(f"[ExtractEngine] Vision OCR失败: {exc}")
        return ""


def _vision_extract_items_single(img_bytes: bytes, client) -> List[StockItem]:
    """Gemini Vision 直接从图片提取完整股票 JSON，避免 OCR -> 再映射链路丢单。"""
    b64 = base64.b64encode(img_bytes).decode("utf-8")
    mime = _detect_mime(img_bytes)
    try:
        resp = client.chat.completions.create(
            model=_GEMINI_MODEL,
            messages=[{
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:{mime};base64,{b64}"},
                    },
                    {
                        "type": "text",
                        "text": (
                            "请识别这张图片中出现的所有 A 股股票，必须完整覆盖整张图中每一只可见股票，"
                            "不能只返回前几只，也不能省略名称。"
                            "直接输出 JSON 数组，不要 Markdown，不要解释。"
                            "每个元素格式固定为 {\"name\":\"官方中文简称\",\"code\":\"6位数字代码\"}。"
                            "如果图片里同时出现名称和代码，必须同时返回两者；"
                            "如果只看到了代码，就把 name 填成同一个代码。"
                            "同一只股票重复出现时只保留一次。"
                            "如果图片中没有股票，返回 []。"
                        ),
                    },
                ],
            }],
            max_tokens=1600,
            temperature=0,
        )
        raw_text = (resp.choices[0].message.content or "").strip()
        logger.debug(f"[ExtractEngine] Vision直提原始响应: {raw_text[:200]}")
        return _items_from_parsed(_parse_llm_json(raw_text), source="image")
    except Exception as exc:
        logger.error(f"[ExtractEngine] Vision直提失败: {exc}")
        return []


def extract_from_images(files) -> List[StockItem]:
    """
    从多张图片中批量提取股票列表。

    V8.2 修复：
      · 彻底删除 ImageStockExtractor 优先逻辑——旧插件只提取 1 个代码就 continue，
        导致后续图片根本不走 Gemini Vision，严重漏单。
      · 所有图片 100% 直接进入 _vision_ocr_single（Gemini Vision），无任何旁路。
      · all_texts 用换行符拼接，不干扰 JSON 解析。
    """
    if not files:
        return []

    ensure_proxy()
    client    = _get_client()
    all_texts: List[str] = []
    all_items: List[StockItem] = []
    seen_codes: set = set()

    file_list = files if isinstance(files, (list, tuple)) else [files]
    for f in file_list:
        # seek 回起点，防止 Streamlit UploadedFile 读完后指针在末尾
        if hasattr(f, "seek"):
            f.seek(0)
        img_bytes = f.read() if hasattr(f, "read") else bytes(f)
        if not img_bytes:
            continue

        # 主路：直接从图片提取完整 JSON，避免 OCR 文本丢行后再映射导致漏单/丢名。
        if client:
            vision_items = _vision_extract_items_single(img_bytes, client)
            if vision_items:
                for item in vision_items:
                    code = item["code"]
                    if code not in seen_codes:
                        seen_codes.add(code)
                        all_items.append(item)
                logger.info(f"[ExtractEngine] Vision直提成功，当前累计{len(all_items)}只")
                continue

            # 兜底：若直提失败，再走 OCR -> 文本映射。
            text = _vision_ocr_single(img_bytes, client)
            if text:
                all_texts.append(text)
                logger.debug(f"[ExtractEngine] Vision OCR结果: {text[:120]}")
        else:
            logger.warning("[ExtractEngine] Gemini 客户端不可用，无法处理图片")

    if all_texts:
        combined = "\n".join(all_texts)
        mapped_items = llm_map_to_items(combined, source="image", is_voice=False)
        for item in mapped_items:
            code = item["code"]
            if code not in seen_codes:
                seen_codes.add(code)
                all_items.append(item)

    return all_items


# =============================================================================
# 语音转写 + 提取（谐音纠错 + 防幻觉）
# =============================================================================

def transcribe_audio(audio_bytes: bytes) -> str:
    """
    语音 → 文字（只转写，不提取代码，防止幻觉）。
    成功返回转写文本，失败返回空串（绝不捏造代码）。

    调用链：
      1. Gemini 多模态 chat completions（base64 音频，兼容模式）
      2. 降级：google.generativeai 原生 SDK
      3. 降级：OpenAI Whisper（若有 OPENAI_API_KEY）
    """
    if not audio_bytes:
        return ""

    ensure_proxy()

    # ── 主路：Gemini 兼容模式 ──────────────────────────────────────────────────
    client = _get_client()
    if client:
        try:
            b64  = base64.b64encode(audio_bytes).decode("utf-8")
            resp = client.chat.completions.create(
                model=_GEMINI_MODEL,
                messages=[{
                    "role": "user",
                    "content": [
                        {
                            "type": "input_audio",
                            "input_audio": {"data": b64, "format": "wav"},
                        },
                        {
                            "type": "text",
                            "text": (
                                "请将上方音频逐字转录为中文文字。"
                                "保留所有词语（包括公司名称和数字），"
                                "不要添加任何解释、标点修正或内容补充。"
                                "若音频无法识别，输出空字符串。"
                            ),
                        },
                    ],
                }],
                max_tokens=500,
            )
            text = (resp.choices[0].message.content or "").strip()
            if text:
                logger.info(f"[ExtractEngine] 转写成功（兼容模式）: {text[:80]}")
                return text
        except Exception as exc:
            logger.warning(f"[ExtractEngine] Gemini音频兼容模式失败: {exc}")

    # ── 降级 A：google.generativeai 原生 SDK ──────────────────────────────────
    _root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if _root not in sys.path:
        sys.path.insert(0, _root)
    from src.config import get_config
    cfg        = get_config()
    gemini_key = getattr(cfg, "gemini_api_key", None) or os.getenv("GEMINI_API_KEY", "")
    if gemini_key:
        try:
            import google.generativeai as genai  # type: ignore
            genai.configure(api_key=gemini_key)
            model = genai.GenerativeModel(_GEMINI_MODEL)
            resp  = model.generate_content([
                "请将音频逐字转录为中文，保留所有词语，不要分析或解释。",
                {"mime_type": "audio/wav", "data": audio_bytes},
            ])
            text = (resp.text or "").strip()
            if text:
                logger.info(f"[ExtractEngine] 转写成功（原生SDK）: {text[:80]}")
                return text
        except Exception as exc:
            logger.warning(f"[ExtractEngine] Gemini原生SDK音频失败: {exc}")

    # ── 降级 B：OpenAI Whisper ─────────────────────────────────────────────────
    openai_key = getattr(cfg, "openai_api_key", None) or os.getenv("OPENAI_API_KEY", "")
    if openai_key:
        try:
            import openai as _openai
            oa = _openai.OpenAI(api_key=openai_key)
            result = oa.audio.transcriptions.create(
                model="whisper-1",
                file=("audio.wav", BytesIO(audio_bytes), "audio/wav"),
                language="zh",
            )
            text = (result.text or "").strip()
            if text:
                logger.info(f"[ExtractEngine] 转写成功（Whisper）: {text[:80]}")
                return text
        except Exception as exc:
            logger.warning(f"[ExtractEngine] Whisper失败: {exc}")

    logger.error("[ExtractEngine] 语音转写全部失败，返回空串（绝不捏造代码）")
    return ""


def extract_from_voice(audio_bytes: bytes) -> List[StockItem]:
    """
    语音 → 转写文字 → 谐音纠错 → 代码映射，返回 StockItem 列表。

    V8.1：转写后使用 is_voice=True，触发含严格谐音纠错规则的
    _VOICE_MAPPING_PROMPT，确保"托维信息"→"拓维信息"等错误被纠正。
    """
    transcript = transcribe_audio(audio_bytes)
    if not transcript:
        return []
    return llm_map_to_items(transcript, source="voice", is_voice=True)
