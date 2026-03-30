# -*- coding: utf-8 -*-
"""
Clean extraction engine for text, image, and voice stock entity mapping.
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
_GEMINI_MODEL = "gemini-2.5-flash"

StockItem = Dict  # {"name": str, "code": str, "valid": bool, "source": str}


def ensure_proxy() -> None:
    """Inject a local proxy only when the workstation gateway requires it."""
    if os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy"):
        return
    gateway_ip = os.getenv("GATEWAY_IP", "").strip()
    if gateway_ip != "10.10.10.252":
        return
    host = os.getenv("PROXY_HOST", gateway_ip).strip()
    port = os.getenv("PROXY_PORT", "").strip()
    if host and port:
        proxy_url = f"http://{host}:{port}"
        for key in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
            os.environ[key] = proxy_url
        logger.info("[ExtractEngine] local proxy injected: %s", proxy_url)


def _get_client():
    """Return the OpenAI-compatible Gemini client."""
    ensure_proxy()
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if root not in sys.path:
        sys.path.insert(0, root)
    from src.config import setup_env
    setup_env()
    from src.config import get_config

    cfg = get_config()
    api_key = getattr(cfg, "gemini_api_key", None) or os.getenv("GEMINI_API_KEY", "")
    if not api_key:
        logger.warning("[ExtractEngine] GEMINI_API_KEY is missing")
        return None
    try:
        import openai
        return openai.OpenAI(api_key=api_key, base_url=_GEMINI_BASE_URL)
    except Exception as exc:
        logger.error("[ExtractEngine] failed to create Gemini client: %s", exc)
        return None


def _regex_extract(text: str) -> List[str]:
    """Fallback regex extractor for plain six-digit stock codes."""
    seen, result = set(), []
    for raw in re.findall(r"\b(\d{6})\b", text or ""):
        if raw not in seen and raw != "000000":
            seen.add(raw)
            result.append(raw)
    return result


def _make_item(name: str, code: str, source: str, valid: bool = True) -> StockItem:
    return {"name": name, "code": code, "valid": valid, "source": source}


def _coerce_confidence(value) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        if isinstance(value, str):
            cleaned = value.strip().rstrip("%")
            if not cleaned:
                return None
            number = float(cleaned)
        else:
            number = float(value)
    except (TypeError, ValueError):
        return None

    if number > 1:
        if number <= 100:
            number = number / 100.0
        else:
            return None
    if number < 0:
        return None
    return min(number, 1.0)


def _first_non_empty(obj: dict, *keys: str):
    if not isinstance(obj, dict):
        return None
    for key in keys:
        value = obj.get(key)
        if value not in (None, ""):
            return value
    return None


def _extract_item_fields(obj: dict) -> dict:
    return {
        "name": _first_non_empty(
            obj,
            "name", "Name",
            "stock_name", "stockName",
            "security_name", "securityName",
            "company_name", "companyName",
            "symbol_name", "symbolName",
            "ticker_name", "tickerName",
        ),
        "code": _first_non_empty(
            obj,
            "code", "Code",
            "stock_code", "stockCode",
            "security_code", "securityCode",
            "ticker", "Ticker",
            "symbol", "Symbol",
        ),
        "market": _first_non_empty(
            obj,
            "market", "Market",
            "exchange", "Exchange",
        ),
        "mention": _first_non_empty(
            obj,
            "mention", "Mention",
            "alias", "Alias",
            "raw", "Raw",
            "raw_text", "rawText",
        ),
        "confidence": _coerce_confidence(
            _first_non_empty(
                obj,
                "confidence", "Confidence",
                "score", "Score",
                "probability", "Probability",
                "conf", "Conf",
            )
        ),
    }


def _items_from_parsed(parsed: Optional[list], source: str) -> List[StockItem]:
    """Normalize parsed JSON objects into StockItem entries."""
    items: List[StockItem] = []
    if not isinstance(parsed, list):
        return items

    seen_keys: set = set()
    for obj in parsed:
        if not isinstance(obj, dict):
            continue
        fields = _extract_item_fields(obj)
        name = str(fields.get("name") or "").strip()
        code_raw = str(fields.get("code") or "").strip()
        market = str(fields.get("market") or "").strip()
        mention = str(fields.get("mention") or "").strip()
        confidence = fields.get("confidence")
        if confidence is not None and confidence < 0.3:
            continue
        code = re.sub(r"[^\d]", "", code_raw).zfill(6) if code_raw else ""
        has_valid_code = bool(re.match(r"^\d{6}$", code) and code != "000000")
        display_name = name or code_raw or ""
        if not has_valid_code and not display_name:
            continue

        dedupe_key = code if has_valid_code else f"name:{display_name.lower()}"
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)

        item_code = code if has_valid_code else (code_raw or display_name)
        item = _make_item(display_name or item_code, item_code, source)
        if market:
            item["market"] = market
        if mention:
            item["mention"] = mention
        if confidence is not None:
            item["confidence"] = confidence
        items.append(item)
    return items


def _strip_markdown_fences(raw_text: str) -> str:
    text = (raw_text or "").strip().lstrip("\ufeff")
    if not text:
        return ""
    text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*```\s*$", "", text)
    return text.strip()


def _parse_llm_json(raw_text: str) -> Optional[list]:
    """Parse LLM JSON with a short, predictable flow."""
    text = (raw_text or "").strip().lstrip("\ufeff")
    if not text:
        return None

    def _coerce_list(obj) -> Optional[list]:
        if isinstance(obj, list):
            return obj
        if isinstance(obj, dict):
            for key in ("stocks", "Stocks", "items", "Items", "data", "Data", "result", "Result", "results", "Results"):
                value = obj.get(key)
                if isinstance(value, list):
                    return value
        return None

    def _try_load(candidate: str) -> Optional[list]:
        try:
            result = _coerce_list(json.loads(candidate))
            if result is not None:
                return result
        except (json.JSONDecodeError, ValueError, TypeError) as e:
            print(f"JSON Decode Error: {e}")
        return None

    # 1) 标准 JSON 直读
    result = _try_load(text)
    if result is not None:
        return result

    # 2) 去掉 Markdown 代码块头尾后再读
    stripped = text
    if stripped.startswith("```json"):
        stripped = stripped[len("```json"):].strip()
    elif stripped.startswith("```"):
        stripped = stripped[len("```"):].strip()
    if stripped.endswith("```"):
        stripped = stripped[:-3].strip()
    if stripped != text:
        result = _try_load(stripped)
        if result is not None:
            return result

    return None


def _extract_message_text(message_content) -> str:
    if isinstance(message_content, str):
        return message_content.strip()
    if isinstance(message_content, list):
        parts: List[str] = []
        for part in message_content:
            if isinstance(part, dict):
                text = part.get("text")
                if isinstance(text, str):
                    parts.append(text)
        return "\n".join(p for p in parts if p).strip()
    return str(message_content or "").strip()


_TEXT_MAPPING_PROMPT = """
你是一个股票语义实体提取引擎，负责从非结构化中文或中英混杂文本中识别被明确提及的股票。

任务要求：
1. 优先识别股票简称、黑话、缩写、谐音、错别字、口语别名，并尽量还原为官方股票简称和 6 位数字代码。
2. 如果文本里只有模糊影射，但无法高置信确认，请直接丢弃，宁缺毋滥，不要猜测，不要硬编。
3. 同一只股票即使被多次提及，也只输出一次。
4. 只提取股票，不要把行业、产品、人物、题材词误判成股票。
5. 仅过滤极低置信条目。若 confidence 低于 0.3，请不要输出该条。

输出规则：
1. 你必须返回严格的 JSON 数组，不要解释，不要 Markdown，不要任何前后缀。
2. 每个对象的键名必须严格使用全小写字母：
   `code`, `name`, `market`, `confidence`, `mention`
3. 绝对不要改成 `Code`、`Name`、`stock_name`、`symbol` 或任何其他变体。
4. 不要缺失必要键；如果某个字段不确定，也要保留键名并给出空字符串或合理默认值。
5. 如果没有识别到任何高置信股票，输出 []。

标准示例：
[{"name":"拓维信息","code":"002261","market":"A","confidence":0.92,"mention":"托维信息"},{"name":"宁德时代","code":"300750","market":"A","confidence":0.95,"mention":"宁王"}]

待识别文本：
{content}
"""

_MAPPING_PROMPT = """
你是一个股票名称标准化引擎。请从输入内容中提取股票，并返回严格 JSON 数组。

要求：
1. 尽量输出官方股票简称和 6 位数字代码。
2. 同一只股票只保留一次。
3. 不要输出解释，不要输出 Markdown。
4. 你必须严格使用全小写键名：`code`, `name`, `market`, `confidence`, `mention`。
5. 如果无法识别，返回 []。

输出示例：
[{"name":"贵州茅台","code":"600519","market":"A","confidence":0.96,"mention":"茅台"},{"name":"宁德时代","code":"300750","market":"A","confidence":0.95,"mention":"宁德时代"}]

待识别内容：
{content}
"""

_STRICT_JSON_RETRY_PROMPT = """
请从下面内容中提取股票，并且只返回 JSON 数组。
不要解释，不要 Markdown，不要任何前后缀。
你必须严格返回如下结构，并且键名必须全部小写，不允许变体：
[{"name":"官方中文简称","code":"6位数字代码","market":"A","confidence":0.90,"mention":"原文提法"}]
绝对不要返回 `Name`、`Code`、`stock_name`、`symbol` 等其他键名。
如果没有识别到股票，返回 []。
待识别内容：
{content}
"""

_VOICE_MAPPING_PROMPT = """
你是一个语音转写纠错后的股票提取引擎。

要求：
1. 输入来自语音识别，可能包含大量谐音字、错别字和口语表达。
2. 你需要先做股票名称纠错，再映射为官方简称和 6 位数字代码。
3. 如果无法高置信确认，请直接放弃，不要猜测。
4. 同一只股票只输出一次。

输出规则：
1. 只能输出 JSON 数组，不要解释，不要 Markdown。
2. 每个对象的键名必须严格使用全小写字母：
   `code`, `name`, `market`, `confidence`, `mention`
3. 不允许改键名，不允许缺键。
4. 若没有识别到，输出 []。

标准结构：
[{"name":"官方股票简称","code":"6位数字代码","market":"A","confidence":0.88,"mention":"原始语音文本中的提法"}]

待识别语音转写内容：
{content}
"""


def llm_map_to_items(content: str, source: str = "text", is_voice: bool = False) -> List[StockItem]:
    """Map natural language or OCR text into StockItem entries via LLM."""
    client = _get_client()
    items: List[StockItem] = []
    prompt_content = content[:12000]

    if is_voice:
        prompt = _VOICE_MAPPING_PROMPT
    elif source == "text":
        prompt = _TEXT_MAPPING_PROMPT
    else:
        prompt = _MAPPING_PROMPT

    def _render_prompt(template: str) -> str:
        return template.replace("{content}", prompt_content)

    def _call_and_parse(prompt_text: str, label: str) -> List[StockItem]:
        resp = client.chat.completions.create(
            model=_GEMINI_MODEL,
            messages=[{"role": "user", "content": prompt_text}],
            max_tokens=1600,
            temperature=0,
        )
        raw_text = _extract_message_text(getattr(resp.choices[0].message, "content", ""))
        print(f"\n{'='*20} [DEBUG: RAW LLM OUTPUT] {'='*20}\n{raw_text}\n{'='*60}\n")
        logger.debug("[ExtractEngine] %s raw response: %s", label, raw_text[:300])
        try:
            parsed = _parse_llm_json(raw_text)
        except Exception as exc:
            print(f"\n{'='*20} [DEBUG: LLM PARSE ERROR] {'='*20}\nlabel={label}\nerror={exc}\n{'='*60}\n")
            logger.warning("[ExtractEngine] %s parse failed: %s", label, exc)
            return []
        if parsed is None:
            print(f"\n{'='*20} [DEBUG: LLM PARSE EMPTY] {'='*20}\nlabel={label}\nreason=parse returned None\n{'='*60}\n")
        return _items_from_parsed(parsed, source)

    if client:
        try:
            items = _call_and_parse(_render_prompt(prompt), "llm_primary")
            if items:
                logger.info("[ExtractEngine] primary extraction succeeded with %s items", len(items))
                return items

            items = _call_and_parse(_render_prompt(_STRICT_JSON_RETRY_PROMPT), "llm_retry")
            if items:
                logger.info("[ExtractEngine] retry extraction succeeded with %s items", len(items))
                return items
            print(f"\n{'='*20} [DEBUG: LLM EMPTY RESULT] {'='*20}\nsource={source}\nreason=no parseable or high-confidence items after retry\n{'='*60}\n")
            logger.warning("[ExtractEngine] LLM returned no parseable stock JSON after retry")
        except Exception as exc:
            print(f"\n{'='*20} [DEBUG: LLM CALL ERROR] {'='*20}\nsource={source}\nerror={exc}\n{'='*60}\n")
            logger.warning("[ExtractEngine] LLM call failed: %s", exc)

    has_named_signal = bool(re.search(r"[\u4e00-\u9fffA-Za-z]", content))
    if has_named_signal:
        logger.warning("[ExtractEngine] named text detected but LLM extraction failed; skip regex fallback")
        return []

    logger.info("[ExtractEngine] falling back to regex-only code extraction")
    for code in _regex_extract(content):
        items.append(_make_item(code, code, source))
    return items


def extract_from_text(text: str) -> List[StockItem]:
    """Extract stock items from pasted plain text."""
    if not text or not text.strip():
        return []
    return llm_map_to_items(text.strip(), source="text", is_voice=False)


def _detect_mime(data: bytes) -> str:
    if data[:4] == b"\x89PNG":
        return "image/png"
    if data[:2] == b"\xff\xd8":
        return "image/jpeg"
    if data[:4] in (b"RIFF", b"WEBP"):
        return "image/webp"
    return "image/jpeg"


def _vision_ocr_single(img_bytes: bytes, client) -> str:
    """Run Gemini Vision OCR on a single image and return raw text."""
    b64 = base64.b64encode(img_bytes).decode("utf-8")
    mime = _detect_mime(img_bytes)
    try:
        resp = client.chat.completions.create(
            model=_GEMINI_MODEL,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": f"data:{mime};base64,{b64}"}},
                    {"type": "text", "text": "请只做 OCR，按自然顺序输出图片中的股票相关原始文字，不要总结，不要解释；如果没有可读文字，返回空字符串。"},
                ],
            }],
            max_tokens=1600,
            temperature=0,
        )
        return _extract_message_text(getattr(resp.choices[0].message, "content", ""))
    except Exception as exc:
        logger.error("[ExtractEngine] Vision OCR failed: %s", exc)
        return ""


def _vision_extract_items_single(img_bytes: bytes, client) -> List[StockItem]:
    """Map a single image directly to stock items with Gemini Vision."""
    b64 = base64.b64encode(img_bytes).decode("utf-8")
    mime = _detect_mime(img_bytes)
    try:
        resp = client.chat.completions.create(
            model=_GEMINI_MODEL,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": f"data:{mime};base64,{b64}"}},
                    {
                        "type": "text",
                        "text": (
                            "请直接从图片中识别所有被明确提及的股票，并只返回 JSON 数组。"
                            "不要解释，不要 Markdown。"
                            "数组元素格式："
                            "[{\"name\":\"官方股票简称\",\"code\":\"6位数字代码\",\"confidence\":0.90}]。"
                            "如果无法识别到股票，返回 []。"
                        ),
                    },
                ],
            }],
            max_tokens=1600,
            temperature=0,
        )
        raw_text = _extract_message_text(getattr(resp.choices[0].message, "content", ""))
        logger.debug("[ExtractEngine] Vision direct extraction raw response: %s", raw_text[:200])
        return _items_from_parsed(_parse_llm_json(raw_text), source="image")
    except Exception as exc:
        logger.error("[ExtractEngine] Vision direct extraction failed: %s", exc)
        return []


def extract_from_images(files) -> List[StockItem]:
    """Extract stock items from one or more images."""
    if not files:
        return []

    ensure_proxy()
    client = _get_client()
    all_texts: List[str] = []
    all_items: List[StockItem] = []
    seen_codes: set = set()

    file_list = files if isinstance(files, (list, tuple)) else [files]
    for file_obj in file_list:
        if hasattr(file_obj, "seek"):
            file_obj.seek(0)
        img_bytes = file_obj.read() if hasattr(file_obj, "read") else bytes(file_obj)
        if not img_bytes:
            continue

        if client:
            vision_items = _vision_extract_items_single(img_bytes, client)
            if vision_items:
                for item in vision_items:
                    code = str(item.get("code") or item.get("Code") or "").strip()
                    if code and code not in seen_codes:
                        seen_codes.add(code)
                        all_items.append(item)
                logger.info("[ExtractEngine] Vision direct extraction accumulated %s items", len(all_items))
                continue

            text = _vision_ocr_single(img_bytes, client)
            if text:
                all_texts.append(text)
                logger.debug("[ExtractEngine] Vision OCR text: %s", text[:120])
        else:
            logger.warning("[ExtractEngine] Gemini client missing; image extraction skipped")

    if all_texts:
        combined = "\n".join(all_texts)
        mapped_items = llm_map_to_items(combined, source="image", is_voice=False)
        for item in mapped_items:
            code = str(item.get("code") or item.get("Code") or "").strip()
            if code and code not in seen_codes:
                seen_codes.add(code)
                all_items.append(item)

    return all_items


def transcribe_audio(audio_bytes: bytes) -> str:
    """Transcribe audio bytes with Gemini-compatible speech recognition."""
    if not audio_bytes:
        return ""

    ensure_proxy()
    client = _get_client()
    if client:
        try:
            b64 = base64.b64encode(audio_bytes).decode("utf-8")
            resp = client.chat.completions.create(
                model=_GEMINI_MODEL,
                messages=[{
                    "role": "user",
                    "content": [
                        {"type": "input_audio", "input_audio": {"data": b64, "format": "wav"}},
                        {"type": "text", "text": "请将这段中文音频转写为纯文本，只返回转写结果，不要解释；如果听不清，请返回空字符串。"},
                    ],
                }],
                max_tokens=500,
                temperature=0,
            )
            text = _extract_message_text(getattr(resp.choices[0].message, "content", ""))
            if text:
                logger.info("[ExtractEngine] audio transcript via Gemini chat: %s", text[:80])
                return text
        except Exception as exc:
            logger.warning("[ExtractEngine] Gemini chat transcription failed: %s", exc)

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if root not in sys.path:
        sys.path.insert(0, root)
    from src.config import get_config

    cfg = get_config()
    gemini_key = getattr(cfg, "gemini_api_key", None) or os.getenv("GEMINI_API_KEY", "")
    if gemini_key:
        try:
            import google.generativeai as genai  # type: ignore
            genai.configure(api_key=gemini_key)
            model = genai.GenerativeModel(_GEMINI_MODEL)
            resp = model.generate_content([
                "请将这段中文音频转写为纯文本，只返回转写结果，不要解释。",
                {"mime_type": "audio/wav", "data": audio_bytes},
            ])
            text = (resp.text or "").strip()
            if text:
                logger.info("[ExtractEngine] audio transcript via google.generativeai: %s", text[:80])
                return text
        except Exception as exc:
            logger.warning("[ExtractEngine] google.generativeai transcription failed: %s", exc)

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
                logger.info("[ExtractEngine] audio transcript via whisper: %s", text[:80])
                return text
        except Exception as exc:
            logger.warning("[ExtractEngine] Whisper transcription failed: %s", exc)

    logger.error("[ExtractEngine] all audio transcription backends failed")
    return ""


def extract_from_voice(audio_bytes: bytes) -> List[StockItem]:
    """Transcribe audio first, then map the transcript into stock items."""
    transcript = transcribe_audio(audio_bytes)
    if not transcript:
        return []
    return llm_map_to_items(transcript, source="voice", is_voice=True)
