import json
import logging
from typing import Any

import httpx
import redis
from pydantic import BaseModel, ValidationError

from ..settings import settings

logger = logging.getLogger(__name__)
redis_conn = redis.from_url(settings.REDIS_URL)

LLM_CACHE_KEY = "ai:llm:{user_id}:{alert_id}"


class LlmRecommendation(BaseModel):
    recommendation: str
    confidence: str
    rationale: str
    risks: str


def get_trade_recommendation(context: dict[str, Any]) -> dict[str, str]:
    user_id = context.get("user_id")
    alert_id = context.get("alert_id")
    cache_key = None
    if user_id and alert_id:
        cache_key = LLM_CACHE_KEY.format(user_id=user_id, alert_id=alert_id)
        cached = _get_cached(cache_key)
        if cached:
            return cached

    payload = _build_openai_payload(context)
    api_key = settings.OPENAI_API_KEY
    if not api_key:
        logger.warning("llm_missing_api_key")
        fallback = {
            "recommendation": "WAIT",
            "confidence": "LOW",
            "rationale": "LLM unavailable; defaulting to WAIT.",
            "risks": "Recommendation unavailable due to missing API key.",
        }
        _set_cached(cache_key, fallback)
        return fallback

    headers = {"Authorization": f"Bearer {api_key}"}
    response_data = None
    for attempt in range(max(settings.LLM_MAX_RETRIES, 0) + 1):
        try:
            with httpx.Client(timeout=settings.LLM_TIMEOUT_SECONDS) as client:
                response = client.post(settings.LLM_API_BASE, headers=headers, json=payload)
            if response.is_success:
                response_data = response.json()
                break
            logger.warning(
                "llm_request_failed status=%s body=%s",
                response.status_code,
                response.text[:200],
            )
        except Exception:
            logger.exception("llm_request_exception attempt=%s", attempt + 1)

    if response_data is None:
        fallback = {
            "recommendation": "WAIT",
            "confidence": "LOW",
            "rationale": "LLM unavailable; defaulting to WAIT.",
            "risks": "Recommendation unavailable due to request failure.",
        }
        _set_cached(cache_key, fallback)
        return fallback

    parsed = _parse_openai_response(response_data)
    _set_cached(cache_key, parsed)
    return parsed


def _build_openai_payload(context: dict[str, Any]) -> dict[str, Any]:
    system_prompt = (
        "You are a conservative trade assistant. Provide read-only decision support only. "
        "No financial advice. If information is insufficient or ambiguous, return WAIT. "
        "Respond with strict JSON only."
    )
    user_prompt = (
        "Given the alert context, provide a recommendation.\n"
        "Return JSON with keys: recommendation (BUY/WAIT/SKIP), confidence (HIGH/MEDIUM/LOW), "
        "rationale (short sentence), risks (short sentence)."
    )
    return {
        "model": settings.LLM_MODEL,
        "temperature": 0.2,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
            {"role": "user", "content": json.dumps(context, ensure_ascii=True)},
        ],
    }


def _parse_openai_response(payload: dict[str, Any]) -> dict[str, str]:
    try:
        content = payload["choices"][0]["message"]["content"]
        raw = json.loads(content)
        parsed = LlmRecommendation.model_validate(raw)
    except (KeyError, IndexError, json.JSONDecodeError, ValidationError):
        logger.exception("llm_response_parse_failed")
        return {
            "recommendation": "WAIT",
            "confidence": "LOW",
            "rationale": "LLM response invalid; defaulting to WAIT.",
            "risks": "Invalid LLM response payload.",
        }

    recommendation = parsed.recommendation.strip().upper()
    confidence = parsed.confidence.strip().upper()
    if recommendation not in {"BUY", "WAIT", "SKIP"}:
        recommendation = "WAIT"
    if confidence not in {"HIGH", "MEDIUM", "LOW"}:
        confidence = "LOW"

    return {
        "recommendation": recommendation,
        "confidence": confidence,
        "rationale": parsed.rationale.strip(),
        "risks": parsed.risks.strip(),
    }


def _get_cached(cache_key: str | None) -> dict[str, str] | None:
    if not cache_key:
        return None
    try:
        cached = redis_conn.get(cache_key)
        if cached:
            return json.loads(cached)
    except Exception:
        logger.exception("llm_cache_read_failed key=%s", cache_key)
    return None


def _set_cached(cache_key: str | None, payload: dict[str, str]) -> None:
    if not cache_key:
        return
    try:
        redis_conn.set(cache_key, json.dumps(payload, ensure_ascii=True), ex=settings.LLM_CACHE_TTL_SECONDS)
    except Exception:
        logger.exception("llm_cache_write_failed key=%s", cache_key)
