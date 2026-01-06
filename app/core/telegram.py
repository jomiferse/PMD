import logging
from typing import Any

import httpx

from ..settings import settings
from ..http_logging import HttpxTimer, log_httpx_response

logger = logging.getLogger(__name__)


def send_telegram_message(
    chat_id: str | int,
    text: str,
    reply_markup: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    if not settings.TELEGRAM_BOT_TOKEN:
        logger.warning("telegram_disabled")
        return None
    url = f"https://api.telegram.org/bot{settings.TELEGRAM_BOT_TOKEN}/sendMessage"
    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup

    try:
        with httpx.Client(timeout=10) as client:
            timer = HttpxTimer()
            response = client.post(url, json=payload)
        log_httpx_response(response, timer.elapsed(), log_error=False)
        if response.is_success:
            return response.json()
        logger.error("telegram_send_failed status=%s body=%s", response.status_code, response.text[:200])
    except Exception:
        logger.exception("telegram_send_exception")
    return None


def answer_callback_query(callback_query_id: str, text: str | None = None) -> None:
    if not settings.TELEGRAM_BOT_TOKEN:
        return
    url = f"https://api.telegram.org/bot{settings.TELEGRAM_BOT_TOKEN}/answerCallbackQuery"
    payload: dict[str, Any] = {"callback_query_id": callback_query_id}
    if text:
        payload["text"] = text
    try:
        with httpx.Client(timeout=10) as client:
            timer = HttpxTimer()
            response = client.post(url, json=payload)
        log_httpx_response(response, timer.elapsed())
    except Exception:
        logger.exception("telegram_callback_exception")


def edit_message_reply_markup(
    chat_id: str | int,
    message_id: str,
    reply_markup: dict[str, Any] | None = None,
) -> None:
    if not settings.TELEGRAM_BOT_TOKEN:
        return
    url = f"https://api.telegram.org/bot{settings.TELEGRAM_BOT_TOKEN}/editMessageReplyMarkup"
    payload: dict[str, Any] = {"chat_id": chat_id, "message_id": message_id}
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    try:
        with httpx.Client(timeout=10) as client:
            timer = HttpxTimer()
            response = client.post(url, json=payload)
        log_httpx_response(response, timer.elapsed())
    except Exception:
        logger.exception("telegram_edit_markup_exception")
