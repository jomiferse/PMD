import logging
import time
from typing import Any

import httpx

from ..external import TELEGRAM_BREAKER, TELEGRAM_SEMAPHORE, limited
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

    return _post_with_retries(url, payload, log_error=True)


def answer_callback_query(callback_query_id: str, text: str | None = None) -> None:
    if not settings.TELEGRAM_BOT_TOKEN:
        return
    url = f"https://api.telegram.org/bot{settings.TELEGRAM_BOT_TOKEN}/answerCallbackQuery"
    payload: dict[str, Any] = {"callback_query_id": callback_query_id}
    if text:
        payload["text"] = text
    _post_with_retries(url, payload, log_error=False)


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
    _post_with_retries(url, payload, log_error=False)


def _post_with_retries(
    url: str,
    payload: dict[str, Any],
    *,
    log_error: bool,
) -> dict[str, Any] | None:
    if not TELEGRAM_BREAKER.allow():
        logger.warning("telegram_circuit_open")
        return None

    attempts = max(int(settings.TELEGRAM_MAX_RETRIES), 0) + 1
    for attempt in range(attempts):
        try:
            with limited(TELEGRAM_SEMAPHORE):
                with httpx.Client(timeout=_telegram_timeout()) as client:
                    timer = HttpxTimer()
                    response = client.post(url, json=payload)
            log_httpx_response(response, timer.elapsed(), log_error=log_error)
            if response.is_success:
                TELEGRAM_BREAKER.record_success()
                return response.json()
            if log_error:
                logger.error(
                    "telegram_request_failed status=%s body=%s",
                    response.status_code,
                    response.text[:200],
                )
        except Exception:
            logger.exception("telegram_request_exception attempt=%s", attempt + 1)
        if attempt < attempts - 1:
            backoff = max(float(settings.TELEGRAM_RETRY_BACKOFF_SECONDS), 0.1) * (2**attempt)
            time.sleep(backoff)

    TELEGRAM_BREAKER.record_failure()
    return None


def _telegram_timeout() -> httpx.Timeout:
    return httpx.Timeout(
        settings.TELEGRAM_TIMEOUT_SECONDS,
        connect=settings.TELEGRAM_CONNECT_TIMEOUT_SECONDS,
    )
