import json
import logging
from dataclasses import dataclass
from typing import Any

import httpx
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY
from sqlalchemy.orm import Session

from ..settings import settings
from .polymarket_credentials import (
    get_user_polymarket_credentials,
    set_user_polymarket_credentials,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OrderExecutionResult:
    ok: bool
    reason: str | None
    order_payload: dict[str, Any] | None
    response: dict[str, Any] | None


def execute_confirmed_order(
    db: Session,
    user_id,
    market_id: str,
    outcome_label: str,
    price: float,
    size: float,
) -> OrderExecutionResult:
    if not market_id:
        return OrderExecutionResult(False, "missing_market_id", None, None)
    if not outcome_label:
        return OrderExecutionResult(False, "missing_outcome_label", None, None)
    if price <= 0 or size <= 0:
        return OrderExecutionResult(False, "invalid_order_values", None, None)

    creds = get_user_polymarket_credentials(db, user_id)
    if not creds:
        return OrderExecutionResult(False, "missing_user_credentials", None, None)

    token_id = _resolve_token_id(market_id, outcome_label)
    if not token_id:
        return OrderExecutionResult(False, "token_id_not_found", None, None)

    client = _build_clob_client(db, user_id, creds)
    if client is None:
        return OrderExecutionResult(False, "client_not_configured", None, None)

    try:
        market = client.get_market(token_id)
        order_args = OrderArgs(
            token_id=str(token_id),
            price=float(price),
            size=float(size),
            side=BUY,
        )
        options = {
            "tick_size": market.get("tickSize"),
            "neg_risk": market.get("negRisk"),
        }
        response = client.create_and_post_order(order_args, options=options, order_type=OrderType.GTC)
    except Exception:
        logger.exception("polymarket_order_submit_failed market_id=%s token_id=%s", market_id, token_id)
        return OrderExecutionResult(False, "order_submit_failed", None, None)

    order_payload = {
        "token_id": str(token_id),
        "price": float(price),
        "size": float(size),
        "side": "BUY",
        "order_type": "GTC",
    }
    return OrderExecutionResult(True, None, order_payload, response)


def _build_clob_client(db: Session, user_id, creds: dict[str, Any]) -> ClobClient | None:
    private_key = str(creds.get("private_key") or "").strip()
    if not private_key:
        logger.error("polymarket_private_key_missing user_id=%s", user_id)
        return None
    host = settings.POLY_CLOB_HOST.rstrip("/")
    chain_id = settings.POLY_CHAIN_ID

    base_client = ClobClient(host, key=private_key, chain_id=chain_id)
    api_creds = _resolve_user_api_creds(base_client, db, user_id, creds)
    if not api_creds:
        logger.error("polymarket_api_creds_missing")
        return None

    signature_type = int(creds.get("signature_type") or settings.POLY_SIGNATURE_TYPE)
    funder = str(creds.get("funder_address") or settings.POLY_FUNDER_ADDRESS or "").strip()
    if not funder:
        logger.error("polymarket_funder_missing user_id=%s", user_id)
        return None

    return ClobClient(
        host,
        key=private_key,
        chain_id=chain_id,
        creds=api_creds,
        signature_type=signature_type,
        funder=funder,
    )


def _resolve_user_api_creds(
    client: ClobClient,
    db: Session,
    user_id,
    creds: dict[str, Any],
) -> dict[str, str] | None:
    api_key = str(creds.get("api_key") or "").strip()
    api_secret = str(creds.get("api_secret") or "").strip()
    api_passphrase = str(creds.get("api_passphrase") or "").strip()
    if api_key and api_secret and api_passphrase:
        return {
            "apiKey": api_key,
            "secret": api_secret,
            "passphrase": api_passphrase,
        }
    try:
        derived = client.create_or_derive_api_creds()
        if derived:
            updated = dict(creds)
            updated["api_key"] = derived.get("apiKey")
            updated["api_secret"] = derived.get("secret")
            updated["api_passphrase"] = derived.get("passphrase")
            set_user_polymarket_credentials(db, user_id, updated, commit=True)
        return derived
    except Exception:
        logger.exception("polymarket_api_creds_derive_failed")
        return None


def _resolve_token_id(market_id: str, outcome_label: str) -> str | None:
    market = _fetch_market(market_id)
    if not market:
        return None

    normalized_target = _normalize_label(outcome_label)
    labeled_tokens = _extract_labeled_tokens(market)
    for label, token_id in labeled_tokens:
        if _normalize_label(label) == normalized_target:
            return token_id

    indexed_tokens = _extract_indexed_tokens(market)
    if normalized_target == "YES" and indexed_tokens:
        return indexed_tokens[0]
    if normalized_target == "NO" and len(indexed_tokens) > 1:
        return indexed_tokens[1]
    return None


def _fetch_market(market_id: str) -> dict[str, Any] | None:
    base_url = settings.POLYMARKET_BASE_URL.rstrip("/")
    candidates = [
        f"{base_url}/markets/{market_id}",
        f"{base_url}/markets?slug={market_id}",
        f"{base_url}/markets?market_id={market_id}",
        f"{base_url}/markets?id={market_id}",
    ]
    for url in candidates:
        try:
            response = httpx.get(url, timeout=10)
            if not response.is_success:
                continue
            payload = response.json()
        except Exception:
            logger.exception("polymarket_gamma_fetch_failed url=%s", url)
            continue
        market = _extract_market_payload(payload, market_id)
        if market:
            return market
    return None


def _extract_market_payload(payload: Any, market_id: str) -> dict[str, Any] | None:
    if isinstance(payload, dict):
        market = payload.get("market")
        if isinstance(market, dict):
            return market
        markets = payload.get("markets")
        if isinstance(markets, list):
            return _select_market(markets, market_id)
    if isinstance(payload, list):
        return _select_market(payload, market_id)
    return None


def _select_market(markets: list[Any], market_id: str) -> dict[str, Any] | None:
    if not markets:
        return None
    for market in markets:
        if not isinstance(market, dict):
            continue
        candidate = str(
            market.get("slug")
            or market.get("id")
            or market.get("marketId")
            or market.get("market_id")
            or ""
        )
        if candidate == market_id:
            return market
    return markets[0] if isinstance(markets[0], dict) else None


def _extract_labeled_tokens(market: dict[str, Any]) -> list[tuple[str, str]]:
    tokens: list[tuple[str, str]] = []

    outcome_tokens = market.get("outcomeTokens") or market.get("outcomeToken") or []
    if isinstance(outcome_tokens, str):
        try:
            outcome_tokens = json.loads(outcome_tokens)
        except json.JSONDecodeError:
            outcome_tokens = []
    if isinstance(outcome_tokens, list):
        for token in outcome_tokens:
            if not isinstance(token, dict):
                continue
            label = (
                token.get("outcome")
                or token.get("label")
                or token.get("name")
                or token.get("title")
            )
            token_id = token.get("tokenId") or token.get("clobTokenId") or token.get("id")
            if label and token_id is not None:
                tokens.append((str(label), str(token_id)))

    outcomes = _coerce_list(
        market.get("outcomes")
        or market.get("outcomeNames")
        or market.get("outcomeLabels")
        or market.get("outcomeTokenNames")
    )
    token_ids = _coerce_list(
        market.get("outcomeTokenIds")
        or market.get("clobTokenIds")
        or market.get("tokenIds")
    )
    if outcomes and token_ids:
        for idx, label in enumerate(outcomes):
            if idx >= len(token_ids):
                break
            if label is None:
                continue
            tokens.append((str(label), str(token_ids[idx])))

    tokens.extend(_extract_tokens_from_nested(market.get("tokens")))
    return tokens


def _extract_indexed_tokens(market: dict[str, Any]) -> list[str]:
    token_ids = _coerce_list(
        market.get("outcomeTokenIds")
        or market.get("clobTokenIds")
        or market.get("tokenIds")
    )
    if token_ids:
        return [str(token_id) for token_id in token_ids if token_id is not None]
    outcome_tokens = market.get("outcomeTokens") or []
    if isinstance(outcome_tokens, list):
        ordered: list[str] = []
        for token in outcome_tokens:
            if not isinstance(token, dict):
                continue
            token_id = token.get("tokenId") or token.get("clobTokenId") or token.get("id")
            if token_id is not None:
                ordered.append(str(token_id))
        return ordered
    return []


def _extract_tokens_from_nested(tokens: Any) -> list[tuple[str, str]]:
    if not isinstance(tokens, list):
        return []
    labeled: list[tuple[str, str]] = []
    for token in tokens:
        if not isinstance(token, dict):
            continue
        label = token.get("outcome") or token.get("label") or token.get("name") or token.get("title")
        token_id = token.get("tokenId") or token.get("clobTokenId") or token.get("id")
        if label and token_id is not None:
            labeled.append((str(label), str(token_id)))
    return labeled


def _coerce_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _normalize_label(label: str | None) -> str:
    if not label:
        return ""
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in str(label).strip()).strip("_")
    return cleaned.upper()
