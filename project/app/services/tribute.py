from __future__ import annotations

import asyncio
import hmac
import json
import logging
from dataclasses import dataclass
from hashlib import sha256
from typing import Any, Callable, Dict, Optional

from aiohttp import web

from ..config import config
from ..db.queries import CustomerRepository
from .business import PaymentService

logger = logging.getLogger(__name__)


@dataclass
class TributePayload:
    subscription_name: str
    subscription_id: int
    period_id: int
    period: str
    price: int
    amount: int
    currency: str
    user_id: int
    telegram_user_id: int
    channel_id: int
    channel_name: str
    expires_at: str


@dataclass
class TributeWebhook:
    name: str
    payload: TributePayload


def _convert_period_to_months(period: str) -> int:
    period = period.lower()
    if period in {"monthly"}:
        return 1
    if period in {"quarterly", "3-month", "3months", "3-months", "q"}:
        return 3
    if period in {"halfyearly"}:
        return 6
    if period in {"yearly", "annual", "y"}:
        return 12
    return 1


def build_tribute_handler(payment_service: PaymentService, customer_repo: CustomerRepository) -> web.View:
    async def handler(request: web.Request) -> web.Response:
        body = await request.read()
        signature = request.headers.get("trbt-signature")
        if not signature:
            return web.Response(status=401, text="missing signature")

        expected = hmac.new(config.tribute_api_key.encode(), body, sha256).hexdigest()
        if not hmac.compare_digest(expected, signature):
            logger.warning("tribute webhook: bad signature expected=%s got=%s", expected, signature)
            return web.Response(status=401, text="invalid signature")

        try:
            payload_json = json.loads(body.decode())
            webhook = TributeWebhook(
                name=payload_json.get("name", ""),
                payload=TributePayload(**payload_json.get("payload", {})),
            )
        except Exception as err:  # noqa: BLE001
            logger.exception("tribute webhook: invalid payload: %s", err)
            return web.Response(status=400, text="invalid payload")

        try:
            if webhook.name == "cancelled_subscription":
                await payment_service.cancel_tribute_purchase(webhook.payload.telegram_user_id)
            elif webhook.name in {"", "new_subscription"}:
                months = _convert_period_to_months(webhook.payload.period)
                customer = await customer_repo.find_by_telegram_id(webhook.payload.telegram_user_id)
                if not customer:
                    raise RuntimeError(f"customer not found for telegram_id={webhook.payload.telegram_user_id}")
                _, purchase_id, _ = await payment_service.create_purchase(
                    amount=float(webhook.payload.amount),
                    months=months,
                    customer=customer,
                    invoice_type="tribute",
                    username=None,
                )
                await payment_service.process_purchase_by_id(purchase_id, username=None)
        except Exception as err:  # noqa: BLE001
            logger.exception("tribute webhook error: %s", err)
            return web.Response(status=500, text="internal error")

        return web.Response(status=200, text="ok")

    return handler

