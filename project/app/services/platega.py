from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import aiohttp
from aiohttp import web

from ..config import config
from ..db.queries import PurchaseRepository

logger = logging.getLogger(__name__)


class PlategaClient:
    def __init__(self, base_url: str, merchant_id: str, secret: str, session: aiohttp.ClientSession) -> None:
        self.base_url = base_url.rstrip("/")
        self.merchant_id = merchant_id
        self.secret = secret
        self.session = session

    def _headers(self) -> Dict[str, str]:
        return {
            "Content-Type": "application/json",
            "X-MerchantId": self.merchant_id,
            "X-Secret": self.secret,
        }

    async def create_transaction(
        self,
        amount: int,
        description: str,
        return_url: str,
        failed_url: str,
        payload: str,
        payment_method: int = 2,
        currency: str = "RUB",
    ) -> Dict[str, Any]:
        body = {
            "paymentMethod": payment_method,
            "paymentDetails": {"amount": amount, "currency": currency},
            "description": description,
            "return": return_url,
            "failedUrl": failed_url,
            "payload": payload,
        }
        async with self.session.post(f"{self.base_url}/transaction/process", json=body, headers=self._headers()) as resp:
            try:
                data = await resp.json()
            except Exception:  # noqa: BLE001
                text = await resp.text()
                raise RuntimeError(f"Platega create transaction failed: status={resp.status}, body={text}") from None
            if resp.status not in (200, 201):
                raise RuntimeError(f"Platega create transaction failed: status={resp.status}, body={data}")
            return data

    async def get_transaction(self, transaction_id: str) -> Dict[str, Any]:
        async with self.session.get(
            f"{self.base_url}/transaction/{transaction_id}", headers=self._headers()
        ) as resp:
            try:
                data = await resp.json()
            except Exception:  # noqa: BLE001
                text = await resp.text()
                raise RuntimeError(f"Platega get transaction failed: status={resp.status}, body={text}") from None
            if resp.status != 200:
                raise RuntimeError(f"Platega get transaction failed: status={resp.status}, body={data}")
            return data


def build_platega_handler(payment_service: "PaymentService", purchase_repo: PurchaseRepository) -> web.View:
    async def handler(request: web.Request) -> web.Response:
        merchant_header = request.headers.get("X-MerchantId")
        secret_header = request.headers.get("X-Secret")
        if merchant_header != config.platega_merchant_id or secret_header != config.platega_secret:
            return web.Response(status=401, text="unauthorized")

        try:
            payload: Dict[str, Any] = await request.json()
        except Exception as err:  # noqa: BLE001
            logger.warning("platega webhook: invalid json: %s", err)
            return web.Response(status=400, text="invalid json")

        transaction_id = payload.get("id") or payload.get("transactionId")
        status = (payload.get("status") or "").upper()
        if not transaction_id:
            return web.Response(status=400, text="missing transaction id")

        purchase = await purchase_repo.find_by_platega_transaction_id(transaction_id)
        if not purchase:
            logger.warning("platega webhook: purchase not found for transaction id=%s", transaction_id)
            return web.Response(status=200, text="ok")

        try:
            if purchase.status == "paid":
                return web.Response(status=200, text="ok")
            if status == "CONFIRMED":
                await payment_service.process_purchase_by_id(purchase.id, username=None)
            elif status == "CANCELED":
                await payment_service.cancel_platega_payment(purchase.id)
        except Exception as err:  # noqa: BLE001
            logger.exception("platega webhook error: %s", err)
            return web.Response(status=500, text="internal error")
        return web.Response(status=200, text="ok")

    return handler

