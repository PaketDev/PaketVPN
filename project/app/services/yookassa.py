from __future__ import annotations

import base64
import uuid
from typing import Any, Dict, Optional

import aiohttp

from ..config import config


def _month_string(month: int) -> str:
    if month == 1:
        return "ђ?ђз‘?‘?‘Е"
    if month in {3, 4}:
        return "ђ?ђз‘?‘?‘Еђш"
    return "ђ?ђз‘?‘?‘Еђзђ?"


class YookassaClient:
    def __init__(self, base_url: str, shop_id: str, secret_key: str, session: aiohttp.ClientSession) -> None:
        self.base_url = base_url.rstrip("/")
        auth = f"{shop_id}:{secret_key}"
        self.auth_header = f"Basic {base64.b64encode(auth.encode()).decode()}"
        self.session = session

    async def create_invoice(
        self,
        amount: int,
        month: int,
        customer_id: int,
        purchase_id: int,
        username: Optional[str],
    ) -> Dict[str, Any]:
        rub = {"value": str(amount), "currency": "RUB"}
        description = f"ђ?ђ?ђ?ђхђс‘?ђуђш ђ?ђш {month} {_month_string(month)}"
        receipt = {
            "customer": {"email": config.yookasa_email},
            "items": [
                {
                    "vat_code": 1,
                    "quantity": "1",
                    "description": description,
                    "amount": rub,
                    "payment_subject": "payment",
                    "payment_mode": "full_payment",
                }
            ],
        }
        metadata = {"customerId": customer_id, "purchaseId": purchase_id}
        if username:
            metadata["username"] = username

        return_url = config.bot_url or config.mini_app_url or config.support_url or ""
        request = {
            "amount": rub,
            "confirmation": {"type": "redirect", "return_url": return_url},
            "capture": True,
            "description": description,
            "receipt": receipt,
            "metadata": metadata,
        }

        headers = {
            "Content-Type": "application/json",
            "Authorization": self.auth_header,
            "Idempotence-Key": str(uuid.uuid4()),
        }
        async with self.session.post(f"{self.base_url}/payments", json=request, headers=headers) as resp:
            data = await resp.json()
            if resp.status not in (200, 201):
                raise RuntimeError(f"Yookassa create payment failed: status={resp.status}, body={data}")
            return data

    async def get_payment(self, payment_id: str) -> Dict[str, Any]:
        headers = {"Authorization": self.auth_header}
        async with self.session.get(f"{self.base_url}/payments/{payment_id}", headers=headers) as resp:
            data = await resp.json()
            if resp.status != 200:
                raise RuntimeError(f"Yookassa get payment failed: status={resp.status}, body={data}")
            return data
