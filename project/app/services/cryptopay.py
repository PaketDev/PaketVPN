from __future__ import annotations

from typing import Any, Dict, List, Optional

import aiohttp


class CryptoPayClient:
    def __init__(self, base_url: str, token: str, session: aiohttp.ClientSession) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.session = session

    async def create_invoice(self, invoice_request: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}/api/createInvoice"
        headers = {"Content-Type": "application/json", "Crypto-Pay-API-Token": self.token}
        async with self.session.post(url, json=invoice_request, headers=headers) as resp:
            data = await resp.json()
            if resp.status != 200 or not data.get("ok"):
                raise RuntimeError(f"CryptoPay create invoice failed: status={resp.status}, body={data}")
            return data["result"]

    async def get_invoices(
        self,
        status: str = "",
        fiat: str = "",
        asset: str = "",
        invoice_ids: str = "",
        offset: int = 0,
        limit: int = 0,
    ) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/api/getInvoices"
        params: Dict[str, Any] = {}
        if status:
            params["status"] = status
        if fiat:
            params["fiat"] = fiat
        if asset:
            params["asset"] = asset
        if invoice_ids:
            params["invoice_ids"] = invoice_ids
        if offset:
            params["offset"] = offset
        if limit:
            params["limit"] = limit

        headers = {"Crypto-Pay-API-Token": self.token}
        async with self.session.get(url, params=params, headers=headers) as resp:
            data = await resp.json()
            if resp.status != 200 or not data.get("ok"):
                raise RuntimeError(f"CryptoPay get invoices failed: status={resp.status}, body={data}")
            return data["result"]["items"]

