from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import aiohttp


class AuthError(Exception):
    pass


class RetryableError(Exception):
    pass


@dataclass
class DeviceInfo:
    sourceDeviceId: str = "*"
    sourceType: str = "WEB"
    appVersion: str = "1.0.0"
    metaDetails: Optional[Dict[str, Any]] = None


class MoynalogClient:
    def __init__(self, base_url: str, username: str, password: str, session: aiohttp.ClientSession) -> None:
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.session = session
        self._token: Optional[str] = None
        self._auth_lock = asyncio.Lock()

    async def _authenticate(self) -> None:
        async with self._auth_lock:
            if self._token:
                return
            payload = {
                "username": self.username,
                "password": self.password,
                "deviceInfo": DeviceInfo().__dict__,
            }
            async with self.session.post(f"{self.base_url}/auth/lkfl", json=payload) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    raise AuthError(f"auth failed: {resp.status} {text}")
                data = await resp.json()
                self._token = data.get("token")
                if not self._token:
                    raise AuthError("auth token missing")

    async def create_income(self, amount: float, comment: str) -> Dict[str, Any]:
        max_retries = 3
        base_delay = 0.5
        auth_retries = 0
        last_err: Optional[Exception] = None

        for attempt in range(max_retries):
            try:
                return await self._create_income_once(amount, comment)
            except AuthError as err:
                last_err = err
                self._token = None
                if auth_retries >= 2:
                    break
                auth_retries += 1
                continue
            except RetryableError as err:
                last_err = err
                await asyncio.sleep(base_delay * (2**attempt))
                continue
            except Exception as err:  # noqa: BLE001
                last_err = err
                break
        if last_err:
            raise last_err
        raise RuntimeError("create_income failed without explicit error")

    async def _create_income_once(self, amount: float, comment: str) -> Dict[str, Any]:
        if not self._token:
            await self._authenticate()
        now = datetime.utcnow().isoformat()
        payload = {
            "operationTime": now,
            "requestTime": now,
            "services": [{"name": comment, "amount": amount, "quantity": 1}],
            "totalAmount": f"{amount:.2f}",
            "client": {"incomeType": "FROM_INDIVIDUAL"},
            "paymentType": "CASH",
            "ignoreMaxTotalIncomeRestriction": False,
        }
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",
            "Authorization": f"Bearer {self._token}",
        }
        async with self.session.post(f"{self.base_url}/income", json=payload, headers=headers) as resp:
            if resp.status in (401, 403):
                raise AuthError(f"status {resp.status}")
            if resp.status >= 500:
                text = await resp.text()
                raise RetryableError(f"status {resp.status} body={text}")
            if resp.status not in (200, 201):
                text = await resp.text()
                raise RuntimeError(f"create income failed: {resp.status} body={text}")
            return await resp.json()

