from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass
import json
from typing import Any, Dict, List, Optional

import aiohttp

from ..config import config


@dataclass
class RemnawaveUser:
    uuid: str
    telegram_id: Optional[int]
    expire_at: dt.datetime
    subscription_url: Optional[str]
    traffic_limit_bytes: Optional[int] = None
    traffic_used_bytes: Optional[int] = None
    description: Optional[str] = None
    raw: Dict[str, Any] = None


def _pick_int(source: Dict[str, Any], keys: List[str]) -> Optional[int]:
    for key in keys:
        if key not in source:
            continue
        try:
            return int(source[key])
        except (TypeError, ValueError):
            continue
    return None


def _iso(dt_value: dt.datetime) -> str:
    value = dt_value
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.timezone.utc)
    return value.isoformat().replace("+00:00", "Z")


def _parse_dt(value: Optional[str]) -> Optional[dt.datetime]:
    if not value:
        return None
    try:
        return dt.datetime.fromisoformat(value)
    except ValueError:
        return None


def _add_days(current: dt.datetime, days: int) -> dt.datetime:
    if days <= 0:
        candidate = current + dt.timedelta(days=days)
        if candidate < dt.datetime.utcnow():
            return dt.datetime.utcnow() + dt.timedelta(days=1)
        return candidate
    if current < dt.datetime.utcnow():
        return dt.datetime.utcnow() + dt.timedelta(days=days)
    return current + dt.timedelta(days=days)


def _normalize_tag(tag: Optional[str]) -> Optional[str]:
    if tag is None:
        return None
    raw = str(tag).strip()
    if not raw:
        return ""
    # Remnawave: only uppercase letters, digits and underscore are allowed.
    return re.sub(r"[^A-Za-z0-9_]+", "_", raw).upper().strip("_")


class RemnawaveClient:
    """Simplified Remnawave HTTP client. Endpoints are inferred from Go code."""

    def __init__(self, base_url: str, token: str, mode: str, session: aiohttp.ClientSession) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.local = mode == "local"
        self.session = session
        self.default_headers = {"Authorization": f"Bearer {token}", **config.remnawave_headers}

    async def ping(self) -> None:
        await self._request("GET", "/users", params={"limit": 1, "offset": 0})

    def _extract_user_items(self, data: Any) -> Optional[List[Dict[str, Any]]]:
        if isinstance(data, list):
            if not data:
                return []
            if all(isinstance(item, dict) for item in data):
                return data
            return None
        if not isinstance(data, dict):
            return None

        for key in ("response", "data", "users", "items", "result", "list", "rows"):
            if key not in data:
                continue
            nested = self._extract_user_items(data.get(key))
            if nested is not None:
                return nested

        for value in data.values():
            if isinstance(value, list) and (not value or all(isinstance(item, dict) for item in value)):
                return value
        return None

    async def _request(
        self, method: str, path: str, params: Optional[Dict[str, Any]] = None, json: Any = None
    ) -> Any:
        url = f"{self.base_url}{path}"
        headers = dict(self.default_headers)
        if self.local:
            headers["x-forwarded-for"] = "127.0.0.1"
            headers["x-forwarded-proto"] = "https"
        async with self.session.request(method, url, params=params, json=json, headers=headers) as resp:
            if resp.status >= 400:
                body = await resp.text()
                raise RuntimeError(f"Remnawave request failed: {resp.status} {body}")
            if resp.content_type == "application/json":
                return await resp.json()
            return await resp.text()

    async def _get_users_by_path(self, path: str) -> List[RemnawaveUser]:
        users: List[RemnawaveUser] = []
        # Some Remnawave deployments hard-cap page size to 25 and may ignore larger limits.
        # Use small explicit paging and stop when page yields no new user ids.
        limit = 25
        offset = 0
        page_size_hint: Optional[int] = None
        seen_ids: set[str] = set()
        max_pages = 2000
        pages_processed = 0
        while True:
            pages_processed += 1
            if pages_processed > max_pages:
                break
            data = await self._request("GET", path, params={"limit": limit, "offset": offset})
            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except json.JSONDecodeError:
                    break
            items = self._extract_user_items(data)
            if not isinstance(items, list):
                break
            if not items:
                break

            if page_size_hint is None:
                page_size_hint = len(items)

            new_in_page = 0
            for raw in items:
                raw_id = str(raw.get("uuid") or raw.get("id") or "")
                if raw_id and raw_id in seen_ids:
                    continue
                if raw_id:
                    seen_ids.add(raw_id)
                users.append(self._map_user(raw))
                new_in_page += 1

            # Endpoint may ignore offset and keep returning same first page.
            if new_in_page == 0:
                break

            step = page_size_hint or len(items) or limit
            offset += step
        return users

    async def get_users(self) -> List[RemnawaveUser]:
        for path in ("/api/users", "/users"):
            try:
                users = await self._get_users_by_path(path)
            except Exception:
                continue
            if users:
                return users
        return []

    def _map_user(self, raw: Dict[str, Any]) -> RemnawaveUser:
        expire = _parse_dt(raw.get("expireAt") or raw.get("expire_at")) or dt.datetime.utcnow()
        if expire.tzinfo:
            expire = expire.astimezone(dt.timezone.utc).replace(tzinfo=None)
        limit_val = _pick_int(
            raw,
            [
                "trafficLimitBytes",
                "traffic_limit_bytes",
                "trafficLimit",
                "traffic_limit",
                "limitBytes",
                "limit",
            ],
        )
        used_val = _pick_int(
            raw,
            [
                "trafficUsedBytes",
                "traffic_used_bytes",
                "trafficUsed",
                "traffic_used",
                "usedBytes",
                "used",
            ],
        )
        nested = raw.get("traffic") or raw.get("usage") or raw.get("stats") or raw.get("userTraffic")
        if isinstance(nested, dict):
            if limit_val is None:
                limit_val = _pick_int(
                    nested,
                    ["trafficLimitBytes", "traffic_limit_bytes", "limitBytes", "limit"],
                )
            if used_val is None:
                used_val = _pick_int(
                    nested,
                    [
                        "trafficUsedBytes",
                        "traffic_used_bytes",
                        "usedBytes",
                        "used",
                        "usedTrafficBytes",
                        "lifetimeUsedTrafficBytes",
                    ],
                )
        return RemnawaveUser(
            uuid=str(raw.get("uuid") or raw.get("id")),
            telegram_id=raw.get("telegramId") or raw.get("telegram_id"),
            expire_at=expire,
            subscription_url=raw.get("subscriptionUrl") or raw.get("subscription_url"),
            traffic_limit_bytes=limit_val,
            traffic_used_bytes=used_val,
            description=raw.get("description"),
            raw=raw,
        )

    async def _get_user_by_telegram(self, telegram_id: int) -> Optional[RemnawaveUser]:
        for path in (f"/api/users/by-telegram-id/{telegram_id}", f"/users/by-telegram-id/{telegram_id}"):
            try:
                data = await self._request("GET", path)
            except Exception:
                continue
            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except json.JSONDecodeError:
                    continue
            items = self._extract_user_items(data)
            if not items:
                continue
            matched: Optional[RemnawaveUser] = None
            suffix = f"_{telegram_id}"
            for raw in items:
                if suffix in str(raw.get("username", "")):
                    matched = self._map_user(raw)
                    break
            if not matched:
                matched = self._map_user(items[0])
            return matched
        return None

    async def fetch_user_by_telegram(self, telegram_id: int) -> Optional[RemnawaveUser]:
        return await self._get_user_by_telegram(telegram_id)

    async def reset_user_traffic(self, user_uuid: str) -> bool:
        if not user_uuid:
            return False
        candidates = [
            ("POST", f"/api/users/{user_uuid}/reset-traffic"),
            ("POST", f"/api/users/reset-traffic/{user_uuid}"),
            ("POST", f"/api/users/{user_uuid}/resetTraffic"),
            ("POST", f"/api/users/resetTraffic/{user_uuid}"),
            ("PATCH", f"/api/users/{user_uuid}/reset-traffic"),
            ("PATCH", f"/api/users/reset-traffic/{user_uuid}"),
        ]
        headers = dict(self.default_headers)
        if self.local:
            headers["x-forwarded-for"] = "127.0.0.1"
            headers["x-forwarded-proto"] = "https"
        for method, path in candidates:
            url = f"{self.base_url}{path}"
            async with self.session.request(method, url, headers=headers) as resp:
                if 200 <= resp.status < 300:
                    return True
                if resp.status in {404, 405}:
                    continue
                body = await resp.text()
                raise RuntimeError(f"Remnawave reset traffic failed: {resp.status} {body}")
        return False

    async def reset_user_traffic_by_telegram(self, telegram_id: int) -> bool:
        user = await self._get_user_by_telegram(telegram_id)
        if not user:
            return False
        return await self.reset_user_traffic(user.uuid)

    async def delete_user(self, user_uuid: str) -> bool:
        if not user_uuid:
            return False
        candidates = [
            ("DELETE", f"/api/users/{user_uuid}"),
            ("DELETE", f"/users/{user_uuid}"),
            ("DELETE", f"/api/users/delete/{user_uuid}"),
            ("DELETE", f"/users/delete/{user_uuid}"),
            ("POST", f"/api/users/{user_uuid}/delete"),
            ("POST", f"/users/{user_uuid}/delete"),
        ]
        headers = dict(self.default_headers)
        if self.local:
            headers["x-forwarded-for"] = "127.0.0.1"
            headers["x-forwarded-proto"] = "https"
        for method, path in candidates:
            url = f"{self.base_url}{path}"
            async with self.session.request(method, url, headers=headers) as resp:
                if 200 <= resp.status < 300:
                    return True
                if resp.status in {404, 405}:
                    continue
                body = await resp.text()
                raise RuntimeError(f"Remnawave delete user failed: {resp.status} {body}")
        return False

    async def delete_user_by_telegram(self, telegram_id: int) -> bool:
        user = await self._get_user_by_telegram(telegram_id)
        if not user:
            # User is already absent in Remnawave.
            return True
        return await self.delete_user(user.uuid)

    async def decrease_subscription(self, telegram_id: int, traffic_limit_bytes: int, days: int) -> Optional[dt.datetime]:
        user = await self._get_user_by_telegram(telegram_id)
        if not user:
            return None
        new_expire = _add_days(user.expire_at, days)
        payload = self._build_update_payload(user, traffic_limit_bytes, days, description=None, expire_at=new_expire)
        updated = await self._request("PATCH", "/api/users", json=payload)
        mapped = self._map_user(updated.get("response", updated))
        return mapped.expire_at

    async def set_user_expire_at(
        self,
        telegram_id: int,
        expire_at: dt.datetime,
        traffic_limit_bytes: Optional[int] = None,
        is_trial_user: bool = False,
        description: Optional[str] = None,
    ) -> Optional[RemnawaveUser]:
        user = await self._get_user_by_telegram(telegram_id)
        if not user:
            return None
        limit_bytes = int(traffic_limit_bytes or user.traffic_limit_bytes or config.traffic_limit_bytes)
        payload = self._build_update_payload(
            user=user,
            traffic_limit_bytes=limit_bytes,
            days=0,
            description=description,
            expire_at=expire_at,
            is_trial_user=is_trial_user,
        )
        updated = await self._request("PATCH", "/api/users", json=payload)
        return self._map_user(updated.get("response", updated))

    def _select_squads(self, is_trial_user: bool) -> List[str]:
        squads = config.trial_internal_squads if is_trial_user and config.trial_internal_squads else config.squad_uuids
        if squads:
            return [str(uid) for uid in squads.values()]
        return []

    def _build_update_payload(
        self,
        user: RemnawaveUser,
        traffic_limit_bytes: int,
        days: int,
        description: Optional[str],
        expire_at: Optional[dt.datetime] = None,
        is_trial_user: bool = False,
        tag_override: Optional[str] = None,
    ) -> Dict[str, Any]:
        expire_value = expire_at or _add_days(user.expire_at, days)
        payload: Dict[str, Any] = {
            "uuid": user.uuid,
            "expireAt": _iso(expire_value),
            "status": "ACTIVE",
            "trafficLimitBytes": traffic_limit_bytes,
            "activeInternalSquads": self._select_squads(is_trial_user),
            "trafficLimitStrategy": config.traffic_limit_reset_strategy if hasattr(config, "traffic_limit_reset_strategy") else "MONTH",
        }
        external = config.trial_external_squad_uuid if is_trial_user and config.trial_external_squad_uuid else config.external_squad_uuid
        if external:
            payload["external_squad_uuid"] = str(external)
        tag = _normalize_tag(
            tag_override
            if tag_override is not None
            else (config.trial_remnawave_tag if is_trial_user and config.trial_remnawave_tag else config.remnawave_tag)
        )
        if tag:
            payload["tag"] = tag
        if description:
            payload["description"] = description
        return payload

    async def _update_user(
        self,
        user: RemnawaveUser,
        traffic_limit_bytes: int,
        days: int,
        is_trial_user: bool,
        description: Optional[str],
        tag: Optional[str],
    ) -> RemnawaveUser:
        payload = self._build_update_payload(
            user=user,
            traffic_limit_bytes=traffic_limit_bytes,
            days=days,
            description=description,
            is_trial_user=is_trial_user,
            tag_override=tag,
        )
        data = await self._request("PATCH", "/api/users", json=payload)
        return self._map_user(data.get("response", data))

    async def _create_user(
        self,
        customer_id: int,
        telegram_id: int,
        traffic_limit_bytes: int,
        days: int,
        is_trial_user: bool,
        description: Optional[str],
        tag: Optional[str],
    ) -> RemnawaveUser:
        expire_at = dt.datetime.utcnow() + dt.timedelta(days=days)
        payload = {
            "username": f"{customer_id}_{telegram_id}",
            "activeInternalSquads": self._select_squads(is_trial_user),
            "status": "ACTIVE",
            "telegramId": telegram_id,
            "expireAt": _iso(expire_at),
            "trafficLimitBytes": traffic_limit_bytes,
            "trafficLimitStrategy": config.trial_traffic_limit_reset_strategy if is_trial_user else getattr(config, "traffic_limit_reset_strategy", "MONTH"),
        }
        external = config.trial_external_squad_uuid if is_trial_user and config.trial_external_squad_uuid else config.external_squad_uuid
        if external:
            payload["external_squad_uuid"] = str(external)
        tag = _normalize_tag(
            tag
            if tag is not None
            else (config.trial_remnawave_tag if is_trial_user and config.trial_remnawave_tag else config.remnawave_tag)
        )
        if tag:
            payload["tag"] = tag
        if description:
            payload["description"] = description
        data = await self._request("POST", "/api/users", json=payload)
        return self._map_user(data.get("response", data))

    async def create_or_update_user(
        self,
        customer_id: int,
        telegram_id: int,
        traffic_limit_bytes: int,
        days: int,
        is_trial_user: bool,
        username: Optional[str],
        tag: Optional[str] = None,
    ) -> RemnawaveUser:
        existing = await self._get_user_by_telegram(telegram_id)
        if existing:
            return await self._update_user(existing, traffic_limit_bytes, days, is_trial_user, username, tag)
        return await self._create_user(customer_id, telegram_id, traffic_limit_bytes, days, is_trial_user, username, tag)
