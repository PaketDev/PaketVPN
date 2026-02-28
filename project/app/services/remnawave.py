from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass
import json
from typing import Any, Dict, List, Optional
from urllib.parse import quote

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
        page = 1
        while True:
            pages_processed += 1
            if pages_processed > max_pages:
                break
            data = await self._request(
                "GET",
                path,
                params={"limit": limit, "offset": offset, "page": page},
            )
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
            page += 1
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

    def _extract_user_object(self, data: Any) -> Optional[Dict[str, Any]]:
        if isinstance(data, dict):
            if data.get("uuid") and (
                data.get("expireAt")
                or data.get("expire_at")
                or data.get("username")
                or data.get("telegramId")
                or data.get("telegram_id")
            ):
                return data

            for key in ("response", "data", "result", "user", "item"):
                if key not in data:
                    continue
                nested = self._extract_user_object(data.get(key))
                if nested:
                    return nested

            for value in data.values():
                nested = self._extract_user_object(value)
                if nested:
                    return nested
            return None

        if isinstance(data, list):
            for item in data:
                nested = self._extract_user_object(item)
                if nested:
                    return nested
        return None

    async def _get_user_by_uuid(self, user_uuid: str) -> Optional[RemnawaveUser]:
        if not user_uuid:
            return None
        for path in (f"/api/users/{user_uuid}", f"/users/{user_uuid}"):
            try:
                data = await self._request("GET", path)
            except Exception:
                continue
            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except json.JSONDecodeError:
                    continue
            raw_user = self._extract_user_object(data)
            if not raw_user:
                continue
            return self._map_user(raw_user)
        return None

    def _extract_device_candidates(self, payload: Any, parent_key: str = "") -> List[Dict[str, Any]]:
        result: List[Dict[str, Any]] = []
        parent_lower = parent_key.lower()

        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    result.extend(self._extract_device_candidates(item, parent_key))
                elif isinstance(item, str) and "device" in parent_lower:
                    result.append({"deviceId": item, "name": item})
            return result

        if not isinstance(payload, dict):
            return result

        keys_lower = {str(key).lower() for key in payload.keys()}
        id_keys = {
            "deviceid",
            "device_id",
            "sourcedeviceid",
            "source_device_id",
            "clientid",
            "client_id",
        }
        meta_keys = {
            "lastseen",
            "last_seen",
            "lastseenat",
            "last_seen_at",
            "ip",
            "ipaddress",
            "address",
            "remoteaddress",
            "useragent",
            "os",
            "platform",
            "model",
        }
        if (id_keys & keys_lower) or ("device" in parent_lower and (meta_keys & keys_lower)):
            result.append(payload)

        device_list_keys = {"clients", "clientstats", "sessions", "connections"}
        generic_list_keys = {"items", "list", "rows"}
        for key, value in payload.items():
            key_lower = str(key).lower()
            if isinstance(value, list):
                if "device" in key_lower or key_lower in device_list_keys or (
                    key_lower in generic_list_keys and "device" in parent_lower
                ):
                    for item in value:
                        if isinstance(item, dict):
                            result.append(item)
                        elif isinstance(item, str):
                            result.append({"deviceId": item, "name": item})
                else:
                    result.extend(self._extract_device_candidates(value, key_lower))
            elif isinstance(value, dict):
                result.extend(self._extract_device_candidates(value, key_lower))
        return result

    def _normalize_device(self, raw_device: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        device_id = str(
            raw_device.get("deviceId")
            or raw_device.get("device_id")
            or raw_device.get("sourceDeviceId")
            or raw_device.get("source_device_id")
            or raw_device.get("hwid")
            or raw_device.get("clientId")
            or raw_device.get("client_id")
            or raw_device.get("id")
            or raw_device.get("uuid")
            or ""
        ).strip()
        if not device_id:
            return None

        name = str(
            raw_device.get("deviceName")
            or raw_device.get("device_name")
            or raw_device.get("name")
            or raw_device.get("title")
            or raw_device.get("remark")
            or raw_device.get("deviceModel")
            or raw_device.get("device_model")
            or raw_device.get("userAgent")
            or raw_device.get("user_agent")
            or ""
        ).strip()
        if not name:
            model = str(raw_device.get("model") or "").strip()
            platform = str(raw_device.get("platform") or raw_device.get("os") or "").strip()
            if model and platform:
                name = f"{platform} {model}"
            elif model:
                name = model
            elif platform:
                name = platform
            else:
                name = device_id

        ip_value = str(
            raw_device.get("ip")
            or raw_device.get("ipAddress")
            or raw_device.get("ip_address")
            or raw_device.get("remoteAddress")
            or raw_device.get("remote_address")
            or raw_device.get("address")
            or ""
        ).strip()

        last_seen_value = (
            raw_device.get("lastSeenAt")
            or raw_device.get("last_seen_at")
            or raw_device.get("lastSeen")
            or raw_device.get("last_seen")
            or raw_device.get("updatedAt")
            or raw_device.get("updated_at")
            or raw_device.get("createdAt")
            or raw_device.get("created_at")
        )
        last_seen = str(last_seen_value).strip() if last_seen_value is not None else ""

        current_raw = raw_device.get("isCurrent")
        if current_raw is None:
            current_raw = raw_device.get("currentDevice")
        if current_raw is None:
            current_raw = raw_device.get("current")
        is_current = (
            bool(current_raw)
            if isinstance(current_raw, bool)
            else str(current_raw).strip().lower() in {"1", "true", "yes", "current"}
        )

        online_raw = raw_device.get("isOnline")
        if online_raw is None:
            online_raw = raw_device.get("online")
        if online_raw is None:
            online_raw = raw_device.get("isActive")
        if online_raw is None:
            online_raw = raw_device.get("active")
        is_online = (
            bool(online_raw)
            if isinstance(online_raw, bool)
            else str(online_raw).strip().lower() in {"1", "true", "yes", "online", "active"}
        )

        return {
            "id": device_id,
            "name": name,
            "ip": ip_value,
            "last_seen": last_seen,
            "is_current": is_current,
            "is_online": is_online,
            "raw": raw_device,
        }

    def _extract_device_usage(self, raw_user: Dict[str, Any], devices: List[Dict[str, Any]]) -> tuple[Optional[int], Optional[int]]:
        limit = _pick_int(
            raw_user,
            [
                "deviceLimit",
                "device_limit",
                "maxDevices",
                "max_devices",
                "activeDeviceLimit",
                "active_device_limit",
                "hwidDeviceLimit",
                "hwid_device_limit",
                "ipLimit",
                "ip_limit",
                "limitIp",
            ],
        )
        used = _pick_int(
            raw_user,
            [
                "usedDevices",
                "used_devices",
                "activeDevices",
                "active_devices",
                "devicesCount",
                "devices_count",
                "deviceCount",
                "device_count",
            ],
        )

        devices_section = raw_user.get("devices")
        if isinstance(devices_section, dict):
            if limit is None:
                limit = _pick_int(
                    devices_section,
                    [
                        "deviceLimit",
                        "device_limit",
                        "maxDevices",
                        "max_devices",
                        "limit",
                    ],
                )
            if used is None:
                used = _pick_int(
                    devices_section,
                    [
                        "used",
                        "count",
                        "active",
                        "activeCount",
                        "active_count",
                        "total",
                    ],
                )

        if used is None and devices:
            used = len(devices)

        return used, limit

    def _extract_hwid_devices_payload(self, data: Any) -> tuple[List[Dict[str, Any]], Optional[int]]:
        payload = data
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                return [], None

        containers: List[Any] = []
        if isinstance(payload, dict):
            containers.append(payload)
            for key in ("response", "data", "result"):
                if key in payload and isinstance(payload.get(key), dict):
                    containers.append(payload.get(key))
        elif isinstance(payload, list):
            containers.append({"devices": payload, "total": len(payload)})

        for container in containers:
            if not isinstance(container, dict):
                continue
            raw_devices = container.get("devices")
            if isinstance(raw_devices, list):
                total = _pick_int(container, ["total", "count", "devicesCount", "devices_count"])
                return [item for item in raw_devices if isinstance(item, dict)], total
        return [], None

    async def _get_hwid_devices(self, user_uuid: str) -> tuple[List[Dict[str, Any]], Optional[int]]:
        if not user_uuid:
            return [], None
        candidates = [
            ("GET", f"/api/hwid/devices/{user_uuid}", None),
            ("GET", f"/hwid/devices/{user_uuid}", None),
            ("GET", f"/api/hwid/devices", {"userUuid": user_uuid}),
            ("GET", f"/hwid/devices", {"userUuid": user_uuid}),
            ("GET", f"/api/users/{user_uuid}/devices", None),
            ("GET", f"/users/{user_uuid}/devices", None),
        ]
        for method, path, params in candidates:
            try:
                data = await self._request(method, path, params=params)
            except Exception:
                continue
            devices, total = self._extract_hwid_devices_payload(data)
            if devices or total is not None:
                return devices, total
        return [], None

    async def get_user_devices_by_telegram(self, telegram_id: int) -> tuple[List[Dict[str, Any]], Optional[int], Optional[int]]:
        user = await self._get_user_by_telegram(telegram_id)
        if not user:
            return [], None, None
        raw_user = user.raw if isinstance(user.raw, dict) else {}

        raw_devices = self._extract_device_candidates(raw_user)
        hwid_devices, hwid_total = await self._get_hwid_devices(user.uuid)
        raw_devices.extend(hwid_devices)
        devices: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for raw_device in raw_devices:
            normalized = self._normalize_device(raw_device)
            if not normalized:
                continue
            marker = normalized["id"]
            if marker in seen:
                continue
            seen.add(marker)
            devices.append(normalized)

        used, limit = self._extract_device_usage(raw_user, devices)
        if hwid_total is not None:
            used = hwid_total
        if limit is None:
            full_user = await self._get_user_by_uuid(user.uuid)
            if full_user and isinstance(full_user.raw, dict):
                _, detail_limit = self._extract_device_usage(full_user.raw, devices)
                if detail_limit is not None:
                    limit = detail_limit
        if used is None and devices:
            used = len(devices)
        return devices, used, limit

    async def unlink_user_device(self, user_uuid: str, device_id: str, telegram_id: Optional[int] = None) -> bool:
        if not user_uuid or not device_id:
            return False

        encoded_device_id = quote(str(device_id), safe="")
        candidates: List[tuple[str, str, Optional[Dict[str, Any]]]] = [
            ("POST", "/api/hwid/devices/delete", {"userUuid": user_uuid, "hwid": device_id}),
            ("POST", "/hwid/devices/delete", {"userUuid": user_uuid, "hwid": device_id}),
            ("DELETE", f"/api/users/{user_uuid}/devices/{encoded_device_id}", None),
            ("DELETE", f"/users/{user_uuid}/devices/{encoded_device_id}", None),
            ("POST", f"/api/users/{user_uuid}/devices/{encoded_device_id}/disconnect", None),
            ("POST", f"/users/{user_uuid}/devices/{encoded_device_id}/disconnect", None),
            ("POST", f"/api/users/{user_uuid}/devices/disconnect", {"deviceId": device_id}),
            ("POST", f"/users/{user_uuid}/devices/disconnect", {"deviceId": device_id}),
            ("POST", "/api/users/disconnect-device", {"uuid": user_uuid, "deviceId": device_id}),
            ("POST", "/users/disconnect-device", {"uuid": user_uuid, "deviceId": device_id}),
            ("DELETE", f"/api/devices/{encoded_device_id}", None),
            ("DELETE", f"/devices/{encoded_device_id}", None),
            ("POST", f"/api/devices/{encoded_device_id}/disconnect", None),
            ("POST", f"/devices/{encoded_device_id}/disconnect", None),
        ]
        if telegram_id is not None:
            candidates.extend(
                [
                    ("POST", "/api/users/disconnect-device", {"telegramId": telegram_id, "deviceId": device_id}),
                    ("POST", "/users/disconnect-device", {"telegramId": telegram_id, "deviceId": device_id}),
                ]
            )

        headers = dict(self.default_headers)
        if self.local:
            headers["x-forwarded-for"] = "127.0.0.1"
            headers["x-forwarded-proto"] = "https"

        for method, path, payload in candidates:
            url = f"{self.base_url}{path}"
            async with self.session.request(method, url, headers=headers, json=payload) as resp:
                if 200 <= resp.status < 300:
                    return True
                if resp.status in {400, 404, 405, 409, 422}:
                    continue
                body = await resp.text()
                raise RuntimeError(f"Remnawave unlink device failed: {resp.status} {body}")
        return False

    async def unlink_user_device_by_telegram(self, telegram_id: int, device_id: str) -> bool:
        user = await self._get_user_by_telegram(telegram_id)
        if not user:
            return False
        return await self.unlink_user_device(user.uuid, device_id, telegram_id=telegram_id)

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
