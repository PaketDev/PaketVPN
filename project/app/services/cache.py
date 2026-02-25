import asyncio
import time
from typing import Dict, Optional


class TTLCache:
    def __init__(self, ttl_seconds: int = 1800) -> None:
        self.ttl = ttl_seconds
        self._data: Dict[int, tuple[int, float]] = {}
        self._lock = asyncio.Lock()

    async def set(self, key: int, value: int) -> None:
        async with self._lock:
            self._data[key] = (value, time.time() + self.ttl)

    async def get(self, key: int) -> Optional[int]:
        async with self._lock:
            if key not in self._data:
                return None
            value, expires_at = self._data[key]
            if time.time() > expires_at:
                del self._data[key]
                return None
            return value

    async def delete(self, key: int) -> None:
        async with self._lock:
            self._data.pop(key, None)

