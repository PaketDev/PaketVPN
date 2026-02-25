import asyncio
from pathlib import Path
from typing import Optional

import aiosqlite

_db: Optional[aiosqlite.Connection] = None
_lock = asyncio.Lock()


async def init_db(path: Path) -> aiosqlite.Connection:
    """Initialize a shared aiosqlite connection with sane pragmas."""
    global _db
    async with _lock:
        if _db is None:
            _db = await aiosqlite.connect(path)
            _db.row_factory = aiosqlite.Row
            await _db.execute("PRAGMA foreign_keys = ON;")
            await _db.execute("PRAGMA journal_mode = WAL;")
            await _db.execute("PRAGMA synchronous = NORMAL;")
    return _db


async def get_db() -> aiosqlite.Connection:
    if _db is None:
        raise RuntimeError("Database not initialized")
    return _db


async def close_db() -> None:
    global _db
    async with _lock:
        if _db is not None:
            await _db.close()
            _db = None

