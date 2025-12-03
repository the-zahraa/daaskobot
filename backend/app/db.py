# backend/app/db.py
from __future__ import annotations

import os
import asyncpg
from typing import Optional

# Try DATABASE_URL first, then fall back to SUPABASE_DB_URL
DATABASE_URL = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")

_pool: Optional[asyncpg.Pool] = None


async def init_db() -> None:
    """
    Initialize a global asyncpg connection pool.
    Called once at startup in bot_worker.py.
    """
    global _pool
    if _pool is not None:
        return

    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL / SUPABASE_DB_URL is not set")

    _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)


async def get_pool() -> asyncpg.Pool:
    """
    Return the global pool; initialize it if needed.
    """
    global _pool
    if _pool is None:
        await init_db()
    return _pool


class ConnectionContext:
    """
    Async context manager for DB connections.

    Usage:
        async with get_con() as con:
            rows = await con.fetch("SELECT 1")
    """
    def __init__(self) -> None:
        self._con: Optional[asyncpg.Connection] = None

    async def __aenter__(self) -> asyncpg.Connection:
        pool = await get_pool()
        self._con = await pool.acquire()
        return self._con

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._con is not None:
            pool = await get_pool()
            await pool.release(self._con)
            self._con = None


def get_con() -> ConnectionContext:
    return ConnectionContext()

async def close_db() -> None:
    """
    Compatibility stub for bot_worker.

    If you later add a real DB pool (e.g. asyncpg pool),
    you can close it here. For now it just exists so
    bot_worker and Pylint are happy.
    """
    return
