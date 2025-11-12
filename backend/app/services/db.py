# backend/app/services/db.py
from __future__ import annotations

import os
import logging
from typing import Optional

import asyncpg

logger = logging.getLogger(__name__)

# Global pool
pool: Optional[asyncpg.Pool] = None

DB_DSN = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")


async def init_db() -> None:
    """Initialize global asyncpg pool once."""
    global pool
    if pool is not None:
        return
    if not DB_DSN:
        raise RuntimeError("SUPABASE_DB_URL (or DATABASE_URL) is required for Postgres")
    logger.info("Connecting to database...")
    pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=1, max_size=10)
    logger.info("Database pool ready.")


async def close_db() -> None:
    """Close global pool."""
    global pool
    if pool is not None:
        await pool.close()
        pool = None


async def get_pool() -> asyncpg.Pool:
    """Return active pool or raise if not initialized."""
    if pool is None:
        raise RuntimeError("DB pool is not initialized. Call init_db() first.")
    return pool


class ConnectionContext:
    """
    Async context manager wrapper around the global asyncpg pool.

    Usage:
        async with get_con() as con:
            await con.fetch("SELECT 1")
    """

    def __init__(self) -> None:
        self._con: Optional[asyncpg.Connection] = None

    async def __aenter__(self) -> asyncpg.Connection:
        p = await get_pool()
        self._con = await p.acquire()
        return self._con

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._con is None:
            return
        p = await get_pool()
        await p.release(self._con)
        self._con = None


def get_con() -> ConnectionContext:
    """
    Return an async context manager for a DB connection.

    Example:
        async with get_con() as con:
            await con.fetch("SELECT 1")
    """
    return ConnectionContext()
