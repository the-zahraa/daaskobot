# backend/app/services/db.py
import os
import ssl
import asyncpg
from typing import Optional

_pool: Optional[asyncpg.Pool] = None

async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is not None:
        return _pool

    dsn = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL") or ""
    if not dsn:
        raise RuntimeError("DATABASE_URL (Supabase Postgres) is not set in backend/.env")

    # Supabase requires TLS; relax verification in serverless envs
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    _pool = await asyncpg.create_pool(dsn=dsn, ssl=ctx, min_size=1, max_size=5)
    return _pool

async def close_pool():
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
