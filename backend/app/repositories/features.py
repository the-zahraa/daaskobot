# backend/app/repositories/features.py
from typing import Dict, Any
from app.services.db import get_pool

async def get_tenant_features(tenant_id: str) -> Dict[str, Any]:
    pool = await get_pool()
    async with pool.acquire() as con:
        await con.execute("select ensure_tenant_features($1)", tenant_id)
        row = await con.fetchrow(
            "select tenant_id, force_join_enabled from tenant_features where tenant_id = $1",
            tenant_id
        )
        return {
            "tenant_id": str(row["tenant_id"]),
            "force_join_enabled": bool(row["force_join_enabled"])
        } if row else {"tenant_id": tenant_id, "force_join_enabled": False}

async def set_force_join(tenant_id: str, enabled: bool) -> None:
    pool = await get_pool()
    async with pool.acquire() as con:
        await con.execute("select ensure_tenant_features($1)", tenant_id)
        await con.execute(
            """
            update tenant_features
               set force_join_enabled = $2, updated_at = now()
             where tenant_id = $1
            """,
            tenant_id, enabled
        )
