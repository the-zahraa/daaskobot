from __future__ import annotations
from typing import Optional
import app.db as app_db

# Table: public.tenant_features
# Columns:
#   tenant_id uuid PK
#   force_join_enabled boolean not null default false
#   updated_at timestamptz not null default now()

async def get_force_join_enabled(tenant_id: str) -> bool:
    """
    Return True if force join is enabled for the tenant, else False.
    """
    async with app_db.get_con() as con:
        row = await con.fetchrow(
            "select force_join_enabled from public.tenant_features where tenant_id = $1",
            tenant_id,
        )
    if row is None:
        return False
    return bool(row["force_join_enabled"])

async def set_force_join_enabled(tenant_id: str, enabled: bool) -> None:
    """
    Upsert force_join_enabled flag for a tenant.
    """
    async with app_db.get_con() as con:
        await con.execute(
            """
            insert into public.tenant_features (tenant_id, force_join_enabled, updated_at)
            values ($1, $2, now())
            on conflict (tenant_id) do update set
              force_join_enabled = excluded.force_join_enabled,
              updated_at         = now()
            """,
            tenant_id, enabled,
        )

async def ensure_tenant_features_row(tenant_id: str) -> None:
    """
    Make sure the row exists for this tenant, with defaults.
    """
    async with app_db.get_con() as con:
        await con.execute(
            """
            insert into public.tenant_features (tenant_id)
            values ($1)
            on conflict (tenant_id) do nothing
            """,
            tenant_id,
        )

async def toggle_force_join(tenant_id: str) -> bool:
    """
    Flip the flag and return the new value.
    """
    current = await get_force_join_enabled(tenant_id)
    new_value = not current
    await set_force_join_enabled(tenant_id, new_value)
    return new_value
