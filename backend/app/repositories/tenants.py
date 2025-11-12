# backend/app/repositories/tenants.py
from __future__ import annotations
from typing import Optional, List, Dict, Any
import app.db as app_db

# -----------------------------------------------------------------------------
# Tenants & user↔tenant linkage
# -----------------------------------------------------------------------------

async def ensure_personal_tenant(owner_tg_id: int, name: str) -> str:
    """
    Ensure a tenant exists for this owner_tg_id; return its UUID (as str).
    Schema: public.tenants(id uuid PK default gen_random_uuid(), owner_tg_id bigint not null, name text not null, created_at timestamptz default now()).
    """
    async with app_db.get_con() as con:
        row = await con.fetchrow(
            "select id from public.tenants where owner_tg_id = $1",
            owner_tg_id,
        )
        if row:
            return str(row["id"])
        row = await con.fetchrow(
            "insert into public.tenants (owner_tg_id, name) values ($1,$2) returning id",
            owner_tg_id, name or f"Tenant {owner_tg_id}",
        )
        return str(row["id"])

async def link_user_to_tenant(tg_id: int, tenant_id: str) -> None:
    """
    Upsert mapping in public.user_tenants(tg_id bigint, tenant_id uuid).
    """
    async with app_db.get_con() as con:
        await con.execute(
            """
            insert into public.user_tenants (tg_id, tenant_id)
            values ($1,$2)
            on conflict (tg_id) do update set tenant_id = excluded.tenant_id
            """,
            tg_id, tenant_id,
        )

async def get_user_tenant(tg_id: int) -> Optional[str]:
    """
    Return tenant_id (str) for a given user, if any.
    """
    async with app_db.get_con() as con:
        row = await con.fetchrow(
            "select tenant_id from public.user_tenants where tg_id = $1",
            tg_id,
        )
    return str(row["tenant_id"]) if row and row["tenant_id"] is not None else None

async def get_tenant(tenant_id: str) -> Optional[Dict[str, Any]]:
    """
    Fetch tenant basic info.
    """
    async with app_db.get_con() as con:
        r = await con.fetchrow(
            "select id, name, owner_tg_id, created_at from public.tenants where id = $1",
            tenant_id,
        )
    if not r:
        return None
    return {
        "id": str(r["id"]),
        "name": r["name"],
        "owner_tg_id": int(r["owner_tg_id"]) if r["owner_tg_id"] is not None else None,
        "created_at": r["created_at"].strftime("%Y-%m-%d"),
    }

async def count_active_tenants() -> int:
    """
    Count active tenants (your schema has a view/table public.active_tenants(tenant_id uuid)).
    """
    async with app_db.get_con() as con:
        row = await con.fetchrow("select count(*) as c from public.active_tenants")
    return int(row["c"]) if row else 0

# -----------------------------------------------------------------------------
# Tenant listings with stats
# -----------------------------------------------------------------------------

async def list_tenants_page_with_stats(limit: int, offset: int) -> List[Dict[str, Any]]:
    """
    Paginated tenants with:
      - chat_count from public.chats
      - latest plan across any user linked to the tenant from public.subscriptions
        (not expiry-filtered; mirrors your current logic)
    """
    async with app_db.get_con() as con:
        rows = await con.fetch(
            """
            with latest_plan as (
              select ut.tenant_id, s.plan,
                     row_number() over (partition by ut.tenant_id order by s.started_at desc) as rn
              from public.user_tenants ut
              join public.subscriptions s on s.tg_id = ut.tg_id
            ),
            chat_counts as (
              select tenant_id, count(*) as chat_count
              from public.chats
              group by tenant_id
            )
            select t.id, t.name, t.owner_tg_id, t.created_at,
                   coalesce(cc.chat_count, 0) as chat_count,
                   lower(coalesce(lp.plan, 'inactive')) as plan
            from public.tenants t
            left join chat_counts cc on cc.tenant_id = t.id
            left join latest_plan lp on lp.tenant_id = t.id and lp.rn = 1
            order by t.created_at desc
            limit $1 offset $2
            """,
            limit, offset,
        )
    return [
        {
            "id": str(r["id"]),
            "name": r["name"],
            "owner_tg_id": int(r["owner_tg_id"]) if r["owner_tg_id"] is not None else None,
            "created_at": r["created_at"].strftime("%Y-%m-%d"),
            "chat_count": int(r["chat_count"]),
            "plan": str(r["plan"]),
        } for r in rows
    ]

async def search_tenants_page_with_stats(q: str, limit: int, offset: int) -> List[Dict[str, Any]]:
    """
    Search by name, owner_tg_id, or id (uuid::text), with same stats as above.
    """
    async with app_db.get_con() as con:
        rows = await con.fetch(
            """
            with latest_plan as (
              select ut.tenant_id, s.plan,
                     row_number() over (partition by ut.tenant_id order by s.started_at desc) as rn
              from public.user_tenants ut
              left join public.subscriptions s on s.tg_id = ut.tg_id
            ),
            chat_counts as (
              select tenant_id, count(*) as chat_count
              from public.chats
              group by tenant_id
            )
            select t.id, t.name, t.owner_tg_id, t.created_at,
                   coalesce(cc.chat_count, 0) as chat_count,
                   lower(coalesce(lp.plan, 'inactive')) as plan
            from public.tenants t
            left join chat_counts cc on cc.tenant_id = t.id
            left join latest_plan lp on lp.tenant_id = t.id and lp.rn = 1
            where (t.name ilike '%'||$1||'%'
               or cast(t.owner_tg_id as text) ilike '%'||$1||'%'
               or cast(t.id as text) ilike '%'||$1||'%')
            order by t.created_at desc
            limit $2 offset $3
            """,
            q, limit, offset,
        )
    return [
        {
            "id": str(r["id"]),
            "name": r["name"],
            "owner_tg_id": int(r["owner_tg_id"]) if r["owner_tg_id"] is not None else None,
            "created_at": r["created_at"].strftime("%Y-%m-%d"),
            "chat_count": int(r["chat_count"]),
            "plan": str(r["plan"]),
        } for r in rows
    ]

async def export_all_tenants_with_stats() -> List[Dict[str, Any]]:
    """
    Full export with same columns as list/search, ordered by created_at desc.
    """
    async with app_db.get_con() as con:
        rows = await con.fetch(
            """
            with latest_plan as (
              select ut.tenant_id, s.plan,
                     row_number() over (partition by ut.tenant_id order by s.started_at desc) as rn
              from public.user_tenants ut
              left join public.subscriptions s on s.tg_id = ut.tg_id
            ),
            chat_counts as (
              select tenant_id, count(*) as chat_count
              from public.chats
              group by tenant_id
            )
            select t.id, t.name, t.owner_tg_id, t.created_at,
                   coalesce(cc.chat_count, 0) as chat_count,
                   lower(coalesce(lp.plan, 'inactive')) as plan
            from public.tenants t
            left join chat_counts cc on cc.tenant_id = t.id
            left join latest_plan lp on lp.tenant_id = t.id and lp.rn = 1
            order by t.created_at desc
            """
        )
    return [
        {
            "id": str(r["id"]),
            "name": r["name"],
            "owner_tg_id": int(r["owner_tg_id"]) if r["owner_tg_id"] is not None else None,
            "created_at": r["created_at"].strftime("%Y-%m-%d"),
            "chat_count": int(r["chat_count"]),
            "plan": str(r["plan"]),
        } for r in rows
    ]
