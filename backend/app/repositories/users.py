# backend/app/repositories/users.py
from typing import Optional, List
from app.services.db import get_pool

async def upsert_user(
    tg_id: int,
    first_name: Optional[str],
    last_name: Optional[str],
    username: Optional[str],
    language_code: Optional[str],
    phone_e164: Optional[str],
    region: Optional[str],
    is_premium: bool
) -> None:
    """
    Upsert user without erasing existing values when new ones are NULL.
    Notably, we keep stored phone_e164 if caller passes None.
    """
    pool = await get_pool()
    async with pool.acquire() as con:
        await con.execute(
            """
            insert into users (tg_id, first_name, last_name, username, language_code, phone_e164, region, is_premium)
            values ($1,$2,$3,$4,$5,$6,$7,$8)
            on conflict (tg_id) do update set
              first_name    = coalesce(excluded.first_name, users.first_name),
              last_name     = coalesce(excluded.last_name,  users.last_name),
              username      = coalesce(excluded.username,   users.username),
              language_code = coalesce(excluded.language_code, users.language_code),
              phone_e164    = coalesce(excluded.phone_e164, users.phone_e164),
              region        = coalesce(excluded.region,     users.region),
              is_premium    = excluded.is_premium,
              last_seen_at  = now();
            """,
            tg_id, first_name, last_name, username, language_code, phone_e164, region, is_premium
        )

async def has_phone(tg_id: int) -> bool:
    pool = await get_pool()
    async with pool.acquire() as con:
        row = await con.fetchrow("select phone_e164 from users where tg_id = $1", tg_id)
        return bool(row and row["phone_e164"])

async def touch_seen(tg_id: int) -> None:
    pool = await get_pool()
    async with pool.acquire() as con:
        await con.execute("update users set last_seen_at = now() where tg_id = $1", tg_id)

async def count_all_users() -> int:
    pool = await get_pool()
    async with pool.acquire() as con:
        row = await con.fetchrow("select count(*) c from users")
        return int(row["c"]) if row else 0

async def count_premium_users() -> int:
    pool = await get_pool()
    async with pool.acquire() as con:
        row = await con.fetchrow("select count(*) c from users where is_premium")
        return int(row["c"]) if row else 0

async def list_all_user_ids() -> List[int]:
    pool = await get_pool()
    async with pool.acquire() as con:
        rows = await con.fetch("select tg_id from users")
        return [int(r["tg_id"]) for r in rows]
