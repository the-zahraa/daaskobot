# backend/app/repositories/stats.py
from datetime import date, datetime
from typing import List, Tuple
from app.services.db import get_pool

async def inc_join(chat_id: int, d: date) -> None:
    pool = await get_pool()
    async with pool.acquire() as con:
        await con.execute(
            """
            insert into chat_members_daily (chat_id, day, joins, leaves)
            values ($1,$2,1,0)
            on conflict (chat_id, day) do update set joins = chat_members_daily.joins + 1
            """,
            chat_id, d
        )

async def inc_leave(chat_id: int, d: date) -> None:
    pool = await get_pool()
    async with pool.acquire() as con:
        await con.execute(
            """
            insert into chat_members_daily (chat_id, day, joins, leaves)
            values ($1,$2,0,1)
            on conflict (chat_id, day) do update set leaves = chat_members_daily.leaves + 1
            """,
            chat_id, d
        )

async def record_event(chat_id: int, tg_id: int, happened_at: datetime, kind: str) -> None:
    pool = await get_pool()
    async with pool.acquire() as con:
        await con.execute(
            """
            insert into member_events (chat_id, tg_id, happened_at, kind)
            values ($1,$2,$3,$4)
            on conflict do nothing
            """,
            chat_id, tg_id, happened_at, kind
        )

async def upsert_chat_user_index(chat_id: int, tg_id: int, is_member: bool, ts: datetime) -> None:
    pool = await get_pool()
    async with pool.acquire() as con:
        await con.execute(
            """
            insert into chat_user_index (chat_id, tg_id, first_seen_at, last_seen_at, is_member)
            values ($1,$2,$3,$3,$4)
            on conflict (chat_id, tg_id) do update set
              last_seen_at = excluded.last_seen_at,
              is_member    = excluded.is_member
            """,
            chat_id, tg_id, ts, is_member
        )

async def get_last_days(chat_id: int, days: int = 30) -> List[Tuple[str, int, int]]:
    pool = await get_pool()
    async with pool.acquire() as con:
        rows = await con.fetch(
            """
            select to_char(day,'YYYY-MM-DD') d, joins, leaves
            from chat_members_daily
            where chat_id = $1
            order by day desc
            limit $2
            """,
            chat_id, days
        )
        return [(r["d"], int(r["joins"]), int(r["leaves"])) for r in rows]
