# backend/app/repositories/chats.py
from typing import List, Tuple, Optional
from app.services.db import get_pool

async def upsert_chat(
    tg_chat_id: int,
    tenant_id: str,
    title: Optional[str],
    chat_type: str
) -> None:
    pool = await get_pool()
    async with pool.acquire() as con:
        await con.execute(
            """
            insert into chats (tg_chat_id, tenant_id, title, chat_type, linked_at, type)
            values ($1, $2, $3, $4, now(), coalesce($4,'group'))
            on conflict (tg_chat_id) do update set
              tenant_id = excluded.tenant_id,
              title = excluded.title,
              chat_type = excluded.chat_type,
              type = excluded.chat_type,
              linked_at = now()
            """,
            tg_chat_id, tenant_id, title, chat_type
        )

async def list_tenant_chats(tenant_id: str) -> List[Tuple[int, str, str]]:
    pool = await get_pool()
    async with pool.acquire() as con:
        rows = await con.fetch(
            """
            select tg_chat_id, chat_type, coalesce(title, '—') as title
            from chats
            where tenant_id = $1
            order by linked_at desc
            """,
            tenant_id
        )
        return [(int(r["tg_chat_id"]), str(r["chat_type"]), str(r["title"])) for r in rows]

async def count_all_chats() -> int:
    pool = await get_pool()
    async with pool.acquire() as con:
        row = await con.fetchrow("select count(*) c from chats")
        return int(row["c"]) if row else 0
