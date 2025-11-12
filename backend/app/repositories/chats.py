from __future__ import annotations
from typing import List, Tuple, Optional
import app.db as app_db

# Schema (public.chats):
# tg_chat_id BIGINT PK, tenant_id UUID NULL, type TEXT NOT NULL,
# title TEXT NULL, created_at TIMESTAMPTZ DEFAULT now(),
# chat_type TEXT NULL, linked_at TIMESTAMPTZ DEFAULT now()

# ---------- Existing functions ----------

async def upsert_chat(
    tg_chat_id: int,
    tenant_id: str,
    title: Optional[str],
    chat_type: str,  # expected values: 'group' | 'supergroup' | 'channel'
) -> None:
    async with app_db.get_con() as con:
        await con.execute(
            """
            insert into public.chats (tg_chat_id, tenant_id, title, chat_type, linked_at, type)
            values ($1, $2, $3, $4, now(), coalesce($4,'group'))
            on conflict (tg_chat_id) do update set
              tenant_id = excluded.tenant_id,
              title     = excluded.title,
              chat_type = excluded.chat_type,
              type      = excluded.chat_type,
              linked_at = now()
            """,
            tg_chat_id, tenant_id, title, chat_type,
        )

async def list_tenant_chats(tenant_id: str) -> List[Tuple[int, str, str]]:
    async with app_db.get_con() as con:
        rows = await con.fetch(
            """
            select tg_chat_id, chat_type, coalesce(title, '—') as title
            from public.chats
            where tenant_id = $1
            order by linked_at desc
            """,
            tenant_id,
        )
    return [(int(r["tg_chat_id"]), str(r["chat_type"]), str(r["title"])) for r in rows]

async def count_all_chats() -> int:
    async with app_db.get_con() as con:
        row = await con.fetchrow("select count(*) as c from public.chats")
    return int(row["c"]) if row else 0

async def chat_exists(tg_chat_id: int) -> bool:
    async with app_db.get_con() as con:
        row = await con.fetchrow(
            "select 1 from public.chats where tg_chat_id = $1 limit 1",
            tg_chat_id,
        )
    return bool(row)

async def get_chat_tenant(tg_chat_id: int) -> Optional[str]:
    async with app_db.get_con() as con:
        row = await con.fetchrow(
            "select tenant_id from public.chats where tg_chat_id = $1",
            tg_chat_id,
        )
    return str(row["tenant_id"]) if row and row["tenant_id"] is not None else None

# ---------- New helpers (needed by scheduler) ----------

async def list_all_channels_ids(con) -> List[int]:
    rows = await con.fetch(
        """
        select tg_chat_id
        from public.chats
        where chat_type in ('channel','supergroup')
        """
    )
    return [int(r["tg_chat_id"]) for r in rows]

async def list_all_channels() -> List[int]:
    """
    Wrapper using a managed connection (for scheduler calls).
    """
    async with app_db.get_con() as con:
        return await list_all_channels_ids(con)
