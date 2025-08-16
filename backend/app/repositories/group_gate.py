# backend/app/repositories/group_gate.py
from typing import Optional
from app.services.db import get_pool

async def set_group_required(chat_id: int, required: str, set_by: int) -> None:
    pool = await get_pool()
    async with pool.acquire() as con:
        await con.execute(
            """
            insert into group_force_join (chat_id, required, set_by)
            values ($1, $2, $3)
            on conflict (chat_id) do update set required = excluded.required, set_by = excluded.set_by, set_at = now()
            """,
            chat_id, required, set_by
        )

async def unset_group_required(chat_id: int) -> None:
    pool = await get_pool()
    async with pool.acquire() as con:
        await con.execute("delete from group_force_join where chat_id = $1", chat_id)

async def get_group_required(chat_id: int) -> Optional[str]:
    pool = await get_pool()
    async with pool.acquire() as con:
        row = await con.fetchrow("select required from group_force_join where chat_id = $1", chat_id)
        return row["required"] if row else None
