# backend/app/repositories/required.py
from typing import List, Dict, Optional
from app.services.db import get_pool

# ------- BOT-WIDE required membership -------

async def list_required_targets() -> List[str]:
    pool = await get_pool()
    async with pool.acquire() as con:
        rows = await con.fetch("select target from required_membership order by added_at asc")
        return [r["target"] for r in rows]

async def add_required_target(target: str, added_by: int | None) -> None:
    pool = await get_pool()
    async with pool.acquire() as con:
        await con.execute(
            """
            insert into required_membership (target, added_by)
            values ($1,$2)
            on conflict (target) do nothing
            """,
            target, added_by
        )

async def remove_required_target(target: str) -> None:
    pool = await get_pool()
    async with pool.acquire() as con:
        await con.execute("delete from required_membership where target = $1", target)

# ------- GROUP-LEVEL force-join (multiple targets per group) -------

async def set_group_required(chat_id: int, target: str, set_by: int | None, join_url: Optional[str] = None) -> None:
    """
    Upsert requirement. If already exists, update join_url if provided.
    """
    pool = await get_pool()
    async with pool.acquire() as con:
        await con.execute(
            """
            insert into group_force_join_requirements (chat_id, target, set_by, join_url)
            values ($1,$2,$3,$4)
            on conflict (chat_id, target) do update set
              join_url = coalesce(excluded.join_url, group_force_join_requirements.join_url)
            """,
            chat_id, target, set_by, join_url
        )

async def unset_group_required(chat_id: int, target: str | None = None) -> None:
    pool = await get_pool()
    async with pool.acquire() as con:
        if target:
            await con.execute(
                "delete from group_force_join_requirements where chat_id = $1 and target = $2",
                chat_id, target
            )
        else:
            await con.execute(
                "delete from group_force_join_requirements where chat_id = $1",
                chat_id
            )

async def list_group_required(chat_id: int) -> List[Dict[str, Optional[str]]]:
    """
    Returns a list of dicts: {'target': str, 'join_url': Optional[str]}
    """
    pool = await get_pool()
    async with pool.acquire() as con:
        rows = await con.fetch(
            """
            select target, join_url
            from group_force_join_requirements
            where chat_id = $1
            order by set_at asc
            """,
            chat_id
        )
        return [{"target": r["target"], "join_url": r["join_url"]} for r in rows]
