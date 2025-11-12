# backend/app/repositories/required.py
from __future__ import annotations
from typing import List, Optional, Tuple, Dict
from app.db import get_con

# -----------------------------------------------------------------------------
# GLOBAL required membership (used by /start gate in DM)
# Table: public.required_membership(
#   target text primary key,
#   added_by bigint null,
#   added_at timestamptz not null default now()
# )
# -----------------------------------------------------------------------------

async def list_required_targets() -> List[str]:
    """
    Return global required targets (e.g., '@MyChannel' or a t.me invite URL),
    ordered by when they were added.
    """
    async with get_con() as con:
        rows = await con.fetch(
            "SELECT target FROM public.required_membership ORDER BY added_at ASC"
        )
    return [str(r["target"]) for r in rows]

async def add_required_target(target: str, added_by: Optional[int]) -> None:
    """
    Add a global required target. Idempotent.
    """
    target = (target or "").strip()
    if not target:
        return
    async with get_con() as con:
        await con.execute(
            """
            INSERT INTO public.required_membership (target, added_by, added_at)
            VALUES ($1, $2, now())
            ON CONFLICT (target) DO NOTHING
            """,
            target, added_by
        )

async def remove_required_target(target: str) -> None:
    """
    Remove a global required target by exact match.
    """
    async with get_con() as con:
        await con.execute(
            "DELETE FROM public.required_membership WHERE target = $1",
            (target or "").strip()
        )

# -----------------------------------------------------------------------------
# PER-GROUP force-join requirements (used inside groups)
# Table: public.group_force_join_requirements(
#   chat_id  bigint not null,
#   target   text   not null,
#   set_by   bigint null,
#   set_at   timestamptz not null default now(),
#   join_url text null,
#   PRIMARY KEY(chat_id, target)
# )
# -----------------------------------------------------------------------------

async def list_group_targets(chat_id: int) -> List[Dict[str, Optional[str]]]:
    """
    Return required targets for a specific group, with optional join_url.
    [{ 'target': '@MyChannel', 'join_url': 'https://t.me/...' }, ...]
    """
    async with get_con() as con:
        rows = await con.fetch(
            """
            SELECT target, join_url
            FROM public.group_force_join_requirements
            WHERE chat_id = $1
            ORDER BY set_at ASC
            """,
            chat_id
        )
    return [{"target": str(r["target"]), "join_url": (str(r["join_url"]) if r["join_url"] is not None else None)} for r in rows]

async def add_group_target(
    chat_id: int,
    target: str,
    set_by: Optional[int],
    join_url: Optional[str] = None
) -> None:
    """
    Upsert a single required target for a group. If it exists, updates join_url/set_by/set_at.
    """
    target = (target or "").strip()
    if not target:
        return
    async with get_con() as con:
        await con.execute(
            """
            INSERT INTO public.group_force_join_requirements (chat_id, target, join_url, set_by, set_at)
            VALUES ($1, $2, $3, $4, now())
            ON CONFLICT (chat_id, target) DO UPDATE
              SET join_url = EXCLUDED.join_url,
                  set_by   = EXCLUDED.set_by,
                  set_at   = EXCLUDED.set_at
            """,
            chat_id, target, join_url, set_by
        )

async def remove_group_target(chat_id: int, target: str) -> None:
    """
    Delete a single required target for a group.
    """
    async with get_con() as con:
        await con.execute(
            "DELETE FROM public.group_force_join_requirements WHERE chat_id=$1 AND target=$2",
            chat_id, (target or "").strip()
        )

async def clear_group_targets(chat_id: int) -> None:
    """
    Clear all required targets for a group.
    """
    async with get_con() as con:
        await con.execute(
            "DELETE FROM public.group_force_join_requirements WHERE chat_id=$1",
            chat_id
        )
