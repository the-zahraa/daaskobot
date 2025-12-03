# backend/app/repositories/audience.py
from __future__ import annotations
from typing import List
from app.db import get_con

"""
Audience = distinct users who:
  - belong to your tenant (via user_tenants -> chats -> chat_user_index)
  - are currently is_member = TRUE
  - were active in the last N days (last_seen_at)
Optional filter: has_phone yes / no / any (users.phone_e164).
"""


async def count_audience(
    owner_tg_id: int,
    last_active_days: int,
    has_phone_mode: str = "any",  # 'any' | 'yes' | 'no'
) -> int:
    async with get_con() as con:
        row = await con.fetchrow(
            """
            WITH my_tenants AS (
              SELECT tenant_id
              FROM public.user_tenants
              WHERE tg_id = $1
            ),
            my_chats AS (
              SELECT tg_chat_id
              FROM public.chats
              WHERE tenant_id IN (SELECT tenant_id FROM my_tenants)
            )
            SELECT COUNT(DISTINCT cui.tg_id) AS c
            FROM public.chat_user_index cui
            JOIN my_chats mc ON mc.tg_chat_id = cui.chat_id
            LEFT JOIN public.users u ON u.tg_id = cui.tg_id
            WHERE cui.is_member = TRUE
              AND cui.last_seen_at >= now() - ($2::int) * interval '1 day'
              AND (
                $3::text = 'any'
                OR ($3::text = 'yes' AND u.phone_e164 IS NOT NULL)
                OR ($3::text = 'no'  AND u.phone_e164 IS NULL)
              )
            """,
            owner_tg_id,
            last_active_days,
            has_phone_mode,
        )
    return int(row["c"]) if row and row["c"] is not None else 0


async def get_audience_user_ids(
    owner_tg_id: int,
    last_active_days: int,
    has_phone_mode: str = "any",
    limit: int = 10000,
) -> List[int]:
    """
    Returns DISTINCT tg_id for users in your tenant’s audience,
    respecting filters. Limit protects from absurdly large lists.
    """
    async with get_con() as con:
        rows = await con.fetch(
            """
            WITH my_tenants AS (
              SELECT tenant_id
              FROM public.user_tenants
              WHERE tg_id = $1
            ),
            my_chats AS (
              SELECT tg_chat_id
              FROM public.chats
              WHERE tenant_id IN (SELECT tenant_id FROM my_tenants)
            )
            SELECT DISTINCT cui.tg_id
            FROM public.chat_user_index cui
            JOIN my_chats mc ON mc.tg_chat_id = cui.chat_id
            LEFT JOIN public.users u ON u.tg_id = cui.tg_id
            WHERE cui.is_member = TRUE
              AND cui.last_seen_at >= now() - ($2::int) * interval '1 day'
              AND (
                $3::text = 'any'
                OR ($3::text = 'yes' AND u.phone_e164 IS NOT NULL)
                OR ($3::text = 'no'  AND u.phone_e164 IS NULL)
              )
            LIMIT $4
            """,
            owner_tg_id,
            last_active_days,
            has_phone_mode,
            limit,
        )
    return [int(r["tg_id"]) for r in rows]
