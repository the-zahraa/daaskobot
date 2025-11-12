# backend/app/repositories/stats.py
from __future__ import annotations
from typing import List, Tuple
from datetime import date, datetime

from app.db import get_con

# ---------------------------
# WRITERS (Counters / Streams)
# ---------------------------

async def inc_join(chat_id: int, d: date) -> None:
    async with get_con() as con:
        await con.execute(
            """
            INSERT INTO chat_members_daily (chat_id, day, joins, leaves)
            VALUES ($1,$2,1,0)
            ON CONFLICT (chat_id, day) DO UPDATE
              SET joins = chat_members_daily.joins + 1
            """,
            chat_id, d
        )

async def inc_leave(chat_id: int, d: date) -> None:
    async with get_con() as con:
        await con.execute(
            """
            INSERT INTO chat_members_daily (chat_id, day, joins, leaves)
            VALUES ($1,$2,0,1)
            ON CONFLICT (chat_id, day) DO UPDATE
              SET leaves = chat_members_daily.leaves + 1
            """,
            chat_id, d
        )

async def record_event(chat_id: int, tg_id: int, happened_at: datetime, kind: str) -> None:
    async with get_con() as con:
        await con.execute(
            """
            INSERT INTO member_events (chat_id, tg_id, happened_at, kind)
            VALUES ($1,$2,$3,$4)
            ON CONFLICT DO NOTHING
            """,
            chat_id, tg_id, happened_at, kind
        )

async def upsert_chat_user_index(chat_id: int, tg_id: int, is_member: bool, ts: datetime) -> None:
    async with get_con() as con:
        await con.execute(
            """
            INSERT INTO chat_user_index (chat_id, tg_id, first_seen_at, last_seen_at, is_member)
            VALUES ($1,$2,$3,$3,$4)
            ON CONFLICT (chat_id, tg_id) DO UPDATE SET
              last_seen_at = excluded.last_seen_at,
              is_member    = excluded.is_member
            """,
            chat_id, tg_id, ts, is_member
        )

async def upsert_channel_member_count(chat_id: int, d: date, count: int) -> None:
    async with get_con() as con:
        await con.execute(
            """
            INSERT INTO channel_member_counts (chat_id, day, member_count)
            VALUES ($1,$2,$3)
            ON CONFLICT (chat_id, day) DO UPDATE SET
              member_count = excluded.member_count
            """,
            chat_id, d, count
        )

async def inc_message_count(chat_id: int, d: date, user_id: int | None = None, count: int = 1) -> None:
    """
    Increments:
      - messages_daily (chat total)
      - messages_by_user_daily (per user) if user_id provided
      - dau_daily (unique user per day) if user_id provided
    """
    async with get_con() as con:
        # chat total per day
        await con.execute(
            """
            INSERT INTO messages_daily (chat_id, date, message_count)
            VALUES ($1,$2,$3)
            ON CONFLICT (chat_id, date) DO UPDATE
              SET message_count = messages_daily.message_count + $3
            """,
            chat_id, d, count
        )
        if user_id is not None:
            # per user per day
            await con.execute(
                """
                INSERT INTO messages_by_user_daily (chat_id, date, user_id, message_count)
                VALUES ($1,$2,$3,$4)
                ON CONFLICT (chat_id, date, user_id) DO UPDATE
                  SET message_count = messages_by_user_daily.message_count + $4
                """,
                chat_id, d, user_id, count
            )
            # DAU (avoid duplicate row)
            await con.execute(
                """
                INSERT INTO dau_daily (chat_id, date, user_id)
                SELECT $1, $2, $3
                WHERE NOT EXISTS (
                  SELECT 1 FROM dau_daily WHERE chat_id=$1 AND date=$2 AND user_id=$3
                )
                """,
                chat_id, d, user_id
            )

# ---------------------------
# READERS (Joins/Leaves series)
# ---------------------------

async def get_last_days(chat_id: int, days: int = 30) -> List[Tuple[str, int, int]]:
    """
    Window of last `days` for joins/leaves (DESC), zero-filled for missing days.
    Output: [(YYYY-MM-DD, joins, leaves), ...]
    """
    async with get_con() as con:
        rows = await con.fetch(
            """
            WITH days AS (
              SELECT d::date AS day
              FROM generate_series(current_date - ($2::int - 1), current_date, interval '1 day') AS d
            )
            SELECT to_char(days.day, 'YYYY-MM-DD') AS d,
                   COALESCE(cmd.joins, 0)  AS joins,
                   COALESCE(cmd.leaves, 0) AS leaves
            FROM days
            LEFT JOIN chat_members_daily cmd
              ON cmd.chat_id = $1
             AND cmd.day     = days.day
            ORDER BY days.day DESC
            """,
            chat_id, days
        )
    return [(r["d"], int(r["joins"]), int(r["leaves"])) for r in rows]
