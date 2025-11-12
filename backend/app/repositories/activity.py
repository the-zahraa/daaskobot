from __future__ import annotations
from typing import List, Tuple, Optional
from datetime import datetime

from app.db import get_con

# ---------- Writers ----------
async def record_message_event(chat_id: int, user_id: int, happened_at: datetime) -> None:
    async with get_con() as con:
        await con.execute(
            "INSERT INTO messages_by_user (chat_id, tg_id, happened_at) VALUES ($1,$2,$3)",
            chat_id, user_id, happened_at
        )

async def record_stream_event(chat_id: int, happened_at: datetime) -> None:
    async with get_con() as con:
        await con.execute(
            "INSERT INTO messages_stream (chat_id, happened_at) VALUES ($1,$2)",
            chat_id, happened_at
        )

# ---------- Readers (triple fallback for totals; stream-aware) ----------
async def get_messages_daily(chat_id: int, days: int = 7) -> List[Tuple[str, int]]:
    async with get_con() as con:
        rows = await con.fetch(
            """
            WITH days AS (
              SELECT d::date AS day
              FROM generate_series(current_date - ($2::int - 1), current_date, interval '1 day') AS d
            ),
            by_user AS (
              SELECT date, SUM(message_count)::int AS total
              FROM messages_by_user_daily
              WHERE chat_id = $1
                AND date >= current_date - ($2::int - 1)
              GROUP BY date
            ),
            evt AS (
              SELECT (happened_at AT TIME ZONE 'UTC')::date AS date, COUNT(*)::int AS total
              FROM (
                SELECT happened_at FROM messages_by_user
                WHERE chat_id = $1 AND happened_at >= current_date - ($2::int - 1)
                UNION ALL
                SELECT happened_at FROM messages_stream
                WHERE chat_id = $1 AND happened_at >= current_date - ($2::int - 1)
              ) x
              GROUP BY 1
            )
            SELECT to_char(days.day,'YYYY-MM-DD') AS d,
                   COALESCE(md.message_count, by_user.total, evt.total, 0) AS count
            FROM days
            LEFT JOIN messages_daily md
              ON md.chat_id = $1 AND md.date = days.day
            LEFT JOIN by_user ON by_user.date = days.day
            LEFT JOIN evt     ON evt.date = days.day
            ORDER BY days.day DESC
            """,
            chat_id, days
        )
    return [(r["d"], int(r["count"])) for r in rows]

async def get_dau_daily(chat_id: int, days: int = 7) -> List[Tuple[str, int]]:
    async with get_con() as con:
        rows = await con.fetch(
            """
            WITH days AS (
              SELECT d::date AS day
              FROM generate_series(current_date - ($2::int - 1), current_date, interval '1 day') AS d
            ),
            per_day AS (
              SELECT date AS day, COUNT(DISTINCT user_id) AS dau
              FROM dau_daily
              WHERE chat_id = $1
                AND date >= current_date - ($2::int - 1)
              GROUP BY date
            )
            SELECT to_char(days.day,'YYYY-MM-DD') AS d,
                   COALESCE(per_day.dau, 0) AS count
            FROM days
            LEFT JOIN per_day ON per_day.day = days.day
            ORDER BY days.day DESC
            """,
            chat_id, days
        )
    return [(r["d"], int(r["count"])) for r in rows]

async def get_top_talkers(chat_id: int, *, days: int = 7, limit: int = 5) -> List[Tuple[int, int]]:
    async with get_con() as con:
        rows = await con.fetch(
            """
            SELECT user_id, SUM(message_count) AS total
            FROM messages_by_user_daily
            WHERE chat_id = $1
              AND date >= current_date - ($2::int - 1)
            GROUP BY user_id
            ORDER BY total DESC
            LIMIT $3
            """,
            chat_id, days, limit
        )
        if rows:
            return [(int(r["user_id"]), int(r["total"])) for r in rows]
        rows2 = await con.fetch(
            """
            SELECT tg_id AS user_id, COUNT(*)::int AS total
            FROM messages_by_user
            WHERE chat_id = $1
              AND happened_at >= now() - ($2::int) * interval '1 day'
            GROUP BY tg_id
            ORDER BY total DESC
            LIMIT $3
            """,
            chat_id, days, limit
        )
    return [(int(r["user_id"]), int(r["total"])) for r in rows2]

async def get_most_active_user(chat_id: int, *, days: int = 30) -> Optional[Tuple[int, int]]:
    async with get_con() as con:
        row = await con.fetchrow(
            """
            SELECT user_id, SUM(message_count) AS total
            FROM messages_by_user_daily
            WHERE chat_id = $1
              AND date >= current_date - ($2::int - 1)
            GROUP BY user_id
            ORDER BY total DESC
            LIMIT 1
            """,
            chat_id, days
        )
        if row:
            return int(row["user_id"]), int(row["total"])
        row2 = await con.fetchrow(
            """
            SELECT tg_id AS user_id, COUNT(*)::int AS total
            FROM messages_by_user
            WHERE chat_id = $1
              AND happened_at >= now() - ($2::int) * interval '1 day'
            GROUP BY tg_id
            ORDER BY total DESC
            LIMIT 1
            """,
            chat_id, days
        )
    if row2:
        return int(row2["user_id"]), int(row2["total"])
    return None

async def get_peak_hour(chat_id: int, *, days: int = 30, tz: str = 'UTC') -> Optional[Tuple[int, int]]:
    async with get_con() as con:
        row = await con.fetchrow(
            f"""
            WITH u AS (
              SELECT EXTRACT(HOUR FROM happened_at AT TIME ZONE '{tz}')::int AS hour
              FROM messages_by_user
              WHERE chat_id = $1
                AND happened_at >= now() - ($2::int) * interval '1 day'
            ),
            c AS (
              SELECT EXTRACT(HOUR FROM happened_at AT TIME ZONE '{tz}')::int AS hour
              FROM messages_stream
              WHERE chat_id = $1
                AND happened_at >= now() - ($2::int) * interval '1 day'
            ),
            allh AS (
              SELECT hour FROM u
              UNION ALL
              SELECT hour FROM c
            )
            SELECT hour, COUNT(*)::int AS cnt
            FROM allh
            GROUP BY hour
            ORDER BY cnt DESC, hour ASC
            LIMIT 1
            """,
            chat_id, days
        )
    if not row:
        return None
    return int(row["hour"]), int(row["cnt"])
