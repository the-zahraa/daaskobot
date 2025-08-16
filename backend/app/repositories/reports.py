# backend/app/repositories/reports.py
from __future__ import annotations

from typing import List, Tuple, Optional
from app.services.db import get_pool


# ---------- Aggregations ----------

async def agg_daily(chat_id: int, days: int = 30) -> List[Tuple[str, int, int, int]]:
    """
    Return rows: (YYYY-MM-DD, joins, leaves, net) for the last `days`.
    Uses chat_members_daily; fills missing days with zeros.
    """
    pool = await get_pool()
    async with pool.acquire() as con:
        rows = await con.fetch(
            """
            with days as (
              select generate_series((current_date - ($2::int - 1))::date, current_date, interval '1 day')::date d
            ),
            a as (
              select day::date d, sum(joins) j, sum(leaves) l
              from chat_members_daily
              where chat_id = $1
              group by 1
            )
            select to_char(d, 'YYYY-MM-DD') as dd,
                   coalesce(a.j,0) as joins,
                   coalesce(a.l,0) as leaves,
                   coalesce(a.j,0) - coalesce(a.l,0) as net
            from days
            left join a on a.d = days.d
            order by d desc
            """,
            chat_id, days,
        )
        return [(r["dd"], int(r["joins"]), int(r["leaves"]), int(r["net"])) for r in rows]


async def agg_weekly(chat_id: int, weeks: int = 12) -> List[Tuple[str, int, int, int]]:
    """
    Return rows: (ISO-YYYY-WW, joins, leaves, net) for the last `weeks`.
    """
    pool = await get_pool()
    async with pool.acquire() as con:
        rows = await con.fetch(
            """
            select to_char(day, 'IYYY-IW') as wk,
                   sum(joins) as j, sum(leaves) as l,
                   sum(joins) - sum(leaves) as net
            from chat_members_daily
            where chat_id = $1
              and day >= (current_date - ($2::int * 7))
            group by 1
            order by 1 desc
            """,
            chat_id, weeks,
        )
        return [(r["wk"], int(r["j"]), int(r["l"]), int(r["net"])) for r in rows]


async def agg_monthly(chat_id: int, months: int = 12) -> List[Tuple[str, int, int, int]]:
    """
    Return rows: (YYYY-MM, joins, leaves, net) for the last `months`.
    """
    pool = await get_pool()
    async with pool.acquire() as con:
        rows = await con.fetch(
            """
            select to_char(day, 'YYYY-MM') as ym,
                   sum(joins) as j, sum(leaves) as l,
                   sum(joins) - sum(leaves) as net
            from chat_members_daily
            where chat_id = $1
              and day >= (date_trunc('month', current_date) - (($2::int - 1) * interval '1 month'))
            group by 1
            order by 1 desc
            """,
            chat_id, months,
        )
        return [(r["ym"], int(r["j"]), int(r["l"]), int(r["net"])) for r in rows]


async def peak_hours(chat_id: int, days: int = 7) -> List[Tuple[int, int]]:
    """
    Return list of (hour_0_23, join_events_count) for the last `days`.
    Uses member_events (kind='join').
    """
    pool = await get_pool()
    async with pool.acquire() as con:
        rows = await con.fetch(
            """
            select extract(hour from happened_at at time zone 'UTC')::int as hh,
                   count(*) as c
            from member_events
            where chat_id = $1
              and kind = 'join'
              and happened_at >= (now() - ($2::int || ' days')::interval)
            group by 1
            order by 1 asc
            """,
            chat_id, days,
        )
        return [(int(r["hh"]), int(r["c"])) for r in rows]


# ---------- Filtering ----------

async def filter_members(
    chat_id: int,
    name_q: Optional[str] = None,
    phone_country: Optional[str] = None,
    has_phone_only: bool = False,
) -> List[Tuple[int, str, Optional[str], Optional[str]]]:
    """
    Return up to 500 rows of (tg_id, full_name, username, phone_e164) for users seen in this chat.

    Notes:
    - Results are based on chat_user_index (who we’ve seen join/leave) joined with users (those who DM’d the bot).
    - Filters:
        name_q: case-insensitive match on first_name + last_name
        phone_country: e.g., '+33', '+98' (prefix match)
        has_phone_only: only users who shared a phone
    """
    pool = await get_pool()
    async with pool.acquire() as con:
        conds = ["c.chat_id = $1"]
        args = [chat_id]
        i = 2

        if has_phone_only:
            conds.append("u.phone_e164 is not null")

        if phone_country:
            conds.append(f"u.phone_e164 like ${i}")
            args.append(phone_country + "%")
            i += 1

        if name_q:
            conds.append(f"(coalesce(u.first_name,'') || ' ' || coalesce(u.last_name,'')) ilike ${i}")
            args.append(f"%{name_q}%")
            i += 1

        sql = f"""
            select
              c.tg_id,
              nullif(trim(coalesce(u.first_name,'') || ' ' || coalesce(u.last_name,'')), '') as full_name,
              u.username,
              u.phone_e164
            from chat_user_index c
            left join users u on u.tg_id = c.tg_id
            where {' and '.join(conds)}
            order by c.last_seen_at desc
            limit 500
        """
        rows = await con.fetch(sql, *args)
        return [
            (
                int(r["tg_id"]),
                r["full_name"] or "",
                r["username"],
                r["phone_e164"],
            )
            for r in rows
        ]
