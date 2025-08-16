# backend/app/repositories/subscriptions.py
from typing import Optional
from app.services.db import get_pool

async def get_user_subscription_status(tg_id: int) -> str:
    pool = await get_pool()
    async with pool.acquire() as con:
        row = await con.fetchrow(
            """
            select plan
            from subscriptions
            where tg_id = $1
            order by started_at desc
            limit 1
            """,
            tg_id
        )
        if not row or not row["plan"]:
            return "inactive"
        return str(row["plan"]).lower()

async def upsert_user_subscription(tg_id: int, plan: str) -> None:
    pool = await get_pool()
    async with pool.acquire() as con:
        await con.execute(
            """
            insert into subscriptions (tg_id, plan, started_at)
            values ($1, $2, now())
            on conflict (tg_id) do update set
              plan = excluded.plan,
              started_at = now(),
              expires_at = null
            """,
            tg_id, plan
        )

async def set_user_plan_days(tg_id: int, plan: str, days: int | None) -> None:
    """
    Set plan and, if days provided, set expires_at = now() + days.
    For 'free' we usually clear expires_at; for 'pro' set duration.
    """
    pool = await get_pool()
    async with pool.acquire() as con:
        if days is None:
            await con.execute(
                """
                insert into subscriptions (tg_id, plan, started_at, expires_at)
                values ($1, $2, now(), null)
                on conflict (tg_id) do update set
                  plan = excluded.plan,
                  started_at = now(),
                  expires_at = null
                """,
                tg_id, plan
            )
        else:
            await con.execute(
                """
                insert into subscriptions (tg_id, plan, started_at, expires_at)
                values ($1, $2, now(), now() + ($3 || ' days')::interval)
                on conflict (tg_id) do update set
                  plan = excluded.plan,
                  started_at = now(),
                  expires_at = now() + ($3 || ' days')::interval
                """,
                tg_id, plan, days
            )
