# backend/app/repositories/subscriptions.py
from __future__ import annotations

from typing import Optional
from datetime import datetime, timezone, timedelta

from app.db import get_con
from app.services.owners import is_owner

UTC = timezone.utc

__all__ = [
    "get_user_subscription_expiry",
    "get_user_subscription_status",
    "upsert_subscription_on_payment",
    # legacy alias
    "get_user_subscription",
]


async def get_user_subscription_expiry(tg_id: int) -> Optional[datetime]:
    """
    Return the latest expires_at for a user, or None.
    """
    async with get_con() as con:
        r = await con.fetchrow(
            "SELECT MAX(expires_at) AS exp FROM public.subscriptions WHERE tg_id=$1",
            tg_id,
        )
    return r["exp"] if r and r["exp"] else None


async def get_user_subscription_status(tg_id: int) -> str:
    """
    Returns 'Pro' if the user has an active subscription.
    Bot owner(s) are always treated as Pro.
    """
    if is_owner(tg_id):
        return "Pro"

    exp = await get_user_subscription_expiry(tg_id)
    if exp and exp > datetime.now(UTC):
        return "Pro"
    return "Free"


async def upsert_subscription_on_payment(
    tg_id: int,
    plan_code: str,
    duration_days: int,
    amount_stars: int,
    provider_payment_charge_id: Optional[str],
    telegram_payment_charge_id: Optional[str],
) -> None:
    """
    Extend or start a subscription:
      base = greatest(existing_expires_at, now)
      new_expires_at = base + duration_days
    Also attempts to write an audit row into payments (best-effort).
    """
    now = datetime.now(UTC)

    async with get_con() as con:
        base = await con.fetchval(
            "SELECT GREATEST(MAX(expires_at), $1) FROM public.subscriptions WHERE tg_id=$2",
            now,
            tg_id,
        )
        if not base:
            base = now
        new_expires = base + timedelta(days=int(duration_days))

        # Single-row upsert by tg_id (your schema has tg_id as PRIMARY KEY)
        await con.execute(
            """
            INSERT INTO public.subscriptions (tg_id, plan, started_at, expires_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (tg_id) DO UPDATE
              SET plan       = EXCLUDED.plan,
                  started_at = EXCLUDED.started_at,
                  expires_at = EXCLUDED.expires_at
            """,
            tg_id,
            plan_code,
            now,
            new_expires,
        )

        # Optional audit (do not block on failure)
        try:
            await con.execute(
                """
                INSERT INTO public.payments (
                  id, tenant_id, tg_user_id, amount, currency, method,
                  provider_payload, status, created_at
                )
                VALUES (
                  gen_random_uuid(), NULL, $1, $2, 'XTR', 'stars',
                  jsonb_build_object(
                    'plan', $3,
                    'provider_payment_charge_id', $4,
                    'telegram_payment_charge_id', $5
                  ),
                  'paid', $6
                )
                """,
                tg_id,
                amount_stars,
                plan_code,
                provider_payment_charge_id,
                telegram_payment_charge_id,
                now,
            )
        except Exception:
            # best-effort logging could be added here
            pass


# --------- Legacy compatibility (optional) ---------

async def get_user_subscription(tg_id: int) -> str:
    """
    Legacy alias so older code can import the old name.
    """
    return await get_user_subscription_status(tg_id)
