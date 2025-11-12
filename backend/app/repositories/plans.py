# backend/app/repositories/plans.py
from __future__ import annotations

from typing import List, Dict, Optional

from app.db import get_con

__all__ = [
    "list_active_plans",
    "get_plan_by_code",
    "list_plans",
    "set_plan_price",
    "toggle_plan_active",
    "upsert_plan",
]


async def list_active_plans() -> List[Dict]:
    """
    Return active plans sorted by price ascending.
    Columns: code, title, description, price_stars, duration_days, is_active
    """
    async with get_con() as con:
        rows = await con.fetch(
            """
            SELECT code, title, description, price_stars, duration_days, is_active
            FROM public.plans
            WHERE is_active = TRUE
            ORDER BY price_stars ASC, duration_days ASC, code ASC
            """
        )
    return [dict(r) for r in rows]


async def get_plan_by_code(code: str) -> Optional[Dict]:
    async with get_con() as con:
        r = await con.fetchrow(
            """
            SELECT code, title, description, price_stars, duration_days, is_active
            FROM public.plans
            WHERE code = $1
            """,
            code,
        )
    return dict(r) if r else None


# ----------------- Admin-facing helpers -----------------

async def list_plans(active_only: bool = True) -> List[Dict]:
    """
    List plans. When active_only=False, returns all plans.
    """
    if active_only:
        return await list_active_plans()

    async with get_con() as con:
        rows = await con.fetch(
            """
            SELECT code, title, description, price_stars, duration_days, is_active
            FROM public.plans
            ORDER BY is_active DESC, price_stars ASC, duration_days ASC, code ASC
            """
        )
    return [dict(r) for r in rows]


async def set_plan_price(code: str, price_stars: int) -> Optional[Dict]:
    """
    Update plan price. Negative values are clamped to 0.
    Returns the updated row or None if not found.
    """
    price = int(price_stars)
    if price < 0:
        price = 0

    async with get_con() as con:
        r = await con.fetchrow(
            """
            UPDATE public.plans
               SET price_stars = $2
             WHERE code = $1
         RETURNING code, title, description, price_stars, duration_days, is_active
            """,
            code,
            price,
        )
    return dict(r) if r else None


async def toggle_plan_active(code: str, is_active: Optional[bool] = None) -> Optional[Dict]:
    """
    Toggle or set plan active flag.
      - If is_active is None, flip the current value.
      - If is_active is True/False, set explicitly.
    Returns the updated row or None if not found.
    """
    async with get_con() as con:
        if is_active is None:
            r = await con.fetchrow(
                """
                UPDATE public.plans
                   SET is_active = NOT COALESCE(is_active, FALSE)
                 WHERE code = $1
             RETURNING code, title, description, price_stars, duration_days, is_active
                """,
                code,
            )
        else:
            r = await con.fetchrow(
                """
                UPDATE public.plans
                   SET is_active = $2
                 WHERE code = $1
             RETURNING code, title, description, price_stars, duration_days, is_active
                """,
                code,
                bool(is_active),
            )
    return dict(r) if r else None


async def upsert_plan(
    code: str,
    title: str,
    description: str,
    price_stars: int,
    duration_days: int,
    is_active: bool = True,
) -> Dict:
    """
    Create or update a plan (code is primary key).
    Returns the upserted row.

    (Kept for future use; current admin UI only edits existing defaults.)
    """
    async with get_con() as con:
        r = await con.fetchrow(
            """
            INSERT INTO public.plans (code, title, description, price_stars, duration_days, is_active)
            VALUES ($1,$2,$3,$4,$5,$6)
            ON CONFLICT (code) DO UPDATE SET
              title         = EXCLUDED.title,
              description   = EXCLUDED.description,
              price_stars   = EXCLUDED.price_stars,
              duration_days = EXCLUDED.duration_days,
              is_active     = EXCLUDED.is_active,
              updated_at    = now()
            RETURNING code, title, description, price_stars, duration_days, is_active
            """,
            code,
            title,
            description,
            int(price_stars),
            int(duration_days),
            bool(is_active),
        )
    return dict(r)
