from __future__ import annotations
from typing import Optional, List
from app.db import get_con

async def upsert_user(
    tg_id: int,
    first_name: Optional[str],
    last_name: Optional[str],
    username: Optional[str],
    language_code: Optional[str],
    phone_e164: Optional[str],
    region: Optional[str],
    is_premium: bool,
) -> None:
    async with get_con() as con:
        await con.execute(
            """
            INSERT INTO public.users
                (tg_id, first_name, last_name, username, language_code,
                 phone_e164, region, is_premium, created_at, updated_at, last_seen_at)
            VALUES
                ($1, $2, $3, $4, $5, $6, $7, $8, now(), now(), now())
            ON CONFLICT (tg_id) DO UPDATE SET
                first_name    = EXCLUDED.first_name,
                last_name     = EXCLUDED.last_name,
                username      = EXCLUDED.username,
                language_code = EXCLUDED.language_code,
                phone_e164    = COALESCE(EXCLUDED.phone_e164, public.users.phone_e164),
                region        = COALESCE(EXCLUDED.region, public.users.region),
                is_premium    = EXCLUDED.is_premium,
                updated_at    = now(),
                last_seen_at  = now()
            """,
            tg_id, first_name, last_name, username, language_code,
            phone_e164, region, is_premium
        )

async def has_phone(tg_id: int) -> bool:
    async with get_con() as con:
        row = await con.fetchrow(
            "SELECT phone_e164 FROM public.users WHERE tg_id = $1",
            tg_id
        )
    return bool(row and row["phone_e164"])

# --- New: admin/broadcast helpers ---

async def count_all_users() -> int:
    async with get_con() as con:
        row = await con.fetchrow("select count(*) as c from public.users")
    return int(row["c"]) if row else 0

async def count_premium_users() -> int:
    async with get_con() as con:
        row = await con.fetchrow("select count(*) as c from public.users where is_premium = true")
    return int(row["c"]) if row else 0

async def list_all_user_ids() -> List[int]:
    async with get_con() as con:
        rows = await con.fetch("select tg_id from public.users")
    return [int(r["tg_id"]) for r in rows]


# --- i18n helpers ------------------------------------------------------------

async def get_language(tg_id: int) -> str | None:
    async with get_con() as con:
        row = await con.fetchrow(
            "SELECT language FROM public.users WHERE tg_id = $1",
            tg_id
        )
    return (row["language"] if row and row["language"] else None)

async def set_language(tg_id: int, lang: str) -> None:
    async with get_con() as con:
        await con.execute(
            """
            INSERT INTO public.users (tg_id, language, updated_at)
            VALUES ($1, $2, now())
            ON CONFLICT (tg_id) DO UPDATE
              SET language  = EXCLUDED.language,
                  updated_at = now()
            """,
            tg_id, lang
        )
