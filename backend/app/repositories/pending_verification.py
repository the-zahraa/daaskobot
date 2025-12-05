from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Tuple

from app.db import get_con


async def add_pending_verification(chat_id: int, user_id: int, ttl_seconds: int = 120) -> None:
    """
    Create or update a pending verification record for (chat_id, user_id).
    Stores deadline = NOW() + ttl_seconds * INTERVAL '1 second'.
    """
    async with get_con() as con:
        await con.execute(
            """
            INSERT INTO public.pending_verifications (chat_id, user_id, deadline, verified)
            VALUES ($1, $2, NOW() + $3 * INTERVAL '1 second', FALSE)
            ON CONFLICT (chat_id, user_id) DO UPDATE
            SET deadline = EXCLUDED.deadline,
                verified = FALSE
            """,
            chat_id,
            user_id,
            ttl_seconds,
        )


async def mark_verified_for_user(user_id: int) -> List[int]:
    async with get_con() as con:
        rows = await con.fetch(
            """
            UPDATE public.pending_verifications
            SET verified = TRUE
            WHERE user_id = $1
              AND verified = FALSE
              AND deadline >= NOW()
            RETURNING chat_id
            """,
            user_id,
        )
    return [int(r["chat_id"]) for r in rows]


async def should_ban(chat_id: int, user_id: int) -> bool:
    async with get_con() as con:
        row = await con.fetchrow(
            """
            SELECT verified, deadline
            FROM public.pending_verifications
            WHERE chat_id = $1 AND user_id = $2
            """,
            chat_id,
            user_id,
        )

    if not row:
        return False
    if row["verified"]:
        return False

    deadline = row["deadline"]
    if isinstance(deadline, datetime):
        return deadline < datetime.now(timezone.utc)
    return False


async def get_expired_unverified(limit: int = 50) -> List[Tuple[int, int]]:
    async with get_con() as con:
        rows = await con.fetch(
            """
            SELECT chat_id, user_id
            FROM public.pending_verifications
            WHERE verified = FALSE
              AND deadline < NOW()
            LIMIT $1
            """,
            limit,
        )
    return [(int(r["chat_id"]), int(r["user_id"])) for r in rows]
