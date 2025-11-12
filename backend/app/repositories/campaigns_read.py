# backend/app/repositories/campaigns_read.py
from __future__ import annotations
from typing import List, Tuple
from app.db import get_con

async def get_top_campaigns_30d(chat_id: int, limit: int = 5) -> List[Tuple[str, int]]:
    """
    Reads from campaign_joins (simple, reliable).
    """
    async with get_con() as con:
        rows = await con.fetch(
            """
            SELECT campaign_name, COUNT(*)::int AS joins_30d
            FROM public.campaign_joins
            WHERE chat_id = $1
              AND happened_at >= now() - interval '30 days'
            GROUP BY campaign_name
            ORDER BY joins_30d DESC, campaign_name
            LIMIT $2
            """,
            chat_id, limit
        )
    return [(r["campaign_name"], int(r["joins_30d"])) for r in rows]
