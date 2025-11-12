# backend/app/repositories/campaign_links.py
from __future__ import annotations
from typing import List, Dict, Optional
from urllib.parse import urlparse
import re

from app.db import get_con

# Normalize and extract the invite code to make matching resilient.
# Handles: https://t.me/+CODE or https://t.me/joinchat/CODE (and without scheme).
_CODE_RE = re.compile(r"(?:joinchat/|\+)([A-Za-z0-9_-]+)$")

def _extract_code(invite_url: str | None) -> Optional[str]:
    if not invite_url:
        return None
    u = invite_url.strip()
    # Quick regex for +CODE or joinchat/CODE at the end
    m = _CODE_RE.search(u)
    if m:
        return m.group(1)
    # Fallback: try last path segment
    try:
        p = urlparse(u)
        if p.path:
            seg = p.path.rstrip("/").split("/")[-1]
            if seg and seg != "joinchat":
                return seg
    except Exception:
        pass
    return None


async def create_campaign_link_record(
    chat_id: int,
    invite_link_url: str,
    campaign_name: str,
    created_by: int,
) -> Dict[str, str]:
    """
    Store a campaign link. We also tolerate duplicates by unique(invite_link).
    tenant_id is derived from chats.
    """
    async with get_con() as con:
        row = await con.fetchrow(
            """
            INSERT INTO public.campaign_links (id, tenant_id, chat_id, invite_link, campaign_name, created_by, created_at)
            VALUES (gen_random_uuid(),
                    (SELECT tenant_id FROM public.chats WHERE tg_chat_id = $1 LIMIT 1),
                    $1, $2, $3, $4, now())
            ON CONFLICT (invite_link) DO UPDATE
              SET campaign_name = EXCLUDED.campaign_name,
                  created_by    = EXCLUDED.created_by
            RETURNING id, invite_link, campaign_name
            """,
            chat_id, invite_link_url, campaign_name, created_by
        )
    return {"id": str(row["id"]), "invite_link": row["invite_link"], "campaign_name": row["campaign_name"]}


async def list_campaign_links(chat_id: int) -> List[Dict[str, str]]:
    async with get_con() as con:
        rows = await con.fetch(
            """
            SELECT invite_link, campaign_name, created_at
            FROM public.campaign_links
            WHERE chat_id = $1
            ORDER BY created_at DESC
            """,
            chat_id
        )
    return [{"invite_link": r["invite_link"], "campaign_name": r["campaign_name"], "created_at": r["created_at"].isoformat()} for r in rows]


async def clear_campaign_links(chat_id: int) -> None:
    async with get_con() as con:
        await con.execute("DELETE FROM public.campaign_links WHERE chat_id = $1", chat_id)


async def get_campaign_name_by_invite_link(chat_id: int, invite_link_url: str) -> Optional[str]:
    """
    Robust mapper:
      1) exact match on full invite_link
      2) fallback: match by invite code suffix so URL format differences don't matter
    """
    code = _extract_code(invite_link_url)

    async with get_con() as con:
        # Try exact first
        row = await con.fetchrow(
            """
            SELECT campaign_name
            FROM public.campaign_links
            WHERE chat_id = $1 AND invite_link = $2
            LIMIT 1
            """,
            chat_id, invite_link_url
        )
        if row:
            return str(row["campaign_name"])

        if code:
            # Fallback by code suffix (case-sensitive safe for codes)
            row2 = await con.fetchrow(
                """
                SELECT campaign_name
                FROM public.campaign_links
                WHERE chat_id = $1 AND invite_link LIKE '%' || $2
                ORDER BY created_at DESC
                LIMIT 1
                """,
                chat_id, code
            )
            if row2:
                return str(row2["campaign_name"])

    return None
