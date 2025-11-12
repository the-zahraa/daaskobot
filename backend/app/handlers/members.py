# backend/app/handlers/members.py
from __future__ import annotations

from aiogram import F

import logging
from datetime import datetime, timezone, date
from typing import Optional

from aiogram.types import ChatMemberUpdated, Message, ChatPermissions
from aiogram.enums.chat_type import ChatType

from app.db import get_con
from app.repositories.stats import (
    inc_join,
    inc_leave,
    record_event,
    upsert_chat_user_index,
)
from app.repositories.required import list_group_targets
from app.handlers.start import _is_member, force_join_kb_group
from app.services.i18n import t  # ← i18n

log = logging.getLogger("handlers.members")
UTC = timezone.utc


def _now() -> datetime:
    return datetime.now(UTC)


def _today() -> date:
    return _now().date()


async def _ensure_groups_channels_row(chat_id: int) -> None:
    """
    Your join_logs.chat_id has a FK to groups_channels(telegram_id).
    Make sure a row exists to satisfy the FK, but keep it minimal.
    """
    try:
        async with get_con() as con:
            await con.execute(
                """
                INSERT INTO public.groups_channels (telegram_id)
                VALUES ($1)
                ON CONFLICT (telegram_id) DO NOTHING
                """,
                str(chat_id),  # groups_channels.telegram_id is TEXT
            )
    except Exception as e:
        # Don't block attribution if this fails; we'll still try to write campaign_joins
        log.warning("ensure groups_channels failed for chat=%s: %s", chat_id, e)


async def _lookup_campaign_name(chat_id: int, invite_url: str) -> Optional[str]:
    """
    Map invite URL -> campaign_name for this chat.
    Try exact URL, then fallback to invite-code suffix (+CODE or joinchat/CODE).
    """
    async with get_con() as con:
        # Exact URL
        row = await con.fetchrow(
            """
            SELECT campaign_name
            FROM public.campaign_links
            WHERE chat_id = $1 AND invite_link = $2
            ORDER BY created_at DESC
            LIMIT 1
            """,
            chat_id, invite_url
        )
        if row:
            return row["campaign_name"]

        # Fallback by code suffix
        row = await con.fetchrow(
            """
            WITH url_code AS (
              SELECT NULLIF(
                       REGEXP_REPLACE($2, '^.*(?:joinchat/|\\+)([A-Za-z0-9_-]+)$', '\\1'),
                       $2
                     ) AS code
            )
            SELECT cl.campaign_name
            FROM public.campaign_links cl, url_code uc
            WHERE cl.chat_id = $1
              AND uc.code IS NOT NULL
              AND NULLIF(
                    REGEXP_REPLACE(cl.invite_link, '^.*(?:joinchat/|\\+)([A-Za-z0-9_-]+)$', '\\1'),
                    cl.invite_link
                  ) = uc.code
            ORDER BY cl.created_at DESC
            LIMIT 1
            """,
            chat_id, invite_url
        )
        return row["campaign_name"] if row else None


async def _record_campaign_join(chat_id: int, user_id: int, invite_link_url: str) -> None:
    """
    Write-through persistence:
      • join_logs (audit) — may fail because of FK, but must not block attribution
      • campaign_joins (final attribution used by Top 30d)
    """
    # 1) Try to satisfy FK upfront; ignore failures
    await _ensure_groups_channels_row(chat_id)

    # 2) Try audit (join_logs). If it fails, log and continue.
    try:
        async with get_con() as con:
            await con.execute(
                """
                INSERT INTO public.join_logs (chat_id, user_id, event_type, invite_link, "timestamp")
                VALUES ($1, $2, 'join', $3, now())
                """,
                str(chat_id),  # join_logs.chat_id is TEXT in your DB
                user_id,
                invite_link_url,
            )
    except Exception as e:
        log.warning("join_logs insert failed (non-fatal): chat=%s user=%s err=%s", chat_id, user_id, e)

    # 3) Map and write to campaign_joins (this powers Top 30d)
    try:
        campaign_name = await _lookup_campaign_name(chat_id, invite_link_url)
        if campaign_name:
            async with get_con() as con:
                await con.execute(
                    """
                    INSERT INTO public.campaign_joins (chat_id, user_id, campaign_name, happened_at)
                    VALUES ($1, $2, $3, now())
                    """,
                    chat_id, user_id, campaign_name
                )
            log.info("campaign attribution: %r chat=%s user=%s", campaign_name, chat_id, user_id)
        else:
            log.info("campaign attribution: none chat=%s user=%s (link=%s)", chat_id, user_id, invite_link_url)
    except Exception as e:
        log.warning("campaign_joins insert failed: chat=%s user=%s err=%s", chat_id, user_id, e)


async def _handle_join(chat_id: int, user_id: int, invite_link_url: Optional[str]) -> None:
    ts = _now()
    d = ts.date()
    await inc_join(chat_id, d)
    await record_event(chat_id, user_id, ts, "join")
    await upsert_chat_user_index(chat_id, user_id, True, ts)
    if invite_link_url:
        await _record_campaign_join(chat_id, user_id, invite_link_url)
    # Note: actual restricting + DM happens in handlers where `bot` is available.


async def _handle_leave(chat_id: int, user_id: int) -> None:
    ts = _now()
    d = ts.date()
    await inc_leave(chat_id, d)
    await record_event(chat_id, user_id, ts, "leave")
    await upsert_chat_user_index(chat_id, user_id, False, ts)


# ---- Single source of truth: ChatMemberUpdated only (avoid double counts) ----

async def on_member_update(upd: ChatMemberUpdated) -> None:
    """
    Telegram provides `invite_link` here if user joined via a link CREATED BY THIS BOT.
    """
    chat_id = upd.chat.id
    old_status = getattr(upd.old_chat_member, "status", None)
    new_status = getattr(upd.new_chat_member, "status", None)

    joined_user = getattr(upd.new_chat_member, "user", None)
    user_id = getattr(joined_user, "id", None)
    if user_id is None:
        return

    MEMBERish = {"member", "restricted", "administrator", "creator"}
    LEFTish = {"left", "kicked"}

    # JOIN
    if (new_status in MEMBERish) and (old_status not in MEMBERish):
        invite_url: Optional[str] = None
        inv = getattr(upd, "invite_link", None)
        if inv is not None:
            invite_url = getattr(inv, "invite_link", None) or None
            if invite_url is not None:
                invite_url = str(invite_url)
        if invite_url:
            log.info("campaign: ChatMemberUpdated invited via link=%s chat=%s user=%s", invite_url, chat_id, user_id)

        await _handle_join(chat_id, user_id, invite_url)

        # 🔒 Immediate restrict + DM if required
        try:
            targets = await list_group_targets(chat_id)
            if targets:
                try:
                    perms = ChatPermissions(
                        can_send_messages=False,
                        can_send_audios=False,
                        can_send_documents=False,
                        can_send_photos=False,
                        can_send_videos=False,
                        can_send_video_notes=False,
                        can_send_voice_notes=False,
                        can_send_polls=False,
                        can_send_other_messages=False,
                        can_add_web_page_previews=False
                    )
                    await upd.bot.restrict_chat_member(chat_id, user_id, permissions=perms)
                except Exception:
                    pass
                # DM prompt
                try:
                    await upd.bot.send_message(
                        user_id,
                        t("force_join.dm_prompt", user_id=user_id, group=getattr(upd.chat, "title", "this group")),
                        reply_markup=force_join_kb_group(user_id, chat_id, targets),
                    )
                except Exception:
                    pass
        except Exception:
            pass
        return

    # LEAVE
    if (new_status in LEFTish) and (old_status in MEMBERish):
        await _handle_leave(chat_id, user_id)
        return


# ---- Public groups fallback (service messages) ----
def register(dp) -> None:
    # Primary: chat_member updates
    dp.chat_member.register(on_member_update)

    # Fallbacks ONLY for public groups (have a username); used when ChatMemberUpdated isn't delivered
    async def on_new_members_service(msg: Message):
        if msg.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
            return
        # Only public groups (username present)
        if not getattr(msg.chat, "username", None):
            return
        users = msg.new_chat_members or []
        for u in users:
            if not u or u.is_bot:
                continue

            await _handle_join(msg.chat.id, u.id, invite_link_url=None)

            # Restrict + DM as in on_member_update
            # (No outer try without except — errors handled per call)
            try:
                targets = await list_group_targets(msg.chat.id)
            except Exception:
                targets = []

            if targets:
                # Restrict
                try:
                    perms = ChatPermissions(
                        can_send_messages=False,
                        can_send_audios=False,
                        can_send_documents=False,
                        can_send_photos=False,
                        can_send_videos=False,
                        can_send_video_notes=False,
                        can_send_voice_notes=False,
                        can_send_polls=False,
                        can_send_other_messages=False,
                        can_add_web_page_previews=False
                    )
                    await msg.bot.restrict_chat_member(msg.chat.id, u.id, permissions=perms)
                except Exception:
                    pass

                # DM prompt
                try:
                    await msg.bot.send_message(
                        u.id,
                        t("force_join.dm_prompt", user_id=u.id, group=(msg.chat.title or "this group")),
                        reply_markup=force_join_kb_group(u.id, msg.chat.id, targets),
                    )
                except Exception:
                    pass

    async def on_left_member_service(msg: Message):
        if msg.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
            return
        if not getattr(msg.chat, "username", None):
            return
        u = msg.left_chat_member
        if not u or u.is_bot:
            return
        await _handle_leave(msg.chat.id, u.id)

    dp.message.register(on_new_members_service, F.new_chat_members)
    dp.message.register(on_left_member_service, F.left_chat_member)
