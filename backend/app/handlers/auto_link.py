# backend/app/handlers/auto_link.py
from __future__ import annotations

import logging
from typing import Optional

from aiogram.types import ChatMemberUpdated
from aiogram.enums.chat_type import ChatType
from aiogram.enums.chat_member_status import ChatMemberStatus

from app.repositories.users import upsert_user
from app.repositories.tenants import ensure_personal_tenant, link_user_to_tenant
from app.repositories.chats import upsert_chat
from app.services.i18n import t  # i18n

log = logging.getLogger("handlers.auto_link")


def _is_admin_status(status: object) -> bool:
    """
    Be tolerant across Telegram / Aiogram versions.
    """
    if isinstance(status, ChatMemberStatus):
        status = status.value
    s = str(status or "").lower()
    return s in {"administrator", "creator", "owner"}


async def _ensure_tenant_for_user(
    tg_id: int,
    first_name: Optional[str],
    last_name: Optional[str],
    username: Optional[str],
) -> Optional[str]:
    """
    Same idea as _ensure_user_tenant_from_msg in chat_link.py,
    but adapted for ChatMemberUpdated (no Message object here).
    """
    try:
        await upsert_user(
            tg_id=tg_id,
            first_name=first_name,
            last_name=last_name,
            username=username,
            language_code=None,
            phone_e164=None,
            region=None,
            is_premium=False,
        )
    except Exception as e:
        log.warning("auto_link: upsert_user failed for %s: %s", tg_id, e)

    display = (first_name or username or f"User {tg_id}")
    try:
        tenant_id = await ensure_personal_tenant(tg_id, display)
        await link_user_to_tenant(tg_id, tenant_id)
        return tenant_id
    except Exception as e:
        log.error("auto_link: ensure_personal_tenant failed for %s: %s", tg_id, e)
        return None


async def on_bot_admin_change(upd: ChatMemberUpdated) -> None:
    """
    Called when THIS BOT's status changes in a chat (my_chat_member update).

    We auto-link when:
      - chat is group / supergroup / channel
      - new status is admin
      - old status was not admin
      - a human user performed the action (from_user)
    """
    chat = upd.chat
    chat_type_enum = chat.type

    if chat_type_enum not in (ChatType.GROUP, ChatType.SUPERGROUP, ChatType.CHANNEL):
        return

    old_status = getattr(upd.old_chat_member, "status", None)
    new_status = getattr(upd.new_chat_member, "status", None)

    if not (_is_admin_status(new_status) and not _is_admin_status(old_status)):
        # Not a promotion to admin → ignore
        return

    actor = upd.from_user  # the admin who added/promoted the bot
    if not actor or actor.is_bot:
        log.info("auto_link: skipped (no human actor) chat=%s", chat.id)
        return

    log.info(
        "auto_link: bot promoted to admin in chat=%s type=%s by user=%s",
        chat.id,
        chat_type_enum,
        actor.id,
    )

    # 1) Ensure tenant for this admin (same logic as /link)
    tenant_id = await _ensure_tenant_for_user(
        actor.id,
        getattr(actor, "first_name", None),
        getattr(actor, "last_name", None),
        getattr(actor, "username", None),
    )
    if not tenant_id:
        # rare failure: warn the admin in DM (localized)
        try:
            await upd.bot.send_message(
                actor.id,
                t("auto_link.failed_dm", user_id=actor.id),
            )
        except Exception:
            pass
        return

    # 2) Upsert chat using the SAME helper as /link
    if chat_type_enum == ChatType.GROUP:
        chat_type = "group"
    elif chat_type_enum == ChatType.SUPERGROUP:
        chat_type = "supergroup"
    else:  # ChatType.CHANNEL
        chat_type = "channel"

    title = chat.title
    try:
        await upsert_chat(int(chat.id), tenant_id, title, chat_type)
        log.info(
            "auto_link: upsert_chat done chat_id=%s tenant_id=%s type=%s title=%r",
            chat.id,
            tenant_id,
            chat_type,
            title,
        )
    except Exception as e:
        log.error(
            "auto_link: upsert_chat failed for chat=%s tenant=%s: %s",
            chat.id,
            tenant_id,
            e,
        )
        return

    # 3) Confirmation INSIDE THE CHAT ONLY (no success DM)
    try:
        if chat_type_enum == ChatType.CHANNEL:
            text = t(
                "link.linked_channel",
                user_id=actor.id,
                title=(title or str(chat.id)),
            )
        else:
            text = t("link.linked_ok", user_id=actor.id)
        await upd.bot.send_message(chat.id, text)
    except Exception as e:
        log.warning("auto_link: failed to send in-chat confirmation: %s", e)


def register(dp) -> None:
    """
    Register handler on my_chat_member updates (aiogram v3 style).
    """
    dp.my_chat_member.register(on_bot_admin_change)
    log.info("auto_link: my_chat_member handler registered")
