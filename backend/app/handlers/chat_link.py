from __future__ import annotations
import re
from typing import Optional, cast, Union

from aiogram import F, Bot
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.enums.chat_type import ChatType
from aiogram.enums.chat_member_status import ChatMemberStatus

from ..repositories.users import upsert_user
from ..repositories.tenants import ensure_personal_tenant, link_user_to_tenant
from ..repositories.chats import upsert_chat
from ..services.i18n import t  # i18n


def _normalize_target(s: str) -> Union[str, int]:
    s = (s or "").strip()
    m = re.search(r"(?:^|https?://)?t\.me/([A-Za-z0-9_]+)$", s)
    if m:
        return "@" + m.group(1)
    if s.startswith("-100") and s[4:].isdigit():
        try:
            return int(s)
        except Exception:
            return s
    if s.startswith("@"):
        return s
    if re.fullmatch(r"[A-Za-z0-9_]{5,}", s):
        return "@" + s
    return s


async def _ensure_user_tenant_from_msg(msg: Message) -> Optional[str]:
    if not msg.from_user:
        return None
    u = msg.from_user
    await upsert_user(
        tg_id=u.id,
        first_name=u.first_name,
        last_name=u.last_name,
        username=u.username,
        language_code=u.language_code,
        phone_e164=None,
        region=None,
        is_premium=bool(getattr(u, "is_premium", False)),
    )
    display = (u.first_name or u.username or f"User {u.id}")
    tid = await ensure_personal_tenant(u.id, display)
    await link_user_to_tenant(u.id, tid)
    return tid


def _is_admin_status(status: str) -> bool:
    # Be tolerant across Telegram/Aiogram versions
    return status in (
        "administrator",
        "creator",
        "owner",
        ChatMemberStatus.ADMINISTRATOR,
        ChatMemberStatus.CREATOR,
    )


def register(dp):
    # /link inside groups/supergroups
    async def link_in_group(msg: Message):
        if not msg.from_user:
            return
        try:
            member = await msg.chat.get_member(msg.from_user.id)
            status = getattr(member, "status", "")
            if not _is_admin_status(status):
                await msg.answer(t("link.admins_only_link", user_id=msg.from_user.id))
                return
        except Exception:
            # If Telegram fails to return membership, optimistically continue but warn
            await msg.answer(t("link.couldnt_verify_admin", user_id=msg.from_user.id))

        tenant_id = await _ensure_user_tenant_from_msg(msg)
        if not tenant_id:
            await msg.answer(t("link.tenant_fail", user_id=msg.from_user.id))
            return

        chat_type = "group" if msg.chat.type == ChatType.GROUP else "supergroup"
        await upsert_chat(msg.chat.id, tenant_id, msg.chat.title, chat_type)
        await msg.answer(t("link.linked_ok", user_id=msg.from_user.id))

    # /link in channels redirects
    async def link_in_channel(channel_post: Message):
        bot = cast(Bot, channel_post.bot)
        await bot.send_message(channel_post.chat.id, t("link.channel_redirect"))

    # /link_channel in DM (public @ or -100 id)
    async def link_channel_dm(msg: Message):
        parts = (msg.text or "").split(maxsplit=1)
        if len(parts) < 2:
            await msg.answer(t("link.usage", user_id=(msg.from_user.id if msg.from_user else None)))
            return
        target_raw = parts[1]
        target = _normalize_target(target_raw)
        bot = cast(Bot, msg.bot)

        try:
            chat = await bot.get_chat(target)
        except Exception:
            await msg.answer(t("link.not_found", user_id=(msg.from_user.id if msg.from_user else None)))
            return

        if chat.type != ChatType.CHANNEL:
            await msg.answer(t("link.wrong_type", user_id=(msg.from_user.id if msg.from_user else None)))
            return

        try:
            me_user = await bot.me()
            me = await bot.get_chat_member(chat.id, me_user.id)
            if not _is_admin_status(getattr(me, "status", "")):
                await msg.answer(t("link.make_me_admin", user_id=(msg.from_user.id if msg.from_user else None)))
                return
        except Exception:
            await msg.answer(t("link.cant_verify_my_admin", user_id=(msg.from_user.id if msg.from_user else None)))
            return

        tenant_id = await _ensure_user_tenant_from_msg(msg)
        if not tenant_id:
            await msg.answer(t("link.tenant_fail_you", user_id=(msg.from_user.id if msg.from_user else None)))
            return

        await upsert_chat(chat.id, tenant_id, chat.title, "channel")
        await msg.answer(t("link.linked_channel", user_id=(msg.from_user.id if msg.from_user else None), title=(chat.title or chat.id)))

    dp.message.register(link_in_group, Command("link"), F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}))
    dp.channel_post.register(link_in_channel, Command("link"))
    dp.message.register(link_channel_dm, Command("link_channel"), F.chat.type == ChatType.PRIVATE)
