# backend/app/handlers/chat_link.py
from __future__ import annotations
import re
from typing import Optional, cast, Union

from aiogram import F, Bot
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.enums.chat_type import ChatType

from ..repositories.users import upsert_user
from ..repositories.tenants import ensure_personal_tenant, link_user_to_tenant
from ..repositories.chats import upsert_chat

def _normalize_target(s: str) -> Union[str, int]:
    """
    Accepts:
      - @username
      - -1001234567890 (returns int)
      - t.me/username (-> @username)
      - bare username (-> @username)
    """
    s = (s or "").strip()
    m = re.search(r"(?:^|https?://)?t\.me/([A-Za-z0-9_]+)$", s)
    if m:
        return "@"+m.group(1)
    if s.startswith("-100") and s[4:].isdigit():
        try:
            return int(s)  # IMPORTANT: pass int for private channels
        except Exception:
            return s
    if s.startswith("@"):
        return s
    if re.fullmatch(r"[A-Za-z0-9_]{5,}", s):
        return "@"+s
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

def register(dp):
    # /link in groups/supergroups
    async def link_in_group(msg: Message):
        if not msg.from_user:
            return
        member = await msg.chat.get_member(msg.from_user.id)
        if member.status not in ("creator", "administrator"):
            await msg.answer("Only chat admins can link this chat.")
            return
        tenant_id = await _ensure_user_tenant_from_msg(msg)
        if not tenant_id:
            await msg.answer("Could not resolve tenant."); return
        chat_type = "group" if msg.chat.type == ChatType.GROUP else "supergroup"
        await upsert_chat(msg.chat.id, tenant_id, msg.chat.title, chat_type)
        await msg.answer("✅ Linked! This chat is now associated with your tenant.")

    # Channel /link still redirects (Telegram hides human sender)
    async def link_in_channel(channel_post: Message):
        bot = cast(Bot, channel_post.bot)
        await bot.send_message(channel_post.chat.id, "For channels, please DM me: /link_channel @thischannel")

    # /link_channel in DM (works for @public and -100 private)
    async def link_channel_dm(msg: Message):
        parts = (msg.text or "").split(maxsplit=1)
        if len(parts) < 2:
            await msg.answer("Usage: /link_channel @channel_username or /link_channel -1001234567890")
            return
        target_raw = parts[1]
        target = _normalize_target(target_raw)
        bot = cast(Bot, msg.bot)

        # 1) get_chat
        try:
            chat = await bot.get_chat(target)
        except Exception:
            # If they pasted something weird, hint and bail
            await msg.answer(
                "I couldn't find that channel. Make sure:\n"
                "• The channel exists\n"
                "• The bot is added as ADMIN in that channel\n"
                "• You used @username (public) or the full -100… ID (private)"
            )
            return

        if chat.type != ChatType.CHANNEL:
            await msg.answer("This command links channels only. For groups, use /link inside the group.")
            return

        # 2) verify bot is admin there
        try:
            me_user = await bot.me()
            me = await bot.get_chat_member(chat.id, me_user.id)
            if me.status not in ("administrator", "creator"):
                await msg.answer("Please make me an admin in that channel first.")
                return
        except Exception:
            await msg.answer("Could not verify my admin rights in that channel.")
            return

        # 3) tie to sender's tenant
        tenant_id = await _ensure_user_tenant_from_msg(msg)
        if not tenant_id:
            await msg.answer("Could not resolve your tenant."); return

        await upsert_chat(chat.id, tenant_id, chat.title, "channel")
        await msg.answer(f"✅ Linked channel: {chat.title or chat.id}")

    dp.message.register(link_in_group, Command("link"), F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}))
    dp.channel_post.register(link_in_channel, Command("link"))
    dp.message.register(link_channel_dm, Command("link_channel"), F.chat.type == ChatType.PRIVATE)
