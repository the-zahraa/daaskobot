# backend/app/handlers/members.py
from __future__ import annotations
import re
from typing import cast, List, Optional, Dict
from datetime import datetime, timezone

from aiogram import F, Bot
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery
from aiogram.enums.chat_type import ChatType
from aiogram.types.chat_permissions import ChatPermissions
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from ..repositories.required import (
    set_group_required,
    unset_group_required,
    list_group_required,
)
from ..repositories.stats import inc_join, inc_leave, record_event, upsert_chat_user_index

UTC = timezone.utc

def _normalize_target(s: str) -> str:
    s = s.strip()
    if s.startswith("-100") and s[4:].isdigit():
        return s
    if s.startswith("@"):
        return s
    m = re.search(r"(?:t\.me/|https?://t\.me/)([A-Za-z0-9_]+)$", s)
    if m:
        return "@"+m.group(1)
    if re.fullmatch(r"[A-Za-z0-9_]{5,}", s):
        return "@"+s
    return s

def _looks_like_url(s: str) -> bool:
    return bool(re.match(r"^https?://t\.me/(?:\+|joinchat/|[A-Za-z0-9_]+)$", s))

def _verify_kb(chat_id: int, user_id: int, missing: List[Dict[str, Optional[str]]]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for item in missing:
        t = item["target"]
        url = item.get("join_url")
        if url:
            open_url = url
        elif t and t.startswith("@"):
            open_url = f"https://t.me/{t[1:]}"
        else:
            open_url = "https://t.me/"
        rows.append([InlineKeyboardButton(text=f"🔗 Open {t}", url=open_url)])
    rows.insert(0, [InlineKeyboardButton(text="✅ I joined", callback_data=f"gfj_check:{chat_id}:{user_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

async def _is_member(bot: Bot, target: str, user_id: int) -> bool:
    try:
        m = await bot.get_chat_member(target, user_id)
        return m.status in ("creator","administrator","member","restricted")
    except Exception:
        return False

def register(dp):
    async def set_force_join_cmd(msg: Message):
        if msg.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
            await msg.answer("Use this command in your group."); return
        if not msg.from_user: return
        member = await msg.chat.get_member(msg.from_user.id)
        if member.status not in ("creator","administrator"):
            await msg.answer("Only group admins can set this."); return

        parts = (msg.text or "").split()
        if len(parts) < 2:
            await msg.answer(
                "Usage:\n"
                "<code>/set_force_join @PublicChannel</code>\n"
                "<code>/set_force_join -1001234567890</code>\n"
                "Private? include invite link:\n"
                "<code>/set_force_join -1001234567890 https://t.me/+InviteCode</code>"
            )
            return

        target = _normalize_target(parts[1])
        if not (target.startswith("@") or (target.startswith("-100") and target[4:].isdigit())):
            await msg.answer("Provide a valid @username or -100… id as first argument.")
            return
        join_url = None
        if len(parts) >= 3 and _looks_like_url(parts[2]):
            join_url = parts[2]

        await set_group_required(msg.chat.id, target, msg.from_user.id, join_url)
        reqs = await list_group_required(msg.chat.id)
        lines = [f"✅ Added: {target}"]
        if join_url:
            lines.append("🔗 Invite link saved.")
        lines.append("Current requirements:")
        for r in reqs:
            lines.append(f"• {r['target']}" + (" (link set)" if r.get('join_url') else ""))
        await msg.answer("\n".join(lines))

    async def unset_force_join_cmd(msg: Message):
        if msg.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
            await msg.answer("Use this command in your group."); return
        if not msg.from_user: return
        member = await msg.chat.get_member(msg.from_user.id)
        if member.status not in ("creator","administrator"):
            await msg.answer("Only group admins can unset this."); return
        parts = (msg.text or "").split(maxsplit=1)
        if len(parts) >= 2:
            target = parts[1].strip()
            await unset_group_required(msg.chat.id, target)
            await msg.answer(f"✅ Removed: {target}")
        else:
            await unset_group_required(msg.chat.id)
            await msg.answer("✅ All requirements cleared.")
        reqs = await list_group_required(msg.chat.id)
        if reqs:
            lines = ["Current requirements:"]
            for r in reqs:
                lines.append(f"• {r['target']}" + (" (link set)" if r.get('join_url') else ""))
            await msg.answer("\n".join(lines))

    async def on_member_join(msg: Message):
        if msg.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
            return
        now = datetime.now(tz=UTC)
        await inc_join(msg.chat.id, msg.date.date())
        for user in msg.new_chat_members or []:
            # log event & index
            await record_event(msg.chat.id, user.id, now, "join")
            await upsert_chat_user_index(msg.chat.id, user.id, True, now)

        reqs = await list_group_required(msg.chat.id)
        if not reqs:
            return
        for user in msg.new_chat_members or []:
            missing: List[Dict[str, Optional[str]]] = []
            for r in reqs:
                t = r["target"]
                if not await _is_member(cast(Bot, msg.bot), t, user.id):
                    missing.append(r)
            if not missing:
                continue
            try:
                await msg.chat.restrict(user.id, permissions=ChatPermissions(can_send_messages=False))
            except Exception:
                pass
            lines = [f"👋 {user.full_name}, please join all required before chatting:"]
            lines += [f"• {r['target']}" for r in missing]
            lines.append("Tap “I joined” after joining — I’ll unmute you automatically.")
            await msg.answer("\n".join(lines), reply_markup=_verify_kb(msg.chat.id, user.id, missing))

    async def verify_after_join(cb: CallbackQuery):
        if not cb.from_user or not cb.data:
            await cb.answer(); return
        try:
            _, chat_id_s, user_id_s = cb.data.split(":")
            chat_id = int(chat_id_s)
            target_user_id = int(user_id_s)
        except Exception:
            await cb.answer(); return
        if cb.from_user.id != target_user_id:
            await cb.answer("This button isn’t for you.", show_alert=True)
            return
        reqs = await list_group_required(chat_id)
        missing = []
        for r in reqs:
            if not await _is_member(cast(Bot, cb.bot), r["target"], cb.from_user.id):
                missing.append(r["target"])
        if missing:
            await cb.answer("Still missing some memberships. Join all and tap again.", show_alert=True)
            return
        try:
            await cb.bot.restrict_chat_member(
                chat_id=chat_id,
                user_id=cb.from_user.id,
                permissions=ChatPermissions(can_send_messages=True)
            )
        except Exception:
            pass
        try:
            if cb.message:
                await cb.message.edit_text("✅ Verified! You can chat now.", reply_markup=None)
        except Exception:
            pass
        await cb.answer("Verified.")

    async def on_member_leave(msg: Message):
        if msg.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
            return
        now = datetime.now(tz=UTC)
        await inc_leave(msg.chat.id, msg.date.date())
        if msg.left_chat_member:
            await record_event(msg.chat.id, msg.left_chat_member.id, now, "leave")
            await upsert_chat_user_index(msg.chat.id, msg.left_chat_member.id, False, now)

    dp.message.register(set_force_join_cmd, Command("set_force_join"))
    dp.message.register(unset_force_join_cmd, Command("unset_force_join"))
    dp.message.register(on_member_join, F.new_chat_members)
    dp.message.register(on_member_leave, F.left_chat_member)
    dp.callback_query.register(verify_after_join, F.data.startswith("gfj_check:"))
