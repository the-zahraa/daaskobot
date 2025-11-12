# backend/app/handlers/campaigns.py
from __future__ import annotations

import html
import logging
import random
from typing import List, Tuple, Dict, Optional, cast

from aiogram import Router, F, Bot
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiogram.filters import Command
from aiogram.enums.chat_type import ChatType

# 🔁 Use relative imports (like in start.py)
from ..db import get_con
from ..repositories.subscriptions import get_user_subscription_status
from ..repositories.campaign_links import (
    create_campaign_link_record,
    list_campaign_links,
    clear_campaign_links,
)
from ..repositories.campaigns_read import get_top_campaigns_30d
from ..services.i18n import t  # i18n

router = Router()
log = logging.getLogger("app.handlers.campaigns")

# ---------------------------
# Helpers
# ---------------------------

async def _is_pro(user_id: int) -> bool:
    plan = await get_user_subscription_status(user_id)
    return str(plan).lower() == "pro"

async def _user_is_admin_or_owner(bot: Bot, chat_id: int, user_id: int) -> bool:
    try:
        m = await bot.get_chat_member(chat_id, user_id)
        status = str(getattr(m, "status", "")).lower()
        if status in {"administrator", "creator", "owner"}:
            return True
        for a in await bot.get_chat_administrators(chat_id):
            if getattr(a, "user", None) and a.user.id == user_id:
                return True
    except Exception:
        pass
    return False

async def _chat_is_public(bot: Bot, chat_id: int) -> bool:
    try:
        ch = await bot.get_chat(chat_id)
        # Public chats have a username (e.g., @mygroup). Private do not.
        return bool(getattr(ch, "username", None))
    except Exception:
        return False

async def _list_user_chats_simple(user_tg_id: int) -> List[Tuple[int, str, str]]:
    async with get_con() as con:
        rows = await con.fetch(
            """
            SELECT c.tg_chat_id, c.type, COALESCE(c.title,'—') AS title
            FROM public.chats c
            JOIN public.user_tenants ut ON ut.tenant_id = c.tenant_id
            WHERE ut.tg_id = $1
            ORDER BY c.created_at DESC
            LIMIT 50
            """,
            user_tg_id
        )
    return [(int(r["tg_chat_id"]), str(r["type"]), str(r["title"])) for r in rows]

def _kb_campaigns_root(user_id: int, chats: List[Tuple[int, str, str]]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for cid, ctype, title in chats[:30]:
        rows.append([InlineKeyboardButton(text=f"{title or cid} ({ctype})", callback_data=f"camp:chat:{cid}")])
    rows.append([InlineKeyboardButton(text=t("camp.buttons.back", user_id=user_id), callback_data="tenant_overview")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def _kb_private_menu(user_id: int, chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t("camp.buttons.new_quick", user_id=user_id), callback_data=f"camp:newq:{chat_id}")],
        [InlineKeyboardButton(text=t("camp.buttons.list", user_id=user_id),      callback_data=f"camp:list:{chat_id}")],
        [InlineKeyboardButton(text=t("camp.buttons.top", user_id=user_id),       callback_data=f"camp:stats:{chat_id}")],
        [InlineKeyboardButton(text=t("camp.buttons.back", user_id=user_id),      callback_data="tenant_campaigns")],
    ])

def _kb_public_menu(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t("camp.buttons.back", user_id=user_id), callback_data="tenant_campaigns")],
    ])

async def _ensure_pro_and_admin(cb: CallbackQuery, chat_id: int) -> bool:
    if not cb.from_user:
        await cb.answer()
        return False
    if not await _is_pro(cb.from_user.id):
        await cb.message.answer(t("camp.pro_gate", user_id=cb.from_user.id))
        await cb.answer()
        return False
    if not await _user_is_admin_or_owner(cast(Bot, cb.bot), chat_id, cb.from_user.id):
        await cb.message.answer(t("camp.admin_only", user_id=cb.from_user.id))
        await cb.answer()
        return False
    return True

async def _safe_edit(cb: CallbackQuery, text: str, kb: Optional[InlineKeyboardMarkup] = None) -> None:
    try:
        if cb.message and (cb.message.text or "") == text and (cb.message.reply_markup == kb):
            await cb.answer()
            return
        await cb.message.edit_text(text, reply_markup=kb, disable_web_page_preview=True)
    except Exception:
        try:
            await cb.message.answer(text, reply_markup=kb, disable_web_page_preview=True)
        except Exception:
            pass
    try:
        await cb.answer()
    except Exception:
        pass

# ---------------------------
# In-bot UI (callbacks)
# ---------------------------

@router.callback_query(F.data == "tenant_campaigns")
async def on_tenant_campaigns(cb: CallbackQuery):
    if not cb.from_user:
        return await cb.answer()
    chats = await _list_user_chats_simple(cb.from_user.id)
    if not chats:
        return await _safe_edit(cb, t("camp.no_chats", user_id=cb.from_user.id))
    await _safe_edit(cb, t("camp.pick_chat", user_id=cb.from_user.id), _kb_campaigns_root(cb.from_user.id, chats))

@router.callback_query(F.data.startswith("camp:chat:"))
async def on_chat_menu(cb: CallbackQuery):
    if not cb.from_user:
        return await cb.answer()
    bot = cast(Bot, cb.bot)
    chat_id = int((cb.data or "0").split(":")[2])
    if not await _ensure_pro_and_admin(cb, chat_id):
        return
    is_public = await _chat_is_public(bot, chat_id)
    if is_public:
        return await _safe_edit(cb, t("camp.public_block", user_id=cb.from_user.id), _kb_public_menu(cb.from_user.id))
    await _safe_edit(cb, t("camp.menu_head", user_id=cb.from_user.id), _kb_private_menu(cb.from_user.id, chat_id))

@router.callback_query(F.data.startswith("camp:list:"))
async def on_list_links(cb: CallbackQuery):
    if not cb.from_user:
        return await cb.answer()
    bot = cast(Bot, cb.bot)
    chat_id = int((cb.data or "0").split(":")[2])
    if not await _ensure_pro_and_admin(cb, chat_id):
        return
    if await _chat_is_public(bot, chat_id):
        return await _safe_edit(cb, t("camp.public_block", user_id=cb.from_user.id), _kb_public_menu(cb.from_user.id))

    rows = await list_campaign_links(chat_id)
    if not rows:
        return await _safe_edit(cb, t("camp.list.none", user_id=cb.from_user.id), _kb_private_menu(cb.from_user.id, chat_id))

    lines = [t("camp.list.title", user_id=cb.from_user.id)]
    for r in rows:
        lines.append(f"• <b>{html.escape(r['campaign_name'])}</b>\n  <code>{r['invite_link']}</code>")
    await _safe_edit(cb, "\n".join(lines), _kb_private_menu(cb.from_user.id, chat_id))

@router.callback_query(F.data.startswith("camp:stats:"))
async def on_stats(cb: CallbackQuery):
    if not cb.from_user:
        return await cb.answer()
    bot = cast(Bot, cb.bot)
    chat_id = int((cb.data or "0").split(":")[2])
    if not await _ensure_pro_and_admin(cb, chat_id):
        return
    if await _chat_is_public(bot, chat_id):
        return await _safe_edit(cb, t("camp.public_block", user_id=cb.from_user.id), _kb_public_menu(cb.from_user.id))

    top = await get_top_campaigns_30d(chat_id, limit=20)
    if not top:
        return await _safe_edit(cb, t("camp.stats.none", user_id=cb.from_user.id), _kb_private_menu(cb.from_user.id, chat_id))

    lines = [t("camp.stats.title", user_id=cb.from_user.id)]
    for name, cnt in top:
        lines.append(f"• {html.escape(name)} — <b>{cnt}</b>")
    await _safe_edit(cb, "\n".join(lines), _kb_private_menu(cb.from_user.id, chat_id))

# ---- Create quick link (auto label) ----

@router.callback_query(F.data.startswith("camp:newq:"))
async def on_new_quick_link(cb: CallbackQuery):
    if not cb.from_user:
        return await cb.answer()
    bot = cast(Bot, cb.bot)
    chat_id = int((cb.data or "0").split(":")[2])
    if not await _ensure_pro_and_admin(cb, chat_id):
        return
    if await _chat_is_public(bot, chat_id):
        return await _safe_edit(cb, t("camp.public_block", user_id=cb.from_user.id), _kb_public_menu(cb.from_user.id))

    label = f"Quick-{random.randint(10000, 99999)}"
    try:
        invite = await bot.create_chat_invite_link(
            chat_id,
            name=label,                  # optional label for admins
            creates_join_request=False,  # direct join so invite_link is present
        )
    except Exception as e:
        return await _safe_edit(
            cb,
            t("camp.create.cant_create", user_id=cb.from_user.id, err=html.escape(str(e))),
            _kb_private_menu(cb.from_user.id, chat_id)
        )

    try:
        await create_campaign_link_record(
            chat_id=chat_id,
            invite_link_url=invite.invite_link,
            campaign_name=label,
            created_by=cb.from_user.id,
        )
    except Exception:
        # Even if DB insert fails, joins will still attribute via invite_link seen in chat_member updates.
        pass

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t("camp.buttons.open_invite", user_id=cb.from_user.id), url=invite.invite_link)],
        [InlineKeyboardButton(text=t("camp.buttons.back", user_id=cb.from_user.id), callback_data=f"camp:chat:{chat_id}")]
    ])
    await _safe_edit(
        cb,
        t("camp.create.created", user_id=cb.from_user.id, label=html.escape(label), url=invite.invite_link),
        kb
    )

# ---------------------------
# Optional slash commands (kept simple)
# ---------------------------

@router.message(Command("campaigns"))
async def campaigns_help_cmd(msg: Message) -> None:
    pro = await _is_pro(msg.from_user.id) if msg.from_user else False
    if not pro:
        await msg.answer(t("camp.cmd.help_free", user_id=(msg.from_user.id if msg.from_user else None)))
        return
    await msg.answer(t("camp.cmd.help_pro", user_id=(msg.from_user.id if msg.from_user else None)))

@router.message(Command("campaigns_clear"))
async def campaigns_clear_cmd(msg: Message) -> None:
    if msg.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP, ChatType.CHANNEL):
        await msg.answer(t("camp.cmd.clear_wrong_place", user_id=(msg.from_user.id if msg.from_user else None)))
        return
    if not (msg.from_user and await _is_pro(msg.from_user.id)):
        await msg.answer(t("camp.pro_gate", user_id=(msg.from_user.id if msg.from_user else None)))
        return
    if not await _user_is_admin_or_owner(cast(Bot, msg.bot), msg.chat.id, msg.from_user.id):
        await msg.answer(t("camp.cmd.admins_only", user_id=msg.from_user.id))
        return

    await clear_campaign_links(msg.chat.id)
    await msg.answer(t("camp.cmd.cleared", user_id=msg.from_user.id))
