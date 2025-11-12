# backend/app/handlers/group_tools_dm.py
from __future__ import annotations
import html
from typing import List, Tuple, Dict, Optional, cast

from aiogram import Router, F, Bot
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, Message
from aiogram.enums.chat_type import ChatType

from app.db import get_con
from app.repositories.subscriptions import get_user_subscription_status
from app.repositories.required import add_group_target, list_group_targets, clear_group_targets
from app.services.i18n import t  # ← i18n

router = Router()

# ---------- helpers ----------

async def _is_pro(user_id: int) -> bool:
    plan = await get_user_subscription_status(user_id)
    return str(plan).lower() == "pro"

async def _user_is_admin_or_owner(bot: Bot, chat_id: int, user_id: int) -> bool:
    try:
        m = await bot.get_chat_member(chat_id, user_id)
        if str(getattr(m, "status", "")).lower() in {"administrator", "creator", "owner"}:
            return True
        for a in await bot.get_chat_administrators(chat_id):
            if getattr(a, "user", None) and a.user.id == user_id:
                return True
    except Exception:
        pass
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

def _kb_roots(user_id: int, chats: List[Tuple[int, str, str]]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for cid, ctype, title in chats[:30]:
        rows.append([InlineKeyboardButton(text=f"{title or cid} ({ctype})", callback_data=f"gt:chat:{cid}")])
    rows.append([InlineKeyboardButton(text=t("gt.buttons.back", user_id=user_id), callback_data="tenant_overview")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def _kb_menu(chat_id: int, user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t("gt.buttons.add", user_id=user_id),   callback_data=f"gt:add:{chat_id}")],
        [InlineKeyboardButton(text=t("gt.buttons.list", user_id=user_id),  callback_data=f"gt:list:{chat_id}")],
        [InlineKeyboardButton(text=t("gt.buttons.clear", user_id=user_id), callback_data=f"gt:clear:{chat_id}")],
        [InlineKeyboardButton(text=t("gt.buttons.how", user_id=user_id),   callback_data=f"gt:how:{chat_id}")],
        [InlineKeyboardButton(text=t("gt.buttons.back", user_id=user_id),  callback_data="tenant_group_tools")],
    ])

_PENDING_TARGET_FOR: Dict[int, int] = {}  # user_id -> chat_id

async def _edit(cb: CallbackQuery, text: str, kb: Optional[InlineKeyboardMarkup] = None) -> None:
    try:
        if (cb.message.text or "") == text and (cb.message.reply_markup == kb):
            await cb.answer(); return
        await cb.message.edit_text(text, reply_markup=kb, disable_web_page_preview=True)
    except Exception:
        try:
            await cb.message.answer(text, reply_markup=kb, disable_web_page_preview=True)
        except Exception:
            pass
    await cb.answer()

# ---------- entry ----------

@router.callback_query(F.data == "tenant_group_tools")
async def on_tools_root(cb: CallbackQuery):
    if not cb.from_user:
        return await cb.answer()
    if not await _is_pro(cb.from_user.id):
        await _edit(cb, f"{t('gt.title', user_id=cb.from_user.id)}\n{t('gt.pro_gate', user_id=cb.from_user.id)}")
        return
    chats = await _list_user_chats_simple(cb.from_user.id)
    if not chats:
        await _edit(cb, f"{t('gt.title', user_id=cb.from_user.id)}\n{t('gt.none_chats', user_id=cb.from_user.id)}")
        return
    await _edit(cb, f"{t('gt.title', user_id=cb.from_user.id)}\n{t('gt.pick_group', user_id=cb.from_user.id)}", _kb_roots(cb.from_user.id, chats))

@router.callback_query(F.data.startswith("gt:chat:"))
async def on_chat_menu(cb: CallbackQuery):
    chat_id = int((cb.data or "0").split(":")[2])
    if not cb.from_user:
        return await cb.answer()
    if not await _is_pro(cb.from_user.id):
        await _edit(cb, t("gt.errors.pro_only", user_id=cb.from_user.id))
        return
    if not await _user_is_admin_or_owner(cast(Bot, cb.bot), chat_id, cb.from_user.id):
        await _edit(cb, t("gt.errors.admin_only", user_id=cb.from_user.id))
        return
    await _edit(cb, t("gt.menu_lead", user_id=cb.from_user.id), _kb_menu(chat_id, cb.from_user.id))

@router.callback_query(F.data.startswith("gt:list:"))
async def on_list(cb: CallbackQuery):
    chat_id = int((cb.data or "0").split(":")[2])
    targets = await list_group_targets(chat_id)  # [{target, join_url}]
    if not targets:
        await _edit(cb, t("gt.list.none", user_id=cb.from_user.id), _kb_menu(chat_id, cb.from_user.id)); return
    lines = [t("gt.list.title", user_id=cb.from_user.id)]
    for row in targets:
        tval = row.get("target") or ""
        ju = row.get("join_url")
        if ju:
            lines.append(f"• {html.escape(tval)} — <code>{html.escape(ju)}</code>")
        else:
            lines.append(f"• {html.escape(tval)}")
    await _edit(cb, "\n".join(lines), _kb_menu(chat_id, cb.from_user.id))

@router.callback_query(F.data.startswith("gt:clear:"))
async def on_clear(cb: CallbackQuery):
    chat_id = int((cb.data or "0").split(":")[2])
    await clear_group_targets(chat_id)
    await _edit(cb, t("gt.clear.ok", user_id=cb.from_user.id), _kb_menu(chat_id, cb.from_user.id))

@router.callback_query(F.data.startswith("gt:how:"))
async def on_how(cb: CallbackQuery):
    chat_id = int((cb.data or "0").split(":")[2])
    text = t("gt.how.body", user_id=cb.from_user.id)
    await _edit(cb, text, _kb_menu(chat_id, cb.from_user.id))

@router.callback_query(F.data.startswith("gt:add:"))
async def on_add(cb: CallbackQuery):
    if not cb.from_user: return await cb.answer()
    chat_id = int((cb.data or "0").split(":")[2])
    _PENDING_TARGET_FOR[cb.from_user.id] = chat_id
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t("gt.buttons.cancel", user_id=cb.from_user.id), callback_data=f"gt:chat:{chat_id}")]
    ])
    await _edit(cb, t("gt.add.prompt", user_id=cb.from_user.id), kb)

@router.message(F.chat.type == ChatType.PRIVATE)
async def on_add_target_text(msg: Message):
    if not (msg.from_user and msg.from_user.id in _PENDING_TARGET_FOR):
        return
    chat_id = _PENDING_TARGET_FOR.pop(msg.from_user.id)
    val = (msg.text or "").strip()
    if not val:
        await msg.answer(t("gt.errors.need_valid", user_id=msg.from_user.id)); return
    # Normalize
    target = val if (val.startswith("@") or val.startswith("-100") or val.startswith("https://t.me/")) else f"@{val}"
    await add_group_target(chat_id, target, msg.from_user.id, target if target.startswith("https://t.me/") else None)
    kb = _kb_menu(chat_id, msg.from_user.id)
    await msg.answer(t("gt.add.added", user_id=msg.from_user.id, target=html.escape(target)), reply_markup=kb)
