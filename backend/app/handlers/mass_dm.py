# backend/app/handlers/mass_dm.py
from __future__ import annotations
import os
from typing import cast, Optional

from aiogram import Router, F, Bot
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ChatType

from app.services.i18n import t
from app.repositories.referrals import (
    get_or_create_ref_code,
    count_referred,
    count_referred_with_filters,
    select_user_ids_for_customer,
)
from app.repositories.subscriptions import get_user_subscription_status
from app.handlers.broadcast import _send_in_chunks  # reuse

router = Router()

# ---- FSM ----
class MassDMState(StatesGroup):
    composing = State()
    set_un_len = State()
    set_nm_len = State()

# ---- helpers ----

async def _is_pro(user_id: int) -> bool:
    plan = await get_user_subscription_status(user_id)
    return str(plan).lower() == "pro"

def _home_kb(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔗 Get my link", callback_data="mdm:get_link")],
        [InlineKeyboardButton(text="👥 My people",  callback_data="mdm:aud")],
        [InlineKeyboardButton(text="📤 Send message", callback_data="mdm:compose")],
        [InlineKeyboardButton(text=t("common.back", user_id=user_id), callback_data="tenant_overview")],
    ])

def _aud_kb(user_id: int, has_phone: Optional[bool], un_min: int, nm_min: int) -> InlineKeyboardMarkup:
    # cycle buttons
    hp_label = "Has phone: Any"
    hp_next = "any"
    if has_phone is True:
        hp_label, hp_next = "Has phone: Yes", "no"
    elif has_phone is False:
        hp_label, hp_next = "Has phone: No", "any"

    def _len_label(n: int) -> str:
        return f"Min username: {n}"

    def _nmlabel(n: int) -> str:
        return f"Min name: {n}"

    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=hp_label, callback_data=f"mdm:hp:{hp_next}")],
        [InlineKeyboardButton(text=_len_label(un_min), callback_data="mdm:set_un")],
        [InlineKeyboardButton(text=_nmlabel(nm_min), callback_data="mdm:set_nm")],
        [InlineKeyboardButton(text="📤 Send message", callback_data="mdm:compose")],
        [InlineKeyboardButton(text="⬅️ Back", callback_data="mdm:back")]
    ])

def _send_back_kb(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Back", callback_data="mdm:back")]
    ])

# ---- Filters state helpers ----

async def _get_filters(state: FSMContext):
    data = await state.get_data()
    return (
        data.get("has_phone", None),
        int(data.get("min_username_len", 0)),
        int(data.get("min_name_len", 0)),
    )

async def _set_filters(state: FSMContext, *, has_phone=None, un_min=None, nm_min=None):
    data = await state.get_data()
    if has_phone is not None:
        data["has_phone"] = has_phone
    if un_min is not None:
        data["min_username_len"] = int(un_min)
    if nm_min is not None:
        data["min_name_len"] = int(nm_min)
    await state.update_data(**data)

# ---- Entry ----

@router.callback_query(F.data == "massdm_home")
async def mdm_home(cb: CallbackQuery, state: FSMContext):
    if not cb.from_user or (cb.message and cb.message.chat.type != ChatType.PRIVATE):
        await cb.answer()
        return

    # Pro gate
    if not await _is_pro(cb.from_user.id):
        # Short upsell using existing i18n text
        await cb.message.edit_text(t("pro.popups.massdm", user_id=cb.from_user.id))
        await cb.answer()
        return

    await state.clear()
    await cb.message.edit_text(
        "📣 <b>Mass DM</b>\nSend messages to people who <b>joined with your link</b>.",
        reply_markup=_home_kb(cb.from_user.id),
    )
    await cb.answer()

# ---- Get link ----

@router.callback_query(F.data == "mdm:get_link")
async def mdm_get_link(cb: CallbackQuery, state: FSMContext):
    if not cb.from_user:
        await cb.answer()
        return
    bot = cast(Bot, cb.bot)
    code = await get_or_create_ref_code(cb.from_user.id)
    me = await bot.get_me()
    link = f"https://t.me/{me.username}?start=ref-{code}"
    total = await count_referred(cb.from_user.id)
    text = (
        "🔗 <b>Your invite link</b>\n"
        f"{link}\n\n"
        "Share this link. People who start the bot from it become <b>your audience</b>.\n"
        f"Current audience: <b>{total}</b>"
    )
    await cb.message.edit_text(text, reply_markup=_home_kb(cb.from_user.id))
    await cb.answer()

# ---- Audience & filters ----

@router.callback_query(F.data == "mdm:aud")
async def mdm_aud(cb: CallbackQuery, state: FSMContext):
    if not cb.from_user:
        await cb.answer()
        return
    has_phone, un_min, nm_min = await _get_filters(state)
    count = await count_referred_with_filters(cb.from_user.id, has_phone, un_min, nm_min)
    text = (
        "👥 <b>My people</b>\n"
        f"Matched now: <b>{count}</b>\n\n"
        "Filters:"
    )
    await cb.message.edit_text(text, reply_markup=_aud_kb(cb.from_user.id, has_phone, un_min, nm_min))
    await cb.answer()

@router.callback_query(F.data.startswith("mdm:hp:"))
async def mdm_toggle_hp(cb: CallbackQuery, state: FSMContext):
    nxt = (cb.data or "").split(":")[-1]
    val = None if nxt == "any" else (True if nxt == "yes" else False)
    await _set_filters(state, has_phone=val)
    await mdm_aud(cb, state)

@router.callback_query(F.data == "mdm:set_un")
async def mdm_set_un(cb: CallbackQuery, state: FSMContext):
    await state.set_state(MassDMState.set_un_len)
    await cb.message.edit_text(
        "Send a number for <b>min username letters</b> (e.g., 0, 5, 8).",
        reply_markup=_send_back_kb(cb.from_user.id),
    )
    await cb.answer()

@router.message(MassDMState.set_un_len)
async def mdm_recv_un(msg: Message, state: FSMContext):
    try:
        n = max(0, int((msg.text or "0").strip()))
    except Exception:
        await msg.answer(
            "Please send a number like 0, 5, 8.",
            reply_markup=_send_back_kb(msg.from_user.id),
        )
        return
    await _set_filters(state, un_min=n)
    await state.clear()
    await msg.answer("Saved.", reply_markup=None)

@router.callback_query(F.data == "mdm:set_nm")
async def mdm_set_nm(cb: CallbackQuery, state: FSMContext):
    await state.set_state(MassDMState.set_nm_len)
    await cb.message.edit_text(
        "Send a number for <b>min name letters</b> (e.g., 0, 5, 8).",
        reply_markup=_send_back_kb(cb.from_user.id),
    )
    await cb.answer()

@router.message(MassDMState.set_nm_len)
async def mdm_recv_nm(msg: Message, state: FSMContext):
    try:
        n = max(0, int((msg.text or "0").strip()))
    except Exception:
        await msg.answer(
            "Please send a number like 0, 5, 8.",
            reply_markup=_send_back_kb(msg.from_user.id),
        )
        return
    await _set_filters(state, nm_min=n)
    await state.clear()
    await msg.answer("Saved.", reply_markup=None)

@router.callback_query(F.data == "mdm:back")
async def mdm_back(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.edit_text(
        "📣 <b>Mass DM</b>\nSend messages to people who <b>joined with your link</b>.",
        reply_markup=_home_kb(cb.from_user.id),
    )
    await cb.answer()

# ---- Compose & send ----

@router.callback_query(F.data == "mdm:compose")
async def mdm_compose(cb: CallbackQuery, state: FSMContext):
    if not cb.from_user:
        await cb.answer()
        return
    await state.set_state(MassDMState.composing)
    await cb.message.edit_text(
        "Send the message text to broadcast.\nUse /cancel to stop.",
        reply_markup=_send_back_kb(cb.from_user.id),
    )
    await cb.answer()

@router.message(MassDMState.composing)
async def mdm_send(msg: Message, state: FSMContext):
    if not msg.from_user:
        return
    text = msg.text or msg.caption
    if not text:
        await msg.answer("Please send text.")
        return

    # Get filters BEFORE clearing state
    has_phone, un_min, nm_min = await _get_filters(state)
    await state.clear()

    user_ids = await select_user_ids_for_customer(
        msg.from_user.id,
        has_phone,
        un_min or 0,
        nm_min or 0,
    )

    if not user_ids:
        await msg.answer("No one matches your filters yet.")
        return

    await msg.answer(f"Sending to ~{len(user_ids)}. Please wait…")
    bot = cast(Bot, msg.bot)
    # exclude self
    user_ids = [u for u in user_ids if u != msg.from_user.id]
    sent, failed = await _send_in_chunks(bot, user_ids, text)
    await msg.answer(f"Done ✅  Sent: {sent}, Failed: {failed}")
