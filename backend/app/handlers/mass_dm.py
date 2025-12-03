# backend/app/handlers/mass_dm.py
from __future__ import annotations
from typing import cast, Optional, Tuple

from aiogram import Router, F, Bot
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ChatType

from app.services.i18n import t
from app.repositories.subscriptions import get_user_subscription_status
from app.repositories.audience import count_audience, get_audience_user_ids
from app.handlers.broadcast import _send_in_chunks  # reuse your existing helper

router = Router()


# ----------------- FSM -----------------
class MassDMState(StatesGroup):
    composing = State()


# ----------------- helpers -----------------
async def _is_pro(user_id: int) -> bool:
    plan = await get_user_subscription_status(user_id)
    return str(plan).strip().lower().startswith("pro")


def _default_filters() -> Tuple[str, int]:
    """
    has_phone_mode: 'any' | 'yes' | 'no'
    last_active_days: 7 | 30 | 90
    """
    return "any", 30


async def _get_filters(state: FSMContext) -> Tuple[str, int]:
    data = await state.get_data()
    has_phone_mode = data.get("has_phone_mode") or "any"
    last_active_days = int(data.get("last_active_days") or 30)
    if last_active_days not in (7, 30, 90):
        last_active_days = 30
    if has_phone_mode not in ("any", "yes", "no"):
        has_phone_mode = "any"
    return has_phone_mode, last_active_days


async def _set_filters(
    state: FSMContext,
    *,
    has_phone_mode: Optional[str] = None,
    last_active_days: Optional[int] = None,
) -> None:
    cur = await state.get_data()
    if has_phone_mode is not None:
        cur["has_phone_mode"] = has_phone_mode
    if last_active_days is not None:
        cur["last_active_days"] = int(last_active_days)
    await state.update_data(**cur)


def _home_kb(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=t("massdm.buttons.audience", user_id=user_id), callback_data="mdm:aud")],
            [InlineKeyboardButton(text=t("massdm.buttons.send", user_id=user_id), callback_data="mdm:compose")],
            [InlineKeyboardButton(text=t("massdm.buttons.back", user_id=user_id), callback_data="tenant_overview")],
        ]
    )


def _audience_kb(user_id: int, has_phone_mode: str, last_active_days: int) -> InlineKeyboardMarkup:
    # Has phone label
    if has_phone_mode == "yes":
        hp_label = t("massdm.filter.has_phone_yes", user_id=user_id)
        hp_next = "no"
    elif has_phone_mode == "no":
        hp_label = t("massdm.filter.has_phone_no", user_id=user_id)
        hp_next = "any"
    else:
        hp_label = t("massdm.filter.has_phone_any", user_id=user_id)
        hp_next = "yes"

    # Last active label
    if last_active_days <= 7:
        la_label = t("massdm.filter.last_active_7", user_id=user_id)
        la_next = 30
    elif last_active_days <= 30:
        la_label = t("massdm.filter.last_active_30", user_id=user_id)
        la_next = 90
    else:
        la_label = t("massdm.filter.last_active_90", user_id=user_id)
        la_next = 7

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=hp_label, callback_data=f"mdm:hp:{hp_next}")],
            [InlineKeyboardButton(text=la_label, callback_data=f"mdm:la:{la_next}")],
            [InlineKeyboardButton(text=t("massdm.buttons.send", user_id=user_id), callback_data="mdm:compose")],
            [InlineKeyboardButton(text=t("massdm.buttons.back_simple", user_id=user_id), callback_data="mdm:home")],
        ]
    )


def _back_kb(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=t("massdm.buttons.back_simple", user_id=user_id), callback_data="mdm:home")]]
    )


# ----------------- Entry from dashboard -----------------
@router.callback_query(F.data == "massdm_home")
async def mdm_home(cb: CallbackQuery, state: FSMContext):
    if not cb.from_user or (cb.message and cb.message.chat.type != ChatType.PRIVATE):
        await cb.answer()
        return

    # Pro gate
    if not await _is_pro(cb.from_user.id):
        await cb.message.edit_text(t("pro.popups.massdm", user_id=cb.from_user.id))
        await cb.answer()
        return

    await state.clear()
    has_phone_mode, last_active_days = _default_filters()
    await _set_filters(state, has_phone_mode=has_phone_mode, last_active_days=last_active_days)

    intro = t("massdm.home", user_id=cb.from_user.id)
    await cb.message.edit_text(intro, reply_markup=_home_kb(cb.from_user.id))
    await cb.answer()


@router.callback_query(F.data == "mdm:home")
async def mdm_home_back(cb: CallbackQuery, state: FSMContext):
    if not cb.from_user:
        await cb.answer()
        return
    intro = t("massdm.home", user_id=cb.from_user.id)
    await cb.message.edit_text(intro, reply_markup=_home_kb(cb.from_user.id))
    await cb.answer()


# ----------------- Audience & filters -----------------
@router.callback_query(F.data == "mdm:aud")
async def mdm_audience(cb: CallbackQuery, state: FSMContext):
    if not cb.from_user:
        await cb.answer()
        return

    has_phone_mode, last_active_days = await _get_filters(state)
    total = await count_audience(cb.from_user.id, last_active_days, has_phone_mode)

    hp_txt = {
        "any": t("massdm.filters.hp_any_label", user_id=cb.from_user.id),
        "yes": t("massdm.filters.hp_yes_label", user_id=cb.from_user.id),
        "no": t("massdm.filters.hp_no_label", user_id=cb.from_user.id),
    }.get(has_phone_mode, t("massdm.filters.hp_any_label", user_id=cb.from_user.id))

    lines = [
        t("massdm.aud.title", user_id=cb.from_user.id),
        t("massdm.aud.matched", user_id=cb.from_user.id, total=total),
        "",
        t("massdm.aud.filters", user_id=cb.from_user.id, hp=hp_txt, days=last_active_days),
        "",
        t("massdm.aud.explain", user_id=cb.from_user.id),
        "",
        t("massdm.aud.note", user_id=cb.from_user.id),
    ]
    text = "\n".join(lines)

    await cb.message.edit_text(text, reply_markup=_audience_kb(cb.from_user.id, has_phone_mode, last_active_days))
    await cb.answer()


@router.callback_query(F.data.startswith("mdm:hp:"))
async def mdm_toggle_has_phone(cb: CallbackQuery, state: FSMContext):
    if not cb.from_user:
        await cb.answer()
        return

    nxt = (cb.data or "").split(":")[-1]
    if nxt not in ("any", "yes", "no"):
        nxt = "any"
    await _set_filters(state, has_phone_mode=nxt)
    await mdm_audience(cb, state)


@router.callback_query(F.data.startswith("mdm:la:"))
async def mdm_toggle_last_active(cb: CallbackQuery, state: FSMContext):
    if not cb.from_user:
        await cb.answer()
        return

    try:
        la = int((cb.data or "").split(":")[-1])
    except Exception:
        la = 30
    if la not in (7, 30, 90):
        la = 30
    await _set_filters(state, last_active_days=la)
    await mdm_audience(cb, state)


# ----------------- Compose & send -----------------
@router.callback_query(F.data == "mdm:compose")
async def mdm_compose(cb: CallbackQuery, state: FSMContext):
    if not cb.from_user:
        await cb.answer()
        return

    await state.set_state(MassDMState.composing)
    await cb.message.edit_text(
        t("massdm.compose.prompt", user_id=cb.from_user.id),
        reply_markup=_back_kb(cb.from_user.id),
    )
    await cb.answer()


@router.message(MassDMState.composing)
async def mdm_send(msg: Message, state: FSMContext):
    if not msg.from_user:
        return

    # Allow /cancel
    if (msg.text or "").strip().lower() == "/cancel":
        await state.clear()
        await msg.answer(t("massdm.compose.cancelled", user_id=msg.from_user.id))
        return

    text = msg.text or msg.caption
    if not text:
        await msg.answer(t("massdm.send.no_text", user_id=msg.from_user.id))
        return

    has_phone_mode, last_active_days = await _get_filters(state)
    await state.clear()

    # Load audience
    user_ids = await get_audience_user_ids(
        msg.from_user.id,
        last_active_days,
        has_phone_mode,
    )

    # Exclude self
    user_ids = [u for u in user_ids if u != msg.from_user.id]

    if not user_ids:
        await msg.answer(t("massdm.send.no_audience", user_id=msg.from_user.id))
        return

    approx = len(user_ids)
    await msg.answer(t("massdm.send.estimate", user_id=msg.from_user.id, n=approx))

    bot = cast(Bot, msg.bot)
    sent, failed = await _send_in_chunks(bot, user_ids, text)

    await msg.answer(t("massdm.send.result", user_id=msg.from_user.id, sent=sent, failed=failed))
