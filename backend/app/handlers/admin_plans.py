from __future__ import annotations
import os
from typing import Optional, List, Dict, Any

from aiogram import F, Bot
from aiogram.filters import Command
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.exceptions import TelegramBadRequest

from app.db import get_con
from app.repositories.plans import (
    list_plans,
    get_plan_by_code,
    set_plan_price,
    toggle_plan_active,
)
from app.services.i18n import t  # i18n

# ----------------- owner helpers -----------------

_owner_env = os.getenv("OWNER_ID", "").strip()
try:
    OWNER_ID: Optional[int] = int(_owner_env) if _owner_env else None
except Exception:
    OWNER_ID = None


def _is_owner(user_id: Optional[int]) -> bool:
    return bool(OWNER_ID and user_id and OWNER_ID == user_id)


# ----------------- FSM -----------------

class CustomPriceStates(StatesGroup):
    waiting_price = State()


# ----------------- UI builders -----------------

def _home_kb(user_id: int, has_plans: bool) -> InlineKeyboardMarkup:
    rows = []
    if has_plans:
        rows.append(
            [
                InlineKeyboardButton(
                    text=t("ap.home_btn_list", user_id=user_id),
                    callback_data="ap:list",
                )
            ]
        )
    rows.append(
        [
            InlineKeyboardButton(
                text=t("ap.home_btn_create_defaults", user_id=user_id),
                callback_data="ap:create_defaults",
            )
        ]
    )
    rows.append(
        [
            InlineKeyboardButton(
                text=t("ap.home_btn_refresh", user_id=user_id),
                callback_data="ap:home",
            )
        ]
    )
    rows.append(
        [
            InlineKeyboardButton(
                text=t("ap.btn_back_admin", user_id=user_id),
                callback_data="admin_overview",
            )
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _plans_list_kb(user_id: int, plans: List[Dict[str, Any]]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for p in plans:
        code = p["code"]
        price = int(p.get("price_stars") or 0)
        active = "✅" if p.get("is_active") else "❌"
        rows.append(
            [
                InlineKeyboardButton(
                    text=t(
                        "ap.plan_row",
                        user_id=user_id,
                        code=code,
                        price=price,
                        active=active,
                    ),
                    callback_data=f"ap:view:{code}",
                )
            ]
        )
    rows.append(
        [
            InlineKeyboardButton(
                text=t("ap.btn_back", user_id=user_id),
                callback_data="ap:home",
            )
        ]
    )
    rows.append(
        [
            InlineKeyboardButton(
                text=t("ap.btn_back_admin", user_id=user_id),
                callback_data="admin_overview",
            )
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _plan_view_kb(
    user_id: int, code: str, current_price: int, is_active: bool
) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(
                text=t("ap.view_delta_minus50", user_id=user_id),
                callback_data=f"ap:delta:{code}:-50",
            ),
            InlineKeyboardButton(
                text=t("ap.view_delta_minus10", user_id=user_id),
                callback_data=f"ap:delta:{code}:-10",
            ),
            InlineKeyboardButton(
                text=t("ap.view_delta_plus10", user_id=user_id),
                callback_data=f"ap:delta:{code}:10",
            ),
            InlineKeyboardButton(
                text=t("ap.view_delta_plus50", user_id=user_id),
                callback_data=f"ap:delta:{code}:50",
            ),
        ],
        [
            InlineKeyboardButton(
                text=t("ap.view_set_custom", user_id=user_id),
                callback_data=f"ap:set_custom:{code}",
            ),
            InlineKeyboardButton(
                text=t(
                    "ap.view_toggle",
                    user_id=user_id,
                    label=("✅ Disable" if is_active else "✅ Enable"),
                ),
                callback_data=f"ap:toggle:{code}",
            ),
        ],
        [
            InlineKeyboardButton(
                text=t("ap.view_back_to_list", user_id=user_id),
                callback_data="ap:list",
            )
        ],
        [
            InlineKeyboardButton(
                text=t("ap.btn_back_admin", user_id=user_id),
                callback_data="admin_overview",
            )
        ],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ----------------- helpers -----------------

async def _ensure_defaults():
    """
    Create / refresh default plans:
      - PRO_WEEK   (7 days)
      - PRO_MONTH  (30 days)
      - PRO_YEAR   (365 days)
    Admin can later change their prices from the UI.
    """
    async with get_con() as con:
        for code, title, desc, price, days in [
            (
                "PRO_WEEK",
                "Pro (7 days)",
                "Unlock Force Join + analytics + reports for a week",
                80,
                7,
            ),
            (
                "PRO_MONTH",
                "Pro (30 days)",
                "Unlock Force Join + analytics + reports",
                300,
                30,
            ),
            (
                "PRO_YEAR",
                "Pro (365 days)",
                "All Pro features for a year",
                3000,
                365,
            ),
        ]:
            await con.execute(
                """
                INSERT INTO public.plans (code, title, description, price_stars, duration_days, is_active)
                VALUES ($1,$2,$3,$4,$5,true)
                ON CONFLICT (code) DO UPDATE SET
                  title         = EXCLUDED.title,
                  description   = EXCLUDED.description,
                  price_stars   = EXCLUDED.price_stars,
                  duration_days = EXCLUDED.duration_days,
                  is_active     = true,
                  updated_at    = now()
                """,
                code,
                title,
                desc,
                price,
                days,
            )


async def _render_home(msg_or_cb):
    # Always ensure defaults exist when opening the admin plans UI
    await _ensure_defaults()

    uid = (
        msg_or_cb.from_user.id
        if hasattr(msg_or_cb, "from_user") and msg_or_cb.from_user
        else None
    )
    plans = await list_plans(active_only=False)
    text = t("ap.home_title", user_id=uid)
    kb = _home_kb(uid or 0, has_plans=bool(plans))

    if isinstance(msg_or_cb, Message):
        await msg_or_cb.answer(text, reply_markup=kb)
    else:
        # CallbackQuery
        await msg_or_cb.message.edit_text(text, reply_markup=kb)


async def _render_list(cb: CallbackQuery):
    plans = await list_plans(active_only=False)
    if not plans:
        await cb.message.edit_text(
            t("ap.list_none", user_id=cb.from_user.id),
            reply_markup=_home_kb(cb.from_user.id, has_plans=False),
        )
        return
    await cb.message.edit_text(
        t("ap.list_title", user_id=cb.from_user.id),
        reply_markup=_plans_list_kb(cb.from_user.id, plans),
    )


async def _render_plan(cb_or_msg, code: str):
    p = await get_plan_by_code(code)
    user_id = cb_or_msg.from_user.id if cb_or_msg.from_user else None
    if not p:
        # cb_or_msg can be Message or CallbackQuery
        if isinstance(cb_or_msg, CallbackQuery):
            await cb_or_msg.answer(t("ap.plan_not_found", user_id=user_id), show_alert=True)
        else:
            await cb_or_msg.answer(t("ap.plan_not_found", user_id=user_id))
        return

    price = int(p.get("price_stars") or 0)
    active = bool(p.get("is_active"))
    title = p.get("title") or code
    desc = p.get("description") or ""
    dur = int(p.get("duration_days") or 0)

    text = t(
        "ap.view_block",
        user_id=user_id,
        title=title,
        code=code,
        desc=desc,
        days=dur,
        price=price,
        status=(
            t("ap.view_status_active", user_id=user_id)
            if active
            else t("ap.view_status_inactive", user_id=user_id)
        ),
    )

    kb = _plan_view_kb(user_id, code, price, active)
    if isinstance(cb_or_msg, CallbackQuery):
        await cb_or_msg.message.edit_text(text, reply_markup=kb)
    else:
        await cb_or_msg.answer(text, reply_markup=kb)


# ----------------- handlers -----------------

async def admin_plans_cmd(msg: Message):
    if not _is_owner(msg.from_user.id if msg.from_user else None):
        await msg.answer(t("ap.not_allowed", user_id=(msg.from_user.id if msg.from_user else None)))
        return
    await _render_home(msg)


async def ap_home(cb: CallbackQuery):
    if not _is_owner(cb.from_user.id if cb.from_user else None):
        try:
            await cb.answer(t("ap.not_allowed", user_id=cb.from_user.id), show_alert=True)
        except TelegramBadRequest:
            pass
        return
    try:
        await cb.answer()
    except TelegramBadRequest:
        pass
    await _render_home(cb)


async def ap_list(cb: CallbackQuery):
    if not _is_owner(cb.from_user.id if cb.from_user else None):
        try:
            await cb.answer(t("ap.not_allowed", user_id=cb.from_user.id), show_alert=True)
        except TelegramBadRequest:
            pass
        return
    try:
        await cb.answer()
    except TelegramBadRequest:
        pass
    await _render_list(cb)


async def ap_create_defaults(cb: CallbackQuery):
    if not _is_owner(cb.from_user.id if cb.from_user else None):
        try:
            await cb.answer(t("ap.not_allowed", user_id=cb.from_user.id), show_alert=True)
        except TelegramBadRequest:
            pass
        return
    try:
        await cb.answer()
    except TelegramBadRequest:
        pass

    await _ensure_defaults()
    try:
        await cb.message.answer(t("ap.defaults_done", user_id=cb.from_user.id))
    except Exception:
        pass

    await _render_list(cb)


async def ap_view(cb: CallbackQuery):
    if not _is_owner(cb.from_user.id if cb.from_user else None):
        try:
            await cb.answer(t("ap.not_allowed", user_id=cb.from_user.id), show_alert=True)
        except TelegramBadRequest:
            pass
        return
    try:
        await cb.answer()
    except TelegramBadRequest:
        pass

    parts = (cb.data or "").split(":", 2)
    code = parts[2] if len(parts) == 3 else ""
    await _render_plan(cb, code)


async def ap_toggle(cb: CallbackQuery):
    if not _is_owner(cb.from_user.id if cb.from_user else None):
        try:
            await cb.answer(t("ap.not_allowed", user_id=cb.from_user.id), show_alert=True)
        except TelegramBadRequest:
            pass
        return
    try:
        await cb.answer()
    except TelegramBadRequest:
        pass

    parts = (cb.data or "").split(":", 2)
    code = parts[2] if len(parts) == 3 else ""
    p = await get_plan_by_code(code)
    if not p:
        await cb.message.answer(t("ap.plan_not_found", user_id=cb.from_user.id))
        return

    await toggle_plan_active(code, not bool(p.get("is_active")))
    await _render_plan(cb, code)


async def ap_delta(cb: CallbackQuery):
    if not _is_owner(cb.from_user.id if cb.from_user else None):
        try:
            await cb.answer(t("ap.not_allowed", user_id=cb.from_user.id), show_alert=True)
        except TelegramBadRequest:
            pass
        return
    try:
        await cb.answer()
    except TelegramBadRequest:
        pass

    parts = (cb.data or "").split(":", 3)
    if len(parts) != 4:
        return

    _, _, code, delta_str = parts
    try:
        delta = int(delta_str)
    except Exception:
        await cb.message.answer(t("ap.invalid_delta", user_id=cb.from_user.id))
        return

    p = await get_plan_by_code(code)
    if not p:
        await cb.message.answer(t("ap.plan_not_found", user_id=cb.from_user.id))
        return

    old_price = int(p.get("price_stars") or 0)
    updated = await set_plan_price(code, old_price + delta)
    if not updated:
        await cb.message.answer(t("ap.plan_not_found", user_id=cb.from_user.id))
        return

    await _render_plan(cb, code)


async def ap_set_custom(cb: CallbackQuery, state: FSMContext):
    if not _is_owner(cb.from_user.id if cb.from_user else None):
        try:
            await cb.answer(t("ap.not_allowed", user_id=cb.from_user.id), show_alert=True)
        except TelegramBadRequest:
            pass
        return
    try:
        await cb.answer()
    except TelegramBadRequest:
        pass

    parts = (cb.data or "").split(":", 2)
    code = parts[2] if len(parts) == 3 else ""

    await state.set_state(CustomPriceStates.waiting_price)
    await state.update_data(plan_code=code)
    await cb.message.answer(t("ap.send_custom_prompt", user_id=cb.from_user.id, code=code))


async def ap_receive_custom_price(msg: Message, state: FSMContext):
    if not _is_owner(msg.from_user.id if msg.from_user else None):
        await state.clear()
        await msg.answer(t("ap.not_allowed", user_id=(msg.from_user.id if msg.from_user else None)))
        return

    data = await state.get_data()
    code = data.get("plan_code")
    if not code:
        await state.clear()
        await msg.answer("/admin_plans")
        return

    try:
        stars = int((msg.text or "").strip())
    except Exception:
        await msg.answer(t("ap.send_custom_invalid", user_id=msg.from_user.id))
        return

    updated = await set_plan_price(code, stars)
    await state.clear()

    if not updated:
        await msg.answer(t("ap.plan_not_found", user_id=msg.from_user.id))
        return

    # Optional short confirmation
    try:
        await msg.answer(
            t(
                "ap.custom_set_ok",
                user_id=msg.from_user.id,
                code=code,
                price=updated["price_stars"],
            )
        )
    except Exception:
        pass

    # Render updated plan card
    await _render_plan(msg, code)


# ---- helper called from admin_panel.py ----

async def _render_admin_plans(cb_or_msg):
    await _render_home(cb_or_msg)


# ----------------- register -----------------

def register(dp):
    # Commands
    dp.message.register(admin_plans_cmd, Command("admin_plans", "admin_plan"))

    # Callback queries
    dp.callback_query.register(ap_home, F.data == "ap:home")
    dp.callback_query.register(ap_list, F.data == "ap:list")
    dp.callback_query.register(ap_create_defaults, F.data == "ap:create_defaults")
    dp.callback_query.register(ap_view, F.data.startswith("ap:view:"))
    dp.callback_query.register(ap_toggle, F.data.startswith("ap:toggle:"))
    dp.callback_query.register(ap_delta, F.data.startswith("ap:delta:"))
    dp.callback_query.register(ap_set_custom, F.data.startswith("ap:set_custom:"))

    # FSM message handler for custom price
    dp.message.register(ap_receive_custom_price, CustomPriceStates.waiting_price)
