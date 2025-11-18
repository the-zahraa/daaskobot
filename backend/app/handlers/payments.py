from __future__ import annotations

import html
import logging
from datetime import datetime, timezone
from typing import Optional, List, Dict

from aiogram import Router, F, Bot
from aiogram.filters import Command
from aiogram.types import (
    Message,
    LabeledPrice,
    PreCheckoutQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
)
from aiogram.exceptions import TelegramBadRequest  # aiogram v3

from app.db import get_con
from app.repositories.plans import list_active_plans, get_plan_by_code
from app.repositories.subscriptions import (
    get_user_subscription_status,
    get_user_subscription_expiry,
    upsert_subscription_on_payment,
)
from app.services.i18n import t  # i18n

log = logging.getLogger("handlers.payments")
router = Router()
UTC = timezone.utc


# ---------------- UI helpers ----------------

def _plans_kb(user_id: int, plans: List[Dict]) -> InlineKeyboardMarkup:
    rows = []
    for p in plans:
        code = str(p["code"])
        title = str(p["title"])
        price = int(p["price_stars"])
        dur = int(p["duration_days"])
        label = (
            t("pay.buy_btn_free", user_id=user_id, title=title, days=dur)
            if price == 0 else
            t("pay.buy_btn", user_id=user_id, title=title, price=price, days=dur)
        )
        rows.append([
            InlineKeyboardButton(
                text=label,
                callback_data=f"pro_buy:{code}",
            )
        ])
    # Back button to dashboard
    rows.append([InlineKeyboardButton(text=t("pay.back_btn", user_id=user_id), callback_data="tenant_overview")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _back_to_dashboard_kb(user_id: int) -> InlineKeyboardMarkup:
    """
    Minimal keyboard to let the user return to the main dashboard.
    """
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=t("pay.back_btn", user_id=user_id),
                    callback_data="tenant_overview",
                )
            ]
        ]
    )


def _status_text(user_id: int, plan_status: str, expiry: Optional[datetime]) -> str:
    if str(plan_status).lower() == "pro":
        if expiry:
            exp_txt = expiry.astimezone(UTC).strftime("%Y-%m-%d %H:%M UTC")
            return t("pay.status_pro_with_exp", user_id=user_id, expiry=exp_txt)
        return t("pay.status_pro_no_exp", user_id=user_id)
    return t("pay.status_free_head", user_id=user_id)


async def _safe_answer(msg: Message, text: str, kb: Optional[InlineKeyboardMarkup] = None) -> None:
    try:
        await msg.answer(text, reply_markup=kb)
    except TelegramBadRequest:
        try:
            await msg.reply(text, reply_markup=kb)
        except Exception:
            pass


# ---------------- Handlers ----------------

@router.message(Command("pro"))
async def cmd_pro(msg: Message) -> None:
    log.info("Received /pro from %s", msg.from_user and msg.from_user.id)
    try:
        uid = msg.from_user.id if msg.from_user else 0
        status = await get_user_subscription_status(uid)
        expiry = await get_user_subscription_expiry(uid)

        plans = await list_active_plans()
        if not plans:
            await _safe_answer(msg, t("pay.no_plans", user_id=uid))
            return

        await _safe_answer(msg, _status_text(uid, status, expiry), _plans_kb(uid, plans))
    except Exception as e:
        log.exception("cmd_pro failed: %s", e)
        await _safe_answer(msg, t("pay.load_error", user_id=(msg.from_user.id if msg.from_user else None), err=html.escape(str(e))))


@router.callback_query(F.data == "pro_open")
async def on_tenant_pro(cb: CallbackQuery) -> None:
    """
    Handler for the “⭐ Upgrade to Pro” button on the dashboard.
    Reuses the /pro flow so behavior stays consistent.
    """
    if not cb.from_user:
        return await cb.answer()

    # ACK callback
    try:
        await cb.answer()
    except TelegramBadRequest:
        pass

    uid = cb.from_user.id

    try:
        status = await get_user_subscription_status(uid)
        expiry = await get_user_subscription_expiry(uid)
        plans = await list_active_plans()

        if not plans:
            await cb.message.edit_text(t("pay.no_plans", user_id=uid))
            return

        text = _status_text(uid, status, expiry)
        await cb.message.edit_text(text, reply_markup=_plans_kb(uid, plans))
    except Exception as e:
        log.exception("on_tenant_pro failed: %s", e)
        await cb.message.answer(
            t(
                "pay.load_error",
                user_id=uid,
                err=html.escape(str(e)),
            )
        )


@router.callback_query(F.data.startswith("pro_buy:"))
async def on_buy(cb: CallbackQuery) -> None:
    if not cb.from_user:
        return await cb.answer()

    # ACK immediately to avoid “query is too old”
    try:
        await cb.answer()
    except TelegramBadRequest:
        pass

    try:
        code = (cb.data or "").split(":", 1)[1]
        plan = await get_plan_by_code(code)
        if not plan or not plan.get("is_active", True):
            await cb.message.answer(t("pay.plan_unavailable", user_id=cb.from_user.id))
            return

        title = str(plan["title"])
        description = str(plan["description"])
        price_stars = int(plan["price_stars"])
        duration_days = int(plan["duration_days"])

        # If plan is FREE (0⭐) → activate immediately (no invoice)
        if price_stars == 0:
            uid = cb.from_user.id

            # Prevent stacking the free plan by spamming the button:
            # if the user already has an active Pro subscription, don't extend it.
            current_exp = await get_user_subscription_expiry(uid)
            if current_exp and current_exp > datetime.now(UTC):
                back_kb = _back_to_dashboard_kb(uid)
                exp_txt = current_exp.astimezone(UTC).strftime("%Y-%m-%d %H:%M UTC")
                await cb.message.answer(
                    t("pay.already_pro_no_extend", user_id=uid, expiry=exp_txt),
                    reply_markup=back_kb,
                )
                return

            await upsert_subscription_on_payment(
                uid,
                code,
                duration_days,
                0,              # amount_stars
                None,           # provider_payment_charge_id
                None,           # telegram_payment_charge_id
            )
            expiry = await get_user_subscription_expiry(uid)
            back_kb = _back_to_dashboard_kb(uid)

            if expiry:
                exp_txt = expiry.astimezone(UTC).strftime("%Y-%m-%d %H:%M UTC")
                await cb.message.answer(
                    t("pay.activated_free_ok", user_id=uid, expiry=exp_txt),
                    reply_markup=back_kb,
                )
            else:
                await cb.message.answer(
                    t("pay.activated_free_ok_noexp", user_id=uid),
                    reply_markup=back_kb,
                )
            return

        # Stars payment flow
        prices = [LabeledPrice(label=title, amount=price_stars)]
        payload = f"plan:{code}"

        await cb.message.answer_invoice(
            title=title,
            description=description,
            currency="XTR",
            prices=prices,
            payload=payload,
            provider_token="",  # <-- REQUIRED for Stars
        )
    except Exception as e:
        log.exception("on_buy failed: %s", e)
        await cb.message.answer(t("pay.invoice_fail", user_id=cb.from_user.id, err=html.escape(str(e))))


@router.pre_checkout_query()
async def on_precheckout(pcq: PreCheckoutQuery, bot: Bot) -> None:
    try:
        await bot.answer_pre_checkout_query(pre_checkout_query_id=pcq.id, ok=True)
    except Exception as e:
        log.exception("on_precheckout failed: %s", e)


@router.message(F.successful_payment)
async def on_successful_payment(msg: Message) -> None:
    sp = msg.successful_payment
    if not (msg.from_user and sp):
        return

    try:
        uid = msg.from_user.id
        payload = sp.invoice_payload or ""
        plan_code = payload.split(":", 1)[1] if ":" in payload else None
        if not plan_code:
            return await _safe_answer(msg, t("pay.paid_unknown_plan", user_id=uid))

        plan = await get_plan_by_code(plan_code)
        if not plan:
            return await _safe_answer(msg, t("pay.paid_plan_missing", user_id=uid))

        amount_stars = int(sp.total_amount or 0)
        duration_days = int(plan["duration_days"])
        provider_payment_charge_id = getattr(sp, "provider_payment_charge_id", None)
        telegram_payment_charge_id = getattr(sp, "telegram_payment_charge_id", None)

        await upsert_subscription_on_payment(
            uid,
            plan_code,
            duration_days,
            amount_stars,
            provider_payment_charge_id,
            telegram_payment_charge_id,
        )

        expiry = await get_user_subscription_expiry(uid)
        back_kb = _back_to_dashboard_kb(uid)

        if expiry:
            exp_txt = expiry.astimezone(UTC).strftime("%Y-%m-%d %H:%M UTC")
            await _safe_answer(
                msg,
                t("pay.paid_ok_with_exp", user_id=uid, expiry=exp_txt),
                back_kb,
            )
        else:
            await _safe_answer(
                msg,
                t("pay.paid_ok", user_id=uid),
                back_kb,
            )
    except Exception as e:
        log.exception("successful_payment failed: %s", e)
        await _safe_answer(msg, t("pay.paid_update_fail", user_id=(msg.from_user.id if msg.from_user else None), err=html.escape(str(e))))
