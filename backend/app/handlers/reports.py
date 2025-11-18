# backend/app/handlers/reports.py
from __future__ import annotations
from typing import Optional, cast

from aiogram import Router, F, Bot
from aiogram.types import CallbackQuery, BufferedInputFile, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.exceptions import TelegramBadRequest

from app.db import get_con
from app.repositories.subscriptions import get_user_subscription_status
from app.services.i18n import t
from app.services.reports import build_report_pdf_bytes

router = Router()


async def _user_owns_chat(user_tg_id: int, chat_id: int) -> bool:
    async with get_con() as con:
        row = await con.fetchrow(
            """
            SELECT 1
            FROM public.chats c
            JOIN public.user_tenants ut ON ut.tenant_id = c.tenant_id
            WHERE ut.tg_id = $1 AND c.tg_chat_id = $2
            LIMIT 1
            """,
            user_tg_id, chat_id
        )
    return bool(row)

async def _get_chat_title(chat_id: int) -> Optional[str]:
    async with get_con() as con:
        row = await con.fetchrow("SELECT title FROM public.chats WHERE tg_chat_id = $1 LIMIT 1", chat_id)
    return (row and row["title"]) or None


def _back_kb(user_id: int) -> InlineKeyboardMarkup:
    """
    Simple 'Back' button that returns to the reports chat-list screen.
    """
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t("reports.buttons.back", user_id=user_id), callback_data="tenant_reports")]
    ])


@router.callback_query(F.data.startswith("rep:chat:"))
async def on_generate_report(cb: CallbackQuery):
    if not cb.from_user:
        return await cb.answer()

    parts = (cb.data or "").split(":")
    try:
        chat_id = int(parts[2])
        days = int(parts[3]) if len(parts) >= 4 else 30
    except Exception:
        try:
            await cb.answer(
                t("reports.ui.invalid_request", user_id=(cb.from_user and cb.from_user.id)),
                show_alert=True
            )
        except TelegramBadRequest:
            pass
        return

    uid = cb.from_user.id
    kb = _back_kb(uid)

    # Ownership check
    if not await _user_owns_chat(uid, chat_id):
        try:
            await cb.answer(t("reports.ui.not_allowed", user_id=uid), show_alert=True)
        except TelegramBadRequest:
            pass
        return

    # Pro check (service double-enforces)
    plan = await get_user_subscription_status(uid)
    if str(plan).lower() != "pro":
        try:
            await cb.answer(t("reports.errors.pro_only", user_id=uid), show_alert=True)
        except TelegramBadRequest:
            pass
        return

    title = await _get_chat_title(chat_id)

    # Show "generating..." + Back
    try:
        if cb.message:
            await cb.message.edit_text(
                t("reports.ui.generating", user_id=uid),
                reply_markup=kb
            )
    except Exception:
        pass

    # Build the PDF
    try:
        pdf_bytes, filename = await build_report_pdf_bytes(
            chat_id=chat_id,
            chat_title=title,
            days=days,
            is_pro=True,            # checked above; service enforces again
            user_id=uid,
            lang=None,              # service can resolve user language if configured
            tz="Europe/Helsinki",
        )
    except PermissionError as e:
        # Permission in service layer (e.g. not allowed)
        try:
            if cb.message:
                await cb.message.edit_text(str(e), reply_markup=kb)
        except Exception:
            pass
        return
    except Exception as e:
        # Generic failure
        try:
            if cb.message:
                await cb.message.edit_text(
                    t("reports.ui.failed", user_id=uid, err=str(e)),
                    reply_markup=kb
                )
        except Exception:
            pass
        return

    # Send the file
    try:
        file = BufferedInputFile(pdf_bytes, filename=filename)
        caption = t(
            "reports.ui.sent_caption",
            user_id=uid,
            title=(title or str(chat_id)),
            days=days
        )
        bot = cast(Bot, cb.bot)
        await bot.send_document(cb.message.chat.id, document=file, caption=caption)
        try:
            if cb.message:
                await cb.message.edit_text(t("reports.ui.done", user_id=uid), reply_markup=kb)
        except Exception:
            pass
        try:
            await cb.answer(t("reports.ui.done", user_id=uid))
        except TelegramBadRequest:
            pass
    except Exception:
        try:
            if cb.message:
                await cb.message.edit_text(t("reports.ui.sent_no_file", user_id=uid), reply_markup=kb)
        except Exception:
            pass
