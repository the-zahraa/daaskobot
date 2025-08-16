# backend/app/handlers/tenant.py
from typing import cast, List, Tuple
from aiogram import F, Bot
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, Message

from app.repositories.tenants import get_user_tenant
from app.repositories.subscriptions import get_user_subscription_status
from app.repositories.chats import list_tenant_chats

def tenant_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 Dashboard Overview", callback_data="tenant_overview")],
        [InlineKeyboardButton(text="🔗 Linked Chats", callback_data="tenant_chats")],
        [InlineKeyboardButton(text="📈 Analytics", callback_data="tenant_analytics")],
        [InlineKeyboardButton(text="🛠 Settings", callback_data="tenant_settings")],
        [InlineKeyboardButton(text="⬅️ Back", callback_data="back_home")],
    ])

def register(dp):
    async def ten_overview(cb: CallbackQuery):
        tg_id = cb.from_user.id if cb.from_user else 0
        tenant_id = await get_user_tenant(tg_id)
        status = await get_user_subscription_status(tg_id)

        lines = [
            "🏠 <b>Dashboard</b>",
            f"• Subscription: <b>{status}</b>",
        ]
        text = "\n".join(lines)

        try:
            if cb.message:
                await cb.message.edit_text(text, reply_markup=tenant_menu_kb())
            else:
                bot = cast(Bot, cb.bot)
                await bot.send_message(tg_id, text, reply_markup=tenant_menu_kb())
        except Exception:
            # Fallback to sending a new message if edit fails
            bot = cast(Bot, cb.bot)
            await bot.send_message(tg_id, text, reply_markup=tenant_menu_kb())
        await cb.answer()

    async def ten_chats(cb: CallbackQuery):
        tg_id = cb.from_user.id if cb.from_user else 0
        tenant_id = await get_user_tenant(tg_id)
        if not tenant_id:
            await cb.answer("No tenant found. Start in DM first.", show_alert=True)
            return

        chats: List[Tuple[int, str, str]] = await list_tenant_chats(tenant_id)
        lines = ["🔗 <b>Linked Chats</b>", "", "• chat_id — title — chat_type"]
        for cid, ctype, title in chats:
            lines.append(f"• <code>{cid}</code> — {title} — <i>{ctype}</i>")
        if len(chats) == 0:
            lines.append("• (none yet)")

        text = "\n".join(lines)
        try:
            if cb.message:
                await cb.message.edit_text(text, reply_markup=tenant_menu_kb())
            else:
                bot = cast(Bot, cb.bot)
                await bot.send_message(tg_id, text, reply_markup=tenant_menu_kb())
        except Exception:
            bot = cast(Bot, cb.bot)
            await bot.send_message(tg_id, text, reply_markup=tenant_menu_kb())
        await cb.answer()

    async def ten_analytics(cb: CallbackQuery):
        tg_id = cb.from_user.id if cb.from_user else 0
        text = "📈 <b>Analytics</b>\nComing soon."
        try:
            if cb.message:
                await cb.message.edit_text(text, reply_markup=tenant_menu_kb())
            else:
                bot = cast(Bot, cb.bot)
                await bot.send_message(tg_id, text, reply_markup=tenant_menu_kb())
        except Exception:
            bot = cast(Bot, cb.bot)
            await bot.send_message(tg_id, text, reply_markup=tenant_menu_kb())
        await cb.answer()

    async def ten_settings(cb: CallbackQuery):
        tg_id = cb.from_user.id if cb.from_user else 0
        text = "🛠 <b>Settings</b>\nComing soon."
        try:
            if cb.message:
                await cb.message.edit_text(text, reply_markup=tenant_menu_kb())
            else:
                bot = cast(Bot, cb.bot)
                await bot.send_message(tg_id, text, reply_markup=tenant_menu_kb())
        except Exception:
            bot = cast(Bot, cb.bot)
            await bot.send_message(tg_id, text, reply_markup=tenant_menu_kb())
        await cb.answer()

    dp.callback_query.register(ten_overview, F.data == "tenant_overview")
    dp.callback_query.register(ten_chats,    F.data == "tenant_chats")
    dp.callback_query.register(ten_analytics, F.data == "tenant_analytics")
    dp.callback_query.register(ten_settings,  F.data == "tenant_settings")
