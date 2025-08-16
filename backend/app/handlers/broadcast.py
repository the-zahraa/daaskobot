# backend/app/handlers/broadcast.py
import os
import asyncio
from typing import cast, List

from aiogram import F, Bot
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.exceptions import TelegramForbiddenError, TelegramBadRequest, TelegramConflictError

from ..repositories.users import list_all_user_ids

_owner_env = os.getenv("OWNER_ID", "").strip()
try:
    OWNER_ID = int(_owner_env) if _owner_env else None
except ValueError:
    OWNER_ID = None

class BroadcastStates(StatesGroup):
    waiting_text = State()

def broadcast_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Compose broadcast", callback_data="admin_broadcast_compose")],
        [InlineKeyboardButton(text="⬅️ Back", callback_data="admin_overview")],
    ])

def _authorized(user_id: int | None) -> bool:
    return OWNER_ID is not None and user_id == OWNER_ID

async def _send_in_chunks(bot: Bot, user_ids: List[int], text: str) -> tuple[int, int]:
    sent = 0
    failed = 0
    CHUNK = 30
    PAUSE = 1.2
    for i in range(0, len(user_ids), CHUNK):
        batch = user_ids[i:i+CHUNK]
        tasks = [bot.send_message(chat_id=uid, text=text, disable_web_page_preview=True) for uid in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, Exception):
                failed += 1
            else:
                sent += 1
        await asyncio.sleep(PAUSE)
    return sent, failed

def register(dp):
    async def admin_broadcast(cb: CallbackQuery, state: FSMContext):
        if not _authorized(cb.from_user.id if cb.from_user else None):
            await cb.answer(); return
        await state.clear()
        await cb.message.answer(
            "💬 <b>Broadcast</b>\nTap <i>Compose broadcast</i>, then send the message text.\nUse /cancel to abort.",
            reply_markup=broadcast_kb()
        )
        await cb.answer()

    async def admin_broadcast_compose(cb: CallbackQuery, state: FSMContext):
        if not _authorized(cb.from_user.id if cb.from_user else None):
            await cb.answer(); return
        await state.set_state(BroadcastStates.waiting_text)
        await cb.message.answer("✍️ Send the message to broadcast, or /cancel.")
        await cb.answer()

    async def cancel_cmd(msg: Message, state: FSMContext):
        if _authorized(msg.from_user.id if msg.from_user else None):
            await state.clear()
            await msg.answer("❌ Broadcast cancelled.")

    async def received_broadcast_text(msg: Message, state: FSMContext):
        if not _authorized(msg.from_user.id if msg.from_user else None):
            return
        text = msg.text or msg.caption
        if not text:
            await msg.answer("Please send text.")
            return
        await state.clear()
        await msg.answer("📤 Sending…")
        bot = cast(Bot, msg.bot)
        user_ids = await list_all_user_ids()
        # Exclude owner and the current sender (usually same)
        excluded = {uid for uid in [OWNER_ID, msg.from_user.id] if uid}
        user_ids = [u for u in user_ids if u not in excluded]
        sent, failed = await _send_in_chunks(bot, user_ids, text)
        await msg.answer(f"✅ Done. Sent: {sent}, Failed: {failed}")

    dp.callback_query.register(admin_broadcast, F.data == "admin_broadcast")
    dp.callback_query.register(admin_broadcast_compose, F.data == "admin_broadcast_compose")
    dp.message.register(cancel_cmd, F.text == "/cancel")
    dp.message.register(received_broadcast_text, BroadcastStates.waiting_text)
