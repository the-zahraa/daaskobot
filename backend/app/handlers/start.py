# backend/app/handlers/start.py
from __future__ import annotations
import os
from typing import Optional, cast, List, Tuple

from aiogram import F, Bot
from aiogram.filters import Command
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
)

from ..repositories.users import upsert_user, has_phone
from ..repositories.tenants import ensure_personal_tenant, link_user_to_tenant, get_user_tenant
from ..repositories.subscriptions import get_user_subscription_status
from ..repositories.chats import list_tenant_chats
from ..repositories.stats import get_last_days
from ..repositories.required import list_required_targets

OWNER_ID: Optional[int] = None
_owner_env = os.getenv("OWNER_ID", "").strip()
try:
    OWNER_ID = int(_owner_env) if _owner_env else None
except ValueError:
    OWNER_ID = None

# ---------------- UI ----------------

def owner_home_kb() -> InlineKeyboardMarkup:
    # Owner can access both Admin Panel and their own customer dashboard
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👑 Open Admin Panel", callback_data="admin_overview")],
        [InlineKeyboardButton(text="🧭 Open My Dashboard", callback_data="owner_dashboard")],
    ])

def user_dashboard_kb() -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(text="🏠 Overview", callback_data="tenant_overview"),
            InlineKeyboardButton(text="🔗 Linked Chats", callback_data="tenant_chats"),
        ],
        [
            InlineKeyboardButton(text="📈 Analytics", callback_data="tenant_analytics"),
            InlineKeyboardButton(text="🧾 Reports", callback_data="tenant_reports"),
        ],
        [
            InlineKeyboardButton(text="📎 Group tools", callback_data="group_tools"),
            InlineKeyboardButton(text="❓ Help", callback_data="help"),
        ],
        [
            InlineKeyboardButton(text="⚙️ Settings", callback_data="tenant_settings"),
        ],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

def request_phone_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📱 Share phone number", request_contact=True)]],
        resize_keyboard=True,
        one_time_keyboard=True,
        selective=True,
    )

def force_join_kb(targets: List[str]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for t in targets:
        url = f"https://t.me/{t[1:]}" if t.startswith("@") else "https://t.me/"
        rows.append([InlineKeyboardButton(text=f"🔗 Open {t}", url=url)])
    rows.insert(0, [InlineKeyboardButton(text="✅ I joined", callback_data="force_check_global")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

# ---------------- Helpers ----------------

def _is_owner(uid: Optional[int]) -> bool:
    return OWNER_ID is not None and uid == OWNER_ID

async def _is_member(bot: Bot, target: str, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(target, user_id)
        return member.status in ("creator", "administrator", "member", "restricted")
    except Exception:
        return False

async def _enforce_global_requirements(bot: Bot, user_id: int) -> bool:
    """Return True if user is member of ALL required targets; else send gate and return False."""
    targets = await list_required_targets()
    if not targets:
        return True
    for t in targets:
        ok = await _is_member(bot, t, user_id)
        if not ok:
            await bot.send_message(
                user_id,
                "🔒 Please join the required channels/groups to continue.",
                reply_markup=force_join_kb(targets),
            )
            return False
    return True

async def _ensure_user_and_tenant(msg: Message) -> str:
    u = msg.from_user
    # IMPORTANT: pass phone_e164=None here; upsert_user now COALESCEs, so it will NOT erase stored phone
    await upsert_user(
        tg_id=u.id,
        first_name=u.first_name,
        last_name=u.last_name,
        username=u.username,
        language_code=u.language_code,
        phone_e164=None,   # keep existing phone if present
        region=None,
        is_premium=bool(getattr(u, "is_premium", False)),
    )
    display_name = f"{(u.first_name or '').strip()} {(u.last_name or '').strip()}".strip() or (u.username or f"User {u.id}")
    tenant_id = await ensure_personal_tenant(u.id, display_name)
    await link_user_to_tenant(u.id, tenant_id)
    return tenant_id

async def _render_dashboard(bot: Bot, chat_id: int, tg_id: int):
    plan = await get_user_subscription_status(tg_id)
    text = (
        "🧭 <b>Dashboard</b>\n"
        f"• Your plan: <b>{plan}</b>\n"
        "\n"
        "Use the menu below to navigate."
    )
    await bot.send_message(chat_id, text, reply_markup=user_dashboard_kb())

def _analytics_list_kb(chats: List[Tuple[int, str, str]]) -> InlineKeyboardMarkup:
    rows = []
    for cid, ctype, title in chats[:30]:
        rows.append([InlineKeyboardButton(text=f"{title or cid} ({ctype})", callback_data=f"tenant_analytics_view:{cid}")])
    rows.append([InlineKeyboardButton(text="⬅️ Back", callback_data="tenant_overview")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

# ---------------- Register ----------------

def register(dp):
    async def start_cmd(msg: Message):
        bot = cast(Bot, msg.bot)
        if not msg.from_user:
            return

        if _is_owner(msg.from_user.id):
            # Owner sees Admin & My Dashboard entry point (no phone / force-join gate)
            await bot.send_message(msg.chat.id, "Welcome, owner.\n\nChoose an option:", reply_markup=owner_home_kb())
            return

        # ONE-TIME phone gate (thanks to upsert_user COALESCE, phone stays saved)
        if not await has_phone(msg.from_user.id):
            await bot.send_message(msg.chat.id, "Before we continue, please share your phone number.", reply_markup=request_phone_kb())
            return

        await _ensure_user_and_tenant(msg)

        # GLOBAL required membership (owner is exempt above)
        if not await _enforce_global_requirements(bot, msg.from_user.id):
            return

        await _render_dashboard(bot, msg.chat.id, msg.from_user.id)

    async def owner_dashboard(cb: CallbackQuery):
        # Owner can open their own dashboard without phone/force-join gates
        if not cb.from_user or not _is_owner(cb.from_user.id):
            await cb.answer(); return
        await _ensure_user_and_tenant(cb.message)  # create/link tenant if needed
        await _render_dashboard(cast(Bot, cb.bot), cb.message.chat.id, cb.from_user.id)
        await cb.answer()

    async def force_check_global(cb: CallbackQuery):
        if not cb.from_user: await cb.answer(); return
        bot = cast(Bot, cb.bot)
        if _is_owner(cb.from_user.id):
            # Owner: just open dashboard
            try:
                if cb.message:
                    await cb.message.edit_text("Thanks! You’re verified.", reply_markup=None)
            except Exception:
                pass
            await _render_dashboard(bot, cb.message.chat.id, cb.from_user.id)
            await cb.answer(); return

        ok = await _enforce_global_requirements(bot, cb.from_user.id)
        if not ok:
            await cb.answer("Still not joined. Please join and try again.", show_alert=True)
            return
        # Verified → show dashboard immediately (no /start needed)
        try:
            if cb.message:
                await cb.message.edit_text("Thanks! You’re verified.", reply_markup=None)
        except Exception:
            pass
        await _render_dashboard(bot, cb.message.chat.id, cb.from_user.id)
        await cb.answer()

    async def contact_shared(msg: Message):
        bot = cast(Bot, msg.bot)
        if not msg.from_user or not msg.contact:
            return
        # Save phone ONCE; subsequent upserts will COALESCE and keep it
        raw = msg.contact.phone_number or ""
        phone_e164 = raw if raw.startswith("+") else f"+{raw}" if raw else None
        u = msg.from_user
        await upsert_user(
            tg_id=u.id,
            first_name=u.first_name,
            last_name=u.last_name,
            username=u.username,
            language_code=u.language_code,
            phone_e164=phone_e164,
            region=None,
            is_premium=bool(getattr(u, "is_premium", False)),
        )
        await _ensure_user_and_tenant(msg)
        # Global gate (owner never reaches here anyway)
        if not await _enforce_global_requirements(bot, u.id):
            return
        await bot.send_message(msg.chat.id, "Thanks! You’re in ✅", reply_markup=ReplyKeyboardRemove())
        await _render_dashboard(bot, msg.chat.id, u.id)

    # ---------- Dashboard callbacks ----------

    async def tenant_overview(cb: CallbackQuery):
        if not cb.from_user: await cb.answer(); return
        if not _is_owner(cb.from_user.id):
            if not await _enforce_global_requirements(cast(Bot, cb.bot), cb.from_user.id): return
        plan = await get_user_subscription_status(cb.from_user.id)
        text = "🏠 <b>Overview</b>\n" f"• Current plan: <b>{plan}</b>\n"
        try:
            if cb.message and cb.message.text != text: await cb.message.edit_text(text, reply_markup=user_dashboard_kb())
            else: await cb.message.answer(text, reply_markup=user_dashboard_kb())
        except Exception:
            await cb.message.answer(text, reply_markup=user_dashboard_kb())
        await cb.answer()

    async def tenant_chats_cb(cb: CallbackQuery):
        if not cb.from_user: await cb.answer(); return
        if not _is_owner(cb.from_user.id):
            if not await _enforce_global_requirements(cast(Bot, cb.bot), cb.from_user.id): return
        tenant_id = await get_user_tenant(cb.from_user.id)
        if not tenant_id: await cb.answer("No tenant found.", show_alert=True); return
        chats = await list_tenant_chats(tenant_id)
        if not chats:
            text = "🔗 <b>Linked Chats</b>\nNo chats yet.\n\nTip: add the bot as admin in a group/channel, then use /link (group/channel) or /link_channel in DM."
        else:
            lines = ["🔗 <b>Linked Chats</b>"]
            for cid, ctype, title in chats[:30]:
                lines.append(f"• <code>{cid}</code> — {ctype} — {title}")
            if len(chats) > 30:
                lines.append(f"… and {len(chats)-30} more")
            text = "\n".join(lines)
        try:
            if cb.message and cb.message.text != text: await cb.message.edit_text(text, reply_markup=user_dashboard_kb())
            else: await cb.message.answer(text, reply_markup=user_dashboard_kb())
        except Exception:
            await cb.message.answer(text, reply_markup=user_dashboard_kb())
        await cb.answer()

    async def tenant_analytics_cb(cb: CallbackQuery):
        if not cb.from_user: await cb.answer(); return
        if not _is_owner(cb.from_user.id):
            if not await _enforce_global_requirements(cast(Bot, cb.bot), cb.from_user.id): return
        tenant_id = await get_user_tenant(cb.from_user.id)
        if not tenant_id: await cb.answer("No tenant found.", show_alert=True); return
        chats = await list_tenant_chats(tenant_id)
        if not chats:
            await cb.message.edit_text("📈 Analytics\nNo linked chats yet.", reply_markup=user_dashboard_kb()); await cb.answer(); return
        await cb.message.edit_text("Select a chat to view analytics:", reply_markup=_analytics_list_kb(chats))
        await cb.answer()

    async def tenant_analytics_view(cb: CallbackQuery):
        if not cb.from_user: await cb.answer(); return
        if not _is_owner(cb.from_user.id):
            if not await _enforce_global_requirements(cast(Bot, cb.bot), cb.from_user.id): return
        parts = (cb.data or "").split(":")
        chat_id = int(parts[1]) if len(parts) >= 2 else 0
        rows = await get_last_days(chat_id, 30)
        if not rows:
            text = "📈 Analytics\nNo data for this chat yet."
        else:
            total_joins = sum(j for _, j, _ in rows)
            total_leaves = sum(l for _, _, l in rows)
            lines = ["📈 <b>Analytics (30d)</b>"]
            lines.append(f"• Total joins: <b>{total_joins}</b>")
            lines.append(f"• Total leaves: <b>{total_leaves}</b>")
            lines.append("\nLast 7 days:")
            for d, j, l in rows[:7]:
                lines.append(f"{d}: +{j} / -{l}")
            text = "\n".join(lines)
        await cb.message.edit_text(text, reply_markup=user_dashboard_kb())
        await cb.answer()

    async def tenant_reports_cb(cb: CallbackQuery):
        if not cb.from_user: await cb.answer(); return
        if not _is_owner(cb.from_user.id):
            if not await _enforce_global_requirements(cast(Bot, cb.bot), cb.from_user.id): return
        text = (
            "🧾 <b>Reports</b>\n"
            "Available soon:\n"
            "• Daily/weekly/monthly aggregates (joins/leaves)\n"
            "• Top campaigns by invite link\n"
            "• Export CSV/PDF\n"
            "• Peak activity hours\n"
            "• Net growth per chat\n"
        )
        await cb.message.edit_text(text, reply_markup=user_dashboard_kb())
        await cb.answer()

    async def group_tools(cb: CallbackQuery):
        text = (
            "📎 <b>Group tools</b>\n"
            "Require new members to join specific channels before chatting in your <b>group</b>.\n\n"
            "1) Add this bot as <b>admin</b> in your group and in the required channel(s)\n"
            "2) In your group, run:\n"
            "   • <code>/set_force_join @PublicChannel</code>\n"
            "   • <code>/set_force_join -1001234567890</code>\n"
            "   • For <b>private</b> targets, include an invite link so the button opens:\n"
            "     <code>/set_force_join -1001234567890 https://t.me/+InviteCode</code>\n"
            "   • Repeat to add more; <code>/unset_force_join @Target</code> to remove one; or <code>/unset_force_join</code> to clear all.\n\n"
            "How it works:\n"
            "• When someone joins the group, we check membership in ALL required targets.\n"
            "• If missing, we mute them and show a button to verify after joining.\n"
            "• Once they tap the button and we verify, they’re auto-unmuted.\n\n"
            "Note: Channels are broadcast-only; this feature targets <b>groups</b> (where users chat)."
        )
        await cb.message.edit_text(text, reply_markup=user_dashboard_kb())
        await cb.answer()

    async def help_cb(cb: CallbackQuery):
        text = (
            "❓ <b>Help</b>\n\n"
            "• <b>Link chats</b>: In a group use <code>/link</code>. For channels, DM me <code>/link_channel @Channel</code> (or <code>-100…</code> ID).\n"
            "• <b>Group force-join</b>: In your group use <code>/set_force_join @Channel</code> or <code>-100…</code>; "
            "for private chats include invite URL so the button works. Non-members are muted until they join and tap verify.\n"
            "• <b>Analytics</b>: Open your dashboard → Analytics → pick a linked chat.\n"
            "• Make sure the bot is <b>admin</b> in the group and in required channels so it can verify membership."
        )
        await cb.message.edit_text(text, reply_markup=user_dashboard_kb())
        await cb.answer()

    async def tenant_settings_cb(cb: CallbackQuery):
        if not cb.from_user: await cb.answer(); return
        if not _is_owner(cb.from_user.id):
            if not await _enforce_global_requirements(cast(Bot, cb.bot), cb.from_user.id): return
        await cb.message.edit_text("⚙️ Settings\nComing soon.", reply_markup=user_dashboard_kb())
        await cb.answer()

    # Register
    dp.message.register(start_cmd, Command("start"))
    dp.message.register(contact_shared, F.contact)

    dp.callback_query.register(owner_dashboard, F.data == "owner_dashboard")
    dp.callback_query.register(force_check_global, F.data == "force_check_global")

    dp.callback_query.register(tenant_overview, F.data == "tenant_overview")
    dp.callback_query.register(tenant_chats_cb, F.data == "tenant_chats")
    dp.callback_query.register(tenant_analytics_cb, F.data == "tenant_analytics")
    dp.callback_query.register(tenant_analytics_view, F.data.startswith("tenant_analytics_view:"))
    dp.callback_query.register(tenant_reports_cb, F.data == "tenant_reports")
    dp.callback_query.register(group_tools, F.data == "group_tools")
    dp.callback_query.register(help_cb, F.data == "help")
    dp.callback_query.register(tenant_settings_cb, F.data == "tenant_settings")
