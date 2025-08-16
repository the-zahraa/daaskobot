# backend/app/handlers/admin_panel.py
from __future__ import annotations
import os
import io
import re
from typing import Optional, cast, List, Dict, Any

from aiogram import F, Bot
from aiogram.filters import Command
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    Message,
    BufferedInputFile,
)
from aiogram.exceptions import TelegramBadRequest

from ..repositories.users import count_all_users, count_premium_users
from ..repositories.tenants import (
    count_active_tenants,
    list_tenants_page_with_stats,
    export_all_tenants_with_stats,
    get_tenant,
    search_tenants_page_with_stats,   # make sure your tenants.py includes this
)
from ..repositories.chats import count_all_chats, list_tenant_chats
from ..repositories.required import list_required_targets, add_required_target, remove_required_target

# ---------- Config ----------
_owner_env = os.getenv("OWNER_ID", "").strip()
try:
    OWNER_ID: Optional[int] = int(_owner_env) if _owner_env else None
except ValueError:
    OWNER_ID = None

PAGE_SIZE = 5  # tenants per page

class AdminStates(StatesGroup):
    waiting_required_target = State()
    waiting_tenant_search = State()

# ---------- Keyboards ----------

def _admin_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Overview", callback_data="admin_overview")],
        [InlineKeyboardButton(text="👥 Tenants", callback_data="admin_tenants:page:0")],
        [InlineKeyboardButton(text="🔎 Search Tenants", callback_data="admin_tenants_search")],
        [InlineKeyboardButton(text="💬 Broadcast", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="🔐 Bot Force-Join", callback_data="admin_required")],
        [InlineKeyboardButton(text="⚙️ Settings", callback_data="admin_settings")],
    ])

def _tenants_nav_kb(page: int, has_prev: bool, has_next: bool) -> InlineKeyboardMarkup:
    row = []
    row.append(InlineKeyboardButton(text="⬅️ Prev", callback_data=f"admin_tenants:page:{page-1}") if has_prev
               else InlineKeyboardButton(text="—", callback_data="noop"))
    row.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"admin_tenants:page:{page+1}") if has_next
               else InlineKeyboardButton(text="—", callback_data="noop"))
    return InlineKeyboardMarkup(inline_keyboard=[
        row,
        [InlineKeyboardButton(text="📥 Export PDF", callback_data="admin_tenants_export_pdf")],
        [InlineKeyboardButton(text="🏠 Back", callback_data="admin_overview")],
    ])

def _tenant_view_kb(tenant_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔗 Linked Chats", callback_data=f"tenant_chats:{tenant_id}")],
        [InlineKeyboardButton(text="⬅️ Back", callback_data="admin_tenants:page:0")],
    ])

def _tenant_chats_back_kb(tenant_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Back to Tenant", callback_data=f"tenant_view:{tenant_id}")]
    ])

def _required_kb(targets: List[str]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for t in targets:
        rows.append([InlineKeyboardButton(text=f"❌ Remove {t}", callback_data=f"admin_required_remove:{t}")])
    rows.append([InlineKeyboardButton(text="➕ Add target", callback_data="admin_required_add")])
    rows.append([InlineKeyboardButton(text="⬅️ Back", callback_data="admin_overview")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

# ---------- Utilities ----------

def _authorized(uid: Optional[int]) -> bool:
    return OWNER_ID is not None and uid == OWNER_ID

async def _edit_or_send(cb: CallbackQuery, text: str, kb: Optional[InlineKeyboardMarkup] = None):
    bot = cast(Bot, cb.bot)
    chat_id = cb.message.chat.id if cb.message else cb.from_user.id
    if cb.message:
        try:
            if cb.message.text != text:
                await cb.message.edit_text(text, reply_markup=kb, disable_web_page_preview=True)
            else:
                await cb.answer()
        except TelegramBadRequest:
            await bot.send_message(chat_id, text, reply_markup=kb, disable_web_page_preview=True)
    else:
        await bot.send_message(chat_id, text, reply_markup=kb, disable_web_page_preview=True)
    await cb.answer()

def _build_pdf(rows: List[Dict[str, Any]]) -> bytes:
    """
    Try to generate a proper PDF (reportlab). If reportlab isn't installed,
    fall back to CSV bytes so the export still works.
    """
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.pdfgen import canvas
        from reportlab.lib.units import mm
        from reportlab.lib.utils import simpleSplit
    except Exception:
        # Minimal CSV-as-bytes fallback
        buf = io.StringIO()
        buf.write("tenant_id,name,owner_tg_id,created_at,chat_count,plan\n")
        for r in rows:
            buf.write(f"{r['id']},{r['name']},{r['owner_tg_id']},{r['created_at']},{r['chat_count']},{r['plan']}\n")
        return buf.getvalue().encode("utf-8")

    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    width, height = A4
    x_margin = 15*mm
    y = height - 20*mm

    c.setFont("Helvetica-Bold", 14)
    c.drawString(x_margin, y, "Tenants Export")
    y -= 10*mm
    c.setFont("Helvetica", 9)
    headers = ["tenant_id", "name", "owner_tg_id", "created_at", "#chats", "plan"]
    col_x = [x_margin, x_margin+45*mm, x_margin+95*mm, x_margin+125*mm, x_margin+155*mm, x_margin+175*mm]
    c.setFont("Helvetica-Bold", 9)
    for i, h in enumerate(headers):
        c.drawString(col_x[i], y, h)
    y -= 6*mm
    c.setFont("Helvetica", 9)

    for r in rows:
        vals = [r["id"], r["name"], str(r["owner_tg_id"]), r["created_at"], str(r["chat_count"]), r["plan"]]
        for i, v in enumerate(vals):
            lines = simpleSplit(v or "", "Helvetica", 9, 35*mm if i==1 else 30*mm)
            for ln in lines:
                c.drawString(col_x[i], y, ln)
                y -= 5*mm
        y -= 2*mm
        if y < 20*mm:
            c.showPage()
            y = height - 20*mm
            c.setFont("Helvetica", 9)

    c.showPage()
    c.save()
    return buf.getvalue()

def _norm_target_line(s: str) -> Optional[str]:
    s = (s or "").strip()
    if not s:
        return None
    if s.startswith("@"):
        return s
    if s.startswith("-100") and s[4:].isdigit():
        return s
    m = re.search(r"(?:^|https?://)?t\.me/([A-Za-z0-9_]+)$", s)
    if m:
        return "@"+m.group(1)
    if re.fullmatch(r"[A-Za-z0-9_]{5,}", s):
        return "@"+s
    return None

# ---------- Register ----------

def register(dp):
    # /admin (owner only)
    async def admin_command(msg: Message):
        if not _authorized(msg.from_user.id if msg.from_user else None):
            await msg.answer("Not authorized."); return
        await msg.answer("👑 Admin Panel", reply_markup=_admin_kb())

    # Overview
    async def admin_overview(cb: CallbackQuery):
        if not _authorized(cb.from_user.id if cb.from_user else None):
            await cb.answer(); return
        total_users = await count_all_users()
        premium_users = await count_premium_users()
        active_tenants = await count_active_tenants()
        chats = await count_all_chats()
        text = (
            "📊 <b>Overview</b>\n"
            f"• Users: <b>{total_users}</b>\n"
            f"• Telegram Premium users: <b>{premium_users}</b>\n"
            f"• Active tenants: <b>{active_tenants}</b>\n"
            f"• Linked chats: <b>{chats}</b>\n"
        )
        await _edit_or_send(cb, text, _admin_kb())

    # Tenants: paginated list
    async def admin_tenants_list(cb: CallbackQuery):
        if not _authorized(cb.from_user.id if cb.from_user else None):
            await cb.answer(); return
        parts = (cb.data or "").split(":")
        page = int(parts[-1]) if parts and parts[-2] == "page" else 0
        offset = page * PAGE_SIZE
        tenants = await list_tenants_page_with_stats(PAGE_SIZE + 1, offset)
        has_next = len(tenants) > PAGE_SIZE
        tenants = tenants[:PAGE_SIZE]
        has_prev = page > 0

        if not tenants:
            text = "👥 <b>Tenants</b>\nNo tenants yet."
            await _edit_or_send(cb, text, _tenants_nav_kb(page, has_prev, has_next))
            return

        lines = ["👥 <b>Tenants</b> (latest)"]
        kb_rows = []
        for t in tenants:
            tid = t["id"]
            summary = f"{t['name']} — owner {t['owner_tg_id']} — {t['chat_count']} chats — plan {t['plan']} — {t['created_at']}"
            lines.append(f"• {summary}")
            kb_rows.append([InlineKeyboardButton(text=summary, callback_data=f"tenant_view:{tid}")])

        kb = InlineKeyboardMarkup(inline_keyboard=kb_rows + _tenants_nav_kb(page, has_prev, has_next).inline_keyboard)
        await _edit_or_send(cb, "\n".join(lines), kb)

    # Tenants: search (FSM)
    async def admin_tenants_search(cb: CallbackQuery, state: FSMContext):
        if not _authorized(cb.from_user.id if cb.from_user else None):
            await cb.answer(); return
        await state.set_state(AdminStates.waiting_tenant_search)
        await _edit_or_send(cb, "🔎 Send a search query (name, owner tg id, or tenant id). Type /cancel to abort.")

    async def search_query_received(msg: Message, state: FSMContext):
        if not _authorized(msg.from_user.id if msg.from_user else None):
            return
        q = (msg.text or "").strip()
        await state.clear()
        tenants = await search_tenants_page_with_stats(q, PAGE_SIZE + 1, 0)
        has_next = len(tenants) > PAGE_SIZE
        tenants = tenants[:PAGE_SIZE]

        lines = [f"🔎 <b>Search results</b> for <code>{q}</code>"]
        kb_rows = []
        for t in tenants:
            tid = t["id"]
            summary = f"{t['name']} — owner {t['owner_tg_id']} — {t['chat_count']} chats — plan {t['plan']} — {t['created_at']}"
            lines.append(f"• {summary}")
            kb_rows.append([InlineKeyboardButton(text=summary, callback_data=f"tenant_view:{tid}")])
        if not tenants:
            lines.append("No results.")
        nav = _tenants_nav_kb(0, False, has_next)
        await msg.answer("\n".join(lines), reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows + nav.inline_keyboard))

    # Tenants: export PDF
    async def admin_tenants_export_pdf(cb: CallbackQuery):
        if not _authorized(cb.from_user.id if cb.from_user else None):
            await cb.answer(); return
        rows = await export_all_tenants_with_stats()
        data = _build_pdf(rows)
        # FIX: BufferedInputFile expects raw bytes as first positional arg
        file = BufferedInputFile(data, filename="tenants_export.pdf")
        await cb.message.answer_document(file, caption="Full tenants export (PDF)")
        await cb.answer("Exported.", show_alert=False)

    # Tenant detail
    async def tenant_view(cb: CallbackQuery):
        if not _authorized(cb.from_user.id if cb.from_user else None):
            await cb.answer(); return
        parts = (cb.data or "").split(":")
        tenant_id = parts[1] if len(parts) >= 2 else ""
        t = await get_tenant(tenant_id)
        if not t:
            await _edit_or_send(cb, "Tenant not found.", _admin_kb()); return
        text = (
            "👤 <b>Tenant</b>\n"
            f"• Name: <b>{t['name']}</b>\n"
            f"• Owner: <b>{t['owner_tg_id']}</b>\n"
            f"• Created: <code>{t['created_at']}</code>\n"
        )
        await _edit_or_send(cb, text, _tenant_view_kb(tenant_id))

    # Tenant linked chats
    async def tenant_chats_view(cb: CallbackQuery):
        if not _authorized(cb.from_user.id if cb.from_user else None):
            await cb.answer(); return
        parts = (cb.data or "").split(":")
        tenant_id = parts[1] if len(parts) >= 2 else ""
        chats = await list_tenant_chats(tenant_id)
        if not chats:
            text = "🔗 <b>Linked Chats</b>\nNo chats yet."
        else:
            lines = ["🔗 <b>Linked Chats</b>"]
            for cid, ctype, title in chats[:50]:
                lines.append(f"• <code>{cid}</code> — {ctype} — {title}")
            if len(chats) > 50:
                lines.append(f"… and {len(chats)-50} more")
            text = "\n".join(lines)
        await _edit_or_send(cb, text, _tenant_chats_back_kb(tenant_id))

    # Bot-wide force-join list
    async def admin_required(cb: CallbackQuery):
        if not _authorized(cb.from_user.id if cb.from_user else None):
            await cb.answer(); return
        targets = await list_required_targets()
        text = (
            "🔐 <b>Bot Force-Join</b>\n"
            "Users must be member of ALL targets below to use the bot.\n"
        )
        if not targets:
            text += "No required channels/groups.\nTap ➕ Add target."
        else:
            text += "\n".join(f"• {t}" for t in targets)
        await _edit_or_send(cb, text, _required_kb(targets))

    # Add to bot-wide force-join (FSM)
    async def admin_required_add(cb: CallbackQuery, state: FSMContext):
        if not _authorized(cb.from_user.id if cb.from_user else None):
            await cb.answer(); return
        await state.set_state(AdminStates.waiting_required_target)
        await cb.message.answer("Send target(s) to add (e.g., @channel or -100id). You can send multiple lines. Use /cancel to abort.")
        await cb.answer()

    async def required_target_received(msg: Message, state: FSMContext):
        if not _authorized(msg.from_user.id if msg.from_user else None):
            return
        raw = (msg.text or "").strip()
        await state.clear()
        added = []
        for line in raw.splitlines():
            t = _norm_target_line(line)
            if t:
                await add_required_target(t, msg.from_user.id)
                added.append(t)
        if not added:
            await msg.answer("No valid targets found. Examples: @channel, -1001234567890")
        else:
            await msg.answer("Added:\n" + "\n".join(f"• {t}" for t in added))
        targets = await list_required_targets()
        kb = _required_kb(targets)
        text = "🔐 <b>Bot Force-Join</b>\n" + ("\n".join(f"• {t}" for t in targets) if targets else "No required channels/groups.")
        await msg.answer(text, reply_markup=kb)

    # Remove from bot-wide force-join
    async def admin_required_remove(cb: CallbackQuery):
        if not _authorized(cb.from_user.id if cb.from_user else None):
            await cb.answer(); return
        parts = (cb.data or "").split(":", 1)
        target = parts[1] if len(parts) == 2 else ""
        if target:
            await remove_required_target(target)
        targets = await list_required_targets()
        text = "Removed.\n\n" + ("Current list:\n" + "\n".join(f"• {t}" for t in targets) if targets else "No targets.")
        await _edit_or_send(cb, text, _required_kb(targets))

    # Settings placeholder
    async def admin_settings(cb: CallbackQuery):
        if not _authorized(cb.from_user.id if cb.from_user else None):
            await cb.answer(); return
        await _edit_or_send(cb, "⚙️ Global settings — placeholder.", _admin_kb())

    # Register
    dp.message.register(admin_command, Command("admin"))
    dp.callback_query.register(admin_overview, F.data == "admin_overview")

    dp.callback_query.register(admin_tenants_list, F.data.startswith("admin_tenants:page:"))
    dp.callback_query.register(admin_tenants_search, F.data == "admin_tenants_search")
    dp.message.register(search_query_received, AdminStates.waiting_tenant_search)

    dp.callback_query.register(admin_tenants_export_pdf, F.data == "admin_tenants_export_pdf")
    dp.callback_query.register(tenant_view, F.data.startswith("tenant_view:"))
    dp.callback_query.register(tenant_chats_view, F.data.startswith("tenant_chats:"))

    dp.callback_query.register(admin_required, F.data == "admin_required")
    dp.callback_query.register(admin_required_add, F.data == "admin_required_add")
    dp.message.register(required_target_received, AdminStates.waiting_required_target)
    dp.callback_query.register(admin_required_remove, F.data.startswith("admin_required_remove:"))

    dp.callback_query.register(admin_settings, F.data == "admin_settings")
