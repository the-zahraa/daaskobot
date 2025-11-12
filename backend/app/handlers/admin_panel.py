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
    search_tenants_page_with_stats,
)
from ..repositories.chats import count_all_chats, list_tenant_chats
from ..repositories.required import list_required_targets, add_required_target, remove_required_target
from ..services.i18n import t  # i18n
from .start import render_settings  # reuse same settings UI

# Import the renderer from admin_plans (router still included by bot_worker)
try:
    from .admin_plans import _render_admin_plans as render_admin_plans_ui
except Exception:
    render_admin_plans_ui = None  # fallback handled in handler

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


# ---------- Keyboards (language-aware) ----------

def _admin_kb(user_id: int) -> InlineKeyboardMarkup:
    """
    Main admin panel keyboard.

    Includes a quick way back to the owner dashboard so the owner never needs
    to /start again just to see their own tenant dashboard.
    """
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t("admin.kb.back_to_dashboard", user_id=user_id), callback_data="owner_dashboard")],
        [InlineKeyboardButton(text=t("admin.kb.overview", user_id=user_id),        callback_data="admin_overview")],
        [InlineKeyboardButton(text=t("admin.kb.tenants", user_id=user_id),         callback_data="admin_tenants:page:0")],
        [InlineKeyboardButton(text=t("admin.kb.search_tenants", user_id=user_id),  callback_data="admin_tenants_search")],
        [InlineKeyboardButton(text=t("admin.kb.broadcast", user_id=user_id),       callback_data="admin_broadcast")],
        [InlineKeyboardButton(text=t("admin.kb.force_join", user_id=user_id),      callback_data="admin_required")],
        [InlineKeyboardButton(text=t("admin.kb.plans", user_id=user_id),           callback_data="admin_plans_root")],
        [InlineKeyboardButton(text=t("admin.kb.settings", user_id=user_id),        callback_data="admin_settings")],
    ])


def _tenants_nav_kb(user_id: int, page: int, has_prev: bool, has_next: bool) -> InlineKeyboardMarkup:
    row = []
    row.append(
        InlineKeyboardButton(
            text=t("admin.tenants.prev", user_id=user_id),
            callback_data=f"admin_tenants:page:{page-1}",
        ) if has_prev else InlineKeyboardButton(text="—", callback_data="noop")
    )
    row.append(
        InlineKeyboardButton(
            text=t("admin.tenants.next", user_id=user_id),
            callback_data=f"admin_tenants:page:{page+1}",
        ) if has_next else InlineKeyboardButton(text="—", callback_data="noop")
    )
    return InlineKeyboardMarkup(inline_keyboard=[
        row,
        [InlineKeyboardButton(text=t("admin.tenants.export_pdf", user_id=user_id), callback_data="admin_tenants_export_pdf")],
        [InlineKeyboardButton(text=t("admin.tenants.back", user_id=user_id), callback_data="admin_overview")],
    ])


def _tenant_view_kb(user_id: int, tenant_id: str) -> InlineKeyboardMarkup:
    """
    Tenant detail screen:
      1) See linked chats
      2) Back to tenants list
      3) Back to admin overview
    """
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text=t("tenant_ui.kb.linked_chats", user_id=user_id),
                callback_data=f"tenant_chats:{tenant_id}",
            )
        ],
        [
            InlineKeyboardButton(
                text=t("admin.tenants.back_to_tenant", user_id=user_id),
                callback_data="admin_tenants:page:0",
            )
        ],
        [
            InlineKeyboardButton(
                text=t("admin.tenants.back_overview", user_id=user_id),
                callback_data="admin_overview",
            )
        ],
    ])


def _tenant_chats_back_kb(user_id: int, tenant_id: str) -> InlineKeyboardMarkup:
    """
    Tenant linked chats screen:
      1) Back to this tenant
      2) Back to admin overview
    """
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text=t("admin.tenants.back_to_tenant", user_id=user_id),
                callback_data=f"tenant_view:{tenant_id}",
            )
        ],
        [
            InlineKeyboardButton(
                text=t("admin.tenants.back_overview", user_id=user_id),
                callback_data="admin_overview",
            )
        ],
    ])


def _required_kb(user_id: int, targets: List[str]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for t_target in targets:
        rows.append([
            InlineKeyboardButton(
                text=t("admin.tenants.remove", user_id=user_id, target=t_target),
                callback_data=f"admin_required_remove:{t_target}",
            )
        ])
    rows.append([InlineKeyboardButton(text=t("admin.tenants.add_target", user_id=user_id), callback_data="admin_required_add")])
    rows.append([InlineKeyboardButton(text=t("admin.tenants.back_overview", user_id=user_id), callback_data="admin_overview")])
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
                # Text is the same; still try to update markup if it changed
                if kb is not None:
                    try:
                        await cb.message.edit_reply_markup(reply_markup=kb)
                    except TelegramBadRequest:
                        pass
                await cb.answer()
                return
        except TelegramBadRequest:
            await bot.send_message(chat_id, text, reply_markup=kb, disable_web_page_preview=True)
    else:
        await bot.send_message(chat_id, text, reply_markup=kb, disable_web_page_preview=True)
    await cb.answer()


def _build_pdf(rows: List[Dict[str, Any]]) -> tuple[bytes, str]:
    """
    Build tenants export.

    Returns (data, filename). If reportlab is not available, falls back to CSV
    with a .csv filename so it still opens correctly.
    """
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.pdfgen import canvas
        from reportlab.lib.units import mm
        from reportlab.lib.utils import simpleSplit
    except Exception:
        # Fallback: export as CSV instead of fake PDF
        buf = io.StringIO()
        buf.write("tenant_id,name,owner_tg_id,created_at,chat_count,plan\n")
        for r in rows:
            buf.write(f"{r['id']},{r['name']},{r['owner_tg_id']},{r['created_at']},{r['chat_count']},{r['plan']}\n")
        return buf.getvalue().encode("utf-8"), "tenants_export.csv"

    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    width, height = A4
    x_margin = 15 * mm
    y = height - 20 * mm

    c.setFont("Helvetica-Bold", 14)
    c.drawString(x_margin, y, "Tenants Export")
    y -= 10 * mm
    c.setFont("Helvetica", 9)
    headers = ["tenant_id", "name", "owner_tg_id", "created_at", "#chats", "plan"]
    col_x = [x_margin, x_margin + 45 * mm, x_margin + 95 * mm, x_margin + 125 * mm, x_margin + 155 * mm, x_margin + 175 * mm]
    c.setFont("Helvetica-Bold", 9)
    for i, h in enumerate(headers):
        c.drawString(col_x[i], y, h)
    y -= 6 * mm
    c.setFont("Helvetica", 9)

    for r in rows:
        vals = [r["id"], r["name"], str(r["owner_tg_id"]), r["created_at"], str(r["chat_count"]), r["plan"]]
        from reportlab.lib.utils import simpleSplit as ss
        for i, v in enumerate(vals):
            lines = ss(v or "", "Helvetica", 9, 35 * mm if i == 1 else 30 * mm)
            for ln in lines:
                c.drawString(col_x[i], y, ln)
                y -= 5 * mm
        y -= 2 * mm
        if y < 20 * mm:
            c.showPage()
            y = height - 20 * mm
            c.setFont("Helvetica", 9)

    c.showPage()
    c.save()
    return buf.getvalue(), "tenants_export.pdf"


def _norm_target_line(s: str) -> Optional[str]:
    """
    Normalize a target line from admin input.

    Accepts:
      - @username
      - bare username
      - -100123456789 (chat/channel id)
      - t.me/username (→ @username)
      - https://t.me/+code, https://t.me/joinchat/... (kept as URL)
      - any http/https link (kept as-is, later treated as join_url)
    """
    s = (s or "").strip()
    if not s:
        return None

    low = s.lower()

    # Accept full URLs (including join links)
    if low.startswith("http://") or low.startswith("https://"):
        return s

    if s.startswith("@"):
        return s
    if s.startswith("-100") and s[4:].isdigit():
        return s
    m = re.search(r"(?:^|https?://)?t\.me/([A-Za-z0-9_]+)$", s)
    if m:
        return "@" + m.group(1)
    if re.fullmatch(r"[A-Za-z0-9_]{5,}", s):
        return "@" + s
    return None


# ---------- Register ----------

def register(dp):
    # /admin (owner only)
    async def admin_command(msg: Message):
        if not _authorized(msg.from_user.id if msg.from_user else None):
            await msg.answer(t("admin.not_auth", user_id=(msg.from_user.id if msg.from_user else None)))
            return
        # Entry point text + menu
        await msg.answer(t("admin.panel", user_id=msg.from_user.id), reply_markup=_admin_kb(msg.from_user.id))

    # Overview (admin "home" / stats screen)
    async def admin_overview(cb: CallbackQuery):
        if not _authorized(cb.from_user.id if cb.from_user else None):
            await cb.answer()
            return
        total_users = await count_all_users()
        premium_users = await count_premium_users()
        active_tenants = await count_active_tenants()
        chats = await count_all_chats()
        text = (
            f"{t('admin.overview_title', user_id=cb.from_user.id)}\n"
            + t(
                "admin.overview_lines",
                user_id=cb.from_user.id,
                users=total_users,
                premium=premium_users,
                tenants=active_tenants,
                chats=chats,
            )
        )
        await _edit_or_send(cb, text, _admin_kb(cb.from_user.id))

    # Tenants: paginated list
    async def admin_tenants_list(cb: CallbackQuery):
        if not _authorized(cb.from_user.id if cb.from_user else None):
            await cb.answer()
            return
        parts = (cb.data or "").split(":")
        page = int(parts[-1]) if parts and parts[-2] == "page" else 0
        offset = page * PAGE_SIZE
        tenants = await list_tenants_page_with_stats(PAGE_SIZE + 1, offset)
        has_next = len(tenants) > PAGE_SIZE
        tenants = tenants[:PAGE_SIZE]
        has_prev = page > 0

        if not tenants:
            text = t("admin.tenants.none", user_id=cb.from_user.id)
            await _edit_or_send(cb, text, _tenants_nav_kb(cb.from_user.id, page, has_prev, has_next))
            return

        lines = [t("admin.tenants.title_latest", user_id=cb.from_user.id)]
        kb_rows = []
        for t_row in tenants:
            tid = t_row["id"]
            summary = f"{t_row['name']} — owner {t_row['owner_tg_id']} — {t_row['chat_count']} chats — plan {t_row['plan']} — {t_row['created_at']}"
            lines.append(f"• {summary}")
            kb_rows.append([InlineKeyboardButton(text=summary, callback_data=f"tenant_view:{tid}")])

        kb = InlineKeyboardMarkup(
            inline_keyboard=kb_rows + _tenants_nav_kb(cb.from_user.id, page, has_prev, has_next).inline_keyboard
        )
        await _edit_or_send(cb, "\n".join(lines), kb)

    # Tenants: search (FSM)
    async def admin_tenants_search(cb: CallbackQuery, state: FSMContext):
        if not _authorized(cb.from_user.id if cb.from_user else None):
            await cb.answer()
            return
        await state.set_state(AdminStates.waiting_tenant_search)
        await _edit_or_send(cb, t("admin.tenants.search_prompt", user_id=cb.from_user.id), _admin_kb(cb.from_user.id))

    async def search_query_received(msg: Message, state: FSMContext):
        if not _authorized(msg.from_user.id if msg.from_user else None):
            return
        q = (msg.text or "").strip()
        await state.clear()
        tenants = await search_tenants_page_with_stats(q, PAGE_SIZE + 1, 0)
        has_next = len(tenants) > PAGE_SIZE
        tenants = tenants[:PAGE_SIZE]

        lines = [t("admin.tenants.search_title", user_id=msg.from_user.id, q=q)]
        kb_rows = []
        for t_row in tenants:
            tid = t_row["id"]
            summary = f"{t_row['name']} — owner {t_row['owner_tg_id']} — {t_row['chat_count']} chats — plan {t_row['plan']} — {t_row['created_at']}"
            lines.append(f"• {summary}")
            kb_rows.append([InlineKeyboardButton(text=summary, callback_data=f"tenant_view:{tid}")])
        if not tenants:
            lines.append(t("admin.tenants.no_results", user_id=msg.from_user.id))
        nav = _tenants_nav_kb(msg.from_user.id, 0, False, has_next)
        await msg.answer(
            "\n".join(lines),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows + nav.inline_keyboard),
        )

    # Tenants: export PDF/CSV
    async def admin_tenants_export_pdf(cb: CallbackQuery):
        if not _authorized(cb.from_user.id if cb.from_user else None):
            await cb.answer()
            return

        rows = await export_all_tenants_with_stats()
        data, filename = _build_pdf(rows)
        file = BufferedInputFile(data, filename=filename)

        # 1) Send the document
        await cb.message.answer_document(
            file,
            caption=t("admin.tenants.export_caption", user_id=cb.from_user.id),
        )

        # 2) Send a short navigation message so admin never needs to scroll
        nav_kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=t("admin.tenants.back_to_tenant", user_id=cb.from_user.id),  # "⬅️ Back to tenants list"
                    callback_data="admin_tenants:page:0",
                )
            ],
            [
                InlineKeyboardButton(
                    text=t("admin.tenants.back_overview", user_id=cb.from_user.id),   # "🏠 Back to admin panel"
                    callback_data="admin_overview",
                )
            ],
        ])

        await cb.message.answer(
            t("admin.tenants.export_done", user_id=cb.from_user.id),
            reply_markup=nav_kb,
        )

        await cb.answer(t("admin.tenants.export_done", user_id=cb.from_user.id), show_alert=False)

    # Tenant detail
    async def tenant_view(cb: CallbackQuery):
        if not _authorized(cb.from_user.id if cb.from_user else None):
            await cb.answer()
            return
        parts = (cb.data or "").split(":")
        tenant_id = parts[1] if len(parts) >= 2 else ""
        tnt = await get_tenant(tenant_id)
        if not tnt:
            await _edit_or_send(cb, t("admin.tenants.tenant_not_found", user_id=cb.from_user.id), _admin_kb(cb.from_user.id))
            return
        text = (
            f"{t('admin.tenants.tenant_title', user_id=cb.from_user.id)}\n"
            + t(
                "admin.tenants.tenant_lines",
                user_id=cb.from_user.id,
                name=tnt["name"],
                owner=tnt["owner_tg_id"],
                created=tnt["created_at"],
            )
        )
        await _edit_or_send(cb, text, _tenant_view_kb(cb.from_user.id, tenant_id))

    # Tenant linked chats
    async def tenant_chats_view(cb: CallbackQuery):
        if not _authorized(cb.from_user.id if cb.from_user else None):
            await cb.answer()
            return
        parts = (cb.data or "").split(":")
        tenant_id = parts[1] if len(parts) >= 2 else ""
        chats = await list_tenant_chats(tenant_id)
        if not chats:
            text = t("admin.tenants.linked_none", user_id=cb.from_user.id)
        else:
            lines = [t("admin.tenants.linked_title", user_id=cb.from_user.id)]
            for cid, ctype, title in chats[:50]:
                lines.append(f"• <code>{cid}</code> — {ctype} — {title}")
            if len(chats) > 50:
                lines.append(t("admin.tenants.linked_more", user_id=cb.from_user.id, n=len(chats) - 50))
            text = "\n".join(lines)
        await _edit_or_send(cb, text, _tenant_chats_back_kb(cb.from_user.id, tenant_id))

    # Bot-wide force-join list
    async def admin_required(cb: CallbackQuery):
        if not _authorized(cb.from_user.id if cb.from_user else None):
            await cb.answer()
            return
        targets = await list_required_targets()
        text = t("admin.tenants.force_title", user_id=cb.from_user.id)
        if not targets:
            text += t("admin.tenants.force_none", user_id=cb.from_user.id)
        else:
            text += "\n".join(f"• {tgt}" for tgt in targets)
        await _edit_or_send(cb, text, _required_kb(cb.from_user.id, targets))

    # Add to bot-wide force-join (FSM)
    async def admin_required_add(cb: CallbackQuery, state: FSMContext):
        if not _authorized(cb.from_user.id if cb.from_user else None):
            await cb.answer()
            return
        await state.set_state(AdminStates.waiting_required_target)
        await cb.message.answer(t("admin.tenants.add_prompt", user_id=cb.from_user.id))
        await cb.answer()

    async def required_target_received(msg: Message, state: FSMContext):
        if not _authorized(msg.from_user.id if msg.from_user else None):
            return
        raw = (msg.text or "").strip()
        await state.clear()
        added = []
        for line in raw.splitlines():
            tline = _norm_target_line(line)
            if tline:
                await add_required_target(tline, msg.from_user.id)
                added.append(tline)
        if not added:
            await msg.answer(t("admin.tenants.invalid_lines", user_id=msg.from_user.id))
        else:
            await msg.answer(
                t("admin.tenants.force_added", user_id=msg.from_user.id, lines="\n".join(f"• {x}" for x in added))
            )
        targets = await list_required_targets()
        kb = _required_kb(msg.from_user.id, targets)
        head = t("admin.tenants.force_list_head", user_id=msg.from_user.id)
        text = head + (
            "\n".join(f"• {tgt}" for tgt in targets)
            if targets
            else t("admin.tenants.force_no_targets", user_id=msg.from_user.id)
        )
        await msg.answer(text, reply_markup=kb)

    # Remove from bot-wide force-join
    async def admin_required_remove(cb: CallbackQuery):
        if not _authorized(cb.from_user.id if cb.from_user else None):
            await cb.answer()
            return
        parts = (cb.data or "").split(":", 1)
        target = parts[1] if len(parts) == 2 else ""
        if target:
            await remove_required_target(target)
        targets = await list_required_targets()
        current = (
            t(
                "admin.tenants.current_list",
                user_id=cb.from_user.id,
                lines="\n".join(f"• {x}" for x in targets),
            )
            if targets
            else t("admin.tenants.force_no_targets", user_id=cb.from_user.id)
        )
        text = t("admin.tenants.force_removed_now", user_id=cb.from_user.id, current=current)
        await _edit_or_send(cb, text, _required_kb(cb.from_user.id, targets))

    # Settings (reuse the same language/settings UI as normal users)
    async def admin_settings(cb: CallbackQuery):
        if not _authorized(cb.from_user.id if cb.from_user else None):
            await cb.answer()
            return
        await render_settings(cb, cb.from_user.id)

    # NEW: Plans & Pricing entry
    async def admin_plans_root(cb: CallbackQuery):
        if not _authorized(cb.from_user.id if cb.from_user else None):
            await cb.answer()
            return
        if render_admin_plans_ui is None:
            await _edit_or_send(cb, t("admin.plans_fallback", user_id=cb.from_user.id), _admin_kb(cb.from_user.id))
            return
        await render_admin_plans_ui(cb)

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

    dp.callback_query.register(admin_plans_root, F.data == "admin_plans_root")
    dp.callback_query.register(admin_settings, F.data == "admin_settings")
