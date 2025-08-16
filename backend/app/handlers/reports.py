# backend/app/handlers/reports.py
from __future__ import annotations
import csv
import io
from typing import List, Tuple, Optional, cast

from aiogram import F, Bot
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile

from ..repositories.tenants import get_user_tenant
from ..repositories.chats import list_tenant_chats
from ..repositories.reports import agg_daily, agg_weekly, agg_monthly, peak_hours, filter_members

def _kb_reports_root(chats: List[Tuple[int,str,str]]) -> InlineKeyboardMarkup:
    rows = []
    for cid, ctype, title in chats[:30]:
        rows.append([InlineKeyboardButton(text=f"{title or cid} ({ctype})", callback_data=f"rep_chat:{cid}")])
    rows.append([InlineKeyboardButton(text="⬅️ Back", callback_data="tenant_overview")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def _kb_period(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 Daily (30d)", callback_data=f"rep_daily:{chat_id}")],
        [InlineKeyboardButton(text="🗓 Weekly (12w)", callback_data=f"rep_weekly:{chat_id}")],
        [InlineKeyboardButton(text="📆 Monthly (12m)", callback_data=f"rep_monthly:{chat_id}")],
        [InlineKeyboardButton(text="⏰ Peak hours (7d)", callback_data=f"rep_peak:{chat_id}")],
        [InlineKeyboardButton(text="📤 Export CSV", callback_data=f"rep_export_csv:{chat_id}")],
        [InlineKeyboardButton(text="🧾 Export PDF", callback_data=f"rep_export_pdf:{chat_id}")],
        [InlineKeyboardButton(text="🔎 Filter members", callback_data=f"rep_filter:{chat_id}")],
        [InlineKeyboardButton(text="⬅️ Back", callback_data="tenant_reports")]
    ])

def _format_table(rows: List[Tuple], headers: List[str]) -> str:
    if not rows:
        return "No data."
    lines = [" | ".join(headers), "-"*32]
    for r in rows:
        lines.append(" | ".join(str(x) for x in r))
    return "\n".join(lines)

async def _export_csv(bot: Bot, chat_id: int, to_id: int):
    rows = await agg_daily(chat_id, 90)
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["date","joins","leaves","net"])
    for d, j, l, n in rows:
        w.writerow([d, j, l, n])
    data = buf.getvalue().encode("utf-8")
    file = BufferedInputFile(data, filename=f"report_{chat_id}.csv")
    await bot.send_document(to_id, file, caption="Daily report (last 90 days)")

def _pdf_from_daily(rows: List[Tuple[str,int,int,int]]) -> bytes:
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib import colors
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.lib.units import mm
    except Exception:
        # fallback CSV
        buf = io.StringIO()
        buf.write("date,joins,leaves,net\n")
        for r in rows:
            buf.write(",".join(map(str,r))+"\n")
        return buf.getvalue().encode("utf-8")

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=12*mm, rightMargin=12*mm, topMargin=12*mm, bottomMargin=12*mm)
    styles = getSampleStyleSheet()
    story = []
    story.append(Paragraph("Daily Report (last 90 days)", styles["Title"]))
    story.append(Spacer(1, 6))

    data = [["Date","Joins","Leaves","Net"]]
    for d,j,l,n in rows:
        data.append([d, j, l, n])

    table = Table(data, colWidths=[30*mm, 25*mm, 25*mm, 20*mm], repeatRows=1)
    table.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#f1f3f5")),
        ("GRID", (0,0), (-1,-1), 0.2, colors.HexColor("#bbb")),
        ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
        ("FONTSIZE", (0,0), (-1,0), 10),
        ("FONTSIZE", (0,1), (-1,-1), 9),
        ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.whitesmoke, colors.HexColor("#fafafa")]),
    ]))
    story.append(table)
    doc.build(story)
    return buf.getvalue()

async def _export_pdf(bot: Bot, chat_id: int, to_id: int):
    rows = await agg_daily(chat_id, 90)
    data = _pdf_from_daily(rows)
    file = BufferedInputFile(data, filename=f"report_{chat_id}.pdf")
    await bot.send_document(to_id, file, caption="Daily report (last 90 days)")

def register(dp):
    async def tenant_reports_root(cb: CallbackQuery):
        if not cb.from_user: await cb.answer(); return
        tenant_id = await get_user_tenant(cb.from_user.id)
        if not tenant_id:
            await cb.message.edit_text("🧾 Reports\nNo tenant found.")
            await cb.answer(); return
        chats = await list_tenant_chats(tenant_id)
        if not chats:
            await cb.message.edit_text("🧾 Reports\nNo linked chats yet.")
            await cb.answer(); return
        await cb.message.edit_text("🧾 Reports — pick a chat:", reply_markup=_kb_reports_root(chats))
        await cb.answer()

    async def reports_for_chat(cb: CallbackQuery):
        parts = (cb.data or "").split(":")
        chat_id = int(parts[1]) if len(parts) >= 2 else 0
        await cb.message.edit_text("Select a report:", reply_markup=_kb_period(chat_id))
        await cb.answer()

    async def show_daily(cb: CallbackQuery):
        chat_id = int(cb.data.split(":")[1])
        rows = await agg_daily(chat_id, 30)
        text = "📅 <b>Daily (30d)</b>\n" + _format_table(rows, ["Date","Joins","Leaves","Net"])
        await cb.message.edit_text(text, reply_markup=_kb_period(chat_id))
        await cb.answer()

    async def show_weekly(cb: CallbackQuery):
        chat_id = int(cb.data.split(":")[1])
        rows = await agg_weekly(chat_id, 12)
        text = "🗓 <b>Weekly (12w)</b>\n" + _format_table(rows, ["Week","Joins","Leaves","Net"])
        await cb.message.edit_text(text, reply_markup=_kb_period(chat_id))
        await cb.answer()

    async def show_monthly(cb: CallbackQuery):
        chat_id = int(cb.data.split(":")[1])
        rows = await agg_monthly(chat_id, 12)
        text = "📆 <b>Monthly (12m)</b>\n" + _format_table(rows, ["Month","Joins","Leaves","Net"])
        await cb.message.edit_text(text, reply_markup=_kb_period(chat_id))
        await cb.answer()

    async def show_peak(cb: CallbackQuery):
        chat_id = int(cb.data.split(":")[1])
        rows = await peak_hours(chat_id, 7)
        # format: "00: 5\n01: 2..."
        if not rows:
            text = "⏰ <b>Peak hours (7d)</b>\nNo data."
        else:
            lines = ["⏰ <b>Peak hours (7d)</b>"]
            for hh, c in rows:
                lines.append(f"{hh:02d}: {c}")
            text = "\n".join(lines)
        await cb.message.edit_text(text, reply_markup=_kb_period(chat_id))
        await cb.answer()

    async def export_csv(cb: CallbackQuery):
        chat_id = int(cb.data.split(":")[1])
        await _export_csv(cast(Bot, cb.bot), chat_id, cb.message.chat.id)
        await cb.answer("CSV exported.")

    async def export_pdf(cb: CallbackQuery):
        chat_id = int(cb.data.split(":")[1])
        await _export_pdf(cast(Bot, cb.bot), chat_id, cb.message.chat.id)
        await cb.answer("PDF exported.")

    async def filter_ui(cb: CallbackQuery):
        chat_id = int(cb.data.split(":")[1])
        text = (
            "🔎 <b>Filter members</b>\n"
            "Send one line with optional filters:\n"
            "<code>name=Ali; phone=+33; has_phone=1</code>\n"
            "Supported keys: name, phone, has_phone (0/1)"
        )
        # reuse same keyboard
        await cb.message.edit_text(text, reply_markup=_kb_period(chat_id))
        await cb.answer()

    # register
    dp.callback_query.register(tenant_reports_root, F.data == "tenant_reports")
    dp.callback_query.register(reports_for_chat, F.data.startswith("rep_chat:"))
    dp.callback_query.register(show_daily,  F.data.startswith("rep_daily:"))
    dp.callback_query.register(show_weekly, F.data.startswith("rep_weekly:"))
    dp.callback_query.register(show_monthly,F.data.startswith("rep_monthly:"))
    dp.callback_query.register(show_peak,   F.data.startswith("rep_peak:"))
    dp.callback_query.register(export_csv,  F.data.startswith("rep_export_csv:"))
    dp.callback_query.register(export_pdf,  F.data.startswith("rep_export_pdf:"))
    dp.callback_query.register(filter_ui,   F.data.startswith("rep_filter:"))
