# backend/app/handlers/start.py
from __future__ import annotations
import os
import logging
from typing import Optional, cast, List, Tuple, Dict

from aiogram import F, Bot, Router, BaseMiddleware
from aiogram.filters import Command
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
    ChatPermissions,
)

from ..db import get_con  # for reports chat listing
from ..repositories.users import upsert_user, has_phone, get_language, set_language
from ..repositories.tenants import ensure_personal_tenant, link_user_to_tenant, get_user_tenant
from ..repositories.subscriptions import get_user_subscription_status
from ..repositories.chats import list_tenant_chats
from ..repositories.stats import get_last_days
from ..repositories.required import list_required_targets, list_group_targets  # global + per-group (simple fallback)
from ..repositories.activity import (
    get_messages_daily, get_dau_daily, get_top_talkers, get_peak_hour, get_most_active_user
)

from ..services.i18n import t, remember_language

logger = logging.getLogger(__name__)

router = Router()

OWNER_ID: Optional[int] = None
_owner_env = os.getenv("OWNER_ID", "").strip()
try:
    OWNER_ID = int(_owner_env) if _owner_env else None
except ValueError:
    OWNER_ID = None


# -------------- ACCESS POLICY (Europe-only + +888 anonymous) --------------
ALLOWED_PREFIXES = ["+888"]  # Telegram anonymous number

def _is_allowed(phone: Optional[str]) -> bool:
    """
    Allow:
      • +888 (Telegram anonymous)
      • Europe-only country codes: +30 … +59
    Block everything else (e.g. +7 Russia, +98 Iran, etc.).
    """
    if not phone:
        return False
    phone = phone.strip()
    # Anonymous number
    for p in ALLOWED_PREFIXES:
        if phone.startswith(p):
            return True
    # Block +7 range outright
    if phone.startswith("+7"):
        return False
    # Europe bucket check: +30..+59 (covers EU/EFTA & nearby European ranges we allow)
    if phone.startswith("+") and len(phone) >= 3 and phone[1:3].isdigit():
        try:
            two = int(phone[1:3])
            return 30 <= two <= 59
        except Exception:
            return False
    return False


# ---------------- UI ----------------

def owner_home_kb(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t("ui.owner.admin_panel", user_id=user_id), callback_data="admin_overview")],
        [InlineKeyboardButton(text=t("ui.owner.my_dashboard", user_id=user_id), callback_data="owner_dashboard")],
    ])

def user_dashboard_kb(user_id: int) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(text=t("dash.buttons.overview", user_id=user_id), callback_data="tenant_overview"),
            InlineKeyboardButton(text=t("dash.buttons.linked_chats", user_id=user_id), callback_data="tenant_chats"),
        ],
        [
            InlineKeyboardButton(text=t("dash.buttons.analytics", user_id=user_id), callback_data="tenant_analytics"),
            InlineKeyboardButton(text=t("dash.buttons.reports", user_id=user_id), callback_data="tenant_reports"),
        ],
        [
            InlineKeyboardButton(text=t("dash.buttons.campaigns", user_id=user_id), callback_data="tenant_campaigns"),
            InlineKeyboardButton(text=t("dash.buttons.group_tools", user_id=user_id), callback_data="tenant_group_tools"),
        ],
        [
            InlineKeyboardButton(text=t("dash.buttons.help", user_id=user_id), callback_data="help"),
            InlineKeyboardButton(text=t("dash.buttons.settings", user_id=user_id), callback_data="tenant_settings"),
        ],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

def request_phone_kb(user_id: int) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=t("request_phone.button", user_id=user_id), request_contact=True)]],
        resize_keyboard=True,
        one_time_keyboard=True,
        selective=True,
    )

# --------- normalization helpers (for legacy list[str]) ----------

def _parse_simple_target(s: str) -> Dict[str, Optional[str]]:
    """
    Turn a legacy stored string (from required_membership.target) into a unified dict:
      { 'target': '@name' | '-100id' | None, 'join_url': 'https://t.me/...' | None }
    """
    s = (s or "").strip()
    if not s:
        return {"target": None, "join_url": None}
    low = s.lower()

    # raw URL
    if low.startswith("http://") or low.startswith("https://"):
        return {"target": None, "join_url": s}

    # t.me/... forms
    if low.startswith("t.me/") or low.startswith("https://t.me/") or low.startswith("http://t.me/"):
        try:
            uname = s.split("/", 3)[-1].strip()
            if uname.startswith("+") or uname.startswith("joinchat/"):
                # joinchat / +CODE type → treat as URL
                return {"target": None, "join_url": f"https://t.me/{uname}"}
            if uname:
                # username → @channel
                return {"target": f"@{uname.lstrip('@')}", "join_url": None}
        except Exception:
            pass

    # direct @username or -100id
    if s.startswith("@") or s.startswith("-100"):
        return {"target": s, "join_url": None}

    # bare username
    if s.isalnum() or s.replace("_", "").isalnum():
        return {"target": f"@{s}", "join_url": None}

    return {"target": None, "join_url": None}


async def _list_required_targets_full() -> List[Dict[str, Optional[str]]]:
    """
    Unified accessor for global required targets.

    Current schema stores only `target` (text). We read the raw strings via
    repositories.required.list_required_targets() and normalize into:

        [{ "target": "@MyChannel" | "-100...", "join_url": "https://t.me/..." | None }, ...]

    This keeps your behavior but avoids any dynamic imports.
    """
    simple = await list_required_targets()
    return [_parse_simple_target(s) for s in simple]

# ----- keyboards -----

def force_join_kb(user_id: int, targets: List[Dict[str, Optional[str]]]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for row in targets:
        tgt = (row.get("target") or "").strip() if row.get("target") else ""
        ju  = (row.get("join_url") or "").strip() if row.get("join_url") else ""
        url: Optional[str] = None
        if ju:
            url = ju
        elif tgt.startswith("@"):
            url = f"https://t.me/{tgt[1:]}"
        elif tgt.lower().startswith(("http://", "https://")):
            url = tgt
        label_target = tgt if tgt else (ju if ju else "channel")
        label = t("force_join.open_target", user_id=user_id, target=label_target)
        if url:
            rows.append([InlineKeyboardButton(text=label, url=url)])
    rows.insert(0, [InlineKeyboardButton(text=t("force_join.ijoined_button", user_id=user_id), callback_data="force_check_global")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def force_join_kb_group(user_id: int, chat_id: int, targets: List[Dict[str, Optional[str]]]) -> InlineKeyboardMarkup:
    btn_rows: List[List[InlineKeyboardButton]] = []
    for row in targets:
        tgt = (row.get("target") or "").strip() if row.get("target") else ""
        ju  = (row.get("join_url") or "").strip() if row.get("join_url") else ""
        url: Optional[str] = None
        if ju:
            url = ju
        elif tgt.startswith("@"):
            url = f"https://t.me/{tgt[1:]}"
        elif tgt.lower().startswith(("http://", "https://")):
            url = tgt
        label_target = tgt if tgt else (ju if ju else "channel")
        label = t("force_join.open_target", user_id=user_id, target=label_target)
        if url:
            btn_rows.append([InlineKeyboardButton(text=label, url=url)])
    btn_rows.insert(0, [InlineKeyboardButton(text=t("force_join.ijoined_button", user_id=user_id), callback_data=f"force_check_group:{chat_id}")])
    return InlineKeyboardMarkup(inline_keyboard=btn_rows)

# ---------------- Helpers ----------------

def _is_owner(uid: Optional[int]) -> bool:
    return OWNER_ID is not None and uid == OWNER_ID

async def _is_member(bot: Bot, target: str, user_id: int) -> bool:
    tval = (target or "").strip()
    if not tval:
        return True
    if tval.lower().startswith(("http://", "https://")):
        return True
    try:
        chat_ref = tval
        if tval.startswith("-100"):
            chat_ref = int(tval)
        member = await bot.get_chat_member(chat_ref, user_id)
        return member.status in ("creator", "administrator", "member", "restricted")
    except Exception:
        return False

async def _enforce_global_requirements(bot: Bot, user_id: int) -> bool:
    targets = await _list_required_targets_full()
    if not targets:
        return True
    for row in targets:
        tgt = (row.get("target") or "").strip() if row.get("target") else ""
        if tgt and not await _is_member(bot, tgt, user_id):
            await bot.send_message(
                user_id,
                t("force_join.prompt_private_aware", user_id=user_id),
                reply_markup=force_join_kb(user_id, targets),
            )
            return False
    return True

async def _ensure_user_and_tenant(msg: Message) -> str:
    u = msg.from_user
    await upsert_user(
        tg_id=u.id,
        first_name=u.first_name,
        last_name=u.last_name,
        username=u.username,
        language_code=u.language_code,
        phone_e164=None,
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
        f"{t('dashboard.title', user_id=tg_id)}\n"
        f"{t('dashboard.plan', user_id=tg_id, plan=plan)}\n"
        "\n"
        f"{t('dashboard.tip', user_id=tg_id)}"
    )
    await bot.send_message(chat_id, text, reply_markup=user_dashboard_kb(tg_id))

def _analytics_list_kb(user_id: int, chats: List[Tuple[int, str, str]]) -> InlineKeyboardMarkup:
    rows = []
    for cid, ctype, title in chats[:30]:
        rows.append([InlineKeyboardButton(text=f"{title or cid} ({ctype})", callback_data=f"tenant_analytics_view:{cid}")])
    rows.append([InlineKeyboardButton(text=t("common.back", user_id=user_id), callback_data="tenant_overview")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

async def _list_user_chats_simple(user_tg_id: int) -> list[dict]:
    async with get_con() as con:
        rows = await con.fetch(
            """
            SELECT c.tg_chat_id, c.title, c.type
            FROM public.chats c
            JOIN public.user_tenants ut ON ut.tenant_id = c.tenant_id
            WHERE ut.tg_id = $1
            ORDER BY c.created_at DESC
            LIMIT 25
            """,
            user_tg_id
        )
    return [dict(r) for r in rows]

def _reports_kb(user_id: int, chats: list[dict]) -> InlineKeyboardMarkup:
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    kb = InlineKeyboardBuilder()
    for r in chats:
        chat_id = int(r["tg_chat_id"])
        title = r.get("title") or str(chat_id)
        kb.button(text=t("reports.chat_button", user_id=user_id, title=title, type=r.get("type", "")), callback_data=f"rep:chat:{chat_id}:30")
    kb.adjust(1)
    return kb.as_markup()

async def _edit_or_send(cb: CallbackQuery, text: str, kb=None):
    try:
        if cb.message:
            if cb.message.text != text:
                await cb.message.edit_text(text, reply_markup=kb)
            else:
                if kb:
                    try:
                        await cb.message.edit_reply_markup(reply_markup=kb)
                    except Exception:
                        pass
        await cb.answer()
    except Exception:
        try:
            await cb.message.answer(text, reply_markup=kb)
        except Exception:
            pass
        await cb.answer()

# --- Clear any leftover ReplyKeyboardMarkup (old “Dashboard/Help/…”) ---
async def _clear_reply_keyboard(bot: Bot, chat_id: int):
    try:
        await bot.send_message(chat_id, " ", reply_markup=ReplyKeyboardRemove())
    except Exception:
        pass

# ---------------- GLOBAL FORCE-JOIN GUARD (runs before any private handler) ----------------
class PrivateForceJoinGuard(BaseMiddleware):
    BYPASS_CB_PREFIXES = ("force_check_global", "settings:set_lang:", "settings:", "admin_")
    BYPASS_CMDS = ("/start",)

    async def __call__(self, handler, event, data):
        bot: Bot = data["bot"]

        # PRIVATE messages
        if isinstance(event, Message):
            m: Message = event
            if not m.from_user or m.chat.type != "private":
                return await handler(event, data)
            if _is_owner(m.from_user.id):
                return await handler(event, data)
            if any((m.text or "").strip().startswith(cmd) for cmd in self.BYPASS_CMDS):
                return await handler(event, data)

            targets = await _list_required_targets_full()
            if targets:
                for row in targets:
                    tgt = (row.get("target") or "").strip() if row.get("target") else ""
                    if not tgt:
                        continue
                    if not await _is_member(bot, tgt, m.from_user.id):
                        await bot.send_message(
                            m.chat.id,
                            t("force_join.prompt_private_aware", user_id=m.from_user.id),
                            reply_markup=force_join_kb(m.from_user.id, targets),
                        )
                        return  # stop downstream handlers

        # PRIVATE callback queries
        if isinstance(event, CallbackQuery):
            cb: CallbackQuery = event
            if not cb.from_user:
                return await handler(event, data)
            chat_type = (cb.message.chat.type if cb.message and cb.message.chat else "private")
            if chat_type != "private":
                return await handler(event, data)
            if _is_owner(cb.from_user.id):
                return await handler(event, data)
            d = (cb.data or "")
            if d.startswith(self.BYPASS_CB_PREFIXES):
                return await handler(event, data)

            targets = await _list_required_targets_full()
            if targets:
                for row in targets:
                    tgt = (row.get("target") or "").strip() if row.get("target") else ""
                    if not tgt:
                        continue
                    if not await _is_member(bot, tgt, cb.from_user.id):
                        try:
                            if cb.message:
                                await cb.message.edit_text(
                                    t("force_join.prompt_private_aware", user_id=cb.from_user.id),
                                    reply_markup=force_join_kb(cb.from_user.id, targets),
                                )
                            else:
                                await bot.send_message(
                                    cb.from_user.id,
                                    t("force_join.prompt_private_aware", user_id=cb.from_user.id),
                                    reply_markup=force_join_kb(cb.from_user.id, targets),
                                )
                        except Exception:
                            await bot.send_message(
                                cb.from_user.id,
                                t("force_join.prompt_private_aware", user_id=cb.from_user.id),
                                reply_markup=force_join_kb(cb.from_user.id, targets),
                            )
                        await cb.answer(t("force_join.not_joined_alert", user_id=cb.from_user.id), show_alert=True)
                        return

        return await handler(event, data)

# Attach guard to THIS router too
router.message.outer_middleware(PrivateForceJoinGuard())
router.callback_query.outer_middleware(PrivateForceJoinGuard())

# ---------------- Settings (language flow) ----------------

def settings_kb(user_id: int) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=t("settings.buttons.language", user_id=user_id), callback_data="settings:lang")],
        [InlineKeyboardButton(text=t("settings.buttons.back", user_id=user_id), callback_data="tenant_overview")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

def language_kb() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=t("lang.buttons.en", lang="en"), callback_data="settings:set_lang:en")],
        [InlineKeyboardButton(text=t("lang.buttons.fr", lang="fr"), callback_data="settings:set_lang:fr")],
        [InlineKeyboardButton(text="⬅️", callback_data="settings:back")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

async def render_settings(cb_or_msg, user_id: int):
    lang = await get_language(user_id)
    if not lang:
        lc = ((cb_or_msg.from_user.language_code if hasattr(cb_or_msg, "from_user") else None) or "en").lower()
        lang = "fr" if lc.startswith("fr") else "en"
        await set_language(user_id, lang)
    remember_language(user_id, lang)

    lang_name = t(f"lang.names.{lang}", lang=lang)
    text = f"⚙️ <b>{t('settings.title', user_id=user_id)}</b>\n\n" + t("settings.current_language", user_id=user_id, lang_name=lang_name)
    kb = settings_kb(user_id)
    if isinstance(cb_or_msg, CallbackQuery):
        await _edit_or_send(cb_or_msg, text, kb)
    else:
        await cb_or_msg.answer(text, reply_markup=kb)

# ---------------- Register ----------------

@router.message(Command("start"))
async def start_cmd(msg: Message):
    bot = cast(Bot, msg.bot)
    if not msg.from_user:
        return

    # Clear any lingering ReplyKeyboardMarkup (old keyboards)
    await _clear_reply_keyboard(bot, msg.chat.id)

    if _is_owner(msg.from_user.id):
        await bot.send_message(
            msg.chat.id,
            t("start.owner_welcome", user_id=msg.from_user.id),
            reply_markup=owner_home_kb(msg.from_user.id)
        )
        return

    # If we don't have a phone yet, ask once
    if not await has_phone(msg.from_user.id):
        await bot.send_message(
            msg.chat.id,
            t("request_phone.prompt", user_id=msg.from_user.id),
            reply_markup=request_phone_kb(msg.from_user.id)
        )
        return

    # User is already verified (phone stored) → proceed normally
    await _ensure_user_and_tenant(msg)

    # pre-warm language cache
    try:
        lang = await get_language(msg.from_user.id)
        if lang:
            remember_language(msg.from_user.id, lang)
    except Exception:
        pass

    if not await _enforce_global_requirements(bot, msg.from_user.id):
        return

    await _render_dashboard(bot, msg.chat.id, msg.from_user.id)


@router.callback_query(F.data == "owner_dashboard")
async def owner_dashboard(cb: CallbackQuery):
    if not cb.from_user or not _is_owner(cb.from_user.id):
        await cb.answer(); return
    await _ensure_user_and_tenant(cb.message)
    await _render_dashboard(cast(Bot, cb.bot), cb.message.chat.id, cb.from_user.id)
    await cb.answer()


@router.callback_query(F.data == "force_check_global")
async def force_check_global(cb: CallbackQuery):
    if not cb.from_user: await cb.answer(); return
    bot = cast(Bot, cb.bot)
    if _is_owner(cb.from_user.id):
        try:
            if cb.message:
                await cb.message.edit_text(t("force_join.verified", user_id=cb.from_user.id), reply_markup=None)
        except Exception:
            pass
        await _render_dashboard(bot, cb.message.chat.id, cb.from_user.id)
        await cb.answer(); return

    ok = await _enforce_global_requirements(bot, cb.from_user.id)
    if not ok:
        await cb.answer(t("force_join.not_joined_alert", user_id=cb.from_user.id), show_alert=True)
        return
    try:
        if cb.message:
            await cb.message.edit_text(t("force_join.verified", user_id=cb.from_user.id), reply_markup=None)
    except Exception:
        pass
    await _render_dashboard(bot, cb.message.chat.id, cb.from_user.id)
    await cb.answer()


# ---------- Per-group re-check / unmute ----------
@router.callback_query(F.data.startswith("force_check_group:"))
async def force_check_group(cb: CallbackQuery):
    if not cb.from_user:
        await cb.answer()
        return
    bot = cast(Bot, cb.bot)
    parts = (cb.data or "").split(":")
    chat_id = int(parts[1]) if len(parts) >= 2 else 0
    targets = await list_group_targets(chat_id)  # [{target, join_url}]
    if not targets:
        try:
            if cb.message:
                await cb.message.edit_text(t("force_join.verified", user_id=cb.from_user.id), reply_markup=None)
        except Exception:
            pass
        await cb.answer()
        return

    for row in targets:
        tgt = (row.get("target") or "").strip()
        if not tgt:
            continue
        ok = await _is_member(bot, tgt, cb.from_user.id)
        if not ok:
            try:
                if cb.message:
                    await cb.message.edit_text(
                        t("force_join.group_still_need", user_id=cb.from_user.id),
                        reply_markup=force_join_kb_group(cb.from_user.id, chat_id, targets)
                    )
            except Exception:
                pass
            await cb.answer(t("force_join.not_joined_alert", user_id=cb.from_user.id), show_alert=True)
            return

    try:
        perms = ChatPermissions(
            can_send_messages=True,
            can_send_audios=True,
            can_send_documents=True,
            can_send_photos=True,
            can_send_videos=True,
            can_send_video_notes=True,
            can_send_voice_notes=True,
            can_send_polls=True,
            can_send_other_messages=True,
            can_add_web_page_previews=True
        )
        await bot.restrict_chat_member(chat_id, cb.from_user.id, permissions=perms)
    except Exception:
        pass

    try:
        if cb.message:
            await cb.message.edit_text(t("force_join.group_verified", user_id=cb.from_user.id), reply_markup=None)
    except Exception:
        pass
    await cb.answer()


@router.message(F.contact)
async def contact_shared(msg: Message):
    """
    This is the ONLY place we verify the phone → verification happens once.
    If it passes here, the user won't be asked again (we store the phone).
    """
    bot = cast(Bot, msg.bot)
    if not msg.from_user or not msg.contact:
        return

    # Normalize phone to E.164-ish
    raw = msg.contact.phone_number or ""
    phone_e164 = raw if raw.startswith("+") else f"+{raw}" if raw else None

    # Enforce access policy
    if not _is_allowed(phone_e164):
        try:
            await bot.send_message(msg.chat.id, t("access.denied_geofence", user_id=msg.from_user.id), reply_markup=ReplyKeyboardRemove())
        except Exception:
            pass
        return  # stop here — do NOT store / proceed

    # Allowed → store once and continue normally
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
    if not await _enforce_global_requirements(bot, u.id):
        return
    await bot.send_message(msg.chat.id, t("contact.thanks_in", user_id=u.id), reply_markup=ReplyKeyboardRemove())
    await _render_dashboard(bot, msg.chat.id, u.id)


@router.callback_query(F.data == "tenant_overview")
async def tenant_overview(cb: CallbackQuery):
    if not cb.from_user: await cb.answer(); return
    if not _is_owner(cb.from_user.id):
        if not await _enforce_global_requirements(cast(Bot, cb.bot), cb.from_user.id): return
    plan = await get_user_subscription_status(cb.from_user.id)
    text = f"{t('overview.title', user_id=cb.from_user.id)}\n" + t("overview.current_plan", user_id=cb.from_user.id, plan=plan)
    await _edit_or_send(cb, text, user_dashboard_kb(cb.from_user.id))


@router.callback_query(F.data == "tenant_chats")
async def tenant_chats_cb(cb: CallbackQuery):
    if not cb.from_user: await cb.answer(); return
    if not _is_owner(cb.from_user.id):
        if not await _enforce_global_requirements(cast(Bot, cb.bot), cb.from_user.id): return
    tenant_id = await get_user_tenant(cb.from_user.id)
    if not tenant_id: await cb.answer(t("errors.no_tenant", user_id=cb.from_user.id), show_alert=True); return
    chats = await list_tenant_chats(tenant_id)
    if not chats:
        text = f"{t('chats.linked_title', user_id=cb.from_user.id)}\n{t('chats.none_tip', user_id=cb.from_user.id)}"
    else:
        lines = [t("chats.linked_title", user_id=cb.from_user.id)]
        for cid, ctype, title in chats[:30]:
            lines.append(t("chats.item", user_id=cb.from_user.id, cid=cid, ctype=ctype, title=title))
        if len(chats) > 30:
            lines.append(t("chats.more", user_id=cb.from_user.id, n=len(chats)-30))
        text = "\n".join(lines)
    await _edit_or_send(cb, text, user_dashboard_kb(cb.from_user.id))


@router.callback_query(F.data == "tenant_analytics")
async def tenant_analytics_cb(cb: CallbackQuery):
    if not cb.from_user: await cb.answer(); return
    if not _is_owner(cb.from_user.id):
        if not await _enforce_global_requirements(cast(Bot, cb.bot), cb.from_user.id): return
    tenant_id = await get_user_tenant(cb.from_user.id)
    if not tenant_id: await cb.answer(t("errors.no_tenant", user_id=cb.from_user.id), show_alert=True); return
    chats = await list_tenant_chats(tenant_id)
    if not chats:
        await _edit_or_send(cb, t("analytics.none_chats", user_id=cb.from_user.id), user_dashboard_kb(cb.from_user.id)); return
    await _edit_or_send(cb, t("analytics.select_chat", user_id=cb.from_user.id), _analytics_list_kb(cb.from_user.id, chats))


@router.callback_query(F.data.startswith("tenant_analytics_view:"))
async def tenant_analytics_view(cb: CallbackQuery):
    if not cb.from_user: await cb.answer(); return
    if not _is_owner(cb.from_user.id):
        if not await _enforce_global_requirements(cast(Bot, cb.bot), cb.from_user.id): return
    parts = (cb.data or "").split(":")
    chat_id = int(parts[1]) if len(parts) >= 2 else 0

    rows = await get_last_days(chat_id, 30)
    lines = [t("analytics.title_30d", user_id=cb.from_user.id)]

    total_joins = sum(j for _, j, _ in rows)
    total_leaves = sum(l for _, _, l in rows)
    lines.append(t("analytics.total_joins", user_id=cb.from_user.id, n=total_joins))
    lines.append(t("analytics.total_leaves", user_id=cb.from_user.id, n=total_leaves))

    plan = await get_user_subscription_status(cb.from_user.id)
    is_pro = str(plan).lower() == "pro"

    if is_pro:
        msgs_7d = await get_messages_daily(chat_id, 7)
        dau_7d  = await get_dau_daily(chat_id, 7)
        peak    = await get_peak_hour(chat_id, days=30, tz='Europe/Helsinki')
        top1    = await get_most_active_user(chat_id, days=30)

        if msgs_7d:
            total_msgs = sum(int(c) for _, c in msgs_7d)
            lines.append(t("analytics.messages_7d", user_id=cb.from_user.id, n=total_msgs))
        if dau_7d:
            avg_dau = round(sum(int(c) for _, c in dau_7d) / max(len(dau_7d), 1), 1)
            lines.append(t("analytics.avg_dau_7d", user_id=cb.from_user.id, avg=avg_dau))
        if peak:
            hour_str = f"{peak[0]:02d}"
            lines.append(t("analytics.peak_hour", user_id=cb.from_user.id, hour=hour_str, count=peak[1]))
        if top1:
            lines.append(t("analytics.top_user_30d", user_id=cb.from_user.id, user=top1[0], count=top1[1]))
    else:
        lines.append(t("analytics.pro_required_note", user_id=cb.from_user.id))

    lines.append(t("analytics.last7_header", user_id=cb.from_user.id))
    for d, j, l in rows[:7]:
        lines.append(t("analytics.day_line", user_id=cb.from_user.id, date=d, joins=j, leaves=l))

    text = "\n".join(lines)
    await _edit_or_send(cb, text, user_dashboard_kb(cb.from_user.id))


@router.callback_query(F.data == "tenant_reports")
async def tenant_reports_cb(cb: CallbackQuery):
    if not cb.from_user:
        await cb.answer(); return
    if not _is_owner(cb.from_user.id):
        if not await _enforce_global_requirements(cast(Bot, cb.bot), cb.from_user.id):
            return

    chats = await _list_user_chats_simple(cb.from_user.id)
    if not chats:
        await _edit_or_send(cb, t("reports.none_chats", user_id=cb.from_user.id), user_dashboard_kb(cb.from_user.id))
        return

    await _edit_or_send(cb, t("reports.select_chat", user_id=cb.from_user.id), _reports_kb(cb.from_user.id, chats))


@router.callback_query(F.data == "group_tools")
async def group_tools(cb: CallbackQuery):
    plan = await get_user_subscription_status(cb.from_user.id)
    is_pro = str(plan).lower() == "pro"

    help_text = t("group_tools.help", user_id=cb.from_user.id)

    if is_pro:
        text = t("group_tools.pro_enabled_prefix", user_id=cb.from_user.id) + help_text
    else:
        text = t("group_tools.pro_required_prefix", user_id=cb.from_user.id) + help_text

    await cb.message.edit_text(text, reply_markup=None)
    await cb.answer()


@router.callback_query(F.data == "help")
async def help_cb(cb: CallbackQuery):
    text = t("help.title", user_id=cb.from_user.id) + t("help.body", user_id=cb.from_user.id)
    await _edit_or_send(cb, text, user_dashboard_kb(cb.from_user.id))


@router.callback_query(F.data == "tenant_settings")
async def tenant_settings_cb(cb: CallbackQuery):
    if not cb.from_user: await cb.answer(); return
    if not _is_owner(cb.from_user.id):
        if not await _enforce_global_requirements(cast(Bot, cb.bot), cb.from_user.id): return
    await render_settings(cb, cb.from_user.id)

@router.callback_query(F.data == "settings:lang")
async def cb_open_language(cb: CallbackQuery):
    if not cb.from_user:
        await cb.answer(); 
        return
    await _edit_or_send(cb, t("lang.title", user_id=cb.from_user.id), language_kb())

@router.callback_query(F.data.startswith("settings:set_lang:"))
async def cb_set_language(cb: CallbackQuery):
    if not cb.from_user:
        await cb.answer(); 
        return
    lang = (cb.data or "").split(":")[-1]
    if lang not in ("en", "fr"):
        await cb.answer(); 
        return
    await set_language(cb.from_user.id, lang)
    remember_language(cb.from_user.id, lang)
    lang_name = t(f"lang.names.{lang}", lang=lang)
    await _edit_or_send(cb, t("lang.saved", lang=lang, lang_name=lang_name), language_kb())

@router.callback_query(F.data == "settings:back")
async def cb_settings_back(cb: CallbackQuery):
    if not cb.from_user:
        await cb.answer(); 
        return
    await render_settings(cb, cb.from_user.id)
