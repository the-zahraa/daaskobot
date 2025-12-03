# backend/app/handlers/start.py
from __future__ import annotations
import os
import logging
from typing import Optional, cast, List, Tuple, Dict, Any
import asyncio

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

from ..db import get_con
from ..repositories.pending_verification import mark_verified_for_user
from ..repositories.users import upsert_user, has_phone, get_language, set_language
from ..repositories.tenants import ensure_personal_tenant, link_user_to_tenant, get_user_tenant
from ..repositories.subscriptions import get_user_subscription_status
from ..repositories.chats import list_tenant_chats
from ..repositories.stats import get_last_days
from ..repositories.required import list_required_targets, list_group_targets
from ..repositories.activity import (
    get_messages_daily,
    get_dau_daily,
    get_top_talkers,
    get_peak_hour,
    get_most_active_user,
    get_active_users_window,   # NEW
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

# ---------------- Plan helpers (fix Pro detection) ----------------
def _normalize_plan(plan: Any) -> str:
    if plan is None:
        return ""
    if isinstance(plan, str):
        return plan.strip().lower()
    if isinstance(plan, dict):
        for k in ("plan", "code", "tier", "name"):
            v = plan.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip().lower()
        return str(plan).strip().lower()
    for attr in ("plan", "code", "tier", "name"):
        v = getattr(plan, attr, None)
        if isinstance(v, str) and v.strip():
            return v.strip().lower()
    return str(plan).strip().lower()

async def _is_pro_user(user_id: int) -> bool:
    plan = await get_user_subscription_status(user_id)
    code = _normalize_plan(plan)
    return code in {
        "pro", "pro_week", "pro_month", "pro_year",
        "pro_plus", "premium", "paid", "tier_pro", "owner_pro"
    }

# -------------- ACCESS POLICY --------------
# -------------- ACCESS POLICY --------------



ALLOWED_EU_MIN = 30   # inclusive
ALLOWED_EU_MAX = 59   # inclusive

def _is_allowed(phone: Optional[str]) -> bool:
    """
    Allow:
      • EU (+30..+59)
      • +888 anonymous numbers
    Block everything else (including +98 Iran).
    """
    if not phone:
        return False

    phone = phone.strip()

    # Always allow +888 (anonymous Telegram numbers)
    if phone.startswith("+888"):
        return True

    # EU-only window (+30..+59)
    if phone.startswith("+") and len(phone) >= 3 and phone[1:3].isdigit():
        try:
            cc = int(phone[1:3])
        except ValueError:
            return False
        return ALLOWED_EU_MIN <= cc <= ALLOWED_EU_MAX

    return False



# ---------------- UI ----------------
def owner_home_kb(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t("ui.owner.admin_panel", user_id=user_id), callback_data="admin_overview")],
        [InlineKeyboardButton(text=t("ui.owner.my_dashboard", user_id=user_id), callback_data="owner_dashboard")],
    ])

def user_dashboard_kb(user_id: int) -> InlineKeyboardMarkup:
    rows = [
        # NEW: big CTA button
        [
            InlineKeyboardButton(
                text=t("dash.buttons.get_started", user_id=user_id),
                callback_data="dash_get_started",
            )
        ],
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
            # 🔒 Require Channels → open in-bot group tools wizard
            InlineKeyboardButton(text=t("dash.buttons.force_join", user_id=user_id), callback_data="tenant_group_tools"),
        ],
        [
            # 📣 Mass DM → open Mass DM panel directly
            InlineKeyboardButton(text=t("dash.buttons.mass_dm", user_id=user_id), callback_data="massdm_home"),
            InlineKeyboardButton(text=t("dash.buttons.upgrade_pro", user_id=user_id), callback_data="pro_open"),
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

# --------- normalization helpers ----------
def _parse_simple_target(s: str) -> Dict[str, Optional[str]]:
    s = (s or "").strip()
    if not s:
        return {"target": None, "join_url": None}
    low = s.lower()
    if low.startswith("http://") or low.startswith("https://"):
        return {"target": None, "join_url": s}
    if low.startswith("t.me/") or low.startswith("https://t.me/") or low.startswith("http://t.me/"):
        try:
            uname = s.split("/", 3)[-1].strip()
            if uname.startswith("+") or uname.startswith("joinchat/"):
                return {"target": None, "join_url": f"https://t.me/{uname}"}
            if uname:
                return {"target": f"@{uname.lstrip('@')}", "join_url": None}
        except Exception:
            pass
    if s.startswith("@") or s.startswith("-100"):
        return {"target": s, "join_url": None}
    if s.isalnum() or s.replace("_", "").isalnum():
        return {"target": f"@{s}", "join_url": None}
    return {"target": None, "join_url": None}

async def _list_required_targets_full() -> List[Dict[str, Optional[str]]]:
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
    """
    Reports chat selector + Back button.
    """
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    kb = InlineKeyboardBuilder()
    for r in chats:
        chat_id = int(r["tg_chat_id"])
        title = r.get("title") or str(chat_id)
        kb.button(
            text=t("reports.chat_button", user_id=user_id, title=title, type=r.get("type", "")),
            callback_data=f"rep:chat:{chat_id}:30",
        )
    # NEW: Back button to overview
    kb.button(text=t("common.back", user_id=user_id), callback_data="tenant_overview")
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

async def _clear_reply_keyboard(bot: Bot, chat_id: int):
    try:
        await bot.send_message(chat_id, " ", reply_markup=ReplyKeyboardRemove())
    except Exception:
        pass
    
async def _delete_message_later(bot: Bot, chat_id: int, message_id: int, delay: int = 120) -> None:
    """
    Delete a message after 'delay' seconds.
    Used so verification / welcome messages fade automatically.
    """
    await asyncio.sleep(delay)
    try:
        await bot.delete_message(chat_id, message_id)
    except Exception:
        # not critical if it fails (e.g. already deleted)
        pass


# ---------------- Middleware ----------------
class PrivateForceJoinGuard(BaseMiddleware):
    BYPASS_CB_PREFIXES = ("force_check_global", "settings:set_lang:", "settings:", "admin_")
    BYPASS_CMDS = ("/start",)

    async def __call__(self, handler, event, data):
        bot: Bot = data["bot"]

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
                        return

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

router.message.outer_middleware(PrivateForceJoinGuard())
router.callback_query.outer_middleware(PrivateForceJoinGuard())

# ---------------- Settings ----------------
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

    u = msg.from_user

    # -------- Deep-link payload handling (verify_<user_id>) --------
    text = (msg.text or "").strip()
    payload = ""
    if text.startswith("/start"):
        parts = text.split(maxsplit=1)
        if len(parts) == 2:
            # e.g. "/start verify_5044723871"
            payload = parts[1].split()[0].strip()

    if payload.startswith("verify_"):
        # Extract user id from payload
        target_id = None
        try:
            target_id = int(payload.split("_", 1)[1])
        except Exception:
            target_id = None

        if target_id is not None and target_id != u.id:
            # Someone else clicked another user's link -> sassy reply and stop
            await bot.send_message(
                msg.chat.id,
                "😏 This verification link isn’t for you.",
            )
            return
        # If target_id == u.id, continue as normal (ask for phone, etc.)

    await _clear_reply_keyboard(bot, msg.chat.id)

    if _is_owner(msg.from_user.id):
        await bot.send_message(
            msg.chat.id,
            t("start.owner_welcome", user_id=msg.from_user.id),
            reply_markup=owner_home_kb(msg.from_user.id)
        )
        return

    if not await has_phone(msg.from_user.id):
        await bot.send_message(
            msg.chat.id,
            t("request_phone.prompt", user_id=msg.from_user.id),
            reply_markup=request_phone_kb(msg.from_user.id)
        )
        return

    await _ensure_user_and_tenant(msg)

    try:
        lang = await get_language(msg.from_user.id)
        if lang:
            remember_language(msg.from_user.id, lang)
    except Exception:
        pass

    if not await _enforce_global_requirements(bot, msg.from_user.id):
        return

    await _render_dashboard(bot, msg.chat.id, msg.from_user.id)

    bot = cast(Bot, msg.bot)
    if not msg.from_user:
        return

    await _clear_reply_keyboard(bot, msg.chat.id)

    if _is_owner(msg.from_user.id):
        await bot.send_message(
            msg.chat.id,
            t("start.owner_welcome", user_id=msg.from_user.id),
            reply_markup=owner_home_kb(msg.from_user.id)
        )
        return

    if not await has_phone(msg.from_user.id):
        await bot.send_message(
            msg.chat.id,
            t("request_phone.prompt", user_id=msg.from_user.id),
            reply_markup=request_phone_kb(msg.from_user.id)
        )
        return

    await _ensure_user_and_tenant(msg)

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

@router.callback_query(F.data == "dash_get_started")
async def dash_get_started(cb: CallbackQuery):
    if not cb.from_user:
        await cb.answer(); return
    bot = cast(Bot, cb.bot)
    try:
        me = await bot.get_me()
        deep_link = f"https://t.me/{me.username}?startgroup=new"
    except Exception:
        deep_link = ""
    text = t("dash.get_started", user_id=cb.from_user.id, link=deep_link)
    await _edit_or_send(cb, text, user_dashboard_kb(cb.from_user.id))

@router.callback_query(F.data == "force_check_global")
async def force_check_global(cb: CallbackQuery):
    if not cb.from_user:
        await cb.answer()
        return
    bot = cast(Bot, cb.bot)
    if _is_owner(cb.from_user.id):
        try:
            if cb.message:
                await cb.message.edit_text(t("force_join.verified", user_id=cb.from_user.id), reply_markup=None)
        except Exception:
            pass
        await _render_dashboard(bot, cb.message.chat.id, cb.from_user.id)
        await cb.answer()
        return

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

@router.callback_query(F.data.startswith("force_check_group:"))
async def force_check_group(cb: CallbackQuery):
    if not cb.from_user:
        await cb.answer()
        return
    bot = cast(Bot, cb.bot)
    parts = (cb.data or "").split(":")
    chat_id = int(parts[1]) if len(parts) >= 2 else 0
    targets = await list_group_targets(chat_id)
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
    bot = cast(Bot, msg.bot)
    if not msg.from_user or not msg.contact:
        return

    raw = msg.contact.phone_number or ""
    phone_e164 = raw if raw.startswith("+") else f"+{raw}" if raw else None

    # Geofence: only EU +30..+59 and +888 (anon). Everything else (incl. +98) is blocked.
    if not _is_allowed(phone_e164):
        try:
            await bot.send_message(
                msg.chat.id,
                t("access.denied_geofence", user_id=msg.from_user.id),
                reply_markup=ReplyKeyboardRemove(),
            )
        except Exception:
            pass
        return

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

    # User has a valid stored phone -> mark as verified in pending_verifications
    chat_ids = await mark_verified_for_user(u.id)

    # For each chat where they were pending, unmute and send a welcome message
    if chat_ids:
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
            can_add_web_page_previews=True,
        )
        # Build a simple display name
        first = (u.first_name or "").strip()
        last = (u.last_name or "").strip()
        username = (u.username or "").strip()
        display_name = (first + " " + last).strip() or (f"@{username}" if username else "user")
        mention = f'<a href="tg://user?id={u.id}">{display_name}</a>'

        for cid in chat_ids:
            try:
                # Unmute
                await bot.restrict_chat_member(cid, u.id, permissions=perms)
            except Exception:
                pass
            try:
                # Welcome message that auto-fades
                welcome = await bot.send_message(
                    cid,
                    f"✅ {mention} is now verified and can talk here.",
                )
                asyncio.create_task(
                    _delete_message_later(bot, cid, welcome.message_id, delay=120)
                )
            except Exception:
                pass

    await _ensure_user_and_tenant(msg)
    if not await _enforce_global_requirements(bot, u.id):
        return

    await bot.send_message(
        msg.chat.id,
        t("contact.thanks_in", user_id=u.id),
        reply_markup=ReplyKeyboardRemove(),
    )
    await _render_dashboard(bot, msg.chat.id, u.id)

    bot = cast(Bot, msg.bot)
    if not msg.from_user or not msg.contact:
        return

    raw = msg.contact.phone_number or ""
    phone_e164 = raw if raw.startswith("+") else f"+{raw}" if raw else None

    # Geofence: only +30..+59. Everything else (including +888) is blocked.
    if not _is_allowed(phone_e164):
        try:
            await bot.send_message(
                msg.chat.id,
                t("access.denied_geofence", user_id=msg.from_user.id),
                reply_markup=ReplyKeyboardRemove(),
            )
        except Exception:
            pass
        return

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

        # User has a valid stored phone -> mark as verified in pending_verifications
    chat_ids = await mark_verified_for_user(u.id)

    # Unmute user in chats where verification was pending
    for cid in chat_ids:
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
                can_add_web_page_previews=True,
            )
            await bot.restrict_chat_member(cid, u.id, permissions=perms)
        except Exception as e:
            logger.warning("Failed to unrestrict verified user=%s in chat=%s: %s", u.id, cid, e)

    await _ensure_user_and_tenant(msg)
    if not await _enforce_global_requirements(bot, u.id):
        return

    await bot.send_message(
        msg.chat.id,
        t("contact.thanks_in", user_id=u.id),
        reply_markup=ReplyKeyboardRemove(),
    )
    await _render_dashboard(bot, msg.chat.id, u.id)

@router.callback_query(F.data == "tenant_overview")
async def tenant_overview(cb: CallbackQuery):
    if not cb.from_user:
        await cb.answer()
        return
    if not _is_owner(cb.from_user.id):
        if not await _enforce_global_requirements(cast(Bot, cb.bot), cb.from_user.id):
            return
    plan = await get_user_subscription_status(cb.from_user.id)
    text = f"{t('overview.title', user_id=cb.from_user.id)}\n" + t("overview.current_plan", user_id=cb.from_user.id, plan=plan)
    await _edit_or_send(cb, text, user_dashboard_kb(cb.from_user.id))

@router.callback_query(F.data == "tenant_chats")
async def tenant_chats_cb(cb: CallbackQuery):
    if not cb.from_user:
        await cb.answer()
        return
    if not _is_owner(cb.from_user.id):
        if not await _enforce_global_requirements(cast(Bot, cb.bot), cb.from_user.id):
            return
    tenant_id = await get_user_tenant(cb.from_user.id)
    if not tenant_id:
        await cb.answer(t("errors.no_tenant", user_id=cb.from_user.id), show_alert=True)
        return
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
    if not cb.from_user:
        await cb.answer()
        return
    if not _is_owner(cb.from_user.id):
        if not await _enforce_global_requirements(cast(Bot, cb.bot), cb.from_user.id):
            return
    tenant_id = await get_user_tenant(cb.from_user.id)
    if not tenant_id:
        await cb.answer(t("errors.no_tenant", user_id=cb.from_user.id), show_alert=True)
        return
    chats = await list_tenant_chats(tenant_id)
    if not chats:
        await _edit_or_send(cb, t("analytics.none_chats", user_id=cb.from_user.id), user_dashboard_kb(cb.from_user.id))
        return
    await _edit_or_send(cb, t("analytics.select_chat", user_id=cb.from_user.id), _analytics_list_kb(cb.from_user.id, chats))

@router.callback_query(F.data.startswith("tenant_analytics_view:"))
async def tenant_analytics_view(cb: CallbackQuery):
    if not cb.from_user:
        await cb.answer()
        return
    if not _is_owner(cb.from_user.id):
        if not await _enforce_global_requirements(cast(Bot, cb.bot), cb.from_user.id):
            return

    parts = (cb.data or "").split(":")
    chat_id = int(parts[1]) if len(parts) >= 2 else 0

    rows = await get_last_days(chat_id, 30)
    lines = [t("analytics.title_30d", user_id=cb.from_user.id)]

    total_joins = sum(j for _, j, _ in rows)
    total_leaves = sum(l for _, _, l in rows)
    lines.append(t("analytics.total_joins", user_id=cb.from_user.id, n=total_joins))
    lines.append(t("analytics.total_leaves", user_id=cb.from_user.id, n=total_leaves))

    if await _is_pro_user(cb.from_user.id):
        # Existing KPIs
        msgs_7d = await get_messages_daily(chat_id, 7)
        dau_7d  = await get_dau_daily(chat_id, 7)
        peak    = await get_peak_hour(chat_id, days=30, tz='Europe/Helsinki')
        top1    = await get_most_active_user(chat_id, days=30)

        if msgs_7d:
            total_msgs_7d = sum(int(c) for _, c in msgs_7d)
            lines.append(t("analytics.messages_7d", user_id=cb.from_user.id, n=total_msgs_7d))
        else:
            total_msgs_7d = 0

        if dau_7d:
            avg_dau = round(sum(int(c) for _, c in dau_7d) / max(len(dau_7d), 1), 1)
            lines.append(t("analytics.avg_dau_7d", user_id=cb.from_user.id, avg=avg_dau))

        if peak:
            hour_str = f"{peak[0]:02d}"
            lines.append(t("analytics.peak_hour", user_id=cb.from_user.id, hour=hour_str, count=peak[1]))

        if top1:
            lines.append(t("analytics.top_user_30d", user_id=cb.from_user.id, user=top1[0], count=top1[1]))

        # NEW: last-active 7 / 30 / 90 days
        active_7  = await get_active_users_window(chat_id, 7)
        active_30 = await get_active_users_window(chat_id, 30)
        active_90 = await get_active_users_window(chat_id, 90)

        lines.append(t("analytics.active_7", user_id=cb.from_user.id, n=active_7))
        lines.append(t("analytics.active_30", user_id=cb.from_user.id, n=active_30))
        lines.append(t("analytics.active_90", user_id=cb.from_user.id, n=active_90))

        # NEW: simple insight – average messages per active user per day over last 30 days
        msgs_30d = await get_messages_daily(chat_id, 30)
        total_msgs_30d = sum(int(c) for _, c in msgs_30d) if msgs_30d else 0
        if active_30 > 0 and total_msgs_30d > 0:
            avg_per_active_per_day = round(total_msgs_30d / (active_30 * 30), 1)
            lines.append(t("analytics.insight_avg_msgs", user_id=cb.from_user.id, avg=avg_per_active_per_day))
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
        await cb.answer()
        return
    if not _is_owner(cb.from_user.id):
        if not await _enforce_global_requirements(cast(Bot, cb.bot), cb.from_user.id):
            return

    chats = await _list_user_chats_simple(cb.from_user.id)
    if not chats:
        await _edit_or_send(cb, t("reports.none_chats", user_id=cb.from_user.id), user_dashboard_kb(cb.from_user.id))
        return

    await _edit_or_send(cb, t("reports.select_chat", user_id=cb.from_user.id), _reports_kb(cb.from_user.id, chats))

# --- Feature explainers (Pro gating honoured) ---
@router.callback_query(F.data == "feature_card_force_join")
async def feature_card_force_join(cb: CallbackQuery):
    is_pro = await _is_pro_user(cb.from_user.id)
    body = (
        "Require users to join your channels before speaking.\n"
        "• Auto-mute new members\n"
        "• DM with join buttons\n"
        "• One-tap unmute after join\n\n"
    )
    if is_pro:
        text = "🔒 <b>Require Channels</b>\n\n" + body + "You have Pro ✅. Use the <b>🔒 Require Channels</b> button in the dashboard to configure it."
    else:
        text = "🔒 <b>Require Channels</b>\n\n" + body + "Available in <b>Pro</b>. Use /pro or the ⭐ button to upgrade."
    await _edit_or_send(cb, text, user_dashboard_kb(cb.from_user.id))

@router.callback_query(F.data == "feature_card_mass_dm")
async def feature_card_mass_dm(cb: CallbackQuery):
    is_pro = await _is_pro_user(cb.from_user.id)
    body = (
        "DM the audience who joined with your referral link.\n"
        "• Filter by phone / username / name length\n"
        "• Sends in safe batches\n\n"
    )
    if is_pro:
        text = "📣 <b>Mass DM</b>\n\n" + body + "You have Pro ✅. Use the <b>📣 Mass DM</b> button in the dashboard to send campaigns."
    else:
        text = "📣 <b>Mass DM</b>\n\n" + body + "Available in <b>Pro</b>. Use /pro or the ⭐ button to upgrade."
    await _edit_or_send(cb, text, user_dashboard_kb(cb.from_user.id))

@router.callback_query(F.data == "help")
async def help_cb(cb: CallbackQuery):
    text = t("help.title", user_id=cb.from_user.id) + t("help.body", user_id=cb.from_user.id)
    await _edit_or_send(cb, text, user_dashboard_kb(cb.from_user.id))

@router.callback_query(F.data == "tenant_settings")
async def tenant_settings_cb(cb: CallbackQuery):
    if not cb.from_user:
        await cb.answer()
        return
    if not _is_owner(cb.from_user.id):
        if not await _enforce_global_requirements(cast(Bot, cb.bot), cb.from_user.id):
            return
    await render_settings(cb, cb.from_user.id)

@router.callback_query(F.data == "settings:lang")
async def cb_open_language(cb: CallbackQuery):
    if not cb.from_user:
        await cb.answer()
        return
    await _edit_or_send(cb, t("lang.title", user_id=cb.from_user.id), language_kb())

@router.callback_query(F.data.startswith("settings:set_lang:"))
async def cb_set_language(cb: CallbackQuery):
    if not cb.from_user:
        await cb.answer()
        return
    lang = (cb.data or "").split(":")[-1]
    if lang not in ("en", "fr"):
        await cb.answer()
        return
    await set_language(cb.from_user.id, lang)
    remember_language(cb.from_user.id, lang)
    lang_name = t(f"lang.names.{lang}", lang=lang)
    await _edit_or_send(cb, t("lang.saved", lang=lang, lang_name=lang_name), language_kb())

@router.callback_query(F.data == "settings:back")
async def cb_settings_back(cb: CallbackQuery):
    if not cb.from_user:
        await cb.answer()
        return
    await render_settings(cb, cb.from_user.id)
