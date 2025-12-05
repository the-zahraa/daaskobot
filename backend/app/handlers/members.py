# backend/app/handlers/members.py
from __future__ import annotations

import logging
from datetime import datetime, timezone, date, timedelta
from typing import Optional, Set
import asyncio
import time

from aiogram import F
from aiogram.types import (
    ChatMemberUpdated,
    Message,
    ChatPermissions,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ChatJoinRequest,  # ✅ NEW
)
from aiogram.enums.chat_type import ChatType

from app.db import get_con
from app.repositories.stats import (
    inc_join,
    inc_leave,
    record_event,
    upsert_chat_user_index,
)
from app.repositories.required import list_group_targets
from app.repositories.users import has_phone
from app.repositories.pending_verification import add_pending_verification, should_ban

from app.handlers.start import _is_member, force_join_kb_group
from app.services.i18n import t  # i18n

log = logging.getLogger("handlers.members")
UTC = timezone.utc

# Chats where we know Telegram sends real ChatMemberUpdated JOIN/LEAVE
_CHATS_WITH_REAL_CM: Set[int] = set()

# ---------------- Raid detection (join floods) ----------------

JOIN_WINDOW_SECONDS = 30
RAID_THRESHOLD = 30              # joins per JOIN_WINDOW_SECONDS
RAID_DURATION_SECONDS = 5 * 60   # raid mode lasts 5 minutes

# chat_id -> list[join_timestamps]
_JOIN_HISTORY: dict[int, list[float]] = {}
# chat_id -> raid_mode_until (monotonic time)
_RAID_MODE_UNTIL: dict[int, float] = {}


def _is_in_raid_mode(chat_id: int) -> bool:
    now = time.monotonic()
    until = _RAID_MODE_UNTIL.get(chat_id, 0.0)
    return now < until


def _record_join_and_check_raid(chat_id: int) -> bool:
    """
    Record a join timestamp for this chat and decide if raid mode is active.
    """
    now = time.monotonic()
    history = _JOIN_HISTORY.get(chat_id, [])
    history.append(now)
    cutoff = now - JOIN_WINDOW_SECONDS
    history = [t for t in history if t >= cutoff]
    _JOIN_HISTORY[chat_id] = history

    # trigger raid mode if threshold exceeded
    if len(history) >= RAID_THRESHOLD:
        _RAID_MODE_UNTIL[chat_id] = now + RAID_DURATION_SECONDS
        return True

    return _is_in_raid_mode(chat_id)


# ---------------- Raid detection for JOIN REQUESTS (private groups) ----------------
# ✅ NEW: protects private groups that use "join requests" instead of open joins

REQ_WINDOW_SECONDS = 30
REQ_THRESHOLD = 30               # requests per REQ_WINDOW_SECONDS
REQ_RAID_DURATION_SECONDS = 5 * 60  # 5 minutes

# chat_id -> list[request_timestamps]
_REQ_HISTORY: dict[int, list[float]] = {}
# chat_id -> request-raid-mode-until (monotonic time)
_REQ_RAID_UNTIL: dict[int, float] = {}


def _req_is_in_raid_mode(chat_id: int) -> bool:
    now = time.monotonic()
    until = _REQ_RAID_UNTIL.get(chat_id, 0.0)
    return now < until


def _record_join_request_and_check_raid(chat_id: int) -> bool:
    """
    Record a join-request timestamp for this chat and decide if request-raid mode is active.
    """
    now = time.monotonic()
    history = _REQ_HISTORY.get(chat_id, [])
    history.append(now)
    cutoff = now - REQ_WINDOW_SECONDS
    history = [t for t in history if t >= cutoff]
    _REQ_HISTORY[chat_id] = history

    # trigger raid mode for join requests
    if len(history) >= REQ_THRESHOLD:
        _REQ_RAID_UNTIL[chat_id] = now + REQ_RAID_DURATION_SECONDS
        return True

    return _req_is_in_raid_mode(chat_id)


async def _delete_message_later(bot, chat_id: int, message_id: int, delay: int = 120) -> None:
    """
    Delete a message after 'delay' seconds.
    Used to auto-remove verification prompts from the group.
    """
    await asyncio.sleep(delay)
    try:
        await bot.delete_message(chat_id, message_id)
    except Exception:
        pass


async def _ban_if_not_verified_later(bot, chat_id: int, user_id: int, delay: int = 120) -> None:
    """
    After 'delay' seconds, if user is still unverified for this chat, ban them.
    """
    await asyncio.sleep(delay)
    try:
        if await should_ban(chat_id, user_id):
            try:
                await bot.ban_chat_member(chat_id, user_id)
                log.info(
                    "verify-gate: banned user=%s from chat=%s (not verified in time)",
                    user_id,
                    chat_id,
                )
            except Exception as e:
                log.warning(
                    "verify-gate: ban_chat_member failed chat=%s user=%s err=%s",
                    chat_id,
                    user_id,
                    e,
                )
    except Exception as e:
        log.warning(
            "verify-gate: should_ban check failed chat=%s user=%s err=%s",
            chat_id,
            user_id,
            e,
        )


async def _maybe_require_phone_verification(
    chat_id: int,
    user_id: int,
    joined_user,
    chat_title: str,
    bot,
) -> None:
    """
    Enforce verification by muting, showing ONE group message with a personal button,
    and banning later if they don't verify.
    """
    # Ignore bots
    if joined_user and getattr(joined_user, "is_bot", False):
        return

    # Already verified -> do nothing
    try:
        if await has_phone(user_id):
            return
    except Exception as e:
        log.warning(
            "verify-gate: has_phone failed chat=%s user=%s err=%s",
            chat_id,
            user_id,
            e,
        )

    # Raid mode: instant ban
    in_raid = _record_join_and_check_raid(chat_id)
    if in_raid:
        try:
            await bot.ban_chat_member(chat_id, user_id)
            log.info("verify-gate: raid-mode ban chat=%s user=%s", chat_id, user_id)
        except Exception as e:
            log.warning(
                "verify-gate: raid-mode ban failed chat=%s user=%s err=%s",
                chat_id,
                user_id,
                e,
            )
        return

    # Mute immediately (block messaging)
    try:
        perms = ChatPermissions(
            can_send_messages=False,
            can_send_audios=False,
            can_send_documents=False,
            can_send_photos=False,
            can_send_videos=False,
            can_send_video_notes=False,
            can_send_voice_notes=False,
            can_send_polls=False,
            can_send_other_messages=False,
            can_add_web_page_previews=False,
        )
        await bot.restrict_chat_member(chat_id, user_id, permissions=perms)
    except Exception as e:
        log.warning(
            "verify-gate: restrict failed chat=%s user=%s err=%s",
            chat_id,
            user_id,
            e,
        )

    # Add pending verification (2 minutes)
    await add_pending_verification(chat_id, user_id, ttl_seconds=120)

    # Get bot username for deep link
    bot_username = None
    try:
        me = await bot.get_me()
        bot_username = me.username
    except Exception as e:
        log.info(
            "verify-gate: get_me failed chat=%s user=%s err=%s",
            chat_id,
            user_id,
            e,
        )

    # Build a nice personal mention with real name
    if joined_user:
        first = (joined_user.first_name or "").strip()
        last = (joined_user.last_name or "").strip()
        username = (joined_user.username or "").strip()
        display_name = (first + " " + last).strip() or (f"@{username}" if username else "user")
    else:
        display_name = "user"

    mention = f'<a href="tg://user?id={user_id}">{display_name}</a>'

    # Group message with ONE personal button
    try:
        text_lines = [
            f"👋 Welcome {mention}!",
            f"To talk in <b>{chat_title or 'this chat'}</b>, you must verify your phone number.",
            "",
            "⏳ You have <b>2 minutes</b> to verify, otherwise you will be removed automatically.",
            "",
            "Tap the button below:",
        ]
        text = "\n".join(text_lines)

        kb = None
        if bot_username:
            payload = f"verify_{user_id}"
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text="🔐 Verify Now",
                            url=f"https://t.me/{bot_username}?start={payload}",
                        )
                    ]
                ]
            )

        verify_msg = await bot.send_message(
            chat_id,
            text,
            reply_markup=kb,
            disable_web_page_preview=True,
        )

        # Auto-delete the verification prompt after ~2 minutes
        asyncio.create_task(
            _delete_message_later(bot, chat_id, verify_msg.message_id, delay=130)
        )
    except Exception as e:
        log.info(
            "verify-gate: group msg with button failed chat=%s user=%s err=%s",
            chat_id,
            user_id,
            e,
        )

    # Schedule ban after 2 minutes if still not verified
    asyncio.create_task(_ban_if_not_verified_later(bot, chat_id, user_id, delay=120))


def _now() -> datetime:
    return datetime.now(UTC)


def _today() -> date:
    return _now().date()


def _status_code(raw) -> str:
    """
    Normalize ChatMember status (enum or string) to a lowercase string.
    """
    if raw is None:
        return ""
    val = getattr(raw, "value", raw)
    return str(val).lower().strip()


# ---------------- Dedup helper ----------------

async def _recent_member_event_exists(
    chat_id: int,
    user_id: int,
    kind: str,
    window_seconds: int = 300,  # 5 minutes
) -> bool:
    """
    Check if we already recorded the same kind of event (join/leave)
    for this chat + user in the last `window_seconds`.
    """
    since = _now() - timedelta(seconds=window_seconds)
    async with get_con() as con:
        row = await con.fetchrow(
            """
            SELECT 1
            FROM member_events
            WHERE chat_id = $1
              AND tg_id   = $2
              AND kind    = $3
              AND happened_at >= $4
            LIMIT 1
            """,
            chat_id,
            user_id,
            kind,
            since,
        )
    return bool(row)


# ---------------- Campaign helpers ----------------

async def _ensure_groups_channels_row(chat_id: int) -> None:
    """
    join_logs.chat_id has a FK to groups_channels(telegram_id TEXT).
    Make sure a row exists, but don't block if it fails.
    """
    try:
        async with get_con() as con:
            await con.execute(
                """
                INSERT INTO public.groups_channels (telegram_id)
                VALUES ($1)
                ON CONFLICT (telegram_id) DO NOTHING
                """,
                str(chat_id),
            )
    except Exception as e:
        log.warning("ensure groups_channels failed for chat=%s: %s", chat_id, e)


async def _lookup_campaign_name(chat_id: int, invite_url: str) -> Optional[str]:
    """
    Map invite URL -> campaign_name for this chat.
    Try exact URL, then fallback to invite-code suffix (+CODE or joinchat/CODE).
    """
    async with get_con() as con:
        # Exact URL
        row = await con.fetchrow(
            """
            SELECT campaign_name
            FROM public.campaign_links
            WHERE chat_id = $1 AND invite_link = $2
            ORDER BY created_at DESC
            LIMIT 1
            """,
            chat_id,
            invite_url,
        )
        if row:
            return row["campaign_name"]

        # Fallback by code suffix
        row = await con.fetchrow(
            """
            WITH url_code AS (
              SELECT NULLIF(
                       REGEXP_REPLACE($2, '^.*(?:joinchat/|\\+)([A-Za-z0-9_-]+)$', '\\1'),
                       $2
                     ) AS code
            )
            SELECT cl.campaign_name
            FROM public.campaign_links cl, url_code uc
            WHERE cl.chat_id = $1
              AND uc.code IS NOT NULL
              AND NULLIF(
                    REGEXP_REPLACE(cl.invite_link, '^.*(?:joinchat/|\\+)([A-Za-z0-9_-]+)$', '\\1'),
                    cl.invite_link
                  ) = uc.code
            ORDER BY cl.created_at DESC
            LIMIT 1
            """,
            chat_id,
            invite_url,
        )
        return row["campaign_name"] if row else None


async def _record_campaign_join(chat_id: int, user_id: int, invite_link_url: str) -> None:
    """
    Write-through:
      • join_logs (audit; may fail FK, non-fatal)
      • campaign_joins (used for analytics)
    """
    await _ensure_groups_channels_row(chat_id)

    # Audit
    try:
        async with get_con() as con:
            await con.execute(
                """
                INSERT INTO public.join_logs (chat_id, user_id, event_type, invite_link, "timestamp")
                VALUES ($1, $2, 'join', $3, now())
                """,
                str(chat_id),
                user_id,
                invite_link_url,
            )
    except Exception as e:
        log.warning(
            "join_logs insert failed (non-fatal): chat=%s user=%s err=%s",
            chat_id,
            user_id,
            e,
        )

    # Attribution
    try:
        campaign_name = await _lookup_campaign_name(chat_id, invite_link_url)
        if campaign_name:
            async with get_con() as con:
                await con.execute(
                    """
                    INSERT INTO public.campaign_joins (chat_id, user_id, campaign_name, happened_at)
                    VALUES ($1, $2, $3, now())
                    """,
                    chat_id,
                    user_id,
                    campaign_name,
                )
            log.info(
                "campaign attribution: %r chat=%s user=%s",
                campaign_name,
                chat_id,
                user_id,
            )
        else:
            log.info(
                "campaign attribution: none chat=%s user=%s (link=%s)",
                chat_id,
                user_id,
                invite_link_url,
            )
    except Exception as e:
        log.warning(
            "campaign_joins insert failed: chat=%s user=%s err=%s",
            chat_id,
            user_id,
            e,
        )


# ---------------- Core join/leave writers ----------------

async def _handle_join(chat_id: int, user_id: int, invite_link_url: Optional[str]) -> None:
    # 🔁 Dedup: if we already saw a JOIN very recently for this user+chat, skip
    if await _recent_member_event_exists(chat_id, user_id, "join"):
        log.info(
            "members: SKIP duplicate JOIN chat=%s user=%s",
            chat_id,
            user_id,
        )
        return

    ts = _now()
    d = ts.date()
    log.info(
        "members: JOIN detected chat=%s user=%s date=%s invite=%s",
        chat_id,
        user_id,
        d,
        invite_link_url,
    )
    await inc_join(chat_id, d)
    await record_event(chat_id, user_id, ts, "join")
    await upsert_chat_user_index(chat_id, user_id, True, ts)
    if invite_link_url:
        await _record_campaign_join(chat_id, user_id, invite_link_url)


async def _handle_leave(chat_id: int, user_id: int) -> None:
    # 🔁 Dedup: if we already saw a LEAVE very recently for this user+chat, skip
    if await _recent_member_event_exists(chat_id, user_id, "leave"):
        log.info(
            "members: SKIP duplicate LEAVE chat=%s user=%s",
            chat_id,
            user_id,
        )
        return

    ts = _now()
    d = ts.date()
    log.info("members: LEAVE detected chat=%s user=%s date=%s", chat_id, user_id, d)
    await inc_leave(chat_id, d)
    await record_event(chat_id, user_id, ts, "leave")
    await upsert_chat_user_index(chat_id, user_id, False, ts)


# ---------------- ChatMemberUpdated handler ----------------

async def on_member_update(upd: ChatMemberUpdated) -> None:
    """
    Main source of truth where Telegram actually sends member transitions.

    JOIN when:  old ∉ MEMBERish  AND new ∈ MEMBERish
    LEAVE when: old ∈ MEMBERish  AND new ∈ LEFTish
    """
    chat_id = upd.chat.id

    old_status_raw = getattr(upd.old_chat_member, "status", None)
    new_status_raw = getattr(upd.new_chat_member, "status", None)
    old_status = _status_code(old_status_raw)
    new_status = _status_code(new_status_raw)

    joined_user = getattr(upd.new_chat_member, "user", None)
    user_id = getattr(joined_user, "id", None)
    if user_id is None:
        return

    log.info(
        "members: ChatMemberUpdated chat=%s user=%s old=%s new=%s",
        chat_id,
        user_id,
        old_status,
        new_status,
    )

    MEMBERish = {"member", "restricted", "administrator", "creator"}
    LEFTish = {"left", "kicked"}

    joined = (new_status in MEMBERish) and (old_status not in MEMBERish)
    left = (new_status in LEFTish) and (old_status in MEMBERish)

    if joined or left:
        _CHATS_WITH_REAL_CM.add(chat_id)

    # JOIN
    if joined:
        invite_url: Optional[str] = None
        inv = getattr(upd, "invite_link", None)
        if inv is not None:
            raw_link = getattr(inv, "invite_link", None)
            if raw_link:
                invite_url = str(raw_link)
        if invite_url:
            log.info(
                "campaign: ChatMemberUpdated invited via link=%s chat=%s user=%s",
                invite_url,
                chat_id,
                user_id,
            )

        await _handle_join(chat_id, user_id, invite_url)

        # Phone verification gate (groups + supergroups + channels)
        chat_title = getattr(upd.chat, "title", "this chat")
        await _maybe_require_phone_verification(
            chat_id=chat_id,
            user_id=user_id,
            joined_user=joined_user,
            chat_title=chat_title,
            bot=upd.bot,
        )

        # Force-join applies only to groups / supergroups
        try:
            if upd.chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
                targets = await list_group_targets(chat_id)
            else:
                targets = []
        except Exception:
            targets = []

        if targets:
            # Mute again (harmless if already muted)
            try:
                perms = ChatPermissions(
                    can_send_messages=False,
                    can_send_audios=False,
                    can_send_documents=False,
                    can_send_photos=False,
                    can_send_videos=False,
                    can_send_video_notes=False,
                    can_send_voice_notes=False,
                    can_send_polls=False,
                    can_send_other_messages=False,
                    can_add_web_page_previews=False,
                )
                await upd.bot.restrict_chat_member(chat_id, user_id, permissions=perms)
            except Exception:
                pass

            # DM with force-join buttons
            try:
                await upd.bot.send_message(
                    user_id,
                    t(
                        "force_join.dm_prompt",
                        user_id=user_id,
                        group=getattr(upd.chat, "title", "this group"),
                    ),
                    reply_markup=force_join_kb_group(user_id, chat_id, targets),
                )
            except Exception:
                pass

        return

    # LEAVE
    if left:
        await _handle_leave(chat_id, user_id)
        return


# ---------------- Service message fallbacks ----------------

def register(dp) -> None:
    # Primary: CM updates
    dp.chat_member.register(on_member_update)

    # --- Join request handler (private groups / request-only groups) ---
    async def on_join_request(req: ChatJoinRequest):
        chat_id = req.chat.id
        user = req.from_user

        if not user:
            return

        # 1) Always block real Telegram bots at request stage
        if getattr(user, "is_bot", False):
            try:
                await req.decline()
            except Exception:
                pass
            return

        # 2) Flood detection for JOIN REQUESTS (many requests in a short window)
        in_raid = _record_join_request_and_check_raid(chat_id)
        if in_raid:
            # Request-raid mode: decline everyone for a while
            try:
                await req.decline()
            except Exception:
                pass
            return

        # 3) Not raid, not bot -> approve after a small delay
        #    After approval, your existing on_member_update + phone/EU gate will run.
        async def _approve_later(r: ChatJoinRequest, delay: int = 5):
            await asyncio.sleep(delay)
            try:
                await r.approve()
            except Exception:
                # If another admin already approved/declined, this can fail. That's fine.
                pass

        asyncio.create_task(_approve_later(req))

    dp.chat_join_request.register(on_join_request)

    # Fallbacks ONLY for groups/supergroups where CM events don't fire
    async def on_new_members_service(msg: Message):
        if msg.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
            return

        # If we already have real CM events for this chat, don't double-count
        if msg.chat.id in _CHATS_WITH_REAL_CM:
            return

        users = msg.new_chat_members or []
        for u in users:
            if not u or u.is_bot:
                continue

            log.info(
                "members: FALLBACK join via service message chat=%s user=%s",
                msg.chat.id,
                u.id,
            )

            await _handle_join(msg.chat.id, u.id, invite_link_url=None)

            # Phone verification gate for fallback joins
            await _maybe_require_phone_verification(
                chat_id=msg.chat.id,
                user_id=u.id,
                joined_user=u,
                chat_title=(msg.chat.title or "this chat"),
                bot=msg.bot,
            )

            # Force-join only for groups with targets
            try:
                targets = await list_group_targets(msg.chat.id)
            except Exception:
                targets = []

            if targets:
                # Mute
                try:
                    perms = ChatPermissions(
                        can_send_messages=False,
                        can_send_audios=False,
                        can_send_documents=False,
                        can_send_photos=False,
                        can_send_videos=False,
                        can_send_video_notes=False,
                        can_send_voice_notes=False,
                        can_send_polls=False,
                        can_send_other_messages=False,
                        can_add_web_page_previews=False,
                    )
                    await msg.bot.restrict_chat_member(
                        msg.chat.id,
                        u.id,
                        permissions=perms,
                    )
                except Exception:
                    pass

                # DM
                try:
                    await msg.bot.send_message(
                        u.id,
                        t(
                            "force_join.dm_prompt",
                            user_id=u.id,
                            group=(msg.chat.title or "this group"),
                        ),
                        reply_markup=force_join_kb_group(u.id, msg.chat.id, targets),
                    )
                except Exception:
                    pass

    async def on_left_member_service(msg: Message):
        if msg.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
            return
        if msg.chat.id in _CHATS_WITH_REAL_CM:
            return

        u = msg.left_chat_member
        if not u or u.is_bot:
            return

        log.info(
            "members: FALLBACK leave via service message chat=%s user=%s",
            msg.chat.id,
            u.id,
        )
        await _handle_leave(msg.chat.id, u.id)

    dp.message.register(on_new_members_service, F.new_chat_members)
    dp.message.register(on_left_member_service, F.left_chat_member)
