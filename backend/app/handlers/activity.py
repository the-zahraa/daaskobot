# backend/app/handlers/activity.py
from __future__ import annotations

import logging
from datetime import datetime, timezone
import time
from collections import defaultdict

from aiogram import F
from aiogram.types import Message, ChatPermissions
from aiogram.enums.chat_type import ChatType

from ..repositories.stats import inc_message_count
from ..repositories.required import list_group_targets
from ..handlers.start import _is_member, force_join_kb_group  # reuse helpers
from ..services.i18n import t  # i18n

log = logging.getLogger("app.handlers.activity")
UTC = timezone.utc

# ---------------- Simple anti-spam ----------------

# (chat_id, user_id) -> [timestamps]
_msg_history = defaultdict(list)
MAX_MSG_PER_10S = 8


def _has_content(msg: Message) -> bool:
    # Count any real content: text or media or service captions
    return bool(
        msg.text
        or msg.caption
        or msg.photo
        or msg.video
        or msg.document
        or msg.animation
        or msg.sticker
        or msg.voice
        or msg.video_note
        or msg.audio
        or msg.poll
        or msg.location
        or msg.contact
        or msg.dice
    )


async def _is_group_admin(msg: Message) -> bool:
    """
    Return True if sender is admin/owner of the group.
    If we can't verify (API error), we *assume* not admin.
    """
    if not msg.from_user:
        return False
    try:
        member = await msg.chat.get_member(msg.from_user.id)
        status = str(getattr(member, "status", "")).lower()
        return status in {"administrator", "creator", "owner"}
    except Exception:
        return False


# backend/app/handlers/activity.py  (only this function changed before; kept)
async def _enforce_group_gate_if_needed(msg: Message) -> bool:
    """
    Returns True if user is allowed to speak, False if they were gated (muted + DM sent).
    """
    if not msg.from_user or msg.from_user.is_bot:
        return True  # ignore bots/anonymous

    # ✅ Do NOT gate group admins / owners
    try:
        member = await msg.chat.get_member(msg.from_user.id)
        status = getattr(member, "status", "")
        status_str = str(status.value if hasattr(status, "value") else status).lower()
        if status_str in {"administrator", "creator", "owner"}:
            return True
    except Exception:
        # If we can’t resolve admin status, fall back to normal gating
        pass

    targets = await list_group_targets(msg.chat.id)  # [{target, join_url}]
    if not targets:
        return True

    # Check membership across all targets
    ok_all = True
    for row in targets:
        t_target = (row.get("target") or "").strip()
        if not t_target:
            continue
        ok = await _is_member(msg.bot, t_target, msg.from_user.id)
        if not ok:
            ok_all = False
            break

    if ok_all:
        return True

    # Not compliant -> delete message, mute, DM prompt
    try:
        await msg.delete()
    except Exception:
        pass

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
            can_add_web_page_previews=False
        )
        await msg.bot.restrict_chat_member(msg.chat.id, msg.from_user.id, permissions=perms)
    except Exception:
        pass

    # DM
    try:
        await msg.bot.send_message(
            msg.from_user.id,
            t("force_join.dm_prompt", user_id=msg.from_user.id, group=(msg.chat.title or "this group")),
            reply_markup=force_join_kb_group(msg.from_user.id, msg.chat.id, targets),
        )
    except Exception:
        # If DM fails (user didn’t start bot), post a brief notice in group (transient)
        try:
            await msg.bot.send_message(
                msg.chat.id,
                t("force_join.group_notice", user_id=msg.from_user.id, mention=msg.from_user.mention_html()),
                parse_mode="HTML"
            )
        except Exception:
            pass

    return False


def register(dp):
    async def on_group_message(msg: Message):
        # Only groups/supergroups, ignore bot messages
        if msg.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
            return
        if not _has_content(msg):
            return

        # 🚧 Force-Join enforcement on first real message (if enabled)
        allowed = await _enforce_group_gate_if_needed(msg)
        if not allowed:
            return  # do not count gated message

        # 🔒 Anti-spam: simple flood control (groups only)
        if msg.from_user and not msg.from_user.is_bot:
            user_id_for_spam = msg.from_user.id
            key = (msg.chat.id, user_id_for_spam)
            now = time.monotonic()
            history = _msg_history[key]
            history.append(now)
            cutoff = now - 10
            history = [t for t in history if t >= cutoff]
            _msg_history[key] = history

            if len(history) > MAX_MSG_PER_10S:
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
                try:
                    await msg.bot.restrict_chat_member(msg.chat.id, user_id_for_spam, permissions=perms)
                    await msg.answer("🚫 Anti-spam: you are muted for flooding.")
                except Exception:
                    pass
                return  # don't count spam message

        # user_id is optional (people can "send as channel" => no from_user)
        user_id = None
        if msg.from_user and not msg.from_user.is_bot:
            user_id = msg.from_user.id

        d = (msg.date or datetime.now(tz=UTC)).date()

        try:
            # ✅ Correct order: (chat_id, date, user_id?, count)
            await inc_message_count(chat_id=msg.chat.id, d=d, user_id=user_id, count=1)
            log.info(
                "analytics: counted group msg chat=%s user=%s type=%s at=%s",
                msg.chat.id, user_id, msg.content_type, (msg.date or datetime.now(tz=UTC)).isoformat()
            )
        except Exception as e:
            log.exception("analytics: failed to count group message: %s", e)

    async def on_channel_post(msg: Message):
        # Channels have no user; still count for daily totals
        if msg.chat.type != ChatType.CHANNEL:
            return
        if not _has_content(msg):
            return

        d = (msg.date or datetime.now(tz=UTC)).date()
        try:
            await inc_message_count(chat_id=msg.chat.id, d=d, user_id=None, count=1)
            log.info(
                "analytics: counted channel post chat=%s type=%s at=%s",
                msg.chat.id, msg.content_type, (msg.date or datetime.now(tz=UTC)).isoformat()
            )
        except Exception as e:
            log.exception("analytics: failed to count channel post: %s", e)

    # Register handlers
    dp.message.register(on_group_message, F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}))
    dp.channel_post.register(on_channel_post, F.chat.type == ChatType.CHANNEL)
