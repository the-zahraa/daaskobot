from __future__ import annotations
import asyncio
import os
from datetime import date
from typing import List
from aiogram import Bot

from app.repositories.chats import list_all_channels
from app.repositories.stats import upsert_channel_member_count

SNAPSHOT_INTERVAL_MIN = int(os.getenv("CHANNEL_SNAPSHOT_EVERY_MIN", "30"))

async def snapshot_once(bot: Bot) -> None:
    try:
        channels: List[int] = await list_all_channels()
        for cid in channels:
            try:
                count = await bot.get_chat_member_count(cid)
                await upsert_channel_member_count(cid, date.today(), int(count))
            except Exception:
                continue
    except Exception:
        pass

async def start_channel_snapshot_loop(bot: Bot):
    await snapshot_once(bot)
    while True:
        await asyncio.sleep(SNAPSHOT_INTERVAL_MIN * 60)
        await snapshot_once(bot)
