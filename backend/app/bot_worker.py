# backend/app/bot_worker.py
from __future__ import annotations

import asyncio
import logging
import os

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
root_logger = logging.getLogger()
if not root_logger.handlers:
    logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bot_worker")

# -----------------------------------------------------------------------------
# Load .env
# -----------------------------------------------------------------------------
load_dotenv(override=False)

# -----------------------------------------------------------------------------
# DB facade
# -----------------------------------------------------------------------------
from app import db as app_db  # provides init_db(), close_db(), get_con()

# -----------------------------------------------------------------------------
# i18n
# -----------------------------------------------------------------------------
from app.services.i18n import init_i18n

def _safe_import(module_path: str):
    try:
        return __import__(module_path, fromlist=["*"])
    except Exception as e:
        logger.warning("Optional module '%s' not imported: %s", module_path, e)
        return None


async def run_polling() -> None:
    token = os.getenv("BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("BOT_TOKEN is required")

    bot = Bot(token=token, parse_mode=ParseMode.HTML)
    dp = Dispatcher()

    # -------------------------------------------------------------------------
    # Init i18n before handlers are loaded
    # -------------------------------------------------------------------------
    init_i18n(default_lang="en")

    # IMPORTANT: keep 'payments' first to avoid greedy handlers swallowing /pro
    modules = [
        "app.handlers.payments",        # <<— moved to the top
        "app.handlers.start",
        "app.handlers.group_tools_dm",
        "app.handlers.members",
        "app.handlers.reports",
        "app.handlers.admin_plans",
        "app.handlers.admin_panel",
        "app.handlers.broadcast",
        "app.handlers.campaigns",
        "app.handlers.chat_link",
        "app.handlers.activity",
    ]
    for name in modules:
        mdl = _safe_import(name)
        if not mdl:
            continue
        if hasattr(mdl, "router"):
            dp.include_router(mdl.router)
            logger.info("Included: %s.router", name)
        elif hasattr(mdl, "register"):
            try:
                mdl.register(dp)  # type: ignore[attr-defined]
                logger.info("Included via register(dp): %s", name)
            except Exception as e:
                logger.warning("Failed to register '%s': %s", name, e)

    await app_db.init_db()
    logger.info("Database pool ready.")

    # Optional compatibility hook
    try:
        import app.repositories.activity as repo_activity
        if hasattr(repo_activity, "ensure_activity_tables"):
            async with app_db.get_con() as con:
                await repo_activity.ensure_activity_tables(con)  # type: ignore
                logger.info("ensure_activity_tables done.")
    except Exception as e:
        logger.warning("ensure_activity_tables skipped: %s", e)

    # Make sure Telegram sends only the update types we actually use
    allowed = dp.resolve_used_update_types()
    logger.info("Allowed updates resolved: %s", allowed)

    logger.info("Start polling…")
    await dp.start_polling(bot, allowed_updates=allowed)

    try:
        await app_db.close_db()
    except Exception:
        pass


if __name__ == "__main__":
    asyncio.run(run_polling())
