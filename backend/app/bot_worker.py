# backend/app/bot_worker.py
from __future__ import annotations

import os
import sys
import asyncio
import logging
from typing import Optional

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher
from aiogram.exceptions import TelegramBadRequest

# -----------------------------------------------------------------------------
# .env & PYTHONPATH
# -----------------------------------------------------------------------------
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))            # .../backend/app
BACKEND_DIR = os.path.dirname(CURRENT_DIR)                           # .../backend
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

load_dotenv(dotenv_path=os.path.join(BACKEND_DIR, ".env"))

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not TELEGRAM_TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN not set in backend/.env")

OWNER_ID: Optional[int] = None
_owner_env = os.getenv("OWNER_ID", "").strip()
try:
    OWNER_ID = int(_owner_env) if _owner_env else None
except ValueError:
    OWNER_ID = None

FRONTEND_WEBAPP_URL = os.getenv("FRONTEND_WEBAPP_URL", "").strip()

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s: %(message)s",
)
logger = logging.getLogger("bot_worker")

# -----------------------------------------------------------------------------
# Bot / Dispatcher
# -----------------------------------------------------------------------------
bot = Bot(token=TELEGRAM_TOKEN, parse_mode="HTML")
dp = Dispatcher()

# -----------------------------------------------------------------------------
# Register all handlers
# -----------------------------------------------------------------------------
def register_handlers():
    from app.handlers.start import register as register_start
    from app.handlers.tenant import register as register_tenant
    from app.handlers.admin_panel import register as register_admin
    from app.handlers.broadcast import register as register_broadcast
    from app.handlers.chat_link import register as register_chat_link
    from app.handlers.members import register as register_members
    from app.handlers.reports import register as register_reports
    # from app.handlers.payments import register as register_payments

    register_start(dp)
    register_tenant(dp)
    register_admin(dp)
    register_broadcast(dp)
    register_chat_link(dp)
    register_members(dp)
    register_reports(dp)
    # register_payments(dp)

# -----------------------------------------------------------------------------
# Run polling
# -----------------------------------------------------------------------------
async def run_polling():
    from app.services.db import close_pool  # ensure we can close on shutdown

    logger.info("Starting bot polling...")
    if FRONTEND_WEBAPP_URL:
        logger.info("FRONTEND_WEBAPP_URL=%s", FRONTEND_WEBAPP_URL)
    if OWNER_ID is not None:
        logger.info("OWNER_ID=%s", OWNER_ID)
    else:
        logger.warning("OWNER_ID is not set or invalid; /admin will be inaccessible.")

    try:
        register_handlers()
        await dp.start_polling(bot)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down by interrupt...")
    except TelegramBadRequest as e:
        logger.exception("Telegram API error: %s", e)
    except Exception as e:
        logger.exception("Unexpected error in polling: %s", e)
    finally:
        try:
            await close_pool()
        except Exception:
            pass
        try:
            await bot.session.close()
        except Exception:
            pass
        logger.info("Bot stopped.")

if __name__ == "__main__":
    asyncio.run(run_polling())
