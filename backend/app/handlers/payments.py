# backend/app/handlers/payments.py
from __future__ import annotations
import os
from typing import Optional, List

from aiogram import F
from aiogram.types import Message, LabeledPrice, PreCheckoutQuery
from aiogram.filters import Command

from ..repositories.tenants import get_user_tenant
from ..repositories.subscriptions import activate_subscription

# Plans in Stars (XTR)
PLANS = {
    "PRO_MONTH":  {"title": "Pro — 30 days", "amount": 300, "days": 30},
    "PRO_QUARTER":{"title": "Pro — 90 days", "amount": 800, "days": 90},
}

BOT_USERNAME = os.getenv("BOT_USERNAME") or os.getenv("VITE_BOT_USERNAME") or ""

def _parse_start_payload(text: str) -> Optional[str]:
    # /start BUY_PRO_PRO_MONTH  or  /start BUY_PRO
    parts = text.strip().split()
    if len(parts) < 2:
        return None
    payload = parts[1]
    if payload.startswith("BUY_PRO_"):
        code = payload.replace("BUY_PRO_", "", 1)
        return code if code in PLANS else None
    elif payload == "BUY_PRO":
        return "PRO_MONTH"
    return None

def register(dp):
    # Deep-link entry: /start BUY_PRO_* (DM)
    async def start_buy(msg: Message):
        if not msg.from_user:
            return
        code = _parse_start_payload(msg.text or "")
        if not code:
            return
        tenant_id = await get_user_tenant(msg.from_user.id)
        if not tenant_id:
            await msg.answer("Please /start and verify first.")
            return

        plan = PLANS[code]
        title = plan["title"]
        prices: List[LabeledPrice] = [LabeledPrice(label=title, amount=plan["amount"])]

        # For Telegram Stars just set currency='XTR' and DO NOT pass provider_token
        await msg.answer_invoice(
            title=title,
            description="Unlock advanced analytics, campaigns & filtering.",
            payload=f"{tenant_id}:{code}",
            currency="XTR",
            prices=prices,
            # provider_token omitted for Stars
            need_name=False,
            need_phone_number=False,
            need_email=False,
            need_shipping_address=False,
        )

    # Fallback command for owners/testers: /buy_pro
    async def buy_pro_cmd(msg: Message):
        if not msg.from_user:
            return
        msg.text = "/start BUY_PRO_PRO_MONTH"
        await start_buy(msg)

    # Pre-checkout (still fires for Stars)
    async def on_pre_checkout(pre: PreCheckoutQuery):
        await pre.answer(ok=True)

    # Successful payment
    async def on_success(msg: Message):
        if not msg.successful_payment or not msg.from_user:
            return
        payload = msg.successful_payment.invoice_payload  # "tenant_id:CODE"
        try:
            tenant_id, code = payload.split(":")
        except Exception:
            await msg.answer("Payment recorded, but payload invalid. Contact support.")
            return
        plan = PLANS.get(code)
        if not plan:
            await msg.answer("Payment recorded, unknown plan. Contact support.")
            return
        await activate_subscription(tenant_id, code, plan["days"])
        await msg.answer(f"✅ Subscription activated: {plan['title']} (expires in {plan['days']} days).")

    dp.message.register(start_buy, Command("start"))
    dp.message.register(buy_pro_cmd, F.text == "/buy_pro")
    dp.pre_checkout_query.register(on_pre_checkout)
    dp.message.register(on_success, F.successful_payment)
