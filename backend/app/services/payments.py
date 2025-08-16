# backend/app/services/payments.py
from __future__ import annotations
import time
from dataclasses import dataclass
from typing import List, Tuple
from aiogram.types import LabeledPrice

# --- Plans you sell via Stars ---
# Adjust amounts as you wish. For Stars invoices, currency is "XTR".
# IMPORTANT: 'amount' is an integer number of Stars.
@dataclass(frozen=True)
class StarsPlan:
    code: str
    title: str
    description: str
    amount_stars: int      # integer number of stars for the whole invoice
    duration_days: int     # how long the subscription should last

PLANS: dict[str, StarsPlan] = {
    "PRO_MONTH": StarsPlan(
        code="PRO_MONTH",
        title="Pro — 30 days",
        description="Advanced analytics, invite link tracking, mass DM, filters.",
        amount_stars=300,
        duration_days=30,
    ),
    "PRO_QUARTER": StarsPlan(
        code="PRO_QUARTER",
        title="Pro — 90 days",
        description="3 months of Pro at a discount.",
        amount_stars=800,
        duration_days=90,
    ),
}

def get_plan(plan_code: str) -> StarsPlan:
    if plan_code not in PLANS:
        raise ValueError("Unknown plan code")
    return PLANS[plan_code]

def stars_labeled_prices(plan: StarsPlan) -> List[LabeledPrice]:
    # For Stars, one line item is fine; the amount is the number of stars.
    return [LabeledPrice(label=plan.title, amount=plan.amount_stars)]

def build_payload(user_id: int, tenant_id: str, plan_code: str) -> str:
    # Payload is opaque to Telegram; we’ll parse it on success.
    # Keep it short (<= 128 bytes).
    ts = int(time.time())
    return f"TEN:{tenant_id}|USR:{user_id}|PLAN:{plan_code}|TS:{ts}"
