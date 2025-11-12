# backend/app/services/payments.py
from __future__ import annotations

from typing import Dict, Any, List
from aiogram.types import LabeledPrice

from app.repositories.plans import get_plan_by_code
from app.services.owners import is_owner

# Fallbacks used when DB has no plans yet
PLANS_FALLBACK: Dict[str, Dict[str, Any]] = {
    "PRO_WEEK": {
        "code": "PRO_WEEK",
        "title": "Pro (7 days)",
        "description": "Unlock Force Join + advanced analytics + reports for a week",
        "price_stars": 80,
        "duration_days": 7,
        "is_active": True,
    },
    "PRO_MONTH": {
        "code": "PRO_MONTH",
        "title": "Pro (30 days)",
        "description": "Unlock Force Join + advanced analytics + reports",
        "price_stars": 300,
        "duration_days": 30,
        "is_active": True,
    },
    "PRO_YEAR": {
        "code": "PRO_YEAR",
        "title": "Pro (365 days)",
        "description": "Unlock Force Join + advanced analytics + reports",
        "price_stars": 3000,
        "duration_days": 365,
        "is_active": True,
    },
}

def _owner_free_plan() -> Dict[str, Any]:
    return {
        "code": "OWNER_PRO",
        "title": "Pro (Owner Free)",
        "description": "All features unlocked for bot owner",
        "price_stars": 0,
        "duration_days": 36500,  # ~100 years
        "is_active": True,
    }

def stars_labeled_prices(plan: Dict[str, Any]) -> List[LabeledPrice]:
    """
    Convert a plan into Telegram LabeledPrice for Stars payments.
    """
    label = plan.get("title") or plan.get("code") or "Pro"
    amount = int(plan.get("price_stars") or 0)
    return [LabeledPrice(label=label, amount=amount)]


async def get_plan_resolved(plan_code: str, user_id: int | None = None) -> Dict[str, Any]:
    """
    Resolve plan from DB; if missing/inactive, fallback to in-memory defaults.
    Bot owner bypasses payment and always receives a free Pro plan.
    """
    if user_id is not None and is_owner(user_id):
        return _owner_free_plan()

    db_plan = await get_plan_by_code(plan_code)
    if db_plan and db_plan.get("is_active"):
        return db_plan

    # Fallbacks
    return PLANS_FALLBACK.get(plan_code, PLANS_FALLBACK["PRO_MONTH"])
