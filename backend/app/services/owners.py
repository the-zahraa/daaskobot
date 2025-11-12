# backend/app/services/owners.py
from __future__ import annotations

import os

def _load_owner_id() -> int | None:
    """
    Load the single OWNER_ID from environment (.env).
    Example in .env:
        OWNER_ID=123456789
    """
    raw = os.getenv("OWNER_ID", "").strip()
    if raw.isdigit():
        return int(raw)
    return None

_OWNER_ID: int | None = _load_owner_id()

def is_owner(user_id: int | None) -> bool:
    """
    True if the given user_id matches the OWNER_ID from env.
    """
    if _OWNER_ID is None or user_id is None:
        return False
    try:
        return int(user_id) == _OWNER_ID
    except Exception:
        return False

def owner_id() -> int | None:
    return _OWNER_ID
