"""
Simulation test for join-request handling logic.

We build a fake ChatJoinRequest-like object and reproduce the logic
used in members.register(on_join_request), but without Telegram.

Run with:
    cd backend
    python test_join_request_handler_sim.py
"""

import asyncio
from dataclasses import dataclass

from app.handlers.members import (
    _record_join_request_and_check_raid,
    _REQ_HISTORY,
    _REQ_RAID_UNTIL,
)


# --- Fake minimal Telegram types -------------------------------------------

@dataclass
class DummyUser:
    id: int
    is_bot: bool = False


@dataclass
class DummyChat:
    id: int


class DummyReq:
    """
    Minimal stand-in for aiogram.types.ChatJoinRequest used only in tests.
    """
    def __init__(self, chat_id: int, user_id: int, is_bot: bool = False):
        self.chat = DummyChat(chat_id)
        self.from_user = DummyUser(user_id, is_bot=is_bot)
        self._approved = False
        self._declined = False

    async def approve(self):
        # In real bot this would call Telegram API.
        # Here we just mark a flag for assertions.
        self._approved = True
        print(f"approve() called for user={self.from_user.id}")

    async def decline(self):
        self._declined = True
        print(f"decline() called for user={self.from_user.id}")


# --- Test helper: simulate on_join_request logic ---------------------------

async def simulate_on_join_request(req: DummyReq, delay_for_approve: float = 0.01):
    """
    This mirrors the logic we added in members.register(on_join_request),
    but uses DummyReq instead of real ChatJoinRequest.
    """
    chat_id = req.chat.id
    user = req.from_user

    if not user:
        return

    # 1) Block bots
    if getattr(user, "is_bot", False):
        await req.decline()
        return

    # 2) Flood detection for JOIN REQUESTS
    in_raid = _record_join_request_and_check_raid(chat_id)
    if in_raid:
        await req.decline()
        return

    # 3) Not raid, not bot -> approve after a small delay
    await asyncio.sleep(delay_for_approve)
    await req.approve()


# --- Actual tests ----------------------------------------------------------

async def test_bot_declined():
    _REQ_HISTORY.clear()
    _REQ_RAID_UNTIL.clear()

    req = DummyReq(chat_id=-100111222333, user_id=1, is_bot=True)
    await simulate_on_join_request(req)

    assert req._declined is True, "Bot join request should be declined."
    assert req._approved is False, "Bot join request must not be approved."
    print("✅ test_bot_declined passed")


async def test_flood_declined():
    _REQ_HISTORY.clear()
    _REQ_RAID_UNTIL.clear()

    chat_id = -100444555666

    # Pre-fill enough requests to enter raid mode
    for i in range(30):
        _record_join_request_and_check_raid(chat_id)

    # Now a new (human) request should be declined because in_raid == True
    req = DummyReq(chat_id=chat_id, user_id=10, is_bot=False)
    await simulate_on_join_request(req)

    assert req._declined is True, "Join request during raid should be declined."
    assert req._approved is False, "Join request during raid must not be approved."
    print("✅ test_flood_declined passed")


async def test_normal_human_approved():
    _REQ_HISTORY.clear()
    _REQ_RAID_UNTIL.clear()

    chat_id = -100777888999

    # Single human request, no prior flood
    req = DummyReq(chat_id=chat_id, user_id=42, is_bot=False)
    await simulate_on_join_request(req)

    assert req._declined is False, "Normal human request should not be declined."
    assert req._approved is True, "Normal human request should be approved."
    print("✅ test_normal_human_approved passed")


async def main():
    await test_bot_declined()
    await test_flood_declined()
    await test_normal_human_approved()
    print("\n🎉 ALL JOIN-REQUEST HANDLER TESTS PASSED")


if __name__ == "__main__":
    asyncio.run(main())
