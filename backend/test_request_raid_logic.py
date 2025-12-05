"""
Heavy test for join-request raid detection logic.

It directly exercises:
    - _record_join_request_and_check_raid
    - _req_is_in_raid_mode
    - _REQ_HISTORY / _REQ_RAID_UNTIL

Run with:
    cd backend
    python test_request_raid_logic.py
"""

from app.handlers.members import (
    _record_join_request_and_check_raid,
    _req_is_in_raid_mode,
    _REQ_HISTORY,
    _REQ_RAID_UNTIL,
)

def main():
    chat_id = -100999888777

    # Reset internal state
    _REQ_HISTORY.clear()
    _REQ_RAID_UNTIL.clear()

    print(f"Testing join-request raid logic for chat_id={chat_id}\n")

    # Simulate 35 join requests in a short window
    raid_triggered_at = None
    for i in range(1, 36):
        in_raid = _record_join_request_and_check_raid(chat_id)
        print(f"request #{i:2d} -> raid_mode={in_raid}")
        if in_raid and raid_triggered_at is None:
            raid_triggered_at = i

    final_raid = _req_is_in_raid_mode(chat_id)
    history_size = len(_REQ_HISTORY.get(chat_id, []))

    print("\nSummary:")
    print(f"  raid_triggered_at_request = {raid_triggered_at}")
    print(f"  final_raid_mode           = {final_raid}")
    print(f"  history_size              = {history_size}")

    # Assertions: RAID_THRESHOLD is 30, so raid must trigger at or before 30
    assert raid_triggered_at is not None, "Raid mode never triggered but it should have."
    assert raid_triggered_at <= 30, "Raid mode triggered too late."
    assert final_raid is True, "Raid mode should still be active right after burst."
    assert history_size <= 36, "History should not grow unbounded."

    print("\n✅ ALL ASSERTIONS PASSED (join-request raid logic looks correct).")


if __name__ == "__main__":
    main()
