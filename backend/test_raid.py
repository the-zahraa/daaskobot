from app.handlers.members import _record_join_and_check_raid, _is_in_raid_mode, _JOIN_HISTORY, _RAID_MODE_UNTIL

def main():
    chat_id = -100999888777

    # reset state for clean test
    _JOIN_HISTORY.clear()
    _RAID_MODE_UNTIL.clear()

    print("Simulating joins for chat:", chat_id)

    for i in range(1, 35):  # 34 joins
        in_raid = _record_join_and_check_raid(chat_id)
        print(f"join #{i:2d} -> raid_mode={in_raid}")

    print("Final raid_mode:", _is_in_raid_mode(chat_id))
    print("Join history size:", len(_JOIN_HISTORY.get(chat_id, [])))

if __name__ == "__main__":
    main()
