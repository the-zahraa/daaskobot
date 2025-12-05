import asyncio

from app.repositories.pending_verification import add_pending_verification


async def main():
    await add_pending_verification(
        chat_id=-1001234567890,
        user_id=999001,
        ttl_seconds=120,
    )


if __name__ == "__main__":
    asyncio.run(main())
