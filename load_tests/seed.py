import asyncio
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from src.core.config import settings
from src.core.security import get_password_hash
from src.models.event import Event
from src.models.ticket_type import TicketType
from src.models.user import User

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

USERS_COUNT = 10_000
TEST_PASSWORD = "extremely_strong_password"
BATCH_SIZE = 5_000


async def seed_db(engine: AsyncEngine) -> None:
    """向数据库写入压测所需的活动、票种和用户测试数据。"""
    AsyncSessionLocal: async_sessionmaker[AsyncSession] = async_sessionmaker(
        bind=engine,
        expire_on_commit=False,
        autoflush=False,
    )

    logger.info("正在生成测试用户的密码哈希……")
    real_password_hash = get_password_hash(TEST_PASSWORD)

    async with AsyncSessionLocal() as session:
        async with session.begin():
            stmt = select(Event).where(Event.title == "Rammstein: Europe Stadium Tour")
            existing_event = (await session.execute(stmt)).scalar_one_or_none()

            if not existing_event:
                logger.info("正在创建压测活动……")
                event = Event(
                    title="Rammstein: Europe Stadium Tour",
                    date=datetime.now(UTC) + timedelta(days=30),
                    country="Poland",
                    city="Wrocław",
                    street_address="Tarczyński Arena",
                )
                session.add(event)
                await session.flush()
            else:
                logger.info("压测活动已存在。")
                event = existing_event

            tt_stmt = select(TicketType).where(TicketType.event_id == event.id)
            existing_tts = (await session.execute(tt_stmt)).scalars().all()

            if not existing_tts:
                logger.info("正在创建压测票种……")
                ticket_types = [
                    TicketType(
                        event_id=event.id,
                        name="Fan Zone",
                        price=15000,
                        tickets_quantity=5000,
                        tickets_sold=0,
                    ),
                    TicketType(
                        event_id=event.id,
                        name="VIP",
                        price=50000,
                        tickets_quantity=500,
                        tickets_sold=0,
                    ),
                ]
                session.add_all(ticket_types)
                await session.flush()
                tt_ids = [tt.id for tt in ticket_types]
            else:
                logger.info("压测票种已存在。")
                tt_ids = [tt.id for tt in existing_tts]

            user_check = await session.execute(select(User.id).limit(1))
            if not user_check.scalar_one_or_none():
                logger.info(f"准备写入 {USERS_COUNT} 个测试用户……")
                users_data: list[dict[str, Any]] = [
                    {
                        "email": f"loadtest_{i}@example.com",
                        "hashed_password": real_password_hash,
                        "is_active": True,
                    }
                    for i in range(1, USERS_COUNT + 1)
                ]

                for i in range(0, len(users_data), BATCH_SIZE):
                    batch = users_data[i : i + BATCH_SIZE]
                    await session.execute(insert(User).values(batch))
            else:
                logger.info("测试用户已初始化。")

            logger.info("压测数据初始化完成！")
            logger.info(f"--- TARGET_EVENT_ID = {event.id}")
            logger.info(f"--- AVAILABLE_TICKET_TYPES = {tt_ids}")


async def main() -> None:
    """压测数据初始化脚本入口，执行完成后释放数据库引擎。"""
    engine = create_async_engine(str(settings.DATABASE_URL), pool_pre_ping=True)
    try:
        await seed_db(engine)
    except Exception as e:
        logger.error(f"压测数据初始化失败：{e}")
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
