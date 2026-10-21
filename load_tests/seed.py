"""补齐专用压测数据；不删除票据、不重置数据库或 Redis 库存。"""

import argparse
import asyncio
import logging
import secrets
from datetime import UTC, datetime, timedelta
from pathlib import Path

from argon2 import PasswordHasher
from sqlalchemy import insert, select, update
from sqlalchemy.engine import URL, make_url
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from load_tests.targets import DEFAULT_MANIFEST, LoadTestTargets, write_targets
from src.models import Base, Event, TicketType, User

logger = logging.getLogger(__name__)

USERS_COUNT = 10_000
BATCH_SIZE = 1000


def validate_database_url(database_url: str | URL) -> URL:
    """只允许 CLI 写入名称以 _loadtest 结尾的显式 PostgreSQL 数据库。"""
    try:
        url = make_url(database_url)
    except Exception as error:
        raise ValueError("必须提供专用 _loadtest 数据库 URL") from error
    if (
        url.drivername != "postgresql+asyncpg"
        or not url.database
        or not url.database.endswith("_loadtest")
    ):
        raise ValueError("只允许 postgresql+asyncpg 且数据库名以 _loadtest 结尾")
    return url


async def seed_db(
    engine: AsyncEngine,
    users_count: int = USERS_COUNT,
    *,
    create_schema: bool = False,
    tickets_per_type: int | None = None,
) -> LoadTestTargets:
    """补齐专用用户并返回真实 ID；SQLite 仅用于本地回归测试。"""
    if type(users_count) is not int or users_count <= 0:
        raise ValueError("users_count 必须是正整数")
    if tickets_per_type is not None and (
        type(tickets_per_type) is not int or tickets_per_type <= 0
    ):
        raise ValueError("tickets_per_type 必须是正整数")
    if engine.url.get_backend_name() != "sqlite":
        validate_database_url(engine.url)
    if create_schema:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory.begin() as session:
        event = await session.scalar(
            select(Event).where(Event.title == "Rammstein: Europe Stadium Tour")
        )
        if event is None:
            event = Event(
                title="Rammstein: Europe Stadium Tour",
                date=datetime.now(UTC) + timedelta(days=30),
                country="Poland",
                city="Wrocław",
                street_address="Tarczyński Arena",
            )
            session.add(event)
            await session.flush()

        ticket_types = list(
            await session.scalars(
                select(TicketType)
                .where(TicketType.event_id == event.id)
                .order_by(TicketType.id)
            )
        )
        if not ticket_types:
            ticket_types = [
                TicketType(
                    event_id=event.id,
                    name=name,
                    price=price,
                    tickets_quantity=(
                        quantity if tickets_per_type is None else tickets_per_type
                    ),
                    tickets_sold=0,
                )
                for name, price, quantity in (
                    ("Fan Zone", 15000, 5000),
                    ("VIP", 50000, 500),
                )
            ]
            session.add_all(ticket_types)
            await session.flush()

        user_ids: list[int] = []
        password_hash: str | None = None
        for start in range(1, users_count + 1, BATCH_SIZE):
            emails = [
                f"loadtest_{index}@example.com"
                for index in range(start, min(start + BATCH_SIZE, users_count + 1))
            ]
            existing = {
                user.email: user
                for user in await session.scalars(
                    select(User).where(User.email.in_(emails))
                )
            }
            missing = [email for email in emails if email not in existing]
            if missing:
                if password_hash is None:
                    password_hash = PasswordHasher().hash(secrets.token_urlsafe(32))
                await session.execute(
                    insert(User),
                    [
                        {
                            "email": email,
                            "hashed_password": password_hash,
                            "is_active": True,
                        }
                        for email in missing
                    ],
                )
            # 只激活约定邮箱集合中的专用用户，不修改普通用户或已有密码。
            await session.execute(
                update(User)
                .where(User.email.in_(emails), User.is_active.is_(False))
                .values(is_active=True)
            )
            rows = await session.execute(
                select(User.email, User.id).where(
                    User.email.in_(emails), User.is_active.is_(True)
                )
            )
            actual = {email: user_id for email, user_id in rows}
            user_ids.extend(actual[email] for email in emails)

        targets = LoadTestTargets(
            event_id=event.id,
            ticket_type_ids=[ticket_type.id for ticket_type in ticket_types],
            user_ids=user_ids,
        )
    logger.info(
        "压测目标已准备：活动 %s，票种 %s，启用用户 %s 个",
        event.id,
        targets.ticket_type_ids,
        len(user_ids),
    )
    return targets


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """解析参数，数据库地址必须由调用者显式提供。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database-url", required=True, help="专用 _loadtest 数据库 URL"
    )
    parser.add_argument("--users-count", type=int, default=USERS_COUNT)
    parser.add_argument(
        "--tickets-per-type",
        type=int,
        help="新建票种的总票量；不改变已有票种、票据或占用",
    )
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--create-schema", action="store_true", help="显式创建缺失表")
    return parser.parse_args(argv)


async def main(argv: list[str] | None = None) -> None:
    """在专用数据库初始化数据，失败向 CLI 返回非零状态。"""
    args = parse_args(argv)
    url = validate_database_url(args.database_url)
    engine = create_async_engine(url, pool_pre_ping=True)
    try:
        targets = await seed_db(
            engine,
            users_count=args.users_count,
            create_schema=args.create_schema,
            tickets_per_type=args.tickets_per_type,
        )
        write_targets(targets, args.manifest)
        logger.info("manifest 已写入 %s", args.manifest)
        logger.info("未重置已有票据或库存；Redis 库存预热由基线驱动单独处理。")
    finally:
        await engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
    asyncio.run(main())
