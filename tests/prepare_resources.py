"""显式初始化专用测试资源标记；不会从业务配置读取连接地址。"""

import argparse
import asyncio
import os

import asyncpg
from redis.asyncio import Redis

from tests.resource_safety import RESOURCE_MARKER_KEY, TestResources


async def prepare(resources: TestResources) -> None:
    """创建测试库并标记专用资源，不删除数据库或现有 Redis 数据。"""
    connection = await asyncpg.connect(resources.admin_dsn)
    try:
        exists = await connection.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1", resources.database_name
        )
        if not exists:
            await connection.execute(f'CREATE DATABASE "{resources.database_name}"')
        comment = await connection.fetchval(
            "SELECT format('COMMENT ON DATABASE %I IS %L', $1::text, $2::text)",
            resources.database_name,
            resources.marker,
        )
        await connection.execute(comment)
    finally:
        await connection.close()
    async with Redis.from_url(resources.redis_control_url) as redis:
        await redis.set(RESOURCE_MARKER_KEY, resources.marker)


def main() -> None:
    """仅在明确指定初始化开关后，标记操作者已选定的专用测试实例。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--initialize-isolated-resources", action="store_true")
    arguments = parser.parse_args()
    if not arguments.initialize_isolated_resources:
        parser.error("需显式传入 --initialize-isolated-resources，确认目标为专用实例")
    resources = TestResources.from_environment(os.environ)
    asyncio.run(prepare(resources))
    print("专用测试资源标记已初始化；后续 pytest 才允许重置测试数据。")


if __name__ == "__main__":
    main()
