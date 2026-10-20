import asyncio

from sqlalchemy import select

from src.core.security import get_password_hash
from src.db.session import AsyncSessionLocal
from src.models import User


async def create_superuser(email: str, password: str) -> None:
    """通过命令行初始化管理员账号。

    email 是管理员邮箱，password 用于生成密码哈希。
    邮箱已存在时直接退出；不存在时创建 is_superuser=True 的用户。
    """
    async with AsyncSessionLocal() as session:
        query = select(User).where(User.email == email)
        result = await session.execute(query)
        if result.scalar_one_or_none():
            print(f"邮箱 {email} 对应的用户已存在")
            return

        admin = User(
            email=email,
            hashed_password=get_password_hash(password),
            is_active=True,
            is_superuser=True,
        )
        session.add(admin)
        await session.commit()
        print(f"管理员账号 {email} 创建成功")


if __name__ == "__main__":
    email = input("请输入管理员邮箱：")
    password = input("请输入管理员密码：")

    asyncio.run(create_superuser(email, password))
