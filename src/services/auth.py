from sqlalchemy import exists, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from src.core import logger
from src.core.exception import (
    InactiveUserError,
    InvalidCredentialsError,
    UserAlreadyExistsError,
)
from src.core.security import create_access_token, get_password_hash, verify_password
from src.models.user import User
from src.schemas import PasswordChange, UserCreate, UserResponse


class AuthService:
    """用户认证业务层，处理注册、登录和密码修改。"""

    def __init__(self, session: AsyncSession):
        self.db = session

    async def register(self, user_in: UserCreate) -> UserResponse:
        """注册普通用户，检查邮箱是否重复，将密码哈希后保存并返回用户公开信息。

        user_in 提供注册参数，邮箱重复时抛出 UserAlreadyExistsError。
        数据库异常时回滚事务。
        """
        check_query = select(exists().where(User.email == user_in.email))
        if await self.db.scalar(check_query):
            raise UserAlreadyExistsError("该邮箱已被注册")

        new_user = User(
            email=user_in.email,
            hashed_password=get_password_hash(user_in.password),
            is_active=True,
            is_superuser=False,
        )

        self.db.add(new_user)
        try:
            await self.db.commit()
            await self.db.refresh(new_user)

            logger.info(f"用户注册成功，用户 ID：{new_user.id}")
        except SQLAlchemyError:
            await self.db.rollback()
            raise

        return UserResponse.model_validate(new_user, from_attributes=True)

    async def authenticate(self, email: str, password: str) -> str:
        """验证邮箱、密码及账号启用状态，成功后生成 JWT 访问令牌。

        凭证错误时抛出 InvalidCredentialsError，账号停用时抛出 InactiveUserError。
        """
        user = await self.db.scalar(select(User).where(User.email == email))

        if not user or not verify_password(password, user.hashed_password):
            logger.warning(f"登录失败，邮箱：{email}")
            raise InvalidCredentialsError("邮箱或密码不正确")

        if not user.is_active:
            logger.warning(f"未启用的账号尝试登录，用户 ID：{user.id}")
            raise InactiveUserError("账号未启用")

        logger.info(f"用户 {user.id} 登录成功")
        return create_access_token(subject=user.id)

    async def change_password(
        self, user_id: int, passwords: PasswordChange
    ) -> UserResponse:
        """验证 user_id 对应用户的原密码，再保存新密码哈希。

        passwords 包含原密码和新密码；成功时返回公开用户信息，数据库异常时回滚。
        """
        user = await self.db.get(User, user_id)

        if not user:
            raise InvalidCredentialsError("用户不存在")

        if not verify_password(passwords.old_password, user.hashed_password):
            logger.warning(f"用户 {user_id} 修改密码失败：原密码不正确")
            raise InvalidCredentialsError("原密码不正确")

        user.hashed_password = get_password_hash(passwords.new_password)

        try:
            await self.db.commit()
            await self.db.refresh(user)

            logger.info(f"用户 {user_id} 已修改密码")
        except SQLAlchemyError:
            await self.db.rollback()
            raise

        return UserResponse.model_validate(user, from_attributes=True)
