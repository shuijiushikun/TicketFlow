from datetime import UTC, datetime, timedelta
from typing import Any, cast

import jwt
from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError

from src.core import settings

ph = PasswordHasher()


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """使用 Argon2 验证明文密码是否与已保存的密码哈希匹配。

    匹配时返回 True，密码不符时返回 False。
    """
    try:
        return bool(ph.verify(hashed_password, plain_password))
    except VerifyMismatchError:
        return False


def get_password_hash(password: str) -> str:
    """将明文密码转换为 Argon2 哈希，数据库保存哈希而不是明文密码。"""
    return str(ph.hash(password=password))


def create_access_token(
    subject: str | Any, expires_delta: timedelta | None = None
) -> str:
    """生成 JWT 访问令牌。

    subject 通常是用户 ID，写入 sub 字段；exp 保存过期时间。
    未指定 expires_delta 时，使用配置中的默认有效期，按配置的密钥和算法签名。
    """
    if expires_delta:
        expire = datetime.now(UTC) + expires_delta
    else:
        expire = datetime.now(UTC) + timedelta(
            minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES
        )

    to_encode = {"exp": expire, "sub": str(subject)}
    encoded_jwt = jwt.encode(
        to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM
    )

    return cast(str, encoded_jwt)
