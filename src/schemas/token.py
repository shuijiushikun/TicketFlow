from pydantic import BaseModel, Field


class Token(BaseModel):
    """符合 OAuth2 约定的登录响应，包含访问令牌和令牌类型。"""

    access_token: str = Field(
        description="JWT 访问令牌，后续请求通过 Authorization 请求头携带"
    )
    token_type: str = Field(description="令牌类型，登录成功时为 bearer")


class TokenPayload(BaseModel):
    """JWT 载荷校验模型，读取用户 ID；令牌过期时间由 JWT 解码流程校验。"""

    sub: str = Field(..., description="用户 ID")
