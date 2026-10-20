from pydantic import (
    BaseModel,
    ConfigDict,
    EmailStr,
    Field,
    PositiveInt,
    field_validator,
)


class UserBase(BaseModel):
    """用户公共字段，包含邮箱和账号启用状态。"""

    model_config = ConfigDict(str_strip_whitespace=True)

    email: EmailStr = Field(
        ...,
        min_length=5,
        max_length=254,
        description="用户邮箱",
        examples=["seliukovvladyslav@gmail.com"],
    )
    is_active: bool = Field(default=True, description="账号是否启用")


class UserCreate(UserBase):
    """用户注册请求，包含邮箱和明文密码；密码仅用于生成哈希。"""

    password: str = Field(
        ...,
        min_length=8,
        max_length=100,
        description="用户密码，长度为 8～100 个字符",
        examples=["very_secure_password"],
    )

    @field_validator("email", mode="before")
    @classmethod
    def lowercase_email(cls, v: str) -> str:
        """校验邮箱前先转换为小写，统一注册邮箱格式。"""
        if isinstance(v, str):
            return v.lower()
        return v


class UserResponse(UserBase):
    """用户公开信息响应，不包含明文密码和密码哈希。"""

    id: PositiveInt = Field(description="用户 ID")
    is_superuser: bool = Field(default=False, description="是否为管理员账号")

    model_config = ConfigDict(from_attributes=True)


class UserUpdate(BaseModel):
    """修改用户信息的请求字段定义，按需提供待修改字段。"""

    model_config = ConfigDict(str_strip_whitespace=True)

    email: EmailStr | None = Field(
        None,
        min_length=5,
        max_length=254,
        description="用户邮箱",
        examples=["seliukovvladyslav@gmail.com"],
    )
    password: str | None = Field(
        None,
        min_length=8,
        max_length=100,
        description="用户密码，长度为 8～100 个字符",
        examples=["very_secure_password"],
    )
    is_active: bool | None = Field(None, description="账号是否启用", examples=["True"])

    @field_validator("email", mode="before")
    @classmethod
    def lowercase_email(cls, v: str | None) -> str | None:
        """提供邮箱时，在校验前将其转换为小写。"""
        if isinstance(v, str):
            return v.lower()
        return v


class PasswordChange(BaseModel):
    """修改密码的请求体，包含原密码和新密码。"""

    model_config = ConfigDict(str_strip_whitespace=True)

    old_password: str = Field(
        ...,
        max_length=100,
        description="原密码",
    )
    new_password: str = Field(
        ...,
        min_length=8,
        max_length=100,
        description="新密码，长度为 8～100 个字符",
        examples=["very_secure_password"],
    )
