from typing import Annotated

from fastapi import APIRouter, status
from fastapi.params import Depends
from fastapi.security import OAuth2PasswordRequestForm

from src.api.deps import AuthServiceDep, get_current_user
from src.models.user import User
from src.schemas import PasswordChange, Token, UserCreate, UserResponse

router = APIRouter()


@router.post(
    "/signup",
    response_model=UserResponse,
    status_code=status.HTTP_201_CREATED,
    summary="注册普通用户",
)
async def register_user(
    user_in: UserCreate, auth_service: AuthServiceDep
) -> UserResponse:
    """注册普通用户。

    user_in 提供邮箱和密码。密码经过哈希处理后保存，成功时返回用户公开信息。
    注册接口不会创建管理员账号，也不会在响应中返回密码或密码哈希。
    """
    return await auth_service.register(user_in=user_in)


@router.post("/login", response_model=Token, summary="登录并获取访问令牌")
async def login_for_access_token(
    auth_service: AuthServiceDep,
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
) -> dict[str, str]:
    """登录并获取访问令牌。

    使用表单提交登录信息：username 填邮箱，password 填密码。
    验证成功后返回 access_token 和 token_type。
    后续请求通过 Authorization 请求头携带令牌。
    """
    access_token = await auth_service.authenticate(
        email=form_data.username, password=form_data.password
    )

    return {"access_token": access_token, "token_type": "bearer"}


@router.patch(
    "/password",
    response_model=UserResponse,
    status_code=status.HTTP_200_OK,
    summary="修改当前用户的密码",
)
async def change_user_password(
    passwords: PasswordChange,
    auth_service: AuthServiceDep,
    current_user: Annotated[User, Depends(get_current_user)],
) -> UserResponse:
    """修改当前用户的密码。

    需要先登录，提交 old_password（原密码）和 new_password（新密码）。
    原密码校验通过后更新密码哈希，返回用户公开信息。
    """
    return await auth_service.change_password(
        user_id=current_user.id, passwords=passwords
    )
