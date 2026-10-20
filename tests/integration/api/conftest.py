from collections.abc import Generator
from typing import cast

import pytest
from httpx import AsyncClient

from src.api.deps import get_current_user
from src.main import app
from src.models import User


@pytest.fixture
async def user_token_headers(client: AsyncClient, normal_user: User) -> dict[str, str]:
    login_data = {"username": normal_user.email, "password": "very_secure_password"}

    response = await client.post("/api/v1/auth/login", data=login_data)
    assert response.status_code == 200
    token = response.json()["access_token"]

    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
async def superuser_token_headers(
    client: AsyncClient, superuser: User
) -> dict[str, str]:
    login_data = {"username": superuser.email, "password": "very_secure_password"}

    response = await client.post("/api/v1/auth/login", data=login_data)
    assert response.status_code == 200

    token = response.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
async def authorized_user(
    client: AsyncClient, user_token_headers: dict[str, str]
) -> AsyncClient:
    client.headers.update(user_token_headers)
    return client


@pytest.fixture
async def authorized_superuser(
    client: AsyncClient, superuser_token_headers: dict[str, str]
) -> AsyncClient:
    client.headers.update(superuser_token_headers)
    return client


@pytest.fixture
def bypass_auth(superuser: User) -> Generator[None, None, None]:
    app.dependency_overrides[get_current_user] = lambda: superuser
    yield
    app.dependency_overrides.pop(get_current_user, None)


@pytest.fixture
def api_client(request: pytest.FixtureRequest) -> AsyncClient:
    return cast(AsyncClient, request.getfixturevalue(request.param))
