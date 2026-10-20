from datetime import timedelta
from typing import Any

import pytest
from fastapi import status
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.security import create_access_token
from src.models import User
from tests.factories import UserFactory
from tests.utils import get_missing_field_cases


@pytest.mark.asyncio
class TestAuthSignup:
    BASE_URL = "/api/v1/auth/signup"
    SIGNUP_PAYLOAD = {
        "email": "seliukovvladyslav@gmail.com",
        "password": "very_secure_password",
    }

    async def test_valid(
        self, client: AsyncClient, db_connection: AsyncSession
    ) -> None:
        response = await client.post(self.BASE_URL, json=self.SIGNUP_PAYLOAD)
        assert response.status_code == status.HTTP_201_CREATED

        data = response.json()
        assert data["email"] == self.SIGNUP_PAYLOAD["email"]
        assert "id" in data
        assert "password" not in data
        assert "hashed_password" not in data

        query = select(User).where(User.email == self.SIGNUP_PAYLOAD["email"])
        result = await db_connection.execute(query)
        created_user = result.scalar_one_or_none()

        assert created_user is not None
        assert created_user.is_active is True
        assert created_user.is_superuser is False

        assert created_user.hashed_password != self.SIGNUP_PAYLOAD["password"]
        assert created_user.hashed_password.startswith("$argon2")

    async def test_email_duplicate(
        self, client: AsyncClient, normal_user: User
    ) -> None:
        payload = self.SIGNUP_PAYLOAD.copy()
        payload["email"] = normal_user.email

        response = await client.post(self.BASE_URL, json=payload)
        assert response.status_code == status.HTTP_409_CONFLICT

        data = response.json()
        assert "detail" in data

    async def test_email_case_insensitivity_duplicate(
        self, client: AsyncClient, normal_user: User
    ) -> None:
        payload = self.SIGNUP_PAYLOAD.copy()
        payload["email"] = normal_user.email.upper()

        response = await client.post(self.BASE_URL, json=payload)
        assert response.status_code == status.HTTP_409_CONFLICT

    async def test_short_password(self, client: AsyncClient) -> None:
        payload = self.SIGNUP_PAYLOAD.copy()
        payload["password"] = "1234567"

        response = await client.post(self.BASE_URL, json=payload)
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    @pytest.mark.parametrize(
        "missing_field, payload", get_missing_field_cases(SIGNUP_PAYLOAD)
    )
    async def test_signup_missing_fields(
        self, client: AsyncClient, missing_field: str, payload: dict[str, str]
    ) -> None:
        response = await client.post(self.BASE_URL, json=payload)
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    @pytest.mark.parametrize(
        "invalid_email",
        [
            "not-an-email",
            "missing_at_sign.com",
            "@missing_username.com",
            "spaces in@email.com",
        ],
    )
    async def test_invalid_email_format(
        self, client: AsyncClient, invalid_email: str
    ) -> None:
        payload = self.SIGNUP_PAYLOAD.copy()
        payload["email"] = invalid_email

        response = await client.post(self.BASE_URL, json=payload)
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT


@pytest.mark.asyncio
class TestAuthLogin:
    BASE_URL = "/api/v1/auth/login"
    EVENT_URL = "/api/v1/events"
    LOGIN_PAYLOAD: dict[str, Any] = {
        "username": "seliukovvladyslav@gmail.com",
        "password": "very_secure_password",
    }

    async def test_valid(self, client: AsyncClient, normal_user: User) -> None:
        payload = self.LOGIN_PAYLOAD.copy()
        payload["username"] = normal_user.email

        response = await client.post(self.BASE_URL, data=payload)
        assert response.status_code == status.HTTP_200_OK

        token_data = response.json()
        assert "access_token" in token_data
        assert token_data["token_type"] == "bearer"

    async def test_login_invalid_password(
        self, client: AsyncClient, normal_user: User
    ) -> None:
        payload = {
            "username": normal_user.email,
            "password": "wrong_password_123",
        }

        response = await client.post(self.BASE_URL, data=payload)
        assert response.status_code == status.HTTP_401_UNAUTHORIZED

        assert (
            "incorrect email or password" in response.json().get("detail", "").lower()
        )

    async def test_login_non_existent_user(self, client: AsyncClient) -> None:
        payload = {
            "username": "does_not_exist@example.com",
            "password": "very_secure_password",
        }

        response = await client.post(self.BASE_URL, data=payload)
        assert response.status_code == status.HTTP_401_UNAUTHORIZED

        assert (
            "incorrect email or password" in response.json().get("detail", "").lower()
        )

    async def test_login_inactive_user(
        self, client: AsyncClient, db_connection: AsyncSession
    ) -> None:
        inactive_user = UserFactory.build(is_active=False)

        db_connection.add(inactive_user)
        await db_connection.commit()
        await db_connection.refresh(inactive_user)

        inactive_payload = self.LOGIN_PAYLOAD.copy()
        inactive_payload = {
            "username": inactive_user.email,
            "password": self.LOGIN_PAYLOAD.get("password"),
        }

        response = await client.post(self.BASE_URL, data=inactive_payload)
        assert response.status_code == status.HTTP_401_UNAUTHORIZED

        assert response.json()["detail"] == "账号未启用"

    async def test_invalid_token(self, client: AsyncClient) -> None:
        headers = {"Authorization": "Bearer invalid_token"}

        response = await client.post(self.EVENT_URL, headers=headers, json={})
        assert response.status_code == status.HTTP_401_UNAUTHORIZED

    @pytest.mark.parametrize(
        "missing_field, payload", get_missing_field_cases(LOGIN_PAYLOAD)
    )
    async def test_login_missing_fields(
        self, client: AsyncClient, missing_field: str, payload: dict[str, Any]
    ) -> None:
        response = await client.post(self.BASE_URL, data=payload)
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    async def test_expired_token(
        self, client: AsyncClient, db_connection: AsyncSession, user_in_db: User
    ) -> None:
        expired_token = create_access_token(
            subject=user_in_db.id, expires_delta=timedelta(minutes=-1)
        )
        headers = {"Authorization": f"Bearer {expired_token}"}

        response = await client.post(self.EVENT_URL, headers=headers, json={})
        assert response.status_code == status.HTTP_401_UNAUTHORIZED

        assert response.json()["detail"].lower() == "登录凭证无效或已过期，请重新登录"
