from types import SimpleNamespace
from typing import Any

import pytest
from pydantic import ValidationError

from src.schemas import UserCreate, UserResponse, UserUpdate
from tests.utils import get_missing_field_cases


def valid_create_payload() -> dict[str, Any]:
    return {"email": "seliukovvladyslav@gmail.com", "password": "very_secure_password"}


class TestUserCreate:
    def test_valid(self) -> None:
        payload = valid_create_payload()
        user = UserCreate(**payload)

        assert user.email == payload["email"]
        assert user.password == payload["password"]

    @pytest.mark.parametrize(
        "missing_field, payload", get_missing_field_cases(valid_create_payload())
    )
    def test_missing_field(self, missing_field: str, payload: dict[str, Any]) -> None:
        with pytest.raises(ValidationError) as exc:
            UserCreate(**payload)

        errors = exc.value.errors()
        if missing_field != "all":
            assert any(e["loc"][0] == missing_field for e in errors)

    @pytest.mark.parametrize(
        "field, invalid_value, error_message_part",
        [
            ("email", "non-an-email", "value is not a valid email address"),
            ("email", "", "value is not a valid email address"),
            ("password", "", "String should have at least 8 characters"),
        ],
    )
    def test_invalid_cases(
        self, field: str, invalid_value: str, error_message_part: str
    ) -> None:
        payload = valid_create_payload()
        payload[field] = invalid_value

        with pytest.raises(ValidationError) as exc:
            UserCreate(**payload)

        assert error_message_part in str(exc.value)


def valid_update_payload() -> dict[str, Any]:
    return {
        "email": "seliukovvladyslav@gmail.com",
        "password": "very_secure_password",
        "is_active": True,
    }


class TestUserUpdate:
    def test_valid(self) -> None:
        payload = valid_update_payload()
        updated_user = UserUpdate(**payload)

        assert updated_user.email == payload["email"]
        assert updated_user.is_active == payload["is_active"]
        assert updated_user.password == payload["password"]
        assert not hasattr(updated_user, "is_superuser")

    def test_partial_update(self) -> None:
        payload: dict[str, Any] = {
            "email": "seliukovvladyslav@gmail.com",
            "is_active": False,
        }

        user = UserUpdate(**payload)

        assert user.is_active == payload["is_active"]
        assert user.password is None

    @pytest.mark.parametrize(
        "field, invalid_value, error_message_part",
        [
            ("password", "", "String should have at least 8 characters"),
            ("is_active", "", "Input should be a valid boolean"),
        ],
    )
    def test_invalid_cases(
        self, field: str, invalid_value: str, error_message_part: str
    ) -> None:
        payload = valid_update_payload()
        payload[field] = invalid_value

        with pytest.raises(ValidationError) as exc:
            UserUpdate(**payload)

        errors = exc.value.errors()
        for error in errors:
            if error["loc"][0] == field:
                assert error_message_part in error["msg"]
                return
        pytest.fail("Error not found")


def valid_response_payload() -> dict[str, Any]:
    return {
        "id": 1,
        "email": "seliukovvladyslav@gmail.com",
        "is_active": True,
        "is_superuser": False,
        "hashed_password": "$2b$12$...",
        "created_at": "2026-02-15T12:00:00",
    }


class TestUserResponse:
    def test_response_security(self) -> None:
        payload = valid_response_payload()
        user = UserResponse(**payload)

        result = user.model_dump()

        assert result["id"] == payload["id"]
        assert result["email"] == payload["email"]

        assert "hashed_password" not in result
        assert "created_at" not in result

    def test_response_from_orm(self) -> None:
        payload = valid_response_payload()
        mock_orm_obj = SimpleNamespace(**payload)

        user = UserResponse.model_validate(mock_orm_obj)

        assert user.id == payload["id"]
        assert user.email == payload["email"]
        assert user.is_active is True

    def test_response_default(self) -> None:
        minimal_payload = {"id": 1, "email": "seliukovvladyslav@gmail.com"}
        user = UserResponse.model_validate(minimal_payload)

        assert user.is_active is True
        assert user.is_superuser is False
