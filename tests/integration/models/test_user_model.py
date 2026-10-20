import pytest
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.security import get_password_hash, verify_password
from src.models import Ticket, User
from tests.factories import UserFactory


@pytest.mark.asyncio
class TestUserModel:
    VALID_PAYLOAD = {
        "email": "seliukovvladyslav@gmail.com",
        "password": "very_secure_password",
    }

    async def test_valid(self, db_connection: AsyncSession) -> None:
        user = UserFactory.build()

        db_connection.add(user)
        await db_connection.commit()
        await db_connection.refresh(user)

        assert user.id is not None
        assert isinstance(user.id, int)

    async def test_default_value(self, db_connection: AsyncSession) -> None:
        raw_password = self.VALID_PAYLOAD["password"]
        user = User(
            email=self.VALID_PAYLOAD["email"],
            hashed_password=get_password_hash(raw_password),
        )

        db_connection.add(user)
        await db_connection.commit()
        await db_connection.refresh(user)

        assert user.is_superuser is False
        assert user.is_active is True

        assert user.hashed_password is not raw_password
        assert verify_password(raw_password, user.hashed_password)

    async def test_defaults_override(self, db_connection: AsyncSession) -> None:
        user = UserFactory.build(is_active=False, is_superuser=True)

        db_connection.add(user)
        await db_connection.commit()
        await db_connection.refresh(user)

        assert user.is_active is False
        assert user.is_superuser is True

    async def test_unique_constraint(
        self, db_connection: AsyncSession, user_in_db: User
    ) -> None:
        new_user = User(
            email=user_in_db.email,
            hashed_password=get_password_hash(self.VALID_PAYLOAD["password"]),
        )

        db_connection.add(new_user)

        with pytest.raises(IntegrityError):
            await db_connection.commit()

        await db_connection.rollback()

    @pytest.mark.parametrize(
        "field, invalid_value",
        [("email", "a" * 255), ("hashed_password", "a" * 256)],
        ids=["email_too_long", "password_too_long"],
    )
    async def test_string_length_limits(
        self, db_connection: AsyncSession, field: str, invalid_value: str
    ) -> None:
        kwargs = {field: invalid_value}
        user = UserFactory.build(**kwargs)

        db_connection.add(user)

        with pytest.raises(DBAPIError):
            await db_connection.commit()

        await db_connection.rollback()

    @pytest.mark.parametrize(
        "field", ["email", "hashed_password"], ids=["missing_email", "missing_password"]
    )
    async def test_not_null_constraints(
        self, db_connection: AsyncSession, field: str
    ) -> None:
        kwargs = {field: None}
        user = UserFactory.build(**kwargs)

        db_connection.add(user)

        with pytest.raises(IntegrityError):
            await db_connection.commit()

        await db_connection.rollback()

    async def test_delete_user_with_tickets(
        self, db_connection: AsyncSession, ticket_in_db: Ticket
    ) -> None:
        await db_connection.delete(ticket_in_db.owner)

        with pytest.raises(IntegrityError):
            await db_connection.commit()

        await db_connection.rollback()
