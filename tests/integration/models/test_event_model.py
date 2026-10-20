import pytest
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from tests.factories import EventFactory


@pytest.mark.asyncio
class TestEventModel:
    async def test_valid(self, db_connection: AsyncSession) -> None:
        event = EventFactory.build()

        db_connection.add(event)
        await db_connection.commit()
        await db_connection.refresh(event)

        assert event.id is not None
        assert isinstance(event.id, int)

    @pytest.mark.parametrize("field", ["title", "country", "city", "street_address"])
    async def test_strings_length_constraint(
        self, db_connection: AsyncSession, field: str
    ) -> None:
        kwargs = {field: "a" * 101}
        event = EventFactory.build(**kwargs)

        db_connection.add(event)

        with pytest.raises(DBAPIError):
            await db_connection.commit()

        await db_connection.rollback()

    @pytest.mark.parametrize(
        "field",
        ["title", "date", "country", "city", "street_address"],
    )
    async def test_nullable_fields(
        self, db_connection: AsyncSession, field: str
    ) -> None:
        kwargs = {field: None}
        event = EventFactory.build(**kwargs)

        db_connection.add(event)

        with pytest.raises(IntegrityError):
            await db_connection.commit()

        await db_connection.rollback()
