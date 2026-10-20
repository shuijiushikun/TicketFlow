import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from src.models import Event
from tests.factories import TicketTypeFactory


@pytest.mark.asyncio
class TestTicketTypeModel:
    async def test_valid(self, db_connection: AsyncSession, event_in_db: Event) -> None:
        ticket_type = TicketTypeFactory.build(event=event_in_db)

        db_connection.add(ticket_type)
        await db_connection.commit()
        await db_connection.refresh(ticket_type)

        assert ticket_type.id is not None
        assert isinstance(ticket_type.id, int)

    async def test_non_existent_foreign_key(self, db_connection: AsyncSession) -> None:
        ticket_type = TicketTypeFactory.build(event=None, event_id=9999)

        db_connection.add(ticket_type)

        with pytest.raises(IntegrityError):
            await db_connection.commit()

    @pytest.mark.parametrize("quantity, sold", [(10, 11), (50, 100), (0, 1)])
    async def test_check_constraint_sold_limit(
        self, quantity: int, sold: int, db_connection: AsyncSession
    ) -> None:
        ticket_type = TicketTypeFactory.build(
            tickets_quantity=quantity, tickets_sold=sold
        )

        db_connection.add(ticket_type)

        with pytest.raises(IntegrityError):
            await db_connection.commit()

    @pytest.mark.parametrize(
        "field_name, invalid_value",
        [
            ("tickets_quantity", 0),
            ("tickets_quantity", -1),
            ("tickets_quantity", -100),
            ("tickets_sold", -1),
            ("tickets_sold", -100),
            ("price", -1),
            ("price", -100),
        ],
    )
    async def test_negative_or_zero_constraints(
        self, field_name: str, invalid_value: int, db_connection: AsyncSession
    ) -> None:
        ticket_type = TicketTypeFactory.build(event=None, event_id=1)

        setattr(ticket_type, field_name, invalid_value)

        db_connection.add(ticket_type)

        with pytest.raises(IntegrityError):
            await db_connection.commit()

    async def test_restrict_delete_event_with_ticket_types(
        self, db_connection: AsyncSession, event_in_db: Event
    ) -> None:
        ticket_type = TicketTypeFactory.build(event=event_in_db)
        db_connection.add(ticket_type)
        await db_connection.commit()

        await db_connection.delete(event_in_db)

        with pytest.raises(IntegrityError):
            await db_connection.commit()

        await db_connection.rollback()
