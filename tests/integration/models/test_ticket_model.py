import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from src.models import Ticket, TicketType, User
from tests.factories import TicketFactory


@pytest.mark.asyncio
class TestTicketModel:
    async def test_valid(
        self, db_connection: AsyncSession, ticket_type_in_db: TicketType
    ) -> None:
        ticket = TicketFactory.build(ticket_type=ticket_type_in_db)
        db_connection.add(ticket)
        await db_connection.commit()
        await db_connection.refresh(ticket)

        assert ticket.ticket_type_id == ticket_type_in_db.id

    @pytest.mark.parametrize("invalid_fk_field", ["owner_id", "ticket_type_id"])
    async def test_foreign_key_constraints(
        self,
        normal_user: User,
        invalid_fk_field: str,
        db_connection: AsyncSession,
        ticket_type_in_db: TicketType,
    ) -> None:
        ticket = TicketFactory.build(
            owner_id=normal_user.id,
            ticket_type_id=ticket_type_in_db.id,
            owner=None,
            ticket_type=None,
        )

        setattr(ticket, invalid_fk_field, 9999)

        db_connection.add(ticket)

        with pytest.raises(IntegrityError):
            await db_connection.commit()

        await db_connection.rollback()

    async def test_restrict_delete_owner(
        self,
        ticket_in_db: Ticket,
        db_connection: AsyncSession,
    ) -> None:
        await db_connection.delete(ticket_in_db.owner)

        with pytest.raises(IntegrityError):
            await db_connection.commit()

        await db_connection.rollback()

    async def test_event_title_property_with_loaded_relations(
        self,
        normal_user: User,
        ticket_type_in_db: TicketType,
        db_connection: AsyncSession,
    ) -> None:
        ticket = TicketFactory.build(owner=normal_user, ticket_type=ticket_type_in_db)
        db_connection.add(ticket)
        await db_connection.commit()

        query = (
            select(Ticket)
            .options(selectinload(Ticket.ticket_type).joinedload(TicketType.event))
            .where(Ticket.id == ticket.id)
        )
        loaded_ticket = await db_connection.scalar(query)

        assert loaded_ticket is not None
        assert loaded_ticket.event_title == ticket_type_in_db.event.title
