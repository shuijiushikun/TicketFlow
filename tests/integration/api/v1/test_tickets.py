from typing import Any

import pytest
from fastapi import status
from httpx import AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models import Ticket, TicketType, User
from src.models.ticket import TicketStatus
from src.schemas import TicketCreate
from tests.factories import TicketFactory, TicketPayloadFactory

BASE_URL = "/api/v1/tickets"


@pytest.mark.asyncio
class TestTicketPost:
    @pytest.fixture
    def payload(self, ticket_type_in_db: TicketType) -> TicketCreate:
        return TicketPayloadFactory.build(ticket_type_id=ticket_type_in_db.id)

    async def test_valid(
        self,
        payload: TicketCreate,
        db_connection: AsyncSession,
        authorized_user: AsyncClient,
        ticket_type_in_db: TicketType,
    ) -> None:
        orig_sold = ticket_type_in_db.tickets_sold
        ticket_type_id = ticket_type_in_db.id

        response = await authorized_user.post(
            BASE_URL, json=payload.model_dump(mode="json")
        )
        assert response.status_code == status.HTTP_201_CREATED

        data = response.json()
        assert data["ticket_type_id"] == payload.ticket_type_id

        db_connection.expire_all()

        updated_ticket_type = await db_connection.get(TicketType, ticket_type_id)

        assert updated_ticket_type is not None
        assert updated_ticket_type.tickets_sold == orig_sold + 1

    async def test_access_denied(
        self,
        client: AsyncClient,
        payload: TicketCreate,
    ) -> None:
        response = await client.post(BASE_URL, json=payload.model_dump(mode="json"))
        assert response.status_code == status.HTTP_401_UNAUTHORIZED

    async def test_sold_out(
        self,
        payload: TicketCreate,
        db_connection: AsyncSession,
        authorized_user: AsyncClient,
        ticket_type_in_db: TicketType,
    ) -> None:
        ticket_type_in_db.tickets_sold = ticket_type_in_db.tickets_quantity
        db_connection.add(ticket_type_in_db)
        await db_connection.commit()

        response = await authorized_user.post(
            BASE_URL, json=payload.model_dump(mode="json")
        )
        assert response.status_code == status.HTTP_409_CONFLICT

    async def test_non_existent_ticket_type(self, authorized_user: AsyncClient) -> None:
        payload = {"ticket_type_id": 999}

        response = await authorized_user.post(BASE_URL, json=payload)
        assert response.status_code == status.HTTP_404_NOT_FOUND

    @pytest.mark.parametrize(
        "invalid_payload",
        [
            {"ticket_type_id": -1},
            {"ticket_type_id": "a"},
            {},
        ],
    )
    async def test_invalid_payload(
        self, authorized_user: AsyncClient, invalid_payload: dict[str, Any]
    ) -> None:
        response = await authorized_user.post(BASE_URL, json=invalid_payload)
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT

    class TestIdempotency:
        async def test_valid(
            self,
            payload: TicketCreate,
            db_connection: AsyncSession,
            authorized_user: AsyncClient,
            idempotency_header: dict[str, str],
        ) -> None:
            response_first = await authorized_user.post(
                BASE_URL,
                json=payload.model_dump(mode="json"),
                headers=idempotency_header,
            )
            assert response_first.status_code == status.HTTP_201_CREATED

            response_second = await authorized_user.post(
                BASE_URL,
                json=payload.model_dump(mode="json"),
                headers=idempotency_header,
            )
            assert response_second.status_code == status.HTTP_201_CREATED

            query = select(func.count(Ticket.id)).where(
                Ticket.ticket_type_id == payload.ticket_type_id
            )
            tickets_count = await db_connection.scalar(query)

            assert tickets_count == 1

        async def test_key_mismatch_payload(
            self,
            payload: TicketCreate,
            db_connection: AsyncSession,
            authorized_user: AsyncClient,
            idempotency_header: dict[str, str],
        ) -> None:
            response_valid = await authorized_user.post(
                BASE_URL,
                json=payload.model_dump(mode="json"),
                headers=idempotency_header,
            )
            assert response_valid.status_code == status.HTTP_201_CREATED

            mismatched_payload = payload.model_copy(
                update={"ticket_type_id": payload.ticket_type_id + 1}
            )

            response_invalid = await authorized_user.post(
                BASE_URL,
                json=mismatched_payload.model_dump(mode="json"),
                headers=idempotency_header,
            )

            assert response_invalid.status_code == status.HTTP_409_CONFLICT

            query_valid = select(func.count(Ticket.id)).where(
                Ticket.ticket_type_id == payload.ticket_type_id
            )
            count_valid = await db_connection.scalar(query_valid)
            assert count_valid == 1

            mismatched_query = select(func.count(Ticket.id)).where(
                Ticket.ticket_type_id == mismatched_payload.ticket_type_id
            )
            mismatched_count = await db_connection.scalar(mismatched_query)
            assert mismatched_count == 0


@pytest.mark.asyncio
class TestTicketGet:
    async def test_get_ticket(
        self,
        normal_user: User,
        db_connection: AsyncSession,
        authorized_user: AsyncClient,
        ticket_type_in_db: TicketType,
    ) -> None:
        ticket = TicketFactory.build(
            owner=normal_user,
            ticket_type=ticket_type_in_db,
        )
        db_connection.add(ticket)
        await db_connection.commit()
        await db_connection.refresh(ticket)

        response = await authorized_user.get(f"{BASE_URL}/{ticket.id}")
        assert response.status_code == status.HTTP_200_OK

        data = response.json()
        assert data["id"] == ticket.id

    async def test_get_all_tickets(
        self,
        normal_user: User,
        db_connection: AsyncSession,
        authorized_user: AsyncClient,
        ticket_type_in_db: TicketType,
    ) -> None:
        factory_tickets = TicketFactory.batch(
            10,
            owner=normal_user,
            ticket_type=ticket_type_in_db,
        )

        db_connection.add_all(factory_tickets)
        await db_connection.commit()

        response = await authorized_user.get(BASE_URL)
        assert response.status_code == status.HTTP_200_OK

        response_tickets = response.json()
        assert isinstance(response_tickets, list)
        assert len(response_tickets) == 10

        returned_tickets = sorted([item["id"] for item in response_tickets])
        created_tickets = sorted([t.id for t in factory_tickets])

        assert returned_tickets == created_tickets

    async def test_non_existent_ticket(self, authorized_user: AsyncClient) -> None:
        id = 999
        response = await authorized_user.get(f"{BASE_URL}{id}")
        assert response.status_code == status.HTTP_404_NOT_FOUND

    async def test_access_denied_other_user_ticket(
        self,
        superuser: User,
        db_connection: AsyncSession,
        authorized_user: AsyncClient,
        ticket_type_in_db: TicketType,
    ) -> None:
        other_user_ticket = TicketFactory.build(
            owner=superuser, ticket_type=ticket_type_in_db
        )
        db_connection.add(other_user_ticket)
        await db_connection.commit()
        await db_connection.refresh(other_user_ticket)

        response = await authorized_user.get(f"{BASE_URL}{other_user_ticket.id}")
        assert response.status_code == status.HTTP_404_NOT_FOUND

    async def test_ticket_pagination(
        self,
        normal_user: User,
        db_connection: AsyncSession,
        authorized_user: AsyncClient,
        ticket_type_in_db: TicketType,
    ) -> None:
        factory_tickets = TicketFactory.batch(
            10,
            owner=normal_user,
            ticket_type=ticket_type_in_db,
        )
        db_connection.add_all(factory_tickets)
        await db_connection.commit()

        response_p1 = await authorized_user.get(f"{BASE_URL}?limit=5&offset=0")
        assert response_p1.status_code == status.HTTP_200_OK

        tickets_p1 = response_p1.json()
        assert len(tickets_p1) == 5

        response_p2 = await authorized_user.get(f"{BASE_URL}?limit=5&offset=5")
        assert response_p2.status_code == status.HTTP_200_OK

        tickets_p2 = response_p2.json()
        assert len(tickets_p2) == 5

        response_p3 = await authorized_user.get(f"{BASE_URL}?limit=5&offset=10")
        assert response_p3.status_code == status.HTTP_200_OK

        tickets_p3 = response_p3.json()
        assert len(tickets_p3) == 0
        assert isinstance(tickets_p3, list)


@pytest.mark.asyncio
class TestTicketDelete:
    async def test_valid(
        self,
        normal_user: User,
        db_connection: AsyncSession,
        authorized_user: AsyncClient,
        ticket_type_in_db: TicketType,
    ) -> None:
        ticket_type_in_db.tickets_sold = 1
        ticket = TicketFactory.build(
            owner=normal_user,
            ticket_type=ticket_type_in_db,
        )
        db_connection.add(ticket)
        await db_connection.commit()
        await db_connection.refresh(ticket)

        ticket_id = ticket.id
        ticket_type_id = ticket_type_in_db.id

        response = await authorized_user.delete(f"{BASE_URL}/{ticket_id}")
        assert response.status_code == status.HTTP_204_NO_CONTENT

        db_connection.expunge_all()

        db_ticket = await db_connection.get(Ticket, ticket_id)
        assert db_ticket is not None
        assert db_ticket.status == TicketStatus.CANCELED

        updated_ticket_type = await db_connection.get(TicketType, ticket_type_id)
        assert updated_ticket_type is not None
        assert updated_ticket_type.tickets_sold == 0

    async def test_access_denied(
        self,
        normal_user: User,
        client: AsyncClient,
        db_connection: AsyncSession,
        ticket_type_in_db: TicketType,
    ) -> None:
        ticket = TicketFactory.build(owner=normal_user, ticket_type=ticket_type_in_db)
        db_connection.add(ticket)
        await db_connection.commit()
        await db_connection.refresh(ticket)

        response = await client.delete(f"{BASE_URL}/{ticket.id}")
        assert response.status_code == status.HTTP_401_UNAUTHORIZED

        db_connection.expunge_all()

        db_ticket = await db_connection.get(Ticket, ticket.id)
        assert db_ticket is not None

    async def test_non_existent_ticket(
        self,
        db_connection: AsyncSession,
        authorized_user: AsyncClient,
    ) -> None:
        non_existent = 999

        response = await authorized_user.delete(f"{BASE_URL}{non_existent}")
        assert response.status_code == status.HTTP_404_NOT_FOUND

        db_ticket = await db_connection.get(Ticket, non_existent)
        assert db_ticket is None

    async def test_access_denied_other_user_ticket(
        self,
        superuser: User,
        db_connection: AsyncSession,
        authorized_user: AsyncClient,
        ticket_type_in_db: TicketType,
    ) -> None:
        other_user_ticket = TicketFactory.build(
            owner=superuser, ticket_type=ticket_type_in_db
        )
        db_connection.add(other_user_ticket)
        await db_connection.commit()
        await db_connection.refresh(other_user_ticket)

        response = await authorized_user.delete(f"{BASE_URL}{other_user_ticket.id}")
        assert response.status_code == status.HTTP_404_NOT_FOUND

        db_connection.expunge_all()

        db_ticket = await db_connection.get(Ticket, other_user_ticket.id)
        assert db_ticket is not None
