import pytest
from fastapi import status
from httpx import AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models import Event, TicketType
from src.schemas.ticket_type import TicketTypeCreate
from tests.factories import TicketFactory, TicketTypePayloadFactory

BASE_URL = "/api/v1/ticket-types"


@pytest.mark.asyncio
class TestTicketTypePost:
    @pytest.fixture
    async def payload(self, event_in_db: Event) -> TicketTypeCreate:
        return TicketTypePayloadFactory.build(event_id=event_in_db.id)

    async def test_valid(
        self,
        payload: TicketTypeCreate,
        db_connection: AsyncSession,
        authorized_superuser: AsyncClient,
    ) -> None:
        response = await authorized_superuser.post(
            BASE_URL, json=payload.model_dump(mode="json")
        )
        assert response.status_code == status.HTTP_201_CREATED

        data = response.json()
        assert data["name"] == payload.name

        found_ticket_type = await db_connection.get(TicketType, data["id"])

        assert found_ticket_type is not None
        assert found_ticket_type.name == payload.name

    @pytest.mark.parametrize(
        "api_client, expected_status",
        [
            ("authorized_user", status.HTTP_403_FORBIDDEN),
            ("client", status.HTTP_401_UNAUTHORIZED),
        ],
        indirect=["api_client"],
    )
    async def test_access_denied(
        self,
        expected_status: int,
        api_client: AsyncClient,
        payload: TicketTypeCreate,
        db_connection: AsyncSession,
    ) -> None:
        response = await api_client.post(BASE_URL, json=payload.model_dump(mode="json"))
        assert response.status_code == expected_status

        queue = (
            select(TicketType)
            .where(TicketType.event_id == payload.event_id)
            .where(TicketType.name == payload.name)
        )
        found_ticket_type = await db_connection.scalar(queue)
        assert found_ticket_type is None

    class TestIdempotency:
        async def test_valid(
            self,
            payload: TicketTypeCreate,
            db_connection: AsyncSession,
            authorized_superuser: AsyncClient,
            idempotency_header: dict[str, str],
        ) -> None:
            request_first = await authorized_superuser.post(
                BASE_URL,
                json=payload.model_dump(mode="json"),
                headers=idempotency_header,
            )
            assert request_first.status_code == status.HTTP_201_CREATED

            request_second = await authorized_superuser.post(
                BASE_URL,
                json=payload.model_dump(mode="json"),
                headers=idempotency_header,
            )
            assert request_second.status_code == status.HTTP_201_CREATED

            query = select(func.count(TicketType.id)).where(
                TicketType.event_id == payload.event_id
            )
            ticket_type_count = await db_connection.scalar(query)

            assert ticket_type_count == 1

        async def test_key_mismatch_payload(
            self,
            payload: TicketTypeCreate,
            db_connection: AsyncSession,
            authorized_superuser: AsyncClient,
            idempotency_header: dict[str, str],
        ) -> None:
            response_valid = await authorized_superuser.post(
                BASE_URL,
                json=payload.model_dump(mode="json"),
                headers=idempotency_header,
            )
            assert response_valid.status_code == status.HTTP_201_CREATED

            mismatched_payload = payload.model_copy(
                update={"event_id": payload.event_id + 1}
            )
            response_invalid = await authorized_superuser.post(
                BASE_URL,
                json=mismatched_payload.model_dump(mode="json"),
                headers=idempotency_header,
            )
            assert response_invalid.status_code == status.HTTP_409_CONFLICT

            query_valid = select(func.count(TicketType.id)).where(
                TicketType.event_id == payload.event_id
            )
            count_valid = await db_connection.scalar(query_valid)
            assert count_valid == 1

            query_mismatched = select(func.count(TicketType.id)).where(
                TicketType.event_id == mismatched_payload.event_id
            )
            count_mismatched = await db_connection.scalar(query_mismatched)
            assert count_mismatched == 0


@pytest.mark.asyncio
class TestTicketTypeGet:
    async def test_get_by_id(
        self, client: AsyncClient, ticket_type_in_db: TicketType
    ) -> None:
        responses = await client.get(f"{BASE_URL}/{ticket_type_in_db.id}")
        assert responses.status_code == status.HTTP_200_OK

        ticket_type = responses.json()
        assert ticket_type["name"] == ticket_type_in_db.name

    async def test_get_non_existent_ticket_type(
        self, client: AsyncClient, db_connection: AsyncSession
    ) -> None:
        id = 999
        result = await client.get(f"{BASE_URL}/{id}")
        assert result.status_code == status.HTTP_404_NOT_FOUND

        db_ticket_type = await db_connection.get(TicketType, id)
        assert db_ticket_type is None


@pytest.mark.asyncio
class TestTicketTypePatch:
    @pytest.fixture
    async def payload(self) -> dict[str, str]:
        return {"name": "new name"}

    async def test_valid(
        self,
        payload: dict[str, str],
        db_connection: AsyncSession,
        ticket_type_in_db: TicketType,
        authorized_superuser: AsyncClient,
    ) -> None:
        ticket_type_id = ticket_type_in_db.id
        ticket_type_name = ticket_type_in_db.name

        result = await authorized_superuser.patch(
            f"{BASE_URL}/{ticket_type_in_db.id}", json=payload
        )
        assert result.status_code == status.HTTP_200_OK

        updated_ticket_type = result.json()

        assert updated_ticket_type["name"] != ticket_type_name
        assert updated_ticket_type["name"] == payload["name"]

        db_connection.expire_all()

        db_ticket_type = await db_connection.get(TicketType, ticket_type_id)
        assert db_ticket_type is not None
        assert db_ticket_type.name != ticket_type_name
        assert db_ticket_type.name == payload["name"]

    @pytest.mark.parametrize(
        "api_client, expected_status",
        [
            ("authorized_user", status.HTTP_403_FORBIDDEN),
            ("client", status.HTTP_401_UNAUTHORIZED),
        ],
        indirect=["api_client"],
    )
    async def test_access_denied(
        self,
        payload: dict[str, str],
        expected_status: int,
        api_client: AsyncClient,
        db_connection: AsyncSession,
        ticket_type_in_db: TicketType,
    ) -> None:
        result = await api_client.patch(
            f"{BASE_URL}/{ticket_type_in_db.id}", json=payload
        )
        assert result.status_code == expected_status

        db_ticket_type = await db_connection.get(TicketType, ticket_type_in_db.id)
        assert db_ticket_type is not None

    async def test_non_existent_ticket_type(
        self,
        payload: dict[str, str],
        db_connection: AsyncSession,
        authorized_superuser: AsyncClient,
    ) -> None:
        non_existent_id = 999

        result = await authorized_superuser.patch(
            f"{BASE_URL}/{non_existent_id}", json=payload
        )
        assert result.status_code == status.HTTP_404_NOT_FOUND

        db_ticket_type = await db_connection.get(TicketType, non_existent_id)
        assert db_ticket_type is None

    async def test_patch_quantity_less_than_sold(
        self,
        db_connection: AsyncSession,
        ticket_type_in_db: TicketType,
        authorized_superuser: AsyncClient,
    ) -> None:
        ticket_type_in_db.tickets_quantity = 100
        ticket_type_in_db.tickets_sold = 50
        db_connection.add(ticket_type_in_db)
        await db_connection.commit()

        payload = {"tickets_quantity": 10}

        result = await authorized_superuser.patch(
            f"{BASE_URL}/{ticket_type_in_db.id}", json=payload
        )

        assert result.status_code == status.HTTP_409_CONFLICT

    async def test_cannot_change_event_id(
        self,
        db_connection: AsyncSession,
        ticket_type_in_db: TicketType,
        authorized_superuser: AsyncClient,
    ) -> None:
        payload = {"event_id": 99999}

        result = await authorized_superuser.patch(
            f"{BASE_URL}/{ticket_type_in_db.id}", json=payload
        )
        assert result.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.asyncio
class TestTicketTypeDelete:
    async def test_valid(
        self,
        db_connection: AsyncSession,
        ticket_type_in_db: TicketType,
        authorized_superuser: AsyncClient,
    ) -> None:
        result = await authorized_superuser.delete(f"{BASE_URL}/{ticket_type_in_db.id}")
        assert result.status_code == status.HTTP_204_NO_CONTENT

        db_connection.expunge_all()

        db_ticket_type = await db_connection.get(TicketType, ticket_type_in_db.id)
        assert db_ticket_type is None

    @pytest.mark.parametrize(
        "api_client, expected_status",
        [
            ("authorized_user", status.HTTP_403_FORBIDDEN),
            ("client", status.HTTP_401_UNAUTHORIZED),
        ],
        indirect=["api_client"],
    )
    async def test_access_denied(
        self,
        expected_status: int,
        api_client: AsyncClient,
        db_connection: AsyncSession,
        ticket_type_in_db: TicketType,
    ) -> None:
        result = await api_client.delete(f"{BASE_URL}/{ticket_type_in_db.id}")
        assert result.status_code == expected_status

        db_ticket_type = await db_connection.get(TicketType, ticket_type_in_db.id)
        assert db_ticket_type is not None

    async def test_non_existent_ticket_type(
        self, authorized_superuser: AsyncClient, db_connection: AsyncSession
    ) -> None:
        non_existent_id = 999

        result = await authorized_superuser.delete(f"{BASE_URL}/{non_existent_id}")
        assert result.status_code == status.HTTP_404_NOT_FOUND

        db_ticket_type = await db_connection.get(TicketType, non_existent_id)
        assert db_ticket_type is None

    async def test_delete_with_ticket_types(
        self,
        ticket_type_in_db: TicketType,
        db_connection: AsyncSession,
        authorized_superuser: AsyncClient,
    ) -> None:
        tickets = TicketFactory.batch(size=5, ticket_type=ticket_type_in_db)
        db_connection.add_all(tickets)
        await db_connection.commit()

        db_connection.expunge_all()

        result = await authorized_superuser.delete(f"{BASE_URL}/{ticket_type_in_db.id}")
        assert result.status_code == status.HTTP_409_CONFLICT
