import pytest
from fastapi import status
from httpx import AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models import Event, TicketType, User
from src.schemas import EventCreate
from tests.factories import (
    EventFactory,
    EventPayloadFactory,
    TicketFactory,
    TicketTypeFactory,
)

BASE_URL = "/api/v1/events"


@pytest.mark.asyncio
class TestEventPost:
    @pytest.fixture
    async def payload(self) -> EventCreate:
        return EventPayloadFactory.build()

    async def test_valid(
        self,
        payload: EventCreate,
        db_connection: AsyncSession,
        authorized_superuser: AsyncClient,
    ) -> None:
        response = await authorized_superuser.post(
            BASE_URL, json=payload.model_dump(mode="json")
        )
        assert response.status_code == status.HTTP_201_CREATED

        data = response.json()
        assert data["title"] == payload.title

        found_event = await db_connection.get(Event, data["id"])

        assert found_event is not None
        assert found_event.title == payload.title

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
        payload: EventCreate,
        api_client: AsyncClient,
        db_connection: AsyncSession,
    ) -> None:
        response = await api_client.post(BASE_URL, json=payload.model_dump(mode="json"))
        assert response.status_code == expected_status

        query = select(Event).where(Event.title == payload.title)
        db_event = await db_connection.scalar(query)
        assert db_event is None

    class TestIdempotency:
        async def test_valid(
            self,
            payload: EventCreate,
            db_connection: AsyncSession,
            authorized_superuser: AsyncClient,
            idempotency_header: dict[str, str],
        ) -> None:
            response_first = await authorized_superuser.post(
                BASE_URL,
                json=payload.model_dump(mode="json"),
                headers=idempotency_header,
            )
            assert response_first.status_code == status.HTTP_201_CREATED

            response_second = await authorized_superuser.post(
                BASE_URL,
                json=payload.model_dump(mode="json"),
                headers=idempotency_header,
            )
            assert response_second.status_code == status.HTTP_201_CREATED

            query = select(func.count(Event.id)).where(Event.title == payload.title)
            event_query = await db_connection.scalar(query)

            assert event_query == 1

        async def test_key_mismatch_payload(
            self,
            payload: EventCreate,
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
                update={"title": f"{payload.title[:50]} (FAKE)"}
            )
            response_invalid = await authorized_superuser.post(
                BASE_URL,
                json=mismatched_payload.model_dump(mode="json"),
                headers=idempotency_header,
            )
            assert response_invalid.status_code == status.HTTP_409_CONFLICT

            query_valid = select(func.count(Event.id)).where(
                Event.title == payload.title
            )
            count_valid = await db_connection.scalar(query_valid)
            assert count_valid == 1

            query_mismatched = select(func.count(Event.id)).where(
                Event.title == mismatched_payload.title
            )
            count_mismatched = await db_connection.scalar(query_mismatched)
            assert count_mismatched == 0


@pytest.mark.asyncio
class TestEventGet:
    async def test_get_event(self, client: AsyncClient, event_in_db: Event) -> None:
        response = await client.get(f"{BASE_URL}/{event_in_db.id}")

        assert response.status_code == status.HTTP_200_OK
        event = response.json()
        assert event["title"] == event_in_db.title

    async def test_get_all_events(
        self, client: AsyncClient, db_connection: AsyncSession
    ) -> None:
        factory_events = EventFactory.batch(10)

        db_connection.add_all(factory_events)
        await db_connection.commit()

        response = await client.get(BASE_URL)
        assert response.status_code == status.HTTP_200_OK

        response_events = response.json()
        assert isinstance(response_events, list)
        assert len(response_events) == 10

        returned_events = sorted([item["id"] for item in response_events])
        created_events = sorted([e.id for e in factory_events])

        assert returned_events == created_events

    async def test_get_non_existent_event(
        self, client: AsyncClient, db_connection: AsyncSession
    ) -> None:
        id = 999
        response = await client.get(f"{BASE_URL}{id}")
        assert response.status_code == status.HTTP_404_NOT_FOUND

        db_event = await db_connection.get(Event, id)
        assert db_event is None

    async def test_event_pagination(
        self, client: AsyncClient, db_connection: AsyncSession
    ) -> None:
        factory_events = EventFactory.batch(10)
        db_connection.add_all(factory_events)
        await db_connection.commit()

        response_p1 = await client.get(f"{BASE_URL}?limit=5&offset=0")
        assert response_p1.status_code == status.HTTP_200_OK

        events_p1 = response_p1.json()
        assert isinstance(events_p1, list)
        assert len(events_p1) == 5

        response_p2 = await client.get(f"{BASE_URL}?limit=5&offset=5")
        assert response_p2.status_code == status.HTTP_200_OK

        events_p2 = response_p2.json()
        assert isinstance(events_p2, list)
        assert len(events_p2) == 5

        response_p3 = await client.get(f"{BASE_URL}?limit=5&offset=10")
        assert response_p3.status_code == status.HTTP_200_OK

        events_p3 = response_p3.json()
        assert len(events_p3) == 0
        assert isinstance(events_p3, list)

    class TestTicketTypeForEvent:
        async def test_get_ticket_type_for_event(
            self, client: AsyncClient, event_in_db: Event, db_connection: AsyncSession
        ) -> None:
            factory_ticket_types = TicketTypeFactory.batch(size=5, event=event_in_db)
            db_connection.add_all(factory_ticket_types)
            await db_connection.commit()

            resource = await client.get(f"{BASE_URL}/{event_in_db.id}/ticket-types")
            assert resource.status_code == status.HTTP_200_OK

            data = resource.json()
            assert isinstance(data, list)
            assert len(data) == 5

            for item in data:
                assert item["event_id"] == event_in_db.id

        async def test_pagination_for_get_all_ticket_type_for_event(
            self, client: AsyncClient, event_in_db: Event, db_connection: AsyncSession
        ) -> None:
            factory_ticket_types = TicketTypeFactory.batch(size=10, event=event_in_db)
            db_connection.add_all(factory_ticket_types)
            await db_connection.commit()

            resource1 = await client.get(
                f"{BASE_URL}/{event_in_db.id}/ticket-types?limit=5&offset=0"
            )
            assert resource1.status_code == status.HTTP_200_OK

            data1 = resource1.json()
            assert isinstance(data1, list)
            assert len(data1) == 5

            for item in data1:
                assert item["event_id"] == event_in_db.id

            resource2 = await client.get(
                f"{BASE_URL}/{event_in_db.id}/ticket-types?limit=5&offset=5"
            )
            assert resource2.status_code == status.HTTP_200_OK

            data2 = resource2.json()
            assert isinstance(data2, list)
            assert len(data2) == 5

            for item in data2:
                assert item["event_id"] == event_in_db.id

            resource3 = await client.get(
                f"{BASE_URL}/{event_in_db.id}/ticket-types?limit=5&offset=10"
            )
            assert resource3.status_code == status.HTTP_200_OK

            data3 = resource3.json()
            assert isinstance(data3, list)
            assert len(data3) == 0


@pytest.mark.asyncio
class TestEventPatch:
    async def test_valid(
        self,
        event_in_db: Event,
        db_connection: AsyncSession,
        authorized_superuser: AsyncClient,
    ) -> None:
        orig_id = event_in_db.id
        payload = {"title": "New Title"}

        response = await authorized_superuser.patch(
            f"{BASE_URL}/{event_in_db.id}", json=payload
        )
        assert response.status_code == status.HTTP_200_OK

        db_connection.expire_all()

        updated_event = await db_connection.get(Event, orig_id)

        assert updated_event is not None
        assert updated_event.title == payload.get("title")

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
        event_in_db: Event,
        expected_status: int,
        api_client: AsyncClient,
        db_connection: AsyncSession,
    ) -> None:
        orig_title = event_in_db.title
        payload = {"title": "New Title"}

        response = await api_client.patch(f"{BASE_URL}/{event_in_db.id}", json=payload)
        assert response.status_code == expected_status

        db_event = await db_connection.get(Event, event_in_db.id)

        assert db_event is not None
        assert db_event.title == orig_title
        assert db_event.title != payload.get("title")

    async def test_non_existent_event(
        self,
        authorized_superuser: AsyncClient,
        db_connection: AsyncSession,
    ) -> None:
        non_existent = 999
        payload = {"title": "New Title"}

        response = await authorized_superuser.patch(
            f"{BASE_URL}{non_existent}", json=payload
        )
        assert response.status_code == status.HTTP_404_NOT_FOUND

        db_event = await db_connection.get(Event, non_existent)
        assert db_event is None


@pytest.mark.asyncio
class TestEventDelete:
    async def test_valid(
        self,
        event_in_db: Event,
        db_connection: AsyncSession,
        authorized_superuser: AsyncClient,
    ) -> None:
        event_id = event_in_db.id

        response = await authorized_superuser.delete(f"{BASE_URL}/{event_id}")
        assert response.status_code == status.HTTP_204_NO_CONTENT

        db_connection.expunge_all()

        db_event = await db_connection.get(Event, event_id)
        assert db_event is None

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
        event_in_db: Event,
        expected_status: int,
        api_client: AsyncClient,
        db_connection: AsyncSession,
    ) -> None:
        event_id = event_in_db.id

        response = await api_client.delete(f"{BASE_URL}/{event_id}")
        assert response.status_code == expected_status

        db_event = await db_connection.get(Event, event_id)
        assert db_event is not None

    async def test_non_existent_event(
        self,
        db_connection: AsyncSession,
        authorized_superuser: AsyncClient,
    ) -> None:
        non_existent = 999

        response = await authorized_superuser.delete(f"{BASE_URL}/{non_existent}")
        assert response.status_code == status.HTTP_404_NOT_FOUND

        db_event = await db_connection.get(Event, non_existent)
        assert db_event is None

    async def test_delete_event_with_existing_tickets(
        self,
        normal_user: User,
        event_in_db: Event,
        db_connection: AsyncSession,
        ticket_type_in_db: TicketType,
        authorized_superuser: AsyncClient,
    ) -> None:
        ticket = TicketFactory.build(owner=normal_user, ticket_type=ticket_type_in_db)
        db_connection.add(ticket)
        await db_connection.commit()

        response = await authorized_superuser.delete(f"{BASE_URL}/{event_in_db.id}")
        assert response.status_code == status.HTTP_409_CONFLICT

        db_connection.expunge_all()
