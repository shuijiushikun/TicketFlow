import asyncio

import pytest
from httpx import AsyncClient, Response
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.api.deps import get_current_user
from src.main import app
from src.models import TicketType, User
from tests.factories import EventFactory, TicketTypeFactory, UserFactory


@pytest.mark.asyncio
async def test_race_condition_overselling(
    stress_client: AsyncClient,
    test_session_factory: async_sessionmaker[AsyncSession],
    cleanup_physical_db: None,
) -> None:

    async with test_session_factory() as session:
        user = UserFactory.build()
        session.add(user)
        await session.commit()

        event = EventFactory.build()
        session.add(event)
        await session.commit()

        ticket_type = TicketTypeFactory.build(
            event=event, tickets_quantity=5, tickets_sold=0
        )
        session.add(ticket_type)
        await session.commit()

        await session.refresh(user)
        await session.refresh(ticket_type)

        ticket_type_id = ticket_type.id
        physical_user_id = user.id

    app.dependency_overrides[get_current_user] = lambda: User(
        id=physical_user_id, email="stress@test.com", is_active=True
    )

    try:
        ticket_payload = {"ticket_type_id": ticket_type_id}

        async def buy_ticket() -> Response:
            return await stress_client.post("/api/v1/tickets", json=ticket_payload)

        buy_ticket_tasks = [buy_ticket() for _ in range(50)]
        responses = await asyncio.gather(*buy_ticket_tasks)

        first_error = next(
            (r for r in responses if r.status_code not in (201, 409)), None
        )
        if first_error:
            pytest.fail(
                f"Request failed! Status: {first_error.status_code}, "
                f"Body: {first_error.json()}"
            )

        success_count = sum(1 for r in responses if r.status_code == 201)
        conflict_count = sum(1 for r in responses if r.status_code == 409)

        assert success_count == 5
        assert conflict_count == 45

        async with test_session_factory() as session:
            updated_ticket_type = await session.get(TicketType, ticket_type_id)
            assert updated_ticket_type is not None
            assert updated_ticket_type.tickets_sold == 5

    finally:
        app.dependency_overrides.pop(get_current_user, None)
