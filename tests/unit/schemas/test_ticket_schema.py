from typing import Any

import pytest
from pydantic import ValidationError

from src.models import Event, Ticket, TicketType
from src.models.ticket import TicketStatus
from src.schemas import TicketCreate, TicketResponse


class TestTicketCreateSchema:
    VALID_PAYLOAD: dict[str, int] = {"ticket_type_id": 1}

    def test_valid(self) -> None:
        schema = TicketCreate(**self.VALID_PAYLOAD)

        assert schema.ticket_type_id == self.VALID_PAYLOAD["ticket_type_id"]

    @pytest.mark.parametrize("invalid_type_id", [None, "string_id", -1, 0])
    def test_invalid_ticket_type_id(self, invalid_type_id: Any) -> None:
        invalid_data = {"ticket_type_id": invalid_type_id}
        with pytest.raises(ValidationError):
            TicketCreate(**invalid_data)

    def test_missing_fields(self) -> None:
        payload: dict[str, Any] = {}
        with pytest.raises(ValidationError):
            TicketCreate(**payload)


class TestTicketResponseSchema:
    def test_model_validate_from_orm(self) -> None:
        event = Event(title="Korn 2026")
        ticket_type = TicketType(id=1, event=event)
        ticket = Ticket(
            id=1,
            ticket_type_id=1,
            ticket_type=ticket_type,
            status=TicketStatus.RESERVED,
        )

        ticket_response = TicketResponse.model_validate(ticket)

        assert ticket_response.id == 1
        assert ticket_response.ticket_type_id == 1
