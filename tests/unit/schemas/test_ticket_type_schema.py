from typing import Any

import pytest
from pydantic import ValidationError

from src.models import Event, TicketType
from src.schemas.ticket_type import (
    TicketTypeCreate,
    TicketTypeResponse,
    TicketTypeUpdate,
)


class TestTicketTypeCreate:
    VALID_PAYLOAD: dict[str, Any] = {
        "event_id": 1,
        "name": "Basic",
        "price": 10000,
        "tickets_quantity": 100,
    }

    def test_valid(self) -> None:
        ticket_type: TicketTypeCreate = TicketTypeCreate(**self.VALID_PAYLOAD)

        assert ticket_type.event_id == self.VALID_PAYLOAD["event_id"]
        assert ticket_type.name == self.VALID_PAYLOAD["name"]
        assert ticket_type.price == self.VALID_PAYLOAD["price"]
        assert ticket_type.tickets_quantity == self.VALID_PAYLOAD["tickets_quantity"]

    @pytest.mark.parametrize(
        "field, invalid_value, error_message_part",
        [
            ("price", -1, "Input should be greater than or equal to 0"),
            ("price", -100, "Input should be greater than or equal to 0"),
            ("tickets_quantity", 0, "Input should be greater than 0"),
            ("tickets_quantity", -1, "Input should be greater than 0"),
            ("tickets_quantity", -100, "Input should be greater than 0"),
            ("name", "", "String should have at least 1 character"),
            ("name", "   ", "String should have at least 1 character"),
            ("name", "a" * 51, "String should have at most 50 characters"),
            ("event_id", 0, "Input should be greater than 0"),
            ("event_id", -1, "Input should be greater than 0"),
        ],
    )
    def test_invalid_params(
        self, field: str, invalid_value: Any, error_message_part: str
    ) -> None:
        payload = self.VALID_PAYLOAD.copy()
        payload[field] = invalid_value

        with pytest.raises(ValidationError) as exc:
            TicketTypeCreate(**payload)

        for error in exc.value.errors():
            if error["loc"][0] == field:
                assert error_message_part in error["msg"]
                return
        pytest.fail("Error not found")

    @pytest.mark.parametrize("field", ["name", "price", "tickets_quantity", "event_id"])
    def test_missing_fields(self, field: str) -> None:
        payload = self.VALID_PAYLOAD.copy()
        payload[field] = None

        with pytest.raises(ValidationError):
            TicketTypeCreate(**payload)


class TestTicketTypeUpdateSchema:
    @pytest.mark.parametrize(
        "update_payload", [{"price": 1000}, {"name": "VIP"}, {"tickets_quantity": 500}]
    )
    def test_valid_partial_update(self, update_payload: dict[str, Any]) -> None:
        schema = TicketTypeUpdate(**update_payload)

        for field, expected_value in update_payload.items():
            assert getattr(schema, field) == expected_value

        dumped_data = schema.model_dump(exclude_unset=True)
        assert dumped_data == update_payload

    @pytest.mark.parametrize(
        "update_payload",
        [
            {"name": ""},
            {"name": "a" * 51},
            {"price": -1},
            {"price": -100},
            {"tickets_quantity": 0},
            {"tickets_quantity": -1},
            {"tickets_quantity": -100},
        ],
    )
    def test_invalid_partial_update(self, update_payload: dict[str, Any]) -> None:
        with pytest.raises(ValidationError):
            TicketTypeUpdate(**update_payload)


class TestTicketTypeResponseSchema:
    def test_model_validate_from_orm(self) -> None:
        event = Event(id=1, title="Korn 2026")

        ticket_type = TicketType(
            id=42,
            name="VIP",
            price=15000,
            tickets_quantity=100,
            tickets_sold=10,
            event_id=event.id,
            event=event,
        )

        response = TicketTypeResponse.model_validate(ticket_type)

        assert response.id == 42
        assert response.name == "VIP"
        assert response.price == 15000
        assert response.tickets_quantity == 100
        assert response.event_id == 1
