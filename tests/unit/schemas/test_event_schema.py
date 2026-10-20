from datetime import UTC, datetime
from typing import Any

import pytest
from pydantic import ValidationError

from src.schemas import EventCreate, EventUpdate
from tests.utils import get_missing_field_cases


class TestEventCreateSchema:
    VALID_PAYLOAD: dict[str, Any] = {
        "title": "Korn Europe Tour 2026",
        "date": "2026-01-01T14:15:45Z",
        "country": "Poland",
        "city": "Wroclaw",
        "street_address": "Sucha 1",
    }

    def test_valid(self) -> None:
        event: EventCreate = EventCreate(**self.VALID_PAYLOAD)

        assert event.title == self.VALID_PAYLOAD["title"]
        assert event.date == datetime.fromisoformat(self.VALID_PAYLOAD["date"])
        assert event.country == self.VALID_PAYLOAD["country"]
        assert event.city == self.VALID_PAYLOAD["city"]
        assert event.street_address == self.VALID_PAYLOAD["street_address"]

    @pytest.mark.parametrize(
        "missing_field, payload", get_missing_field_cases(VALID_PAYLOAD)
    )
    def test_with_missing_field(
        self, missing_field: str, payload: dict[str, Any]
    ) -> None:
        with pytest.raises(ValidationError) as exc:
            EventCreate(**payload)

        if missing_field != "all":
            assert any(e["loc"][0] == missing_field for e in exc.value.errors())

    @pytest.mark.parametrize(
        "field, invalid_value, error_message_part",
        [
            ("title", "", "String should have at least 1 character"),
            ("title", "a" * 101, "String should have at most 100 characters"),
            ("city", "", "String should have at least 1 character"),
            ("city", "a" * 101, "String should have at most 100 characters"),
            ("country", "", "String should have at least 1 character"),
            ("country", "a" * 101, "String should have at most 100 characters"),
            ("street_address", "", "String should have at least 1 character"),
            ("street_address", "a" * 101, "String should have at most 100 characters"),
        ],
    )
    def test_invalid_string_length(
        self, field: str, invalid_value: str, error_message_part: str
    ) -> None:
        payload = self.VALID_PAYLOAD.copy()
        payload[field] = invalid_value

        with pytest.raises(ValidationError) as exc:
            EventCreate(**payload)

        for error in exc.value.errors():
            if error["loc"][0] == field:
                assert error_message_part in error["msg"]
                return
        pytest.fail("Error not found")

    @pytest.mark.parametrize(
        "invalid_value, error_message_part",
        [
            ("not-a-date", "Input should be a valid datetime"),
            ("2026-13-45", "Input should be a valid datetime"),
        ],
    )
    def test_invalid_date(self, invalid_value: Any, error_message_part: str) -> None:
        payload = self.VALID_PAYLOAD.copy()
        payload["date"] = invalid_value

        with pytest.raises(ValidationError) as exc:
            EventCreate(**payload)

        for error in exc.value.errors():
            if error["loc"][0] == "date":
                assert error_message_part in error["msg"]
                return
        pytest.fail("Error not found")


class TestEventUpdateSchema:
    @pytest.mark.parametrize(
        "update_payload",
        [
            {"title": "Korn Europe Tour 2026"},
            {"date": datetime.now(UTC)},
            {"country": "Poland"},
            {"city": "Wroclaw"},
            {"street_address": "Sucha 1"},
        ],
    )
    def test_valid_partial_update(self, update_payload: dict[str, Any]) -> None:
        schema = EventUpdate(**update_payload)

        for field, expected_value in update_payload.items():
            assert getattr(schema, field) == expected_value

        dumped_data = schema.model_dump(exclude_unset=True)
        assert dumped_data == update_payload

    @pytest.mark.parametrize(
        "update_payload",
        [
            {"title": ""},
            {"title": "a" * 101},
            {"city": ""},
            {"city": "a" * 101},
            {"country": ""},
            {"country": "a" * 101},
            {"street_address": ""},
            {"street_address": "a" * 101},
        ],
    )
    def test_invalid_partial_update(self, update_payload: dict[str, Any]) -> None:
        with pytest.raises(ValidationError):
            EventUpdate(**update_payload)
