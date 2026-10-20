from datetime import UTC, datetime
from typing import TypeVar

from polyfactory import Use
from polyfactory.factories.pydantic_factory import ModelFactory
from polyfactory.factories.sqlalchemy_factory import SQLAlchemyFactory

from src.core.security import get_password_hash
from src.models import Event, Ticket, TicketType, User
from src.models.ticket import TicketStatus
from src.schemas import EventCreate, TicketCreate, UserCreate
from src.schemas.ticket_type import TicketTypeCreate

T = TypeVar("T")


class BaseSQLFactory(SQLAlchemyFactory[T]):
    __is_base_factory__ = True
    __set_primary_key__ = False


class UserFactory(BaseSQLFactory[User]):
    __model__ = User

    is_superuser = False
    is_active = True
    tickets: list[Ticket] = []

    @classmethod
    def hashed_password(cls) -> str:
        return get_password_hash("very_secure_password")

    @classmethod
    def email(cls) -> str:
        return str(cls.__faker__.unique.email())


class EventFactory(BaseSQLFactory[Event]):
    __model__ = Event

    ticket_types: list[TicketType] = []

    @classmethod
    def date(cls) -> datetime:
        return cls.__faker__.date_time(tzinfo=UTC)

    @classmethod
    def title(cls) -> str:
        return cls.__faker__.catch_phrase()

    @classmethod
    def country(cls) -> str:
        return cls.__faker__.country()

    @classmethod
    def city(cls) -> str:
        return cls.__faker__.city()

    @classmethod
    def street_address(cls) -> str:
        return cls.__faker__.street_address()


class TicketTypeFactory(BaseSQLFactory[TicketType]):
    __model__ = TicketType

    event = Use(EventFactory.build)
    tickets: list[Ticket] = []

    @classmethod
    def name(cls) -> str:
        return cls.__faker__.word()

    @classmethod
    def tickets_quantity(cls) -> int:
        return cls.__faker__.random_int(min=50, max=500)

    @classmethod
    def tickets_sold(cls) -> int:
        return cls.__faker__.random_int(min=0, max=49)

    @classmethod
    def price(cls) -> int:
        return cls.__faker__.random_int(min=10, max=500)


class TicketFactory(BaseSQLFactory[Ticket]):
    __model__ = Ticket

    owner = Use(UserFactory.build)
    ticket_type = Use(TicketTypeFactory.build)
    status = TicketStatus.RESERVED


class EventPayloadFactory(ModelFactory[EventCreate]):
    __model__ = EventCreate


class TicketTypePayloadFactory(ModelFactory[TicketTypeCreate]):
    __model__ = TicketTypeCreate


class TicketPayloadFactory(ModelFactory[TicketCreate]):
    __model__ = TicketCreate


class UserPayloadFactory(ModelFactory[UserCreate]):
    __model__ = UserCreate
