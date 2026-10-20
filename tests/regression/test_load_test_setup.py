"""压测数据和客户端配置使用本地 SQLite 验证，不连接业务资源。"""

import importlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import jwt
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from load_tests.seed import seed_db
from src.models import Base, Event, Ticket, TicketType, User
from src.models.ticket import TicketStatus


@pytest.mark.asyncio
async def test_seed_fills_dedicated_users_and_preserves_existing_sales(
    tmp_path: Path,
) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'loadtest.db'}")
    try:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        async with AsyncSession(engine, expire_on_commit=False) as session:
            ordinary = User(
                id=40,
                email="ordinary@example.com",
                hashed_password="ordinary-password",
                is_active=False,
            )
            existing = User(
                id=90,
                email="loadtest_1@example.com",
                hashed_password="existing-password",
                is_active=False,
            )
            event = Event(
                title="Rammstein: Europe Stadium Tour",
                date=datetime.now(UTC) + timedelta(days=30),
                country="Poland",
                city="Wrocław",
                street_address="Tarczyński Arena",
            )
            ticket_type = TicketType(
                event=event,
                name="Fan Zone",
                price=15000,
                tickets_quantity=12,
                tickets_sold=1,
            )
            ticket = Ticket(
                owner=existing, ticket_type=ticket_type, status=TicketStatus.SOLD
            )
            session.add_all([ordinary, ticket])
            await session.commit()
            event_id, ticket_type_id, ticket_id = event.id, ticket_type.id, ticket.id

        targets = await seed_db(engine, users_count=3)
        repeated = await seed_db(engine, users_count=3)

        assert targets == repeated
        assert targets.event_id == event_id
        assert ticket_type_id in targets.ticket_type_ids
        assert len(targets.user_ids) == 3
        assert 90 in targets.user_ids
        assert 40 not in targets.user_ids
        async with AsyncSession(engine) as session:
            users = (await session.scalars(select(User))).all()
            assert len(users) == 4
            dedicated = [user for user in users if user.id in targets.user_ids]
            assert all(user.is_active for user in dedicated)
            assert {user.email for user in dedicated} == {
                f"loadtest_{i}@example.com" for i in range(1, 4)
            }
            ordinary = await session.get(User, 40)
            assert ordinary is not None and not ordinary.is_active
            assert ordinary.hashed_password == "ordinary-password"
            saved_type = await session.get(TicketType, ticket_type_id)
            assert saved_type is not None
            assert saved_type.tickets_quantity == 12
            assert saved_type.tickets_sold == 1
            saved_ticket = await session.get(Ticket, ticket_id)
            assert saved_ticket is not None and saved_ticket.status == TicketStatus.SOLD
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_seed_can_explicitly_create_schema(tmp_path: Path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'new.db'}")
    try:
        targets = await seed_db(engine, users_count=2, create_schema=True)
        assert len(targets.user_ids) == 2
        assert len(targets.ticket_type_ids) == 2
    finally:
        await engine.dispose()


@pytest.mark.parametrize("users_count", [0, -1, True])
@pytest.mark.asyncio
async def test_seed_rejects_invalid_user_count(users_count: int) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    try:
        with pytest.raises(ValueError, match="users_count"):
            await seed_db(engine, users_count=users_count, create_schema=True)
    finally:
        await engine.dispose()


def test_manifest_round_trip_contains_only_target_ids(tmp_path: Path) -> None:
    from load_tests.targets import LoadTestTargets, load_targets, write_targets

    targets = LoadTestTargets(event_id=9, ticket_type_ids=[21, 30], user_ids=[40, 90])
    manifest = tmp_path / "artifacts" / "targets.json"
    write_targets(targets, manifest)
    assert load_targets(manifest) == targets
    assert json.loads(manifest.read_text(encoding="utf-8")) == {
        "event_id": 9,
        "ticket_type_ids": [21, 30],
        "user_ids": [40, 90],
    }


@pytest.mark.parametrize(
    "payload",
    [
        {"event_id": 0, "ticket_type_ids": [1], "user_ids": [1]},
        {"event_id": True, "ticket_type_ids": [1], "user_ids": [1]},
        {"event_id": 1, "ticket_type_ids": [], "user_ids": [1]},
        {"event_id": 1, "ticket_type_ids": [1], "user_ids": []},
        {"event_id": 1, "ticket_type_ids": [1], "user_ids": ["2"]},
        {"event_id": 1, "ticket_type_ids": [1]},
        [],
    ],
)
def test_manifest_rejects_invalid_ids(tmp_path: Path, payload: object) -> None:
    from load_tests.targets import load_targets

    manifest = tmp_path / "targets.json"
    manifest.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="manifest"):
        load_targets(manifest)


@pytest.mark.parametrize(
    "database_url",
    [
        "postgresql+asyncpg://tester@localhost/ticketcore",
        "postgresql+asyncpg://tester@localhost/ticketcore_test",
        "postgresql+asyncpg://tester@localhost/ticketcore_loadtest_backup",
        "sqlite+aiosqlite:///ticketcore_loadtest",
        "",
    ],
)
def test_seed_cli_rejects_non_dedicated_database(database_url: str) -> None:
    from load_tests.seed import validate_database_url

    with pytest.raises(ValueError, match="_loadtest"):
        validate_database_url(database_url)


def test_seed_cli_requires_explicit_database_url() -> None:
    from load_tests.seed import parse_args, validate_database_url

    with pytest.raises(SystemExit) as failure:
        parse_args([])
    assert failure.value.code == 2
    url = "postgresql+asyncpg://tester@localhost/ticketcore_loadtest"
    assert validate_database_url(url).database == "ticketcore_loadtest"


def test_locust_requires_secret_and_refreshes_before_token_expiry(
    tmp_path: Path,
) -> None:
    from load_tests.targets import LoadTestConfig, TokenProvider

    manifest = tmp_path / "targets.json"
    manifest.write_text(
        '{"event_id":9,"ticket_type_ids":[21],"user_ids":[90]}', encoding="utf-8"
    )
    environment = {"LOADTEST_MANIFEST": str(manifest)}
    with pytest.raises(ValueError, match="LOADTEST_SECRET_KEY"):
        LoadTestConfig.from_env(environment)
    environment["LOADTEST_SECRET_KEY"] = "test-signing-key-" * 3
    config = LoadTestConfig.from_env(environment)
    assert config.secret_key not in repr(config)
    provider = TokenProvider(90, config)
    now = datetime.now(UTC).timestamp()
    first = provider.authorization(now=now)
    assert provider.authorization(now=now + 1) == first
    refreshed = provider.authorization(now=now + 1741)
    assert refreshed != first
    claims = jwt.decode(
        refreshed.removeprefix("Bearer "),
        config.secret_key,
        algorithms=[config.algorithm],
    )
    assert claims["sub"] == "90"
    assert claims["exp"] == int(now + 1741 + config.token_ttl_seconds)


@pytest.mark.parametrize(
    "overrides",
    [
        {"LOADTEST_SECRET_KEY": " "},
        {"LOADTEST_JWT_ALGORITHM": "none"},
        {"LOADTEST_TOKEN_TTL_SECONDS": "0"},
        {"LOADTEST_TOKEN_TTL_SECONDS": "bad"},
        {"LOADTEST_TOKEN_REFRESH_SECONDS": "1800"},
        {"LOADTEST_TOKEN_REFRESH_SECONDS": "-1"},
    ],
)
def test_locust_rejects_invalid_auth_config(
    tmp_path: Path, overrides: dict[str, str]
) -> None:
    from load_tests.targets import LoadTestConfig

    manifest = tmp_path / "targets.json"
    manifest.write_text(
        '{"event_id":9,"ticket_type_ids":[21],"user_ids":[90]}', encoding="utf-8"
    )
    environment = {
        "LOADTEST_MANIFEST": str(manifest),
        "LOADTEST_SECRET_KEY": "test-signing-key-" * 3,
        **overrides,
    }
    with pytest.raises(ValueError, match="LOADTEST_"):
        LoadTestConfig.from_env(environment)


def test_locust_sends_one_ticket_and_a_new_idempotency_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("LOCUST_SKIP_MONKEY_PATCH", "1")
    monkeypatch.setenv("LOADTEST_SECRET_KEY", "test-signing-key-" * 3)
    manifest = tmp_path / "targets.json"
    manifest.write_text(
        '{"event_id":9,"ticket_type_ids":[21],"user_ids":[90]}', encoding="utf-8"
    )
    monkeypatch.setenv("LOADTEST_MANIFEST", str(manifest))
    locustfile = importlib.import_module("load_tests.locustfile")
    from locust.env import Environment

    requests: list[dict[str, Any]] = []

    class Response:
        status_code = 201

        def __enter__(self) -> "Response":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def success(self) -> None:
            pass

    class Client:
        def post(self, url: str, **kwargs: Any) -> Response:
            requests.append({"url": url, **kwargs})
            return Response()

        def get(self, url: str, **kwargs: Any) -> None:
            requests.append({"url": url, **kwargs})

    # CLI 的 runner 会把环境中的 host 写入用户类；这里直接构造用户时补上该步骤。
    monkeypatch.setattr(locustfile.RammsteinFan, "host", "http://localhost")
    user = locustfile.RammsteinFan(Environment(host="http://localhost"))
    user.client = Client()
    user.on_start()
    user.attempt_purchase()
    user.attempt_purchase()
    user.browse_event()
    assert [request["json"] for request in requests[:2]] == [
        {"ticket_type_id": 21},
        {"ticket_type_id": 21},
    ]
    keys = [request["headers"]["Idempotency-Key"] for request in requests[:2]]
    assert len(set(keys)) == 2
    assert all(10 <= len(key) <= 100 for key in keys)
    assert requests[2]["url"] == "/api/v1/events/9"
    for request in requests:
        claims = jwt.decode(
            request["headers"]["Authorization"].removeprefix("Bearer "),
            "test-signing-key-" * 3,
            algorithms=["HS256"],
        )
        assert claims["sub"] == "90"
