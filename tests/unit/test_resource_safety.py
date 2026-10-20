from urllib.parse import quote

import pytest

from tests.resource_safety import TestResources


def resource_environment() -> dict[str, str]:
    return {
        "TICKETCORE_TEST_ALLOW_RESET": "1",
        "TICKETCORE_TEST_POSTGRES_DSN": (
            "postgresql://tester:password@127.0.0.1:55433/ticketcore_integration_test"
        ),
        "TICKETCORE_TEST_REDIS_URL": "redis://127.0.0.1:56379/0",
        "TICKETCORE_TEST_RESOURCE_TOKEN": "isolated-resource-token",
    }


@pytest.mark.parametrize("missing", list(resource_environment()))
def test_explicit_test_resources_are_required(missing: str) -> None:
    environment = resource_environment()
    del environment[missing]
    with pytest.raises(ValueError, match="专用测试"):
        TestResources.from_environment(environment)


@pytest.mark.parametrize(
    "dsn",
    [
        "postgresql://tester:password@127.0.0.1:55433/ticketcore",
        "postgresql://tester:password@127.0.0.1:55433/postgres",
        "postgresql://tester:password@127.0.0.1/ticketcore_test",
        "sqlite:///ticketcore_test",
        "postgresql://tester:password@127.0.0.1:55433/unsafe_test?options=x",
        'postgresql://tester:password@127.0.0.1:55433/unsafe"_test',
    ],
)
def test_unsafe_database_targets_are_rejected(dsn: str) -> None:
    environment = resource_environment()
    environment["TICKETCORE_TEST_POSTGRES_DSN"] = dsn
    with pytest.raises(ValueError, match="测试 PostgreSQL"):
        TestResources.from_environment(environment)


@pytest.mark.parametrize(
    "url",
    [
        "redis://127.0.0.1:56379/1",
        "redis://127.0.0.1/0",
        "redis://:password@127.0.0.1:56379/0",
        "http://127.0.0.1:56379/0",
    ],
)
def test_unsupported_redis_targets_are_rejected(url: str) -> None:
    environment = resource_environment()
    environment["TICKETCORE_TEST_REDIS_URL"] = url
    with pytest.raises(ValueError, match="测试 Redis"):
        TestResources.from_environment(environment)


def test_test_settings_override_business_endpoints() -> None:
    environment = resource_environment()
    environment.update(POSTGRES_DB="business", REDIS_PORT="6379")
    resources = TestResources.from_environment(environment)
    resources.configure_environment(environment)
    assert environment["ENVIRONMENT"] == "test"
    assert environment["POSTGRES_DB"] == "ticketcore_integration_test"
    assert environment["POSTGRES_PORT"] == "55433"
    assert environment["REDIS_PORT"] == "56379"
    assert resources.marker == "ticketcore-test:isolated-resource-token"


def test_encoded_credentials_are_decoded_without_leaking() -> None:
    environment = resource_environment()
    password = "password@with:punctuation"
    environment["TICKETCORE_TEST_POSTGRES_DSN"] = (
        f"postgresql://tester:{quote(password, safe='')}"
        "@127.0.0.1:55433/ticketcore_integration_test"
    )
    resources = TestResources.from_environment(environment)
    resources.configure_environment(environment)
    assert environment["POSTGRES_PASSWORD"] == password
    assert password not in repr(resources)


@pytest.mark.parametrize("marker", [None, "", "another-test-run"])
def test_unowned_resources_cannot_be_reset(marker: str | None) -> None:
    resources = TestResources.from_environment(resource_environment())
    with pytest.raises(RuntimeError, match="拒绝重置"):
        resources.require_marker(marker)


def test_control_marker_is_separate_from_flushed_business_database() -> None:
    resources = TestResources.from_environment(resource_environment())
    assert resources.redis_control_url == "redis://127.0.0.1:56379/15"
    resources.require_marker(resources.marker)
