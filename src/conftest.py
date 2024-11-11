import datetime
from typing import Any

import clickhouse_connect
import pytest
from clickhouse_connect.driver import Client

from users.models import EventType
from users.use_cases.create_user import CreateUserRequest


@pytest.fixture(scope="module")
def f_ch_client() -> Client:
    client = clickhouse_connect.get_client(host="clickhouse")
    yield client
    client.close()


@pytest.fixture(autouse=True)
def user_context() -> dict[str, str]:
    return {"email": "test@email.com", "last_name": "Testovich", "first_name": "Test"}


@pytest.fixture(autouse=True)
def event(user_context: dict[str, str]) -> dict[str, Any]:
    return {
        "id": 222,
        "event_type": EventType.USER_CREATED,
        "event_date_time": datetime.datetime.now(tz=datetime.UTC),
        "environment": "test",
        "event_context": user_context,
        "metadata_version": 1,
        "processed": False,
    }


@pytest.fixture(autouse=True)
def create_user_request(user_context: dict[str, str]) -> CreateUserRequest:
    return CreateUserRequest(
        email=user_context["email"],
        first_name=user_context["first_name"],
        last_name=user_context["last_name"],
    )
