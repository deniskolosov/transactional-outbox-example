import json
import uuid
from collections.abc import Generator
from typing import Any
from unittest.mock import ANY, patch

import pytest
from clickhouse_connect.driver import Client
from django.conf import settings
from django.db import IntegrityError

from core.event_log_client import EventLogClient
from users.clickhouse import prepare_clickhouse_record
from users.models import EventOutbox, EventType, User
from users.tasks import process_event_outbox
from users.use_cases import CreateUser, CreateUserRequest, UserCreated

pytestmark = [pytest.mark.django_db]


@pytest.fixture()
def f_use_case() -> CreateUser:
    return CreateUser()


@pytest.fixture(autouse=True)
def f_clean_up_event_log(f_ch_client: Client) -> Generator:
    f_ch_client.query(f'TRUNCATE TABLE {settings.CLICKHOUSE_EVENT_LOG_TABLE_NAME}')
    yield


def test_user_created(f_use_case: CreateUser) -> None:
    request = CreateUserRequest(
        email='test@email.com', first_name='Test', last_name='Testovich',
    )

    response = f_use_case.execute(request)

    assert response.result.email == 'test@email.com'
    assert response.error == ''


def test_emails_are_unique(f_use_case: CreateUser) -> None:
    request = CreateUserRequest(
        email='test@email.com', first_name='Test', last_name='Testovich',
    )

    f_use_case.execute(request)
    response = f_use_case.execute(request)

    assert response.result is None
    assert response.error == 'User with this email already exists'

def test_event_log_entry_published(
    f_use_case: CreateUser,
    f_ch_client: Client,
) -> None:
    email = f'test_{uuid.uuid4()}@email.com'
    request = CreateUserRequest(
        email=email, first_name='Test', last_name='Testovich',
    )

    f_use_case.execute(request)
    process_event_outbox()
    log = f_ch_client.query("SELECT * FROM default.event_log WHERE event_type = 'user_created'")

    assert log.result_rows == [
        (
            'user_created',
            ANY,
            'Local',
            UserCreated(email=email, first_name='Test', last_name='Testovich').model_dump_json(),
            1,
        ),
    ]


def test_create_user_success(create_user_request: CreateUserRequest,
                             user_context: dict[str, str]) -> None:
    use_case = CreateUser()
    _ = use_case.execute(create_user_request)
    outbox_record = EventOutbox.objects.first()
    assert outbox_record.event_type == EventType.USER_CREATED
    assert outbox_record.event_context == user_context

def test_create_user_atomicity(user_context: dict[str, str]) -> None:
    with patch('users.use_cases.create_user.EventOutbox.objects.create') as mock_outbox_create:
        request = CreateUserRequest(
            last_name=user_context['last_name'],
            email=user_context['email'],
            first_name=user_context['first_name'],
        )

        create_user_use_case = CreateUser()

        mock_outbox_create.side_effect = IntegrityError('Simulated failure in outbox entry creation')

        with pytest.raises(IntegrityError):
            create_user_use_case._execute(request)

        assert not User.objects.filter(email=user_context['email']).exists()
        assert not EventOutbox.objects.exists()

def test_process_event_outbox_success(user_context: dict[str, str]) -> None:
    event = EventOutbox.objects.create(
        event_type=EventType.USER_CREATED,
        environment='test',
        event_context=user_context,
        metadata_version=1,
        processed=False,
    )
    process_event_outbox()

    event.refresh_from_db()
    assert event.processed, "The outbox entry should be marked as processed."

def test_process_event_outbox_handles_exceptions(user_context: dict[str, str]) -> None:
    event = EventOutbox.objects.create(
        event_type=EventType.USER_CREATED,
        environment='test',
        event_context=user_context,
        metadata_version=1,
        processed=False,
    )

    with patch('users.tasks.batch_insert_into_clickhouse') as mock_batch_insert:
        mock_batch_insert.side_effect = Exception('Simulated failure in batch insertion')

        process_event_outbox()

        event.refresh_from_db()
        assert not event.processed, "The outbox entry should not be marked as processed."

def test_batch_insert_into_clickhouse(user_context: dict[str, str]) -> None:
    test_env = 'test'
    _ = EventOutbox.objects.create(
        event_type=EventType.USER_CREATED,
        environment=test_env,
        event_context=user_context,
        metadata_version=1,
        processed=False,
    )

    process_event_outbox()

    with EventLogClient.init() as client:
        query = f"SELECT event_context FROM {settings.CLICKHOUSE_EVENT_LOG_TABLE_NAME}"  # noqa: S608 potential SQL injection! Ignoring as this is toy example
        result = client.query(query)


    assert json.loads(result[0][0]) == user_context

def test_prepare_clickhouse_record(user_context: dict[str, str], event: dict[str, Any]) -> None:
    result = prepare_clickhouse_record(event)
    assert result == UserCreated(**user_context)

