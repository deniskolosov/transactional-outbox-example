from collections.abc import Sequence
from typing import Any

import structlog
from celery import shared_task
from sentry_sdk import start_transaction

from core.event_log_client import EventLogClient
from users.use_cases import UserCreated

from .models import EventOutbox, EventType

logger = structlog.get_logger(__name__)

def prepare_clickhouse_record(event: dict[str, Any]) -> UserCreated:
    event_context = event.get("event_context", {})
    logger.debug('Preparing Clickhouse record for event: %s', event)
    if event.get('event_type') == EventType.USER_CREATED:
        return UserCreated(
            email=event_context.get('email'),
            first_name=event_context.get('first_name'),
            last_name=event_context.get('last_name'),
        )

def batch_insert_into_clickhouse(events: Sequence[dict[str, Any]], chunk_size: int = 1000) -> None:
    with start_transaction(op="task", name="Batch Insert Into Clickhouse"):
        logger.info('Batch inserting into Clickhouse: %d events', len(events))
        with EventLogClient.init() as client:
            prepared_events = [prepare_clickhouse_record(event) for event in events]
            res = client.insert(data=prepared_events, chunk_size=chunk_size)
            logger.info('Successfully inserted %d events into Clickhouse', len(prepared_events))
            return res

@shared_task
def process_event_outbox() -> None:
    with start_transaction(op="task", name="Process Event Outbox"):
        logger.info('Processing event outbox')
        unprocessed_events = EventOutbox.objects.filter(processed=False)

        if not unprocessed_events.exists():
            logger.info('No unprocessed events found')
            return

        events_to_process = list(unprocessed_events.values())
        logger.info('Found %d unprocessed events', len(events_to_process))

        try:
            batch_insert_into_clickhouse(events_to_process)
            unprocessed_events.update(processed=True)
            logger.info('Successfully marked %d events as processed', len(events_to_process))
        except Exception as e:
            logger.error('Error during batch insertion into Clickhouse: %s', str(e))
