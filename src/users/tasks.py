from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Any

import structlog
from celery import shared_task
from sentry_sdk import start_transaction

from core.base_model import Model
from core.event_log_client import EventLogClient
from users.use_cases import UserCreated

from .models import EventOutbox, EventType

logger = structlog.get_logger(__name__)

class EventRecordPreparer(ABC):
    @abstractmethod
    def prepare_record(self, event_context: dict[str, Any]) -> Model:
        pass

# Implement specific classes for each event type
class UserCreatedPreparer(EventRecordPreparer):
    def prepare_record(self, event_context: dict[str, str]) -> UserCreated:
        return UserCreated(
            email=event_context['email'],
            first_name=event_context['first_name'],
            last_name=event_context['last_name'],
        )

def get_event_preparer(event_type: str) -> EventRecordPreparer:
    if event_type == EventType.USER_CREATED:
        return UserCreatedPreparer()
    raise ValueError(f"Unsupported event type: {event_type}")

def prepare_clickhouse_record(event: dict[str, Any]) -> Model:
    event_context = event.get("event_context", {})
    event_type = event["event_type"]
    logger.debug('Preparing Clickhouse record for event: %s', event)
    preparer = get_event_preparer(event_type)
    return preparer.prepare_record(event_context)

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
