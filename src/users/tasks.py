from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Any

import structlog
from celery import shared_task
from django.db import transaction
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
        logger.info("Processing event outbox")

        try:
            with transaction.atomic():
                # Use skip_locked to avoid deadlocks
                unprocessed_events = list(
                    EventOutbox.objects.select_for_update(
                        skip_locked=True,
                    ).filter(processed=False).values("id", "event_context", "event_type"),
                )
                if not unprocessed_events:
                    logger.info("No unprocessed events")
                    return
                logger.info(f"Found {len(unprocessed_events)} unprocessed events")
                try:
                    batch_insert_into_clickhouse(unprocessed_events)
                except Exception as batch_exc:
                    logger.exception(f"Failed to insert into CH, will not commit to EventOutbox: {batch_exc}")
                    # Rollback if exception raised
                    raise

                EventOutbox.objects.filter(id__in=[event['id'] for event in unprocessed_events]).update(processed=True)
                logger.info(f"Marked {len(unprocessed_events)} events as processed")
        except Exception as overall_exception:
            logger.exception(f"Transaction rolled back, error: {overall_exception}")
