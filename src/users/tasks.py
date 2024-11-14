
import structlog
from celery import shared_task
from django.db import transaction
from sentry_sdk import start_transaction

from users.clickhouse import batch_insert_into_clickhouse

from .models import EventOutbox

logger = structlog.get_logger(__name__)

@shared_task
def process_event_outbox() -> None:
    with start_transaction(op="task", name="Process Event Outbox"):
        logger.info("Processing event outbox")
        try:
            with transaction.atomic():
                # Use skip_locked to avoid deadlocks
                unprocessed_events = EventOutbox.objects.select_for_update(
                    skip_locked=True,
                ).filter(processed=False).values("id", "event_context", "event_type")

                if not unprocessed_events.exists():
                    logger.info("No unprocessed events")
                    return
                logger.info(f"Found {unprocessed_events.count()} unprocessed events")
                batch_insert_into_clickhouse(list(unprocessed_events))

                EventOutbox.objects.filter(id__in=[event['id'] for event in unprocessed_events]).update(processed=True)
                logger.info(f"Marked {unprocessed_events.count()} events as processed")
        except Exception as overall_exception:
            logger.exception(f"Transaction rolled back, error: {overall_exception}")
