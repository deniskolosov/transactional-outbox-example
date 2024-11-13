
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
