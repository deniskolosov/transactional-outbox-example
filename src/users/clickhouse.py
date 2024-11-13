from collections.abc import Sequence
from typing import Any

import structlog
from sentry_sdk import start_transaction

from core.base_model import Model
from core.event_log_client import EventLogClient
from users.prepare_events import get_event_preparer

logger = structlog.get_logger(__name__)

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
