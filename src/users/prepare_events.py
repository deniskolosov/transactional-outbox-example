from abc import ABC, abstractmethod
from typing import Any

import structlog

from core.base_model import Model
from users.use_cases import UserCreated

from .models import EventType

logger = structlog.get_logger(__name__)

class EventRecordPreparer(ABC):
    @abstractmethod
    def prepare_record(self, event_context: dict[str, Any]) -> Model:
        pass

class UserCreatedPreparer(EventRecordPreparer):
    def prepare_record(self, event_context: dict[str, str]) -> UserCreated:
        return UserCreated(
            email=event_context['email'],
            first_name=event_context['first_name'],
            last_name=event_context['last_name'],
        )

# Extension point for future events
def get_event_preparer(event_type: str) -> EventRecordPreparer:
    if event_type == EventType.USER_CREATED:
        return UserCreatedPreparer()
    raise ValueError(f"Unsupported event type: {event_type}")
