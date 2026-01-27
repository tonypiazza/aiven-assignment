# ==============================================================================
# Clickstream Event Models
# ==============================================================================
"""
Pydantic models for clickstream events and sessions.

These models are used for:
- Validating data read from CSV
- Serializing/deserializing Kafka messages
- Type safety throughout the application
"""

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field


class EventType(str, Enum):
    """Event types matching the source data."""

    VIEW = "view"
    ADD_TO_CART = "addtocart"
    TRANSACTION = "transaction"


class ClickstreamEvent(BaseModel):
    """
    Represents a single clickstream event from the source data.

    Attributes:
        timestamp: Unix timestamp in milliseconds when the event occurred
        visitor_id: Unique identifier for the visitor
        event: Type of event (view, addtocart, transaction)
        item_id: Unique identifier for the item/product
        transaction_id: Transaction ID (only present for transaction events)
    """

    timestamp: int = Field(..., description="Unix timestamp in milliseconds")
    visitor_id: int = Field(..., alias="visitorid", description="Visitor identifier")
    event: EventType = Field(..., description="Event type")
    item_id: int = Field(..., alias="itemid", description="Item/product identifier")
    transaction_id: int | None = Field(
        None, alias="transactionid", description="Transaction ID (nullable)"
    )

    model_config = {"populate_by_name": True}

    @property
    def event_time(self) -> datetime:
        """Convert timestamp to datetime object."""
        return datetime.fromtimestamp(self.timestamp / 1000.0)

    def to_kafka_message(self) -> dict:
        """Serialize event for Kafka message value."""
        return {
            "timestamp": self.timestamp,
            "visitor_id": self.visitor_id,
            "event": self.event.value,
            "item_id": self.item_id,
            "transaction_id": self.transaction_id,
        }

    @classmethod
    def from_kafka_message(cls, data: dict) -> "ClickstreamEvent":
        """Deserialize event from Kafka message value."""
        return cls(
            timestamp=data["timestamp"],
            visitorid=data["visitor_id"],
            event=data["event"],
            itemid=data["item_id"],
            transactionid=data.get("transaction_id"),
        )


class Session(BaseModel):
    """
    Represents an aggregated user session.

    A session is a sequence of events from the same visitor within a
    configurable inactivity timeout period.

    Attributes:
        visitor_id: Unique identifier for the visitor
        session_start: Timestamp of first event in session
        session_end: Timestamp of last event in session
        events: List of events in this session
    """

    visitor_id: int = Field(..., description="Visitor identifier")
    session_start: datetime = Field(..., description="Session start time")
    session_end: datetime = Field(..., description="Session end time")
    events: list[ClickstreamEvent] = Field(
        default_factory=list, description="Events in this session"
    )

    @property
    def duration_seconds(self) -> int:
        """Calculate session duration in seconds."""
        return int((self.session_end - self.session_start).total_seconds())

    @property
    def event_count(self) -> int:
        """Total number of events in session."""
        return len(self.events)

    @property
    def view_count(self) -> int:
        """Number of view events in session."""
        return sum(1 for e in self.events if e.event == EventType.VIEW)

    @property
    def cart_count(self) -> int:
        """Number of add-to-cart events in session."""
        return sum(1 for e in self.events if e.event == EventType.ADD_TO_CART)

    @property
    def transaction_count(self) -> int:
        """Number of transaction events in session."""
        return sum(1 for e in self.events if e.event == EventType.TRANSACTION)

    @property
    def items_viewed(self) -> list[int]:
        """List of unique item IDs viewed in session."""
        return list({e.item_id for e in self.events if e.event == EventType.VIEW})

    @property
    def items_carted(self) -> list[int]:
        """List of unique item IDs added to cart in session."""
        return list({e.item_id for e in self.events if e.event == EventType.ADD_TO_CART})

    @property
    def items_purchased(self) -> list[int]:
        """List of unique item IDs purchased in session."""
        return list({e.item_id for e in self.events if e.event == EventType.TRANSACTION})

    def to_db_record(self) -> dict:
        """Convert session to database record format."""
        return {
            "visitor_id": self.visitor_id,
            "session_start": self.session_start,
            "session_end": self.session_end,
            "duration_seconds": self.duration_seconds,
            "event_count": self.event_count,
            "view_count": self.view_count,
            "cart_count": self.cart_count,
            "transaction_count": self.transaction_count,
            "items_viewed": self.items_viewed,
            "items_carted": self.items_carted,
            "items_purchased": self.items_purchased,
        }
